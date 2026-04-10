use super::*;

impl SchemaValidator {
    /// Build the IR from the validated AST.
    pub(super) fn build_ir(self) -> Result<SchemaIr> {
        let mut ir = SchemaIr::new();

        if let Some(datasource) = self.schema.datasource() {
            ir.datasource = Some(self.build_datasource_ir(datasource)?);
        }

        if let Some(generator) = self.schema.generator() {
            ir.generator = Some(self.build_generator_ir(generator)?);
        }

        for enum_decl in self.schema.enums() {
            let enum_ir = self.build_enum_ir(enum_decl);
            ir.enums.insert(enum_ir.logical_name.clone(), enum_ir);
        }

        for type_decl in self.schema.types() {
            let composite_ir = self.build_composite_type_ir(type_decl)?;
            ir.composite_types
                .insert(composite_ir.logical_name.clone(), composite_ir);
        }

        for model in self.schema.models() {
            let model_ir = self.build_model_ir(model)?;
            ir.models.insert(model_ir.logical_name.clone(), model_ir);
        }

        Ok(ir)
    }

    pub(super) fn build_datasource_ir(&self, datasource: &DatasourceDecl) -> Result<DatasourceIr> {
        let provider = Self::datasource_provider_value(datasource)?;
        let url = Self::datasource_url_value(datasource)?;
        let direct_url = Self::datasource_direct_url_value(datasource)?;

        Ok(DatasourceIr {
            name: datasource.name.value.clone(),
            provider,
            url,
            direct_url,
            span: datasource.span,
        })
    }

    pub(super) fn build_generator_ir(&self, generator: &GeneratorDecl) -> Result<GeneratorIr> {
        let (provider, client_provider) = Self::generator_provider_info(generator)?;
        let output = Self::generator_output_value(generator)?;
        let interface = Self::generator_interface_kind(generator)?;
        let recursive_type_depth =
            Self::generator_recursive_type_depth(generator, client_provider, &provider)?;

        Ok(GeneratorIr {
            name: generator.name.value.clone(),
            provider,
            output,
            interface,
            recursive_type_depth,
            span: generator.span,
        })
    }

    pub(super) fn build_enum_ir(&self, enum_decl: &EnumDecl) -> EnumIr {
        EnumIr {
            logical_name: enum_decl.name.value.clone(),
            variants: enum_decl
                .variants
                .iter()
                .map(|v| v.name.value.clone())
                .collect(),
            span: enum_decl.span,
        }
    }

    pub(super) fn build_composite_type_ir(&self, type_decl: &TypeDecl) -> Result<CompositeTypeIr> {
        let fields = type_decl
            .fields
            .iter()
            .map(|f| {
                let logical_name = f.name.value.clone();
                let db_name = f.column_name().to_string();
                let field_type = self.resolve_field_type(f)?;
                let is_required = !f.is_optional() && !f.is_array();
                let is_array = f.is_array();
                let storage_strategy = f.attributes.iter().find_map(|attr| {
                    if let FieldAttribute::Store { strategy, .. } = attr {
                        Some(*strategy)
                    } else {
                        None
                    }
                });
                Ok(CompositeFieldIr {
                    logical_name,
                    db_name,
                    field_type,
                    is_required,
                    is_array,
                    storage_strategy,
                    span: f.span,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(CompositeTypeIr {
            logical_name: type_decl.name.value.clone(),
            fields,
            span: type_decl.span,
        })
    }

    pub(super) fn build_model_ir(&self, model: &ModelDecl) -> Result<ModelIr> {
        let logical_name = model.name.value.clone();
        let db_name = model.table_name().to_string();

        let fields = model
            .fields
            .iter()
            .map(|f| self.build_field_ir(f, model))
            .collect::<Result<Vec<_>>>()?;

        let primary_key = self.build_primary_key_ir(model);
        let unique_constraints = self.build_unique_constraints(model);
        let indexes = self.build_indexes(model);

        let field_name_map: std::collections::HashMap<String, String> = model
            .fields
            .iter()
            .map(|f| (f.name.value.clone(), f.column_name().to_string()))
            .collect();

        let check_constraints: Vec<String> = model
            .attributes
            .iter()
            .filter_map(|attr| match attr {
                ModelAttribute::Check { expr, .. } => Some(expr.to_sql_mapped(&|name: &str| {
                    field_name_map
                        .get(name)
                        .cloned()
                        .unwrap_or_else(|| name.to_string())
                })),
                _ => None,
            })
            .collect();

        Ok(ModelIr {
            logical_name,
            db_name,
            fields,
            primary_key,
            unique_constraints,
            indexes,
            check_constraints,
            span: model.span,
        })
    }

    pub(super) fn build_field_ir(&self, field: &FieldDecl, model: &ModelDecl) -> Result<FieldIr> {
        let logical_name = field.name.value.clone();
        let db_name = field.column_name().to_string();
        let field_type = self.resolve_field_type(field)?;
        let is_required = !field.is_optional() && !field.is_array();
        let is_array = field.is_array();
        let default_value = self.extract_default_value(field)?;
        let is_unique = field
            .attributes
            .iter()
            .any(|a| matches!(a, FieldAttribute::Unique));

        let is_updated_at = field
            .attributes
            .iter()
            .any(|a| matches!(a, FieldAttribute::UpdatedAt { .. }));

        let field_name_map: std::collections::HashMap<String, String> = model
            .fields
            .iter()
            .map(|f| (f.name.value.clone(), f.column_name().to_string()))
            .collect();

        let computed = field.attributes.iter().find_map(|a| {
            if let FieldAttribute::Computed { expr, kind, .. } = a {
                Some((
                    expr.to_sql_mapped(&|name: &str| {
                        field_name_map
                            .get(name)
                            .cloned()
                            .unwrap_or_else(|| name.to_string())
                    }),
                    *kind,
                ))
            } else {
                None
            }
        });

        let check = field.attributes.iter().find_map(|a| {
            if let FieldAttribute::Check { expr, .. } = a {
                Some(expr.to_sql_mapped(&|name: &str| {
                    field_name_map
                        .get(name)
                        .cloned()
                        .unwrap_or_else(|| name.to_string())
                }))
            } else {
                None
            }
        });

        let storage_strategy = field.attributes.iter().find_map(|attr| {
            if let FieldAttribute::Store { strategy, .. } = attr {
                Some(*strategy)
            } else {
                None
            }
        });

        if field.is_not_null() && matches!(field_type, ResolvedFieldType::Relation(_)) {
            return Err(SchemaError::Validation(
                "The `!` modifier cannot be used on relation fields — NOT NULL applies only to scalar and enum columns.".to_string(),
                field.span,
            ));
        }

        let datasource_provider = self
            .schema
            .datasource()
            .and_then(|datasource| datasource.provider());
        self.validate_composite_storage_strategy(
            field,
            &field_type,
            storage_strategy,
            datasource_provider,
        )?;
        self.validate_array_storage_strategy(
            field,
            &field_type,
            is_array,
            storage_strategy,
            datasource_provider,
        )?;

        Ok(FieldIr {
            logical_name,
            db_name,
            field_type,
            is_required,
            is_array,
            storage_strategy,
            default_value,
            is_unique,
            is_updated_at,
            computed,
            check,
            span: field.span,
        })
    }

    fn validate_composite_storage_strategy(
        &self,
        field: &FieldDecl,
        field_type: &ResolvedFieldType,
        storage_strategy: Option<StorageStrategy>,
        datasource_provider: Option<&str>,
    ) -> Result<()> {
        if !matches!(field_type, ResolvedFieldType::CompositeType { .. }) {
            return Ok(());
        }

        let Some(provider_str) = datasource_provider else {
            return Ok(());
        };

        match provider_str.parse::<DatabaseProvider>() {
            Ok(DatabaseProvider::Postgres) => {
                if storage_strategy == Some(StorageStrategy::Json) {
                    return Err(SchemaError::Validation(
                        "PostgreSQL supports native composite types. Remove @store(Json) from this field.".to_string(),
                        field.span,
                    ));
                }
            }
            Ok(db_provider @ (DatabaseProvider::Mysql | DatabaseProvider::Sqlite)) => {
                if storage_strategy.is_none() {
                    return Err(SchemaError::Validation(
                        format!(
                            "{} does not support native composite types. Add @store(Json) to store as JSON.",
                            db_provider.display_name()
                        ),
                        field.span,
                    ));
                }
                if storage_strategy == Some(StorageStrategy::Native) {
                    return Err(SchemaError::Validation(
                        format!(
                            "{} does not support native composite types. Use @store(Json) instead.",
                            db_provider.display_name()
                        ),
                        field.span,
                    ));
                }
            }
            Err(_) => {
                if storage_strategy.is_none() {
                    return Err(SchemaError::Validation(
                        "Composite type fields require explicit storage strategy via @store(Json) or are only natively supported on PostgreSQL.".to_string(),
                        field.span,
                    ));
                }
            }
        }

        Ok(())
    }

    fn validate_array_storage_strategy(
        &self,
        field: &FieldDecl,
        field_type: &ResolvedFieldType,
        is_array: bool,
        storage_strategy: Option<StorageStrategy>,
        datasource_provider: Option<&str>,
    ) -> Result<()> {
        if !is_array || !matches!(field_type, ResolvedFieldType::Scalar(_)) {
            return Ok(());
        }

        let Some(provider_str) = datasource_provider else {
            return Ok(());
        };

        match provider_str.parse::<DatabaseProvider>() {
            Ok(DatabaseProvider::Postgres) => {
                if storage_strategy == Some(StorageStrategy::Json) {
                    return Err(SchemaError::Validation(
                        "PostgreSQL supports native arrays. Use @store(native) or omit @store attribute.".to_string(),
                        field.span,
                    ));
                }
            }
            Ok(db_provider @ (DatabaseProvider::Mysql | DatabaseProvider::Sqlite)) => {
                if storage_strategy.is_none() {
                    return Err(SchemaError::Validation(
                        format!(
                            "{} does not support native array types. Add @store(json) to use JSON serialization for array fields.",
                            db_provider.display_name()
                        ),
                        field.span,
                    ));
                }
                if storage_strategy == Some(StorageStrategy::Native) {
                    return Err(SchemaError::Validation(
                        format!(
                            "{} does not support native array types. Use @store(json) instead.",
                            db_provider.display_name()
                        ),
                        field.span,
                    ));
                }
            }
            Err(_) => {
                if storage_strategy.is_none() {
                    return Err(SchemaError::Validation(
                        "Array fields require explicit storage strategy via @store(json) or @store(native)".to_string(),
                        field.span,
                    ));
                }
            }
        }

        Ok(())
    }

    pub(super) fn resolve_field_type(&self, field: &FieldDecl) -> Result<ResolvedFieldType> {
        match &field.field_type {
            FieldType::String => Ok(ResolvedFieldType::Scalar(ScalarType::String)),
            FieldType::Boolean => Ok(ResolvedFieldType::Scalar(ScalarType::Boolean)),
            FieldType::Int => Ok(ResolvedFieldType::Scalar(ScalarType::Int)),
            FieldType::BigInt => Ok(ResolvedFieldType::Scalar(ScalarType::BigInt)),
            FieldType::Float => Ok(ResolvedFieldType::Scalar(ScalarType::Float)),
            FieldType::Decimal { precision, scale } => {
                Ok(ResolvedFieldType::Scalar(ScalarType::Decimal {
                    precision: *precision,
                    scale: *scale,
                }))
            }
            FieldType::DateTime => Ok(ResolvedFieldType::Scalar(ScalarType::DateTime)),
            FieldType::Bytes => Ok(ResolvedFieldType::Scalar(ScalarType::Bytes)),
            FieldType::Json => Ok(ResolvedFieldType::Scalar(ScalarType::Json)),
            FieldType::Uuid => Ok(ResolvedFieldType::Scalar(ScalarType::Uuid)),
            FieldType::Jsonb => Ok(ResolvedFieldType::Scalar(ScalarType::Jsonb)),
            FieldType::Xml => Ok(ResolvedFieldType::Scalar(ScalarType::Xml)),
            FieldType::Char { length } => Ok(ResolvedFieldType::Scalar(ScalarType::Char {
                length: *length,
            })),
            FieldType::VarChar { length } => Ok(ResolvedFieldType::Scalar(ScalarType::VarChar {
                length: *length,
            })),
            FieldType::UserType(type_name) => {
                if self.enums.contains_key(type_name) {
                    return Ok(ResolvedFieldType::Enum {
                        enum_name: type_name.clone(),
                    });
                }

                if self.composite_types.contains_key(type_name) {
                    return Ok(ResolvedFieldType::CompositeType {
                        type_name: type_name.clone(),
                    });
                }

                if self.models.contains_key(type_name) {
                    for attr in &field.attributes {
                        if let FieldAttribute::Relation {
                            name,
                            fields,
                            references,
                            on_delete,
                            on_update,
                            ..
                        } = attr
                        {
                            return Ok(ResolvedFieldType::Relation(RelationIr {
                                name: name.clone(),
                                target_model: type_name.clone(),
                                fields: fields
                                    .as_ref()
                                    .map(|f| f.iter().map(|i| i.value.clone()).collect())
                                    .unwrap_or_default(),
                                references: references
                                    .as_ref()
                                    .map(|r| r.iter().map(|i| i.value.clone()).collect())
                                    .unwrap_or_default(),
                                on_delete: *on_delete,
                                on_update: *on_update,
                            }));
                        }
                    }

                    return Ok(ResolvedFieldType::Relation(RelationIr {
                        name: None,
                        target_model: type_name.clone(),
                        fields: vec![],
                        references: vec![],
                        on_delete: None,
                        on_update: None,
                    }));
                }

                Err(SchemaError::Validation(
                    format!("Unknown type '{}'", type_name),
                    field.span,
                ))
            }
        }
    }

    pub(super) fn extract_default_value(&self, field: &FieldDecl) -> Result<Option<DefaultValue>> {
        for attr in &field.attributes {
            if let FieldAttribute::Default(expr, _) = attr {
                return Ok(Some(self.expr_to_default_value(expr)?));
            }
        }
        Ok(None)
    }

    pub(super) fn expr_to_default_value(&self, expr: &Expr) -> Result<DefaultValue> {
        match expr {
            Expr::Literal(Literal::String(s, _)) => Ok(DefaultValue::String(s.clone())),
            Expr::Literal(Literal::Number(n, _)) => Ok(DefaultValue::Number(n.clone())),
            Expr::Literal(Literal::Boolean(b, _)) => Ok(DefaultValue::Boolean(*b)),
            Expr::Ident(ident) => Ok(DefaultValue::EnumVariant(ident.value.clone())),
            Expr::FunctionCall { name, args, .. } => Ok(DefaultValue::Function(FunctionCall {
                name: name.value.clone(),
                args: args
                    .iter()
                    .filter_map(|arg| {
                        if let Expr::Literal(Literal::String(s, _)) = arg {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .collect(),
            })),
            _ => Err(SchemaError::Validation(
                "Unsupported default value expression".to_string(),
                expr.span(),
            )),
        }
    }

    pub(super) fn build_primary_key_ir(&self, model: &ModelDecl) -> PrimaryKeyIr {
        for attr in &model.attributes {
            if let ModelAttribute::Id(fields) = attr {
                let field_names = fields.iter().map(|f| f.value.clone()).collect();
                return PrimaryKeyIr::Composite(field_names);
            }
        }

        for field in &model.fields {
            for attr in &field.attributes {
                if matches!(attr, FieldAttribute::Id) {
                    return PrimaryKeyIr::Single(field.name.value.clone());
                }
            }
        }

        if let Some(first_field) = model.fields.first() {
            PrimaryKeyIr::Single(first_field.name.value.clone())
        } else {
            PrimaryKeyIr::Composite(vec![])
        }
    }

    pub(super) fn build_unique_constraints(&self, model: &ModelDecl) -> Vec<UniqueConstraintIr> {
        let mut constraints = Vec::new();

        for field in &model.fields {
            for attr in &field.attributes {
                if matches!(attr, FieldAttribute::Unique) {
                    constraints.push(UniqueConstraintIr {
                        fields: vec![field.name.value.clone()],
                    });
                }
            }
        }

        for attr in &model.attributes {
            if let ModelAttribute::Unique(fields) = attr {
                constraints.push(UniqueConstraintIr {
                    fields: fields.iter().map(|f| f.value.clone()).collect(),
                });
            }
        }

        constraints
    }

    pub(super) fn build_indexes(&self, model: &ModelDecl) -> Vec<IndexIr> {
        let mut indexes = Vec::new();

        for attr in &model.attributes {
            if let ModelAttribute::Index {
                fields,
                index_type,
                name,
                map,
            } = attr
            {
                let index_type_ir = index_type
                    .as_ref()
                    .and_then(|t| t.value.parse::<IndexType>().ok());
                indexes.push(IndexIr {
                    fields: fields.iter().map(|f| f.value.clone()).collect(),
                    index_type: index_type_ir,
                    name: name.clone(),
                    map: map.clone(),
                });
            }
        }

        indexes
    }
}
