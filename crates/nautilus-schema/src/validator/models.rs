use super::*;

impl SchemaValidator {
    pub(super) fn validate_models(&mut self) {
        let models: Vec<_> = self.schema.models().cloned().collect();
        for model in &models {
            self.validate_model(model);
        }
    }

    pub(super) fn validate_model(&mut self, model: &ModelDecl) {
        let mut field_names = HashMap::new();
        for field in &model.fields {
            let name = &field.name.value;
            if field_names.contains_key(name) {
                self.errors.push_back(SchemaError::Validation(
                    format!(
                        "Duplicate field name '{}' in model '{}'",
                        name, model.name.value
                    ),
                    field.name.span,
                ));
            } else {
                field_names.insert(name.clone(), field.name.span);
            }

            self.validate_field_type(field, &model.name.value);

            if let FieldType::Decimal { precision, scale } = field.field_type {
                if precision == 0 {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Decimal precision must be greater than 0, got {}",
                            precision
                        ),
                        field.span,
                    ));
                }
                if scale > precision {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Decimal scale ({}) cannot exceed precision ({})",
                            scale, precision
                        ),
                        field.span,
                    ));
                }
            }
        }

        if model.has_composite_key() {
            for attr in &model.attributes {
                if let ModelAttribute::Id(fields) = attr {
                    for field_ident in fields {
                        match model.find_field(&field_ident.value) {
                            Some(field) => {
                                if field.is_array() {
                                    self.errors.push_back(SchemaError::Validation(
                                        format!(
                                            "Composite primary key field '{}' cannot be an array",
                                            field_ident.value
                                        ),
                                        field_ident.span,
                                    ));
                                }
                                if matches!(field.field_type, FieldType::UserType(_))
                                    && field.has_relation_attribute()
                                {
                                    self.errors.push_back(SchemaError::Validation(
                                        format!(
                                            "Composite primary key field '{}' cannot be a relation",
                                            field_ident.value
                                        ),
                                        field_ident.span,
                                    ));
                                }
                            }
                            None => {
                                self.errors.push_back(SchemaError::Validation(
                                    format!(
                                        "@@id references non-existent field '{}' in model '{}'",
                                        field_ident.value, model.name.value
                                    ),
                                    field_ident.span,
                                ));
                            }
                        }
                    }
                }
            }
        }

        for attr in &model.attributes {
            if let ModelAttribute::Unique(fields) = attr {
                for field_ident in fields {
                    if model.find_field(&field_ident.value).is_none() {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "@@unique references non-existent field '{}' in model '{}'",
                                field_ident.value, model.name.value
                            ),
                            field_ident.span,
                        ));
                    }
                }
            }
        }

        let provider = self.schema.datasource().and_then(|ds| ds.provider());
        for attr in &model.attributes {
            if let ModelAttribute::Index {
                fields, index_type, ..
            } = attr
            {
                for field_ident in fields {
                    if model.find_field(&field_ident.value).is_none() {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "@@index references non-existent field '{}' in model '{}'",
                                field_ident.value, model.name.value
                            ),
                            field_ident.span,
                        ));
                    }
                }
                if let Some(type_ident) = index_type {
                    match type_ident.value.parse::<IndexType>() {
                        Err(_) => {
                            self.errors.push_back(SchemaError::Validation(
                                format!(
                                    "Unknown index type '{}'. Valid types are: BTree, Hash, Gin, Gist, Brin, FullText",
                                    type_ident.value
                                ),
                                type_ident.span,
                            ));
                        }
                        Ok(idx_type) => {
                            if let Some(prov) = provider {
                                if let Ok(db_provider) = prov.parse::<DatabaseProvider>() {
                                    if !idx_type.supported_by(db_provider) {
                                        self.errors.push_back(SchemaError::Validation(
                                            format!(
                                                "Index type '{}' is not supported by provider '{}' (supported by: {})",
                                                idx_type.as_str(),
                                                prov,
                                                idx_type.supported_providers()
                                            ),
                                            type_ident.span,
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    pub(super) fn validate_field_type(&mut self, field: &FieldDecl, model_name: &str) {
        if let FieldType::UserType(type_name) = &field.field_type {
            if !self.models.contains_key(type_name)
                && !self.enums.contains_key(type_name)
                && !self.composite_types.contains_key(type_name)
            {
                self.errors.push_back(SchemaError::Validation(
                    format!(
                        "Unknown type '{}' for field '{}' in model '{}'",
                        type_name, field.name.value, model_name
                    ),
                    field.span,
                ));
            }
        }

        let provider = self.schema.datasource().and_then(|ds| ds.provider());
        if let Some(prov) = provider {
            if let Ok(db_provider) = prov.parse::<DatabaseProvider>() {
                let scalar_opt: Option<ScalarType> = match &field.field_type {
                    FieldType::Citext => Some(ScalarType::Citext),
                    FieldType::Hstore => Some(ScalarType::Hstore),
                    FieldType::Ltree => Some(ScalarType::Ltree),
                    FieldType::Jsonb => Some(ScalarType::Jsonb),
                    FieldType::Xml => Some(ScalarType::Xml),
                    FieldType::Char { length } => Some(ScalarType::Char { length: *length }),
                    FieldType::VarChar { length } => Some(ScalarType::VarChar { length: *length }),
                    _ => None,
                };
                if let Some(scalar) = scalar_opt {
                    if !scalar.supported_by(db_provider) {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "Type '{}' is not supported by provider '{}' (supported by: {})",
                                field.field_type,
                                prov,
                                scalar.supported_providers()
                            ),
                            field.span,
                        ));
                    }
                }

                if db_provider == DatabaseProvider::Postgres {
                    let required_extension = match &field.field_type {
                        FieldType::Citext => Some("citext"),
                        FieldType::Hstore => Some("hstore"),
                        FieldType::Ltree => Some("ltree"),
                        _ => None,
                    };

                    if let Some(required_extension) = required_extension {
                        let declared_extensions = self
                            .schema
                            .datasource()
                            .map(Self::datasource_extensions_value)
                            .unwrap_or_default();

                        if !declared_extensions
                            .iter()
                            .any(|ext| ext == required_extension)
                        {
                            self.warnings.push_back(SchemaError::Warning(
                                format!(
                                    "Field '{}' in model '{}' uses type '{}' which relies on the PostgreSQL '{}' extension. Consider adding `extensions = [{}]` to the datasource for reproducible migrations.",
                                    field.name.value,
                                    model_name,
                                    field.field_type,
                                    required_extension,
                                    required_extension
                                ),
                                field.span,
                            ));
                        }
                    }
                }
            }
        }
    }

    pub(super) fn validate_updated_at_fields(&mut self) {
        let models: Vec<_> = self.schema.models().cloned().collect();
        for model in &models {
            for field in &model.fields {
                let updated_at_span = field.attributes.iter().find_map(|a| {
                    if let FieldAttribute::UpdatedAt { span } = a {
                        Some(*span)
                    } else {
                        None
                    }
                });

                let Some(updated_at_span) = updated_at_span else {
                    continue;
                };

                if field.field_type != FieldType::DateTime {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "@updatedAt can only be applied to DateTime fields, \
                             but field '{}' in model '{}' has a different type",
                            field.name.value, model.name.value
                        ),
                        updated_at_span,
                    ));
                }

                if field
                    .attributes
                    .iter()
                    .any(|a| matches!(a, FieldAttribute::Id))
                {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Field '{}' in model '{}' cannot be both @id and @updatedAt",
                            field.name.value, model.name.value
                        ),
                        updated_at_span,
                    ));
                }

                if let Some(default_span) = field.attributes.iter().find_map(|a| {
                    if let FieldAttribute::Default(_, span) = a {
                        Some(*span)
                    } else {
                        None
                    }
                }) {
                    self.warnings.push_back(SchemaError::Warning(
                        format!(
                            "Field '{}' in model '{}' has both @updatedAt and @default. \
                             The @default value is redundant because @updatedAt always \
                             overrides it on every write",
                            field.name.value, model.name.value
                        ),
                        default_span,
                    ));
                }
            }
        }
    }

    pub(super) fn validate_computed_fields(&mut self) {
        let provider: Option<DatabaseProvider> = self
            .schema
            .datasource()
            .and_then(|ds| ds.provider())
            .and_then(|p| p.parse::<DatabaseProvider>().ok());

        let models: Vec<_> = self.schema.models().cloned().collect();
        for model in &models {
            for field in &model.fields {
                let computed_attr = field.attributes.iter().find_map(|a| {
                    if let FieldAttribute::Computed { kind, span, .. } = a {
                        Some((*kind, *span))
                    } else {
                        None
                    }
                });

                let Some((kind, span)) = computed_attr else {
                    continue;
                };

                if field
                    .attributes
                    .iter()
                    .any(|a| matches!(a, FieldAttribute::Id))
                {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Field '{}' in model '{}' cannot be both @id and @computed",
                            field.name.value, model.name.value
                        ),
                        span,
                    ));
                }

                if field
                    .attributes
                    .iter()
                    .any(|a| matches!(a, FieldAttribute::Default(_, _)))
                {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Field '{}' in model '{}' has both @computed and @default; \
                             computed columns derive their value from the expression",
                            field.name.value, model.name.value
                        ),
                        span,
                    ));
                }

                if field
                    .attributes
                    .iter()
                    .any(|a| matches!(a, FieldAttribute::UpdatedAt { .. }))
                {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Field '{}' in model '{}' cannot be both @updatedAt and @computed",
                            field.name.value, model.name.value
                        ),
                        span,
                    ));
                }

                if kind == ComputedKind::Virtual
                    && matches!(provider, Some(DatabaseProvider::Postgres))
                {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Field '{}' in model '{}': Virtual computed columns are not \
                             supported by PostgreSQL. Use Stored instead.",
                            field.name.value, model.name.value
                        ),
                        span,
                    ));
                }

                if field.is_array() {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Field '{}' in model '{}': @computed cannot be applied to array fields",
                            field.name.value, model.name.value
                        ),
                        span,
                    ));
                }
            }
        }
    }

    /// Validate `@check` / `@@check` constraints.
    pub(super) fn validate_check_constraints(&mut self) {
        let models: Vec<_> = self.schema.models().cloned().collect();
        let enum_decls: HashMap<String, Vec<String>> = self
            .schema
            .enums()
            .map(|e| {
                (
                    e.name.value.clone(),
                    e.variants.iter().map(|v| v.name.value.clone()).collect(),
                )
            })
            .collect();

        for model in &models {
            let scalar_fields: Vec<&str> = model
                .fields
                .iter()
                .filter(|f| {
                    !matches!(f.field_type, FieldType::UserType(ref name) if self.models.contains_key(name))
                })
                .map(|f| f.name.value.as_str())
                .collect();

            let field_enum_map: HashMap<&str, &str> = model
                .fields
                .iter()
                .filter_map(|field| match &field.field_type {
                    FieldType::UserType(type_name) if self.enums.contains_key(type_name) => {
                        Some((field.name.value.as_str(), type_name.as_str()))
                    }
                    _ => None,
                })
                .collect();

            for field in &model.fields {
                for attr in &field.attributes {
                    let (expr, span) = match attr {
                        FieldAttribute::Check { expr, span } => (expr, *span),
                        _ => continue,
                    };

                    if field
                        .attributes
                        .iter()
                        .any(|a| matches!(a, FieldAttribute::Computed { .. }))
                    {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "Field '{}' in model '{}' cannot have both @computed and @check",
                                field.name.value, model.name.value
                            ),
                            span,
                        ));
                    }

                    let is_relation_field = matches!(
                        &field.field_type,
                        FieldType::UserType(type_name) if self.models.contains_key(type_name)
                    );
                    if is_relation_field {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "Field '{}' in model '{}': @check cannot be applied to relation fields",
                                field.name.value, model.name.value
                            ),
                            span,
                        ));
                    }

                    if field.is_array() {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "Field '{}' in model '{}': @check cannot be applied to array fields",
                                field.name.value, model.name.value
                            ),
                            span,
                        ));
                    }

                    let refs = expr.field_references();
                    for r in &refs {
                        if *r != field.name.value {
                            self.errors.push_back(SchemaError::Validation(
                                format!(
                                    "Field-level @check on '{}' in model '{}' can only reference \
                                     its own field, but references '{}'",
                                    field.name.value, model.name.value, r
                                ),
                                span,
                            ));
                        }
                    }

                    for (field_name, variants) in expr.enum_in_lists() {
                        let Some(enum_name) = field_enum_map.get(field_name) else {
                            continue;
                        };
                        let Some(enum_variants) = enum_decls.get(*enum_name) else {
                            continue;
                        };

                        for variant in &variants {
                            if enum_variants.iter().any(|candidate| candidate == variant) {
                                continue;
                            }

                            self.errors.push_back(SchemaError::Validation(
                                format!(
                                    "Unknown variant '{}' for enum '{}' in @check on field '{}' \
                                     in model '{}'",
                                    variant, enum_name, field.name.value, model.name.value
                                ),
                                span,
                            ));
                        }
                    }
                }
            }

            for attr in &model.attributes {
                let (expr, span) = match attr {
                    ModelAttribute::Check { expr, span } => (expr, *span),
                    _ => continue,
                };

                let refs = expr.field_references();
                for r in &refs {
                    if !scalar_fields.contains(r) {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "@@check references non-existent or relation field '{}' in model '{}'",
                                r, model.name.value
                            ),
                            span,
                        ));
                    }
                }

                for (field_name, variants) in expr.enum_in_lists() {
                    let Some(enum_name) = field_enum_map.get(field_name) else {
                        continue;
                    };
                    let Some(enum_variants) = enum_decls.get(*enum_name) else {
                        continue;
                    };

                    for variant in &variants {
                        if enum_variants.iter().any(|candidate| candidate == variant) {
                            continue;
                        }

                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "Unknown variant '{}' for enum '{}' in @@check \
                                 in model '{}'",
                                variant, enum_name, model.name.value
                            ),
                            span,
                        ));
                    }
                }
            }
        }
    }
}
