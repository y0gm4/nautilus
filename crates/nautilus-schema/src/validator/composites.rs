use super::*;

impl SchemaValidator<'_> {
    pub(super) fn validate_composite_types(&mut self) {
        let types: Vec<_> = self.schema.types().cloned().collect();
        for type_decl in &types {
            self.validate_composite_type(type_decl);
        }
    }

    pub(super) fn validate_composite_type(&mut self, type_decl: &TypeDecl) {
        let mut field_names: HashMap<String, Span> = HashMap::new();
        for field in &type_decl.fields {
            let name = &field.name.value;
            if field_names.contains_key(name) {
                self.errors.push_back(SchemaError::Validation(
                    format!(
                        "Duplicate field name '{}' in type '{}'",
                        name, type_decl.name.value
                    ),
                    field.name.span,
                ));
            } else {
                field_names.insert(name.clone(), field.name.span);
            }

            if let FieldType::UserType(type_name) = &field.field_type {
                if self.models.contains_key(type_name) {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Field '{}' in type '{}' cannot have a model type '{}'. Composite type fields must be scalar, enum, or array of scalars/enums.",
                            name, type_decl.name.value, type_name
                        ),
                        field.span,
                    ));
                } else if self.composite_types.contains_key(type_name) {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Field '{}' in type '{}' cannot reference another composite type '{}'. Nested composite types are not supported.",
                            name, type_decl.name.value, type_name
                        ),
                        field.span,
                    ));
                } else if !self.enums.contains_key(type_name) {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Unknown type '{}' for field '{}' in type '{}'",
                            type_name, name, type_decl.name.value
                        ),
                        field.span,
                    ));
                }
            }

            for attr in &field.attributes {
                match attr {
                    FieldAttribute::Map(_) | FieldAttribute::Store { .. } => {}
                    FieldAttribute::Id => {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "@id is not allowed on fields inside a composite type (field '{}' in type '{}')",
                                name, type_decl.name.value
                            ),
                            field.span,
                        ));
                    }
                    FieldAttribute::Unique => {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "@unique is not allowed on fields inside a composite type (field '{}' in type '{}')",
                                name, type_decl.name.value
                            ),
                            field.span,
                        ));
                    }
                    FieldAttribute::Relation { .. } => {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "@relation is not allowed on fields inside a composite type (field '{}' in type '{}')",
                                name, type_decl.name.value
                            ),
                            field.span,
                        ));
                    }
                    FieldAttribute::Computed { .. } => {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "@computed is not allowed on fields inside a composite type (field '{}' in type '{}')",
                                name, type_decl.name.value
                            ),
                            field.span,
                        ));
                    }
                    FieldAttribute::Check { .. } => {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "@check is not allowed on fields inside a composite type (field '{}' in type '{}')",
                                name, type_decl.name.value
                            ),
                            field.span,
                        ));
                    }
                    FieldAttribute::Default(_, _) => {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "@default is not allowed on fields inside a composite type (field '{}' in type '{}')",
                                name, type_decl.name.value
                            ),
                            field.span,
                        ));
                    }
                    FieldAttribute::UpdatedAt { .. } => {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "@updatedAt is not allowed on fields inside a composite type (field '{}' in type '{}')",
                                name, type_decl.name.value
                            ),
                            field.span,
                        ));
                    }
                }
            }
        }
    }
}
