use super::*;

impl SchemaValidator<'_> {
    pub(super) fn validate_defaults(&mut self) {
        let models: Vec<_> = self.schema.models().cloned().collect();
        for model in &models {
            for field in &model.fields {
                for attr in &field.attributes {
                    if let FieldAttribute::Default(expr, _) = attr {
                        self.validate_default_value(
                            &field.field_type,
                            expr,
                            &field.name.value,
                            &model.name.value,
                        );
                    }
                }
            }
        }
    }

    pub(super) fn validate_default_value(
        &mut self,
        field_type: &FieldType,
        expr: &Expr,
        field_name: &str,
        model_name: &str,
    ) {
        match expr {
            Expr::Literal(lit) => {
                self.validate_literal_default(field_type, lit, field_name, model_name);
            }
            Expr::Ident(ident) => {
                self.validate_ident_default(field_type, ident, field_name, model_name);
            }
            Expr::FunctionCall { name, args, span } => {
                self.validate_function_default(
                    field_type,
                    &name.value,
                    args,
                    *span,
                    field_name,
                    model_name,
                );
            }
            _ => {
                self.errors.push_back(SchemaError::Validation(
                    format!(
                        "Unsupported default value expression for field '{}' in model '{}'",
                        field_name, model_name
                    ),
                    expr.span(),
                ));
            }
        }
    }

    fn validate_ident_default(
        &mut self,
        field_type: &FieldType,
        ident: &Ident,
        field_name: &str,
        model_name: &str,
    ) {
        let FieldType::UserType(type_name) = field_type else {
            self.push_non_enum_default_identifier_error(ident, field_name, model_name);
            return;
        };
        let Some(enum_decl) = self.schema.enums().find(|e| e.name.value == *type_name) else {
            self.push_non_enum_default_identifier_error(ident, field_name, model_name);
            return;
        };

        if enum_decl
            .variants
            .iter()
            .any(|variant| variant.name.value == ident.value)
        {
            return;
        }

        self.errors.push_back(SchemaError::Validation(
            format!(
                "Enum variant '{}' does not exist in enum '{}' for field '{}' in model '{}'",
                ident.value, type_name, field_name, model_name
            ),
            ident.span,
        ));
    }

    fn push_non_enum_default_identifier_error(
        &mut self,
        ident: &Ident,
        field_name: &str,
        model_name: &str,
    ) {
        self.errors.push_back(SchemaError::Validation(
            format!(
                "Default value for field '{}' in model '{}' uses identifier '{}' but field type is not an enum",
                field_name, model_name, ident.value
            ),
            ident.span,
        ));
    }

    pub(super) fn validate_literal_default(
        &mut self,
        field_type: &FieldType,
        lit: &Literal,
        field_name: &str,
        model_name: &str,
    ) {
        match (field_type, lit) {
            (FieldType::String, Literal::String(_, _)) => {}
            (FieldType::Boolean, Literal::Boolean(_, _)) => {}
            (
                FieldType::Int | FieldType::BigInt | FieldType::Float | FieldType::Decimal { .. },
                Literal::Number(_, _),
            ) => {}
            _ => {
                self.errors.push_back(SchemaError::Validation(
                    format!(
                        "Type mismatch: field '{}' in model '{}' has type {:?} but default value is {:?}",
                        field_name, model_name, field_type, lit
                    ),
                    lit.span(),
                ));
            }
        }
    }

    pub(super) fn validate_function_default(
        &mut self,
        field_type: &FieldType,
        func_name: &str,
        _args: &[Expr],
        span: Span,
        field_name: &str,
        model_name: &str,
    ) {
        match func_name {
            "autoincrement" => {
                if !matches!(field_type, FieldType::Int | FieldType::BigInt) {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "autoincrement() can only be used with Int or BigInt fields, but field '{}' in model '{}' has type {:?}",
                            field_name, model_name, field_type
                        ),
                        span,
                    ));
                }
            }
            "uuid" => {
                if !matches!(field_type, FieldType::Uuid) {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "uuid() can only be used with Uuid fields, but field '{}' in model '{}' has type {:?}",
                            field_name, model_name, field_type
                        ),
                        span,
                    ));
                }
            }
            "now" => {
                if !matches!(field_type, FieldType::DateTime) {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "now() can only be used with DateTime fields, but field '{}' in model '{}' has type {:?}",
                            field_name, model_name, field_type
                        ),
                        span,
                    ));
                }
            }
            "env" => {}
            _ => {
                self.errors.push_back(SchemaError::Validation(
                    format!(
                        "Unknown function '{}' in default value for field '{}' in model '{}'",
                        func_name, field_name, model_name
                    ),
                    span,
                ));
            }
        }
    }
}
