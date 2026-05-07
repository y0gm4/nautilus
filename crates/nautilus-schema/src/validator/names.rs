use super::*;

impl SchemaValidator<'_> {
    pub(super) fn collect_names(&mut self) {
        for decl in &self.schema.declarations {
            match decl {
                Declaration::Model(model) => {
                    let name = model.name.value.clone();
                    if let Some(existing_span) = self.models.get(&name) {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "Duplicate model name '{}' (first defined at {})",
                                name, existing_span
                            ),
                            model.name.span,
                        ));
                    } else if self
                        .enums
                        .get(&name)
                        .or(self.composite_types.get(&name))
                        .is_some()
                    {
                        self.errors.push_back(SchemaError::Validation(
                            format!("Name '{}' is already used by an enum or type", name),
                            model.name.span,
                        ));
                    } else {
                        self.models.insert(name, model.name.span);
                    }
                }
                Declaration::Enum(enum_decl) => {
                    let name = enum_decl.name.value.clone();
                    if let Some(existing_span) = self.enums.get(&name) {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "Duplicate enum name '{}' (first defined at {})",
                                name, existing_span
                            ),
                            enum_decl.name.span,
                        ));
                    } else if self
                        .models
                        .get(&name)
                        .or(self.composite_types.get(&name))
                        .is_some()
                    {
                        self.errors.push_back(SchemaError::Validation(
                            format!("Name '{}' is already used by a model or type", name),
                            enum_decl.name.span,
                        ));
                    } else {
                        self.enums.insert(name, enum_decl.name.span);
                    }
                }
                Declaration::Type(type_decl) => {
                    let name = type_decl.name.value.clone();
                    if let Some(existing_span) = self.composite_types.get(&name) {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "Duplicate type name '{}' (first defined at {})",
                                name, existing_span
                            ),
                            type_decl.name.span,
                        ));
                    } else if self.models.get(&name).or(self.enums.get(&name)).is_some() {
                        self.errors.push_back(SchemaError::Validation(
                            format!("Name '{}' is already used by a model or enum", name),
                            type_decl.name.span,
                        ));
                    } else {
                        self.composite_types.insert(name, type_decl.name.span);
                    }
                }
                _ => {}
            }
        }
    }

    pub(super) fn check_physical_name_collisions(&mut self) {
        let mut table_names: HashMap<String, (String, Span)> = HashMap::new();
        for decl in &self.schema.declarations {
            if let Declaration::Model(model) = decl {
                let table_name = model.table_name().to_string();
                if let Some((existing_model, existing_span)) = table_names.get(&table_name) {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Physical table name '{}' is used by both model '{}' and '{}' (first defined at {})",
                            table_name, model.name.value, existing_model, existing_span
                        ),
                        model.span,
                    ));
                } else {
                    table_names.insert(table_name, (model.name.value.clone(), model.span));
                }

                let mut column_names: HashMap<String, (String, Span)> = HashMap::new();
                for field in &model.fields {
                    let column_name = field.column_name().to_string();
                    if let Some((existing_field, existing_span)) = column_names.get(&column_name) {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "Physical column name '{}' in model '{}' is used by both field '{}' and '{}' (first defined at {})",
                                column_name, model.name.value, field.name.value, existing_field, existing_span
                            ),
                            field.span,
                        ));
                    } else {
                        column_names.insert(column_name, (field.name.value.clone(), field.span));
                    }
                }
            }
        }
    }
}
