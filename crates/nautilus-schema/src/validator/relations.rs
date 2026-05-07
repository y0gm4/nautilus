use super::*;

impl SchemaValidator<'_> {
    pub(super) fn validate_relations(&mut self) {
        let models: Vec<_> = self.schema.models().cloned().collect();
        for model in &models {
            self.validate_model_relations(model);
        }
    }

    pub(super) fn validate_model_relations(&mut self, model: &ModelDecl) {
        let mut relations_to_models: HashMap<String, Vec<Option<String>>> = HashMap::new();

        for field in &model.fields {
            for attr in &field.attributes {
                let FieldAttribute::Relation {
                    name,
                    fields,
                    references,
                    on_delete,
                    on_update,
                    ..
                } = attr
                else {
                    continue;
                };

                let target_model = match &field.field_type {
                    FieldType::UserType(name) => name,
                    _ => {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "Relation field '{}' must have a model type, not a scalar or enum",
                                field.name.value
                            ),
                            field.span,
                        ));
                        continue;
                    }
                };

                if !self.models.contains_key(target_model) {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Relation field '{}' references unknown model '{}'",
                            field.name.value, target_model
                        ),
                        field.span,
                    ));
                    continue;
                }

                relations_to_models
                    .entry(target_model.clone())
                    .or_default()
                    .push(name.clone());

                if field.modifier == FieldModifier::Array {
                    if fields.is_some() || references.is_some() {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "Relation field '{}' is an array (back-reference) and must not specify 'fields' or 'references'. Move them to the opposite field on model '{}'",
                                field.name.value, target_model
                            ),
                            field.span,
                        ));
                    }
                    if on_delete.is_some() || on_update.is_some() {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "Relation field '{}' is an array (back-reference) and must not specify 'onDelete' or 'onUpdate'. Move them to the opposite field on model '{}'",
                                field.name.value, target_model
                            ),
                            field.span,
                        ));
                    }
                    continue;
                }

                let (Some(fk_fields), Some(ref_fields)) = (fields, references) else {
                    continue;
                };
                if fk_fields.len() != ref_fields.len() {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Relation '{}' has {} fields but {} references (must be equal)",
                            field.name.value,
                            fk_fields.len(),
                            ref_fields.len()
                        ),
                        field.span,
                    ));
                    continue;
                }

                for fk_field in fk_fields {
                    if model.find_field(&fk_field.value).is_some() {
                        continue;
                    }

                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Relation field '{}' references non-existent field '{}' in model '{}'",
                            field.name.value, fk_field.value, model.name.value
                        ),
                        fk_field.span,
                    ));
                }

                let Some(target_model_decl) = self
                    .schema
                    .models()
                    .find(|candidate| candidate.name.value == *target_model)
                    .cloned()
                else {
                    continue;
                };

                let mut all_reference_fields_exist = true;
                for ref_field in ref_fields {
                    if target_model_decl.find_field(&ref_field.value).is_some() {
                        continue;
                    }

                    all_reference_fields_exist = false;
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Relation references non-existent field '{}' in model '{}'",
                            ref_field.value, target_model
                        ),
                        ref_field.span,
                    ));
                }
                if !all_reference_fields_exist {
                    continue;
                }

                self.validate_referenced_fields_are_unique(
                    &target_model_decl,
                    ref_fields,
                    &field.name.value,
                    field.span,
                );
            }
        }

        for (target_model, relation_names) in relations_to_models {
            if relation_names.len() > 1 {
                let named_count = relation_names.iter().filter(|n| n.is_some()).count();
                if named_count < relation_names.len() {
                    self.errors.push_back(SchemaError::Validation(
                        format!(
                            "Model '{}' has multiple relations to '{}' but not all have unique 'name' parameters",
                            model.name.value, target_model
                        ),
                        model.span,
                    ));
                } else {
                    let mut seen_names = HashSet::new();
                    for n in relation_names.into_iter().flatten() {
                        if !seen_names.insert(n.clone()) {
                            self.errors.push_back(SchemaError::Validation(
                                format!(
                                    "Duplicate relation name '{}' in model '{}' to model '{}'",
                                    n, model.name.value, target_model
                                ),
                                model.span,
                            ));
                        }
                    }
                }
            }
        }
    }

    /// For every owning-side relation (non-array with `fields`+`references`),
    /// verify that the target model has a corresponding back-relation field of type
    /// `CurrentModel[]`.
    pub(super) fn validate_back_relations(&mut self) {
        let models: Vec<_> = self.schema.models().cloned().collect();

        for model in &models {
            let mut used_back_fields: HashMap<(String, String), String> = HashMap::new();

            for field in &model.fields {
                if field.modifier == FieldModifier::Array {
                    continue;
                }

                for attr in &field.attributes {
                    let FieldAttribute::Relation {
                        name,
                        fields,
                        references,
                        ..
                    } = attr
                    else {
                        continue;
                    };

                    let (Some(_), Some(_)) = (fields, references) else {
                        continue;
                    };

                    let FieldType::UserType(target_model_name) = &field.field_type else {
                        continue;
                    };

                    let Some(target_model_decl) = self
                        .schema
                        .models()
                        .find(|candidate| candidate.name.value == *target_model_name)
                        .cloned()
                    else {
                        continue;
                    };

                    let back_relation_candidates: Vec<&FieldDecl> = target_model_decl
                        .fields
                        .iter()
                        .filter(|candidate| {
                            Self::is_back_relation_candidate(candidate, &model.name.value)
                        })
                        .collect();

                    let matching_back_relations =
                        Self::matching_back_relations(&back_relation_candidates, name.as_deref());

                    if matching_back_relations.is_empty() {
                        let message = if back_relation_candidates.is_empty() {
                            Self::missing_back_relation_message(
                                field,
                                &model.name.value,
                                target_model_name,
                                name.as_deref(),
                            )
                        } else if let Some(relation_name) = name.as_deref() {
                            format!(
                                "Relation field '{}' on model '{}' expects an opposite relation field on model '{}' with @relation(name: \"{}\").",
                                field.name.value, model.name.value, target_model_name, relation_name
                            )
                        } else {
                            let candidate_names = back_relation_candidates
                                .iter()
                                .map(|candidate| format!("'{}'", candidate.name.value))
                                .collect::<Vec<_>>()
                                .join(", ");
                            format!(
                                "Relation field '{}' on model '{}' has multiple possible opposite relation fields on model '{}': {}. Use matching @relation(name: ...) to disambiguate.",
                                field.name.value, model.name.value, target_model_name, candidate_names
                            )
                        };

                        self.errors
                            .push_back(SchemaError::Validation(message, field.span));
                        continue;
                    }

                    if matching_back_relations.len() > 1 {
                        let candidate_names = matching_back_relations
                            .iter()
                            .map(|candidate| format!("'{}'", candidate.name.value))
                            .collect::<Vec<_>>()
                            .join(", ");
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "Relation field '{}' on model '{}' has multiple possible opposite relation fields on model '{}': {}. Use matching @relation(name: ...) to disambiguate.",
                                field.name.value, model.name.value, target_model_name, candidate_names
                            ),
                            field.span,
                        ));
                        continue;
                    }

                    let back_field = matching_back_relations[0];
                    let back_field_key = (target_model_name.clone(), back_field.name.value.clone());
                    if let Some(existing_field) =
                        used_back_fields.insert(back_field_key, field.name.value.clone())
                    {
                        self.errors.push_back(SchemaError::Validation(
                            format!(
                                "Opposite relation field '{}' on model '{}' is already paired with relation field '{}' on model '{}', so it cannot also satisfy relation field '{}'. Add a distinct opposite relation field.",
                                back_field.name.value,
                                target_model_name,
                                existing_field,
                                model.name.value,
                                field.name.value,
                            ),
                            field.span,
                        ));
                    }
                }
            }
        }
    }

    pub(super) fn is_back_relation_candidate(field: &FieldDecl, current_model_name: &str) -> bool {
        matches!(&field.field_type, FieldType::UserType(name) if name == current_model_name)
            && !Self::relation_is_owning_side(field)
    }

    pub(super) fn relation_is_owning_side(field: &FieldDecl) -> bool {
        field.attributes.iter().any(|attr| {
            matches!(
                attr,
                FieldAttribute::Relation {
                    fields,
                    references,
                    ..
                } if fields.is_some() || references.is_some()
            )
        })
    }

    pub(super) fn relation_name(field: &FieldDecl) -> Option<&str> {
        field.attributes.iter().find_map(|attr| match attr {
            FieldAttribute::Relation { name, .. } => name.as_deref(),
            _ => None,
        })
    }

    pub(super) fn matching_back_relations<'a>(
        candidates: &[&'a FieldDecl],
        relation_name: Option<&str>,
    ) -> Vec<&'a FieldDecl> {
        if let Some(relation_name) = relation_name {
            let named_matches: Vec<&FieldDecl> = candidates
                .iter()
                .copied()
                .filter(|candidate| Self::relation_name(candidate) == Some(relation_name))
                .collect();
            if !named_matches.is_empty() {
                return named_matches;
            }

            if candidates.len() == 1 && Self::relation_name(candidates[0]).is_none() {
                return vec![candidates[0]];
            }

            return Vec::new();
        }

        let unnamed_matches: Vec<&FieldDecl> = candidates
            .iter()
            .copied()
            .filter(|candidate| Self::relation_name(candidate).is_none())
            .collect();
        if !unnamed_matches.is_empty() {
            return unnamed_matches;
        }

        if candidates.len() == 1 {
            return vec![candidates[0]];
        }

        Vec::new()
    }

    pub(super) fn missing_back_relation_message(
        field: &FieldDecl,
        model_name: &str,
        target_model_name: &str,
        relation_name: Option<&str>,
    ) -> String {
        if let Some(relation_name) = relation_name {
            format!(
                "Relation field '{}' on model '{}' is missing an opposite relation field on model '{}'. Add a '{}?' (one-to-one) or '{}[]' (one-to-many) field to model '{}' with @relation(name: \"{}\").",
                field.name.value,
                model_name,
                target_model_name,
                model_name,
                model_name,
                target_model_name,
                relation_name,
            )
        } else {
            format!(
                "Relation field '{}' on model '{}' is missing an opposite relation field on model '{}'. Add a '{}?' (one-to-one) or '{}[]' (one-to-many) field to model '{}'.",
                field.name.value,
                model_name,
                target_model_name,
                model_name,
                model_name,
                target_model_name,
            )
        }
    }

    pub(super) fn validate_referenced_field_is_unique(
        &mut self,
        target_model: &ModelDecl,
        field_name: &str,
        span: Span,
        relation_name: &str,
    ) {
        let is_pk = target_model.find_field(field_name).is_some_and(|field| {
            field
                .attributes
                .iter()
                .any(|attr| matches!(attr, FieldAttribute::Id))
        }) || target_model.attributes.iter().any(|attr| {
            matches!(attr, ModelAttribute::Id(fields) if fields.len() == 1 && fields[0].value == field_name)
        });
        if is_pk {
            return;
        }

        let is_unique = target_model.find_field(field_name).is_some_and(|field| {
            field
                .attributes
                .iter()
                .any(|attr| matches!(attr, FieldAttribute::Unique))
        }) || target_model.attributes.iter().any(|attr| {
            matches!(attr, ModelAttribute::Unique(fields) if fields.len() == 1 && fields[0].value == field_name)
        });
        if is_unique {
            return;
        }

        self.errors.push_back(SchemaError::Validation(
            format!(
                "Relation '{}' references field '{}' in model '{}', but it is not a primary key or unique field",
                relation_name, field_name, target_model.name.value
            ),
            span,
        ));
    }

    pub(super) fn validate_referenced_fields_are_unique(
        &mut self,
        target_model: &ModelDecl,
        ref_fields: &[Ident],
        relation_name: &str,
        relation_span: Span,
    ) {
        if ref_fields.len() == 1 {
            let ref_field = &ref_fields[0];
            self.validate_referenced_field_is_unique(
                target_model,
                &ref_field.value,
                ref_field.span,
                relation_name,
            );
            return;
        }

        let ref_names: Vec<&str> = ref_fields
            .iter()
            .map(|field| field.value.as_str())
            .collect();

        let matches_composite_pk = target_model.attributes.iter().any(|attr| match attr {
            ModelAttribute::Id(fields) => {
                fields.len() == ref_names.len()
                    && fields
                        .iter()
                        .map(|field| field.value.as_str())
                        .eq(ref_names.iter().copied())
            }
            _ => false,
        });

        let matches_composite_unique = target_model.attributes.iter().any(|attr| match attr {
            ModelAttribute::Unique(fields) => {
                fields.len() == ref_names.len()
                    && fields
                        .iter()
                        .map(|field| field.value.as_str())
                        .eq(ref_names.iter().copied())
            }
            _ => false,
        });

        if !matches_composite_pk && !matches_composite_unique {
            self.errors.push_back(SchemaError::Validation(
                format!(
                    "Relation '{}' references fields [{}] in model '{}', but they do not match a composite primary key or unique constraint",
                    relation_name,
                    ref_names.join(", "),
                    target_model.name.value
                ),
                relation_span,
            ));
        }
    }
}
