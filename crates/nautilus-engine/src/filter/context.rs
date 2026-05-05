use std::borrow::Cow;

use super::*;

pub(super) struct NestedIncludeContext<'a> {
    pub(super) relations: Cow<'a, RelationMap>,
    pub(super) field_types: Cow<'a, FieldTypeMap>,
    pub(super) logical_to_db: Cow<'a, HashMap<String, String>>,
    pub(super) target_table: Cow<'a, str>,
}

pub(super) struct RelationFilterContext<'a> {
    pub(super) relations: Cow<'a, RelationMap>,
    pub(super) field_types: Cow<'a, FieldTypeMap>,
    pub(super) logical_to_db: Cow<'a, HashMap<String, String>>,
}

pub(super) fn nested_include_context<'a>(
    field: &str,
    relations: &'a RelationMap,
    schema_context: SchemaContext<'a>,
) -> Result<Option<NestedIncludeContext<'a>>, ProtocolError> {
    let Some(rel_info) = relations.get(field) else {
        return Ok(None);
    };

    if let Some(state) = schema_context.state() {
        let Some((target_model, target_metadata)) =
            state.related_model(&rel_info.target_logical_name)
        else {
            return Ok(None);
        };

        return Ok(Some(NestedIncludeContext {
            relations: Cow::Borrowed(state.relation_map_for_model(target_model)?),
            field_types: Cow::Borrowed(target_metadata.field_types()),
            logical_to_db: Cow::Borrowed(target_metadata.logical_to_db()),
            target_table: Cow::Borrowed(rel_info.target_table.as_str()),
        }));
    }

    let Some(all_models) = schema_context.models() else {
        return Ok(None);
    };
    let Some(target_model) = all_models.get(&rel_info.target_logical_name) else {
        return Ok(None);
    };

    Ok(Some(NestedIncludeContext {
        relations: Cow::Owned(crate::handlers::build_relation_map(
            target_model,
            all_models,
        )?),
        field_types: Cow::Owned(crate::metadata::build_field_type_map(target_model)),
        logical_to_db: Cow::Owned(crate::metadata::build_logical_to_db_map(target_model)),
        target_table: Cow::Owned(rel_info.target_table.clone()),
    }))
}

pub(super) fn relation_filter_context<'a>(
    rel: &RelationInfo,
    schema_context: SchemaContext<'a>,
) -> Result<Option<RelationFilterContext<'a>>, ProtocolError> {
    if let Some(state) = schema_context.state() {
        let Some((target_model, target_metadata)) = state.related_model(&rel.target_logical_name)
        else {
            return Ok(None);
        };

        return Ok(Some(RelationFilterContext {
            relations: Cow::Borrowed(state.relation_map_for_model(target_model)?),
            field_types: Cow::Borrowed(target_metadata.field_types()),
            logical_to_db: Cow::Borrowed(target_metadata.logical_to_db()),
        }));
    }

    let Some(all_models) = schema_context.models() else {
        return Ok(None);
    };
    let Some(target_model) = all_models.get(&rel.target_logical_name) else {
        return Ok(None);
    };

    Ok(Some(RelationFilterContext {
        relations: Cow::Owned(crate::handlers::build_relation_map(
            target_model,
            all_models,
        )?),
        field_types: Cow::Owned(crate::metadata::build_field_type_map(target_model)),
        logical_to_db: Cow::Owned(crate::metadata::build_logical_to_db_map(target_model)),
    }))
}
