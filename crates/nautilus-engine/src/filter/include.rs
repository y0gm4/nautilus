use super::context::nested_include_context;
use super::ordering::parse_order_by;
use super::where_filter::{parse_where_filter, qualify_filter_columns};
use super::*;

pub(super) fn parse_select(select_value: &JsonValue) -> Result<HashSet<String>, ProtocolError> {
    let obj = select_value
        .as_object()
        .ok_or_else(|| ProtocolError::InvalidParams("select must be an object".to_string()))?;

    let mut result = HashSet::new();
    for (field, flag) in obj {
        match flag {
            JsonValue::Bool(true) => {
                result.insert(field.clone());
            }
            JsonValue::Bool(false) => {}
            _ => {
                return Err(ProtocolError::InvalidParams(format!(
                    "select.{} must be true or false",
                    field
                )));
            }
        }
    }
    Ok(result)
}

fn insert_path(map: &mut HashMap<String, IncludeNode>, segments: &[&str]) {
    if segments.is_empty() {
        return;
    }
    let entry = map
        .entry(segments[0].to_string())
        .or_insert_with(|| IncludeNode {
            filter: None,
            nested: HashMap::new(),
            take: None,
            skip: None,
            order_by: vec![],
        });
    insert_path(&mut entry.nested, &segments[1..]);
}

pub(super) fn parse_include(
    include_value: &JsonValue,
    relations: &RelationMap,
    models: Option<&HashMap<String, ModelIr>>,
) -> Result<HashMap<String, IncludeNode>, ProtocolError> {
    if let Some(arr) = include_value.as_array() {
        if arr.iter().any(|v| v.as_str() == Some("*")) {
            return Ok(relations
                .keys()
                .map(|k| {
                    (
                        k.clone(),
                        IncludeNode {
                            filter: None,
                            nested: HashMap::new(),
                            take: None,
                            skip: None,
                            order_by: vec![],
                        },
                    )
                })
                .collect());
        }

        let mut map = HashMap::new();
        for v in arr {
            if let Some(s) = v.as_str() {
                let segments: Vec<&str> = s.split('.').collect();
                insert_path(&mut map, &segments);
            }
        }
        return Ok(map);
    }

    let obj = include_value.as_object().ok_or_else(|| {
        ProtocolError::InvalidParams("include must be an object or array".to_string())
    })?;

    let mut result = HashMap::new();
    for (field, spec) in obj {
        let node = match spec {
            JsonValue::Bool(true) => IncludeNode {
                filter: None,
                nested: HashMap::new(),
                take: None,
                skip: None,
                order_by: vec![],
            },
            JsonValue::Bool(false) => continue,
            JsonValue::Object(child_obj) => {
                let child_ctx = nested_include_context(field, relations, models)?;
                let filter = if let Some(where_val) = child_obj.get("where") {
                    if let Some((
                        child_relations,
                        child_field_types,
                        child_logical_to_db,
                        target_table,
                    )) = &child_ctx
                    {
                        let parsed = parse_where_filter(
                            where_val,
                            child_relations,
                            child_field_types,
                            models,
                        )?;
                        Some(qualify_filter_columns(
                            parsed,
                            target_table,
                            child_logical_to_db,
                        ))
                    } else {
                        Some(parse_where_filter(
                            where_val,
                            &RelationMap::new(),
                            &FieldTypeMap::new(),
                            None,
                        )?)
                    }
                } else {
                    None
                };
                let nested = if let Some(inc_val) = child_obj.get("include") {
                    if let Some((child_relations, _, _, _)) = &child_ctx {
                        parse_include(inc_val, child_relations, models)?
                    } else {
                        parse_include(inc_val, &RelationMap::new(), models)?
                    }
                } else {
                    HashMap::new()
                };
                let take = child_obj
                    .get("take")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32);
                let skip = child_obj
                    .get("skip")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32);
                let order_by = if let Some(ob_val) = child_obj.get("orderBy") {
                    let child_field_types = child_ctx
                        .as_ref()
                        .map(|(_, child_field_types, _, _)| child_field_types);
                    let mut parsed = parse_order_by(ob_val, child_field_types)?;
                    if let Some((_, _, child_logical_to_db, _)) = &child_ctx {
                        for order in &mut parsed {
                            order.column = child_logical_to_db
                                .get(order.column.as_str())
                                .cloned()
                                .unwrap_or_else(|| order.column.clone());
                        }
                    }
                    parsed
                } else {
                    vec![]
                };
                IncludeNode {
                    filter,
                    nested,
                    take,
                    skip,
                    order_by,
                }
            }
            _ => {
                return Err(ProtocolError::InvalidParams(format!(
                    "include.{} must be true or an object",
                    field
                )));
            }
        };
        result.insert(field.clone(), node);
    }
    Ok(result)
}
