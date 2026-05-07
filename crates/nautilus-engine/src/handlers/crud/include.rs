//! Include hydration: load relation children for a set of parent rows and attach
//! them to each row as a `<field>_json` column.
//!
//! Two execution strategies coexist:
//!
//! - **Batched**: a single `WHERE child_fk IN (parent_pks...)` query loads
//!   children for every parent at once, then groups them in memory. This
//!   eliminates the N+1 query pattern on includes without per-parent pagination.
//! - **Per-parent fallback**: used when the include node carries `take`/`skip`,
//!   because per-parent pagination cannot be expressed by a single batched query
//!   without window functions. Each parent triggers its own child query.
//!
//! Nested includes recurse through `execute_find_many_rows`, so each nesting
//! level is itself batched whenever it qualifies.
use std::collections::HashMap;

use nautilus_connector::Row;
use nautilus_core::{Expr, Value};
use nautilus_protocol::ProtocolError;
use nautilus_schema::ir::ModelIr;
use serde_json::Value as JsonValue;

use super::read::execute_find_many_rows;
use crate::filter::{IncludeNode, QueryArgs, RelationInfo};
use crate::state::EngineState;

fn include_alias(field_name: &str) -> String {
    format!("{}_json", field_name)
}

fn parent_join_column(rel_info: &RelationInfo) -> String {
    format!("{}__{}", rel_info.parent_table, rel_info.pk_db)
}

fn child_join_column(rel_info: &RelationInfo) -> String {
    format!("{}__{}", rel_info.target_table, rel_info.fk_db)
}

fn empty_relation_value(rel_info: &RelationInfo) -> Value {
    if rel_info.is_array {
        Value::Json(JsonValue::Array(vec![]))
    } else {
        Value::Null
    }
}

/// Stable string key for grouping by a SQL value. PKs are I32/I64/Uuid/String
/// in real schemas, so the JSON shape is unambiguous.
fn value_key(value: &Value) -> String {
    value.to_json_plain().to_string()
}

fn row_to_json_value(model: &ModelIr, row: &Row) -> JsonValue {
    let mut obj = serde_json::Map::with_capacity(row.len());
    for field in model.scalar_fields() {
        let alias = format!("{}__{}", model.db_name, field.db_name);
        if let Some(value) = row.get(&alias) {
            obj.insert(field.logical_name.clone(), value.to_json_plain());
        }
    }
    for (name, value) in row.iter() {
        if name.ends_with("_json") {
            obj.insert(name.to_string(), value.to_json_plain());
        }
    }
    JsonValue::Object(obj)
}

/// Single-parent fallback path. Used when the batched path cannot run safely
/// (e.g. include node carries per-parent `take`/`skip`).
async fn load_relation_include_value(
    state: &EngineState,
    parent_row: &Row,
    rel_info: &RelationInfo,
    include_node: &IncludeNode,
    tx_id: Option<&str>,
) -> Result<Value, ProtocolError> {
    let parent_join = parent_join_column(rel_info);
    let Some(parent_value) = parent_row.get(&parent_join).cloned() else {
        return Ok(empty_relation_value(rel_info));
    };

    let target_model = state
        .models
        .get(&rel_info.target_logical_name)
        .ok_or_else(|| {
            ProtocolError::QueryPlanning(format!(
                "Model '{}' not found",
                rel_info.target_logical_name
            ))
        })?;

    let join_filter = Expr::column(child_join_column(rel_info)).eq(Expr::param(parent_value));
    let filter = Some(if let Some(child_filter) = include_node.filter.clone() {
        join_filter.and(child_filter)
    } else {
        join_filter
    });

    let query_args = QueryArgs {
        filter,
        order_by: include_node.order_by.clone(),
        take: if rel_info.is_array {
            include_node.take
        } else {
            include_node.take.or(Some(1))
        },
        skip: include_node.skip,
        include: include_node.nested.clone(),
        select: std::collections::HashSet::new(),
        cursor: None,
        backward: false,
        distinct: vec![],
        nearest: None,
    };

    let child_rows = Box::pin(execute_find_many_rows(
        state,
        target_model,
        query_args,
        tx_id,
    ))
    .await?;

    if rel_info.is_array {
        Ok(Value::Json(JsonValue::Array(
            child_rows
                .iter()
                .map(|row| row_to_json_value(target_model, row))
                .collect(),
        )))
    } else {
        Ok(child_rows
            .first()
            .map(|row| Value::Json(row_to_json_value(target_model, row)))
            .unwrap_or(Value::Null))
    }
}

/// Batched path: load all children for `parent_rows` in one query, then group
/// in memory. Returns the per-parent include value in the same order as
/// `parent_rows`. Returns `Ok(None)` when the caller should fall back to
/// per-parent execution.
async fn batch_load_relation_include(
    state: &EngineState,
    parent_rows: &[Row],
    rel_info: &RelationInfo,
    include_node: &IncludeNode,
    tx_id: Option<&str>,
) -> Result<Option<Vec<Value>>, ProtocolError> {
    if include_node.take.is_some() || include_node.skip.is_some() {
        return Ok(None);
    }

    let parent_join = parent_join_column(rel_info);

    let mut row_keys: Vec<Option<String>> = Vec::with_capacity(parent_rows.len());
    let mut seen: std::collections::HashSet<String> =
        std::collections::HashSet::with_capacity(parent_rows.len());
    let mut unique_values: Vec<Value> = Vec::with_capacity(parent_rows.len());

    for parent_row in parent_rows {
        match parent_row.get(&parent_join) {
            Some(Value::Null) | None => row_keys.push(None),
            Some(value) => {
                let key = value_key(value);
                if seen.insert(key.clone()) {
                    unique_values.push(value.clone());
                }
                row_keys.push(Some(key));
            }
        }
    }

    if unique_values.is_empty() {
        return Ok(Some(
            parent_rows
                .iter()
                .map(|_| empty_relation_value(rel_info))
                .collect(),
        ));
    }

    let target_model = state
        .models
        .get(&rel_info.target_logical_name)
        .ok_or_else(|| {
            ProtocolError::QueryPlanning(format!(
                "Model '{}' not found",
                rel_info.target_logical_name
            ))
        })?;

    let in_predicate = Expr::column(child_join_column(rel_info))
        .in_list(unique_values.into_iter().map(Expr::param).collect());
    let filter = Some(if let Some(child_filter) = include_node.filter.clone() {
        in_predicate.and(child_filter)
    } else {
        in_predicate
    });

    let query_args = QueryArgs {
        filter,
        order_by: include_node.order_by.clone(),
        take: None,
        skip: None,
        include: include_node.nested.clone(),
        select: std::collections::HashSet::new(),
        cursor: None,
        backward: false,
        distinct: vec![],
        nearest: None,
    };

    let child_rows = Box::pin(execute_find_many_rows(
        state,
        target_model,
        query_args,
        tx_id,
    ))
    .await?;

    let child_join = child_join_column(rel_info);
    let mut grouped: HashMap<String, Vec<JsonValue>> = HashMap::new();
    for child_row in &child_rows {
        let Some(fk_value) = child_row.get(&child_join) else {
            continue;
        };
        let key = value_key(fk_value);
        grouped
            .entry(key)
            .or_default()
            .push(row_to_json_value(target_model, child_row));
    }

    let result = row_keys
        .into_iter()
        .map(|maybe_key| match maybe_key {
            None => empty_relation_value(rel_info),
            Some(key) => {
                let children = grouped.get(&key).cloned().unwrap_or_default();
                if rel_info.is_array {
                    Value::Json(JsonValue::Array(children))
                } else {
                    children
                        .into_iter()
                        .next()
                        .map(Value::Json)
                        .unwrap_or(Value::Null)
                }
            }
        })
        .collect();

    Ok(Some(result))
}

/// Attach include payloads (one column per relation field) to every row in
/// `rows`, preferring a single batched child query per relation. Falls back to
/// per-parent execution when the include node carries `take`/`skip`.
pub(super) async fn hydrate_rows_with_includes(
    state: &EngineState,
    model: &ModelIr,
    rows: Vec<Row>,
    includes: &HashMap<String, IncludeNode>,
    tx_id: Option<&str>,
) -> Result<Vec<Row>, ProtocolError> {
    if rows.is_empty() || includes.is_empty() {
        return Ok(rows);
    }

    let relation_map = state.relation_map_for_model(model)?;

    let mut per_relation_values: Vec<(String, Vec<Value>)> = Vec::with_capacity(includes.len());

    for (field_name, include_node) in includes {
        let Some(rel_info) = relation_map.get(field_name) else {
            continue;
        };

        let values =
            match batch_load_relation_include(state, &rows, rel_info, include_node, tx_id).await? {
                Some(values) => values,
                None => {
                    let mut values = Vec::with_capacity(rows.len());
                    for parent_row in &rows {
                        values.push(
                            load_relation_include_value(
                                state,
                                parent_row,
                                rel_info,
                                include_node,
                                tx_id,
                            )
                            .await?,
                        );
                    }
                    values
                }
            };

        per_relation_values.push((field_name.clone(), values));
    }

    let mut hydrated = Vec::with_capacity(rows.len());
    for (idx, row) in rows.into_iter().enumerate() {
        let mut hydrated_row = row;
        for (field_name, values) in &mut per_relation_values {
            let value = std::mem::replace(&mut values[idx], Value::Null);
            hydrated_row.push_column(include_alias(field_name), value);
        }
        hydrated.push(hydrated_row);
    }

    Ok(hydrated)
}
