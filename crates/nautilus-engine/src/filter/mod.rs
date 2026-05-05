use std::collections::{HashMap, HashSet};

use serde_json::Value as JsonValue;

use nautilus_core::{BinaryOp, Expr, OrderBy, OrderDir, Select, Value, VectorMetric};
use nautilus_protocol::ProtocolError;
use nautilus_schema::ir::{ModelIr, ResolvedFieldType, ScalarType};

mod context;
mod include;
mod ordering;
#[cfg(test)]
mod tests;
mod where_filter;

use include::{parse_include, parse_select};
use ordering::{parse_int, parse_order_by, parse_signed_int};

pub(crate) use ordering::{parse_group_by_order_by, parse_having, GroupByOrderItem};
pub(crate) use where_filter::{parse_where_filter, qualify_filter_columns};

use crate::conversion::{json_to_value, json_to_value_field};
use crate::state::EngineState;

/// Map from a model's logical field name to its resolved field type.
/// Used during filter parsing to emit `Value::Enum` for enum-typed fields.
pub(crate) type FieldTypeMap = HashMap<String, ResolvedFieldType>;

/// Metadata needed to compile EXISTS / NOT EXISTS subqueries for a single relation field.
#[derive(Debug, Clone)]
pub struct RelationInfo {
    /// Database table name of the parent model (the one being queried).
    pub parent_table: String,
    /// Logical model name of the target / child model (key in SchemaIr.models).
    pub target_logical_name: String,
    /// Database table name of the target / child model.
    pub target_table: String,
    /// DB-level column name of the FK in the **child** table (e.g. `"user_id"`).
    pub fk_db: String,
    /// DB-level column name of the PK in the **parent** table (e.g. `"id"`).
    pub pk_db: String,
    /// Whether this relation is one-to-many (`true`) or one-to-one / FK-side (`false`).
    pub is_array: bool,
}

/// A map from relation *field* name (logical, as used in the `where` / `include` payload)
/// to its join metadata. Pass an empty map when no schema context is available.
pub type RelationMap = HashMap<String, RelationInfo>;

#[derive(Clone, Copy, Default)]
pub(crate) struct SchemaContext<'a> {
    models: Option<&'a HashMap<String, ModelIr>>,
    state: Option<&'a EngineState>,
}

impl<'a> SchemaContext<'a> {
    pub(crate) const fn none() -> Self {
        Self {
            models: None,
            state: None,
        }
    }

    #[cfg(test)]
    pub(crate) const fn with_models(models: &'a HashMap<String, ModelIr>) -> Self {
        Self {
            models: Some(models),
            state: None,
        }
    }

    pub(crate) fn with_state(state: &'a EngineState) -> Self {
        Self {
            models: Some(&state.models),
            state: Some(state),
        }
    }

    pub(super) const fn models(self) -> Option<&'a HashMap<String, ModelIr>> {
        self.models
    }

    pub(super) const fn state(self) -> Option<&'a EngineState> {
        self.state
    }
}

/// A node in the include tree for one relation.
#[derive(Debug, Clone)]
pub struct IncludeNode {
    /// Optional WHERE filter applied to the child relation query.
    pub filter: Option<Expr>,
    /// Nested includes: child's relation field name -> its own IncludeNode.
    pub nested: HashMap<String, IncludeNode>,
    /// LIMIT to apply to the child relation subquery (array relations only).
    pub take: Option<i32>,
    /// OFFSET to apply to the child relation subquery (array relations only).
    pub skip: Option<u32>,
    /// ORDER BY clauses to apply to the child relation subquery.
    pub order_by: Vec<OrderBy>,
}

/// pgvector nearest-neighbor search specification parsed from query args.
#[derive(Debug, Clone)]
pub struct VectorNearestQuery {
    /// Logical field name of the vector field.
    pub field: String,
    /// Query embedding.
    pub query: Vec<f32>,
    /// Distance metric used for ordering.
    pub metric: VectorMetric,
}

/// Parse query arguments from JSON into query components.
#[derive(Debug)]
pub struct QueryArgs {
    pub filter: Option<Expr>,
    pub order_by: Vec<OrderBy>,
    /// Absolute number of rows to fetch (direction is in `backward`).
    pub take: Option<i32>,
    /// Number of rows to skip (OFFSET).
    pub skip: Option<u32>,
    /// Relation fields to eager-load. Key = logical field name.
    pub include: HashMap<String, IncludeNode>,
    /// Projection: set of logical field names to SELECT. Empty = select all columns.
    pub select: HashSet<String>,
    /// Cursor for stable pagination: PK field name -> value, parsed from the `"cursor"` key.
    pub cursor: Option<HashMap<String, Value>>,
    /// True when the caller passed a negative `take`, requesting backward pagination.
    pub backward: bool,
    /// Columns to deduplicate on (maps to SELECT DISTINCT / DISTINCT ON).
    pub distinct: Vec<String>,
    /// Optional pgvector nearest-neighbor ordering.
    pub nearest: Option<VectorNearestQuery>,
}

impl QueryArgs {
    /// Parse with no relation or field-type context (backward-compatible).
    pub fn parse(args: Option<JsonValue>) -> Result<Self, ProtocolError> {
        Self::parse_with_context(
            args,
            &RelationMap::new(),
            &FieldTypeMap::new(),
            SchemaContext::none(),
        )
    }

    /// Parse without relation context but with field-type context.
    ///
    /// Used by `update` / `delete` / `findUnique` handlers where the model is
    /// known but there are no eager-loaded relations.
    pub fn parse_typed(
        args: Option<JsonValue>,
        field_types: &FieldTypeMap,
    ) -> Result<Self, ProtocolError> {
        Self::parse_with_context(
            args,
            &RelationMap::new(),
            field_types,
            SchemaContext::none(),
        )
    }

    /// Parse with relation context so that `some` / `none` / `every` predicates
    /// and structured `include` objects can be understood.
    pub fn parse_with_relations(
        args: Option<JsonValue>,
        relations: &RelationMap,
        field_types: &FieldTypeMap,
    ) -> Result<Self, ProtocolError> {
        Self::parse_with_context(args, relations, field_types, SchemaContext::none())
    }

    /// Parse with relation context and full schema access so nested include
    /// payloads can reuse the child model's field mappings.
    pub(crate) fn parse_with_context(
        args: Option<JsonValue>,
        relations: &RelationMap,
        field_types: &FieldTypeMap,
        schema_context: SchemaContext<'_>,
    ) -> Result<Self, ProtocolError> {
        let args = match args {
            Some(JsonValue::Object(map)) => map,
            Some(_) => {
                return Err(ProtocolError::InvalidParams(
                    "args must be an object".to_string(),
                ));
            }
            None => {
                return Ok(QueryArgs {
                    filter: None,
                    order_by: vec![],
                    take: None,
                    skip: None,
                    include: HashMap::new(),
                    select: HashSet::new(),
                    cursor: None,
                    backward: false,
                    distinct: vec![],
                    nearest: None,
                });
            }
        };

        let filter = if let Some(where_value) = args.get("where") {
            Some(parse_where_filter(
                where_value,
                relations,
                field_types,
                schema_context,
            )?)
        } else {
            None
        };

        let order_by = if let Some(order_value) = args.get("orderBy") {
            parse_order_by(order_value, Some(field_types))?
        } else {
            vec![]
        };

        let (take, backward) = if let Some(take_value) = args.get("take") {
            let n = parse_signed_int(take_value, "take")?;
            if n < 0 {
                (Some(n.unsigned_abs() as i32), true)
            } else {
                (Some(n as i32), false)
            }
        } else {
            (None, false)
        };

        let skip = if let Some(skip_value) = args.get("skip") {
            Some(parse_int(skip_value, "skip")?)
        } else {
            None
        };

        let cursor = if let Some(cursor_value) = args.get("cursor") {
            let obj = cursor_value.as_object().ok_or_else(|| {
                ProtocolError::InvalidParams("cursor must be an object".to_string())
            })?;
            let mut map = HashMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), json_to_value(v)?);
            }
            Some(map)
        } else {
            None
        };

        let include = if let Some(include_value) = args.get("include") {
            parse_include(include_value, relations, schema_context)?
        } else {
            HashMap::new()
        };

        let select = if let Some(select_value) = args.get("select") {
            parse_select(select_value)?
        } else {
            HashSet::new()
        };

        if !select.is_empty() && !include.is_empty() {
            return Err(ProtocolError::InvalidParams(
                "'select' and 'include' cannot be used together. Use 'select' for projection only, or 'include' for relation loading.".to_string(),
            ));
        }

        let distinct = if let Some(distinct_value) = args.get("distinct") {
            let arr = distinct_value.as_array().ok_or_else(|| {
                ProtocolError::InvalidParams(
                    "'distinct' must be an array of field names".to_string(),
                )
            })?;
            arr.iter()
                .map(|v| {
                    v.as_str()
                        .ok_or_else(|| {
                            ProtocolError::InvalidParams(
                                "each entry in 'distinct' must be a string field name".to_string(),
                            )
                        })
                        .map(str::to_string)
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![]
        };

        let nearest = if let Some(nearest_value) = args.get("nearest") {
            Some(parse_nearest_query(nearest_value, field_types)?)
        } else {
            None
        };

        if nearest.is_some() {
            if !matches!(take, Some(value) if value > 0) {
                return Err(ProtocolError::InvalidParams(
                    "'nearest' requires a positive 'take' limit".to_string(),
                ));
            }
            if backward {
                return Err(ProtocolError::InvalidParams(
                    "'nearest' does not support backward pagination".to_string(),
                ));
            }
            if cursor.is_some() {
                return Err(ProtocolError::InvalidParams(
                    "'nearest' cannot be combined with 'cursor'".to_string(),
                ));
            }
            if !distinct.is_empty() {
                return Err(ProtocolError::InvalidParams(
                    "'nearest' cannot be combined with 'distinct'".to_string(),
                ));
            }
        }

        Ok(QueryArgs {
            filter,
            order_by,
            take,
            skip,
            include,
            select,
            cursor,
            backward,
            distinct,
            nearest,
        })
    }
}

fn parse_nearest_query(
    value: &JsonValue,
    field_types: &FieldTypeMap,
) -> Result<VectorNearestQuery, ProtocolError> {
    let obj = value
        .as_object()
        .ok_or_else(|| ProtocolError::InvalidParams("'nearest' must be an object".to_string()))?;

    let field = obj
        .get("field")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            ProtocolError::InvalidParams("'nearest.field' must be a string".to_string())
        })?
        .to_string();

    let field_type = field_types.get(&field).ok_or_else(|| {
        ProtocolError::InvalidParams(format!(
            "'nearest.field' references unknown field '{}'",
            field
        ))
    })?;

    let ResolvedFieldType::Scalar(ScalarType::Vector { .. }) = field_type else {
        return Err(ProtocolError::InvalidParams(format!(
            "'nearest.field' must reference a Vector field, got '{}'",
            field
        )));
    };

    let query_json = obj
        .get("query")
        .ok_or_else(|| ProtocolError::InvalidParams("'nearest.query' is required".to_string()))?;
    let query = match json_to_value_field(query_json, field_type)? {
        Value::Vector(values) => values,
        _ => {
            return Err(ProtocolError::InvalidParams(
                "'nearest.query' must be a vector".to_string(),
            ));
        }
    };

    let metric = match obj.get("metric").and_then(JsonValue::as_str) {
        Some("l2") => VectorMetric::L2,
        Some("innerProduct") => VectorMetric::InnerProduct,
        Some("cosine") => VectorMetric::Cosine,
        Some(other) => {
            return Err(ProtocolError::InvalidParams(format!(
                "Unsupported nearest metric '{}'; expected one of: l2, innerProduct, cosine",
                other
            )));
        }
        None => {
            return Err(ProtocolError::InvalidParams(
                "'nearest.metric' is required".to_string(),
            ));
        }
    };

    Ok(VectorNearestQuery {
        field,
        query,
        metric,
    })
}
