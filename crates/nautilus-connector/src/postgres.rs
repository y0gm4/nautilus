//! PostgreSQL executor implementation.

use std::time::Duration;

use crate::error::{ConnectorError as Error, Result};
use crate::{ConnectorPoolOptions, Executor, PgRowStream, Row};
use futures::future::BoxFuture;
use nautilus_core::Value;
use nautilus_dialect::Sql;
use sqlx::postgres::types::PgHstore;
use sqlx::postgres::{PgPool, PgPoolOptions};

/// PostgreSQL executor using sqlx.
///
/// Manages a connection pool and executes queries against PostgreSQL databases.
///
/// ## Example
///
/// ```rust,ignore
/// use nautilus_connector::PgExecutor;
///
/// #[tokio::main]
/// async fn main() -> nautilus_core::Result<()> {
///     let executor = PgExecutor::new("postgres://user:pass@localhost/mydb").await?;
///     // Use executor to run queries...
///     Ok(())
/// }
/// ```
pub struct PgExecutor {
    pool: PgPool,
}

impl PgExecutor {
    /// Create a new PostgreSQL executor with a connection pool.
    ///
    /// ## Parameters
    ///
    /// - `url`: PostgreSQL connection URL (e.g., `postgres://user:pass@localhost/dbname`)
    ///
    /// ## Errors
    ///
    /// Returns `ConnectorError::Connection` if the pool cannot be created or if
    /// an initial connection test fails.
    pub async fn new(url: &str) -> Result<Self> {
        Self::new_with_options(url, ConnectorPoolOptions::default()).await
    }

    /// Create a new PostgreSQL executor with explicit pool overrides.
    ///
    /// Any override not provided keeps the same default used by [`Self::new`].
    pub async fn new_with_options(url: &str, pool_options: ConnectorPoolOptions) -> Result<Self> {
        let pool = pool_options
            .apply_to(
                PgPoolOptions::new()
                    .max_connections(10)
                    .min_connections(1)
                    .acquire_timeout(Duration::from_secs(10))
                    .idle_timeout(Duration::from_secs(300))
                    .test_before_acquire(true),
            )
            .connect(url)
            .await
            .map_err(|e| Error::connection(e, "Failed to connect to database"))?;

        Ok(Self { pool })
    }

    /// Get a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Execute a raw SQL statement with no result rows (e.g., DDL).
    pub async fn execute_raw(&self, sql: &str) -> Result<()> {
        sqlx::query(sql)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Error::database(e, "DDL error"))
    }

    fn execute_collect_internal<'conn>(
        &'conn self,
        sql: &'conn Sql,
    ) -> BoxFuture<'conn, Result<Vec<Row>>> {
        Box::pin(async move {
            let mut conn = self
                .pool
                .acquire()
                .await
                .map_err(|e| Error::connection(e, "Failed to acquire connection"))?;

            let mut query = sqlx::query(&sql.text);
            for param in &sql.params {
                query = bind_value(query, param)?;
            }

            // Fetch ALL rows at once so the connection completes the full
            // PostgreSQL extended-query cycle (portal close + ReadyForQuery)
            // before being returned to the pool. The previous streaming
            // approach (`query.fetch`) could leave the connection with an
            // open portal when the stream was dropped mid-iteration, causing
            // sqlx to discard the "dirty" connection and eventually exhaust
            // the pool.
            let pg_rows = query
                .fetch_all(&mut *conn)
                .await
                .map_err(|e| Error::database(e, "Query execution failed"))?;

            drop(conn);

            pg_rows
                .into_iter()
                .map(crate::postgres_stream::decode_row_internal)
                .collect()
        })
    }

    fn execute_and_fetch_collect_internal<'conn>(
        &'conn self,
        mutation: &'conn Sql,
        fetch: &'conn Sql,
    ) -> BoxFuture<'conn, Result<Vec<Row>>> {
        Box::pin(async move {
            use sqlx::Executor as _;

            let mut conn = self
                .pool
                .acquire()
                .await
                .map_err(|e| Error::connection(e, "Failed to acquire connection"))?;

            let mut mutation_query = sqlx::query(&mutation.text);
            for param in &mutation.params {
                mutation_query = bind_value(mutation_query, param)?;
            }

            (&mut *conn)
                .execute(mutation_query)
                .await
                .map_err(|e| Error::database(e, "Mutation failed"))?;

            let mut fetch_query = sqlx::query(&fetch.text);
            for param in &fetch.params {
                fetch_query = bind_value(fetch_query, param)?;
            }

            let pg_rows = fetch_query
                .fetch_all(&mut *conn)
                .await
                .map_err(|e| Error::database(e, "Fetch failed"))?;

            drop(conn);

            pg_rows
                .into_iter()
                .map(crate::postgres_stream::decode_row_internal)
                .collect()
        })
    }

    impl_execute_affected!();
}

/// [`Executor`] implementation backed by a PostgreSQL connection pool.
impl Executor for PgExecutor {
    type Row<'conn>
        = Row
    where
        Self: 'conn;
    type RowStream<'conn>
        = PgRowStream<'conn>
    where
        Self: 'conn;

    fn execute<'conn>(&'conn self, sql: &'conn Sql) -> Self::RowStream<'conn> {
        PgRowStream::from_rows_future(self.execute_collect_internal(sql))
    }

    fn execute_and_fetch<'conn>(
        &'conn self,
        mutation: &'conn Sql,
        fetch: &'conn Sql,
    ) -> Self::RowStream<'conn> {
        PgRowStream::from_rows_future(self.execute_and_fetch_collect_internal(mutation, fetch))
    }

    fn execute_collect<'conn>(
        &'conn self,
        sql: &'conn Sql,
    ) -> BoxFuture<'conn, Result<Vec<Self::Row<'conn>>>>
    where
        Self: 'conn,
    {
        self.execute_collect_internal(sql)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum PgArrayBinding {
    Strings(Vec<String>),
    Hstores(Vec<PgHstore>),
    Geometries(Vec<String>),
    Geographies(Vec<String>),
    I32s(Vec<i32>),
    I64s(Vec<i64>),
    F64s(Vec<f64>),
    Bools(Vec<bool>),
}

fn bindable_pg_array(items: &[Value]) -> Result<Option<PgArrayBinding>> {
    let Some(first) = items.first() else {
        return Ok(Some(PgArrayBinding::Strings(Vec::new())));
    };

    match first {
        Value::String(_) => {
            let mut values = Vec::with_capacity(items.len());
            for (idx, item) in items.iter().enumerate() {
                match item {
                    Value::String(value) => values.push(value.clone()),
                    Value::Null => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL typed array binding does not support NULL element at index {}",
                            idx
                        )));
                    }
                    other => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL array element at index {} has type {:?}; expected String",
                            idx, other
                        )));
                    }
                }
            }
            Ok(Some(PgArrayBinding::Strings(values)))
        }
        Value::Hstore(_) => {
            let mut values = Vec::with_capacity(items.len());
            for (idx, item) in items.iter().enumerate() {
                match item {
                    Value::Hstore(value) => values.push(PgHstore(value.clone())),
                    Value::Null => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL typed array binding does not support NULL element at index {}",
                            idx
                        )));
                    }
                    other => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL array element at index {} has type {:?}; expected Hstore",
                            idx, other
                        )));
                    }
                }
            }
            Ok(Some(PgArrayBinding::Hstores(values)))
        }
        Value::Geometry(_) => {
            let mut values = Vec::with_capacity(items.len());
            for (idx, item) in items.iter().enumerate() {
                match item {
                    Value::Geometry(value) => values.push(value.clone()),
                    Value::Null => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL typed array binding does not support NULL element at index {}",
                            idx
                        )));
                    }
                    other => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL array element at index {} has type {:?}; expected Geometry",
                            idx, other
                        )));
                    }
                }
            }
            Ok(Some(PgArrayBinding::Geometries(values)))
        }
        Value::Geography(_) => {
            let mut values = Vec::with_capacity(items.len());
            for (idx, item) in items.iter().enumerate() {
                match item {
                    Value::Geography(value) => values.push(value.clone()),
                    Value::Null => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL typed array binding does not support NULL element at index {}",
                            idx
                        )));
                    }
                    other => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL array element at index {} has type {:?}; expected Geography",
                            idx, other
                        )));
                    }
                }
            }
            Ok(Some(PgArrayBinding::Geographies(values)))
        }
        Value::I32(_) => {
            let mut values = Vec::with_capacity(items.len());
            for (idx, item) in items.iter().enumerate() {
                match item {
                    Value::I32(value) => values.push(*value),
                    Value::Null => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL typed array binding does not support NULL element at index {}",
                            idx
                        )));
                    }
                    other => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL array element at index {} has type {:?}; expected I32",
                            idx, other
                        )));
                    }
                }
            }
            Ok(Some(PgArrayBinding::I32s(values)))
        }
        Value::I64(_) => {
            let mut values = Vec::with_capacity(items.len());
            for (idx, item) in items.iter().enumerate() {
                match item {
                    Value::I64(value) => values.push(*value),
                    Value::Null => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL typed array binding does not support NULL element at index {}",
                            idx
                        )));
                    }
                    other => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL array element at index {} has type {:?}; expected I64",
                            idx, other
                        )));
                    }
                }
            }
            Ok(Some(PgArrayBinding::I64s(values)))
        }
        Value::F64(_) => {
            let mut values = Vec::with_capacity(items.len());
            for (idx, item) in items.iter().enumerate() {
                match item {
                    Value::F64(value) => values.push(*value),
                    Value::Null => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL typed array binding does not support NULL element at index {}",
                            idx
                        )));
                    }
                    other => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL array element at index {} has type {:?}; expected F64",
                            idx, other
                        )));
                    }
                }
            }
            Ok(Some(PgArrayBinding::F64s(values)))
        }
        Value::Bool(_) => {
            let mut values = Vec::with_capacity(items.len());
            for (idx, item) in items.iter().enumerate() {
                match item {
                    Value::Bool(value) => values.push(*value),
                    Value::Null => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL typed array binding does not support NULL element at index {}",
                            idx
                        )));
                    }
                    other => {
                        return Err(Error::database_msg(format!(
                            "PostgreSQL array element at index {} has type {:?}; expected Bool",
                            idx, other
                        )));
                    }
                }
            }
            Ok(Some(PgArrayBinding::Bools(values)))
        }
        _ => Ok(None),
    }
}

/// Binds a [`Value`] to a PostgreSQL sqlx query as a typed parameter.
///
/// Uses native binding for `Decimal`, `DateTime`, and `Uuid` (PG-specific).
/// Array values are bound as typed slices when the element type is known; unknown
/// or mixed-type arrays fall back to JSON string serialization.
pub(crate) fn bind_value<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    value: &'q Value,
) -> Result<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>> {
    match value {
        Value::Null => Ok(query.bind(None::<String>)),
        Value::Bool(b) => Ok(query.bind(b)),
        Value::I32(i) => Ok(query.bind(i)),
        Value::I64(i) => Ok(query.bind(i)),
        Value::F64(f) => Ok(query.bind(f)),
        Value::Decimal(d) => Ok(query.bind(d)),
        Value::DateTime(dt) => Ok(query.bind(*dt)),
        Value::Uuid(u) => Ok(query.bind(*u)),
        Value::String(s) => Ok(query.bind(s.as_str())),
        Value::Hstore(map) => Ok(query.bind(PgHstore(map.clone()))),
        Value::Geometry(raw) | Value::Geography(raw) => Ok(query.bind(raw.as_str())),
        Value::Vector(values) => Ok(query.bind(format_pg_vector(values)?)),
        Value::Bytes(b) => Ok(query.bind(b.as_slice())),
        Value::Json(j) => Ok(query.bind(j.to_string())),
        Value::Array(items) => match bindable_pg_array(items)? {
            Some(PgArrayBinding::Strings(values)) => Ok(query.bind(values)),
            Some(PgArrayBinding::Hstores(values)) => Ok(query.bind(values)),
            Some(PgArrayBinding::Geometries(values)) => Ok(query.bind(values)),
            Some(PgArrayBinding::Geographies(values)) => Ok(query.bind(values)),
            Some(PgArrayBinding::I32s(values)) => Ok(query.bind(values)),
            Some(PgArrayBinding::I64s(values)) => Ok(query.bind(values)),
            Some(PgArrayBinding::F64s(values)) => Ok(query.bind(values)),
            Some(PgArrayBinding::Bools(values)) => Ok(query.bind(values)),
            None => {
                let strings: Vec<String> = items
                    .iter()
                    .map(|v| crate::utils::value_to_json(v).to_string())
                    .collect();
                Ok(query.bind(strings))
            }
        },
        Value::Array2D(_) => {
            // Bind 2D arrays as a JSON string.
            // sqlx does not support multi-dimensional PostgreSQL arrays directly,
            // so we serialize to JSON and let the query cast if necessary.
            Ok(query.bind(crate::utils::value_to_json(value).to_string()))
        }
        // The PG dialect already appends `::type_name` to the placeholder, so
        // we only need to bind the underlying string value here.
        Value::Enum { value, .. } => Ok(query.bind(value.as_str())),
    }
}

fn format_pg_vector(values: &[f32]) -> Result<String> {
    let mut out = String::with_capacity(values.len().saturating_mul(8) + 2);
    out.push('[');
    for (idx, value) in values.iter().enumerate() {
        if !value.is_finite() {
            return Err(Error::database_msg(format!(
                "PostgreSQL vector element at index {} is not finite",
                idx
            )));
        }
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&value.to_string());
    }
    out.push(']');
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bindable_pg_array_keeps_homogeneous_strings() {
        let binding = bindable_pg_array(&[
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ])
        .expect("string array should bind");

        assert_eq!(
            binding,
            Some(PgArrayBinding::Strings(vec![
                "a".to_string(),
                "b".to_string()
            ]))
        );
    }

    #[test]
    fn bindable_pg_array_rejects_nulls_in_typed_arrays() {
        let err = bindable_pg_array(&[Value::I32(1), Value::Null]).unwrap_err();
        assert!(err.to_string().contains("NULL element"));
    }

    #[test]
    fn bindable_pg_array_keeps_homogeneous_hstores() {
        let binding = bindable_pg_array(&[
            Value::Hstore(std::collections::BTreeMap::from([(
                "display_name".to_string(),
                Some("Bob".to_string()),
            )])),
            Value::Hstore(std::collections::BTreeMap::from([(
                "nickname".to_string(),
                None,
            )])),
        ])
        .expect("hstore array should bind");

        assert_eq!(
            binding,
            Some(PgArrayBinding::Hstores(vec![
                PgHstore(std::collections::BTreeMap::from([(
                    "display_name".to_string(),
                    Some("Bob".to_string()),
                )])),
                PgHstore(std::collections::BTreeMap::from([(
                    "nickname".to_string(),
                    None,
                )])),
            ]))
        );
    }

    #[test]
    fn bindable_pg_array_rejects_mixed_typed_arrays() {
        let err =
            bindable_pg_array(&[Value::Bool(true), Value::String("nope".to_string())]).unwrap_err();
        assert!(err.to_string().contains("expected Bool"));
    }

    #[test]
    fn bindable_pg_array_falls_back_for_unsupported_types() {
        let binding = bindable_pg_array(&[Value::Decimal(rust_decimal::Decimal::new(123, 2))])
            .expect("unsupported arrays should fall back");
        assert_eq!(binding, None);
    }

    #[test]
    fn format_pg_vector_uses_pgvector_text_literal() {
        assert_eq!(format_pg_vector(&[1.0, 2.5, 3.25]).unwrap(), "[1,2.5,3.25]");
    }

    #[test]
    fn format_pg_vector_rejects_non_finite_values() {
        let err = format_pg_vector(&[1.0, f32::NAN]).unwrap_err();
        assert!(err.to_string().contains("not finite"));
    }
}
