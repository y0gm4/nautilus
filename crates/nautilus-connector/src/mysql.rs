//! MySQL executor implementation.

use crate::error::{ConnectorError as Error, Result};
use crate::{ConnectorPoolOptions, Executor, MysqlRowStream, Row};
use nautilus_core::Value;
use nautilus_dialect::Sql;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

/// MySQL executor using sqlx.
///
/// Manages a connection pool and executes queries against MySQL databases.
///
/// ## Example
///
/// ```rust,ignore
/// use nautilus_connector::MysqlExecutor;
///
/// #[tokio::main]
/// async fn main() -> nautilus_core::Result<()> {
///     let executor = MysqlExecutor::new("mysql://user:pass@localhost/mydb").await?;
///     // Use executor to run queries...
///     Ok(())
/// }
/// ```
pub struct MysqlExecutor {
    pool: MySqlPool,
}

impl MysqlExecutor {
    /// Create a new MySQL executor with a connection pool.
    ///
    /// ## Parameters
    ///
    /// - `url`: MySQL connection URL (e.g., `mysql://user:pass@localhost/mydb`)
    ///
    /// ## Errors
    ///
    /// Returns `ConnectorError::Connection` if the pool cannot be created.
    pub async fn new(url: &str) -> Result<Self> {
        Self::new_with_options(url, ConnectorPoolOptions::default()).await
    }

    /// Create a new MySQL executor with explicit pool overrides.
    ///
    /// Any override not provided keeps the same default used by [`Self::new`].
    pub async fn new_with_options(url: &str, pool_options: ConnectorPoolOptions) -> Result<Self> {
        let pool = pool_options
            .apply_to(MySqlPoolOptions::new().max_connections(5))
            .connect(url)
            .await
            .map_err(|e| Error::connection(e, "Failed to connect to MySQL"))?;

        Ok(Self { pool })
    }

    /// Get a reference to the underlying connection pool.
    pub fn pool(&self) -> &MySqlPool {
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

    impl_execute_affected!();
}

/// [`Executor`] implementation backed by a MySQL connection pool.
impl Executor for MysqlExecutor {
    type Row<'conn>
        = Row
    where
        Self: 'conn;
    type RowStream<'conn>
        = MysqlRowStream
    where
        Self: 'conn;

    fn execute<'conn>(&'conn self, sql: &'conn Sql) -> Self::RowStream<'conn> {
        let pool = self.pool.clone();
        let sql_text = sql.text.clone();
        let params = sql.params.clone();

        let stream = async_stream::stream! {
            let mut conn = match pool.acquire().await {
                Ok(c) => c,
                Err(e) => {
                    yield Err(Error::connection(e, "Failed to acquire connection"));
                    return;
                }
            };

            let mut query = sqlx::query(&sql_text);
            for param in &params {
                query = match bind_value(query, param) {
                    Ok(q) => q,
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                };
            }

            let mysql_rows = match query.fetch_all(&mut *conn).await {
                Ok(rows) => rows,
                Err(e) => {
                    yield Err(Error::database(e, "Query execution failed"));
                    return;
                }
            };

            drop(conn);

            for mysql_row in mysql_rows {
                match crate::mysql_stream::decode_row_internal(mysql_row) {
                    Ok(row) => yield Ok(row),
                    Err(e) => yield Err(e),
                }
            }
        };

        MysqlRowStream::new_from_stream(Box::pin(stream))
    }

    fn execute_and_fetch<'conn>(
        &'conn self,
        mutation: &'conn Sql,
        fetch: &'conn Sql,
    ) -> Self::RowStream<'conn> {
        let pool = self.pool.clone();
        let mutation_text = mutation.text.clone();
        let mutation_params = mutation.params.clone();
        let fetch_text = fetch.text.clone();
        let fetch_params = fetch.params.clone();

        let stream = async_stream::stream! {
            use sqlx::Executor as _;

            let mut conn = match pool.acquire().await {
                Ok(c) => c,
                Err(e) => {
                    yield Err(Error::connection(e, "Failed to acquire connection"));
                    return;
                }
            };

            let mut mutation_query = sqlx::query(&mutation_text);
            for param in &mutation_params {
                mutation_query = match bind_value(mutation_query, param) {
                    Ok(q) => q,
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                };
            }

            if let Err(e) = (&mut *conn).execute(mutation_query).await {
                yield Err(Error::database(e, "Mutation failed"));
                return;
            }

            let mut fetch_query = sqlx::query(&fetch_text);
            for param in &fetch_params {
                fetch_query = match bind_value(fetch_query, param) {
                    Ok(q) => q,
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                };
            }

            let mysql_rows = match fetch_query.fetch_all(&mut *conn).await {
                Ok(rows) => rows,
                Err(e) => {
                    yield Err(Error::database(e, "Fetch failed"));
                    return;
                }
            };

            drop(conn);

            for mysql_row in mysql_rows {
                match crate::mysql_stream::decode_row_internal(mysql_row) {
                    Ok(row) => yield Ok(row),
                    Err(e) => yield Err(e),
                }
            }
        };

        MysqlRowStream::new_from_stream(Box::pin(stream))
    }
}

/// Binds a [`Value`] to a MySQL sqlx query as a typed parameter.
///
/// `Decimal`, `DateTime`, and `Uuid` are serialized to strings because the
/// MySQL driver has no native support for those types.  Arrays are serialized
/// as JSON strings.
pub(crate) fn bind_value<'q>(
    query: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    value: &'q Value,
) -> Result<sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>> {
    match value {
        Value::Null => Ok(query.bind(None::<String>)),
        Value::Bool(b) => Ok(query.bind(b)),
        Value::I32(i) => Ok(query.bind(i)),
        Value::I64(i) => Ok(query.bind(i)),
        Value::F64(f) => Ok(query.bind(f)),
        Value::Decimal(d) => Ok(query.bind(d.to_string())),
        Value::DateTime(dt) => Ok(query.bind(dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string())),
        Value::Uuid(u) => Ok(query.bind(u.to_string())),
        Value::String(s) => Ok(query.bind(s.as_str())),
        Value::Geometry(raw) | Value::Geography(raw) => Ok(query.bind(raw.as_str())),
        Value::Hstore(_) => Err(Error::database_msg(
            "HSTORE values are only supported on PostgreSQL",
        )),
        Value::Vector(_) => Err(Error::database_msg(
            "VECTOR values are only supported on PostgreSQL",
        )),
        Value::Bytes(b) => Ok(query.bind(b.as_slice())),
        Value::Json(j) => Ok(query.bind(j.to_string())),
        Value::Array(_) => Ok(query.bind(crate::utils::value_to_json(value).to_string())),
        Value::Enum { value, .. } => Ok(query.bind(value.as_str())),
        Value::Array2D(_) => Ok(query.bind(crate::utils::value_to_json(value).to_string())),
    }
}
