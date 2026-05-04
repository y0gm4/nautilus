//! SQLite executor implementation.

use crate::error::{ConnectorError as Error, Result};
use crate::{ConnectorPoolOptions, Executor, Row, SqliteRowStream};
use futures::future::BoxFuture;
use nautilus_core::Value;
use nautilus_dialect::Sql;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

/// SQLite executor using sqlx.
///
/// Manages a connection pool and executes queries against SQLite databases.
///
/// ## Example
///
/// ```rust,ignore
/// use nautilus_connector::SqliteExecutor;
///
/// #[tokio::main]
/// async fn main() -> nautilus_core::Result<()> {
///     // File-based database
///     let executor = SqliteExecutor::new("sqlite:mydb.db").await?;
///     // Or in-memory database
///     let executor = SqliteExecutor::new("sqlite::memory:").await?;
///     // Use executor to run queries...
///     Ok(())
/// }
/// ```
pub struct SqliteExecutor {
    pool: SqlitePool,
}

impl SqliteExecutor {
    /// Create a new SQLite executor with a connection pool.
    ///
    /// ## Parameters
    ///
    /// - `url`: SQLite connection URL (e.g., `sqlite:mydb.db` or `sqlite::memory:`)
    ///
    /// ## Errors
    ///
    /// Returns `ConnectorError::Connection` if the pool cannot be created.
    pub async fn new(url: &str) -> Result<Self> {
        Self::new_with_options(url, ConnectorPoolOptions::default()).await
    }

    /// Create a new SQLite executor with explicit pool overrides.
    ///
    /// Any override not provided keeps the same default used by [`Self::new`].
    pub async fn new_with_options(url: &str, pool_options: ConnectorPoolOptions) -> Result<Self> {
        let pool = pool_options
            .apply_to(SqlitePoolOptions::new().max_connections(5))
            .connect(url)
            .await
            .map_err(|e| Error::connection(e, "Failed to connect to SQLite"))?;

        Ok(Self { pool })
    }

    /// Get a reference to the underlying connection pool.
    pub fn pool(&self) -> &SqlitePool {
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

            let sqlite_rows = query
                .fetch_all(&mut *conn)
                .await
                .map_err(|e| Error::database(e, "Query execution failed"))?;

            drop(conn);

            sqlite_rows
                .into_iter()
                .map(crate::sqlite_stream::decode_row_internal)
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

            let sqlite_rows = fetch_query
                .fetch_all(&mut *conn)
                .await
                .map_err(|e| Error::database(e, "Fetch failed"))?;

            drop(conn);

            sqlite_rows
                .into_iter()
                .map(crate::sqlite_stream::decode_row_internal)
                .collect()
        })
    }

    impl_execute_affected!();
}

/// [`Executor`] implementation backed by a SQLite connection pool.
impl Executor for SqliteExecutor {
    type Row<'conn>
        = Row
    where
        Self: 'conn;
    type RowStream<'conn>
        = SqliteRowStream<'conn>
    where
        Self: 'conn;

    fn execute<'conn>(&'conn self, sql: &'conn Sql) -> Self::RowStream<'conn> {
        SqliteRowStream::from_rows_future(self.execute_collect_internal(sql))
    }

    fn execute_and_fetch<'conn>(
        &'conn self,
        mutation: &'conn Sql,
        fetch: &'conn Sql,
    ) -> Self::RowStream<'conn> {
        SqliteRowStream::from_rows_future(self.execute_and_fetch_collect_internal(mutation, fetch))
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

/// Binds a [`Value`] to a SQLite sqlx query as a typed parameter.
///
/// `Decimal`, `DateTime`, and `Uuid` are serialized to strings because SQLite
/// has no native support for those types.  Arrays are serialized as JSON strings.
/// Note: `SqliteArguments` carries a lifetime parameter `'q`, unlike the PG/MySQL
/// argument types, which is why this function cannot be made generic across all
/// three backends with a single signature.
pub(crate) fn bind_value<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    value: &'q Value,
) -> Result<sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>> {
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
