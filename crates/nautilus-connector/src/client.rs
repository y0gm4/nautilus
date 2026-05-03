//! Client combining a SQL dialect with a database executor.

use std::future::Future;
use std::sync::Arc;

use crate::error::{ConnectorError as Error, Result};
use crate::transaction::TransactionOptions;
use crate::ConnectorPoolOptions;
use nautilus_dialect::Dialect;

use crate::Executor;

/// A Client holding both a Dialect (for SQL rendering) and an Executor (for query execution).
///
/// The Client uses `Arc` internally, making it cheap to clone and thread-safe.
/// This allows the same client to be shared across multiple parts of your application.
///
/// The Client is generic over the Executor type to work around limitations with
/// trait objects and Generic Associated Types (GATs).
///
/// # Example
///
/// ```no_run
/// # use nautilus_connector::{Client, ConnectorResult};
/// # async fn example() -> ConnectorResult<()> {
/// let client = Client::postgres("postgres://user:pass@localhost/db").await?;
/// // client can now be cloned and passed around cheaply
/// let clone = client.clone();
/// # Ok(())
/// # }
/// ```
pub struct Client<E>
where
    E: Executor,
{
    dialect: Arc<dyn Dialect + Send + Sync>,
    executor: Arc<E>,
}

impl<E> Client<E>
where
    E: Executor,
{
    /// Creates a new Client from a dialect and an executor.
    ///
    /// This is the generic constructor that works with any Dialect and Executor implementation.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use nautilus_connector::{Client, PgExecutor, ConnectorResult};
    /// # use nautilus_dialect::PostgresDialect;
    /// # async fn example() -> ConnectorResult<()> {
    /// let executor = PgExecutor::new("postgres://localhost/mydb").await?;
    /// let dialect = PostgresDialect;
    /// let client = Client::new(dialect, executor);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new<D>(dialect: D, executor: E) -> Self
    where
        D: Dialect + Send + Sync + 'static,
    {
        Self {
            dialect: Arc::new(dialect),
            executor: Arc::new(executor),
        }
    }

    /// Returns a reference to the underlying Dialect.
    ///
    /// Use this to render queries into SQL.
    pub fn dialect(&self) -> &(dyn Dialect + Send + Sync) {
        &*self.dialect
    }

    /// Returns a reference to the underlying Executor.
    ///
    /// Use this to execute rendered SQL queries against the database.
    pub fn executor(&self) -> &E {
        &self.executor
    }
}

async fn set_transaction_isolation(
    tx_executor: &crate::transaction::TransactionExecutor,
    isolation_level: Option<crate::IsolationLevel>,
) -> Result<()> {
    let Some(isolation_level) = isolation_level else {
        return Ok(());
    };

    let sql = nautilus_dialect::Sql {
        text: format!(
            "SET TRANSACTION ISOLATION LEVEL {}",
            isolation_level.as_sql()
        ),
        params: vec![],
    };

    crate::execute_all(tx_executor, &sql).await?;
    Ok(())
}

async fn drive_transaction<F, Fut, T, D>(
    tx_executor: crate::transaction::TransactionExecutor,
    dialect: D,
    opts: TransactionOptions,
    supports_isolation_level: bool,
    f: F,
) -> Result<T>
where
    F: FnOnce(Client<crate::transaction::TransactionExecutor>) -> Fut,
    Fut: Future<Output = Result<T>> + Send,
    T: Send + 'static,
    D: Dialect + Send + Sync + 'static,
{
    let TransactionOptions {
        timeout,
        isolation_level,
    } = opts;

    if supports_isolation_level {
        set_transaction_isolation(&tx_executor, isolation_level).await?;
    }

    let tx_client = Client::new(dialect, tx_executor);

    let result = if timeout.is_zero() {
        f(tx_client.clone()).await
    } else {
        match tokio::time::timeout(timeout, f(tx_client.clone())).await {
            Ok(result) => result,
            Err(_) => {
                let _ = tx_client.executor().rollback().await;
                return Err(Error::database_msg("Transaction timed out"));
            }
        }
    };

    match &result {
        Ok(_) => tx_client.executor().commit().await?,
        Err(_) => {
            let _ = tx_client.executor().rollback().await;
        }
    }

    result
}

/// Convenience constructors for specific database backends.
impl Client<crate::postgres::PgExecutor> {
    /// Creates a new PostgreSQL client.
    ///
    /// This is a convenience constructor that creates both a PostgresDialect
    /// and a PgExecutor, then wraps them in a Client.
    ///
    /// # Arguments
    ///
    /// * `url` - PostgreSQL connection string (e.g., "postgres://user:pass@localhost/db")
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use nautilus_connector::{Client, ConnectorResult};
    /// # async fn example() -> ConnectorResult<()> {
    /// let client = Client::postgres("postgres://localhost/mydb").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn postgres(url: &str) -> Result<Self> {
        Self::postgres_with_options(url, ConnectorPoolOptions::default()).await
    }

    /// Creates a new PostgreSQL client with explicit pool overrides.
    pub async fn postgres_with_options(
        url: &str,
        pool_options: ConnectorPoolOptions,
    ) -> Result<Self> {
        use crate::postgres::PgExecutor;
        use nautilus_dialect::PostgresDialect;

        let executor = PgExecutor::new_with_options(url, pool_options).await?;
        let dialect = PostgresDialect;
        Ok(Self::new(dialect, executor))
    }

    /// Execute an async closure inside a database transaction.
    ///
    /// The closure receives a `Client<TransactionExecutor>` whose queries all
    /// run on the same underlying connection.  If the closure returns `Ok`,
    /// the transaction is committed; if it returns `Err` (or panics), the
    /// transaction is rolled back.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use nautilus_connector::{Client, ConnectorResult};
    /// # async fn example() -> ConnectorResult<()> {
    /// let client = Client::postgres("postgres://localhost/mydb").await?;
    /// let result = client.transaction(Default::default(), |tx| Box::pin(async move {
    ///     // tx.executor() runs queries inside the transaction
    ///     Ok(42)
    /// })).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn transaction<F, Fut, T>(&self, opts: TransactionOptions, f: F) -> Result<T>
    where
        F: FnOnce(Client<crate::transaction::TransactionExecutor>) -> Fut,
        Fut: Future<Output = Result<T>> + Send,
        T: Send + 'static,
    {
        let sqlx_tx = self
            .executor()
            .pool()
            .begin()
            .await
            .map_err(|e| Error::connection(e, "Failed to begin transaction"))?;
        let tx_executor = crate::transaction::TransactionExecutor::postgres(sqlx_tx);

        drive_transaction(
            tx_executor,
            nautilus_dialect::PostgresDialect,
            opts,
            true,
            f,
        )
        .await
    }
}

/// Convenience constructor for MySQL.
impl Client<crate::mysql::MysqlExecutor> {
    /// Creates a new MySQL client.
    ///
    /// This is a convenience constructor that creates both a MysqlDialect
    /// and a MysqlExecutor, then wraps them in a Client.
    ///
    /// # Arguments
    ///
    /// * `url` - MySQL connection string (e.g., "mysql://user:pass@localhost/db")
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use nautilus_connector::{Client, ConnectorResult};
    /// # async fn example() -> ConnectorResult<()> {
    /// let client = Client::mysql("mysql://user:pass@localhost/mydb").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn mysql(url: &str) -> Result<Self> {
        Self::mysql_with_options(url, ConnectorPoolOptions::default()).await
    }

    /// Creates a new MySQL client with explicit pool overrides.
    pub async fn mysql_with_options(url: &str, pool_options: ConnectorPoolOptions) -> Result<Self> {
        use crate::mysql::MysqlExecutor;
        use nautilus_dialect::MysqlDialect;

        let executor = MysqlExecutor::new_with_options(url, pool_options).await?;
        let dialect = MysqlDialect;
        Ok(Self::new(dialect, executor))
    }

    /// Execute an async closure inside a MySQL transaction.
    ///
    /// See [`Client::<PgExecutor>::transaction`] for full documentation.
    pub async fn transaction<F, Fut, T>(&self, opts: TransactionOptions, f: F) -> Result<T>
    where
        F: FnOnce(Client<crate::transaction::TransactionExecutor>) -> Fut,
        Fut: Future<Output = Result<T>> + Send,
        T: Send + 'static,
    {
        let sqlx_tx = self
            .executor()
            .pool()
            .begin()
            .await
            .map_err(|e| Error::connection(e, "Failed to begin transaction"))?;
        let tx_executor = crate::transaction::TransactionExecutor::mysql(sqlx_tx);

        drive_transaction(tx_executor, nautilus_dialect::MysqlDialect, opts, true, f).await
    }
}

/// Convenience constructor for SQLite.
impl Client<crate::sqlite::SqliteExecutor> {
    /// Creates a new SQLite client.
    ///
    /// This is a convenience constructor that creates both a SqliteDialect
    /// and a SqliteExecutor, then wraps them in a Client.
    ///
    /// # Arguments
    ///
    /// * `url` - SQLite connection URL (e.g., `sqlite:mydb.db` or `sqlite::memory:`)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use nautilus_connector::{Client, ConnectorResult};
    /// # async fn example() -> ConnectorResult<()> {
    /// let client = Client::sqlite("sqlite::memory:").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn sqlite(url: &str) -> Result<Self> {
        Self::sqlite_with_options(url, ConnectorPoolOptions::default()).await
    }

    /// Creates a new SQLite client with explicit pool overrides.
    pub async fn sqlite_with_options(
        url: &str,
        pool_options: ConnectorPoolOptions,
    ) -> Result<Self> {
        use crate::sqlite::SqliteExecutor;
        use nautilus_dialect::SqliteDialect;

        let executor = SqliteExecutor::new_with_options(url, pool_options).await?;
        let dialect = SqliteDialect;
        Ok(Self::new(dialect, executor))
    }

    /// Execute an async closure inside a SQLite transaction.
    ///
    /// See [`Client::<PgExecutor>::transaction`] for full documentation.
    /// SQLite ignores `TransactionOptions::isolation_level` because it does not
    /// support `SET TRANSACTION ISOLATION LEVEL`.
    pub async fn transaction<F, Fut, T>(&self, opts: TransactionOptions, f: F) -> Result<T>
    where
        F: FnOnce(Client<crate::transaction::TransactionExecutor>) -> Fut,
        Fut: Future<Output = Result<T>> + Send,
        T: Send + 'static,
    {
        let sqlx_tx = self
            .executor()
            .pool()
            .begin()
            .await
            .map_err(|e| Error::connection(e, "Failed to begin transaction"))?;
        let tx_executor = crate::transaction::TransactionExecutor::sqlite(sqlx_tx);

        drive_transaction(tx_executor, nautilus_dialect::SqliteDialect, opts, false, f).await
    }
}

impl<E> Clone for Client<E>
where
    E: Executor,
{
    /// Cloning a Client is cheap - it only clones the Arc pointers, not the underlying data.
    fn clone(&self) -> Self {
        Self {
            dialect: Arc::clone(&self.dialect),
            executor: Arc::clone(&self.executor),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Client<crate::postgres::PgExecutor>>();
        assert_send_sync::<Client<crate::sqlite::SqliteExecutor>>();
        assert_send_sync::<Client<crate::mysql::MysqlExecutor>>();
    }

    #[tokio::test]
    async fn sqlite_transaction_ignores_isolation_level() {
        let client = Client::sqlite("sqlite::memory:")
            .await
            .expect("sqlite client should be created");

        let result = client
            .transaction(
                TransactionOptions {
                    timeout: std::time::Duration::from_secs(1),
                    isolation_level: Some(crate::IsolationLevel::Serializable),
                },
                |tx| {
                    Box::pin(async move {
                        let sql = nautilus_dialect::Sql {
                            text: "SELECT 1 AS one".to_string(),
                            params: vec![],
                        };
                        let rows = crate::execute_all(tx.executor(), &sql).await?;
                        assert_eq!(rows.len(), 1);
                        Ok(())
                    })
                },
            )
            .await;

        assert!(
            result.is_ok(),
            "sqlite transaction should not fail when an isolation level is requested: {result:?}"
        );
    }
}
