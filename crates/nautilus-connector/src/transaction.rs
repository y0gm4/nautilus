//! Transaction executor for Nautilus.
//!
//! This module provides [`TransactionExecutor`], a single type that wraps a
//! live database transaction for any of the three supported backends
//! (PostgreSQL, MySQL, SQLite).  It replaces the previous per-backend trio
//! `TxPgExecutor` / `TxMysqlExecutor` / `TxSqliteExecutor`, which had
//! identical structure in three copies.
//!
//! ## Architecture note
//!
//! sqlx's `Transaction<'static, Db>` is parameterised by `Db`, making a true
//! Rust generic impossible without fighting GAT lifetime constraints (SQLite's
//! `SqliteArguments<'q>` carries a `'q` lifetime that PG/MySQL arguments do
//! not).  The type instead uses a private `TransactionInner` enum to hold
//! whichever backend's transaction is live, while presenting a uniform public
//! API to all callers.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use nautilus_core::Value;
use tokio::sync::Mutex;

use nautilus_dialect::Sql;

use crate::error::{ConnectorError as Error, Result};
use crate::row_stream::RowStream;
use crate::{Executor, Row};

/// Options for starting a transaction.
#[derive(Debug, Clone)]
pub struct TransactionOptions {
    /// Maximum duration before the transaction is automatically rolled back.
    pub timeout: Duration,
    /// Optional isolation level override.
    pub isolation_level: Option<IsolationLevel>,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            isolation_level: None,
        }
    }
}

/// Transaction isolation level.
///
/// Re-exported from `nautilus-protocol` for convenience; the connector uses
/// the same enum so callers don't need to depend on the protocol crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Read uncommitted — allows dirty reads.
    ReadUncommitted,
    /// Read committed — default for most databases.
    ReadCommitted,
    /// Repeatable read — prevents non-repeatable reads.
    RepeatableRead,
    /// Serializable — strictest isolation level.
    Serializable,
}

impl IsolationLevel {
    /// Returns the SQL representation (e.g., `"READ COMMITTED"`).
    pub fn as_sql(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }
}

/// Per-backend transaction storage.
///
/// This is a private implementation detail — callers always interact with the
/// outer [`TransactionExecutor`] type.
enum TransactionInner {
    Postgres(Arc<Mutex<Option<sqlx::Transaction<'static, sqlx::Postgres>>>>),
    Mysql(Arc<Mutex<Option<sqlx::Transaction<'static, sqlx::MySql>>>>),
    Sqlite(Arc<Mutex<Option<sqlx::Transaction<'static, sqlx::Sqlite>>>>),
}

type TxHandle<DB> = Arc<Mutex<Option<sqlx::Transaction<'static, DB>>>>;

/// An executor that runs queries inside a live database transaction.
///
/// This single type works with PostgreSQL, MySQL, and SQLite, replacing the
/// previous per-backend `TxPgExecutor` / `TxMysqlExecutor` / `TxSqliteExecutor`
/// trio.  Internally it holds a [`TransactionInner`] enum; callers see one
/// consistent API regardless of the backend in use.
///
/// The underlying sqlx transaction is stored behind an
/// `Arc<Mutex<Option<…>>>` so the executor can be shared cheaply through
/// [`crate::client::Client`]'s `Arc<E>` wrapping.
///
/// # Example
///
/// ```no_run
/// # use nautilus_connector::{Client, ConnectorResult};
/// # async fn example() -> ConnectorResult<()> {
/// let client = Client::postgres("postgres://localhost/mydb").await?;
/// let result = client.transaction(Default::default(), |tx| Box::pin(async move {
///     // tx is Client<TransactionExecutor>; all queries run inside the transaction.
///     Ok(42i64)
/// })).await?;
/// # Ok(())
/// # }
/// ```
pub struct TransactionExecutor {
    inner: TransactionInner,
}

impl TransactionExecutor {
    /// Wrap an already-begun PostgreSQL transaction.
    pub fn postgres(tx: sqlx::Transaction<'static, sqlx::Postgres>) -> Self {
        Self {
            inner: TransactionInner::Postgres(Arc::new(Mutex::new(Some(tx)))),
        }
    }

    /// Wrap an already-begun MySQL transaction.
    pub fn mysql(tx: sqlx::Transaction<'static, sqlx::MySql>) -> Self {
        Self {
            inner: TransactionInner::Mysql(Arc::new(Mutex::new(Some(tx)))),
        }
    }

    /// Wrap an already-begun SQLite transaction.
    pub fn sqlite(tx: sqlx::Transaction<'static, sqlx::Sqlite>) -> Self {
        Self {
            inner: TransactionInner::Sqlite(Arc::new(Mutex::new(Some(tx)))),
        }
    }

    async fn take_transaction<DB>(tx_arc: &TxHandle<DB>) -> Result<sqlx::Transaction<'static, DB>>
    where
        DB: sqlx::Database,
    {
        tx_arc
            .lock()
            .await
            .take()
            .ok_or_else(|| Error::database_msg("Transaction already closed"))
    }

    async fn transaction_is_open<DB>(tx_arc: &TxHandle<DB>) -> bool
    where
        DB: sqlx::Database,
    {
        tx_arc.lock().await.is_some()
    }

    fn execute_affected_on<DB, Bind, RowsAffected>(
        tx_arc: TxHandle<DB>,
        sql_text: String,
        params: Vec<Value>,
        bind: Bind,
        rows_affected: RowsAffected,
    ) -> BoxFuture<'static, Result<usize>>
    where
        DB: sqlx::Database + Send + 'static,
        for<'c> &'c mut <DB as sqlx::Database>::Connection: sqlx::Executor<'c, Database = DB>,
        for<'q> <DB as sqlx::Database>::Arguments<'q>: sqlx::IntoArguments<'q, DB>,
        for<'q> Bind: Fn(
                sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>,
                &'q Value,
            )
                -> Result<sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>>
            + Copy
            + Send
            + 'static,
        RowsAffected: Fn(<DB as sqlx::Database>::QueryResult) -> u64 + Copy + Send + 'static,
    {
        Box::pin(async move {
            let mut guard = tx_arc.lock().await;
            let tx = guard
                .as_mut()
                .ok_or_else(|| Error::database_msg("Transaction already closed"))?;
            let mut query = sqlx::query(&sql_text);
            for param in &params {
                query = bind(query, param)?;
            }

            use sqlx::Executor as _;
            let result = (&mut **tx)
                .execute(query)
                .await
                .map_err(|e| Error::database(e, "Mutation failed"))?;
            Ok(rows_affected(result) as usize)
        })
    }

    fn execute_collect_on<DB, Bind, Decode>(
        tx_arc: TxHandle<DB>,
        sql_text: String,
        params: Vec<Value>,
        bind: Bind,
        decode: Decode,
        query_context: &'static str,
    ) -> BoxFuture<'static, Result<Vec<Row>>>
    where
        DB: sqlx::Database + Send + 'static,
        for<'c> &'c mut <DB as sqlx::Database>::Connection: sqlx::Executor<'c, Database = DB>,
        for<'q> <DB as sqlx::Database>::Arguments<'q>: sqlx::IntoArguments<'q, DB>,
        for<'q> Bind: Fn(
                sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>,
                &'q Value,
            )
                -> Result<sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>>
            + Copy
            + Send
            + 'static,
        Decode: Fn(<DB as sqlx::Database>::Row) -> Result<Row> + Copy + Send + 'static,
    {
        Box::pin(async move {
            let mut guard = tx_arc.lock().await;
            let tx = guard
                .as_mut()
                .ok_or_else(|| Error::database_msg("Transaction already closed"))?;

            let mut query = sqlx::query(&sql_text);
            for param in &params {
                query = bind(query, param)?;
            }

            use sqlx::Executor as _;
            let rows = (&mut **tx)
                .fetch_all(query)
                .await
                .map_err(|e| Error::database(e, query_context))?;
            drop(guard);

            rows.into_iter().map(decode).collect()
        })
    }

    fn execute_and_fetch_collect_on<DB, Bind, Decode>(
        tx_arc: TxHandle<DB>,
        mutation_text: String,
        mutation_params: Vec<Value>,
        fetch_text: String,
        fetch_params: Vec<Value>,
        bind: Bind,
        decode: Decode,
    ) -> BoxFuture<'static, Result<Vec<Row>>>
    where
        DB: sqlx::Database + Send + 'static,
        for<'c> &'c mut <DB as sqlx::Database>::Connection: sqlx::Executor<'c, Database = DB>,
        for<'q> <DB as sqlx::Database>::Arguments<'q>: sqlx::IntoArguments<'q, DB>,
        for<'q> Bind: Fn(
                sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>,
                &'q Value,
            )
                -> Result<sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>>
            + Copy
            + Send
            + 'static,
        Decode: Fn(<DB as sqlx::Database>::Row) -> Result<Row> + Copy + Send + 'static,
    {
        Box::pin(async move {
            let mut guard = tx_arc.lock().await;
            let tx = guard
                .as_mut()
                .ok_or_else(|| Error::database_msg("Transaction already closed"))?;

            let mut mutation_query = sqlx::query(&mutation_text);
            for param in &mutation_params {
                mutation_query = bind(mutation_query, param)?;
            }

            use sqlx::Executor as _;
            (&mut **tx)
                .execute(mutation_query)
                .await
                .map_err(|e| Error::database(e, "Mutation failed"))?;

            let mut fetch_query = sqlx::query(&fetch_text);
            for param in &fetch_params {
                fetch_query = bind(fetch_query, param)?;
            }

            let rows = (&mut **tx)
                .fetch_all(fetch_query)
                .await
                .map_err(|e| Error::database(e, "Fetch failed"))?;
            drop(guard);

            rows.into_iter().map(decode).collect()
        })
    }

    /// Commit the transaction. After this, further queries will return an error.
    pub async fn commit(&self) -> Result<()> {
        match &self.inner {
            TransactionInner::Postgres(mx) => {
                let tx = Self::take_transaction(mx).await?;
                tx.commit()
                    .await
                    .map_err(|e| Error::database(e, "Commit failed"))
            }
            TransactionInner::Mysql(mx) => {
                let tx = Self::take_transaction(mx).await?;
                tx.commit()
                    .await
                    .map_err(|e| Error::database(e, "Commit failed"))
            }
            TransactionInner::Sqlite(mx) => {
                let tx = Self::take_transaction(mx).await?;
                tx.commit()
                    .await
                    .map_err(|e| Error::database(e, "Commit failed"))
            }
        }
    }

    /// Rollback the transaction. After this, further queries will return an error.
    pub async fn rollback(&self) -> Result<()> {
        match &self.inner {
            TransactionInner::Postgres(mx) => {
                let tx = Self::take_transaction(mx).await?;
                tx.rollback()
                    .await
                    .map_err(|e| Error::database(e, "Rollback failed"))
            }
            TransactionInner::Mysql(mx) => {
                let tx = Self::take_transaction(mx).await?;
                tx.rollback()
                    .await
                    .map_err(|e| Error::database(e, "Rollback failed"))
            }
            TransactionInner::Sqlite(mx) => {
                let tx = Self::take_transaction(mx).await?;
                tx.rollback()
                    .await
                    .map_err(|e| Error::database(e, "Rollback failed"))
            }
        }
    }

    /// Returns `true` if the transaction has not yet been committed or rolled back.
    pub async fn is_open(&self) -> bool {
        match &self.inner {
            TransactionInner::Postgres(mx) => Self::transaction_is_open(mx).await,
            TransactionInner::Mysql(mx) => Self::transaction_is_open(mx).await,
            TransactionInner::Sqlite(mx) => Self::transaction_is_open(mx).await,
        }
    }

    /// Execute a mutation SQL inside this transaction and return the number of
    /// affected rows.
    ///
    /// Used when `return_data = false` so no RETURNING clause is emitted and
    /// the affected-row count comes from the database execution result.
    pub async fn execute_affected(&self, sql: &Sql) -> Result<usize> {
        match &self.inner {
            TransactionInner::Postgres(tx_arc) => {
                Self::execute_affected_on(
                    Arc::clone(tx_arc),
                    sql.text.clone(),
                    sql.params.clone(),
                    crate::postgres::bind_value,
                    |result: sqlx::postgres::PgQueryResult| result.rows_affected(),
                )
                .await
            }
            TransactionInner::Mysql(tx_arc) => {
                Self::execute_affected_on(
                    Arc::clone(tx_arc),
                    sql.text.clone(),
                    sql.params.clone(),
                    crate::mysql::bind_value,
                    |result: sqlx::mysql::MySqlQueryResult| result.rows_affected(),
                )
                .await
            }
            TransactionInner::Sqlite(tx_arc) => {
                Self::execute_affected_on(
                    Arc::clone(tx_arc),
                    sql.text.clone(),
                    sql.params.clone(),
                    crate::sqlite::bind_value,
                    |result: sqlx::sqlite::SqliteQueryResult| result.rows_affected(),
                )
                .await
            }
        }
    }
}

impl Executor for TransactionExecutor {
    type Row<'conn>
        = Row
    where
        Self: 'conn;
    type RowStream<'conn>
        = RowStream<'conn>
    where
        Self: 'conn;

    fn execute<'conn>(&'conn self, sql: &'conn Sql) -> Self::RowStream<'conn> {
        match &self.inner {
            TransactionInner::Postgres(tx_arc) => {
                RowStream::from_rows_future(Self::execute_collect_on(
                    Arc::clone(tx_arc),
                    sql.text.clone(),
                    sql.params.clone(),
                    crate::postgres::bind_value,
                    crate::postgres_stream::decode_row_internal,
                    "Query failed",
                ))
            }
            TransactionInner::Mysql(tx_arc) => {
                RowStream::from_rows_future(Self::execute_collect_on(
                    Arc::clone(tx_arc),
                    sql.text.clone(),
                    sql.params.clone(),
                    crate::mysql::bind_value,
                    crate::mysql_stream::decode_row_internal,
                    "Query failed",
                ))
            }
            TransactionInner::Sqlite(tx_arc) => {
                RowStream::from_rows_future(Self::execute_collect_on(
                    Arc::clone(tx_arc),
                    sql.text.clone(),
                    sql.params.clone(),
                    crate::sqlite::bind_value,
                    crate::sqlite_stream::decode_row_internal,
                    "Query failed",
                ))
            }
        }
    }

    fn execute_and_fetch<'conn>(
        &'conn self,
        mutation: &'conn Sql,
        fetch: &'conn Sql,
    ) -> Self::RowStream<'conn> {
        match &self.inner {
            TransactionInner::Postgres(tx_arc) => {
                RowStream::from_rows_future(Self::execute_and_fetch_collect_on(
                    Arc::clone(tx_arc),
                    mutation.text.clone(),
                    mutation.params.clone(),
                    fetch.text.clone(),
                    fetch.params.clone(),
                    crate::postgres::bind_value,
                    crate::postgres_stream::decode_row_internal,
                ))
            }
            TransactionInner::Mysql(tx_arc) => {
                RowStream::from_rows_future(Self::execute_and_fetch_collect_on(
                    Arc::clone(tx_arc),
                    mutation.text.clone(),
                    mutation.params.clone(),
                    fetch.text.clone(),
                    fetch.params.clone(),
                    crate::mysql::bind_value,
                    crate::mysql_stream::decode_row_internal,
                ))
            }
            TransactionInner::Sqlite(tx_arc) => {
                RowStream::from_rows_future(Self::execute_and_fetch_collect_on(
                    Arc::clone(tx_arc),
                    mutation.text.clone(),
                    mutation.params.clone(),
                    fetch.text.clone(),
                    fetch.params.clone(),
                    crate::sqlite::bind_value,
                    crate::sqlite_stream::decode_row_internal,
                ))
            }
        }
    }

    fn execute_collect<'conn>(
        &'conn self,
        sql: &'conn Sql,
    ) -> BoxFuture<'conn, Result<Vec<Self::Row<'conn>>>>
    where
        Self: 'conn,
    {
        match &self.inner {
            TransactionInner::Postgres(tx_arc) => Self::execute_collect_on(
                Arc::clone(tx_arc),
                sql.text.clone(),
                sql.params.clone(),
                crate::postgres::bind_value,
                crate::postgres_stream::decode_row_internal,
                "Query failed",
            ),
            TransactionInner::Mysql(tx_arc) => Self::execute_collect_on(
                Arc::clone(tx_arc),
                sql.text.clone(),
                sql.params.clone(),
                crate::mysql::bind_value,
                crate::mysql_stream::decode_row_internal,
                "Query failed",
            ),
            TransactionInner::Sqlite(tx_arc) => Self::execute_collect_on(
                Arc::clone(tx_arc),
                sql.text.clone(),
                sql.params.clone(),
                crate::sqlite::bind_value,
                crate::sqlite_stream::decode_row_internal,
                "Query failed",
            ),
        }
    }
}
