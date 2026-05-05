use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use nautilus_connector::{
    execute_all, Client, ConnectorPoolOptions, MysqlExecutor, PgExecutor, Row, SqliteExecutor,
    SqlxErrorKind, TransactionExecutor,
};
use nautilus_dialect::{Dialect, MysqlDialect, PostgresDialect, Sql, SqliteDialect};
use nautilus_migrate::DatabaseProvider;
use nautilus_protocol::ProtocolError;
use nautilus_schema::ir::{ModelIr, SchemaIr};

use crate::filter::RelationMap;
use crate::metadata::ModelMetadata;

const EXPIRED_TRANSACTION_RETENTION: Duration = Duration::from_secs(60);

/// Convert a [`nautilus_connector::ConnectorError`] to the appropriate [`ProtocolError`],
/// mapping specific constraint violation kinds to their dedicated error codes.
fn connector_to_protocol(e: nautilus_connector::ConnectorError, context: &str) -> ProtocolError {
    let msg = format!("{}: {}", context, e);
    match e.sqlx_kind() {
        SqlxErrorKind::UniqueConstraint => ProtocolError::UniqueConstraintViolation(msg),
        SqlxErrorKind::ForeignKeyConstraint => ProtocolError::ForeignKeyConstraintViolation(msg),
        SqlxErrorKind::CheckConstraint => ProtocolError::CheckConstraintViolation(msg),
        SqlxErrorKind::NullConstraint => ProtocolError::NullConstraintViolation(msg),
        SqlxErrorKind::Deadlock => ProtocolError::Deadlock(msg),
        SqlxErrorKind::SerializationFailure => ProtocolError::SerializationFailure(msg),
        SqlxErrorKind::PoolTimedOut | SqlxErrorKind::PoolClosed => {
            ProtocolError::ConnectionFailed(msg)
        }
        _ => ProtocolError::DatabaseExecution(msg),
    }
}

/// Engine state holding parsed schema and database connection.
pub struct EngineState {
    /// Model lookup map (logical name -> IR).
    pub models: HashMap<String, ModelIr>,
    /// Cached per-model metadata reused by the hot query paths.
    model_metadata: HashMap<String, ModelMetadata>,
    /// The full validated schema IR.
    pub schema: SchemaIr,
    /// SQL dialect renderer.
    pub dialect: Arc<dyn Dialect + Send + Sync>,
    /// Database connection (pooled / proxied URL).
    pub client: DatabaseClient,
    /// Optional direct connection that bypasses poolers like PgBouncer.
    /// Used for raw SQL queries when `direct_url` is configured in the schema.
    direct_client: Option<DatabaseClient>,
    /// Active interactive transactions, keyed by transaction ID.
    pub transactions: Arc<Mutex<HashMap<String, ActiveTransaction>>>,
    /// Recently expired interactive transactions, kept briefly so late follow-up
    /// calls still report a timeout instead of an unknown transaction.
    expired_transactions: Arc<Mutex<HashMap<String, Instant>>>,
}

/// An active interactive transaction managed by the engine.
#[derive(Clone)]
pub struct ActiveTransaction {
    /// The transaction-scoped database client.
    pub client: TransactionClient,
    /// When this transaction was started.
    pub created_at: Instant,
    /// Maximum lifetime before auto-rollback.
    pub timeout: Duration,
}

/// A transaction-scoped database client shared across all backends.
pub type TransactionClient = Client<TransactionExecutor>;

/// Enum to hold different client types.
pub enum DatabaseClient {
    /// PostgreSQL client.
    Postgres(Client<PgExecutor>),
    /// MySQL client.
    Mysql(Client<MysqlExecutor>),
    /// SQLite client.
    Sqlite(Client<SqliteExecutor>),
}

/// Dispatch an expression across all [`DatabaseClient`] variants.
macro_rules! with_client {
    ($self:expr, $client:ident => $body:expr) => {
        match $self {
            DatabaseClient::Postgres($client) => $body,
            DatabaseClient::Mysql($client) => $body,
            DatabaseClient::Sqlite($client) => $body,
        }
    };
}

impl DatabaseClient {
    /// Execute a rendered SQL query and return all result rows.
    pub async fn execute_query(&self, sql: &Sql, context: &str) -> Result<Vec<Row>, ProtocolError> {
        with_client!(self, client => {
            execute_all(client.executor(), sql)
                .await
                .map_err(|e| connector_to_protocol(e, context))
        })
    }

    /// Execute a mutation SQL and return the number of affected rows.
    pub async fn execute_affected(&self, sql: &Sql, context: &str) -> Result<usize, ProtocolError> {
        with_client!(self, client => {
            client.executor()
                .execute_affected(sql)
                .await
                .map_err(|e| connector_to_protocol(e, context))
        })
    }

    /// Execute a raw DDL statement (no parameters, no result rows).
    pub async fn execute_raw(&self, stmt: &str) -> Result<(), Box<dyn std::error::Error>> {
        with_client!(self, client => client.executor().execute_raw(stmt).await?);
        Ok(())
    }
}

impl EngineState {
    /// Connect to a database and return a `(dialect, client)` pair.
    async fn build_client(
        provider: DatabaseProvider,
        url: &str,
        pool_options: ConnectorPoolOptions,
    ) -> Result<(Arc<dyn Dialect + Send + Sync>, DatabaseClient), Box<dyn std::error::Error>> {
        match provider {
            DatabaseProvider::Postgres => {
                let pg_client = Client::postgres_with_options(url, pool_options).await?;
                Ok((
                    Arc::new(PostgresDialect),
                    DatabaseClient::Postgres(pg_client),
                ))
            }
            DatabaseProvider::Mysql => {
                let mysql_client = Client::mysql_with_options(url, pool_options).await?;
                Ok((Arc::new(MysqlDialect), DatabaseClient::Mysql(mysql_client)))
            }
            DatabaseProvider::Sqlite => {
                let sqlite_client = Client::sqlite_with_options(url, pool_options).await?;
                Ok((
                    Arc::new(SqliteDialect),
                    DatabaseClient::Sqlite(sqlite_client),
                ))
            }
        }
    }

    /// Create a new engine state by connecting to the database.
    ///
    /// `direct_url`, when provided, opens a second connection that bypasses
    /// poolers (e.g. PgBouncer). Raw SQL queries prefer this connection.
    pub async fn new(
        schema: SchemaIr,
        database_url: String,
        direct_url: Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_pool_options(
            schema,
            database_url,
            direct_url,
            ConnectorPoolOptions::default(),
        )
        .await
    }

    /// Create a new engine state by connecting to the database with explicit pool overrides.
    ///
    /// `direct_url`, when provided, opens a second connection that bypasses
    /// poolers (e.g. PgBouncer). Raw SQL queries prefer this connection.
    pub async fn new_with_pool_options(
        schema: SchemaIr,
        database_url: String,
        direct_url: Option<String>,
        pool_options: ConnectorPoolOptions,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let models = schema.models.clone();
        let model_metadata = models
            .iter()
            .map(|(name, model)| (name.clone(), ModelMetadata::new(model)))
            .collect();

        let datasource = schema
            .datasource
            .as_ref()
            .ok_or("No datasource found in schema")?;

        let provider = DatabaseProvider::from_schema_provider(&datasource.provider)
            .ok_or_else(|| format!("Unsupported database provider: {}", datasource.provider))?;

        let resolved_url = resolve_database_url(&database_url)?;
        let (dialect, client) = Self::build_client(provider, &resolved_url, pool_options).await?;

        let direct_client = if let Some(raw_direct) = direct_url {
            let resolved_direct = resolve_database_url(&raw_direct)?;
            let (_, dc) = Self::build_client(provider, &resolved_direct, pool_options).await?;
            Some(dc)
        } else {
            None
        };

        Ok(EngineState {
            models,
            model_metadata,
            schema,
            dialect,
            client,
            direct_client,
            transactions: Arc::new(Mutex::new(HashMap::new())),
            expired_transactions: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Return cached metadata for a validated model.
    pub(crate) fn model_metadata(&self, model: &ModelIr) -> &ModelMetadata {
        self.model_metadata
            .get(&model.logical_name)
            .expect("engine metadata missing for validated model")
    }

    /// Return the lazily cached relation map for a validated model.
    pub(crate) fn relation_map_for_model(
        &self,
        model: &ModelIr,
    ) -> Result<&RelationMap, ProtocolError> {
        self.model_metadata(model).relation_map(model, &self.models)
    }

    /// Look up a related model together with its cached metadata.
    pub(crate) fn related_model(&self, model_name: &str) -> Option<(&ModelIr, &ModelMetadata)> {
        Some((
            self.models.get(model_name)?,
            self.model_metadata.get(model_name)?,
        ))
    }

    /// Execute raw DDL SQL statements against the database.
    ///
    /// Used for running migrations (CREATE TABLE, etc.).
    pub async fn execute_ddl_sql(
        &self,
        statements: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for stmt in &statements {
            if stmt.trim().is_empty() {
                continue;
            }
            self.client.execute_raw(stmt).await?;
        }
        Ok(())
    }

    /// Execute a SQL query, optionally inside a transaction.
    ///
    /// If `tx_id` is `Some`, the query runs on the transaction's connection;
    /// otherwise it runs on the pool-backed default connection.
    pub async fn execute_query_on(
        &self,
        sql: &Sql,
        context: &str,
        tx_id: Option<&str>,
    ) -> Result<Vec<Row>, ProtocolError> {
        match tx_id {
            None => self.client.execute_query(sql, context).await,
            Some(id) => {
                let tx_client = self.transaction_client_for_request(id).await?;
                execute_all(tx_client.executor(), sql)
                    .await
                    .map_err(|e| connector_to_protocol(e, context))
            }
        }
    }

    /// Execute a SQL query using the direct connection when available, otherwise the pooled one.
    ///
    /// Raw SQL queries should use this so they bypass connection poolers (e.g. PgBouncer)
    /// that do not support prepared statements. If a `tx_id` is provided the query always
    /// runs on the transaction's connection regardless.
    pub async fn execute_direct_query_on(
        &self,
        sql: &Sql,
        context: &str,
        tx_id: Option<&str>,
    ) -> Result<Vec<Row>, ProtocolError> {
        if tx_id.is_some() {
            return self.execute_query_on(sql, context, tx_id).await;
        }
        match &self.direct_client {
            Some(direct) => direct.execute_query(sql, context).await,
            None => self.client.execute_query(sql, context).await,
        }
    }

    /// Execute a mutation SQL and return the affected-row count, optionally
    /// inside a transaction.
    ///
    /// Use this when `return_data = false` so no RETURNING clause is emitted.
    pub async fn execute_affected_on(
        &self,
        sql: &Sql,
        context: &str,
        tx_id: Option<&str>,
    ) -> Result<usize, ProtocolError> {
        match tx_id {
            None => self.client.execute_affected(sql, context).await,
            Some(id) => {
                let tx_client = self.transaction_client_for_request(id).await?;
                tx_client
                    .executor()
                    .execute_affected(sql)
                    .await
                    .map_err(|e| connector_to_protocol(e, context))
            }
        }
    }

    /// Begin a new interactive transaction.
    pub async fn begin_transaction(
        &self,
        id: String,
        timeout: Duration,
        isolation_level: Option<nautilus_protocol::IsolationLevel>,
    ) -> Result<(), ProtocolError> {
        let tx_client = match &self.client {
            DatabaseClient::Postgres(c) => {
                let sqlx_tx = c.executor().pool().begin().await.map_err(|e| {
                    ProtocolError::TransactionFailed(format!("BEGIN failed: {}", e))
                })?;
                let tx_exec = TransactionExecutor::postgres(sqlx_tx);
                if let Some(iso) = isolation_level {
                    let iso_sql = format!("SET TRANSACTION ISOLATION LEVEL {}", iso.as_sql());
                    let sql = Sql {
                        text: iso_sql,
                        params: vec![],
                    };
                    execute_all(&tx_exec, &sql).await.map_err(|e| {
                        ProtocolError::TransactionFailed(format!("SET ISOLATION failed: {}", e))
                    })?;
                }
                Client::new(PostgresDialect, tx_exec)
            }
            DatabaseClient::Mysql(c) => {
                let sqlx_tx = c.executor().pool().begin().await.map_err(|e| {
                    ProtocolError::TransactionFailed(format!("BEGIN failed: {}", e))
                })?;
                let tx_exec = TransactionExecutor::mysql(sqlx_tx);
                if let Some(iso) = isolation_level {
                    let iso_sql = format!("SET TRANSACTION ISOLATION LEVEL {}", iso.as_sql());
                    let sql = Sql {
                        text: iso_sql,
                        params: vec![],
                    };
                    execute_all(&tx_exec, &sql).await.map_err(|e| {
                        ProtocolError::TransactionFailed(format!("SET ISOLATION failed: {}", e))
                    })?;
                }
                Client::new(MysqlDialect, tx_exec)
            }
            DatabaseClient::Sqlite(c) => {
                let sqlx_tx = c.executor().pool().begin().await.map_err(|e| {
                    ProtocolError::TransactionFailed(format!("BEGIN failed: {}", e))
                })?;
                let tx_exec = TransactionExecutor::sqlite(sqlx_tx);
                // SQLite doesn't support SET TRANSACTION ISOLATION LEVEL
                Client::new(SqliteDialect, tx_exec)
            }
        };

        let active = ActiveTransaction {
            client: tx_client,
            created_at: Instant::now(),
            timeout,
        };

        self.expired_transactions.lock().await.remove(&id);
        self.transactions.lock().await.insert(id, active);
        Ok(())
    }

    /// Register an already-open transaction client so engine requests can reuse it.
    ///
    /// This is used by embedded generated clients that manage the database
    /// transaction outside the engine but still want all query semantics to flow
    /// through the engine handlers.
    pub async fn register_external_transaction(
        &self,
        id: String,
        client: TransactionClient,
        timeout: Duration,
    ) {
        let active = ActiveTransaction {
            client,
            created_at: Instant::now(),
            timeout,
        };

        self.expired_transactions.lock().await.remove(&id);
        self.transactions.lock().await.insert(id, active);
    }

    /// Remove a previously registered external transaction without committing it.
    ///
    /// The caller remains responsible for committing or rolling back the actual
    /// database transaction.
    pub async fn unregister_external_transaction(&self, id: &str) {
        self.transactions.lock().await.remove(id);
        self.expired_transactions.lock().await.remove(id);
    }

    /// Commit a transaction by ID and remove it from the map.
    pub async fn commit_transaction(&self, id: &str) -> Result<(), ProtocolError> {
        let active = self.take_transaction(id).await?;
        if active.created_at.elapsed() > active.timeout {
            self.expire_active_transaction(id, active).await;
            return Err(Self::transaction_timeout_error(id));
        }
        active
            .client
            .executor()
            .commit()
            .await
            .map_err(|e| ProtocolError::TransactionFailed(format!("Commit failed: {}", e)))
    }

    /// Rollback a transaction by ID and remove it from the map.
    pub async fn rollback_transaction(&self, id: &str) -> Result<(), ProtocolError> {
        let active = self.take_transaction(id).await?;
        if active.created_at.elapsed() > active.timeout {
            self.expire_active_transaction(id, active).await;
            return Err(Self::transaction_timeout_error(id));
        }
        active
            .client
            .executor()
            .rollback()
            .await
            .map_err(|e| ProtocolError::TransactionFailed(format!("Rollback failed: {}", e)))
    }

    /// Expire (rollback + remove) a timed-out transaction.
    async fn expire_transaction(&self, id: &str) {
        if let Some(active) = self.transactions.lock().await.remove(id) {
            self.expire_active_transaction(id, active).await;
        }
    }

    /// Reap all timed-out transactions. Called periodically by the engine.
    pub async fn reap_expired_transactions(&self) {
        let expired: Vec<(String, ActiveTransaction)> = {
            let mut txs = self.transactions.lock().await;
            let expired_ids: Vec<String> = txs
                .iter()
                .filter(|(_, tx)| tx.created_at.elapsed() > tx.timeout)
                .map(|(id, _)| id.clone())
                .collect();
            expired_ids
                .into_iter()
                .filter_map(|id| txs.remove(&id).map(|active| (id, active)))
                .collect()
        };
        for (id, active) in expired {
            eprintln!("[engine] Reaping expired transaction: {}", id);
            self.expire_active_transaction(&id, active).await;
        }
    }

    fn transaction_timeout_error(id: &str) -> ProtocolError {
        ProtocolError::TransactionTimeout(format!("Transaction '{}' timed out", id))
    }

    fn transaction_not_found_error(id: &str) -> ProtocolError {
        ProtocolError::TransactionNotFound(format!("Transaction '{}' not found", id))
    }

    async fn transaction_lookup_error(&self, id: &str) -> ProtocolError {
        let mut expired = self.expired_transactions.lock().await;
        expired.retain(|_, expired_at| expired_at.elapsed() <= EXPIRED_TRANSACTION_RETENTION);
        if expired.contains_key(id) {
            Self::transaction_timeout_error(id)
        } else {
            Self::transaction_not_found_error(id)
        }
    }

    async fn transaction_client_for_request(
        &self,
        id: &str,
    ) -> Result<TransactionClient, ProtocolError> {
        enum TransactionLookup {
            Ready(TransactionClient),
            TimedOut,
            Missing,
        }

        let lookup = {
            let txs = self.transactions.lock().await;
            match txs.get(id) {
                Some(active) if active.created_at.elapsed() > active.timeout => {
                    TransactionLookup::TimedOut
                }
                Some(active) => TransactionLookup::Ready(active.client.clone()),
                None => TransactionLookup::Missing,
            }
        };

        match lookup {
            TransactionLookup::Ready(client) => Ok(client),
            TransactionLookup::TimedOut => {
                self.expire_transaction(id).await;
                Err(Self::transaction_timeout_error(id))
            }
            TransactionLookup::Missing => Err(self.transaction_lookup_error(id).await),
        }
    }

    async fn take_transaction(&self, id: &str) -> Result<ActiveTransaction, ProtocolError> {
        match self.transactions.lock().await.remove(id) {
            Some(active) => Ok(active),
            None => Err(self.transaction_lookup_error(id).await),
        }
    }

    async fn expire_active_transaction(&self, id: &str, active: ActiveTransaction) {
        {
            let mut expired = self.expired_transactions.lock().await;
            expired.retain(|_, expired_at| expired_at.elapsed() <= EXPIRED_TRANSACTION_RETENTION);
            expired.insert(id.to_string(), Instant::now());
        }
        let _ = active.client.executor().rollback().await;
    }
}

/// Resolve database URL, handling env() references.
fn resolve_database_url(url: &str) -> Result<String, Box<dyn std::error::Error>> {
    nautilus_schema::resolve_env_url(url).map_err(|msg| msg.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Arc;

    use nautilus_core::Value;
    use nautilus_migrate::{DatabaseProvider, DdlGenerator};
    use nautilus_schema::validate_schema_source;
    use tempfile::TempDir;

    fn parse_ir(source: &str) -> SchemaIr {
        validate_schema_source(source)
            .expect("validation failed")
            .ir
    }

    fn test_db_url() -> (String, TempDir) {
        let dir = tempfile::Builder::new()
            .prefix("transaction-timeout-state-tests")
            .tempdir()
            .expect("failed to create sqlite test directory");

        let path = dir.path().join("test.db");
        fs::File::create(&path).expect("failed to create sqlite test file");
        let url = format!("sqlite:///{}", path.to_string_lossy().replace('\\', "/"));
        (url, dir)
    }

    async fn sqlite_state(schema_source: &str) -> (EngineState, TempDir) {
        let schema = parse_ir(schema_source);
        let (database_url, temp_dir) = test_db_url();
        let state = EngineState::new(schema.clone(), database_url, None)
            .await
            .expect("failed to create engine state");

        let ddl = DdlGenerator::new(DatabaseProvider::Sqlite)
            .generate_create_tables(&schema)
            .expect("failed to build ddl");
        state
            .execute_ddl_sql(ddl)
            .await
            .expect("failed to apply ddl");

        (state, temp_dir)
    }

    fn schema_source() -> &'static str {
        r#"
datasource db {
  provider = "sqlite"
  url      = "sqlite::memory:"
}

model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#
    }

    fn insert_user_sql(name: &str) -> Sql {
        Sql {
            text: r#"INSERT INTO "User" ("name") VALUES (?)"#.to_string(),
            params: vec![Value::String(name.to_string())],
        }
    }

    fn long_running_sql(iterations: usize) -> Sql {
        Sql {
            text: format!(
                "WITH RECURSIVE cnt(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM cnt WHERE x < {iterations}) SELECT MAX(x) AS value FROM cnt"
            ),
            params: vec![],
        }
    }

    async fn count_users(state: &EngineState) -> usize {
        let sql = Sql {
            text: r#"SELECT "id" FROM "User""#.to_string(),
            params: vec![],
        };
        state
            .execute_query_on(&sql, "count users", None)
            .await
            .expect("count query should succeed")
            .len()
    }

    #[tokio::test]
    async fn commit_after_timeout_returns_timeout_and_rolls_back() {
        let (state, temp_dir) = sqlite_state(schema_source()).await;
        let tx_id = "commit-timeout".to_string();

        state
            .begin_transaction(tx_id.clone(), Duration::from_millis(10), None)
            .await
            .expect("transaction should start");
        state
            .execute_affected_on(&insert_user_sql("Alice"), "insert user", Some(&tx_id))
            .await
            .expect("insert inside tx should succeed");

        tokio::time::sleep(Duration::from_millis(30)).await;

        let err = state
            .commit_transaction(&tx_id)
            .await
            .expect_err("commit should time out");
        assert!(matches!(err, ProtocolError::TransactionTimeout(_)));
        assert_eq!(count_users(&state).await, 0);

        let lookup_err = state
            .commit_transaction(&tx_id)
            .await
            .expect_err("late commit should keep surfacing timeout");
        assert!(matches!(lookup_err, ProtocolError::TransactionTimeout(_)));

        drop(state);
        drop(temp_dir);
    }

    #[tokio::test]
    async fn rollback_after_timeout_returns_timeout() {
        let (state, temp_dir) = sqlite_state(schema_source()).await;
        let tx_id = "rollback-timeout".to_string();

        state
            .begin_transaction(tx_id.clone(), Duration::from_millis(10), None)
            .await
            .expect("transaction should start");

        tokio::time::sleep(Duration::from_millis(30)).await;

        let err = state
            .rollback_transaction(&tx_id)
            .await
            .expect_err("rollback should time out");
        assert!(matches!(err, ProtocolError::TransactionTimeout(_)));

        let lookup_err = state
            .rollback_transaction(&tx_id)
            .await
            .expect_err("late rollback should keep surfacing timeout");
        assert!(matches!(lookup_err, ProtocolError::TransactionTimeout(_)));

        drop(state);
        drop(temp_dir);
    }

    #[tokio::test]
    async fn reaping_idle_transactions_rolls_back_uncommitted_changes() {
        let (state, temp_dir) = sqlite_state(schema_source()).await;
        let tx_id = "idle-timeout".to_string();

        state
            .begin_transaction(tx_id.clone(), Duration::from_millis(10), None)
            .await
            .expect("transaction should start");
        state
            .execute_affected_on(&insert_user_sql("Bob"), "insert user", Some(&tx_id))
            .await
            .expect("insert inside tx should succeed");

        tokio::time::sleep(Duration::from_millis(30)).await;
        state.reap_expired_transactions().await;

        assert_eq!(count_users(&state).await, 0);
        let err = state
            .execute_affected_on(&insert_user_sql("Carol"), "insert user", Some(&tx_id))
            .await
            .expect_err("expired tx should now reject further work");
        assert!(matches!(err, ProtocolError::TransactionTimeout(_)));

        drop(state);
        drop(temp_dir);
    }

    #[tokio::test]
    async fn registered_external_transaction_exposes_uncommitted_rows_to_engine_queries() {
        let (state, temp_dir) = sqlite_state(schema_source()).await;

        let tx_client = match &state.client {
            DatabaseClient::Sqlite(client) => {
                let sqlx_tx = client
                    .executor()
                    .pool()
                    .begin()
                    .await
                    .expect("sqlite transaction should start");
                let tx_exec = TransactionExecutor::sqlite(sqlx_tx);
                Client::new(SqliteDialect, tx_exec)
            }
            _ => panic!("expected sqlite engine state"),
        };

        let tx_id = "external-tx".to_string();
        state
            .register_external_transaction(tx_id.clone(), tx_client.clone(), Duration::from_secs(5))
            .await;

        state
            .execute_affected_on(&insert_user_sql("Dora"), "insert user", Some(&tx_id))
            .await
            .expect("engine should execute writes on registered external transaction");

        let rows = state
            .execute_query_on(
                &Sql {
                    text: r#"SELECT "name" FROM "User""#.to_string(),
                    params: vec![],
                },
                "select users in tx",
                Some(&tx_id),
            )
            .await
            .expect("engine should read uncommitted rows on registered external transaction");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("name"),
            Some(&Value::String("Dora".to_string())),
        );
        assert_eq!(count_users(&state).await, 0);

        state.unregister_external_transaction(&tx_id).await;
        tx_client
            .executor()
            .rollback()
            .await
            .expect("rollback should succeed");

        drop(state);
        drop(temp_dir);
    }

    #[tokio::test]
    async fn long_running_transaction_query_does_not_block_new_transactions() {
        let (state, temp_dir) = sqlite_state(schema_source()).await;
        let state = Arc::new(state);
        let tx_id = "long-query".to_string();

        state
            .begin_transaction(tx_id.clone(), Duration::from_secs(5), None)
            .await
            .expect("transaction should start");

        let query_state = Arc::clone(&state);
        let query_tx_id = tx_id.clone();
        let long_query = tokio::spawn(async move {
            query_state
                .execute_query_on(
                    &long_running_sql(5_000_000),
                    "long-running transaction query",
                    Some(&query_tx_id),
                )
                .await
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let second_tx_id = "independent-tx".to_string();
        tokio::time::timeout(
            Duration::from_millis(100),
            state.begin_transaction(second_tx_id.clone(), Duration::from_secs(5), None),
        )
        .await
        .expect("independent transaction start should not wait on another transaction query")
        .expect("second transaction should start successfully");

        state
            .rollback_transaction(&second_tx_id)
            .await
            .expect("second transaction rollback should succeed");

        let rows = long_query
            .await
            .expect("long query task should join")
            .expect("long query should succeed");
        assert_eq!(rows.len(), 1);

        state
            .rollback_transaction(&tx_id)
            .await
            .expect("long-query transaction rollback should succeed");

        drop(state);
        drop(temp_dir);
    }
}
