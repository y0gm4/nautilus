//! Shared connection-pool and statement-cache overrides for connector executors.

use std::time::Duration;

use sqlx::{
    mysql::MySqlConnectOptions, pool::PoolOptions as SqlxPoolOptions, postgres::PgConnectOptions,
    sqlite::SqliteConnectOptions, Database,
};

/// Optional overrides for the sqlx connection pool and statement cache used by
/// Nautilus executors.
///
/// Any field left unset preserves the backend-specific defaults used by
/// [`crate::PgExecutor::new`], [`crate::MysqlExecutor::new`], or
/// [`crate::SqliteExecutor::new`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConnectorPoolOptions {
    max_connections: Option<u32>,
    min_connections: Option<u32>,
    acquire_timeout: Option<Duration>,
    idle_timeout: Option<Option<Duration>>,
    test_before_acquire: Option<bool>,
    statement_cache_capacity: Option<usize>,
}

impl ConnectorPoolOptions {
    /// Create an empty set of pool overrides.
    pub const fn new() -> Self {
        Self {
            max_connections: None,
            min_connections: None,
            acquire_timeout: None,
            idle_timeout: None,
            test_before_acquire: None,
            statement_cache_capacity: None,
        }
    }

    /// Override the maximum number of pooled connections.
    pub fn max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = Some(max_connections);
        self
    }

    /// Override the minimum number of pooled connections kept warm.
    pub fn min_connections(mut self, min_connections: u32) -> Self {
        self.min_connections = Some(min_connections);
        self
    }

    /// Override the maximum time spent waiting for a pooled connection.
    pub fn acquire_timeout(mut self, acquire_timeout: Duration) -> Self {
        self.acquire_timeout = Some(acquire_timeout);
        self
    }

    /// Override the maximum idle duration for pooled connections.
    ///
    /// Pass `None` to disable idle reaping entirely.
    pub fn idle_timeout(mut self, idle_timeout: impl Into<Option<Duration>>) -> Self {
        self.idle_timeout = Some(idle_timeout.into());
        self
    }

    /// Override whether sqlx pings a connection before returning it from the pool.
    pub fn test_before_acquire(mut self, test_before_acquire: bool) -> Self {
        self.test_before_acquire = Some(test_before_acquire);
        self
    }

    /// Override the per-connection statement cache capacity used by sqlx.
    ///
    /// Set this to `0` to disable statement caching entirely.
    pub fn statement_cache_capacity(mut self, statement_cache_capacity: usize) -> Self {
        self.statement_cache_capacity = Some(statement_cache_capacity);
        self
    }

    /// Return the configured maximum-connection override, if any.
    pub const fn get_max_connections(&self) -> Option<u32> {
        self.max_connections
    }

    /// Return the configured minimum-connection override, if any.
    pub const fn get_min_connections(&self) -> Option<u32> {
        self.min_connections
    }

    /// Return the configured acquire-timeout override, if any.
    pub const fn get_acquire_timeout(&self) -> Option<Duration> {
        self.acquire_timeout
    }

    /// Return the configured idle-timeout override, if any.
    ///
    /// `None` means "use the executor default". `Some(None)` means "disable
    /// idle timeout". `Some(Some(duration))` sets a custom timeout.
    pub const fn get_idle_timeout(&self) -> Option<Option<Duration>> {
        self.idle_timeout
    }

    /// Return the configured `test_before_acquire` override, if any.
    pub const fn get_test_before_acquire(&self) -> Option<bool> {
        self.test_before_acquire
    }

    /// Return the configured statement-cache-capacity override, if any.
    pub const fn get_statement_cache_capacity(&self) -> Option<usize> {
        self.statement_cache_capacity
    }

    pub(crate) fn apply_to<DB: Database>(
        &self,
        mut options: SqlxPoolOptions<DB>,
    ) -> SqlxPoolOptions<DB> {
        if let Some(max_connections) = self.max_connections {
            options = options.max_connections(max_connections);
        }
        if let Some(min_connections) = self.min_connections {
            options = options.min_connections(min_connections);
        }
        if let Some(acquire_timeout) = self.acquire_timeout {
            options = options.acquire_timeout(acquire_timeout);
        }
        if let Some(idle_timeout) = self.idle_timeout {
            options = options.idle_timeout(idle_timeout);
        }
        if let Some(test_before_acquire) = self.test_before_acquire {
            options = options.test_before_acquire(test_before_acquire);
        }
        options
    }

    pub(crate) fn apply_to_postgres_connect_options(
        &self,
        mut options: PgConnectOptions,
    ) -> PgConnectOptions {
        if let Some(statement_cache_capacity) = self.statement_cache_capacity {
            options = options.statement_cache_capacity(statement_cache_capacity);
        }
        options
    }

    pub(crate) fn apply_to_mysql_connect_options(
        &self,
        mut options: MySqlConnectOptions,
    ) -> MySqlConnectOptions {
        if let Some(statement_cache_capacity) = self.statement_cache_capacity {
            options = options.statement_cache_capacity(statement_cache_capacity);
        }
        options
    }

    pub(crate) fn apply_to_sqlite_connect_options(
        &self,
        mut options: SqliteConnectOptions,
    ) -> SqliteConnectOptions {
        if let Some(statement_cache_capacity) = self.statement_cache_capacity {
            options = options.statement_cache_capacity(statement_cache_capacity);
        }
        options
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sqlx::{
        mysql::MySqlConnectOptions,
        postgres::{PgConnectOptions, PgPoolOptions},
        sqlite::SqliteConnectOptions,
        ConnectOptions,
    };

    use super::ConnectorPoolOptions;

    #[test]
    fn apply_to_preserves_unspecified_backend_defaults() {
        let base = PgPoolOptions::new()
            .max_connections(10)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .idle_timeout(Duration::from_secs(300))
            .test_before_acquire(true);

        let applied = ConnectorPoolOptions::new()
            .max_connections(24)
            .apply_to(base);

        assert_eq!(applied.get_max_connections(), 24);
        assert_eq!(applied.get_min_connections(), 1);
        assert_eq!(applied.get_acquire_timeout(), Duration::from_secs(10));
        assert_eq!(applied.get_idle_timeout(), Some(Duration::from_secs(300)));
        assert!(applied.get_test_before_acquire());
    }

    #[test]
    fn apply_to_can_disable_idle_timeout() {
        let base = PgPoolOptions::new().idle_timeout(Duration::from_secs(300));

        let applied = ConnectorPoolOptions::new()
            .idle_timeout(None)
            .apply_to(base);

        assert_eq!(applied.get_idle_timeout(), None);
    }

    #[test]
    fn apply_to_postgres_connect_options_can_override_statement_cache_capacity() {
        let applied = ConnectorPoolOptions::new()
            .statement_cache_capacity(7)
            .apply_to_postgres_connect_options(
                "postgres://localhost/nautilus"
                    .parse::<PgConnectOptions>()
                    .expect("postgres url should parse"),
            );

        let query = applied.to_url_lossy();
        assert_eq!(
            query
                .query_pairs()
                .find(|(key, _)| key == "statement-cache-capacity")
                .map(|(_, value)| value.into_owned())
                .as_deref(),
            Some("7")
        );
    }

    #[test]
    fn apply_to_mysql_connect_options_can_override_statement_cache_capacity() {
        let applied = ConnectorPoolOptions::new()
            .statement_cache_capacity(9)
            .apply_to_mysql_connect_options(
                "mysql://root:password@localhost/nautilus"
                    .parse::<MySqlConnectOptions>()
                    .expect("mysql url should parse"),
            );

        let query = applied.to_url_lossy();
        assert_eq!(
            query
                .query_pairs()
                .find(|(key, _)| key == "statement-cache-capacity")
                .map(|(_, value)| value.into_owned())
                .as_deref(),
            Some("9")
        );
    }

    #[test]
    fn apply_to_sqlite_connect_options_can_override_statement_cache_capacity() {
        let applied = ConnectorPoolOptions::new()
            .statement_cache_capacity(0)
            .apply_to_sqlite_connect_options(
                "sqlite://nautilus.db"
                    .parse::<SqliteConnectOptions>()
                    .expect("sqlite url should parse"),
            );

        assert!(
            format!("{applied:?}").contains("statement_cache_capacity: 0"),
            "sqlite connect options should reflect the overridden statement cache capacity: {applied:?}"
        );
    }
}
