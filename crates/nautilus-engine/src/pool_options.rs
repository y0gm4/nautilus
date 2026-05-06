use std::time::Duration;

use nautilus_connector::ConnectorPoolOptions;

/// Engine-level connection-pool overrides exposed by the subprocess clients.
///
/// This keeps the engine CLI and generated non-Rust clients decoupled from the
/// connector crate while still mapping 1:1 to the underlying pool controls.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EnginePoolOptions {
    max_connections: Option<u32>,
    min_connections: Option<u32>,
    acquire_timeout: Option<Duration>,
    idle_timeout: Option<Option<Duration>>,
    test_before_acquire: Option<bool>,
    statement_cache_capacity: Option<usize>,
}

impl EnginePoolOptions {
    /// Create an empty set of engine pool overrides.
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

    /// Override the maximum time spent waiting for a pooled connection in milliseconds.
    pub fn acquire_timeout_ms(self, acquire_timeout_ms: u64) -> Self {
        self.acquire_timeout(Duration::from_millis(acquire_timeout_ms))
    }

    /// Override the maximum idle duration for pooled connections.
    ///
    /// Pass `None` to disable idle reaping entirely.
    pub fn idle_timeout(mut self, idle_timeout: impl Into<Option<Duration>>) -> Self {
        self.idle_timeout = Some(idle_timeout.into());
        self
    }

    /// Override the maximum idle duration for pooled connections in milliseconds.
    pub fn idle_timeout_ms(self, idle_timeout_ms: u64) -> Self {
        self.idle_timeout(Duration::from_millis(idle_timeout_ms))
    }

    /// Disable idle reaping for pooled connections.
    pub fn disable_idle_timeout(self) -> Self {
        self.idle_timeout(None::<Duration>)
    }

    /// Override whether pooled connections are pinged before acquisition.
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

    /// Convert engine-level overrides into connector-level pool options.
    pub fn to_connector_pool_options(self) -> ConnectorPoolOptions {
        let mut options = ConnectorPoolOptions::new();
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
        if let Some(statement_cache_capacity) = self.statement_cache_capacity {
            options = options.statement_cache_capacity(statement_cache_capacity);
        }
        options
    }

    /// Convert connector-level pool overrides into engine-level pool options.
    pub fn from_connector_pool_options(options: ConnectorPoolOptions) -> Self {
        Self {
            max_connections: options.get_max_connections(),
            min_connections: options.get_min_connections(),
            acquire_timeout: options.get_acquire_timeout(),
            idle_timeout: options.get_idle_timeout(),
            test_before_acquire: options.get_test_before_acquire(),
            statement_cache_capacity: options.get_statement_cache_capacity(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::EnginePoolOptions;
    use std::time::Duration;

    #[test]
    fn converts_to_connector_pool_options() {
        let engine = EnginePoolOptions::new()
            .max_connections(24)
            .min_connections(4)
            .acquire_timeout(Duration::from_secs(3))
            .disable_idle_timeout()
            .test_before_acquire(false)
            .statement_cache_capacity(12);

        let connector = engine.to_connector_pool_options();

        assert_eq!(connector.get_max_connections(), Some(24));
        assert_eq!(connector.get_min_connections(), Some(4));
        assert_eq!(
            connector.get_acquire_timeout(),
            Some(Duration::from_secs(3))
        );
        assert_eq!(connector.get_idle_timeout(), Some(None));
        assert_eq!(connector.get_test_before_acquire(), Some(false));
        assert_eq!(connector.get_statement_cache_capacity(), Some(12));
    }

    #[test]
    fn round_trips_connector_pool_options() {
        let connector = nautilus_connector::ConnectorPoolOptions::new()
            .max_connections(16)
            .idle_timeout(Duration::from_secs(30))
            .test_before_acquire(true)
            .statement_cache_capacity(4);

        let engine = EnginePoolOptions::from_connector_pool_options(connector);

        assert_eq!(engine.get_max_connections(), Some(16));
        assert_eq!(
            engine.get_idle_timeout(),
            Some(Some(Duration::from_secs(30)))
        );
        assert_eq!(engine.get_test_before_acquire(), Some(true));
        assert_eq!(engine.get_statement_cache_capacity(), Some(4));
    }
}
