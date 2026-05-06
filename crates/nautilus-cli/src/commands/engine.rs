use clap::Subcommand;
use nautilus_engine::EnginePoolOptions;
use std::time::Duration;

/// Engine runtime subcommands
#[derive(Subcommand)]
pub enum EngineCommand {
    /// Start the JSON-RPC engine server on stdin/stdout (used by client libraries)
    Serve {
        /// Path to the schema file (auto-detect the first .nautilus file if not specified)
        #[arg(short, long)]
        schema: Option<String>,

        /// Database URL (overrides schema datasource direct_url/url and DATABASE_URL)
        #[arg(long)]
        database_url: Option<String>,

        /// Run DDL migrations before entering the request loop
        #[arg(long)]
        migrate: bool,

        /// Override the maximum number of pooled connections used by the engine.
        #[arg(long)]
        max_connections: Option<u32>,

        /// Override the minimum number of pooled connections kept warm by the engine.
        #[arg(long)]
        min_connections: Option<u32>,

        /// Override the connection-acquire timeout, in milliseconds.
        #[arg(long)]
        acquire_timeout_ms: Option<u64>,

        /// Override the idle-timeout reap threshold, in milliseconds.
        #[arg(long, conflicts_with = "disable_idle_timeout")]
        idle_timeout_ms: Option<u64>,

        /// Disable idle-timeout reaping for engine-managed connection pools.
        #[arg(long, conflicts_with = "idle_timeout_ms")]
        disable_idle_timeout: bool,

        /// Override whether pooled connections are pinged before acquisition.
        #[arg(long)]
        test_before_acquire: Option<bool>,

        /// Override the per-connection sqlx statement cache capacity.
        #[arg(long)]
        statement_cache_capacity: Option<usize>,
    },
}

pub async fn run(cmd: EngineCommand) -> anyhow::Result<()> {
    match cmd {
        EngineCommand::Serve {
            schema,
            database_url,
            migrate,
            max_connections,
            min_connections,
            acquire_timeout_ms,
            idle_timeout_ms,
            disable_idle_timeout,
            test_before_acquire,
            statement_cache_capacity,
        } => {
            let mut pool_options = EnginePoolOptions::new();
            if let Some(max_connections) = max_connections {
                pool_options = pool_options.max_connections(max_connections);
            }
            if let Some(min_connections) = min_connections {
                pool_options = pool_options.min_connections(min_connections);
            }
            if let Some(acquire_timeout_ms) = acquire_timeout_ms {
                pool_options =
                    pool_options.acquire_timeout(Duration::from_millis(acquire_timeout_ms));
            }
            if disable_idle_timeout {
                pool_options = pool_options.disable_idle_timeout();
            } else if let Some(idle_timeout_ms) = idle_timeout_ms {
                pool_options = pool_options.idle_timeout(Duration::from_millis(idle_timeout_ms));
            }
            if let Some(test_before_acquire) = test_before_acquire {
                pool_options = pool_options.test_before_acquire(test_before_acquire);
            }
            if let Some(statement_cache_capacity) = statement_cache_capacity {
                pool_options = pool_options.statement_cache_capacity(statement_cache_capacity);
            }

            nautilus_engine::run_engine_with_schema_resolution(
                schema,
                database_url,
                migrate,
                pool_options,
            )
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
        }
    }
}
