use clap::Subcommand;

/// Engine runtime subcommands
#[derive(Subcommand)]
pub enum EngineCommand {
    /// Start the JSON-RPC engine server on stdin/stdout (used by client libraries)
    Serve {
        /// Path to the schema file
        #[arg(short, long)]
        schema: String,

        /// Database URL (overrides schema datasource direct_url/url and DATABASE_URL)
        #[arg(long)]
        database_url: Option<String>,

        /// Run DDL migrations before entering the request loop
        #[arg(long)]
        migrate: bool,
    },
}

pub async fn run(cmd: EngineCommand) -> anyhow::Result<()> {
    match cmd {
        EngineCommand::Serve {
            schema,
            database_url,
            migrate,
        } => nautilus_engine::run_engine(schema, database_url, migrate)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e)),
    }
}
