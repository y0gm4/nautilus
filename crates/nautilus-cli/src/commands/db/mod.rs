use clap::Subcommand;

pub mod connection;
mod drop;
mod pull;
mod push;
mod reset;
mod seed;
mod status;

#[derive(Subcommand)]
pub enum DbCommand {
    /// Push the current schema state to the database, creating or updating tables
    Push {
        /// Path to the schema file (default: ./schema.nautilus)
        #[arg(short, long)]
        schema: Option<String>,

        /// Database URL (overrides schema datasource direct_url/url and DATABASE_URL)
        #[arg(long)]
        database_url: Option<String>,

        /// Skip interactive confirmation for destructive changes (CI/non-interactive mode)
        #[arg(long)]
        accept_data_loss: bool,

        /// Apply schema changes without triggering client generation afterwards
        #[arg(long)]
        no_generate: bool,
    },
    /// Show pending schema changes without applying them (dry-run diff)
    Status {
        /// Path to the schema file (default: ./schema.nautilus)
        #[arg(short, long)]
        schema: Option<String>,

        /// Database URL (overrides schema datasource direct_url/url and DATABASE_URL)
        #[arg(long)]
        database_url: Option<String>,
    },
    /// Introspect live database and write an equivalent .nautilus schema file
    Pull {
        /// Path to the schema file (used to read the datasource URL if not provided explicitly)
        #[arg(short, long)]
        schema: Option<String>,

        /// Database URL (overrides schema datasource direct_url/url and DATABASE_URL)
        #[arg(long)]
        database_url: Option<String>,

        /// Output file path (default: pulled.nautilus)
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Drop all tables permanently without recreating them
    Drop {
        /// Path to the schema file (default: ./schema.nautilus)
        #[arg(short, long)]
        schema: Option<String>,

        /// Database URL (overrides schema datasource direct_url/url and DATABASE_URL)
        #[arg(long)]
        database_url: Option<String>,

        /// Skip interactive confirmation (ALL DATA WILL BE LOST)
        #[arg(long)]
        force: bool,
    },
    /// Drop all tables then re-push the schema from scratch
    Reset {
        /// Path to the schema file (default: ./schema.nautilus)
        #[arg(short, long)]
        schema: Option<String>,

        /// Database URL (overrides schema datasource direct_url/url and DATABASE_URL)
        #[arg(long)]
        database_url: Option<String>,

        /// Skip interactive confirmation (ALL DATA WILL BE LOST)
        #[arg(long)]
        force: bool,

        /// Delete all rows but keep the table structure (TRUNCATE instead of DROP + recreate)
        #[arg(long)]
        only_data: bool,
    },
    /// Run a SQL seed script against the database
    Seed {
        /// Path to the SQL seed file
        file: String,

        /// Database URL (overrides DATABASE_URL)
        #[arg(long)]
        database_url: Option<String>,
    },
}

pub async fn run(cmd: DbCommand) -> anyhow::Result<()> {
    match cmd {
        DbCommand::Push {
            schema,
            database_url,
            accept_data_loss,
            no_generate,
        } => push::run(schema, database_url, accept_data_loss, no_generate).await,
        DbCommand::Status {
            schema,
            database_url,
        } => status::run(schema, database_url).await,
        DbCommand::Pull {
            schema,
            database_url,
            output,
        } => pull::run(schema, database_url, output).await,
        DbCommand::Drop {
            schema,
            database_url,
            force,
        } => drop::run(schema, database_url, force).await,
        DbCommand::Reset {
            schema,
            database_url,
            force,
            only_data,
        } => reset::run(schema, database_url, force, only_data).await,
        DbCommand::Seed { file, database_url } => seed::run(file, database_url).await,
    }
}
