use clap::{Subcommand, ValueEnum};
use nautilus_migrate::{PullNameCase, PullNamingOptions};

pub mod connection;
mod drop;
mod pull;
mod push;
mod reset;
mod seed;
mod status;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum PullNameCaseArg {
    /// Preserve the current `db pull` naming behaviour.
    #[default]
    Auto,
    /// Render logical names in `snake_case`.
    Snake,
    /// Render logical names in `PascalCase`.
    Pascal,
}

impl From<PullNameCaseArg> for PullNameCase {
    fn from(value: PullNameCaseArg) -> Self {
        match value {
            PullNameCaseArg::Auto => PullNameCase::Auto,
            PullNameCaseArg::Snake => PullNameCase::Snake,
            PullNameCaseArg::Pascal => PullNameCase::Pascal,
        }
    }
}

fn pull_naming_options(
    model_case: PullNameCaseArg,
    field_case: PullNameCaseArg,
) -> PullNamingOptions {
    PullNamingOptions {
        model_case: model_case.into(),
        field_case: field_case.into(),
    }
}

#[derive(Subcommand)]
pub enum DbCommand {
    /// Push the current schema state to the database, creating or updating tables
    Push {
        /// Path to the schema file (auto-detect the first .nautilus file if not specified)
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
        /// Path to the schema file (auto-detect the first .nautilus file if not specified)
        #[arg(short, long)]
        schema: Option<String>,

        /// Database URL (overrides schema datasource direct_url/url and DATABASE_URL)
        #[arg(long)]
        database_url: Option<String>,
    },
    /// Introspect live database and write an equivalent .nautilus schema file
    Pull {
        /// Path to the schema file (used to read the datasource URL if not provided explicitly; auto-detects the first .nautilus file if omitted)
        #[arg(short, long)]
        schema: Option<String>,

        /// Database URL (overrides schema datasource direct_url/url and DATABASE_URL)
        #[arg(long)]
        database_url: Option<String>,

        /// Output file path (default: pulled.nautilus)
        #[arg(short, long)]
        output: Option<String>,

        /// Logical naming mode for generated model names.
        #[arg(long, value_enum, default_value_t = PullNameCaseArg::Auto)]
        model_case: PullNameCaseArg,

        /// Logical naming mode for generated field names.
        #[arg(long, value_enum, default_value_t = PullNameCaseArg::Auto)]
        field_case: PullNameCaseArg,
    },
    /// Drop all tables permanently without recreating them
    Drop {
        /// Path to the schema file (auto-detect the first .nautilus file if not specified)
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
        /// Path to the schema file (auto-detect the first .nautilus file if not specified)
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
            model_case,
            field_case,
        } => {
            pull::run(
                schema,
                database_url,
                output,
                pull_naming_options(model_case, field_case),
            )
            .await
        }
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
