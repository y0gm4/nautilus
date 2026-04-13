use clap::Subcommand;

pub mod apply;
pub mod generate;
pub mod rollback;
mod shared;
pub mod status;

/// Manage versioned SQL migration files.
#[derive(clap::Args)]
pub struct MigrateArgs {
    #[command(subcommand)]
    pub command: MigrateCommand,
}

#[derive(Subcommand)]
pub enum MigrateCommand {
    /// Create a new migration file from the current schema diff.
    Generate {
        /// Human-readable label for this migration (e.g. "add_users").
        label: Option<String>,

        /// Path to the schema file (auto-detect the first .nautilus file if not specified).
        #[arg(short, long)]
        schema: Option<String>,

        /// Database URL (overrides schema datasource direct_url/url and DATABASE_URL).
        #[arg(long)]
        database_url: Option<String>,

        /// Directory to write migration files into (default: migrations/ next to schema file).
        #[arg(long)]
        migrations_dir: Option<String>,
    },

    /// Apply all pending migration files in chronological order.
    Apply {
        /// Path to the schema file (auto-detect the first .nautilus file if not specified).
        #[arg(short, long)]
        schema: Option<String>,

        /// Database URL (overrides schema datasource direct_url/url and DATABASE_URL).
        #[arg(long)]
        database_url: Option<String>,

        /// Directory containing migration files (default: migrations/ next to schema file).
        #[arg(long)]
        migrations_dir: Option<String>,
    },

    /// Roll back the last N applied migrations (default: 1).
    Rollback {
        /// Number of migrations to roll back.
        #[arg(long, default_value = "1")]
        steps: usize,

        /// Path to the schema file (auto-detect the first .nautilus file if not specified).
        #[arg(short, long)]
        schema: Option<String>,

        /// Database URL (overrides schema datasource direct_url/url and DATABASE_URL).
        #[arg(long)]
        database_url: Option<String>,

        /// Directory containing migration files (default: migrations/ next to schema file).
        #[arg(long)]
        migrations_dir: Option<String>,
    },

    /// Show the pending / applied status of every migration file.
    Status {
        /// Path to the schema file (auto-detect the first .nautilus file if not specified).
        #[arg(short, long)]
        schema: Option<String>,

        /// Database URL (overrides schema datasource direct_url/url and DATABASE_URL).
        #[arg(long)]
        database_url: Option<String>,

        /// Directory containing migration files (default: migrations/ next to schema file).
        #[arg(long)]
        migrations_dir: Option<String>,
    },
}

pub async fn run(args: MigrateArgs) -> anyhow::Result<()> {
    match args.command {
        MigrateCommand::Generate {
            label,
            schema,
            database_url,
            migrations_dir,
        } => generate::run(label, schema, database_url, migrations_dir).await,
        MigrateCommand::Apply {
            schema,
            database_url,
            migrations_dir,
        } => apply::run(schema, database_url, migrations_dir).await,
        MigrateCommand::Rollback {
            steps,
            schema,
            database_url,
            migrations_dir,
        } => rollback::run(steps, schema, database_url, migrations_dir).await,
        MigrateCommand::Status {
            schema,
            database_url,
            migrations_dir,
        } => status::run(schema, database_url, migrations_dir).await,
    }
}
