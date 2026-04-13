#![forbid(unsafe_code)]

use clap::{Parser, Subcommand};

mod commands;
#[cfg(test)]
mod test_support;
mod tui;

#[derive(Parser)]
#[command(
    name = "nautilus",
    about = "Nautilus ORM CLI",
    version,
    propagate_version = true
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Database management commands (push, status, ...)
    Db {
        #[command(subcommand)]
        subcommand: commands::db::DbCommand,
    },
    /// Generate client code from a schema file
    Generate {
        /// Path to the schema file (auto-detect the first .nautilus file if not specified)
        #[arg(long)]
        schema: Option<String>,
        /// Skip automatic package installation after generation
        #[arg(long)]
        no_install: bool,
        /// Verbose output
        #[arg(short, long)]
        verbose: bool,
        /// (Rust only) Also generate a Cargo.toml for the output crate.
        /// Default mode assumes integration into an existing Cargo workspace.
        #[arg(long)]
        standalone: bool,
    },
    /// Validate a schema file without generating code
    Validate {
        /// Path to the schema file (auto-detect the first .nautilus file if not specified)
        #[arg(long)]
        schema: Option<String>,
    },
    /// Engine runtime - used internally by client libraries
    Engine {
        #[command(subcommand)]
        subcommand: commands::engine::EngineCommand,
    },
    /// Format (canonically indent) a .nautilus schema file in-place
    Format {
        /// Path to the schema file (auto-detect the first .nautilus file if not specified)
        #[arg(long)]
        schema: Option<String>,
    },
    /// Manage versioned SQL migration files (generate, apply, rollback, status)
    Migrate(commands::migrate::MigrateArgs),
    /// Python integration - install or remove the site-packages .pth shim
    Python {
        #[command(subcommand)]
        subcommand: commands::python::PythonCommand,
    },
    /// Manage the Nautilus Studio Next.js app checkout/build and launch it
    Studio(commands::studio::StudioArgs),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Command::Db { subcommand } => commands::db::run(subcommand).await,
        Command::Generate {
            schema,
            no_install,
            verbose,
            standalone,
        } => tokio::task::spawn_blocking(move || {
            commands::generate::run_generate(schema, no_install, verbose, standalone)
        })
        .await
        .unwrap_or_else(|e| Err(anyhow::anyhow!("Task error: {}", e))),
        Command::Validate { schema } => {
            tokio::task::spawn_blocking(move || commands::generate::run_validate(schema))
                .await
                .unwrap_or_else(|e| Err(anyhow::anyhow!("Task error: {}", e)))
        }
        Command::Engine { subcommand } => commands::engine::run(subcommand).await,
        Command::Format { schema } => commands::format::run(schema).await,
        Command::Migrate(args) => commands::migrate::run(args).await,
        Command::Python { subcommand } => commands::python::run(subcommand).await,
        Command::Studio(args) => commands::studio::run(args).await,
    };

    if let Err(e) = result {
        tui::print_fatal_error(&tui::format_error_chain(&e));
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command};
    use crate::commands::db::DbCommand;
    use clap::{CommandFactory, Parser};

    fn top_level_help() -> String {
        Cli::command().render_long_help().to_string()
    }

    fn help_for(subcommand: &str) -> String {
        let mut cmd = Cli::command();
        cmd.find_subcommand_mut(subcommand)
            .expect("subcommand should exist")
            .render_long_help()
            .to_string()
    }

    fn crate_readme() -> String {
        std::fs::read_to_string(format!("{}/README.md", env!("CARGO_MANIFEST_DIR")))
            .expect("failed to read CLI README")
    }

    fn workspace_readme() -> String {
        std::fs::read_to_string(format!("{}/../../README.md", env!("CARGO_MANIFEST_DIR")))
            .expect("failed to read workspace README")
    }

    fn help_for_nested(parent: &str, child: &str) -> String {
        let mut cmd = Cli::command();
        cmd.find_subcommand_mut(parent)
            .expect("parent subcommand should exist")
            .find_subcommand_mut(child)
            .expect("child subcommand should exist")
            .render_long_help()
            .to_string()
    }

    #[test]
    fn generate_help_mentions_schema_flag_not_positional_argument() {
        let help = help_for("generate");
        assert!(help.contains("--schema <SCHEMA>"));
        assert!(help.contains("auto-detect the first .nautilus file if not specified"));
        assert!(!help.contains("Usage: nautilus.exe generate <SCHEMA>"));
    }

    #[test]
    fn format_help_mentions_schema_auto_detection() {
        let help = help_for("format");
        assert!(help.contains("--schema <SCHEMA>"));
        assert!(help.contains("auto-detect the first .nautilus file if not specified"));
    }

    #[test]
    fn db_help_mentions_no_generate_on_push() {
        let help = help_for_nested("db", "push");
        assert!(help.contains("auto-detect the first .nautilus file if not specified"));
        assert!(help.contains("--no-generate"));
    }

    #[test]
    fn db_pull_help_mentions_case_flags() {
        let help = help_for_nested("db", "pull");
        assert!(help.contains("auto-detects the first .nautilus file if omitted"));
        assert!(help.contains("--model-case <MODEL_CASE>"));
        assert!(help.contains("--field-case <FIELD_CASE>"));
        assert!(help.contains("auto"));
        assert!(help.contains("snake"));
        assert!(help.contains("pascal"));
    }

    #[test]
    fn migrate_generate_help_mentions_schema_auto_detection() {
        let help = help_for_nested("migrate", "generate");
        assert!(help.contains("auto-detect the first .nautilus file if not specified"));
    }

    #[test]
    fn engine_serve_help_mentions_schema_auto_detection() {
        let help = help_for_nested("engine", "serve");
        assert!(help.contains("auto-detect the first .nautilus file if not specified"));
    }

    #[test]
    fn top_level_help_mentions_engine_python_and_studio_commands() {
        let help = top_level_help();
        assert!(help.contains("engine"));
        assert!(help.contains("python"));
        assert!(help.contains("studio"));
    }

    #[test]
    fn generate_rejects_positional_schema_path() {
        let err = match Cli::try_parse_from(["nautilus", "generate", "schema.nautilus"]) {
            Ok(_) => panic!("positional schema path should be rejected"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("unexpected argument 'schema.nautilus'"));
    }

    #[test]
    fn generate_accepts_schema_flag() {
        let cli = Cli::try_parse_from(["nautilus", "generate", "--schema", "schema.nautilus"])
            .expect("schema flag should parse");

        match cli.command {
            Command::Generate { schema, .. } => {
                assert_eq!(schema.as_deref(), Some("schema.nautilus"));
            }
            other => panic!(
                "expected generate command, got {:?}",
                std::mem::discriminant(&other)
            ),
        }
    }

    #[test]
    fn db_push_accepts_no_generate_flag() {
        let cli = Cli::try_parse_from([
            "nautilus",
            "db",
            "push",
            "--schema",
            "schema.nautilus",
            "--no-generate",
        ])
        .expect("db push --no-generate should parse");

        match cli.command {
            Command::Db { subcommand } => match subcommand {
                DbCommand::Push {
                    schema,
                    no_generate,
                    ..
                } => {
                    assert_eq!(schema.as_deref(), Some("schema.nautilus"));
                    assert!(no_generate);
                }
                other => panic!(
                    "expected db push command, got {:?}",
                    std::mem::discriminant(&other)
                ),
            },
            other => panic!(
                "expected db command, got {:?}",
                std::mem::discriminant(&other)
            ),
        }
    }

    #[test]
    fn db_pull_accepts_name_case_flags() {
        let cli = Cli::try_parse_from([
            "nautilus",
            "db",
            "pull",
            "--model-case",
            "pascal",
            "--field-case",
            "snake",
        ])
        .expect("db pull naming flags should parse");

        match cli.command {
            Command::Db { subcommand } => match subcommand {
                DbCommand::Pull {
                    model_case,
                    field_case,
                    ..
                } => {
                    assert_eq!(format!("{model_case:?}"), "Pascal");
                    assert_eq!(format!("{field_case:?}"), "Snake");
                }
                other => panic!(
                    "expected db pull command, got {:?}",
                    std::mem::discriminant(&other)
                ),
            },
            other => panic!(
                "expected db command, got {:?}",
                std::mem::discriminant(&other)
            ),
        }
    }

    #[test]
    fn studio_help_mentions_supported_flags() {
        let help = help_for("studio");
        assert!(help.contains("--update"));
        assert!(help.contains("--uninstall"));
    }

    #[test]
    fn studio_accepts_update_flag() {
        let cli = Cli::try_parse_from(["nautilus", "studio", "--update"])
            .expect("studio flags should parse");

        match cli.command {
            Command::Studio(args) => {
                assert!(args.update);
                assert!(!args.uninstall);
            }
            other => panic!(
                "expected studio command, got {:?}",
                std::mem::discriminant(&other)
            ),
        }
    }

    #[test]
    fn cli_readme_tracks_local_generation_and_python_shim_behavior() {
        let readme = crate_readme();
        assert!(readme.contains("`nautilus-client-py`"));
        assert!(readme.contains("`nautilus-client-js`"));
        assert!(
            readme.contains("The normal workflow is to import the generated `output` directory")
        );
        assert!(readme.contains("it does not install or publish generated ORM clients"));
    }

    #[test]
    fn workspace_readme_tracks_current_public_cli_surface() {
        let readme = workspace_readme();
        assert!(readme.contains("| `engine serve` |"));
        assert!(readme.contains("| `python install`, `python uninstall` |"));
        assert!(readme.contains("| `studio` |"));
        assert!(readme
            .contains("| `db push`, `db status`, `db pull`, `db drop`, `db reset`, `db seed` |"));
        assert!(
            readme.contains("Generated clients are local build artifacts, not registry packages.")
        );
    }
}
