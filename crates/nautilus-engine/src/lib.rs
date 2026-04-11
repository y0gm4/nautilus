//! Nautilus Engine — library entry point.
//!
//! The binary (`nautilus-engine` / `nautilus engine serve`) is a thin shell over this crate.

#![forbid(unsafe_code)]

pub mod args;
pub mod conversion;
pub mod filter;
pub mod handlers;
pub mod state;
pub mod transport;

use nautilus_migrate::{DatabaseProvider, DdlGenerator};
use nautilus_schema::{ir::SchemaIr, validate_schema_source};

pub use args::CliArgs;
pub use state::EngineState;

/// Run the engine with explicit parameters.
///
/// Parses the schema, connects to the database, optionally runs migrations,
/// then serves JSON-RPC requests on stdin/stdout until EOF.
pub async fn run_engine(
    schema_path: String,
    database_url: Option<String>,
    migrate: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema_source = std::fs::read_to_string(&schema_path)?;
    let schema_ir = validate_schema_source(&schema_source)?.ir;

    if migrate {
        eprintln!("[engine] Running schema migrations (--migrate)...");

        let datasource = schema_ir
            .datasource
            .as_ref()
            .ok_or("No datasource found in schema")?;

        let db_provider =
            DatabaseProvider::from_schema_provider(&datasource.provider).ok_or_else(|| {
                format!(
                    "Unsupported provider for migration: {}",
                    datasource.provider
                )
            })?;

        let migration_url = resolve_engine_migration_url(database_url.as_deref(), &schema_ir)?;
        let generator = DdlGenerator::new(db_provider);
        let statements = generator.generate_create_tables(&schema_ir)?;
        let migration_state = EngineState::new(schema_ir.clone(), migration_url, None).await?;
        migration_state.execute_ddl_sql(statements).await?;

        eprintln!("[engine] Migrations applied successfully");
    }

    let runtime_url = resolve_engine_runtime_url(database_url.as_deref(), &schema_ir)?;

    // When no explicit --database-url override is given, pass the schema's direct_url
    // so raw SQL queries can bypass poolers (e.g. PgBouncer) that reject prepared statements.
    // If the env variable is not set we warn and continue without a direct pool rather than
    // failing engine startup.
    let direct_url = if database_url.is_none() {
        schema_ir
            .datasource
            .as_ref()
            .and_then(|ds| ds.direct_url.as_deref())
            .and_then(|raw| match resolve_datasource_url(raw) {
                Ok(url) => Some(url),
                Err(e) => {
                    eprintln!(
                        "[engine] Warning: direct_url could not be resolved ({}), \
                         raw queries will use the pooled connection",
                        e
                    );
                    None
                }
            })
    } else {
        None
    };

    let state = EngineState::new(schema_ir.clone(), runtime_url, direct_url).await?;

    eprintln!("[engine] Engine initialized, entering request loop");

    transport::run_request_loop(state).await?;

    eprintln!("[engine] Shutting down gracefully");
    Ok(())
}

/// Convenience entry point for the standalone binary: parses argv then calls [`run_engine`].
pub async fn run_engine_from_cli() -> Result<(), Box<dyn std::error::Error>> {
    let args = CliArgs::parse()?;
    run_engine(args.schema_path, args.database_url, args.migrate).await
}

fn resolve_engine_runtime_url(
    database_url_arg: Option<&str>,
    schema_ir: &SchemaIr,
) -> Result<String, Box<dyn std::error::Error>> {
    let raw_url = database_url_arg
        .map(str::to_string)
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .or_else(|| {
            schema_ir
                .datasource
                .as_ref()
                .map(|ds| ds.runtime_url().to_string())
        })
        .ok_or(
            "No runtime database URL provided. Use --database-url, set DATABASE_URL, or set datasource url/direct_url.",
        )?;

    resolve_datasource_url(&raw_url)
}

fn resolve_engine_migration_url(
    database_url_arg: Option<&str>,
    schema_ir: &SchemaIr,
) -> Result<String, Box<dyn std::error::Error>> {
    let raw_url = database_url_arg
        .map(str::to_string)
        .or_else(|| {
            schema_ir
                .datasource
                .as_ref()
                .map(|ds| ds.admin_url().to_string())
        })
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .ok_or(
            "No migration database URL provided. Use --database-url, set datasource direct_url/url, or set DATABASE_URL.",
        )?;

    resolve_datasource_url(&raw_url)
}

fn resolve_datasource_url(raw: &str) -> Result<String, Box<dyn std::error::Error>> {
    nautilus_schema::resolve_env_url(raw).map_err(|msg| msg.into())
}

#[cfg(test)]
mod tests {
    use super::{resolve_datasource_url, resolve_engine_migration_url, resolve_engine_runtime_url};
    use nautilus_schema::validate_schema_source;

    struct EnvVarGuard {
        key: &'static str,
        old: Option<String>,
    }

    impl EnvVarGuard {
        fn unset(key: &'static str) -> Self {
            let old = std::env::var(key).ok();
            std::env::remove_var(key);
            Self { key, old }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.old {
                Some(value) => {
                    std::env::set_var(self.key, value);
                }
                None => {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    fn parse_schema_ir(source: &str) -> nautilus_schema::ir::SchemaIr {
        validate_schema_source(source)
            .expect("schema should validate")
            .ir
    }

    #[test]
    fn runtime_url_prefers_datasource_url() {
        let _env_guard = EnvVarGuard::unset("DATABASE_URL");
        let schema_ir = parse_schema_ir(
            r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://pooled/runtime"
  direct_url = "postgres://direct/admin"
}

model User {
  id Int @id
}
"#,
        );

        let url = resolve_engine_runtime_url(None, &schema_ir).expect("expected runtime url");
        assert_eq!(url, "postgres://pooled/runtime");
    }

    #[test]
    fn migration_url_prefers_direct_url() {
        let _env_guard = EnvVarGuard::unset("DATABASE_URL");
        let schema_ir = parse_schema_ir(
            r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://pooled/runtime"
  direct_url = "postgres://direct/admin"
}

model User {
  id Int @id
}
"#,
        );

        let url = resolve_engine_migration_url(None, &schema_ir).expect("expected migration url");
        assert_eq!(url, "postgres://direct/admin");
    }

    #[test]
    fn direct_url_plain_string_resolves() {
        let schema_ir = parse_schema_ir(
            r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://pooled/runtime"
  direct_url = "postgres://direct/admin"
}

model User {
  id Int @id
}
"#,
        );

        let direct = schema_ir
            .datasource
            .as_ref()
            .and_then(|ds| ds.direct_url.as_deref())
            .and_then(|raw| resolve_datasource_url(raw).ok());

        assert_eq!(direct.as_deref(), Some("postgres://direct/admin"));
    }

    #[test]
    fn direct_url_missing_env_var_yields_none_not_error() {
        let schema_ir = parse_schema_ir(
            r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://pooled/runtime"
  direct_url = env("__NAUTILUS_TEST_UNSET_DIRECT_URL__")
}

model User {
  id Int @id
}
"#,
        );

        // Env var is deliberately unset; resolution should fail so the engine
        // falls back to None (no direct pool) rather than aborting startup.
        let direct = schema_ir
            .datasource
            .as_ref()
            .and_then(|ds| ds.direct_url.as_deref())
            .and_then(|raw| resolve_datasource_url(raw).ok());

        assert_eq!(
            direct, None,
            "unresolvable env() should produce None, not an error"
        );
    }
}
