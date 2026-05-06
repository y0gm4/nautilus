//! Nautilus Engine — library entry point.
//!
//! The binary (`nautilus-engine` / `nautilus engine serve`) is a thin shell over this crate.

#![forbid(unsafe_code)]

pub mod args;
pub mod conversion;
pub mod filter;
pub mod handlers;
mod metadata;
mod plan_cache;
pub mod pool_options;
pub mod state;
pub mod transport;

use nautilus_migrate::{DatabaseProvider, DdlGenerator};
use nautilus_schema::{ir::SchemaIr, validate_schema_source};

pub use args::CliArgs;
pub use pool_options::EnginePoolOptions;
pub use state::EngineState;

fn eprint_warning(message: &str) {
    eprintln!(
        "{} {}",
        console::style("[engine] warning:").yellow().bold(),
        console::style(message).yellow()
    );
}

/// Resolve the schema path, auto-detecting the first `.nautilus` file in the
/// current working directory when `--schema` is omitted.
pub fn resolve_schema_path_arg(
    schema_path: Option<String>,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(path) = schema_path {
        return Ok(path);
    }

    let nautilus_files = nautilus_schema::discover_schema_paths_in_current_dir()?;
    let schema_path = nautilus_files.first().cloned().ok_or(
        "No .nautilus schema file found in current directory.\n\n\
         Hint: Pass --schema <path> or create a .nautilus file in the current directory.",
    )?;

    if nautilus_files.len() > 1 {
        eprint_warning(&format!(
            "multiple .nautilus files found, using: {}",
            schema_path.display()
        ));
    }

    Ok(schema_path.to_string_lossy().into_owned())
}

/// Resolve the schema path (with auto-detection when omitted) and then run the engine.
pub async fn run_engine_with_schema_resolution(
    schema_path: Option<String>,
    database_url: Option<String>,
    migrate: bool,
    pool_options: EnginePoolOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema_path = resolve_schema_path_arg(schema_path)?;
    run_engine(schema_path, database_url, migrate, pool_options).await
}

/// Run the engine with explicit parameters.
///
/// Parses the schema, connects to the database, optionally runs migrations,
/// then serves JSON-RPC requests on stdin/stdout until EOF.
pub async fn run_engine(
    schema_path: String,
    database_url: Option<String>,
    migrate: bool,
    pool_options: EnginePoolOptions,
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
        let migration_state = EngineState::new_with_engine_pool_options(
            schema_ir.clone(),
            migration_url,
            None,
            pool_options,
        )
        .await?;
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
                    eprint_warning(&format!(
                        "direct_url could not be resolved ({}), raw queries will use the pooled connection",
                        e
                    ));
                    None
                }
            })
    } else {
        None
    };

    let state = EngineState::new_with_engine_pool_options(
        schema_ir.clone(),
        runtime_url,
        direct_url,
        pool_options,
    )
    .await?;

    eprintln!("[engine] Engine initialized, entering request loop");

    transport::run_request_loop(state).await?;

    eprintln!("[engine] Shutting down gracefully");
    Ok(())
}

/// Convenience entry point for the standalone binary: parses argv then calls [`run_engine`].
pub async fn run_engine_from_cli() -> Result<(), Box<dyn std::error::Error>> {
    let args = CliArgs::parse()?;
    run_engine_with_schema_resolution(
        args.schema_path,
        args.database_url,
        args.migrate,
        args.pool_options,
    )
    .await
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
    if let Some(raw) = database_url_arg {
        return resolve_datasource_url(raw);
    }

    let datasource = schema_ir.datasource.as_ref();

    if let Some(raw) = datasource.and_then(|ds| ds.direct_url.as_deref()) {
        if let Ok(url) = resolve_datasource_url(raw) {
            return Ok(url);
        }
    }

    if let Ok(raw) = std::env::var("DATABASE_URL") {
        return resolve_datasource_url(&raw);
    }

    if let Some(raw) = datasource
        .map(|ds| ds.url.as_str())
        .filter(|url| !url.is_empty())
    {
        return resolve_datasource_url(raw);
    }

    Err(
        "No migration database URL provided. Use --database-url, set datasource direct_url/url, or set DATABASE_URL."
            .into(),
    )
}

fn resolve_datasource_url(raw: &str) -> Result<String, Box<dyn std::error::Error>> {
    nautilus_schema::resolve_env_url(raw).map_err(|msg| msg.into())
}

#[cfg(test)]
mod tests {
    use super::{
        resolve_datasource_url, resolve_engine_migration_url, resolve_engine_runtime_url,
        resolve_schema_path_arg,
    };
    use nautilus_schema::validate_schema_source;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock};
    use tempfile::TempDir;

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

    fn working_dir_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct CurrentDirGuard {
        original: PathBuf,
    }

    impl CurrentDirGuard {
        fn set(path: &Path) -> Self {
            let original = std::env::current_dir().expect("current dir should exist");
            std::env::set_current_dir(path).expect("failed to switch current dir");
            Self { original }
        }
    }

    impl Drop for CurrentDirGuard {
        fn drop(&mut self) {
            std::env::set_current_dir(&self.original).expect("failed to restore current dir");
        }
    }

    #[test]
    fn resolve_schema_path_arg_auto_detects_first_nautilus_file() {
        let _cwd_lock = working_dir_lock().lock().expect("cwd lock");
        let temp_dir = TempDir::new().expect("temp dir");
        let _dir_guard = CurrentDirGuard::set(temp_dir.path());

        std::fs::write(
            temp_dir.path().join("zeta.nautilus"),
            "model User { id Int @id }\n",
        )
        .expect("failed to write zeta schema");
        std::fs::write(
            temp_dir.path().join("alpha.nautilus"),
            "model Post { id Int @id }\n",
        )
        .expect("failed to write alpha schema");

        let resolved = resolve_schema_path_arg(None).expect("schema should auto-resolve");
        assert_eq!(
            Path::new(&resolved)
                .file_name()
                .and_then(|name| name.to_str()),
            Some("alpha.nautilus")
        );
    }

    #[test]
    fn resolve_schema_path_arg_errors_when_no_nautilus_files_exist() {
        let _cwd_lock = working_dir_lock().lock().expect("cwd lock");
        let temp_dir = TempDir::new().expect("temp dir");
        let _dir_guard = CurrentDirGuard::set(temp_dir.path());

        let err = resolve_schema_path_arg(None).expect_err("missing schema should fail");
        assert!(
            err.to_string()
                .contains("No .nautilus schema file found in current directory"),
            "got: {err}"
        );
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
