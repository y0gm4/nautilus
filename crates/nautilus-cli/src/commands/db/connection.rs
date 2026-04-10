//! Shared database connection helpers used by all `nautilus db` subcommands.

use anyhow::{bail, Context};
use nautilus_migrate::{
    order_changes_for_apply, Change, ChangeRisk, DatabaseProvider, DiffApplier, LiveSchema,
};
use nautilus_schema::{ir::SchemaIr, validate_schema_source};
use std::path::{Path, PathBuf};

use crate::tui;

/// Locate the `.nautilus` schema file.
///
/// Priority: explicit `--schema` argument -> `schema.nautilus` in the current
/// directory. Returns an error if neither is available.
pub fn resolve_schema_path(schema_arg: Option<String>) -> anyhow::Result<PathBuf> {
    schema_arg
        .map(PathBuf::from)
        .or_else(|| {
            let default = PathBuf::from("schema.nautilus");
            if default.exists() {
                Some(default)
            } else {
                None
            }
        })
        .context(
            "Schema file not found. Pass --schema <path> or \
             create schema.nautilus in the current directory.",
        )
}

/// Lex, parse, and validate a schema file, returning the [`SchemaIr`].
pub fn parse_and_validate_schema(path: &std::path::Path) -> anyhow::Result<SchemaIr> {
    let source = std::fs::read_to_string(path)
        .with_context(|| format!("Cannot read schema file: {}", path.display()))?;

    let path_str = path.to_string_lossy();

    validate_schema_source(&source)
        .map(|validated| validated.ir)
        .map_err(|e| anyhow::anyhow!("{}", e.format_with_file(&path_str, &source)))
}

/// Resolve an admin/database-tooling URL from (in order): explicit flag,
/// datasource `direct_url`, `DATABASE_URL` env var, or datasource `url`.
///
/// This mirrors Prisma-style behavior where CLI/admin flows can prefer a
/// direct connection while runtime traffic continues to use the pooled `url`.
pub fn resolve_db_url(db_url_arg: Option<String>, schema_ir: &SchemaIr) -> anyhow::Result<String> {
    let raw = db_url_arg
        .or_else(|| {
            schema_ir
                .datasource
                .as_ref()
                .and_then(|ds| ds.direct_url.clone())
        })
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .or_else(|| {
            schema_ir
                .datasource
                .as_ref()
                .filter(|ds| !ds.url.is_empty())
                .map(|ds| ds.url.clone())
        })
        .context(
            "No database URL found. Use --database-url, set datasource direct_url/url, \
             or set DATABASE_URL.",
        )?;
    resolve_url(&raw)
}

/// Tri-variant connection wrapper around sqlx pool types.
///
/// Each `nautilus db` subcommand resolves a database URL and uses this enum to
/// execute raw SQL against SQLite, PostgreSQL, or MySQL without the caller
/// needing to know the concrete driver.
pub enum Connection {
    Sqlite(sqlx::SqlitePool),
    Postgres(sqlx::PgPool),
    Mysql(sqlx::MySqlPool),
}

/// Execute `$body` against the inner pool of every [`Connection`] variant.
///
/// `$self` must be a `&Connection`, `$pool` is the binding name for the
/// inner pool reference. The macro expands a `match` arm for each variant.
macro_rules! with_pool {
    ($self:expr, $pool:ident => $body:expr) => {
        match $self {
            Connection::Sqlite($pool) => $body,
            Connection::Postgres($pool) => $body,
            Connection::Mysql($pool) => $body,
        }
    };
}

impl Connection {
    /// Open a connection pool for the given `provider`.
    ///
    /// For SQLite the database file is created if it does not exist.
    pub async fn connect(url: &str, provider: DatabaseProvider) -> anyhow::Result<Self> {
        match provider {
            DatabaseProvider::Sqlite => {
                use sqlx::sqlite::SqliteConnectOptions;
                use std::str::FromStr;
                let opts = SqliteConnectOptions::from_str(url)
                    .context("Invalid SQLite URL")?
                    .create_if_missing(true);
                let pool = sqlx::SqlitePool::connect_with(opts)
                    .await
                    .context("SQLite connection failed")?;
                Ok(Connection::Sqlite(pool))
            }
            DatabaseProvider::Postgres => {
                let pool = sqlx::PgPool::connect_with(postgres_connect_options(url)?)
                    .await
                    .context("PostgreSQL connection failed")?;
                Ok(Connection::Postgres(pool))
            }
            DatabaseProvider::Mysql => {
                let pool = sqlx::MySqlPool::connect(url)
                    .await
                    .context("MySQL connection failed")?;
                Ok(Connection::Mysql(pool))
            }
        }
    }

    /// Execute multiple SQL statements inside a single transaction.
    ///
    /// On any error the transaction is rolled back and the error is returned.
    pub async fn execute_in_transaction(&self, stmts: &[String]) -> anyhow::Result<()> {
        with_pool!(self, pool => {
            let mut tx = pool.begin().await.context("begin transaction")?;
            for sql in stmts {
                sqlx::query(sql)
                    .execute(&mut *tx)
                    .await
                    .context("transaction error")?;
            }
            tx.commit().await.context("commit transaction")?;
        });
        Ok(())
    }

    /// Execute a raw SQL script inside a single transaction.
    ///
    /// The script is passed through to the database driver unchanged so the
    /// server, rather than the CLI, determines real statement boundaries.
    pub async fn execute_script_in_transaction(&self, script: &str) -> anyhow::Result<()> {
        with_pool!(self, pool => {
            let mut tx = pool.begin().await.context("begin transaction")?;
            sqlx::raw_sql(script)
                .execute(&mut *tx)
                .await
                .context("transaction error")?;
            tx.commit().await.context("commit transaction")?;
        });
        Ok(())
    }
}

fn postgres_connect_options(url: &str) -> anyhow::Result<sqlx::postgres::PgConnectOptions> {
    use std::str::FromStr;

    // Disable SQLx's persistent statement cache for CLI/admin Postgres commands.
    // This keeps `nautilus db *` compatible with PgBouncer transaction pooling
    // and similar proxies that reject reusing named prepared statements.
    sqlx::postgres::PgConnectOptions::from_str(url)
        .map(|options| options.statement_cache_capacity(0))
        .context("Invalid PostgreSQL URL")
}

/// Unwrap `env(VAR)` syntax; otherwise return the URL as-is.
pub fn resolve_url(raw: &str) -> anyhow::Result<String> {
    nautilus_schema::resolve_env_url(raw).map_err(|msg| anyhow::anyhow!(msg))
}

/// Infer the [`DatabaseProvider`] from a connection URL prefix.
pub fn detect_provider(url: &str) -> anyhow::Result<DatabaseProvider> {
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        Ok(DatabaseProvider::Postgres)
    } else if url.starts_with("mysql://") {
        Ok(DatabaseProvider::Mysql)
    } else if url.starts_with("sqlite:") {
        Ok(DatabaseProvider::Sqlite)
    } else {
        bail!("Cannot detect database provider from URL: {}", url)
    }
}

/// Replace the password/token segment of a URL with `***` for safe display.
pub fn obfuscate_url(url: &str) -> String {
    if let Some(at) = url.rfind('@') {
        if let Some(scheme_end) = url.find("://") {
            let scheme = &url[..scheme_end + 3];
            let host_onwards = &url[at..];
            return format!("{}***{}", scheme, host_onwards);
        }
    }
    url.to_string()
}

/// Everything a `nautilus db` subcommand typically needs after loading the
/// schema, resolving the URL, and connecting to the database.
pub struct DbContext {
    pub schema_ir: SchemaIr,
    pub database_url: String,
    pub provider: DatabaseProvider,
    pub conn: Connection,
}

impl DbContext {
    /// Parse a schema file, resolve the database URL, connect, and inspect the
    /// provider — the shared preamble of `push`, `status`, and `reset`.
    pub async fn build(
        schema_arg: Option<String>,
        db_url_arg: Option<String>,
    ) -> anyhow::Result<Self> {
        let schema_path = resolve_schema_path(schema_arg)?;

        load_dotenv_for_schema(&schema_path);

        let sp = tui::spinner("Parsing schema…");
        let schema_ir = parse_and_validate_schema(&schema_path)?;

        let model_count = schema_ir.models.len();
        let provider_name = schema_ir
            .datasource
            .as_ref()
            .map(|ds| ds.provider.clone())
            .unwrap_or_else(|| "unknown".to_string());

        tui::spinner_ok(
            sp,
            &format!(
                "Schema parsed  ({} model{}, {})",
                model_count,
                if model_count == 1 { "" } else { "s" },
                provider_name,
            ),
        );

        let database_url = resolve_db_url(db_url_arg, &schema_ir)?;

        let sp = tui::spinner("Connecting to database…");
        let provider = detect_provider(&database_url)?;
        let conn = Connection::connect(&database_url, provider)
            .await
            .with_context(|| format!("Failed to connect to {}", database_url))?;
        tui::spinner_ok(sp, &format!("Connected  {}", obfuscate_url(&database_url)));

        Ok(DbContext {
            schema_ir,
            database_url,
            provider,
            conn,
        })
    }
}

/// Short human-readable label for a [`Change`] (used in progress lines).
pub fn change_display_name(change: &Change) -> String {
    match change {
        Change::NewTable(m) => m.db_name.clone(),
        Change::DroppedTable { name } => name.clone(),
        Change::AddedColumn { table, field } => format!("{}.{}", table, field.db_name),
        Change::DroppedColumn { table, column }
        | Change::TypeChanged { table, column, .. }
        | Change::NullabilityChanged { table, column, .. }
        | Change::DefaultChanged { table, column, .. }
        | Change::ComputedExprChanged { table, column, .. } => format!("{}.{}", table, column),
        Change::CheckChanged {
            table,
            column: Some(col),
            ..
        } => format!("{}.{}", table, col),
        Change::CheckChanged {
            table,
            column: None,
            ..
        } => format!("{} (CHECK)", table),
        Change::PrimaryKeyChanged { table } => format!("{} (PK)", table),
        Change::IndexAdded { table, columns, .. } | Change::IndexDropped { table, columns, .. } => {
            format!("{} ({})", table, columns.join(","))
        }
        Change::CreateCompositeType { name }
        | Change::DropCompositeType { name }
        | Change::AlterCompositeType { name, .. } => format!("type:{}", name),
        Change::CreateEnum { name, .. }
        | Change::DropEnum { name }
        | Change::AlterEnum { name, .. } => format!("enum:{}", name),
        Change::ForeignKeyAdded { table, columns, .. } => {
            format!("{} (fk:{})", table, columns.join(","))
        }
        Change::ForeignKeyDropped {
            table,
            constraint_name,
        } => format!("{} (fk:{})", table, constraint_name),
    }
}

/// Apply a list of classified changes through the given [`DiffApplier`],
/// executing **all** generated SQL inside a single transaction so that a
/// failure causes a full rollback with no partial state left in the database.
///
/// Returns `(ok, failed)` counts where `ok` is the number of changes applied
/// and `failed` is 0 on success or the total number of changes on failure.
pub async fn apply_changes(
    classified: &[(Change, ChangeRisk)],
    applier: &DiffApplier<'_>,
    live: &LiveSchema,
    conn: &Connection,
) -> anyhow::Result<(usize, usize)> {
    let ordered_changes = order_changes_for_apply(
        &classified
            .iter()
            .map(|(change, _risk)| change.clone())
            .collect::<Vec<_>>(),
        live,
    );
    let mut change_stmts: Vec<(String, Vec<String>)> = Vec::new();
    for change in &ordered_changes {
        let label = change_display_name(change);
        let stmts = applier
            .sql_for(change)
            .map_err(|e| anyhow::anyhow!("SQL generation failed for {}: {}", label, e))?;
        change_stmts.push((label, stmts));
    }

    let all_stmts: Vec<String> = change_stmts
        .iter()
        .flat_map(|(_, stmts)| stmts.iter().cloned())
        .collect();

    let sp = tui::spinner("Applying…");
    match conn.execute_in_transaction(&all_stmts).await {
        Ok(()) => {
            tui::spinner_ok(sp, "Transaction committed");
            for (label, _) in &change_stmts {
                tui::print_ok(label);
            }
            Ok((change_stmts.len(), 0))
        }
        Err(e) => {
            tui::spinner_err(sp, "Transaction failed — rolled back");
            for (label, stmts) in &change_stmts {
                tui::print_err_line(label);
                for sql in stmts {
                    eprintln!("  [sql] {}", sql);
                }
            }
            tui::print_table_err("Transaction", &format!("{:#}", e));
            Ok((0, change_stmts.len()))
        }
    }
}

/// Load a `.env` file and inject its entries into the process environment.
///
/// Search order (first file found wins):
///   1. Directory containing the schema file.
///   2. Current working directory.
///
/// Already-set variables are never overwritten (shell exports take priority).
/// Supports `KEY=VALUE` and `KEY="VALUE"` / `KEY='VALUE'`; `#` comments; blank
/// lines. No variable-expansion is performed.
pub(crate) fn load_dotenv_for_schema(schema_path: &Path) {
    let search_dirs: &[PathBuf] = &[
        schema_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from(".")),
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
    ];

    for dir in search_dirs {
        let candidate = dir.join(".env");
        if !candidate.is_file() {
            continue;
        }
        if let Ok(contents) = std::fs::read_to_string(&candidate) {
            for line in contents.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((key, value)) = line.split_once('=') {
                    let key = key.trim();
                    let mut value = value.trim();
                    if value.len() >= 2 {
                        let (first, last) =
                            (value.as_bytes()[0], value.as_bytes()[value.len() - 1]);
                        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
                            value = &value[1..value.len() - 1];
                        }
                    }
                    if !key.is_empty() && std::env::var(key).is_err() {
                        // SAFETY: single-threaded context (before async spawn)
                        #[allow(clippy::disallowed_methods)]
                        std::env::set_var(key, value);
                    }
                }
            }
        }
        return; // first file found wins
    }
}

#[cfg(test)]
mod tests {
    use super::{postgres_connect_options, resolve_db_url, resolve_schema_path};
    use crate::test_support::{lock_working_dir, CurrentDirGuard, EnvVarGuard};
    use nautilus_schema::validate_schema_source;
    use tempfile::TempDir;

    fn parse_schema_ir(source: &str) -> nautilus_schema::ir::SchemaIr {
        validate_schema_source(source)
            .expect("schema should validate")
            .ir
    }

    #[test]
    fn resolve_schema_path_only_falls_back_to_schema_nautilus() {
        let _cwd_lock = lock_working_dir();
        let temp_dir = TempDir::new().expect("temp dir");
        let _dir_guard = CurrentDirGuard::set(temp_dir.path());

        std::fs::write(
            temp_dir.path().join("custom.nautilus"),
            "model User { id Int @id }\n",
        )
        .expect("failed to write custom schema");

        let err = resolve_schema_path(None).expect_err("only schema.nautilus should auto-resolve");
        assert!(err
            .to_string()
            .contains("Schema file not found. Pass --schema <path> or create schema.nautilus"));

        std::fs::write(
            temp_dir.path().join("schema.nautilus"),
            "model User { id Int @id }\n",
        )
        .expect("failed to write default schema");

        let resolved = resolve_schema_path(None).expect("default schema should resolve");
        assert_eq!(
            resolved.file_name().and_then(|name| name.to_str()),
            Some("schema.nautilus")
        );
    }

    #[test]
    fn postgres_connect_options_disable_statement_cache() {
        let options = postgres_connect_options("postgres://user:pass@localhost/db")
            .expect("expected valid PostgreSQL options");

        let rendered = format!("{options:?}");
        assert!(rendered.contains("statement_cache_capacity: 0"));
    }

    #[test]
    fn resolve_db_url_prefers_direct_url_for_admin_flows() {
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

        let url = resolve_db_url(None, &schema_ir).expect("expected database url");
        assert_eq!(url, "postgres://direct/admin");
    }

    #[test]
    fn resolve_db_url_falls_back_to_runtime_url_when_direct_url_missing() {
        let _env_guard = EnvVarGuard::unset("DATABASE_URL");
        let schema_ir = parse_schema_ir(
            r#"
datasource db {
  provider = "postgresql"
  url      = "postgres://pooled/runtime"
}

model User {
  id Int @id
}
"#,
        );

        let url = resolve_db_url(None, &schema_ir).expect("expected database url");
        assert_eq!(url, "postgres://pooled/runtime");
    }
}
