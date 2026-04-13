use anyhow::{bail, Context};
use nautilus_migrate::{serialize_live_schema_with_options, PullNamingOptions, SchemaInspector};
use nautilus_schema::parse_schema_source_with_recovery;
use std::path::{Path, PathBuf};

use super::connection::{
    detect_provider, load_dotenv_for_schema, maybe_resolve_schema_path, obfuscate_url,
    parse_and_validate_schema, resolve_db_url, resolve_url,
};
use crate::tui;

/// Execute `nautilus db pull` — introspect the live database and write an
/// equivalent `.nautilus` schema file.
pub async fn run(
    schema_arg: Option<String>,
    db_url_arg: Option<String>,
    output_arg: Option<String>,
    naming_options: PullNamingOptions,
) -> anyhow::Result<()> {
    tui::print_header("db pull");

    let database_url = resolve_database_url_for_pull(schema_arg.as_deref(), db_url_arg)?;

    let provider = detect_provider(&database_url)?;
    let sp = tui::spinner(&format!(
        "Connecting & introspecting {}…",
        obfuscate_url(&database_url)
    ));
    let live = SchemaInspector::new(provider, &database_url)
        .inspect()
        .await
        .context("Failed to inspect live schema")?;
    tui::spinner_ok(
        sp,
        &format!(
            "Introspected  ({} table{})",
            live.tables.len(),
            if live.tables.len() == 1 { "" } else { "s" },
        ),
    );

    let output_path = std::path::PathBuf::from(output_arg.as_deref().unwrap_or("pulled.nautilus"));

    let write_path: std::path::PathBuf = if output_path.exists() {
        let new_path = next_available_path(&output_path);
        let opt_new = format!("Create new file  ({})", new_path.display());
        let opt_over = format!("Overwrite        ({})", output_path.display());
        let options = [opt_new.as_str(), opt_over.as_str(), "Cancel"];

        match tui::select_option(
            &format!("{} already exists", output_path.display()),
            &options,
        ) {
            Some(0) => new_path,
            Some(1) => output_path,
            _ => {
                tui::print_summary_err("Cancelled", "No file was written");
                bail!("Cancelled by user");
            }
        }
    } else {
        output_path
    };

    let schema_text =
        serialize_live_schema_with_options(&live, provider, &database_url, naming_options);

    std::fs::write(&write_path, &schema_text)
        .with_context(|| format!("Cannot write {}", write_path.display()))?;

    tui::print_summary_ok(
        "Schema pulled",
        &format!(
            "{}  ({} table{})",
            write_path.display(),
            live.tables.len(),
            if live.tables.len() == 1 { "" } else { "s" },
        ),
    );

    Ok(())
}

/// Best-effort extraction of the database URL for `db pull`.
///
/// Prefer the shared validated admin resolver so `db pull` matches the rest of
/// the CLI. If the schema is otherwise invalid, fall back to a datasource-only
/// parse with recovery so unrelated parse errors do not stop introspection.
fn resolve_database_url_for_pull(
    schema_arg: Option<&str>,
    db_url_arg: Option<String>,
) -> anyhow::Result<String> {
    if let Some(raw) = db_url_arg.as_deref() {
        return resolve_url(raw);
    }

    let schema_path = prepare_schema_env_for_pull(schema_arg)?;

    if let Some(path) = schema_path.as_deref() {
        if let Ok(schema_ir) = parse_and_validate_schema(path) {
            return resolve_db_url(None, &schema_ir);
        }
    }

    let raw_url = schema_path
        .as_deref()
        .and_then(|path| resolve_url_from_schema_path(path, "direct_url"))
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .or_else(|| {
            schema_path
                .as_deref()
                .and_then(|path| resolve_url_from_schema_path(path, "url"))
        })
        .context(
            "No database URL found. \
            Use --database-url, set DATABASE_URL, or add a datasource direct_url/url to your schema file.",
        )?;

    resolve_url(&raw_url)
}

fn prepare_schema_env_for_pull(schema_arg: Option<&str>) -> anyhow::Result<Option<PathBuf>> {
    let schema_path = maybe_resolve_schema_path(schema_arg)?;
    let dotenv_anchor = schema_path
        .clone()
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    load_dotenv_for_schema(&dotenv_anchor);
    Ok(schema_path)
}

fn resolve_url_from_schema_path(path: &Path, field_name: &str) -> Option<String> {
    let source = std::fs::read_to_string(path).ok()?;
    let ast = parse_schema_source_with_recovery(&source).ok()?.ast;
    ast.datasource()
        .and_then(|ds| ds.find_field(field_name))
        .and_then(|f| match &f.value {
            nautilus_schema::ast::Expr::Literal(nautilus_schema::ast::Literal::String(s, _)) => {
                Some(s.clone())
            }
            nautilus_schema::ast::Expr::FunctionCall { name, args, .. } if name.value == "env" => {
                if let Some(nautilus_schema::ast::Expr::Literal(
                    nautilus_schema::ast::Literal::String(var, _),
                )) = args.first()
                {
                    std::env::var(var).ok()
                } else {
                    None
                }
            }
            _ => None,
        })
}

/// Return the first `<stem>_N.<ext>` path that does not yet exist on disk.
fn next_available_path(base: &std::path::Path) -> std::path::PathBuf {
    let stem = base
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("pulled");
    let ext = base
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("nautilus");
    let parent = base.parent().unwrap_or(std::path::Path::new("."));

    let mut i = 1usize;
    loop {
        let candidate = parent.join(format!("{}_{}.{}", stem, i, ext));
        if !candidate.exists() {
            return candidate;
        }
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_database_url_for_pull;
    use crate::test_support::{lock_working_dir, CurrentDirGuard, EnvVarGuard};
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    fn write_schema(dir: &Path, env_var: &str) -> PathBuf {
        let schema_path = dir.join("schema.nautilus");
        let schema = format!(
            r#"datasource db {{
  provider = "sqlite"
  url      = env("{env_var}")
}}

model User {{
  id Int @id
}}
"#
        );
        std::fs::write(&schema_path, schema).expect("failed to write schema");
        schema_path
    }

    #[test]
    fn resolve_database_url_for_pull_loads_dotenv_next_to_schema() {
        let _db_url_guard = EnvVarGuard::unset("DATABASE_URL");
        let env_key = "NAUTILUS_PULL_SCHEMA_DIR_URL";
        let _env_guard = EnvVarGuard::unset(env_key);

        let project = TempDir::new().expect("temp dir");
        let schema_path = write_schema(project.path(), env_key);
        std::fs::write(
            project.path().join(".env"),
            format!("{env_key}=sqlite:./from-schema-dir.db\n"),
        )
        .expect("failed to write dotenv");

        let url = resolve_database_url_for_pull(schema_path.to_str(), None)
            .expect("expected db pull URL from schema directory dotenv");

        assert_eq!(url, "sqlite:./from-schema-dir.db");
    }

    #[test]
    fn resolve_database_url_for_pull_falls_back_to_cwd_dotenv() {
        let _cwd_lock = lock_working_dir();
        let _db_url_guard = EnvVarGuard::unset("DATABASE_URL");
        let env_key = "NAUTILUS_PULL_CWD_URL";
        let _env_guard = EnvVarGuard::unset(env_key);

        let schema_dir = TempDir::new().expect("schema temp dir");
        let cwd_dir = TempDir::new().expect("cwd temp dir");
        let schema_path = write_schema(schema_dir.path(), env_key);

        std::fs::write(
            cwd_dir.path().join(".env"),
            format!("{env_key}=sqlite:./from-cwd.db\n"),
        )
        .expect("failed to write cwd dotenv");

        let _dir_guard = CurrentDirGuard::set(cwd_dir.path());

        let url = resolve_database_url_for_pull(schema_path.to_str(), None)
            .expect("expected db pull URL from cwd dotenv");

        assert_eq!(url, "sqlite:./from-cwd.db");
    }

    #[test]
    fn resolve_database_url_for_pull_auto_detects_first_nautilus_file() {
        let _cwd_lock = lock_working_dir();
        let _db_url_guard = EnvVarGuard::unset("DATABASE_URL");
        let project = TempDir::new().expect("temp dir");
        let _dir_guard = CurrentDirGuard::set(project.path());

        std::fs::write(
            project.path().join("zeta.nautilus"),
            r#"datasource db {
  provider = "sqlite"
  url      = "sqlite:./zeta.db"
}

model User {
  id Int @id
}
"#,
        )
        .expect("failed to write zeta schema");
        std::fs::write(
            project.path().join("alpha.nautilus"),
            r#"datasource db {
  provider = "sqlite"
  url      = "sqlite:./alpha.db"
}

model Post {
  id Int @id
}
"#,
        )
        .expect("failed to write alpha schema");

        let url = resolve_database_url_for_pull(None, None)
            .expect("expected db pull URL from auto-detected schema");

        assert_eq!(url, "sqlite:./alpha.db");
    }

    #[test]
    fn resolve_database_url_for_pull_prefers_direct_url_from_schema() {
        let env_key = "NAUTILUS_PULL_DIRECT_URL";
        let _db_url_guard = EnvVarGuard::unset("DATABASE_URL");
        let _env_guard = EnvVarGuard::unset(env_key);
        let project = TempDir::new().expect("temp dir");
        let schema_path = project.path().join("schema.nautilus");
        std::fs::write(
            &schema_path,
            format!(
                r#"datasource db {{
  provider   = "postgresql"
  url        = "postgres://pooled/runtime"
  direct_url = env("{env_key}")
}}

model User {{
  id Int @id
}}
"#
            ),
        )
        .expect("failed to write schema");
        std::fs::write(
            project.path().join(".env"),
            format!("{env_key}=postgres://direct/admin\n"),
        )
        .expect("failed to write dotenv");

        let url = resolve_database_url_for_pull(schema_path.to_str(), None)
            .expect("expected db pull URL from direct_url");

        assert_eq!(url, "postgres://direct/admin");
    }

    #[test]
    fn resolve_database_url_for_pull_recovers_datasource_from_parse_errors() {
        let env_key = "NAUTILUS_PULL_RECOVERY_DIRECT_URL";
        let _db_url_guard = EnvVarGuard::unset("DATABASE_URL");
        let _env_guard = EnvVarGuard::unset(env_key);
        let project = TempDir::new().expect("temp dir");
        let schema_path = project.path().join("schema.nautilus");
        std::fs::write(
            &schema_path,
            format!(
                r#"datasource db {{
  provider   = "postgresql"
  url        = "postgres://pooled/runtime"
  direct_url = env("{env_key}")
}}

model User {{
  id
}}
"#
            ),
        )
        .expect("failed to write schema");
        std::fs::write(
            project.path().join(".env"),
            format!("{env_key}=postgres://direct/admin\n"),
        )
        .expect("failed to write dotenv");

        let url = resolve_database_url_for_pull(schema_path.to_str(), None)
            .expect("expected db pull URL from recovered datasource");

        assert_eq!(url, "postgres://direct/admin");
    }
}
