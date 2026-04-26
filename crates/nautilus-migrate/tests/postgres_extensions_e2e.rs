mod common;

use std::sync::atomic::{AtomicU64, Ordering};

use nautilus_migrate::{
    change_risk, serialize_live_schema, Change, ChangeRisk, DatabaseProvider, DdlGenerator,
    DiffApplier, SchemaDiff, SchemaInspector,
};
use sqlx::{PgPool, Row};

static NEXT_SCHEMA_ID: AtomicU64 = AtomicU64::new(0);

fn database_url() -> String {
    std::env::var("NAUTILUS_TEST_POSTGRES_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgres://nautilus:nautilus@localhost/nautilus_test".to_string())
}

fn unique_schema(prefix: &str) -> String {
    let id = NEXT_SCHEMA_ID.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}_{}", prefix, std::process::id(), id)
}

fn quote_ident(name: &str) -> String {
    assert!(
        name.chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_'),
        "test schema names must be simple identifiers"
    );
    format!("\"{}\"", name)
}

fn schema_scoped_url(base_url: &str, schema: &str) -> String {
    let sep = if base_url.contains('?') { '&' } else { '?' };
    format!("{base_url}{sep}options=-c%20search_path%3D{schema}%2Cpublic")
}

fn escape_schema_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn render_extension_name(name: &str) -> String {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return "\"\"".to_string();
    };
    if (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        name.to_string()
    } else {
        format!("\"{}\"", escape_schema_string(name))
    }
}

async fn create_schema(pool: &PgPool, schema: &str) -> Result<(), sqlx::Error> {
    sqlx::query(&format!("CREATE SCHEMA {}", quote_ident(schema)))
        .execute(pool)
        .await?;
    Ok(())
}

async fn drop_schema(pool: &PgPool, schema: &str) {
    let _ = sqlx::query(&format!(
        "DROP SCHEMA IF EXISTS {} CASCADE",
        quote_ident(schema)
    ))
    .execute(pool)
    .await;
}

async fn execute_all(pool: &PgPool, statements: &[String]) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    for statement in statements {
        sqlx::query(statement)
            .persistent(false)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance (run `docker-compose up -d` first)"]
async fn db_push_style_extensions_round_trip_and_diff_to_noop(
) -> Result<(), Box<dyn std::error::Error>> {
    let base_url = database_url();
    let admin_pool = PgPool::connect(&base_url).await?;
    let schema = unique_schema("nautilus_ext_push");
    create_schema(&admin_pool, &schema).await?;

    let result = async {
        let scoped_url = schema_scoped_url(&base_url, &schema);
        let source = format!(
            r#"
datasource db {{
  provider   = "postgresql"
  url        = "{}"
  extensions = [citext, hstore, ltree]
}}

model PgExtDoc {{
  id    Int     @id
  email Citext
  meta  Hstore?
  path  Ltree?
}}
"#,
            escape_schema_string(&scoped_url)
        );
        let target = common::parse(&source)?;
        let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
        let statements = ddl.generate_create_tables(&target)?;

        let create_table_idx = statements
            .iter()
            .position(|sql| sql.starts_with("CREATE TABLE"))
            .expect("expected CREATE TABLE");
        for extension in ["citext", "hstore", "ltree"] {
            let create_ext_idx = statements
                .iter()
                .position(|sql| sql == &format!("CREATE EXTENSION IF NOT EXISTS \"{extension}\""))
                .unwrap_or_else(|| panic!("missing CREATE EXTENSION for {extension}"));
            assert!(
                create_ext_idx < create_table_idx,
                "extension DDL must run before table DDL: {statements:?}"
            );
        }

        let scoped_pool = PgPool::connect(&scoped_url).await?;
        execute_all(&scoped_pool, &statements).await?;

        let live = SchemaInspector::new(DatabaseProvider::Postgres, &scoped_url)
            .inspect()
            .await?;
        for extension in ["citext", "hstore", "ltree"] {
            assert!(
                live.extensions.contains_key(extension),
                "missing inspected extension {extension}: {:?}",
                live.extensions
            );
        }

        let table = live
            .tables
            .get("PgExtDoc")
            .expect("expected PgExtDoc in inspected schema");
        let column_type = |name: &str| {
            table
                .columns
                .iter()
                .find(|column| column.name == name)
                .map(|column| column.col_type.as_str())
                .unwrap_or_else(|| panic!("missing inspected column {name}"))
        };
        assert_eq!(column_type("email"), "citext");
        assert_eq!(column_type("meta"), "hstore");
        assert_eq!(column_type("path"), "ltree");

        let pulled = serialize_live_schema(&live, DatabaseProvider::Postgres, &scoped_url);
        assert!(pulled.contains("extensions = ["), "pulled schema: {pulled}");
        for extension in ["citext", "hstore", "ltree"] {
            assert!(
                pulled.contains(extension),
                "pulled schema should include {extension}: {pulled}"
            );
        }

        let mut live_extensions: Vec<&str> = live.extensions.keys().map(String::as_str).collect();
        live_extensions.sort_unstable();
        let no_op_source = format!(
            r#"
datasource db {{
  provider   = "postgresql"
  url        = "{}"
  extensions = [{}]
}}

model PgExtDoc {{
  id    Int     @id
  email Citext
  meta  Hstore?
  path  Ltree?
}}
"#,
            escape_schema_string(&scoped_url),
            live_extensions
                .iter()
                .map(|name| render_extension_name(name))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let no_op_target = common::parse(&no_op_source)?;
        let changes = SchemaDiff::compute(&live, &no_op_target, DatabaseProvider::Postgres);
        assert!(changes.is_empty(), "expected no-op diff, got: {changes:?}");

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    drop_schema(&admin_pool, &schema).await;
    result
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance with pgvector available (run `docker-compose up -d` first)"]
async fn pgvector_round_trip_with_hnsw_index_and_destructive_drop(
) -> Result<(), Box<dyn std::error::Error>> {
    let base_url = database_url();
    let admin_pool = PgPool::connect(&base_url).await?;
    let schema = unique_schema("nautilus_pgvector");
    create_schema(&admin_pool, &schema).await?;

    let result = async {
        let scoped_url = schema_scoped_url(&base_url, &schema);
        let scoped_pool = PgPool::connect(&scoped_url).await?;

        // Detect whether pgvector is available on this server; gracefully skip
        // otherwise so the test stays useful on a vanilla PostgreSQL image.
        let probe = sqlx::query("CREATE EXTENSION IF NOT EXISTS \"vector\"")
            .persistent(false)
            .execute(&scoped_pool)
            .await;
        if let Err(err) = probe {
            let message = err.to_string().to_lowercase();
            if message.contains("not available") || message.contains("could not open extension") {
                eprintln!("skipping pgvector round-trip test: {message}");
                return Ok(());
            }
            return Err(Box::<dyn std::error::Error>::from(err));
        }
        // Drop it again so the generated DDL is responsible for creating it,
        // mirroring the production "fresh database" flow.
        sqlx::query("DROP EXTENSION IF EXISTS \"vector\"")
            .persistent(false)
            .execute(&scoped_pool)
            .await?;

        let source = format!(
            r#"
datasource db {{
  provider   = "postgresql"
  url        = "{}"
  extensions = [vector]
}}

model Embedding {{
  id        Int       @id
  embedding Vector(3)

  @@index([embedding], type: Hnsw, opclass: vector_cosine_ops, m: 16, ef_construction: 64)
}}
"#,
            escape_schema_string(&scoped_url)
        );
        let target = common::parse(&source)?;
        let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
        let statements = ddl.generate_create_tables(&target)?;

        let create_ext_idx = statements
            .iter()
            .position(|sql| sql == "CREATE EXTENSION IF NOT EXISTS \"vector\"")
            .expect("missing CREATE EXTENSION for vector");
        let create_table_idx = statements
            .iter()
            .position(|sql| sql.starts_with("CREATE TABLE"))
            .expect("expected CREATE TABLE");
        assert!(
            create_ext_idx < create_table_idx,
            "extension DDL must run before table DDL: {statements:?}"
        );
        let create_index_stmt = statements
            .iter()
            .find(|sql| sql.to_lowercase().contains("using hnsw"))
            .unwrap_or_else(|| panic!("expected USING HNSW index DDL: {statements:?}"));
        assert!(
            create_index_stmt.contains("vector_cosine_ops"),
            "index DDL missing opclass: {create_index_stmt}"
        );
        assert!(
            create_index_stmt.contains("m = 16"),
            "index DDL missing m parameter: {create_index_stmt}"
        );
        assert!(
            create_index_stmt.contains("ef_construction = 64"),
            "index DDL missing ef_construction parameter: {create_index_stmt}"
        );

        execute_all(&scoped_pool, &statements).await?;

        let live = SchemaInspector::new(DatabaseProvider::Postgres, &scoped_url)
            .inspect()
            .await?;
        assert!(
            live.extensions.contains_key("vector"),
            "missing inspected vector extension: {:?}",
            live.extensions
        );

        let table = live
            .tables
            .get("Embedding")
            .expect("expected Embedding in inspected schema");
        let column_type = table
            .columns
            .iter()
            .find(|column| column.name == "embedding")
            .map(|column| column.col_type.as_str())
            .expect("missing inspected embedding column");
        assert_eq!(column_type, "vector(3)");

        let live_index = table
            .indexes
            .iter()
            .find(|i| i.columns == ["embedding"])
            .expect("missing inspected hnsw index");
        let pgvector = live_index
            .kind
            .pgvector()
            .expect("expected pgvector index kind");
        assert_eq!(pgvector.method, nautilus_schema::ir::PgvectorMethod::Hnsw);
        assert_eq!(
            pgvector.opclass,
            Some(nautilus_schema::ir::PgvectorOpClass::CosineOps)
        );
        assert_eq!(pgvector.options.m, Some(16));
        assert_eq!(pgvector.options.ef_construction, Some(64));

        let pulled = serialize_live_schema(&live, DatabaseProvider::Postgres, &scoped_url);
        assert!(pulled.contains("extensions = ["), "pulled schema: {pulled}");
        assert!(
            pulled.contains("vector"),
            "pulled schema should mention the vector extension: {pulled}"
        );
        assert!(
            pulled.contains("Vector(3)"),
            "pulled schema should mention the Vector(3) field: {pulled}"
        );

        // No-op diff: the live state must equal the declared target.
        let mut live_extensions: Vec<&str> = live.extensions.keys().map(String::as_str).collect();
        live_extensions.sort_unstable();
        let no_op_source = format!(
            r#"
datasource db {{
  provider   = "postgresql"
  url        = "{}"
  extensions = [{}]
}}

model Embedding {{
  id        Int       @id
  embedding Vector(3)

  @@index([embedding], type: Hnsw, opclass: vector_cosine_ops, m: 16, ef_construction: 64)
}}
"#,
            escape_schema_string(&scoped_url),
            live_extensions
                .iter()
                .map(|name| render_extension_name(name))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let no_op_target = common::parse(&no_op_source)?;
        let changes = SchemaDiff::compute(&live, &no_op_target, DatabaseProvider::Postgres);
        assert!(changes.is_empty(), "expected no-op diff, got: {changes:?}");

        // Destructive drop: a target that no longer references the vector
        // extension must produce a destructive `DropExtension`, and the
        // generated DDL must omit `CASCADE` so the apply fails while the
        // dependent `Embedding` table is still present.
        let drop_target = common::parse("model Other { id Int @id }")?;
        let drop_changes = SchemaDiff::compute(&live, &drop_target, DatabaseProvider::Postgres);
        let drop_ext_change = drop_changes
            .iter()
            .find(|c| matches!(c, Change::DropExtension { name } if name == "vector"))
            .expect("expected DropExtension { name: \"vector\" } in diff");
        assert_eq!(change_risk(drop_ext_change), ChangeRisk::Destructive);

        let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &drop_target, &live);
        let drop_sql = applier.sql_for(drop_ext_change)?;
        assert_eq!(drop_sql, vec!["DROP EXTENSION IF EXISTS \"vector\""]);
        assert!(!drop_sql[0].contains("CASCADE"));

        let err = sqlx::query(&drop_sql[0])
            .persistent(false)
            .execute(&scoped_pool)
            .await
            .expect_err(
                "DROP EXTENSION without CASCADE should fail while Embedding still depends on it",
            );
        let err_text = err.to_string();
        assert!(
            err_text.contains("depend") || err_text.contains("dependent"),
            "unexpected PostgreSQL error: {err}"
        );

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    drop_schema(&admin_pool, &schema).await;
    result
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance (run `docker-compose up -d` first)"]
async fn drop_extension_fails_without_cascade_when_objects_depend_on_it(
) -> Result<(), Box<dyn std::error::Error>> {
    let base_url = database_url();
    let admin_pool = PgPool::connect(&base_url).await?;
    let schema = unique_schema("nautilus_ext_drop");
    create_schema(&admin_pool, &schema).await?;

    let result = async {
        let scoped_url = schema_scoped_url(&base_url, &schema);
        let scoped_pool = PgPool::connect(&scoped_url).await?;

        sqlx::query("CREATE EXTENSION IF NOT EXISTS \"citext\"")
            .persistent(false)
            .execute(&scoped_pool)
            .await?;
        sqlx::query("CREATE TABLE \"DependsOnCitext\" (\"email\" CITEXT NOT NULL)")
            .persistent(false)
            .execute(&scoped_pool)
            .await?;

        let ir = common::parse("model Dummy { id Int @id }")?;
        let live = SchemaInspector::new(DatabaseProvider::Postgres, &scoped_url)
            .inspect()
            .await?;
        let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
        let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);
        let change = Change::DropExtension {
            name: "citext".to_string(),
        };
        assert_eq!(change_risk(&change), ChangeRisk::Destructive);
        let statements = applier.sql_for(&change)?;
        assert_eq!(statements, vec!["DROP EXTENSION IF EXISTS \"citext\""]);
        assert!(!statements[0].contains("CASCADE"));

        let err = sqlx::query(&statements[0])
            .persistent(false)
            .execute(&scoped_pool)
            .await
            .expect_err("DROP EXTENSION without CASCADE should fail with dependent objects");
        assert!(
            err.to_string().contains("depend") || err.to_string().contains("dependent"),
            "unexpected PostgreSQL error: {err}"
        );

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    drop_schema(&admin_pool, &schema).await;
    result
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL instance (run `docker-compose up -d` first)"]
async fn introspects_extension_installed_outside_public_schema(
) -> Result<(), Box<dyn std::error::Error>> {
    let base_url = database_url();
    let admin_pool = PgPool::connect(&base_url).await?;

    let already_installed: Option<String> =
        sqlx::query_scalar("SELECT extname FROM pg_extension WHERE extname = 'btree_gist'")
            .fetch_optional(&admin_pool)
            .await?;
    if already_installed.is_some() {
        eprintln!("skipping btree_gist namespace test because extension already exists");
        return Ok(());
    }

    let schema = unique_schema("nautilus_ext_namespace");
    create_schema(&admin_pool, &schema).await?;

    let result = async {
        let create_result = sqlx::query(&format!(
            "CREATE EXTENSION \"btree_gist\" WITH SCHEMA {}",
            quote_ident(&schema)
        ))
        .persistent(false)
        .execute(&admin_pool)
        .await;

        if let Err(err) = create_result {
            let message = err.to_string();
            if message.contains("extension") && message.contains("not available") {
                eprintln!("skipping btree_gist namespace test: {message}");
                return Ok(());
            }
            return Err(Box::<dyn std::error::Error>::from(err));
        }

        let namespace: String = sqlx::query(
            "SELECT n.nspname \
             FROM pg_extension e \
             JOIN pg_namespace n ON n.oid = e.extnamespace \
             WHERE e.extname = 'btree_gist'",
        )
        .fetch_one(&admin_pool)
        .await?
        .try_get("nspname")?;
        assert_eq!(namespace, schema);

        let scoped_url = schema_scoped_url(&base_url, &schema);
        let live = SchemaInspector::new(DatabaseProvider::Postgres, &scoped_url)
            .inspect()
            .await?;
        assert!(
            live.extensions.contains_key("btree_gist"),
            "extension installed outside public should still be introspected: {:?}",
            live.extensions
        );

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    let _ = sqlx::query("DROP EXTENSION IF EXISTS \"btree_gist\"")
        .persistent(false)
        .execute(&admin_pool)
        .await;
    drop_schema(&admin_pool, &schema).await;
    result
}
