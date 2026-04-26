mod common;

use nautilus_migrate::live::{LiveCompositeField, LiveCompositeType, LiveSchema};
use nautilus_migrate::{DatabaseProvider, DdlGenerator};

#[test]
fn test_generate_postgres_ddl() {
    let source = r#"
enum Role {
  USER
  ADMIN
}

model User {
  id    Int    @id
  email String @unique
  role  Role   @default(USER)
}
"#;
    let ir = common::parse(source).unwrap();

    let generator = DdlGenerator::new(DatabaseProvider::Postgres);
    let statements = generator.generate_create_tables(&ir).unwrap();

    assert!(statements.len() >= 2);
    assert!(statements[0].contains("CREATE TYPE"));
    assert!(statements[1].contains("CREATE TABLE"));
    assert!(statements[1].contains("\"id\""));
    assert!(statements[1].contains("\"email\""));
}

#[test]
fn test_generate_postgres_ddl_includes_secondary_indexes() {
    let source = r#"
model User {
  id        Int      @id
  createdAt DateTime @map("created_at")

  @@map("users")
  @@index([createdAt])
}
"#;
    let ir = common::parse(source).unwrap();

    let generator = DdlGenerator::new(DatabaseProvider::Postgres);
    let statements = generator.generate_create_tables(&ir).unwrap();

    assert!(
        statements
            .iter()
            .any(|sql| sql.contains("CREATE INDEX IF NOT EXISTS \"idx_users_created_at\"")),
        "expected CREATE INDEX statement after CREATE TABLE: {:?}",
        statements
    );
}

#[test]
fn test_generate_postgres_ddl_with_extension_backed_scalar_types() {
    let source = r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  extensions = [citext, hstore, ltree]
}

model User {
  id    Int    @id
  email Citext
  meta  Hstore
  path  Ltree
}
"#;
    let ir = common::parse(source).unwrap();

    let generator = DdlGenerator::new(DatabaseProvider::Postgres);
    let statements = generator.generate_create_tables(&ir).unwrap();
    let table_stmt = statements
        .iter()
        .find(|sql| sql.contains("CREATE TABLE"))
        .expect("missing create table statement");

    assert!(
        table_stmt.contains("\"email\" CITEXT"),
        "sql: {}",
        table_stmt
    );
    assert!(
        table_stmt.contains("\"meta\" HSTORE"),
        "sql: {}",
        table_stmt
    );
    assert!(table_stmt.contains("\"path\" LTREE"), "sql: {}", table_stmt);
}

#[test]
fn test_generate_postgres_ddl_with_pgvector_type() {
    let source = r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  extensions = [vector]
}

model Embedding {
  id     Int @id
  vector Vector(1536)
}
"#;
    let ir = common::parse(source).unwrap();

    let generator = DdlGenerator::new(DatabaseProvider::Postgres);
    let statements = generator.generate_create_tables(&ir).unwrap();
    let table_stmt = statements
        .iter()
        .find(|sql| sql.contains("CREATE TABLE"))
        .expect("missing create table statement");

    assert!(
        statements
            .iter()
            .any(|sql| sql == "CREATE EXTENSION IF NOT EXISTS \"vector\""),
        "statements: {:?}",
        statements
    );
    assert!(
        table_stmt.contains("\"vector\" VECTOR(1536)"),
        "sql: {}",
        table_stmt
    );
}

#[test]
fn test_generate_postgres_ddl_with_pgvector_hnsw_index() {
    let source = r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  extensions = [vector]
}

model Embedding {
  id        Int @id
  embedding Vector(3)

  @@index([embedding], type: Hnsw, opclass: vector_cosine_ops, m: 16, ef_construction: 64)
}
"#;
    let ir = common::parse(source).unwrap();

    let generator = DdlGenerator::new(DatabaseProvider::Postgres);
    let statements = generator.generate_create_tables(&ir).unwrap();
    let index_stmt = statements
        .iter()
        .find(|sql| sql.contains("USING HNSW"))
        .expect("missing create index statement");

    assert!(index_stmt.contains("(\"embedding\" vector_cosine_ops)"));
    assert!(index_stmt.contains("WITH (m = 16, ef_construction = 64)"));
}

#[test]
fn test_generate_sqlite_ddl() {
    let source = r#"
model Post {
  id    Int    @id
  title String
}
"#;
    let ir = common::parse(source).unwrap();

    let generator = DdlGenerator::new(DatabaseProvider::Sqlite);
    let statements = generator.generate_create_tables(&ir).unwrap();

    assert_eq!(statements.len(), 1);
    assert!(statements[0].contains("CREATE TABLE"));
    assert!(statements[0].contains("\"Post\""));
}

#[test]
fn test_composite_type_postgres_ddl() {
    let source = r#"
datasource db {
  provider = "postgresql"
  url      = "postgres://localhost/test"
}

type Address {
  street String
  city   String
  zip    String
}

model User {
  id      Int     @id
  address Address
}
"#;
    let ir = common::parse(source).unwrap();
    let generator = DdlGenerator::new(DatabaseProvider::Postgres);
    let statements = generator.generate_create_tables(&ir).unwrap();

    let composite_stmt = statements
        .iter()
        .find(|s| s.contains("CREATE TYPE \"address\" AS"));
    assert!(
        composite_stmt.is_some(),
        "Missing CREATE TYPE statement for composite type"
    );
    let stmt = composite_stmt.unwrap();
    assert!(stmt.contains("\"street\" TEXT"));
    assert!(stmt.contains("\"city\" TEXT"));
    assert!(stmt.contains("\"zip\" TEXT"));

    let table_stmt = statements.iter().find(|s| s.contains("CREATE TABLE"));
    assert!(table_stmt.is_some());
    assert!(table_stmt.unwrap().contains("address"));
}

#[test]
fn test_composite_type_sqlite_json_ddl() {
    let source = r#"
datasource db {
  provider = "sqlite"
  url      = "file:./dev.db"
}

type Address {
  street String
  city   String
}

model User {
  id      Int     @id
  address Address @store(json)
}
"#;
    let ir = common::parse(source).unwrap();
    let generator = DdlGenerator::new(DatabaseProvider::Sqlite);
    let statements = generator.generate_create_tables(&ir).unwrap();

    let table_stmt = statements
        .iter()
        .find(|s| s.contains("CREATE TABLE"))
        .unwrap();
    assert!(table_stmt.contains("\"address\" TEXT"));
}

#[test]
fn test_composite_type_mysql_json_ddl() {
    let source = r#"
datasource db {
  provider = "mysql"
  url      = "mysql://localhost/test"
}

type Address {
  street String
  city   String
}

model User {
  id      Int     @id
  address Address @store(json)
}
"#;
    let ir = common::parse(source).unwrap();
    let generator = DdlGenerator::new(DatabaseProvider::Mysql);
    let statements = generator.generate_create_tables(&ir).unwrap();

    let table_stmt = statements
        .iter()
        .find(|s| s.contains("CREATE TABLE"))
        .unwrap();
    assert!(table_stmt.contains("`address` JSON"));
}

#[test]
fn test_postgres_drop_tables_drops_composites_before_enums() {
    let source = r#"
datasource db {
  provider = "postgresql"
  url      = "postgres://localhost/test"
}

enum Status {
  DRAFT
  PUBLISHED
}

type Address {
  status Status
  street String
}

model User {
  id      Int     @id
  address Address
}
"#;
    let ir = common::parse(source).unwrap();
    let generator = DdlGenerator::new(DatabaseProvider::Postgres);
    let statements = generator.generate_drop_tables(&ir).unwrap();

    let composite_idx = statements
        .iter()
        .position(|s| s == "DROP TYPE IF EXISTS \"address\"")
        .unwrap();
    let enum_idx = statements
        .iter()
        .position(|s| s == "DROP TYPE IF EXISTS \"status\"")
        .unwrap();

    assert!(composite_idx < enum_idx);
}

#[test]
fn test_postgres_drop_live_tables_drops_composites_before_enums() {
    let mut live = LiveSchema::default();
    live.enums.insert(
        "status".to_string(),
        vec!["DRAFT".to_string(), "PUBLISHED".to_string()],
    );
    live.composite_types.insert(
        "address".to_string(),
        LiveCompositeType {
            name: "address".to_string(),
            fields: vec![
                LiveCompositeField {
                    name: "status".to_string(),
                    col_type: "status".to_string(),
                },
                LiveCompositeField {
                    name: "street".to_string(),
                    col_type: "text".to_string(),
                },
            ],
        },
    );

    let generator = DdlGenerator::new(DatabaseProvider::Postgres);
    let statements = generator.generate_drop_live_tables(&live);

    let composite_idx = statements
        .iter()
        .position(|s| s == "DROP TYPE IF EXISTS \"address\"")
        .unwrap();
    let enum_idx = statements
        .iter()
        .position(|s| s == "DROP TYPE IF EXISTS \"status\"")
        .unwrap();

    assert!(composite_idx < enum_idx);
}

#[test]
fn test_postgres_drop_live_tables_quotes_mixed_case_type_names() {
    let mut live = LiveSchema::default();
    live.enums.insert(
        "PostStatus".to_string(),
        vec!["DRAFT".to_string(), "PUBLISHED".to_string()],
    );
    live.composite_types.insert(
        "Address".to_string(),
        LiveCompositeType {
            name: "Address".to_string(),
            fields: vec![LiveCompositeField {
                name: "street".to_string(),
                col_type: "text".to_string(),
            }],
        },
    );

    let generator = DdlGenerator::new(DatabaseProvider::Postgres);
    let statements = generator.generate_drop_live_tables(&live);

    assert!(
        statements
            .iter()
            .any(|sql| sql == "DROP TYPE IF EXISTS \"Address\""),
        "expected quoted DROP TYPE for mixed-case composite: {:?}",
        statements
    );
    assert!(
        statements
            .iter()
            .any(|sql| sql == "DROP TYPE IF EXISTS \"PostStatus\""),
        "expected quoted DROP TYPE for mixed-case enum: {:?}",
        statements
    );
}

#[test]
fn test_postgres_extensions_emitted_before_tables() {
    let source = r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  extensions = [pg_trgm, "uuid-ossp"]
}

model Doc {
  id   String @id
  body String
}
"#;
    let ir = common::parse(source).unwrap();
    let generator = DdlGenerator::new(DatabaseProvider::Postgres);
    let statements = generator.generate_create_tables(&ir).unwrap();

    let ext_trgm = statements
        .iter()
        .position(|s| s == "CREATE EXTENSION IF NOT EXISTS \"pg_trgm\"")
        .expect("expected pg_trgm CREATE EXTENSION");
    let ext_uuid = statements
        .iter()
        .position(|s| s == "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
        .expect("expected uuid-ossp CREATE EXTENSION");
    let create_table_idx = statements
        .iter()
        .position(|s| s.starts_with("CREATE TABLE"))
        .expect("expected CREATE TABLE");

    assert!(ext_trgm < create_table_idx, "{statements:?}");
    assert!(ext_uuid < create_table_idx, "{statements:?}");
}

#[test]
fn test_sqlite_ignores_extensions() {
    let source = r#"
datasource db {
  provider = "sqlite"
  url      = "file:./test.db"
}

model Doc { id Int @id body String }
"#;
    let ir = common::parse(source).unwrap();
    let generator = DdlGenerator::new(DatabaseProvider::Sqlite);
    let statements = generator.generate_create_tables(&ir).unwrap();

    assert!(
        !statements.iter().any(|s| s.contains("CREATE EXTENSION")),
        "SQLite output must not contain CREATE EXTENSION: {statements:?}"
    );
}
