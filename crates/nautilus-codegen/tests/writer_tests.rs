//! Integration tests for `writer.rs` — verifies that generated file trees are
//! created correctly on disk using a temporary directory.

use nautilus_codegen::{
    composite_type_gen::generate_all_composite_types,
    enum_gen::generate_all_enums,
    generator::generate_all_models,
    java::generate_java_client,
    python::{
        generate_all_python_models, generate_python_client, generate_python_enums,
        python_runtime_files,
    },
    writer::{write_java_code, write_python_code, write_rust_code},
};
use nautilus_schema::validate_schema_source;

fn validate(source: &str) -> nautilus_schema::ir::SchemaIr {
    validate_schema_source(source)
        .expect("validation failed")
        .ir
}

const SIMPLE_SCHEMA: &str = r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#;

const ENUM_SCHEMA: &str = r#"
enum Status {
  ACTIVE
  INACTIVE
}

model User {
  id     Int    @id @default(autoincrement())
  status Status
}
"#;

const RELATION_SCHEMA: &str = r#"
model User {
  id    Int    @id @default(autoincrement())
  email String @unique @map("user_email")
  posts Post[]
}

model Post {
  id     Int    @id @default(autoincrement())
  title  String
  userId Int    @map("user_id")
  user   User   @relation(fields: [userId], references: [id])
}
"#;

const COMPOSITE_ENUM_SCHEMA: &str = r#"
type Address {
  street String
  city   String
}

enum Status {
  ACTIVE
  INACTIVE
}

model User {
  id      Int     @id @default(autoincrement())
  address Address?
  status  Status
}
"#;

const JAVA_SCHEMA: &str = r#"
generator client {
  provider    = "nautilus-client-java"
  output      = "./generated-java"
  package     = "com.acme.db"
  group_id    = "com.acme"
  artifact_id = "db-client"
}

enum Role {
  ADMIN
  MEMBER
}

model User {
  id   Int    @id @default(autoincrement())
  name String
  role Role
}
"#;

/// Non-standalone mode creates src/lib.rs and src/<model>.rs but no Cargo.toml.
#[test]
fn test_write_rust_code_creates_model_and_lib_files() {
    let ir = validate(SIMPLE_SCHEMA);
    let models = generate_all_models(&ir, false);
    let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
    let path = tmp.path().to_str().unwrap();

    write_rust_code(path, &models, None, None, SIMPLE_SCHEMA, false)
        .expect("write_rust_code failed");

    assert!(
        tmp.path().join("src").join("lib.rs").exists(),
        "src/lib.rs not created"
    );
    assert!(
        tmp.path().join("src").join("user.rs").exists(),
        "src/user.rs not created"
    );
    assert!(
        tmp.path().join("src").join("runtime.rs").exists(),
        "src/runtime.rs not created"
    );
    assert!(
        !tmp.path().join("Cargo.toml").exists(),
        "Cargo.toml should not be created in non-standalone mode"
    );
}

/// Standalone mode additionally creates a Cargo.toml at the output root.
#[test]
fn test_write_rust_code_standalone_creates_cargo_toml() {
    let ir = validate(SIMPLE_SCHEMA);
    let models = generate_all_models(&ir, false);
    let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
    let path = tmp.path().to_str().unwrap();

    write_rust_code(path, &models, None, None, SIMPLE_SCHEMA, true)
        .expect("write_rust_code (standalone) failed");

    assert!(
        tmp.path().join("Cargo.toml").exists(),
        "Cargo.toml not created in standalone mode"
    );
    let cargo_content = std::fs::read_to_string(tmp.path().join("Cargo.toml")).unwrap();
    assert!(
        cargo_content.contains("[package]"),
        "Cargo.toml missing [package] section:\n{cargo_content}"
    );
}

/// When enums are present an enums.rs file is written to src/.
#[test]
fn test_write_rust_code_writes_enums_file() {
    let ir = validate(ENUM_SCHEMA);
    let models = generate_all_models(&ir, false);
    let enums_code = Some(generate_all_enums(&ir.enums));
    let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
    let path = tmp.path().to_str().unwrap();

    write_rust_code(path, &models, enums_code, None, ENUM_SCHEMA, false)
        .expect("write_rust_code failed");

    assert!(
        tmp.path().join("src").join("enums.rs").exists(),
        "src/enums.rs not created"
    );
}

#[test]
fn test_write_rust_code_lib_rs_contains_template_exports() {
    let ir = validate(COMPOSITE_ENUM_SCHEMA);
    let models = generate_all_models(&ir, false);
    let enums_code = Some(generate_all_enums(&ir.enums));
    let composite_types_code = generate_all_composite_types(&ir);
    let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
    let path = tmp.path().to_str().unwrap();

    write_rust_code(
        path,
        &models,
        enums_code,
        composite_types_code,
        COMPOSITE_ENUM_SCHEMA,
        false,
    )
    .expect("write_rust_code failed");

    let lib_content =
        std::fs::read_to_string(tmp.path().join("src").join("lib.rs")).expect("missing lib.rs");

    assert!(
        lib_content.contains("pub(crate) const SCHEMA_SOURCE: &str = "),
        "lib.rs should contain the embedded schema source:\n{lib_content}"
    );
    assert!(
        lib_content.contains("pub mod types;"),
        "lib.rs should declare the composite types module:\n{lib_content}"
    );
    assert!(
        lib_content.contains("pub mod enums;"),
        "lib.rs should declare the enums module:\n{lib_content}"
    );
    assert!(
        lib_content.contains("pub mod user;"),
        "lib.rs should declare model modules:\n{lib_content}"
    );
    assert!(
        lib_content.contains("pub use types::*;"),
        "lib.rs should re-export composite types:\n{lib_content}"
    );
    assert!(
        lib_content.contains("pub use enums::*;"),
        "lib.rs should re-export enums:\n{lib_content}"
    );
    assert!(
        lib_content.contains("pub use user::*;"),
        "lib.rs should re-export models:\n{lib_content}"
    );

    let types_idx = lib_content
        .find("pub mod types;")
        .expect("missing types module declaration");
    let enums_idx = lib_content
        .find("pub mod enums;")
        .expect("missing enums module declaration");
    let user_idx = lib_content
        .find("pub mod user;")
        .expect("missing user module declaration");
    assert!(
        types_idx < enums_idx && enums_idx < user_idx,
        "lib.rs module declarations should be ordered types -> enums -> models:\n{lib_content}"
    );
}

/// Multiple models each get their own snake_case .rs file.
#[test]
fn test_write_rust_code_multiple_models() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
model Post {
  id    Int    @id @default(autoincrement())
  title String
}
"#,
    );
    let models = generate_all_models(&ir, false);
    let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
    let path = tmp.path().to_str().unwrap();

    write_rust_code(
        path,
        &models,
        None,
        None,
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
model Post {
  id    Int    @id @default(autoincrement())
  title String
}
"#,
        false,
    )
    .expect("write_rust_code failed");

    assert!(
        tmp.path().join("src").join("user.rs").exists(),
        "src/user.rs not created"
    );
    assert!(
        tmp.path().join("src").join("post.rs").exists(),
        "src/post.rs not created"
    );
}

/// A generated standalone Rust client with relations should compile as a crate.
#[test]
fn test_write_rust_code_standalone_generated_client_compiles() {
    let ir = validate(RELATION_SCHEMA);
    let models = generate_all_models(&ir, false);
    let workspace_root = std::env::current_dir().expect("failed to get current directory");
    let tmp = tempfile::tempdir_in(workspace_root).expect("failed to create temp dir");
    let path = tmp.path().to_str().unwrap();

    write_rust_code(path, &models, None, None, RELATION_SCHEMA, true)
        .expect("write_rust_code failed");

    let status = std::process::Command::new("cargo")
        .args(["check", "--quiet", "--offline", "--manifest-path"])
        .arg(tmp.path().join("Cargo.toml"))
        .status()
        .expect("failed to run cargo check on generated client");

    assert!(
        status.success(),
        "cargo check failed for generated Rust client"
    );
}

/// A generated async Rust client can execute count/group_by via the embedded engine.
#[test]
fn test_write_rust_code_generated_client_runs_count_and_group_by() {
    let schema = r#"
datasource db {
  provider = "sqlite"
  url      = "sqlite::memory:"
}

enum Role {
  ADMIN
  MEMBER
}

model User {
  id          Int    @id @default(autoincrement()) @map("user_id")
  displayName String @map("display_name")
  role        Role
  views       Int

  @@map("users")
}
"#;
    let ir = validate(schema);
    let models = generate_all_models(&ir, true);
    let enums_code = Some(generate_all_enums(&ir.enums));
    let workspace_root = std::env::current_dir().expect("failed to get current directory");
    let tmp = tempfile::tempdir_in(workspace_root).expect("failed to create temp dir");
    let path = tmp.path().to_str().unwrap();

    write_rust_code(path, &models, enums_code, None, schema, true).expect("write_rust_code failed");

    let tests_dir = tmp.path().join("tests");
    std::fs::create_dir_all(&tests_dir).expect("failed to create generated tests dir");
    std::fs::write(
        tests_dir.join("aggregates.rs"),
        r#"
use nautilus_client::{
    Client, Role, TransactionOptions, User, UserCountAggregateInput, UserCountArgs,
    UserCreateInput, UserGroupByArgs, UserGroupByOrderBy, UserMinAggregateInput,
    UserScalarField, UserSortOrder, UserSumAggregateInput,
};

fn core_to_connector(err: nautilus_core::Error) -> nautilus_connector::ConnectorError {
    nautilus_connector::ConnectorError::database_msg(err.to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn generated_client_supports_count_and_group_by() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = std::env::temp_dir().join(format!("nautilus-generated-aggregates-{}.db", uuid::Uuid::new_v4()));
    std::fs::File::create(&db_path)?;
    let url = format!("sqlite:{}", db_path.to_string_lossy().replace('\\', "/"));

    let db = Client::sqlite(&url).await?;
    let users = User::nautilus(&db);

    users
        .raw_query(
            "CREATE TABLE users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                display_name TEXT NOT NULL,
                role TEXT NOT NULL,
                views INTEGER NOT NULL
            )",
        )
        .await?;

    users
        .create(UserCreateInput {
            display_name: Some("Alice".to_string()),
            role: Some(Role::ADMIN),
            views: Some(12),
            ..Default::default()
        })
        .await?;
    users
        .create(UserCreateInput {
            display_name: Some("Bob".to_string()),
            role: Some(Role::MEMBER),
            views: Some(7),
            ..Default::default()
        })
        .await?;

    let admin_count = users
        .count(UserCountArgs {
            where_: Some(User::role().eq(Role::ADMIN)),
            ..Default::default()
        })
        .await?;
    assert_eq!(admin_count, 1);

    let grouped = users
        .group_by(UserGroupByArgs {
            by: vec![UserScalarField::Role],
            count: Some(UserCountAggregateInput {
                _all: true,
                display_name: true,
                ..Default::default()
            }),
            sum: Some(UserSumAggregateInput {
                views: true,
                ..Default::default()
            }),
            min: Some(UserMinAggregateInput {
                display_name: true,
                ..Default::default()
            }),
            order_by: vec![UserGroupByOrderBy::Field {
                field: UserScalarField::Role,
                direction: UserSortOrder::Asc,
            }],
            ..Default::default()
        })
        .await?;

    assert_eq!(grouped.len(), 2);
    let admin_group = grouped
        .iter()
        .find(|row| row.role == Some(Role::ADMIN))
        .expect("missing ADMIN group");
    assert_eq!(admin_group._count.as_ref().and_then(|count| count._all), Some(1));
    assert_eq!(
        admin_group
            ._count
            .as_ref()
            .and_then(|count| count.display_name),
        Some(1)
    );
    assert_eq!(admin_group._sum.as_ref().and_then(|sum| sum.views), Some(12));
    assert_eq!(
        admin_group
            ._min
            .as_ref()
            .and_then(|min| min.display_name.clone()),
        Some("Alice".to_string())
    );

    db.transaction(TransactionOptions::default(), |tx| async move {
        let tx_users = User::nautilus(&tx);
        tx_users
            .create(UserCreateInput {
                display_name: Some("Cara".to_string()),
                role: Some(Role::ADMIN),
                views: Some(5),
                ..Default::default()
            })
            .await
            .map_err(core_to_connector)?;

        let tx_count = tx_users
            .count(UserCountArgs {
                where_: Some(User::role().eq(Role::ADMIN)),
                ..Default::default()
            })
            .await
            .map_err(core_to_connector)?;
        assert_eq!(tx_count, 2);

        let tx_groups = tx_users
            .group_by(UserGroupByArgs {
                by: vec![UserScalarField::Role],
                count: Some(UserCountAggregateInput {
                    _all: true,
                    display_name: true,
                    ..Default::default()
                }),
                sum: Some(UserSumAggregateInput {
                    views: true,
                    ..Default::default()
                }),
                order_by: vec![UserGroupByOrderBy::Field {
                    field: UserScalarField::Role,
                    direction: UserSortOrder::Asc,
                }],
                ..Default::default()
            })
            .await
            .map_err(core_to_connector)?;

        let admin_group = tx_groups
            .iter()
            .find(|row| row.role == Some(Role::ADMIN))
            .expect("missing ADMIN group inside transaction");
        assert_eq!(admin_group._count.as_ref().and_then(|count| count._all), Some(2));
        assert_eq!(
            admin_group
                ._count
                .as_ref()
                .and_then(|count| count.display_name),
            Some(2)
        );
        assert_eq!(admin_group._sum.as_ref().and_then(|sum| sum.views), Some(17));

        Ok(())
    })
    .await?;

    let committed_count = users
        .count(UserCountArgs {
            where_: Some(User::role().eq(Role::ADMIN)),
            ..Default::default()
        })
        .await?;
    assert_eq!(committed_count, 2);

    Ok(())
}
"#,
    )
    .expect("failed to write generated aggregate smoke test");

    let status = std::process::Command::new("cargo")
        .args(["test", "--quiet", "--offline", "--manifest-path"])
        .arg(tmp.path().join("Cargo.toml"))
        .status()
        .expect("failed to run cargo test on generated client");

    assert!(
        status.success(),
        "cargo test failed for generated Rust client aggregate smoke test"
    );
}

/// Verifies the expected Python package directory structure is created.
#[test]
fn test_write_python_code_creates_package_structure() {
    let ir = validate(SIMPLE_SCHEMA);
    let models = generate_all_python_models(&ir, false, 0);
    let enums_code = None;
    let client_code = Some(generate_python_client(&ir.models, "schema.nautilus", false));
    let runtime_files = python_runtime_files();
    let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
    let path = tmp.path().to_str().unwrap();

    write_python_code(path, &models, enums_code, None, client_code, &runtime_files)
        .expect("write_python_code failed");

    let root = tmp.path();
    assert!(root.join("__init__.py").exists(), "__init__.py missing");
    assert!(root.join("client.py").exists(), "client.py missing");
    assert!(root.join("py.typed").exists(), "py.typed missing");
    assert!(
        root.join("transaction.py").exists(),
        "transaction.py missing"
    );
    assert!(
        root.join("models").join("__init__.py").exists(),
        "models/__init__.py missing"
    );
    assert!(
        root.join("models").join("user.py").exists(),
        "models/user.py missing"
    );
    assert!(
        root.join("enums").join("__init__.py").exists(),
        "enums/__init__.py missing"
    );
    assert!(
        root.join("errors").join("__init__.py").exists(),
        "errors/__init__.py missing"
    );
    assert!(
        root.join("_internal").join("__init__.py").exists(),
        "_internal/__init__.py missing"
    );
}

/// When enums are present an enums.py file is written under enums/.
#[test]
fn test_write_python_code_with_enums() {
    let ir = validate(ENUM_SCHEMA);
    let models = generate_all_python_models(&ir, false, 0);
    let enums_code = Some(generate_python_enums(&ir.enums));
    let runtime_files = python_runtime_files();
    let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
    let path = tmp.path().to_str().unwrap();

    write_python_code(path, &models, enums_code, None, None, &runtime_files)
        .expect("write_python_code failed");

    assert!(
        tmp.path().join("enums").join("enums.py").exists(),
        "enums/enums.py missing"
    );
}

/// client.py is only created when a client_code is supplied (Some).
#[test]
fn test_write_python_code_without_client_no_client_py() {
    let ir = validate(SIMPLE_SCHEMA);
    let models = generate_all_python_models(&ir, false, 0);
    let runtime_files = python_runtime_files();
    let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
    let path = tmp.path().to_str().unwrap();

    write_python_code(path, &models, None, None, None, &runtime_files)
        .expect("write_python_code failed");

    assert!(
        !tmp.path().join("client.py").exists(),
        "client.py should not be created when client_code is None"
    );
}

#[test]
fn test_write_java_code_creates_maven_module_structure() {
    let ir = validate(JAVA_SCHEMA);
    let files =
        generate_java_client(&ir, "schema.nautilus", false).expect("generate_java_client failed");
    let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
    let path = tmp.path().to_str().unwrap();

    write_java_code(path, &files).expect("write_java_code failed");

    let root = tmp.path();
    let package_root = root.join("src/main/java/com/acme/db");

    assert!(root.join("pom.xml").exists(), "pom.xml missing");
    assert!(
        package_root.join("client").join("Nautilus.java").exists(),
        "client/Nautilus.java missing"
    );
    assert!(
        package_root
            .join("client")
            .join("UserDelegate.java")
            .exists(),
        "client/UserDelegate.java missing"
    );
    assert!(
        package_root.join("model").join("User.java").exists(),
        "model/User.java missing"
    );
    assert!(
        package_root.join("enums").join("Role.java").exists(),
        "enums/Role.java missing"
    );
    assert!(
        package_root.join("dsl").join("UserDsl.java").exists(),
        "dsl/UserDsl.java missing"
    );
    assert!(
        package_root
            .join("internal")
            .join("GlobalNautilusRegistry.java")
            .exists(),
        "internal/GlobalNautilusRegistry.java missing"
    );

    let pom = std::fs::read_to_string(root.join("pom.xml")).expect("failed to read pom.xml");
    assert!(
        pom.contains("<maven.compiler.release>21</maven.compiler.release>"),
        "pom.xml should target Java 21:\n{pom}"
    );
    assert!(
        pom.contains("jackson-databind"),
        "pom.xml should include Jackson databind:\n{pom}"
    );
}
