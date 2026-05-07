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

    write_rust_code(path, &models, None, None, &[], SIMPLE_SCHEMA, false)
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

    write_rust_code(path, &models, None, None, &[], SIMPLE_SCHEMA, true)
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

    write_rust_code(path, &models, enums_code, None, &[], ENUM_SCHEMA, false)
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
        &[],
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
        lib_content.contains("pub use nautilus_connector::ConnectorPoolOptions;"),
        "lib.rs should re-export ConnectorPoolOptions for runtime tuning:\n{lib_content}"
    );
    assert!(
        lib_content.contains("pub use runtime::{Client, EngineMode};"),
        "lib.rs should re-export EngineMode alongside Client:\n{lib_content}"
    );
    assert!(
        lib_content
            .contains("pub use nautilus_connector::{execute_all, execute_one, execute_optional};"),
        "lib.rs should re-export execute helpers for generated fast paths:\n{lib_content}"
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

#[test]
fn test_write_rust_code_runtime_exposes_pool_options_for_embedded_and_direct_paths() {
    let ir = validate(SIMPLE_SCHEMA);
    let models = generate_all_models(&ir, false);
    let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
    let path = tmp.path().to_str().unwrap();

    write_rust_code(path, &models, None, None, &[], SIMPLE_SCHEMA, false)
        .expect("write_rust_code failed");

    let runtime_content = std::fs::read_to_string(tmp.path().join("src").join("runtime.rs"))
        .expect("missing runtime.rs");

    assert!(
        runtime_content.contains("pub async fn postgres_with_options("),
        "runtime.rs should expose a Postgres constructor with pool options:\n{runtime_content}"
    );
    assert!(
        runtime_content.contains("pub async fn mysql_with_options("),
        "runtime.rs should expose a MySQL constructor with pool options:\n{runtime_content}"
    );
    assert!(
        runtime_content.contains("pub async fn sqlite_with_options("),
        "runtime.rs should expose a SQLite constructor with pool options:\n{runtime_content}"
    );
    assert!(
        runtime_content.contains("EngineState::new_with_pool_options("),
        "runtime.rs should propagate pool options into the embedded engine path:\n{runtime_content}"
    );
    assert!(
        runtime_content.contains("pool_options: ConnectorPoolOptions"),
        "runtime.rs should store pool options on the generated client:\n{runtime_content}"
    );
    assert!(
        runtime_content.contains("ConnectorPoolOptions::default()"),
        "runtime.rs should keep using ConnectorPoolOptions so statement-cache tuning stays available on generated clients:\n{runtime_content}"
    );
    assert!(
        runtime_content.contains("pub enum EngineMode"),
        "runtime.rs should expose EngineMode so callers can pick Auto/Always/Never:\n{runtime_content}"
    );
    assert!(
        runtime_content.contains("pub fn with_engine_mode"),
        "runtime.rs should let callers override EngineMode on generated clients:\n{runtime_content}"
    );
    assert!(
        runtime_content.contains("EngineMode::Auto"),
        "runtime.rs should default connector-backed generated clients to EngineMode::Auto:\n{runtime_content}"
    );
    assert!(
        runtime_content.contains("EngineMode::Auto => !args.include.is_empty()"),
        "runtime.rs should keep simple findMany/findFirst/findUnique queries on the direct path in EngineMode::Auto:\n{runtime_content}"
    );
    assert!(
        runtime_content.contains("fn uses_engine_for_simple_crud(self) -> bool")
            && runtime_content.contains("matches!(self, Self::Always)"),
        "runtime.rs should reserve embedded-engine mutations for EngineMode::Always so Auto stays on the direct CRUD path:\n{runtime_content}"
    );
    assert!(
        runtime_content.contains("fn should_try_engine_for_aggregate(&self) -> bool")
            && runtime_content.contains("self.engine_mode.allows_engine()"),
        "runtime.rs should keep aggregate queries on the embedded-engine path whenever engine usage is allowed:\n{runtime_content}"
    );
    assert!(
        runtime_content.contains("handlers::handle_find_many_typed")
            && runtime_content.contains("handlers::handle_find_unique_typed")
            && runtime_content.contains("handlers::handle_create_typed")
            && runtime_content.contains("handlers::handle_count_typed"),
        "runtime.rs should call typed embedded engine handlers directly for Rust in-process paths:\n{runtime_content}"
    );
    assert!(
        runtime_content.contains(
            "static GENERATED_SCHEMA_IR: OnceLock<Arc<nautilus_schema::ir::SchemaIr>>"
        ) && runtime_content.contains("fn generated_schema_ir()")
            && runtime_content.contains("generated_schema_ir()?"),
        "runtime.rs should cache the validated embedded schema so repeated client construction avoids re-running schema validation:\n{runtime_content}"
    );
    assert!(
        !runtime_content.contains("row_from_wire_json"),
        "runtime.rs should no longer round-trip engine rows through JSON objects:\n{runtime_content}"
    );
    assert!(
        !runtime_content.contains("nautilus_protocol::RpcRequest"),
        "runtime.rs should no longer build JSON-RPC envelopes for typed embedded engine calls:\n{runtime_content}"
    );
}

#[test]
fn test_write_rust_code_auto_engine_mode_keeps_direct_and_engine_paths_separate() {
    let ir = validate(RELATION_SCHEMA);
    let models = generate_all_models(&ir, false);
    let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
    let path = tmp.path().to_str().unwrap();

    write_rust_code(path, &models, None, None, &[], RELATION_SCHEMA, false)
        .expect("write_rust_code failed");

    let user_content =
        std::fs::read_to_string(tmp.path().join("src").join("user.rs")).expect("missing user.rs");

    assert!(
        user_content.contains("include queries require the embedded engine path in the generated Rust client"),
        "generated delegates should keep include-heavy reads on the embedded engine path:\n{user_content}"
    );
    assert!(
        user_content.contains("crate::runtime::try_find_unique_via_engine::<_, User>("),
        "generated find_unique delegates should use the dedicated embedded engine fast path:\n{user_content}"
    );
    assert!(
        user_content.contains(
            "count queries require the embedded engine path in the generated Rust client"
        ),
        "generated delegates should keep count() on the embedded engine path:\n{user_content}"
    );
    assert!(
        user_content.contains(
            "groupBy queries require the embedded engine path in the generated Rust client"
        ),
        "generated delegates should keep group_by() on the embedded engine path:\n{user_content}"
    );
}

#[test]
fn test_write_rust_code_uses_execute_fast_paths_in_generated_queries() {
    let ir = validate(SIMPLE_SCHEMA);
    let models = generate_all_models(&ir, false);
    let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
    let path = tmp.path().to_str().unwrap();

    write_rust_code(path, &models, None, None, &[], SIMPLE_SCHEMA, false)
        .expect("write_rust_code failed");

    let user_content =
        std::fs::read_to_string(tmp.path().join("src").join("user.rs")).expect("missing user.rs");

    assert!(
        user_content.contains("crate::execute_all("),
        "generated query builders should use execute_all fast paths when collecting rows:\n{user_content}"
    );
    assert!(
        user_content.contains("crate::execute_one("),
        "generated create builders should use execute_one for single-row mutations:\n{user_content}"
    );
    assert!(
        user_content.contains("crate::execute_optional("),
        "generated single-row read builders should use execute_optional when decoding optional rows:\n{user_content}"
    );
    assert!(
        user_content.contains("row.into_columns_iter().enumerate()"),
        "generated model decoders should consume projected rows positionally on the hot path:\n{user_content}"
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
        &[],
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

    write_rust_code(path, &models, None, None, &[], RELATION_SCHEMA, true)
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

    write_python_code(
        path,
        &models,
        enums_code,
        None,
        &[],
        client_code,
        &runtime_files,
    )
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

    write_python_code(path, &models, enums_code, None, &[], None, &runtime_files)
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

    write_python_code(path, &models, None, None, &[], None, &runtime_files)
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
