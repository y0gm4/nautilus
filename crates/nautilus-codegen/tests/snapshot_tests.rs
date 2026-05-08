//! Snapshot tests for the code generator: parse a schema, generate code, and
//! optionally assert the full rendered output against local-only snapshots.
//!
//! Snapshot baselines live in `tests/snapshots/`, which is gitignored on
//! purpose. Regular test runs ignore those local `.snap` files so stale
//! baselines do not break unrelated codegen work. To force snapshot assertions
//! or generate fresh local baselines, run with `NAUTILUS_LOCAL_SNAPSHOTS=1`
//! (typically alongside `INSTA_UPDATE=always`). To skip snapshot assertions
//! explicitly even when that env var is set, run with
//! `NAUTILUS_SKIP_LOCAL_SNAPSHOTS=1`.

use std::sync::OnceLock;

use nautilus_codegen::{
    enum_gen::generate_all_enums,
    extension_types::{
        generate_java_extension_files, generate_js_extension_files,
        generate_python_extension_files, generate_rust_extension_files, ExtensionRegistry,
    },
    generator::generate_all_models,
    java::generate_java_client,
    js::{generate_all_js_models, generate_js_client, js_runtime_files},
    python::{
        generate_all_python_models, generate_python_client, generate_python_enums,
        python_runtime_files,
    },
};
use nautilus_schema::validate_schema_source;

fn local_snapshots_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();

    *ENABLED.get_or_init(|| {
        if std::env::var_os("NAUTILUS_SKIP_LOCAL_SNAPSHOTS").is_some() {
            return false;
        }

        if std::env::var_os("NAUTILUS_LOCAL_SNAPSHOTS").is_some() {
            return true;
        }

        false
    })
}

macro_rules! assert_local_snapshot {
    ($value:expr $(,)?) => {{
        let snapshot_value = &$value;
        assert!(
            !snapshot_value.is_empty(),
            "generated snapshot content should not be empty"
        );
        if local_snapshots_enabled() {
            insta::assert_snapshot!(snapshot_value);
        }
    }};
    ($name:expr, $value:expr $(,)?) => {{
        let snapshot_value = &$value;
        assert!(
            !snapshot_value.is_empty(),
            "generated snapshot content should not be empty"
        );
        if local_snapshots_enabled() {
            insta::assert_snapshot!($name, snapshot_value);
        }
    }};
}

fn validate(source: &str) -> nautilus_schema::ir::SchemaIr {
    validate_schema_source(source)
        .expect("validation failed")
        .ir
}

fn generated_java_file<'a>(files: &'a [(String, String)], suffix: &str) -> &'a str {
    files
        .iter()
        .find(|(path, _)| path.ends_with(suffix))
        .map(|(_, code)| code.as_str())
        .unwrap_or_else(|| panic!("missing generated Java file ending with '{suffix}'"))
}

fn generated_python_file<'a>(files: &'a [(String, String)], file_name: &str) -> &'a str {
    files
        .iter()
        .find(|(path, _)| path == file_name)
        .map(|(_, code)| code.as_str())
        .unwrap_or_else(|| panic!("missing generated Python file '{file_name}'"))
}

fn generated_named_file<'a>(files: &'a [(String, String)], file_name: &str) -> &'a str {
    files
        .iter()
        .find(|(path, _)| path == file_name)
        .map(|(_, code)| code.as_str())
        .unwrap_or_else(|| panic!("missing generated file '{file_name}'"))
}

#[test]
fn test_rust_struct_is_generated() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let models = generate_all_models(&ir, false);
    let code = models.get("User").expect("User model missing");
    assert_local_snapshot!(code);
}

#[test]
fn test_rust_optional_field_is_option() {
    let ir = validate(
        r#"
model Post {
  id      Int     @id @default(autoincrement())
  content String?
}
"#,
    );
    let models = generate_all_models(&ir, false);
    let code = models.get("Post").expect("Post model missing");
    assert_local_snapshot!(code);
}

#[test]
fn test_rust_generates_find_many_builder() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let models = generate_all_models(&ir, false);
    let code = models.get("User").expect("User model missing");
    assert!(
        code.contains("FindMany"),
        "expected FindMany builder:\n{code}"
    );
    assert_local_snapshot!(code);
}

#[test]
fn test_rust_generates_count_and_group_by_api() {
    let ir = validate(
        r#"
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
"#,
    );
    let models = generate_all_models(&ir, false);
    let code = models.get("User").expect("User model missing");

    assert!(
        code.contains("pub struct UserCountArgs"),
        "expected generated Rust code to expose count args:\n{code}"
    );
    assert!(
        code.contains("pub fn count("),
        "expected generated Rust code to expose count():\n{code}"
    );
    assert!(
        code.contains("pub fn group_by("),
        "expected generated Rust code to expose group_by():\n{code}"
    );
    assert!(
        code.contains("pub enum UserScalarField"),
        "expected generated Rust code to expose scalar field enums for group_by():\n{code}"
    );
    assert!(
        code.contains("Self::DisplayName => \"displayName\""),
        "expected mapped fields to serialize through logical names in aggregate APIs:\n{code}"
    );
    assert!(
        code.contains("pub struct UserGroupByOutput"),
        "expected generated Rust code to expose a typed group_by output:\n{code}"
    );
}

#[test]
fn test_rust_generates_create_input() {
    let ir = validate(
        r#"
model User {
  id    Int    @id @default(autoincrement())
  email String @unique
}
"#,
    );
    let models = generate_all_models(&ir, false);
    let code = models.get("User").expect("User model missing");
    assert_local_snapshot!(code);
}

#[test]
fn test_rust_generated_query_builders_use_static_column_markers() {
    let ir = validate(
        r#"
model User {
  id    Int    @id @default(autoincrement())
  email String @unique
  name  String?

  @@map("users")
}
"#,
    );
    let models = generate_all_models(&ir, false);
    let code = models.get("User").expect("User model missing");

    assert!(
        code.contains("ColumnMarker::from_static(\"users\", \"email\")"),
        "expected generated Rust code to use borrowed column metadata for known columns:\n{code}"
    );
    assert!(
        code.contains("ColumnMarker::from_static(\"users\", \"id\")"),
        "expected generated Rust code to reuse borrowed PK metadata in returning/select paths:\n{code}"
    );
}

#[test]
fn test_rust_enum_generation() {
    let ir = validate(
        r#"
enum Status {
  ACTIVE
  INACTIVE
  PENDING
}

model User {
  id     Int    @id @default(autoincrement())
  status Status
}
"#,
    );
    let enums_code = generate_all_enums(&ir.enums);
    assert_local_snapshot!(enums_code);
}

#[test]
fn test_rust_multiple_models_generated() {
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
    assert!(models.contains_key("User"), "expected User model");
    assert!(models.contains_key("Post"), "expected Post model");
}

#[test]
fn test_rust_async_generates_async_fns() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let sync_models = generate_all_models(&ir, false);
    let async_models = generate_all_models(&ir, true);
    let sync_code = sync_models.get("User").unwrap();
    let async_code = async_models.get("User").unwrap();
    assert!(
        async_code.contains("async"),
        "expected async in async mode:\n{async_code}"
    );
    assert_ne!(sync_code, async_code, "sync and async should differ");
    assert_local_snapshot!("rust_user_async", async_code);
}

#[test]
fn test_rust_from_row_impl_generated() {
    let ir = validate(
        r#"
model Product {
  id    Int    @id @default(autoincrement())
  name  String
  price Float
}
"#,
    );
    let models = generate_all_models(&ir, false);
    let code = models.get("Product").expect("Product missing");
    assert_local_snapshot!(code);
}

#[test]
fn test_rust_model_generates_schema_aware_read_hints() {
    let ir = validate(
        r#"
model User {
  id         Int           @id @default(autoincrement())
  externalId Uuid
  price      Decimal(10, 2)
  profile    Json
  tags       String[]      @store(json)
}
"#,
    );
    let models = generate_all_models(&ir, false);
    let code = models.get("User").expect("User missing");

    assert!(
        code.contains("normalize_value_with_hint"),
        "expected generated Rust model to normalize projected values inline during decode:\n{code}"
    );
    assert!(
        code.contains("FromValue::from_value_owned"),
        "expected generated Rust model to decode normalized values without extra cloning:\n{code}"
    );
    assert!(
        code.contains("Some(crate::ValueHint::Uuid)"),
        "expected generated Rust model to emit a UUID read hint:\n{code}"
    );
    assert!(
        code.contains("Some(crate::ValueHint::Decimal)"),
        "expected generated Rust model to emit a Decimal read hint:\n{code}"
    );
    assert!(
        code.contains("Some(crate::ValueHint::Json)"),
        "expected generated Rust model to emit JSON read hints:\n{code}"
    );
}

/// Exercises RelationContext: a model with both a to-one and a to-many relation.
#[test]
fn test_rust_model_with_relation() {
    let ir = validate(
        r#"
model User {
  id    Int    @id @default(autoincrement())
  name  String
  posts Post[]
}

model Post {
  id       Int    @id @default(autoincrement())
  title    String
  authorId Int
  author   User   @relation(fields: [authorId], references: [id])
}
"#,
    );
    let models = generate_all_models(&ir, false);
    let user_code = models.get("User").expect("User missing");
    let post_code = models.get("Post").expect("Post missing");
    assert_local_snapshot!("rust_user_with_posts_relation", user_code);
    assert_local_snapshot!("rust_post_with_author_relation", post_code);
}

#[test]
fn test_rust_async_delegate_exposes_stream_many() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let async_models = generate_all_models(&ir, true);
    let async_code = async_models.get("User").expect("User missing");

    assert!(
        async_code.contains("pub fn stream_many("),
        "expected async delegate to expose stream_many:\n{async_code}"
    );
    assert!(
        async_code.contains("execute_owned(sql)"),
        "expected stream_many to drive the executor's owned-stream path:\n{async_code}"
    );
    assert!(
        async_code.contains("stream_many does not support backward pagination"),
        "expected stream_many to reject backward pagination explicitly:\n{async_code}"
    );

    let sync_models = generate_all_models(&ir, false);
    let sync_code = sync_models.get("User").expect("User missing");
    assert!(
        !sync_code.contains("pub fn stream_many("),
        "stream_many should not be emitted for sync clients (the runtime would have to block on iteration); got:\n{sync_code}"
    );
}

#[test]
fn test_rust_relation_include_routes_through_engine_path() {
    let ir = validate(
        r#"
model User {
  id    Int    @id @default(autoincrement())
  posts Post[]
}

model Post {
  id       Int    @id @default(autoincrement())
  title    String
  authorId Int
  author   User   @relation(fields: [authorId], references: [id])
}
"#,
    );
    let models = generate_all_models(&ir, false);
    let user_code = models.get("User").expect("User missing");

    assert!(
        user_code.contains("crate::runtime::try_find_many_via_engine::<_, User>("),
        "expected relation include reads to route through the embedded engine path:\n{user_code}"
    );
    assert!(
        user_code.contains("if !args.include.is_empty() {"),
        "expected generated delegate to treat include queries as engine-only in the local fallback:\n{user_code}"
    );
    assert!(
        user_code.contains("include queries require the embedded engine path in the generated Rust client"),
        "expected the fallback path to explain why include queries stay on the engine path:\n{user_code}"
    );
}

#[test]
fn test_rust_delete_uses_single_record_fast_path_for_unique_filters() {
    let ir = validate(
        r#"
model User {
  id       Int    @id @default(autoincrement())
  email    String @unique
  tenantId Int
  slug     String

  @@unique([tenantId, slug])
}
"#,
    );
    let models = generate_all_models(&ir, false);
    let user_code = models.get("User").expect("User missing");

    assert!(
        user_code.contains("fn is_single_record_filter(filter: &nautilus_core::Expr) -> bool"),
        "expected generated Rust code to recognize single-record filters:\n{user_code}"
    );
    assert!(
        user_code.contains("&[\"tenant_id\", \"slug\"]"),
        "expected composite unique constraints to participate in the delete fast path:\n{user_code}"
    );
    assert!(
        user_code.contains(
            "if self.client.dialect().supports_returning() && is_single_record_filter(&filter)"
        ),
        "expected delete() to use the single-query fast path for unique filters:\n{user_code}"
    );
}

#[test]
fn test_rust_upsert_attempts_update_before_find_on_returning_backends() {
    let ir = validate(
        r#"
model User {
  id    Int    @id @default(autoincrement())
  email String @unique
  name  String
}
"#,
    );
    let models = generate_all_models(&ir, false);
    let user_code = models.get("User").expect("User missing");

    let update_idx = user_code
        .find("if self.client.dialect().supports_returning() && has_update_assignments {")
        .expect("missing upsert update-first fast path");
    let find_idx = user_code
        .find("let existing = self.find_first(")
        .expect("missing upsert fallback lookup");

    assert!(
        update_idx < find_idx,
        "expected upsert() to try the update path before the read fallback:\n{user_code}"
    );
    assert!(
        user_code.contains("let has_update_assignments = args.update.has_assignments();"),
        "expected generated upsert() to reuse update-input assignment detection:\n{user_code}"
    );
}

#[test]
fn test_rust_named_inverse_relations_use_matching_relation_name() {
    let ir = validate(
        r#"
model User {
  id            Int    @id @default(autoincrement())
  authoredPosts Post[] @relation(name: "AuthoredPosts")
  reviewedPosts Post[] @relation(name: "ReviewedPosts")
}

model Post {
  id         Int    @id @default(autoincrement())
  title      String
  authorId   Int
  reviewerId Int
  author     User   @relation(name: "AuthoredPosts", fields: [authorId], references: [id])
  reviewer   User   @relation(name: "ReviewedPosts", fields: [reviewerId], references: [id])
}
"#,
    );
    let models = generate_all_models(&ir, false);
    let user_code = models.get("User").expect("User missing");

    assert!(
        user_code.contains(
            "nautilus_core::Expr::relation_some(\n            \"reviewed_posts\",\n            \"User\",\n            \"Post\",\n            \"reviewerId\",\n            \"id\","
        ),
        "expected reviewed_posts inverse relation helpers to target reviewer_id instead of another FK:\n{user_code}"
    );
}

#[test]
fn test_python_class_is_generated() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let models = generate_all_python_models(&ir, false, 0);
    let (_, code) = models
        .iter()
        .find(|(name, _)| name == "user.py")
        .expect("user model missing");
    assert_local_snapshot!(code);
}

#[test]
fn test_python_optional_field_is_optional_type() {
    let ir = validate(
        r#"
model Post {
  id      Int     @id @default(autoincrement())
  content String?
}
"#,
    );
    let models = generate_all_python_models(&ir, false, 0);
    let (_, code) = models
        .iter()
        .find(|(name, _)| name == "post.py")
        .expect("post missing");
    assert_local_snapshot!(code);
}

#[test]
fn test_python_enum_class() {
    let ir = validate(
        r#"
enum Role {
  USER
  ADMIN
}

model User {
  id   Int  @id @default(autoincrement())
  role Role
}
"#,
    );
    let enums_code = generate_python_enums(&ir.enums);
    assert_local_snapshot!(enums_code);
}

#[test]
fn test_python_async_generates_async_defs() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let sync_models = generate_all_python_models(&ir, false, 0);
    let async_models = generate_all_python_models(&ir, true, 0);
    let (_, sync_code) = sync_models.iter().find(|(n, _)| n == "user.py").unwrap();
    let (_, async_code) = async_models.iter().find(|(n, _)| n == "user.py").unwrap();
    assert!(
        async_code.contains("async def"),
        "expected async def:\n{async_code}"
    );
    assert_ne!(sync_code, async_code, "sync and async should differ");
    assert_local_snapshot!("python_user_async", async_code);
}

#[test]
fn test_python_multiple_models_generated() {
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
    let models = generate_all_python_models(&ir, false, 0);
    let names: Vec<&str> = models.iter().map(|(n, _)| n.as_str()).collect();
    assert!(names.contains(&"user.py"), "expected user in {names:?}");
    assert!(names.contains(&"post.py"), "expected post in {names:?}");
}

/// Exercises `generate_python_client`: verifies the output contains the top-level
/// `NautilusClient` class and per-model delegate attributes.
#[test]
fn test_python_client_generation() {
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
    let client_sync = generate_python_client(&ir.models, "schema.nautilus", false);
    let client_async = generate_python_client(&ir.models, "schema.nautilus", true);
    assert!(
        client_sync.contains("NautilusClient"),
        "expected NautilusClient:\n{client_sync}"
    );
    assert!(
        client_async.contains("async def") || client_async.contains("async"),
        "expected async keyword in async client:\n{client_async}"
    );
    assert_ne!(
        client_sync, client_async,
        "sync and async clients should differ"
    );
    assert_local_snapshot!("python_client_sync", &client_sync);
}

#[test]
fn test_js_client_exposes_batch_transactions_and_runtime_stays_on_protocol_v1() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let (client_js, client_dts) = generate_js_client(&ir.models, "schema.nautilus");
    let runtime = js_runtime_files();
    let client_runtime = runtime
        .iter()
        .find(|(name, _)| name == "_client.js")
        .expect("missing JS runtime client")
        .1
        .as_str();
    let protocol_runtime = runtime
        .iter()
        .find(|(name, _)| name == "_protocol.js")
        .expect("missing JS runtime protocol")
        .1
        .as_str();
    let error_runtime = runtime
        .iter()
        .find(|(name, _)| name == "_errors.js")
        .expect("missing JS runtime errors")
        .1
        .as_str();
    let tx_runtime = runtime
        .iter()
        .find(|(name, _)| name == "_transaction.js")
        .expect("missing JS runtime transaction")
        .1
        .as_str();

    assert!(
        client_js.contains("async $transactionBatch(operations, options)"),
        "expected generated JS client to expose $transactionBatch():\n{client_js}"
    );
    assert!(
        client_dts.contains("$transactionBatch("),
        "expected generated JS declarations to expose $transactionBatch():\n{client_dts}"
    );
    assert!(
        protocol_runtime.contains("export const PROTOCOL_VERSION = 1;")
            && client_runtime.contains("protocolVersion: PROTOCOL_VERSION")
            && client_runtime.contains("client expects ${PROTOCOL_VERSION}")
            && client_runtime.contains("transaction.batch")
            && client_runtime.contains("async *_streamRpc(")
            && client_runtime.contains("method: 'request.cancel'"),
        "expected JS runtime client to reuse the shared protocol version constant and expose transaction.batch:\n{client_runtime}\n\nProtocol:\n{protocol_runtime}"
    );
    assert!(
        error_runtime.contains("this.data = details?.data"),
        "expected JS runtime errors to retain error.data from the engine:\n{error_runtime}"
    );
    assert!(
        !tx_runtime.contains("snapshot"),
        "expected JS runtime isolation levels to match the protocol exactly:\n{tx_runtime}"
    );
}

#[test]
fn test_python_runtime_stays_on_protocol_v1_and_preserves_error_data() {
    let runtime = python_runtime_files();
    let client_runtime = runtime
        .iter()
        .find(|(name, _)| name == "_client.py")
        .expect("missing Python runtime client")
        .1
        .as_str();
    let protocol_runtime = runtime
        .iter()
        .find(|(name, _)| name == "_protocol.py")
        .expect("missing Python runtime protocol")
        .1
        .as_str();
    let error_runtime = runtime
        .iter()
        .find(|(name, _)| name == "_errors.py")
        .expect("missing Python runtime errors")
        .1
        .as_str();
    let tx_runtime = runtime
        .iter()
        .find(|(name, _)| name == "_transaction.py")
        .expect("missing Python runtime transaction")
        .1
        .as_str();

    assert!(
        protocol_runtime.contains("PROTOCOL_VERSION = 1")
            && client_runtime.contains("\"protocolVersion\": PROTOCOL_VERSION")
            && client_runtime.contains("client expects {PROTOCOL_VERSION}")
            && client_runtime.contains("async def transaction_batch(")
            && client_runtime.contains("async def _stream_rpc(")
            && client_runtime.contains("method=\"request.cancel\""),
        "expected Python runtime client to reuse the shared protocol version constant and keep transaction_batch():\n{client_runtime}\n\nProtocol:\n{protocol_runtime}"
    );
    assert!(
        protocol_runtime.contains("self.error.data"),
        "expected Python runtime protocol to preserve error.data:\n{protocol_runtime}"
    );
    assert!(
        error_runtime.contains("self.data = data"),
        "expected Python runtime errors to retain error.data from the engine:\n{error_runtime}"
    );
    assert!(
        !tx_runtime.contains("SNAPSHOT"),
        "expected Python runtime isolation levels to match the protocol exactly:\n{tx_runtime}"
    );
}

#[test]
fn test_python_runtime_exposes_engine_pool_options() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let client = generate_python_client(&ir.models, "schema.nautilus", false);
    let runtime = python_runtime_files();
    let engine_runtime = runtime
        .iter()
        .find(|(name, _)| name == "_engine.py")
        .expect("missing Python runtime engine")
        .1
        .as_str();

    assert!(
        client.contains("pool_options: EnginePoolOptions | None = None"),
        "expected generated Python client to expose pool_options:\n{client}"
    );
    assert!(
        engine_runtime.contains("class EnginePoolOptions:")
            && engine_runtime.contains("--max-connections")
            && engine_runtime.contains("--disable-idle-timeout")
            && engine_runtime.contains("--test-before-acquire")
            && engine_runtime.contains("--statement-cache-capacity"),
        "expected Python runtime engine to forward pool options to the CLI:\n{engine_runtime}"
    );
}

#[test]
fn test_python_create_many_normalizes_mapped_fields() {
    let ir = validate(
        r#"
enum Role {
  USER
  ADMIN
}

model User {
  id          Int    @id @default(autoincrement()) @map("user_id")
  displayName String @map("display_name")
  role        Role   @map("user_role")

  @@map("users")
}
"#,
    );
    let models = generate_all_python_models(&ir, false, 0);
    let (_, code) = models
        .iter()
        .find(|(name, _)| name == "user.py")
        .expect("user model missing");

    assert!(
        code.contains(r#"_process_create_data(_entry, _users_py_to_db)"#),
        "expected create_many() to normalize each entry through _process_create_data:\n{code}"
    );
}

#[test]
fn test_python_hydrates_relation_json_payloads_recursively() {
    let ir = validate(
        r#"
model User {
  id          Int       @id @default(autoincrement())
  displayName String    @map("display_name")
  posts       Post[]
  comments    Comment[]

  @@map("users")
}

model Post {
  id       Int       @id @default(autoincrement()) @map("post_id")
  authorId Int       @map("author_id")
  author   User      @relation(fields: [authorId], references: [id])
  comments Comment[]

  @@map("blog_posts")
}

model Comment {
  id     Int    @id @default(autoincrement()) @map("comment_id")
  postId Int    @map("post_id")
  userId Int    @map("user_id")
  post   Post   @relation(fields: [postId], references: [id])
  user   User   @relation(fields: [userId], references: [id])

  @@map("comments")
}
"#,
    );
    let models = generate_all_python_models(&ir, false, 0);
    let (_, user_code) = models
        .iter()
        .find(|(name, _)| name == "user.py")
        .expect("user model missing");
    let (_, post_code) = models
        .iter()
        .find(|(name, _)| name == "post.py")
        .expect("post model missing");
    let (_, comment_code) = models
        .iter()
        .find(|(name, _)| name == "comment.py")
        .expect("comment model missing");

    assert!(
        user_code.contains(r#"_get_wire_value(row, "users__display_name", "displayName")"#),
        "expected Python hydration to read nested logical scalar keys for mapped fields:\n{user_code}"
    );
    assert!(
        user_code.contains(r#"kwargs["display_name"] = _coerce_user_scalar("display_name", value)"#),
        "expected Python hydration to map logical scalar keys back to snake_case model fields:\n{user_code}"
    );
    assert!(
        post_code.contains(r#"relation_value = _get_wire_value(row, "author_json")"#),
        "expected Python hydration to read relation JSON columns on nested models:\n{post_code}"
    );
    assert!(
        post_code.contains(r#"from .user import _user_from_wire"#),
        "expected Python nested include hydration to recurse into related models:\n{post_code}"
    );
    assert!(
        comment_code.contains(r#"relation_value = _get_wire_value(row, "post_json")"#)
            && comment_code.contains(r#"relation_value = _get_wire_value(row, "user_json")"#),
        "expected Python top-level include hydration to read multiple relation JSON columns:\n{comment_code}"
    );
}

#[test]
fn test_python_composite_write_inputs_use_generated_types() {
    let ir = validate(
        r#"
type Address {
  street String
  city   String
}

model User {
  id              Int      @id @default(autoincrement())
  shippingAddress Address?
  shippingAddresses Address[]
}
"#,
    );
    let models = generate_all_python_models(&ir, false, 0);
    let (_, code) = models
        .iter()
        .find(|(name, _)| name == "user.py")
        .expect("user model missing");

    assert!(
        code.contains("shippingAddress: NotRequired[Address]"),
        "expected composite create/update inputs to use the generated Address type:\n{code}"
    );
    assert!(
        code.contains("shippingAddresses: NotRequired[List[Address]]"),
        "expected composite array write inputs to use List[Address]:\n{code}"
    );
    assert!(
        code.contains("result[db_key] = _serialize_scalar_input(key, value)"),
        "expected composite payload serialization to flow through _serialize_scalar_input:\n{code}"
    );
}

#[test]
fn test_js_composite_write_inputs_use_generated_types() {
    let ir = validate(
        r#"
type Address {
  street String
  city   String
}

model User {
  id              Int      @id @default(autoincrement())
  shippingAddress Address?
  shippingAddresses Address[]
}
"#,
    );
    let (_js_models, dts_models) = generate_all_js_models(&ir);
    let (_, code) = dts_models
        .iter()
        .find(|(name, _)| name == "user.d.ts")
        .expect("user declaration missing");

    assert!(
        code.contains("shippingAddress?: Address;"),
        "expected composite create input to use Address instead of object:\n{code}"
    );
    assert!(
        code.contains("shippingAddresses?: Address[];"),
        "expected composite array create input to use Address[] instead of object[]:\n{code}"
    );
    assert!(
        code.contains("shippingAddress?: Address | null;"),
        "expected composite update input to use Address instead of object:\n{code}"
    );
}

#[test]
fn test_js_hydrates_relation_json_payloads_recursively() {
    let ir = validate(
        r#"
model User {
  id          Int       @id @default(autoincrement())
  displayName String    @map("display_name")
  posts       Post[]
  comments    Comment[]

  @@map("users")
}

model Post {
  id       Int       @id @default(autoincrement()) @map("post_id")
  authorId Int       @map("author_id")
  author   User      @relation(fields: [authorId], references: [id])
  comments Comment[]

  @@map("blog_posts")
}

model Comment {
  id     Int    @id @default(autoincrement()) @map("comment_id")
  postId Int    @map("post_id")
  userId Int    @map("user_id")
  post   Post   @relation(fields: [postId], references: [id])
  user   User   @relation(fields: [userId], references: [id])

  @@map("comments")
}
"#,
    );
    let (js_models, _dts_models) = generate_all_js_models(&ir);
    let (_, user_code) = js_models
        .iter()
        .find(|(name, _)| name == "user.js")
        .expect("user runtime missing");
    let (_, post_code) = js_models
        .iter()
        .find(|(name, _)| name == "post.js")
        .expect("post runtime missing");
    let (_, comment_code) = js_models
        .iter()
        .find(|(name, _)| name == "comment.js")
        .expect("comment runtime missing");

    assert!(
        user_code
            .contains("const value = _getWireValue(row, 'users__display_name', 'displayName');"),
        "expected JS hydration to read nested logical scalar keys for mapped fields:\n{user_code}"
    );
    assert!(
        post_code.contains("import { _coerceUser as _coerceUser_for_author } from './user.js';"),
        "expected JS nested include hydration to import the related model coercer:\n{post_code}"
    );
    assert!(
        post_code.contains("const relationValue = _getWireValue(row, 'author_json');"),
        "expected JS hydration to read relation JSON columns on nested models:\n{post_code}"
    );
    assert!(
        comment_code.contains("const relationValue = _getWireValue(row, 'post_json');")
            && comment_code.contains("const relationValue = _getWireValue(row, 'user_json');"),
        "expected JS top-level include hydration to read multiple relation JSON columns:\n{comment_code}"
    );
}

#[test]
fn test_python_select_input_supports_projection_safe_models() {
    let ir = validate(
        r#"
model User {
  id          Int    @id @default(autoincrement()) @map("user_id")
  displayName String @map("display_name")

  @@map("users")
}
"#,
    );
    let models = generate_all_python_models(&ir, false, 0);
    let (_, code) = models
        .iter()
        .find(|(name, _)| name == "user.py")
        .expect("user model missing");

    assert!(
        code.contains("display_name: Optional[str] = None"),
        "expected projected Python models to allow missing non-PK fields:\n{code}"
    );
    assert!(
        code.contains("class UserSelectInput(TypedDict, total=False):"),
        "expected a typed UserSelectInput to be generated:\n{code}"
    );
    assert!(
        code.contains("display_name: NotRequired[bool]"),
        "expected select input to expose the Python model field name:\n{code}"
    );
    assert!(
        code.contains("\"display_name\": \"displayName\""),
        "expected select serialization to map Python field names back to logical names:\n{code}"
    );
    assert!(
        code.contains("args[\"select\"] = _process_select_fields(select, _users_py_to_logical)"),
        "expected find_many() to forward select through the logical-name serializer:\n{code}"
    );
}

#[test]
fn test_python_find_many_exposes_chunk_size() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let models = generate_all_python_models(&ir, false, 0);
    let (_, code) = models
        .iter()
        .find(|(name, _)| name == "user.py")
        .expect("user model missing");

    assert!(
        code.contains("chunk_size: Optional[int] = None"),
        "expected generated Python find_many() to expose chunk_size:\n{code}"
    );
    assert!(
        code.contains("payload[\"chunkSize\"] = chunk_size"),
        "expected generated Python find_many() to forward chunk_size as protocol chunkSize:\n{code}"
    );
}

#[test]
fn test_python_async_delegate_exposes_stream_many() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let async_models = generate_all_python_models(&ir, true, 0);
    let sync_models = generate_all_python_models(&ir, false, 0);
    let async_code = generated_python_file(&async_models, "user.py");
    let sync_code = generated_python_file(&sync_models, "user.py");

    assert!(
        async_code.contains("async def stream_many("),
        "expected generated async Python delegate to expose stream_many():\n{async_code}"
    );
    assert!(
        async_code.contains(") -> AsyncIterator[User]:"),
        "expected generated async Python stream_many() to return an AsyncIterator:\n{async_code}"
    );
    assert!(
        async_code.contains(
            "async for chunk in self._client._stream_rpc(\"query.findMany\", payload):"
        ),
        "expected generated async Python stream_many() to consume chunked RPC frames:\n{async_code}"
    );
    assert!(
        async_code.contains("\"chunkSize\": chunk_size"),
        "expected generated async Python stream_many() to force protocol chunking:\n{async_code}"
    );
    assert!(
        !sync_code.contains("def stream_many("),
        "stream_many should not be emitted for sync Python clients:\n{sync_code}"
    );
}

#[test]
fn test_python_single_row_finds_use_dedicated_engine_methods() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let py_models = generate_all_python_models(&ir, false, 0);
    let py_model = generated_python_file(&py_models, "user.py");

    assert!(
        py_model.contains(r#""query.findFirst", payload"#),
        "expected generated Python find_first() to call query.findFirst:\n{py_model}"
    );
    assert!(
        py_model.contains("from .._internal.protocol import PROTOCOL_VERSION")
            && py_model.contains("\"protocolVersion\": PROTOCOL_VERSION"),
        "expected generated Python delegates to reuse the shared protocol version constant:\n{py_model}"
    );
    assert!(
        py_model.contains(r#""query.findUnique", payload"#),
        "expected generated Python find_unique() to call query.findUnique when possible:\n{py_model}"
    );
    assert!(
        py_model.contains("if select is not None or include is not None:"),
        "expected generated Python find_unique() to fall back to the single-row projection path when select/include are used:\n{py_model}"
    );
    assert!(
        !py_model.contains("rows = self.find_many(where=where, order_by=order_by, take=1, select=select, include=include)"),
        "generated Python find_first() should no longer delegate to find_many():\n{py_model}"
    );
    assert!(
        !py_model
            .contains("rows = self.find_many(where=where, take=1, select=select, include=include)"),
        "generated Python find_unique() should no longer delegate to find_many():\n{py_model}"
    );
}

#[test]
fn test_js_select_input_supports_projection_safe_models() {
    let ir = validate(
        r#"
model User {
  id          Int    @id @default(autoincrement()) @map("user_id")
  displayName String @map("display_name")

  @@map("users")
}
"#,
    );
    let (js_models, dts_models) = generate_all_js_models(&ir);
    let (_, dts_code) = dts_models
        .iter()
        .find(|(name, _)| name == "user.d.ts")
        .expect("user declaration missing");
    let (_, js_code) = js_models
        .iter()
        .find(|(name, _)| name == "user.js")
        .expect("user runtime missing");

    assert!(
        dts_code.contains("displayName?: string;"),
        "expected projected JS models to make non-PK fields optional:\n{dts_code}"
    );
    assert!(
        dts_code.contains("export interface UserSelectInput {"),
        "expected a typed UserSelectInput to be generated:\n{dts_code}"
    );
    assert!(
        dts_code.contains("displayName?: boolean;"),
        "expected select input to expose logical field names:\n{dts_code}"
    );
    assert!(
        dts_code.contains("select?:   UserSelectInput;"),
        "expected select to be exposed on generated query methods:\n{dts_code}"
    );
    assert!(
        js_code.contains("if (args?.select   != null) rpcArgs['select']  = args.select;"),
        "expected runtime delegate to forward select to the engine:\n{js_code}"
    );
}

#[test]
fn test_js_find_many_exposes_chunk_size() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let (js_models, dts_models) = generate_all_js_models(&ir);
    let (_, dts_code) = dts_models
        .iter()
        .find(|(name, _)| name == "user.d.ts")
        .expect("user declaration missing");
    let (_, js_code) = js_models
        .iter()
        .find(|(name, _)| name == "user.js")
        .expect("user runtime missing");

    assert!(
        dts_code.contains("chunkSize?: number;"),
        "expected generated JS findMany() typings to expose chunkSize:\n{dts_code}"
    );
    assert!(
        js_code.contains("if (args?.chunkSize != null) request['chunkSize'] = args.chunkSize;"),
        "expected generated JS findMany() to forward chunkSize at the protocol level:\n{js_code}"
    );
}

#[test]
fn test_js_async_delegate_exposes_stream_many() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let (js_models, dts_models) = generate_all_js_models(&ir);
    let js_code = generated_named_file(&js_models, "user.js");
    let dts_code = generated_named_file(&dts_models, "user.d.ts");

    assert!(
        dts_code.contains("streamMany(args?: {"),
        "expected generated JS typings to expose streamMany():\n{dts_code}"
    );
    assert!(
        dts_code.contains("}): AsyncIterable<UserModel>;"),
        "expected generated JS streamMany() typings to return an AsyncIterable:\n{dts_code}"
    );
    assert!(
        js_code.contains("async *streamMany(args) {"),
        "expected generated JS delegate to expose streamMany():\n{js_code}"
    );
    assert!(
        js_code.contains(
            "for await (const chunk of this.client._streamRpc('query.findMany', payload)) {"
        ),
        "expected generated JS streamMany() to consume chunked RPC frames:\n{js_code}"
    );
    assert!(
        js_code.contains("chunkSize,"),
        "expected generated JS streamMany() to force protocol chunking:\n{js_code}"
    );
}

#[test]
fn test_js_single_row_finds_use_dedicated_engine_methods() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let (js_models, _dts_models) = generate_all_js_models(&ir);
    let js_model = generated_named_file(&js_models, "user.js");

    assert!(
        js_model.contains("this.client._rpc('query.findFirst', request)"),
        "expected generated JS findFirst() to call query.findFirst:\n{js_model}"
    );
    assert!(
        js_model.contains("import { PROTOCOL_VERSION } from '../_internal/_protocol.js';")
            && js_model.contains("protocolVersion: PROTOCOL_VERSION"),
        "expected generated JS delegates to reuse the shared protocol version constant:\n{js_model}"
    );
    assert!(
        js_model.contains("this.client._rpc('query.findUnique', request)"),
        "expected generated JS findUnique() to call query.findUnique when possible:\n{js_model}"
    );
    assert!(
        js_model.contains("if (args.select != null || args.include != null)"),
        "expected generated JS findUnique() to fall back to the single-row projection path when select/include are used:\n{js_model}"
    );
    assert!(
        !js_model.contains("const rows = await this.findMany({ where: args?.where, orderBy: args?.orderBy, take: 1, select: args?.select, include: args?.include });"),
        "generated JS findFirst() should no longer delegate to findMany():\n{js_model}"
    );
    assert!(
        !js_model.contains("const rows = await this.findMany({ where: args.where, take: 1, select: args.select, include: args.include });"),
        "generated JS findUnique() should no longer delegate to findMany():\n{js_model}"
    );
}

#[test]
fn test_js_runtime_exposes_engine_pool_options() {
    let ir = validate(
        r#"
model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let (_client_js, client_dts) = generate_js_client(&ir.models, "schema.nautilus");
    let runtime = js_runtime_files();
    let client_runtime = runtime
        .iter()
        .find(|(name, _)| name == "_client.d.ts")
        .expect("missing JS runtime client declarations")
        .1
        .as_str();
    let engine_runtime_dts = runtime
        .iter()
        .find(|(name, _)| name == "_engine.d.ts")
        .expect("missing JS runtime engine declarations")
        .1
        .as_str();
    let engine_runtime = runtime
        .iter()
        .find(|(name, _)| name == "_engine.js")
        .expect("missing JS runtime engine")
        .1
        .as_str();

    assert!(
        client_dts.contains("constructor(options?: NautilusClientOptions);")
            && client_runtime.contains("pool?: EnginePoolOptions;"),
        "expected generated JS declarations to expose engine pool options:\n{client_dts}"
    );
    assert!(
        engine_runtime_dts.contains("export interface EnginePoolOptions")
            && engine_runtime.contains("--max-connections")
            && engine_runtime.contains("--disable-idle-timeout")
            && engine_runtime.contains("--test-before-acquire")
            && engine_runtime.contains("--statement-cache-capacity"),
        "expected JS runtime engine to forward pool options to the CLI:\n{engine_runtime}"
    );
}

#[test]
fn test_java_sync_generation_exposes_model_delegate_and_autoregister_accessor() {
    let ir = validate(
        r#"
generator client {
  provider    = "nautilus-client-java"
  output      = "./generated-java"
  package     = "com.acme.db"
  group_id    = "com.acme"
  artifact_id = "db-client"
  interface   = "sync"
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
"#,
    );
    let files =
        generate_java_client(&ir, "schema.nautilus", false).expect("generate_java_client failed");
    let user_model = generated_java_file(&files, "model/User.java");
    let nautilus_client = generated_java_file(&files, "client/Nautilus.java");

    assert!(
        user_model.contains("public static UserDelegate nautilus()"),
        "expected generated Java model to expose static nautilus() accessor:\n{user_model}"
    );
    assert!(
        user_model.contains("GlobalNautilusRegistry.require()"),
        "expected generated Java model to resolve the auto-registered client:\n{user_model}"
    );
    assert!(
        nautilus_client.contains("GlobalNautilusRegistry.register(this);"),
        "expected generated Java client to auto-register itself when configured:\n{nautilus_client}"
    );

    assert_local_snapshot!("java_user_model_sync", user_model);
}

#[test]
fn test_java_async_generation_exposes_completable_future_transaction_api() {
    let ir = validate(
        r#"
generator client {
  provider    = "nautilus-client-java"
  output      = "./generated-java"
  package     = "com.acme.db"
  group_id    = "com.acme"
  artifact_id = "db-client"
  interface   = "async"
}

model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let files =
        generate_java_client(&ir, "schema.nautilus", true).expect("generate_java_client failed");
    let delegate = generated_java_file(&files, "client/UserDelegate.java");
    let nautilus_client = generated_java_file(&files, "client/Nautilus.java");

    assert!(
        delegate.contains("CompletableFuture<List<User>> findMany()"),
        "expected generated Java async delegate to expose CompletableFuture APIs:\n{delegate}"
    );
    assert!(
        nautilus_client.contains(
            "public <T> CompletableFuture<T> transaction(Function<TransactionClient, CompletableFuture<T>> callback)"
        ),
        "expected generated Java async client to expose CompletableFuture transaction API:\n{nautilus_client}"
    );

    assert_local_snapshot!("java_nautilus_async", nautilus_client);
}

#[test]
fn test_java_generation_exposes_stream_many_over_chunked_rpc() {
    let ir = validate(
        r#"
generator client {
  provider    = "nautilus-client-java"
  output      = "./generated-java"
  package     = "com.acme.db"
  group_id    = "com.acme"
  artifact_id = "db-client"
  interface   = "async"
}

model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let async_files =
        generate_java_client(&ir, "schema.nautilus", true).expect("generate_java_client failed");
    let sync_files =
        generate_java_client(&ir, "schema.nautilus", false).expect("generate_java_client failed");

    let async_delegate = generated_java_file(&async_files, "client/UserDelegate.java");
    let sync_delegate = generated_java_file(&sync_files, "client/UserDelegate.java");
    let dsl = generated_java_file(&async_files, "dsl/UserDsl.java");
    let rpc_caller = generated_java_file(&async_files, "internal/RpcCaller.java");
    let base_client = generated_java_file(&async_files, "internal/BaseNautilusClient.java");
    let base_tx_client = generated_java_file(&async_files, "internal/BaseTransactionClient.java");

    assert!(
        async_delegate.contains("public Stream<User> streamMany()")
            && sync_delegate.contains("public Stream<User> streamMany()"),
        "expected generated Java delegates to expose streamMany():\nasync:\n{async_delegate}\n\nsync:\n{sync_delegate}"
    );
    assert!(
        async_delegate.contains("DEFAULT_STREAM_CHUNK_SIZE = 128")
            && async_delegate.contains("streamMany chunkSize must be a positive integer")
            && async_delegate.contains(
                "return rows(streamRpc(\"query.findMany\", request), User::fromJsonNode);"
            ),
        "expected generated Java async delegate to stream chunked findMany rows:\n{async_delegate}"
    );
    assert!(
        rpc_caller.contains("Stream<JsonNode> streamRpc(String method, ObjectNode params);"),
        "expected Java RpcCaller to expose streamRpc():\n{rpc_caller}"
    );
    assert!(
        dsl.contains("public ObjectNode whereNode()")
            && dsl.contains("values.add(orderBy.node());"),
        "expected Java FindManyArgs to expose whereNode() and serialize orderBy as an array:\n{dsl}"
    );
    assert!(
        base_client
            .contains("private final Map<Long, StreamState> streams = new ConcurrentHashMap<>();")
            && base_client.contains("request.put(\"method\", \"request.cancel\");")
            && base_client.contains(
                "return StreamSupport.stream(spliterator, false).onClose(cursor::close);"
            ),
        "expected Java runtime to stream chunked responses and cancel early closes:\n{base_client}"
    );
    assert!(
        base_tx_client.contains("return this.parent.streamRpc(method, actual);"),
        "expected transaction clients to forward streamRpc() through the parent client:\n{base_tx_client}"
    );
}

#[test]
fn test_java_runtime_loads_dotenv_before_spawning_engine() {
    let ir = validate(
        r#"
generator client {
  provider    = "nautilus-client-java"
  output      = "./generated-java"
  package     = "com.acme.db"
  group_id    = "com.acme"
  artifact_id = "db-client"
  interface   = "sync"
}

model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let files =
        generate_java_client(&ir, "schema.nautilus", false).expect("generate_java_client failed");
    let engine_process = generated_java_file(&files, "internal/EngineProcess.java");

    assert!(
        engine_process.contains("loadDotenv(builder.environment(), schemaPath);"),
        "expected generated Java runtime to load .env before starting the engine:\n{engine_process}"
    );
    assert!(
        engine_process.contains("Path candidate = root.resolve(\".env\");"),
        "expected generated Java runtime to search for .env files near the schema:\n{engine_process}"
    );
    assert!(
        engine_process.contains("environment.putIfAbsent(key, value);"),
        "expected generated Java runtime to preserve pre-existing environment variables:\n{engine_process}"
    );
    assert!(
        engine_process.contains("Optional<String> localBinary = findLocalBinary(schemaPath);"),
        "expected generated Java runtime to prefer a local nautilus binary before PATH lookup:\n{engine_process}"
    );
}

#[test]
fn test_java_runtime_exposes_engine_pool_options() {
    let ir = validate(
        r#"
generator client {
  provider    = "nautilus-client-java"
  output      = "./generated-java"
  package     = "com.acme.db"
  group_id    = "com.acme"
  artifact_id = "db-client"
  interface   = "sync"
}

model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let files =
        generate_java_client(&ir, "schema.nautilus", false).expect("generate_java_client failed");
    let options = generated_java_file(&files, "client/NautilusOptions.java");
    let engine_process = generated_java_file(&files, "internal/EngineProcess.java");

    assert!(
        options.contains("public NautilusOptions maxConnections(Integer maxConnections)")
            && options
                .contains("public NautilusOptions disableIdleTimeout(boolean disableIdleTimeout)")
            && options.contains("public Boolean testBeforeAcquire()"),
        "expected generated Java options to expose engine pool settings:\n{options}"
    );
    assert!(
        engine_process.contains("command.add(\"--max-connections\");")
            && engine_process.contains("command.add(\"--disable-idle-timeout\");")
            && engine_process.contains("command.add(\"--test-before-acquire\");"),
        "expected generated Java runtime engine to forward pool options to the CLI:\n{engine_process}"
    );
}

#[test]
fn test_generated_clients_exclude_non_orderable_fields_from_order_by() {
    let ir = validate(
        r#"
datasource db {
  provider   = "postgresql"
  url        = env("DATABASE_URL")
  extensions = [hstore, vector]
}

generator client {
  provider    = "nautilus-client-java"
  output      = "./generated-java"
  package     = "com.acme.db"
  group_id    = "com.acme"
  artifact_id = "db-client"
  interface   = "sync"
}

model User {
  id      Int      @id @default(autoincrement())
  title   String
  active  Boolean
  meta    Hstore?
  payload Json?
  embedding Vector(3)
}
"#,
    );

    let (_, dts_models) = generate_all_js_models(&ir);
    let js_dts = dts_models
        .iter()
        .find(|(name, _)| name == "user.d.ts")
        .map(|(_, code)| code.as_str())
        .expect("user declaration missing");
    assert!(js_dts.contains("title?: SortOrder;"));
    assert!(!js_dts.contains("active?: SortOrder;"));
    assert!(!js_dts.contains("meta?: SortOrder;"));
    assert!(!js_dts.contains("payload?: SortOrder;"));
    assert!(!js_dts.contains("embedding?: SortOrder;"));

    let py_models = generate_all_python_models(&ir, false, 1);
    let py_model = generated_python_file(&py_models, "user.py");
    assert!(py_model.contains("title: NotRequired[Literal[\"asc\", \"desc\"]]"));
    assert!(!py_model.contains("active: NotRequired[Literal[\"asc\", \"desc\"]]"));
    assert!(!py_model.contains("meta: NotRequired[Literal[\"asc\", \"desc\"]]"));
    assert!(!py_model.contains("payload: NotRequired[Literal[\"asc\", \"desc\"]]"));
    assert!(!py_model.contains("embedding: NotRequired[Literal[\"asc\", \"desc\"]]"));

    let java_files =
        generate_java_client(&ir, "schema.nautilus", false).expect("generate_java_client failed");
    let user_dsl = generated_java_file(&java_files, "dsl/UserDsl.java");
    assert!(user_dsl.contains("public OrderBy title(SortOrder order)"));
    assert!(!user_dsl.contains("public OrderBy active(SortOrder order)"));
    assert!(!user_dsl.contains("public OrderBy meta(SortOrder order)"));
    assert!(!user_dsl.contains("public OrderBy payload(SortOrder order)"));
    assert!(!user_dsl.contains("public OrderBy embedding(SortOrder order)"));
}

#[test]
fn test_generated_hstore_filters_are_typed_in_js_and_python() {
    let ir = validate(
        r#"
datasource db {
  provider   = "postgresql"
  url        = env("DATABASE_URL")
  extensions = [hstore]
}

model User {
  id   Int     @id @default(autoincrement())
  meta Hstore?
}
"#,
    );

    let (_, dts_models) = generate_all_js_models(&ir);
    let js_dts = dts_models
        .iter()
        .find(|(name, _)| name == "user.d.ts")
        .map(|(_, code)| code.as_str())
        .expect("user declaration missing");
    assert!(js_dts.contains("export interface HstoreFilter {"));
    assert!(js_dts.contains("export type HstoreValue = Record<string, string | null>;"));
    // With the `hstore` extension declared, filter inputs accept the generated
    // wrapper or the raw `HstoreValue` payload via the `HstoreInput` union.
    assert!(js_dts.contains("equals?: HstoreInput;"));
    assert!(js_dts.contains("not?:    HstoreInput;"));
    assert!(js_dts.contains("isNull?: boolean;"));
    assert!(js_dts.contains("meta?: HstoreInput | HstoreFilter;"));

    let py_models = generate_all_python_models(&ir, false, 1);
    let py_model = generated_python_file(&py_models, "user.py");
    assert!(py_model.contains("HstoreValue = Dict[str, Optional[str]]"));
    assert!(py_model.contains("class HstoreFilter(TypedDict, total=False):"));
    // With the `hstore` extension declared the filter accepts the wrapper too.
    assert!(py_model.contains("equals: NotRequired[HstoreInput]"));
    assert!(py_model.contains("not_: NotRequired[HstoreInput]"));
    assert!(py_model.contains("is_null: NotRequired[bool]"));
    assert!(py_model.contains("meta: NotRequired[Union[HstoreInput, HstoreFilter]]"));
}

#[test]
fn test_generated_vector_filters_are_typed_in_js_and_python() {
    let ir = validate(
        r#"
datasource db {
  provider   = "postgresql"
  url        = env("DATABASE_URL")
  extensions = [vector]
}

generator client {
  provider    = "nautilus-client-java"
  output      = "./db"
  package     = "com.example.db"
  group_id    = "com.example"
  artifact_id = "db"
}

model User {
  id        Int       @id @default(autoincrement())
  embedding Vector(3)
}
"#,
    );

    let (_, dts_models) = generate_all_js_models(&ir);
    let js_dts = dts_models
        .iter()
        .find(|(name, _)| name == "user.d.ts")
        .map(|(_, code)| code.as_str())
        .expect("user declaration missing");
    assert!(js_dts.contains("export interface VectorFilter {"));
    // With the `vector` extension declared, the filter accepts the wrapper
    // `Vector` instance or the raw `number[]` via the `VectorInput` union.
    assert!(js_dts.contains("equals?: VectorInput;"));
    assert!(js_dts.contains("not?:    VectorInput;"));
    assert!(js_dts.contains("isNull?: boolean;"));
    assert!(js_dts.contains("embedding?: VectorInput | VectorFilter;"));
    assert!(js_dts.contains("export type VectorMetric = 'l2' | 'innerProduct' | 'cosine';"));
    assert!(js_dts.contains("export type UserVectorFieldKeys = 'embedding';"));
    assert!(js_dts.contains("export interface UserNearestInput {"));
    assert!(js_dts.contains("nearest?:  UserNearestInput;"));
    // The Nearest input also widens its `query` to accept the wrapper.
    assert!(js_dts.contains("query:  VectorInput;"));

    let py_models = generate_all_python_models(&ir, false, 1);
    let py_model = generated_python_file(&py_models, "user.py");
    assert!(py_model.contains("class VectorFilter(TypedDict, total=False):"));
    assert!(py_model.contains("equals: NotRequired[VectorInput]"));
    assert!(py_model.contains("not_: NotRequired[VectorInput]"));
    assert!(py_model.contains("is_null: NotRequired[bool]"));
    assert!(py_model.contains("embedding: NotRequired[Union[VectorInput, VectorFilter]]"));
    assert!(py_model.contains("VectorMetric = Literal[\"l2\", \"innerProduct\", \"cosine\"]"));
    assert!(py_model.contains("UserVectorFieldKeys = Literal[\"embedding\"]"));
    assert!(py_model.contains("class UserNearestInput(TypedDict):"));
    assert!(py_model.contains("nearest: Optional[UserNearestInput] = None"));

    let java_files =
        generate_java_client(&ir, "schema.nautilus", false).expect("java client generation");
    let java_dsl = java_files
        .iter()
        .find(|(name, _)| name.ends_with("/UserDsl.java"))
        .map(|(_, code)| code.as_str())
        .expect("UserDsl.java missing");
    assert!(java_dsl.contains("public enum VectorMetric {"));
    assert!(java_dsl.contains("public Nearest embedding() {"));
    assert!(java_dsl.contains("public FindManyArgs nearest(Consumer<Nearest> spec) {"));
}

#[test]
fn test_extension_input_builders_are_generated_across_codegens() {
    let ir = validate(
        r#"
datasource db {
  provider   = "postgresql"
  url        = env("DATABASE_URL")
  extensions = [citext, hstore, ltree, postgis, vector]
}

model Example {
  id          Int        @id @default(autoincrement())
  email       Citext
  path        Ltree?
  meta        Hstore?
  footprint   Geometry?
  serviceArea Geography?
  embedding   Vector(3)
}
"#,
    );

    let extensions = ExtensionRegistry::from_schema(&ir);

    let py_ext_files = generate_python_extension_files(&extensions);
    let citext_py = generated_named_file(&py_ext_files, "citext/types.py");
    let hstore_py = generated_named_file(&py_ext_files, "hstore/types.py");
    let ltree_py = generated_named_file(&py_ext_files, "ltree/types.py");
    let postgis_py = generated_named_file(&py_ext_files, "postgis/types.py");
    let vector_py = generated_named_file(&py_ext_files, "vector/types.py");
    assert!(citext_py.contains("CitextInput = Union[\"Citext\", str, CitextBuilderInput]"));
    assert!(citext_py.contains("class CitextValueInput(TypedDict):"));
    assert!(ltree_py.contains("LtreeInput = Union[\"Ltree\", str, LtreeBuilderInput]"));
    assert!(hstore_py
        .contains("HstoreInput = Union[\"Hstore\", HstoreSource, HstoreEntriesBuilderInput]"));
    assert!(hstore_py.contains("class HstoreEntriesBuilderInput(TypedDict):"));
    assert!(postgis_py.contains("class GeometryPointInput(TypedDict, total=False):"));
    assert!(postgis_py.contains("class GeographyPointInput(TypedDict, total=False):"));
    assert!(postgis_py.contains("GeometryInput = Union[\"Geometry\", str, GeometryBuilderInput]"));
    assert!(
        postgis_py.contains("GeographyInput = Union[\"Geography\", str, GeographyBuilderInput]")
    );
    assert!(vector_py.contains("class VectorValuesInput(TypedDict):"));
    assert!(vector_py.contains("VectorInput = Union[\"Vector\", VectorSource, VectorValuesInput]"));

    let (_, js_ext_dts) = generate_js_extension_files(&extensions);
    let citext_dts = generated_named_file(&js_ext_dts, "extensions/citext/types.d.ts");
    let hstore_dts = generated_named_file(&js_ext_dts, "extensions/hstore/types.d.ts");
    let ltree_dts = generated_named_file(&js_ext_dts, "extensions/ltree/types.d.ts");
    let postgis_dts = generated_named_file(&js_ext_dts, "extensions/postgis/types.d.ts");
    let vector_dts = generated_named_file(&js_ext_dts, "extensions/vector/types.d.ts");
    assert!(citext_dts.contains("export interface CitextValueInput {"));
    assert!(citext_dts.contains("export type CitextInput = Citext | string | CitextBuilderInput;"));
    assert!(ltree_dts.contains("export type LtreeInput = Ltree | string | LtreeBuilderInput;"));
    assert!(hstore_dts.contains("export interface HstoreEntriesBuilderInput {"));
    assert!(hstore_dts.contains("export type HstoreInput = Hstore | HstoreBuilderInput;"));
    assert!(postgis_dts.contains("export interface GeometryPointInput {"));
    assert!(postgis_dts.contains("export interface GeographyPointInput {"));
    assert!(postgis_dts
        .contains("export type GeometryInput = Geometry | string | GeometryBuilderInput;"));
    assert!(postgis_dts
        .contains("export type GeographyInput = Geography | string | GeographyBuilderInput;"));
    assert!(vector_dts.contains("export interface VectorValuesInput {"));
    assert!(vector_dts.contains("export type VectorInput = Vector | VectorBuilderInput;"));

    let py_models = generate_all_python_models(&ir, false, 1);
    let py_model = generated_python_file(&py_models, "example.py");
    assert!(py_model.contains("email: CitextInput"));
    assert!(py_model.contains("path: NotRequired[LtreeInput]"));
    assert!(py_model.contains("meta: NotRequired[HstoreInput]"));
    assert!(py_model.contains("footprint: NotRequired[GeometryInput]"));
    assert!(py_model.contains("serviceArea: NotRequired[GeographyInput]"));
    assert!(py_model.contains("embedding: VectorInput"));
    assert!(py_model.contains("footprint: NotRequired[Union[GeometryInput, StringFilter]]"));
    assert!(py_model.contains("serviceArea: NotRequired[Union[GeographyInput, StringFilter]]"));
    assert!(py_model.contains("embedding: NotRequired[Union[VectorInput, VectorFilter]]"));

    let (_, js_models) = generate_all_js_models(&ir);
    let js_model = js_models
        .iter()
        .find(|(name, _)| name == "example.d.ts")
        .map(|(_, code)| code.as_str())
        .expect("example.d.ts missing");
    assert!(js_model.contains("email: CitextInput;"));
    assert!(js_model.contains("path?: LtreeInput;"));
    assert!(js_model.contains("meta?: HstoreInput | null;"));
    assert!(js_model.contains("footprint?: GeometryInput | null;"));
    assert!(js_model.contains("serviceArea?: GeographyInput | null;"));
    assert!(js_model.contains("embedding: VectorInput;"));
    assert!(js_model.contains("footprint?: GeometryInput | StringFilter;"));
    assert!(js_model.contains("serviceArea?: GeographyInput | StringFilter;"));
    assert!(js_model.contains("embedding?: VectorInput | VectorFilter;"));

    let java_ext_files = generate_java_extension_files(&extensions, "com.acme.db");
    let geometry_java = generated_java_file(&java_ext_files, "Geometry.java");
    let geography_java = generated_java_file(&java_ext_files, "Geography.java");
    let hstore_java = generated_java_file(&java_ext_files, "Hstore.java");
    let vector_java = generated_java_file(&java_ext_files, "Vector.java");
    let citext_java = generated_java_file(&java_ext_files, "Citext.java");
    let ltree_java = generated_java_file(&java_ext_files, "Ltree.java");
    assert!(citext_java.contains("public static Citext of(String value)"));
    assert!(ltree_java.contains("public static Ltree of(String value)"));
    assert!(geometry_java.contains("public static Geometry point(double x, double y)"));
    assert!(geography_java.contains("public static Geography point(double lon, double lat)"));
    assert!(hstore_java
        .contains("public static Hstore ofEntries(Map.Entry<String, String>... entries)"));
    assert!(vector_java.contains("public static Vector of(double... values)"));

    let rust_ext_files = generate_rust_extension_files(&extensions);
    let postgis_rust = generated_named_file(&rust_ext_files, "extensions/postgis/types.rs");
    let hstore_rust = generated_named_file(&rust_ext_files, "extensions/hstore/types.rs");
    let vector_rust = generated_named_file(&rust_ext_files, "extensions/vector/types.rs");
    let citext_rust = generated_named_file(&rust_ext_files, "extensions/citext/types.rs");
    let ltree_rust = generated_named_file(&rust_ext_files, "extensions/ltree/types.rs");
    assert!(citext_rust.contains("pub fn of(value: impl Into<String>) -> Self"));
    assert!(ltree_rust.contains("pub fn of(value: impl Into<String>) -> Self"));
    assert!(postgis_rust.contains("impl Geometry {"));
    assert!(postgis_rust
        .contains("pub fn point(x: impl std::fmt::Display, y: impl std::fmt::Display) -> Self"));
    assert!(postgis_rust.contains("impl Geography {"));
    assert!(postgis_rust.contains(
        "pub fn point(lon: impl std::fmt::Display, lat: impl std::fmt::Display) -> Self"
    ));
    assert!(hstore_rust.contains("pub fn from_entries<K, V, I>(entries: I) -> Self"));
    assert!(vector_rust.contains("pub fn of<I, N>(values: I) -> Self"));
}

#[test]
fn test_python_filter_operator_names_are_normalized_for_engine() {
    let ir = validate(
        r#"
model User {
  id    Int     @id @default(autoincrement())
  title String?
}
"#,
    );

    let py_models = generate_all_python_models(&ir, false, 1);
    let py_model = generated_python_file(&py_models, "user.py");

    assert!(py_model.contains("\"in_\": \"in\""));
    assert!(py_model.contains("\"not_\": \"not\""));
    assert!(py_model.contains("\"not_in\": \"notIn\""));
    assert!(py_model.contains("\"startswith\": \"startsWith\""));
    assert!(py_model.contains("\"endswith\": \"endsWith\""));
    assert!(py_model.contains("\"is_null\": \"isNull\""));
}

#[test]
fn test_generated_object_like_where_values_require_explicit_equals_in_js_and_python() {
    let ir = validate(
        r#"
datasource db {
  provider   = "postgresql"
  url        = env("DATABASE_URL")
  extensions = [hstore]
}

model User {
  id      Int    @id @default(autoincrement())
  payload Jsonb?
  meta    Hstore?
}
"#,
    );

    let (js_models, dts_models) = generate_all_js_models(&ir);
    let js_model = js_models
        .iter()
        .find(|(name, _)| name == "user.js")
        .map(|(_, code)| code.as_str())
        .expect("user runtime missing");
    let js_dts = dts_models
        .iter()
        .find(|(name, _)| name == "user.d.ts")
        .map(|(_, code)| code.as_str())
        .expect("user declaration missing");
    assert!(js_model.contains("ObjectValueDbFields = new Set(["));
    assert!(js_model.contains("_objectEqualityRequiresExplicitEquals"));
    assert!(js_model.contains("Use { equals: ... } for object equality filters."));
    assert!(js_model.contains("const actualOp = op === 'equals' ? 'eq' : op;"));
    assert!(js_dts.contains("export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];"));
    assert!(js_dts.contains("export interface JsonFilter {"));
    assert!(js_dts.contains("equals?: JsonValue;"));
    assert!(js_dts.contains("payload?: JsonScalarOrArray | JsonFilter;"));

    let py_models = generate_all_python_models(&ir, false, 1);
    let py_model = generated_python_file(&py_models, "user.py");
    assert!(py_model.contains("JsonValue = Union[JsonPrimitive, Dict[str, Any], List[Any]]"));
    assert!(py_model.contains("_object_value_db_fields: frozenset = frozenset({"));
    assert!(py_model.contains("_object_equality_requires_explicit_equals"));
    assert!(py_model.contains("Use {'equals': ...} for object equality filters."));
    assert!(py_model.contains("\"equals\": \"eq\""));
    assert!(py_model.contains("class JsonFilter(TypedDict, total=False):"));
    assert!(py_model.contains("equals: NotRequired[JsonValue]"));
    assert!(py_model.contains("payload: NotRequired[Union[JsonScalarOrArray, JsonFilter]]"));
}

#[test]
fn test_java_single_row_finds_use_dedicated_engine_methods() {
    let ir = validate(
        r#"
generator client {
  provider    = "nautilus-client-java"
  output      = "./generated-java"
  package     = "com.acme.db"
  group_id    = "com.acme"
  artifact_id = "db-client"
}

model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#,
    );
    let java_files =
        generate_java_client(&ir, "schema.nautilus", false).expect("generate_java_client failed");
    let delegate = generated_java_file(&java_files, "client/UserDelegate.java");

    assert!(
        delegate.contains("JsonNode result = rpc(\"query.findFirst\", request);"),
        "expected generated Java findFirst() to call query.findFirst:\n{delegate}"
    );
    assert!(
        delegate.contains("request.put(\"protocolVersion\", JsonSupport.PROTOCOL_VERSION);"),
        "expected generated Java delegates to reuse the shared protocol version constant:\n{delegate}"
    );
    assert!(
        delegate.contains("JsonNode result = rpc(\"query.findUnique\", request);"),
        "expected generated Java findUnique() to call query.findUnique when possible:\n{delegate}"
    );
    assert!(
        delegate.contains("if (node.size() == 1 && node.has(\"where\"))"),
        "expected generated Java findUnique() to gate the unique-only fast path conservatively:\n{delegate}"
    );
    assert!(
        !delegate.contains("return findFirst(spec);"),
        "generated Java findUnique() should no longer alias directly to findFirst():\n{delegate}"
    );
}

#[test]
fn test_generated_java_hstore_uses_runtime_type_that_preserves_null_values() {
    let ir = validate(
        r#"
datasource db {
  provider   = "postgresql"
  url        = env("DATABASE_URL")
  extensions = [hstore]
}

generator client {
  provider    = "nautilus-client-java"
  output      = "./generated-java"
  package     = "com.acme.db"
  group_id    = "com.acme"
  artifact_id = "db-client"
  interface   = "sync"
}

model User {
  id   Int     @id @default(autoincrement())
  meta Hstore?
}
"#,
    );

    let java_files =
        generate_java_client(&ir, "schema.nautilus", false).expect("generate_java_client failed");
    let user_model = generated_java_file(&java_files, "model/User.java");
    let json_support = generated_java_file(&java_files, "internal/JsonSupport.java");

    // With the `hstore` extension declared the model field uses the generated
    // `Hstore` wrapper class (which itself wraps `JsonSupport.Hstore` to
    // preserve null-aware key/value semantics on the wire).
    assert!(user_model.contains("Hstore meta"));
    assert!(user_model.contains("import com.acme.db.extensions.hstore.types.Hstore;"));
    assert!(json_support
        .contains("public static final class Hstore extends LinkedHashMap<String, String>"));
    assert!(json_support.contains("public static Hstore asHstore(JsonNode node)"));
}
