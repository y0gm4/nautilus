//! Snapshot tests for the code generator: parse a schema, generate code, and
//! optionally assert the full rendered output against local-only snapshots.
//!
//! Snapshot baselines live in `tests/snapshots/`, which is gitignored on
//! purpose. If `.snap` files already exist locally, these tests compare against
//! them automatically. To force snapshot assertions or generate fresh local
//! baselines, run with `NAUTILUS_LOCAL_SNAPSHOTS=1` (typically alongside
//! `INSTA_UPDATE=always`). To skip snapshot assertions even when local
//! baselines exist, run with `NAUTILUS_SKIP_LOCAL_SNAPSHOTS=1`.

use std::{path::PathBuf, sync::OnceLock};

use nautilus_codegen::{
    enum_gen::generate_all_enums,
    generator::generate_all_models,
    java::generate_java_client,
    js::{generate_all_js_models, generate_js_client, js_runtime_files},
    python::{
        generate_all_python_models, generate_python_client, generate_python_enums,
        python_runtime_files,
    },
};
use nautilus_schema::validate_schema_source;

fn local_snapshot_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("snapshots")
}

fn local_snapshots_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();

    *ENABLED.get_or_init(|| {
        if std::env::var_os("NAUTILUS_SKIP_LOCAL_SNAPSHOTS").is_some() {
            return false;
        }

        if std::env::var_os("NAUTILUS_LOCAL_SNAPSHOTS").is_some() {
            return true;
        }

        std::fs::read_dir(local_snapshot_dir())
            .map(|entries| {
                entries.flatten().any(|entry| {
                    entry.path().extension().and_then(|ext| ext.to_str()) == Some("snap")
                })
            })
            .unwrap_or(false)
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
        code.contains("normalize_row_with_hints"),
        "expected generated Rust model to normalize rows before FromRow:\n{code}"
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
        .find(|(name, _)| *name == "_client.js")
        .expect("missing JS runtime client")
        .1;
    let error_runtime = runtime
        .iter()
        .find(|(name, _)| *name == "_errors.js")
        .expect("missing JS runtime errors")
        .1;
    let tx_runtime = runtime
        .iter()
        .find(|(name, _)| *name == "_transaction.js")
        .expect("missing JS runtime transaction")
        .1;

    assert!(
        client_js.contains("async $transactionBatch(operations, options)"),
        "expected generated JS client to expose $transactionBatch():\n{client_js}"
    );
    assert!(
        client_dts.contains("$transactionBatch("),
        "expected generated JS declarations to expose $transactionBatch():\n{client_dts}"
    );
    assert!(
        client_runtime.contains("protocolVersion: 1")
            && client_runtime.contains("client expects 1")
            && client_runtime.contains("transaction.batch"),
        "expected JS runtime client to speak protocol v1 and expose transaction.batch:\n{client_runtime}"
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
        .find(|(name, _)| *name == "_client.py")
        .expect("missing Python runtime client")
        .1;
    let protocol_runtime = runtime
        .iter()
        .find(|(name, _)| *name == "_protocol.py")
        .expect("missing Python runtime protocol")
        .1;
    let error_runtime = runtime
        .iter()
        .find(|(name, _)| *name == "_errors.py")
        .expect("missing Python runtime errors")
        .1;
    let tx_runtime = runtime
        .iter()
        .find(|(name, _)| *name == "_transaction.py")
        .expect("missing Python runtime transaction")
        .1;

    assert!(
        client_runtime.contains("\"protocolVersion\": 1")
            && client_runtime.contains("client expects 1")
            && client_runtime.contains("async def transaction_batch("),
        "expected Python runtime client to speak protocol v1 and keep transaction_batch():\n{client_runtime}"
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
        code.contains("result[db_key] = _serialize_wire_value(value)"),
        "expected composite payload serialization to flow through _serialize_wire_value:\n{code}"
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
    assert!(js_dts.contains("equals?: HstoreValue;"));
    assert!(js_dts.contains("not?:    HstoreValue;"));
    assert!(js_dts.contains("isNull?: boolean;"));
    assert!(js_dts.contains("meta?: HstoreFilter;"));

    let py_models = generate_all_python_models(&ir, false, 1);
    let py_model = generated_python_file(&py_models, "user.py");
    assert!(py_model.contains("HstoreValue = Dict[str, Optional[str]]"));
    assert!(py_model.contains("class HstoreFilter(TypedDict, total=False):"));
    assert!(py_model.contains("equals: NotRequired[HstoreValue]"));
    assert!(py_model.contains("not_: NotRequired[HstoreValue]"));
    assert!(py_model.contains("is_null: NotRequired[bool]"));
    assert!(py_model.contains("meta: NotRequired[HstoreFilter]"));
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
    assert!(js_dts.contains("equals?: number[];"));
    assert!(js_dts.contains("not?:    number[];"));
    assert!(js_dts.contains("isNull?: boolean;"));
    assert!(js_dts.contains("embedding?: number[] | VectorFilter;"));
    assert!(js_dts.contains("export type VectorMetric = 'l2' | 'innerProduct' | 'cosine';"));
    assert!(js_dts.contains("export type UserVectorFieldKeys = 'embedding';"));
    assert!(js_dts.contains("export interface UserNearestInput {"));
    assert!(js_dts.contains("nearest?:  UserNearestInput;"));

    let py_models = generate_all_python_models(&ir, false, 1);
    let py_model = generated_python_file(&py_models, "user.py");
    assert!(py_model.contains("class VectorFilter(TypedDict, total=False):"));
    assert!(py_model.contains("equals: NotRequired[List[float]]"));
    assert!(py_model.contains("not_: NotRequired[List[float]]"));
    assert!(py_model.contains("is_null: NotRequired[bool]"));
    assert!(py_model.contains("embedding: NotRequired[Union[List[float], VectorFilter]]"));
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

    assert!(user_model.contains("JsonSupport.Hstore meta"));
    assert!(json_support
        .contains("public static final class Hstore extends LinkedHashMap<String, String>"));
    assert!(json_support.contains("public static Hstore asHstore(JsonNode node)"));
}
