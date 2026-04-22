mod common;

use common::parse_schema as parse;
use nautilus_schema::{validate_schema, SchemaError};

#[test]
fn test_duplicate_model_names() {
    let source = r#"
model User {
  id Int @id
}

model User {
  id Int @id
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("Duplicate model name 'User'"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_duplicate_enum_names() {
    let source = r#"
enum Role {
  USER
  ADMIN
}

enum Role {
  STAFF
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("Duplicate enum name 'Role'"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_duplicate_field_names() {
    let source = r#"
model User {
  id Int @id
  email String
  email String
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("Duplicate field name 'email'"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_unknown_type() {
    let source = r#"
model Post {
  id Int @id
  author UnknownType
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("Unknown type 'UnknownType'"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_invalid_decimal_precision_zero() {
    let source = r#"
model Product {
  id Int @id
  price Decimal(0, 2)
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("precision must be greater than 0"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_invalid_decimal_scale_exceeds_precision() {
    let source = r#"
model Product {
  id Int @id
  price Decimal(5, 10)
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("scale") && msg.contains("precision"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_composite_pk_nonexistent_field() {
    let source = r#"
model User {
  id Int @id
  @@id([id, nonexistent])
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("@@id references non-existent field 'nonexistent'"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_composite_pk_array_field() {
    let source = r#"
model User {
  id Int @id
  tags String[]
  @@id([id, tags])
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("cannot be an array"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_relation_field_count_mismatch() {
    let source = r#"
model User {
  id Int @id
}

model Post {
  id Int @id
  userId Int
  extraId Int
  user User @relation(fields: [userId, extraId], references: [id])
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("has 2 fields but 1 references"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_relation_unknown_target_model() {
    let source = r#"
model Post {
  id Int @id
  author UnknownModel @relation(fields: [authorId], references: [id])
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("Unknown type 'UnknownModel'") || msg.contains("unknown model"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_relation_nonexistent_fk_field() {
    let source = r#"
model User {
  id Int @id
}

model Post {
  id Int @id
  user User @relation(fields: [nonexistent], references: [id])
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("references non-existent field 'nonexistent'"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_relation_nonexistent_reference_field() {
    let source = r#"
model User {
  id Int @id
}

model Post {
  id Int @id
  userId Int
  user User @relation(fields: [userId], references: [nonexistent])
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("references non-existent field 'nonexistent'"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_relation_references_non_unique_field() {
    let source = r#"
model User {
  id Int @id
  email String
}

model Post {
  id Int @id
  userEmail String
  user User @relation(fields: [userEmail], references: [email])
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("not a primary key or unique field"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_relation_references_unique_field_ok() {
    let source = r#"
model User {
  id Int @id
  email String @unique

  posts Post[]
}

model Post {
  id Int @id
  userEmail String
  user User @relation(fields: [userEmail], references: [email])
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();
    assert_eq!(ir.models.len(), 2);
}

#[test]
fn test_relation_references_composite_primary_key_ok() {
    let source = r#"
model User {
  firstName String
  lastName  String

  posts Post[]

  @@id([firstName, lastName])
}

model Post {
  id             Int    @id
  userFirstName  String
  userLastName   String
  user User @relation(fields: [userFirstName, userLastName], references: [firstName, lastName])
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();
    assert_eq!(ir.models.len(), 2);
}

#[test]
fn test_relation_references_composite_unique_ok() {
    let source = r#"
model User {
  id       Int    @id
  email    String
  username String

  posts Post[]

  @@unique([email, username])
}

model Post {
  id           Int    @id
  userEmail    String
  userUsername String
  user User @relation(fields: [userEmail, userUsername], references: [email, username])
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();
    assert_eq!(ir.models.len(), 2);
}

#[test]
fn test_relation_references_non_unique_composite_fields() {
    let source = r#"
model User {
  id       Int    @id
  email    String
  username String

  posts Post[]
}

model Post {
  id           Int    @id
  userEmail    String
  userUsername String
  user User @relation(fields: [userEmail, userUsername], references: [email, username])
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("composite primary key or unique constraint"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_multiple_relations_without_name() {
    let source = r#"
model User {
  id Int @id
}

model Post {
  id Int @id
  authorId Int
  reviewerId Int
  author User @relation(fields: [authorId], references: [id])
  reviewer User @relation(fields: [reviewerId], references: [id])
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("multiple relations") && msg.contains("unique 'name' parameters"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_multiple_relations_with_name_ok() {
    let source = r#"
model User {
  id Int @id

  authoredPosts Post[] @relation(name: "AuthoredPosts")
  reviewedPosts Post[] @relation(name: "ReviewedPosts")
}

model Post {
  id Int @id
  authorId Int
  reviewerId Int
  author User @relation(name: "AuthoredPosts", fields: [authorId], references: [id])
  reviewer User @relation(name: "ReviewedPosts", fields: [reviewerId], references: [id])
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();
    assert_eq!(ir.models.len(), 2);
}

#[test]
fn test_named_relations_cannot_share_one_opposite_field() {
    let source = r#"
model User {
  id Int @id

  authoredPosts Post[] @relation(name: "AuthoredPosts")
}

model Post {
  id Int @id
  authorId Int
  reviewerId Int
  author User @relation(name: "AuthoredPosts", fields: [authorId], references: [id])
  reviewer User @relation(name: "ReviewedPosts", fields: [reviewerId], references: [id])
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(
                msg.contains("expects an opposite relation field")
                    || msg.contains("missing an opposite relation field"),
                "unexpected message: {}",
                msg
            );
            assert!(msg.contains("ReviewedPosts"), "unexpected message: {}", msg);
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_default_enum_variant_not_found() {
    let source = r#"
enum Role {
  USER
  ADMIN
}

model User {
  id Int @id
  role Role @default(INVALID)
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("variant 'INVALID'"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_default_enum_variant_ok() {
    let source = r#"
enum Role {
  USER
  ADMIN
}

model User {
  id Int @id
  role Role @default(USER)
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();
    assert_eq!(ir.enums.len(), 1);
}

#[test]
fn test_default_type_mismatch_string() {
    let source = r#"
model User {
  id Int @id
  name String @default(123)
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("Type mismatch"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_default_autoincrement_on_string() {
    let source = r#"
model User {
  id String @id @default(autoincrement())
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("autoincrement()") && msg.contains("Int or BigInt"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_default_uuid_on_int() {
    let source = r#"
model User {
  id Int @id @default(uuid())
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("uuid()") && msg.contains("Uuid"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_default_now_on_int() {
    let source = r#"
model User {
  id Int @id @default(now())
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("now()") && msg.contains("DateTime"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_physical_table_name_collision() {
    let source = r#"
model User {
  id Int @id
  @@map("people")
}

model Person {
  id Int @id
  @@map("people")
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("Physical table name 'people'"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_physical_column_name_collision() {
    let source = r#"
model User {
  id Int @id
  userName String @map("name")
  fullName String @map("name")
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("Physical column name 'name'"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_datasource_missing_provider() {
    let source = r#"
datasource db {
  url = "postgres://localhost/test"
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("missing required 'provider'"), "got: {}", msg);
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_datasource_missing_url() {
    let source = r#"
datasource db {
  provider = "postgresql"
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("missing required 'url'"), "got: {}", msg);
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_datasource_unknown_field() {
    let source = r#"
datasource db {
  provider = "postgresql"
  url      = "postgres://localhost/test"
  foo      = "bar"
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("Unknown field 'foo'"), "got: {}", msg);
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_datasource_accepts_direct_url() {
    let source = r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  direct_url = env("DIRECT_DATABASE_URL")
}

model User {
  id Int @id
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).expect("direct_url should validate");
    assert_eq!(
        ir.datasource
            .as_ref()
            .and_then(|ds| ds.direct_url.as_deref()),
        Some("env(DIRECT_DATABASE_URL)")
    );
}

#[test]
fn test_generator_missing_provider() {
    let source = r#"
generator client {
  output = "../generated"
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("missing required 'provider'"), "got: {}", msg);
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_generator_unknown_field() {
    let source = r#"
generator client {
  provider = "nautilus-client-rs"
  output   = "../generated"
  foo      = "bar"
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("Unknown field 'foo'"), "got: {}", msg);
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_generator_output_must_be_string_literal() {
    let source = r#"
generator client {
  provider = "nautilus-client-rs"
  output   = 123
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(
                msg.contains("Generator 'output' must be a string literal"),
                "got: {}",
                msg
            );
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_generator_interface_must_be_string_literal() {
    let source = r#"
generator client {
  provider  = "nautilus-client-rs"
  interface = 123
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(
                msg.contains("Generator 'interface' must be a string literal"),
                "got: {}",
                msg
            );
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_generator_recursive_type_depth_python_only_error_on_rust() {
    let source = r#"
generator client {
  provider             = "nautilus-client-rs"
  recursive_type_depth = 3
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(
                msg.contains("recursive_type_depth") && msg.contains("nautilus-client-py"),
                "got: {}",
                msg
            );
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_generator_recursive_type_depth_python_only_error_on_js() {
    let source = r#"
generator client {
  provider             = "nautilus-client-js"
  recursive_type_depth = 3
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(
                msg.contains("recursive_type_depth") && msg.contains("nautilus-client-py"),
                "got: {}",
                msg
            );
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_generator_recursive_type_depth_valid_for_python() {
    let source = r#"
generator client {
  provider             = "nautilus-client-py"
  output               = "../generated"
  recursive_type_depth = 3
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();
    let gen = ir.generator.as_ref().unwrap();
    assert_eq!(gen.recursive_type_depth, 3);
}

#[test]
fn test_generator_java_fields_validate_for_java_provider() {
    let source = r#"
generator client {
  provider    = "nautilus-client-java"
  output      = "./generated-java"
  package     = "com.acme.db"
  group_id    = "com.acme"
  artifact_id = "db-client"
  mode        = "jar"
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();
    let gen = ir.generator.as_ref().unwrap();
    assert_eq!(gen.provider, "nautilus-client-java");
    assert_eq!(gen.output.as_deref(), Some("./generated-java"));
    assert_eq!(gen.java_package.as_deref(), Some("com.acme.db"));
    assert_eq!(gen.java_group_id.as_deref(), Some("com.acme"));
    assert_eq!(gen.java_artifact_id.as_deref(), Some("db-client"));
    assert_eq!(
        gen.java_mode,
        Some(nautilus_schema::ir::JavaGenerationMode::Jar)
    );
}

#[test]
fn test_generator_java_fields_rejected_for_non_java_provider() {
    let source = r#"
generator client {
  provider = "nautilus-client-py"
  output   = "./generated"
  package  = "com.acme.db"
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(
                msg.contains("package") && msg.contains("nautilus-client-java"),
                "got: {}",
                msg
            );
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_generator_java_requires_output() {
    let source = r#"
generator client {
  provider    = "nautilus-client-java"
  package     = "com.acme.db"
  group_id    = "com.acme"
  artifact_id = "db-client"
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(
                msg.contains("output") && msg.contains("nautilus-client-java"),
                "got: {}",
                msg
            );
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_generator_java_mode_defaults_to_maven() {
    let source = r#"
generator client {
  provider    = "nautilus-client-java"
  output      = "./generated-java"
  package     = "com.acme.db"
  group_id    = "com.acme"
  artifact_id = "db-client"
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();
    let gen = ir.generator.as_ref().unwrap();
    assert_eq!(
        gen.java_mode,
        Some(nautilus_schema::ir::JavaGenerationMode::Maven)
    );
}

#[test]
fn test_generator_java_mode_rejected_for_non_java_provider() {
    let source = r#"
generator client {
  provider = "nautilus-client-js"
  output   = "./generated"
  mode     = "jar"
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("mode") && msg.contains("nautilus-client-java"));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_generator_java_mode_rejects_invalid_value() {
    let source = r#"
generator client {
  provider    = "nautilus-client-java"
  output      = "./generated-java"
  package     = "com.acme.db"
  group_id    = "com.acme"
  artifact_id = "db-client"
  mode        = "uber"
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("mode") && msg.contains("\"maven\", \"jar\""));
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_missing_back_relation_error() {
    let source = r#"
model User {
  id Int @id
}

model Post {
  id       Int  @id
  authorId Int
  author   User @relation(fields: [authorId], references: [id])
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(
                msg.contains("missing an opposite relation field") && msg.contains("Post[]"),
                "unexpected message: {}",
                msg
            );
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_back_relation_present_ok() {
    let source = r#"
model User {
  id    Int    @id

  posts Post[]
}

model Post {
  id       Int  @id
  authorId Int
  author   User @relation(fields: [authorId], references: [id])
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();
    assert_eq!(ir.models.len(), 2);
}

#[test]
fn test_datasource_extensions_populated_in_ir() {
    let source = r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  extensions = [pg_trgm, pgcrypto, "uuid-ossp"]
}

model User { id Int @id }
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).expect("extensions should validate");
    let ds = ir.datasource.expect("datasource IR");
    // Extensions are normalized to lower-case and sorted alphabetically;
    // '_' (0x5F) sorts before 'c' (0x63), so "pg_trgm" < "pgcrypto".
    assert_eq!(ds.extensions, vec!["pg_trgm", "pgcrypto", "uuid-ossp"]);
}

#[test]
fn test_datasource_extensions_rejected_for_mysql() {
    let source = r#"
datasource db {
  provider   = "mysql"
  url        = "mysql://localhost/test"
  extensions = [pg_trgm]
}

model User { id Int @id }
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(
                msg.contains("'extensions' is only supported for the 'postgresql'"),
                "got: {}",
                msg
            );
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_datasource_extensions_duplicate_is_error() {
    let source = r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  extensions = [pg_trgm, "pg_trgm"]
}

model User { id Int @id }
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(
                msg.contains("Duplicate extension 'pg_trgm'"),
                "got: {}",
                msg
            );
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_datasource_extensions_unknown_name_emits_warning() {
    use nautilus_schema::analysis::analyze;
    use nautilus_schema::Severity;

    let source = r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  extensions = [postgis]
}

model User { id Int @id }
"#;
    let result = analyze(source);
    assert!(
        result.ir.is_some(),
        "unknown extension should still validate"
    );
    assert!(
        result
            .diagnostics
            .iter()
            .any(|d| d.severity == Severity::Warning && d.message.contains("postgis")),
        "expected warning for unknown extension, got: {:?}",
        result.diagnostics
    );
}

#[test]
fn test_datasource_extensions_must_be_array() {
    let source = r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  extensions = "pg_trgm"
}

model User { id Int @id }
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(msg.contains("must be an array"), "got: {}", msg);
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_postgres_extension_backed_types_rejected_for_mysql() {
    let source = r#"
datasource db {
  provider = "mysql"
  url      = "mysql://localhost/test"
}

model User {
  id    Int    @id
  email Citext
}
"#;
    let ast = parse(source).unwrap();
    let err = validate_schema(ast).unwrap_err();
    match err {
        SchemaError::Validation(msg, _) => {
            assert!(
                msg.contains("Citext") && msg.contains("provider 'mysql'"),
                "got: {}",
                msg
            );
        }
        _ => panic!("Expected validation error"),
    }
}

#[test]
fn test_postgres_extension_backed_types_emit_missing_extension_warning() {
    use nautilus_schema::analysis::analyze;
    use nautilus_schema::Severity;

    let source = r#"
datasource db {
  provider = "postgresql"
  url      = "postgres://localhost/test"
}

model User {
  id    Int    @id
  email Citext
}
"#;
    let result = analyze(source);
    assert!(result.ir.is_some(), "schema should still validate");
    assert!(
        result.diagnostics.iter().any(|d| {
            d.severity == Severity::Warning
                && d.message.contains("Citext")
                && d.message.contains("extensions = [citext]")
        }),
        "expected missing-extension warning, got: {:?}",
        result.diagnostics
    );
}
