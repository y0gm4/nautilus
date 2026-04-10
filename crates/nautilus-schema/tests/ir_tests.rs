mod common;

use common::parse_schema as parse;
use nautilus_schema::{ir::*, validate_schema};

#[test]
fn test_ir_physical_names() {
    let source = r#"
model User {
  id Int @id @map("user_id")
  @@map("users")
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();

    let user_model = ir.models.get("User").unwrap();
    assert_eq!(user_model.logical_name, "User");
    assert_eq!(user_model.db_name, "users");

    let id_field = &user_model.fields[0];
    assert_eq!(id_field.logical_name, "id");
    assert_eq!(id_field.db_name, "user_id");
}

#[test]
fn test_ir_resolved_enum_type() {
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

    let user_model = ir.models.get("User").unwrap();
    let role_field = user_model
        .fields
        .iter()
        .find(|f| f.logical_name == "role")
        .unwrap();

    match &role_field.field_type {
        ResolvedFieldType::Enum { enum_name } => {
            assert_eq!(enum_name, "Role");
        }
        _ => panic!("Expected enum type"),
    }

    match role_field.default_value.as_ref().unwrap() {
        DefaultValue::EnumVariant(variant) => {
            assert_eq!(variant, "USER");
        }
        _ => panic!("Expected enum variant default"),
    }
}

#[test]
fn test_ir_resolved_relation_type() {
    let source = r#"
model User {
  id Int @id @unique

  posts Post[]
}

model Post {
  id Int @id
  userId Int
  user User @relation(fields: [userId], references: [id], onDelete: Cascade)
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();

    let post_model = ir.models.get("Post").unwrap();
    let user_field = post_model
        .fields
        .iter()
        .find(|f| f.logical_name == "user")
        .unwrap();

    match &user_field.field_type {
        ResolvedFieldType::Relation(rel) => {
            assert_eq!(rel.target_model, "User");
            assert_eq!(rel.fields, vec!["userId"]);
            assert_eq!(rel.references, vec!["id"]);
            assert_eq!(
                rel.on_delete,
                Some(nautilus_schema::ast::ReferentialAction::Cascade)
            );
        }
        _ => panic!("Expected relation type"),
    }
}

#[test]
fn test_ir_resolved_composite_relation_type() {
    let source = r#"
model User {
  firstName String
  lastName  String

  posts Post[]

  @@id([firstName, lastName])
}

model Post {
  id            Int    @id
  userFirstName String
  userLastName  String
  user User @relation(fields: [userFirstName, userLastName], references: [firstName, lastName], onDelete: Cascade)
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();

    let post_model = ir.models.get("Post").unwrap();
    let user_field = post_model
        .fields
        .iter()
        .find(|f| f.logical_name == "user")
        .unwrap();

    match &user_field.field_type {
        ResolvedFieldType::Relation(rel) => {
            assert_eq!(rel.target_model, "User");
            assert_eq!(rel.fields, vec!["userFirstName", "userLastName"]);
            assert_eq!(rel.references, vec!["firstName", "lastName"]);
            assert_eq!(
                rel.on_delete,
                Some(nautilus_schema::ast::ReferentialAction::Cascade)
            );
        }
        _ => panic!("Expected relation type"),
    }
}

#[test]
fn test_ir_primary_key_single() {
    let source = r#"
model User {
  id Int @id
  email String
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();

    let user_model = ir.models.get("User").unwrap();
    match &user_model.primary_key {
        PrimaryKeyIr::Single(field) => {
            assert_eq!(field, "id");
        }
        _ => panic!("Expected single primary key"),
    }
}

#[test]
fn test_ir_primary_key_composite() {
    let source = r#"
model User {
  userId Int
  accountId Int
  @@id([userId, accountId])
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();

    let user_model = ir.models.get("User").unwrap();
    match &user_model.primary_key {
        PrimaryKeyIr::Composite(fields) => {
            assert_eq!(fields, &vec!["userId", "accountId"]);
        }
        _ => panic!("Expected composite primary key"),
    }
}

#[test]
fn test_ir_unique_constraints() {
    let source = r#"
model User {
  id Int @id
  email String @unique
  username String
  @@unique([username, email])
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();

    let user_model = ir.models.get("User").unwrap();
    assert_eq!(user_model.unique_constraints.len(), 2);

    let email_unique = user_model
        .unique_constraints
        .iter()
        .find(|c| c.fields == vec!["email"])
        .expect("email unique constraint not found");
    assert_eq!(email_unique.fields, vec!["email"]);

    let composite_unique = user_model
        .unique_constraints
        .iter()
        .find(|c| c.fields.len() == 2)
        .expect("composite unique constraint not found");
    assert_eq!(composite_unique.fields, vec!["username", "email"]);
}

#[test]
fn test_ir_indexes() {
    let source = r#"
model User {
  id Int @id
  email String
  name String
  @@index([email])
  @@index([name, email])
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();

    let user_model = ir.models.get("User").unwrap();
    assert_eq!(user_model.indexes.len(), 2);

    let email_index = user_model
        .indexes
        .iter()
        .find(|i| i.fields == vec!["email"])
        .expect("email index not found");
    assert_eq!(email_index.fields, vec!["email"]);

    let composite_index = user_model
        .indexes
        .iter()
        .find(|i| i.fields.len() == 2)
        .expect("composite index not found");
    assert_eq!(composite_index.fields, vec!["name", "email"]);
}

#[test]
fn test_ir_field_modifiers() {
    let source = r#"
model User {
  id Int @id
  required String
  optional String?
  array String[]
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();

    let user_model = ir.models.get("User").unwrap();

    let required = user_model.find_field("required").unwrap();
    assert!(required.is_required);
    assert!(!required.is_array);

    let optional = user_model.find_field("optional").unwrap();
    assert!(!optional.is_required);
    assert!(!optional.is_array);

    let array = user_model.find_field("array").unwrap();
    assert!(!array.is_required);
    assert!(array.is_array);
}

#[test]
fn test_ir_default_values() {
    let source = r#"
model User {
  id Int @id @default(autoincrement())
  uuid Uuid @default(uuid())
  createdAt DateTime @default(now())
  name String @default("John")
  count Int @default(42)
  active Boolean @default(true)
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();

    let user_model = ir.models.get("User").unwrap();

    let id = user_model.find_field("id").unwrap();
    match id.default_value.as_ref().unwrap() {
        DefaultValue::Function(f) => assert_eq!(f.name, "autoincrement"),
        _ => panic!("Expected function default"),
    }

    let uuid_field = user_model.find_field("uuid").unwrap();
    match uuid_field.default_value.as_ref().unwrap() {
        DefaultValue::Function(f) => assert_eq!(f.name, "uuid"),
        _ => panic!("Expected function default"),
    }

    let created_at = user_model.find_field("createdAt").unwrap();
    match created_at.default_value.as_ref().unwrap() {
        DefaultValue::Function(f) => assert_eq!(f.name, "now"),
        _ => panic!("Expected function default"),
    }

    let name = user_model.find_field("name").unwrap();
    match name.default_value.as_ref().unwrap() {
        DefaultValue::String(s) => assert_eq!(s, "John"),
        _ => panic!("Expected string default"),
    }

    let count = user_model.find_field("count").unwrap();
    match count.default_value.as_ref().unwrap() {
        DefaultValue::Number(n) => assert_eq!(n, "42"),
        _ => panic!("Expected number default"),
    }

    let active = user_model.find_field("active").unwrap();
    match active.default_value.as_ref().unwrap() {
        DefaultValue::Boolean(b) => assert!(*b),
        _ => panic!("Expected boolean default"),
    }
}

#[test]
fn test_ir_datasource() {
    let source = r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost"
  direct_url = "postgres://localhost/direct"
}

model User {
  id Int @id
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();

    let datasource = ir.datasource.as_ref().unwrap();
    assert_eq!(datasource.name, "db");
    assert_eq!(datasource.provider, "postgresql");
    assert_eq!(datasource.url, "postgres://localhost");
    assert_eq!(
        datasource.direct_url.as_deref(),
        Some("postgres://localhost/direct")
    );
}

#[test]
fn test_ir_generator() {
    let source = r#"
generator client {
  provider = "nautilus-client-rs"
  output = "../generated"
}

model User {
  id Int @id
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();

    let generator = ir.generator.as_ref().unwrap();
    assert_eq!(generator.name, "client");
    assert_eq!(generator.provider, "nautilus-client-rs");
    assert_eq!(generator.output.as_ref().unwrap(), "../generated");
}

#[test]
fn test_ir_enum() {
    let source = r#"
enum Role {
  USER
  ADMIN
  MODERATOR
}

model User {
  id Int @id
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();

    let role_enum = ir.enums.get("Role").unwrap();
    assert_eq!(role_enum.logical_name, "Role");
    assert_eq!(role_enum.variants, vec!["USER", "ADMIN", "MODERATOR"]);
    assert!(role_enum.has_variant("USER"));
    assert!(role_enum.has_variant("ADMIN"));
    assert!(!role_enum.has_variant("GUEST"));
}

#[test]
fn test_ir_scalar_types() {
    let source = r#"
model Data {
  id Int @id
  name String
  active Boolean
  bigNum BigInt
  price Float
  amount Decimal(10, 2)
  createdAt DateTime
  data Bytes
  metadata Json
  uuid Uuid
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();

    let data_model = ir.models.get("Data").unwrap();

    let check_scalar = |field_name: &str, expected: ScalarType| {
        let field = data_model.find_field(field_name).unwrap();
        match &field.field_type {
            ResolvedFieldType::Scalar(t) => assert_eq!(*t, expected),
            _ => panic!("Expected scalar type for {}", field_name),
        }
    };

    check_scalar("id", ScalarType::Int);
    check_scalar("name", ScalarType::String);
    check_scalar("active", ScalarType::Boolean);
    check_scalar("bigNum", ScalarType::BigInt);
    check_scalar("price", ScalarType::Float);
    check_scalar(
        "amount",
        ScalarType::Decimal {
            precision: 10,
            scale: 2,
        },
    );
    check_scalar("createdAt", ScalarType::DateTime);
    check_scalar("data", ScalarType::Bytes);
    check_scalar("metadata", ScalarType::Json);
    check_scalar("uuid", ScalarType::Uuid);
}

#[test]
fn test_ir_phase_9_1_2_schema() {
    let source = r#"
datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

generator client {
  provider = "nautilus-client-rs"
  output   = "../crates/nautilus-connector/src/generated"
}

enum Role {
  USER
  ADMIN
}

model User {
  id        Uuid     @id @default(uuid()) @map("user_id")
  email     String   @unique
  role      Role     @default(USER)
  createdAt DateTime @default(now()) @map("created_at")

  posts     Post[]

  @@map("users")
}

model Post {
  id        BigInt   @id @default(autoincrement())
  userId    Uuid     @map("user_id")
  title     String
  rating    Decimal(10, 2)
  createdAt DateTime @default(now()) @map("created_at")

  user      User     @relation(fields: [userId], references: [id], onUpdate: Cascade, onDelete: Cascade)

  @@map("posts")
}
"#;
    let ast = parse(source).unwrap();
    let ir = validate_schema(ast).unwrap();

    assert!(ir.datasource.is_some());
    let datasource = ir.datasource.as_ref().unwrap();
    assert_eq!(datasource.provider, "postgresql");

    assert!(ir.generator.is_some());

    assert_eq!(ir.enums.len(), 1);
    assert!(ir.enums.contains_key("Role"));

    assert_eq!(ir.models.len(), 2);

    let user = ir.models.get("User").unwrap();
    assert_eq!(user.logical_name, "User");
    assert_eq!(user.db_name, "users");
    assert_eq!(user.fields.len(), 5);

    let id_field = user.find_field("id").unwrap();
    assert_eq!(id_field.db_name, "user_id");
    assert!(matches!(
        &id_field.field_type,
        ResolvedFieldType::Scalar(ScalarType::Uuid)
    ));

    let email_field = user.find_field("email").unwrap();
    assert!(email_field.is_unique);

    let role_field = user.find_field("role").unwrap();
    assert!(matches!(
        &role_field.field_type,
        ResolvedFieldType::Enum { .. }
    ));

    let post = ir.models.get("Post").unwrap();
    assert_eq!(post.logical_name, "Post");
    assert_eq!(post.db_name, "posts");

    let rating_field = post.find_field("rating").unwrap();
    assert!(matches!(
        &rating_field.field_type,
        ResolvedFieldType::Scalar(ScalarType::Decimal {
            precision: 10,
            scale: 2
        })
    ));

    let user_field = post.find_field("user").unwrap();
    match &user_field.field_type {
        ResolvedFieldType::Relation(rel) => {
            assert_eq!(rel.target_model, "User");
            assert_eq!(rel.fields, vec!["userId"]);
            assert_eq!(rel.references, vec!["id"]);
        }
        _ => panic!("Expected relation type"),
    }
}
