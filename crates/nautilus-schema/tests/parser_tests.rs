//! Integration tests for the schema parser.

mod common;

use common::parse_schema as parse;
use nautilus_schema::ast::*;

#[test]
fn test_parse_multiple_models() {
    let source = r#"
model User {
  id Int @id
}

model Post {
  id Int @id
}

model Comment {
  id Int @id
}
"#;

    let schema = parse(source).unwrap();
    assert_eq!(schema.models().count(), 3);
}

#[test]
fn test_parse_composite_primary_key() {
    let source = r#"
model UserRole {
  userId Int
  roleId Int
  
  @@id([userId, roleId])
}
"#;

    let schema = parse(source).unwrap();
    let model = schema.models().next().unwrap();
    assert!(model.has_composite_key());

    match &model.attributes[0] {
        ModelAttribute::Id(fields) => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].value, "userId");
            assert_eq!(fields[1].value, "roleId");
        }
        _ => panic!("Expected @@id attribute"),
    }
}

#[test]
fn test_parse_composite_unique() {
    let source = r#"
model User {
  email String
  username String
  
  @@unique([email, username])
}
"#;

    let schema = parse(source).unwrap();
    let model = schema.models().next().unwrap();

    match &model.attributes[0] {
        ModelAttribute::Unique(fields) => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].value, "email");
            assert_eq!(fields[1].value, "username");
        }
        _ => panic!("Expected @@unique attribute"),
    }
}

#[test]
fn test_parse_index() {
    let source = r#"
model Post {
  title String
  createdAt DateTime
  
  @@index([createdAt, title])
}
"#;

    let schema = parse(source).unwrap();
    let model = schema.models().next().unwrap();

    match &model.attributes[0] {
        ModelAttribute::Index { fields, .. } => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].value, "createdAt");
            assert_eq!(fields[1].value, "title");
        }
        _ => panic!("Expected @@index attribute"),
    }
}

#[test]
fn test_parse_pgvector_index_options() {
    let source = r#"
model Embedding {
  id        Int @id
  embedding Vector(3)

  @@index([embedding], type: Hnsw, opclass: vector_cosine_ops, m: 16, ef_construction: 64)
}
"#;

    let schema = parse(source).unwrap();
    let model = schema.models().next().unwrap();

    match &model.attributes[0] {
        ModelAttribute::Index {
            fields,
            index_type,
            opclass,
            m,
            ef_construction,
            lists,
            ..
        } => {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].value, "embedding");
            assert_eq!(
                index_type.as_ref().map(|value| value.value.as_str()),
                Some("Hnsw")
            );
            assert_eq!(
                opclass.as_ref().map(|value| value.value.as_str()),
                Some("vector_cosine_ops")
            );
            assert_eq!(*m, Some(16));
            assert_eq!(*ef_construction, Some(64));
            assert_eq!(*lists, None);
        }
        _ => panic!("Expected @@index attribute"),
    }
}

#[test]
fn test_parse_all_scalar_types() {
    let source = r#"
model AllTypes {
  str      String
  bool     Boolean
  int      Int
  bigInt   BigInt
  float    Float
  decimal  Decimal(10, 2)
  dateTime DateTime
  bytes    Bytes
  json     Json
  uuid     Uuid
  citext   Citext
  hstore   Hstore
  ltree    Ltree
  vector   Vector(1536)
}
"#;

    let schema = parse(source).unwrap();
    let model = schema.models().next().unwrap();
    assert_eq!(model.fields.len(), 14);

    assert!(matches!(model.fields[0].field_type, FieldType::String));
    assert!(matches!(model.fields[1].field_type, FieldType::Boolean));
    assert!(matches!(model.fields[2].field_type, FieldType::Int));
    assert!(matches!(model.fields[3].field_type, FieldType::BigInt));
    assert!(matches!(model.fields[4].field_type, FieldType::Float));
    assert!(matches!(
        model.fields[5].field_type,
        FieldType::Decimal {
            precision: 10,
            scale: 2
        }
    ));
    assert!(matches!(model.fields[6].field_type, FieldType::DateTime));
    assert!(matches!(model.fields[7].field_type, FieldType::Bytes));
    assert!(matches!(model.fields[8].field_type, FieldType::Json));
    assert!(matches!(model.fields[9].field_type, FieldType::Uuid));
    assert!(matches!(model.fields[10].field_type, FieldType::Citext));
    assert!(matches!(model.fields[11].field_type, FieldType::Hstore));
    assert!(matches!(model.fields[12].field_type, FieldType::Ltree));
    assert!(matches!(
        model.fields[13].field_type,
        FieldType::Vector { dimension: 1536 }
    ));
}

#[test]
fn test_parse_default_expressions() {
    let source = r#"
model User {
  id        Int      @id @default(autoincrement())
  uuid      Uuid     @default(uuid())
  createdAt DateTime @default(now())
  role      String   @default("USER")
  count     Int      @default(0)
  active    Boolean  @default(true)
}
"#;

    let schema = parse(source).unwrap();
    let model = schema.models().next().unwrap();

    for field in &model.fields {
        let default_attr = field
            .attributes
            .iter()
            .find(|a| matches!(a, FieldAttribute::Default(..)));
        assert!(
            default_attr.is_some(),
            "Field {} missing @default",
            field.name.value
        );
    }
}

#[test]
fn test_parse_optional_and_array_fields() {
    let source = r#"
model User {
  optional String?
  array    String[]
  required String
}
"#;

    let schema = parse(source).unwrap();
    let model = schema.models().next().unwrap();

    assert!(model.fields[0].is_optional());
    assert!(!model.fields[0].is_array());

    assert!(!model.fields[1].is_optional());
    assert!(model.fields[1].is_array());

    assert!(!model.fields[2].is_optional());
    assert!(!model.fields[2].is_array());
}

#[test]
fn test_parse_relation_with_all_options() {
    let source = r#"
model Post {
  authorId Int
  author   User @relation(fields: [authorId], references: [id], onDelete: Cascade, onUpdate: Restrict)
}
"#;

    let schema = parse(source).unwrap();
    let model = schema.models().next().unwrap();
    let author_field = model.find_field("author").unwrap();

    match &author_field.attributes[0] {
        FieldAttribute::Relation {
            fields,
            references,
            on_delete,
            on_update,
            ..
        } => {
            assert_eq!(fields.as_ref().unwrap().len(), 1);
            assert_eq!(fields.as_ref().unwrap()[0].value, "authorId");
            assert_eq!(references.as_ref().unwrap().len(), 1);
            assert_eq!(references.as_ref().unwrap()[0].value, "id");
            assert_eq!(*on_delete, Some(ReferentialAction::Cascade));
            assert_eq!(*on_update, Some(ReferentialAction::Restrict));
        }
        _ => panic!("Expected relation attribute"),
    }
}

#[test]
fn test_parse_relation_with_name() {
    let source = r#"
model Post {
  authorId   Int
  reviewerId Int
  author     User @relation(name: "AuthoredPosts", fields: [authorId], references: [id])
  reviewer   User @relation(name: "ReviewedPosts", fields: [reviewerId], references: [id])
}
"#;

    let schema = parse(source).unwrap();
    let model = schema.models().next().unwrap();

    let author_field = model.find_field("author").unwrap();
    match &author_field.attributes[0] {
        FieldAttribute::Relation {
            name,
            fields,
            references,
            ..
        } => {
            assert_eq!(name.as_ref().unwrap(), "AuthoredPosts");
            assert_eq!(fields.as_ref().unwrap()[0].value, "authorId");
            assert_eq!(references.as_ref().unwrap()[0].value, "id");
        }
        _ => panic!("Expected relation attribute"),
    }

    let reviewer_field = model.find_field("reviewer").unwrap();
    match &reviewer_field.attributes[0] {
        FieldAttribute::Relation {
            name,
            fields,
            references,
            ..
        } => {
            assert_eq!(name.as_ref().unwrap(), "ReviewedPosts");
            assert_eq!(fields.as_ref().unwrap()[0].value, "reviewerId");
            assert_eq!(references.as_ref().unwrap()[0].value, "id");
        }
        _ => panic!("Expected relation attribute"),
    }
}

#[test]
fn test_model_helper_methods() {
    let source = r#"
model User {
  id    Int    @id @map("user_id")
  email String @unique
  posts Post[]
  
  @@map("users")
}
"#;

    let schema = parse(source).unwrap();
    let model = schema.models().next().unwrap();

    assert_eq!(model.table_name(), "users");

    assert!(model.find_field("id").is_some());
    assert!(model.find_field("nonexistent").is_none());

    let relation_fields: Vec<_> = model.relation_fields().collect();
    assert_eq!(relation_fields.len(), 1);
    assert_eq!(relation_fields[0].name.value, "posts");

    assert!(!model.has_composite_key());
}

#[test]
fn test_field_helper_methods() {
    let source = r#"
model User {
  id       Int     @id @map("user_id")
  email    String  @unique
  optional String?
  array    String[]
}
"#;

    let schema = parse(source).unwrap();
    let model = schema.models().next().unwrap();

    let id_field = model.find_field("id").unwrap();
    assert_eq!(id_field.column_name(), "user_id");
    assert!(id_field.find_attribute("id").is_some());
    assert!(id_field.find_attribute("nonexistent").is_none());

    let optional_field = model.find_field("optional").unwrap();
    assert!(optional_field.is_optional());
    assert!(!optional_field.is_array());

    let array_field = model.find_field("array").unwrap();
    assert!(!array_field.is_optional());
    assert!(array_field.is_array());
}

#[test]
fn test_error_recovery_continues_parsing() {
    let source = r#"
model Bad {
  id Int @id
  // Missing closing brace - this would cause an error

model Good {
  id Int @id
}
"#;

    let schema = parse(source).unwrap();

    assert!(schema.models().any(|m| m.name.value == "Good"));
}

#[test]
fn test_schema_with_comments() {
    let source = r#"
// This is a datasource
datasource db {
  provider = "postgresql"
}

/* Multi-line
   comment */
model User {
  id Int @id // inline comment
}
"#;

    let schema = parse(source).unwrap();
    assert_eq!(schema.declarations.len(), 2);
}
