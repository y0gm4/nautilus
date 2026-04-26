use super::*;
use nautilus_core::{BinaryOp, Expr, OrderDir, Value, VectorMetric};
use nautilus_schema::validate_schema_source;
use serde_json::json;

fn parse_ir(source: &str) -> nautilus_schema::ir::SchemaIr {
    validate_schema_source(source)
        .expect("validation failed")
        .ir
}

fn user_query_context(
    source: &str,
) -> (
    RelationMap,
    FieldTypeMap,
    HashMap<String, nautilus_schema::ir::ModelIr>,
) {
    let ir = parse_ir(source);
    let user_model = ir.models.get("User").expect("User model missing");
    (
        crate::handlers::build_relation_map(user_model, &ir.models)
            .expect("relation map should build"),
        crate::handlers::build_field_type_map(user_model),
        ir.models,
    )
}

fn expr_contains_column(expr: &Expr, expected: &str) -> bool {
    match expr {
        Expr::Column(name) => name == expected,
        Expr::Binary { left, right, .. } => {
            expr_contains_column(left, expected) || expr_contains_column(right, expected)
        }
        Expr::Not(inner) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            expr_contains_column(inner, expected)
        }
        Expr::Filter { expr, predicate } => {
            expr_contains_column(expr, expected) || expr_contains_column(predicate, expected)
        }
        Expr::FunctionCall { args, .. } | Expr::List(args) => {
            args.iter().any(|arg| expr_contains_column(arg, expected))
        }
        Expr::Relation { relation, .. } => expr_contains_column(&relation.filter, expected),
        Expr::Exists(select) | Expr::NotExists(select) | Expr::ScalarSubquery(select) => {
            select_contains_column(select, expected)
        }
        Expr::CaseWhen { condition, then } => {
            expr_contains_column(condition, expected) || expr_contains_column(then, expected)
        }
        Expr::Param(_) | Expr::Literal(_) | Expr::Star => false,
    }
}

fn select_contains_column(select: &nautilus_core::Select, expected: &str) -> bool {
    select
        .filter
        .as_ref()
        .is_some_and(|filter| expr_contains_column(filter, expected))
}

#[test]
fn vector_fields_reject_classic_order_by() {
    let (relations, field_types, models) = user_query_context(
        r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  extensions = [vector]
}

model User {
  id        Int @id
  embedding Vector(3)
}
"#,
    );
    let args = json!({ "orderBy": [{ "embedding": "asc" }] });

    let err =
        match QueryArgs::parse_with_context(Some(args), &relations, &field_types, Some(&models)) {
            Ok(_) => panic!("expected Vector orderBy to be rejected"),
            Err(err) => err,
        };
    assert!(err
        .to_string()
        .contains("cannot be used with classic orderBy"));
}

#[test]
fn vector_fields_reject_range_filters() {
    let (_, field_types, _) = user_query_context(
        r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  extensions = [vector]
}

model User {
  id        Int @id
  embedding Vector(3)
}
"#,
    );
    let filter = json!({ "embedding": { "gt": [0.1, 0.2, 0.3] } });

    let err = parse_where_filter(&filter, &RelationMap::new(), &field_types, None).unwrap_err();
    assert!(err.to_string().contains("not supported for Vector"));
}

#[test]
fn vector_nearest_query_parses_with_metric_and_take() {
    let (relations, field_types, models) = user_query_context(
        r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  extensions = [vector]
}

model User {
  id        Int @id
  embedding Vector(3)
}
"#,
    );
    let args = json!({
        "nearest": {
            "field": "embedding",
            "query": [0.1, 0.2, 0.3],
            "metric": "cosine"
        },
        "take": 5
    });

    let parsed = QueryArgs::parse_with_context(Some(args), &relations, &field_types, Some(&models))
        .expect("nearest query should parse");

    let nearest = parsed.nearest.expect("nearest query missing");
    assert_eq!(nearest.field, "embedding");
    assert_eq!(nearest.metric, VectorMetric::Cosine);
    assert_eq!(nearest.query, vec![0.1, 0.2, 0.3]);
    assert_eq!(parsed.take, Some(5));
}

#[test]
fn vector_nearest_query_requires_positive_take() {
    let (relations, field_types, models) = user_query_context(
        r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  extensions = [vector]
}

model User {
  id        Int @id
  embedding Vector(3)
}
"#,
    );
    let args = json!({
        "nearest": {
            "field": "embedding",
            "query": [0.1, 0.2, 0.3],
            "metric": "l2"
        }
    });

    let err = QueryArgs::parse_with_context(Some(args), &relations, &field_types, Some(&models))
        .expect_err("nearest without take should fail");
    assert!(err.to_string().contains("requires a positive 'take'"));
}

#[test]
fn vector_nearest_query_requires_vector_field() {
    let (relations, field_types, models) = user_query_context(
        r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  extensions = [vector]
}

model User {
  id    Int    @id
  email String
}
"#,
    );
    let args = json!({
        "nearest": {
            "field": "email",
            "query": [0.1, 0.2, 0.3],
            "metric": "cosine"
        },
        "take": 3
    });

    let err = QueryArgs::parse_with_context(Some(args), &relations, &field_types, Some(&models))
        .expect_err("nearest on non-vector field should fail");
    assert!(err.to_string().contains("must reference a Vector field"));
}

fn expr_contains_enum(expr: &Expr, expected_value: &str, expected_type: &str) -> bool {
    match expr {
        Expr::Param(Value::Enum { value, type_name }) => {
            value == expected_value && type_name == expected_type
        }
        Expr::Binary { left, right, .. } => {
            expr_contains_enum(left, expected_value, expected_type)
                || expr_contains_enum(right, expected_value, expected_type)
        }
        Expr::Not(inner) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            expr_contains_enum(inner, expected_value, expected_type)
        }
        Expr::Filter { expr, predicate } => {
            expr_contains_enum(expr, expected_value, expected_type)
                || expr_contains_enum(predicate, expected_value, expected_type)
        }
        Expr::FunctionCall { args, .. } | Expr::List(args) => args
            .iter()
            .any(|arg| expr_contains_enum(arg, expected_value, expected_type)),
        Expr::Relation { relation, .. } => {
            expr_contains_enum(&relation.filter, expected_value, expected_type)
        }
        Expr::Exists(select) | Expr::NotExists(select) | Expr::ScalarSubquery(select) => select
            .filter
            .as_ref()
            .is_some_and(|filter| expr_contains_enum(filter, expected_value, expected_type)),
        Expr::CaseWhen { condition, then } => {
            expr_contains_enum(condition, expected_value, expected_type)
                || expr_contains_enum(then, expected_value, expected_type)
        }
        Expr::Column(_) | Expr::Param(_) | Expr::Literal(_) | Expr::Star => false,
    }
}

fn expr_contains_subquery_table(expr: &Expr, expected_table: &str) -> bool {
    match expr {
        Expr::Relation { relation, .. } => {
            relation.target_table == expected_table
                || expr_contains_subquery_table(&relation.filter, expected_table)
        }
        Expr::Exists(select) | Expr::NotExists(select) | Expr::ScalarSubquery(select) => {
            select.table == expected_table
                || select
                    .filter
                    .as_ref()
                    .is_some_and(|filter| expr_contains_subquery_table(filter, expected_table))
        }
        Expr::Binary { left, right, .. } => {
            expr_contains_subquery_table(left, expected_table)
                || expr_contains_subquery_table(right, expected_table)
        }
        Expr::Not(inner) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            expr_contains_subquery_table(inner, expected_table)
        }
        Expr::Filter { expr, predicate } => {
            expr_contains_subquery_table(expr, expected_table)
                || expr_contains_subquery_table(predicate, expected_table)
        }
        Expr::FunctionCall { args, .. } | Expr::List(args) => args
            .iter()
            .any(|arg| expr_contains_subquery_table(arg, expected_table)),
        Expr::CaseWhen { condition, then } => {
            expr_contains_subquery_table(condition, expected_table)
                || expr_contains_subquery_table(then, expected_table)
        }
        Expr::Column(_) | Expr::Param(_) | Expr::Literal(_) | Expr::Star => false,
    }
}

#[test]
fn nested_include_uses_child_field_mapping_for_where_and_order_by() {
    let schema = r#"
model User {
  id    Int    @id @default(autoincrement())
  posts Post[]
}

model Post {
  id        Int      @id @default(autoincrement()) @map("post_id")
  published Boolean  @map("is_published")
  createdAt DateTime @map("created_at")
  authorId  Int      @map("author_id")
  author    User     @relation(fields: [authorId], references: [id])

  @@map("blog_posts")
}
"#;
    let (relations, field_types, models) = user_query_context(schema);
    let args = json!({
        "include": {
            "posts": {
                "where": { "published": true },
                "orderBy": [{ "createdAt": "desc" }]
            }
        }
    });

    let query_args =
        QueryArgs::parse_with_context(Some(args), &relations, &field_types, Some(&models))
            .expect("query args should parse");

    let posts = query_args
        .include
        .get("posts")
        .expect("posts include missing");
    assert_eq!(posts.order_by.len(), 1);
    assert_eq!(posts.order_by[0].column, "created_at");
    assert_eq!(posts.order_by[0].direction, OrderDir::Desc);

    match posts.filter.as_ref().expect("posts include filter missing") {
        Expr::Binary { left, op, right } => {
            assert_eq!(*op, BinaryOp::Eq);
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(name), Expr::Param(Value::Bool(true))) => {
                    assert_eq!(name, "blog_posts__is_published");
                }
                other => panic!("unexpected filter shape: {other:?}"),
            }
        }
        other => panic!("unexpected include filter: {other:?}"),
    }
}

#[test]
fn nested_include_recurses_with_target_model_context() {
    let schema = r#"
model User {
  id    Int    @id @default(autoincrement())
  posts Post[]
}

model Post {
  id        Int       @id @default(autoincrement()) @map("post_id")
  authorId  Int       @map("author_id")
  author    User      @relation(fields: [authorId], references: [id])
  comments  Comment[]

  @@map("blog_posts")
}

model Comment {
  id        Int      @id @default(autoincrement()) @map("comment_id")
  bodyText  String   @map("body_text")
  createdAt DateTime @map("created_at")
  postId    Int      @map("post_id")
  post      Post     @relation(fields: [postId], references: [id])

  @@map("comments")
}
"#;
    let (relations, field_types, models) = user_query_context(schema);
    let args = json!({
        "include": {
            "posts": {
                "include": {
                    "comments": {
                        "where": { "bodyText": { "contains": "hello" } },
                        "orderBy": [{ "createdAt": "asc" }]
                    }
                }
            }
        }
    });

    let query_args =
        QueryArgs::parse_with_context(Some(args), &relations, &field_types, Some(&models))
            .expect("query args should parse");

    let comments = query_args
        .include
        .get("posts")
        .and_then(|posts| posts.nested.get("comments"))
        .expect("comments include missing");

    assert_eq!(comments.order_by.len(), 1);
    assert_eq!(comments.order_by[0].column, "created_at");
    assert_eq!(comments.order_by[0].direction, OrderDir::Asc);

    match comments
        .filter
        .as_ref()
        .expect("comments include filter missing")
    {
        Expr::Binary { left, op, right } => {
            assert_eq!(*op, BinaryOp::Like);
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(name), Expr::Param(Value::String(pattern))) => {
                    assert_eq!(name, "comments__body_text");
                    assert_eq!(pattern, "%hello%");
                }
                other => panic!("unexpected nested filter shape: {other:?}"),
            }
        }
        other => panic!("unexpected nested include filter: {other:?}"),
    }
}

#[test]
fn select_and_include_are_mutually_exclusive() {
    let schema = r#"
model User {
  id    Int    @id @default(autoincrement())
  posts Post[]
}

model Post {
  id       Int    @id @default(autoincrement())
  authorId Int
  author   User   @relation(fields: [authorId], references: [id])
}
"#;
    let (relations, field_types, models) = user_query_context(schema);
    let args = json!({
        "select": { "id": true },
        "include": { "posts": true }
    });

    match QueryArgs::parse_with_context(Some(args), &relations, &field_types, Some(&models)) {
        Err(ProtocolError::InvalidParams(message)) => {
            assert!(
                message.contains("'select' and 'include' cannot be used together"),
                "unexpected error message: {message}"
            );
        }
        Ok(_) => panic!("select + include should be rejected"),
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn relation_filters_use_child_schema_context_for_mapped_fields_enums_and_nested_relations() {
    let schema = r#"
enum PostStatus {
  DRAFT
  PUBLISHED
}

model User {
  id    Int    @id @default(autoincrement())
  posts Post[]
}

model Post {
  id        Int        @id @default(autoincrement()) @map("post_id")
  status    PostStatus @map("post_status")
  authorId  Int        @map("author_id")
  author    User       @relation(fields: [authorId], references: [id])
  comments  Comment[]

  @@map("blog_posts")
}

model Comment {
  id       Int      @id @default(autoincrement()) @map("comment_id")
  bodyText String   @map("body_text")
  postId   Int      @map("post_id")
  post     Post     @relation(fields: [postId], references: [id])
  reports  Report[]

  @@map("comments")
}

model Report {
  id        Int     @id @default(autoincrement()) @map("report_id")
  kindText  String  @map("kind_text")
  commentId Int     @map("comment_id")
  comment   Comment @relation(fields: [commentId], references: [id])

  @@map("comment_reports")
}
"#;
    let (relations, field_types, models) = user_query_context(schema);
    let args = json!({
        "where": {
            "posts": {
                "some": {
                    "status": "PUBLISHED",
                    "comments": {
                        "every": {
                            "reports": {
                                "none": {
                                    "kindText": { "contains": "spam" }
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    let query_args =
        QueryArgs::parse_with_context(Some(args), &relations, &field_types, Some(&models))
            .expect("query args should parse");

    let filter = query_args.filter.expect("relation filter missing");
    assert!(matches!(filter, Expr::Exists(_)));
    assert!(
        expr_contains_column(&filter, "blog_posts__post_status"),
        "expected mapped child column qualification in relation filter: {filter:?}"
    );
    assert!(
        expr_contains_enum(&filter, "PUBLISHED", "poststatus"),
        "expected enum coercion in relation filter: {filter:?}"
    );
    assert!(
        expr_contains_subquery_table(&filter, "comments"),
        "expected nested every() relation subquery: {filter:?}"
    );
    assert!(
        expr_contains_subquery_table(&filter, "comment_reports"),
        "expected nested none() relation subquery: {filter:?}"
    );
    assert!(
        expr_contains_column(&filter, "comment_reports__kind_text"),
        "expected mapped grandchild column qualification in nested relation filter: {filter:?}"
    );
}
