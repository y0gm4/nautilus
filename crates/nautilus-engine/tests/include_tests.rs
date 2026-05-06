mod common;

use common::{call_rpc_json, sqlite_state};
use std::collections::HashMap;

use nautilus_core::{Column, FindManyArgs, IncludeRelation, OrderBy};
use nautilus_engine::handlers;
use nautilus_protocol::{PROTOCOL_VERSION, QUERY_CREATE, QUERY_FIND_MANY};
use serde_json::json;

fn schema_source() -> &'static str {
    r#"
datasource db {
  provider = "sqlite"
  url      = "sqlite::memory:"
}

model User {
  id    Int    @id @default(autoincrement())
  email String
  posts Post[]
}

model Post {
  id       Int       @id @default(autoincrement()) @map("post_id")
  title    String
  sort     Int       @map("sort_index")
  authorId Int       @map("author_id")
  author   User      @relation(fields: [authorId], references: [id])
  comments Comment[]

  @@map("blog_posts")
}

model Comment {
  id     Int    @id @default(autoincrement()) @map("comment_id")
  body   String
  sort   Int    @map("sort_index")
  postId Int    @map("post_id")
  post   Post   @relation(fields: [postId], references: [id])

  @@map("post_comments")
}
"#
}

#[tokio::test]
async fn array_includes_apply_ordering_and_pagination_before_json_serialization() {
    let (state, temp_dir) = sqlite_state("include-tests", schema_source()).await;

    let created_user = call_rpc_json(
        &state,
        QUERY_CREATE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "data": { "email": "alice@example.com" }
        }),
    )
    .await;
    let user_id = created_user["data"][0]["User__id"]
        .as_i64()
        .expect("user id should be present");

    for (title, sort) in [("first", 1), ("second", 2), ("third", 3)] {
        let created_post = call_rpc_json(
            &state,
            QUERY_CREATE,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "Post",
                "data": {
                    "title": title,
                    "sort": sort,
                    "authorId": user_id
                }
            }),
        )
        .await;
        let post_id = created_post["data"][0]["blog_posts__post_id"]
            .as_i64()
            .expect("post id should be present");

        for (body, comment_sort) in [
            (format!("{title}-low"), 1),
            (format!("{title}-mid"), 2),
            (format!("{title}-high"), 3),
        ] {
            let _created_comment = call_rpc_json(
                &state,
                QUERY_CREATE,
                json!({
                    "protocolVersion": PROTOCOL_VERSION,
                    "model": "Comment",
                    "data": {
                        "body": body,
                        "sort": comment_sort,
                        "postId": post_id
                    }
                }),
            )
            .await;
        }
    }

    let found = call_rpc_json(
        &state,
        QUERY_FIND_MANY,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "args": {
                "where": { "id": user_id },
                "include": {
                    "posts": {
                        "orderBy": [{ "sort": "desc" }],
                        "skip": 1,
                        "take": 1,
                        "include": {
                            "comments": {
                                "orderBy": [{ "sort": "desc" }],
                                "skip": 1,
                                "take": 1
                            }
                        }
                    }
                }
            }
        }),
    )
    .await;

    let rows = found["data"]
        .as_array()
        .expect("find_many should return rows");
    assert_eq!(rows.len(), 1);

    let posts = rows[0]["posts_json"]
        .as_array()
        .expect("posts include should be a JSON array");
    assert_eq!(posts.len(), 1, "posts include should honor take/skip");
    assert_eq!(posts[0]["title"], json!("second"));
    assert_eq!(posts[0]["sort"], json!(2));
    assert!(
        posts[0].get("blog_posts__sort_index").is_none(),
        "included rows should use logical field names inside JSON payloads: {:?}",
        posts[0]
    );

    let comments = posts[0]["comments_json"]
        .as_array()
        .expect("nested comments include should be a JSON array");
    assert_eq!(comments.len(), 1, "nested include should honor take/skip");
    assert_eq!(comments[0]["body"], json!("second-mid"));
    assert_eq!(comments[0]["sort"], json!(2));

    drop(state);
    drop(temp_dir);
}

#[tokio::test]
async fn typed_find_many_includes_bypass_rpc_and_preserve_include_semantics() {
    let (state, temp_dir) = sqlite_state("include-tests", schema_source()).await;

    let created_user = call_rpc_json(
        &state,
        QUERY_CREATE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "data": { "email": "alice@example.com" }
        }),
    )
    .await;
    let user_id = created_user["data"][0]["User__id"]
        .as_i64()
        .expect("user id should be present");

    for (title, sort) in [("first", 1), ("second", 2), ("third", 3)] {
        let created_post = call_rpc_json(
            &state,
            QUERY_CREATE,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "Post",
                "data": {
                    "title": title,
                    "sort": sort,
                    "authorId": user_id
                }
            }),
        )
        .await;
        let post_id = created_post["data"][0]["blog_posts__post_id"]
            .as_i64()
            .expect("post id should be present");

        for (body, comment_sort) in [
            (format!("{title}-low"), 1),
            (format!("{title}-mid"), 2),
            (format!("{title}-high"), 3),
        ] {
            let _created_comment = call_rpc_json(
                &state,
                QUERY_CREATE,
                json!({
                    "protocolVersion": PROTOCOL_VERSION,
                    "model": "Comment",
                    "data": {
                        "body": body,
                        "sort": comment_sort,
                        "postId": post_id
                    }
                }),
            )
            .await;
        }
    }

    let rows = handlers::handle_find_many_typed(
        &state,
        "User",
        &FindManyArgs {
            where_: Some(Column::<i64>::new("User", "id").eq(user_id)),
            include: HashMap::from([(
                "posts".to_string(),
                IncludeRelation::plain()
                    .with_order_by(OrderBy::desc("sort"))
                    .with_skip(1)
                    .with_take(1)
                    .with_include(
                        "comments",
                        IncludeRelation::plain()
                            .with_order_by(OrderBy::desc("sort"))
                            .with_skip(1)
                            .with_take(1),
                    ),
            )]),
            ..Default::default()
        },
        None,
    )
    .await
    .expect("typed findMany should succeed");

    assert_eq!(rows.len(), 1);

    let posts = rows[0]
        .get("posts_json")
        .expect("posts include should be present")
        .to_json_plain();
    let posts = posts
        .as_array()
        .expect("posts include should be a JSON array");
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0]["title"], json!("second"));
    assert_eq!(posts[0]["sort"], json!(2));
    assert!(posts[0].get("blog_posts__sort_index").is_none());

    let comments = posts[0]["comments_json"]
        .as_array()
        .expect("nested comments include should be a JSON array");
    assert_eq!(comments.len(), 1);
    assert_eq!(comments[0]["body"], json!("second-mid"));
    assert_eq!(comments[0]["sort"], json!(2));

    drop(state);
    drop(temp_dir);
}
