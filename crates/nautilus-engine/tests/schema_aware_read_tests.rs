mod common;

use common::{call_embedded, call_rpc_json, sqlite_state};
use nautilus_core::Value;
use nautilus_engine::handlers::EmbeddedResponse;
use nautilus_protocol::{
    PROTOCOL_VERSION, QUERY_COUNT, QUERY_CREATE, QUERY_DELETE, QUERY_FIND_MANY, QUERY_FIND_UNIQUE,
    QUERY_GROUP_BY, QUERY_RAW, QUERY_UPDATE,
};
use serde_json::json;
use uuid::Uuid;

fn schema_source() -> &'static str {
    r#"
datasource db {
  provider = "sqlite"
  url      = "sqlite::memory:"
}

model User {
  id    Int      @id @default(autoincrement())
  tags  String[] @store(json)
}
"#
}

fn typed_scalar_schema_source() -> &'static str {
    r#"
datasource db {
  provider = "sqlite"
  url      = "sqlite::memory:"
}

model Invoice {
  id        Uuid           @id @default(uuid())
  reference String         @unique
  amount    Decimal(10, 2)
  issued_at DateTime
  metadata  Json
}
"#
}

fn updated_at_schema_source() -> &'static str {
    r#"
datasource db {
  provider = "sqlite"
  url      = "sqlite::memory:"
}

model User {
  id        Int      @id @default(autoincrement())
  email     String
  updatedAt DateTime @updatedAt @map("updated_at")
}
"#
}

fn mapped_group_by_schema_source() -> &'static str {
    r#"
datasource db {
  provider = "sqlite"
  url      = "sqlite::memory:"
}

enum Role {
  USER
  ADMIN
}

model User {
  id           Int    @id @default(autoincrement()) @map("user_pk")
  email        String @unique @map("user_email")
  display_name String @map("display_name_db")
  account_role Role   @default(USER) @map("account_role_db")

  @@map("app_users")
}
"#
}

fn decimal_string(value: &serde_json::Value) -> rust_decimal::Decimal {
    value
        .as_str()
        .expect("decimal values should be serialized as strings")
        .parse()
        .expect("decimal string should parse")
}

fn parse_wire_datetime(value: &serde_json::Value) -> chrono::NaiveDateTime {
    chrono::DateTime::parse_from_rfc3339(
        value
            .as_str()
            .expect("datetime values should be serialized as strings"),
    )
    .expect("datetime string should parse")
    .naive_utc()
}

#[tokio::test]
async fn sqlite_modeled_reads_apply_schema_aware_hints() {
    let (state, temp_dir) = sqlite_state("schema-aware-tests", schema_source()).await;

    let created = call_rpc_json(
        &state,
        QUERY_CREATE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "data": {
                "tags": ["orm", "sqlite"]
            }
        }),
    )
    .await;

    assert_eq!(created["count"], json!(1));
    assert_eq!(created["data"][0]["User__tags"], json!(["orm", "sqlite"]));

    let updated = call_rpc_json(
        &state,
        QUERY_UPDATE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "filter": { "id": 1 },
            "data": {
                "tags": ["typed", "json"]
            }
        }),
    )
    .await;

    assert_eq!(updated["count"], json!(1));
    assert_eq!(updated["data"][0]["User__tags"], json!(["typed", "json"]));

    let found_unique = call_rpc_json(
        &state,
        QUERY_FIND_UNIQUE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "filter": { "id": 1 }
        }),
    )
    .await;

    assert_eq!(
        found_unique["data"][0]["User__tags"],
        json!(["typed", "json"])
    );

    let found_many = call_rpc_json(
        &state,
        QUERY_FIND_MANY,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User"
        }),
    )
    .await;

    assert_eq!(
        found_many["data"][0]["User__tags"],
        json!(["typed", "json"])
    );

    drop(state);
    drop(temp_dir);
}

#[tokio::test]
async fn sqlite_embedded_modeled_paths_return_typed_rows_and_counts() {
    let (state, temp_dir) = sqlite_state("schema-aware-tests", schema_source()).await;

    let created = call_embedded(
        &state,
        QUERY_CREATE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "data": {
                "tags": ["orm", "sqlite"]
            }
        }),
    )
    .await;

    match created {
        EmbeddedResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0]
                    .get("User__tags")
                    .expect("create should return the projected json column")
                    .to_json_plain(),
                json!(["orm", "sqlite"])
            );
        }
        other => panic!("expected embedded create to return rows, got {other:?}"),
    }

    let found_many = call_embedded(
        &state,
        QUERY_FIND_MANY,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User"
        }),
    )
    .await;

    match found_many {
        EmbeddedResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0]
                    .get("User__tags")
                    .expect("findMany should preserve typed json values")
                    .to_json_plain(),
                json!(["orm", "sqlite"])
            );
        }
        other => panic!("expected embedded findMany to return rows, got {other:?}"),
    }

    let counted = call_embedded(
        &state,
        QUERY_COUNT,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User"
        }),
    )
    .await;

    match counted {
        EmbeddedResponse::Count(count) => assert_eq!(count, 1),
        other => panic!("expected embedded count to return a count, got {other:?}"),
    }

    drop(state);
    drop(temp_dir);
}

#[tokio::test]
async fn sqlite_raw_query_keeps_jsonish_columns_raw() {
    let (state, temp_dir) = sqlite_state("schema-aware-tests", schema_source()).await;

    let _created = call_rpc_json(
        &state,
        QUERY_CREATE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "data": {
                "tags": ["orm", "sqlite"]
            }
        }),
    )
    .await;

    let raw = call_rpc_json(
        &state,
        QUERY_RAW,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "sql": r#"SELECT tags FROM "User""#
        }),
    )
    .await;

    assert_eq!(raw["data"][0]["tags"], json!(r#"["orm","sqlite"]"#));

    drop(state);
    drop(temp_dir);
}

#[tokio::test]
async fn sqlite_group_by_returns_logical_names_for_mapped_fields() {
    let (state, temp_dir) =
        sqlite_state("schema-aware-tests", mapped_group_by_schema_source()).await;

    let _ = call_rpc_json(
        &state,
        QUERY_CREATE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "data": {
                "email": "admin@example.com",
                "display_name": "Admin User",
                "account_role": "ADMIN"
            }
        }),
    )
    .await;

    let _ = call_rpc_json(
        &state,
        QUERY_CREATE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "data": {
                "email": "member@example.com",
                "display_name": "Member User",
                "account_role": "USER"
            }
        }),
    )
    .await;

    let grouped = call_rpc_json(
        &state,
        QUERY_GROUP_BY,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "args": {
                "by": ["account_role"],
                "count": {
                    "_all": true,
                    "display_name": true
                },
                "orderBy": [
                    { "account_role": "asc" }
                ]
            }
        }),
    )
    .await;

    let rows = grouped["data"]
        .as_array()
        .expect("groupBy should return a data array");
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter().all(|row| row.get("account_role").is_some()),
        "grouped rows should use logical field names: {rows:?}"
    );
    assert!(
        rows.iter().all(|row| row.get("account_role_db").is_none()),
        "grouped rows should not leak db field names: {rows:?}"
    );

    let admin_group = rows
        .iter()
        .find(|row| row["account_role"] == json!("ADMIN"))
        .expect("missing ADMIN group");
    assert_eq!(admin_group["_count"]["_all"], json!(1));
    assert_eq!(admin_group["_count"]["display_name"], json!(1));

    drop(state);
    drop(temp_dir);
}

#[tokio::test]
async fn sqlite_embedded_group_by_returns_shaped_rows() {
    let (state, temp_dir) =
        sqlite_state("schema-aware-tests", mapped_group_by_schema_source()).await;

    for (email, display_name, account_role) in [
        ("admin@example.com", "Admin User", "ADMIN"),
        ("member@example.com", "Member User", "USER"),
    ] {
        let _ = call_rpc_json(
            &state,
            QUERY_CREATE,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "User",
                "data": {
                    "email": email,
                    "display_name": display_name,
                    "account_role": account_role
                }
            }),
        )
        .await;
    }

    let grouped = call_embedded(
        &state,
        QUERY_GROUP_BY,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "args": {
                "by": ["account_role"],
                "count": {
                    "_all": true,
                    "display_name": true
                },
                "orderBy": [
                    { "account_role": "asc" }
                ]
            }
        }),
    )
    .await;

    match grouped {
        EmbeddedResponse::Rows(rows) => {
            assert_eq!(rows.len(), 2);
            let admin = rows
                .iter()
                .find(|row| {
                    row.get("account_role").map(Value::to_json_plain) == Some(json!("ADMIN"))
                })
                .expect("missing ADMIN group");
            assert_eq!(
                admin
                    .get("_count")
                    .expect("groupBy should expose _count as shaped JSON")
                    .to_json_plain(),
                json!({
                    "_all": 1,
                    "display_name": 1
                })
            );
            assert!(admin.get("account_role_db").is_none());
        }
        other => panic!("expected embedded groupBy to return rows, got {other:?}"),
    }

    drop(state);
    drop(temp_dir);
}

#[tokio::test]
async fn sqlite_mapped_filters_work_for_unique_update_delete_and_count() {
    let (state, temp_dir) =
        sqlite_state("schema-aware-tests", mapped_group_by_schema_source()).await;

    for (email, display_name, account_role) in [
        ("alice@example.com", "Alice", "ADMIN"),
        ("bob@example.com", "Bob", "USER"),
    ] {
        let created = call_rpc_json(
            &state,
            QUERY_CREATE,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "model": "User",
                "data": {
                    "email": email,
                    "display_name": display_name,
                    "account_role": account_role
                }
            }),
        )
        .await;

        assert_eq!(created["count"], json!(1));
    }

    let updated = call_rpc_json(
        &state,
        QUERY_UPDATE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "filter": { "email": "alice@example.com" },
            "data": {
                "display_name": "Alicia"
            }
        }),
    )
    .await;

    assert_eq!(updated["count"], json!(1));
    assert_eq!(
        updated["data"][0]["app_users__display_name_db"],
        json!("Alicia")
    );

    let found_unique = call_rpc_json(
        &state,
        QUERY_FIND_UNIQUE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "filter": { "email": "alice@example.com" }
        }),
    )
    .await;

    assert_eq!(
        found_unique["data"][0]["app_users__display_name_db"],
        json!("Alicia")
    );

    let deleted = call_rpc_json(
        &state,
        QUERY_DELETE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "filter": { "display_name": "Alicia" }
        }),
    )
    .await;

    assert_eq!(deleted["count"], json!(1));
    assert_eq!(
        deleted["data"][0]["app_users__user_email"],
        json!("alice@example.com")
    );

    let counted = call_rpc_json(
        &state,
        QUERY_COUNT,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "args": {
                "where": {
                    "display_name": "Bob"
                }
            }
        }),
    )
    .await;

    assert_eq!(counted["count"], json!(1));

    drop(state);
    drop(temp_dir);
}

#[tokio::test]
async fn sqlite_modeled_reads_round_trip_typed_scalars_on_create_and_update() {
    let (state, temp_dir) = sqlite_state("schema-aware-tests", typed_scalar_schema_source()).await;

    let created = call_rpc_json(
        &state,
        QUERY_CREATE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "Invoice",
            "data": {
                "reference": "engine-invoice-001",
                "amount": "19.95",
                "issued_at": "2026-01-15T09:30:00",
                "metadata": {
                    "source": "engine",
                    "retries": 0,
                    "tags": ["smoke", "typed"]
                }
            }
        }),
    )
    .await;

    assert_eq!(created["count"], json!(1));
    let created_row = &created["data"][0];
    let created_id = created_row["Invoice__id"]
        .as_str()
        .expect("create should return a UUID id");
    Uuid::parse_str(created_id).expect("create should return a valid UUID");
    assert_eq!(created_row["Invoice__amount"], json!("19.95"));
    assert_eq!(
        created_row["Invoice__issued_at"],
        json!("2026-01-15T09:30:00Z")
    );
    assert_eq!(created_row["Invoice__metadata"]["source"], json!("engine"));

    let found = call_rpc_json(
        &state,
        QUERY_FIND_UNIQUE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "Invoice",
            "filter": {
                "reference": "engine-invoice-001"
            }
        }),
    )
    .await;

    assert_eq!(found["data"][0]["Invoice__id"], json!(created_id));
    assert_eq!(found["data"][0]["Invoice__amount"], json!("19.95"));
    assert_eq!(
        found["data"][0]["Invoice__metadata"]["tags"],
        json!(["smoke", "typed"])
    );

    let updated = call_rpc_json(
        &state,
        QUERY_UPDATE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "Invoice",
            "filter": {
                "reference": "engine-invoice-001"
            },
            "data": {
                "amount": "21.50",
                "metadata": {
                    "source": "engine",
                    "approved": true,
                    "retries": 1
                }
            }
        }),
    )
    .await;

    assert_eq!(updated["count"], json!(1));
    assert_eq!(updated["data"][0]["Invoice__id"], json!(created_id));
    assert_eq!(
        decimal_string(&updated["data"][0]["Invoice__amount"]),
        rust_decimal::Decimal::new(2150, 2)
    );
    assert_eq!(
        updated["data"][0]["Invoice__metadata"]["approved"],
        json!(true)
    );

    drop(state);
    drop(temp_dir);
}

#[tokio::test]
async fn sqlite_mutations_fill_updated_at_inside_engine() {
    let (state, temp_dir) = sqlite_state("schema-aware-tests", updated_at_schema_source()).await;

    let created_from_null = call_rpc_json(
        &state,
        QUERY_CREATE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "data": {
                "email": "null@example.com",
                "updatedAt": null
            }
        }),
    )
    .await;

    let null_updated_at = &created_from_null["data"][0]["User__updated_at"];
    let _parsed_null_timestamp = parse_wire_datetime(null_updated_at);

    let created_with_explicit_value = call_rpc_json(
        &state,
        QUERY_CREATE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "data": {
                "email": "manual@example.com",
                "updatedAt": "2001-01-01T00:00:00Z"
            }
        }),
    )
    .await;

    let explicit_id = created_with_explicit_value["data"][0]["User__id"].clone();
    assert_eq!(
        created_with_explicit_value["data"][0]["User__updated_at"],
        json!("2001-01-01T00:00:00Z")
    );

    let updated = call_rpc_json(
        &state,
        QUERY_UPDATE,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "model": "User",
            "filter": { "id": explicit_id },
            "data": {
                "email": "manual+updated@example.com"
            }
        }),
    )
    .await;

    let refreshed_updated_at = &updated["data"][0]["User__updated_at"];
    assert_ne!(refreshed_updated_at, &json!("2001-01-01T00:00:00Z"));
    let refreshed_timestamp = parse_wire_datetime(refreshed_updated_at);
    assert!(
        refreshed_timestamp
            > chrono::NaiveDate::from_ymd_opt(2001, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
    );

    drop(state);
    drop(temp_dir);
}
