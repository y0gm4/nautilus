mod common;

use nautilus_migrate::live::{
    ComputedKind, LiveColumn, LiveCompositeField, LiveCompositeType, LiveForeignKey, LiveIndex,
    LiveSchema, LiveTable,
};
use nautilus_migrate::{
    serialize_live_schema, serialize_live_schema_with_options, DatabaseProvider, PullNameCase,
    PullNamingOptions,
};
use nautilus_schema::ir::{DefaultValue, ResolvedFieldType, ScalarType};

#[test]
fn serialises_single_table() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "users".to_string(),
        columns: vec![
            LiveColumn {
                name: "id".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "email".to_string(),
                col_type: "text".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(
        &live,
        DatabaseProvider::Postgres,
        "postgres://localhost/test",
    );

    assert!(out.contains("datasource db {"));
    assert!(out.contains("provider = \"postgresql\""));
    assert!(out.contains("model Users {"));
    assert!(out.contains("@id"));
    assert!(out.contains("@@map(\"users\")"));
}

#[test]
fn serialises_postgres_extensions_in_datasource_block() {
    let mut live = LiveSchema::default();
    live.extensions
        .insert("uuid-ossp".to_string(), "1.1".to_string());
    live.extensions
        .insert("pg_trgm".to_string(), "1.6".to_string());

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");
    let datasource = common::parse(&out)
        .unwrap()
        .datasource
        .expect("datasource IR");

    assert!(
        out.contains("extensions = [pg_trgm, \"uuid-ossp\"]"),
        "{out}"
    );
    assert_eq!(datasource.extensions, vec!["pg_trgm", "uuid-ossp"]);
}

#[test]
fn serialises_jsonb_columns_without_degrading_to_json() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "events".to_string(),
        columns: vec![
            LiveColumn {
                name: "id".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "payload".to_string(),
                col_type: "jsonb".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");
    let schema = common::parse(&out).expect("schema should parse");
    let field = schema
        .models
        .get("Events")
        .expect("Events model missing")
        .fields
        .iter()
        .find(|field| field.logical_name == "payload")
        .expect("payload field missing");

    assert!(
        out.lines()
            .any(|line| line.contains("payload") && line.contains("Jsonb")),
        "{out}"
    );
    assert!(matches!(
        field.field_type,
        ResolvedFieldType::Scalar(ScalarType::Jsonb)
    ));
}

#[test]
fn serialises_nullable_column() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "posts".to_string(),
        columns: vec![
            LiveColumn {
                name: "id".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "body".to_string(),
                col_type: "text".to_string(),
                nullable: true,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Sqlite, "sqlite:test.db");

    assert!(out.contains("String?"));
}

#[test]
fn serialises_nullable_array_without_optional_marker() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "posts".to_string(),
        columns: vec![
            LiveColumn {
                name: "id".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "tags".to_string(),
                col_type: "text[]".to_string(),
                nullable: true,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");

    assert!(out.contains("String[]"));
    assert!(!out.contains("String[]?"));
}

#[test]
fn serialises_composite_pk() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "order_items".to_string(),
        columns: vec![
            LiveColumn {
                name: "order_id".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "product_id".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
        ],
        primary_key: vec!["order_id".to_string(), "product_id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");

    assert!(out.contains("@@id([order_id, product_id])"));
    let lines: Vec<&str> = out.lines().collect();
    let has_standalone_id = lines.iter().any(|l| {
        let trimmed = l.trim();
        trimmed.contains("@id") && !trimmed.starts_with("@@id")
    });
    assert!(!has_standalone_id, "should not have per-column @id:\n{out}");
}

#[test]
fn serialises_indexes() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "users".to_string(),
        columns: vec![LiveColumn {
            name: "id".to_string(),
            col_type: "integer".to_string(),
            nullable: false,
            default_value: None,
            generated_expr: None,
            computed_kind: None,
            check_expr: None,
        }],
        primary_key: vec!["id".to_string()],
        indexes: vec![
            LiveIndex {
                name: "idx_users_email".to_string(),
                columns: vec!["email".to_string()],
                unique: true,
                method: None,
            },
            LiveIndex {
                name: "idx_users_name".to_string(),
                columns: vec!["name".to_string()],
                unique: false,
                method: None,
            },
        ],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");

    assert!(out.contains("@@unique([email])"));
    assert!(out.contains("@@index([name])"));
}

#[test]
fn serialises_index_type_and_map() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "users".to_string(),
        columns: vec![
            LiveColumn {
                name: "id".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "created_at".to_string(),
                col_type: "timestamp".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![LiveIndex {
            name: "idx_users_created".to_string(),
            columns: vec!["created_at".to_string()],
            unique: false,
            method: Some("brin".to_string()),
        }],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");

    assert!(out.contains("@@index([created_at], type: Brin, map: \"idx_users_created\")"));
}

#[test]
fn serialises_default_value() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "config".to_string(),
        columns: vec![
            LiveColumn {
                name: "id".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "active".to_string(),
                col_type: "boolean".to_string(),
                nullable: false,
                default_value: Some("true".to_string()),
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");

    assert!(out.contains("@default(true)"));
}

#[test]
fn serialises_autoincrement_default() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "users".to_string(),
        columns: vec![LiveColumn {
            name: "id".to_string(),
            col_type: "integer".to_string(),
            nullable: false,
            default_value: Some("nextval('users_id_seq')".to_string()),
            generated_expr: None,
            computed_kind: None,
            check_expr: None,
        }],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");

    assert!(out.contains("@default(autoincrement())"));
}

#[test]
fn serialises_computed_column() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "orders".to_string(),
        columns: vec![
            LiveColumn {
                name: "id".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "total".to_string(),
                col_type: "integer".to_string(),
                nullable: true,
                default_value: None,
                generated_expr: Some("price * quantity".to_string()),
                computed_kind: Some(ComputedKind::Stored),
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");

    assert!(out.contains("@computed(price * quantity, Stored)"));
    assert!(!out.contains("@default"));
}

#[test]
fn serialises_column_check_constraint() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "products".to_string(),
        columns: vec![
            LiveColumn {
                name: "id".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "price".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: Some("price > 0".to_string()),
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");

    assert!(out.contains("@check(price > 0)"));
}

#[test]
fn serialises_table_check_constraint() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "events".to_string(),
        columns: vec![
            LiveColumn {
                name: "id".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "start_date".to_string(),
                col_type: "timestamp".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "end_date".to_string(),
                col_type: "timestamp".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec!["start_date < end_date".to_string()],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");

    assert!(out.contains("@@check(start_date < end_date)"));
}

#[test]
fn serialises_and_reparses_computed_column_with_sql_string_literal() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "users".to_string(),
        columns: vec![
            LiveColumn {
                name: "id".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "first_name".to_string(),
                col_type: "text".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "last_name".to_string(),
                col_type: "text".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "display_name".to_string(),
                col_type: "text".to_string(),
                nullable: true,
                default_value: None,
                generated_expr: Some("first_name || ' ' || last_name".to_string()),
                computed_kind: Some(ComputedKind::Stored),
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");
    let ir = common::parse(&out).unwrap();
    let users = ir.models.get("Users").unwrap();
    let display_name = users.find_field("display_name").unwrap();

    assert!(matches!(
        &display_name.computed,
        Some((expr, ComputedKind::Stored)) if expr == "first_name || \" \" || last_name"
    ));
}

#[test]
fn serialises_and_reparses_check_constraints_with_sql_string_literals() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "accounts".to_string(),
        columns: vec![
            LiveColumn {
                name: "id".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "status".to_string(),
                col_type: "text".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: Some("status IN ['Draft', 'PUBLISHED']".to_string()),
            },
            LiveColumn {
                name: "role".to_string(),
                col_type: "text".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec!["role IN ['ADMIN', 'User']".to_string()],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");
    let ir = common::parse(&out).unwrap();
    let accounts = ir.models.get("Accounts").unwrap();
    let status = accounts.find_field("status").unwrap();

    assert_eq!(
        status.check.as_deref(),
        Some("status IN ('Draft', 'PUBLISHED')")
    );
    assert_eq!(
        accounts.check_constraints,
        vec!["role IN ('ADMIN', 'User')"]
    );
}

#[test]
fn serialises_and_reparses_postgres_composites_and_arrays() {
    let mut live = LiveSchema::default();

    live.enums.insert(
        "status".to_string(),
        vec!["ACTIVE".to_string(), "INACTIVE".to_string()],
    );
    live.composite_types.insert(
        "address".to_string(),
        LiveCompositeType {
            name: "address".to_string(),
            fields: vec![
                LiveCompositeField {
                    name: "street".to_string(),
                    col_type: "text".to_string(),
                },
                LiveCompositeField {
                    name: "zip_code".to_string(),
                    col_type: "integer".to_string(),
                },
                LiveCompositeField {
                    name: "status".to_string(),
                    col_type: "status".to_string(),
                },
            ],
        },
    );
    live.tables.insert(
        "profiles".to_string(),
        LiveTable {
            name: "profiles".to_string(),
            columns: vec![
                LiveColumn {
                    name: "id".to_string(),
                    col_type: "integer".to_string(),
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
                LiveColumn {
                    name: "primary_address".to_string(),
                    col_type: "address".to_string(),
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
                LiveColumn {
                    name: "previous_addresses".to_string(),
                    col_type: "address[]".to_string(),
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
                LiveColumn {
                    name: "status_history".to_string(),
                    col_type: "status[]".to_string(),
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
                LiveColumn {
                    name: "lucky_numbers".to_string(),
                    col_type: "integer[]".to_string(),
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: vec![],
            check_constraints: vec![],
            foreign_keys: vec![],
        },
    );

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");
    let ir = common::parse(&out).unwrap();

    let address = ir.composite_types.get("Address").unwrap();
    assert_eq!(address.fields.len(), 3);
    assert!(matches!(
        &address.fields[2].field_type,
        ResolvedFieldType::Enum { enum_name } if enum_name == "Status"
    ));

    let profiles = ir.models.get("Profiles").unwrap();

    let primary_address = profiles.find_field("primary_address").unwrap();
    assert!(matches!(
        &primary_address.field_type,
        ResolvedFieldType::CompositeType { type_name } if type_name == "Address"
    ));
    assert!(!primary_address.is_array);

    let previous_addresses = profiles.find_field("previous_addresses").unwrap();
    assert!(matches!(
        &previous_addresses.field_type,
        ResolvedFieldType::CompositeType { type_name } if type_name == "Address"
    ));
    assert!(previous_addresses.is_array);

    let status_history = profiles.find_field("status_history").unwrap();
    assert!(matches!(
        &status_history.field_type,
        ResolvedFieldType::Enum { enum_name } if enum_name == "Status"
    ));
    assert!(status_history.is_array);

    let lucky_numbers = profiles.find_field("lucky_numbers").unwrap();
    assert!(matches!(
        &lucky_numbers.field_type,
        ResolvedFieldType::Scalar(ScalarType::Int)
    ));
    assert!(lucky_numbers.is_array);
}

#[test]
fn serialises_varchar_lengths() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "users".to_string(),
        columns: vec![
            LiveColumn {
                name: "id".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "username".to_string(),
                col_type: "varchar(30)".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");

    assert!(out.contains("VarChar(30)"));
}

#[test]
fn serialises_relation_actions_with_schema_casing() {
    let live = common::make_live_schema(vec![
        LiveTable {
            name: "users".to_string(),
            columns: vec![LiveColumn {
                name: "id".to_string(),
                col_type: "integer".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            }],
            primary_key: vec!["id".to_string()],
            indexes: vec![],
            check_constraints: vec![],
            foreign_keys: vec![],
        },
        LiveTable {
            name: "posts".to_string(),
            columns: vec![
                LiveColumn {
                    name: "id".to_string(),
                    col_type: "integer".to_string(),
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
                LiveColumn {
                    name: "user_id".to_string(),
                    col_type: "integer".to_string(),
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: vec![],
            check_constraints: vec![],
            foreign_keys: vec![LiveForeignKey {
                constraint_name: "fk_posts_user_id".to_string(),
                columns: vec!["user_id".to_string()],
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: Some("CASCADE".to_string()),
                on_update: Some("SET NULL".to_string()),
            }],
        },
    ]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");

    assert!(out.contains("onDelete: Cascade"));
    assert!(out.contains("onUpdate: SetNull"));
}

#[test]
fn serialises_one_to_one_back_reference_as_optional_scalar() {
    let live = common::make_live_schema(vec![
        LiveTable {
            name: "users".to_string(),
            columns: vec![LiveColumn {
                name: "id".to_string(),
                col_type: "uuid".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            }],
            primary_key: vec!["id".to_string()],
            indexes: vec![],
            check_constraints: vec![],
            foreign_keys: vec![],
        },
        LiveTable {
            name: "profiles".to_string(),
            columns: vec![
                LiveColumn {
                    name: "id".to_string(),
                    col_type: "integer".to_string(),
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
                LiveColumn {
                    name: "user_id".to_string(),
                    col_type: "uuid".to_string(),
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: vec![LiveIndex {
                name: "idx_profiles_user_id".to_string(),
                columns: vec!["user_id".to_string()],
                unique: true,
                method: Some("btree".to_string()),
            }],
            check_constraints: vec![],
            foreign_keys: vec![LiveForeignKey {
                constraint_name: "fk_profiles_user_id".to_string(),
                columns: vec!["user_id".to_string()],
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: Some("CASCADE".to_string()),
                on_update: None,
            }],
        },
    ]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");

    assert!(out.contains("\n  profile  Profiles?\n"));
    assert!(!out.contains("\n  profiles  Profiles[]\n"));
}

#[test]
fn serialises_mixed_case_postgres_enum_columns_and_defaults() {
    let mut live = LiveSchema::default();
    live.enums.insert(
        "ToneType".to_string(),
        vec![
            "FORMAL".to_string(),
            "INFORMAL".to_string(),
            "FRIENDLY".to_string(),
        ],
    );
    live.tables.insert(
        "Agent".to_string(),
        LiveTable {
            name: "Agent".to_string(),
            columns: vec![
                LiveColumn {
                    name: "id".to_string(),
                    col_type: "uuid".to_string(),
                    nullable: false,
                    default_value: Some("gen_random_uuid()".to_string()),
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
                LiveColumn {
                    name: "tone".to_string(),
                    col_type: "tonetype".to_string(),
                    nullable: false,
                    default_value: Some("'FRIENDLY'".to_string()),
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: vec![],
            check_constraints: vec![],
            foreign_keys: vec![],
        },
    );

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");
    let ir = common::parse(&out).unwrap();
    let agent = ir.models.get("Agent").unwrap();
    let tone = agent.find_field("tone").unwrap();

    assert!(matches!(
        &tone.field_type,
        ResolvedFieldType::Enum { enum_name } if enum_name == "ToneType"
    ));
    assert!(matches!(
        &tone.default_value,
        Some(DefaultValue::EnumVariant(variant)) if variant == "FRIENDLY"
    ));
}

#[test]
fn serialises_ambiguous_relations_with_explicit_names() {
    let live = common::make_live_schema(vec![
        LiveTable {
            name: "App".to_string(),
            columns: vec![
                LiveColumn {
                    name: "id".to_string(),
                    col_type: "uuid".to_string(),
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
                LiveColumn {
                    name: "current_version_id".to_string(),
                    col_type: "uuid".to_string(),
                    nullable: true,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: vec![LiveIndex {
                name: "idx_App_current_version_id".to_string(),
                columns: vec!["current_version_id".to_string()],
                unique: true,
                method: Some("btree".to_string()),
            }],
            check_constraints: vec![],
            foreign_keys: vec![LiveForeignKey {
                constraint_name: "App_current_version_id_fkey".to_string(),
                columns: vec!["current_version_id".to_string()],
                referenced_table: "History".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: Some("SET NULL".to_string()),
                on_update: Some("CASCADE".to_string()),
            }],
        },
        LiveTable {
            name: "History".to_string(),
            columns: vec![
                LiveColumn {
                    name: "id".to_string(),
                    col_type: "uuid".to_string(),
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
                LiveColumn {
                    name: "app_id".to_string(),
                    col_type: "uuid".to_string(),
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: vec![],
            check_constraints: vec![],
            foreign_keys: vec![LiveForeignKey {
                constraint_name: "History_app_id_fkey".to_string(),
                columns: vec!["app_id".to_string()],
                referenced_table: "App".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: Some("CASCADE".to_string()),
                on_update: Some("CASCADE".to_string()),
            }],
        },
    ]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");
    let ir = common::parse(&out).unwrap();

    let app = ir.models.get("App").unwrap();
    let current_version = app.find_field("current_version").unwrap();
    let histories = app.find_field("histories").unwrap();
    let history = ir.models.get("History").unwrap();
    let app_field = history.find_field("app").unwrap();
    let app_current_version = history.find_field("app_current_version").unwrap();

    assert!(matches!(
        &current_version.field_type,
        ResolvedFieldType::Relation(rel) if rel.name.as_deref() == Some("App_current_version")
    ));
    assert!(matches!(
        &histories.field_type,
        ResolvedFieldType::Relation(rel) if rel.name.as_deref() == Some("History_app")
    ));
    assert!(matches!(
        &app_field.field_type,
        ResolvedFieldType::Relation(rel) if rel.name.as_deref() == Some("History_app")
    ));
    assert!(matches!(
        &app_current_version.field_type,
        ResolvedFieldType::Relation(rel) if rel.name.as_deref() == Some("App_current_version")
    ));
}

#[test]
fn serialises_self_relations_with_explicit_names() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "Folder".to_string(),
        columns: vec![
            LiveColumn {
                name: "id".to_string(),
                col_type: "uuid".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "parent_id".to_string(),
                col_type: "uuid".to_string(),
                nullable: true,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![LiveForeignKey {
            constraint_name: "Folder_parent_id_fkey".to_string(),
            columns: vec!["parent_id".to_string()],
            referenced_table: "Folder".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: Some("CASCADE".to_string()),
            on_update: Some("CASCADE".to_string()),
        }],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");
    let ir = common::parse(&out).unwrap();
    let folder = ir.models.get("Folder").unwrap();
    let parent = folder.find_field("parent").unwrap();
    let folders = folder.find_field("folders").unwrap();

    assert!(matches!(
        &parent.field_type,
        ResolvedFieldType::Relation(rel) if rel.name.as_deref() == Some("Folder_parent")
    ));
    assert!(matches!(
        &folders.field_type,
        ResolvedFieldType::Relation(rel) if rel.name.is_none()
    ));
}

#[test]
fn serialises_custom_model_and_field_case_with_maps() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "APICollection".to_string(),
        columns: vec![
            LiveColumn {
                name: "id".to_string(),
                col_type: "uuid".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "created_at".to_string(),
                col_type: "timestamp".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema_with_options(
        &live,
        DatabaseProvider::Postgres,
        "postgres://localhost/db",
        PullNamingOptions {
            model_case: PullNameCase::Snake,
            field_case: PullNameCase::Pascal,
        },
    );

    assert!(out.contains("model api_collection {"));
    assert!(out.contains("CreatedAt  DateTime  @map(\"created_at\")"));
    assert!(out.contains("@@map(\"APICollection\")"));
}

#[test]
fn serialises_relations_with_logical_names_under_custom_case() {
    let live = common::make_live_schema(vec![
        LiveTable {
            name: "Users".to_string(),
            columns: vec![LiveColumn {
                name: "id".to_string(),
                col_type: "uuid".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            }],
            primary_key: vec!["id".to_string()],
            indexes: vec![],
            check_constraints: vec![],
            foreign_keys: vec![],
        },
        LiveTable {
            name: "BlogPosts".to_string(),
            columns: vec![
                LiveColumn {
                    name: "id".to_string(),
                    col_type: "uuid".to_string(),
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
                LiveColumn {
                    name: "author_id".to_string(),
                    col_type: "uuid".to_string(),
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: vec![],
            check_constraints: vec![],
            foreign_keys: vec![LiveForeignKey {
                constraint_name: "BlogPosts_author_id_fkey".to_string(),
                columns: vec!["author_id".to_string()],
                referenced_table: "Users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: Some("CASCADE".to_string()),
                on_update: Some("CASCADE".to_string()),
            }],
        },
    ]);

    let out = serialize_live_schema_with_options(
        &live,
        DatabaseProvider::Postgres,
        "postgres://localhost/db",
        PullNamingOptions {
            model_case: PullNameCase::Snake,
            field_case: PullNameCase::Pascal,
        },
    );

    assert!(out.contains("model blog_posts {"));
    assert!(out.contains("AuthorId  Uuid  @map(\"author_id\")"));
    assert!(out.contains(
        "Author  users  @relation(fields: [AuthorId], references: [Id], onDelete: Cascade, onUpdate: Cascade)"
    ));
}

#[test]
fn serialises_reserved_field_name_with_safe_logical_identifier() {
    let live = common::make_live_schema(vec![LiveTable {
        name: "AppAgent".to_string(),
        columns: vec![
            LiveColumn {
                name: "id".to_string(),
                col_type: "uuid".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
            LiveColumn {
                name: "model".to_string(),
                col_type: "text".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let out = serialize_live_schema(&live, DatabaseProvider::Postgres, "postgres://localhost/db");
    let ir = common::parse(&out).unwrap();
    let app_agent = ir.models.get("AppAgent").unwrap();

    assert!(out.contains("model_  String  @map(\"model\")"));
    assert!(app_agent.find_field("model_").is_some());
    assert!(app_agent.find_field("model").is_none());
}
