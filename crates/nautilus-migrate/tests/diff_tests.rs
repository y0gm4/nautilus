mod common;

use std::collections::HashMap;

use nautilus_migrate::live::{
    ComputedKind, LiveColumn, LiveForeignKey, LiveIndex, LiveIndexKind, LiveTable,
};
use nautilus_migrate::{
    change_risk, order_changes_for_apply, Change, ChangeRisk, DatabaseProvider, LiveSchema,
    SchemaDiff,
};
use nautilus_schema::ir::SchemaIr;

#[test]
fn detects_new_table() {
    let target = common::parse("model User { id Int @id }").unwrap();
    let live = LiveSchema::default();

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Sqlite);

    assert!(changes.len() == 1);
    assert!(matches!(changes[0], Change::NewTable(ref m) if m.db_name == "User"));
}

#[test]
fn detects_dropped_table() {
    let target = SchemaIr {
        datasource: None,
        generator: None,
        models: HashMap::new(),
        enums: HashMap::new(),
        composite_types: HashMap::new(),
    };
    let live = common::make_live_schema(vec![LiveTable {
        name: "OldTable".to_string(),
        columns: vec![],
        primary_key: vec![],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Sqlite);

    assert!(changes.len() == 1);
    assert!(matches!(&changes[0], Change::DroppedTable { name } if name == "OldTable"));
}

#[test]
fn detects_added_column() {
    let target = common::parse("model User { id Int @id  email String }").unwrap();

    let live = common::make_live_schema(vec![LiveTable {
        name: "User".to_string(),
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
    }]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Sqlite);

    assert!(changes.iter().any(|c| matches!(
        c,
        Change::AddedColumn { table, field } if table == "User" && field.db_name == "email"
    )));
}

#[test]
fn detects_dropped_column() {
    let target = common::parse("model User { id Int @id }").unwrap();

    let live = common::make_live_schema(vec![LiveTable {
        name: "User".to_string(),
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
                name: "legacy_col".to_string(),
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

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Sqlite);

    assert!(changes.iter().any(|c| matches!(
        c,
        Change::DroppedColumn { table, column }
            if table == "User" && column == "legacy_col"
    )));
}

#[test]
fn detects_type_change() {
    let target = common::parse("model User { id Int @id  score Float }").unwrap();

    let live = common::make_live_schema(vec![LiveTable {
        name: "User".to_string(),
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
                name: "score".to_string(),
                col_type: "integer".to_string(),
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

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Sqlite);

    assert!(changes.iter().any(|c| matches!(
        c,
        Change::TypeChanged { table, column, .. } if table == "User" && column == "score"
    )));
}

#[test]
fn detects_nullability_change() {
    let target = common::parse("model User { id Int @id  email String }").unwrap();

    let live = common::make_live_schema(vec![LiveTable {
        name: "User".to_string(),
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

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Sqlite);

    assert!(changes.iter().any(|c| matches!(
        c,
        Change::NullabilityChanged { table, column, now_required: true }
            if table == "User" && column == "email"
    )));
}

#[test]
fn detects_computed_expr_change() {
    let target =
        common::parse("model Order { id Int @id  total Int @computed(price * quantity, Stored) }")
            .unwrap();

    let live = common::make_live_schema(vec![LiveTable {
        name: "Order".to_string(),
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
                generated_expr: Some("price + quantity".to_string()),
                computed_kind: Some(ComputedKind::Stored),
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(changes.iter().any(|c| matches!(
        c,
        Change::ComputedExprChanged { table, column, .. }
            if table == "Order" && column == "total"
    )));
}

#[test]
fn no_false_positive_when_computed_expr_unchanged() {
    let target =
        common::parse("model Order { id Int @id  total Int @computed(price * quantity, Stored) }")
            .unwrap();

    let live = common::make_live_schema(vec![LiveTable {
        name: "Order".to_string(),
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
                generated_expr: Some("(price * quantity)".to_string()),
                computed_kind: Some(ComputedKind::Stored),
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(
        !changes
            .iter()
            .any(|c| matches!(c, Change::ComputedExprChanged { .. })),
        "should NOT detect a change when expression is the same (modulo parens)"
    );
}

#[test]
fn no_false_positive_when_live_check_uses_bracket_syntax() {
    let target = common::parse(
        r#"model Account {
  id Int @id
  status String @check(status IN ["Draft", "PUBLISHED"])
  role String
  @@check(role IN ["ADMIN", "User"])
}"#,
    )
    .unwrap();

    let live = common::make_live_schema(vec![LiveTable {
        name: "Account".to_string(),
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

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Mysql);

    assert!(
        !changes
            .iter()
            .any(|c| matches!(c, Change::CheckChanged { .. })),
        "should NOT detect a check change when SQL and bracket forms are semantically equivalent"
    );
}

#[test]
fn detects_check_change_when_string_literal_casing_differs() {
    let target =
        common::parse(r#"model Account { id Int @id  status String @check(status IN ["Draft"]) }"#)
            .unwrap();

    let live = common::make_live_schema(vec![LiveTable {
        name: "Account".to_string(),
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
                check_expr: Some("status IN ['draft']".to_string()),
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Mysql);

    assert!(changes.iter().any(|c| matches!(
        c,
        Change::CheckChanged { table, column: Some(column), .. }
            if table == "Account" && column == "status"
    )));
}

#[test]
fn added_required_column_no_default_is_destructive() {
    let target = common::parse("model User { id Int @id  name String }").unwrap();
    let field = target
        .models
        .values()
        .next()
        .unwrap()
        .fields
        .iter()
        .find(|f| f.db_name == "name")
        .unwrap()
        .clone();

    let risk = change_risk(&Change::AddedColumn {
        table: "User".to_string(),
        field,
    });
    assert_eq!(risk, ChangeRisk::Destructive);
}

#[test]
fn added_optional_column_is_safe() {
    let target = common::parse("model User { id Int @id  name String? }").unwrap();
    let field = target
        .models
        .values()
        .next()
        .unwrap()
        .fields
        .iter()
        .find(|f| f.db_name == "name")
        .unwrap()
        .clone();

    let risk = change_risk(&Change::AddedColumn {
        table: "User".to_string(),
        field,
    });
    assert_eq!(risk, ChangeRisk::Safe);
}

#[test]
fn added_required_column_with_default_is_safe() {
    let target = common::parse("model User { id Int @id  score Int @default(0) }").unwrap();
    let field = target
        .models
        .values()
        .next()
        .unwrap()
        .fields
        .iter()
        .find(|f| f.db_name == "score")
        .unwrap()
        .clone();

    let risk = change_risk(&Change::AddedColumn {
        table: "User".to_string(),
        field,
    });
    assert_eq!(risk, ChangeRisk::Safe);
}

fn base_user_table(indexes: Vec<LiveIndex>) -> LiveTable {
    LiveTable {
        name: "User".to_string(),
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
        indexes,
        check_constraints: vec![],
        foreign_keys: vec![],
    }
}

#[test]
fn map_becomes_ddl_name_in_index_added() {
    let target =
        common::parse(r#"model User { id Int @id  @@index([id], map: "my_idx") }"#).unwrap();
    let live = common::make_live_schema(vec![base_user_table(vec![])]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(
        changes.iter().any(|c| matches!(
            c,
            Change::IndexAdded { index_name: Some(n), .. } if n == "my_idx"
        )),
        "expected IndexAdded with name 'my_idx', got: {:?}",
        changes
    );
}

#[test]
fn name_only_uses_auto_generated_ddl_name() {
    let target =
        common::parse(r#"model User { id Int @id  @@index([id], name: "logical") }"#).unwrap();
    let live = common::make_live_schema(vec![base_user_table(vec![])]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(
        changes.iter().any(|c| matches!(
            c,
            Change::IndexAdded { index_name: Some(n), .. } if n == "idx_User_id"
        )),
        "name: must not override DDL name; expected 'idx_User_id', got: {:?}",
        changes
    );
}

#[test]
fn no_change_when_map_matches_live_name() {
    let target =
        common::parse(r#"model User { id Int @id  @@index([id], map: "custom_idx") }"#).unwrap();
    let live = common::make_live_schema(vec![base_user_table(vec![LiveIndex {
        name: "custom_idx".to_string(),
        columns: vec!["id".to_string()],
        unique: false,
        kind: LiveIndexKind::Basic(nautilus_schema::ir::BasicIndexType::BTree),
    }])]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(
        !changes
            .iter()
            .any(|c| matches!(c, Change::IndexAdded { .. } | Change::IndexDropped { .. })),
        "no index change expected when map: matches live name"
    );
}

#[test]
fn detects_index_method_change() {
    let target = common::parse(r#"model User { id Int @id  @@index([id], type: Hash) }"#).unwrap();
    let live = common::make_live_schema(vec![base_user_table(vec![LiveIndex {
        name: "idx_User_id".to_string(),
        columns: vec!["id".to_string()],
        unique: false,
        kind: LiveIndexKind::Basic(nautilus_schema::ir::BasicIndexType::BTree),
    }])]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(
        changes
            .iter()
            .any(|c| matches!(c, Change::IndexDropped { .. })),
        "expected IndexDropped for stale btree index"
    );
    assert!(
        changes.iter().any(|c| matches!(
            c,
            Change::IndexAdded { kind, .. }
                if matches!(kind, nautilus_schema::ir::IndexKind::Basic(nautilus_schema::ir::BasicIndexType::Hash))
        )),
        "expected IndexAdded with Hash method"
    );
}

#[test]
fn detects_pgvector_index_opclass_change() {
    let target = common::parse(
        r#"
datasource db {
  provider   = "postgresql"
  url        = env("DATABASE_URL")
  extensions = [vector]
}

model Embedding {
  id        Int @id
  embedding Vector(3)

  @@index([embedding], type: Hnsw, opclass: vector_cosine_ops, m: 16)
}
"#,
    )
    .unwrap();
    let live = common::make_live_schema(vec![LiveTable {
        name: "Embedding".to_string(),
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
                name: "embedding".to_string(),
                col_type: "vector(3)".to_string(),
                nullable: false,
                default_value: None,
                generated_expr: None,
                computed_kind: None,
                check_expr: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        indexes: vec![LiveIndex {
            name: "idx_Embedding_embedding".to_string(),
            columns: vec!["embedding".to_string()],
            unique: false,
            kind: LiveIndexKind::Pgvector(nautilus_schema::ir::PgvectorIndex {
                method: nautilus_schema::ir::PgvectorMethod::Hnsw,
                opclass: Some(nautilus_schema::ir::PgvectorOpClass::L2Ops),
                options: nautilus_schema::ir::PgvectorIndexOptions {
                    m: Some(16),
                    ef_construction: None,
                    lists: None,
                },
            }),
        }],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(changes
        .iter()
        .any(|c| matches!(c, Change::IndexDropped { .. })));
    assert!(changes.iter().any(|c| matches!(
        c,
        Change::IndexAdded { kind: nautilus_schema::ir::IndexKind::Pgvector(p), .. }
            if p.opclass == Some(nautilus_schema::ir::PgvectorOpClass::CosineOps)
    )));
}

#[test]
fn no_false_positive_when_method_is_default_btree() {
    let target = common::parse(r#"model User { id Int @id  @@index([id]) }"#).unwrap();
    let live = common::make_live_schema(vec![base_user_table(vec![LiveIndex {
        name: "idx_User_id".to_string(),
        columns: vec!["id".to_string()],
        unique: false,
        kind: LiveIndexKind::Basic(nautilus_schema::ir::BasicIndexType::BTree),
    }])]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(
        !changes
            .iter()
            .any(|c| matches!(c, Change::IndexAdded { .. } | Change::IndexDropped { .. })),
        "BTree default vs live 'btree' must not produce a diff"
    );
}

#[test]
fn dropped_index_carries_live_physical_name() {
    let target = common::parse("model User { id Int @id }").unwrap();
    let live = common::make_live_schema(vec![base_user_table(vec![LiveIndex {
        name: "custom_legacy_idx".to_string(),
        columns: vec!["id".to_string()],
        unique: false,
        kind: LiveIndexKind::Unknown(None),
    }])]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(
        changes.iter().any(|c| matches!(
            c,
            Change::IndexDropped { index_name, .. } if index_name == "custom_legacy_idx"
        )),
        "IndexDropped must carry the live physical name, not an auto-generated one"
    );
}

#[test]
fn unique_constraint_name_mismatch_does_not_trigger_index_churn() {
    let target = common::parse("model User { id Int @id  email String @unique }").unwrap();
    let live = common::make_live_schema(vec![LiveTable {
        name: "User".to_string(),
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
        indexes: vec![LiveIndex {
            name: "users_email_key".to_string(),
            columns: vec!["email".to_string()],
            unique: true,
            kind: LiveIndexKind::Basic(nautilus_schema::ir::BasicIndexType::BTree),
        }],
        check_constraints: vec![],
        foreign_keys: vec![],
    }]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(
        !changes
            .iter()
            .any(|c| matches!(c, Change::IndexAdded { .. } | Change::IndexDropped { .. })),
        "unique constraint-backed indexes with different physical names should not churn: {:?}",
        changes
    );
}

#[test]
fn detects_foreign_key_added_with_actions() {
    let target = common::parse(
        r#"
model User {
  id Int @id
  posts Post[]
}

model Post {
  id Int @id
  authorId Int
  author User @relation(fields: [authorId], references: [id], onDelete: Cascade, onUpdate: Restrict)
}
"#,
    )
    .unwrap();

    let live = common::make_live_schema(vec![
        LiveTable {
            name: "User".to_string(),
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
            name: "Post".to_string(),
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
                    name: "authorId".to_string(),
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
            foreign_keys: vec![],
        },
    ]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(changes.iter().any(|c| matches!(
        c,
        Change::ForeignKeyAdded {
            table,
            constraint_name,
            columns,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
        }
            if table == "Post"
                && constraint_name == "fk_Post_authorId"
                && columns == &vec!["authorId".to_string()]
                && referenced_table == "User"
                && referenced_columns == &vec!["id".to_string()]
                && on_delete.as_deref() == Some("CASCADE")
                && on_update.as_deref() == Some("RESTRICT")
    )));
}

#[test]
fn detects_foreign_key_action_change_as_drop_and_add() {
    let target = common::parse(
        r#"
model User {
  id Int @id
  posts Post[]
}

model Post {
  id Int @id
  authorId Int
  author User @relation(fields: [authorId], references: [id], onDelete: Cascade)
}
"#,
    )
    .unwrap();

    let live = common::make_live_schema(vec![
        LiveTable {
            name: "User".to_string(),
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
            name: "Post".to_string(),
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
                    name: "authorId".to_string(),
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
                constraint_name: "post_author_fk_old".to_string(),
                columns: vec!["authorId".to_string()],
                referenced_table: "User".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: None,
                on_update: None,
            }],
        },
    ]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(changes.iter().any(|c| matches!(
        c,
        Change::ForeignKeyDropped {
            table,
            constraint_name,
        } if table == "Post" && constraint_name == "post_author_fk_old"
    )));
    assert!(changes.iter().any(|c| matches!(
        c,
        Change::ForeignKeyAdded {
            table,
            constraint_name,
            on_delete,
            ..
        } if table == "Post"
            && constraint_name == "fk_Post_authorId"
            && on_delete.as_deref() == Some("CASCADE")
    )));
}

#[test]
fn detects_foreign_key_dropped() {
    let target = common::parse(
        r#"
model User {
  id Int @id
}

model Post {
  id Int @id
  authorId Int
}
"#,
    )
    .unwrap();

    let live = common::make_live_schema(vec![
        LiveTable {
            name: "User".to_string(),
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
            name: "Post".to_string(),
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
                    name: "authorId".to_string(),
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
                constraint_name: "fk_Post_authorId".to_string(),
                columns: vec!["authorId".to_string()],
                referenced_table: "User".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: Some("CASCADE".to_string()),
                on_update: None,
            }],
        },
    ]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(changes.iter().any(|c| matches!(
        c,
        Change::ForeignKeyDropped {
            table,
            constraint_name,
        } if table == "Post" && constraint_name == "fk_Post_authorId"
    )));
}

#[test]
fn mysql_default_restrict_foreign_key_does_not_churn() {
    let target = common::parse(
        r#"
model User {
  id Int @id
  posts Post[]
}

model Post {
  id Int @id
  authorId Int
  author User @relation(fields: [authorId], references: [id])
}
"#,
    )
    .unwrap();

    let live = common::make_live_schema(vec![
        LiveTable {
            name: "User".to_string(),
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
            name: "Post".to_string(),
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
                    name: "authorId".to_string(),
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
                constraint_name: "fk_Post_authorId".to_string(),
                columns: vec!["authorId".to_string()],
                referenced_table: "User".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: Some("RESTRICT".to_string()),
                on_update: Some("RESTRICT".to_string()),
            }],
        },
    ]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Mysql);

    assert!(
        !changes.iter().any(|c| matches!(
            c,
            Change::ForeignKeyAdded { .. } | Change::ForeignKeyDropped { .. }
        )),
        "MySQL default RESTRICT actions should not churn foreign keys: {:?}",
        changes
    );
}

#[test]
fn order_changes_moves_foreign_key_drop_before_dropped_column() {
    let target = common::parse(
        r#"
model User {
  id Int @id
}

model Post {
  id Int @id
}
"#,
    )
    .unwrap();

    let live = common::make_live_schema(vec![
        LiveTable {
            name: "User".to_string(),
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
            name: "Post".to_string(),
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
                    name: "authorId".to_string(),
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
                constraint_name: "fk_Post_authorId".to_string(),
                columns: vec!["authorId".to_string()],
                referenced_table: "User".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: None,
                on_update: None,
            }],
        },
    ]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);
    let ordered = order_changes_for_apply(&changes, &live);

    let fk_drop_idx = ordered
        .iter()
        .position(|change| {
            matches!(
                change,
                Change::ForeignKeyDropped { table, constraint_name }
                    if table == "Post" && constraint_name == "fk_Post_authorId"
            )
        })
        .expect("expected foreign key drop");
    let dropped_column_idx = ordered
        .iter()
        .position(|change| {
            matches!(
                change,
                Change::DroppedColumn { table, column }
                    if table == "Post" && column == "authorId"
            )
        })
        .expect("expected dropped column");

    assert!(fk_drop_idx < dropped_column_idx, "{ordered:?}");
}

#[test]
fn order_changes_moves_foreign_key_drop_before_dropped_table() {
    let target = common::parse(
        r#"
model Post {
  id Int @id
  authorId Int
}
"#,
    )
    .unwrap();

    let live = common::make_live_schema(vec![
        LiveTable {
            name: "User".to_string(),
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
            name: "Post".to_string(),
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
                    name: "authorId".to_string(),
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
                constraint_name: "fk_Post_authorId".to_string(),
                columns: vec!["authorId".to_string()],
                referenced_table: "User".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: None,
                on_update: None,
            }],
        },
    ]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Mysql);
    let ordered = order_changes_for_apply(&changes, &live);

    let fk_drop_idx = ordered
        .iter()
        .position(|change| {
            matches!(
                change,
                Change::ForeignKeyDropped { table, constraint_name }
                    if table == "Post" && constraint_name == "fk_Post_authorId"
            )
        })
        .expect("expected foreign key drop");
    let dropped_table_idx = ordered
        .iter()
        .position(|change| {
            matches!(
                change,
                Change::DroppedTable { name } if name == "User"
            )
        })
        .expect("expected dropped table");

    assert!(fk_drop_idx < dropped_table_idx, "{ordered:?}");
}

#[test]
fn order_changes_drops_tables_in_reverse_live_dependency_order() {
    let target = SchemaIr {
        datasource: None,
        generator: None,
        models: HashMap::new(),
        enums: HashMap::new(),
        composite_types: HashMap::new(),
    };
    let live = common::make_live_schema(vec![
        LiveTable {
            name: "User".to_string(),
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
            name: "Post".to_string(),
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
                    name: "authorId".to_string(),
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
                constraint_name: "fk_Post_authorId".to_string(),
                columns: vec!["authorId".to_string()],
                referenced_table: "User".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: None,
                on_update: None,
            }],
        },
    ]);

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Sqlite);
    let ordered = order_changes_for_apply(&changes, &live);

    let user_idx = ordered
        .iter()
        .position(|change| matches!(change, Change::DroppedTable { name } if name == "User"))
        .expect("expected dropped User");
    let post_idx = ordered
        .iter()
        .position(|change| matches!(change, Change::DroppedTable { name } if name == "Post"))
        .expect("expected dropped Post");

    assert!(post_idx < user_idx, "{ordered:?}");
}

#[test]
fn detects_new_extension_when_missing_from_live() {
    let source = r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  extensions = [pg_trgm]
}

model Doc { id Int @id }
"#;
    let target = common::parse(source).unwrap();
    let live = LiveSchema::default();

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(
        changes
            .iter()
            .any(|c| matches!(c, Change::CreateExtension { name, .. } if name == "pg_trgm")),
        "expected CreateExtension(pg_trgm): {changes:?}"
    );
}

#[test]
fn detects_dropped_extension_when_live_has_it_but_target_doesnt() {
    let source = r#"
datasource db {
  provider = "postgresql"
  url      = "postgres://localhost/test"
}

model Doc { id Int @id }
"#;
    let target = common::parse(source).unwrap();
    let mut live = LiveSchema::default();
    live.extensions.insert(
        "pg_trgm".to_string(),
        nautilus_migrate::LiveExtension {
            version: "1.6".to_string(),
            schema: "public".to_string(),
        },
    );

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(
        changes
            .iter()
            .any(|c| matches!(c, Change::DropExtension { name } if name == "pg_trgm")),
        "expected DropExtension(pg_trgm): {changes:?}"
    );
}

#[test]
fn preserve_extensions_suppresses_dropped_extension_changes() {
    let source = r#"
datasource db {
  provider            = "postgresql"
  url                 = "postgres://localhost/test"
  extensions          = [pgcrypto]
  preserve_extensions = true
}

model Doc { id Int @id }
"#;
    let target = common::parse(source).unwrap();
    let mut live = LiveSchema::default();
    live.extensions.insert(
        "pg_trgm".to_string(),
        nautilus_migrate::LiveExtension {
            version: "1.6".to_string(),
            schema: "public".to_string(),
        },
    );

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(
        changes
            .iter()
            .any(|c| matches!(c, Change::CreateExtension { name, .. } if name == "pgcrypto")),
        "declared missing extensions should still be created: {changes:?}"
    );
    assert!(
        !changes
            .iter()
            .any(|c| matches!(c, Change::DropExtension { name } if name == "pg_trgm")),
        "extra live extensions should be preserved: {changes:?}"
    );
}

#[test]
fn no_changes_when_extensions_match() {
    let source = r#"
datasource db {
  provider   = "postgresql"
  url        = "postgres://localhost/test"
  extensions = [pg_trgm, pgcrypto]
}

model Doc { id Int @id }
"#;
    let target = common::parse(source).unwrap();
    let mut live = LiveSchema::default();
    live.extensions.insert(
        "pg_trgm".to_string(),
        nautilus_migrate::LiveExtension {
            version: "1.6".to_string(),
            schema: "public".to_string(),
        },
    );
    live.extensions.insert(
        "pgcrypto".to_string(),
        nautilus_migrate::LiveExtension {
            version: "1.3".to_string(),
            schema: "public".to_string(),
        },
    );
    // Stub in the live Doc table so only extension state is compared.
    live.tables.insert(
        "Doc".to_string(),
        nautilus_migrate::live::LiveTable {
            name: "Doc".to_string(),
            columns: vec![nautilus_migrate::live::LiveColumn {
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
    );

    let changes = SchemaDiff::compute(&live, &target, DatabaseProvider::Postgres);

    assert!(
        !changes.iter().any(|c| matches!(
            c,
            Change::CreateExtension { .. } | Change::DropExtension { .. }
        )),
        "unexpected extension changes: {changes:?}"
    );
}
