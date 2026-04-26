mod common;

use nautilus_migrate::live::{LiveColumn, LiveTable};
use nautilus_migrate::{
    change_risk, Change, ChangeRisk, DatabaseProvider, DdlGenerator, DiffApplier, LiveSchema,
};

#[test]
fn new_table_postgres() {
    let ir = common::parse(
        r#"model User {
  id        Int      @id
  name      String
  createdAt DateTime @map("created_at")

  @@index([createdAt])
}"#,
    )
    .unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
    let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);

    let model = ir.models.values().next().unwrap();
    let stmts = applier.sql_for(&Change::NewTable(model.clone())).unwrap();

    assert_eq!(stmts.len(), 2);
    assert!(stmts[0].contains("CREATE TABLE"));
    assert!(stmts[0].contains("\"User\""));
    assert!(stmts[1].contains("CREATE INDEX IF NOT EXISTS"));
    assert!(stmts[1].contains("\"idx_User_created_at\""));
}

#[test]
fn new_table_mysql() {
    let ir = common::parse("model Post { id Int @id }").unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Mysql);
    let applier = DiffApplier::new(DatabaseProvider::Mysql, &ddl, &ir, &live);

    let model = ir.models.values().next().unwrap();
    let stmts = applier.sql_for(&Change::NewTable(model.clone())).unwrap();

    assert!(stmts[0].contains("`Post`"));
}

#[test]
fn drop_table_sqlite() {
    let ir = common::parse("model Dummy { id Int @id }").unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Sqlite);
    let applier = DiffApplier::new(DatabaseProvider::Sqlite, &ddl, &ir, &live);

    let stmts = applier
        .sql_for(&Change::DroppedTable {
            name: "OldTable".to_string(),
        })
        .unwrap();

    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0], "DROP TABLE IF EXISTS \"OldTable\"");
}

#[test]
fn drop_table_postgres_uses_cascade() {
    let ir = common::parse("model Dummy { id Int @id }").unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
    let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);

    let stmts = applier
        .sql_for(&Change::DroppedTable {
            name: "OldTable".to_string(),
        })
        .unwrap();

    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0], "DROP TABLE IF EXISTS \"OldTable\" CASCADE");
}

#[test]
fn drop_extension_postgres_is_destructive_and_uses_restrictive_drop() {
    let ir = common::parse("model Dummy { id Int @id }").unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
    let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);
    let change = Change::DropExtension {
        name: "citext".to_string(),
    };

    assert_eq!(change_risk(&change), ChangeRisk::Destructive);

    let stmts = applier.sql_for(&change).unwrap();
    assert_eq!(stmts, vec!["DROP EXTENSION IF EXISTS \"citext\""]);
    assert!(
        !stmts[0].contains("CASCADE"),
        "extension drops must not silently cascade: {stmts:?}"
    );
}

#[test]
fn add_column_postgres() {
    let ir = common::parse("model User { id Int @id  email String? }").unwrap();
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

    let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
    let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);

    let email = ir
        .models
        .values()
        .next()
        .unwrap()
        .fields
        .iter()
        .find(|f| f.db_name == "email")
        .unwrap();

    let stmts = applier
        .sql_for(&Change::AddedColumn {
            table: "User".to_string(),
            field: email.clone(),
        })
        .unwrap();

    assert_eq!(stmts.len(), 1);
    assert!(stmts[0].contains("ALTER TABLE"));
    assert!(stmts[0].contains("ADD COLUMN"));
    assert!(stmts[0].contains("\"email\""));
}

#[test]
fn drop_column_postgres() {
    let ir = common::parse("model User { id Int @id }").unwrap();
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
                name: "old".to_string(),
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

    let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
    let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);

    let stmts = applier
        .sql_for(&Change::DroppedColumn {
            table: "User".to_string(),
            column: "old".to_string(),
        })
        .unwrap();

    assert_eq!(stmts.len(), 1);
    assert!(stmts[0].contains("DROP COLUMN"));
}

#[test]
fn drop_column_sqlite_triggers_rebuild() {
    let ir = common::parse("model User { id Int @id }").unwrap();
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
                name: "old".to_string(),
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

    let ddl = DdlGenerator::new(DatabaseProvider::Sqlite);
    let applier = DiffApplier::new(DatabaseProvider::Sqlite, &ddl, &ir, &live);

    let stmts = applier
        .sql_for(&Change::DroppedColumn {
            table: "User".to_string(),
            column: "old".to_string(),
        })
        .unwrap();

    assert_eq!(stmts.len(), 5);
    assert!(stmts[0].contains("DROP TABLE IF EXISTS"));
    assert!(stmts[0].contains("__tmp_User"));
    assert!(stmts[1].contains("CREATE TABLE"));
    assert!(stmts[1].contains("__tmp_User"));
    assert!(stmts[3].contains("DROP TABLE"));
    assert!(stmts[4].contains("RENAME TO"));
}

#[test]
fn index_added_and_dropped() {
    let ir = common::parse("model User { id Int @id  email String }").unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
    let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);

    let cols = vec!["email".to_string()];

    let add_stmts = applier
        .sql_for(&Change::IndexAdded {
            table: "User".to_string(),
            columns: cols.clone(),
            unique: true,
            kind: nautilus_schema::ir::IndexKind::Default,
            index_name: None,
        })
        .unwrap();
    assert_eq!(add_stmts.len(), 1);
    assert!(add_stmts[0].contains("CREATE UNIQUE INDEX"));

    let drop_stmts = applier
        .sql_for(&Change::IndexDropped {
            table: "User".to_string(),
            columns: cols,
            unique: false,
            index_name: "idx_User_email".to_string(),
        })
        .unwrap();
    assert_eq!(drop_stmts.len(), 1);
    assert!(drop_stmts[0].contains("DROP INDEX"));
    assert!(drop_stmts[0].contains("idx_User_email"));
}

#[test]
fn default_changed_set_postgres() {
    let ir = common::parse("model T { id Int @id  n Int @default(42) }").unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
    let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);

    let stmts = applier
        .sql_for(&Change::DefaultChanged {
            table: "T".to_string(),
            column: "n".to_string(),
            from: None,
            to: Some("42".to_string()),
        })
        .unwrap();

    assert_eq!(stmts.len(), 1);
    assert!(stmts[0].contains("SET DEFAULT 42"));
}

#[test]
fn add_required_column_no_default_returns_error() {
    let ir = common::parse("model User { id Int @id  name String }").unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
    let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);

    let field = ir
        .models
        .values()
        .next()
        .unwrap()
        .fields
        .iter()
        .find(|f| f.db_name == "name")
        .unwrap()
        .clone();

    let result = applier.sql_for(&Change::AddedColumn {
        table: "User".to_string(),
        field,
    });

    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("NOT NULL") || msg.contains("no @default"));
}

#[test]
fn default_changed_drop_postgres() {
    let ir = common::parse("model T { id Int @id  n Int }").unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
    let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);

    let stmts = applier
        .sql_for(&Change::DefaultChanged {
            table: "T".to_string(),
            column: "n".to_string(),
            from: Some("42".to_string()),
            to: None,
        })
        .unwrap();

    assert_eq!(stmts.len(), 1);
    assert!(stmts[0].contains("DROP DEFAULT"));
}

#[test]
fn drop_index_uses_live_physical_name() {
    let ir = common::parse("model User { id Int @id }").unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
    let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);

    let stmts = applier
        .sql_for(&Change::IndexDropped {
            table: "User".to_string(),
            columns: vec!["email".to_string()],
            unique: false,
            index_name: "my_custom_idx".to_string(),
        })
        .unwrap();

    assert_eq!(stmts.len(), 1);
    assert!(stmts[0].contains("my_custom_idx"));
    assert!(!stmts[0].contains("idx_User_email"));
}

#[test]
fn drop_index_uses_live_physical_name_mysql() {
    let ir = common::parse("model User { id Int @id }").unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Mysql);
    let applier = DiffApplier::new(DatabaseProvider::Mysql, &ddl, &ir, &live);

    let stmts = applier
        .sql_for(&Change::IndexDropped {
            table: "User".to_string(),
            columns: vec!["email".to_string()],
            unique: false,
            index_name: "my_custom_idx".to_string(),
        })
        .unwrap();

    assert_eq!(stmts.len(), 1);
    assert!(stmts[0].contains("my_custom_idx"));
    assert!(stmts[0].contains("`User`"));
}

#[test]
fn index_added_with_custom_map_generates_correct_create() {
    let ir = common::parse("model User { id Int @id }").unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
    let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);

    let stmts = applier
        .sql_for(&Change::IndexAdded {
            table: "User".to_string(),
            columns: vec!["email".to_string()],
            unique: false,
            kind: nautilus_schema::ir::IndexKind::Default,
            index_name: Some("email_lookup".to_string()),
        })
        .unwrap();

    assert_eq!(stmts.len(), 1);
    assert!(stmts[0].contains("email_lookup"));
    assert!(!stmts[0].contains("idx_User_email"));
}

#[test]
fn index_added_with_hash_type_postgres() {
    use nautilus_schema::ir::{BasicIndexType, IndexKind};

    let ir = common::parse("model User { id Int @id }").unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
    let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);

    let stmts = applier
        .sql_for(&Change::IndexAdded {
            table: "User".to_string(),
            columns: vec!["email".to_string()],
            unique: false,
            kind: IndexKind::Basic(BasicIndexType::Hash),
            index_name: Some("email_hash_idx".to_string()),
        })
        .unwrap();

    assert_eq!(stmts.len(), 1);
    assert!(stmts[0].contains("USING HASH"));
    assert!(stmts[0].contains("email_hash_idx"));
}

#[test]
fn index_added_with_pgvector_hnsw_options_postgres() {
    use nautilus_schema::ir::{
        IndexKind, PgvectorIndex, PgvectorIndexOptions, PgvectorMethod, PgvectorOpClass,
    };

    let ir = common::parse("model User { id Int @id }").unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
    let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);

    let stmts = applier
        .sql_for(&Change::IndexAdded {
            table: "Embedding".to_string(),
            columns: vec!["embedding".to_string()],
            unique: false,
            kind: IndexKind::Pgvector(PgvectorIndex {
                method: PgvectorMethod::Hnsw,
                opclass: Some(PgvectorOpClass::CosineOps),
                options: PgvectorIndexOptions {
                    m: Some(16),
                    ef_construction: Some(64),
                    lists: None,
                },
            }),
            index_name: Some("embedding_hnsw_idx".to_string()),
        })
        .unwrap();

    assert_eq!(stmts.len(), 1);
    assert!(stmts[0].contains("USING HNSW"));
    assert!(stmts[0].contains("(\"embedding\" vector_cosine_ops)"));
    assert!(stmts[0].contains("WITH (m = 16, ef_construction = 64)"));
}

#[test]
fn foreign_key_added_generates_sql_postgres() {
    let ir = common::parse("model User { id Int @id }").unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
    let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);

    let stmts = applier
        .sql_for(&Change::ForeignKeyAdded {
            table: "Post".to_string(),
            constraint_name: "fk_Post_authorId".to_string(),
            columns: vec!["authorId".to_string()],
            referenced_table: "User".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: Some("CASCADE".to_string()),
            on_update: Some("RESTRICT".to_string()),
        })
        .unwrap();

    assert_eq!(stmts.len(), 1);
    assert!(stmts[0].contains("ALTER TABLE \"Post\" ADD CONSTRAINT \"fk_Post_authorId\""));
    assert!(stmts[0].contains("FOREIGN KEY (\"authorId\") REFERENCES \"User\" (\"id\")"));
    assert!(stmts[0].contains("ON DELETE CASCADE"));
    assert!(stmts[0].contains("ON UPDATE RESTRICT"));
}

#[test]
fn foreign_key_dropped_generates_sql_mysql() {
    let ir = common::parse("model User { id Int @id }").unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Mysql);
    let applier = DiffApplier::new(DatabaseProvider::Mysql, &ddl, &ir, &live);

    let stmts = applier
        .sql_for(&Change::ForeignKeyDropped {
            table: "Post".to_string(),
            constraint_name: "fk_Post_authorId".to_string(),
        })
        .unwrap();

    assert_eq!(stmts.len(), 1);
    assert_eq!(
        stmts[0],
        "ALTER TABLE `Post` DROP FOREIGN KEY `fk_Post_authorId`"
    );
}

#[test]
fn drop_enum_postgres_quotes_mixed_case_name() {
    let ir =
        common::parse("enum PostStatus { DRAFT }\nmodel Post { id Int @id status PostStatus }")
            .unwrap();
    let live = LiveSchema::default();
    let ddl = DdlGenerator::new(DatabaseProvider::Postgres);
    let applier = DiffApplier::new(DatabaseProvider::Postgres, &ddl, &ir, &live);

    let stmts = applier
        .sql_for(&Change::DropEnum {
            name: "PostStatus".to_string(),
        })
        .unwrap();

    assert_eq!(
        stmts,
        vec!["DROP TYPE IF EXISTS \"PostStatus\"".to_string()]
    );
}
