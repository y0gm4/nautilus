use super::{
    group_sqlite_foreign_keys, normalize_sqlite_default, normalize_sqlite_type,
    parse_sqlite_check_constraints, parse_sqlite_generated_exprs, SchemaInspector,
};
use crate::error::{MigrationError, Result};
use crate::live::{ComputedKind, LiveColumn, LiveIndex, LiveIndexKind, LiveSchema, LiveTable};

impl SchemaInspector {
    pub(super) async fn inspect_sqlite(&self) -> Result<LiveSchema> {
        use sqlx::Row as _;

        let opts: sqlx::sqlite::SqliteConnectOptions = self
            .url
            .parse::<sqlx::sqlite::SqliteConnectOptions>()
            .map_err(|e| MigrationError::Database(e.to_string()))?
            .create_if_missing(false);

        let pool = sqlx::SqlitePool::connect_with(opts)
            .await
            .map_err(|e| MigrationError::Database(format!("SQLite connection failed: {e}")))?;

        let table_rows = sqlx::query(
            "SELECT name, COALESCE(sql, '') AS create_sql FROM sqlite_master \
             WHERE type = 'table' \
               AND name NOT LIKE 'sqlite_%' \
               AND name NOT LIKE '_nautilus_%' \
             ORDER BY name",
        )
        .fetch_all(&pool)
        .await
        .map_err(|e| MigrationError::Database(e.to_string()))?;

        let tables: Vec<(String, String)> = table_rows
            .into_iter()
            .map(|r| {
                let table_name = r
                    .try_get::<String, _>("name")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let create_sql = r
                    .try_get::<String, _>("create_sql")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                Ok((table_name, create_sql))
            })
            .collect::<Result<_>>()?;

        let mut live = LiveSchema::default();

        for (table_name, create_sql) in tables {
            let pragma_sql = format!("PRAGMA table_xinfo(\"{}\")", table_name);
            let col_rows = sqlx::query(&pragma_sql)
                .fetch_all(&pool)
                .await
                .map_err(|e| MigrationError::Database(e.to_string()))?;

            let gen_exprs = parse_sqlite_generated_exprs(&create_sql);
            let (column_check_map, table_check_constraints) =
                parse_sqlite_check_constraints(&create_sql);

            let mut columns = Vec::new();
            let mut primary_key = Vec::new();

            for row in &col_rows {
                let col_name: String = row
                    .try_get("name")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let type_str: String = row
                    .try_get("type")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let notnull: i64 = row
                    .try_get("notnull")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let dflt_value: Option<String> = row
                    .try_get("dflt_value")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let pk_seq: i64 = row
                    .try_get("pk")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let hidden: i64 = row
                    .try_get("hidden")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;

                let col_type = normalize_sqlite_type(&type_str);
                let is_pk = pk_seq > 0;
                let nullable = notnull == 0 && !is_pk;

                if is_pk {
                    primary_key.push((pk_seq as i32, col_name.clone()));
                }

                let generated_expr = if hidden >= 2 {
                    gen_exprs.get(&col_name.to_lowercase()).cloned()
                } else {
                    None
                };
                let computed_kind = generated_expr.as_ref().map(|_| {
                    if hidden == 2 {
                        ComputedKind::Virtual
                    } else {
                        ComputedKind::Stored
                    }
                });

                columns.push(LiveColumn {
                    name: col_name.clone(),
                    col_type,
                    nullable,
                    default_value: dflt_value.map(|s| normalize_sqlite_default(&s)),
                    generated_expr,
                    computed_kind,
                    check_expr: column_check_map.get(&col_name.to_lowercase()).cloned(),
                });
            }

            primary_key.sort_by_key(|(seq, _)| *seq);
            let primary_key = primary_key.into_iter().map(|(_, name)| name).collect();

            let index_list_sql = format!("PRAGMA index_list(\"{}\")", table_name);
            let idx_list_rows = sqlx::query(&index_list_sql)
                .fetch_all(&pool)
                .await
                .map_err(|e| MigrationError::Database(e.to_string()))?;

            let mut indexes = Vec::new();
            for idx_row in &idx_list_rows {
                let idx_name: String = idx_row
                    .try_get("name")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let unique_val: i64 = idx_row
                    .try_get("unique")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let origin: String = idx_row
                    .try_get("origin")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;

                if origin == "pk" {
                    continue;
                }

                let index_info_sql = format!("PRAGMA index_info(\"{}\")", idx_name);
                let idx_info_rows = sqlx::query(&index_info_sql)
                    .fetch_all(&pool)
                    .await
                    .map_err(|e| MigrationError::Database(e.to_string()))?;

                let mut idx_cols = Vec::new();
                for irow in &idx_info_rows {
                    let seqno: i64 = irow
                        .try_get("seqno")
                        .map_err(|e| MigrationError::Database(e.to_string()))?;
                    let col: String = irow
                        .try_get("name")
                        .map_err(|e| MigrationError::Database(e.to_string()))?;
                    idx_cols.push((seqno, col));
                }
                idx_cols.sort_by_key(|(seq, _)| *seq);

                indexes.push(LiveIndex {
                    name: idx_name,
                    columns: idx_cols.into_iter().map(|(_, col)| col).collect(),
                    unique: unique_val != 0,
                    kind: LiveIndexKind::Unknown(None),
                });
            }

            let fk_pragma_sql = format!("PRAGMA foreign_key_list(\"{}\")", table_name);
            let fk_pragma_rows = sqlx::query(&fk_pragma_sql)
                .fetch_all(&pool)
                .await
                .map_err(|e| MigrationError::Database(e.to_string()))?;

            let foreign_keys = group_sqlite_foreign_keys(&table_name, fk_pragma_rows);

            live.tables.insert(
                table_name.clone(),
                LiveTable {
                    name: table_name,
                    columns,
                    primary_key,
                    indexes,
                    check_constraints: table_check_constraints,
                    foreign_keys,
                },
            );
        }

        Ok(live)
    }
}
