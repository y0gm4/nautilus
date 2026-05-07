use std::collections::HashMap;

use super::{
    group_mysql_foreign_keys, normalize_mysql_check_expr, normalize_mysql_type, SchemaInspector,
};
use crate::error::{MigrationError, Result};
use crate::live::{ComputedKind, LiveColumn, LiveIndex, LiveIndexKind, LiveSchema, LiveTable};
use nautilus_schema::ir::BasicIndexType;

impl SchemaInspector {
    pub(super) async fn inspect_mysql(&self) -> Result<LiveSchema> {
        use sqlx::Row as _;

        let pool = sqlx::MySqlPool::connect(&self.url)
            .await
            .map_err(|e| MigrationError::Database(format!("MySQL connection failed: {e}")))?;

        let table_rows = sqlx::query(
            "SELECT table_name \
             FROM information_schema.tables \
             WHERE table_schema = DATABASE() \
               AND table_type   = 'BASE TABLE' \
             ORDER BY table_name",
        )
        .fetch_all(&pool)
        .await
        .map_err(|e| MigrationError::Database(e.to_string()))?;

        let table_names: Vec<String> = table_rows
            .into_iter()
            .map(|r| r.try_get::<String, _>("table_name"))
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| MigrationError::Database(e.to_string()))?;

        let table_names: Vec<String> = table_names
            .into_iter()
            .filter(|table_name| !table_name.starts_with("_nautilus_"))
            .collect();

        // Group shared metadata once so `db pull`/`db push` do not re-query
        // `information_schema` for every single table.
        let mut columns_by_table = split_mysql_rows_by_table(
            sqlx::query(
                "SELECT table_name, column_name, column_type, is_nullable, column_default, \
                        generation_expression, extra \
                 FROM information_schema.columns \
                 WHERE table_schema = DATABASE() \
                 ORDER BY table_name, ordinal_position",
            )
            .fetch_all(&pool)
            .await
            .map_err(|e| MigrationError::Database(e.to_string()))?,
            "column metadata",
        )?;

        let mut indexes_by_table = split_mysql_rows_by_table(
            sqlx::query(
                "SELECT table_name, index_name, column_name, non_unique, seq_in_index, index_type \
                 FROM information_schema.statistics \
                 WHERE table_schema = DATABASE() \
                 ORDER BY table_name, index_name, seq_in_index",
            )
            .fetch_all(&pool)
            .await
            .map_err(|e| MigrationError::Database(e.to_string()))?,
            "index metadata",
        )?;

        let mut foreign_keys_by_table = split_mysql_rows_by_table(
            sqlx::query(
                "SELECT \
                     kcu.table_name, \
                     kcu.constraint_name, \
                     kcu.column_name, \
                     kcu.referenced_table_name, \
                     kcu.referenced_column_name, \
                     rc.delete_rule, \
                     rc.update_rule \
                 FROM information_schema.key_column_usage kcu \
                 JOIN information_schema.referential_constraints rc \
                   ON kcu.constraint_name   = rc.constraint_name \
                  AND kcu.constraint_schema = rc.constraint_schema \
                 WHERE kcu.table_schema = DATABASE() \
                   AND kcu.referenced_table_name IS NOT NULL \
                 ORDER BY kcu.table_name, kcu.constraint_name, kcu.ordinal_position",
            )
            .fetch_all(&pool)
            .await
            .map_err(|e| MigrationError::Database(e.to_string()))?,
            "foreign key metadata",
        )?;

        let mut checks_by_table = split_mysql_rows_by_table(
            sqlx::query(
                "SELECT tc.table_name, tc.constraint_name, cc.check_clause \
                 FROM information_schema.table_constraints tc \
                 JOIN information_schema.check_constraints cc \
                   ON cc.constraint_schema = tc.constraint_schema \
                  AND cc.constraint_name   = tc.constraint_name \
                 WHERE tc.table_schema     = DATABASE() \
                   AND tc.constraint_type  = 'CHECK' \
                 ORDER BY tc.table_name, tc.constraint_name",
            )
            .fetch_all(&pool)
            .await
            .map_err(|e| MigrationError::Database(e.to_string()))?,
            "CHECK constraints",
        )?;

        let mut live = LiveSchema::default();

        for table_name in table_names {
            let col_rows = columns_by_table.remove(&table_name).unwrap_or_default();

            let mut columns = Vec::with_capacity(col_rows.len());
            for row in &col_rows {
                let col_name: String = row
                    .try_get("column_name")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let column_type: String = row
                    .try_get("column_type")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let is_nullable: String = row
                    .try_get("is_nullable")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let column_default: Option<String> = row
                    .try_get("column_default")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let generation_expression: Option<String> = row
                    .try_get("generation_expression")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let extra: String = row
                    .try_get("extra")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;

                let generated_expr = generation_expression.filter(|s| !s.is_empty());
                let computed_kind = generated_expr.as_ref().map(|_| {
                    if extra.to_lowercase().contains("virtual") {
                        ComputedKind::Virtual
                    } else {
                        ComputedKind::Stored
                    }
                });

                columns.push(LiveColumn {
                    name: col_name,
                    col_type: normalize_mysql_type(&column_type),
                    nullable: is_nullable.eq_ignore_ascii_case("YES"),
                    default_value: column_default,
                    generated_expr,
                    computed_kind,
                    check_expr: None,
                });
            }

            let stat_rows = indexes_by_table.remove(&table_name).unwrap_or_default();

            let mut primary_key = Vec::new();
            let mut idx_order = Vec::new();
            let mut idx_map = std::collections::HashMap::new();

            for row in &stat_rows {
                let index_name: String = row
                    .try_get("index_name")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let col_name: String = row
                    .try_get("column_name")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let non_unique: i8 = row
                    .try_get("non_unique")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let index_type: String = row
                    .try_get("index_type")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;

                if index_name == "PRIMARY" {
                    primary_key.push(col_name);
                    continue;
                }

                if !idx_map.contains_key(&index_name) {
                    idx_order.push(index_name.clone());
                    idx_map.insert(
                        index_name.clone(),
                        (non_unique == 0, index_type, Vec::new()),
                    );
                }
                idx_map.get_mut(&index_name).unwrap().2.push(col_name);
            }

            let indexes: Vec<LiveIndex> = idx_order
                .into_iter()
                .filter_map(|name| {
                    idx_map.remove(&name).map(|(unique, method, columns)| {
                        let lower = method.to_lowercase();
                        let kind = match lower.parse::<BasicIndexType>() {
                            Ok(b) => LiveIndexKind::Basic(b),
                            Err(_) => LiveIndexKind::Unknown(Some(lower)),
                        };
                        LiveIndex {
                            name,
                            columns,
                            unique,
                            kind,
                        }
                    })
                })
                .collect();

            let fk_rows = foreign_keys_by_table
                .remove(&table_name)
                .unwrap_or_default();
            let check_rows = checks_by_table.remove(&table_name).unwrap_or_default();

            let mut table_check_constraints = Vec::new();
            let mut column_check_map = std::collections::HashMap::new();
            let col_prefix = format!("chk_{}_", table_name);
            let column_names: std::collections::HashSet<&str> =
                columns.iter().map(|c| c.name.as_str()).collect();

            for row in &check_rows {
                let con_name: String = row
                    .try_get("constraint_name")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;
                let check_clause: String = row
                    .try_get("check_clause")
                    .map_err(|e| MigrationError::Database(e.to_string()))?;

                let expr = normalize_mysql_check_expr(&check_clause);
                let col_name = con_name
                    .strip_prefix(&col_prefix)
                    .filter(|cand| column_names.contains(cand))
                    .map(|s| s.to_string());

                if let Some(col) = col_name {
                    column_check_map.insert(col, expr);
                } else {
                    table_check_constraints.push(expr);
                }
            }

            for col in &mut columns {
                if let Some(expr) = column_check_map.get(&col.name) {
                    col.check_expr = Some(expr.clone());
                }
            }

            live.tables.insert(
                table_name.clone(),
                LiveTable {
                    name: table_name,
                    columns,
                    primary_key,
                    indexes,
                    check_constraints: table_check_constraints,
                    foreign_keys: group_mysql_foreign_keys(fk_rows),
                },
            );
        }

        Ok(live)
    }
}

fn split_mysql_rows_by_table(
    rows: Vec<sqlx::mysql::MySqlRow>,
    metadata_label: &str,
) -> Result<HashMap<String, Vec<sqlx::mysql::MySqlRow>>> {
    use sqlx::Row as _;

    let mut grouped: HashMap<String, Vec<sqlx::mysql::MySqlRow>> = HashMap::new();
    for row in rows {
        let table_name: String = row.try_get("table_name").map_err(|e| {
            MigrationError::Database(format!(
                "failed to read table_name while grouping MySQL {metadata_label}: {e}"
            ))
        })?;
        grouped.entry(table_name).or_default().push(row);
    }
    Ok(grouped)
}
