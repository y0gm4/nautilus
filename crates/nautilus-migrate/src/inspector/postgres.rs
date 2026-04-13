use super::{
    group_pg_foreign_keys, group_pg_indexes, normalize_pg_check_expr,
    normalize_pg_composite_field_type, normalize_pg_default, normalize_pg_type, SchemaInspector,
};
use crate::error::{MigrationError, Result};
use crate::live::{
    ComputedKind, LiveColumn, LiveCompositeField, LiveCompositeType, LiveSchema, LiveTable,
};

impl SchemaInspector {
    pub(super) async fn inspect_postgres(&self) -> Result<LiveSchema> {
        use sqlx::Row as _;

        let pool = sqlx::PgPool::connect_with(postgres_connect_options(&self.url)?)
            .await
            .map_err(|e| MigrationError::Database(format!("PostgreSQL connection failed: {e}")))?;

        let schema_name: Option<String> = pg_query("SELECT current_schema() AS schema_name")
            .fetch_one(&pool)
            .await
            .map_err(|e| {
                MigrationError::Database(format!(
                    "failed to resolve current PostgreSQL schema: {e}"
                ))
            })?
            .try_get("schema_name")
            .map_err(|e| {
                MigrationError::Database(format!(
                    "failed to read current PostgreSQL schema name: {e}"
                ))
            })?;
        let schema_name = schema_name.unwrap_or_else(|| "public".to_string());

        let table_rows = pg_query(
            "SELECT c.relname AS table_name \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 \
               AND c.relkind IN ('r', 'p') \
               AND c.relname !~ '^_nautilus_' \
             ORDER BY c.relname",
        )
        .bind(&schema_name)
        .fetch_all(&pool)
        .await
        .map_err(|e| {
            MigrationError::Database(format!(
                "failed to list tables in PostgreSQL schema \"{schema_name}\": {e}"
            ))
        })?;

        let table_names: Vec<String> = table_rows
            .into_iter()
            .map(|r| r.try_get::<String, _>("table_name"))
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| {
                MigrationError::Database(format!(
                    "failed to read table metadata in PostgreSQL schema \"{schema_name}\": {e}"
                ))
            })?;

        let mut live = LiveSchema::default();

        for table_name in table_names {
            let col_rows = pg_query(
                "SELECT column_name, \
                        udt_name, \
                        is_nullable, \
                        column_default, \
                        character_maximum_length, \
                        numeric_precision, \
                        numeric_scale, \
                        generation_expression \
                 FROM information_schema.columns \
                 WHERE table_schema = $1 \
                   AND table_name = $2 \
                 ORDER BY ordinal_position",
            )
            .bind(&schema_name)
            .bind(&table_name)
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                MigrationError::Database(format!(
                    "failed to fetch columns for table \"{table_name}\" in schema \"{schema_name}\": {e}"
                ))
            })?;

            let mut columns = Vec::new();
            for row in &col_rows {
                let col_name: String = row.try_get("column_name").map_err(|e| {
                    MigrationError::Database(format!(
                        "failed to read column_name while inspecting table \"{table_name}\": {e}"
                    ))
                })?;
                let udt_name: String = row
                    .try_get("udt_name")
                    .map_err(|e| {
                        MigrationError::Database(format!(
                            "failed to read udt_name for column \"{col_name}\" in table \"{table_name}\": {e}"
                        ))
                    })?;
                let is_nullable: String = row
                    .try_get("is_nullable")
                    .map_err(|e| {
                        MigrationError::Database(format!(
                            "failed to read nullability for column \"{col_name}\" in table \"{table_name}\": {e}"
                        ))
                    })?;
                let column_default: Option<String> = row
                    .try_get("column_default")
                    .map_err(|e| {
                        MigrationError::Database(format!(
                            "failed to read default value for column \"{col_name}\" in table \"{table_name}\": {e}"
                        ))
                    })?;
                let character_maximum_length: Option<i32> = row
                    .try_get("character_maximum_length")
                    .map_err(|e| {
                        MigrationError::Database(format!(
                            "failed to read character_maximum_length for column \"{col_name}\" in table \"{table_name}\": {e}"
                        ))
                    })?;
                let numeric_precision: Option<i32> = row
                    .try_get("numeric_precision")
                    .map_err(|e| {
                        MigrationError::Database(format!(
                            "failed to read numeric_precision for column \"{col_name}\" in table \"{table_name}\": {e}"
                        ))
                    })?;
                let numeric_scale: Option<i32> = row
                    .try_get("numeric_scale")
                    .map_err(|e| {
                        MigrationError::Database(format!(
                            "failed to read numeric_scale for column \"{col_name}\" in table \"{table_name}\": {e}"
                        ))
                    })?;
                let generation_expression: Option<String> = row
                    .try_get("generation_expression")
                    .map_err(|e| {
                        MigrationError::Database(format!(
                            "failed to read generation_expression for column \"{col_name}\" in table \"{table_name}\": {e}"
                        ))
                    })?;

                let col_type = normalize_pg_type(
                    &udt_name,
                    numeric_precision,
                    numeric_scale,
                    character_maximum_length,
                );
                let nullable = is_nullable.eq_ignore_ascii_case("YES");
                let default_value = column_default.map(|d| normalize_pg_default(&d));
                let generated_expr = generation_expression
                    .filter(|s| !s.is_empty())
                    .map(|s| normalize_pg_default(&s));
                let computed_kind = generated_expr.as_ref().map(|_| ComputedKind::Stored);

                columns.push(LiveColumn {
                    name: col_name,
                    col_type,
                    nullable,
                    default_value,
                    generated_expr,
                    computed_kind,
                    check_expr: None,
                });
            }

            let pk_rows = pg_query(
                "SELECT kcu.column_name \
                 FROM information_schema.table_constraints tc \
                 JOIN information_schema.key_column_usage kcu \
                   ON tc.constraint_name = kcu.constraint_name \
                  AND tc.table_schema    = kcu.table_schema \
                 WHERE tc.constraint_type = 'PRIMARY KEY' \
                   AND tc.table_schema    = $1 \
                   AND tc.table_name      = $2 \
                 ORDER BY kcu.ordinal_position",
            )
            .bind(&schema_name)
            .bind(&table_name)
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                MigrationError::Database(format!(
                    "failed to fetch primary key metadata for table \"{table_name}\" in schema \"{schema_name}\": {e}"
                ))
            })?;

            let primary_key: Vec<String> = pk_rows
                .into_iter()
                .map(|r| r.try_get::<String, _>("column_name"))
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| {
                    MigrationError::Database(format!(
                        "failed to read primary key metadata for table \"{table_name}\": {e}"
                    ))
                })?;

            let idx_rows = pg_query(
                "SELECT \
                     idx.relname                                           AS index_name, \
                     attr.attname                                          AS column_name, \
                     ix.indisunique                                        AS is_unique, \
                     am.amname                                             AS index_method \
                 FROM pg_class       tbl \
                 JOIN pg_namespace   ns   ON ns.oid            = tbl.relnamespace \
                 JOIN pg_index       ix   ON tbl.oid           = ix.indrelid \
                 JOIN pg_class       idx  ON idx.oid           = ix.indexrelid \
                 JOIN pg_am          am   ON am.oid            = idx.relam \
                 JOIN pg_attribute   attr ON attr.attrelid      = tbl.oid \
                                         AND attr.attnum        = ANY(ix.indkey) \
                 WHERE ns.nspname = $1 \
                   AND tbl.relname = $2 \
                   AND tbl.relkind = 'r' \
                   AND ix.indisprimary = false \
                 ORDER BY idx.relname, \
                          array_position(ix.indkey, attr.attnum)",
            )
            .bind(&schema_name)
            .bind(&table_name)
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                MigrationError::Database(format!(
                    "failed to fetch index metadata for table \"{table_name}\" in schema \"{schema_name}\": {e}"
                ))
            })?;

            let indexes = group_pg_indexes(idx_rows);

            let check_rows = pg_query(
                "SELECT c.conname AS constraint_name, \
                        pg_get_constraintdef(c.oid) AS constraint_def \
                 FROM pg_constraint c \
                 JOIN pg_class t ON t.oid = c.conrelid \
                 JOIN pg_namespace n ON n.oid = t.relnamespace \
                 WHERE c.contype = 'c' \
                   AND n.nspname = $1 \
                   AND t.relname = $2",
            )
            .bind(&schema_name)
            .bind(&table_name)
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                MigrationError::Database(format!(
                    "failed to fetch CHECK constraints for table \"{table_name}\" in schema \"{schema_name}\": {e}"
                ))
            })?;

            let mut table_check_constraints = Vec::new();
            let mut column_check_map = std::collections::HashMap::new();
            let col_prefix = format!("chk_{}_", table_name);

            for row in &check_rows {
                let con_name: String = row.try_get("constraint_name").map_err(|e| {
                    MigrationError::Database(format!(
                        "failed to read CHECK constraint name for table \"{table_name}\": {e}"
                    ))
                })?;
                let constraint_def: String = row
                    .try_get("constraint_def")
                    .map_err(|e| {
                        MigrationError::Database(format!(
                            "failed to read CHECK constraint definition \"{con_name}\" on table \"{table_name}\": {e}"
                        ))
                    })?;

                let expr = normalize_pg_check_expr(&constraint_def);
                let col_name = con_name
                    .strip_prefix(&col_prefix)
                    .filter(|cand| columns.iter().any(|c| c.name == *cand))
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

            let fk_rows = pg_query(
                "SELECT \
                     c.conname                                    AS constraint_name, \
                     a.attname                                    AS column_name, \
                     rf.relname                                   AS referenced_table, \
                     ra.attname                                   AS referenced_column, \
                     c.confdeltype::text                          AS delete_type, \
                     c.confupdtype::text                          AS update_type \
                 FROM pg_constraint c \
                 JOIN pg_class t   ON t.oid  = c.conrelid \
                 JOIN pg_class rf  ON rf.oid = c.confrelid \
                 JOIN pg_namespace n ON n.oid = t.relnamespace \
                 JOIN LATERAL unnest(c.conkey, c.confkey) \
                      WITH ORDINALITY AS u(local_att, ref_att, pos) ON true \
                 JOIN pg_attribute a  \
                   ON a.attrelid = c.conrelid  AND a.attnum = u.local_att \
                 JOIN pg_attribute ra \
                   ON ra.attrelid = c.confrelid AND ra.attnum = u.ref_att \
                 WHERE c.contype = 'f' \
                   AND n.nspname = $1 \
                   AND t.relname = $2 \
                 ORDER BY c.conname, u.pos",
            )
            .bind(&schema_name)
            .bind(&table_name)
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                MigrationError::Database(format!(
                    "failed to fetch foreign keys for table \"{table_name}\" in schema \"{schema_name}\": {e}"
                ))
            })?;

            let foreign_keys = group_pg_foreign_keys(fk_rows);

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

        let enum_rows = pg_query(
            "SELECT t.typname AS enum_name, e.enumlabel AS variant \
             FROM pg_type t \
             JOIN pg_enum e ON t.oid = e.enumtypid \
             JOIN pg_namespace n ON n.oid = t.typnamespace \
             WHERE n.nspname = $1 \
             ORDER BY t.typname, e.enumsortorder",
        )
        .bind(&schema_name)
        .fetch_all(&pool)
        .await
        .map_err(|e| {
            MigrationError::Database(format!(
                "failed to fetch enum types in PostgreSQL schema \"{schema_name}\": {e}"
            ))
        })?;

        for row in &enum_rows {
            let enum_name: String = row.try_get("enum_name").map_err(|e| {
                MigrationError::Database(format!(
                    "failed to read enum type name in schema \"{schema_name}\": {e}"
                ))
            })?;
            let variant: String = row
                .try_get("variant")
                .map_err(|e| {
                    MigrationError::Database(format!(
                        "failed to read enum variant for type \"{enum_name}\" in schema \"{schema_name}\": {e}"
                    ))
                })?;
            live.enums.entry(enum_name).or_default().push(variant);
        }

        let composite_rows = pg_query(
            "SELECT t.typname AS composite_name, \
                    a.attname AS field_name, \
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS field_type \
             FROM pg_type t \
             JOIN pg_namespace n ON n.oid = t.typnamespace \
             JOIN pg_attribute a ON a.attrelid = t.typrelid \
             WHERE t.typtype = 'c' \
               AND n.nspname = $1 \
               AND a.attnum > 0 \
               AND NOT a.attisdropped \
               AND NOT EXISTS ( \
                   SELECT 1 FROM pg_class c \
                   WHERE c.reltype = t.oid \
                     AND c.relkind IN ('r', 'v', 'm', 'p') \
               ) \
             ORDER BY t.typname, a.attnum",
        )
        .bind(&schema_name)
        .fetch_all(&pool)
        .await
        .map_err(|e| {
            MigrationError::Database(format!(
                "failed to fetch composite types in PostgreSQL schema \"{schema_name}\": {e}"
            ))
        })?;

        for row in &composite_rows {
            let composite_name: String = row.try_get("composite_name").map_err(|e| {
                MigrationError::Database(format!(
                    "failed to read composite type name in schema \"{schema_name}\": {e}"
                ))
            })?;
            let field_name: String = row
                .try_get("field_name")
                .map_err(|e| {
                    MigrationError::Database(format!(
                        "failed to read field name for composite type \"{composite_name}\" in schema \"{schema_name}\": {e}"
                    ))
                })?;
            let field_type: String = row
                .try_get("field_type")
                .map_err(|e| {
                    MigrationError::Database(format!(
                        "failed to read field type for \"{composite_name}.{field_name}\" in schema \"{schema_name}\": {e}"
                    ))
                })?;
            let entry = live
                .composite_types
                .entry(composite_name.clone())
                .or_insert_with(|| LiveCompositeType {
                    name: composite_name,
                    fields: Vec::new(),
                });
            entry.fields.push(LiveCompositeField {
                name: field_name,
                col_type: normalize_pg_composite_field_type(&field_type),
            });
        }

        Ok(live)
    }
}

fn pg_query(sql: &str) -> sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments> {
    // PgBouncer transaction pooling and similar proxies can reject named
    // prepared statements. `persistent(false)` keeps these metadata queries on
    // unnamed statements while still letting us bind parameters safely.
    sqlx::query::<sqlx::Postgres>(sql).persistent(false)
}

fn postgres_connect_options(url: &str) -> Result<sqlx::postgres::PgConnectOptions> {
    use std::str::FromStr;

    // `db pull`/`db push` introspection is often run through PgBouncer or other
    // transaction-pooling proxies where persistent named prepared statements are
    // not safe. Disabling the statement cache plus non-persistent queries keeps
    // introspection portable.
    sqlx::postgres::PgConnectOptions::from_str(url)
        .map(|options| options.statement_cache_capacity(0))
        .map_err(|e| MigrationError::Database(format!("Invalid PostgreSQL URL: {e}")))
}

#[cfg(test)]
mod tests {
    use super::pg_query;

    #[test]
    fn pg_introspection_queries_are_non_persistent() {
        assert!(!sqlx::Execute::persistent(&pg_query("SELECT 1")));
    }
}
