use crate::applier::DiffApplier;
use crate::ddl::{DatabaseProvider, DdlGenerator};
use crate::diff::{order_changes_for_apply, Change};
use crate::error::{MigrationError, Result};
use crate::live::{LiveIndex, LiveSchema, LiveTable};
use crate::migration::Migration;
use crate::provider::{CreateIndex, ProviderStrategy};
use crate::tracker::MigrationTracker;
use nautilus_schema::ir::SchemaIr;
use sqlx::AnyPool;
use std::sync::Arc;
use std::time::Instant;

/// Executes schema migrations
pub struct MigrationExecutor {
    pool: Arc<AnyPool>,
    tracker: MigrationTracker,
    generator: DdlGenerator,
}

impl MigrationExecutor {
    /// Create a new migration executor
    pub fn new(pool: AnyPool, provider: DatabaseProvider) -> Self {
        let pool_arc = Arc::new(pool);
        Self {
            pool: pool_arc.clone(),
            tracker: MigrationTracker::new(pool_arc, provider),
            generator: DdlGenerator::new(provider),
        }
    }

    /// Initialize migration tracking (create _nautilus_migrations table)
    pub async fn init(&self) -> Result<()> {
        self.tracker.init().await
    }

    /// Generate a migration from a schema
    pub fn generate_migration_from_schema(
        &self,
        name: String,
        schema: &SchemaIr,
    ) -> Result<Migration> {
        let up_sql = self.generator.generate_create_tables(schema)?;
        let down_sql = self.generator.generate_drop_tables(schema)?;

        Ok(Migration::new(name, up_sql, down_sql))
    }

    /// Generate a migration from a pre-computed list of [`Change`]s.
    ///
    /// Up SQL is derived by running each change through [`DiffApplier`].
    /// Down SQL contains best-effort reversals: safe changes (new table,
    /// added column, added index) are fully reversed; destructive changes
    /// (dropped table/column, type/PK change) emit a comment placeholder.
    pub fn generate_migration_from_diff(
        &self,
        name: String,
        changes: &[Change],
        schema: &SchemaIr,
        live: &LiveSchema,
    ) -> Result<Migration> {
        let provider = self.generator.provider();
        let applier = DiffApplier::new(provider, &self.generator, schema, live);

        let mut up_sql: Vec<String> = Vec::new();
        let mut down_sql: Vec<String> = Vec::new();

        let ordered_changes = order_changes_for_apply(changes, live);

        for change in &ordered_changes {
            let stmts = applier.sql_for(change)?;
            up_sql.extend(stmts);
            down_sql.extend(self.reverse_change(change, provider, live));
        }

        Ok(Migration::new(name, up_sql, down_sql))
    }

    /// Produce best-effort down-SQL for a single change.
    fn reverse_change(
        &self,
        change: &Change,
        provider: DatabaseProvider,
        live: &LiveSchema,
    ) -> Vec<String> {
        let q = |name: &str| provider.quote_identifier(name);
        let strategy = ProviderStrategy::new(provider);

        match change {
            Change::NewTable(model) => {
                vec![strategy.drop_table_sql(&model.db_name, false)]
            }

            Change::AddedColumn { table, field } => match provider {
                DatabaseProvider::Postgres | DatabaseProvider::Mysql => {
                    vec![format!(
                        "ALTER TABLE {} DROP COLUMN {}",
                        q(table),
                        q(&field.db_name),
                    )]
                }
                DatabaseProvider::Sqlite => {
                    vec![format!(
                        "-- Cannot auto-reverse ADD COLUMN on SQLite: {}.{}",
                        table, field.db_name,
                    )]
                }
            },

            Change::NullabilityChanged {
                table,
                column,
                now_required,
            } => strategy
                .reverse_nullability_change_sql(table, column, *now_required)
                .unwrap_or_else(|| {
                    vec![format!(
                        "-- Cannot auto-reverse nullability change: {}.{}",
                        table, column,
                    )]
                }),

            Change::DefaultChanged {
                table,
                column,
                from,
                ..
            } => strategy
                .reverse_default_change_sql(table, column, from.as_deref())
                .unwrap_or_else(|| {
                    vec![format!(
                        "-- Cannot auto-reverse DEFAULT change: {}.{}",
                        table, column,
                    )]
                }),

            Change::IndexAdded {
                table,
                columns,
                index_name,
                ..
            } => {
                let idx_name = index_name
                    .as_deref()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("idx_{}_{}", table, columns.join("_")));
                match provider {
                    DatabaseProvider::Postgres | DatabaseProvider::Sqlite => {
                        vec![format!("DROP INDEX IF EXISTS {}", q(&idx_name))]
                    }
                    DatabaseProvider::Mysql => {
                        vec![format!("DROP INDEX {} ON {}", q(&idx_name), q(table),)]
                    }
                }
            }

            Change::DroppedTable { name } => {
                if let Some(live_table) = live.tables.get(name) {
                    Self::create_table_sql_from_live(live_table, provider)
                } else {
                    vec![format!(
                        "-- Cannot auto-reverse: table {} was dropped (no live snapshot)",
                        name
                    )]
                }
            }
            Change::DroppedColumn { table, column } => {
                self.reverse_dropped_column(table, column, provider, live)
            }
            Change::TypeChanged {
                table,
                column,
                from,
                ..
            } => strategy
                .reverse_column_type_sql(table, column, from)
                .unwrap_or_else(|| {
                    vec![format!(
                        "-- Cannot auto-reverse TYPE change on {}.{} (was {})",
                        table, column, from,
                    )]
                }),

            Change::PrimaryKeyChanged { table } => {
                if let Some(live_table) = live.tables.get(table) {
                    if !live_table.primary_key.is_empty() {
                        let pk_cols = live_table
                            .primary_key
                            .iter()
                            .map(|c| q(c))
                            .collect::<Vec<_>>()
                            .join(", ");
                        match provider {
                            DatabaseProvider::Postgres => vec![
                                format!("ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}", q(table), q(&format!("{}_pkey", table))),
                                format!("ALTER TABLE {} ADD PRIMARY KEY ({})", q(table), pk_cols),
                            ],
                            DatabaseProvider::Mysql => vec![format!(
                                "ALTER TABLE {} DROP PRIMARY KEY, ADD PRIMARY KEY ({})",
                                q(table), pk_cols,
                            )],
                            DatabaseProvider::Sqlite => vec![format!(
                                "-- Cannot auto-reverse PRIMARY KEY change on {} (SQLite requires table rebuild)",
                                table,
                            )],
                        }
                    } else {
                        vec![format!(
                            "-- Cannot auto-reverse PRIMARY KEY change on {}: no live PK info",
                            table
                        )]
                    }
                } else {
                    vec![format!(
                        "-- Cannot auto-reverse PRIMARY KEY change on {}: no live snapshot",
                        table
                    )]
                }
            }

            Change::ComputedExprChanged { table, column, .. } => {
                vec![format!(
                    "-- Cannot auto-reverse computed expression change: {}.{}",
                    table, column,
                )]
            }

            Change::IndexDropped {
                table,
                columns,
                unique,
                index_name,
            } => {
                if let Some(live_index) = live
                    .tables
                    .get(table)
                    .and_then(|t| t.indexes.iter().find(|i| i.name == *index_name))
                {
                    vec![Self::create_index_sql_from_live(
                        table, live_index, provider,
                    )]
                } else {
                    let unique_kw = if *unique { "UNIQUE " } else { "" };
                    let cols_sql = columns.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
                    match provider {
                        DatabaseProvider::Postgres | DatabaseProvider::Sqlite => vec![format!(
                            "CREATE {}INDEX IF NOT EXISTS {} ON {} ({})",
                            unique_kw,
                            q(index_name),
                            q(table),
                            cols_sql,
                        )],
                        DatabaseProvider::Mysql => vec![format!(
                            "CREATE {}INDEX {} ON {} ({})",
                            unique_kw,
                            q(index_name),
                            q(table),
                            cols_sql,
                        )],
                    }
                }
            }

            Change::CheckChanged { table, column, .. } => {
                let target = match column {
                    Some(col) => format!("{}.{}", table, col),
                    None => table.to_string(),
                };
                vec![format!(
                    "-- Cannot auto-reverse CHECK constraint change on {}",
                    target,
                )]
            }

            Change::CreateCompositeType { name } => {
                if strategy.supports_user_defined_types() {
                    vec![format!("DROP TYPE IF EXISTS {}", q(name))]
                } else {
                    vec![]
                }
            }

            Change::DropCompositeType { name } | Change::AlterCompositeType { name, .. } => {
                if strategy.supports_user_defined_types() {
                    vec![format!(
                        "-- Cannot auto-reverse composite type change for '{}'; restore manually",
                        name,
                    )]
                } else {
                    vec![]
                }
            }

            Change::CreateEnum { name, .. } => {
                if strategy.supports_user_defined_types() {
                    vec![format!("DROP TYPE IF EXISTS {}", q(name))]
                } else {
                    vec![]
                }
            }

            Change::DropEnum { name } | Change::AlterEnum { name, .. } => {
                if strategy.supports_user_defined_types() {
                    vec![format!(
                        "-- Cannot auto-reverse enum type change for '{}'; restore manually",
                        name,
                    )]
                } else {
                    vec![]
                }
            }

            Change::ForeignKeyAdded {
                table,
                constraint_name,
                ..
            } => match provider {
                DatabaseProvider::Sqlite => vec![format!(
                    "-- Cannot auto-reverse ADD FOREIGN KEY on SQLite: {}",
                    constraint_name,
                )],
                DatabaseProvider::Postgres => vec![format!(
                    "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}",
                    q(table),
                    q(constraint_name),
                )],
                DatabaseProvider::Mysql => vec![format!(
                    "ALTER TABLE {} DROP FOREIGN KEY {}",
                    q(table),
                    q(constraint_name),
                )],
            },

            Change::ForeignKeyDropped {
                table,
                constraint_name,
            } => {
                // Reversing a DROP means re-adding the constraint, but we no
                // longer know the columns / referenced table at this point.
                vec![format!(
                    "-- Cannot auto-reverse DROP FOREIGN KEY {} on {}; restore manually",
                    constraint_name, table,
                )]
            }
        }
    }

    fn reverse_dropped_column(
        &self,
        table: &str,
        column: &str,
        provider: DatabaseProvider,
        live: &LiveSchema,
    ) -> Vec<String> {
        let Some(live_table) = live.tables.get(table) else {
            return Self::missing_live_column_snapshot(table, column);
        };
        let Some(col) = live_table
            .columns
            .iter()
            .find(|candidate| candidate.name == column)
        else {
            return Self::missing_live_column_snapshot(table, column);
        };

        match provider {
            DatabaseProvider::Postgres | DatabaseProvider::Mysql => {
                let q = |name: &str| provider.quote_identifier(name);
                let type_str = col.col_type.to_uppercase();
                let not_null = if col.nullable { "" } else { " NOT NULL" };
                let default_clause = col
                    .default_value
                    .as_deref()
                    .map(|default| format!(" DEFAULT {}", default))
                    .unwrap_or_default();

                vec![format!(
                    "ALTER TABLE {} ADD COLUMN {} {}{}{}",
                    q(table),
                    q(column),
                    type_str,
                    not_null,
                    default_clause,
                )]
            }
            DatabaseProvider::Sqlite => vec![format!(
                "-- Cannot auto-reverse dropped column on SQLite: {}.{}",
                table, column,
            )],
        }
    }

    fn missing_live_column_snapshot(table: &str, column: &str) -> Vec<String> {
        vec![format!(
            "-- Cannot auto-reverse: column {}.{} was dropped (no live snapshot)",
            table, column,
        )]
    }

    /// Generate a `CREATE TABLE … ` statement (plus any `CREATE INDEX` statements)
    /// from a live table snapshot. Used to build down-SQL for `DroppedTable`.
    fn create_table_sql_from_live(table: &LiveTable, provider: DatabaseProvider) -> Vec<String> {
        let q = |name: &str| provider.quote_identifier(name);

        // SQLite: single-column INTEGER PK -> must be inlined as
        // `col INTEGER PRIMARY KEY AUTOINCREMENT` (no separate PRIMARY KEY clause).
        let sqlite_inline_pk = provider == DatabaseProvider::Sqlite
            && table.primary_key.len() == 1
            && table
                .columns
                .iter()
                .any(|c| c.name == table.primary_key[0] && c.col_type.to_lowercase() == "integer");

        let mut col_lines: Vec<String> = Vec::new();
        for col in &table.columns {
            let is_pk = table.primary_key.contains(&col.name);
            if sqlite_inline_pk && is_pk {
                col_lines.push(format!(
                    "  {} INTEGER PRIMARY KEY AUTOINCREMENT",
                    q(&col.name)
                ));
            } else {
                let type_upper = col.col_type.to_uppercase();
                let mut parts = vec![q(&col.name), type_upper];
                if !col.nullable {
                    parts.push("NOT NULL".to_string());
                }
                if let Some(default) = &col.default_value {
                    parts.push(format!("DEFAULT {}", default));
                }
                col_lines.push(format!("  {}", parts.join(" ")));
            }
        }

        if !sqlite_inline_pk && !table.primary_key.is_empty() {
            let pk_cols = table
                .primary_key
                .iter()
                .map(|c| q(c))
                .collect::<Vec<_>>()
                .join(", ");
            col_lines.push(format!("  PRIMARY KEY ({})", pk_cols));
        }

        let mut stmts = vec![format!(
            "CREATE TABLE IF NOT EXISTS {} (\n{}\n)",
            q(&table.name),
            col_lines.join(",\n"),
        )];

        for idx in &table.indexes {
            stmts.push(Self::create_index_sql_from_live(&table.name, idx, provider));
        }

        stmts
    }

    fn create_index_sql_from_live(
        table_name: &str,
        index: &LiveIndex,
        provider: DatabaseProvider,
    ) -> String {
        ProviderStrategy::new(provider).create_index_sql(CreateIndex {
            table: table_name,
            name: &index.name,
            columns: &index.columns,
            unique: index.unique,
            method: index.method.as_deref(),
            if_not_exists: true,
        })
    }

    /// Apply a migration (run "up" direction).
    pub async fn apply_migration(&self, migration: &Migration) -> Result<()> {
        if self.tracker.is_applied(&migration.name).await? {
            return Err(MigrationError::AlreadyApplied(migration.name.clone()));
        }

        if !migration.verify_checksum() {
            return Err(MigrationError::InvalidState(
                "Migration checksum verification failed".to_string(),
            ));
        }

        let start = Instant::now();

        let mut tx =
            self.pool.begin().await.map_err(|e| {
                MigrationError::Database(format!("Failed to begin transaction: {}", e))
            })?;

        for sql in &migration.up_sql {
            self.execute_sql_in_tx(&mut tx, sql).await?;
        }

        let execution_time = start.elapsed().as_millis() as i64;

        self.tracker
            .record_migration_in_tx(&mut tx, migration, execution_time)
            .await?;

        tx.commit().await.map_err(|e| {
            MigrationError::Database(format!("Failed to commit transaction: {}", e))
        })?;

        Ok(())
    }

    /// Rollback a migration (run "down" direction)
    pub async fn rollback_migration(&self, migration: &Migration) -> Result<()> {
        if !self.tracker.is_applied(&migration.name).await? {
            return Err(MigrationError::NotFound(format!(
                "Migration '{}' is not applied",
                migration.name
            )));
        }

        let mut tx =
            self.pool.begin().await.map_err(|e| {
                MigrationError::Database(format!("Failed to begin transaction: {}", e))
            })?;

        for sql in &migration.down_sql {
            self.execute_sql_in_tx(&mut tx, sql).await?;
        }

        self.tracker
            .remove_migration_in_tx(&mut tx, &migration.name)
            .await?;

        tx.commit().await.map_err(|e| {
            MigrationError::Database(format!("Failed to commit transaction: {}", e))
        })?;

        Ok(())
    }

    /// Apply all pending migrations
    pub async fn apply_pending(&self, migrations: &[Migration]) -> Result<usize> {
        let mut applied_count = 0;

        for migration in migrations {
            if !self.tracker.is_applied(&migration.name).await? {
                self.apply_migration(migration).await?;
                applied_count += 1;
            }
        }

        Ok(applied_count)
    }

    /// Get the status of all migrations
    pub async fn migration_status(&self, migrations: &[Migration]) -> Result<Vec<(String, bool)>> {
        let mut status = Vec::new();

        for migration in migrations {
            let is_applied = self.tracker.is_applied(&migration.name).await?;
            status.push((migration.name.clone(), is_applied));
        }

        Ok(status)
    }

    /// Execute a SQL statement within a transaction.
    ///
    /// Statements that consist entirely of SQL comments (`--`) or whitespace
    /// are silently skipped — they appear in down-migration files when a change
    /// cannot be automatically reversed.
    async fn execute_sql_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Any>,
        sql: &str,
    ) -> Result<()> {
        let is_comment_only = sql
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .all(|l| l.starts_with("--"));

        if is_comment_only {
            return Ok(());
        }

        sqlx::query(sql)
            .persistent(false)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::{LiveColumn, LiveIndex};
    use nautilus_schema::{validate_schema, Lexer, Parser};

    fn parse(source: &str) -> crate::Result<nautilus_schema::ir::SchemaIr> {
        let mut lexer = Lexer::new(source);
        let mut tokens = Vec::new();
        loop {
            let token = lexer.next_token().map_err(crate::MigrationError::Schema)?;
            let is_eof = matches!(token.kind, nautilus_schema::TokenKind::Eof);
            tokens.push(token);
            if is_eof {
                break;
            }
        }
        let ast = Parser::new(&tokens, source)
            .parse_schema()
            .map_err(crate::MigrationError::Schema)?;
        validate_schema(ast).map_err(crate::MigrationError::Schema)
    }

    fn live_table_with_index(table: &str, column: &str, index: LiveIndex) -> LiveTable {
        LiveTable {
            name: table.to_string(),
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
                    name: column.to_string(),
                    col_type: "text".to_string(),
                    nullable: false,
                    default_value: None,
                    generated_expr: None,
                    computed_kind: None,
                    check_expr: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: vec![index],
            check_constraints: vec![],
            foreign_keys: vec![],
        }
    }

    #[test]
    fn dropped_table_down_sql_preserves_postgres_index_name_and_method() {
        let stmts = MigrationExecutor::create_table_sql_from_live(
            &live_table_with_index(
                "User",
                "email",
                LiveIndex {
                    name: "email_hash_idx".to_string(),
                    columns: vec!["email".to_string()],
                    unique: false,
                    method: Some("hash".to_string()),
                },
            ),
            DatabaseProvider::Postgres,
        );

        assert!(
            stmts.iter().any(|sql| {
                sql.contains("CREATE INDEX IF NOT EXISTS \"email_hash_idx\"")
                    && sql.contains("ON \"User\" USING HASH (\"email\")")
            }),
            "down SQL must preserve the live physical name and USING HASH method: {:?}",
            stmts
        );
        assert!(
            !stmts.iter().any(|sql| sql.contains("idx_User_email")),
            "down SQL must not fall back to auto-generated index names: {:?}",
            stmts
        );
    }

    #[test]
    fn dropped_table_down_sql_preserves_mysql_fulltext_index_name() {
        let stmts = MigrationExecutor::create_table_sql_from_live(
            &live_table_with_index(
                "Post",
                "body",
                LiveIndex {
                    name: "body_search".to_string(),
                    columns: vec!["body".to_string()],
                    unique: false,
                    method: Some("fulltext".to_string()),
                },
            ),
            DatabaseProvider::Mysql,
        );

        assert!(
            stmts.iter().any(|sql| {
                sql.contains("CREATE FULLTEXT INDEX `body_search`")
                    && sql.contains("ON `Post` (`body`)")
            }),
            "down SQL must preserve MySQL FULLTEXT index metadata: {:?}",
            stmts
        );
        assert!(
            !stmts.iter().any(|sql| sql.contains("idx_Post_body")),
            "down SQL must not fall back to auto-generated index names: {:?}",
            stmts
        );
    }

    #[tokio::test]
    #[ignore = "Requires database connection"]
    async fn test_migration_lifecycle() {
        let source = r#"
model User {
  id Int @id
  name String
}
"#;
        let schema = parse(source).unwrap();

        let pool = AnyPool::connect("sqlite::memory:").await.unwrap();
        let executor = MigrationExecutor::new(pool, DatabaseProvider::Sqlite);

        executor.init().await.unwrap();

        let migration = executor
            .generate_migration_from_schema("001_initial".to_string(), &schema)
            .unwrap();

        executor.apply_migration(&migration).await.unwrap();

        let status = executor
            .migration_status(std::slice::from_ref(&migration))
            .await
            .unwrap();
        assert_eq!(status.len(), 1);
        assert!(status[0].1);
    }
}
