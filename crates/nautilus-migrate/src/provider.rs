use crate::ddl::DatabaseProvider;
use crate::error::{MigrationError, Result};
use nautilus_schema::ast::StorageStrategy;
use nautilus_schema::ir::{BasicIndexType, IndexKind};

mod pgvector;

pub(crate) struct CreateIndex<'a> {
    pub(crate) table: &'a str,
    pub(crate) name: &'a str,
    pub(crate) columns: &'a [String],
    pub(crate) unique: bool,
    pub(crate) kind: &'a IndexKind,
    pub(crate) if_not_exists: bool,
}

pub(crate) struct AlterColumnType<'a> {
    pub(crate) table: &'a str,
    pub(crate) column: &'a str,
    pub(crate) target_type: &'a str,
    pub(crate) full_column_definition: Option<&'a str>,
}

pub(crate) struct AlterColumnNullability<'a> {
    pub(crate) table: &'a str,
    pub(crate) column: &'a str,
    pub(crate) now_required: bool,
    pub(crate) is_generated: bool,
    pub(crate) default_sql: Option<&'a str>,
    pub(crate) full_column_definition: Option<&'a str>,
}

pub(crate) struct AlterColumnDefault<'a> {
    pub(crate) table: &'a str,
    pub(crate) column: &'a str,
    pub(crate) new_default: Option<&'a str>,
    pub(crate) preserve_implicit_default: bool,
    pub(crate) full_column_definition: Option<&'a str>,
}

pub(crate) enum ProviderSqlPlan {
    Statements(Vec<String>),
    RequiresTableRebuild,
}

/// Provider-aware SQL fragments shared across DDL generation and migration flows.
pub(crate) struct ProviderStrategy {
    provider: DatabaseProvider,
}

impl ProviderStrategy {
    pub(crate) fn new(provider: DatabaseProvider) -> Self {
        Self { provider }
    }

    pub(crate) fn drop_table_sql(&self, table: &str, cascade: bool) -> String {
        if self.provider == DatabaseProvider::Postgres && cascade {
            format!(
                "DROP TABLE IF EXISTS {} CASCADE",
                self.provider.quote_identifier(table)
            )
        } else {
            format!(
                "DROP TABLE IF EXISTS {}",
                self.provider.quote_identifier(table)
            )
        }
    }

    pub(crate) fn supports_user_defined_types(&self) -> bool {
        self.provider == DatabaseProvider::Postgres
    }

    pub(crate) fn array_storage_sql(
        &self,
        storage_strategy: Option<StorageStrategy>,
    ) -> Option<&'static str> {
        match (self.provider, storage_strategy) {
            (DatabaseProvider::Mysql, Some(StorageStrategy::Json)) => Some("JSON"),
            (DatabaseProvider::Sqlite, Some(StorageStrategy::Json)) => Some("TEXT"),
            _ => None,
        }
    }

    pub(crate) fn composite_storage_sql(
        &self,
        storage_strategy: Option<StorageStrategy>,
    ) -> Option<&'static str> {
        self.array_storage_sql(storage_strategy)
    }

    pub(crate) fn native_array_support_error(&self) -> String {
        format!(
            "{} does not support native array types. Use @store(json) attribute.",
            self.provider_name()
        )
    }

    pub(crate) fn native_composite_support_error(&self, type_name: &str, is_array: bool) -> String {
        let subject = if is_array {
            "native composite type arrays"
        } else {
            "native composite types"
        };
        format!(
            "{} does not support {}. Add @store(Json) to the field using type '{}'.",
            self.provider_name(),
            subject,
            type_name,
        )
    }

    pub(crate) fn create_index_sql(&self, index: CreateIndex<'_>) -> String {
        let q = |name: &str| self.provider.quote_identifier(name);
        let unique_kw = if index.unique { "UNIQUE " } else { "" };

        let opclass_suffix = match index.kind {
            IndexKind::Pgvector(p) => pgvector::opclass_suffix(p),
            _ => None,
        };

        let columns_sql = index
            .columns
            .iter()
            .enumerate()
            .map(|(idx, column)| {
                let mut rendered = q(column);
                if idx == 0 {
                    if let Some(suffix) = opclass_suffix {
                        rendered.push(' ');
                        rendered.push_str(suffix);
                    }
                }
                rendered
            })
            .collect::<Vec<_>>()
            .join(", ");

        if self.provider == DatabaseProvider::Mysql
            && matches!(index.kind, IndexKind::Basic(BasicIndexType::FullText))
        {
            return format!(
                "CREATE FULLTEXT INDEX {} ON {} ({})",
                q(index.name),
                q(index.table),
                columns_sql,
            );
        }

        let using_clause = self.using_clause(index.kind);
        let with_clause = match (self.provider, index.kind) {
            (DatabaseProvider::Postgres, IndexKind::Pgvector(p)) => {
                pgvector::with_clause(p.method, &p.options)
            }
            _ => String::new(),
        };

        match self.provider {
            DatabaseProvider::Postgres | DatabaseProvider::Sqlite => {
                let if_not_exists = if index.if_not_exists {
                    " IF NOT EXISTS"
                } else {
                    ""
                };
                format!(
                    "CREATE {}INDEX{} {} ON {}{} ({})",
                    unique_kw,
                    if_not_exists,
                    q(index.name),
                    q(index.table),
                    using_clause,
                    columns_sql,
                ) + &with_clause
            }
            DatabaseProvider::Mysql => format!(
                "CREATE {}INDEX {} ON {} ({}){}",
                unique_kw,
                q(index.name),
                q(index.table),
                columns_sql,
                using_clause,
            ),
        }
    }

    fn using_clause(&self, kind: &IndexKind) -> String {
        match (self.provider, kind) {
            // Default and BTree are emitted without an explicit USING clause
            // (BTree is the default access method on every supported DBMS).
            (_, IndexKind::Default) => String::new(),
            (_, IndexKind::Basic(BasicIndexType::BTree)) => String::new(),
            (DatabaseProvider::Postgres, IndexKind::Basic(b)) => {
                format!(" USING {}", b.as_ddl_str().to_uppercase())
            }
            (DatabaseProvider::Postgres, IndexKind::Pgvector(p)) => {
                format!(" USING {}", p.method.as_ddl_str().to_uppercase())
            }
            (DatabaseProvider::Mysql, IndexKind::Basic(BasicIndexType::Hash)) => {
                " USING HASH".to_string()
            }
            // FullText on MySQL uses dedicated `CREATE FULLTEXT INDEX` syntax,
            // handled by the caller.
            _ => String::new(),
        }
    }

    pub(crate) fn alter_column_type_sql(
        &self,
        alteration: AlterColumnType<'_>,
    ) -> Result<ProviderSqlPlan> {
        let q = |name: &str| self.provider.quote_identifier(name);

        match self.provider {
            DatabaseProvider::Postgres => Ok(ProviderSqlPlan::Statements(vec![format!(
                "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                q(alteration.table),
                q(alteration.column),
                alteration.target_type,
            )])),
            DatabaseProvider::Mysql => {
                let col_def = alteration.full_column_definition.ok_or_else(|| {
                    MigrationError::Other(format!(
                        "Missing full column definition for MySQL type rewrite on {}.{}",
                        alteration.table, alteration.column
                    ))
                })?;
                Ok(ProviderSqlPlan::Statements(vec![format!(
                    "ALTER TABLE {} MODIFY COLUMN {}",
                    q(alteration.table),
                    col_def,
                )]))
            }
            DatabaseProvider::Sqlite => Ok(ProviderSqlPlan::RequiresTableRebuild),
        }
    }

    pub(crate) fn alter_column_nullability_sql(
        &self,
        alteration: AlterColumnNullability<'_>,
    ) -> Result<ProviderSqlPlan> {
        let q = |name: &str| self.provider.quote_identifier(name);

        match (self.provider, alteration.now_required) {
            (DatabaseProvider::Postgres, false) => Ok(ProviderSqlPlan::Statements(vec![format!(
                "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                q(alteration.table),
                q(alteration.column),
            )])),
            (DatabaseProvider::Postgres, true) => {
                if alteration.is_generated {
                    return Ok(ProviderSqlPlan::Statements(vec![format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                        q(alteration.table),
                        q(alteration.column),
                    )]));
                }

                if let Some(default_sql) = alteration.default_sql {
                    return Ok(ProviderSqlPlan::Statements(vec![
                        format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                            q(alteration.table),
                            q(alteration.column),
                            default_sql,
                        ),
                        format!(
                            "UPDATE {} SET {} = {} WHERE {} IS NULL",
                            q(alteration.table),
                            q(alteration.column),
                            default_sql,
                            q(alteration.column),
                        ),
                        format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                            q(alteration.table),
                            q(alteration.column),
                        ),
                    ]));
                }

                Err(MigrationError::UnsupportedChange(format!(
                    "Column {}.{} cannot be made NOT NULL: no @default() is defined. \
                     Add a default value to the field in your schema before re-running \
                     `db push`, or manually backfill NULLs and apply the constraint by hand.",
                    alteration.table, alteration.column
                )))
            }
            (DatabaseProvider::Mysql, _) => {
                let col_def = alteration.full_column_definition.ok_or_else(|| {
                    MigrationError::Other(format!(
                        "Missing full column definition for MySQL nullability change on {}.{}",
                        alteration.table, alteration.column
                    ))
                })?;
                Ok(ProviderSqlPlan::Statements(vec![format!(
                    "ALTER TABLE {} MODIFY COLUMN {}",
                    q(alteration.table),
                    col_def,
                )]))
            }
            (DatabaseProvider::Sqlite, _) => Ok(ProviderSqlPlan::RequiresTableRebuild),
        }
    }

    pub(crate) fn alter_column_default_sql(
        &self,
        alteration: AlterColumnDefault<'_>,
    ) -> Result<ProviderSqlPlan> {
        let q = |name: &str| self.provider.quote_identifier(name);

        match self.provider {
            DatabaseProvider::Postgres => {
                if alteration.new_default.is_none() && alteration.preserve_implicit_default {
                    return Ok(ProviderSqlPlan::Statements(vec![]));
                }

                Ok(ProviderSqlPlan::Statements(vec![
                    if let Some(default_sql) = alteration.new_default {
                        format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                            q(alteration.table),
                            q(alteration.column),
                            default_sql,
                        )
                    } else {
                        format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                            q(alteration.table),
                            q(alteration.column),
                        )
                    },
                ]))
            }
            DatabaseProvider::Mysql => {
                let col_def = alteration.full_column_definition.ok_or_else(|| {
                    MigrationError::Other(format!(
                        "Missing full column definition for MySQL default change on {}.{}",
                        alteration.table, alteration.column
                    ))
                })?;
                Ok(ProviderSqlPlan::Statements(vec![format!(
                    "ALTER TABLE {} MODIFY COLUMN {}",
                    q(alteration.table),
                    col_def,
                )]))
            }
            DatabaseProvider::Sqlite => Ok(ProviderSqlPlan::RequiresTableRebuild),
        }
    }

    pub(crate) fn reverse_nullability_change_sql(
        &self,
        table: &str,
        column: &str,
        now_required: bool,
    ) -> Option<Vec<String>> {
        let q = |name: &str| self.provider.quote_identifier(name);

        match (self.provider, now_required) {
            (DatabaseProvider::Postgres, true) => Some(vec![format!(
                "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                q(table),
                q(column),
            )]),
            (DatabaseProvider::Postgres, false) => Some(vec![format!(
                "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                q(table),
                q(column),
            )]),
            _ => None,
        }
    }

    pub(crate) fn reverse_default_change_sql(
        &self,
        table: &str,
        column: &str,
        old_default: Option<&str>,
    ) -> Option<Vec<String>> {
        let q = |name: &str| self.provider.quote_identifier(name);

        match self.provider {
            DatabaseProvider::Postgres => Some(vec![if let Some(default_sql) = old_default {
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                    q(table),
                    q(column),
                    default_sql,
                )
            } else {
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                    q(table),
                    q(column),
                )
            }]),
            DatabaseProvider::Mysql | DatabaseProvider::Sqlite => None,
        }
    }

    pub(crate) fn reverse_column_type_sql(
        &self,
        table: &str,
        column: &str,
        old_type: &str,
    ) -> Option<Vec<String>> {
        let q = |name: &str| self.provider.quote_identifier(name);

        match self.provider {
            DatabaseProvider::Postgres => Some(vec![format!(
                "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                q(table),
                q(column),
                old_type,
            )]),
            DatabaseProvider::Mysql | DatabaseProvider::Sqlite => None,
        }
    }

    fn provider_name(&self) -> &'static str {
        match self.provider {
            DatabaseProvider::Postgres => "PostgreSQL",
            DatabaseProvider::Sqlite => "SQLite",
            DatabaseProvider::Mysql => "MySQL",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_hash_index_uses_if_not_exists_and_using_clause() {
        let strategy = ProviderStrategy::new(DatabaseProvider::Postgres);
        let columns = vec!["email".to_string()];
        let kind = IndexKind::Basic(BasicIndexType::Hash);

        let sql = strategy.create_index_sql(CreateIndex {
            table: "User",
            name: "email_hash_idx",
            columns: &columns,
            unique: false,
            kind: &kind,
            if_not_exists: true,
        });

        assert_eq!(
            sql,
            "CREATE INDEX IF NOT EXISTS \"email_hash_idx\" ON \"User\" USING HASH (\"email\")"
        );
    }

    #[test]
    fn mysql_fulltext_index_uses_native_syntax() {
        let strategy = ProviderStrategy::new(DatabaseProvider::Mysql);
        let columns = vec!["body".to_string()];
        let kind = IndexKind::Basic(BasicIndexType::FullText);

        let sql = strategy.create_index_sql(CreateIndex {
            table: "Post",
            name: "body_search",
            columns: &columns,
            unique: false,
            kind: &kind,
            if_not_exists: true,
        });

        assert_eq!(
            sql,
            "CREATE FULLTEXT INDEX `body_search` ON `Post` (`body`)"
        );
    }

    #[test]
    fn postgres_drop_table_can_include_cascade() {
        let strategy = ProviderStrategy::new(DatabaseProvider::Postgres);

        assert_eq!(
            strategy.drop_table_sql("users", true),
            "DROP TABLE IF EXISTS \"users\" CASCADE"
        );
        assert_eq!(
            strategy.drop_table_sql("users", false),
            "DROP TABLE IF EXISTS \"users\""
        );
    }

    #[test]
    fn postgres_not_null_with_default_backfills_before_constraint() {
        let strategy = ProviderStrategy::new(DatabaseProvider::Postgres);

        let plan = strategy
            .alter_column_nullability_sql(AlterColumnNullability {
                table: "User",
                column: "email",
                now_required: true,
                is_generated: false,
                default_sql: Some("'unknown@example.com'"),
                full_column_definition: None,
            })
            .unwrap();

        let ProviderSqlPlan::Statements(sql) = plan else {
            panic!("expected postgres statements plan");
        };

        assert_eq!(
            sql,
            vec![
                "ALTER TABLE \"User\" ALTER COLUMN \"email\" SET DEFAULT 'unknown@example.com'"
                    .to_string(),
                "UPDATE \"User\" SET \"email\" = 'unknown@example.com' WHERE \"email\" IS NULL"
                    .to_string(),
                "ALTER TABLE \"User\" ALTER COLUMN \"email\" SET NOT NULL".to_string(),
            ]
        );
    }

    #[test]
    fn mysql_type_rewrite_uses_modify_column() {
        let strategy = ProviderStrategy::new(DatabaseProvider::Mysql);

        let plan = strategy
            .alter_column_type_sql(AlterColumnType {
                table: "User",
                column: "email",
                target_type: "VARCHAR(255)",
                full_column_definition: Some("`email` VARCHAR(255) NOT NULL"),
            })
            .unwrap();

        let ProviderSqlPlan::Statements(sql) = plan else {
            panic!("expected mysql statements plan");
        };

        assert_eq!(
            sql,
            vec!["ALTER TABLE `User` MODIFY COLUMN `email` VARCHAR(255) NOT NULL".to_string()]
        );
    }

    #[test]
    fn sqlite_column_changes_require_rebuild() {
        let strategy = ProviderStrategy::new(DatabaseProvider::Sqlite);

        let type_plan = strategy
            .alter_column_type_sql(AlterColumnType {
                table: "User",
                column: "email",
                target_type: "TEXT",
                full_column_definition: None,
            })
            .unwrap();
        assert!(matches!(type_plan, ProviderSqlPlan::RequiresTableRebuild));

        let default_plan = strategy
            .alter_column_default_sql(AlterColumnDefault {
                table: "User",
                column: "email",
                new_default: Some("'x'"),
                preserve_implicit_default: false,
                full_column_definition: None,
            })
            .unwrap();
        assert!(matches!(
            default_plan,
            ProviderSqlPlan::RequiresTableRebuild
        ));
    }

    #[test]
    fn postgres_implicit_serial_default_is_preserved() {
        let strategy = ProviderStrategy::new(DatabaseProvider::Postgres);

        let plan = strategy
            .alter_column_default_sql(AlterColumnDefault {
                table: "User",
                column: "id",
                new_default: None,
                preserve_implicit_default: true,
                full_column_definition: None,
            })
            .unwrap();

        let ProviderSqlPlan::Statements(sql) = plan else {
            panic!("expected postgres statements plan");
        };
        assert!(sql.is_empty());
    }

    #[test]
    fn mysql_json_storage_and_udt_support_are_provider_aware() {
        let mysql = ProviderStrategy::new(DatabaseProvider::Mysql);
        let sqlite = ProviderStrategy::new(DatabaseProvider::Sqlite);
        let postgres = ProviderStrategy::new(DatabaseProvider::Postgres);

        assert_eq!(
            mysql.array_storage_sql(Some(StorageStrategy::Json)),
            Some("JSON")
        );
        assert_eq!(
            sqlite.composite_storage_sql(Some(StorageStrategy::Json)),
            Some("TEXT")
        );
        assert!(postgres.supports_user_defined_types());
        assert!(!mysql.supports_user_defined_types());
    }
}
