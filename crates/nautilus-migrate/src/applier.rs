//! Diff applier — translates each [`Change`] into one or more SQL statements
//! that, when executed, bring the live database in sync with the target schema.
//!
//! For single-statement operations the caller can use
//! [`Connection::execute`](crate). For multi-statement operations (e.g. SQLite
//! full-table rebuilds or nullable-required with default on Postgres) the
//! caller **must** execute all returned statements inside a single transaction.

use std::collections::HashSet;

use nautilus_schema::ir::{DefaultValue, FieldIr, ModelIr, ResolvedFieldType, SchemaIr};

use crate::ddl::{DatabaseProvider, DdlGenerator};
use crate::diff::Change;
use crate::error::{MigrationError, Result};
use crate::live::LiveSchema;
use crate::provider::{
    AlterColumnDefault, AlterColumnNullability, AlterColumnType, CreateIndex, ProviderSqlPlan,
    ProviderStrategy,
};

/// Translates schema [`Change`]s into executable SQL statements.
///
/// # Usage
///
/// ```ignore
/// let applier = DiffApplier::new(provider, &ddl, &schema_ir, &live);
/// // Collect all SQL first, then execute atomically in one transaction.
/// let all_stmts: Vec<String> = changes
///     .iter()
///     .flat_map(|c| applier.sql_for(c).unwrap())
///     .collect();
/// conn.execute_in_transaction(&all_stmts).await?;
/// ```
pub struct DiffApplier<'a> {
    provider: DatabaseProvider,
    ddl: &'a DdlGenerator,
    schema: &'a SchemaIr,
    live: &'a LiveSchema,
}

impl<'a> DiffApplier<'a> {
    /// Create a new applier.
    pub fn new(
        provider: DatabaseProvider,
        ddl: &'a DdlGenerator,
        schema: &'a SchemaIr,
        live: &'a LiveSchema,
    ) -> Self {
        Self {
            provider,
            ddl,
            schema,
            live,
        }
    }

    /// Generate SQL statement(s) for a single [`Change`].
    ///
    /// Returns a `Vec<String>`.  When the vec contains **more than one**
    /// element the caller must execute all statements in a single transaction.
    pub fn sql_for(&self, change: &Change) -> Result<Vec<String>> {
        let strategy = ProviderStrategy::new(self.provider);
        match change {
            Change::NewTable(model) => {
                let mut stmts = vec![self.ddl.generate_create_table(model, self.schema)?];
                stmts.extend(self.ddl.generate_create_indexes_for_model(model));
                Ok(stmts)
            }

            Change::DroppedTable { name } => {
                Ok(vec![strategy.drop_table_sql(
                    name,
                    self.provider == DatabaseProvider::Postgres,
                )])
            }

            Change::AddedColumn { table, field } => {
                if field.is_required && field.default_value.is_none() && field.computed.is_none() {
                    return Err(MigrationError::UnsupportedChange(format!(
                        "Column {}.{} is NOT NULL but has no @default(). \
                         Add a default value to the field in your schema before re-running \
                         `db push`, or make the field optional.",
                        table, field.db_name
                    )));
                }
                let col_def = self
                    .ddl
                    .generate_column_definition(field, self.schema, false)?
                    .ok_or_else(|| {
                        MigrationError::UnsupportedChange(format!(
                            "Cannot generate column definition for {}.{}",
                            table, field.db_name
                        ))
                    })?;
                Ok(vec![format!(
                    "ALTER TABLE {} ADD COLUMN {}",
                    self.q(table),
                    col_def,
                )])
            }

            Change::DroppedColumn { table, column } => match self.provider {
                DatabaseProvider::Postgres | DatabaseProvider::Mysql => Ok(vec![format!(
                    "ALTER TABLE {} DROP COLUMN {}",
                    self.q(table),
                    self.q(column),
                )]),
                DatabaseProvider::Sqlite => self.sqlite_rebuild(table),
            },

            Change::TypeChanged { table, column, .. } => {
                let field = self.find_field(table, column)?;
                let type_sql = self.ddl.column_type_sql(field)?;
                let col_def = if self.provider == DatabaseProvider::Mysql {
                    Some(self.full_col_def(field)?)
                } else {
                    None
                };

                self.materialize_provider_plan(
                    table,
                    strategy.alter_column_type_sql(AlterColumnType {
                        table,
                        column,
                        target_type: &type_sql,
                        full_column_definition: col_def.as_deref(),
                    })?,
                )
            }

            Change::NullabilityChanged {
                table,
                column,
                now_required,
            } => {
                let field = self.find_field(table, column)?;
                let default_sql = match &field.default_value {
                    Some(default)
                        if !matches!(
                            default,
                            DefaultValue::Function(func) if func.name == "autoincrement"
                        ) =>
                    {
                        Some(
                            self.ddl
                                .generate_default_value(default, &field.field_type)?,
                        )
                    }
                    _ => None,
                };
                let col_def = if self.provider == DatabaseProvider::Mysql {
                    Some(self.full_col_def(field)?)
                } else {
                    None
                };

                self.materialize_provider_plan(
                    table,
                    strategy.alter_column_nullability_sql(AlterColumnNullability {
                        table,
                        column,
                        now_required: *now_required,
                        is_generated: field.computed.is_some(),
                        default_sql: default_sql.as_deref(),
                        full_column_definition: col_def.as_deref(),
                    })?,
                )
            }

            Change::DefaultChanged {
                table, column, to, ..
            } => {
                let field = if self.provider == DatabaseProvider::Mysql || to.is_none() {
                    Some(self.find_field(table, column)?)
                } else {
                    None
                };
                let preserve_implicit_default = field.is_some_and(|field| {
                    matches!(
                        &field.default_value,
                        Some(DefaultValue::Function(func)) if func.name == "autoincrement"
                    )
                });
                let col_def = if self.provider == DatabaseProvider::Mysql {
                    Some(
                        self.full_col_def(field.expect("field required for MySQL default change"))?,
                    )
                } else {
                    None
                };

                self.materialize_provider_plan(
                    table,
                    strategy.alter_column_default_sql(AlterColumnDefault {
                        table,
                        column,
                        new_default: to.as_deref(),
                        preserve_implicit_default,
                        full_column_definition: col_def.as_deref(),
                    })?,
                )
            }

            Change::PrimaryKeyChanged { table } => match self.provider {
                DatabaseProvider::Postgres => {
                    let model = self.find_model(table)?;
                    let pk_cols = self.pk_col_list(model)?;
                    Ok(vec![
                        format!(
                            "ALTER TABLE {} DROP CONSTRAINT IF EXISTS \"{}_pkey\"",
                            self.q(table),
                            table,
                        ),
                        format!(
                            "ALTER TABLE {} ADD PRIMARY KEY ({})",
                            self.q(table),
                            pk_cols,
                        ),
                    ])
                }
                DatabaseProvider::Mysql => {
                    let model = self.find_model(table)?;
                    let pk_cols = self.pk_col_list(model)?;
                    Ok(vec![
                        format!("ALTER TABLE {} DROP PRIMARY KEY", self.q(table)),
                        format!(
                            "ALTER TABLE {} ADD PRIMARY KEY ({})",
                            self.q(table),
                            pk_cols,
                        ),
                    ])
                }
                DatabaseProvider::Sqlite => self.sqlite_rebuild(table),
            },

            Change::IndexAdded {
                table,
                columns,
                unique,
                index_type,
                index_name,
            } => {
                let idx_name = index_name
                    .as_deref()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| index_name_auto(table, columns));
                Ok(vec![strategy.create_index_sql(CreateIndex {
                    table,
                    name: &idx_name,
                    columns,
                    unique: *unique,
                    method: index_type.map(|index_type| index_type.as_str()),
                    if_not_exists: true,
                })])
            }

            Change::ComputedExprChanged { table, field, .. } => {
                // Generated columns cannot be altered in-place on any provider.
                // Strategy: DROP the old column and re-ADD it with the new expression.
                match self.provider {
                    DatabaseProvider::Sqlite => self.sqlite_rebuild(table),
                    DatabaseProvider::Postgres | DatabaseProvider::Mysql => {
                        let col_def = self
                            .ddl
                            .generate_column_definition(field, self.schema, false)?
                            .ok_or_else(|| {
                                MigrationError::UnsupportedChange(format!(
                                    "Cannot generate column definition for {}.{}",
                                    table, field.db_name
                                ))
                            })?;
                        Ok(vec![
                            format!(
                                "ALTER TABLE {} DROP COLUMN {}",
                                self.q(table),
                                self.q(&field.db_name),
                            ),
                            format!("ALTER TABLE {} ADD COLUMN {}", self.q(table), col_def,),
                        ])
                    }
                }
            }

            Change::IndexDropped {
                table, index_name, ..
            } => match self.provider {
                DatabaseProvider::Postgres | DatabaseProvider::Sqlite => {
                    Ok(vec![format!("DROP INDEX IF EXISTS {}", self.q(index_name))])
                }
                DatabaseProvider::Mysql => Ok(vec![format!(
                    "DROP INDEX {} ON {}",
                    self.q(index_name),
                    self.q(table),
                )]),
            },

            Change::CheckChanged {
                table,
                column,
                from,
                to,
            } => match self.provider {
                DatabaseProvider::Sqlite => self.sqlite_rebuild(table),
                DatabaseProvider::Postgres | DatabaseProvider::Mysql => {
                    let mut stmts = Vec::new();
                    let constraint_name = check_constraint_name(table, column.as_deref());

                    if from.is_some() {
                        match self.provider {
                            DatabaseProvider::Postgres => stmts.push(format!(
                                "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}",
                                self.q(table),
                                self.q(&constraint_name),
                            )),
                            DatabaseProvider::Mysql => stmts.push(format!(
                                "ALTER TABLE {} DROP CHECK {}",
                                self.q(table),
                                self.q(&constraint_name),
                            )),
                            _ => unreachable!(),
                        }
                    }

                    if let Some(expr) = to {
                        stmts.push(format!(
                            "ALTER TABLE {} ADD CONSTRAINT {} CHECK ({})",
                            self.q(table),
                            self.q(&constraint_name),
                            expr,
                        ));
                    }

                    Ok(stmts)
                }
            },

            Change::ForeignKeyAdded {
                table,
                constraint_name,
                columns,
                referenced_table,
                referenced_columns,
                on_delete,
                on_update,
            } => match self.provider {
                DatabaseProvider::Sqlite => self.sqlite_rebuild(table),
                DatabaseProvider::Postgres | DatabaseProvider::Mysql => {
                    let cols = columns
                        .iter()
                        .map(|c| self.q(c))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let ref_cols = referenced_columns
                        .iter()
                        .map(|c| self.q(c))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let mut sql = format!(
                        "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})",
                        self.q(table),
                        self.q(constraint_name),
                        cols,
                        self.q(referenced_table),
                        ref_cols,
                    );
                    if let Some(action) = on_delete {
                        sql.push_str(&format!(" ON DELETE {}", action));
                    }
                    if let Some(action) = on_update {
                        sql.push_str(&format!(" ON UPDATE {}", action));
                    }
                    Ok(vec![sql])
                }
            },

            Change::ForeignKeyDropped {
                table,
                constraint_name,
            } => match self.provider {
                DatabaseProvider::Sqlite => self.sqlite_rebuild(table),
                DatabaseProvider::Postgres => Ok(vec![format!(
                    "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}",
                    self.q(table),
                    self.q(constraint_name),
                )]),
                DatabaseProvider::Mysql => Ok(vec![format!(
                    "ALTER TABLE {} DROP FOREIGN KEY {}",
                    self.q(table),
                    self.q(constraint_name),
                )]),
            },

            Change::CreateCompositeType { name } => {
                if !strategy.supports_user_defined_types() {
                    return Ok(vec![]);
                }
                let ct = self
                    .schema
                    .composite_types
                    .values()
                    .find(|ct| ct.logical_name.to_lowercase() == *name)
                    .ok_or_else(|| {
                        MigrationError::Other(format!(
                            "Composite type definition not found for '{}'",
                            name
                        ))
                    })?;
                Ok(vec![self.ddl.generate_composite_type(ct)?])
            }

            Change::DropCompositeType { name } => {
                if !strategy.supports_user_defined_types() {
                    return Ok(vec![]);
                }
                Ok(vec![format!("DROP TYPE IF EXISTS {}", self.type_q(name))])
            }

            Change::AlterCompositeType {
                name,
                added_fields,
                dropped_fields,
                type_changed_fields,
            } => {
                if !strategy.supports_user_defined_types() {
                    return Ok(vec![]);
                }
                let mut stmts: Vec<String> = Vec::new();
                for (field_name, sql_type) in added_fields {
                    stmts.push(format!(
                        "ALTER TYPE {} ADD ATTRIBUTE {} {}",
                        self.type_q(name),
                        self.q(field_name),
                        sql_type,
                    ));
                }
                for (field_name, _from, to) in type_changed_fields {
                    stmts.push(format!(
                        "ALTER TYPE {} ALTER ATTRIBUTE {} TYPE {} CASCADE",
                        self.type_q(name),
                        self.q(field_name),
                        to,
                    ));
                }
                for field_name in dropped_fields {
                    stmts.push(format!(
                        "ALTER TYPE {} DROP ATTRIBUTE {} CASCADE",
                        self.type_q(name),
                        self.q(field_name),
                    ));
                }
                Ok(stmts)
            }

            Change::CreateEnum { name, variants } => {
                if !strategy.supports_user_defined_types() {
                    return Ok(vec![]);
                }

                if let Some(def) = self
                    .schema
                    .enums
                    .values()
                    .find(|e| e.logical_name.eq_ignore_ascii_case(name))
                {
                    Ok(vec![self.ddl.generate_enum_type(def)])
                } else {
                    let variants_sql = variants
                        .iter()
                        .map(|v| format!("'{}'", v))
                        .collect::<Vec<_>>()
                        .join(", ");
                    Ok(vec![format!(
                        "DO $$ BEGIN CREATE TYPE {} AS ENUM ({}); \
                         EXCEPTION WHEN duplicate_object THEN NULL; END $$",
                        self.type_q(name),
                        variants_sql,
                    )])
                }
            }

            Change::DropEnum { name } => {
                if !strategy.supports_user_defined_types() {
                    return Ok(vec![]);
                }
                Ok(vec![format!("DROP TYPE IF EXISTS {}", self.type_q(name))])
            }

            Change::CreateExtension { name } => {
                if !strategy.supports_user_defined_types() {
                    return Ok(vec![]);
                }
                Ok(vec![self.ddl.generate_create_extension(name)])
            }

            Change::DropExtension { name } => {
                if !strategy.supports_user_defined_types() {
                    return Ok(vec![]);
                }
                Ok(vec![self.ddl.generate_drop_extension(name)])
            }

            Change::AlterEnum {
                name,
                added_variants,
                removed_variants,
            } => {
                if !strategy.supports_user_defined_types() {
                    return Ok(vec![]);
                }

                if removed_variants.is_empty() {
                    let stmts: Vec<String> = added_variants
                        .iter()
                        .map(|v| {
                            format!(
                                "ALTER TYPE {} ADD VALUE IF NOT EXISTS '{}'",
                                self.type_q(name),
                                v
                            )
                        })
                        .collect();
                    Ok(stmts)
                } else {
                    let enum_def = self
                        .schema
                        .enums
                        .values()
                        .find(|e| e.logical_name.eq_ignore_ascii_case(name))
                        .ok_or_else(|| {
                            MigrationError::Other(format!(
                                "Enum definition not found for '{}'",
                                name
                            ))
                        })?;

                    let old_name = format!("{}_old", name);
                    let variants_sql = enum_def
                        .variants
                        .iter()
                        .map(|v| format!("'{}'", v))
                        .collect::<Vec<_>>()
                        .join(", ");

                    let mut stmts = vec![
                        format!(
                            "ALTER TYPE {} RENAME TO {}",
                            self.type_q(name),
                            self.q(&old_name)
                        ),
                        format!(
                            "CREATE TYPE {} AS ENUM ({})",
                            self.type_q(name),
                            variants_sql
                        ),
                    ];

                    for (table_name, table) in &self.live.tables {
                        for col in &table.columns {
                            if col.col_type == *name {
                                if col.default_value.is_some() {
                                    stmts.push(format!(
                                        "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                                        self.q(table_name),
                                        self.q(&col.name),
                                    ));
                                }
                                stmts.push(format!(
                                    "ALTER TABLE {} ALTER COLUMN {} TYPE {} \
                                     USING {}::text::{}",
                                    self.q(table_name),
                                    self.q(&col.name),
                                    self.type_q(name),
                                    self.q(&col.name),
                                    self.type_q(name),
                                ));
                                if let Some(default) = &col.default_value {
                                    let new_default = if let Some(val) =
                                        default.strip_suffix(&format!("::{}", old_name))
                                    {
                                        format!("{}::{}", val, name)
                                    } else if let Some(val) =
                                        default.strip_suffix(&format!("::\"{}\"", old_name))
                                    {
                                        format!("{}::{}", val, name)
                                    } else {
                                        default.clone()
                                    };
                                    stmts.push(format!(
                                        "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                                        self.q(table_name),
                                        self.q(&col.name),
                                        new_default,
                                    ));
                                }
                            }
                        }
                    }

                    stmts.push(format!("DROP TYPE {}", self.type_q(&old_name)));
                    Ok(stmts)
                }
            }
        }
    }

    /// Quote an identifier for the target provider.
    fn q(&self, name: &str) -> String {
        self.provider.quote_identifier(name)
    }

    fn materialize_provider_plan(&self, table: &str, plan: ProviderSqlPlan) -> Result<Vec<String>> {
        match plan {
            ProviderSqlPlan::Statements(stmts) => Ok(stmts),
            ProviderSqlPlan::RequiresTableRebuild => self.sqlite_rebuild(table),
        }
    }

    /// Quote a PostgreSQL type identifier without folding its case.
    fn type_q(&self, name: &str) -> String {
        self.provider.quote_identifier(name)
    }

    /// Find a [`FieldIr`] by table DB-name and column DB-name.
    fn find_field(&self, table: &str, column: &str) -> Result<&FieldIr> {
        let model = self.find_model(table)?;
        model
            .fields
            .iter()
            .find(|f| f.db_name == column)
            .ok_or_else(|| MigrationError::Other(format!("Field not found: {}.{}", table, column)))
    }

    /// Find a [`ModelIr`] by table DB-name.
    fn find_model(&self, table: &str) -> Result<&ModelIr> {
        self.schema
            .models
            .values()
            .find(|m| m.db_name == table)
            .ok_or_else(|| MigrationError::Other(format!("Model not found for table: {}", table)))
    }

    /// Generate the full column definition string for a field.
    /// Used for MySQL `MODIFY COLUMN` which needs the complete definition.
    fn full_col_def(&self, field: &FieldIr) -> Result<String> {
        self.ddl
            .generate_column_definition(field, self.schema, false)?
            .ok_or_else(|| {
                MigrationError::UnsupportedChange(format!(
                    "Cannot generate column definition for field {}",
                    field.db_name,
                ))
            })
    }

    /// Comma-separated quoted primary-key column list for a model.
    fn pk_col_list(&self, model: &ModelIr) -> Result<String> {
        let cols: Vec<String> = model
            .primary_key
            .fields()
            .iter()
            .map(|name| {
                let field = model.find_field(name).ok_or_else(|| {
                    MigrationError::Other(format!(
                        "primary key field '{}' not found in model '{}'",
                        name, model.logical_name
                    ))
                })?;
                Ok(self.q(&field.db_name))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(cols.join(", "))
    }

    /// Generate the 4-statement SQLite full-table rebuild for `table`.
    ///
    /// All four statements must be executed inside a single transaction.
    fn sqlite_rebuild(&self, table: &str) -> Result<Vec<String>> {
        let model = self.find_model(table)?;
        let live_table = self
            .live
            .tables
            .get(table)
            .ok_or_else(|| MigrationError::Other(format!("Live table not found: {}", table)))?;

        let tmp_name = format!("__tmp_{}", table);

        // Generate CREATE TABLE for the temp name by cloning the model with
        // a different db_name so the DDL generator quotes it correctly.
        let mut tmp_model = model.clone();
        tmp_model.db_name = tmp_name.clone();
        let create_tmp = self.ddl.generate_create_table(&tmp_model, self.schema)?;

        let target_cols: HashSet<&str> = model
            .fields
            .iter()
            .filter(|f| !matches!(f.field_type, ResolvedFieldType::Relation(_)))
            .map(|f| f.db_name.as_str())
            .collect();

        let common_cols: Vec<String> = live_table
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .filter(|&name| target_cols.contains(name))
            .map(|name| self.q(name))
            .collect();

        let cols_sql = common_cols.join(", ");

        Ok(vec![
            format!("DROP TABLE IF EXISTS {}", self.q(&tmp_name)),
            create_tmp,
            format!(
                "INSERT INTO {} ({}) SELECT {} FROM {}",
                self.q(&tmp_name),
                cols_sql,
                cols_sql,
                self.q(table),
            ),
            format!("DROP TABLE {}", self.q(table)),
            format!(
                "ALTER TABLE {} RENAME TO {}",
                self.q(&tmp_name),
                self.q(table),
            ),
        ])
    }
}

/// Derive a deterministic index name from the table and column list.
fn index_name_auto(table: &str, columns: &[String]) -> String {
    format!("idx_{}_{}", table, columns.join("_"))
}

/// Derive a deterministic CHECK constraint name from table and optional column.
fn check_constraint_name(table: &str, column: Option<&str>) -> String {
    match column {
        Some(col) => format!("chk_{}_{}", table, col),
        None => format!("chk_{}", table),
    }
}
