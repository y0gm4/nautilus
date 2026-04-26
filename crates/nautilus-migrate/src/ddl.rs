use crate::error::{MigrationError, Result};
use crate::provider::{CreateIndex, ProviderStrategy};
use nautilus_schema::ast::StorageStrategy;
use nautilus_schema::ir::{
    CompositeFieldIr, CompositeTypeIr, ComputedKind, DefaultValue, EnumIr, FieldIr, ModelIr,
    PostgresExtensionIr, ResolvedFieldType, ScalarType, SchemaIr,
};

/// Generates DDL (Data Definition Language) SQL from schema IR
pub struct DdlGenerator {
    provider: DatabaseProvider,
}

/// Supported database providers
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseProvider {
    /// PostgreSQL
    Postgres,
    /// SQLite
    Sqlite,
    /// MySQL
    Mysql,
}

impl DatabaseProvider {
    /// Parse a datasource `provider` string from a `.nautilus` schema.
    pub fn from_schema_provider(provider: &str) -> Option<Self> {
        match provider {
            "postgresql" | "postgres" => Some(Self::Postgres),
            "sqlite" => Some(Self::Sqlite),
            "mysql" => Some(Self::Mysql),
            _ => None,
        }
    }

    /// Return the canonical `.nautilus` datasource provider string.
    pub fn schema_provider_name(self) -> &'static str {
        match self {
            Self::Postgres => "postgresql",
            Self::Sqlite => "sqlite",
            Self::Mysql => "mysql",
        }
    }

    /// Quote an identifier for this database provider.
    ///
    /// PostgreSQL and SQLite use double quotes (`"name"`), MySQL uses backticks
    /// (`` `name` ``).
    pub fn quote_identifier(self, name: &str) -> String {
        match self {
            Self::Postgres | Self::Sqlite => format!("\"{}\"", name),
            Self::Mysql => format!("`{}`", name),
        }
    }
}

impl DdlGenerator {
    /// Create a new DDL generator
    pub fn new(provider: DatabaseProvider) -> Self {
        Self { provider }
    }

    /// Return the database provider this generator is configured for.
    pub fn provider(&self) -> DatabaseProvider {
        self.provider
    }

    /// Generate CREATE TABLE statements for all models
    pub fn generate_create_tables(&self, schema: &SchemaIr) -> Result<Vec<String>> {
        let mut statements = Vec::new();
        let strategy = ProviderStrategy::new(self.provider);

        if strategy.supports_user_defined_types() {
            if let Some(ds) = &schema.datasource {
                for ext in &ds.extensions {
                    statements.push(self.generate_create_extension(ext));
                }
            }
            for enum_def in schema.enums.values() {
                statements.push(self.generate_enum_type(enum_def));
            }
            for composite_type in schema.composite_types.values() {
                statements.push(self.generate_composite_type(composite_type)?);
            }
        }

        // Generate tables in FK-dependency order so FK constraints
        // reference already-created tables.
        let all_models: Vec<&ModelIr> = schema.models.values().collect();
        for model in crate::diff::topo_sort_models(&all_models) {
            statements.push(self.generate_create_table(model, schema)?);
            statements.extend(self.generate_create_indexes_for_model(model));
        }

        Ok(statements)
    }

    /// Generate DROP TABLE statements for all models (in reverse dependency order)
    pub fn generate_drop_tables(&self, schema: &SchemaIr) -> Result<Vec<String>> {
        let mut statements = Vec::new();
        let strategy = ProviderStrategy::new(self.provider);

        // Drop tables in reverse topological order so FK constraints are
        // satisfied.  PostgreSQL also accepts CASCADE to be safe.
        let all_models: Vec<&ModelIr> = schema.models.values().collect();
        let sorted = crate::diff::topo_sort_models(&all_models);
        for model in sorted.into_iter().rev() {
            statements.push(strategy.drop_table_sql(&model.db_name, true));
        }

        if strategy.supports_user_defined_types() {
            for composite_type in schema.composite_types.values() {
                statements.push(format!(
                    "DROP TYPE IF EXISTS {}",
                    self.quote_type_identifier(&composite_type.logical_name.to_lowercase())
                ));
            }
            for enum_def in schema.enums.values() {
                statements.push(format!(
                    "DROP TYPE IF EXISTS {}",
                    self.quote_type_identifier(&enum_def.logical_name.to_lowercase())
                ));
            }
        }

        Ok(statements)
    }

    /// Generate DROP TABLE statements for all tables currently present in the
    /// live database.
    ///
    /// Unlike [`generate_drop_tables`] (which uses the schema IR), this method
    /// operates on the actual live state so it catches tables that exist in the
    /// database but have already been removed from the schema file.
    ///
    /// - **Postgres**: `DROP TABLE IF EXISTS "t" CASCADE` — FK order irrelevant.
    /// - **MySQL**: wraps drops between `SET FOREIGN_KEY_CHECKS=0/1`.
    /// - **SQLite**: `DROP TABLE IF EXISTS "t"` — FK enforcement is off by default.
    pub fn generate_drop_live_tables(&self, live: &crate::live::LiveSchema) -> Vec<String> {
        let mut statements: Vec<String> = Vec::new();
        let strategy = ProviderStrategy::new(self.provider);

        if live.tables.is_empty()
            && (!strategy.supports_user_defined_types()
                || (live.enums.is_empty() && live.composite_types.is_empty()))
        {
            return statements;
        }

        let mut names: Vec<&str> = live.tables.keys().map(String::as_str).collect();
        names.sort_unstable();

        match self.provider {
            DatabaseProvider::Postgres => {
                for name in &names {
                    statements.push(strategy.drop_table_sql(name, true));
                }
            }
            DatabaseProvider::Mysql => {
                statements.push("SET FOREIGN_KEY_CHECKS=0".to_string());
                for name in &names {
                    statements.push(strategy.drop_table_sql(name, false));
                }
                statements.push("SET FOREIGN_KEY_CHECKS=1".to_string());
            }
            DatabaseProvider::Sqlite => {
                for name in &names {
                    statements.push(strategy.drop_table_sql(name, false));
                }
            }
        }

        if strategy.supports_user_defined_types() {
            let mut ct_names: Vec<&str> = live
                .composite_types
                .values()
                .map(|ct| ct.name.as_str())
                .collect();
            ct_names.sort_unstable();
            for name in &ct_names {
                statements.push(format!(
                    "DROP TYPE IF EXISTS {}",
                    self.quote_type_identifier(name)
                ));
            }

            let mut enum_names: Vec<&str> = live.enums.keys().map(String::as_str).collect();
            enum_names.sort_unstable();
            for name in &enum_names {
                statements.push(format!(
                    "DROP TYPE IF EXISTS {}",
                    self.quote_type_identifier(name)
                ));
            }
        }

        statements
    }

    /// Generate statements to delete all rows from every table in the schema,
    /// preserving the table structure.
    ///
    /// - **Postgres**: `TRUNCATE TABLE "t" RESTART IDENTITY CASCADE` — one
    ///   statement per table, FK cycles handled by `CASCADE`.
    /// - **MySQL**: wraps individual `TRUNCATE TABLE \`t\`` statements between
    ///   `SET FOREIGN_KEY_CHECKS=0/1` so FK constraints don't block the operation.
    /// - **SQLite**: `DELETE FROM "t"` (SQLite has no `TRUNCATE`); also emits a
    ///   `DELETE FROM sqlite_sequence WHERE name = 't'` to reset auto-increment
    ///   counters when applicable.
    pub fn generate_truncate_tables(&self, schema: &SchemaIr) -> Result<Vec<String>> {
        let all_models: Vec<&ModelIr> = schema.models.values().collect();
        // Reverse topo order: delete child tables before parents.
        let sorted: Vec<&ModelIr> = crate::diff::topo_sort_models(&all_models)
            .into_iter()
            .rev()
            .collect();

        let mut statements: Vec<String> = Vec::new();

        match self.provider {
            DatabaseProvider::Postgres => {
                for model in &sorted {
                    statements.push(format!(
                        "TRUNCATE TABLE {} RESTART IDENTITY CASCADE",
                        self.quote_identifier(&model.db_name)
                    ));
                }
            }
            DatabaseProvider::Mysql => {
                statements.push("SET FOREIGN_KEY_CHECKS=0".to_string());
                for model in &sorted {
                    statements.push(format!(
                        "TRUNCATE TABLE {}",
                        self.quote_identifier(&model.db_name)
                    ));
                }
                statements.push("SET FOREIGN_KEY_CHECKS=1".to_string());
            }
            DatabaseProvider::Sqlite => {
                for model in &sorted {
                    statements.push(format!(
                        "DELETE FROM {}",
                        self.quote_identifier(&model.db_name)
                    ));
                    // Reset autoincrement counter if the sqlite_sequence table
                    // tracks this table (only exists when AUTOINCREMENT is used).
                    statements.push(format!(
                        "DELETE FROM sqlite_sequence WHERE name = '{}'",
                        model.db_name.replace('\'', "''")
                    ));
                }
            }
        }

        Ok(statements)
    }

    /// Generate statements to delete all rows from every table currently present
    /// in the live database, preserving the table structure.
    ///
    /// Unlike [`generate_truncate_tables`] (which uses the schema IR), this
    /// method operates on the inspected database state so it also clears tables
    /// that still exist live but have already been removed from the schema file.
    pub fn generate_truncate_live_tables(&self, live: &crate::live::LiveSchema) -> Vec<String> {
        let table_names = live_table_names_for_truncate(live);
        let mut statements: Vec<String> = Vec::new();

        match self.provider {
            DatabaseProvider::Postgres => {
                for table_name in &table_names {
                    statements.push(format!(
                        "TRUNCATE TABLE {} RESTART IDENTITY CASCADE",
                        self.quote_identifier(table_name)
                    ));
                }
            }
            DatabaseProvider::Mysql => {
                if !table_names.is_empty() {
                    statements.push("SET FOREIGN_KEY_CHECKS=0".to_string());
                    for table_name in &table_names {
                        statements.push(format!(
                            "TRUNCATE TABLE {}",
                            self.quote_identifier(table_name)
                        ));
                    }
                    statements.push("SET FOREIGN_KEY_CHECKS=1".to_string());
                }
            }
            DatabaseProvider::Sqlite => {
                for table_name in &table_names {
                    statements.push(format!("DELETE FROM {}", self.quote_identifier(table_name)));
                    statements.push(format!(
                        "DELETE FROM sqlite_sequence WHERE name = '{}'",
                        table_name.replace('\'', "''")
                    ));
                }
            }
        }

        statements
    }

    /// Generate CREATE TYPE statement for an enum (Postgres only).
    ///
    /// Uses a `DO` block so re-running against an existing DB is idempotent —
    /// PostgreSQL has no `CREATE TYPE IF NOT EXISTS` syntax.
    pub(crate) fn generate_enum_type(&self, enum_def: &EnumIr) -> String {
        let variants = enum_def
            .variants
            .iter()
            .map(|v| format!("'{}'", v))
            .collect::<Vec<_>>()
            .join(", ");

        // Use lowercase unquoted names so the type name matches what
        // `SchemaInspector::normalize_pg_type` returns (which lowercases
        // the `udt_name` from information_schema without quoting it).
        format!(
            "DO $$ BEGIN CREATE TYPE {} AS ENUM ({}); \
             EXCEPTION WHEN duplicate_object THEN NULL; END $$",
            self.quote_type_identifier(&enum_def.logical_name.to_lowercase()),
            variants
        )
    }

    /// Generate CREATE EXTENSION IF NOT EXISTS for a PostgreSQL extension.
    ///
    /// The extension name is quoted with double quotes because some common
    /// names contain hyphens (e.g. `uuid-ossp`) which cannot appear in an
    /// unquoted identifier. When a target schema is provided, emit
    /// `WITH SCHEMA "<schema>"` so the extension is installed in that namespace
    /// rather than the default (`public`).
    pub(crate) fn generate_create_extension(&self, ext: &PostgresExtensionIr) -> String {
        let name = ext.name.replace('"', "\"\"");
        match ext.schema.as_deref() {
            Some(schema) => format!(
                "CREATE EXTENSION IF NOT EXISTS \"{}\" WITH SCHEMA \"{}\"",
                name,
                schema.replace('"', "\"\"")
            ),
            None => format!("CREATE EXTENSION IF NOT EXISTS \"{}\"", name),
        }
    }

    /// Generate DROP EXTENSION IF EXISTS for a PostgreSQL extension.
    ///
    /// Intentionally **does not** use CASCADE: if objects still depend on the
    /// extension we want the DROP to fail loudly rather than silently destroy
    /// dependent columns or indexes. `CASCADE` is the explicit default (`false`)
    /// so users have to opt out — any future "cascade" knob should preserve
    /// that safety-first behaviour.
    pub(crate) fn generate_drop_extension(&self, name: &str) -> String {
        format!("DROP EXTENSION IF EXISTS \"{}\"", name.replace('"', "\"\""))
    }

    /// Generate CREATE TYPE ... AS (...) statement for a composite type (Postgres only).
    ///
    /// Uses a `DO` block so re-running against an existing DB is idempotent.
    pub(crate) fn generate_composite_type(&self, ct: &CompositeTypeIr) -> Result<String> {
        let columns: Vec<String> = ct
            .fields
            .iter()
            .map(|f| {
                let col_type = self.generate_column_type(
                    &f.field_type,
                    !f.is_required,
                    f.is_array,
                    f.storage_strategy,
                )?;
                Ok(format!(
                    "{} {}",
                    self.quote_identifier(&f.db_name),
                    col_type
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(format!(
            "DO $$ BEGIN CREATE TYPE {} AS ({}); \
             EXCEPTION WHEN duplicate_object THEN NULL; END $$",
            self.quote_type_identifier(&ct.logical_name.to_lowercase()),
            columns.join(", ")
        ))
    }

    fn sqlite_inline_primary_key<'a>(&self, model: &'a ModelIr) -> Option<&'a str> {
        if self.provider != DatabaseProvider::Sqlite {
            return None;
        }

        let pk_fields = model.primary_key.fields();
        let [pk_name] = pk_fields.as_slice() else {
            return None;
        };
        let pk_name = *pk_name;
        let field = model.find_field(pk_name)?;

        matches!(
            &field.default_value,
            Some(DefaultValue::Function(function)) if function.name == "autoincrement"
        )
        .then_some(pk_name)
    }

    /// Generate CREATE TABLE statement for a model
    pub(crate) fn generate_create_table(
        &self,
        model: &ModelIr,
        schema: &SchemaIr,
    ) -> Result<String> {
        let mut lines = Vec::new();

        // For SQLite: detect single-field PK with autoincrement() — needs inline column definition
        let sqlite_inline_pk = self.sqlite_inline_primary_key(model);

        for field in &model.fields {
            let is_inline_pk = sqlite_inline_pk.is_some_and(|name| field.logical_name == name);
            if let Some(column_def) =
                self.generate_column_definition(field, schema, is_inline_pk)?
            {
                lines.push(format!("  {}", column_def));
            }
        }

        let pk_fields = model.primary_key.fields();
        if !pk_fields.is_empty() && sqlite_inline_pk.is_none() {
            let pk_columns = pk_fields
                .iter()
                .map(|name| {
                    let field = model.find_field(name).unwrap();
                    self.quote_identifier(&field.db_name)
                })
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("  PRIMARY KEY ({})", pk_columns));
        }

        for unique_constraint in &model.unique_constraints {
            let unique_columns = unique_constraint
                .fields
                .iter()
                .map(|name| {
                    let field = model.find_field(name).unwrap();
                    self.quote_identifier(&field.db_name)
                })
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("  UNIQUE ({})", unique_columns));
        }

        for field in &model.fields {
            if let ResolvedFieldType::Relation(rel) = &field.field_type {
                if !rel.fields.is_empty() {
                    let fk_columns = rel
                        .fields
                        .iter()
                        .map(|name| {
                            let fk_field = model.find_field(name).unwrap();
                            self.quote_identifier(&fk_field.db_name)
                        })
                        .collect::<Vec<_>>()
                        .join(", ");

                    let target_model = schema.models.get(&rel.target_model).unwrap();
                    let ref_columns = rel
                        .references
                        .iter()
                        .map(|name| {
                            let ref_field = target_model.find_field(name).unwrap();
                            self.quote_identifier(&ref_field.db_name)
                        })
                        .collect::<Vec<_>>()
                        .join(", ");

                    let mut fk_def = format!(
                        "  FOREIGN KEY ({}) REFERENCES {} ({})",
                        fk_columns,
                        self.quote_identifier(&target_model.db_name),
                        ref_columns
                    );

                    if let Some(action) = &rel.on_delete {
                        fk_def.push_str(&format!(
                            " ON DELETE {}",
                            self.referential_action_sql(action)
                        ));
                    }

                    if let Some(action) = &rel.on_update {
                        fk_def.push_str(&format!(
                            " ON UPDATE {}",
                            self.referential_action_sql(action)
                        ));
                    }

                    lines.push(fk_def);
                }
            }
        }

        // Column-level CHECK constraints as named table constraints.
        // Named constraints give the inspector a stable, predictable name to
        // use for categorisation ("chk_{table}_{col}") so it can correctly
        // distinguish column-level checks from table-level ones.
        // SQLite does not support ALTER TABLE ADD CONSTRAINT, so inline anonymous
        // CHECKs are used there instead (handled in generate_column_definition).
        if self.provider != DatabaseProvider::Sqlite {
            for field in &model.fields {
                if let Some(ref check_expr) = field.check {
                    let cname = format!("chk_{}_{}", model.db_name, field.db_name);
                    lines.push(format!(
                        "  CONSTRAINT {} CHECK ({})",
                        self.quote_identifier(&cname),
                        check_expr,
                    ));
                }
            }
        }

        // Table-level CHECK constraints (named for PG/MySQL, anonymous for SQLite).
        for check_expr in &model.check_constraints {
            if self.provider == DatabaseProvider::Sqlite {
                lines.push(format!("  CHECK ({})", check_expr));
            } else {
                let cname = format!("chk_{}", model.db_name);
                lines.push(format!(
                    "  CONSTRAINT {} CHECK ({})",
                    self.quote_identifier(&cname),
                    check_expr,
                ));
            }
        }

        let table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (\n{}\n)",
            self.quote_identifier(&model.db_name),
            lines.join(",\n")
        );

        Ok(table_sql)
    }

    /// Generate standalone CREATE INDEX statements for the model's `@@index` declarations.
    ///
    /// Unique constraints are emitted inline in `generate_create_table`, so this
    /// only covers explicit secondary indexes.
    pub(crate) fn generate_create_indexes_for_model(&self, model: &ModelIr) -> Vec<String> {
        let strategy = ProviderStrategy::new(self.provider);
        model
            .indexes
            .iter()
            .map(|idx| {
                let columns: Vec<String> = idx
                    .fields
                    .iter()
                    .map(|name| {
                        model
                            .find_field(name)
                            .map(|f| f.db_name.clone())
                            .unwrap_or_else(|| name.clone())
                    })
                    .collect();
                let mut name_parts = columns.clone();
                name_parts.sort();
                let index_name = idx
                    .map
                    .clone()
                    .unwrap_or_else(|| format!("idx_{}_{}", model.db_name, name_parts.join("_")));
                strategy.create_index_sql(CreateIndex {
                    table: &model.db_name,
                    name: &index_name,
                    columns: &columns,
                    unique: false,
                    kind: &idx.kind,
                    if_not_exists: true,
                })
            })
            .collect()
    }

    /// Generate column definition
    pub(crate) fn generate_column_definition(
        &self,
        field: &FieldIr,
        _schema: &SchemaIr,
        is_inline_pk: bool,
    ) -> Result<Option<String>> {
        if matches!(field.field_type, ResolvedFieldType::Relation(_)) {
            return Ok(None);
        }

        // SQLite: autoincrement PK must be `col INTEGER PRIMARY KEY AUTOINCREMENT` inline
        if is_inline_pk && self.provider == DatabaseProvider::Sqlite {
            return Ok(Some(format!(
                "{} INTEGER PRIMARY KEY AUTOINCREMENT",
                self.quote_identifier(&field.db_name)
            )));
        }

        // Postgres: autoincrement() -> SERIAL / BIGSERIAL (has implicit NOT NULL + sequence)
        let is_autoincrement = matches!(
            &field.default_value,
            Some(DefaultValue::Function(f)) if f.name == "autoincrement"
        );
        if is_autoincrement && self.provider == DatabaseProvider::Postgres {
            let serial_type = if matches!(
                &field.field_type,
                ResolvedFieldType::Scalar(ScalarType::BigInt)
            ) {
                "BIGSERIAL"
            } else {
                "SERIAL"
            };
            return Ok(Some(format!(
                "{} {}",
                self.quote_identifier(&field.db_name),
                serial_type,
            )));
        }

        // Computed/generated column — uses database-specific generated column syntax.
        if let Some((ref expr, kind)) = field.computed {
            let col_name = self.quote_identifier(&field.db_name);
            let col_type =
                self.generate_column_type(&field.field_type, !field.is_required, false, None)?;
            let sql = match self.provider {
                DatabaseProvider::Postgres => {
                    // PostgreSQL only supports STORED (validated earlier).
                    format!(
                        "{} {} GENERATED ALWAYS AS ({}) STORED",
                        col_name, col_type, expr
                    )
                }
                DatabaseProvider::Mysql => {
                    let kind_str = match kind {
                        ComputedKind::Stored => "STORED",
                        ComputedKind::Virtual => "VIRTUAL",
                    };
                    format!(
                        "{} {} GENERATED ALWAYS AS ({}) {}",
                        col_name, col_type, expr, kind_str
                    )
                }
                DatabaseProvider::Sqlite => {
                    let kind_str = match kind {
                        ComputedKind::Stored => "STORED",
                        ComputedKind::Virtual => "VIRTUAL",
                    };
                    format!("{} {} AS ({}) {}", col_name, col_type, expr, kind_str)
                }
            };
            return Ok(Some(sql));
        }

        let mut parts = Vec::new();

        parts.push(self.quote_identifier(&field.db_name));

        let is_optional = !field.is_required;
        parts.push(self.generate_column_type(
            &field.field_type,
            is_optional,
            field.is_array,
            field.storage_strategy,
        )?);

        if field.is_required {
            parts.push("NOT NULL".to_string());
        }

        // DEFAULT value — skip autoincrement() (handled above as SERIAL/BIGSERIAL)
        if let Some(default) = &field.default_value {
            if !matches!(default, DefaultValue::Function(f) if f.name == "autoincrement") {
                parts.push(format!(
                    "DEFAULT {}",
                    self.generate_default_value(default, &field.field_type)?
                ));
            }
        }

        // @updatedAt: emit DEFAULT CURRENT_TIMESTAMP; MySQL also gets ON UPDATE.
        if field.is_updated_at {
            parts.push("DEFAULT CURRENT_TIMESTAMP".to_string());
            if self.provider == DatabaseProvider::Mysql {
                parts.push("ON UPDATE CURRENT_TIMESTAMP".to_string());
            }
        }

        // Column-level CHECK constraint: inline only for SQLite (which has no
        // ALTER TABLE ADD CONSTRAINT).  PG and MySQL get a named CONSTRAINT
        // entry in generate_create_table so the inspector can categorise it.
        if self.provider == DatabaseProvider::Sqlite {
            if let Some(ref check_expr) = field.check {
                parts.push(format!("CHECK ({})", check_expr));
            }
        }

        Ok(Some(parts.join(" ")))
    }

    /// Generate SQL type for a field
    fn generate_column_type(
        &self,
        field_type: &ResolvedFieldType,
        _is_optional: bool,
        is_array: bool,
        storage_strategy: Option<StorageStrategy>,
    ) -> Result<String> {
        let strategy = ProviderStrategy::new(self.provider);

        if is_array {
            if let ResolvedFieldType::Scalar(scalar) = field_type {
                if self.provider == DatabaseProvider::Postgres {
                    let base = self.scalar_to_pg_type(scalar)?;
                    return Ok(format!("{}[]", base));
                }
                if let Some(storage_sql) = strategy.array_storage_sql(storage_strategy) {
                    return Ok(storage_sql.to_string());
                }
                return Err(MigrationError::ValidationError(
                    strategy.native_array_support_error(),
                ));
            } else if let ResolvedFieldType::CompositeType { type_name } = field_type {
                if strategy.supports_user_defined_types() {
                    return Ok(format!("{}[]", type_name.to_lowercase()));
                }
                if let Some(storage_sql) = strategy.composite_storage_sql(storage_strategy) {
                    return Ok(storage_sql.to_string());
                }
                return Err(MigrationError::ValidationError(
                    strategy.native_composite_support_error(type_name, true),
                ));
            } else {
                return Ok("".to_string());
            }
        }

        let base_type = match field_type {
            ResolvedFieldType::Scalar(scalar) => match scalar {
                ScalarType::String => match self.provider {
                    DatabaseProvider::Postgres => "TEXT",
                    DatabaseProvider::Sqlite => "TEXT",
                    DatabaseProvider::Mysql => "VARCHAR(255)",
                },
                ScalarType::Boolean => match self.provider {
                    DatabaseProvider::Postgres => "BOOLEAN",
                    DatabaseProvider::Sqlite => "INTEGER",
                    DatabaseProvider::Mysql => "BOOLEAN",
                },
                ScalarType::Int => match self.provider {
                    DatabaseProvider::Postgres => "INTEGER",
                    DatabaseProvider::Sqlite => "INTEGER",
                    DatabaseProvider::Mysql => "INT",
                },
                ScalarType::BigInt => match self.provider {
                    DatabaseProvider::Postgres => "BIGINT",
                    DatabaseProvider::Sqlite => "INTEGER",
                    DatabaseProvider::Mysql => "BIGINT",
                },
                ScalarType::Float => match self.provider {
                    DatabaseProvider::Postgres => "DOUBLE PRECISION",
                    DatabaseProvider::Sqlite => "REAL",
                    DatabaseProvider::Mysql => "DOUBLE",
                },
                ScalarType::Decimal { precision, scale } => match self.provider {
                    DatabaseProvider::Postgres => &format!("DECIMAL({}, {})", precision, scale),
                    // Use a NUMERIC-affinity name that carries precision info for pull.
                    DatabaseProvider::Sqlite => &format!("DECIMAL({}, {})", precision, scale),
                    DatabaseProvider::Mysql => &format!("DECIMAL({}, {})", precision, scale),
                },
                ScalarType::DateTime => match self.provider {
                    DatabaseProvider::Postgres => "TIMESTAMP",
                    // SQLite stores everything as text; use a descriptive type name
                    // so that `db pull` can reconstruct the correct Nautilus type.
                    DatabaseProvider::Sqlite => "DATETIME",
                    DatabaseProvider::Mysql => "DATETIME",
                },
                ScalarType::Bytes => match self.provider {
                    DatabaseProvider::Postgres => "BYTEA",
                    DatabaseProvider::Sqlite => "BLOB",
                    DatabaseProvider::Mysql => "BLOB",
                },
                ScalarType::Json => match self.provider {
                    DatabaseProvider::Postgres => "JSONB",
                    DatabaseProvider::Sqlite => "JSON",
                    DatabaseProvider::Mysql => "JSON",
                },
                ScalarType::Citext => "CITEXT",
                ScalarType::Hstore => "HSTORE",
                ScalarType::Ltree => "LTREE",
                ScalarType::Vector { dimension } => {
                    return Ok(format!("VECTOR({})", dimension));
                }
                ScalarType::Jsonb => "JSONB",
                ScalarType::Xml => "XML",
                ScalarType::Char { length } => {
                    return Ok(format!("CHAR({})", length));
                }
                ScalarType::VarChar { length } => {
                    return Ok(format!("VARCHAR({})", length));
                }
                ScalarType::Uuid => match self.provider {
                    DatabaseProvider::Postgres => "UUID",
                    // CHAR(36) has TEXT affinity and unambiguously signals UUID.
                    DatabaseProvider::Sqlite => "CHAR(36)",
                    DatabaseProvider::Mysql => "CHAR(36)",
                },
            },
            ResolvedFieldType::Enum { enum_name } => match self.provider {
                // Use lowercase unquoted name so that `column_type_sql`
                // (which lowercases the result) produces a plain `role`
                // rather than `"role"` — matching what the inspector
                // returns from `normalize_pg_type`.
                DatabaseProvider::Postgres => return Ok(enum_name.to_lowercase()),
                DatabaseProvider::Sqlite | DatabaseProvider::Mysql => "TEXT",
            },
            ResolvedFieldType::Relation(_) => return Ok("".to_string()),
            ResolvedFieldType::CompositeType { type_name } => {
                if strategy.supports_user_defined_types() {
                    return Ok(type_name.to_lowercase());
                }
                if let Some(storage_sql) = strategy.composite_storage_sql(storage_strategy) {
                    storage_sql
                } else {
                    return Err(MigrationError::ValidationError(
                        strategy.native_composite_support_error(type_name, false),
                    ));
                }
            }
        };

        Ok(base_type.to_string())
    }

    /// Generate DEFAULT value SQL
    pub(crate) fn generate_default_value(
        &self,
        default: &DefaultValue,
        _field_type: &ResolvedFieldType,
    ) -> Result<String> {
        match default {
            DefaultValue::String(s) => Ok(format!("'{}'", s.replace('\'', "''"))),
            DefaultValue::Number(n) => Ok(n.clone()),
            DefaultValue::Boolean(b) => match self.provider {
                DatabaseProvider::Postgres | DatabaseProvider::Mysql => {
                    Ok(if *b { "TRUE" } else { "FALSE" }.to_string())
                }
                DatabaseProvider::Sqlite => Ok(if *b { "1" } else { "0" }.to_string()),
            },
            DefaultValue::Function(func) => match func.name.as_str() {
                "autoincrement" => Ok("AUTOINCREMENT".to_string()),
                "uuid" => match self.provider {
                    DatabaseProvider::Postgres => Ok("gen_random_uuid()".to_string()),
                    DatabaseProvider::Sqlite => Ok("(lower(hex(randomblob(4)))||'-'||lower(hex(randomblob(2)))||'-'||lower(hex(randomblob(2)))||'-'||lower(hex(randomblob(2)))||'-'||lower(hex(randomblob(6))))".to_string()),
                    DatabaseProvider::Mysql => Ok("(UUID())".to_string()),
                },
                "now" => match self.provider {
                    DatabaseProvider::Postgres => Ok("CURRENT_TIMESTAMP".to_string()),
                    DatabaseProvider::Sqlite => Ok("CURRENT_TIMESTAMP".to_string()),
                    DatabaseProvider::Mysql => Ok("CURRENT_TIMESTAMP".to_string()),
                },
                _ => Ok(format!("{}()", func.name)),
            },
            DefaultValue::EnumVariant(variant) => Ok(format!("'{}'", variant)),
        }
    }

    /// Generate referential action SQL
    fn referential_action_sql(
        &self,
        action: &nautilus_schema::ast::ReferentialAction,
    ) -> &'static str {
        use nautilus_schema::ast::ReferentialAction;
        match action {
            ReferentialAction::Cascade => "CASCADE",
            ReferentialAction::Restrict => "RESTRICT",
            ReferentialAction::NoAction => "NO ACTION",
            ReferentialAction::SetNull => "SET NULL",
            ReferentialAction::SetDefault => "SET DEFAULT",
        }
    }

    /// Return the canonical SQL type string for a field (used by the diff engine).
    ///
    /// The result is lower-cased so it can be compared directly with the
    /// normalised live-DB type returned by `SchemaInspector`.
    pub fn column_type_sql(&self, field: &FieldIr) -> Result<String> {
        self.generate_column_type(
            &field.field_type,
            !field.is_required,
            field.is_array,
            field.storage_strategy,
        )
        .map(|s| s.to_lowercase())
    }

    /// Return the canonical SQL type string for a composite-type field (used by the diff engine).
    ///
    /// Mirrors `column_type_sql` but operates on [`CompositeFieldIr`] which lacks
    /// `default_value`, `is_required`, and other table-column-specific metadata.
    pub fn column_type_sql_for_composite(&self, field: &CompositeFieldIr) -> Result<String> {
        self.generate_column_type(
            &field.field_type,
            !field.is_required,
            field.is_array,
            field.storage_strategy,
        )
        .map(|s| s.to_lowercase())
    }

    /// Return the canonical SQL default string for a field (used by the diff engine).
    pub fn column_default_sql(&self, field: &FieldIr) -> Result<Option<String>> {
        match &field.default_value {
            // autoincrement is handled at PK level, not as a column DEFAULT
            Some(DefaultValue::Function(f)) if f.name == "autoincrement" => Ok(None),
            Some(d) => self
                .generate_default_value(d, &field.field_type)
                .map(|s| Some(s.to_lowercase())),
            None => Ok(None),
        }
    }

    /// Map scalar type to PostgreSQL base type for arrays
    fn scalar_to_pg_type(&self, scalar: &ScalarType) -> Result<String> {
        Ok(match scalar {
            ScalarType::String => "TEXT",
            ScalarType::Boolean => "BOOLEAN",
            ScalarType::Int => "INTEGER",
            ScalarType::BigInt => "BIGINT",
            ScalarType::Float => "DOUBLE PRECISION",
            ScalarType::Decimal { precision, scale } => {
                return Ok(format!("DECIMAL({}, {})", precision, scale));
            }
            ScalarType::DateTime => "TIMESTAMP",
            ScalarType::Bytes => "BYTEA",
            ScalarType::Json => "JSONB",
            ScalarType::Uuid => "UUID",
            ScalarType::Citext => "CITEXT",
            ScalarType::Hstore => "HSTORE",
            ScalarType::Ltree => "LTREE",
            ScalarType::Vector { dimension } => {
                return Ok(format!("VECTOR({})", dimension));
            }
            ScalarType::Jsonb => "JSONB",
            ScalarType::Xml => "XML",
            ScalarType::Char { length } => {
                return Ok(format!("CHAR({})", length));
            }
            ScalarType::VarChar { length } => {
                return Ok(format!("VARCHAR({})", length));
            }
        }
        .to_string())
    }

    /// Quote an identifier for the target database
    pub(crate) fn quote_identifier(&self, name: &str) -> String {
        self.provider.quote_identifier(name)
    }

    /// Quote a PostgreSQL type identifier.
    ///
    /// We quote all emitted type names so mixed-case live types like
    /// `"PostStatus"` are addressed exactly as stored and never folded to
    /// `poststatus` by the SQL parser.
    fn quote_type_identifier(&self, name: &str) -> String {
        self.quote_identifier(name)
    }
}

fn live_table_names_for_truncate(live: &crate::live::LiveSchema) -> Vec<String> {
    use std::collections::{BTreeSet, HashMap};

    let mut remaining: BTreeSet<String> = live.tables.keys().cloned().collect();
    let mut dependencies: HashMap<String, BTreeSet<String>> = live
        .tables
        .iter()
        .map(|(name, table)| {
            let deps = table
                .foreign_keys
                .iter()
                .map(|fk| fk.referenced_table.clone())
                .filter(|dep| dep != name && live.tables.contains_key(dep))
                .collect::<BTreeSet<_>>();
            (name.clone(), deps)
        })
        .collect();

    let mut ordered = Vec::new();

    while !remaining.is_empty() {
        let ready: Vec<String> = remaining
            .iter()
            .filter(|name| {
                dependencies
                    .get(*name)
                    .map(|deps| deps.is_empty())
                    .unwrap_or(true)
            })
            .cloned()
            .collect();

        if ready.is_empty() {
            ordered.extend(remaining.into_iter());
            break;
        }

        for name in &ready {
            remaining.remove(name);
        }

        for deps in dependencies.values_mut() {
            for name in &ready {
                deps.remove(name);
            }
        }

        ordered.extend(ready);
    }

    ordered.reverse();
    ordered
}
