//! Intermediate representation (IR) of a validated schema.
//!
//! This module defines a provider-agnostic IR that represents a schema after
//! semantic validation. All type references are resolved, relations are validated,
//! and both logical and physical names are stored explicitly.

pub use crate::ast::ComputedKind;
use crate::ast::{ReferentialAction, StorageStrategy};
use crate::span::Span;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

/// Validated intermediate representation of a complete schema.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaIr {
    /// The datasource declaration (if present).
    pub datasource: Option<DatasourceIr>,
    /// The generator declaration (if present).
    pub generator: Option<GeneratorIr>,
    /// All models in the schema, indexed by logical name.
    pub models: HashMap<String, ModelIr>,
    /// All enums in the schema, indexed by logical name.
    pub enums: HashMap<String, EnumIr>,
    /// All composite types in the schema, indexed by logical name.
    pub composite_types: HashMap<String, CompositeTypeIr>,
}

impl SchemaIr {
    /// Creates a new empty schema IR.
    pub fn new() -> Self {
        Self {
            datasource: None,
            generator: None,
            models: HashMap::new(),
            enums: HashMap::new(),
            composite_types: HashMap::new(),
        }
    }

    /// Gets a model by logical name.
    pub fn get_model(&self, name: &str) -> Option<&ModelIr> {
        self.models.get(name)
    }

    /// Gets an enum by logical name.
    pub fn get_enum(&self, name: &str) -> Option<&EnumIr> {
        self.enums.get(name)
    }

    /// Gets a composite type by logical name.
    pub fn get_composite_type(&self, name: &str) -> Option<&CompositeTypeIr> {
        self.composite_types.get(name)
    }
}

impl Default for SchemaIr {
    fn default() -> Self {
        Self::new()
    }
}

/// Validated datasource configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct DatasourceIr {
    /// The datasource name (e.g., "db").
    pub name: String,
    /// The provider (e.g., "postgresql", "mysql", "sqlite").
    pub provider: String,
    /// The connection URL (may contain env() references).
    pub url: String,
    /// Optional direct connection URL for admin/introspection paths.
    ///
    /// When present, tooling such as `db pull`, `db push`, and migrations can
    /// prefer this over `url` so runtime traffic can continue to use a pooled
    /// connection string.
    pub direct_url: Option<String>,
    /// PostgreSQL extensions declared in the datasource block.
    ///
    /// Names are lower-cased, deduplicated and sorted for stable output.
    /// Empty for non-Postgres providers (enforced by the validator).
    pub extensions: Vec<String>,
    /// Span of the datasource block.
    pub span: Span,
}

impl DatasourceIr {
    /// Returns the preferred runtime URL expression.
    ///
    /// Runtime clients should prefer `url` and only fall back to `direct_url`
    /// when `url` is unavailable.
    pub fn runtime_url(&self) -> &str {
        if !self.url.is_empty() {
            &self.url
        } else {
            self.direct_url.as_deref().unwrap_or(&self.url)
        }
    }

    /// Returns the preferred admin/introspection URL expression.
    ///
    /// Admin tooling should prefer `direct_url` when present, then fall back to
    /// the normal runtime `url`.
    pub fn admin_url(&self) -> &str {
        self.direct_url.as_deref().unwrap_or(&self.url)
    }
}

/// Whether the generated client API uses async or sync methods.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum InterfaceKind {
    /// Synchronous API (default). Methods are plain `fn`, Rust uses
    /// `tokio::task::block_in_place` internally; Python uses `asyncio.run()`.
    #[default]
    Sync,
    /// Asynchronous API. Methods are `async fn` in Rust and `async def` in Python.
    Async,
}

/// Packaging mode for the generated Java client bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JavaGenerationMode {
    /// Generate the default Maven module layout rooted at `output/`.
    #[default]
    Maven,
    /// Generate the Maven module layout and also build a plain Java jar bundle.
    Jar,
}

/// Validated generator configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct GeneratorIr {
    /// The generator name (e.g., "client").
    pub name: String,
    /// The provider (e.g., "nautilus-client-rs").
    pub provider: String,
    /// The output path (if specified).
    pub output: Option<String>,
    /// Whether to generate a sync or async client interface.
    /// Defaults to [`InterfaceKind::Sync`] when the `interface` field is omitted.
    pub interface: InterfaceKind,
    /// Depth of recursive include TypedDicts generated for the Python client.
    pub recursive_type_depth: usize,
    /// Root Java package for the generated client (Java provider only).
    pub java_package: Option<String>,
    /// Maven groupId for the generated Java module (Java provider only).
    pub java_group_id: Option<String>,
    /// Maven artifactId for the generated Java module (Java provider only).
    pub java_artifact_id: Option<String>,
    /// Java packaging mode (Java provider only).
    pub java_mode: Option<JavaGenerationMode>,
    /// Span of the generator block.
    pub span: Span,
}

/// Validated model with fully resolved fields and metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelIr {
    /// The logical name as defined in the schema (e.g., "User").
    pub logical_name: String,
    /// The physical database table name (from @@map or logical_name).
    pub db_name: String,
    /// All fields in the model.
    pub fields: Vec<FieldIr>,
    /// Primary key metadata.
    pub primary_key: PrimaryKeyIr,
    /// Unique constraints (from @unique and @@unique).
    pub unique_constraints: Vec<UniqueConstraintIr>,
    /// Indexes (from @@index).
    pub indexes: Vec<IndexIr>,
    /// Table-level CHECK constraint expressions (SQL strings).
    pub check_constraints: Vec<String>,
    /// Span of the model declaration.
    pub span: Span,
}

impl ModelIr {
    /// Finds a field by logical name.
    pub fn find_field(&self, name: &str) -> Option<&FieldIr> {
        self.fields.iter().find(|f| f.logical_name == name)
    }

    /// Returns an iterator over scalar fields (non-relations).
    pub fn scalar_fields(&self) -> impl Iterator<Item = &FieldIr> {
        self.fields
            .iter()
            .filter(|f| !matches!(f.field_type, ResolvedFieldType::Relation(_)))
    }

    /// Returns an iterator over relation fields.
    pub fn relation_fields(&self) -> impl Iterator<Item = &FieldIr> {
        self.fields
            .iter()
            .filter(|f| matches!(f.field_type, ResolvedFieldType::Relation(_)))
    }
}

/// Validated field with resolved type.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldIr {
    /// The logical field name as defined in the schema (e.g., "userId").
    pub logical_name: String,
    /// The physical database column name (from @map or logical_name).
    pub db_name: String,
    /// The resolved field type (scalar, enum, or relation).
    pub field_type: ResolvedFieldType,
    /// Whether the field is required (not optional and not array).
    pub is_required: bool,
    /// Whether the field is an array.
    pub is_array: bool,
    /// Storage strategy for array fields (None for non-arrays or native support).
    pub storage_strategy: Option<StorageStrategy>,
    /// Default value (if specified via @default).
    pub default_value: Option<DefaultValue>,
    /// Whether the field has @unique.
    pub is_unique: bool,
    /// Whether the field has @updatedAt — auto-set to now() on every write.
    pub is_updated_at: bool,
    /// Computed column expression and kind — `None` for regular fields.
    pub computed: Option<(String, ComputedKind)>,
    /// Column-level CHECK constraint expression (SQL string). `None` for unconstrained fields.
    pub check: Option<String>,
    /// Span of the field declaration.
    pub span: Span,
}

/// Resolved field type after validation.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedFieldType {
    /// A scalar type (String, Int, etc.).
    Scalar(ScalarType),
    /// An enum type with the enum's logical name.
    Enum {
        /// The logical name of the enum.
        enum_name: String,
    },
    /// A relation to another model.
    Relation(RelationIr),
    /// A composite type (embedded struct).
    CompositeType {
        /// The logical name of the composite type.
        type_name: String,
    },
}

/// Scalar type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarType {
    /// UTF-8 string type.
    String,
    /// Boolean type (true/false).
    Boolean,
    /// 32-bit integer.
    Int,
    /// 64-bit integer.
    BigInt,
    /// 64-bit floating point.
    Float,
    /// Fixed-precision decimal number.
    Decimal {
        /// Number of total digits.
        precision: u32,
        /// Number of digits after decimal point.
        scale: u32,
    },
    /// Date and time.
    DateTime,
    /// Binary data.
    Bytes,
    /// JSON value.
    Json,
    /// UUID value.
    Uuid,
    /// Case-insensitive text value (PostgreSQL + citext extension).
    Citext,
    /// Key/value string map (PostgreSQL + hstore extension).
    Hstore,
    /// Label tree path (PostgreSQL + ltree extension).
    Ltree,
    /// JSONB value (PostgreSQL only).
    Jsonb,
    /// XML value (PostgreSQL only).
    Xml,
    /// Fixed-length character type.
    Char {
        /// Column length.
        length: u32,
    },
    /// Variable-length character type.
    VarChar {
        /// Maximum column length.
        length: u32,
    },
}

impl ScalarType {
    /// Returns the Rust type name for this scalar type.
    pub fn rust_type(&self) -> &'static str {
        match self {
            ScalarType::String => "String",
            ScalarType::Boolean => "bool",
            ScalarType::Int => "i32",
            ScalarType::BigInt => "i64",
            ScalarType::Float => "f64",
            ScalarType::Decimal { .. } => "rust_decimal::Decimal",
            ScalarType::DateTime => "chrono::NaiveDateTime",
            ScalarType::Bytes => "Vec<u8>",
            ScalarType::Json => "serde_json::Value",
            ScalarType::Uuid => "uuid::Uuid",
            ScalarType::Citext | ScalarType::Ltree => "String",
            ScalarType::Hstore => "std::collections::BTreeMap<String, Option<String>>",
            ScalarType::Jsonb => "serde_json::Value",
            ScalarType::Xml | ScalarType::Char { .. } | ScalarType::VarChar { .. } => "String",
        }
    }

    /// Returns `true` when this scalar type is supported by the given database provider.
    pub fn supported_by(self, provider: DatabaseProvider) -> bool {
        match self {
            ScalarType::Citext
            | ScalarType::Hstore
            | ScalarType::Ltree
            | ScalarType::Jsonb
            | ScalarType::Xml => provider == DatabaseProvider::Postgres,
            ScalarType::Char { .. } | ScalarType::VarChar { .. } => {
                matches!(
                    provider,
                    DatabaseProvider::Postgres | DatabaseProvider::Mysql
                )
            }
            _ => true,
        }
    }

    /// Human-readable list of supported providers (for diagnostics).
    pub fn supported_providers(self) -> &'static str {
        match self {
            ScalarType::Citext
            | ScalarType::Hstore
            | ScalarType::Ltree
            | ScalarType::Jsonb
            | ScalarType::Xml => "PostgreSQL only",
            ScalarType::Char { .. } | ScalarType::VarChar { .. } => "PostgreSQL and MySQL",
            _ => "all databases",
        }
    }
}

/// Validated relation metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct RelationIr {
    /// Optional relation name (required for multiple relations between same models).
    pub name: Option<String>,
    /// The logical name of the target model.
    pub target_model: String,
    /// Foreign key field names in the current model (logical names).
    pub fields: Vec<String>,
    /// Referenced field names in the target model (logical names).
    pub references: Vec<String>,
    /// Referential action on delete.
    pub on_delete: Option<ReferentialAction>,
    /// Referential action on update.
    pub on_update: Option<ReferentialAction>,
}

/// Default value for a field.
#[derive(Debug, Clone, PartialEq)]
pub enum DefaultValue {
    /// A literal string value.
    String(String),
    /// A literal number value (stored as string to preserve precision).
    Number(String),
    /// A literal boolean value.
    Boolean(bool),
    /// An enum variant name.
    EnumVariant(String),
    /// A function call (autoincrement, uuid, now, etc.).
    Function(FunctionCall),
}

/// Function call in a default value.
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    /// The function name (e.g., "autoincrement", "uuid", "now").
    pub name: String,
    /// Function arguments (if any).
    pub args: Vec<String>,
}

/// Primary key metadata.
#[derive(Debug, Clone, PartialEq)]
pub enum PrimaryKeyIr {
    /// Single-field primary key (from @id).
    Single(String),
    /// Composite primary key (from @@id).
    Composite(Vec<String>),
}

impl PrimaryKeyIr {
    /// Returns the field names that form the primary key.
    pub fn fields(&self) -> Vec<&str> {
        match self {
            PrimaryKeyIr::Single(field) => vec![field.as_str()],
            PrimaryKeyIr::Composite(fields) => fields.iter().map(|s| s.as_str()).collect(),
        }
    }

    /// Returns true if this is a single-field primary key.
    pub fn is_single(&self) -> bool {
        matches!(self, PrimaryKeyIr::Single(_))
    }

    /// Returns true if this is a composite primary key.
    pub fn is_composite(&self) -> bool {
        matches!(self, PrimaryKeyIr::Composite(_))
    }
}

/// Unique constraint metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct UniqueConstraintIr {
    /// Field names (logical) that form the unique constraint.
    pub fields: Vec<String>,
}

/// Index access method / algorithm.
///
/// The default (when `None` is stored on [`IndexIr`]) lets the DBMS choose
/// (BTree on every supported database).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    /// B-Tree (default on all databases).
    BTree,
    /// Hash index — PostgreSQL and MySQL 8+.
    Hash,
    /// Generalized Inverted Index — PostgreSQL only (arrays, JSONB, full-text).
    Gin,
    /// Generalized Search Tree — PostgreSQL only (geometry, range types).
    Gist,
    /// Block Range Index — PostgreSQL only (large ordered tables).
    Brin,
    /// Full-text index — MySQL only.
    FullText,
}

/// Error returned when parsing an unknown index type string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseIndexTypeError;

impl fmt::Display for ParseIndexTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unknown index type")
    }
}

impl std::error::Error for ParseIndexTypeError {}

impl FromStr for IndexType {
    type Err = ParseIndexTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "btree" => Ok(IndexType::BTree),
            "hash" => Ok(IndexType::Hash),
            "gin" => Ok(IndexType::Gin),
            "gist" => Ok(IndexType::Gist),
            "brin" => Ok(IndexType::Brin),
            "fulltext" => Ok(IndexType::FullText),
            _ => Err(ParseIndexTypeError),
        }
    }
}

impl IndexType {
    /// Returns `true` when this index type is supported by the given database provider.
    pub fn supported_by(self, provider: DatabaseProvider) -> bool {
        match self {
            IndexType::BTree => true,
            IndexType::Hash => matches!(
                provider,
                DatabaseProvider::Postgres | DatabaseProvider::Mysql
            ),
            IndexType::Gin | IndexType::Gist | IndexType::Brin => {
                provider == DatabaseProvider::Postgres
            }
            IndexType::FullText => provider == DatabaseProvider::Mysql,
        }
    }

    /// Human-readable list of supported providers (for diagnostics).
    pub fn supported_providers(self) -> &'static str {
        match self {
            IndexType::BTree => "all databases",
            IndexType::Hash => "PostgreSQL and MySQL",
            IndexType::Gin => "PostgreSQL only",
            IndexType::Gist => "PostgreSQL only",
            IndexType::Brin => "PostgreSQL only",
            IndexType::FullText => "MySQL only",
        }
    }

    /// The canonical display name used in schema files.
    pub fn as_str(self) -> &'static str {
        match self {
            IndexType::BTree => "BTree",
            IndexType::Hash => "Hash",
            IndexType::Gin => "Gin",
            IndexType::Gist => "Gist",
            IndexType::Brin => "Brin",
            IndexType::FullText => "FullText",
        }
    }
}

/// The three datasource providers recognised by the Nautilus schema language.
///
/// Obtained by parsing the `provider` field of a `datasource` block:
/// ```text
/// datasource db {
///     provider = "postgresql"  // -> DatabaseProvider::Postgres
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseProvider {
    /// PostgreSQL (provider string: `"postgresql"`).
    Postgres,
    /// MySQL / MariaDB (provider string: `"mysql"`).
    Mysql,
    /// SQLite (provider string: `"sqlite"`).
    Sqlite,
}

/// Error returned when parsing an unknown database provider string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseDatabaseProviderError;

impl fmt::Display for ParseDatabaseProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unknown database provider")
    }
}

impl std::error::Error for ParseDatabaseProviderError {}

impl FromStr for DatabaseProvider {
    type Err = ParseDatabaseProviderError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "postgresql" => Ok(DatabaseProvider::Postgres),
            "mysql" => Ok(DatabaseProvider::Mysql),
            "sqlite" => Ok(DatabaseProvider::Sqlite),
            _ => Err(ParseDatabaseProviderError),
        }
    }
}

impl DatabaseProvider {
    /// All valid datasource provider strings.
    pub const ALL: &'static [&'static str] = &["postgresql", "mysql", "sqlite"];

    /// The canonical provider string used in `.nautilus` schema files.
    pub fn as_str(self) -> &'static str {
        match self {
            DatabaseProvider::Postgres => "postgresql",
            DatabaseProvider::Mysql => "mysql",
            DatabaseProvider::Sqlite => "sqlite",
        }
    }

    /// Human-readable display name (for diagnostic messages).
    pub fn display_name(self) -> &'static str {
        match self {
            DatabaseProvider::Postgres => "PostgreSQL",
            DatabaseProvider::Mysql => "MySQL",
            DatabaseProvider::Sqlite => "SQLite",
        }
    }
}

impl std::fmt::Display for DatabaseProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The generator (client) providers recognised by the Nautilus schema language.
///
/// Obtained by parsing the `provider` field of a `generator` block:
/// ```text
/// generator client {
///     provider = "nautilus-client-rs"  // -> ClientProvider::Rust
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientProvider {
    /// Rust client (provider string: `"nautilus-client-rs"`).
    Rust,
    /// Python client (provider string: `"nautilus-client-py"`).
    Python,
    /// JavaScript/TypeScript client (provider string: `"nautilus-client-js"`).
    JavaScript,
    /// Java client (provider string: `"nautilus-client-java"`).
    Java,
}

/// Error returned when parsing an unknown client provider string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseClientProviderError;

impl fmt::Display for ParseClientProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unknown client provider")
    }
}

impl std::error::Error for ParseClientProviderError {}

impl FromStr for ClientProvider {
    type Err = ParseClientProviderError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "nautilus-client-rs" => Ok(ClientProvider::Rust),
            "nautilus-client-py" => Ok(ClientProvider::Python),
            "nautilus-client-js" => Ok(ClientProvider::JavaScript),
            "nautilus-client-java" => Ok(ClientProvider::Java),
            _ => Err(ParseClientProviderError),
        }
    }
}

impl ClientProvider {
    /// All valid generator provider strings.
    pub const ALL: &'static [&'static str] = &[
        "nautilus-client-rs",
        "nautilus-client-py",
        "nautilus-client-js",
        "nautilus-client-java",
    ];

    /// The canonical provider string used in `.nautilus` schema files.
    pub fn as_str(self) -> &'static str {
        match self {
            ClientProvider::Rust => "nautilus-client-rs",
            ClientProvider::Python => "nautilus-client-py",
            ClientProvider::JavaScript => "nautilus-client-js",
            ClientProvider::Java => "nautilus-client-java",
        }
    }
}

impl fmt::Display for ClientProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Index metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexIr {
    /// Field names (logical) that form the index.
    pub fields: Vec<String>,
    /// Optional index type (access method).  `None` -> let the DBMS decide
    /// (BTree on all supported databases).
    pub index_type: Option<IndexType>,
    /// Logical name — for developer reference only.
    pub name: Option<String>,
    /// Physical DDL name.  When set this is used as the `CREATE INDEX` name
    /// instead of the auto-generated `idx_{table}_{cols}` name.
    pub map: Option<String>,
}

/// Validated enum type.
#[derive(Debug, Clone, PartialEq)]
pub struct EnumIr {
    /// The logical enum name (e.g., "Role").
    pub logical_name: String,
    /// Enum variant names.
    pub variants: Vec<String>,
    /// Span of the enum declaration.
    pub span: Span,
}

impl EnumIr {
    /// Checks if a variant exists.
    pub fn has_variant(&self, name: &str) -> bool {
        self.variants.iter().any(|v| v == name)
    }
}

/// A single field within a composite type.
///
/// Only scalar and enum field types are allowed — no relations or nested composite types.
#[derive(Debug, Clone, PartialEq)]
pub struct CompositeFieldIr {
    /// The logical field name as defined in the type block.
    pub logical_name: String,
    /// The physical name (from @map or logical_name).
    pub db_name: String,
    /// The resolved field type (Scalar or Enum only).
    pub field_type: ResolvedFieldType,
    /// Whether the field is required (not optional).
    pub is_required: bool,
    /// Whether the field is an array.
    pub is_array: bool,
    /// Storage strategy for array fields.
    pub storage_strategy: Option<StorageStrategy>,
    /// Span of the field declaration.
    pub span: Span,
}

/// Validated composite type (embedded struct).
#[derive(Debug, Clone, PartialEq)]
pub struct CompositeTypeIr {
    /// The logical type name as defined in the schema (e.g., "Address").
    pub logical_name: String,
    /// All fields of the composite type.
    pub fields: Vec<CompositeFieldIr>,
    /// Span of the type declaration.
    pub span: Span,
}
