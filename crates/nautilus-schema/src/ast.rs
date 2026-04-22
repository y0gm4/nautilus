//! Abstract Syntax Tree (AST) for the nautilus schema language.
//!
//! This module defines the complete AST structure for representing parsed schemas.
//! All nodes include [`Span`] information for precise error diagnostics.
//!
//! The AST supports the Visitor pattern via the [`accept`](Schema::accept) methods,
//! allowing flexible traversal and transformation operations.
//!
//! # Example
//!
//! ```ignore
//! use nautilus_schema::{Lexer, Parser};
//!
//! let source = r#"
//!     model User {
//!       id    Int    @id @default(autoincrement())
//!       email String @unique
//!     }
//! "#;
//!
//! let tokens = Lexer::new(source).collect::<Result<Vec<_>, _>>().unwrap();
//! let schema = Parser::new(&tokens).parse_schema().unwrap();
//!
//! println!("Found {} declarations", schema.declarations.len());
//! ```

use crate::span::Span;
use std::fmt;

/// Top-level schema document containing all declarations.
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    /// All declarations in the schema (datasources, generators, models, enums).
    pub declarations: Vec<Declaration>,
    /// Span covering the entire schema.
    pub span: Span,
}

impl Schema {
    /// Creates a new schema with the given declarations.
    pub fn new(declarations: Vec<Declaration>, span: Span) -> Self {
        Self { declarations, span }
    }

    /// Finds all model declarations in the schema.
    pub fn models(&self) -> impl Iterator<Item = &ModelDecl> {
        self.declarations.iter().filter_map(|d| match d {
            Declaration::Model(m) => Some(m),
            _ => None,
        })
    }

    /// Finds all enum declarations in the schema.
    pub fn enums(&self) -> impl Iterator<Item = &EnumDecl> {
        self.declarations.iter().filter_map(|d| match d {
            Declaration::Enum(e) => Some(e),
            _ => None,
        })
    }

    /// Finds all composite type declarations in the schema.
    pub fn types(&self) -> impl Iterator<Item = &TypeDecl> {
        self.declarations.iter().filter_map(|d| match d {
            Declaration::Type(t) => Some(t),
            _ => None,
        })
    }

    /// Finds the first datasource declaration.
    pub fn datasource(&self) -> Option<&DatasourceDecl> {
        self.declarations.iter().find_map(|d| match d {
            Declaration::Datasource(ds) => Some(ds),
            _ => None,
        })
    }

    /// Finds the first generator declaration.
    pub fn generator(&self) -> Option<&GeneratorDecl> {
        self.declarations.iter().find_map(|d| match d {
            Declaration::Generator(g) => Some(g),
            _ => None,
        })
    }
}

/// A top-level declaration in the schema.
#[derive(Debug, Clone, PartialEq)]
pub enum Declaration {
    /// A datasource block.
    Datasource(DatasourceDecl),
    /// A generator block.
    Generator(GeneratorDecl),
    /// A model block.
    Model(ModelDecl),
    /// An enum block.
    Enum(EnumDecl),
    /// A composite type block.
    Type(TypeDecl),
}

impl Declaration {
    /// Returns the span of this declaration.
    pub fn span(&self) -> Span {
        match self {
            Declaration::Datasource(d) => d.span,
            Declaration::Generator(g) => g.span,
            Declaration::Model(m) => m.span,
            Declaration::Enum(e) => e.span,
            Declaration::Type(t) => t.span,
        }
    }

    /// Returns the name of this declaration.
    pub fn name(&self) -> &str {
        match self {
            Declaration::Datasource(d) => &d.name.value,
            Declaration::Generator(g) => &g.name.value,
            Declaration::Model(m) => &m.name.value,
            Declaration::Enum(e) => &e.name.value,
            Declaration::Type(t) => &t.name.value,
        }
    }
}

/// A datasource block declaration.
///
/// # Example
///
/// ```prisma
/// datasource db {
///   provider = "postgresql"
///   url      = env("DATABASE_URL")
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct DatasourceDecl {
    /// The name of the datasource (e.g., "db").
    pub name: Ident,
    /// Configuration fields (key-value pairs).
    pub fields: Vec<ConfigField>,
    /// Span covering the entire datasource block.
    pub span: Span,
}

impl DatasourceDecl {
    /// Finds a configuration field by name.
    pub fn find_field(&self, name: &str) -> Option<&ConfigField> {
        self.fields.iter().find(|f| f.name.value == name)
    }

    /// Gets the provider value if present.
    pub fn provider(&self) -> Option<&str> {
        self.find_field("provider").and_then(|f| match &f.value {
            Expr::Literal(Literal::String(s, _)) => Some(s.as_str()),
            _ => None,
        })
    }

    /// Gets the declared PostgreSQL extensions (best-effort).
    ///
    /// Accepts both identifiers (`pg_trgm`) and string literals (`"uuid-ossp"`)
    /// as array elements. Returns `None` when the `extensions` field is absent,
    /// and an empty vec when it is present but empty or malformed. Rigorous
    /// validation (including error reporting) happens in the validator.
    pub fn extensions(&self) -> Option<Vec<String>> {
        let field = self.find_field("extensions")?;
        let elements = match &field.value {
            Expr::Array { elements, .. } => elements,
            _ => return Some(Vec::new()),
        };
        Some(
            elements
                .iter()
                .filter_map(|e| match e {
                    Expr::Ident(ident) => Some(ident.value.clone()),
                    Expr::Literal(Literal::String(s, _)) => Some(s.clone()),
                    _ => None,
                })
                .collect(),
        )
    }
}

/// A generator block declaration.
///
/// # Example
///
/// ```prisma
/// generator client {
///   provider = "nautilus-client-rs"
///   output   = "../generated"
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct GeneratorDecl {
    /// The name of the generator (e.g., "client").
    pub name: Ident,
    /// Configuration fields (key-value pairs).
    pub fields: Vec<ConfigField>,
    /// Span covering the entire generator block.
    pub span: Span,
}

impl GeneratorDecl {
    /// Finds a configuration field by name.
    pub fn find_field(&self, name: &str) -> Option<&ConfigField> {
        self.fields.iter().find(|f| f.name.value == name)
    }
}

/// A configuration field in a datasource or generator block.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigField {
    /// The field name.
    pub name: Ident,
    /// The field value (typically a string or function call).
    pub value: Expr,
    /// Span covering the entire field declaration.
    pub span: Span,
}

/// A model block declaration.
///
/// # Example
///
/// ```prisma
/// model User {
///   id    Int    @id @default(autoincrement())
///   email String @unique
///   @@map("users")
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ModelDecl {
    /// The model name (e.g., "User").
    pub name: Ident,
    /// Field declarations.
    pub fields: Vec<FieldDecl>,
    /// Model-level attributes (@@map, @@id, etc.).
    pub attributes: Vec<ModelAttribute>,
    /// Span covering the entire model block.
    pub span: Span,
}

impl ModelDecl {
    /// Finds a field by name.
    pub fn find_field(&self, name: &str) -> Option<&FieldDecl> {
        self.fields.iter().find(|f| f.name.value == name)
    }

    /// Gets the physical table name from @@map attribute, or the model name.
    pub fn table_name(&self) -> &str {
        self.attributes
            .iter()
            .find_map(|attr| match attr {
                ModelAttribute::Map(name) => Some(name.as_str()),
                _ => None,
            })
            .unwrap_or(&self.name.value)
    }

    /// Checks if this model has a composite primary key (@@id).
    pub fn has_composite_key(&self) -> bool {
        self.attributes
            .iter()
            .any(|attr| matches!(attr, ModelAttribute::Id(_)))
    }

    /// Returns all fields that are part of relations.
    /// This includes fields with user-defined types (model/enum references).
    pub fn relation_fields(&self) -> impl Iterator<Item = &FieldDecl> {
        self.fields.iter().filter(|f| {
            f.has_relation_attribute() || matches!(f.field_type, FieldType::UserType(_))
        })
    }
}

/// A field declaration within a model.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDecl {
    /// The field name.
    pub name: Ident,
    /// The field type.
    pub field_type: FieldType,
    /// Optional or array modifier.
    pub modifier: FieldModifier,
    /// Field-level attributes (@id, @unique, etc.).
    pub attributes: Vec<FieldAttribute>,
    /// Span covering the entire field declaration.
    pub span: Span,
}

impl FieldDecl {
    /// Checks if this field is optional (has `?` modifier).
    pub fn is_optional(&self) -> bool {
        matches!(self.modifier, FieldModifier::Optional)
    }

    /// Checks if this field has an explicit not-null modifier (`!`).
    pub fn is_not_null(&self) -> bool {
        matches!(self.modifier, FieldModifier::NotNull)
    }

    /// Checks if this field is an array (has `[]` modifier).
    pub fn is_array(&self) -> bool {
        matches!(self.modifier, FieldModifier::Array)
    }

    /// Finds a field attribute by kind.
    pub fn find_attribute(&self, kind: &str) -> Option<&FieldAttribute> {
        self.attributes.iter().find(|attr| {
            matches!(
                (kind, attr),
                ("id", FieldAttribute::Id)
                    | ("unique", FieldAttribute::Unique)
                    | ("default", FieldAttribute::Default(_, _))
                    | ("map", FieldAttribute::Map(_))
                    | ("relation", FieldAttribute::Relation { .. })
                    | ("check", FieldAttribute::Check { .. })
            )
        })
    }

    /// Checks if this field has a @relation attribute.
    pub fn has_relation_attribute(&self) -> bool {
        self.attributes
            .iter()
            .any(|attr| matches!(attr, FieldAttribute::Relation { .. }))
    }

    /// Gets the physical column name from @map attribute, or the field name.
    pub fn column_name(&self) -> &str {
        self.attributes
            .iter()
            .find_map(|attr| match attr {
                FieldAttribute::Map(name) => Some(name.as_str()),
                _ => None,
            })
            .unwrap_or(&self.name.value)
    }
}

/// Field type modifiers (optional, not-null, or array).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldModifier {
    /// No modifier (required field).
    None,
    /// Optional field (`?`).
    Optional,
    /// Explicit not-null field (`!`).
    NotNull,
    /// Array field (`[]`).
    Array,
}

/// A field type in a model.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    /// String type.
    String,
    /// Boolean type.
    Boolean,
    /// Int type (32-bit).
    Int,
    /// BigInt type (64-bit).
    BigInt,
    /// Float type.
    Float,
    /// Decimal type with precision and scale.
    Decimal {
        /// Precision (total digits).
        precision: u32,
        /// Scale (digits after decimal point).
        scale: u32,
    },
    /// DateTime type.
    DateTime,
    /// Bytes type.
    Bytes,
    /// JSON type.
    Json,
    /// UUID type.
    Uuid,
    /// Case-insensitive text type (PostgreSQL + citext extension).
    Citext,
    /// Key/value string map type (PostgreSQL + hstore extension).
    Hstore,
    /// Label tree path type (PostgreSQL + ltree extension).
    Ltree,
    /// JSONB type (PostgreSQL only).
    Jsonb,
    /// XML type (PostgreSQL only).
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
    /// User-defined type (model or enum reference).
    UserType(String),
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldType::String => write!(f, "String"),
            FieldType::Boolean => write!(f, "Boolean"),
            FieldType::Int => write!(f, "Int"),
            FieldType::BigInt => write!(f, "BigInt"),
            FieldType::Float => write!(f, "Float"),
            FieldType::Decimal { precision, scale } => {
                write!(f, "Decimal({}, {})", precision, scale)
            }
            FieldType::DateTime => write!(f, "DateTime"),
            FieldType::Bytes => write!(f, "Bytes"),
            FieldType::Json => write!(f, "Json"),
            FieldType::Uuid => write!(f, "Uuid"),
            FieldType::Citext => write!(f, "Citext"),
            FieldType::Hstore => write!(f, "Hstore"),
            FieldType::Ltree => write!(f, "Ltree"),
            FieldType::Jsonb => write!(f, "Jsonb"),
            FieldType::Xml => write!(f, "Xml"),
            FieldType::Char { length } => write!(f, "Char({})", length),
            FieldType::VarChar { length } => write!(f, "VarChar({})", length),
            FieldType::UserType(name) => write!(f, "{}", name),
        }
    }
}

/// Storage strategy for array fields on databases without native array support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageStrategy {
    /// Native database array type (PostgreSQL).
    Native,
    /// JSON-serialized array storage (MySQL, SQLite).
    Json,
}

/// Whether a computed column is physically stored or computed on every read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComputedKind {
    /// Column value persisted on disk (PostgreSQL, MySQL, SQLite).
    Stored,
    /// Column value computed on read, not stored (MySQL and SQLite only).
    Virtual,
}

/// A field-level attribute (@id, @unique, etc.).
#[derive(Debug, Clone, PartialEq)]
pub enum FieldAttribute {
    /// @id attribute.
    Id,
    /// @unique attribute.
    Unique,
    /// @default(value) attribute.
    /// The `Span` covers the full `@default(...)` token range.
    Default(Expr, Span),
    /// @map("name") attribute.
    Map(String),
    /// @store(json) attribute for array storage strategy.
    Store {
        /// Storage strategy (currently only "json" supported).
        strategy: StorageStrategy,
        /// Span of the entire attribute.
        span: Span,
    },
    /// @relation(...) attribute.
    Relation {
        /// name: "relationName" (optional, required for multiple relations)
        name: Option<String>,
        /// fields: [field1, field2]
        fields: Option<Vec<Ident>>,
        /// references: [field1, field2]
        references: Option<Vec<Ident>>,
        /// onDelete: Cascade | SetNull | ...
        on_delete: Option<ReferentialAction>,
        /// onUpdate: Cascade | SetNull | ...
        on_update: Option<ReferentialAction>,
        /// Span of the entire attribute.
        span: Span,
    },
    /// @updatedAt — auto-set to current timestamp on every write.
    UpdatedAt {
        /// Span covering `@updatedAt`.
        span: Span,
    },
    /// @computed(expr, Stored | Virtual) — database-generated column.
    Computed {
        /// Parsed SQL expression (e.g. `price * quantity`).
        expr: crate::sql_expr::SqlExpr,
        /// Whether the value is stored on disk or computed on every read.
        kind: ComputedKind,
        /// Span of the entire `@computed(...)` attribute.
        span: Span,
    },
    /// @check(bool_expr) — column-level CHECK constraint.
    Check {
        /// Parsed boolean expression (e.g. `age >= 0 AND age <= 150`).
        expr: crate::bool_expr::BoolExpr,
        /// Span of the entire `@check(...)` attribute.
        span: Span,
    },
}

/// Referential actions for foreign key constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferentialAction {
    /// CASCADE action.
    Cascade,
    /// RESTRICT action.
    Restrict,
    /// NO ACTION.
    NoAction,
    /// SET NULL.
    SetNull,
    /// SET DEFAULT.
    SetDefault,
}

impl fmt::Display for ReferentialAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReferentialAction::Cascade => write!(f, "Cascade"),
            ReferentialAction::Restrict => write!(f, "Restrict"),
            ReferentialAction::NoAction => write!(f, "NoAction"),
            ReferentialAction::SetNull => write!(f, "SetNull"),
            ReferentialAction::SetDefault => write!(f, "SetDefault"),
        }
    }
}

/// A model-level attribute (@@map, @@id, etc.).
#[derive(Debug, Clone, PartialEq)]
pub enum ModelAttribute {
    /// @@map("table_name") attribute.
    Map(String),
    /// @@id([field1, field2]) composite primary key.
    Id(Vec<Ident>),
    /// @@unique([field1, field2]) composite unique constraint.
    Unique(Vec<Ident>),
    /// @@index([field1, field2], type: Hash, name: "idx_name", map: "db_idx") index.
    Index {
        /// Fields that form the index key.
        fields: Vec<Ident>,
        /// Optional index type (`type:` argument). `None` -> let the DBMS choose.
        index_type: Option<Ident>,
        /// Optional logical name (`name:` argument).
        name: Option<String>,
        /// Optional physical DB name (`map:` argument).
        map: Option<String>,
    },
    /// @@check(bool_expr) — table-level CHECK constraint.
    Check {
        /// Parsed boolean expression (e.g. `start_date < end_date`).
        expr: crate::bool_expr::BoolExpr,
        /// Span of the entire `@@check(...)` attribute.
        span: Span,
    },
}

/// An enum block declaration.
///
/// # Example
///
/// ```prisma
/// enum Role {
///   USER
///   ADMIN
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct EnumDecl {
    /// The enum name (e.g., "Role").
    pub name: Ident,
    /// Enum variants.
    pub variants: Vec<EnumVariant>,
    /// Span covering the entire enum block.
    pub span: Span,
}

/// A composite type block declaration.
///
/// Composite types define named struct-like types that can be embedded in models.
/// On PostgreSQL they map to native composite types; on MySQL/SQLite they are
/// serialised to JSON (`@store(Json)` is required on the model field).
///
/// # Example
///
/// ```prisma
/// type Address {
///   street String
///   city   String
///   zip    String
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct TypeDecl {
    /// The type name (e.g., "Address").
    pub name: Ident,
    /// Field declarations (scalars, enums, and arrays — no relations).
    pub fields: Vec<FieldDecl>,
    /// Span covering the entire type block.
    pub span: Span,
}

impl TypeDecl {
    /// Finds a field by name.
    pub fn find_field(&self, name: &str) -> Option<&FieldDecl> {
        self.fields.iter().find(|f| f.name.value == name)
    }
}

/// An enum variant.
#[derive(Debug, Clone, PartialEq)]
pub struct EnumVariant {
    /// The variant name.
    pub name: Ident,
    /// Span covering the variant.
    pub span: Span,
}

/// An identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident {
    /// The identifier value.
    pub value: String,
    /// Span of the identifier.
    pub span: Span,
}

impl Ident {
    /// Creates a new identifier.
    pub fn new(value: String, span: Span) -> Self {
        Self { value, span }
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

/// An expression (used in attribute arguments).
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A literal value.
    Literal(Literal),
    /// A function call: `name(arg1, arg2, ...)`.
    FunctionCall {
        /// Function name.
        name: Ident,
        /// Arguments.
        args: Vec<Expr>,
        /// Span of the entire call.
        span: Span,
    },
    /// An array: `[item1, item2, ...]`.
    Array {
        /// Array elements.
        elements: Vec<Expr>,
        /// Span of the entire array.
        span: Span,
    },
    /// A named argument: `name: value`.
    NamedArg {
        /// Argument name.
        name: Ident,
        /// Argument value.
        value: Box<Expr>,
        /// Span of the entire named argument.
        span: Span,
    },
    /// An identifier reference.
    Ident(Ident),
}

impl Expr {
    /// Returns the span of this expression.
    pub fn span(&self) -> Span {
        match self {
            Expr::Literal(lit) => lit.span(),
            Expr::FunctionCall { span, .. } => *span,
            Expr::Array { span, .. } => *span,
            Expr::NamedArg { span, .. } => *span,
            Expr::Ident(ident) => ident.span,
        }
    }
}

/// A literal value.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// String literal.
    String(String, Span),
    /// Number literal (stored as string, can be int or float).
    Number(String, Span),
    /// Boolean literal.
    Boolean(bool, Span),
}

impl Literal {
    /// Returns the span of this literal.
    pub fn span(&self) -> Span {
        match self {
            Literal::String(_, span) => *span,
            Literal::Number(_, span) => *span,
            Literal::Boolean(_, span) => *span,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_modifier() {
        assert_eq!(FieldModifier::None, FieldModifier::None);
        assert_ne!(FieldModifier::Optional, FieldModifier::Array);
    }

    #[test]
    fn test_field_type_display() {
        assert_eq!(FieldType::String.to_string(), "String");
        assert_eq!(FieldType::Int.to_string(), "Int");
        assert_eq!(
            FieldType::Decimal {
                precision: 10,
                scale: 2
            }
            .to_string(),
            "Decimal(10, 2)"
        );
    }

    #[test]
    fn test_ident() {
        let ident = Ident::new("test".to_string(), Span::new(0, 4));
        assert_eq!(ident.value, "test");
        assert_eq!(ident.to_string(), "test");
    }

    #[test]
    fn test_referential_action_display() {
        assert_eq!(ReferentialAction::Cascade.to_string(), "Cascade");
        assert_eq!(ReferentialAction::SetNull.to_string(), "SetNull");
    }

    #[test]
    fn test_model_table_name() {
        let model = ModelDecl {
            name: Ident::new("User".to_string(), Span::new(0, 4)),
            fields: vec![],
            attributes: vec![ModelAttribute::Map("users".to_string())],
            span: Span::new(0, 10),
        };
        assert_eq!(model.table_name(), "users");
    }

    #[test]
    fn test_model_table_name_default() {
        let model = ModelDecl {
            name: Ident::new("User".to_string(), Span::new(0, 4)),
            fields: vec![],
            attributes: vec![],
            span: Span::new(0, 10),
        };
        assert_eq!(model.table_name(), "User");
    }

    #[test]
    fn test_field_column_name() {
        let field = FieldDecl {
            name: Ident::new("userId".to_string(), Span::new(0, 6)),
            field_type: FieldType::Int,
            modifier: FieldModifier::None,
            attributes: vec![FieldAttribute::Map("user_id".to_string())],
            span: Span::new(0, 20),
        };
        assert_eq!(field.column_name(), "user_id");
    }

    #[test]
    fn test_schema_helpers() {
        let schema = Schema {
            declarations: vec![
                Declaration::Model(ModelDecl {
                    name: Ident::new("User".to_string(), Span::new(0, 4)),
                    fields: vec![],
                    attributes: vec![],
                    span: Span::new(0, 10),
                }),
                Declaration::Enum(EnumDecl {
                    name: Ident::new("Role".to_string(), Span::new(0, 4)),
                    variants: vec![],
                    span: Span::new(0, 10),
                }),
            ],
            span: Span::new(0, 100),
        };

        assert_eq!(schema.models().count(), 1);
        assert_eq!(schema.enums().count(), 1);
        assert!(schema.datasource().is_none());
    }
}
