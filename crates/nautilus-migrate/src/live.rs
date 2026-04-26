//! Live-schema snapshot — the DB state as seen at inspection time.

use std::collections::HashMap;

use nautilus_schema::ir::{BasicIndexType, IndexKind, PgvectorIndex};
pub use nautilus_schema::ComputedKind;

/// A snapshot of the tables currently present in the live database.
#[derive(Debug, Clone, Default)]
pub struct LiveSchema {
    /// Keyed on the *DB* table name (as returned by the database).
    pub tables: HashMap<String, LiveTable>,
    /// PostgreSQL enum types present in the live database.
    ///
    /// Keyed on the *DB* type name (lower-case), value is the ordered list of
    /// variant labels as stored in `pg_enum`. Empty for non-Postgres providers.
    pub enums: HashMap<String, Vec<String>>,
    /// PostgreSQL composite types present in the live database.
    ///
    /// Keyed on the *DB* type name (lower-case). Empty for non-Postgres providers.
    pub composite_types: HashMap<String, LiveCompositeType>,
    /// PostgreSQL extensions currently installed in the live database.
    ///
    /// Keyed on the extension name (lower-case). The built-in `plpgsql`
    /// extension is excluded because it is present in every cluster by default
    /// and not something users declare. Empty for non-Postgres providers.
    pub extensions: HashMap<String, LiveExtension>,
}

/// State of a single PostgreSQL extension as installed in the live database.
///
/// Populated from `pg_extension` joined with `pg_namespace`. We carry both the
/// version (from `extversion`) and the containing schema name (from
/// `extnamespace -> nspname`) so `db pull` can round-trip structured
/// declarations and the diff can reason about schema placement in future
/// iterations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveExtension {
    /// Installed version string (`pg_extension.extversion`).
    pub version: String,
    /// Schema in which the extension is installed
    /// (`pg_namespace.nspname` for `pg_extension.extnamespace`).
    pub schema: String,
}

/// A composite type in the live database.
#[derive(Debug, Clone)]
pub struct LiveCompositeType {
    /// DB type name (lower-case).
    pub name: String,
    /// Fields in declaration order.
    pub fields: Vec<LiveCompositeField>,
}

/// A single field within a live composite type.
#[derive(Debug, Clone)]
pub struct LiveCompositeField {
    /// DB field name.
    pub name: String,
    /// Canonical SQL type, lower-cased and normalised.
    pub col_type: String,
}

/// A single table in the live database.
#[derive(Debug, Clone)]
pub struct LiveTable {
    /// DB table name.
    pub name: String,
    /// Columns in declaration order.
    pub columns: Vec<LiveColumn>,
    /// Primary-key column names (DB names), in key order.
    pub primary_key: Vec<String>,
    /// Non-PK indexes.
    pub indexes: Vec<LiveIndex>,
    /// Table-level CHECK constraint expressions (normalised, lower-cased).
    pub check_constraints: Vec<String>,
    /// Foreign-key constraints on this table.
    pub foreign_keys: Vec<LiveForeignKey>,
}

/// A foreign-key constraint on a live table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveForeignKey {
    /// Constraint name as stored in the database.
    pub constraint_name: String,
    /// Local column names (FK side), in constraint key order.
    pub columns: Vec<String>,
    /// Referenced table name.
    pub referenced_table: String,
    /// Referenced column names, in constraint key order.
    pub referenced_columns: Vec<String>,
    /// ON DELETE action, upper-cased (e.g. `"CASCADE"`, `"SET NULL"`).
    /// `None` means the database default (NO ACTION).
    pub on_delete: Option<String>,
    /// ON UPDATE action, upper-cased (e.g. `"CASCADE"`, `"SET NULL"`).
    /// `None` means the database default (NO ACTION).
    pub on_update: Option<String>,
}

/// A single column in a live table.
#[derive(Debug, Clone)]
pub struct LiveColumn {
    /// DB column name.
    pub name: String,
    /// Canonical SQL type, lower-cased and normalised (e.g. `"text"`, `"integer"`,
    /// `"double precision"`, `"decimal(10, 2)"`).
    pub col_type: String,
    /// `true` if the column allows NULL.
    pub nullable: bool,
    /// Raw DEFAULT expression as returned by the database, lower-cased.
    pub default_value: Option<String>,
    /// Generation expression for computed/generated columns, lower-cased.
    /// `None` for regular (non-generated) columns.
    pub generated_expr: Option<String>,
    /// Storage kind for computed columns. Always `Some` when `generated_expr` is `Some`.
    pub computed_kind: Option<ComputedKind>,
    /// Column-level CHECK constraint expression, lower-cased.
    /// `None` for unconstrained columns.
    pub check_expr: Option<String>,
}

/// A non-PK index on a live table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveIndex {
    /// Physical index name as it exists in the database.
    pub name: String,
    /// DB column names that make up the index key, in index order.
    pub columns: Vec<String>,
    /// Whether the index enforces uniqueness.
    pub unique: bool,
    /// Resolved access method + extension payload.
    pub kind: LiveIndexKind,
}

/// Resolved access method for a live index.
///
/// Mirrors [`nautilus_schema::ir::IndexKind`] but admits an `Unknown(...)`
/// arm for cases where the inspector cannot map the database-reported access
/// method onto a known [`BasicIndexType`] (e.g. SQLite, which does not
/// expose access methods, or a Postgres method we don't handle yet).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveIndexKind {
    /// The provider did not report a recognised access method.
    /// The wrapped string carries the raw `am.amname` (when present) for
    /// diagnostics and round-tripping.
    Unknown(Option<String>),
    /// A built-in access method (`btree`, `hash`, ...).
    Basic(BasicIndexType),
    /// A pgvector index. The payload contains the method, opclass, and
    /// `WITH (...)` parameters parsed from the database.
    Pgvector(PgvectorIndex),
}

impl LiveIndexKind {
    /// Returns the raw access-method string the database reported, if any.
    /// Used for stable diagnostics.
    pub fn method_str(&self) -> Option<&str> {
        match self {
            LiveIndexKind::Unknown(s) => s.as_deref(),
            LiveIndexKind::Basic(b) => Some(b.as_ddl_str()),
            LiveIndexKind::Pgvector(p) => Some(p.method.as_ddl_str()),
        }
    }

    /// Returns `Some(&PgvectorIndex)` when this is a pgvector index.
    pub fn pgvector(&self) -> Option<&PgvectorIndex> {
        match self {
            LiveIndexKind::Pgvector(p) => Some(p),
            _ => None,
        }
    }

    /// Lossy conversion to a target-side [`IndexKind`].
    ///
    /// `Unknown(...)` collapses to [`IndexKind::Default`] because the
    /// information needed to round-trip an unrecognised method (the raw
    /// access-method string) is not part of the target IR vocabulary.
    pub fn to_index_kind(&self) -> IndexKind {
        match self {
            LiveIndexKind::Unknown(_) => IndexKind::Default,
            LiveIndexKind::Basic(b) => IndexKind::Basic(*b),
            LiveIndexKind::Pgvector(p) => IndexKind::Pgvector(p.clone()),
        }
    }
}
