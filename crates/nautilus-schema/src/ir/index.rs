//! Index access methods, both built-in and provider-extension flavours.
//!
//! [`IndexIr`] carries an [`IndexKind`] tag instead of a flat bag of optional
//! pgvector-specific fields. Each extension family lives in its own submodule

pub mod pgvector;

pub use pgvector::{PgvectorIndex, PgvectorIndexOptions, PgvectorMethod, PgvectorOpClass};

use crate::ir::DatabaseProvider;
use std::fmt;
use std::str::FromStr;

/// Index metadata.
///
/// The access method (`BTree`, `Hnsw`, ...) and any provider-specific payload
/// live inside [`Self::kind`]. The remaining fields are pure metadata
/// (logical/physical names, indexed columns) shared by every kind.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexIr {
    /// Field names (logical) that form the index.
    pub fields: Vec<String>,
    /// Access method + extension payload. See [`IndexKind`].
    pub kind: IndexKind,
    /// Logical name — for developer reference only.
    pub name: Option<String>,
    /// Physical DDL name. When set this is used as the `CREATE INDEX` name
    /// instead of the auto-generated `idx_{table}_{cols}` name.
    pub map: Option<String>,
}

/// Tagged union of every supported index access method.
///
/// `Default` means "let the DBMS decide" (BTree on every supported provider).
/// `Basic(...)` covers the access methods that are part of the core SQL or
/// shipped by every supported DBMS. Each provider-extension family gets its
/// own variant carrying a strongly-typed payload, so options that only apply
/// to one extension can never be set on an index of another kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexKind {
    /// No `type:` argument was given — let the database pick.
    Default,
    /// A non-extension index method (`BTree`, `Hash`, `Gin`, ...).
    Basic(BasicIndexType),
    /// A pgvector index (HNSW or IVFFlat) with its operator class and
    /// build-time `WITH (...)` parameters.
    Pgvector(PgvectorIndex),
    // Future: PostGIS(PostGisIndex), FullTextRanked(...), ...
}

impl IndexKind {
    /// Returns `true` when this kind is supported by the given database.
    pub fn supported_by(&self, provider: DatabaseProvider) -> bool {
        match self {
            IndexKind::Default => true,
            IndexKind::Basic(b) => b.supported_by(provider),
            IndexKind::Pgvector(_) => provider == DatabaseProvider::Postgres,
        }
    }

    /// Human-readable list of supported providers (for diagnostics).
    pub fn supported_providers(&self) -> &'static str {
        match self {
            IndexKind::Default => "all databases",
            IndexKind::Basic(b) => b.supported_providers(),
            IndexKind::Pgvector(_) => "PostgreSQL only",
        }
    }

    /// The schema-level type name (`"BTree"`, `"Hnsw"`, ...). `None` when
    /// the kind is [`IndexKind::Default`].
    pub fn as_type_str(&self) -> Option<&'static str> {
        match self {
            IndexKind::Default => None,
            IndexKind::Basic(b) => Some(b.as_str()),
            IndexKind::Pgvector(p) => Some(p.method.as_str()),
        }
    }

    /// The DDL-level access method (`"btree"`, `"hash"`, `"hnsw"`, ...).
    /// `None` for [`IndexKind::Default`] — callers should leave the
    /// `USING ...` clause out entirely when this returns `None`.
    pub fn as_ddl_method(&self) -> Option<&'static str> {
        match self {
            IndexKind::Default => None,
            IndexKind::Basic(b) => Some(b.as_ddl_str()),
            IndexKind::Pgvector(p) => Some(p.method.as_ddl_str()),
        }
    }

    /// Returns `Some(&PgvectorIndex)` when this kind carries pgvector data.
    pub fn pgvector(&self) -> Option<&PgvectorIndex> {
        match self {
            IndexKind::Pgvector(p) => Some(p),
            _ => None,
        }
    }

    /// Returns `true` when this is a pgvector index.
    pub fn is_pgvector(&self) -> bool {
        matches!(self, IndexKind::Pgvector(_))
    }
}

/// Built-in (non-extension) index access methods.
///
/// Methods that require a database extension live in their own
/// [`IndexKind`] variant ([`IndexKind::Pgvector`]) instead of being mixed
/// into this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasicIndexType {
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

/// Error returned when parsing an unknown basic index type string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseBasicIndexTypeError;

impl fmt::Display for ParseBasicIndexTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unknown index type")
    }
}

impl std::error::Error for ParseBasicIndexTypeError {}

impl FromStr for BasicIndexType {
    type Err = ParseBasicIndexTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "btree" => Ok(BasicIndexType::BTree),
            "hash" => Ok(BasicIndexType::Hash),
            "gin" => Ok(BasicIndexType::Gin),
            "gist" => Ok(BasicIndexType::Gist),
            "brin" => Ok(BasicIndexType::Brin),
            "fulltext" => Ok(BasicIndexType::FullText),
            _ => Err(ParseBasicIndexTypeError),
        }
    }
}

impl BasicIndexType {
    /// Returns `true` when this index type is supported by the given database
    /// provider.
    pub fn supported_by(self, provider: DatabaseProvider) -> bool {
        match self {
            BasicIndexType::BTree => true,
            BasicIndexType::Hash => matches!(
                provider,
                DatabaseProvider::Postgres | DatabaseProvider::Mysql
            ),
            BasicIndexType::Gin | BasicIndexType::Gist | BasicIndexType::Brin => {
                provider == DatabaseProvider::Postgres
            }
            BasicIndexType::FullText => provider == DatabaseProvider::Mysql,
        }
    }

    /// Human-readable list of supported providers (for diagnostics).
    pub fn supported_providers(self) -> &'static str {
        match self {
            BasicIndexType::BTree => "all databases",
            BasicIndexType::Hash => "PostgreSQL and MySQL",
            BasicIndexType::Gin => "PostgreSQL only",
            BasicIndexType::Gist => "PostgreSQL only",
            BasicIndexType::Brin => "PostgreSQL only",
            BasicIndexType::FullText => "MySQL only",
        }
    }

    /// Canonical display name used in schema files.
    pub fn as_str(self) -> &'static str {
        match self {
            BasicIndexType::BTree => "BTree",
            BasicIndexType::Hash => "Hash",
            BasicIndexType::Gin => "Gin",
            BasicIndexType::Gist => "Gist",
            BasicIndexType::Brin => "Brin",
            BasicIndexType::FullText => "FullText",
        }
    }

    /// Lower-case form used in DDL `USING <method>` clauses.
    pub fn as_ddl_str(self) -> &'static str {
        match self {
            BasicIndexType::BTree => "btree",
            BasicIndexType::Hash => "hash",
            BasicIndexType::Gin => "gin",
            BasicIndexType::Gist => "gist",
            BasicIndexType::Brin => "brin",
            BasicIndexType::FullText => "fulltext",
        }
    }
}

/// Every valid value for the `type:` argument of `@@index(...)`.
///
/// Used by the validator to produce a stable error message and by the
/// language-server completion provider.
pub const ALL_INDEX_TYPE_NAMES: &[&str] = &[
    "BTree", "Hash", "Gin", "Gist", "Brin", "Hnsw", "Ivfflat", "FullText",
];

/// Parses the `type:` argument of `@@index(...)` into an [`IndexKind`] tag
/// **without** any payload.
///
/// The validator uses this to identify the intended access method up-front;
/// it then attaches the appropriate payload (e.g. [`PgvectorIndex`]) and
/// returns the final [`IndexKind`]. Returns `None` when the string is not a
/// known type name — callers should emit a "unknown index type" diagnostic.
pub fn parse_index_type_tag(s: &str) -> Option<IndexTypeTag> {
    if let Ok(method) = s.parse::<PgvectorMethod>() {
        return Some(IndexTypeTag::Pgvector(method));
    }
    if let Ok(basic) = s.parse::<BasicIndexType>() {
        return Some(IndexTypeTag::Basic(basic));
    }
    None
}

/// Lightweight tag for the `type:` keyword, before payload assembly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexTypeTag {
    /// Built-in method (no payload).
    Basic(BasicIndexType),
    /// pgvector method — payload is filled in from the other `@@index(...)`
    /// arguments by the validator.
    Pgvector(PgvectorMethod),
}
