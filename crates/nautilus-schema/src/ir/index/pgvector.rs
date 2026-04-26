//! pgvector-specific index payload.
//!
//! This module owns every piece of state that only makes sense inside an
//! [`IndexKind::Pgvector`] variant: the access method (`Hnsw` vs `Ivfflat`),
//! the operator class on the indexed column, and the build-time `WITH (...)`
//! parameters. Keeping it isolated lets the rest of the IR remain
//! provider-agnostic and gives future extensions (PostGIS, full-text-ranked,
//! ...) a sibling module to mirror.

use std::fmt;
use std::str::FromStr;

/// A pgvector index payload.
///
/// Carried as the inner value of [`super::IndexKind::Pgvector`].
/// Every field that is meaningful only for pgvector lives here, so the
/// surrounding [`super::IndexIr`] never has to ask "is this index actually a
/// vector index?".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgvectorIndex {
    /// Vector access method (`Hnsw` or `Ivfflat`).
    pub method: PgvectorMethod,
    /// Optional operator class applied to the indexed column.
    pub opclass: Option<PgvectorOpClass>,
    /// Build-time parameters rendered as `WITH (...)`.
    pub options: PgvectorIndexOptions,
}

/// Vector access method for a pgvector index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgvectorMethod {
    /// HNSW graph index.
    Hnsw,
    /// IVFFlat clustered index.
    Ivfflat,
}

/// Error returned when parsing an unknown pgvector method string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsePgvectorMethodError;

impl fmt::Display for ParsePgvectorMethodError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unknown pgvector method")
    }
}

impl std::error::Error for ParsePgvectorMethodError {}

impl FromStr for PgvectorMethod {
    type Err = ParsePgvectorMethodError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "hnsw" => Ok(PgvectorMethod::Hnsw),
            "ivfflat" => Ok(PgvectorMethod::Ivfflat),
            _ => Err(ParsePgvectorMethodError),
        }
    }
}

impl PgvectorMethod {
    /// Canonical schema spelling (`"Hnsw"` or `"Ivfflat"`).
    pub fn as_str(self) -> &'static str {
        match self {
            PgvectorMethod::Hnsw => "Hnsw",
            PgvectorMethod::Ivfflat => "Ivfflat",
        }
    }

    /// Lower-case form used in DDL `USING <method>` clauses.
    pub fn as_ddl_str(self) -> &'static str {
        match self {
            PgvectorMethod::Hnsw => "hnsw",
            PgvectorMethod::Ivfflat => "ivfflat",
        }
    }
}

/// pgvector operator classes supported for HNSW / IVFFlat indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgvectorOpClass {
    /// L2 / Euclidean distance (`vector_l2_ops`).
    L2Ops,
    /// Maximum inner product (`vector_ip_ops`).
    IpOps,
    /// Cosine distance (`vector_cosine_ops`).
    CosineOps,
}

/// Error returned when parsing an unknown pgvector operator class string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsePgvectorOpClassError;

impl fmt::Display for ParsePgvectorOpClassError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unknown vector index opclass")
    }
}

impl std::error::Error for ParsePgvectorOpClassError {}

impl FromStr for PgvectorOpClass {
    type Err = ParsePgvectorOpClassError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "vector_l2_ops" => Ok(Self::L2Ops),
            "vector_ip_ops" => Ok(Self::IpOps),
            "vector_cosine_ops" => Ok(Self::CosineOps),
            _ => Err(ParsePgvectorOpClassError),
        }
    }
}

impl PgvectorOpClass {
    /// Canonical schema spelling (`"vector_l2_ops"`, etc.).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::L2Ops => "vector_l2_ops",
            Self::IpOps => "vector_ip_ops",
            Self::CosineOps => "vector_cosine_ops",
        }
    }
}

/// pgvector index parameters rendered in PostgreSQL `WITH (...)`.
///
/// `m` and `ef_construction` are HNSW-only; `lists` is IVFFlat-only. The
/// validator enforces this binding by construction (the parameters that don't
/// match the chosen method are dropped before reaching this struct).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PgvectorIndexOptions {
    /// HNSW graph connectivity parameter.
    pub m: Option<u32>,
    /// HNSW build-time candidate list size.
    pub ef_construction: Option<u32>,
    /// IVFFlat inverted-list count.
    pub lists: Option<u32>,
}

impl PgvectorIndexOptions {
    /// Returns `true` when no pgvector-specific index options are set.
    pub fn is_empty(&self) -> bool {
        self.m.is_none() && self.ef_construction.is_none() && self.lists.is_none()
    }
}
