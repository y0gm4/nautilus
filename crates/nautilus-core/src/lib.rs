//! Core query AST and type system for Nautilus ORM.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

/// Structured argument objects for the query API.
pub mod args;
/// Typed column references, markers, and selection traits.
pub mod column;
/// Cursor predicate builder for stable pagination.
pub mod cursor;
/// DELETE query AST and builder.
pub mod delete;
/// Error types and result alias.
pub mod error;
/// Expression AST for filters and WHERE clauses.
pub mod expr;
/// INSERT query AST and builder.
pub mod insert;
/// Helpers for converting typed Rust query args into engine wire JSON.
pub mod protocol_json;
/// SELECT query AST and builder.
pub mod select;
/// UPDATE query AST and builder.
pub mod update;
/// Database value representation.
pub mod value;

pub use args::{FindManyArgs, FindUniqueArgs, IncludeRelation, VectorMetric, VectorNearest};
pub use column::{Column, ColumnMarker, FromValue, RowAccess, SelectColumns};
pub use cursor::build_cursor_predicate;
pub use delete::{Delete, DeleteBuilder, DeleteCapacity};
pub use error::{Error, Result};
pub use expr::{BinaryOp, Expr};
pub use insert::{Insert, InsertBuilder, InsertCapacity};
pub use protocol_json::{
    find_many_args_to_protocol_json, find_many_args_to_protocol_object, where_expr_to_protocol_json,
};
pub use select::{
    JoinClause, JoinType, OrderBy, OrderByItem, OrderDir, Select, SelectBuilder, SelectCapacity,
    SelectItem,
};
pub use update::{Update, UpdateBuilder, UpdateCapacity};
pub use value::{Geography, Geometry, Value};
