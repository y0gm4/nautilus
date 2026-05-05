//! `ColumnMarker` — lightweight column identifier with borrowed fast paths.

use std::borrow::Cow;

pub(crate) fn build_column_alias(table: &str, name: &str) -> String {
    let mut alias = String::with_capacity(table.len() + name.len() + 2);
    alias.push_str(table);
    alias.push_str("__");
    alias.push_str(name);
    alias
}

/// A lightweight column identifier for use in selection descriptors.
///
/// Stores either borrowed static metadata or owned runtime strings so callers
/// can avoid allocations for generated/schema-known columns without giving up
/// support for dynamic names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMarker {
    /// Table name.
    pub table: Cow<'static, str>,
    /// Column name.
    pub name: Cow<'static, str>,
}

impl ColumnMarker {
    /// Create a new column marker.
    ///
    /// Accepts any type that implements `Into<String>`, so both
    /// `&str` literals and owned `String` values work without ceremony.
    pub fn new(table: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            table: Cow::Owned(table.into()),
            name: Cow::Owned(name.into()),
        }
    }

    /// Create a new marker backed by borrowed static metadata.
    pub const fn from_static(table: &'static str, name: &'static str) -> Self {
        Self {
            table: Cow::Borrowed(table),
            name: Cow::Borrowed(name),
        }
    }

    /// Returns the join-safe alias for this column.
    ///
    /// The alias uses the format "table__column" which is safe to use
    /// in queries with joins, preventing column name conflicts.
    ///
    /// # Example
    ///
    /// ```
    /// use nautilus_core::ColumnMarker;
    ///
    /// let marker = ColumnMarker::new("users", "id");
    /// assert_eq!(marker.alias(), "users__id");
    /// ```
    pub fn alias(&self) -> String {
        build_column_alias(&self.table, &self.name)
    }
}
