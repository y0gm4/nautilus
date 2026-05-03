//! Row representation for database results.

use nautilus_core::{RowAccess, Value};
use std::collections::HashMap;
use std::sync::OnceLock;

/// A database row with hybrid access patterns.
///
/// Stores columns as `Vec<(String, Value)>` to preserve order and allow duplicates.
/// Lazily builds a name-to-index map on first `get()` call for efficient lookup.
///
/// ## Duplicate Column Policy
///
/// If multiple columns have the same name, `get(name)` returns the first occurrence.
#[derive(Debug)]
pub struct Row {
    columns: Vec<(String, Value)>,
    index: OnceLock<HashMap<String, usize>>,
}

impl Row {
    /// Create a new row from column-value pairs.
    pub fn new(columns: Vec<(String, Value)>) -> Self {
        Self {
            columns,
            index: OnceLock::new(),
        }
    }

    /// Get a value by column position (0-indexed).
    pub fn get_by_pos(&self, idx: usize) -> Option<&Value> {
        self.columns.get(idx).map(|(_, v)| v)
    }

    /// Get a value by column name.
    ///
    /// Lazily builds an index on first call. If duplicate columns exist,
    /// returns the first occurrence.
    pub fn get(&self, name: &str) -> Option<&Value> {
        let index = self.index.get_or_init(|| {
            let mut map = HashMap::new();
            for (i, (col_name, _)) in self.columns.iter().enumerate() {
                map.entry(col_name.clone()).or_insert(i);
            }
            map
        });
        index.get(name).and_then(|&idx| self.get_by_pos(idx))
    }

    /// Get the column name at the given position.
    pub fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns.get(idx).map(|(name, _)| name.as_str())
    }

    /// Iterate over all columns as `(name, value)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.columns.iter().map(|(name, val)| (name.as_str(), val))
    }

    /// Return the number of columns.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Check if the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Get all columns as a slice.
    pub fn columns(&self) -> &[(String, Value)] {
        &self.columns
    }

    /// Consume the row and return the owned columns.
    pub fn into_columns(self) -> Vec<(String, Value)> {
        self.columns
    }
}

/// Implement RowAccess trait for the owned Row type.
///
/// This allows Row to be used with the GAT-based executor interface
/// while maintaining backward compatibility with existing code.
impl<'row> RowAccess<'row> for Row {
    fn get(&'row self, name: &str) -> Option<&'row Value> {
        Row::get(self, name)
    }

    fn get_by_pos(&'row self, idx: usize) -> Option<&'row Value> {
        Row::get_by_pos(self, idx)
    }

    fn column_name(&'row self, idx: usize) -> Option<&'row str> {
        Row::column_name(self, idx)
    }

    fn len(&self) -> usize {
        Row::len(self)
    }

    fn is_empty(&self) -> bool {
        Row::is_empty(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_core::Value;

    #[test]
    fn test_row_positional_access() {
        let row = Row::new(vec![
            ("id".to_string(), Value::I64(1)),
            ("name".to_string(), Value::String("Alice".to_string())),
        ]);

        assert_eq!(row.get_by_pos(0), Some(&Value::I64(1)));
        assert_eq!(row.get_by_pos(1), Some(&Value::String("Alice".to_string())));
        assert_eq!(row.get_by_pos(2), None);
    }

    #[test]
    fn test_row_named_access() {
        let row = Row::new(vec![
            ("id".to_string(), Value::I64(1)),
            ("name".to_string(), Value::String("Alice".to_string())),
        ]);

        assert_eq!(row.get("id"), Some(&Value::I64(1)));
        assert_eq!(row.get("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(row.get("age"), None);
    }

    #[test]
    fn test_row_duplicate_columns() {
        let row = Row::new(vec![
            ("id".to_string(), Value::I64(1)),
            ("id".to_string(), Value::I64(2)),
            ("name".to_string(), Value::String("Alice".to_string())),
        ]);

        assert_eq!(row.get("id"), Some(&Value::I64(1)));
        assert_eq!(row.get_by_pos(0), Some(&Value::I64(1)));
        assert_eq!(row.get_by_pos(1), Some(&Value::I64(2)));
    }

    #[test]
    fn test_row_iterator() {
        let row = Row::new(vec![
            ("id".to_string(), Value::I64(1)),
            ("name".to_string(), Value::String("Alice".to_string())),
        ]);

        let items: Vec<_> = row.iter().collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], ("id", &Value::I64(1)));
        assert_eq!(items[1], ("name", &Value::String("Alice".to_string())));
    }

    #[test]
    fn test_row_empty() {
        let row = Row::new(vec![]);
        assert!(row.is_empty());
        assert_eq!(row.len(), 0);
        assert_eq!(row.get_by_pos(0), None);
        assert_eq!(row.get("any"), None);
    }

    #[test]
    fn test_row_column_name() {
        let row = Row::new(vec![
            ("id".to_string(), Value::I64(1)),
            ("name".to_string(), Value::String("Alice".to_string())),
        ]);

        assert_eq!(row.column_name(0), Some("id"));
        assert_eq!(row.column_name(1), Some("name"));
        assert_eq!(row.column_name(2), None);
    }

    #[test]
    fn test_row_columns_slice() {
        let row = Row::new(vec![
            ("x".to_string(), Value::I64(10)),
            ("y".to_string(), Value::Bool(false)),
        ]);

        let cols = row.columns();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].0, "x");
        assert_eq!(cols[0].1, Value::I64(10));
        assert_eq!(cols[1].0, "y");
        assert_eq!(cols[1].1, Value::Bool(false));
    }
}
