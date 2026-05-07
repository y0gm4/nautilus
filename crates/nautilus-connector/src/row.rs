//! Row representation for database results.

use nautilus_core::{RowAccess, Value};
use rustc_hash::FxHasher;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::OnceLock;

const LINEAR_SCAN_LOOKUP_THRESHOLD: usize = 8;
const INLINE_ROW_COLUMN_CAPACITY: usize = 8;

type NameIndexMap = HashMap<u64, NameIndexEntry, BuildHasherDefault<U64IdentityHasher>>;
type RowColumns = SmallVec<[(String, Value); INLINE_ROW_COLUMN_CAPACITY]>;

#[derive(Debug)]
enum NameIndexEntry {
    Single(usize),
    Multiple(Vec<usize>),
}

#[derive(Debug)]
struct RowNameIndex {
    entries: NameIndexMap,
}

#[derive(Default)]
struct U64IdentityHasher(u64);

impl Hasher for U64IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hasher = FxHasher::default();
        hasher.write(bytes);
        self.0 = hasher.finish();
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

impl RowNameIndex {
    fn new(columns: &[(String, Value)]) -> Self {
        let mut entries =
            NameIndexMap::with_capacity_and_hasher(columns.len(), BuildHasherDefault::default());

        for (idx, (name, _)) in columns.iter().enumerate() {
            let hash = hash_column_name(name);
            match entries.entry(hash) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(NameIndexEntry::Single(idx));
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => match entry.get_mut() {
                    NameIndexEntry::Single(first_idx) => {
                        let existing = *first_idx;
                        entry.insert(NameIndexEntry::Multiple(vec![existing, idx]));
                    }
                    NameIndexEntry::Multiple(indices) => indices.push(idx),
                },
            }
        }

        Self { entries }
    }

    fn find(&self, columns: &[(String, Value)], name: &str) -> Option<usize> {
        self.find_hashed(columns, hash_column_name(name), name)
    }

    fn find_hashed(&self, columns: &[(String, Value)], hash: u64, name: &str) -> Option<usize> {
        let entry = self.entries.get(&hash)?;
        match entry {
            NameIndexEntry::Single(idx) => {
                let (column_name, _) = columns.get(*idx)?;
                (column_name == name).then_some(*idx)
            }
            NameIndexEntry::Multiple(indices) => indices.iter().copied().find(|idx| {
                columns
                    .get(*idx)
                    .is_some_and(|(column_name, _)| column_name == name)
            }),
        }
    }
}

/// Hash a column name with `rustc-hash`'s lightweight `FxHasher`.
fn hash_column_name(name: &str) -> u64 {
    let mut hasher = FxHasher::default();
    hasher.write(name.as_bytes());
    hasher.finish()
}

/// A database row with hybrid access patterns.
///
/// Stores columns as `Vec<(String, Value)>` to preserve order and allow duplicates.
/// Small rows use a linear scan to avoid index-allocation overhead; wider rows
/// lazily build a compact name-to-index map on first `get()` call.
///
/// ## Duplicate Column Policy
///
/// If multiple columns have the same name, `get(name)` returns the first occurrence.
#[derive(Debug)]
pub struct Row {
    columns: RowColumns,
    index: OnceLock<RowNameIndex>,
}

impl Row {
    /// Create a new row from column-value pairs.
    pub fn new(columns: Vec<(String, Value)>) -> Self {
        Self {
            columns: SmallVec::from_vec(columns),
            index: OnceLock::new(),
        }
    }

    /// Create an empty row with enough capacity for the expected column count.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            columns: SmallVec::with_capacity(capacity),
            index: OnceLock::new(),
        }
    }

    /// Append a column while constructing or reshaping a row.
    ///
    /// This invalidates the lazy name index so subsequent lookups stay correct.
    pub fn push_column(&mut self, name: String, value: Value) {
        self.columns.push((name, value));
        self.index = OnceLock::new();
    }

    /// Get a value by column position (0-indexed).
    pub fn get_by_pos(&self, idx: usize) -> Option<&Value> {
        self.columns.get(idx).map(|(_, v)| v)
    }

    /// Get a value by column name.
    ///
    /// Narrow rows use a direct scan. Wider rows lazily build an index on the
    /// first lookup. If duplicate columns exist, returns the first occurrence.
    pub fn get(&self, name: &str) -> Option<&Value> {
        if self.columns.len() <= LINEAR_SCAN_LOOKUP_THRESHOLD {
            return self
                .columns
                .iter()
                .find(|(column_name, _)| column_name == name)
                .map(|(_, value)| value);
        }

        let index = self.index.get_or_init(|| RowNameIndex::new(&self.columns));
        index
            .find(&self.columns, name)
            .and_then(|idx| self.get_by_pos(idx))
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

    /// Consume the row and iterate over owned `(name, value)` pairs without
    /// forcing the internal storage back into a `Vec`.
    pub fn into_columns_iter(self) -> impl Iterator<Item = (String, Value)> {
        self.columns.into_iter()
    }

    /// Consume the row and return the owned columns.
    pub fn into_columns(self) -> Vec<(String, Value)> {
        self.columns.into_vec()
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

    #[test]
    fn test_row_wide_named_access_uses_index_without_cloning_names() {
        let mut columns = Vec::new();
        for idx in 0..=LINEAR_SCAN_LOOKUP_THRESHOLD {
            columns.push((format!("col_{idx}"), Value::I64(idx as i64)));
        }
        let row = Row::new(columns);

        assert_eq!(
            row.get(&format!("col_{}", LINEAR_SCAN_LOOKUP_THRESHOLD)),
            Some(&Value::I64(LINEAR_SCAN_LOOKUP_THRESHOLD as i64))
        );
        assert_eq!(row.get("missing"), None);
    }

    #[test]
    fn test_row_name_index_disambiguates_colliding_candidates() {
        let columns = vec![
            ("first".to_string(), Value::I64(1)),
            ("second".to_string(), Value::I64(2)),
        ];
        let mut entries = NameIndexMap::with_capacity_and_hasher(1, BuildHasherDefault::default());
        entries.insert(42, NameIndexEntry::Multiple(vec![0, 1]));
        let index = RowNameIndex { entries };

        assert_eq!(index.find_hashed(&columns, 42, "first"), Some(0));
        assert_eq!(index.find_hashed(&columns, 42, "second"), Some(1));
        assert_eq!(index.find_hashed(&columns, 42, "third"), None);
    }
}
