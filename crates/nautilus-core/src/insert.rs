//! INSERT query AST and builder.

use crate::column::ColumnMarker;
use crate::error::{Error, Result};
use crate::value::Value;

/// INSERT query AST node.
#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    /// Table name.
    pub table: String,
    /// Columns to insert into.
    pub columns: Vec<ColumnMarker>,
    /// Rows of values to insert (each inner Vec is one row).
    pub values: Vec<Vec<Value>>,
    /// Columns to return (RETURNING clause). Empty = no RETURNING.
    pub returning: Vec<ColumnMarker>,
}

impl Insert {
    /// Creates a new INSERT query builder for the given table.
    pub fn into_table(table: impl Into<String>) -> InsertBuilder {
        InsertBuilder {
            table: table.into(),
            columns: Vec::new(),
            values: Vec::new(),
            returning: Vec::new(),
        }
    }
}

/// Reserved capacities for the `Vec`s maintained by an [`InsertBuilder`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InsertCapacity {
    /// Expected number of insert columns.
    pub columns: usize,
    /// Expected number of rows to insert.
    pub rows: usize,
    /// Expected number of `RETURNING` columns.
    pub returning: usize,
}

/// Builder for INSERT queries.
#[derive(Debug, Clone)]
pub struct InsertBuilder {
    table: String,
    columns: Vec<ColumnMarker>,
    values: Vec<Vec<Value>>,
    returning: Vec<ColumnMarker>,
}

impl InsertBuilder {
    /// Reserve capacity for the builder's internal vectors.
    #[must_use]
    pub fn with_capacity(mut self, capacity: InsertCapacity) -> Self {
        self.columns.reserve(capacity.columns);
        self.values.reserve(capacity.rows);
        self.returning.reserve(capacity.returning);
        self
    }

    /// Sets the columns to insert into.
    #[must_use]
    pub fn columns(mut self, columns: Vec<ColumnMarker>) -> Self {
        self.columns = columns;
        self
    }

    /// Adds a column to insert into.
    #[must_use]
    pub fn column(mut self, column: ColumnMarker) -> Self {
        self.columns.push(column);
        self
    }

    /// Adds a row of values to insert.
    ///
    /// Each call adds one row. The number of values must match
    /// the number of columns when `build()` is called.
    #[must_use]
    pub fn values(mut self, row: Vec<Value>) -> Self {
        self.values.push(row);
        self
    }

    /// Sets all rows to insert in one call.
    #[must_use]
    pub fn rows(mut self, rows: Vec<Vec<Value>>) -> Self {
        self.values = rows;
        self
    }

    /// Sets the RETURNING clause columns.
    #[must_use]
    pub fn returning(mut self, columns: Vec<ColumnMarker>) -> Self {
        self.returning = columns;
        self
    }

    /// Builds the final INSERT query.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table name is empty
    /// - No columns are specified
    /// - No rows of values are specified
    /// - Any row has a different number of values than columns
    pub fn build(self) -> Result<Insert> {
        if self.table.is_empty() {
            return Err(Error::MissingField("table".to_string()));
        }

        if self.columns.is_empty() {
            return Err(Error::MissingField("columns".to_string()));
        }

        if self.values.is_empty() {
            return Err(Error::MissingField("values".to_string()));
        }

        let col_count = self.columns.len();
        for (i, row) in self.values.iter().enumerate() {
            if row.len() != col_count {
                return Err(Error::InvalidQuery(format!(
                    "row {} has {} values but {} columns were specified",
                    i,
                    row.len(),
                    col_count
                )));
            }
        }

        Ok(Insert {
            table: self.table,
            columns: self.columns,
            values: self.values,
            returning: self.returning,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_insert() {
        let insert = Insert::into_table("users")
            .column(ColumnMarker::new("users", "email"))
            .values(vec![Value::String("alice@example.com".to_string())])
            .build()
            .unwrap();

        assert_eq!(insert.table, "users");
        assert_eq!(insert.columns.len(), 1);
        assert_eq!(insert.columns[0].name, "email");
        assert_eq!(insert.values.len(), 1);
        assert!(insert.returning.is_empty());
    }

    #[test]
    fn test_multi_column_insert() {
        let insert = Insert::into_table("users")
            .columns(vec![
                ColumnMarker::new("users", "email"),
                ColumnMarker::new("users", "name"),
            ])
            .values(vec![
                Value::String("alice@example.com".to_string()),
                Value::String("Alice".to_string()),
            ])
            .build()
            .unwrap();

        assert_eq!(insert.columns.len(), 2);
        assert_eq!(insert.values.len(), 1);
        assert_eq!(insert.values[0].len(), 2);
    }

    #[test]
    fn test_batch_insert() {
        let insert = Insert::into_table("users")
            .column(ColumnMarker::new("users", "email"))
            .values(vec![Value::String("alice@example.com".to_string())])
            .values(vec![Value::String("bob@example.com".to_string())])
            .values(vec![Value::String("charlie@example.com".to_string())])
            .build()
            .unwrap();

        assert_eq!(insert.values.len(), 3);
    }

    #[test]
    fn test_insert_with_returning() {
        let insert = Insert::into_table("users")
            .column(ColumnMarker::new("users", "email"))
            .values(vec![Value::String("alice@example.com".to_string())])
            .returning(vec![
                ColumnMarker::new("users", "id"),
                ColumnMarker::new("users", "email"),
            ])
            .build()
            .unwrap();

        assert_eq!(insert.returning.len(), 2);
        assert_eq!(insert.returning[0].name, "id");
        assert_eq!(insert.returning[1].name, "email");
    }

    #[test]
    fn test_missing_table() {
        let result = Insert::into_table("")
            .column(ColumnMarker::new("users", "email"))
            .values(vec![Value::String("test".to_string())])
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_missing_columns() {
        let result = Insert::into_table("users")
            .values(vec![Value::String("test".to_string())])
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_missing_values() {
        let result = Insert::into_table("users")
            .column(ColumnMarker::new("users", "email"))
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_mismatched_values_count() {
        let result = Insert::into_table("users")
            .columns(vec![
                ColumnMarker::new("users", "email"),
                ColumnMarker::new("users", "name"),
            ])
            .values(vec![Value::String("alice@example.com".to_string())])
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::InvalidQuery(_)));
    }
}
