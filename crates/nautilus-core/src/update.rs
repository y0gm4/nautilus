//! UPDATE query AST and builder.

use crate::column::ColumnMarker;
use crate::error::{Error, Result};
use crate::expr::Expr;
use crate::value::Value;

/// UPDATE query AST node.
#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    /// Table name.
    pub table: String,
    /// Column-value assignments (SET clause).
    pub assignments: Vec<(ColumnMarker, Value)>,
    /// WHERE clause.
    pub filter: Option<Expr>,
    /// Columns to return (RETURNING clause). Empty = no RETURNING.
    pub returning: Vec<ColumnMarker>,
}

impl Update {
    /// Creates a new UPDATE query builder for the given table.
    pub fn table(table: impl Into<String>) -> UpdateBuilder {
        UpdateBuilder {
            table: table.into(),
            assignments: Vec::new(),
            filter: None,
            returning: Vec::new(),
        }
    }
}

/// Reserved capacities for the `Vec`s maintained by an [`UpdateBuilder`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UpdateCapacity {
    /// Expected number of column assignments.
    pub assignments: usize,
    /// Expected number of `RETURNING` columns.
    pub returning: usize,
}

/// Builder for UPDATE queries.
#[derive(Debug, Clone)]
pub struct UpdateBuilder {
    table: String,
    assignments: Vec<(ColumnMarker, Value)>,
    filter: Option<Expr>,
    returning: Vec<ColumnMarker>,
}

impl UpdateBuilder {
    /// Reserve capacity for the builder's internal vectors.
    #[must_use]
    pub fn with_capacity(mut self, capacity: UpdateCapacity) -> Self {
        self.assignments.reserve(capacity.assignments);
        self.returning.reserve(capacity.returning);
        self
    }

    /// Adds a column-value assignment to the SET clause.
    #[must_use]
    pub fn set(mut self, column: ColumnMarker, value: Value) -> Self {
        self.assignments.push((column, value));
        self
    }

    /// Sets all column-value assignments in one call.
    #[must_use]
    pub fn assignments(mut self, assignments: Vec<(ColumnMarker, Value)>) -> Self {
        self.assignments = assignments;
        self
    }

    /// Adds a WHERE clause filter.
    #[must_use]
    pub fn filter(mut self, expr: Expr) -> Self {
        self.filter = Some(expr);
        self
    }

    /// Sets the RETURNING clause columns.
    #[must_use]
    pub fn returning(mut self, columns: Vec<ColumnMarker>) -> Self {
        self.returning = columns;
        self
    }

    /// Builds the final UPDATE query.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table name is empty
    /// - No assignments are specified
    pub fn build(self) -> Result<Update> {
        if self.table.is_empty() {
            return Err(Error::MissingField("table".to_string()));
        }

        if self.assignments.is_empty() {
            return Err(Error::MissingField("assignments".to_string()));
        }

        Ok(Update {
            table: self.table,
            assignments: self.assignments,
            filter: self.filter,
            returning: self.returning,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_update() {
        let update = Update::table("users")
            .set(
                ColumnMarker::new("users", "email"),
                Value::String("new@example.com".to_string()),
            )
            .build()
            .unwrap();

        assert_eq!(update.table, "users");
        assert_eq!(update.assignments.len(), 1);
        assert_eq!(update.assignments[0].0.name, "email");
        assert!(update.filter.is_none());
        assert!(update.returning.is_empty());
    }

    #[test]
    fn test_multi_set_update() {
        let update = Update::table("users")
            .set(
                ColumnMarker::new("users", "email"),
                Value::String("new@example.com".to_string()),
            )
            .set(
                ColumnMarker::new("users", "name"),
                Value::String("Alice".to_string()),
            )
            .build()
            .unwrap();

        assert_eq!(update.assignments.len(), 2);
        assert_eq!(update.assignments[0].0.name, "email");
        assert_eq!(update.assignments[1].0.name, "name");
    }

    #[test]
    fn test_update_with_filter() {
        let update = Update::table("users")
            .set(
                ColumnMarker::new("users", "email"),
                Value::String("new@example.com".to_string()),
            )
            .filter(Expr::column("id").eq(Expr::param(Value::I64(1))))
            .build()
            .unwrap();

        assert!(update.filter.is_some());
    }

    #[test]
    fn test_update_with_returning() {
        let update = Update::table("users")
            .set(
                ColumnMarker::new("users", "email"),
                Value::String("new@example.com".to_string()),
            )
            .returning(vec![
                ColumnMarker::new("users", "id"),
                ColumnMarker::new("users", "email"),
            ])
            .build()
            .unwrap();

        assert_eq!(update.returning.len(), 2);
        assert_eq!(update.returning[0].name, "id");
        assert_eq!(update.returning[1].name, "email");
    }

    #[test]
    fn test_update_with_null() {
        let update = Update::table("users")
            .set(ColumnMarker::new("users", "name"), Value::Null)
            .build()
            .unwrap();

        assert_eq!(update.assignments[0].1, Value::Null);
    }

    #[test]
    fn test_missing_table() {
        let result = Update::table("")
            .set(
                ColumnMarker::new("users", "email"),
                Value::String("test".to_string()),
            )
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_missing_assignments() {
        let result = Update::table("users").build();

        assert!(result.is_err());
    }
}
