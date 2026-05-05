//! DELETE query AST and builder.

use crate::column::ColumnMarker;
use crate::error::{Error, Result};
use crate::expr::Expr;

/// DELETE query AST node.
#[derive(Debug, Clone, PartialEq)]
pub struct Delete {
    /// Table name.
    pub table: String,
    /// WHERE clause.
    pub filter: Option<Expr>,
    /// Columns to return (RETURNING clause). Empty = no RETURNING.
    pub returning: Vec<ColumnMarker>,
}

impl Delete {
    /// Creates a new DELETE query builder for the given table.
    pub fn from_table(table: impl Into<String>) -> DeleteBuilder {
        DeleteBuilder {
            table: table.into(),
            filter: None,
            returning: Vec::new(),
        }
    }
}

/// Reserved capacities for the `Vec`s maintained by a [`DeleteBuilder`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeleteCapacity {
    /// Expected number of `RETURNING` columns.
    pub returning: usize,
}

/// Builder for DELETE queries.
#[derive(Debug, Clone)]
pub struct DeleteBuilder {
    table: String,
    filter: Option<Expr>,
    returning: Vec<ColumnMarker>,
}

impl DeleteBuilder {
    /// Reserve capacity for the builder's internal vectors.
    #[must_use]
    pub fn with_capacity(mut self, capacity: DeleteCapacity) -> Self {
        self.returning.reserve(capacity.returning);
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

    /// Builds the final DELETE query.
    ///
    /// # Errors
    ///
    /// Returns an error if the table name is empty.
    pub fn build(self) -> Result<Delete> {
        if self.table.is_empty() {
            return Err(Error::MissingField("table".to_string()));
        }

        Ok(Delete {
            table: self.table,
            filter: self.filter,
            returning: self.returning,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn test_simple_delete() {
        let delete = Delete::from_table("users").build().unwrap();

        assert_eq!(delete.table, "users");
        assert!(delete.filter.is_none());
        assert!(delete.returning.is_empty());
    }

    #[test]
    fn test_delete_with_filter() {
        let delete = Delete::from_table("users")
            .filter(Expr::column("id").eq(Expr::param(Value::I64(1))))
            .build()
            .unwrap();

        assert!(delete.filter.is_some());
    }

    #[test]
    fn test_delete_with_returning() {
        let delete = Delete::from_table("users")
            .returning(vec![
                ColumnMarker::new("users", "id"),
                ColumnMarker::new("users", "email"),
            ])
            .build()
            .unwrap();

        assert_eq!(delete.returning.len(), 2);
        assert_eq!(delete.returning[0].name, "id");
        assert_eq!(delete.returning[1].name, "email");
    }

    #[test]
    fn test_delete_with_complex_filter() {
        let filter = Expr::column("id")
            .ge(Expr::param(Value::I64(10)))
            .and(Expr::column("active").eq(Expr::param(Value::Bool(false))));

        let delete = Delete::from_table("users")
            .filter(filter)
            .returning(vec![
                ColumnMarker::new("users", "id"),
                ColumnMarker::new("users", "email"),
            ])
            .build()
            .unwrap();

        assert!(delete.filter.is_some());
        assert_eq!(delete.returning.len(), 2);
    }

    #[test]
    fn test_missing_table() {
        let result = Delete::from_table("").build();

        assert!(result.is_err());
    }
}
