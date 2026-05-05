//! SELECT query AST and builder.

use crate::column::ColumnMarker;
use crate::error::{Error, Result};
use crate::expr::Expr;

/// A select list item that can be either a simple column or a computed expression.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// A simple column reference.
    Column(ColumnMarker),
    /// A computed expression with an alias.
    Computed {
        /// The expression to compute.
        expr: Expr,
        /// The alias for the computed expression.
        alias: String,
    },
}

impl SelectItem {
    /// Creates a SelectItem from a ColumnMarker.
    pub fn column(marker: ColumnMarker) -> Self {
        SelectItem::Column(marker)
    }

    /// Creates a computed SelectItem with an expression and alias.
    pub fn computed(expr: Expr, alias: impl Into<String>) -> Self {
        SelectItem::Computed {
            expr,
            alias: alias.into(),
        }
    }
}

impl From<ColumnMarker> for SelectItem {
    fn from(marker: ColumnMarker) -> Self {
        SelectItem::Column(marker)
    }
}

/// Sort direction for ORDER BY clauses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDir {
    /// Ascending.
    Asc,
    /// Descending.
    Desc,
}

/// ORDER BY clause item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBy {
    /// Column name.
    pub column: String,
    /// Sort direction.
    pub direction: OrderDir,
}

impl OrderBy {
    /// Creates a new ORDER BY clause.
    pub fn new(column: impl Into<String>, direction: OrderDir) -> Self {
        OrderBy {
            column: column.into(),
            direction,
        }
    }

    /// Creates an ascending ORDER BY clause.
    pub fn asc(column: impl Into<String>) -> Self {
        OrderBy::new(column, OrderDir::Asc)
    }

    /// Creates a descending ORDER BY clause.
    pub fn desc(column: impl Into<String>) -> Self {
        OrderBy::new(column, OrderDir::Desc)
    }
}

/// Reserved capacities for the `Vec`s maintained by a [`SelectBuilder`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SelectCapacity {
    /// Expected number of select-list items.
    pub items: usize,
    /// Expected number of JOIN clauses.
    pub joins: usize,
    /// Expected number of column-based `ORDER BY` items.
    pub order_by_columns: usize,
    /// Expected number of expression-based `ORDER BY` items.
    pub order_by_exprs: usize,
    /// Expected number of `GROUP BY` columns.
    pub group_by: usize,
    /// Expected number of `DISTINCT` columns.
    pub distinct: usize,
}

/// One ORDER BY item in the original user-specified sequence.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderByItem {
    /// An ORDER BY over a plain column reference.
    Column(OrderBy),
    /// An ORDER BY over an arbitrary expression, such as an aggregate function.
    Expr(Expr, OrderDir),
}

/// Type of JOIN operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// INNER JOIN — only matching rows from both tables.
    Inner,
    /// LEFT JOIN — all rows from the left table, matching rows from the right.
    Left,
}

/// A JOIN clause attached to a SELECT query.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    /// The type of join (INNER, LEFT).
    pub join_type: JoinType,
    /// The table to join.
    pub table: String,
    /// The ON condition expression.
    pub on: Expr,
    /// Select items (columns or computed expressions) from the joined table.
    pub items: Vec<SelectItem>,
}

impl JoinClause {
    /// Creates a new JOIN clause.
    pub fn new(
        join_type: JoinType,
        table: impl Into<String>,
        on: Expr,
        items: Vec<SelectItem>,
    ) -> Self {
        Self {
            join_type,
            table: table.into(),
            on,
            items,
        }
    }
}

/// SELECT query AST node.
#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    /// Table name.
    pub table: String,
    /// Select items (columns or computed expressions).
    pub items: Vec<SelectItem>,
    /// JOIN clauses.
    pub joins: Vec<JoinClause>,
    /// WHERE clause.
    pub filter: Option<Expr>,
    /// ORDER BY clauses.
    pub order_by: Vec<OrderBy>,
    /// Row count to return (maps to SQL LIMIT).
    ///
    /// Positive values limit forward; negative values signal backward pagination
    /// (callers must negate and reverse `ORDER BY` in application code).
    /// The absolute value is used when building the SQL `LIMIT` clause.
    pub take: Option<i32>,
    /// Row offset to skip (maps to SQL OFFSET).
    pub skip: Option<u32>,
    /// GROUP BY columns.
    pub group_by: Vec<ColumnMarker>,
    /// Columns to deduplicate on.
    ///
    /// Non-empty activates deduplication:
    /// - Postgres: `SELECT DISTINCT ON (cols)`
    /// - SQLite / MySQL: plain `SELECT DISTINCT`
    pub distinct: Vec<String>,
    /// HAVING clause (post-GROUP BY filter).
    pub having: Option<Expr>,
    /// ORDER BY items preserved in their original mixed column/expression order.
    pub order_by_items: Vec<OrderByItem>,
    /// ORDER BY expression items (for aggregate functions, e.g. `COUNT(*) DESC`).
    pub order_by_exprs: Vec<(Expr, OrderDir)>,
}

impl Select {
    /// Creates a new SELECT query builder for the given table.
    pub fn from_table(table: impl Into<String>) -> SelectBuilder {
        SelectBuilder {
            table: table.into(),
            items: Vec::new(),
            joins: Vec::new(),
            filter: None,
            order_by: Vec::new(),
            take: None,
            skip: None,
            group_by: Vec::new(),
            distinct: Vec::new(),
            having: None,
            order_by_items: Vec::new(),
            order_by_exprs: Vec::new(),
        }
    }
}

/// Builder for SELECT queries.
#[derive(Debug, Clone)]
pub struct SelectBuilder {
    table: String,
    items: Vec<SelectItem>,
    joins: Vec<JoinClause>,
    filter: Option<Expr>,
    order_by: Vec<OrderBy>,
    take: Option<i32>,
    skip: Option<u32>,
    group_by: Vec<ColumnMarker>,
    distinct: Vec<String>,
    having: Option<Expr>,
    order_by_items: Vec<OrderByItem>,
    order_by_exprs: Vec<(Expr, OrderDir)>,
}

impl SelectBuilder {
    /// Reserve capacity for the builder's internal vectors.
    #[must_use]
    pub fn with_capacity(mut self, capacity: SelectCapacity) -> Self {
        self.items.reserve(capacity.items);
        self.joins.reserve(capacity.joins);
        self.order_by.reserve(capacity.order_by_columns);
        self.group_by.reserve(capacity.group_by);
        self.distinct.reserve(capacity.distinct);
        self.order_by_items
            .reserve(capacity.order_by_columns + capacity.order_by_exprs);
        self.order_by_exprs.reserve(capacity.order_by_exprs);
        self
    }

    /// Sets the select items.
    #[must_use]
    pub fn items(mut self, items: Vec<SelectItem>) -> Self {
        self.items = items;
        self
    }

    /// Adds a select item.
    #[must_use]
    pub fn item(mut self, item: SelectItem) -> Self {
        self.items.push(item);
        self
    }

    /// Adds a computed expression with an alias.
    #[must_use]
    pub fn computed(mut self, expr: Expr, alias: impl Into<String>) -> Self {
        self.items.push(SelectItem::computed(expr, alias));
        self
    }

    /// Adds a WHERE clause filter.
    #[must_use]
    pub fn filter(mut self, expr: Expr) -> Self {
        self.filter = Some(expr);
        self
    }

    /// Adds an ORDER BY clause.
    #[must_use]
    pub fn order_by(mut self, column: impl Into<String>, direction: OrderDir) -> Self {
        let order = OrderBy::new(column, direction);
        self.order_by.push(order.clone());
        self.order_by_items.push(OrderByItem::Column(order));
        self
    }

    /// Adds an ORDER BY ASC clause.
    #[must_use]
    pub fn order_by_asc(mut self, column: impl Into<String>) -> Self {
        let order = OrderBy::asc(column);
        self.order_by.push(order.clone());
        self.order_by_items.push(OrderByItem::Column(order));
        self
    }

    /// Adds an ORDER BY DESC clause.
    #[must_use]
    pub fn order_by_desc(mut self, column: impl Into<String>) -> Self {
        let order = OrderBy::desc(column);
        self.order_by.push(order.clone());
        self.order_by_items.push(OrderByItem::Column(order));
        self
    }

    /// Sets the row count (maps to SQL LIMIT).
    ///
    /// Pass a positive value for forward pagination. Negative values signal
    /// backward pagination to callers; the dialect renders the absolute value.
    #[must_use]
    pub fn take(mut self, n: i32) -> Self {
        self.take = Some(n);
        self
    }

    /// Sets the row offset (maps to SQL OFFSET).
    #[must_use]
    pub fn skip(mut self, n: u32) -> Self {
        self.skip = Some(n);
        self
    }

    /// Adds a JOIN clause.
    #[must_use]
    pub fn join(mut self, clause: JoinClause) -> Self {
        self.joins.push(clause);
        self
    }

    /// Adds an INNER JOIN clause.
    #[must_use]
    pub fn inner_join(self, table: impl Into<String>, on: Expr, items: Vec<SelectItem>) -> Self {
        self.join(JoinClause::new(JoinType::Inner, table, on, items))
    }

    /// Adds a LEFT JOIN clause.
    #[must_use]
    pub fn left_join(self, table: impl Into<String>, on: Expr, items: Vec<SelectItem>) -> Self {
        self.join(JoinClause::new(JoinType::Left, table, on, items))
    }

    /// Adds a GROUP BY clause.
    #[must_use]
    pub fn group_by_column(mut self, column: ColumnMarker) -> Self {
        self.group_by.push(column);
        self
    }

    /// Adds multiple columns to the GROUP BY clause.
    #[must_use]
    pub fn group_by(mut self, columns: Vec<ColumnMarker>) -> Self {
        self.group_by.extend(columns);
        self
    }

    /// Sets the HAVING clause (post-GROUP BY filter).
    #[must_use]
    pub fn having(mut self, expr: Expr) -> Self {
        self.having = Some(expr);
        self
    }

    /// Adds an ORDER BY clause using an arbitrary expression (e.g. an aggregate function).
    #[must_use]
    pub fn order_by_expr(mut self, expr: Expr, direction: OrderDir) -> Self {
        self.order_by_exprs.push((expr.clone(), direction));
        self.order_by_items.push(OrderByItem::Expr(expr, direction));
        self
    }

    /// Sets the columns to deduplicate on (SELECT DISTINCT / DISTINCT ON).
    ///
    /// - **Postgres**: emits `SELECT DISTINCT ON (col, ...)` and requires those
    ///   columns to appear first in `ORDER BY` (callers should prepend them).
    /// - **SQLite / MySQL**: emits plain `SELECT DISTINCT`.
    #[must_use]
    pub fn distinct(mut self, columns: Vec<String>) -> Self {
        self.distinct = columns;
        self
    }

    /// Builds the final SELECT query.
    pub fn build(self) -> Result<Select> {
        if self.table.is_empty() {
            return Err(Error::MissingField("table".to_string()));
        }

        Ok(Select {
            table: self.table,
            items: self.items,
            joins: self.joins,
            filter: self.filter,
            order_by: self.order_by,
            take: self.take,
            skip: self.skip,
            group_by: self.group_by,
            distinct: self.distinct,
            having: self.having,
            order_by_items: self.order_by_items,
            order_by_exprs: self.order_by_exprs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Expr;

    #[test]
    fn test_order_by() {
        let asc = OrderBy::asc("id");
        assert_eq!(asc.column, "id");
        assert_eq!(asc.direction, OrderDir::Asc);

        let desc = OrderBy::desc("created_at");
        assert_eq!(desc.column, "created_at");
        assert_eq!(desc.direction, OrderDir::Desc);
    }

    #[test]
    fn test_simple_select() {
        let query = Select::from_table("users").build().unwrap();

        assert_eq!(query.table, "users");
        assert!(query.items.is_empty());
        assert!(query.joins.is_empty());
        assert!(query.filter.is_none());
        assert!(query.order_by.is_empty());
        assert!(query.take.is_none());
        assert!(query.skip.is_none());
    }

    #[test]
    fn test_select_with_columns() {
        let query = Select::from_table("users")
            .item(SelectItem::from(ColumnMarker::new("users", "id")))
            .item(SelectItem::from(ColumnMarker::new("users", "email")))
            .build()
            .unwrap();

        assert_eq!(query.items.len(), 2);
        if let SelectItem::Column(col) = &query.items[0] {
            assert_eq!(col.table, "users");
            assert_eq!(col.name, "id");
        }
        if let SelectItem::Column(col) = &query.items[1] {
            assert_eq!(col.table, "users");
            assert_eq!(col.name, "email");
        }
    }

    #[test]
    fn test_select_with_filter() {
        let filter = Expr::column("age").ge(Expr::param(18i64));
        let query = Select::from_table("users")
            .filter(filter.clone())
            .build()
            .unwrap();

        assert_eq!(query.filter, Some(filter));
    }

    #[test]
    fn test_select_with_order_by() {
        let query = Select::from_table("users")
            .order_by_desc("created_at")
            .order_by_asc("email")
            .build()
            .unwrap();

        assert_eq!(query.order_by.len(), 2);
        assert_eq!(query.order_by[0].column, "created_at");
        assert_eq!(query.order_by[0].direction, OrderDir::Desc);
        assert_eq!(query.order_by[1].column, "email");
        assert_eq!(query.order_by[1].direction, OrderDir::Asc);
    }

    #[test]
    fn test_select_with_take_and_skip() {
        let query = Select::from_table("users")
            .take(10)
            .skip(20)
            .build()
            .unwrap();

        assert_eq!(query.take, Some(10));
        assert_eq!(query.skip, Some(20));
    }

    #[test]
    fn test_complex_select() {
        let filter = Expr::column("age")
            .ge(Expr::param(18i64))
            .and(Expr::column("email").like(Expr::param("%@gmail.com")));

        let query = Select::from_table("users")
            .items(vec![
                SelectItem::from(ColumnMarker::new("users", "id")),
                SelectItem::from(ColumnMarker::new("users", "email")),
                SelectItem::from(ColumnMarker::new("users", "age")),
            ])
            .filter(filter)
            .order_by_desc("id")
            .take(10)
            .build()
            .unwrap();

        assert_eq!(query.table, "users");
        assert_eq!(query.items.len(), 3);
        assert!(query.filter.is_some());
        assert_eq!(query.order_by.len(), 1);
        assert_eq!(query.take, Some(10));
    }

    #[test]
    fn test_select_with_inner_join() {
        let on = Expr::column("users__id").eq(Expr::column("posts__user_id"));
        let query = Select::from_table("users")
            .item(SelectItem::from(ColumnMarker::new("users", "id")))
            .inner_join(
                "posts",
                on.clone(),
                vec![
                    SelectItem::from(ColumnMarker::new("posts", "id")),
                    SelectItem::from(ColumnMarker::new("posts", "title")),
                ],
            )
            .build()
            .unwrap();

        assert_eq!(query.joins.len(), 1);
        assert_eq!(query.joins[0].join_type, JoinType::Inner);
        assert_eq!(query.joins[0].table, "posts");
        assert_eq!(query.joins[0].on, on);
        assert_eq!(query.joins[0].items.len(), 2);
    }

    #[test]
    fn test_select_with_left_join() {
        let on = Expr::column("users__id").eq(Expr::column("posts__user_id"));
        let query = Select::from_table("users")
            .item(SelectItem::from(ColumnMarker::new("users", "id")))
            .left_join(
                "posts",
                on,
                vec![SelectItem::from(ColumnMarker::new("posts", "title"))],
            )
            .build()
            .unwrap();

        assert_eq!(query.joins.len(), 1);
        assert_eq!(query.joins[0].join_type, JoinType::Left);
        assert_eq!(query.joins[0].table, "posts");
        assert_eq!(query.joins[0].items.len(), 1);
    }

    #[test]
    fn test_select_with_multiple_joins() {
        let query = Select::from_table("users")
            .inner_join(
                "posts",
                Expr::column("users__id").eq(Expr::column("posts__user_id")),
                vec![SelectItem::from(ColumnMarker::new("posts", "title"))],
            )
            .left_join(
                "comments",
                Expr::column("posts__id").eq(Expr::column("comments__post_id")),
                vec![SelectItem::from(ColumnMarker::new("comments", "body"))],
            )
            .build()
            .unwrap();

        assert_eq!(query.joins.len(), 2);
        assert_eq!(query.joins[0].join_type, JoinType::Inner);
        assert_eq!(query.joins[0].table, "posts");
        assert_eq!(query.joins[1].join_type, JoinType::Left);
        assert_eq!(query.joins[1].table, "comments");
    }
}
