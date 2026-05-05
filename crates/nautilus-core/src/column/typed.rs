//! Typed `Column<T>` reference and `SelectColumns` multi-column selection trait.

use std::marker::PhantomData;

use super::from_value::FromValue;
use super::marker::{build_column_alias, ColumnMarker};
use super::row_access::RowAccess;
use crate::error::Result;
use crate::expr::Expr;
use crate::select::{OrderBy, OrderDir};
use crate::value::Value;

/// Typed column reference.
///
/// Column is a zero-cost abstraction that carries type information
/// at compile time without runtime overhead. It is Copy and uses
/// only static string references.
///
/// # Examples
///
/// ```
/// use nautilus_core::{Column, Expr, Value};
///
/// let id_col: Column<i64> = Column::new("users", "id");
/// let expr = id_col.eq(42);
/// ```
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Column<T> {
    table: &'static str,
    name: &'static str,
    _phantom: PhantomData<T>,
}

impl<T> Column<T> {
    /// Creates a new typed column reference.
    ///
    /// This is a const fn, so columns can be defined as constants.
    ///
    /// # Examples
    ///
    /// ```
    /// use nautilus_core::Column;
    ///
    /// const USER_ID: Column<i64> = Column::new("users", "id");
    /// const USER_EMAIL: Column<String> = Column::new("users", "email");
    /// ```
    pub const fn new(table: &'static str, name: &'static str) -> Self {
        Column {
            table,
            name,
            _phantom: PhantomData,
        }
    }

    /// Returns the table name.
    pub const fn table(&self) -> &'static str {
        self.table
    }

    /// Returns the column name.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Returns a column marker for this column.
    ///
    /// Static generated columns borrow their metadata without allocating.
    pub fn marker(&self) -> ColumnMarker {
        ColumnMarker::from_static(self.table, self.name)
    }

    /// Returns the join-safe alias for this column.
    ///
    /// The alias uses the format `"table__column"` which is safe to use
    /// in queries with joins, preventing column name conflicts.
    ///
    /// Matches the naming of [`ColumnMarker::alias`].
    ///
    /// # Examples
    ///
    /// ```
    /// use nautilus_core::Column;
    ///
    /// let id: Column<i64> = Column::new("users", "id");
    /// assert_eq!(id.alias(), "users__id");
    /// ```
    pub fn alias(&self) -> String {
        build_column_alias(self.table, self.name)
    }

    /// Creates an equality comparison (`=`).
    ///
    /// # Examples
    ///
    /// ```
    /// use nautilus_core::{Column, Expr, Value};
    ///
    /// let id: Column<i64> = Column::new("users", "id");
    /// let expr = id.eq(42);
    /// ```
    #[must_use]
    pub fn eq<V: Into<Value>>(self, value: V) -> Expr {
        Expr::column(self.alias()).eq(Expr::param(value.into()))
    }

    /// Creates a column-to-column equality expression.
    ///
    /// This is useful for JOIN ON conditions where both sides are columns.
    ///
    /// # Examples
    ///
    /// ```
    /// use nautilus_core::{Column, Expr};
    ///
    /// let user_id: Column<i64> = Column::new("users", "id");
    /// let post_user_id: Column<i64> = Column::new("posts", "user_id");
    /// let on = user_id.eq_column(post_user_id);
    /// ```
    #[must_use]
    pub fn eq_column<U>(self, other: Column<U>) -> Expr {
        Expr::column(self.alias()).eq(Expr::column(other.alias()))
    }

    /// Creates a descending ORDER BY clause.
    ///
    /// # Examples
    ///
    /// ```
    /// use nautilus_core::{Column, OrderBy, OrderDir};
    ///
    /// let email: Column<String> = Column::new("users", "email");
    /// let order = email.desc();
    /// assert_eq!(order.direction, OrderDir::Desc);
    /// ```
    #[must_use]
    pub fn desc(self) -> OrderBy {
        OrderBy::new(self.alias(), OrderDir::Desc)
    }

    /// Creates an ascending ORDER BY clause.
    ///
    /// # Examples
    ///
    /// ```
    /// use nautilus_core::{Column, OrderBy, OrderDir};
    ///
    /// let email: Column<String> = Column::new("users", "email");
    /// let order = email.asc();
    /// assert_eq!(order.direction, OrderDir::Asc);
    /// ```
    #[must_use]
    pub fn asc(self) -> OrderBy {
        OrderBy::new(self.alias(), OrderDir::Asc)
    }
}

impl Column<String> {
    /// Creates a LIKE expression for suffix matching.
    ///
    /// Generates a SQL LIKE pattern of the form `%suffix`.
    ///
    /// # Examples
    ///
    /// ```
    /// use nautilus_core::{Column, Expr, Value};
    ///
    /// let email: Column<String> = Column::new("users", "email");
    /// let expr = email.ends_with("example.com");
    /// ```
    #[must_use]
    pub fn ends_with(self, suffix: impl Into<String>) -> Expr {
        let suffix = suffix.into();
        let mut pattern = String::with_capacity(suffix.len() + 1);
        pattern.push('%');
        pattern.push_str(&suffix);
        Expr::column(self.alias()).like(Expr::param(Value::String(pattern)))
    }

    /// Creates a LIKE expression for prefix matching.
    ///
    /// Generates a SQL LIKE pattern of the form `prefix%`.
    ///
    /// # Examples
    ///
    /// ```
    /// use nautilus_core::{Column, Expr, Value};
    ///
    /// let email: Column<String> = Column::new("users", "email");
    /// let expr = email.starts_with("admin");
    /// ```
    #[must_use]
    pub fn starts_with(self, prefix: impl Into<String>) -> Expr {
        let prefix = prefix.into();
        let mut pattern = String::with_capacity(prefix.len() + 1);
        pattern.push_str(&prefix);
        pattern.push('%');
        Expr::column(self.alias()).like(Expr::param(Value::String(pattern)))
    }

    /// Creates a LIKE expression for substring matching.
    ///
    /// Generates a SQL LIKE pattern of the form `%substring%`.
    ///
    /// # Examples
    ///
    /// ```
    /// use nautilus_core::{Column, Expr, Value};
    ///
    /// let email: Column<String> = Column::new("users", "email");
    /// let expr = email.contains("example");
    /// ```
    #[must_use]
    pub fn contains(self, substring: impl Into<String>) -> Expr {
        let substring = substring.into();
        let mut pattern = String::with_capacity(substring.len() + 2);
        pattern.push('%');
        pattern.push_str(&substring);
        pattern.push('%');
        Expr::column(self.alias()).like(Expr::param(Value::String(pattern)))
    }
}

impl<T> Column<Vec<T>>
where
    T: Into<Value> + Clone,
{
    /// Creates an array contains expression (`@>` in PostgreSQL).
    ///
    /// Returns rows where the column array contains the given element.
    ///
    /// # Examples
    ///
    /// ```
    /// use nautilus_core::{Column, Expr, Value};
    ///
    /// let tags: Column<Vec<String>> = Column::new("posts", "tags");
    /// let expr = tags.contains_elem("rust".to_string());
    /// ```
    #[must_use]
    pub fn contains_elem(self, elem: T) -> Expr {
        let array = vec![elem.into()];
        Expr::Binary {
            left: Box::new(Expr::column(self.alias())),
            op: crate::expr::BinaryOp::ArrayContains,
            right: Box::new(Expr::param(Value::Array(array))),
        }
    }

    /// Creates an array contains expression for multiple elements (`@>` in PostgreSQL).
    ///
    /// Returns rows where the column array contains all the given elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use nautilus_core::{Column, Expr, Value};
    ///
    /// let tags: Column<Vec<String>> = Column::new("posts", "tags");
    /// let expr = tags.contains_all(vec!["rust".to_string(), "programming".to_string()]);
    /// ```
    #[must_use]
    pub fn contains_all(self, elements: Vec<T>) -> Expr {
        let array = elements.into_iter().map(|e| e.into()).collect();
        Expr::Binary {
            left: Box::new(Expr::column(self.alias())),
            op: crate::expr::BinaryOp::ArrayContains,
            right: Box::new(Expr::param(Value::Array(array))),
        }
    }

    /// Creates an array contained by expression (`<@` in PostgreSQL).
    ///
    /// Returns rows where the column array is contained by the given array.
    ///
    /// # Examples
    ///
    /// ```
    /// use nautilus_core::{Column, Expr, Value};
    ///
    /// let tags: Column<Vec<String>> = Column::new("posts", "tags");
    /// let expr = tags.contained_by(vec!["rust".to_string(), "go".to_string()]);
    /// ```
    #[must_use]
    pub fn contained_by(self, container: Vec<T>) -> Expr {
        let array = container.into_iter().map(|e| e.into()).collect();
        Expr::Binary {
            left: Box::new(Expr::column(self.alias())),
            op: crate::expr::BinaryOp::ArrayContainedBy,
            right: Box::new(Expr::param(Value::Array(array))),
        }
    }

    /// Creates an array overlaps expression (`&&` in PostgreSQL).
    ///
    /// Returns rows where the column array has any elements in common with the given array.
    ///
    /// # Examples
    ///
    /// ```
    /// use nautilus_core::{Column, Expr, Value};
    ///
    /// let tags: Column<Vec<String>> = Column::new("posts", "tags");
    /// let expr = tags.overlaps(vec!["rust".to_string(), "go".to_string()]);
    /// ```
    #[must_use]
    pub fn overlaps(self, other: Vec<T>) -> Expr {
        let array = other.into_iter().map(|e| e.into()).collect();
        Expr::Binary {
            left: Box::new(Expr::column(self.alias())),
            op: crate::expr::BinaryOp::ArrayOverlaps,
            right: Box::new(Expr::param(Value::Array(array))),
        }
    }
}

impl<T> From<Column<T>> for Expr {
    /// Converts a typed column into an expression.
    ///
    /// # Examples
    ///
    /// ```
    /// use nautilus_core::{Column, Expr};
    ///
    /// let id: Column<i64> = Column::new("users", "id");
    /// let expr: Expr = id.into();
    /// ```
    fn from(col: Column<T>) -> Expr {
        Expr::Column(col.alias())
    }
}

/// Converts a typed column into a [`ColumnMarker`] for use in query building.
impl<T> From<Column<T>> for ColumnMarker {
    fn from(col: Column<T>) -> ColumnMarker {
        col.marker()
    }
}

/// Trait for extracting column metadata from selections.
///
/// This trait enables the query builder to convert typed column references
/// into selection descriptors and decode row data into typed tuples.
///
/// Note: This trait is implemented for tuples only. Single-column selections
/// must use tuple syntax: `.select(|u| (u.id(),))` not `.select(|u| u.id())`.
/// Trait for selecting typed column sets from a query.
///
/// `SelectColumns` connects typed `Column<T>` references to the `RowAccess`
/// trait, providing both the list of [`ColumnMarker`]s to include in the
/// `SELECT` clause and a type-safe `decode` method that extracts all values
/// in one call.
///
/// # Implementations
///
/// Implementations are provided for tuples of 1–8 `Column<T>` elements.
/// Queries returning more than 8 columns should use a struct implementing
/// [`FromRow`](crate::column) instead.
pub trait SelectColumns {
    /// The decoded Rust type that corresponds to this selection.
    ///
    /// For example, `(Column<i64>, Column<String>)` decodes to `(i64, String)`.
    type Output;

    /// Returns column markers for this selection.
    ///
    /// This extracts table and column names without additional allocations
    /// beyond the Vec itself.
    fn columns(&self) -> Vec<ColumnMarker>;

    /// Decode a row into the output type.
    ///
    /// Uses positional decoding to extract values from the row in the same
    /// order as the columns were specified. Each value is converted using
    /// the `FromValue` trait.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A column is missing at the expected position
    /// - A value has an unexpected type
    /// - A NULL value is found for a non-nullable field
    fn decode<'a>(&self, row: &'a impl RowAccess<'a>) -> Result<Self::Output>;
}

macro_rules! impl_select_columns {
    ($($T:ident),+; $($idx:tt),+) => {
        impl<$($T),+> SelectColumns for ($(Column<$T>,)+)
        where
            $($T: FromValue,)+
        {
            type Output = ($($T,)+);

            fn columns(&self) -> Vec<ColumnMarker> {
                vec![$(self.$idx.marker(),)+]
            }

            fn decode<'a>(&self, row: &'a impl RowAccess<'a>) -> Result<Self::Output> {
                Ok((
                    $(
                        {
                            let value = row.get_by_pos($idx)
                                .ok_or_else(|| crate::Error::TypeError(
                                    format!("missing column at position {}", $idx)
                                ))?;
                            $T::from_value(value)?
                        },
                    )+
                ))
            }
        }
    };
}

impl_select_columns!(T1; 0);
impl_select_columns!(T1, T2; 0, 1);
impl_select_columns!(T1, T2, T3; 0, 1, 2);
impl_select_columns!(T1, T2, T3, T4; 0, 1, 2, 3);
impl_select_columns!(T1, T2, T3, T4, T5; 0, 1, 2, 3, 4);
impl_select_columns!(T1, T2, T3, T4, T5, T6; 0, 1, 2, 3, 4, 5);
impl_select_columns!(T1, T2, T3, T4, T5, T6, T7; 0, 1, 2, 3, 4, 5, 6);
impl_select_columns!(T1, T2, T3, T4, T5, T6, T7, T8; 0, 1, 2, 3, 4, 5, 6, 7);
