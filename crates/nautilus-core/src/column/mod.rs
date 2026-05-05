//! Typed column references for type-safe query building.

pub mod from_value;
pub mod marker;
pub mod row_access;
pub mod typed;

pub use from_value::FromValue;
pub use marker::ColumnMarker;
pub use row_access::RowAccess;
pub use typed::{Column, SelectColumns};

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;
    use crate::expr::Expr;
    use crate::select::OrderDir;
    use crate::value::Value;

    #[test]
    fn test_column_new() {
        let col: Column<i64> = Column::new("users", "id");
        assert_eq!(col.table(), "users");
        assert_eq!(col.name(), "id");

        let col: Column<String> = Column::new("users", "email");
        assert_eq!(col.table(), "users");
        assert_eq!(col.name(), "email");
    }

    #[test]
    fn test_column_alias() {
        let col: Column<i64> = Column::new("users", "id");
        assert_eq!(col.alias(), "users__id");

        let col: Column<String> = Column::new("posts", "title");
        assert_eq!(col.alias(), "posts__title");
    }

    #[test]
    fn test_column_marker() {
        let col: Column<i64> = Column::new("users", "id");
        let marker = col.marker();
        assert_eq!(marker.table, "users");
        assert_eq!(marker.name, "id");
        assert!(matches!(marker.table, Cow::Borrowed("users")));
        assert!(matches!(marker.name, Cow::Borrowed("id")));
    }

    #[test]
    fn test_column_to_expr() {
        let col: Column<i64> = Column::new("users", "id");
        let expr: Expr = col.into();
        assert_eq!(expr, Expr::Column("users__id".to_string()));
    }

    #[test]
    fn test_eq_operator() {
        let col: Column<i64> = Column::new("users", "id");
        let expr = col.eq(42i64);
        assert_eq!(
            expr,
            Expr::column("users__id").eq(Expr::param(Value::I64(42)))
        );
    }

    #[test]
    fn test_eq_operator_string() {
        let col: Column<String> = Column::new("users", "email");
        let expr = col.eq("test@example.com");
        assert_eq!(
            expr,
            Expr::column("users__email")
                .eq(Expr::param(Value::String("test@example.com".to_string())))
        );
    }

    #[test]
    fn test_desc_order() {
        let col: Column<String> = Column::new("users", "email");
        let order = col.desc();
        assert_eq!(order.column, "users__email");
        assert_eq!(order.direction, OrderDir::Desc);
    }

    #[test]
    fn test_asc_order() {
        let col: Column<i64> = Column::new("users", "id");
        let order = col.asc();
        assert_eq!(order.column, "users__id");
        assert_eq!(order.direction, OrderDir::Asc);
    }

    #[test]
    fn test_ends_with() {
        let col: Column<String> = Column::new("users", "email");
        let expr = col.ends_with("example.com");
        assert_eq!(
            expr,
            Expr::column("users__email")
                .like(Expr::param(Value::String("%example.com".to_string())))
        );
    }

    #[test]
    fn test_starts_with() {
        let col: Column<String> = Column::new("users", "email");
        let expr = col.starts_with("admin");
        assert_eq!(
            expr,
            Expr::column("users__email").like(Expr::param(Value::String("admin%".to_string())))
        );
    }

    #[test]
    fn test_contains() {
        let col: Column<String> = Column::new("users", "email");
        let expr = col.contains("example");
        assert_eq!(
            expr,
            Expr::column("users__email").like(Expr::param(Value::String("%example%".to_string())))
        );
    }

    #[test]
    fn test_from_value_i64() {
        assert_eq!(i64::from_value(&Value::I64(42)).unwrap(), 42);
        assert!(i64::from_value(&Value::Null).is_err());
        assert!(i64::from_value(&Value::String("test".to_string())).is_err());
    }

    #[test]
    fn test_from_value_string() {
        assert_eq!(
            String::from_value(&Value::String("test".to_string())).unwrap(),
            "test"
        );
        assert!(String::from_value(&Value::Null).is_err());
        assert!(String::from_value(&Value::I64(42)).is_err());
    }

    #[test]
    fn test_from_value_bool() {
        assert!(bool::from_value(&Value::Bool(true)).unwrap());
        assert!(!bool::from_value(&Value::Bool(false)).unwrap());
        assert!(bool::from_value(&Value::I64(1)).unwrap());
        assert!(!bool::from_value(&Value::I64(0)).unwrap());
        assert!(bool::from_value(&Value::Null).is_err());
        assert!(bool::from_value(&Value::I64(2)).is_err());
    }

    #[test]
    fn test_select_columns_single() {
        let selection = (Column::<i64>::new("users", "id"),);
        let columns = selection.columns();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].table, "users");
        assert_eq!(columns[0].name, "id");
    }

    #[test]
    fn test_select_columns_two() {
        let selection = (
            Column::<i64>::new("users", "id"),
            Column::<String>::new("users", "email"),
        );
        let columns = selection.columns();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].table, "users");
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[1].table, "users");
        assert_eq!(columns[1].name, "email");
    }

    #[test]
    fn test_select_columns_multiple_tables() {
        let selection = (
            Column::<i64>::new("users", "id"),
            Column::<String>::new("posts", "title"),
        );
        let columns = selection.columns();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].table, "users");
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[1].table, "posts");
        assert_eq!(columns[1].name, "title");
    }

    #[test]
    fn test_column_copy() {
        let col1: Column<i64> = Column::new("users", "id");
        let col2 = col1;
        let col3 = col1;
        assert_eq!(col1.name(), col2.name());
        assert_eq!(col2.name(), col3.name());
    }

    #[test]
    fn test_column_marker_from() {
        let col: Column<i64> = Column::new("users", "id");
        let marker: ColumnMarker = col.into();
        assert_eq!(marker.table, "users");
        assert_eq!(marker.name, "id");
    }

    #[test]
    fn test_column_marker_alias() {
        let marker = ColumnMarker::new("users", "id");
        assert_eq!(marker.alias(), "users__id");

        let marker = ColumnMarker::new("posts", "title");
        assert_eq!(marker.alias(), "posts__title");
    }

    #[test]
    fn test_dynamic_column_marker_owns_runtime_strings() {
        let table = String::from("users");
        let name = String::from("email");
        let marker = ColumnMarker::new(table, name);

        assert!(matches!(marker.table, Cow::Owned(_)));
        assert!(matches!(marker.name, Cow::Owned(_)));
    }

    struct MockRow {
        values: Vec<Value>,
    }

    impl<'row> RowAccess<'row> for MockRow {
        fn get_by_pos(&'row self, idx: usize) -> Option<&'row Value> {
            self.values.get(idx)
        }

        fn get(&'row self, _name: &str) -> Option<&'row Value> {
            None
        }

        fn column_name(&'row self, _idx: usize) -> Option<&'row str> {
            None
        }

        fn len(&self) -> usize {
            self.values.len()
        }
    }

    #[test]
    fn test_select_columns_decode_single() {
        let selection = (Column::<i64>::new("users", "id"),);
        let mock_row = MockRow {
            values: vec![Value::I64(42)],
        };

        let result = selection.decode(&mock_row).unwrap();
        assert_eq!(result, (42,));
    }

    #[test]
    fn test_select_columns_decode_two() {
        let selection = (
            Column::<i64>::new("users", "id"),
            Column::<String>::new("users", "email"),
        );
        let mock_row = MockRow {
            values: vec![
                Value::I64(42),
                Value::String("test@example.com".to_string()),
            ],
        };

        let result = selection.decode(&mock_row).unwrap();
        assert_eq!(result, (42, "test@example.com".to_string()));
    }

    #[test]
    fn test_select_columns_decode_three() {
        let selection = (
            Column::<i64>::new("users", "id"),
            Column::<String>::new("users", "email"),
            Column::<bool>::new("users", "active"),
        );
        let mock_row = MockRow {
            values: vec![
                Value::I64(99),
                Value::String("admin@example.com".to_string()),
                Value::Bool(true),
            ],
        };

        let result = selection.decode(&mock_row).unwrap();
        assert_eq!(result, (99, "admin@example.com".to_string(), true));
    }

    #[test]
    fn test_select_columns_decode_missing_column() {
        let selection = (
            Column::<i64>::new("users", "id"),
            Column::<String>::new("users", "email"),
        );
        let mock_row = MockRow {
            values: vec![Value::I64(42)],
        };

        let result = selection.decode(&mock_row);
        assert!(result.is_err());
    }

    #[test]
    fn test_select_columns_decode_type_error() {
        let selection = (Column::<i64>::new("users", "id"),);
        let mock_row = MockRow {
            values: vec![Value::String("not a number".to_string())],
        };

        let result = selection.decode(&mock_row);
        assert!(result.is_err());
    }
}
