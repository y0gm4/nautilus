//! Integration tests for nautilus-core query builders.
//!
//! These tests exercise the builder APIs end-to-end, combining multiple modules
//! (Select, Insert, Update, Delete) with Expr filters, JoinClauses, ordering,
//! and pagination.

use nautilus_core::{
    ColumnMarker, Delete, DeleteCapacity, Expr, Insert, InsertCapacity, JoinClause, JoinType,
    OrderDir, Select, SelectCapacity, SelectItem, Update, UpdateCapacity, Value,
};

#[test]
fn select_with_filter_take_skip() {
    let filter = Expr::column("users__active").eq(Expr::param(Value::Bool(true)));

    let sel = Select::from_table("users")
        .with_capacity(SelectCapacity {
            items: 2,
            order_by_columns: 1,
            ..SelectCapacity::default()
        })
        .items(vec![
            SelectItem::column(ColumnMarker::new("users", "id")),
            SelectItem::column(ColumnMarker::new("users", "email")),
        ])
        .filter(filter.clone())
        .order_by_asc("users__id")
        .take(10)
        .skip(20)
        .build()
        .unwrap();

    assert_eq!(sel.table, "users");
    assert_eq!(sel.items.len(), 2);
    assert_eq!(sel.filter, Some(filter));
    assert_eq!(sel.order_by.len(), 1);
    assert_eq!(sel.order_by[0].column, "users__id");
    assert_eq!(sel.order_by[0].direction, OrderDir::Asc);
    assert_eq!(sel.take, Some(10));
    assert_eq!(sel.skip, Some(20));
}

#[test]
fn select_multiple_order_by() {
    let sel = Select::from_table("posts")
        .order_by_desc("posts__created_at")
        .order_by_asc("posts__id")
        .build()
        .unwrap();

    assert_eq!(sel.order_by.len(), 2);
    assert_eq!(sel.order_by[0].direction, OrderDir::Desc);
    assert_eq!(sel.order_by[1].direction, OrderDir::Asc);
}

#[test]
fn select_with_join() {
    let on = Expr::column("posts__author_id").eq(Expr::column("users__id"));
    let join = JoinClause::new(
        JoinType::Left,
        "users",
        on,
        vec![SelectItem::column(ColumnMarker::new("users", "email"))],
    );

    let sel = Select::from_table("posts")
        .item(SelectItem::column(ColumnMarker::new("posts", "id")))
        .join(join)
        .build()
        .unwrap();

    assert_eq!(sel.joins.len(), 1);
    assert_eq!(sel.joins[0].table, "users");
    assert_eq!(sel.joins[0].join_type, JoinType::Left);
    assert_eq!(sel.joins[0].items.len(), 1);
}

#[test]
fn select_empty_table_is_error() {
    let result = Select::from_table("").build();
    assert!(result.is_err());
}

#[test]
fn select_distinct() {
    let sel = Select::from_table("users")
        .distinct(vec!["users__email".to_string()])
        .build()
        .unwrap();

    assert_eq!(sel.distinct, vec!["users__email"]);
}

#[test]
fn insert_with_returning() {
    let ins = Insert::into_table("users")
        .with_capacity(InsertCapacity {
            columns: 2,
            rows: 1,
            returning: 1,
        })
        .columns(vec![
            ColumnMarker::new("users", "email"),
            ColumnMarker::new("users", "name"),
        ])
        .values(vec![
            Value::String("alice@example.com".into()),
            Value::String("Alice".into()),
        ])
        .returning(vec![ColumnMarker::new("users", "id")])
        .build()
        .unwrap();

    assert_eq!(ins.table, "users");
    assert_eq!(ins.columns.len(), 2);
    assert_eq!(ins.values.len(), 1);
    assert_eq!(ins.returning.len(), 1);
    assert_eq!(ins.returning[0].name, "id");
}

#[test]
fn insert_mismatched_column_value_count_is_error() {
    let result = Insert::into_table("users")
        .columns(vec![
            ColumnMarker::new("users", "email"),
            ColumnMarker::new("users", "name"),
        ])
        .values(vec![Value::String("only-one-value".into())])
        .build();

    assert!(result.is_err());
}

#[test]
fn update_with_filter_and_returning() {
    let filter = Expr::column("users__id").eq(Expr::param(Value::I64(1)));

    let upd = Update::table("users")
        .with_capacity(UpdateCapacity {
            assignments: 1,
            returning: 1,
        })
        .assignments(vec![(
            ColumnMarker::new("users", "email"),
            Value::String("new@example.com".into()),
        )])
        .filter(filter.clone())
        .returning(vec![ColumnMarker::new("users", "id")])
        .build()
        .unwrap();

    assert_eq!(upd.table, "users");
    assert_eq!(upd.assignments.len(), 1);
    assert_eq!(upd.assignments[0].0.name, "email");
    assert_eq!(
        upd.assignments[0].1,
        Value::String("new@example.com".into())
    );
    assert_eq!(upd.filter, Some(filter));
    assert_eq!(upd.returning.len(), 1);
}

#[test]
fn update_empty_table_is_error() {
    let result = Update::table("")
        .set(ColumnMarker::new("users", "email"), Value::Null)
        .build();
    assert!(result.is_err());
}

#[test]
fn update_no_assignments_is_error() {
    let result = Update::table("users").build();
    assert!(result.is_err());
}

#[test]
fn delete_with_filter() {
    let filter = Expr::column("users__id").eq(Expr::param(Value::I64(42)));

    let del = Delete::from_table("users")
        .filter(filter.clone())
        .build()
        .unwrap();

    assert_eq!(del.table, "users");
    assert_eq!(del.filter, Some(filter));
    assert!(del.returning.is_empty());
}

#[test]
fn delete_with_returning() {
    let del = Delete::from_table("users")
        .with_capacity(DeleteCapacity { returning: 1 })
        .filter(Expr::column("users__id").eq(Expr::param(Value::I64(1))))
        .returning(vec![ColumnMarker::new("users", "id")])
        .build()
        .unwrap();

    assert_eq!(del.returning.len(), 1);
}

#[test]
fn insert_rows_builder_batches_rows() {
    let ins = Insert::into_table("users")
        .with_capacity(InsertCapacity {
            columns: 1,
            rows: 2,
            returning: 0,
        })
        .columns(vec![ColumnMarker::new("users", "email")])
        .rows(vec![
            vec![Value::String("alice@example.com".into())],
            vec![Value::String("bob@example.com".into())],
        ])
        .build()
        .unwrap();

    assert_eq!(ins.values.len(), 2);
}

#[test]
fn delete_empty_table_is_error() {
    let result = Delete::from_table("").build();
    assert!(result.is_err());
}
