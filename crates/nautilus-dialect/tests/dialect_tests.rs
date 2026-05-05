//! Shared integration-style tests for the three SQL dialect renderers.

use nautilus_core::{
    ColumnMarker, Delete, Expr, Insert, OrderDir, Select, SelectItem, Update, Value,
};
use nautilus_dialect::{Dialect, MysqlDialect, PostgresDialect, SqliteDialect};

struct Harness {
    name: &'static str,
    dialect: Box<dyn Dialect>,
    /// Quote a SQL identifier in this dialect's style.
    q: fn(&str) -> String,
    /// Return the placeholder string for the Nth (1-based) bound parameter.
    p: fn(usize) -> String,
}

impl Harness {
    fn pg() -> Self {
        Self {
            name: "postgres",
            dialect: Box::new(PostgresDialect),
            q: dq,
            p: pg_param,
        }
    }

    fn mysql() -> Self {
        Self {
            name: "mysql",
            dialect: Box::new(MysqlDialect),
            q: bq,
            p: qm_param,
        }
    }

    fn sqlite() -> Self {
        Self {
            name: "sqlite",
            dialect: Box::new(SqliteDialect),
            q: dq,
            p: qm_param,
        }
    }
}

fn all_harnesses() -> [Harness; 3] {
    [Harness::pg(), Harness::mysql(), Harness::sqlite()]
}

fn returning_harnesses() -> [Harness; 2] {
    [Harness::pg(), Harness::sqlite()]
}

fn dq(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn bq(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

fn pg_param(n: usize) -> String {
    format!("${n}")
}

fn qm_param(_n: usize) -> String {
    "?".to_string()
}

fn check_select_star(h: &Harness) {
    let select = Select::from_table("users").build().unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!("SELECT * FROM {}", (h.q)("users")),
        "[{}]",
        h.name
    );
    assert!(sql.params.is_empty(), "[{}]", h.name);
}

fn check_select_columns(h: &Harness) {
    let q = h.q;
    let select = Select::from_table("users")
        .item(SelectItem::from(ColumnMarker::new("users", "id")))
        .item(SelectItem::from(ColumnMarker::new("users", "email")))
        .build()
        .unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT {}.{} AS {}, {}.{} AS {} FROM {}",
            q("users"),
            q("id"),
            q("users__id"),
            q("users"),
            q("email"),
            q("users__email"),
            q("users"),
        ),
        "[{}]",
        h.name
    );
    assert!(sql.params.is_empty(), "[{}]", h.name);
}

fn check_where_clause(h: &Harness) {
    let filter = Expr::column("age").ge(Expr::param(Value::I64(18)));
    let select = Select::from_table("users").filter(filter).build().unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT * FROM {} WHERE ({} >= {})",
            (h.q)("users"),
            (h.q)("age"),
            (h.p)(1),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(sql.params, vec![Value::I64(18)], "[{}]", h.name);
}

fn check_complex_where(h: &Harness) {
    let q = h.q;
    let filter = Expr::column("age")
        .ge(Expr::param(Value::I64(18)))
        .and(Expr::column("email").like(Expr::param(Value::String("%@gmail.com".to_string()))));
    let select = Select::from_table("users")
        .items(vec![
            SelectItem::from(ColumnMarker::new("users", "id")),
            SelectItem::from(ColumnMarker::new("users", "email")),
        ])
        .filter(filter)
        .build()
        .unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT {}.{} AS {}, {}.{} AS {} FROM {} WHERE (({} >= {}) AND ({} LIKE {}))",
            q("users"),
            q("id"),
            q("users__id"),
            q("users"),
            q("email"),
            q("users__email"),
            q("users"),
            q("age"),
            (h.p)(1),
            q("email"),
            (h.p)(2),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(
        sql.params,
        vec![Value::I64(18), Value::String("%@gmail.com".to_string())],
        "[{}]",
        h.name,
    );
}

fn check_owned_select_matches_borrowed(h: &Harness) {
    let select =
        Select::from_table("users")
            .item(SelectItem::from(ColumnMarker::new("users", "id")))
            .filter(Expr::column("age").ge(Expr::param(Value::I64(18))).and(
                Expr::column("email").like(Expr::param(Value::String("%@gmail.com".to_string()))),
            ))
            .order_by("users__id", OrderDir::Desc)
            .build()
            .unwrap();

    let expected = h.dialect.render_select(&select).unwrap();
    let actual = h.dialect.render_select_owned(select).unwrap();
    assert_eq!(actual, expected, "[{}]", h.name);
}

fn check_owned_insert_matches_borrowed(h: &Harness) {
    let insert = Insert::into_table("users")
        .columns(vec![
            ColumnMarker::new("users", "email"),
            ColumnMarker::new("users", "name"),
        ])
        .values(vec![
            Value::String("alice@example.com".to_string()),
            Value::String("Alice".to_string()),
        ])
        .values(vec![
            Value::String("bob@example.com".to_string()),
            Value::String("Bob".to_string()),
        ])
        .returning(vec![ColumnMarker::new("users", "id")])
        .build()
        .unwrap();

    let expected = h.dialect.render_insert(&insert).unwrap();
    let actual = h.dialect.render_insert_owned(insert).unwrap();
    assert_eq!(actual, expected, "[{}]", h.name);
}

fn check_owned_update_matches_borrowed(h: &Harness) {
    let update = Update::table("users")
        .set(
            ColumnMarker::new("users", "email"),
            Value::String("new@example.com".to_string()),
        )
        .set(ColumnMarker::new("users", "name"), Value::Null)
        .filter(Expr::column("id").eq(Expr::param(Value::I64(7))))
        .returning(vec![ColumnMarker::new("users", "id")])
        .build()
        .unwrap();

    let expected = h.dialect.render_update(&update).unwrap();
    let actual = h.dialect.render_update_owned(update).unwrap();
    assert_eq!(actual, expected, "[{}]", h.name);
}

fn check_owned_delete_matches_borrowed(h: &Harness) {
    let delete = Delete::from_table("users")
        .filter(
            Expr::column("id")
                .eq(Expr::param(Value::I64(7)))
                .and(Expr::column("active").eq(Expr::param(Value::Bool(true)))),
        )
        .returning(vec![ColumnMarker::new("users", "id")])
        .build()
        .unwrap();

    let expected = h.dialect.render_delete(&delete).unwrap();
    let actual = h.dialect.render_delete_owned(delete).unwrap();
    assert_eq!(actual, expected, "[{}]", h.name);
}

fn check_order_by(h: &Harness) {
    let select = Select::from_table("users")
        .order_by("id", OrderDir::Desc)
        .order_by("email", OrderDir::Asc)
        .build()
        .unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT * FROM {} ORDER BY {} DESC, {} ASC",
            (h.q)("users"),
            (h.q)("id"),
            (h.q)("email"),
        ),
        "[{}]",
        h.name
    );
    assert!(sql.params.is_empty(), "[{}]", h.name);
}

fn check_qualified_order_by_without_projection_alias(h: &Harness) {
    let q = h.q;
    let select = Select::from_table("users")
        .computed(Expr::function_call("COUNT", vec![Expr::star()]), "total")
        .order_by("users__id", OrderDir::Asc)
        .build()
        .unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT (COUNT(*)) AS {} FROM {} ORDER BY {}.{} ASC",
            q("total"),
            q("users"),
            q("users"),
            q("id"),
        ),
        "[{}]",
        h.name
    );
    assert!(sql.params.is_empty(), "[{}]", h.name);
}

fn check_mixed_order_by_preserves_sequence(h: &Harness) {
    let q = h.q;
    let select = Select::from_table("metrics")
        .items(vec![
            SelectItem::from(ColumnMarker::new("metrics", "bucket")),
            SelectItem::from(ColumnMarker::new("metrics", "label")),
        ])
        .computed(
            Expr::function_call("SUM", vec![Expr::column("metrics__points")]),
            "total_points",
        )
        .group_by(vec![
            ColumnMarker::new("metrics", "bucket"),
            ColumnMarker::new("metrics", "label"),
        ])
        .order_by("metrics__bucket", OrderDir::Asc)
        .order_by_expr(
            Expr::function_call("SUM", vec![Expr::column("metrics__points")]),
            OrderDir::Desc,
        )
        .order_by("metrics__label", OrderDir::Asc)
        .build()
        .unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT {}.{} AS {}, {}.{} AS {}, (SUM({}.{})) AS {} FROM {} GROUP BY {}.{}, {}.{} ORDER BY {}.{} ASC, SUM({}.{}) DESC, {}.{} ASC",
            q("metrics"),
            q("bucket"),
            q("metrics__bucket"),
            q("metrics"),
            q("label"),
            q("metrics__label"),
            q("metrics"),
            q("points"),
            q("total_points"),
            q("metrics"),
            q("metrics"),
            q("bucket"),
            q("metrics"),
            q("label"),
            q("metrics"),
            q("bucket"),
            q("metrics"),
            q("points"),
            q("metrics"),
            q("label"),
        ),
        "[{}]",
        h.name
    );
    assert!(sql.params.is_empty(), "[{}]", h.name);
}

fn check_take_and_skip(h: &Harness) {
    let select = Select::from_table("users")
        .take(10)
        .skip(20)
        .build()
        .unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!("SELECT * FROM {} LIMIT 10 OFFSET 20", (h.q)("users")),
        "[{}]",
        h.name
    );
    assert!(sql.params.is_empty(), "[{}]", h.name);
}

fn check_not_operator(h: &Harness) {
    let filter = !Expr::column("deleted").eq(Expr::param(Value::Bool(true)));
    let select = Select::from_table("users").filter(filter).build().unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT * FROM {} WHERE NOT (({} = {}))",
            (h.q)("users"),
            (h.q)("deleted"),
            (h.p)(1),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(sql.params, vec![Value::Bool(true)], "[{}]", h.name);
}

fn check_all_features(h: &Harness) {
    let q = h.q;
    let filter = Expr::column("age")
        .ge(Expr::param(Value::I64(21)))
        .and(Expr::column("active").eq(Expr::param(Value::Bool(true))));
    let select = Select::from_table("users")
        .items(vec![
            SelectItem::from(ColumnMarker::new("users", "id")),
            SelectItem::from(ColumnMarker::new("users", "name")),
            SelectItem::from(ColumnMarker::new("users", "email")),
        ])
        .filter(filter)
        .order_by("name", OrderDir::Asc)
        .take(50)
        .skip(100)
        .build()
        .unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT {}.{} AS {}, {}.{} AS {}, {}.{} AS {} FROM {} WHERE (({} >= {}) AND ({} = {})) ORDER BY {} ASC LIMIT 50 OFFSET 100",
            q("users"), q("id"),    q("users__id"),
            q("users"), q("name"),  q("users__name"),
            q("users"), q("email"), q("users__email"),
            q("users"),
            q("age"),    (h.p)(1),
            q("active"), (h.p)(2),
            q("name"),
        ),
        "[{}]", h.name
    );
    assert_eq!(
        sql.params,
        vec![Value::I64(21), Value::Bool(true)],
        "[{}]",
        h.name
    );
}

fn check_multiple_operators(h: &Harness) {
    let filter = Expr::column("price")
        .gt(Expr::param(Value::F64(10.0)))
        .and(Expr::column("price").lt(Expr::param(Value::F64(100.0))));
    let select = Select::from_table("products")
        .filter(filter)
        .build()
        .unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT * FROM {} WHERE (({} > {}) AND ({} < {}))",
            (h.q)("products"),
            (h.q)("price"),
            (h.p)(1),
            (h.q)("price"),
            (h.p)(2),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(
        sql.params,
        vec![Value::F64(10.0), Value::F64(100.0)],
        "[{}]",
        h.name
    );
}

fn check_in_operator(h: &Harness) {
    let expr = Expr::column("status").in_list(vec![
        Expr::param(Value::String("active".to_string())),
        Expr::param(Value::String("pending".to_string())),
        Expr::param(Value::String("approved".to_string())),
    ]);
    let select = Select::from_table("users").filter(expr).build().unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT * FROM {} WHERE ({} IN ({}, {}, {}))",
            (h.q)("users"),
            (h.q)("status"),
            (h.p)(1),
            (h.p)(2),
            (h.p)(3),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(sql.params.len(), 3, "[{}]", h.name);
    assert_eq!(sql.params[0], Value::String("active".to_string()));
    assert_eq!(sql.params[1], Value::String("pending".to_string()));
    assert_eq!(sql.params[2], Value::String("approved".to_string()));
}

fn check_not_in_operator(h: &Harness) {
    let expr = Expr::column("role").not_in_list(vec![
        Expr::param(Value::String("admin".to_string())),
        Expr::param(Value::String("superuser".to_string())),
    ]);
    let select = Select::from_table("users").filter(expr).build().unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT * FROM {} WHERE ({} NOT IN ({}, {}))",
            (h.q)("users"),
            (h.q)("role"),
            (h.p)(1),
            (h.p)(2),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(sql.params.len(), 2, "[{}]", h.name);
    assert_eq!(sql.params[0], Value::String("admin".to_string()));
    assert_eq!(sql.params[1], Value::String("superuser".to_string()));
}

fn check_in_operator_integers(h: &Harness) {
    let expr = Expr::column("id").in_list(vec![
        Expr::param(Value::I64(1)),
        Expr::param(Value::I64(2)),
        Expr::param(Value::I64(5)),
    ]);
    let select = Select::from_table("users").filter(expr).build().unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT * FROM {} WHERE ({} IN ({}, {}, {}))",
            (h.q)("users"),
            (h.q)("id"),
            (h.p)(1),
            (h.p)(2),
            (h.p)(3),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(sql.params.len(), 3, "[{}]", h.name);
    assert_eq!(sql.params[0], Value::I64(1));
    assert_eq!(sql.params[1], Value::I64(2));
    assert_eq!(sql.params[2], Value::I64(5));
}

fn check_insert_single_row(h: &Harness) {
    let insert = Insert::into_table("users")
        .column(ColumnMarker::new("users", "email"))
        .values(vec![Value::String("alice@example.com".to_string())])
        .build()
        .unwrap();
    let sql = h.dialect.render_insert(&insert).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            (h.q)("users"),
            (h.q)("email"),
            (h.p)(1),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(
        sql.params,
        vec![Value::String("alice@example.com".to_string())],
        "[{}]",
        h.name
    );
}

fn check_insert_multi_column(h: &Harness) {
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
    let sql = h.dialect.render_insert(&insert).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "INSERT INTO {} ({}, {}) VALUES ({}, {})",
            (h.q)("users"),
            (h.q)("email"),
            (h.q)("name"),
            (h.p)(1),
            (h.p)(2),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(
        sql.params,
        vec![
            Value::String("alice@example.com".to_string()),
            Value::String("Alice".to_string()),
        ],
        "[{}]",
        h.name,
    );
}

fn check_insert_batch(h: &Harness) {
    let insert = Insert::into_table("users")
        .column(ColumnMarker::new("users", "email"))
        .values(vec![Value::String("alice@example.com".to_string())])
        .values(vec![Value::String("bob@example.com".to_string())])
        .values(vec![Value::String("charlie@example.com".to_string())])
        .build()
        .unwrap();
    let sql = h.dialect.render_insert(&insert).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "INSERT INTO {} ({}) VALUES ({}), ({}), ({})",
            (h.q)("users"),
            (h.q)("email"),
            (h.p)(1),
            (h.p)(2),
            (h.p)(3),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(
        sql.params,
        vec![
            Value::String("alice@example.com".to_string()),
            Value::String("bob@example.com".to_string()),
            Value::String("charlie@example.com".to_string()),
        ],
        "[{}]",
        h.name,
    );
}

fn check_insert_null_value(h: &Harness) {
    let insert = Insert::into_table("users")
        .columns(vec![
            ColumnMarker::new("users", "email"),
            ColumnMarker::new("users", "name"),
        ])
        .values(vec![
            Value::String("alice@example.com".to_string()),
            Value::Null,
        ])
        .build()
        .unwrap();
    let sql = h.dialect.render_insert(&insert).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "INSERT INTO {} ({}, {}) VALUES ({}, NULL)",
            (h.q)("users"),
            (h.q)("email"),
            (h.q)("name"),
            (h.p)(1),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(
        sql.params,
        vec![Value::String("alice@example.com".to_string())],
        "[{}]",
        h.name
    );
}

fn check_insert_with_returning(h: &Harness) {
    let insert = Insert::into_table("users")
        .column(ColumnMarker::new("users", "email"))
        .values(vec![Value::String("alice@example.com".to_string())])
        .returning(vec![
            ColumnMarker::new("users", "id"),
            ColumnMarker::new("users", "email"),
        ])
        .build()
        .unwrap();
    let sql = h.dialect.render_insert(&insert).unwrap();
    let q = h.q;
    assert_eq!(
        sql.text,
        format!(
            "INSERT INTO {} ({}) VALUES ({}) RETURNING {}.{} AS {}, {}.{} AS {}",
            q("users"),
            q("email"),
            (h.p)(1),
            q("users"),
            q("id"),
            q("users__id"),
            q("users"),
            q("email"),
            q("users__email"),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(
        sql.params,
        vec![Value::String("alice@example.com".to_string())],
        "[{}]",
        h.name
    );
}

fn check_insert_batch_with_returning(h: &Harness) {
    let insert = Insert::into_table("users")
        .column(ColumnMarker::new("users", "email"))
        .values(vec![Value::String("alice@example.com".to_string())])
        .values(vec![Value::String("bob@example.com".to_string())])
        .returning(vec![
            ColumnMarker::new("users", "id"),
            ColumnMarker::new("users", "email"),
        ])
        .build()
        .unwrap();
    let sql = h.dialect.render_insert(&insert).unwrap();
    let q = h.q;
    assert_eq!(
        sql.text,
        format!(
            "INSERT INTO {} ({}) VALUES ({}), ({}) RETURNING {}.{} AS {}, {}.{} AS {}",
            q("users"),
            q("email"),
            (h.p)(1),
            (h.p)(2),
            q("users"),
            q("id"),
            q("users__id"),
            q("users"),
            q("email"),
            q("users__email"),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(
        sql.params,
        vec![
            Value::String("alice@example.com".to_string()),
            Value::String("bob@example.com".to_string()),
        ],
        "[{}]",
        h.name,
    );
}

fn check_update_simple(h: &Harness) {
    let update = Update::table("users")
        .set(
            ColumnMarker::new("users", "email"),
            Value::String("new@example.com".to_string()),
        )
        .build()
        .unwrap();
    let sql = h.dialect.render_update(&update).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "UPDATE {} SET {} = {}",
            (h.q)("users"),
            (h.q)("email"),
            (h.p)(1)
        ),
        "[{}]",
        h.name
    );
    assert_eq!(
        sql.params,
        vec![Value::String("new@example.com".to_string())],
        "[{}]",
        h.name
    );
}

fn check_update_multi_set(h: &Harness) {
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
    let sql = h.dialect.render_update(&update).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "UPDATE {} SET {} = {}, {} = {}",
            (h.q)("users"),
            (h.q)("email"),
            (h.p)(1),
            (h.q)("name"),
            (h.p)(2),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(
        sql.params,
        vec![
            Value::String("new@example.com".to_string()),
            Value::String("Alice".to_string()),
        ],
        "[{}]",
        h.name,
    );
}

fn check_update_with_where(h: &Harness) {
    let update = Update::table("users")
        .set(
            ColumnMarker::new("users", "email"),
            Value::String("new@example.com".to_string()),
        )
        .filter(Expr::column("id").eq(Expr::param(Value::I64(42))))
        .build()
        .unwrap();
    let sql = h.dialect.render_update(&update).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "UPDATE {} SET {} = {} WHERE ({} = {})",
            (h.q)("users"),
            (h.q)("email"),
            (h.p)(1),
            (h.q)("id"),
            (h.p)(2),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(
        sql.params,
        vec![Value::String("new@example.com".to_string()), Value::I64(42)],
        "[{}]",
        h.name,
    );
}

fn check_update_with_returning(h: &Harness) {
    let update = Update::table("users")
        .set(
            ColumnMarker::new("users", "email"),
            Value::String("new@example.com".to_string()),
        )
        .filter(Expr::column("id").eq(Expr::param(Value::I64(1))))
        .returning(vec![
            ColumnMarker::new("users", "id"),
            ColumnMarker::new("users", "email"),
        ])
        .build()
        .unwrap();
    let sql = h.dialect.render_update(&update).unwrap();
    let q = h.q;
    assert_eq!(
        sql.text,
        format!(
            "UPDATE {} SET {} = {} WHERE ({} = {}) RETURNING {}.{} AS {}, {}.{} AS {}",
            q("users"),
            q("email"),
            (h.p)(1),
            q("id"),
            (h.p)(2),
            q("users"),
            q("id"),
            q("users__id"),
            q("users"),
            q("email"),
            q("users__email"),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(
        sql.params,
        vec![Value::String("new@example.com".to_string()), Value::I64(1)],
        "[{}]",
        h.name,
    );
}

fn check_update_null_value(h: &Harness) {
    let update = Update::table("users")
        .set(ColumnMarker::new("users", "name"), Value::Null)
        .filter(Expr::column("id").eq(Expr::param(Value::I64(1))))
        .build()
        .unwrap();
    let sql = h.dialect.render_update(&update).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "UPDATE {} SET {} = NULL WHERE ({} = {})",
            (h.q)("users"),
            (h.q)("name"),
            (h.q)("id"),
            (h.p)(1),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(sql.params, vec![Value::I64(1)], "[{}]", h.name);
}

fn check_delete_simple(h: &Harness) {
    let delete = Delete::from_table("users").build().unwrap();
    let sql = h.dialect.render_delete(&delete).unwrap();
    assert_eq!(
        sql.text,
        format!("DELETE FROM {}", (h.q)("users")),
        "[{}]",
        h.name
    );
    assert!(sql.params.is_empty(), "[{}]", h.name);
}

fn check_delete_with_where(h: &Harness) {
    let delete = Delete::from_table("users")
        .filter(Expr::column("id").eq(Expr::param(Value::I64(42))))
        .build()
        .unwrap();
    let sql = h.dialect.render_delete(&delete).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "DELETE FROM {} WHERE ({} = {})",
            (h.q)("users"),
            (h.q)("id"),
            (h.p)(1),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(sql.params, vec![Value::I64(42)], "[{}]", h.name);
}

fn check_delete_with_complex_where(h: &Harness) {
    let filter = Expr::column("id")
        .ge(Expr::param(Value::I64(10)))
        .and(Expr::column("active").eq(Expr::param(Value::Bool(false))));
    let delete = Delete::from_table("users").filter(filter).build().unwrap();
    let sql = h.dialect.render_delete(&delete).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "DELETE FROM {} WHERE (({} >= {}) AND ({} = {}))",
            (h.q)("users"),
            (h.q)("id"),
            (h.p)(1),
            (h.q)("active"),
            (h.p)(2),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(
        sql.params,
        vec![Value::I64(10), Value::Bool(false)],
        "[{}]",
        h.name
    );
}

fn check_delete_with_returning(h: &Harness) {
    let delete = Delete::from_table("users")
        .filter(Expr::column("id").eq(Expr::param(Value::I64(1))))
        .returning(vec![
            ColumnMarker::new("users", "id"),
            ColumnMarker::new("users", "email"),
        ])
        .build()
        .unwrap();
    let sql = h.dialect.render_delete(&delete).unwrap();
    let q = h.q;
    assert_eq!(
        sql.text,
        format!(
            "DELETE FROM {} WHERE ({} = {}) RETURNING {}.{} AS {}, {}.{} AS {}",
            q("users"),
            q("id"),
            (h.p)(1),
            q("users"),
            q("id"),
            q("users__id"),
            q("users"),
            q("email"),
            q("users__email"),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(sql.params, vec![Value::I64(1)], "[{}]", h.name);
}

fn check_inner_join(h: &Harness) {
    let q = h.q;
    let on = Expr::column("users__id").eq(Expr::column("posts__user_id"));
    let select = Select::from_table("users")
        .item(SelectItem::from(ColumnMarker::new("users", "id")))
        .item(SelectItem::from(ColumnMarker::new("users", "email")))
        .inner_join(
            "posts",
            on,
            vec![
                SelectItem::from(ColumnMarker::new("posts", "id")),
                SelectItem::from(ColumnMarker::new("posts", "title")),
            ],
        )
        .build()
        .unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT {}.{} AS {}, {}.{} AS {}, {}.{} AS {}, {}.{} AS {} \
             FROM {} \
             INNER JOIN {} ON ({}.{} = {}.{})",
            q("users"),
            q("id"),
            q("users__id"),
            q("users"),
            q("email"),
            q("users__email"),
            q("posts"),
            q("id"),
            q("posts__id"),
            q("posts"),
            q("title"),
            q("posts__title"),
            q("users"),
            q("posts"),
            q("users"),
            q("id"),
            q("posts"),
            q("user_id"),
        ),
        "[{}]",
        h.name
    );
    assert!(sql.params.is_empty(), "[{}]", h.name);
}

fn check_left_join(h: &Harness) {
    let q = h.q;
    let on = Expr::column("users__id").eq(Expr::column("posts__user_id"));
    let select = Select::from_table("users")
        .item(SelectItem::from(ColumnMarker::new("users", "id")))
        .left_join(
            "posts",
            on,
            vec![SelectItem::from(ColumnMarker::new("posts", "title"))],
        )
        .build()
        .unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT {}.{} AS {}, {}.{} AS {} \
             FROM {} \
             LEFT JOIN {} ON ({}.{} = {}.{})",
            q("users"),
            q("id"),
            q("users__id"),
            q("posts"),
            q("title"),
            q("posts__title"),
            q("users"),
            q("posts"),
            q("users"),
            q("id"),
            q("posts"),
            q("user_id"),
        ),
        "[{}]",
        h.name
    );
    assert!(sql.params.is_empty(), "[{}]", h.name);
}

fn check_join_with_filter(h: &Harness) {
    let q = h.q;
    let on = Expr::column("users__id").eq(Expr::column("posts__user_id"));
    let filter = Expr::column("users__id").eq(Expr::param(Value::I64(42)));
    let select = Select::from_table("users")
        .item(SelectItem::from(ColumnMarker::new("users", "id")))
        .inner_join(
            "posts",
            on,
            vec![SelectItem::from(ColumnMarker::new("posts", "title"))],
        )
        .filter(filter)
        .build()
        .unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT {}.{} AS {}, {}.{} AS {} \
             FROM {} \
             INNER JOIN {} ON ({}.{} = {}.{}) \
             WHERE ({}.{} = {})",
            q("users"),
            q("id"),
            q("users__id"),
            q("posts"),
            q("title"),
            q("posts__title"),
            q("users"),
            q("posts"),
            q("users"),
            q("id"),
            q("posts"),
            q("user_id"),
            q("users"),
            q("id"),
            (h.p)(1),
        ),
        "[{}]",
        h.name
    );
    assert_eq!(sql.params, vec![Value::I64(42)], "[{}]", h.name);
}

fn check_multiple_joins(h: &Harness) {
    let q = h.q;
    let select = Select::from_table("users")
        .item(SelectItem::from(ColumnMarker::new("users", "id")))
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
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT {}.{} AS {}, {}.{} AS {}, {}.{} AS {} \
             FROM {} \
             INNER JOIN {} ON ({}.{} = {}.{}) \
             LEFT JOIN {} ON ({}.{} = {}.{})",
            q("users"),
            q("id"),
            q("users__id"),
            q("posts"),
            q("title"),
            q("posts__title"),
            q("comments"),
            q("body"),
            q("comments__body"),
            q("users"),
            q("posts"),
            q("users"),
            q("id"),
            q("posts"),
            q("user_id"),
            q("comments"),
            q("posts"),
            q("id"),
            q("comments"),
            q("post_id"),
        ),
        "[{}]",
        h.name
    );
    assert!(sql.params.is_empty(), "[{}]", h.name);
}

fn check_join_with_order_and_take(h: &Harness) {
    let q = h.q;
    let on = Expr::column("users__id").eq(Expr::column("posts__user_id"));
    let select = Select::from_table("users")
        .item(SelectItem::from(ColumnMarker::new("users", "id")))
        .inner_join(
            "posts",
            on,
            vec![SelectItem::from(ColumnMarker::new("posts", "title"))],
        )
        .order_by("users__id", OrderDir::Desc)
        .take(10)
        .build()
        .unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        format!(
            "SELECT {}.{} AS {}, {}.{} AS {} \
             FROM {} \
             INNER JOIN {} ON ({}.{} = {}.{}) \
             ORDER BY {}.{} DESC LIMIT 10",
            q("users"),
            q("id"),
            q("users__id"),
            q("posts"),
            q("title"),
            q("posts__title"),
            q("users"),
            q("posts"),
            q("users"),
            q("id"),
            q("posts"),
            q("user_id"),
            q("users"),
            q("id"),
        ),
        "[{}]",
        h.name
    );
    assert!(sql.params.is_empty(), "[{}]", h.name);
}

fn check_join_star_columns(h: &Harness) {
    let on = Expr::column("users__id").eq(Expr::column("posts__user_id"));
    let select = Select::from_table("users")
        .inner_join("posts", on, vec![])
        .build()
        .unwrap();
    let sql = h.dialect.render_select(&select).unwrap();
    let q = h.q;
    assert_eq!(
        sql.text,
        format!(
            "SELECT * FROM {} INNER JOIN {} ON ({}.{} = {}.{})",
            q("users"),
            q("posts"),
            q("users"),
            q("id"),
            q("posts"),
            q("user_id"),
        ),
        "[{}]",
        h.name
    );
    assert!(sql.params.is_empty(), "[{}]", h.name);
}

#[test]
fn select_star() {
    for h in all_harnesses() {
        check_select_star(&h);
    }
}

#[test]
fn select_columns() {
    for h in all_harnesses() {
        check_select_columns(&h);
    }
}

#[test]
fn where_clause() {
    for h in all_harnesses() {
        check_where_clause(&h);
    }
}

#[test]
fn complex_where() {
    for h in all_harnesses() {
        check_complex_where(&h);
    }
}

#[test]
fn order_by() {
    for h in all_harnesses() {
        check_order_by(&h);
    }
}

#[test]
fn qualified_order_by_without_projection_alias() {
    for h in all_harnesses() {
        check_qualified_order_by_without_projection_alias(&h);
    }
}

#[test]
fn mixed_order_by_preserves_sequence() {
    for h in all_harnesses() {
        check_mixed_order_by_preserves_sequence(&h);
    }
}

#[test]
fn take_and_skip() {
    for h in all_harnesses() {
        check_take_and_skip(&h);
    }
}

#[test]
fn not_operator() {
    for h in all_harnesses() {
        check_not_operator(&h);
    }
}

#[test]
fn all_features() {
    for h in all_harnesses() {
        check_all_features(&h);
    }
}

#[test]
fn multiple_operators() {
    for h in all_harnesses() {
        check_multiple_operators(&h);
    }
}

#[test]
fn in_operator() {
    for h in all_harnesses() {
        check_in_operator(&h);
    }
}

#[test]
fn not_in_operator() {
    for h in all_harnesses() {
        check_not_in_operator(&h);
    }
}

#[test]
fn in_operator_integers() {
    for h in all_harnesses() {
        check_in_operator_integers(&h);
    }
}

#[test]
fn insert_single_row() {
    for h in all_harnesses() {
        check_insert_single_row(&h);
    }
}

#[test]
fn insert_multi_column() {
    for h in all_harnesses() {
        check_insert_multi_column(&h);
    }
}

#[test]
fn insert_batch() {
    for h in all_harnesses() {
        check_insert_batch(&h);
    }
}

#[test]
fn insert_null_value() {
    for h in all_harnesses() {
        check_insert_null_value(&h);
    }
}

#[test]
fn insert_with_returning() {
    for h in returning_harnesses() {
        check_insert_with_returning(&h);
    }
}

#[test]
fn insert_batch_with_returning() {
    for h in returning_harnesses() {
        check_insert_batch_with_returning(&h);
    }
}

#[test]
fn update_simple() {
    for h in all_harnesses() {
        check_update_simple(&h);
    }
}

#[test]
fn update_multi_set() {
    for h in all_harnesses() {
        check_update_multi_set(&h);
    }
}

#[test]
fn update_with_where() {
    for h in all_harnesses() {
        check_update_with_where(&h);
    }
}

#[test]
fn update_with_returning() {
    for h in returning_harnesses() {
        check_update_with_returning(&h);
    }
}

#[test]
fn update_null_value() {
    for h in all_harnesses() {
        check_update_null_value(&h);
    }
}

#[test]
fn delete_simple() {
    for h in all_harnesses() {
        check_delete_simple(&h);
    }
}

#[test]
fn delete_with_where() {
    for h in all_harnesses() {
        check_delete_with_where(&h);
    }
}

#[test]
fn delete_with_complex_where() {
    for h in all_harnesses() {
        check_delete_with_complex_where(&h);
    }
}

#[test]
fn delete_with_returning() {
    for h in returning_harnesses() {
        check_delete_with_returning(&h);
    }
}

#[test]
fn inner_join() {
    for h in all_harnesses() {
        check_inner_join(&h);
    }
}

#[test]
fn left_join() {
    for h in all_harnesses() {
        check_left_join(&h);
    }
}

#[test]
fn join_with_filter() {
    for h in all_harnesses() {
        check_join_with_filter(&h);
    }
}

#[test]
fn multiple_joins() {
    for h in all_harnesses() {
        check_multiple_joins(&h);
    }
}

#[test]
fn join_with_order_and_take() {
    for h in all_harnesses() {
        check_join_with_order_and_take(&h);
    }
}

#[test]
fn join_star_columns() {
    for h in all_harnesses() {
        check_join_star_columns(&h);
    }
}

#[test]
fn supports_returning_true() {
    for h in returning_harnesses() {
        assert!(
            h.dialect.supports_returning(),
            "[{}] expected supports_returning() == true",
            h.name
        );
    }
}

#[test]
fn supports_returning_false() {
    assert!(!MysqlDialect.supports_returning());
}

#[test]
fn owned_select_matches_borrowed_rendering() {
    for h in all_harnesses() {
        check_owned_select_matches_borrowed(&h);
    }
}

#[test]
fn owned_insert_matches_borrowed_rendering() {
    for h in all_harnesses() {
        check_owned_insert_matches_borrowed(&h);
    }
}

#[test]
fn owned_update_matches_borrowed_rendering() {
    for h in all_harnesses() {
        check_owned_update_matches_borrowed(&h);
    }
}

#[test]
fn owned_delete_matches_borrowed_rendering() {
    for h in all_harnesses() {
        check_owned_delete_matches_borrowed(&h);
    }
}

#[test]
fn postgres_distinct_on_uses_qualified_identifier_splitting() {
    let dialect = PostgresDialect;
    let select = Select::from_table("users")
        .distinct(vec!["users__profile__slug".to_string()])
        .build()
        .unwrap();

    let sql = dialect.render_select(&select).unwrap();
    assert_eq!(
        sql.text,
        "SELECT DISTINCT ON (\"users\".\"profile__slug\") * FROM \"users\""
    );
}
