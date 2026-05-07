//! Integration tests for SQLite executor.
//!
//! These tests use an in-memory SQLite database — no external server required.

use std::time::Duration;

use futures::stream::StreamExt;
use nautilus_connector::{
    execute_all, normalize_rows_with_hints, Client, ConnectorPoolOptions, Executor, SqliteExecutor,
    ValueHint,
};
use nautilus_core::{BinaryOp, Expr, OrderDir, Select, Value};
use nautilus_dialect::{Dialect, Sql, SqliteDialect};
use serde_json::json;

async fn setup_executor() -> nautilus_connector::ConnectorResult<SqliteExecutor> {
    SqliteExecutor::new("sqlite::memory:").await
}

async fn setup_test_table(executor: &SqliteExecutor) -> nautilus_connector::ConnectorResult<()> {
    let create_table = Sql {
        text: r#"
            CREATE TABLE IF NOT EXISTS test_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER,
                score REAL,
                active BOOLEAN,
                data BLOB
            )
        "#
        .to_string(),
        params: vec![],
    };
    execute_all(executor, &create_table).await?;
    Ok(())
}

async fn setup_array_table(executor: &SqliteExecutor) -> nautilus_connector::ConnectorResult<()> {
    let create_table = Sql {
        text: r#"
            CREATE TABLE IF NOT EXISTS test_array_rows (
                id INTEGER PRIMARY KEY,
                tags TEXT NOT NULL
            )
        "#
        .to_string(),
        params: vec![],
    };
    execute_all(executor, &create_table).await?;
    Ok(())
}

#[tokio::test]
async fn test_sqlite_executor_connection() {
    let executor = setup_executor().await;
    assert!(
        executor.is_ok(),
        "Failed to connect to SQLite: {:?}",
        executor.err()
    );
}

#[tokio::test]
async fn test_execute_query_no_results() {
    let executor = setup_executor().await.expect("Failed to create executor");
    setup_test_table(&executor)
        .await
        .expect("Failed to setup table");

    let sql = Sql {
        text: "SELECT * FROM test_users WHERE id = ?".to_string(),
        params: vec![Value::I64(999)],
    };

    let rows = execute_all(&executor, &sql)
        .await
        .expect("Failed to execute query");
    assert_eq!(rows.len(), 0, "Expected no rows");
}

#[tokio::test]
async fn test_insert_and_select() {
    let executor = setup_executor().await.expect("Failed to create executor");
    setup_test_table(&executor)
        .await
        .expect("Failed to setup table");

    let insert = Sql {
        text: "INSERT INTO test_users (id, name, email, age, score, active, data) VALUES (?, ?, ?, ?, ?, ?, ?)".to_string(),
        params: vec![
            Value::I64(1),
            Value::String("Alice".to_string()),
            Value::String("alice@example.com".to_string()),
            Value::I64(30),
            Value::F64(95.5),
            Value::Bool(true),
            Value::Bytes(vec![1, 2, 3]),
        ],
    };
    execute_all(&executor, &insert)
        .await
        .expect("Failed to insert");

    let select = Sql {
        text: "SELECT id, name, email, age, score, active, data FROM test_users WHERE id = ?"
            .to_string(),
        params: vec![Value::I64(1)],
    };

    let rows = execute_all(&executor, &select)
        .await
        .expect("Failed to select");
    assert_eq!(rows.len(), 1, "Expected 1 row");

    let row = &rows[0];
    assert_eq!(row.get("id"), Some(&Value::I64(1)));
    assert_eq!(row.get("name"), Some(&Value::String("Alice".to_string())));
    assert_eq!(
        row.get("email"),
        Some(&Value::String("alice@example.com".to_string()))
    );
    assert_eq!(row.get("age"), Some(&Value::I64(30)));
    assert_eq!(row.get("score"), Some(&Value::F64(95.5)));
    assert_eq!(row.get("active"), Some(&Value::Bool(true)));
    assert_eq!(row.get("data"), Some(&Value::Bytes(vec![1, 2, 3])));
}

#[tokio::test]
async fn test_row_positional_access() {
    let executor = setup_executor().await.expect("Failed to create executor");
    setup_test_table(&executor)
        .await
        .expect("Failed to setup table");

    let insert = Sql {
        text: "INSERT INTO test_users (id, name, age, score, active) VALUES (?, ?, ?, ?, ?)"
            .to_string(),
        params: vec![
            Value::I64(2),
            Value::String("Bob".to_string()),
            Value::I64(25),
            Value::F64(88.0),
            Value::Bool(false),
        ],
    };
    execute_all(&executor, &insert)
        .await
        .expect("Failed to insert");

    let select = Sql {
        text: "SELECT id, name, email FROM test_users WHERE id = ?".to_string(),
        params: vec![Value::I64(2)],
    };

    let rows = execute_all(&executor, &select)
        .await
        .expect("Failed to select");
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row.get_by_pos(0), Some(&Value::I64(2)));
    assert_eq!(row.get_by_pos(1), Some(&Value::String("Bob".to_string())));
    assert_eq!(row.get_by_pos(2), Some(&Value::Null));
    assert_eq!(row.get_by_pos(3), None);
}

#[tokio::test]
async fn test_row_iterator() {
    let executor = setup_executor().await.expect("Failed to create executor");
    setup_test_table(&executor)
        .await
        .expect("Failed to setup table");

    let insert = Sql {
        text: "INSERT INTO test_users (id, name, email, age, score, active, data) VALUES (?, ?, ?, ?, ?, ?, ?)".to_string(),
        params: vec![
            Value::I64(3),
            Value::String("Charlie".to_string()),
            Value::String("charlie@example.com".to_string()),
            Value::I64(35),
            Value::F64(92.0),
            Value::Bool(true),
            Value::Bytes(vec![]),
        ],
    };
    execute_all(&executor, &insert)
        .await
        .expect("Failed to insert");

    let select = Sql {
        text: "SELECT id, name FROM test_users WHERE id = ?".to_string(),
        params: vec![Value::I64(3)],
    };

    let rows = execute_all(&executor, &select)
        .await
        .expect("Failed to select");
    let row = &rows[0];

    let columns: Vec<_> = row.iter().collect();
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].0, "id");
    assert_eq!(columns[0].1, &Value::I64(3));
    assert_eq!(columns[1].0, "name");
    assert_eq!(columns[1].1, &Value::String("Charlie".to_string()));
}

#[tokio::test]
async fn test_null_values() {
    let executor = setup_executor().await.expect("Failed to create executor");
    setup_test_table(&executor)
        .await
        .expect("Failed to setup table");

    let insert = Sql {
        text: "INSERT INTO test_users (id, name) VALUES (?, ?)".to_string(),
        params: vec![Value::I64(4), Value::String("David".to_string())],
    };
    execute_all(&executor, &insert)
        .await
        .expect("Failed to insert");

    let select = Sql {
        text: "SELECT id, name, email, age, score, active, data FROM test_users WHERE id = ?"
            .to_string(),
        params: vec![Value::I64(4)],
    };

    let rows = execute_all(&executor, &select)
        .await
        .expect("Failed to select");
    let row = &rows[0];

    assert_eq!(row.get("id"), Some(&Value::I64(4)));
    assert_eq!(row.get("name"), Some(&Value::String("David".to_string())));
    assert_eq!(row.get("email"), Some(&Value::Null));
    assert_eq!(row.get("age"), Some(&Value::Null));
    assert_eq!(row.get("score"), Some(&Value::Null));
    assert_eq!(row.get("active"), Some(&Value::Null));
    assert_eq!(row.get("data"), Some(&Value::Null));
}

#[tokio::test]
async fn test_sqlite_array_operators_preserve_json_nulls() {
    let executor = setup_executor().await.expect("Failed to create executor");
    setup_array_table(&executor)
        .await
        .expect("Failed to setup array table");

    for (id, tags) in [
        (
            1i64,
            Value::Array(vec![Value::String("rust".to_string()), Value::Null]),
        ),
        (2i64, Value::Array(vec![Value::String("rust".to_string())])),
        (3i64, Value::Array(vec![Value::Null])),
        (4i64, Value::Array(vec![Value::String("go".to_string())])),
    ] {
        let insert = Sql {
            text: "INSERT INTO test_array_rows (id, tags) VALUES (?, ?)".to_string(),
            params: vec![Value::I64(id), tags],
        };
        execute_all(&executor, &insert)
            .await
            .expect("Failed to insert array row");
    }

    let dialect = SqliteDialect;

    let contains_null = Select::from_table("test_array_rows")
        .filter(Expr::Binary {
            left: Box::new(Expr::column("test_array_rows__tags")),
            op: BinaryOp::ArrayContains,
            right: Box::new(Expr::param(Value::Array(vec![Value::Null]))),
        })
        .order_by("id", OrderDir::Asc)
        .build()
        .unwrap();
    let contains_sql = dialect.render_select(&contains_null).unwrap();
    let contains_rows = execute_all(&executor, &contains_sql)
        .await
        .expect("Failed to execute contains-null query");
    let contains_ids: Vec<i64> = contains_rows
        .iter()
        .map(|row| match row.get("id") {
            Some(Value::I64(id)) => *id,
            other => panic!("unexpected id row: {other:?}"),
        })
        .collect();
    assert_eq!(contains_ids, vec![1, 3]);

    let contained_by_null = Select::from_table("test_array_rows")
        .filter(Expr::Binary {
            left: Box::new(Expr::column("test_array_rows__tags")),
            op: BinaryOp::ArrayContainedBy,
            right: Box::new(Expr::param(Value::Array(vec![Value::Null]))),
        })
        .order_by("id", OrderDir::Asc)
        .build()
        .unwrap();
    let contained_by_sql = dialect.render_select(&contained_by_null).unwrap();
    let contained_by_rows = execute_all(&executor, &contained_by_sql)
        .await
        .expect("Failed to execute contained-by-null query");
    let contained_by_ids: Vec<i64> = contained_by_rows
        .iter()
        .map(|row| match row.get("id") {
            Some(Value::I64(id)) => *id,
            other => panic!("unexpected id row: {other:?}"),
        })
        .collect();
    assert_eq!(contained_by_ids, vec![3]);

    let overlaps_null = Select::from_table("test_array_rows")
        .filter(Expr::Binary {
            left: Box::new(Expr::column("test_array_rows__tags")),
            op: BinaryOp::ArrayOverlaps,
            right: Box::new(Expr::param(Value::Array(vec![Value::Null]))),
        })
        .order_by("id", OrderDir::Asc)
        .build()
        .unwrap();
    let overlaps_sql = dialect.render_select(&overlaps_null).unwrap();
    let overlaps_rows = execute_all(&executor, &overlaps_sql)
        .await
        .expect("Failed to execute overlaps-null query");
    let overlaps_ids: Vec<i64> = overlaps_rows
        .iter()
        .map(|row| match row.get("id") {
            Some(Value::I64(id)) => *id,
            other => panic!("unexpected id row: {other:?}"),
        })
        .collect();
    assert_eq!(overlaps_ids, vec![1, 3]);
}

#[tokio::test]
async fn test_multiple_rows() {
    let executor = setup_executor().await.expect("Failed to create executor");
    setup_test_table(&executor)
        .await
        .expect("Failed to setup table");

    for i in 10..15i64 {
        let insert = Sql {
            text: "INSERT INTO test_users (id, name, email, age, score, active) VALUES (?, ?, ?, ?, ?, ?)".to_string(),
            params: vec![
                Value::I64(i),
                Value::String(format!("User{}", i)),
                Value::String(format!("user{}@example.com", i)),
                Value::I64(20 + i),
                Value::F64(80.0 + i as f64),
                Value::Bool(i % 2 == 0),
            ],
        };
        execute_all(&executor, &insert)
            .await
            .expect("Failed to insert");
    }

    let select = Sql {
        text: "SELECT id, name FROM test_users WHERE id >= ? ORDER BY id".to_string(),
        params: vec![Value::I64(10)],
    };

    let rows = execute_all(&executor, &select)
        .await
        .expect("Failed to select");
    assert_eq!(rows.len(), 5);

    for (i, row) in rows.iter().enumerate() {
        let expected_id = 10 + i as i64;
        assert_eq!(row.get("id"), Some(&Value::I64(expected_id)));
        assert_eq!(
            row.get("name"),
            Some(&Value::String(format!("User{}", expected_id)))
        );
    }
}

#[tokio::test]
async fn test_streaming_execution() {
    let executor = setup_executor().await.expect("Failed to create executor");
    setup_test_table(&executor)
        .await
        .expect("Failed to setup table");

    for i in 20..25i64 {
        let insert = Sql {
            text: "INSERT INTO test_users (id, name, email, age, score, active) VALUES (?, ?, ?, ?, ?, ?)".to_string(),
            params: vec![
                Value::I64(i),
                Value::String(format!("StreamUser{}", i)),
                Value::String(format!("stream{}@example.com", i)),
                Value::I64(20 + i),
                Value::F64(80.0 + i as f64),
                Value::Bool(true),
            ],
        };
        execute_all(&executor, &insert)
            .await
            .expect("Failed to insert");
    }

    let select = Sql {
        text: "SELECT id, name FROM test_users WHERE id >= ? ORDER BY id".to_string(),
        params: vec![Value::I64(20)],
    };

    let mut stream = executor.execute(&select);
    let mut count = 0;
    let mut collected_ids = Vec::new();

    while let Some(result) = stream.next().await {
        let row = result.expect("Failed to get row from stream");
        let id = match row.get("id") {
            Some(Value::I64(i)) => *i,
            _ => panic!("Expected I64 id"),
        };
        collected_ids.push(id);
        count += 1;
    }

    assert_eq!(count, 5, "Expected 5 rows from stream");
    assert_eq!(collected_ids, vec![20, 21, 22, 23, 24]);
}

/// Drop a streaming `execute()` mid-iteration and verify the pool stays
/// usable. If the streaming worker failed to drain the underlying sqlx stream
/// after the consumer disappeared, the connection would be returned dirty
/// (or held forever) and subsequent acquires would fail or time out.
#[tokio::test]
async fn test_streaming_drop_mid_iteration_keeps_pool_usable() {
    use std::time::Duration;

    let url = "sqlite::memory:";
    let pool_options = ConnectorPoolOptions::default()
        .max_connections(1)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(2));
    let executor = SqliteExecutor::new_with_options(url, pool_options)
        .await
        .expect("Failed to create executor");

    setup_test_table(&executor)
        .await
        .expect("Failed to setup table");

    for i in 100..120i64 {
        let insert = Sql {
            text: "INSERT INTO test_users (id, name, email, age, score, active) VALUES (?, ?, ?, ?, ?, ?)".to_string(),
            params: vec![
                Value::I64(i),
                Value::String(format!("DropUser{}", i)),
                Value::String(format!("drop{}@example.com", i)),
                Value::I64(20 + i),
                Value::F64(80.0 + i as f64),
                Value::Bool(true),
            ],
        };
        execute_all(&executor, &insert)
            .await
            .expect("Failed to insert");
    }

    let select = Sql {
        text: "SELECT id FROM test_users WHERE id >= ? ORDER BY id".to_string(),
        params: vec![Value::I64(100)],
    };

    // Repeat the abort cycle several times so a leaked connection on any
    // iteration would surface as an acquire timeout below.
    for _ in 0..5 {
        let mut stream = executor.execute(&select);
        // Read a single row, then drop the stream so the worker task hits the
        // drain branch (consumer gone, more rows pending).
        let first = stream.next().await.expect("expected at least one row");
        first.expect("first row should decode");
        drop(stream);

        // Confirm the pool can still service queries.
        let rows = execute_all(&executor, &select)
            .await
            .expect("pool should still acquire after streaming drop");
        assert_eq!(rows.len(), 20);
    }
}

/// Same drain-on-drop guarantee, but going through the detached `execute_owned`
/// path used by codegen `stream_many`. The returned stream is `'static` and
/// independent of `&executor`, but the worker still owns the pool connection,
/// so dropping the stream mid-iteration must release the connection cleanly.
#[tokio::test]
async fn test_execute_owned_drop_mid_iteration_keeps_pool_usable() {
    use std::time::Duration;

    let url = "sqlite::memory:";
    let pool_options = ConnectorPoolOptions::default()
        .max_connections(1)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(2));
    let executor = SqliteExecutor::new_with_options(url, pool_options)
        .await
        .expect("Failed to create executor");

    setup_test_table(&executor)
        .await
        .expect("Failed to setup table");

    for i in 200..220i64 {
        let insert = Sql {
            text: "INSERT INTO test_users (id, name, email, age, score, active) VALUES (?, ?, ?, ?, ?, ?)".to_string(),
            params: vec![
                Value::I64(i),
                Value::String(format!("OwnedUser{}", i)),
                Value::String(format!("owned{}@example.com", i)),
                Value::I64(20 + i),
                Value::F64(80.0 + i as f64),
                Value::Bool(true),
            ],
        };
        execute_all(&executor, &insert)
            .await
            .expect("Failed to insert");
    }

    let select_template = Sql {
        text: "SELECT id FROM test_users WHERE id >= ? ORDER BY id".to_string(),
        params: vec![Value::I64(200)],
    };

    for _ in 0..5 {
        let mut stream = executor.execute_owned(select_template.clone());
        let first = stream
            .next()
            .await
            .expect("expected at least one row from execute_owned");
        first.expect("first row should decode");
        drop(stream);

        let rows = execute_all(&executor, &select_template)
            .await
            .expect("pool should still acquire after execute_owned drop");
        assert_eq!(rows.len(), 20);
    }
}

/// Smoke test for `execute_owned`: the returned stream is detached from the
/// executor reference, can be moved into a spawned task, and yields all rows.
#[tokio::test]
async fn test_execute_owned_yields_all_rows_when_moved_to_task() {
    let executor = setup_executor().await.expect("Failed to create executor");
    setup_test_table(&executor)
        .await
        .expect("Failed to setup table");

    for i in 30..35i64 {
        let insert = Sql {
            text: "INSERT INTO test_users (id, name, email, age, score, active) VALUES (?, ?, ?, ?, ?, ?)".to_string(),
            params: vec![
                Value::I64(i),
                Value::String(format!("OwnedRowUser{}", i)),
                Value::Null,
                Value::I64(20 + i),
                Value::F64(80.0 + i as f64),
                Value::Bool(true),
            ],
        };
        execute_all(&executor, &insert)
            .await
            .expect("Failed to insert");
    }

    let select = Sql {
        text: "SELECT id FROM test_users WHERE id >= ? ORDER BY id".to_string(),
        params: vec![Value::I64(30)],
    };

    let stream = executor.execute_owned(select);
    let collected = tokio::spawn(async move {
        let mut stream = stream;
        let mut ids = Vec::new();
        while let Some(item) = stream.next().await {
            let row = item.expect("row should decode");
            if let Some(Value::I64(id)) = row.get("id") {
                ids.push(*id);
            }
        }
        ids
    })
    .await
    .expect("spawned task should join");

    assert_eq!(collected, vec![30, 31, 32, 33, 34]);
}

#[tokio::test]
async fn test_duplicate_column_names() {
    let executor = setup_executor().await.expect("Failed to create executor");
    setup_test_table(&executor)
        .await
        .expect("Failed to setup table");

    let insert = Sql {
        text:
            "INSERT INTO test_users (id, name, email, age, score, active) VALUES (?, ?, ?, ?, ?, ?)"
                .to_string(),
        params: vec![
            Value::I64(5),
            Value::String("Eve".to_string()),
            Value::String("eve@example.com".to_string()),
            Value::I64(28),
            Value::F64(90.0),
            Value::Bool(true),
        ],
    };
    execute_all(&executor, &insert)
        .await
        .expect("Failed to insert");

    let select = Sql {
        text: "SELECT id, name, id FROM test_users WHERE id = ?".to_string(),
        params: vec![Value::I64(5)],
    };

    let rows = execute_all(&executor, &select)
        .await
        .expect("Failed to select");
    let row = &rows[0];

    assert_eq!(row.get("id"), Some(&Value::I64(5)));
    assert_eq!(row.len(), 3);

    assert_eq!(row.get_by_pos(0), Some(&Value::I64(5)));
    assert_eq!(row.get_by_pos(1), Some(&Value::String("Eve".to_string())));
    assert_eq!(row.get_by_pos(2), Some(&Value::I64(5)));
}

#[tokio::test]
async fn test_returning_clause() {
    let executor = setup_executor().await.expect("Failed to create executor");
    setup_test_table(&executor)
        .await
        .expect("Failed to setup table");

    let insert = Sql {
        text: "INSERT INTO test_users (id, name, email) VALUES (?, ?, ?) RETURNING id, name, email"
            .to_string(),
        params: vec![
            Value::I64(100),
            Value::String("Returning".to_string()),
            Value::String("ret@example.com".to_string()),
        ],
    };

    let rows = execute_all(&executor, &insert)
        .await
        .expect("Failed to insert with RETURNING");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.get("id"), Some(&Value::I64(100)));
    assert_eq!(
        row.get("name"),
        Some(&Value::String("Returning".to_string()))
    );
    assert_eq!(
        row.get("email"),
        Some(&Value::String("ret@example.com".to_string()))
    );
}

#[tokio::test]
async fn test_update_returning() {
    let executor = setup_executor().await.expect("Failed to create executor");
    setup_test_table(&executor)
        .await
        .expect("Failed to setup table");

    let insert = Sql {
        text: "INSERT INTO test_users (id, name, email) VALUES (?, ?, ?)".to_string(),
        params: vec![
            Value::I64(101),
            Value::String("Before".to_string()),
            Value::String("before@example.com".to_string()),
        ],
    };
    execute_all(&executor, &insert)
        .await
        .expect("Failed to insert");

    let update = Sql {
        text: "UPDATE test_users SET name = ?, email = ? WHERE id = ? RETURNING id, name, email"
            .to_string(),
        params: vec![
            Value::String("After".to_string()),
            Value::String("after@example.com".to_string()),
            Value::I64(101),
        ],
    };

    let rows = execute_all(&executor, &update)
        .await
        .expect("Failed to update with RETURNING");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.get("id"), Some(&Value::I64(101)));
    assert_eq!(row.get("name"), Some(&Value::String("After".to_string())));
    assert_eq!(
        row.get("email"),
        Some(&Value::String("after@example.com".to_string()))
    );
}

#[tokio::test]
async fn test_delete_returning() {
    let executor = setup_executor().await.expect("Failed to create executor");
    setup_test_table(&executor)
        .await
        .expect("Failed to setup table");

    let insert = Sql {
        text: "INSERT INTO test_users (id, name, email) VALUES (?, ?, ?)".to_string(),
        params: vec![
            Value::I64(102),
            Value::String("ToDelete".to_string()),
            Value::String("delete@example.com".to_string()),
        ],
    };
    execute_all(&executor, &insert)
        .await
        .expect("Failed to insert");

    let delete = Sql {
        text: "DELETE FROM test_users WHERE id = ? RETURNING id, name, email".to_string(),
        params: vec![Value::I64(102)],
    };

    let rows = execute_all(&executor, &delete)
        .await
        .expect("Failed to delete with RETURNING");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.get("id"), Some(&Value::I64(102)));
    assert_eq!(
        row.get("name"),
        Some(&Value::String("ToDelete".to_string()))
    );
    assert_eq!(
        row.get("email"),
        Some(&Value::String("delete@example.com".to_string()))
    );

    let select = Sql {
        text: "SELECT * FROM test_users WHERE id = ?".to_string(),
        params: vec![Value::I64(102)],
    };
    let rows = execute_all(&executor, &select)
        .await
        .expect("Failed to select");
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_client_sqlite_constructor() {
    let client = Client::sqlite("sqlite::memory:").await;
    assert!(
        client.is_ok(),
        "Failed to create SQLite client: {:?}",
        client.err()
    );
}

#[tokio::test]
async fn test_sqlite_executor_constructor_with_pool_options() {
    let executor = SqliteExecutor::new_with_options(
        "sqlite::memory:",
        ConnectorPoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(3))
            .idle_timeout(None)
            .test_before_acquire(false),
    )
    .await
    .expect("Failed to create SQLite executor with custom pool options");

    let options = executor.pool().options();
    assert_eq!(options.get_max_connections(), 1);
    assert_eq!(options.get_min_connections(), 1);
    assert_eq!(options.get_acquire_timeout(), Duration::from_secs(3));
    assert_eq!(options.get_idle_timeout(), None);
    assert!(!options.get_test_before_acquire());
}

#[tokio::test]
async fn test_client_sqlite_constructor_with_pool_options() {
    let client = Client::sqlite_with_options(
        "sqlite::memory:",
        ConnectorPoolOptions::new().max_connections(1),
    )
    .await
    .expect("Failed to create SQLite client with custom pool options");

    assert_eq!(client.executor().pool().options().get_max_connections(), 1);
}

#[tokio::test]
async fn test_datetime_decoding() {
    use chrono::NaiveDate;

    let executor = setup_executor().await.expect("Failed to create executor");
    let create_table = Sql {
        text: r#"
            CREATE TABLE IF NOT EXISTS typed_values (
                id INTEGER PRIMARY KEY,
                created_at DATETIME NOT NULL
            )
        "#
        .to_string(),
        params: vec![],
    };
    execute_all(&executor, &create_table)
        .await
        .expect("Failed to create typed_values table");

    let expected_dt = NaiveDate::from_ymd_opt(2026, 3, 30)
        .unwrap()
        .and_hms_opt(12, 34, 56)
        .unwrap();

    let insert = Sql {
        text: "INSERT INTO typed_values (id, created_at) VALUES (?, ?)".to_string(),
        params: vec![Value::I64(1), Value::DateTime(expected_dt)],
    };
    execute_all(&executor, &insert)
        .await
        .expect("Failed to insert datetime row");

    let select = Sql {
        text: "SELECT created_at FROM typed_values WHERE id = ?".to_string(),
        params: vec![Value::I64(1)],
    };
    let rows = execute_all(&executor, &select)
        .await
        .expect("Failed to select datetime row");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("created_at"),
        Some(&Value::DateTime(expected_dt))
    );
}

#[tokio::test]
async fn test_decimal_and_json_columns_survive_sqlite_null_type_fallback() {
    let executor = setup_executor().await.expect("Failed to create executor");
    let create_table = Sql {
        text: r#"
            CREATE TABLE IF NOT EXISTS typed_values (
                id INTEGER PRIMARY KEY,
                amount DECIMAL(10, 2) NOT NULL,
                metadata JSON NOT NULL
            )
        "#
        .to_string(),
        params: vec![],
    };
    execute_all(&executor, &create_table)
        .await
        .expect("Failed to create typed_values table");

    let insert = Sql {
        text: "INSERT INTO typed_values (id, amount, metadata) VALUES (?, ?, ?)".to_string(),
        params: vec![
            Value::I64(1),
            Value::String("19.95".to_string()),
            Value::String(r#"{"source":"sqlite","retries":0}"#.to_string()),
        ],
    };
    execute_all(&executor, &insert)
        .await
        .expect("Failed to insert typed row");

    let select = Sql {
        text: "SELECT amount, metadata FROM typed_values WHERE id = ?".to_string(),
        params: vec![Value::I64(1)],
    };
    let rows = execute_all(&executor, &select)
        .await
        .expect("Failed to select typed row");

    assert_eq!(rows.len(), 1);
    assert!(
        !matches!(rows[0].get("amount"), Some(Value::Null) | None),
        "raw SQLite decode should not drop DECIMAL values: {:?}",
        rows[0]
    );
    assert!(
        !matches!(rows[0].get("metadata"), Some(Value::Null) | None),
        "raw SQLite decode should not drop JSON values: {:?}",
        rows[0]
    );

    let normalized =
        normalize_rows_with_hints(rows, &[Some(ValueHint::Decimal), Some(ValueHint::Json)])
            .expect("Failed to normalize typed row");

    assert_eq!(
        normalized[0].get("amount"),
        Some(&Value::Decimal(rust_decimal::Decimal::new(1995, 2)))
    );
    assert_eq!(
        normalized[0].get("metadata"),
        Some(&Value::Json(json!({"source":"sqlite","retries":0})))
    );
}
