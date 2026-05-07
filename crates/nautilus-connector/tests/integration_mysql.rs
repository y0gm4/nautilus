//! Integration tests for MySQL executor.
//!
//! These tests require a running MySQL database.
//! Run `docker-compose up -d` before running these tests, then execute
//! `cargo test -p nautilus-orm-connector --test integration_mysql -- --ignored --test-threads=1`.
//! Optionally override the connection URL with `MYSQL_URL`.

#[path = "common/assertions.rs"]
mod assertions;
#[path = "common/mysql.rs"]
mod mysql_common;

use assertions::{
    assert_duplicate_projection, assert_null_user_row, assert_positional_projection,
    assert_sequential_user_rows, assert_standard_user_row, StandardUserExpectation,
};
use futures::stream::StreamExt;
use nautilus_connector::{execute_all, ConnectorPoolOptions, Executor, MysqlExecutor};
use nautilus_core::Value;
use nautilus_dialect::Sql;

#[tokio::test]
#[ignore = "requires a running MySQL instance (run `docker-compose up -d` first)"]
async fn test_mysql_executor_connection() {
    let executor = mysql_common::setup_executor().await;
    assert!(
        executor.is_ok(),
        "Failed to connect to database: {:?}",
        executor.err()
    );
}

#[tokio::test]
#[ignore = "requires a running MySQL instance (run `docker-compose up -d` first)"]
async fn test_execute_query_no_results() {
    let executor = mysql_common::setup_executor()
        .await
        .expect("Failed to create executor");
    mysql_common::setup_test_users_table(&executor)
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
#[ignore = "requires a running MySQL instance (run `docker-compose up -d` first)"]
async fn test_insert_and_select() {
    let executor = mysql_common::setup_executor()
        .await
        .expect("Failed to create executor");
    mysql_common::setup_test_users_table(&executor)
        .await
        .expect("Failed to setup table");

    let insert = Sql {
        text: "REPLACE INTO test_users (id, name, email, age, score, active, data) VALUES (?, ?, ?, ?, ?, ?, ?)".to_string(),
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
    assert_standard_user_row(
        &rows[0],
        StandardUserExpectation {
            id: 1,
            name: "Alice",
            email: "alice@example.com",
            age: 30,
            score: 95.5,
            active: true,
            data: &[1, 2, 3],
        },
    );
}

#[tokio::test]
#[ignore = "requires a running MySQL instance (run `docker-compose up -d` first)"]
async fn test_row_positional_access() {
    let executor = mysql_common::setup_executor()
        .await
        .expect("Failed to create executor");
    mysql_common::setup_test_users_table(&executor)
        .await
        .expect("Failed to setup table");

    let insert = Sql {
        text: "REPLACE INTO test_users (id, name, age, score, active) VALUES (?, ?, ?, ?, ?)"
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
    assert_positional_projection(&rows[0], 2, "Bob");
}

#[tokio::test]
#[ignore = "requires a running MySQL instance (run `docker-compose up -d` first)"]
async fn test_row_iterator() {
    let executor = mysql_common::setup_executor()
        .await
        .expect("Failed to create executor");
    mysql_common::setup_test_users_table(&executor)
        .await
        .expect("Failed to setup table");

    let insert = Sql {
        text: "REPLACE INTO test_users (id, name, email, age, score, active, data) VALUES (?, ?, ?, ?, ?, ?, ?)".to_string(),
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
#[ignore = "requires a running MySQL instance (run `docker-compose up -d` first)"]
async fn test_null_values() {
    let executor = mysql_common::setup_executor()
        .await
        .expect("Failed to create executor");
    mysql_common::setup_test_users_table(&executor)
        .await
        .expect("Failed to setup table");

    let insert = Sql {
        text: "REPLACE INTO test_users (id, name) VALUES (?, ?)".to_string(),
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
    assert_null_user_row(&rows[0], 4, "David");
}

#[tokio::test]
#[ignore = "requires a running MySQL instance (run `docker-compose up -d` first)"]
async fn test_duplicate_column_names() {
    let executor = mysql_common::setup_executor()
        .await
        .expect("Failed to create executor");
    mysql_common::setup_test_users_table(&executor)
        .await
        .expect("Failed to setup table");

    let insert = Sql {
        text: "REPLACE INTO test_users (id, name, email, age, score, active) VALUES (?, ?, ?, ?, ?, ?)".to_string(),
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
    assert_duplicate_projection(&rows[0], 5, "Eve");
}

#[tokio::test]
#[ignore = "requires a running MySQL instance (run `docker-compose up -d` first)"]
async fn test_multiple_rows() {
    let executor = mysql_common::setup_executor()
        .await
        .expect("Failed to create executor");
    mysql_common::setup_test_users_table(&executor)
        .await
        .expect("Failed to setup table");

    for i in 10..15 {
        let insert = Sql {
            text: "REPLACE INTO test_users (id, name, email, age, score, active) VALUES (?, ?, ?, ?, ?, ?)".to_string(),
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
        text: "SELECT id, name FROM test_users WHERE id >= ? AND id < ? ORDER BY id".to_string(),
        params: vec![Value::I64(10), Value::I64(15)],
    };

    let rows = execute_all(&executor, &select)
        .await
        .expect("Failed to select");
    assert_eq!(rows.len(), 5);
    assert_sequential_user_rows(&rows, 10);
}

#[tokio::test]
#[ignore = "requires a running MySQL instance (run `docker-compose up -d` first)"]
async fn test_streaming_execution() {
    let executor = mysql_common::setup_executor()
        .await
        .expect("Failed to create executor");
    mysql_common::setup_test_users_table(&executor)
        .await
        .expect("Failed to setup table");

    for i in 20..25 {
        let insert = Sql {
            text: "REPLACE INTO test_users (id, name, email, age, score, active) VALUES (?, ?, ?, ?, ?, ?)".to_string(),
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
        text: "SELECT id, name FROM test_users WHERE id >= ? AND id < ? ORDER BY id".to_string(),
        params: vec![Value::I64(20), Value::I64(25)],
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
/// usable. With a single-connection pool, a connection that was leaked or
/// returned dirty after the drop would surface here as an acquire timeout on
/// the follow-up query.
#[tokio::test]
#[ignore = "requires a running MySQL instance (run `docker-compose up -d` first)"]
async fn test_streaming_drop_mid_iteration_keeps_pool_usable() {
    use std::time::Duration;

    let pool_options = ConnectorPoolOptions::default()
        .max_connections(1)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(2));
    let executor = MysqlExecutor::new_with_options(&mysql_common::database_url(), pool_options)
        .await
        .expect("Failed to create executor");
    mysql_common::setup_test_users_table(&executor)
        .await
        .expect("Failed to setup table");

    for i in 100..120 {
        let insert = Sql {
            text: "REPLACE INTO test_users (id, name, email, age, score, active) VALUES (?, ?, ?, ?, ?, ?)".to_string(),
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
        text: "SELECT id FROM test_users WHERE id >= ? AND id < ? ORDER BY id".to_string(),
        params: vec![Value::I64(100), Value::I64(120)],
    };

    for _ in 0..5 {
        let mut stream = executor.execute(&select);
        let first = stream.next().await.expect("expected at least one row");
        first.expect("first row should decode");
        drop(stream);

        let rows = execute_all(&executor, &select)
            .await
            .expect("pool should still acquire after streaming drop");
        assert_eq!(rows.len(), 20);
    }
}

#[tokio::test]
#[ignore = "requires a running MySQL instance (run `docker-compose up -d` first)"]
async fn test_decimal_and_datetime_decoding() {
    use chrono::NaiveDate;
    use rust_decimal::Decimal;

    let executor = mysql_common::setup_executor()
        .await
        .expect("Failed to create executor");
    let create_table = Sql {
        text: r#"
            CREATE TABLE IF NOT EXISTS typed_values (
                id BIGINT PRIMARY KEY,
                amount DECIMAL(10, 2) NOT NULL,
                created_at DATETIME NOT NULL
            )
        "#
        .to_string(),
        params: vec![],
    };
    execute_all(&executor, &create_table)
        .await
        .expect("Failed to create typed_values table");

    let expected_amount = Decimal::new(12345, 2);
    let expected_dt = NaiveDate::from_ymd_opt(2026, 3, 30)
        .unwrap()
        .and_hms_opt(12, 34, 56)
        .unwrap();

    let insert = Sql {
        text: "REPLACE INTO typed_values (id, amount, created_at) VALUES (?, ?, ?)".to_string(),
        params: vec![
            Value::I64(1),
            Value::Decimal(expected_amount),
            Value::DateTime(expected_dt),
        ],
    };
    execute_all(&executor, &insert)
        .await
        .expect("Failed to insert typed row");

    let select = Sql {
        text: "SELECT amount, created_at FROM typed_values WHERE id = ?".to_string(),
        params: vec![Value::I64(1)],
    };
    let rows = execute_all(&executor, &select)
        .await
        .expect("Failed to select typed row");

    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.get("amount"), Some(&Value::Decimal(expected_amount)));
    assert_eq!(row.get("created_at"), Some(&Value::DateTime(expected_dt)));
}
