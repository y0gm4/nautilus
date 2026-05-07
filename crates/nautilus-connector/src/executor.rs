//! Executor trait for database query execution.

use crate::error::ConnectorError as Error;
use crate::error::Result;
use crate::row_stream::RowStream;
use futures::future::BoxFuture;
use futures::stream::Stream;
use nautilus_core::RowAccess;
use nautilus_dialect::Sql;

/// Trait for executing SQL queries against a database.
///
/// This trait uses Generic Associated Types (GAT) to enable:
/// - Database-specific row types with lifetime support
/// - A uniform row stream interface across backends
/// - Zero-copy optimizations where applicable
///
/// Implementors are responsible for:
/// - Managing database connections (pooling, lifecycle)
/// - Binding parameters from `Sql.params` to the query
/// - Executing the query and returning a buffered stream of results
/// - Decoding database rows into types implementing `RowAccess`
/// - Mapping database errors to `nautilus_core::Error`
///
/// ## Thread Safety
///
/// Executors must be `Send + Sync` to allow sharing across async tasks.
///
/// ## Example
///
/// ```rust,ignore
/// use nautilus_connector::{execute_all, Executor, PgExecutor};
/// use nautilus_dialect::{Dialect, PostgresDialect};
/// use nautilus_core::select::SelectBuilder;
/// use futures::stream::StreamExt;
///
/// async fn example() -> nautilus_core::Result<()> {
///     let executor = PgExecutor::new("postgres://localhost/mydb").await?;
///     let dialect = PostgresDialect;
///     
///     let select = SelectBuilder::new("users")
///         .columns(vec!["id", "name"])
///         .build()?;
///     
///     let sql = dialect.render_select(&select)?;
///     
///     // Buffered stream API
///     let mut stream = executor.execute(&sql);
///     while let Some(row) = stream.next().await {
///         let row = row?;
///         println!("{:?}", row);
///     }
///     
///     // Or materialize all rows
///     let rows = execute_all(&executor, &sql).await?;
///     for row in rows {
///         println!("{:?}", row);
///     }
///     
///     Ok(())
/// }
/// ```
pub trait Executor: Send + Sync {
    /// The row type returned by this executor.
    ///
    /// This associated type allows database-specific row implementations
    /// that can borrow data or provide specialized access methods.
    type Row<'conn>: RowAccess<'conn> + Send
    where
        Self: 'conn;

    /// The stream type yielding rows from query execution.
    ///
    /// Current executors eagerly fetch all rows and then yield them through this
    /// stream interface so call sites can stay uniform across backends.
    type RowStream<'conn>: Stream<Item = Result<Self::Row<'conn>>> + Send
    where
        Self: 'conn;

    /// Execute a SQL query and return a stream of rows.
    ///
    /// ## Parameters
    ///
    /// - `sql`: The SQL query with placeholders and bound parameters
    ///
    /// ## Returns
    ///
    /// A buffered stream that yields already-fetched rows one at a time.
    /// Current implementations complete the database fetch before the first
    /// item is yielded.
    ///
    /// ## Errors
    ///
    /// Individual stream items may be `Err` if:
    /// - `ConnectorError::Database`: Query execution failed
    /// - `ConnectorError::RowDecode`: Failed to decode a database value
    fn execute<'conn>(&'conn self, sql: &'conn Sql) -> Self::RowStream<'conn>;

    /// Execute a mutation SQL, drain its results, then execute a fetch SQL
    /// **on the same database connection** and return the fetch rows.
    ///
    /// This is required for databases like MySQL where session-scoped state
    /// such as `LAST_INSERT_ID()` must be read on the connection that
    /// performed the INSERT.
    ///
    /// ## Parameters
    ///
    /// - `mutation`: The INSERT / UPDATE / DELETE statement to execute first
    /// - `fetch`: The SELECT statement whose rows are returned
    ///
    /// ## Returns
    ///
    /// A buffered stream of rows produced by the `fetch` query.
    fn execute_and_fetch<'conn>(
        &'conn self,
        mutation: &'conn Sql,
        fetch: &'conn Sql,
    ) -> Self::RowStream<'conn>;

    /// Execute a SQL query and return a row stream that owns its work.
    ///
    /// Unlike [`Self::execute`], the returned stream is `'static` and detached
    /// from `&self`: the underlying connection is held by a background task
    /// (see `streaming::spawn_streaming_query`), so the caller can move the
    /// stream freely without keeping the executor borrowed. Dropping the
    /// stream mid-iteration still releases the connection cleanly because the
    /// worker drains the remaining rows before returning the connection to
    /// the pool.
    ///
    /// This is the entry point used by codegen-emitted `stream_many` APIs and
    /// by the engine's row-by-row streaming path.
    ///
    /// ## Parameters
    ///
    /// - `sql`: The SQL query, taken by value so the worker can own its text
    ///   and parameters for the lifetime of the stream.
    fn execute_owned(&self, sql: Sql) -> RowStream<'static>;

    /// Execute a SQL query and materialize all rows into a vector.
    ///
    /// Implementors may override this to bypass the boxed stream path when the
    /// call site already wants all rows eagerly. The default implementation
    /// collects from [`Self::execute`].
    fn execute_collect<'conn>(
        &'conn self,
        sql: &'conn Sql,
    ) -> BoxFuture<'conn, Result<Vec<Self::Row<'conn>>>>
    where
        Self: 'conn,
    {
        Box::pin(async move {
            use futures::stream::StreamExt;

            let stream = self.execute(sql);
            futures::pin_mut!(stream);

            let mut rows = Vec::new();
            while let Some(result) = stream.next().await {
                rows.push(result?);
            }
            Ok(rows)
        })
    }

    /// Execute a SQL query and require exactly one row.
    fn execute_one<'conn>(
        &'conn self,
        sql: &'conn Sql,
    ) -> BoxFuture<'conn, Result<Self::Row<'conn>>>
    where
        Self: 'conn,
    {
        Box::pin(async move {
            let mut rows = self.execute_collect(sql).await?;
            match rows.len() {
                1 => Ok(rows.pop().expect("row count checked above")),
                0 => Err(Error::database_msg("Expected exactly one row, got 0")),
                count => Err(Error::database_msg(format!(
                    "Expected exactly one row, got {}",
                    count
                ))),
            }
        })
    }

    /// Execute a SQL query and accept zero or one row.
    fn execute_optional<'conn>(
        &'conn self,
        sql: &'conn Sql,
    ) -> BoxFuture<'conn, Result<Option<Self::Row<'conn>>>>
    where
        Self: 'conn,
    {
        Box::pin(async move {
            let mut rows = self.execute_collect(sql).await?;
            match rows.len() {
                0 => Ok(None),
                1 => Ok(rows.pop()),
                count => Err(Error::database_msg(format!(
                    "Expected at most one row, got {}",
                    count
                ))),
            }
        })
    }
}

/// Execute a SQL query and materialize all rows into a Vec.
///
/// This is a convenience helper that collects the stream into a vector
/// for cases where you need all rows immediately or want random access.
///
/// ## Parameters
///
/// - `executor`: The executor to run the query against
/// - `sql`: The SQL query with placeholders and bound parameters
///
/// ## Returns
///
/// - `Ok(Vec<E::Row<'conn>>)`: All rows successfully fetched and decoded
/// - `Err(Error)`: Connection, execution, or decoding error
///
/// ## Errors
///
/// - `ConnectorError::Connection`: Failed to acquire database connection
/// - `ConnectorError::Database`: Query execution failed
/// - `ConnectorError::RowDecode`: Failed to decode a row
///
/// ## Example
///
/// ```rust,ignore
/// use nautilus_connector::{execute_all, PgExecutor};
/// use nautilus_dialect::Sql;
///
/// async fn example(executor: &PgExecutor, sql: &Sql) -> nautilus_core::Result<()> {
///     let rows = execute_all(executor, sql).await?;
///     for row in rows {
///         println!("Row: {:?}", row);
///     }
///     Ok(())
/// }
/// ```
pub async fn execute_all<'conn, E>(
    executor: &'conn E,
    sql: &'conn Sql,
) -> Result<Vec<E::Row<'conn>>>
where
    E: Executor + ?Sized,
{
    executor.execute_collect(sql).await
}

/// Execute a SQL query and require exactly one row.
pub async fn execute_one<'conn, E>(executor: &'conn E, sql: &'conn Sql) -> Result<E::Row<'conn>>
where
    E: Executor + ?Sized,
{
    executor.execute_one(sql).await
}

/// Execute a SQL query and accept zero or one row.
pub async fn execute_optional<'conn, E>(
    executor: &'conn E,
    sql: &'conn Sql,
) -> Result<Option<E::Row<'conn>>>
where
    E: Executor + ?Sized,
{
    executor.execute_optional(sql).await
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use futures::stream;
    use nautilus_core::Value;

    struct CountingExecutor {
        execute_calls: Arc<AtomicUsize>,
        collect_calls: Arc<AtomicUsize>,
    }

    impl Executor for CountingExecutor {
        type Row<'conn>
            = crate::Row
        where
            Self: 'conn;
        type RowStream<'conn>
            = stream::Iter<std::vec::IntoIter<Result<crate::Row>>>
        where
            Self: 'conn;

        fn execute<'conn>(&'conn self, _sql: &'conn Sql) -> Self::RowStream<'conn> {
            self.execute_calls.fetch_add(1, Ordering::SeqCst);
            stream::iter(vec![Ok(crate::Row::new(vec![(
                "id".to_string(),
                Value::I64(1),
            )]))])
        }

        fn execute_and_fetch<'conn>(
            &'conn self,
            _mutation: &'conn Sql,
            _fetch: &'conn Sql,
        ) -> Self::RowStream<'conn> {
            self.execute(_fetch)
        }

        fn execute_owned(&self, _sql: Sql) -> RowStream<'static> {
            // Test stub: bridge the iter-style stream into the shared RowStream.
            use futures::stream::StreamExt;
            let inner = stream::iter(vec![Ok(crate::Row::new(vec![(
                "id".to_string(),
                Value::I64(1),
            )]))])
            .map(|item: Result<crate::Row>| item);
            RowStream::new_from_stream(Box::pin(inner))
        }

        fn execute_collect<'conn>(
            &'conn self,
            _sql: &'conn Sql,
        ) -> BoxFuture<'conn, Result<Vec<Self::Row<'conn>>>>
        where
            Self: 'conn,
        {
            self.collect_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(vec![crate::Row::new(vec![(
                    "id".to_string(),
                    Value::I64(1),
                )])])
            })
        }
    }

    struct StreamingExecutor {
        row_count: usize,
    }

    impl Executor for StreamingExecutor {
        type Row<'conn>
            = crate::Row
        where
            Self: 'conn;
        type RowStream<'conn>
            = stream::Iter<std::vec::IntoIter<Result<crate::Row>>>
        where
            Self: 'conn;

        fn execute<'conn>(&'conn self, _sql: &'conn Sql) -> Self::RowStream<'conn> {
            let rows = (0..self.row_count)
                .map(|idx| {
                    Ok(crate::Row::new(vec![(
                        "id".to_string(),
                        Value::I64(idx as i64 + 1),
                    )]))
                })
                .collect::<Vec<_>>();
            stream::iter(rows)
        }

        fn execute_and_fetch<'conn>(
            &'conn self,
            _mutation: &'conn Sql,
            _fetch: &'conn Sql,
        ) -> Self::RowStream<'conn> {
            self.execute(_fetch)
        }

        fn execute_owned(&self, _sql: Sql) -> RowStream<'static> {
            let rows: Vec<Result<crate::Row>> = (0..self.row_count)
                .map(|idx| {
                    Ok(crate::Row::new(vec![(
                        "id".to_string(),
                        Value::I64(idx as i64 + 1),
                    )]))
                })
                .collect();
            RowStream::new_from_stream(Box::pin(stream::iter(rows)))
        }
    }

    #[tokio::test]
    async fn execute_all_prefers_executor_collect_fast_path() {
        let execute_calls = Arc::new(AtomicUsize::new(0));
        let collect_calls = Arc::new(AtomicUsize::new(0));
        let executor = CountingExecutor {
            execute_calls: Arc::clone(&execute_calls),
            collect_calls: Arc::clone(&collect_calls),
        };
        let sql = Sql {
            text: "SELECT 1".to_string(),
            params: vec![],
        };

        let rows = execute_all(&executor, &sql).await.expect("collect rows");

        assert_eq!(rows.len(), 1);
        assert_eq!(collect_calls.load(Ordering::SeqCst), 1);
        assert_eq!(execute_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn execute_one_requires_exactly_one_row() {
        let sql = Sql {
            text: "SELECT 1".to_string(),
            params: vec![],
        };

        let one = StreamingExecutor { row_count: 1 };
        let row = execute_one(&one, &sql).await.expect("one row");
        assert_eq!(row.get("id"), Some(&Value::I64(1)));

        let none = StreamingExecutor { row_count: 0 };
        let err = execute_one(&none, &sql)
            .await
            .expect_err("should reject zero rows");
        assert!(err.to_string().contains("exactly one row"));

        let many = StreamingExecutor { row_count: 2 };
        let err = execute_one(&many, &sql)
            .await
            .expect_err("should reject multiple rows");
        assert!(err.to_string().contains("exactly one row"));
    }

    #[tokio::test]
    async fn execute_optional_allows_zero_or_one_row() {
        let sql = Sql {
            text: "SELECT 1".to_string(),
            params: vec![],
        };

        let none = StreamingExecutor { row_count: 0 };
        assert!(execute_optional(&none, &sql)
            .await
            .expect("optional row")
            .is_none());

        let one = StreamingExecutor { row_count: 1 };
        let row = execute_optional(&one, &sql)
            .await
            .expect("optional row")
            .expect("expected one row");
        assert_eq!(row.get("id"), Some(&Value::I64(1)));

        let many = StreamingExecutor { row_count: 2 };
        let err = execute_optional(&many, &sql)
            .await
            .expect_err("should reject multiple rows");
        assert!(err.to_string().contains("at most one row"));
    }
}
