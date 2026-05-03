#![forbid(unsafe_code)]
//! Database executors and connection management for Nautilus ORM.
//!
//! This crate provides the execution layer for Nautilus, enabling SQL queries
//! to be run against real databases. It defines the `Executor` trait and provides
//! concrete implementations for supported databases.
//!
//! ## Architecture
//!
//! - `Executor` trait: Abstract interface for query execution
//! - `Row`: Database result representation with hybrid access (positional + named)
//! - `PgExecutor`: PostgreSQL implementation using sqlx
//! - `MysqlExecutor`: MySQL implementation using sqlx
//! - `SqliteExecutor`: SQLite implementation using sqlx
//!
//! ## Example
//!
//! ```rust,ignore
//! use nautilus_connector::{execute_all, Executor, PgExecutor, ConnectorResult};
//! use nautilus_dialect::{Dialect, PostgresDialect};
//! use nautilus_core::select::SelectBuilder;
//!
//! #[tokio::main]
//! async fn main() -> ConnectorResult<()> {
//!     // Create executor
//!     let executor = PgExecutor::new("postgres://localhost/mydb").await?;
//!     let dialect = PostgresDialect;
//!     
//!     // Build query
//!     let select = SelectBuilder::new("users")
//!         .columns(vec!["id", "name"])
//!         .build()?;
//!     
//!     // Render and execute
//!     let sql = dialect.render_select(&select)?;
//!     let rows = execute_all(&executor, &sql).await?;
//!     
//!     // Access results
//!     for row in rows {
//!         let id = row.get("id");
//!         let name = row.get("name");
//!         println!("User: {:?}, {:?}", id, name);
//!     }
//!     
//!     Ok(())
//! }
//! ```

#![warn(missing_docs)]

/// Generate `execute_affected` for a pool-backed executor.
///
/// Each backend module defines its own `bind_value` function and exposes a
/// `self.pool` field.  The method body is identical across all three backends
/// (Postgres, MySQL, SQLite), so this macro produces it from a single source.
macro_rules! impl_execute_affected {
    () => {
        /// Execute a mutation SQL and return the number of affected rows.
        ///
        /// Used when `return_data = false` — no `RETURNING` clause is emitted
        /// so the affected-row count must come from the database result.
        pub async fn execute_affected(
            &self,
            sql: &nautilus_dialect::Sql,
        ) -> $crate::error::Result<usize> {
            let mut query = sqlx::query(&sql.text);
            for param in &sql.params {
                query = bind_value(query, param)?;
            }
            let result = query
                .execute(&self.pool)
                .await
                .map_err(|e| $crate::error::ConnectorError::database(e, "Mutation failed"))?;
            Ok(result.rows_affected() as usize)
        }
    };
}

mod client;
pub mod error;
mod executor;
mod from_row;
mod mysql;
mod mysql_stream;
mod pool_options;
mod postgres;
mod postgres_stream;
mod row;
mod row_stream;
mod sqlite;
mod sqlite_stream;
pub mod transaction;
mod utils;
mod value_hint;

pub use client::Client;
pub use error::{ConnectorError, Result as ConnectorResult, SqlxErrorKind};
pub use executor::{execute_all, Executor};
pub use from_row::FromRow;
pub use mysql::MysqlExecutor;
pub use mysql_stream::MysqlRowStream;
pub use pool_options::ConnectorPoolOptions;
pub use postgres::PgExecutor;
pub use postgres_stream::PgRowStream;
pub use row::Row;
pub use row_stream::RowStream;
pub use sqlite::SqliteExecutor;
pub use sqlite_stream::SqliteRowStream;
pub use transaction::{IsolationLevel, TransactionExecutor, TransactionOptions};
pub use value_hint::{
    decode_row_with_hints, normalize_row_with_hints, normalize_rows_with_hints, ValueHint,
};

pub use nautilus_core::RowAccess;

pub use nautilus_core::Column;
pub use nautilus_core::FromValue;
