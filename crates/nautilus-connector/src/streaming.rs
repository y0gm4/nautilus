//! Streaming query driver shared by all backends.
//!
//! Streaming `execute()` aims to reduce the gap between when a query starts
//! and when its first row reaches the consumer, lowering tail latency under
//! concurrency. The naive approach — yielding rows directly from
//! `query.fetch(&mut *conn)` — is unsafe in pooled mode: if the consumer drops
//! the stream mid-iteration, the underlying portal stays open and sqlx
//! discards the connection as "dirty", eventually exhausting the pool.
//!
//! The driver here owns the [`PoolConnection`] inside a spawned worker task.
//! The worker pumps rows into a bounded mpsc channel and, if the receiver is
//! dropped early, keeps draining the underlying sqlx stream so the connection
//! completes its extended-query cycle (portal close + ReadyForQuery) before
//! being released. This keeps the pool clean regardless of consumer behavior.

use crate::error::{ConnectorError as Error, Result};
use crate::row_stream::RowStream;
use crate::Row;
use futures::stream::StreamExt;
use nautilus_core::Value;
use sqlx::pool::PoolConnection;
use sqlx::Pool;
use tokio::sync::mpsc;

/// Channel capacity for the streaming pipeline.
///
/// Sized to absorb short consumer stalls without forcing the worker to block
/// on every row. A value too small re-introduces synchronous coupling between
/// producer and consumer; too large wastes memory on slow consumers.
const STREAMING_CHANNEL_CAPACITY: usize = 64;

/// Inputs needed to drive a streaming query in the background.
///
/// Bundling the parameters keeps the spawn helper's signature readable.
pub(crate) struct StreamingQuery<DB, Bind, Decode>
where
    DB: sqlx::Database,
{
    pub pool: Pool<DB>,
    pub sql_text: String,
    pub params: Vec<Value>,
    pub bind: Bind,
    pub decode: Decode,
    pub query_context: &'static str,
    pub persistent: bool,
}

/// Spawn a background task that drives a streaming query and feed its rows
/// through a [`RowStream`].
///
/// Drop semantics: when the returned stream is dropped before the query
/// finishes, the worker keeps polling the underlying sqlx stream (silently
/// discarding decoded rows) until the database signals end-of-data. Only then
/// is the connection released to the pool, so it never returns dirty.
pub(crate) fn spawn_streaming_query<DB, Bind, Decode>(
    request: StreamingQuery<DB, Bind, Decode>,
) -> RowStream<'static>
where
    DB: sqlx::Database + sqlx::database::HasStatementCache,
    PoolConnection<DB>: Send,
    for<'q> <DB as sqlx::Database>::Arguments<'q>: sqlx::IntoArguments<'q, DB>,
    for<'q> &'q mut <DB as sqlx::Database>::Connection: sqlx::Executor<'q, Database = DB>,
    Bind: for<'q> Fn(
            sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>,
            &'q Value,
        )
            -> Result<sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>>
        + Copy
        + Send
        + 'static,
    Decode: Fn(<DB as sqlx::Database>::Row) -> Result<Row> + Copy + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<Result<Row>>(STREAMING_CHANNEL_CAPACITY);

    tokio::spawn(async move {
        run_streaming_query(request, tx).await;
    });

    RowStream::from_receiver(rx)
}

/// Worker body for [`spawn_streaming_query`].
///
/// Kept as a free async function (rather than inlined in the spawn closure) to
/// make the control flow — and especially the drain-on-consumer-drop branch —
/// straightforward to read and test.
async fn run_streaming_query<DB, Bind, Decode>(
    request: StreamingQuery<DB, Bind, Decode>,
    tx: mpsc::Sender<Result<Row>>,
) where
    DB: sqlx::Database + sqlx::database::HasStatementCache,
    PoolConnection<DB>: Send,
    for<'q> <DB as sqlx::Database>::Arguments<'q>: sqlx::IntoArguments<'q, DB>,
    for<'q> &'q mut <DB as sqlx::Database>::Connection: sqlx::Executor<'q, Database = DB>,
    Bind: for<'q> Fn(
            sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>,
            &'q Value,
        )
            -> Result<sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>>
        + Copy,
    Decode: Fn(<DB as sqlx::Database>::Row) -> Result<Row> + Copy,
{
    let StreamingQuery {
        pool,
        sql_text,
        params,
        bind,
        decode,
        query_context,
        persistent,
    } = request;

    let mut conn = match pool.acquire().await {
        Ok(conn) => conn,
        Err(e) => {
            let _ = tx
                .send(Err(Error::connection(e, "Failed to acquire connection")))
                .await;
            return;
        }
    };

    let mut query = sqlx::query(&sql_text).persistent(persistent);
    for param in &params {
        match bind(query, param) {
            Ok(bound) => query = bound,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        }
    }

    let mut stream = query.fetch(&mut *conn);
    let mut consumer_alive = true;

    while let Some(item) = stream.next().await {
        match item {
            Ok(raw_row) => {
                if !consumer_alive {
                    // Drain the rest of the stream so the connection finishes
                    // its extended-query cycle before being returned to the
                    // pool.
                    continue;
                }
                let decoded = decode(raw_row);
                if tx.send(decoded).await.is_err() {
                    consumer_alive = false;
                }
            }
            Err(e) => {
                if consumer_alive {
                    let _ = tx.send(Err(Error::database(e, query_context))).await;
                }
                // Once the database itself returned an error, the connection
                // state is already determined by sqlx; further polling cannot
                // help drain it.
                break;
            }
        }
    }

    // `stream` and `conn` drop here. The connection is released back to the
    // pool clean (or, on database error above, marked dirty by sqlx).
}
