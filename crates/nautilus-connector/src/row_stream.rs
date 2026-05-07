//! Shared row stream type for all database backends.
//!
//! All three backends (PostgreSQL, MySQL, SQLite) use the single [`RowStream`] type
//! rather than three separate structs. Per-backend type aliases are provided to keep
//! the public API stable and code at call sites readable.

use crate::error::Result;
use crate::Row;
use futures::future::BoxFuture;
use futures::stream::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

/// A type-erased async stream of [`Row`] values.
///
/// Current connector implementations eagerly fetch database results and then
/// expose those rows through this stream interface. The inner stream is
/// heap-allocated and pinned, which is why implementing `Unpin` is safe here.
pub struct RowStream<'conn> {
    inner: Pin<Box<dyn Stream<Item = Result<Row>> + Send + 'conn>>,
}

impl<'conn> RowStream<'conn> {
    /// Create a new `RowStream` wrapping a boxed async stream.
    pub(crate) fn new_from_stream(
        stream: Pin<Box<dyn Stream<Item = Result<Row>> + Send + 'conn>>,
    ) -> Self {
        Self { inner: stream }
    }

    /// Adapt a buffered rows future into the shared stream API.
    pub(crate) fn from_rows_future(future: BoxFuture<'conn, Result<Vec<Row>>>) -> Self {
        let stream = async_stream::stream! {
            match future.await {
                Ok(rows) => {
                    for row in rows {
                        yield Ok(row);
                    }
                }
                Err(error) => yield Err(error),
            }
        };

        Self::new_from_stream(Box::pin(stream))
    }

    /// Adapt an mpsc receiver into the shared stream API.
    ///
    /// Used by the streaming connector path: a background worker task owns the
    /// database connection, drives the underlying sqlx stream, and forwards
    /// rows through `rx`.
    pub(crate) fn from_receiver(rx: mpsc::Receiver<Result<Row>>) -> Self {
        let stream = async_stream::stream! {
            let mut rx = rx;
            while let Some(item) = rx.recv().await {
                yield item;
            }
        };

        Self::new_from_stream(Box::pin(stream))
    }
}

/// Delegates `poll_next` to the inner boxed stream.
impl Stream for RowStream<'_> {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// `RowStream` is `Unpin` because the inner stream is heap-allocated and pinned.
impl<'conn> Unpin for RowStream<'conn> {}
