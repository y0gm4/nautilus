use crate::error::{ConnectorError as Error, Result};
use futures::TryStreamExt;
use nautilus_core::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SingleRowExpectation {
    ExactlyOne,
    ZeroOrOne,
}

impl SingleRowExpectation {
    fn validate(self, row_count: usize) -> Result<()> {
        match self {
            Self::ExactlyOne => match row_count {
                1 => Ok(()),
                0 => Err(Error::database_msg("Expected exactly one row, got 0")),
                count => Err(Error::database_msg(format!(
                    "Expected exactly one row, got {}",
                    count
                ))),
            },
            Self::ZeroOrOne => match row_count {
                0 | 1 => Ok(()),
                count => Err(Error::database_msg(format!(
                    "Expected at most one row, got {}",
                    count
                ))),
            },
        }
    }
}

pub(crate) async fn fetch_single_row<'e, DB, Exec, Bind, Decode>(
    executor: Exec,
    sql_text: &'e str,
    params: &'e [Value],
    bind: Bind,
    decode: Decode,
    query_context: &'static str,
    expectation: SingleRowExpectation,
) -> Result<Option<crate::Row>>
where
    DB: sqlx::Database,
    Exec: sqlx::Executor<'e, Database = DB>,
    for<'q> <DB as sqlx::Database>::Arguments<'q>: sqlx::IntoArguments<'q, DB>,
    for<'q> Bind: Fn(
            sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>,
            &'q Value,
        ) -> Result<sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>>
        + Copy,
    Decode: Fn(<DB as sqlx::Database>::Row) -> Result<crate::Row> + Copy,
{
    let mut query = sqlx::query(sql_text);
    for param in params {
        query = bind(query, param)?;
    }

    let mut stream = query.fetch(executor);
    let mut first = None;
    let mut row_count = 0usize;

    while let Some(raw_row) = stream
        .try_next()
        .await
        .map_err(|e| Error::database(e, query_context))?
    {
        row_count += 1;
        if row_count == 1 {
            first = Some(decode(raw_row)?);
        }
    }

    expectation.validate(row_count)?;
    Ok(first)
}
