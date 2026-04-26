//! Postgres-specific index inspection: turn raw `pg_indexes`-shaped rows
//! into a list of [`LiveIndex`] records carrying a typed [`LiveIndexKind`].

use crate::live::{LiveIndex, LiveIndexKind};
use nautilus_schema::ir::{
    BasicIndexType, PgvectorIndex, PgvectorIndexOptions, PgvectorMethod, PgvectorOpClass,
};

/// Intermediate row aggregator: `(index_name, is_unique, method, columns,
/// opclass, with_options)`. One entry per logical index, accumulated as the
/// `pg_indexes` rows are folded by index name.
type PgIndexAccum = (
    String,
    bool,
    String,
    Vec<String>,
    Option<String>,
    Vec<(String, String)>,
);

/// Groups raw `pg_indexes` rows (one per index/column) into [`LiveIndex`]
/// entries, parsing the access method into a typed [`LiveIndexKind`].
pub(crate) fn group_pg_indexes(rows: Vec<sqlx::postgres::PgRow>) -> Vec<LiveIndex> {
    use sqlx::Row as _;

    let mut ordered: Vec<PgIndexAccum> = Vec::new();

    for row in rows {
        let index_name: String = row.try_get("index_name").unwrap_or_default();
        let column_name: String = row.try_get("column_name").unwrap_or_default();
        let is_unique: bool = row.try_get("is_unique").unwrap_or(false);
        let index_method: String = row.try_get("index_method").unwrap_or_default();
        let row_opclass: Option<String> = row.try_get("opclass").ok().flatten();
        let index_options: Option<Vec<String>> = row.try_get("index_options").ok().flatten();
        let with_options = normalize_pg_index_options(index_options.as_deref());

        if let Some(entry) = ordered
            .iter_mut()
            .find(|(name, _, _, _, _, _)| name == &index_name)
        {
            entry.3.push(column_name);
            if entry.4.is_none() {
                entry.4 = row_opclass;
            }
        } else {
            ordered.push((
                index_name,
                is_unique,
                index_method,
                vec![column_name],
                row_opclass,
                with_options,
            ));
        }
    }

    ordered
        .into_iter()
        .map(
            |(name, unique, method, cols, opclass, with_options)| LiveIndex {
                name,
                columns: cols,
                unique,
                kind: parse_live_index_kind(&method, opclass.as_deref(), &with_options),
            },
        )
        .collect()
}

/// Parses the `reloptions` array values returned by `pg_class` into
/// canonical `(key, value)` tuples sorted by key.
pub(crate) fn normalize_pg_index_options(options: Option<&[String]>) -> Vec<(String, String)> {
    let mut normalized = options
        .unwrap_or(&[])
        .iter()
        .filter_map(|entry| {
            let (key, value) = entry.split_once('=')?;
            Some((key.to_string(), value.to_string()))
        })
        .collect::<Vec<_>>();
    normalized.sort_by(|left, right| left.0.cmp(&right.0));
    normalized
}

/// Maps the database-reported access method (and its associated opclass /
/// `WITH (...)` options) onto a typed [`LiveIndexKind`].
fn parse_live_index_kind(
    method: &str,
    opclass: Option<&str>,
    with_options: &[(String, String)],
) -> LiveIndexKind {
    if let Ok(pgmethod) = method.parse::<PgvectorMethod>() {
        let parsed_opclass = opclass.and_then(|v| v.parse::<PgvectorOpClass>().ok());
        let options = parse_pgvector_options(with_options, pgmethod);
        return LiveIndexKind::Pgvector(PgvectorIndex {
            method: pgmethod,
            opclass: parsed_opclass,
            options,
        });
    }
    if let Ok(basic) = method.parse::<BasicIndexType>() {
        return LiveIndexKind::Basic(basic);
    }
    LiveIndexKind::Unknown(Some(method.to_string()))
}

fn parse_pgvector_options(
    with_options: &[(String, String)],
    method: PgvectorMethod,
) -> PgvectorIndexOptions {
    let mut options = PgvectorIndexOptions::default();
    for (key, value) in with_options {
        let parsed = value.parse::<u32>().ok();
        match (method, key.as_str()) {
            (PgvectorMethod::Hnsw, "m") => options.m = parsed,
            (PgvectorMethod::Hnsw, "ef_construction") => options.ef_construction = parsed,
            (PgvectorMethod::Ivfflat, "lists") => options.lists = parsed,
            _ => {}
        }
    }
    options
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_are_sorted_and_split() {
        let normalized = normalize_pg_index_options(Some(&[
            "ef_construction=64".to_string(),
            "m=16".to_string(),
        ]));
        assert_eq!(
            normalized,
            vec![
                ("ef_construction".to_string(), "64".to_string()),
                ("m".to_string(), "16".to_string())
            ]
        );
    }

    #[test]
    fn hnsw_with_options_parses_into_pgvector_index() {
        let kind = parse_live_index_kind(
            "hnsw",
            Some("vector_l2_ops"),
            &[
                ("ef_construction".to_string(), "64".to_string()),
                ("m".to_string(), "16".to_string()),
            ],
        );
        let LiveIndexKind::Pgvector(p) = kind else {
            panic!("expected pgvector kind");
        };
        assert_eq!(p.method, PgvectorMethod::Hnsw);
        assert_eq!(p.opclass, Some(PgvectorOpClass::L2Ops));
        assert_eq!(p.options.m, Some(16));
        assert_eq!(p.options.ef_construction, Some(64));
        assert_eq!(p.options.lists, None);
    }

    #[test]
    fn ivfflat_drops_hnsw_only_options() {
        let kind = parse_live_index_kind(
            "ivfflat",
            Some("vector_l2_ops"),
            &[
                ("lists".to_string(), "100".to_string()),
                ("m".to_string(), "16".to_string()),
            ],
        );
        let LiveIndexKind::Pgvector(p) = kind else {
            panic!("expected pgvector kind");
        };
        assert_eq!(p.method, PgvectorMethod::Ivfflat);
        assert_eq!(p.options.lists, Some(100));
        assert_eq!(p.options.m, None);
    }

    #[test]
    fn btree_resolves_to_basic() {
        assert_eq!(
            parse_live_index_kind("btree", None, &[]),
            LiveIndexKind::Basic(BasicIndexType::BTree)
        );
    }

    #[test]
    fn unknown_method_is_preserved() {
        let kind = parse_live_index_kind("rum", None, &[]);
        assert_eq!(kind, LiveIndexKind::Unknown(Some("rum".to_string())));
    }
}
