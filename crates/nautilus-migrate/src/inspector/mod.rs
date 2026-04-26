//! Live-schema inspector — queries a running database to build a [`LiveSchema`].

mod mysql;
mod postgres;
mod postgres_indexes;
mod sqlite;

pub(super) use postgres_indexes::group_pg_indexes;

use crate::ddl::DatabaseProvider;
use crate::error::Result;
use crate::live::{LiveForeignKey, LiveSchema};

/// Inspects a live database and returns a snapshot of its current schema.
pub struct SchemaInspector {
    provider: DatabaseProvider,
    url: String,
}

impl SchemaInspector {
    /// Create a new inspector for the given provider and connection URL.
    pub fn new(provider: DatabaseProvider, url: impl Into<String>) -> Self {
        Self {
            provider,
            url: url.into(),
        }
    }

    /// Connect to the database and return the current [`LiveSchema`].
    pub async fn inspect(&self) -> Result<LiveSchema> {
        match self.provider {
            DatabaseProvider::Postgres => self.inspect_postgres().await,
            DatabaseProvider::Sqlite => self.inspect_sqlite().await,
            DatabaseProvider::Mysql => self.inspect_mysql().await,
        }
    }
}

/// Parse generation expressions from a SQLite CREATE TABLE statement.
///
/// Returns a map of lower-cased column name -> expression body with original
/// expression casing preserved.
/// Looks for patterns like `col_name TYPE AS (expr) STORED` or `... VIRTUAL`.
fn parse_sqlite_generated_exprs(create_sql: &str) -> std::collections::HashMap<String, String> {
    let mut result = std::collections::HashMap::new();
    let lower = create_sql.to_lowercase();

    let start = match lower.find('(') {
        Some(i) => i + 1,
        None => return result,
    };

    let bytes = create_sql.as_bytes();
    let mut depth = 0i32;
    let mut seg_start = start;

    for i in start..bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' if depth == 0 => {
                let seg = create_sql[seg_start..i].trim();
                if let Some((name, expr)) = extract_generated_col(seg) {
                    result.insert(name.to_lowercase(), expr);
                }
                break;
            }
            b')' => depth -= 1,
            b',' if depth == 0 => {
                let seg = create_sql[seg_start..i].trim();
                if let Some((name, expr)) = extract_generated_col(seg) {
                    result.insert(name.to_lowercase(), expr);
                }
                seg_start = i + 1;
            }
            _ => {}
        }
    }

    result
}

/// Extract the generation expression from a single column definition segment.
///
/// Looks for `... AS (expr) STORED` or `... AS (expr) VIRTUAL` (case-insensitive).
/// Returns `(column_name, expression)` with original casing preserved.
fn extract_generated_col(col_def: &str) -> Option<(String, String)> {
    let col_lower = col_def.to_lowercase();
    let as_idx = col_lower.find(" as (")?;
    let name = col_def
        .split_whitespace()
        .next()?
        .trim_matches('"')
        .to_string();

    let expr_start = as_idx + 5;
    let mut depth = 1i32;
    for (i, ch) in col_def[expr_start..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    let expr = col_def[expr_start..expr_start + i].trim().to_string();
                    return Some((name, expr));
                }
            }
            _ => {}
        }
    }
    None
}

/// Parse CHECK constraints from a SQLite CREATE TABLE statement.
///
/// Returns `(column_checks, table_checks)` where `column_checks` maps
/// lower-cased column name -> normalized expression body and `table_checks`
/// holds normalized table-level expressions. Original expression casing is
/// preserved.
fn parse_sqlite_check_constraints(
    create_sql: &str,
) -> (std::collections::HashMap<String, String>, Vec<String>) {
    let mut column_checks: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut table_checks: Vec<String> = Vec::new();

    let lower = create_sql.to_lowercase();
    let start = match lower.find('(') {
        Some(i) => i + 1,
        None => return (column_checks, table_checks),
    };

    let bytes = create_sql.as_bytes();
    let mut depth = 0i32;
    let mut seg_start = start;
    let mut segments: Vec<String> = Vec::new();

    for i in start..bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' if depth == 0 => {
                segments.push(create_sql[seg_start..i].trim().to_string());
                break;
            }
            b')' => depth -= 1,
            b',' if depth == 0 => {
                segments.push(create_sql[seg_start..i].trim().to_string());
                seg_start = i + 1;
            }
            _ => {}
        }
    }

    for seg in &segments {
        if seg.is_empty() {
            continue;
        }
        let first_keyword: String = seg
            .chars()
            .skip_while(|c| c.is_whitespace())
            .take_while(|c| c.is_ascii_alphabetic())
            .map(|c| c.to_ascii_lowercase())
            .collect();
        let is_table_constraint = matches!(
            first_keyword.as_str(),
            "check" | "constraint" | "primary" | "unique" | "foreign"
        );

        if is_table_constraint {
            if let Some(expr) = extract_sqlite_check_expr(seg) {
                table_checks.push(normalize_sqlite_check_expr(&expr));
            }
        } else {
            let col_name = seg
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('"')
                .to_string();
            if let Some(expr) = extract_sqlite_check_expr(seg) {
                column_checks.insert(col_name.to_lowercase(), normalize_sqlite_check_expr(&expr));
            }
        }
    }

    (column_checks, table_checks)
}

/// Extract the expression body from the first `CHECK (…)` or `CHECK(…)` pattern in `seg`.
///
/// The `CHECK` keyword is matched case-insensitively; the returned expression
/// body preserves the original casing from `seg`. Both `CHECK (` (with space)
/// and `CHECK(` (without space) are accepted, since SQLite stores the CREATE
/// TABLE SQL verbatim as the user wrote it.
fn extract_sqlite_check_expr(seg: &str) -> Option<String> {
    let seg_lower = seg.to_lowercase();
    let (check_pos, content_offset) = if let Some(p) = seg_lower.find("check (") {
        (p, 7usize)
    } else if let Some(p) = seg_lower.find("check(") {
        (p, 6usize)
    } else {
        return None;
    };
    let after = &seg[check_pos + content_offset..];
    let mut depth = 1i32;
    for (i, ch) in after.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(after[..i].to_string());
                }
            }
            _ => {}
        }
    }
    None
}

/// Normalise a MySQL CHECK expression into a form the Nautilus schema parser accepts.
///
/// MySQL stores `CHECK_CLAUSE` in `information_schema` with backtick-quoted
/// identifiers (e.g. `` `status` in ('Draft', 'PUBLISHED') ``), redundant outer
/// parentheses, and lowercased SQL keywords. Normalisation strips the backtick
/// quoting, removes outer parentheses, collapses whitespace, and converts SQL
/// `IN (...)` syntax to the Nautilus bracket form `IN [...]`.
fn normalize_mysql_check_expr(expr: &str) -> String {
    let s = strip_mysql_backtick_quotes(expr.trim());
    let mut s = s;
    loop {
        let stripped = crate::utils::strip_outer_parens(&s);
        if stripped == s {
            break;
        }
        s = stripped;
    }
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    convert_in_parens_to_brackets(&s)
}

/// Strip MySQL backtick-quoted identifiers, keeping the bare identifier name.
///
/// MySQL wraps column and table names in backticks when storing expressions in
/// `information_schema` (e.g. `` `status` `` → `status`). Backtick-quoted names
/// are passed through verbatim without any further escaping because Nautilus
/// schema identifiers are unquoted plain names.
fn strip_mysql_backtick_quotes(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch == '`' {
            for inner in chars.by_ref() {
                if inner == '`' {
                    break;
                }
                result.push(inner);
            }
        } else {
            result.push(ch);
        }
    }
    result
}

/// Normalise a SQLite CHECK expression into a form the Nautilus schema parser accepts.
fn normalize_sqlite_check_expr(expr: &str) -> String {
    let mut s = expr.trim().to_string();
    loop {
        let stripped = crate::utils::strip_outer_parens(&s);
        if stripped == s {
            break;
        }
        s = stripped;
    }
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    convert_in_parens_to_brackets(&s)
}

/// Normalise a Postgres column type to the same canonical form that
/// `DdlGenerator::column_type_sql` produces (lower-cased).
fn normalize_pg_type(
    udt_name: &str,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
    character_maximum_length: Option<i32>,
    formatted_type: Option<&str>,
) -> String {
    if let Some(formatted) = formatted_type.and_then(normalize_pgvector_formatted_type) {
        return formatted;
    }

    match udt_name.to_lowercase().as_str() {
        "int4" => "integer".to_string(),
        "int8" => "bigint".to_string(),
        "text" => "text".to_string(),
        "bool" => "boolean".to_string(),
        "timestamp" => "timestamp".to_string(),
        "float8" => "double precision".to_string(),
        "jsonb" => "jsonb".to_string(),
        "uuid" => "uuid".to_string(),
        "bytea" => "bytea".to_string(),
        "varchar" => character_maximum_length
            .map(|length| format!("varchar({length})"))
            .unwrap_or_else(|| "varchar".to_string()),
        "bpchar" => character_maximum_length
            .map(|length| format!("char({length})"))
            .unwrap_or_else(|| "char".to_string()),
        "numeric" => match (numeric_precision, numeric_scale) {
            (Some(p), Some(s)) => format!("decimal({p}, {s})"),
            _ => "decimal".to_string(),
        },
        udt if udt.starts_with('_') => {
            let base = normalize_pg_type(
                &udt[1..],
                numeric_precision,
                numeric_scale,
                character_maximum_length,
                None,
            );
            format!("{base}[]")
        }
        other => other.to_string(),
    }
}

fn normalize_pgvector_formatted_type(formatted_type: &str) -> Option<String> {
    let t = formatted_type.trim().to_lowercase();
    let array_suffix = t.strip_suffix("[]").map(|inner| (inner, "[]"));
    let (base, suffix) = array_suffix.unwrap_or((t.as_str(), ""));
    let base = base.rsplit('.').next().unwrap_or(base);

    if base == "vector" {
        return Some(format!("vector{}", suffix));
    }

    if base.starts_with("vector(") && base.ends_with(')') {
        return Some(format!("{}{}", base, suffix));
    }

    None
}

/// Normalise a `pg_catalog.format_type` output to the same canonical form that
/// `DdlGenerator::column_type_sql` produces so that live composite-type fields
/// can be compared against target schema fields without false positives.
///
/// `format_type` is the authoritative human-readable representation Postgres
/// uses for types, but it uses longer forms (`timestamp without time zone`,
/// `character varying(n)`) that differ from what the DDL generator emits.
fn normalize_pg_composite_field_type(s: &str) -> String {
    let t = s.trim().to_lowercase();
    if t == "timestamp without time zone" || t == "timestamp with time zone" {
        return "timestamp".to_string();
    }
    if let Some(vector) = normalize_pgvector_formatted_type(&t) {
        return vector;
    }
    if let Some(rest) = t.strip_prefix("character varying") {
        return format!("varchar{}", rest.trim());
    }
    if let Some(inner) = t.strip_prefix("character(") {
        return format!("char({}", inner);
    }
    t
}

/// Strip Postgres-generated type casts from a default expression.
fn normalize_pg_default(default: &str) -> String {
    let s = strip_pg_casts(default.trim());
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Remove all `::typename` cast suffixes from a Postgres default expression.
fn strip_pg_casts(s: &str) -> String {
    let mut result = s.to_string();
    while let Some(idx) = result.rfind("::") {
        let after = &result[idx + 2..];
        let type_end = if let Some(after_quote) = after.strip_prefix('"') {
            after_quote.find('"').map(|i| i + 2).unwrap_or(after.len())
        } else {
            after
                .find(|c: char| !c.is_alphanumeric() && c != '_' && c != ' ')
                .unwrap_or(after.len())
        };
        result = format!("{}{}", &result[..idx], &result[idx + 2 + type_end..]);
    }
    result
}

/// Normalise a SQLite column type (PRAGMA table_info).
fn normalize_sqlite_type(type_str: &str) -> String {
    let s = type_str.to_lowercase();
    if let Some(pos) = s.find(" primary") {
        s[..pos].trim().to_string()
    } else {
        s.trim().to_string()
    }
}

/// Normalise a SQLite default expression for comparison.
fn normalize_sqlite_default(raw: &str) -> String {
    let s = raw.trim();
    crate::utils::strip_outer_parens(s)
}

/// Normalise a MySQL `column_type` value to the canonical form used by
/// `DdlGenerator::column_type_sql` (lower-cased).
fn normalize_mysql_type(column_type: &str) -> String {
    let s = column_type.to_lowercase();
    if s == "tinyint(1)" {
        return "boolean".to_string();
    }
    let integer_prefixes = ["int(", "bigint(", "tinyint(", "smallint(", "mediumint("];
    for prefix in &integer_prefixes {
        if s.starts_with(prefix) {
            return prefix.trim_end_matches('(').to_string();
        }
    }
    s
}

/// Extract and normalise the expression body from a PostgreSQL CHECK constraint definition.
fn normalize_pg_check_expr(constraint_def: &str) -> String {
    let s = constraint_def.trim();
    let s_lower = s.to_lowercase();

    let s = if let Some(inner_lower) = s_lower.strip_prefix("check (") {
        let inner = &s[7..];
        if inner_lower.ends_with(')') {
            &inner[..inner.len() - 1]
        } else {
            inner
        }
    } else {
        s
    };

    let s = strip_pg_casts(s.trim());
    let s = strip_numeric_paren_literals(&s);

    let mut s = s.trim().to_string();
    loop {
        let stripped = crate::utils::strip_outer_parens(&s);
        if stripped == s {
            break;
        }
        s = stripped;
    }

    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    let s = convert_any_array_to_in(&s);
    convert_in_parens_to_brackets(&s)
}

/// Convert `col = ANY (ARRAY['A', 'B'])` into the Nautilus bracket form.
fn convert_any_array_to_in(s: &str) -> String {
    let lower = s.to_lowercase();
    let marker = "= any (array[";

    let Some(eq_pos) = lower.find(marker) else {
        return s.to_string();
    };

    let field = s[..eq_pos].trim();
    let bracket_start = eq_pos + marker.len();
    let rest = &s[bracket_start..];

    let mut depth = 1i32;
    let mut bracket_end = None;
    for (i, c) in rest.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => {
                depth -= 1;
                if depth == 0 {
                    bracket_end = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }

    let Some(bclose) = bracket_end else {
        return s.to_string();
    };

    let items = &rest[..bclose];
    let after_array = rest[bclose + 1..].trim_start();
    let after_paren = after_array.strip_prefix(')').unwrap_or(after_array);

    if after_paren.is_empty() {
        format!("{} IN [{}]", field, items)
    } else {
        format!("{} IN [{}] {}", field, items, after_paren.trim_start())
    }
}

/// Convert all SQL `col IN (...)` occurrences into Nautilus `col IN [...]`.
fn convert_in_parens_to_brackets(s: &str) -> String {
    let lower = s.to_lowercase();
    let marker = " in (";

    if !lower.contains(marker) {
        return s.to_string();
    }

    let mut result = String::with_capacity(s.len());
    let mut pos = 0usize;

    while let Some(rel) = lower[pos..].find(marker) {
        let abs = pos + rel;
        result.push_str(&s[pos..abs]);
        result.push_str(" IN [");

        let after_open = abs + marker.len();
        let rest = &s[after_open..];

        let mut depth = 1i32;
        let mut close = rest.len();
        for (i, c) in rest.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        close = i;
                        break;
                    }
                }
                _ => {}
            }
        }

        result.push_str(&rest[..close]);
        result.push(']');
        pos = after_open + close + 1;
    }

    result.push_str(&s[pos..]);
    result
}

/// Remove parentheses that wrap a single numeric literal.
fn strip_numeric_paren_literals(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '(' {
            let mut inner = String::new();
            let mut depth = 1i32;
            for c in chars.by_ref() {
                match c {
                    '(' => {
                        depth += 1;
                        inner.push(c);
                    }
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                        inner.push(c);
                    }
                    _ => inner.push(c),
                }
            }
            let trimmed = inner.trim();
            let is_numeric = !trimmed.is_empty()
                && trimmed
                    .chars()
                    .all(|c| c.is_ascii_digit() || c == '.' || c == '-');
            if is_numeric {
                result.push_str(trimmed);
            } else {
                result.push('(');
                result.push_str(&strip_numeric_paren_literals(&inner));
                result.push(')');
            }
        } else {
            result.push(ch);
        }
    }

    result
}

/// Group raw FK rows (one row per FK column) into [`LiveForeignKey`] values.
fn group_pg_foreign_keys(rows: Vec<sqlx::postgres::PgRow>) -> Vec<LiveForeignKey> {
    use sqlx::Row as _;

    let mut ordered: Vec<String> = Vec::new();
    let mut map: std::collections::HashMap<String, LiveForeignKey> =
        std::collections::HashMap::new();

    for row in rows {
        let con_name: String = row.try_get("constraint_name").unwrap_or_default();
        let col: String = row.try_get("column_name").unwrap_or_default();
        let ref_table: String = row.try_get("referenced_table").unwrap_or_default();
        let ref_col: String = row.try_get("referenced_column").unwrap_or_default();
        let del_type: String = row.try_get("delete_type").unwrap_or_default();
        let upd_type: String = row.try_get("update_type").unwrap_or_default();

        if !ordered.contains(&con_name) {
            ordered.push(con_name.clone());
        }

        let entry = map
            .entry(con_name.clone())
            .or_insert_with(|| LiveForeignKey {
                constraint_name: con_name,
                columns: Vec::new(),
                referenced_table: ref_table,
                referenced_columns: Vec::new(),
                on_delete: pg_fk_action(&del_type),
                on_update: pg_fk_action(&upd_type),
            });
        entry.columns.push(col);
        entry.referenced_columns.push(ref_col);
    }

    ordered
        .into_iter()
        .filter_map(|name| map.remove(&name))
        .collect()
}

/// Decode a PostgreSQL single-character FK action code into a SQL string.
fn pg_fk_action(code: &str) -> Option<String> {
    match code {
        "a" => None,
        "r" => Some("RESTRICT".to_string()),
        "c" => Some("CASCADE".to_string()),
        "n" => Some("SET NULL".to_string()),
        "d" => Some("SET DEFAULT".to_string()),
        _ => None,
    }
}

/// Group SQLite `PRAGMA foreign_key_list` rows into [`LiveForeignKey`] values.
fn group_sqlite_foreign_keys(
    table_name: &str,
    rows: Vec<sqlx::sqlite::SqliteRow>,
) -> Vec<LiveForeignKey> {
    use sqlx::Row as _;

    type SqliteFkColumn = (i64, String, String);
    type SqliteFkGroup = (String, Vec<SqliteFkColumn>, String, String);

    let mut ordered: Vec<i64> = Vec::new();
    let mut map: std::collections::HashMap<i64, SqliteFkGroup> = std::collections::HashMap::new();

    for row in rows {
        let id: i64 = row.try_get("id").unwrap_or(0);
        let seq: i64 = row.try_get("seq").unwrap_or(0);
        let ref_table: String = row.try_get("table").unwrap_or_default();
        let from_col: String = row.try_get("from").unwrap_or_default();
        let to_col: String = row.try_get("to").unwrap_or_default();
        let on_update: String = row.try_get("on_update").unwrap_or_default();
        let on_delete: String = row.try_get("on_delete").unwrap_or_default();

        if !ordered.contains(&id) {
            ordered.push(id);
        }
        let entry = map
            .entry(id)
            .or_insert_with(|| (ref_table, Vec::new(), on_delete, on_update));
        entry.1.push((seq, from_col, to_col));
    }

    let mut result: Vec<LiveForeignKey> = Vec::new();
    for fk_id in ordered {
        if let Some((ref_table, mut cols, on_delete, on_update)) = map.remove(&fk_id) {
            cols.sort_by_key(|(seq, _, _)| *seq);
            let fk_cols: Vec<String> = cols.iter().map(|(_, f, _)| f.clone()).collect();
            let ref_cols: Vec<String> = cols.iter().map(|(_, _, t)| t.clone()).collect();
            let constraint_name = fk_auto_name(table_name, &fk_cols);
            result.push(LiveForeignKey {
                constraint_name,
                columns: fk_cols,
                referenced_table: ref_table,
                referenced_columns: ref_cols,
                on_delete: sqlite_fk_action(&on_delete),
                on_update: sqlite_fk_action(&on_update),
            });
        }
    }
    result
}

/// Normalise a SQLite FK action string.
fn sqlite_fk_action(s: &str) -> Option<String> {
    match s.to_uppercase().as_str() {
        "NO ACTION" | "" => None,
        other => Some(other.to_string()),
    }
}

/// Group MySQL FK rows into [`LiveForeignKey`] values.
fn group_mysql_foreign_keys(rows: Vec<sqlx::mysql::MySqlRow>) -> Vec<LiveForeignKey> {
    use sqlx::Row as _;

    let mut ordered: Vec<String> = Vec::new();
    let mut map: std::collections::HashMap<String, LiveForeignKey> =
        std::collections::HashMap::new();

    for row in rows {
        let con_name: String = row.try_get("constraint_name").unwrap_or_default();
        let col: String = row.try_get("column_name").unwrap_or_default();
        let ref_table: String = row.try_get("referenced_table_name").unwrap_or_default();
        let ref_col: String = row.try_get("referenced_column_name").unwrap_or_default();
        let del_rule: String = row.try_get("delete_rule").unwrap_or_default();
        let upd_rule: String = row.try_get("update_rule").unwrap_or_default();

        if !ordered.contains(&con_name) {
            ordered.push(con_name.clone());
        }
        let entry = map
            .entry(con_name.clone())
            .or_insert_with(|| LiveForeignKey {
                constraint_name: con_name,
                columns: Vec::new(),
                referenced_table: ref_table,
                referenced_columns: Vec::new(),
                on_delete: mysql_fk_action(&del_rule),
                on_update: mysql_fk_action(&upd_rule),
            });
        entry.columns.push(col);
        entry.referenced_columns.push(ref_col);
    }

    ordered
        .into_iter()
        .filter_map(|name| map.remove(&name))
        .collect()
}

/// Normalise a MySQL FK action rule string.
fn mysql_fk_action(s: &str) -> Option<String> {
    match s.to_uppercase().as_str() {
        "NO ACTION" | "" => None,
        other => Some(other.to_string()),
    }
}

/// Derive an auto-generated FK constraint name from table and FK column list.
fn fk_auto_name(table: &str, columns: &[String]) -> String {
    format!("fk_{}_{}", table, columns.join("_"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_int4() {
        assert_eq!(normalize_pg_type("int4", None, None, None, None), "integer");
    }

    #[test]
    fn pg_int8() {
        assert_eq!(normalize_pg_type("int8", None, None, None, None), "bigint");
    }

    #[test]
    fn pg_text() {
        assert_eq!(normalize_pg_type("text", None, None, None, None), "text");
    }

    #[test]
    fn pg_bool() {
        assert_eq!(normalize_pg_type("bool", None, None, None, None), "boolean");
    }

    #[test]
    fn pg_float8() {
        assert_eq!(
            normalize_pg_type("float8", None, None, None, None),
            "double precision"
        );
    }

    #[test]
    fn pg_numeric_with_precision() {
        assert_eq!(
            normalize_pg_type("numeric", Some(10), Some(2), None, None),
            "decimal(10, 2)"
        );
    }

    #[test]
    fn pg_numeric_without_precision() {
        assert_eq!(
            normalize_pg_type("numeric", None, None, None, None),
            "decimal"
        );
    }

    #[test]
    fn pg_array_type() {
        assert_eq!(
            normalize_pg_type("_int4", None, None, None, None),
            "integer[]"
        );
        assert_eq!(normalize_pg_type("_text", None, None, None, None), "text[]");
    }

    #[test]
    fn pg_uuid() {
        assert_eq!(normalize_pg_type("uuid", None, None, None, None), "uuid");
    }

    #[test]
    fn pgvector_dimension_from_formatted_type() {
        assert_eq!(
            normalize_pg_type("vector", None, None, None, Some("vector(1536)")),
            "vector(1536)"
        );
        assert_eq!(
            normalize_pg_type("_vector", None, None, None, Some("vector(3)[]")),
            "vector(3)[]"
        );
    }

    #[test]
    fn pg_enum_passthrough() {
        assert_eq!(
            normalize_pg_type("my_custom_enum", None, None, None, None),
            "my_custom_enum"
        );
    }

    #[test]
    fn pg_varchar_with_length() {
        assert_eq!(
            normalize_pg_type("varchar", None, None, Some(30), None),
            "varchar(30)"
        );
    }

    #[test]
    fn pg_char_with_length() {
        assert_eq!(
            normalize_pg_type("bpchar", None, None, Some(12), None),
            "char(12)"
        );
    }

    #[test]
    fn pg_default_strips_cast() {
        assert_eq!(normalize_pg_default("'hello'::text"), "'hello'");
    }

    #[test]
    fn pg_default_no_cast() {
        assert_eq!(normalize_pg_default("42"), "42");
    }

    #[test]
    fn pg_default_preserves_function() {
        assert_eq!(normalize_pg_default("now()"), "now()");
    }

    #[test]
    fn pg_default_nextval_keeps_closing_paren() {
        assert_eq!(
            normalize_pg_default("nextval('tags_id_seq'::regclass)"),
            "nextval('tags_id_seq')"
        );
        assert_eq!(
            normalize_pg_default("nextval('_nautilus_migrations_id_seq'::regclass)"),
            "nextval('_nautilus_migrations_id_seq')"
        );
    }

    #[test]
    fn pg_default_character_varying_cast() {
        assert_eq!(
            normalize_pg_default("'DRAFT'::character varying"),
            "'DRAFT'"
        );
    }

    #[test]
    fn pg_default_quoted_identifier_cast() {
        assert_eq!(normalize_pg_default("'DRAFT'::\"poststatus\""), "'DRAFT'");
        assert_eq!(normalize_pg_default("'user'::\"role\""), "'user'");
        assert_eq!(normalize_pg_default("'USER'::\"Role\""), "'USER'");
    }

    #[test]
    fn sqlite_type_lowercases() {
        assert_eq!(normalize_sqlite_type("TEXT"), "text");
        assert_eq!(normalize_sqlite_type("INTEGER"), "integer");
    }

    #[test]
    fn sqlite_type_strips_pk_suffix() {
        assert_eq!(
            normalize_sqlite_type("INTEGER PRIMARY KEY AUTOINCREMENT"),
            "integer"
        );
    }

    #[test]
    fn sqlite_default_strips_parens() {
        assert_eq!(normalize_sqlite_default("(42)"), "42");
    }

    #[test]
    fn sqlite_default_preserves_nested_parens() {
        assert_eq!(normalize_sqlite_default("((1+2))"), "(1+2)");
    }

    #[test]
    fn sqlite_default_no_parens() {
        assert_eq!(normalize_sqlite_default("'hello'"), "'hello'");
    }

    #[test]
    fn sqlite_default_preserves_string_casing() {
        assert_eq!(normalize_sqlite_default("'Hello World'"), "'Hello World'");
    }

    #[test]
    fn sqlite_generated_expr_preserves_expression_casing() {
        let exprs = parse_sqlite_generated_exprs(
            r#"CREATE TABLE users (
  "Id" INTEGER PRIMARY KEY,
  "FullName" TEXT AS ("FirstName" || ' ' || "LastName") STORED
)"#,
        );

        assert_eq!(
            exprs.get("fullname"),
            Some(&r#""FirstName" || ' ' || "LastName""#.to_string())
        );
    }

    #[test]
    fn sqlite_check_parser_preserves_literals_and_uses_lowercase_keys() {
        let (column_checks, table_checks) = parse_sqlite_check_constraints(
            r#"CREATE TABLE users (
  "Status" TEXT CHECK ("Status" IN ('Draft', 'PUBLISHED')),
  "Role" TEXT,
  CHECK ("Role" IN ('ADMIN', 'User'))
)"#,
        );

        assert_eq!(
            column_checks.get("status"),
            Some(&r#""Status" IN ['Draft', 'PUBLISHED']"#.to_string())
        );
        assert_eq!(
            table_checks,
            vec![r#""Role" IN ['ADMIN', 'User']"#.to_string()]
        );
    }

    #[test]
    fn mysql_tinyint1_is_boolean() {
        assert_eq!(normalize_mysql_type("tinyint(1)"), "boolean");
    }

    #[test]
    fn mysql_strips_int_display_width() {
        assert_eq!(normalize_mysql_type("int(11)"), "int");
        assert_eq!(normalize_mysql_type("bigint(20)"), "bigint");
    }

    #[test]
    fn mysql_keeps_varchar() {
        assert_eq!(normalize_mysql_type("varchar(255)"), "varchar(255)");
    }

    #[test]
    fn mysql_keeps_decimal() {
        assert_eq!(normalize_mysql_type("decimal(10,2)"), "decimal(10,2)");
    }

    #[test]
    fn mysql_check_in_list_preserves_casing() {
        assert_eq!(
            normalize_mysql_check_expr("status IN ('Draft', 'PUBLISHED')"),
            "status IN ['Draft', 'PUBLISHED']"
        );
    }

    #[test]
    fn mysql_check_strips_backtick_quotes() {
        assert_eq!(
            normalize_mysql_check_expr("(`status` in ('Draft', 'PUBLISHED'))"),
            "status IN ['Draft', 'PUBLISHED']"
        );
    }

    #[test]
    fn mysql_check_strips_backtick_quotes_numeric() {
        assert_eq!(
            normalize_mysql_check_expr("(`quantity` > 0)"),
            "quantity > 0"
        );
    }

    #[test]
    fn sqlite_check_in_list_preserves_casing() {
        assert_eq!(
            normalize_sqlite_check_expr(r#""Role" IN ('ADMIN', 'User')"#),
            r#""Role" IN ['ADMIN', 'User']"#
        );
    }

    #[test]
    fn sqlite_check_no_space_before_paren() {
        let (column_checks, table_checks) = parse_sqlite_check_constraints(
            r#"CREATE TABLE users (
  "Status" TEXT CHECK("Status" IN ('Draft', 'PUBLISHED')),
  CHECK("age" > 0)
)"#,
        );

        assert_eq!(
            column_checks.get("status"),
            Some(&r#""Status" IN ['Draft', 'PUBLISHED']"#.to_string())
        );
        assert_eq!(table_checks, vec![r#""age" > 0"#.to_string()]);
    }

    #[test]
    fn sqlite_check_constraint_no_space_table_level() {
        let (column_checks, table_checks) = parse_sqlite_check_constraints(
            r#"CREATE TABLE orders (
  id INTEGER PRIMARY KEY,
  quantity INTEGER,
  CONSTRAINT chk_qty CHECK(quantity > 0)
)"#,
        );

        assert!(column_checks.is_empty());
        assert_eq!(table_checks, vec!["quantity > 0".to_string()]);
    }

    #[test]
    fn pg_check_simple_integer() {
        assert_eq!(
            normalize_pg_check_expr("CHECK ((quantity > 0))"),
            "quantity > 0"
        );
    }

    #[test]
    fn pg_check_numeric_literal_cast() {
        assert_eq!(
            normalize_pg_check_expr("CHECK ((price > (0)::numeric))"),
            "price > 0"
        );
    }

    #[test]
    fn pg_check_gte_numeric() {
        assert_eq!(
            normalize_pg_check_expr("CHECK ((stock >= (0)::numeric))"),
            "stock >= 0"
        );
    }

    #[test]
    fn pg_check_no_cast() {
        assert_eq!(
            normalize_pg_check_expr("CHECK ((total_amount > 0))"),
            "total_amount > 0"
        );
    }

    #[test]
    fn pg_check_in_list_preserves_casing() {
        assert_eq!(
            normalize_pg_check_expr("CHECK ((status IN ('DRAFT', 'PUBLISHED')))"),
            "status IN ['DRAFT', 'PUBLISHED']"
        );
    }

    #[test]
    fn pg_check_any_array_to_in() {
        assert_eq!(
            normalize_pg_check_expr(
                "CHECK ((status = ANY (ARRAY['DRAFT'::character varying, 'PUBLISHED'::character varying])))"
            ),
            "status IN ['DRAFT', 'PUBLISHED']"
        );
    }

    #[test]
    fn pg_check_any_array_no_cast() {
        assert_eq!(
            normalize_pg_check_expr("CHECK ((status = ANY (ARRAY['DRAFT', 'PUBLISHED'])))"),
            "status IN ['DRAFT', 'PUBLISHED']"
        );
    }

    #[test]
    fn pg_check_compound_with_in() {
        assert_eq!(
            normalize_pg_check_expr("CHECK ((price > 0 AND role IN ('ADMIN', 'USER')))"),
            "price > 0 AND role IN ['ADMIN', 'USER']"
        );
    }
}
