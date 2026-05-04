# nautilus-dialect

SQL dialect renderers for the Nautilus ORM.

This crate translates query ASTs produced by `nautilus-core` into dialect-specific
SQL strings with bound parameters ready for execution via `nautilus-connector`.

## Supported dialects

| Dialect | Struct | Placeholders | Quoting | `RETURNING` |
|---------|--------|--------------|---------|-------------|
| PostgreSQL | `PostgresDialect` | `$1, $2, ...` | `"name"` | yes |
| MySQL | `MysqlDialect` | `?` | `` `name` `` | no (emulated) |
| SQLite | `SqliteDialect` | `?` | `"name"` | yes (3.35+) |

## Usage

```rust
use nautilus_dialect::{Dialect, PostgresDialect};
use nautilus_core::{Select, Value};

let select = Select::from_table("users")
    .filter(
        nautilus_core::Expr::column("email")
            .eq(nautilus_core::Expr::param(Value::String("alice@example.com".into())))
    )
    .build()?;

let sql = PostgresDialect.render_select(&select)?;
// sql.text   => SELECT * FROM "users" WHERE ("email" = $1)
// sql.params => [Value::String("alice@example.com")]
```

## Architecture

The crate is organized as follows:

- **`lib.rs`** — Public API: `Dialect` trait, `Sql` struct, shared rendering
  macros (`render_insert_body!`, `render_update_body!`, `render_delete_body!`,
  `render_select_body_core!`, `render_returning!`), and shared render helpers
  for identifier writing and placeholder formatting.

- **`render_estimate.rs`** — Conservative SQL/params capacity estimation used
  by the dialect renderers to preallocate `String` and `Vec<Value>` buffers.

- **`postgres.rs`** — PostgreSQL renderer. Handles `$N` placeholders,
  `DISTINCT ON`, `FILTER (WHERE ...)` on aggregates, and `@>` / `<@` / `&&`
  array operators.

- **`mysql.rs`** — MySQL renderer. Uses `?` placeholders and backtick quoting.
  Emits `LIMIT 18446744073709551615` when only `OFFSET` is requested (MySQL
  requires `LIMIT` before `OFFSET`). `RETURNING` is silently omitted.

- **`sqlite.rs`** — SQLite renderer. Uses `?` placeholders and ANSI double-quote
  quoting. Supports `RETURNING` (SQLite 3.35+) and native `FILTER (WHERE ...)`
  on aggregates.

## Array operators

PostgreSQL natively supports `@>` (contains), `<@` (contained by), and `&&`
(overlaps) array operators via `BinaryOp::ArrayContains`,
`BinaryOp::ArrayContainedBy`, and `BinaryOp::ArrayOverlaps`.

MySQL emulates contains / contained-by via `JSON_CONTAINS(target, candidate)`.
The generic `MysqlDialect` intentionally rejects overlap rendering because the
workspace-wide `"mysql"` provider also covers MySQL-family backends where
`JSON_OVERLAPS` is not guaranteed. SQLite uses `json_each` + correlated
`EXISTS` predicates so JSON `null` elements are preserved instead of being
lost to `IN` / `NOT IN` null semantics. Arrays are bound as JSON strings by
the connector layer, so no special client-side handling is needed.
