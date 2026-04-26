# nautilus-schema

`nautilus-schema` is the parser, validator, formatter, and editor-analysis crate for `.nautilus` files.

## Pipeline

1. Lex source text into typed tokens with spans.
2. Parse tokens into a syntax tree.
3. Validate the tree into a resolved `SchemaIr`.
4. Reuse that result for formatting and editor tooling.

## Main public APIs

| API | Purpose |
| --- | --- |
| `analyze(source)` | One-shot lexer + parser + validator + diagnostics bundle |
| `parse_schema_source(source)` | Strict parse helper for callers that want a syntax AST and want parser recovery errors to fail fast |
| `parse_schema_source_with_recovery(source)` | Parse helper for tools like formatters that need the AST plus recovered parse errors |
| `validate_schema_source(source)` | Parse + validate helper returning both AST and `SchemaIr` |
| `validate_schema(ast)` | Produces `ir::SchemaIr` from an AST |
| `format_schema(ast, source)` | Canonical formatter |
| `completion`, `hover`, `goto_definition` | LSP/editor features |
| `semantic_tokens` | Semantic-token support for editors |
| `Lexer`, `Parser` | Lower-level building blocks for callers that need stage-by-stage access |

## Supported schema constructs

- `datasource` and `generator` blocks
- PostgreSQL datasource extensions via `extensions = [citext, "uuid-ossp"]`
- `model`, `enum`, and `type` declarations
- scalar, enum, composite, relation, optional, and list field types
- mapped names via `@map` / `@@map`
- defaults such as `autoincrement()`, `uuid()`, `now()`
- relation metadata including `fields`, `references`, and referential actions
- indexes, unique constraints, checks, and computed fields

## Minimal usage

## PostgreSQL extensions

Datasource blocks may declare PostgreSQL extensions to install before schema
DDL runs:

```prisma
datasource db {
  provider            = "postgresql"
  url                 = env("DATABASE_URL")
  extensions          = [citext, hstore, ltree, "uuid-ossp"]
  preserve_extensions = true
}
```

The field is PostgreSQL-only. Entries can be bare identifiers or string
literals, are normalized to lowercase, and are deduplicated in the validated IR.
Unknown names produce warnings rather than hard errors so custom extensions can
still be managed. Extension-backed scalar types currently include `Citext`,
`Hstore`, `Ltree`, and pgvector's sized `Vector(dim)` (declared with
`extensions = [vector]`); the validator warns when those types are used without
the matching datasource extension. By default extension management is declarative:
extra live extensions are diffed as destructive drops. Set
`preserve_extensions = true` to keep live extensions that are managed outside
Nautilus.

## Minimal usage

```toml
[dependencies]
nautilus_schema = { package = "nautilus-orm-schema", path = "../crates/nautilus-schema" }
```

```rust
use nautilus_schema::analyze;

let result = analyze(source);

for diagnostic in &result.diagnostics {
    eprintln!("{:?}: {}", diagnostic.severity, diagnostic.message);
}

if let Some(ir) = &result.ir {
    println!("validated {} model(s)", ir.models.len());
}
```

If you need lower-level control:

```rust
use nautilus_schema::{validate_schema, Lexer, Parser, TokenKind};

let mut lexer = Lexer::new(source);
let mut tokens = Vec::new();

loop {
    let token = lexer.next_token()?;
    let is_eof = matches!(token.kind, TokenKind::Eof);
    tokens.push(token);
    if is_eof {
        break;
    }
}

let ast = Parser::new(&tokens, source).parse_schema()?;
let ir = validate_schema(ast)?;
println!("validated {} model(s)", ir.models.len());
# Ok::<(), nautilus_schema::SchemaError>(())
```

## Where it is used

- `nautilus-cli` for validate / format / DB workflows
- `nautilus-codegen` for generation inputs
- `nautilus-engine` for runtime model metadata
- `nautilus-lsp` for diagnostics, completion, hover, definitions, formatting, and semantic tokens

## References

- [GRAMMAR.md](GRAMMAR.md) for the language grammar
- `tests/` for parser, validator, formatter, analysis, and IR coverage

## Testing

```bash
cargo test -p nautilus-orm-schema
```
