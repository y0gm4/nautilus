# nautilus-engine

`nautilus-engine` is the JSON-RPC runtime used by generated multi-language clients.

It loads a validated schema, connects to a database, and serves requests on stdin/stdout.

## Supported RPC methods

| Category | Methods |
| --- | --- |
| Handshake | `engine.handshake` |
| Reads | `query.findMany`, `query.findFirst`, `query.findUnique`, `query.findFirstOrThrow`, `query.findUniqueOrThrow` |
| Writes | `query.create`, `query.createMany`, `query.update`, `query.delete` |
| Aggregation | `query.count`, `query.groupBy` |
| Raw SQL | `query.rawQuery`, `query.rawStmtQuery` |
| Transactions | `transaction.start`, `transaction.commit`, `transaction.rollback`, `transaction.batch` |
| Schema | `schema.validate` |

## Running it

Via the dedicated binary:

```bash
cargo run -p nautilus-orm-engine -- --migrate
```

Via the main CLI:

```bash
nautilus engine serve --migrate
```

If `--schema` is omitted, the engine auto-detects the first `.nautilus` file
in the current directory.

## Runtime notes

- `transactionId` is supported on request types that can run inside an open transaction.
- `query.findMany` also supports protocol-level chunking via `chunkSize`; partial responses are emitted before the final response when the client opts in.
- The engine owns schema-aware field mapping, relation hydration for includes, mutation-side `@updatedAt`, transaction timeout handling, and aggregate/raw-query execution.

## Main modules

| Module | Responsibility |
| --- | --- |
| `args` | Standalone binary CLI parsing |
| `handlers` | RPC routing and method handlers |
| `filter` | JSON query args -> `nautilus-core` expressions |
| `state` | Schema metadata, connector client, transaction registry |
| `transport` | Stdin/stdout request loop |

## Dependencies in the workspace

- `nautilus-schema` for parsing and validated schema metadata
- `nautilus-core` for query AST types
- `nautilus-dialect` for SQL rendering
- `nautilus-connector` for execution
- `nautilus-migrate` for optional startup DDL application
- `nautilus-protocol` for wire-format types
