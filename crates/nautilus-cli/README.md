# nautilus-cli

`nautilus-cli` builds the `nautilus` binary: the main entry point for schema validation, code generation, live-database workflows, migrations, engine startup, a small Python shim installer, and Nautilus Studio lifecycle management.

## Current command surface

| Command | Notes |
| --- | --- |
| `nautilus generate [--schema <path>] [--standalone]` | Validates the schema and generates the client selected by the generator block |
| `nautilus validate [--schema <path>]` | Parse + validate only |
| `nautilus format [--schema <path>]` | Rewrites a schema file in canonical formatting |
| `nautilus db push [--schema <path>] [--accept-data-loss] [--no-generate]` | Diffs the live database against the schema and applies the change set |
| `nautilus db status [--schema <path>]` | Shows the pending diff without applying it |
| `nautilus db pull [--schema <path>] [--output <path>]` | Introspects a live database and writes a `.nautilus` schema |
| `nautilus db drop [--schema <path>] [--force]` | Drops all live tables without recreating them |
| `nautilus db reset [--schema <path>] [--force] [--only-data]` | Drops and recreates schema state, or truncates only data when `--only-data` is set |
| `nautilus db seed <file>` | Executes a SQL seed script |
| `nautilus migrate generate [label]` | Writes versioned `.up.sql` / `.down.sql` files from the current diff |
| `nautilus migrate apply` | Applies pending migrations |
| `nautilus migrate rollback [--steps N]` | Rolls back the last applied migrations |
| `nautilus migrate status` | Shows applied vs pending migrations |
| `nautilus engine serve [--schema <path>]` | Starts the JSON-RPC engine on stdin/stdout |
| `nautilus python install` / `uninstall` | Installs or removes a `.pth` shim so `python -m nautilus` resolves to the CLI binary |
| `nautilus studio [--update] [--uninstall]` | Downloads the latest built Studio release on first run, optionally refreshes or removes it, installs runtime dependencies, and starts it |

## Day-to-day workflows

### Schema-first development

```bash
nautilus validate --schema schema.nautilus
nautilus db status --schema schema.nautilus
nautilus db push --schema schema.nautilus
```

`db push` regenerates the client automatically after a successful diff/apply unless you pass `--no-generate`.

### PostgreSQL extensions

For PostgreSQL, datasource-level extensions are applied as part of `db push`
before table/type DDL:

```prisma
datasource db {
  provider            = "postgresql"
  url                 = env("DATABASE_URL")
  extensions          = [citext, hstore, ltree, vector]
  preserve_extensions = true
}
```

Use `Vector(dim)` fields with the `vector` extension for pgvector embeddings.

`db pull` writes installed PostgreSQL extensions back to the datasource block.
The extension list is declarative: an extension that exists in the live database
but is absent from the schema is shown as a destructive drop. Nautilus emits the
drop without `CASCADE`, so PostgreSQL will reject it while dependent objects
still exist. Set `preserve_extensions = true` to keep extra live extensions
that are managed outside Nautilus.

### Versioned migrations

```bash
nautilus migrate generate add_users --schema schema.nautilus
nautilus migrate apply --schema schema.nautilus
nautilus migrate status --schema schema.nautilus
```

### Local engine debugging

```bash
nautilus engine serve --migrate
```

### Plain Java bundle generation

```bash
nautilus generate --schema schema.nautilus
```

When the schema uses `provider = "nautilus-client-java"` and `mode = "jar"`,
the same command writes the normal Maven module to `output/` and also leaves a
plain Java bundle at `output/dist/{artifact_id}.jar` plus `output/dist/lib/*.jar`.

## Notes

- If `--schema` is omitted, schema-based commands auto-detect the first `.nautilus` file in the current directory.
- The generator provider inside the schema decides the client target: `nautilus-client-rs`, `nautilus-client-py`, `nautilus-client-js`, or `nautilus-client-java`.
- For JS and Python, `nautilus generate` produces local source packages first. The normal workflow is to import the generated `output` directory; `install = true` only copies the same files into local `site-packages/nautilus` or `node_modules/nautilus` for convenience.
- For Java, `nautilus generate` writes a Maven module to the configured `output` directory by default. When the schema sets `mode = "jar"`, generation also builds `output/dist/{artifact_id}.jar` plus `output/dist/lib/*.jar` for plain `java` / `javac` usage, then removes the temporary `.nautilus-build` directory. `install = true` is currently ignored for the Java generator.
- `--standalone` is meaningful only for the Rust generator; it emits a `Cargo.toml` next to the generated Rust sources.
- The Python shim command is intentionally separate from code generation. It exists to make the installed CLI reachable from Python as `python -m nautilus`; it does not install or publish generated ORM clients.
- `nautilus studio` looks up the latest GitHub Release for `STUDIO_GITHUB_REPO`, downloads the ZIP asset for the current platform named `nautilus-orm-studio-${tag}-${os}.zip` (`windows`, `linux`, or `macos`), extracts it into the local Nautilus data directory, installs runtime dependencies from the packaged `package-lock.json`, and launches Next from the project directory where `nautilus studio` was invoked while pointing it at the cached Studio app.

## Main dependencies

- `nautilus-schema` for parsing, validation, and formatting
- `nautilus-codegen` for generation and validation subcommands
- `nautilus-migrate` for schema diffing and migration execution
- `nautilus-engine` for `engine serve`
