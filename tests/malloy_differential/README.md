# Malloy/DuckDB differential harness

This opt-in harness runs the official Malloy compiler/runtime and the local
Sidemantic DuckDB query path against the same deterministic fixture. The
manifest is declarative: `compatible` fixtures execute, while `unsupported`
and `planned` fixture families are reported without creating per-test xfails.

## Setup and run

From the repository root:

```bash
cd tests/malloy_differential
bun install --frozen-lockfile
bun test
```

`bun install` is the only network-capable setup step. The generated lockfile
pins `@malloydata/malloy` and `@malloydata/db-duckdb` to `0.0.332`; the test
runner uses the installed packages only. The Python side is invoked with
`uv run --offline --no-sync`, so dependency resolution and downloads are
disabled at test runtime.

`bun run test` is an equivalent direct runner entrypoint when fixture status
output without Bun's test reporter is preferred.

The runner creates a temporary DuckDB/Parquet workspace, seeds it from the
fixture SQL, executes the Malloy query with `DuckDBConnection`, imports the
same source with `MalloyAdapter(strict=True)`, executes the equivalent
Sidemantic query, normalizes schemas/rows, and compares both results against
the fixture's expected rows.

Add future cases to `fixtures/manifest.json` and keep unsupported behavior
declarative with a status and reason. The JSON shape is documented in
`fixtures/schema.json`.
