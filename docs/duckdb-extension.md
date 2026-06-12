# DuckDB Extension

The Sidemantic DuckDB extension embeds the Rust native runtime in DuckDB. It is a native-format deployment path, separate from the Python CLI and Python adapter ecosystem.

## Status

Current install path: build from source.

Planned community extension path, after publication:

```sql
INSTALL sidemantic FROM community;
LOAD sidemantic;
```

Until community publication is complete, use a local extension artifact.
Local artifacts are unsigned, so start the DuckDB CLI with unsigned-extension loading enabled:

```bash
duckdb -unsigned
```

## Build From Source

The extension build needs Rust, DuckDB extension build tooling, and Ninja.

```bash
cd sidemantic-duckdb
make deps DUCKDB_VERSION=v1.5.3
make
make test
```

`DUCKDB_VERSION` is intentionally guarded to `v1.5.3` because the repository
vendors a matching `extension-ci-tools` checkout. Update both together before
building against a different DuckDB tag.

The local loadable extension is produced at:

```text
sidemantic-duckdb/build/release/extension/sidemantic/sidemantic.duckdb_extension
```

Load it in the DuckDB shell built by the extension workflow:

```bash
./build/release/duckdb -unsigned
```

```sql
LOAD 'build/release/extension/sidemantic/sidemantic.duckdb_extension';
```

For embedded clients, set DuckDB's `allow_unsigned_extensions` database configuration before opening the connection.

## Runtime API

Load native YAML:

```sql
SELECT * FROM sidemantic_load('
version: 1
models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
');
```

Load native YAML from disk:

```sql
SELECT * FROM sidemantic_load_file('/path/to/models.yml');
```

Load native SQL definitions from disk:

```sql
SELECT * FROM sidemantic_load_file('/path/to/models.sql');
```

Native SQL definitions can also be entered directly through the parser extension:

```sql
MODEL (name orders, table orders, primary_key order_id);
DIMENSION (name status, type categorical);
METRIC revenue AS SUM(amount);
```

Compact native SQL model blocks are also accepted directly:

```sql
model orders from orders (
  primary key (order_id)
  status
  sum(amount) as revenue
  count(*) as order_count
);
```

Inspect loaded models:

```sql
SELECT * FROM sidemantic_models();
```

Rewrite semantic SQL without executing it:

```sql
SELECT sidemantic_rewrite_sql('SELECT orders.revenue FROM orders');
```

Run a semantic query through the parser override:

```sql
SELECT orders.revenue FROM orders;
```

The legacy `SEMANTIC SELECT ...` form remains supported.

## Release Path

Use `.github/workflows/duckdb-extension-release.yml`.

The workflow:

- fetches DuckDB with `make deps` and verifies the vendored `extension-ci-tools`,
- builds the Rust-backed extension,
- runs the DuckDB sqllogictests, including native YAML load, native SQL definition file load, relationship rewrite, semantic select, persistence, and invalid-version coverage,
- uploads a Linux extension artifact,
- optionally attaches that artifact to a GitHub release.

The workflow is source-package oriented. It does not publish to the DuckDB community extension registry.

## Compatibility

| Component | Current target |
|---|---:|
| Native format | `1` |
| Rust runtime crate | `0.1.0` |
| DuckDB extension source package | `0.1.0` |
| DuckDB build target | `1.5.3` |

DuckDB extension artifacts are ABI-sensitive. Rebuild the extension when changing the DuckDB target version or the Rust native runtime version.

The extension should accept the same native YAML and native SQL definition contract as the Rust runtime. If extension-only syntax is added, it should either be aligned with the Rust SQL loader or documented as DuckDB-only.
