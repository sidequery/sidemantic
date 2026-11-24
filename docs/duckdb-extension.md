# DuckDB Extension

The sqlctx DuckDB extension embeds the Rust native runtime in DuckDB. It is a native-format deployment path, separate from the Python CLI and Python adapter ecosystem.

## Status

The extension is experimental and targets **DuckDB 2.0.1 and 1.5.6**. It is **not** in the
DuckDB community extension registry, so `INSTALL sqlctx FROM community` fails.
Community publication is planned; once it lands, installation will be:

```sql
INSTALL sqlctx FROM community;
LOAD sqlctx;
```

Build sqlctx from source until a release artifact is available. Start the
DuckDB CLI with unsigned-extension loading enabled:

```bash
duckdb -unsigned
```

[The pre-rename release](https://github.com/sidequery/sqlctx/releases/tag/sidemantic-duckdb-v0.1.0)
contains builds for DuckDB 1.5.6 under the original extension name.

## Build From Source

The extension build needs Rust, DuckDB extension build tooling, and Ninja.

```bash
cd sqlctx-duckdb
make deps DUCKDB_VERSION=v2.0.1
make
SQLCTX_NATIVE_PEG=1 make test
```

For the DuckDB 1.5.6 compatibility frontend, use `make deps DUCKDB_VERSION=v1.5.6`
(the default), then `make` and `make test`. Build each host version in a separate
checkout or clean build directory. The repository vendors the extension build tools.

The local loadable extension is produced at:

```text
sqlctx-duckdb/build/release/extension/sqlctx/sqlctx.duckdb_extension
```

Load it in the DuckDB shell built by the extension workflow:

```bash
./build/release/duckdb -unsigned -cmd "LOAD '$PWD/build/release/extension/sqlctx/sqlctx.duckdb_extension';"
```

The official macOS CLI requires an absolute path when loading the extension.

For embedded clients, set DuckDB's `allow_unsigned_extensions` database configuration before opening the connection.

The extension reads existing `__sidemantic_catalog` snapshots and
`.sidemantic.sql` sidecars. A catalog mutation stores the updated snapshot in
`__sqlctx_catalog`.

## Runtime API

Load native YAML:

```sql
SELECT * FROM sqlctx_load('
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
SELECT * FROM sqlctx_load_file('/path/to/models.yml');
```

Load native SQL definitions from disk:

```sql
SELECT * FROM sqlctx_load_file('/path/to/models.sql');
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
SELECT * FROM sqlctx_models();
```

Rewrite semantic SQL without executing it:

```sql
SELECT sqlctx_rewrite_sql('SELECT orders.revenue FROM orders');
```

Run a semantic query through the parser override:

```sql
SELECT orders.revenue FROM orders;
```

The legacy `SEMANTIC SELECT ...` form remains supported.

## Release Path

Use `.github/workflows/duckdb-extension-release.yml`.

The workflow:

- fetches each selected DuckDB release tag with `make deps` and verifies the vendored `extension-ci-tools`,
- builds the Rust-backed extension for `linux_amd64`, `linux_arm64`, `osx_amd64`, and `osx_arm64`,
- runs the DuckDB sqllogictests, including native YAML load, native SQL definition file load, relationship rewrite, semantic select, persistence, and invalid-version coverage,
- uploads a separate extension artifact for each DuckDB version and platform with its checksum,
- on a `sqlctx-duckdb-v*` tag push (or a manual run with `create_github_release`), creates the GitHub release with the artifacts, `SHA256SUMS`, and `LICENSE`. It never overwrites an existing release.

It does not publish to the DuckDB community extension registry.

## Compatibility

| Component | Current target |
|---|---:|
| Native format | `1` |
| Rust runtime crate | `0.1.0` |
| DuckDB extension release | `0.1.0` |
| DuckDB version | `2.0.1` or `1.5.6` |

DuckDB extension artifacts are ABI-sensitive. Rebuild the extension when changing the DuckDB target version or the Rust native runtime version.

For a database file written in DuckDB 2.0 storage format, open it with DuckDB
2.0.1 and load the matching 2.0.1 extension.

The extension should accept the same native YAML and native SQL definition contract as the Rust runtime. If extension-only syntax is added, it should either be aligned with the Rust SQL loader or documented as DuckDB-only.
