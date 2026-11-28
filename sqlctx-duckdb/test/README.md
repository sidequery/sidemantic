# Testing this extension

The `sql` directory contains executable DuckDB SQLLogicTests for semantic queries,
definitions, native contracts, routing, transactions, restart and legacy migration.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or
```bash
make test_debug
```

For DuckDB 2.0.1, use `SQLCTX_NATIVE_PEG=1 make test`. Its native
grammar suite explicitly disables parser overrides before executing custom syntax.
The same suite is skipped on DuckDB 1.5.6.

Run `test_semantic_input_host.py` with `uv run` and set `SQLCTX_DUCKDB_BINARY`
and `SQLCTX_DUCKDB_EXTENSION` to the freshly built artifacts. These tests execute
returned SQL and check policy isolation and invalid arguments. See the main README
for the complete invocation.

`test_catalog_host.py` uses the same environment variables and verifies
SHOW/DESCRIBE metadata, compatible dimensions, native SELECT, prepared reads,
transaction rollback, and export/import across restart and read-only connections.
It also imports unchanged, checksum-pinned upstream Ossie 0.1.1 and 0.3.0.dev0
TPC-DS files, queries their metrics against seeded physical tables, and repeats
the results after a read-only reopen. Provenance and licenses live beside the
fixtures under `tests/ossie-fixtures/upstream/{faf5810,d27a6be}`; CI needs no network.

`test_client_routing_host.py` additionally requires the Python `duckdb` wheel
matching the built host version. It exercises `con.sql().show()` and fetches,
relation composition, parameters, physical-name collisions, transaction rollback,
persisted views with parser settings, duplicate output labels, and CLI agent mode.

`test_duckflight_host.py` runs the SQL through an authenticated DuckFlight
PostgreSQL listener using psycopg. Set `DUCKFLIGHT_EXTENSION` as well as the two
sqlctx host variables. It checks ordinary and prepared semantic SELECT,
bound predicates, result metadata, discovery, and definition transactions.

`test_release_workflow.py` exercises the release input allowlist without building
or publishing artifacts.

## Catalog benchmark

From the repository root, run the same DuckDB shell with each extension binary.
The benchmark checks result values, field names, types and ordering before timing.

```sh
uv run --no-project sqlctx-duckdb/scripts/benchmark_catalog.py --binary /path/to/duckdb --extension /path/to/sqlctx.duckdb_extension
```

On macOS ARM64 with DuckDB 2.0.1, 25 models shared a 1,000-row table. Each
model declared one dimension and eight decimal sum metrics. The table gives
median wall time across seven samples after two warmups. The baseline extension
had SHA-256 prefix `ca6127873395`.

| Query | Baseline | With deferred metadata binding |
|---|---:|---:|
| List 25 semantic views | 912 ms | 4 ms |
| Read one model's nine names and types | 860 ms | 19 ms |
| Read those column names alone | 844 ms | 5 ms |
| Show one model's eight metrics | 34 ms | 14 ms |
| Query one semantic metric | 4 ms | 5 ms |

For ordinary SQL with the extension loaded, median CPU time fell from 0.978
to 0.250 ms for `select 42` and from 1.096 to 0.336 ms for the physical
aggregate. The same shell without the extension used 0.067 and 0.116 ms
respectively in the latter run.
