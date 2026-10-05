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

For the pinned Cyanoptera build, use `SIDEMANTIC_NATIVE_PEG=1 make test`. Its native
grammar suite explicitly disables parser overrides before executing custom syntax.
The same suite is skipped on DuckDB 1.5.6.

Run `test_semantic_input_host.py` with `uv run` and set `SIDEMANTIC_DUCKDB_BINARY`
and `SIDEMANTIC_DUCKDB_EXTENSION` to the freshly built artifacts. These tests execute
returned SQL and check policy isolation and invalid arguments. See the main README
for the complete invocation.

`test_catalog_host.py` uses the same environment variables and verifies
SHOW/DESCRIBE metadata, compatible dimensions, native SELECT, prepared reads,
transaction rollback, and export/import across restart and read-only connections.

`test_duckflight_host.py` runs the SQL through an authenticated DuckFlight
PostgreSQL listener using psycopg. Set `DUCKFLIGHT_EXTENSION` as well as the two
Sidemantic host variables. It checks ordinary and prepared semantic SELECT,
bound predicates, result metadata, discovery, and definition transactions.

`test_release_workflow.py` exercises the release input allowlist without building
or publishing artifacts.
