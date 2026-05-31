# Native Fixture Suite

The native fixture suite is the shared compatibility corpus for native Sidemantic YAML and SQL projects. It lives under `tests/native-fixtures/` and is driven by `tests/native-fixtures/manifest.yml`.

The suite is intentionally format-first. It does not test LookML, MetricFlow, Cube, Hex, Rill, Malloy, or other external import formats directly. Those adapters should export native YAML/SQL, then native fixtures and Rust validation should prove the exported project behaves correctly.

## Layout

Each fixture should use this shape:

```text
fixture_name/
  README.md
  models/
    *.yml
    *.yaml
    *.sql
  queries/
    *.query.yml
  seed/
    duckdb.sql
  expected/
    validation.json
    result.json
```

`models/` may contain native YAML, native SQL definitions, or both. `queries/` contains structured semantic query payloads that can be passed to Python `SemanticLayer.compile()` and converted into Rust `SemanticQuery`.

`seed/duckdb.sql` and `expected/result.json` are required for fixtures that claim executable result parity. Fixtures that only prove validation or SQL shape may omit `expected_result` from the manifest. Validation-only fixtures may set `queries: []`.

## Manifest Contract

Every fixture is listed in `manifest.yml`.

Valid fixtures default to `valid: true` and contain one or more query cases:

```yaml
- name: basic_model
  valid: true
  seed: seed/duckdb.sql
  expected_validation: expected/validation.json
  queries:
    - name: revenue_by_status
      file: queries/revenue_by_status.query.yml
      expected_result: expected/result.json
      result_columns: [status, total_revenue]
      sql_contains: [SUM, orders, status]
```

Invalid fixtures set `valid: false` and list stable error text fragments:

```yaml
- name: unsupported_version
  valid: false
  expected_validation: expected/validation.json
  error_contains:
    - Unsupported native Sidemantic format version
```

`sql_contains` is a shape check, not SQL string parity. It should assert stable semantic clauses or identifiers, not formatting.

Use `rust_sql_contains` for SQL shape that only the Rust compiler should emit. Use `rust_expected_result` only when row-result parity is intentionally Rust-only, and include `rust_only_reason` explaining the divergence. Shared Python/Rust behavior should use `expected_result`. Current Rust-only expected results are limited to SQL-emitted table calculations because Python's native query API does not accept `table_calculations` yet.

## Current Coverage

The suite currently covers:

- Basic model loading, compilation, and DuckDB execution.
- Native SQL-backed model sources.
- Default time dimensions and default grains.
- Segments composed with metric-local filters.
- Derived and ratio metrics.
- One-hop relationships.
- Composite primary and foreign keys.
- Multi-hop join path resolution.
- Many-to-many join path resolution through a junction model.
- Fanout symmetric aggregation with shared Python/Rust expected rows.
- `source_uri`-only model loading.
- Parameter interpolation in query filters.
- Pre-aggregation routing shape and DuckDB execution against seeded rollup tables.
- Semantic SQL rewrite cases for single-model and relationship queries.
- Query-local table calculations for the shared Python/Rust subset. Python applies these after fetching rows;
  Rust compiles them into SQL window expressions.
- Native `.sql` definition files.
- Native SQL frontmatter model definitions.
- YAML `sql_metrics` and `sql_segments` blocks.
- Mixed YAML and SQL project directories.
- Compile coverage for cumulative, time comparison, conversion, retention, and cohort metrics.
- Invalid duplicate dimensions, invalid default time dimensions, and invalid pre-aggregation references.
- Unsupported native format version rejection.

Planned fixture categories from the roadmap that still need dedicated coverage include Jinja templates, row-result parity for advanced metrics, and additional invalid validation fixtures.

## Test Runners

Python runner:

```bash
uv run pytest tests/native_compat -v
```

The Python runner loads every manifest fixture and asserts `expected/validation.json` agrees with manifest validity. For valid fixtures it compiles each query and executes DuckDB-backed fixtures against `seed/duckdb.sql`. For invalid fixtures it asserts the project does not produce a usable graph and that the reported error text matches the manifest.

Rust runner:

```bash
cd sidemantic-rs
cargo test --test native_fixtures
cargo test --features adbc-exec --test native_fixtures native_fixtures_execute_expected_results_with_duckdb_adbc
```

The default Rust runner loads every manifest fixture, asserts `expected/validation.json` agrees with manifest validity, expects invalid fixtures to fail, converts query YAML into `SemanticQuery`, compiles every valid query, checks SQL shape tokens, and runs manifest semantic SQL rewrite cases.

The `adbc-exec` Rust runner executes every query with `expected_result` or `rust_expected_result` through DuckDB ADBC, using the fixture seed SQL and result columns from the manifest. Any Rust-only expected output must include `rust_only_reason`. It is enabled in CI after installing the DuckDB ADBC driver.

Table-calculation fixture contract:

- Shared table calculations may use `percent_of_total`, `percent_of_previous`, `running_total`, `rank`, `row_number`, or `moving_average`.
- Shared calculations should include deterministic query `order_by` when row order affects the result.
- Python evaluates shared calculations with `TableCalculationProcessor` after query execution.
- Rust evaluates shared calculations by compiling them into SQL expressions.
- Rust-only table calculation types (`dense_rank`, `difference`, `lead`, `lag`) must use `rust_expected_result` and `rust_only_reason`.
- Python-only post-query table calculation types (`percent_of_column_total`, `percentile`) stay out of shared native fixtures until Rust supports them.

## Adding Fixtures

Add the narrowest fixture that proves one semantic behavior. Avoid kitchen-sink fixtures unless the behavior itself is cross-feature interaction.

Every new valid fixture should include:

- A README naming the behavior under test.
- A manifest entry.
- At least one query.
- `sql_contains` tokens for stable SQL shape.
- DuckDB seed and expected rows when the behavior can be executed locally.

Every new invalid fixture should include:

- A README naming the rejected behavior.
- `expected/validation.json`.
- `valid: false` in the manifest.
- `error_contains` tokens that are stable enough to guide users.
