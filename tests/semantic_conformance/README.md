# Semantic compiler conformance

`fixtures/native/*.yml`, `fixtures/cube.yml`, and `fixtures/ossie.yml` are independently authored source
inputs. `fixtures/seed.sql` supplies deliberately small synthetic data; these are
not production datasets. `fixtures/cases.yml` specifies expected columns and
rows independently of either compiler. Its error cases require compile-time
rejection with a specific exception and diagnostic.

`test_reference.py` loads the source adapter into the semantic layer,
compiles the structured query, and executes generated SQL in DuckDB. It checks
the database's actual output column names as well as row values. Each case gets
a fresh layer and connection. `test_cli.py` checks the supported single-query
join rewrite through the CLI for both source formats and executes the output.
The Ossie case uses schema-validated source parsing and lowering, verifies the
complete aggregate expression survives the handoff, and checks its independent
result using the same compiler parameters.

Run with `uv run pytest --no-cov -q tests/semantic_conformance`.

Both tests parameterize explicit `python` and `rust` engine selection. Rust cases
require the real extension; absence skips only the Rust parameters and is not
acceptance evidence. The final acceptance run must execute those parameters
with the versioned handoff implementation installed. `rust_unsupported` entries
require typed capability rejection instead of Python fallback. Independent
result assertions must never be replaced with one compiler's observed output.

`test_approximate_distinct.py` qualifies direct model `approx_count_distinct`
metrics on DuckDB with explicit source-row SQL expressions. Its synthetic
30,000-row population repeats 10,000 identifiers and compares results with an
independent DuckDB approximate query whose estimate differs from exact distinct.
It also covers metric filters, query filters, grouped NULL populations, and
empty grouped and scalar aggregate populations. Rust cases require the real extension.
Joined populations, derived and temporal calculations, cohort aggregates,
snapshots, stored rollup routing, metric-result filters, ungrouped queries, and
other output dialects have their own result or rejection cases in the suite.
Stored scalar estimates must never be summed or treated as mergeable sketches.

## Engine selection and acceptance

The main CI workflow runs the shared suite with `--test-engine python` and
`--test-engine rust`. The Rust selection does not enable fallback. Security,
invariant, Malloy, and planner execution tests follow that selection. Python
optimizer rule names remain implementation tests; planner result comparisons
execute through the selected engine against an independent Python baseline.
The legacy shared-contract harness additionally forbids Python compilation
and checks Rust selection. Its direct Rust graph/compiler probes remain separate.

A successful shared run is not evidence that every optional test executed:

- PostgreSQL conformance cases require `SIDEMANTIC_TEST_POSTGRES_DSN`; the native
  CI job supplies a PostgreSQL service and runs this suite with the built extension.
- Native binding and validation tests may skip their unused Python parameter.
  The Rust parameter must execute. Such skips are not missing Rust coverage.
- SQLite ADBC tests use an installed driver; CI provides `adbc-driver-sqlite`.
  Arrow tests execute when the optional Arrow dependency is present.
- Widget/workbench dependencies and online DAX/Yardstick services are separate
  host or external-service requirements, not semantic-compiler substitutions.
- The default `not integration` selection excludes database/CLI and online
  Power BI tests. The configured PostgreSQL, BigQuery, Snowflake, ClickHouse,
  and ADBC integration jobs run both engines. Tests requiring other live
  services still need their documented environment and credentials.

Unsupported inputs must have explicit rejection assertions, not broad expected
failures. For example, MetricFlow millisecond dimensions and hourly
`grain_to_date` declarations are rejected during import. Retention grouping,
aggregate-result filters, and derived wrappers are not silently accepted as
working query shapes.
