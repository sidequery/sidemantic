# Testing and trust

Sidemantic treats "will my metrics stay correct?" as a first-class product
question. Three mechanisms cover it: golden-query tests, live schema
validation, and the cross-surface SQL guarantee.

## Golden-query tests (`sidemantic test`)

Golden tests pin a query to its expected result so a metric definition cannot
drift silently. They live in the project's `tests/` directory as YAML files
with a top-level `tests` list:

```yaml
tests:
  - name: total revenue
    sql: SELECT orders.revenue FROM orders
    expect:
      value: 250.0
      tolerance: 0.01

  - name: status breakdown shape
    sql: SELECT orders.status, orders.revenue FROM orders ORDER BY orders.status
    expect:
      columns: [status, revenue]
      row_count: 3

  - name: exact rows
    sql: SELECT orders.status, orders.order_count FROM orders ORDER BY orders.status
    expect:
      rows:
        - [paid, 2]
        - [pending, 1]
```

`expect` supports:

| Key | Meaning |
| --- | --- |
| `value` | Single-row, single-column result equals this value |
| `rows` | Full result set equals these rows (in order) |
| `row_count` | Number of result rows |
| `columns` | Result column names (in order) |
| `tolerance` | Absolute tolerance for numeric comparisons |

Run them with:

```bash
sidemantic test                      # project tests/ directory
sidemantic test tests/revenue.yml    # specific files or directories
sidemantic test --json               # machine-readable report
```

The command exits 1 when any test fails, so it slots directly into CI.
`sidemantic init` and `sidemantic demo` scaffold a starting test file.

## Live schema validation (`sidemantic validate --live`)

Static validation cannot see a column that was dropped or renamed in the
warehouse; the model still parses and only fails at query time. `--live` probes
the configured database and fails validation instead:

```bash
sidemantic validate --live                      # uses the project connection
sidemantic validate --live --db data.duckdb
```

Every table-backed model is probed with a zero-row select, and plainly
referenced columns (primary keys, dimension/metric columns, foreign keys) are
checked against the live column list. Missing tables and columns are reported
as validation errors with the model and member name.

## The cross-surface guarantee

The SQL you preview is exactly the SQL that runs — in the CLI (`rewrite`,
`query --dry-run`), the Python API, and the HTTP API's compile endpoints. This
is enforced by the cross-surface equivalence suite
(`tests/core/test_cross_surface_equivalence.py`), which asserts byte-identical
compiled SQL and identical execution results across surfaces for
representative queries. Generated TypeScript/Python clients are covered by
byte-identical codegen tests.
