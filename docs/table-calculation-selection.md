# Selecting result calculations

Declare table calculations in your native model file, then select names with the CLI:

```bash
sidemantic query --explore revenue_overview --order-by 'orders.revenue DESC' \
  --table-calculation running --table-calculation share \
  --models ./models --db data.duckdb
```

Repeat `--table-calculation` in dependency order. Add `--dry-run` to inspect the
SQL, or use `--saved-query` instead of `--explore` for a saved selection.

For Python automation, use
`SemanticLayer.compile(..., table_calculations=["running", "share"])` or
`SemanticLayer.query(...)`. The canonical structured query accepts the same
`table_calculations` array in native Rust and WASM. Unselected declarations are
inert. The SQL rewrite route does not accept calculation selection.

Calculations operate on the finalized semantic result, after security filters,
aggregation, ordering, limit, and offset. Names resolve to result column names;
a calculation can reference an earlier selected calculation. Hidden dependencies
are not injected. Unknown references, duplicate selections, and names colliding
with output columns are rejected. Calculations precede `post_process`.

The supported numeric calculations are formula, percent of total, percent of
previous (percentage change), percent of column total, running total, rank,
row number, percentile, and moving average. Formula syntax supports numeric
constants, `${column}` references, parentheses, unary signs, and `+`, `-`, `*`,
`/`. Exponentiation, modulo, floor division, calls, and bare identifiers are
rejected. Expressions are limited to 8192 UTF-8 bytes and 64 recursive parser
levels. Division by zero produces NULL. Numeric fields must be usable with the
database's arithmetic operators; selection checks names, not runtime value types
or overflow. Database numeric precision and range still apply.

Running total, percent of previous, moving average, row number, and rank require
an explicit `order_by` using selected columns, optionally with ASC/DESC and NULLS
FIRST/LAST. Include a unique tie breaker when row order matters. The initial
result order is captured once and preserved through every calculation. Rank
uses descending values with NULL treated as zero for sorting, while distinguishing
NULL from zero for ties, matching the existing Python row processor.

NULL becomes zero for formula references, sums, and moving averages; moving
average includes NULL rows in its denominator. Percentage totals return zero
when their denominator is zero. Percent of previous returns NULL when the prior
value is NULL or zero. Percentile ignores NULL and interpolates linearly.
Percent of column total alone supports `partition_by`. Other nonempty partition
controls and calculation-level `order_by` are rejected because the Python row
processor ignores them.

DuckDB and PostgreSQL SQL are supported. Empty results retain the selected
calculation column names, unlike the standalone Python row processor, which
returns only the original column names when no rows exist. Nonempty results are
qualified against that independent processor in the semantic conformance suite;
the WASM host executes the same fixture's expected values.
