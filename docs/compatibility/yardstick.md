# Yardstick Compatibility

Sidemantic's Yardstick adapter parses SQL files containing `CREATE VIEW` statements that use the `AS MEASURE` syntax from Julian Hyde's ["Measures in SQL" proposal](https://arxiv.org/abs/2307.14009). It maps Yardstick concepts to Sidemantic's semantic model (Model, Dimension, Metric) and supports the `SEMANTIC SELECT`, `AGGREGATE()`, and `AT` query modifiers for measure-aware SQL queries.

Features are marked **supported**, **partial support**, or **unsupported**. Partial support entries include notes explaining the limitation.

---

## Schema Format

| Feature | Status |
|---------|--------|
| `.sql` files with `CREATE VIEW ... AS SELECT` | Supported |
| Directory parsing (recursive `.sql` discovery) | Supported |
| Multiple `CREATE VIEW` statements in one file | Supported |
| Empty SQL files | Supported (silently skipped) |
| `CREATE OR REPLACE VIEW` | Supported |
| Non-view statements (CREATE TABLE, INSERT, etc.) | Supported (silently skipped; not treated as models) |

The adapter only processes `CREATE VIEW` statements that contain at least one `AS MEASURE` alias. Views without any measures are skipped.

---

## Views (Models)

| Feature | Status |
|---------|--------|
| View name | Supported (becomes the Model name) |
| Simple `FROM table` (single table, no joins/WHERE) | Supported (stored as `Model.table`) |
| `FROM table WHERE condition` | Supported (base relation stored as `Model.sql`; `Model.table` is `None`) |
| `FROM table JOIN ... ON ...` | Supported (full base relation stored as `Model.sql`) |
| CTE-backed views (`WITH ... AS ... SELECT ...`) | Supported (CTEs included in `Model.sql`) |
| `FROM` with table alias | Supported (base relation preserved) |
| `SELECT *` (star projections) | Supported (star columns are silently skipped; only explicitly aliased columns become dimensions) |
| Primary key inference | Supported (defaults to the first dimension's name; falls back to `"id"` if no dimensions) |

The adapter stores the original view SQL in `Model.metadata["yardstick"]["view_sql"]` for reference. When a simple single-table source is detected, the table name is also stored in `metadata["yardstick"]["base_table"]`. For complex base relations, the reconstructed subquery SQL is stored in `metadata["yardstick"]["base_relation_sql"]`.

---

## Dimensions

Non-measure projections in the SELECT list become dimensions. The adapter infers types from the sqlglot AST.

| Feature | Status |
|---------|--------|
| Column references (e.g., `year`, `region`) | Supported |
| Aliased expressions (e.g., `DATE_TRUNC('month', order_date) AS month`) | Supported |
| Star expressions (`*`) | Supported (silently skipped, not added as dimensions) |
| Complex SQL expressions | Supported (expression preserved verbatim via sqlglot DuckDB dialect) |

### Type Inference

| Inferred Type | Detection Rule |
|---------------|----------------|
| `time` (granularity `second`) | Column name contains `timestamp` or `time` |
| `time` (granularity `day`) | Column name contains `date` |
| `time` (granularity varies) | Expression is a time function: `date` -> `day`, `date_trunc` -> `day`, `year` -> `year`, `quarter` -> `quarter`, `month` -> `month`, `week` -> `week`, `day` -> `day`, `hour` -> `hour`, `minute` -> `minute` |
| `boolean` | Expression is a boolean literal |
| `numeric` | Expression is a numeric literal |
| `categorical` | Default fallback |

Not mapped: dimension descriptions, labels, format, visibility, or primary key annotations (Yardstick's SQL format has no syntax for these).

---

## Measures

Projections tagged with `AS MEASURE` become metrics. The adapter classifies measures into several categories based on the expression structure.

### Standard Aggregations

| Feature | Status |
|---------|--------|
| `SUM(expr) AS MEASURE name` | Supported (maps to `agg="sum"`) |
| `AVG(expr) AS MEASURE name` | Supported (maps to `agg="avg"`) |
| `MIN(expr) AS MEASURE name` | Supported (maps to `agg="min"`) |
| `MAX(expr) AS MEASURE name` | Supported (maps to `agg="max"`) |
| `COUNT(*) AS MEASURE name` | Supported (maps to `agg="count"`, `sql="*"`) |
| `COUNT(expr) AS MEASURE name` | Supported (maps to `agg="count"`) |
| `COUNT(DISTINCT expr) AS MEASURE name` | Supported (maps to `agg="count_distinct"`) |
| `MEDIAN(expr) AS MEASURE name` | Supported (maps to `agg="median"`) |
| `STDDEV(expr) AS MEASURE name` | Supported (maps to `agg="stddev"`) |
| `STDDEV_POP(expr) AS MEASURE name` | Supported (maps to `agg="stddev_pop"`) |
| `VARIANCE(expr) AS MEASURE name` | Supported (maps to `agg="variance"`) |
| `VARIANCE_POP(expr) AS MEASURE name` | Supported (maps to `agg="variance_pop"`) |

### Filtered Aggregations

| Feature | Status |
|---------|--------|
| `AGG(expr) FILTER (WHERE condition) AS MEASURE name` | Supported (filter condition extracted and stored in `Metric.filters`) |

The filter condition is extracted from the `FILTER (WHERE ...)` clause and stored as a string in `Metric.filters`. The aggregation type and inner expression are extracted normally.

### Derived Measures

| Feature | Status |
|---------|--------|
| Measure referencing other measures (e.g., `revenue - cost AS MEASURE profit`) | Supported (maps to `type="derived"`) |
| Forward references (measure defined after use) | Supported (all measure names collected before classification) |
| Arithmetic over measures (`revenue * 2`, `a / b`) | Supported |

Derived measure detection works by scanning the expression's column references against the full set of measure names in the view. If any other measure is referenced, the measure is classified as derived.

### Non-Standard Aggregations

| Feature | Status |
|---------|--------|
| `MODE(expr) AS MEASURE name` | Supported (stored as raw SQL expression metric with `agg=None`) |
| `PERCENTILE_CONT(n) WITHIN GROUP (ORDER BY expr) AS MEASURE name` | Supported (stored as raw SQL expression metric) |
| `CASE WHEN AGG(...) THEN ... END AS MEASURE name` | Supported (detected as having aggregate semantics; stored as raw SQL expression metric) |
| Other aggregate functions not in the standard list | Supported (full expression preserved as `Metric.sql`) |

When a measure expression contains aggregate functions (detected by walking the AST for `AggFunc` nodes or known anonymous aggregations like `mode`) but doesn't match a simple aggregation pattern, the full expression is preserved as-is for query-time evaluation.

---

## Query Semantics

The Yardstick adapter works in tandem with Sidemantic's query rewriter to support the `SEMANTIC SELECT`, `AGGREGATE()`, and `AT` modifiers described in the Measures in SQL proposal.

### SEMANTIC Prefix

| Feature | Status |
|---------|--------|
| `SEMANTIC SELECT ...` | Supported (enables measure-aware query rewriting) |
| `SEMANTIC WITH ... SELECT ...` | Supported (CTEs within semantic queries) |
| Implicit measure detection without `SEMANTIC` prefix | Supported (queries containing `AT` modifiers or curly-brace measure references are auto-detected) |

### AGGREGATE() Function

| Feature | Status |
|---------|--------|
| `AGGREGATE(measure_name)` | Supported (evaluates the measure at the query's grouping level) |
| `schema.AGGREGATE(measure_name)` | Supported (schema-qualified function name) |
| `AGGREGATE(measure_name) AS alias` | Supported |
| Multiple `AGGREGATE()` calls in one query | Supported |
| `AGGREGATE()` in arithmetic expressions (`2 * AGGREGATE(revenue)`) | Supported |
| `AGGREGATE(measure) / AGGREGATE(measure) AT (...)` | Supported (each AGGREGATE evaluated independently) |
| Scalar `AGGREGATE()` without GROUP BY | Supported (produces a single grand-total row) |
| `AGGREGATE()` without `SEMANTIC` prefix and without `AT` | Error: raises `ValueError` requiring the `SEMANTIC` prefix |

### AT Modifiers

AT modifiers control the evaluation context of a measure, enabling semi-additive and comparative calculations.

| Feature | Status |
|---------|--------|
| `AT (ALL dimension)` | Supported (removes the named dimension from grouping, producing a subtotal) |
| `AT (ALL dim1 dim2)` | Supported (removes multiple dimensions in a single clause) |
| `AT (ALL)` | Supported (removes all dimensions, producing a grand total) |
| `AT (WHERE condition)` | Supported (filters the measure's evaluation context independently of the outer WHERE) |
| `AT (SET dim = value)` | Supported (pins a dimension to a constant value) |
| `AT (SET dim = dim - 1)` | Supported (pins a dimension to a computed expression, e.g., prior period) |
| `AT (SET dim = CURRENT dim - 1)` | Supported (`CURRENT` resolves to the outer query's current value of the dimension) |
| `AT (SET dim IN (values))` | Supported (predicate-form SET, filters the dimension to a set of values) |
| `AT (VISIBLE)` | Supported (evaluates the measure considering the outer WHERE clause) |
| `AT (SET ... VISIBLE)` | Supported (compound modifier combining SET with VISIBLE) |
| Chained AT: `AT (...) AT (...)` | Supported (modifiers applied left to right) |
| `AT (ALL expression)` with ad-hoc expressions (e.g., `AT (ALL MONTH(order_date))`) | Supported |
| `AT (SET expression = value)` with ad-hoc expressions | Supported |

### Wrapperless Measure References

| Feature | Status |
|---------|--------|
| `measure_name` as bare column reference (without `AGGREGATE()`) | Supported (auto-detected and rewritten when measure is known) |
| `measure_name AT (VISIBLE)` | Supported (measure reference with AT modifier, no AGGREGATE wrapper needed) |
| `{measure_name}` (curly-brace syntax) | Supported (explicit measure reference without AGGREGATE) |

Bare measure names in non-SEMANTIC queries default to evaluating at the full table level (no grouping restriction), whereas `AT (VISIBLE)` constrains evaluation to the outer WHERE context.

### Multi-Fact Joins

| Feature | Status |
|---------|--------|
| `FROM view_a JOIN view_b ON ...` in SEMANTIC queries | Supported (measures from different views evaluated against their own base tables) |
| AT modifiers on joined measures | Supported |

### GROUP BY Variants

| Feature | Status |
|---------|--------|
| Explicit `GROUP BY col1, col2` | Supported |
| Positional `GROUP BY 1, 2` | Supported (ordinals resolved to SELECT dimensions) |
| `GROUP BY` with extra whitespace | Supported |
| `GROUP BY ROLLUP(...)` | Supported |
| Omitted `GROUP BY` (grouping inferred from SELECT dimensions) | Supported |

---

## SQL Dialect

The adapter uses a dynamic dialect factory that extends any sqlglot dialect with `AS MEASURE` alias recognition. The dialect defaults to DuckDB but is configurable via the `dialect` parameter on `YardstickAdapter` (or inherited from `SemanticLayer.dialect` when loaded via `load_from_directory`).

| Feature | Status |
|---------|--------|
| DuckDB SQL syntax | Supported (default dialect) |
| PostgreSQL SQL syntax | Supported (`dialect="postgres"`) |
| Snowflake SQL syntax | Supported (`dialect="snowflake"`) |
| BigQuery SQL syntax | Supported (`dialect="bigquery"`) |
| Other sqlglot dialects | Supported (any dialect recognized by sqlglot) |
| Custom `AS MEASURE` token parsing | Supported (extends sqlglot's `_parse_alias`, portable across all dialects) |
| Standard SQL expressions (CASE, subqueries, window functions) | Supported (parsed by sqlglot) |

When a non-default dialect is used, all SQL expressions (dimension SQL, metric SQL, base relation SQL, metadata SQL) are serialized in the specified dialect.

---

## Export (Roundtrip)

Unsupported. The Yardstick adapter is import-only. There is no export path back to Yardstick SQL format.

---

## Limitations

| Limitation | Detail |
|------------|--------|
| No export/roundtrip | Cannot generate Yardstick SQL from a semantic graph |
| No dimension metadata | Yardstick SQL has no syntax for descriptions, labels, formats, or visibility on dimensions or measures |
| No relationships/joins at model level | Joins are handled at query time via SEMANTIC queries, not stored as model-level Relationships |
| No segments | Yardstick SQL has no concept of named filters at the model level |
| Primary key is heuristic | First dimension is assumed to be the primary key; no explicit PK declaration syntax exists |
| Type inference is heuristic | Dimension types are inferred from column names and expression structure, not from declared types |
