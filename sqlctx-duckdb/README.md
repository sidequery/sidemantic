# sqlctx DuckDB Extension

A DuckDB extension that adds a SQL-first semantic layer. Define metrics and dimensions once, query them anywhere with automatic SQL rewriting.

## Features

- **Pure SQL Definition**: Define models, metrics, and dimensions using SQL statements
- **Automatic Query Rewriting**: Query qualified model fields directly and get proper aggregations automatically
- **Cross-Model JOINs**: Automatically generates JOINs when querying across related models
- **Fan-out Protection**: Uses model keys to protect supported aggregations against duplicated rows from joins
- **Definition Files**: Load native YAML, Cube.js YAML, and native SQL definition files
- **Transactional Definitions**: Commit and roll back model changes with DuckDB transactions
- **Catalog Discovery**: Inspect definitions, metadata, relationships and compatible dimensions with SQL
- **SQL Containers**: Use semantic queries in views, `CREATE TABLE AS`, and `INSERT ... SELECT`
- **Native PEG Grammar**: Composable grammar on DuckDB 2.0.1, with a DuckDB 1.5.6 compatibility frontend

## Installation

Build locally against DuckDB 2.0.1:

```bash
make deps DUCKDB_VERSION=v2.0.1
make
SQLCTX_NATIVE_PEG=1 make test
./build/release/duckdb -unsigned -cmd "LOAD '$PWD/build/release/extension/sqlctx/sqlctx.duckdb_extension';"
```

For embedded clients, set DuckDB's `allow_unsigned_extensions` database configuration before opening the connection. Community extension installation is planned, but this repository does not yet publish the signed multi-platform artifacts required for `INSTALL sqlctx FROM community`.

The [community registry submission guide](community/README.md) prepares an immutable
descriptor for both release lines and documents the registry's monorepo build contract.

The release workflow builds unsigned DuckDB 2.0.1 and 1.5.6 packages for Linux and macOS
on AMD64 and ARM64. Linux builds use a glibc 2.28 baseline. When installing a
release artifact, verify it against `SHA256SUMS` and give it the filename
`sqlctx.duckdb_extension` before loading it.

Semantic queries use ordinary `SELECT`, including in shells that preload
`autocomplete`. The older `SEMANTIC SELECT` spelling remains supported.

## Quick Start (Pure SQL)

Define your semantic layer entirely in SQL, no YAML required:

```sql
-- 1. Create your data table
CREATE TABLE orders (order_id INT, status VARCHAR, amount DECIMAL(10,2));
INSERT INTO orders VALUES
    (1, 'completed', 100.00),
    (2, 'completed', 150.00),
    (3, 'pending', 75.00);

-- 2. Define a semantic model
MODEL (
    name orders_model,
    table orders,
    primary_key order_id
);

-- 3. Define metrics (aggregations)
METRIC revenue AS SUM(amount);
METRIC order_count AS COUNT(*);
METRIC avg_order_value AS AVG(amount);

-- 4. Define dimensions (grouping attributes)
DIMENSION (name status, type categorical);

-- 5. Query using semantic layer: revenue is summed per status
SELECT orders_model.status, orders_model.revenue FROM orders_model ORDER BY 1;

-- ┌───────────┬───────────────┐
-- │  status   │    revenue    │
-- │  varchar  │ decimal(38,2) │
-- ├───────────┼───────────────┤
-- │ completed │        250.00 │
-- │ pending   │         75.00 │
-- └───────────┴───────────────┘

-- The same query through the model's relation in the semantic schema
SELECT status, revenue FROM semantic.orders_model;

-- Inspect the generated SQL
SELECT sqlctx_rewrite_sql('SELECT orders_model.status, orders_model.revenue FROM orders_model');
```

Released Ossie `0.1.1` and flat `0.3.0.dev0` logical models load through
`sqlctx_load_file`. Query model metrics with ordinary SQL, for example
`select total_sales from metrics` for the upstream TPC-DS model.
[Ossie file examples and supported draft semantics](../docs/duckdb-extension.md#ossie-files)

## Discover and manage definitions

```sql
SHOW MODELS;
SHOW METRICS FROM orders_model;
SHOW DIMENSIONS FROM orders_model;
SHOW RELATIONSHIPS FROM orders_model;
SHOW SEGMENTS FROM orders_model;
DESCRIBE MODEL orders_model;

-- Dimensions that can be combined with this metric
SHOW DIMENSIONS FOR orders_model.revenue;

-- Validate the complete selection, predicates and physical database bindings
EXPLAIN SELECT orders_model.status, orders_model.revenue
FROM orders_model WHERE orders_model.status = 'completed';
```

Discovery returns the definition kind, model and qualified name, label,
description, semantic type, data type, SQL, aggregation, relationship
target and cardinality, granularity, visibility, and the JSON definition.
`data_type` is the declared type or, when none is declared, the type the field
produces in the current database. Absent metadata is SQL `NULL` and omitted from
the JSON definition. Definition statements and file loads report what they
created, replaced or dropped. `DESCRIBE MODEL` includes the model and all its
fields. `SHOW DIMENSIONS FOR` checks each dimension with the query compiler;
use `EXPLAIN SELECT` to validate a complete combination of metrics and dimensions.
`SHOW SEMANTIC METRICS`, and the corresponding forms for other kinds, are also
accepted.

Standard SQL clients can also discover loaded models as virtual views in the
current database's `semantic` schema through `information_schema.tables`,
`information_schema.columns`, and the corresponding `pg_catalog` relations.
Query them with ordinary SQL, for example
`select status, revenue from semantic.orders_model`. Dimension and metric column
types come from binding their compiled expressions against the current database.
If a source cannot bind, its fields remain visible with `NULL` data types and
`duckdb_views().is_bound = false`. An existing physical `semantic.orders_model`
table or view takes precedence. Discovery observes transactions and creates no
persistent schema or views; `SET schema = 'semantic'` requires an actual schema
created by the user.

A `semantic.<model>` relation is grouped by the model dimensions the surrounding
query uses, so it can be joined, filtered, aggregated, described and summarized
like a view:

```sql
SELECT s.status, s.revenue, t.target
FROM semantic.orders_model s JOIN status_targets t USING (status);

SELECT sum(revenue) FROM semantic.orders_model WHERE status = 'completed';

DESCRIBE semantic.orders_model;
```

- Dimensions in `SELECT`, `GROUP BY`, `ORDER BY`, join conditions and `USING`
  set the grain. `SELECT *` selects every dimension and metric, so it returns
  one row per distinct combination of all dimensions.
- `WHERE` predicates that read only one relation's dimensions filter the model
  before grouping and do not add to the grain.
- Aggregating a metric again is allowed when each group holds one row of the
  relation, or when the aggregation composes: `sum` of a `sum` or `count`
  metric, `min` of `min`, `max` of `max`. Other cases, such as `avg` of a ratio
  over a coarser grain, are rejected because they compute a different number.
- A relation needs at least one field: `SELECT count(*) FROM semantic.orders_model`
  is rejected.

Qualified definitions work independently of the connection's active model:

```sql
CREATE OR REPLACE METRIC orders_model.revenue AS SUM(amount);
CREATE OR REPLACE DIMENSION orders_model.status AS status;
CREATE SEGMENT orders_model.completed AS status = 'completed';
DROP SEGMENT orders_model.completed;
DROP METRIC IF EXISTS orders_model.old_metric;
DROP MODEL IF EXISTS retired_model;
```

Removal uses dependency restrictions: a referenced model or field cannot be
dropped until the dependent definitions are removed or changed. There is no
implicit cascade. Definitions, removal and import participate in transactions;
`PREPARE` and `EXPLAIN` do not apply a mutation.

```sql
EXPORT SEMANTIC CATALOG;
-- Returns one versioned JSON snapshot in the definition column.
IMPORT SEMANTIC CATALOG '<snapshot returned by EXPORT>';
```

Export includes models, global metrics, parameters, table calculations and
metadata. Import validates the complete candidate and merges it atomically into
the selected database. A definition with the same name is replaced. Export and
discovery work on read-only databases and observe the current transaction.

### Named semantic views

Publish a selection of fields from a model or another semantic view. Each query
of the saved selection chooses its grouping dimensions:

```sql
CREATE SEMANTIC VIEW order_report AS
SELECT status, revenue AS gross_revenue FROM orders_model;

SELECT status, gross_revenue FROM order_report ORDER BY status;
SELECT gross_revenue FROM order_report;
```

The view retains measure definitions, including filters and ratio components.
`CREATE OR REPLACE SEMANTIC VIEW` and `DROP SEMANTIC VIEW` use the same
transaction and dependency rules as models. See the [extension guide](../docs/duckdb-extension.md)
for attached catalogs and ordinary DuckDB views.

## Versioned SemanticInput

The table function `sqlctx_query_semantic_input(input_json, query_json)` accepts
the same SemanticInput v1 contract as the Python and WASM hosts and returns its
query result directly. Its columns and types follow the requested metrics and
dimensions, including retention's cohort table. It does not change models loaded
through the session APIs.

```sql
select * from sqlctx_query_semantic_input(
    '{"version":1,"input_dialect":"duckdb","models":[{"name":"sales","table":"orders","primary_key":"order_id",
      "metrics":[{"name":"revenue","agg":"sum","sql":"amount"}]}]}',
    '{"metrics":["sales.revenue"]}'
);
```

Use this relation in joins, CTEs, views and outer filters. DuckDB binds the
generated query in the calling connection, so temporary tables, uncommitted writes
and rollback have their normal SQL behavior. Arguments must be available at bind
time: literals, constant expressions or bound prepared-statement parameters;
per-row request columns are not supported. Dimensions in the query JSON determine
the aggregation grain; an outer projection does not change that grain.

The scalar functions `sqlctx_compile_semantic_input(input_json, query_json)` and
`sqlctx_rewrite_semantic_input(input_json, sql, context_json)` return generated SQL
for inspection or execution elsewhere. Pass `user_attributes` and
`enforce_visibility` in query JSON for compilation and direct queries, or in the
third argument for rewriting. Policies and invariant filters apply to all three
entrypoints; missing policy attributes, visibility denials and invalid contracts
raise SQL errors. The scalar functions return NULL for SQL NULL arguments; the
relation rejects NULL because it cannot determine a result schema. Embedded NUL
bytes are rejected. Each call uses its own caller context.

CI runs `test/test_semantic_input_host.py` against the freshly built DuckDB shell
and loaded extension. It checks direct relations and returned SQL over real rows,
policy isolation, prepared parameters, transactions, temporary tables, retention,
invalid contracts and argument handling. To run it locally after a build:

```bash
SQLCTX_DUCKDB_BINARY=build/release/duckdb \
SQLCTX_DUCKDB_EXTENSION=build/release/extension/sqlctx/sqlctx.duckdb_extension \
uv run test/test_semantic_input_host.py
```

## SQL Syntax Reference

### MODEL

Creates a new semantic model linked to a physical table.

```sql
MODEL (
    name model_name,
    table physical_table_name,
    primary_key pk_column
);
```

The `CREATE MODEL model_name (...)` form is also supported for interactive compatibility.

### METRIC

Defines a metric (aggregation) on the current model.

```sql
-- Sum aggregation
METRIC revenue AS SUM(amount);

-- Count aggregation
METRIC order_count AS COUNT(*);

-- Average aggregation
METRIC avg_value AS AVG(price);

-- With custom SQL expression
METRIC margin AS SUM(price - cost);

-- Native block form
METRIC (name revenue, agg sum, sql amount);
```

The `CREATE METRIC ...` form is also supported.

### DIMENSION

Defines a dimension (grouping attribute) on the current model.

```sql
-- Simple column reference
DIMENSION (name status, type categorical);

-- With SQL expression
DIMENSION (name order_year, type time, sql created_at, granularity day);
```

The `CREATE DIMENSION ...`, `CREATE SEGMENT ...`, `SEMANTIC CREATE ...`, and `SEMANTIC MODEL ...` forms are also supported for compatibility.

### Semantic SELECT

Queries the semantic layer with automatic SQL rewriting.

```sql
-- Query metric with dimension
SELECT model.dimension, model.metric FROM model;

-- Query just a metric (no grouping)
SELECT model.metric FROM model;

-- With filters and ordering
SELECT model.status, model.revenue
FROM model
WHERE model.status = 'completed'
ORDER BY model.revenue DESC;
```

The older `SEMANTIC SELECT ...` form is still supported as a compatibility fallback.

Aliases and quoted identifiers work with automatic routing:

```sql
SELECT o.status, o.revenue FROM orders_model AS o;
SELECT "o"."revenue" FROM "orders_model" AS "o";
```

Routing inspects DuckDB's parsed table and field references. Comments and strings
do not trigger rewriting; CTEs, subqueries and schema-qualified tables such as
`main.orders` are never models.

A model may share its name with a table or view, as in `name: orders` with
`table: orders`. A query keeps its ordinary SQL meaning while every reference
binds to the table, including `SELECT *` and raw columns. It becomes semantic
when it references a field only the model provides, such as a metric or a time
grain like `created_at__month`. Mixing table-only columns and model-only fields
through one name is an error; query the model as `semantic.orders` or the table
as `main.orders`. Once a semantic field is detected, compiler errors are reported
directly, and unknown fields list the closest matches.
Use `SEMANTIC SELECT` to explicitly request semantic compilation. Unsupported
semantic SQL still raises an error; the extension does not implement every DuckDB
query construct in the Rust compiler.

## Transactions and persistence

Definitions and loaded files are stored in the selected DuckDB database as a
versioned snapshot in the `main.__sqlctx_catalog()` macro. This reserved macro
is extension-owned and should not be edited manually. DuckDB provides persistence,
rollback, transaction isolation and conflicting-writer detection for the snapshot.
In-memory databases keep their definitions in memory.

```sql
BEGIN;
CREATE OR REPLACE METRIC orders_model.revenue AS SUM(amount);
ROLLBACK;
```

Parsing, `EXPLAIN`, and `PREPARE` do not apply definitions or load files. Mutations
happen during execution; prepared semantic queries rebind to current definitions.
`MODEL model_name` selects a connection-local active model, and rollback restores
the prior selection. Definitions are shared through the database catalog, while
active model selections are independent between connections.

Existing `.sqlctx.sql` sidecars remain readable when no native snapshot exists.
The first successful definition update stores the imported definitions in DuckDB.
The sidecar is left untouched and no new sidecars are written. After migration,
edit definitions through SQL or load an updated file explicitly. Invalid legacy
files raise errors when the semantic catalog is accessed.

## Native PEG frontend

DuckDB 1.5.6 uses the compatibility frontend. DuckDB 2.0.1
registers a `sqlctx` grammar extension for definitions, discovery, removal,
import/export and explicit semantic queries, including nested `PREPARE` and `EXPLAIN`. Definition properties
still use the shared Rust configuration parser; semantic query compilation still
uses the Rust SQL AST.

Loading sqlctx enables automatic routing, including a planning hook for
hosts with another parser override. To explicitly compose the native
grammar with other installed grammars, include it in `active_grammar_extensions`:

```sql
SET active_grammar_extensions = ['sqlctx'];
```

The native grammar test disables parser overrides and executes custom syntax,
so it verifies native grammar integration independently of the compatibility path.

## Alternative: Definition Files

For larger deployments or version-controlled definitions, load models from YAML or SQL files.

### YAML Configuration

```sql
SELECT * FROM sqlctx_load('
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
');

-- Query works the same way
SELECT orders.revenue, orders.status FROM orders;
```

### Native SQL Definition Files

Native SQL files support the `MODEL (...)`, `DIMENSION (...)`, `METRIC (...)`, and `SEGMENT (...)` block syntax:

```sql
-- orders.sql
MODEL (name orders, table orders, primary_key order_id);

DIMENSION (name status, type categorical);

METRIC (
  name revenue,
  agg sum,
  sql amount
);

SEGMENT (
  name completed,
  sql {model}.status = 'completed'
);
```

They can also use the compact model-block syntax added for native SQL projects:

```sql
-- orders.sql
model orders from orders (
  primary key (order_id)

  status
  date_trunc('day', created_at) as order_date : time grain day

  segment completed as status = 'completed'

  sum(amount) as revenue
  count(*) as order_count
  revenue / order_count as average_order_value
)
```

Both SQL forms can be pasted directly into DuckDB with the extension loaded, or loaded from a file:

```sql
model orders from orders (
  primary key (order_id)
  status
  sum(amount) as revenue
);

SELECT * FROM sqlctx_load_file('/path/to/orders.sql');

SELECT orders.status, orders.revenue
FROM orders
ORDER BY orders.status;
```

### Loading from Files

```sql
-- Load from a single file
SELECT * FROM sqlctx_load_file('/path/to/models.yaml');
SELECT * FROM sqlctx_load_file('/path/to/orders.sql');

-- Load all YAML and SQL files from a directory
SELECT * FROM sqlctx_load_file('/path/to/models/');
```

File imports use DuckDB's filesystem and obey `enable_external_access`,
`allowed_paths`, `allowed_directories`, and `disabled_filesystems`. Directory
imports resolve inheritance across files and apply atomically. SQL-host imports
treat YAML values literally; they do not expand process environment variables.

### Views, tables, and inserts

Prefix the complete statement with `SEMANTIC` to work with either parser frontend:

```sql
SEMANTIC CREATE VIEW revenue_by_status AS
SELECT orders_model.status, orders_model.revenue FROM orders_model;

SEMANTIC CREATE TABLE revenue_snapshot AS
SELECT orders_model.status, orders_model.revenue FROM orders_model;

SEMANTIC INSERT INTO revenue_snapshot
SELECT orders_model.status, orders_model.revenue FROM orders_model;
```

DuckDB retains control of destination columns, `RETURNING`, transactions, and
read-only restrictions. Prepared inserts accept query parameters. `EXPLAIN`
plans the statement without executing it. Views persist the compiled SQL at
creation time, so replacing a metric does not silently change an existing view;
recreate the view to use the new definition.

### YAML Format Reference

```yaml
models:
  - name: orders
    table: orders
    primary_key: order_id

    dimensions:
      - name: status
        type: categorical
      - name: order_date
        type: time
        sql: created_at

    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
      - name: avg_order_value
        agg: avg
        sql: amount

    segments:
      - name: completed
        sql: "{model}.status = 'completed'"

    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
```

### Cube.js Format (also supported)

```yaml
cubes:
  - name: orders
    sql_table: orders

    dimensions:
      - name: status
        sql: "${CUBE}.status"
        type: string

    measures:
      - name: revenue
        sql: "${CUBE}.amount"
        type: sum
```

## Cross-Model Queries

Query metrics from one model grouped by dimensions from another. JOINs are generated automatically based on relationships.

```sql
SELECT * FROM sqlctx_load('
models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
    relationships:
      - name: customers
        type: many_to_one

  - name: customers
    table: customers
    primary_key: id
    dimensions:
      - name: country
        type: categorical
');

-- Query order revenue by customer country (auto-JOIN)
SELECT orders.revenue, customers.country FROM orders;

-- Joins customers through the relationship and groups revenue by country
```

### Relationship Types

| Type | Description | Fan-out Risk |
|------|-------------|--------------|
| `many_to_one` | Many orders belong to one customer | No |
| `one_to_many` | One customer has many orders | Yes |
| `one_to_one` | One-to-one mapping | No |
| `many_to_many` | Many-to-many (requires bridge table) | Yes |

The Rust compiler uses model keys and protected aggregation plans to prevent
supported metrics from being multiplied by relationship joins. Define correct
primary keys and relationship cardinalities; the compiler relies on that metadata.
The extension suite executes cross-model sums and counts over repeated customer
rows, including equal-valued metrics on distinct customers.

## Current support boundaries

- The query frontend handles `SELECT`, `CREATE VIEW`, `CREATE TABLE AS`, and
  `INSERT ... SELECT`, including `PREPARE` and `EXPLAIN` wrappers.
- Semantic definitions support creation, replacement, loading, discovery,
  export/import, active-model selection and dependency-restricted removal.
  Use `CREATE OR REPLACE` to update definitions; there is no `ALTER MODEL` syntax.
- Semantic queries support a subset of DuckDB SQL. Correlated semantic subqueries
  and grouping modifiers such as `ROLLUP` are rejected; explicit `GROUP BY` must
  repeat the selected semantic dimensions.
- Native execution tests cover derived, ratio, cumulative, time-comparison,
  conversion, retention, and cohort metrics. Retention produces a table with
  cohort date, elapsed periods, active users, cohort size, and retention percentage;
  use `select * from sqlctx_query_semantic_input(input_json, query_json)`. A scalar projection such as
  `SEMANTIC SELECT events.retention FROM events` is rejected. These contracts
  do not establish full parity with every Python adapter or API.
- DuckDB 1.5.6 uses the compatibility frontend. Native PEG support targets
  the released DuckDB 2.0.1 host. Rust parses model
  properties and compiles semantic queries.
- The release workflow packages unsigned Linux and macOS builds on AMD64 and ARM64.
  Signed community installation is not yet provided by this repository.

## Utility Functions

`sqlctx_explain(sql)` returns a JSON report from the same Rust compiler and current
catalog snapshot used by `sqlctx_rewrite_sql`:

```sql
select sqlctx_explain('select customers.country, orders.revenue from orders');
```

The report includes compiled SQL, warnings, and ordered compiler observations:
effective grouping expressions, emitted relationship edges and join kinds,
population preservation, detected fanout risk and the guards actually emitted,
independent aggregate populations, and selected rollups where routing is enabled.
`rollup_candidates` lists declarations, not a claim that each candidate is eligible.
The catalog SQL rewrite route currently leaves materialized rollup routing disabled;
its query events expose that setting. Specialized compiler routes may expose fewer
events, as indicated by the report's `coverage` field. An absent guard event is not
a safety guarantee. Explanation compiles without executing the query or checking
physical table availability; ordinary DuckDB `explain` remains the physical plan.

### sqlctx_models()

List all loaded semantic models.

```sql
SELECT * FROM sqlctx_models();
-- orders
-- customers
```

### sqlctx_rewrite_sql(sql)

Manually rewrite a SQL query (useful for debugging).

```sql
SELECT sqlctx_rewrite_sql('SELECT orders.revenue FROM orders');
-- Returns: SELECT SUM(orders.amount) FROM orders
```

## How It Works

1. DuckDB parses queries; the native PEG or compatibility frontend captures definitions without applying them.
2. At bind time, parsed field references are resolved against the caller's transactional catalog snapshot.
3. The Rust compiler rewrites semantic SQL using a private graph. Stateful FFI calls release the registry lock before compilation; native parser workers reuse their large stacks across calls.
4. DuckDB binds and executes generated SQL in the same client context. Definition statements update the catalog during execution.

## Building from Source

```bash
# From this repository checkout
cd sqlctx-duckdb

# Build the compatibility frontend for DuckDB 1.5.6.
make deps DUCKDB_VERSION=v1.5.6

# Build the extension. CMake builds the sibling sqlctx-rs static library automatically.
make

# Run tests
make test

# Use the extension
./build/release/duckdb -unsigned
```

CI builds both released hosts. For 2.0.1, use `make deps DUCKDB_VERSION=v2.0.1`,
`make`, and `SQLCTX_NATIVE_PEG=1 make test`. Use separate build directories/checkouts
when switching DuckDB versions. Each loadable binary targets its host's C++ ABI.

A preview artifact for a published 2.0 alpha runtime must be built from that
runtime's exact commit and carry its version string, because the C++ extension
API only loads in the identical build. For example, for `v2.0.0-alpha41489`:

```bash
make deps DUCKDB_VERSION=v2.0-cyanoptera DUCKDB_NEXT_COMMIT=10de9573794001c649621013bdd93553b54e00c9
DUCKDB_VERSION=v2.0.0-alpha41489 make
SQLCTX_NATIVE_PEG=1 make test
```

Check `pragma version` in the target runtime first: `library_version` is the
version string and `source_id` the commit prefix.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DuckDB Extension (C++)                   │
│  - Native PEG / compatibility parser and AST routing        │
│  - Transactional catalog definitions and legacy migration   │
│  - Table functions (sqlctx_load, sqlctx_models)     │
│  - Scalar function (sqlctx_rewrite_sql)                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ C FFI
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   sqlctx-rs (Rust)                      │
│  - YAML parsing (native + Cube.js formats)                  │
│  - Semantic graph (models, relationships)                   │
│  - SQL generation and query rewriting                       │
└─────────────────────────────────────────────────────────────┘
```

## License

The DuckDB extension template carries the [MIT license](LICENSE). The embedded
`sqlctx-rs` compiler is licensed under [AGPL-3.0-only](../LICENSE), as declared in
its [Cargo manifest](../sqlctx-rs/Cargo.toml). Distributed binaries include that
compiler; the release workflow includes both license texts.
