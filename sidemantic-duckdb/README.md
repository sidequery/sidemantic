# Sidemantic DuckDB Extension

A DuckDB extension that adds a SQL-first semantic layer. Define metrics and dimensions once, query them anywhere with automatic SQL rewriting.

## Features

- **Pure SQL Definition**: Define models, metrics, and dimensions using SQL statements
- **Automatic Query Rewriting**: Query qualified model fields directly and get proper aggregations automatically
- **Cross-Model JOINs**: Automatically generates JOINs when querying across related models
- **Fan-out Detection**: Warns when joins may cause metric inflation
- **Definition Files**: Load native YAML, Cube.js YAML, and native SQL definition files
- **Transactional Definitions**: Commit and roll back model changes with DuckDB transactions
- **Native PEG Grammar**: Composable grammar on the pinned Cyanoptera development build, with a DuckDB 1.5.6 compatibility frontend

## Installation

Current builds are loaded from a local build or GitHub release artifact:

Start DuckDB with unsigned-extension loading enabled because these artifacts are not signed yet:

```bash
duckdb -unsigned
```

```sql
LOAD '/absolute/path/to/sidemantic.duckdb_extension';
```

For local development:

```bash
make deps DUCKDB_VERSION=v1.5.6
make
make test
./build/release/duckdb -unsigned
```

```sql
LOAD 'build/release/extension/sidemantic/sidemantic.duckdb_extension';
```

For embedded clients, set DuckDB's `allow_unsigned_extensions` database configuration before opening the connection. Community extension installation is planned, but this repository does not yet publish the signed multi-platform artifacts required for `INSTALL sidemantic FROM community`.

In DuckDB 1.5.6 shells that preload `autocomplete` (including Homebrew), use
`SEMANTIC SELECT` for semantic queries. That extension's PEG override consumes
ordinary SQL before Sidemantic receives it, and 1.5.6 has no parser-priority setting.
Clients without that competing override support automatic routing. Explicit
`SEMANTIC SELECT` works in both configurations.

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

-- 5. Query using semantic layer
SEMANTIC SELECT orders_model.status, orders_model.revenue FROM orders_model;
-- Automatically rewrites to:
-- SELECT status, SUM(amount) FROM orders GROUP BY 1

-- Result:
-- ┌───────────┬────────────────────┐
-- │  status   │ sum(orders.amount) │
-- ├───────────┼────────────────────┤
-- │ pending   │              75.00 │
-- │ completed │             250.00 │
-- └───────────┴────────────────────┘
```

## Versioned SemanticInput

The stateless SQL functions `sidemantic_compile_semantic_input(input_json, query_json)`
and `sidemantic_rewrite_semantic_input(input_json, sql, context_json)` accept the
same SemanticInput v1 contract as the Python and WASM hosts. They return executable
SQL and do not change models loaded through the session APIs.

```sql
select sidemantic_compile_semantic_input(
    '{"version":1,"models":[{"name":"sales","table":"orders","primary_key":"order_id",
      "metrics":[{"name":"revenue","agg":"sum","sql":"amount"}]}]}',
    '{"metrics":["sales.revenue"]}'
);
```

Pass `user_attributes` and `enforce_visibility` in query JSON for compilation, or
in the third argument for rewriting. Policies and invariant filters in the input
apply when generating SQL; missing policy attributes, visibility denials and
invalid contracts raise SQL errors. SQL NULL arguments return NULL; embedded NUL
bytes are rejected. Each call uses its own caller context.

CI runs `test/test_semantic_input_host.py` against the freshly built DuckDB shell
and loaded extension, executes the returned SQL over real rows, and checks policy
isolation, invalid contracts and argument handling. To run it locally after a build:

```bash
SIDEMANTIC_DUCKDB_BINARY=build/release/duckdb \
SIDEMANTIC_DUCKDB_EXTENSION=build/release/extension/sidemantic/sidemantic.duckdb_extension \
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
do not trigger rewriting; physical tables, CTEs and subqueries can shadow model
names. Once a semantic field is detected, compiler errors are reported directly.
Use `SEMANTIC SELECT` to explicitly request semantic compilation. Unsupported
semantic SQL still raises an error; the extension does not implement every DuckDB
query construct in the Rust compiler.

## Transactions and persistence

Definitions and loaded files are stored in the selected DuckDB database as a
versioned snapshot in the `main.__sidemantic_catalog()` macro. This reserved macro
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

Existing `.sidemantic.sql` sidecars remain readable when no native snapshot exists.
The first successful definition update stores the imported definitions in DuckDB.
The sidecar is left untouched and no new sidecars are written. After migration,
edit definitions through SQL or load an updated file explicitly. Invalid legacy
files raise errors when the semantic catalog is accessed.

## Native PEG frontend

DuckDB 1.5.6 uses the compatibility frontend. The pinned `v2.0-cyanoptera` build
registers a `sidemantic` grammar extension for model/item declarations and
`SEMANTIC SELECT`, including nested `PREPARE` and `EXPLAIN`. Definition properties
still use the shared Rust configuration parser; semantic query compilation still
uses the Rust SQL AST.

Loading Sidemantic enables its parser override for automatic routing, subject to
the 1.5.6 autocomplete limitation above. To explicitly compose the native
grammar with other installed grammars, include it in `active_grammar_extensions`:

```sql
SET active_grammar_extensions = ['sidemantic'];
```

The native grammar test disables parser overrides and executes custom syntax,
so it verifies native grammar integration independently of the compatibility path.

## Alternative: Definition Files

For larger deployments or version-controlled definitions, load models from YAML or SQL files.

### YAML Configuration

```sql
SELECT * FROM sidemantic_load('
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

SELECT * FROM sidemantic_load_file('/path/to/orders.sql');

SELECT orders.status, orders.revenue
FROM orders
ORDER BY orders.status;
```

### Loading from Files

```sql
-- Load from a single file
SELECT * FROM sidemantic_load_file('/path/to/models.yaml');
SELECT * FROM sidemantic_load_file('/path/to/orders.sql');

-- Load all YAML and SQL files from a directory
SELECT * FROM sidemantic_load_file('/path/to/models/');
```

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
SELECT * FROM sidemantic_load('
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

-- Automatically rewrites to:
-- SELECT SUM(orders.amount), c.country
-- FROM orders
-- LEFT JOIN customers AS c ON orders.customers_id = c.id
-- GROUP BY 2
```

### Relationship Types

| Type | Description | Fan-out Risk |
|------|-------------|--------------|
| `many_to_one` | Many orders belong to one customer | No |
| `one_to_many` | One customer has many orders | Yes |
| `one_to_one` | One-to-one mapping | No |
| `many_to_many` | Many-to-many (requires bridge table) | Yes |

**Fan-out Warning**: When joining from "one" to "many" side, metrics from the "one" side may be inflated. The extension adds a SQL comment warning when this is detected.

## Utility Functions

### sidemantic_models()

List all loaded semantic models.

```sql
SELECT * FROM sidemantic_models();
-- orders
-- customers
```

### sidemantic_rewrite_sql(sql)

Manually rewrite a SQL query (useful for debugging).

```sql
SELECT sidemantic_rewrite_sql('SELECT orders.revenue FROM orders');
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
cd sidemantic-duckdb

# Fetch the DuckDB source version used by CI.
# Stable builds and release artifacts target v1.5.6.
make deps DUCKDB_VERSION=v1.5.6

# Build the extension. CMake builds the sibling sidemantic-rs static library automatically.
make

# Run tests
make test

# Use the extension
./build/release/duckdb -unsigned
```

CI also builds `make deps DUCKDB_VERSION=v2.0-cyanoptera`, pinned in the Makefile to
commit `80e17fc252edd6d9e9b090ae00a1100daef4876a`. Run its tests with
`SIDEMANTIC_NATIVE_PEG=1 make test`. Use separate build directories/checkouts when
switching DuckDB versions. CMake probes host APIs rather than assuming C++ ABI
compatibility across releases. Release packaging targets unsigned Linux amd64
builds for 1.5.6; development-version CI does not publish stable artifacts.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DuckDB Extension (C++)                   │
│  - Native PEG / compatibility parser and AST routing        │
│  - Transactional catalog definitions and legacy migration   │
│  - Table functions (sidemantic_load, sidemantic_models)     │
│  - Scalar function (sidemantic_rewrite_sql)                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ C FFI
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   sidemantic-rs (Rust)                      │
│  - YAML parsing (native + Cube.js formats)                  │
│  - Semantic graph (models, relationships)                   │
│  - SQL generation and query rewriting                       │
└─────────────────────────────────────────────────────────────┘
```

## License

MIT
