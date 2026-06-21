# Sidemantic DuckDB Extension

A DuckDB extension that adds a SQL-first semantic layer. Define metrics and dimensions once, query them anywhere with automatic SQL rewriting.

## Features

- **Pure SQL Definition**: Define models, metrics, and dimensions using SQL statements
- **Automatic Query Rewriting**: Query qualified model fields directly and get proper aggregations automatically
- **Cross-Model JOINs**: Automatically generates JOINs when querying across related models
- **Fan-out Detection**: Warns when joins may cause metric inflation
- **Definition Files**: Load native YAML, Cube.js YAML, and native SQL definition files

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
make deps DUCKDB_VERSION=v1.5.3
make
make test
./build/release/duckdb -unsigned
```

```sql
LOAD 'build/release/extension/sidemantic/sidemantic.duckdb_extension';
```

For embedded clients, set DuckDB's `allow_unsigned_extensions` database configuration before opening the connection. Community extension installation is planned, but this repository does not yet publish the signed multi-platform artifacts required for `INSTALL sidemantic FROM community`.

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
SELECT orders_model.status, orders_model.revenue FROM orders_model;
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

1. **Parser Override**: DuckDB 1.5+ lets the extension intercept qualified semantic `SELECT` queries before native parsing
2. **Parser Extension Fallback**: The legacy `SEMANTIC` prefix still routes through the parser extension
3. **Query Rewriting**: The Rust-based sidemantic library rewrites `model.metric` references to actual SQL aggregations
4. **Execution**: The rewritten SQL is parsed by DuckDB and executed normally

## Building from Source

```bash
# From this repository checkout
cd sidemantic-duckdb

# Fetch the DuckDB source version used by CI.
# extension-ci-tools is vendored in this directory and pinned to v1.5.3.
make deps DUCKDB_VERSION=v1.5.3

# Build the extension. CMake builds the sibling sidemantic-rs static library automatically.
make

# Run tests
make test

# Use the extension
./build/release/duckdb -unsigned
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DuckDB Extension (C++)                    │
│  - Parser override (intercepts qualified semantic SELECTs)  │
│  - Parser extension (supports legacy SEMANTIC queries)      │
│  - Table functions (sidemantic_load, sidemantic_models)     │
│  - Scalar function (sidemantic_rewrite_sql)                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ C FFI
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   sidemantic-rs (Rust)                       │
│  - YAML parsing (native + Cube.js formats)                  │
│  - Semantic graph (models, relationships)                   │
│  - SQL generation and query rewriting                       │
└─────────────────────────────────────────────────────────────┘
```

## License

MIT
