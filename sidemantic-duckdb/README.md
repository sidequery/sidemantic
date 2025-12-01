# Sidemantic DuckDB Extension

A DuckDB extension that adds a SQL-first semantic layer. Define metrics and dimensions once, query them anywhere with automatic SQL rewriting.

## Features

- **Pure SQL Definition**: Define models, metrics, and dimensions using `SEMANTIC CREATE` statements
- **Automatic Query Rewriting**: Use `SEMANTIC SELECT` to automatically rewrite queries with proper aggregations
- **Cross-Model JOINs**: Automatically generates JOINs when querying across related models
- **Fan-out Detection**: Warns when joins may cause metric inflation
- **YAML Support**: Also supports loading definitions from YAML files (native and Cube.js formats)

## Installation

```sql
-- From community extensions (when published)
INSTALL sidemantic FROM community;
LOAD sidemantic;
```

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
SEMANTIC CREATE MODEL orders_model (
    name orders_model,
    table orders,
    primary_key order_id
);

-- 3. Define metrics (aggregations)
SEMANTIC CREATE METRIC revenue AS SUM(amount);
SEMANTIC CREATE METRIC order_count AS COUNT(*);
SEMANTIC CREATE METRIC avg_order_value AS AVG(amount);

-- 4. Define dimensions (grouping attributes)
SEMANTIC CREATE DIMENSION status AS status;

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

## SQL Syntax Reference

### SEMANTIC CREATE MODEL

Creates a new semantic model linked to a physical table.

```sql
SEMANTIC CREATE MODEL model_name (
    name model_name,
    table physical_table_name,
    primary_key pk_column
);
```

### SEMANTIC CREATE METRIC

Defines a metric (aggregation) on the current model.

```sql
-- Sum aggregation
SEMANTIC CREATE METRIC revenue AS SUM(amount);

-- Count aggregation
SEMANTIC CREATE METRIC order_count AS COUNT(*);

-- Average aggregation
SEMANTIC CREATE METRIC avg_value AS AVG(price);

-- With custom SQL expression
SEMANTIC CREATE METRIC margin AS SUM(price - cost);
```

### SEMANTIC CREATE DIMENSION

Defines a dimension (grouping attribute) on the current model.

```sql
-- Simple column reference
SEMANTIC CREATE DIMENSION status AS status;

-- With SQL expression
SEMANTIC CREATE DIMENSION order_year AS YEAR(created_at);
```

### SEMANTIC SELECT

Queries the semantic layer with automatic SQL rewriting.

```sql
-- Query metric with dimension
SEMANTIC SELECT model.dimension, model.metric FROM model;

-- Query just a metric (no grouping)
SEMANTIC SELECT model.metric FROM model;

-- With filters and ordering
SEMANTIC SELECT model.status, model.revenue
FROM model
WHERE model.status = 'completed'
ORDER BY model.revenue DESC;
```

## Alternative: YAML Configuration

For larger deployments or version-controlled definitions, you can also load models from YAML:

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
SEMANTIC SELECT orders.revenue, orders.status FROM orders;
```

### Loading from Files

```sql
-- Load from a single file
SELECT * FROM sidemantic_load_file('/path/to/models.yaml');

-- Load all YAML files from a directory
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
SEMANTIC SELECT orders.revenue, customers.country FROM orders;

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

1. **Parser Extension**: The `SEMANTIC` keyword causes DuckDB's parser to fail, triggering the sidemantic parser extension
2. **Query Rewriting**: The Rust-based sidemantic library rewrites `model.metric` references to actual SQL aggregations
3. **Execution**: The rewritten SQL is parsed by DuckDB and executed normally

## Building from Source

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/your-repo/sidemantic-duckdb.git
cd sidemantic-duckdb

# Build the Rust library first
cd ../sidemantic-rs
cargo build --release
cd ../sidemantic-duckdb

# Build the extension
make

# Run tests
make test

# Use the extension
./build/release/duckdb
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DuckDB Extension (C++)                    │
│  - Parser extension (intercepts SEMANTIC queries)           │
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
