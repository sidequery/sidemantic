# Sidemantic DuckDB Extension

A DuckDB extension that adds a SQL-first semantic layer. Define metrics and dimensions once, query them anywhere with automatic SQL rewriting.

## Features

- **Semantic Models**: Define dimensions (grouping attributes) and metrics (aggregations) on tables
- **Automatic Query Rewriting**: Use `SEMANTIC` keyword to automatically rewrite queries
- **YAML Configuration**: Load model definitions from YAML
- **Multiple Formats**: Supports native sidemantic and Cube.js YAML formats

## Installation

```sql
-- From community extensions (when published)
INSTALL sidemantic FROM community;
LOAD sidemantic;
```

## Quick Start

```sql
-- 1. Load semantic model definitions
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

-- 2. Create test data
CREATE TABLE orders (order_id INT, status VARCHAR, amount DECIMAL(10,2));
INSERT INTO orders VALUES
    (1, 'completed', 100.00),
    (2, 'completed', 150.00),
    (3, 'pending', 75.00);

-- 3. Query using semantic layer (SEMANTIC keyword triggers rewriting)
SEMANTIC SELECT orders.revenue, orders.status FROM orders;
-- Automatically rewrites to:
-- SELECT SUM(orders.amount), orders.status FROM orders GROUP BY 2

-- Result:
-- ┌────────────────────┬───────────┐
-- │ sum(orders.amount) │  status   │
-- ├────────────────────┼───────────┤
-- │              75.00 │ pending   │
-- │             250.00 │ completed │
-- └────────────────────┴───────────┘
```

## Functions

### `sidemantic_load(yaml)`
Load semantic models from a YAML string.

```sql
SELECT * FROM sidemantic_load('models: ...');
```

### `sidemantic_models()`
List all loaded semantic models.

```sql
SELECT * FROM sidemantic_models();
```

### `sidemantic_rewrite_sql(sql)`
Manually rewrite a SQL query (useful for debugging).

```sql
SELECT sidemantic_rewrite_sql('SELECT orders.revenue FROM orders');
-- Returns: SELECT SUM(orders.amount) FROM orders
```

### `SEMANTIC` Keyword
Prefix any SELECT query with `SEMANTIC` to trigger automatic rewriting.

```sql
SEMANTIC SELECT orders.revenue, orders.status FROM orders;
```

## YAML Model Definition

### Native Sidemantic Format

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
