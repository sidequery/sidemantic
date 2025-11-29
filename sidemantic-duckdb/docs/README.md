# Sidemantic DuckDB Extension Documentation

## Overview

Sidemantic is a DuckDB extension that provides a SQL-first semantic layer. It allows you to define business metrics and dimensions once, then query them with automatic SQL generation.

## Core Concepts

### Models
A model maps to a physical table and defines:
- **Dimensions**: Attributes for grouping and filtering (categorical, time, boolean, numeric)
- **Metrics**: Aggregations like sum, count, avg, min, max
- **Relationships**: Joins to other models
- **Segments**: Reusable named filters

### Query Rewriting
When you write:
```sql
SEMANTIC SELECT orders.revenue, orders.status FROM orders
```

Sidemantic rewrites it to:
```sql
SELECT SUM(orders.amount), orders.status FROM orders GROUP BY 2
```

## API Reference

### Table Functions

#### `sidemantic_load(yaml VARCHAR) -> TABLE(result VARCHAR)`
Loads semantic model definitions from a YAML string.

**Parameters:**
- `yaml`: YAML string containing model definitions

**Returns:** Single row with success message

**Example:**
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
');
```

#### `sidemantic_models() -> TABLE(model_name VARCHAR)`
Lists all currently loaded semantic models.

**Returns:** One row per loaded model

**Example:**
```sql
SELECT * FROM sidemantic_models();
-- ┌────────────┐
-- │ model_name │
-- ├────────────┤
-- │ orders     │
-- │ customers  │
-- └────────────┘
```

### Scalar Functions

#### `sidemantic_rewrite_sql(sql VARCHAR) -> VARCHAR`
Rewrites a SQL query using semantic definitions without executing it.

**Parameters:**
- `sql`: SQL query string to rewrite

**Returns:** Rewritten SQL string

**Example:**
```sql
SELECT sidemantic_rewrite_sql('SELECT orders.revenue FROM orders');
-- Returns: SELECT SUM(orders.amount) FROM orders
```

### Parser Extension

#### `SEMANTIC` Keyword
Prefix any SELECT statement with `SEMANTIC` to trigger automatic query rewriting.

**Example:**
```sql
SEMANTIC SELECT orders.revenue, orders.status FROM orders;
```

## YAML Schema

### Native Format

```yaml
models:
  - name: string           # Required: model name
    table: string          # Physical table name
    sql: string            # Or SQL for derived tables
    primary_key: string    # Required: primary key column
    description: string    # Optional description

    dimensions:
      - name: string       # Required: dimension name
        type: string       # categorical, time, boolean, numeric
        sql: string        # SQL expression (defaults to name)
        granularity: string # For time: hour, day, week, month, quarter, year

    metrics:
      - name: string       # Required: metric name
        agg: string        # sum, count, count_distinct, avg, min, max, median
        sql: string        # SQL expression for aggregation
        type: string       # simple (default), derived, ratio
        numerator: string  # For ratio metrics
        denominator: string
        filters: [string]  # Auto-applied filters

    segments:
      - name: string       # Segment name
        sql: string        # Filter expression with {model} placeholder

    relationships:
      - name: string       # Target model name
        type: string       # many_to_one, one_to_many, one_to_one
        foreign_key: string # FK column (defaults to {name}_id)
        primary_key: string # PK in target (defaults to id)

metrics:  # Graph-level metrics
  - name: string
    type: derived
    sql: string            # Expression referencing model metrics
```

### Cube.js Format

```yaml
cubes:
  - name: string
    sql_table: string

    dimensions:
      - name: string
        sql: string        # Use ${CUBE} for table reference
        type: string       # string, number, time, boolean

    measures:
      - name: string
        sql: string
        type: string       # count, countDistinct, sum, avg, min, max

    segments:
      - name: string
        sql: string        # Use ${CUBE} for table reference
```

## Building

### Prerequisites
- Rust toolchain (for sidemantic-rs)
- CMake 3.5+
- C++11 compiler

### Build Steps

```bash
# 1. Build the Rust library
cd sidemantic-rs
cargo build --release

# 2. Build the DuckDB extension
cd sidemantic-duckdb
git submodule update --init --recursive
make

# 3. Run tests
make test
```

### Output Files
- `build/release/duckdb` - DuckDB shell with extension
- `build/release/unittest` - Test runner

## Limitations

- Parser extension requires `SEMANTIC` keyword prefix (DuckDB's parser handles valid SQL first)
- Single-statement queries only
- State is global (models persist across queries in same session)
