# Sidemantic Examples

This directory contains examples demonstrating the key features of sidemantic.

## Available Examples

### 1. **basic_example.py** - Introduction to Sidemantic
A gentle introduction showing the core concepts:
- Defining models with entities, dimensions, and measures
- Creating metrics
- Querying single models and across joins
- SQL transpilation to different dialects (Snowflake, BigQuery, etc.)

**Run it:**
```bash
uv run python examples/basic_example.py
```

### 2. **parameters_example.py** - Dynamic User Input ✨
Demonstrates how to use parameters for type-safe user input:
- String, number, and date parameter types
- Default values and allowed values (for dropdowns)
- SQL-safe interpolation with `{{ parameter_name }}` syntax
- Building dynamic filters

**Key features:**
- Parameters provide a safe way to inject user input into queries
- Automatic type conversion and SQL escaping
- Support for default values to reduce boilerplate

**Run it:**
```bash
uv run python examples/parameters_example.py
```

**Example output:**
```sql
-- With parameter: {{ order_status }} = 'completed'
WHERE orders_cte.status = 'completed'

-- With parameter: {{ order_status }} = 'pending'
WHERE orders_cte.status = 'pending'
```

### 3. **symmetric_aggregates_example.py** - Preventing Double-Counting ✨
Shows how sidemantic automatically prevents double-counting in fan-out joins:
- What are fan-out joins and why they cause problems
- How symmetric aggregates detect and handle fan-out
- Comparison: naive join vs. symmetric aggregates
- The mathematical formula behind it

**Key features:**
- Automatic detection when multiple one-to-many joins create fan-out
- Uses hash-based formula: `SUM(DISTINCT HASH(pk) * 2^20 + value) - SUM(DISTINCT HASH(pk) * 2^20)`
- Only applies when needed (≥2 one-to-many joins in query)

**Run it:**
```bash
uv run python examples/symmetric_aggregates_example.py
```

**Example:**
```
Order 1 has 2 items × 2 shipments = 4 rows after join

Without symmetric aggregates:
  revenue = 100 × 4 = 400 ❌ (wrong!)

With symmetric aggregates:
  revenue = 100 ✅ (correct!)
```

### 4. **comprehensive_example.py** - Complete Feature Showcase ✨
A comprehensive example demonstrating all advanced features:
- **Parameters** - Dynamic user input
- **Symmetric aggregates** - Fan-out join handling
- **Table calculations** - Post-query calculations (percent of total, running total)
- **Advanced metrics** - MTD/YTD, month-over-month growth
- Working with real DuckDB data

**Run it:**
```bash
uv run python examples/comprehensive_example.py
```

**Features demonstrated:**
1. Parameters with type safety
2. Symmetric aggregates preventing double-counting
3. Grain-to-date metrics (MTD, YTD)
4. Offset ratios (MoM growth)
5. Table calculations (percent of total, running total)

### 5. **export_example.py** - YAML Export
Demonstrates exporting semantic layer definitions to YAML format.

**Run it:**
```bash
uv run python examples/export_example.py
```

## New Features (Recently Added) ✨

### Parameters
User input with `{{ parameter_name }}` syntax for dynamic queries:
- **Types**: string, number, date, unquoted, yesno
- **Features**: Default values, allowed values, type safety
- **Use case**: Building dashboards with user filters

### Symmetric Aggregates
Automatic prevention of double-counting in fan-out joins:
- **Detection**: Identifies when ≥2 one-to-many joins create fan-out
- **Formula**: Hash-based deduplication using `SUM(DISTINCT HASH(...))`
- **Use case**: Querying orders + items + shipments without inflating totals

### Table Calculations
Post-query calculations applied to results:
- **Types**: percent_of_total, running_total, rank, moving_average, etc.
- **Execution**: Applied after SQL execution (like Excel formulas)
- **Use case**: Adding calculated columns without complex SQL

### Advanced Metrics
MetricFlow/LookML-style metric features:
- **Grain-to-date**: MTD, QTD, YTD cumulative metrics
- **Offset ratios**: Month-over-month, year-over-year growth
- **Conversion metrics**: Funnel tracking
- **Fill nulls**: Default values for null results

## Running Examples

All examples can be run using `uv`:

```bash
# Run a specific example
uv run python examples/parameters_example.py

# Run the comprehensive example with all features
uv run python examples/comprehensive_example.py
```

## Output

The examples generate SQL that you can:
1. **Inspect** - See how sidemantic translates semantic queries to SQL
2. **Execute** - Run against your database (examples use DuckDB)
3. **Transpile** - Convert to different SQL dialects

## Learn More

- **Documentation**: See the main README.md
- **Tests**: Check `/tests` directory for more examples
- **Source**: Browse `/sidemantic` for implementation details
