# Migrator Example

This example demonstrates how to migrate from raw SQL queries to a semantic layer.

## Directory Structure

```
migrator/
├── raw_queries/           # Raw SQL queries from your application
│   ├── revenue_by_status.sql
│   ├── customer_demographics.sql
│   ├── product_performance.sql
│   ├── monthly_trends.sql
│   ├── high_value_orders.sql
│   ├── customer_orders.sql
│   ├── inventory_analysis.sql
│   └── cancelled_orders.sql
└── README.md
```

## Usage

### Bootstrap Semantic Layer from Queries

Generate model definitions and rewritten queries from your raw SQL:

```bash
cd examples/migrator

# Generate models and rewritten queries
uv run sidemantic migrator --queries raw_queries/ --generate-models output/
```

This will create:
- `output/models/` - YAML model definitions for each table
- `output/rewritten_queries/` - Python code showing how to query using the semantic layer

### Analyze Coverage

If you already have a semantic layer, analyze which queries can be rewritten:

```bash
# Compare queries against existing semantic layer
uv run sidemantic migrator models/ --queries raw_queries/

# Show detailed analysis for each query
uv run sidemantic migrator models/ --queries raw_queries/ --verbose
```

## What Gets Generated

### Model Definitions

From queries like:
```sql
SELECT status, SUM(total_amount), COUNT(*)
FROM orders
GROUP BY status
```

Generates models like:
```yaml
model:
  name: orders
  table: orders
  description: Auto-generated from query analysis
dimensions:
  - name: status
    sql: status
    type: categorical
metrics:
  - name: count
    agg: count
    sql: '*'
  - name: sum_total_amount
    agg: sum
    sql: total_amount
```

### Rewritten Queries

Generates Python code to replace raw SQL:
```python
# Original query:
# SELECT status, SUM(total_amount), COUNT(*)
# FROM orders
# GROUP BY status

result = layer.query(
    dimensions=['orders.status'],
    metrics=['orders.count', 'orders.sum_total_amount']
)
```

## Use Cases

1. **Migration** - Bootstrap semantic layer from existing SQL queries
2. **Discovery** - Find what metrics/dimensions your team actually uses
3. **Standardization** - Identify inconsistent business logic across queries
4. **Coverage** - Track how much of your SQL can be replaced with semantic layer
