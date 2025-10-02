# Common Gotchas and Solutions

This document covers common pitfalls and their solutions when working with Sidemantic.

## Parameters and Filters

### ❌ DON'T: Add quotes around parameter placeholders

```python
# WRONG - creates double quotes
filters = [f"orders.order_date >= '{{{{ start_date }}}}'"]
# Result: orders.order_date >= ''2024-01-01'' (BREAKS!)

# CORRECT
filters = ["orders.order_date >= {{ start_date }}"]
# Result: orders.order_date >= '2024-01-01' ✓
```

**Why**: Parameters are automatically formatted with quotes based on their type. Adding extra quotes creates `''value''` which SQLGlot can't parse.

**Parameter formatting**:
- `string`: Adds quotes → `'value'`
- `date`: Adds quotes → `'2024-01-01'`
- `number`: No quotes → `100`
- `unquoted`: No quotes → `table_name`

### ❌ DON'T: Reference tables that aren't joined

```python
# WRONG - customers table not in query
sql = generator.generate(
    metrics=["orders.revenue"],  # Only orders
    dimensions=[],               # Only orders
    filters=["customers.region = 'US'"]  # References customers!
)
# Error: Table "customers" not found
```

**Why**: Sidemantic only joins tables when they're needed (referenced in metrics or dimensions). If you filter on a table that's not joined, the query fails.

**Solutions**:

1. **Add a dimension from the filtered table**:
```python
sql = generator.generate(
    metrics=["orders.revenue"],
    dimensions=["customers.region"],  # Forces customers join
    filters=["customers.region = 'US'"]
)
```

2. **Conditionally build filters**:
```python
def build_filters(include_customers=False):
    filters = ["orders.order_date >= '2024-01-01'"]
    if include_customers:
        filters.append("customers.region = 'US'")
    return filters

# Query without customers
sql1 = generator.generate(
    metrics=["orders.revenue"],
    filters=build_filters(include_customers=False)
)

# Query with customers
sql2 = generator.generate(
    metrics=["orders.revenue"],
    dimensions=["customers.region"],
    filters=build_filters(include_customers=True)
)
```

## Filter Parsing

### ❌ DON'T: Expect filters to work with any SQL

Filters are parsed and transformed:

```python
# Input filter
"orders.order_date >= '2024-01-01'"

# After parameter interpolation
"orders.order_date >= '2024-01-01'"

# After field replacement (adds CTE prefix, handles measures)
"orders_cte.order_date >= '2024-01-01'"
```

**Potential issues**:
- Complex SQL expressions might not parse correctly
- Subqueries in filters are not supported
- Regex replacement can match inside strings (we fixed this but it's fragile)

**Workaround for complex filters**:
Use metrics with filters instead:

```python
# Instead of complex filter
filters = ["CASE WHEN orders.status = 'X' THEN ... END"]

# Create a filtered measure
Measure(
    name="filtered_revenue",
    agg="sum",
    expr="amount",
    filters=["status = 'completed'"]
)
```

## Symmetric Aggregates

### ❌ DON'T: Expect symmetric aggregates on every query

```python
# Single one-to-many join - NO symmetric aggregates
sql = generator.generate(
    metrics=["orders.revenue", "order_items.quantity"]
)
# Uses: SUM(orders_cte.revenue_raw) - regular sum

# Multiple one-to-many joins - YES symmetric aggregates
sql = generator.generate(
    metrics=["orders.revenue", "order_items.quantity", "shipments.count"]
)
# Uses: SUM(DISTINCT HASH(...) + revenue) - symmetric aggregates
```

**Why**: Symmetric aggregates only apply when you have ≥2 one-to-many joins creating fan-out. With a single join, regular aggregation is correct and faster.

**To verify**:
```python
sql = generator.generate(...)
print(sql)
# Look for: HASH(primary_key) in the SQL
```

## Join Relationships

### ❌ DON'T: Mix up has_many foreign_key vs belongs_to foreign_key

```python
# WRONG
orders = Model(
    name="orders",
    joins=[
        # has_many: foreign_key is on the OTHER table
        Join(name="order_items", type="has_many", foreign_key="id")  # WRONG!
    ]
)

# CORRECT
orders = Model(
    name="orders",
    joins=[
        # has_many: foreign_key is the column in order_items table
        Join(name="order_items", type="has_many", foreign_key="order_id")
    ]
)

order_items = Model(
    name="order_items",
    joins=[
        # belongs_to: foreign_key is the column in THIS table
        Join(name="orders", type="belongs_to", foreign_key="order_id")
    ]
)
```

**Rule of thumb**:
- `has_many`: foreign_key is in the other (child) table
- `belongs_to`: foreign_key is in this table
- `has_one`: foreign_key is in the other table (like has_many but unique)

## SQL Generation

### ❌ DON'T: Forget to set primary_key

```python
# WRONG - no primary key
orders = Model(
    name="orders",
    # primary_key missing!
)

# Result: Symmetric aggregates won't work
# Result: Defaults to "id" which might not exist
```

**Why**: Symmetric aggregates require primary_key to hash for deduplication.

```python
# CORRECT
orders = Model(
    name="orders",
    primary_key="id",  # or whatever your PK is
)
```

## ORDER BY

### ❌ DON'T: Use full references in order_by

```python
# WRONG
sql = generator.generate(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date"],
    order_by=["orders.order_date"]  # Full reference
)
# Error: Table "orders" not found (it's "orders_cte")
```

```python
# CORRECT
sql = generator.generate(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date"],
    order_by=["order_date"]  # Just column name
)
```

**Why**: ORDER BY uses the column alias from SELECT, not the table reference.

## Common Error Messages

### "Table X not found"
**Cause**: Filtering on a table that's not joined.
**Fix**: Add a dimension from that table, or remove the filter.

### "Column X_raw not found"
**Cause**: Trying to reference a measure directly.
**Fix**: Measures are stored as `{name}_raw` in CTEs. Use measures in metrics, not in custom SQL.

### "Failed to parse ... into Condition"
**Cause**: Parameter produced invalid SQL (usually quoting issue).
**Fix**: Don't add quotes around `{{ param }}` placeholders.

### "Overflow in multiplication"
**Cause**: Symmetric aggregate hash overflow (old bug, should be fixed).
**Fix**: We use HUGEINT now, but if this happens, check DuckDB version.

## Performance

### Slow queries with symmetric aggregates?

**Check**:
```python
# See if you actually need all those joins
sql = generator.generate(
    metrics=["orders.revenue", "items.qty", "shipments.count", "notes.count"]
)
# 3 one-to-many joins = slow!
```

**Consider**:
1. Pre-aggregate in separate queries
2. Use materialized views in your database
3. Denormalize your data model
4. Query subsets separately and combine in application

## Debugging

### See what SQL is generated

```python
sql = generator.generate(...)
print(sql)  # Always check the actual SQL!
```

### Check if symmetric aggregates are used

```python
if "HASH(" in sql:
    print("Using symmetric aggregates")
else:
    print("Using regular aggregation")
```

### Verify joins

```python
if "LEFT JOIN customers_cte" in sql:
    print("Customers table is joined")
```

### Test with simple queries first

```python
# Start simple
sql1 = generator.generate(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date"]
)

# Add complexity gradually
sql2 = generator.generate(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date", "customers.region"]
)

# Add filters last
sql3 = generator.generate(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date", "customers.region"],
    filters=["customers.region = 'US'"]
)
```

## Best Practices

1. **Always read the generated SQL** - Don't trust, verify
2. **Start simple** - Add complexity incrementally
3. **Use parameters correctly** - Don't add quotes around placeholders
4. **Match filters to data** - Only filter on tables that are joined
5. **Set primary keys** - Required for symmetric aggregates
6. **Test with real data** - Edge cases matter
7. **Check performance** - `EXPLAIN ANALYZE` your queries
8. **Use descriptive names** - Future you will thank you

## Getting Help

If you're stuck:

1. Print the generated SQL and look for obvious errors
2. Run the SQL directly in DuckDB/your database
3. Check if tables referenced in filters are actually joined
4. Verify parameter interpolation with simple test cases
5. Check the test suite for similar examples

## Examples That Work

See these working examples:

- `examples/parameters_example.py` - Correct parameter usage
- `examples/symmetric_aggregates_example.py` - Fan-out handling
- `examples/streamlit_dashboard.py` - Complete interactive app
- `tests/test_parameters.py` - Parameter test cases
- `tests/test_symmetric_aggregates.py` - Join test cases
