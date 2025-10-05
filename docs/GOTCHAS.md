# Common Gotchas and Solutions

This guide covers common pitfalls and their solutions when working with Sidemantic.

## Parameters and Filters

### Don't add quotes around parameter placeholders

```python
# Wrong - creates double quotes
filters = [f"orders.order_date >= '{{{{ start_date }}}}'"]
# Result: orders.order_date >= ''2024-01-01'' (invalid SQL)

# Correct
filters = ["orders.order_date >= {{ start_date }}"]
# Result: orders.order_date >= '2024-01-01'
```

**Why**: Parameters are automatically formatted with quotes based on their type. Adding extra quotes creates `''value''` which SQLGlot can't parse.

**Parameter formatting**:
- `string`: Adds quotes → `'value'`
- `date`: Adds quotes → `'2024-01-01'`
- `number`: No quotes → `100`
- `unquoted`: No quotes → `table_name`

### Filters automatically trigger joins

Sidemantic automatically joins tables when you reference them in filters:

```python
# This works - customers table is auto-joined
sql = layer.compile(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date"],
    filters=["customers.region = 'US'"]  # Auto-joins customers!
)
```

**Why**: The SQL rewriter detects that `customers.region` is referenced and automatically adds the customers dimension to trigger the join.

**If you encounter "Table not found" errors**:
1. Check that relationships are properly defined
2. Verify the table/dimension name is correct
3. Ensure there's a join path between the tables

## Filter Parsing

### Complex SQL in filters

Filters support standard SQL comparison operators and simple expressions:

```python
# ✅ These work
filters = [
    "orders.order_date >= '2024-01-01'",
    "orders.amount > 100",
    "orders.status IN ('completed', 'shipped')",
    "customers.region = 'US' AND customers.tier = 'premium'"
]
```

**Not supported**:
- Subqueries in filters
- Window functions in filters
- Complex CASE expressions in filters

**Workaround for complex logic**:
Use metric-level filters or derived metrics:

```python
# Instead of complex filter in query
# Create a filtered metric
Metric(
    name="completed_revenue",
    agg="sum",
    sql="amount",
    filters=["{model}.status = 'completed'"]  # Applied automatically
)

# Then query the filtered metric
layer.compile(metrics=["orders.completed_revenue"])
```

## Symmetric Aggregates

### Understanding fan-out handling

Sidemantic automatically uses symmetric aggregates when needed:

```python
# Single one-to-many relationship - regular aggregation
sql = layer.compile(
    metrics=["orders.revenue", "order_items.quantity"]
)
# Uses: SUM(orders_cte.revenue_raw) - regular sum

# Multiple one-to-many relationships - symmetric aggregates
sql = layer.compile(
    metrics=["orders.revenue", "order_items.quantity", "shipments.count"]
)
# Uses: SUM(DISTINCT HASH(...) + revenue) - symmetric aggregates
```

**Why**: Symmetric aggregates prevent double-counting when you have ≥2 one-to-many joins creating fan-out. With a single join, regular aggregation is correct and faster.

**To verify**:
```python
sql = layer.compile(...)
print(sql)
# Look for: HASH(primary_key) in the SQL
```

## Relationships

### Understanding foreign_key direction

Relationship types determine where the foreign key lives:

```python
# many_to_one: foreign_key is in THIS table
orders = Model(
    name="orders",
    table="orders",
    primary_key="order_id",
    relationships=[
        # foreign_key is in orders table (orders.customer_id)
        Relationship(name="customers", type="many_to_one", foreign_key="customer_id")
    ]
)

# one_to_many: foreign_key is in the OTHER table
customers = Model(
    name="customers",
    table="customers",
    primary_key="customer_id",
    relationships=[
        # foreign_key is in orders table (orders.customer_id)
        Relationship(name="orders", type="one_to_many", foreign_key="customer_id")
    ]
)
```

**Rule of thumb**:
- `many_to_one`: foreign_key is in **this** table (most common for fact tables)
- `one_to_many`: foreign_key is in the **other** table (most common for dimension tables)
- `one_to_one`: foreign_key can be in either table, specify which one

## SQL Generation

### Don't forget to set primary_key

```python
# Wrong - no primary key
orders = Model(
    name="orders",
    # primary_key missing!
)

# Result: Symmetric aggregates won't work
# Result: Defaults to "id" which might not exist
```

**Why**: Symmetric aggregates require primary_key to hash for deduplication.

```python
# Correct
orders = Model(
    name="orders",
    primary_key="id",  # or whatever your PK is
)
```

## ORDER BY

### Use column aliases in order_by

```python
# Wrong
sql = layer.compile(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date"],
    order_by=["orders.order_date"]  # Full reference won't work
)

# Correct
sql = layer.compile(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date"],
    order_by=["order_date"]  # Just column name
)
```

**Why**: ORDER BY uses the column alias from the final SELECT, not the table reference.

**Also works with metrics**:
```python
sql = layer.compile(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date"],
    order_by=["revenue DESC", "order_date"]  # Sort by metric and dimension
)
```

## Common Error Messages

### "Table X not found"
**Cause**: Table isn't joined in the query, or relationship is missing.
**Fix**:
1. Check that relationships are properly defined between models
2. Verify the table/dimension reference is correct
3. Ensure there's a join path between the tables

### "Column X_raw not found"
**Cause**: Trying to reference a metric's raw column directly in custom SQL.
**Fix**: Metrics are stored as `{name}_raw` in CTEs. Reference the metric by name in your query, not the raw column.

### "Failed to parse ... into Condition"
**Cause**: Parameter produced invalid SQL (usually a quoting issue).
**Fix**: Don't add quotes around `{{ param }}` placeholders - they're added automatically based on type.

## Performance

### Slow queries with multiple one-to-many joins?

Multiple one-to-many relationships trigger symmetric aggregates which are slower:

```python
# 3 one-to-many joins = symmetric aggregates
sql = layer.compile(
    metrics=["orders.revenue", "items.qty", "shipments.count", "notes.count"]
)
# This works but may be slow on large datasets
```

**Optimization strategies**:

1. **Query subsets separately**: Break into multiple queries and combine results

2. **Use pre-aggregations**: Enable `use_preaggregations=True` with materialized rollups

3. **Denormalize data**: Consider flattening your schema for frequently-joined tables

4. **Filter early**: Add restrictive filters to reduce row counts before joins

## Debugging

### Inspect generated SQL

Always check the generated SQL to understand what's happening:

```python
sql = layer.compile(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date"],
    filters=["orders.status = 'completed'"]
)
print(sql)  # See the actual SQL that will run
```

### Check for symmetric aggregates

```python
if "HASH(" in sql:
    print("Using symmetric aggregates (multiple one-to-many joins)")
else:
    print("Using regular aggregation")
```

### Verify joins

```python
if "LEFT JOIN customers_cte" in sql:
    print("Customers table is joined")
```

### Build complexity incrementally

```python
# Start simple
sql1 = layer.compile(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date"]
)

# Add joins
sql2 = layer.compile(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date", "customers.region"]
)

# Add filters
sql3 = layer.compile(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date", "customers.region"],
    filters=["customers.region = 'US'"]
)
```

## Best Practices

1. **Always inspect the generated SQL** - Use `print(sql)` to see what's generated
2. **Start simple, add complexity incrementally** - Test each addition
3. **Use parameters correctly** - Don't add quotes around `{{ param }}` placeholders
4. **Define relationships properly** - Ensure join paths exist between models
5. **Set primary keys** - Required for symmetric aggregates and some metric types
6. **Test with real data** - Edge cases reveal issues
7. **Use pre-aggregations for performance** - Materialize frequently-queried rollups
8. **Check query plans** - Use `EXPLAIN ANALYZE` for slow queries

## Getting Help

If you encounter issues:

1. **Inspect the SQL**: `print(layer.compile(...))` to see generated SQL
2. **Test directly**: Run the SQL in DuckDB to isolate Sidemantic vs database issues
3. **Check relationships**: Verify join paths exist between models
4. **Simplify**: Remove complexity until it works, then add back incrementally
5. **Review examples**: Check `examples/` directory for working patterns
6. **Check tests**: The test suite has examples of nearly every feature
