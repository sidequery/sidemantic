# E-commerce Semantic Layer Example

A complete example semantic layer for an e-commerce analytics platform, demonstrating both YAML and SQL model definitions.

## Contents

- **models/** - Semantic model definitions
  - `customers.yml` - Customer dimensions and metrics (YAML format)
  - `orders.yml` - Order transactions and metrics (YAML format)
  - `products.sql` - Product catalog (pure SQL format)
  - `order_items.sql` - Order line items (pure SQL format)
  - `metrics.yml` - Cross-model derived metrics

- **data/** - Sample database
  - `create_db.py` - Script to generate sample data
  - `ecommerce.db` - DuckDB database (generated)

## Setup

Generate the sample database:

```bash
uv run examples/ecommerce/data/create_db.py
```

This creates `ecommerce.db` with realistic sample data:
- 200 customers across multiple countries
- 100 products in various categories
- 500 orders with realistic patterns
- Order line items with quantities and discounts

## Usage

### View semantic layer info

```bash
sidemantic info examples/ecommerce/models
```

### Interactive workbench

```bash
sidemantic workbench examples/ecommerce/models --db examples/ecommerce/data/ecommerce.db
```

### Query from command line

Total revenue:
```bash
sidemantic query examples/ecommerce/models \
  --db examples/ecommerce/data/ecommerce.db \
  --sql "SELECT total_revenue FROM orders"
```

Revenue by country:
```bash
sidemantic query examples/ecommerce/models \
  --db examples/ecommerce/data/ecommerce.db \
  --sql "SELECT orders.revenue, customers.country FROM orders ORDER BY orders.revenue DESC"
```

Orders by status:
```bash
sidemantic query examples/ecommerce/models \
  --db examples/ecommerce/data/ecommerce.db \
  --sql "SELECT orders.order_count, orders.revenue, orders.status FROM orders"
```

Customer lifetime value by tier:
```bash
sidemantic query examples/ecommerce/models \
  --db examples/ecommerce/data/ecommerce.db \
  --sql "SELECT customer_lifetime_value, customers.tier FROM customers"
```

Product performance:
```bash
sidemantic query examples/ecommerce/models \
  --db examples/ecommerce/data/ecommerce.db \
  --sql "SELECT order_items.net_revenue, products.category FROM order_items ORDER BY order_items.net_revenue DESC LIMIT 10"
```

### PostgreSQL-compatible server

Start a server that BI tools can connect to:

```bash
sidemantic serve examples/ecommerce/models \
  --db examples/ecommerce/data/ecommerce.db \
  --port 5433
```

Then connect with any PostgreSQL client:
```bash
psql -h localhost -p 5433 -U user
```

## Model Highlights

### Multiple relationship types
- one_to_many: customers → orders
- many_to_one: orders → customers
- many_to_many: orders ↔ products (through order_items)

### Rich metrics
- Simple aggregations: `order_count`, `revenue`
- Filtered metrics: `completed_revenue`, `active_customer_count`
- Ratio metrics: `completion_rate`, `cancellation_rate`
- Derived metrics: `customer_lifetime_value`, `avg_items_per_order`

### Time dimensions
All time dimensions support granularity:
```sql
SELECT revenue, created_at__month FROM orders
SELECT revenue, created_at__year FROM orders
```

### Both YAML and SQL formats
Mix and match based on your preference:
- **YAML**: `customers.yml`, `orders.yml` - declarative, structured
- **SQL**: `products.sql`, `order_items.sql` - pure SQL with MODEL(), DIMENSION(), METRIC() statements

Both formats support:
- Complex SQL expressions
- Filtered metrics with WHERE clauses
- Relationships (many_to_one, one_to_many)
- Segments for reusable filters
- Derived and ratio metrics
