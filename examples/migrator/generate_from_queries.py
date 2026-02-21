"""Generate a semantic layer from real-world SQL queries.

This example shows how sidemantic's Migrator reverse-engineers a complete
semantic layer from the SQL queries your team already writes. It:

  1. Creates a sample e-commerce database (customers, orders, products)
  2. Prints the 26 representative analyst queries it will analyze
  3. Feeds them through the Migrator to generate model definitions
  4. Prints the generated YAML (dimensions, metrics, relationships)
  5. Registers the models and runs queries through the semantic layer
  6. Prints a coverage summary

Run it:
    uv run python examples/migrator/generate_from_queries.py
"""

import yaml

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.core.migrator import Migrator
from sidemantic.db.duckdb import DuckDBAdapter

# ---------------------------------------------------------------------------
# 1. Sample database
# ---------------------------------------------------------------------------

adapter = DuckDBAdapter(":memory:")
db = adapter.conn

db.execute("""
    CREATE TABLE customers (
        id          INTEGER PRIMARY KEY,
        name        VARCHAR,
        email       VARCHAR,
        region      VARCHAR,
        segment     VARCHAR,
        signup_date DATE
    );

    CREATE TABLE orders (
        id             INTEGER PRIMARY KEY,
        customer_id    INTEGER REFERENCES customers(id),
        order_date     DATE,
        status         VARCHAR,
        payment_method VARCHAR,
        amount         DECIMAL(10,2),
        discount       DECIMAL(10,2)
    );

    CREATE TABLE products (
        id         INTEGER PRIMARY KEY,
        name       VARCHAR,
        category   VARCHAR,
        brand      VARCHAR,
        cost       DECIMAL(10,2),
        price      DECIMAL(10,2)
    );

    CREATE TABLE order_items (
        id         INTEGER PRIMARY KEY,
        order_id   INTEGER REFERENCES orders(id),
        product_id INTEGER REFERENCES products(id),
        quantity   INTEGER,
        unit_price DECIMAL(10,2)
    );
""")

db.execute("""
    INSERT INTO customers VALUES
        (1, 'Alice',   'alice@co.com',   'West',  'Enterprise', '2023-01-15'),
        (2, 'Bob',     'bob@co.com',     'East',  'SMB',        '2023-03-22'),
        (3, 'Charlie', 'charlie@co.com', 'West',  'Enterprise', '2023-06-01'),
        (4, 'Diana',   'diana@co.com',   'North', 'Mid-Market', '2023-09-10'),
        (5, 'Eve',     'eve@co.com',     'East',  'SMB',        '2024-01-05'),
        (6, 'Frank',   'frank@co.com',   'North', 'Enterprise', '2024-02-14');

    INSERT INTO orders VALUES
        (101, 1, '2024-01-15', 'completed', 'credit_card', 1200.00, 0.00),
        (102, 1, '2024-02-20', 'completed', 'credit_card', 850.00,  50.00),
        (103, 2, '2024-01-10', 'completed', 'paypal',      320.00,  0.00),
        (104, 2, '2024-03-05', 'cancelled', 'paypal',      150.00,  0.00),
        (105, 3, '2024-02-28', 'completed', 'wire',        4500.00, 200.00),
        (106, 3, '2024-04-12', 'completed', 'wire',        3200.00, 0.00),
        (107, 4, '2024-03-01', 'pending',   'credit_card', 780.00,  30.00),
        (108, 4, '2024-04-18', 'completed', 'credit_card', 960.00,  0.00),
        (109, 5, '2024-04-02', 'completed', 'paypal',      210.00,  10.00),
        (110, 6, '2024-04-25', 'completed', 'wire',        2800.00, 100.00);

    INSERT INTO products VALUES
        (1, 'Widget Pro',     'Hardware',  'Acme',     45.00,  99.00),
        (2, 'Widget Basic',   'Hardware',  'Acme',     20.00,  49.00),
        (3, 'Cloud Suite',    'Software',  'CloudCo',  0.00,   299.00),
        (4, 'Analytics Plus', 'Software',  'DataInc',  0.00,   199.00),
        (5, 'Cable Kit',      'Accessory', 'Acme',     5.00,   19.00);

    INSERT INTO order_items VALUES
        (1,  101, 1, 10, 99.00),
        (2,  101, 5, 5,  19.00),
        (3,  102, 2, 15, 49.00),
        (4,  102, 5, 10, 19.00),
        (5,  103, 2, 5,  49.00),
        (6,  103, 5, 3,  19.00),
        (7,  105, 3, 10, 299.00),
        (8,  105, 4, 5,  199.00),
        (9,  106, 3, 8,  299.00),
        (10, 106, 1, 5,  99.00),
        (11, 108, 4, 4,  199.00),
        (12, 108, 5, 8,  19.00),
        (13, 109, 2, 3,  49.00),
        (14, 110, 3, 5,  299.00),
        (15, 110, 1, 10, 99.00);
""")

# ---------------------------------------------------------------------------
# 2. Queries that analysts actually write
# ---------------------------------------------------------------------------

QUERIES = [
    # --- Single-table basics ---
    """-- Revenue by status
    SELECT status, SUM(amount) AS revenue, COUNT(*) AS orders
    FROM orders
    GROUP BY status
    ORDER BY revenue DESC""",
    """-- Monthly revenue trend
    SELECT DATE_TRUNC('month', order_date) AS month,
           SUM(amount) AS revenue,
           COUNT(DISTINCT customer_id) AS unique_customers
    FROM orders
    WHERE status = 'completed'
    GROUP BY DATE_TRUNC('month', order_date)
    ORDER BY month""",
    """-- Revenue by payment method
    SELECT payment_method, SUM(amount) AS revenue,
           AVG(amount) AS avg_order, COUNT(*) AS orders
    FROM orders
    GROUP BY payment_method""",
    """-- Customer counts by segment
    SELECT segment, region, COUNT(*) AS customers
    FROM customers
    GROUP BY segment, region
    ORDER BY customers DESC""",
    """-- Product catalog by category
    SELECT category, brand,
           COUNT(*) AS products,
           AVG(price) AS avg_price,
           AVG(price - cost) AS avg_margin
    FROM products
    GROUP BY category, brand""",
    # --- Cross-model JOINs ---
    """-- Revenue by customer segment
    SELECT c.segment, c.region,
           SUM(o.amount) AS revenue,
           COUNT(o.id) AS orders,
           COUNT(DISTINCT o.customer_id) AS buyers
    FROM customers c
    JOIN orders o ON c.id = o.customer_id
    WHERE o.status = 'completed'
    GROUP BY c.segment, c.region
    ORDER BY revenue DESC""",
    """-- Monthly revenue by segment
    SELECT DATE_TRUNC('month', o.order_date) AS month,
           c.segment,
           SUM(o.amount) AS revenue
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    WHERE o.status = 'completed'
    GROUP BY DATE_TRUNC('month', o.order_date), c.segment
    ORDER BY month, c.segment""",
    """-- Top products by revenue
    SELECT p.category, p.name,
           SUM(oi.quantity) AS units_sold,
           SUM(oi.unit_price * oi.quantity) AS item_revenue
    FROM order_items oi
    JOIN products p ON oi.product_id = p.id
    GROUP BY p.category, p.name
    ORDER BY item_revenue DESC
    LIMIT 10""",
    """-- Revenue by product category and customer segment
    SELECT p.category, c.segment,
           SUM(oi.unit_price * oi.quantity) AS item_revenue,
           SUM(oi.quantity) AS units
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.id
    JOIN products p ON oi.product_id = p.id
    JOIN customers c ON o.customer_id = c.id
    WHERE o.status = 'completed'
    GROUP BY p.category, c.segment""",
    # --- Derived metrics ---
    """-- Average order value and discount rate
    SELECT status,
           SUM(amount) / COUNT(*) AS avg_order_value,
           SUM(discount) / SUM(amount) AS discount_rate
    FROM orders
    GROUP BY status""",
    """-- Quarterly revenue with avg order size
    SELECT DATE_TRUNC('quarter', order_date) AS quarter,
           SUM(amount) AS revenue,
           COUNT(*) AS orders,
           SUM(amount) / COUNT(*) AS avg_order_size
    FROM orders
    WHERE status = 'completed'
    GROUP BY DATE_TRUNC('quarter', order_date)
    ORDER BY quarter""",
    # --- Filtered slices ---
    """-- Enterprise revenue breakdown
    SELECT c.region,
           SUM(o.amount) AS revenue,
           COUNT(o.id) AS orders
    FROM customers c
    JOIN orders o ON c.id = o.customer_id
    WHERE c.segment = 'Enterprise' AND o.status = 'completed'
    GROUP BY c.region
    ORDER BY revenue DESC""",
    """-- High-value orders (above $1000)
    SELECT payment_method, COUNT(*) AS orders,
           SUM(amount) AS revenue,
           MAX(amount) AS largest_order
    FROM orders
    WHERE amount > 1000
    GROUP BY payment_method
    HAVING COUNT(*) > 0
    ORDER BY revenue DESC""",
    # --- Discount analysis ---
    """-- Discount analysis by payment method
    SELECT payment_method,
           SUM(discount) AS total_discount,
           AVG(discount) AS avg_discount,
           COUNT(*) AS orders
    FROM orders
    WHERE discount > 0
    GROUP BY payment_method""",
    """-- Order items summary
    SELECT order_id,
           COUNT(*) AS line_items,
           SUM(quantity) AS total_units,
           SUM(quantity * unit_price) AS order_total
    FROM order_items
    GROUP BY order_id
    ORDER BY order_total DESC""",
    # --- Complex filters and expressions ---
    """-- Q1 2024 revenue by region (date range + JOIN + multiple filters)
    SELECT c.region,
           SUM(o.amount) AS revenue,
           COUNT(DISTINCT o.customer_id) AS buyers
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-03-31'
      AND o.status = 'completed'
    GROUP BY c.region
    ORDER BY revenue DESC""",
    """-- Customers with multiple orders (HAVING threshold)
    SELECT c.name, c.segment,
           COUNT(*) AS order_count,
           SUM(o.amount) AS lifetime_value
    FROM customers c
    JOIN orders o ON c.id = o.customer_id
    WHERE o.status = 'completed'
    GROUP BY c.name, c.segment
    HAVING COUNT(*) >= 2
    ORDER BY lifetime_value DESC""",
    """-- Revenue by payment method for credit card or wire (OR filter)
    SELECT payment_method,
           SUM(amount) AS revenue,
           AVG(amount) AS avg_order
    FROM orders
    WHERE payment_method IN ('credit_card', 'wire')
      AND status = 'completed'
    GROUP BY payment_method""",
    """-- Orders with and without discounts (CASE WHEN)
    SELECT CASE WHEN discount > 0 THEN 'discounted' ELSE 'full_price' END AS discount_type,
           COUNT(*) AS orders,
           SUM(amount) AS revenue,
           AVG(discount) AS avg_discount
    FROM orders
    GROUP BY CASE WHEN discount > 0 THEN 'discounted' ELSE 'full_price' END""",
    """-- Product performance with margin (LEFT JOIN covers products with no sales)
    SELECT p.name, p.category,
           COALESCE(SUM(oi.quantity), 0) AS units_sold,
           COALESCE(SUM(oi.quantity * oi.unit_price), 0) AS total_revenue
    FROM products p
    LEFT JOIN order_items oi ON p.id = oi.product_id
    GROUP BY p.name, p.category
    ORDER BY total_revenue DESC""",
    """-- Full pipeline: product revenue by customer segment and quarter
    SELECT DATE_TRUNC('quarter', o.order_date) AS quarter,
           c.segment,
           p.category,
           SUM(oi.quantity * oi.unit_price) AS item_revenue,
           COUNT(DISTINCT o.customer_id) AS buyers
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.id
    JOIN customers c ON o.customer_id = c.id
    JOIN products p ON oi.product_id = p.id
    WHERE o.status = 'completed'
      AND o.order_date >= '2024-01-01'
    GROUP BY DATE_TRUNC('quarter', o.order_date), c.segment, p.category
    ORDER BY quarter, item_revenue DESC""",
    """-- Weekly order volume trend
    SELECT DATE_TRUNC('week', order_date) AS week,
           COUNT(*) AS orders,
           SUM(amount) AS revenue,
           SUM(amount) / COUNT(*) AS avg_order_size
    FROM orders
    WHERE status != 'cancelled'
    GROUP BY DATE_TRUNC('week', order_date)
    ORDER BY week""",
    """-- Top spending customers (subquery pattern: analysts often write this)
    SELECT c.name, c.region, c.segment,
           SUM(o.amount) AS total_spent,
           MIN(o.order_date) AS first_order,
           MAX(o.order_date) AS last_order
    FROM customers c
    JOIN orders o ON c.id = o.customer_id
    WHERE o.status = 'completed'
    GROUP BY c.name, c.region, c.segment
    ORDER BY total_spent DESC
    LIMIT 5""",
    """-- Revenue concentration: how much do top customers drive?
    SELECT c.segment,
           COUNT(DISTINCT c.id) AS customer_count,
           SUM(o.amount) AS segment_revenue,
           MAX(o.amount) AS largest_single_order,
           MIN(o.amount) AS smallest_order
    FROM customers c
    JOIN orders o ON c.id = o.customer_id
    WHERE o.status = 'completed'
    GROUP BY c.segment
    ORDER BY segment_revenue DESC""",
    """-- Brand-level product analysis with inventory metrics
    SELECT brand,
           COUNT(*) AS product_count,
           AVG(price) AS avg_price,
           MIN(price) AS min_price,
           MAX(price) AS max_price,
           AVG(price - cost) AS avg_margin
    FROM products
    GROUP BY brand
    ORDER BY avg_margin DESC""",
    # --- Cross-model derived metric ---
    """-- Revenue per customer by segment (cross-model ratio)
    SELECT c.segment,
           SUM(o.amount) / COUNT(DISTINCT c.id) AS revenue_per_customer
    FROM customers c
    JOIN orders o ON c.id = o.customer_id
    WHERE o.status = 'completed'
    GROUP BY c.segment
    ORDER BY revenue_per_customer DESC""",
]


# ---------------------------------------------------------------------------
# 3. Print the input queries
# ---------------------------------------------------------------------------

print()
print("=" * 72)
print("  INPUT QUERIES")
print("=" * 72)

for i, query in enumerate(QUERIES, 1):
    lines = query.strip().splitlines()
    comment = lines[0].strip() if lines[0].strip().startswith("--") else None
    title = comment.lstrip("- ") if comment else f"Query {i}"
    print(f"\n{'─' * 72}")
    print(f"  [{i}] {title}")
    print(f"{'─' * 72}\n")
    for line in lines:
        if line.strip().startswith("--"):
            continue
        print(f"    {line.strip()}")

# ---------------------------------------------------------------------------
# 4. Run the Migrator
# ---------------------------------------------------------------------------

print()
print("=" * 72)
print("  GENERATING SEMANTIC LAYER FROM QUERY HISTORY")
print("=" * 72)

layer = SemanticLayer(connection=adapter, auto_register=False)
migrator = Migrator(layer, connection=db)

report = migrator.analyze_queries(QUERIES)
models = migrator.generate_models(report)
graph_metrics = migrator.generate_graph_metrics(report, models)

print(f"\n  Analyzed {report.total_queries} queries")
print(f"  Parsed successfully: {report.parseable_queries}")
print(f"  Generated {len(models)} models")
print(f"  Generated {len(graph_metrics)} graph-level metrics\n")


# ---------------------------------------------------------------------------
# 5. Print the generated YAML
# ---------------------------------------------------------------------------

print("=" * 72)
print("  GENERATED MODELS")
print("=" * 72)

for model_name, model_def in sorted(models.items()):
    print(f"\n{'─' * 72}")
    print(f"  {model_name}.yml")
    print(f"{'─' * 72}\n")
    print(yaml.dump({"models": [model_def]}, default_flow_style=False, sort_keys=False))

if graph_metrics:
    print(f"{'─' * 72}")
    print("  graph_metrics.yml  (cross-model)")
    print(f"{'─' * 72}\n")
    print(yaml.dump({"models": [], "metrics": graph_metrics}, default_flow_style=False, sort_keys=False))


# ---------------------------------------------------------------------------
# 6. Register models and run semantic layer queries
# ---------------------------------------------------------------------------

print("=" * 72)
print("  QUERYING THE GENERATED SEMANTIC LAYER")
print("=" * 72)

sl = SemanticLayer(connection=adapter, auto_register=False)

for model_name, model_def in models.items():
    dimensions = []
    for d in model_def.get("dimensions", []):
        dim_kwargs = {"name": d["name"], "sql": d["sql"], "type": d.get("type", "categorical")}
        if d.get("type") == "time":
            dim_kwargs["granularity"] = "day"
        dimensions.append(Dimension(**dim_kwargs))

    metrics = []
    for m in model_def.get("metrics", []):
        if m.get("type") == "derived":
            metrics.append(Metric(name=m["name"], sql=m["sql"], type="derived"))
        else:
            metrics.append(Metric(name=m["name"], agg=m["agg"], sql=m.get("sql", "*")))

    relationships = []
    for r in model_def.get("relationships", []):
        # Skip one_to_many: they lack FK info and the many_to_one on the
        # other model already provides the correct join path.
        if r["type"] == "one_to_many":
            continue
        relationships.append(
            Relationship(
                name=r["name"],
                type=r["type"],
                foreign_key=r.get("foreign_key"),
            )
        )

    sl.add_model(
        Model(
            name=model_def["name"],
            table=model_def["table"],
            dimensions=dimensions,
            metrics=metrics,
            relationships=relationships,
        )
    )

# Register graph-level metrics (cross-model derived metrics)
for gm in graph_metrics:
    sl.add_metric(Metric(name=gm["name"], type=gm["type"], sql=gm["sql"]))


def show_query(title, **kwargs):
    """Compile, print, and execute a semantic layer query."""
    print(f"\n{'─' * 72}")
    print(f"  {title}")
    print(f"{'─' * 72}")

    sql = sl.compile(**kwargs)
    print("\n  Compiled SQL:\n")
    for line in sql.split("\n"):
        print(f"    {line}")

    result = db.execute(sql).fetchall()
    print(f"\n  Results ({len(result)} rows):\n")
    for row in result:
        formatted = [f"{v:.2f}" if isinstance(v, float) else str(v) for v in row]
        print(f"    {', '.join(formatted)}")
    print()


show_query(
    "Revenue and order count by status",
    metrics=["orders.revenue", "orders.orders"],
    dimensions=["orders.status"],
    order_by=["orders.revenue desc"],
)

show_query(
    "Monthly revenue (completed orders only)",
    metrics=["orders.revenue", "orders.buyers"],
    dimensions=["orders.order_date__month"],
    filters=["orders.status = 'completed'"],
    order_by=["orders.order_date__month"],
)

show_query(
    "Average order value by payment method",
    metrics=["orders.revenue", "orders.orders", "orders.avg_order"],
    dimensions=["orders.payment_method"],
    order_by=["orders.revenue desc"],
)

show_query(
    "Product catalog breakdown",
    metrics=["products.products", "products.avg_price", "products.avg_margin"],
    dimensions=["products.category"],
    order_by=["products.avg_margin desc"],
)

# --- More complex semantic layer queries ---

show_query(
    "Cross-model: revenue by customer region (JOIN handled automatically)",
    metrics=["orders.revenue", "orders.buyers"],
    dimensions=["customers.region"],
    filters=["orders.status = 'completed'"],
    order_by=["orders.revenue desc"],
)

show_query(
    "Multiple filters: Q1 wire payments only",
    metrics=["orders.revenue", "orders.orders"],
    dimensions=["orders.payment_method"],
    filters=[
        "orders.status = 'completed'",
        "orders.payment_method = 'wire'",
        "orders.order_date >= '2024-01-01'",
        "orders.order_date < '2024-04-01'",
    ],
)

show_query(
    "Top 3 segments by revenue (with limit)",
    metrics=["orders.revenue", "orders.orders", "orders.buyers"],
    dimensions=["customers.segment"],
    filters=["orders.status = 'completed'"],
    order_by=["orders.revenue desc"],
    limit=3,
)

show_query(
    "Quarterly revenue trend",
    metrics=["orders.revenue", "orders.orders"],
    dimensions=["orders.order_date__quarter"],
    filters=["orders.status = 'completed'"],
    order_by=["orders.order_date__quarter"],
)

# --- Graph-level metric queries ---

if graph_metrics:
    show_query(
        "Graph-level metric: revenue per customer by segment",
        metrics=["revenue_per_customer"],
        dimensions=["customers.segment"],
        filters=["orders.status = 'completed'"],
        order_by=["revenue_per_customer desc"],
    )


# ---------------------------------------------------------------------------
# 7. Coverage summary
# ---------------------------------------------------------------------------

migrator2 = Migrator(sl, connection=db)
report2 = migrator2.analyze_queries(QUERIES)

print("=" * 72)
print("  COVERAGE SUMMARY")
print("=" * 72)
print(f"""
  Queries analyzed:    {report2.total_queries}
  Parseable:           {report2.parseable_queries}
  Fully rewritable:    {report2.rewritable_queries}
  Coverage:            {report2.coverage_percentage:.0f}%
""")

if report2.missing_models:
    print(f"  Missing models:      {', '.join(sorted(report2.missing_models))}")
if report2.missing_dimensions:
    print("  Missing dimensions:")
    for m, dims in sorted(report2.missing_dimensions.items()):
        print(f"    {m}: {', '.join(sorted(dims))}")
if report2.missing_metrics:
    print("  Missing metrics:")
    for m, mets in sorted(report2.missing_metrics.items()):
        for agg, col in sorted(mets):
            print(f"    {m}.{agg}({col})")

print("=" * 72)
print()
