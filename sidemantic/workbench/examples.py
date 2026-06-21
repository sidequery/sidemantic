"""Example semantic SQL queries for the workbench."""

EXAMPLE_QUERIES = {
    "Timeseries": """-- Timeseries revenue by month and region
SELECT
  orders.created_month,
  customers.region,
  orders.total_revenue,
  orders.order_count
FROM orders
ORDER BY created_month DESC, region""",
    "Top Customers": """-- Top customers by revenue
SELECT
  customers.name,
  customers.region,
  orders.total_revenue,
  orders.order_count
FROM orders
ORDER BY orders.total_revenue DESC
LIMIT 10""",
    "Aggregates": """-- Revenue metrics by region
SELECT
  customers.region,
  orders.total_revenue,
  orders.avg_order_value,
  orders.order_count
FROM orders
GROUP BY customers.region
ORDER BY orders.total_revenue DESC""",
    "Custom": """-- Write your custom query here
SELECT

FROM """,
}
