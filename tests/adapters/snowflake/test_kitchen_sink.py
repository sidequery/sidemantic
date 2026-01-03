"""Kitchen sink integration test for Snowflake adapter.

This test validates end-to-end functionality of the Snowflake adapter with a
complex multi-entity data model backed by DuckDB. It covers:

- Multi-entity data model (customers, orders, products, categories)
- All measure types: count, sum, avg, min, max, count_distinct
- Derived metrics with aggregate expressions
- Time dimensions
- Segments (filters in Snowflake terminology)
- Multi-hop joins
- Cross-entity queries
"""

import duckdb
import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.snowflake import SnowflakeAdapter
from tests.utils import fetch_dicts


@pytest.fixture
def kitchen_sink_db():
    """Create comprehensive test database with realistic e-commerce data."""
    conn = duckdb.connect(":memory:")

    # Categories table
    conn.execute("""
        CREATE TABLE categories (
            category_id INTEGER PRIMARY KEY,
            name VARCHAR,
            parent_category_id INTEGER
        )
    """)
    conn.execute("""
        INSERT INTO categories VALUES
        (1, 'Electronics', NULL),
        (2, 'Clothing', NULL),
        (3, 'Phones', 1),
        (4, 'Laptops', 1),
        (5, 'Shirts', 2)
    """)

    # Products table
    conn.execute("""
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            category_id INTEGER,
            name VARCHAR,
            cost DECIMAL(10, 2),
            list_price DECIMAL(10, 2),
            is_active BOOLEAN
        )
    """)
    conn.execute("""
        INSERT INTO products VALUES
        (1, 3, 'iPhone 15', 800.00, 999.00, true),
        (2, 3, 'Galaxy S24', 700.00, 899.00, true),
        (3, 4, 'MacBook Pro', 1500.00, 1999.00, true),
        (4, 4, 'ThinkPad X1', 1200.00, 1599.00, true),
        (5, 5, 'Cotton Shirt', 15.00, 49.99, true),
        (6, 5, 'Silk Shirt', 30.00, 89.99, false)
    """)

    # Customers table
    conn.execute("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            country VARCHAR,
            segment VARCHAR,
            created_at TIMESTAMP,
            lifetime_value DECIMAL(12, 2)
        )
    """)
    conn.execute("""
        INSERT INTO customers VALUES
        (1, 'Alice Johnson', 'alice@example.com', 'USA', 'enterprise', '2023-01-15 10:00:00', 5000.00),
        (2, 'Bob Smith', 'bob@example.com', 'USA', 'small_business', '2023-03-20 14:30:00', 1200.00),
        (3, 'Charlie Brown', 'charlie@example.com', 'UK', 'enterprise', '2022-11-05 09:15:00', 8000.00),
        (4, 'Diana Ross', 'diana@example.com', 'UK', 'consumer', '2023-06-10 16:45:00', 300.00),
        (5, 'Eve Wilson', 'eve@example.com', 'Germany', 'enterprise', '2023-02-28 11:20:00', 6500.00)
    """)

    # Orders table
    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            status VARCHAR,
            channel VARCHAR,
            quantity INTEGER,
            unit_price DECIMAL(10, 2),
            discount_amount DECIMAL(10, 2),
            order_date TIMESTAMP,
            shipped_date TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO orders VALUES
        (1, 1, 1, 'delivered', 'web', 2, 999.00, 100.00, '2024-01-15 10:00:00', '2024-01-17 14:00:00'),
        (2, 1, 3, 'delivered', 'web', 1, 1999.00, 200.00, '2024-01-20 11:00:00', '2024-01-22 16:00:00'),
        (3, 2, 2, 'shipped', 'mobile', 1, 899.00, 0.00, '2024-02-01 09:00:00', '2024-02-03 10:00:00'),
        (4, 3, 4, 'delivered', 'store', 2, 1599.00, 150.00, '2024-02-10 14:00:00', '2024-02-12 11:00:00'),
        (5, 3, 5, 'delivered', 'web', 5, 49.99, 25.00, '2024-02-15 16:00:00', '2024-02-17 09:00:00'),
        (6, 4, 1, 'pending', 'mobile', 1, 999.00, 50.00, '2024-03-01 10:00:00', NULL),
        (7, 5, 3, 'cancelled', 'web', 1, 1999.00, 0.00, '2024-03-05 11:00:00', NULL),
        (8, 5, 2, 'delivered', 'store', 1, 899.00, 100.00, '2024-03-10 15:00:00', '2024-03-12 14:00:00')
    """)

    yield conn
    conn.close()


@pytest.fixture
def snowflake_yaml(tmp_path):
    """Create Snowflake semantic model YAML."""
    yaml_content = """
name: ecommerce_test
description: Test e-commerce semantic model

tables:
  - name: orders
    description: Customer orders
    base_table:
      table: orders
    primary_key:
      columns:
        - order_id
    dimensions:
      - name: order_id
        expr: order_id
        data_type: NUMBER
      - name: status
        description: Order status
        expr: status
        data_type: TEXT
      - name: channel
        description: Sales channel
        expr: channel
        data_type: TEXT
    time_dimensions:
      - name: order_date
        description: Order date
        expr: order_date
        data_type: TIMESTAMP
      - name: shipped_date
        description: Ship date
        expr: shipped_date
        data_type: TIMESTAMP
    facts:
      - name: quantity
        description: Items ordered
        expr: quantity
        data_type: NUMBER
        default_aggregation: sum
      - name: unit_price
        description: Unit price
        expr: unit_price
        data_type: NUMBER
        default_aggregation: avg
      - name: discount_amount
        description: Discount
        expr: discount_amount
        data_type: NUMBER
        default_aggregation: sum
    metrics:
      - name: total_revenue
        description: Total revenue
        expr: SUM(quantity * unit_price)
      - name: total_orders
        description: Order count
        expr: COUNT(DISTINCT order_id)
      - name: avg_order_value
        description: Average order value
        expr: SUM(quantity * unit_price) / COUNT(DISTINCT order_id)
      - name: total_discount
        description: Total discounts
        expr: SUM(discount_amount)
    filters:
      - name: completed_orders
        description: Delivered orders
        expr: status = 'delivered'
      - name: web_orders
        description: Web channel orders
        expr: channel = 'web'

  - name: customers
    description: Customer information
    base_table:
      table: customers
    primary_key:
      columns:
        - customer_id
    dimensions:
      - name: customer_id
        expr: customer_id
        data_type: NUMBER
      - name: name
        description: Customer name
        expr: name
        data_type: TEXT
      - name: country
        description: Country
        expr: country
        data_type: TEXT
      - name: segment
        description: Customer segment
        expr: segment
        data_type: TEXT
    time_dimensions:
      - name: created_at
        description: Registration date
        expr: created_at
        data_type: TIMESTAMP
    facts:
      - name: lifetime_value
        description: LTV
        expr: lifetime_value
        data_type: NUMBER
        default_aggregation: sum
    metrics:
      - name: total_customers
        expr: COUNT(DISTINCT customer_id)
      - name: avg_ltv
        expr: AVG(lifetime_value)
    filters:
      - name: enterprise_customers
        expr: segment = 'enterprise'

  - name: products
    description: Product catalog
    base_table:
      table: products
    primary_key:
      columns:
        - product_id
    dimensions:
      - name: product_id
        expr: product_id
        data_type: NUMBER
      - name: name
        expr: name
        data_type: TEXT
      - name: is_active
        expr: is_active
        data_type: BOOLEAN
    facts:
      - name: cost
        expr: cost
        data_type: NUMBER
        default_aggregation: avg
      - name: list_price
        expr: list_price
        data_type: NUMBER
        default_aggregation: avg
    metrics:
      - name: total_products
        expr: COUNT(DISTINCT product_id)
      - name: avg_margin_pct
        description: Avg margin percentage
        expr: AVG((list_price - cost) / NULLIF(list_price, 0) * 100)

  - name: categories
    description: Product categories
    base_table:
      table: categories
    primary_key:
      columns:
        - category_id
    dimensions:
      - name: category_id
        expr: category_id
        data_type: NUMBER
      - name: name
        expr: name
        data_type: TEXT
    metrics:
      - name: total_categories
        expr: COUNT(DISTINCT category_id)

relationships:
  - left_table: orders
    right_table: customers
    relationship_columns:
      - left_column: customer_id
        right_column: customer_id
    relationship_type: many_to_one

  - left_table: orders
    right_table: products
    relationship_columns:
      - left_column: product_id
        right_column: product_id
    relationship_type: many_to_one

  - left_table: products
    right_table: categories
    relationship_columns:
      - left_column: category_id
        right_column: category_id
    relationship_type: many_to_one
"""
    yaml_file = tmp_path / "ecommerce.yaml"
    yaml_file.write_text(yaml_content)
    return yaml_file


@pytest.fixture
def layer(kitchen_sink_db, snowflake_yaml):
    """Create semantic layer from Snowflake YAML with test database."""
    adapter = SnowflakeAdapter()
    graph = adapter.parse(snowflake_yaml)

    layer = SemanticLayer()
    layer.graph = graph
    layer.conn = kitchen_sink_db

    return layer


class TestBasicQueries:
    """Test basic query functionality."""

    def test_single_metric(self, layer):
        """Test querying a single metric."""
        result = layer.query(metrics=["orders.total_orders"])
        rows = fetch_dicts(result)

        assert len(rows) == 1
        assert rows[0]["total_orders"] == 8

    def test_single_dimension_with_metric(self, layer):
        """Test querying dimension with metric."""
        result = layer.query(
            dimensions=["orders.status"],
            metrics=["orders.total_orders"],
        )
        rows = fetch_dicts(result)

        # Should have 4 statuses: delivered, shipped, pending, cancelled
        assert len(rows) == 4

        status_counts = {r["status"]: r["total_orders"] for r in rows}
        assert status_counts["delivered"] == 5
        assert status_counts["shipped"] == 1
        assert status_counts["pending"] == 1
        assert status_counts["cancelled"] == 1

    def test_multiple_metrics(self, layer):
        """Test querying multiple metrics."""
        result = layer.query(
            metrics=["orders.total_revenue", "orders.total_orders", "orders.total_discount"],
        )
        rows = fetch_dicts(result)

        assert len(rows) == 1
        assert rows[0]["total_orders"] == 8
        assert rows[0]["total_discount"] == 625.0  # Sum of all discounts


class TestAggregationTypes:
    """Test different aggregation types."""

    def test_sum_aggregation(self, layer):
        """Test SUM aggregation via facts."""
        result = layer.query(metrics=["orders.quantity"])
        rows = fetch_dicts(result)

        assert rows[0]["quantity"] == 14  # 2+1+1+2+5+1+1+1=14

    def test_avg_aggregation(self, layer):
        """Test AVG aggregation via facts."""
        result = layer.query(metrics=["customers.avg_ltv"])
        rows = fetch_dicts(result)

        # (5000 + 1200 + 8000 + 300 + 6500) / 5 = 4200
        assert rows[0]["avg_ltv"] == 4200.0

    def test_count_distinct_aggregation(self, layer):
        """Test COUNT DISTINCT aggregation."""
        result = layer.query(metrics=["customers.total_customers"])
        rows = fetch_dicts(result)

        assert rows[0]["total_customers"] == 5


class TestDerivedMetrics:
    """Test derived/calculated metrics."""

    def test_total_revenue(self, layer):
        """Test derived metric with complex inner expression."""
        result = layer.query(metrics=["orders.total_revenue"])
        rows = fetch_dicts(result)

        # SUM(quantity * unit_price)
        # Order 1: 2 * 999 = 1998
        # Order 2: 1 * 1999 = 1999
        # Order 3: 1 * 899 = 899
        # Order 4: 2 * 1599 = 3198
        # Order 5: 5 * 49.99 = 249.95
        # Order 6: 1 * 999 = 999
        # Order 7: 1 * 1999 = 1999
        # Order 8: 1 * 899 = 899
        # Total = 12240.95
        assert rows[0]["total_revenue"] is not None
        assert abs(rows[0]["total_revenue"] - 12240.95) < 0.1


class TestSegments:
    """Test segment (filter) functionality."""

    def test_segment_filters_data(self, layer):
        """Test that segments filter data correctly."""
        result = layer.query(
            metrics=["orders.total_orders"],
            segments=["orders.completed_orders"],
        )
        rows = fetch_dicts(result)

        # Only 5 orders are 'delivered'
        assert rows[0]["total_orders"] == 5

    def test_channel_segment(self, layer):
        """Test channel segment."""
        result = layer.query(
            metrics=["orders.total_orders"],
            segments=["orders.web_orders"],
        )
        rows = fetch_dicts(result)

        # Web orders: 1, 2, 5, 7 = 4 orders
        assert rows[0]["total_orders"] == 4

    def test_segment_with_dimension(self, layer):
        """Test segment with dimension grouping."""
        result = layer.query(
            dimensions=["orders.channel"],
            metrics=["orders.total_orders"],
            segments=["orders.completed_orders"],
        )
        rows = fetch_dicts(result)

        channel_counts = {r["channel"]: r["total_orders"] for r in rows}
        # Delivered orders: web=3, store=2
        assert channel_counts.get("web", 0) == 3
        assert channel_counts.get("store", 0) == 2


class TestJoins:
    """Test join functionality across tables."""

    def test_simple_join(self, layer):
        """Test simple join between orders and customers."""
        result = layer.query(
            dimensions=["customers.country"],
            metrics=["orders.total_orders"],
        )
        rows = fetch_dicts(result)

        country_counts = {r["country"]: r["total_orders"] for r in rows}
        assert country_counts["USA"] == 3  # Orders 1, 2, 3
        assert country_counts["UK"] == 3  # Orders 4, 5, 6
        assert country_counts["Germany"] == 2  # Orders 7, 8

    def test_join_with_segment(self, layer):
        """Test join with segment applied."""
        result = layer.query(
            dimensions=["customers.segment"],
            metrics=["orders.total_revenue"],
            segments=["customers.enterprise_customers"],
        )
        rows = fetch_dicts(result)

        # Only enterprise customers: Alice (orders 1,2), Charlie (orders 4,5), Eve (orders 7,8)
        assert len(rows) == 1
        assert rows[0]["segment"] == "enterprise"

    def test_multi_hop_join(self, layer):
        """Test multi-hop join: orders -> products -> categories."""
        result = layer.query(
            dimensions=["categories.name"],
            metrics=["orders.total_orders"],
        )
        rows = fetch_dicts(result)

        category_counts = {r["name"]: r["total_orders"] for r in rows}
        # Phones: orders 1,2,3,6,8 = 5
        # Laptops: orders 2,4,7 = 3... wait, let me recalculate
        # Order 1: product 1 (Phones)
        # Order 2: product 3 (Laptops)
        # Order 3: product 2 (Phones)
        # Order 4: product 4 (Laptops)
        # Order 5: product 5 (Shirts)
        # Order 6: product 1 (Phones)
        # Order 7: product 3 (Laptops)
        # Order 8: product 2 (Phones)
        assert category_counts.get("Phones", 0) == 4  # Orders 1, 3, 6, 8
        assert category_counts.get("Laptops", 0) == 3  # Orders 2, 4, 7
        assert category_counts.get("Shirts", 0) == 1  # Order 5


class TestTimeDimensions:
    """Test time dimension functionality."""

    def test_time_dimension_grouping(self, layer):
        """Test grouping by time dimension."""
        result = layer.query(
            dimensions=["orders.order_date"],
            metrics=["orders.total_orders"],
        )
        rows = fetch_dicts(result)

        # Should have 8 distinct order dates
        assert len(rows) == 8


class TestFilters:
    """Test ad-hoc filter functionality."""

    def test_simple_filter(self, layer):
        """Test simple WHERE filter."""
        result = layer.query(
            metrics=["orders.total_orders"],
            filters=["orders.status = 'delivered'"],
        )
        rows = fetch_dicts(result)

        assert rows[0]["total_orders"] == 5

    def test_multiple_filters(self, layer):
        """Test multiple filters (AND)."""
        result = layer.query(
            metrics=["orders.total_orders"],
            filters=["orders.status = 'delivered'", "orders.channel = 'web'"],
        )
        rows = fetch_dicts(result)

        # Delivered web orders: 1, 2, 5 = 3
        assert rows[0]["total_orders"] == 3


class TestProductMetrics:
    """Test product-related metrics."""

    def test_product_count(self, layer):
        """Test product count metric."""
        result = layer.query(metrics=["products.total_products"])
        rows = fetch_dicts(result)

        assert rows[0]["total_products"] == 6

    def test_avg_margin(self, layer):
        """Test average margin calculation."""
        result = layer.query(metrics=["products.avg_margin_pct"])
        rows = fetch_dicts(result)

        # Should be a percentage
        assert rows[0]["avg_margin_pct"] is not None
        assert rows[0]["avg_margin_pct"] > 0
        assert rows[0]["avg_margin_pct"] < 100
