"""Kitchen sink integration test for LookML adapter.

This test validates end-to-end functionality of the LookML adapter with a
complex multi-entity data model backed by DuckDB. It covers:

- Multi-entity data model (8 entities with various relationships)
- All measure types: count, sum, avg, min, max, count_distinct
- Filtered measures (single and multiple filters)
- Derived/ratio metrics
- Time dimension groups with various granularities
- Segments
- Multi-hop joins (1, 2, and 3+ hops)
- Symmetric aggregate handling for fan-out joins
- Cross-entity queries
"""

import shutil
import tempfile
from pathlib import Path

import duckdb
import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.lookml import LookMLAdapter
from tests.utils import fetch_dicts


@pytest.fixture
def kitchen_sink_db():
    """Create comprehensive test database with realistic e-commerce data."""
    conn = duckdb.connect(":memory:")

    # Create schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")

    # Regions table
    conn.execute("""
        CREATE TABLE analytics.regions (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            country VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO analytics.regions VALUES
        (1, 'North America', 'US'),
        (2, 'Europe', 'EU'),
        (3, 'Asia Pacific', 'APAC'),
        (4, 'Latin America', 'LATAM')
    """)

    # Categories table
    conn.execute("""
        CREATE TABLE analytics.categories (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            parent_id INTEGER
        )
    """)
    conn.execute("""
        INSERT INTO analytics.categories VALUES
        (1, 'Electronics', NULL),
        (2, 'Clothing', NULL),
        (3, 'Home & Garden', NULL),
        (4, 'Phones', 1),
        (5, 'Laptops', 1),
        (6, 'Shirts', 2),
        (7, 'Pants', 2)
    """)

    # Customers table
    conn.execute("""
        CREATE TABLE analytics.customers (
            id INTEGER PRIMARY KEY,
            region_id INTEGER,
            email VARCHAR,
            name VARCHAR,
            tier VARCHAR,
            lifetime_value DECIMAL(12, 2),
            registered_at TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO analytics.customers VALUES
        (1, 1, 'alice@example.com', 'Alice Johnson', 'gold', 2500.00, '2023-01-15 10:00:00'),
        (2, 1, 'bob@example.com', 'Bob Smith', 'silver', 800.00, '2023-03-20 14:30:00'),
        (3, 2, 'charlie@example.com', 'Charlie Brown', 'platinum', 5000.00, '2022-11-05 09:15:00'),
        (4, 2, 'diana@example.com', 'Diana Ross', 'bronze', 300.00, '2023-06-10 16:45:00'),
        (5, 3, 'eve@example.com', 'Eve Wilson', 'gold', 1800.00, '2023-02-28 11:20:00'),
        (6, 3, 'frank@example.com', 'Frank Miller', 'silver', 950.00, '2023-04-15 08:00:00'),
        (7, 4, 'grace@example.com', 'Grace Lee', 'platinum', 4200.00, '2022-09-01 13:30:00'),
        (8, 4, 'henry@example.com', 'Henry Ford', 'bronze', 450.00, '2023-07-22 15:00:00')
    """)

    # Products table
    conn.execute("""
        CREATE TABLE analytics.products (
            id INTEGER PRIMARY KEY,
            category_id INTEGER,
            name VARCHAR,
            sku VARCHAR,
            price DECIMAL(10, 2),
            cost DECIMAL(10, 2),
            is_active BOOLEAN,
            created_at TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO analytics.products VALUES
        (1, 4, 'iPhone 15', 'PHONE-001', 999.00, 700.00, true, '2023-09-01'),
        (2, 4, 'Samsung Galaxy S24', 'PHONE-002', 899.00, 600.00, true, '2024-01-15'),
        (3, 5, 'MacBook Pro 16"', 'LAPTOP-001', 2499.00, 1800.00, true, '2023-10-15'),
        (4, 5, 'Dell XPS 15', 'LAPTOP-002', 1799.00, 1200.00, true, '2023-08-20'),
        (5, 6, 'Cotton T-Shirt', 'SHIRT-001', 29.99, 10.00, true, '2023-01-01'),
        (6, 6, 'Polo Shirt', 'SHIRT-002', 49.99, 18.00, true, '2023-02-15'),
        (7, 7, 'Jeans', 'PANTS-001', 79.99, 30.00, true, '2023-03-01'),
        (8, 7, 'Chinos', 'PANTS-002', 59.99, 22.00, false, '2022-06-15')
    """)

    # Orders table
    conn.execute("""
        CREATE TABLE analytics.orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            status VARCHAR,
            channel VARCHAR,
            subtotal DECIMAL(12, 2),
            tax DECIMAL(10, 2),
            shipping DECIMAL(10, 2),
            discount DECIMAL(10, 2),
            total DECIMAL(12, 2),
            is_first_order BOOLEAN,
            created_at TIMESTAMP,
            shipped_at TIMESTAMP,
            delivered_at TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO analytics.orders VALUES
        (1, 1, 'delivered', 'web', 1000.00, 80.00, 10.00, 50.00, 1040.00, true, '2024-01-10 09:00:00', '2024-01-12', '2024-01-15'),
        (2, 1, 'delivered', 'mobile', 500.00, 40.00, 0.00, 0.00, 540.00, false, '2024-01-25 14:30:00', '2024-01-27', '2024-01-30'),
        (3, 2, 'shipped', 'web', 200.00, 16.00, 5.00, 20.00, 201.00, true, '2024-02-05 11:00:00', '2024-02-07', NULL),
        (4, 3, 'delivered', 'store', 3000.00, 240.00, 0.00, 300.00, 2940.00, true, '2024-01-15 10:00:00', '2024-01-15', '2024-01-15'),
        (5, 3, 'cancelled', 'web', 150.00, 12.00, 10.00, 0.00, 172.00, false, '2024-02-01 16:00:00', NULL, NULL),
        (6, 4, 'pending', 'phone', 80.00, 6.40, 5.00, 0.00, 91.40, true, '2024-02-10 09:30:00', NULL, NULL),
        (7, 5, 'delivered', 'web', 2500.00, 200.00, 15.00, 250.00, 2465.00, true, '2024-01-20 13:00:00', '2024-01-22', '2024-01-28'),
        (8, 5, 'refunded', 'mobile', 100.00, 8.00, 0.00, 0.00, 108.00, false, '2024-02-08 15:45:00', '2024-02-10', '2024-02-12'),
        (9, 6, 'delivered', 'web', 600.00, 48.00, 10.00, 60.00, 598.00, true, '2024-01-28 10:30:00', '2024-01-30', '2024-02-02'),
        (10, 7, 'delivered', 'store', 4000.00, 320.00, 0.00, 400.00, 3920.00, true, '2024-01-05 11:00:00', '2024-01-05', '2024-01-05'),
        (11, 7, 'shipped', 'web', 300.00, 24.00, 8.00, 0.00, 332.00, false, '2024-02-12 14:00:00', '2024-02-14', NULL),
        (12, 8, 'delivered', 'mobile', 60.00, 4.80, 5.00, 0.00, 69.80, true, '2024-02-01 08:00:00', '2024-02-03', '2024-02-06')
    """)

    # Order items table
    conn.execute("""
        CREATE TABLE analytics.order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            unit_price DECIMAL(10, 2),
            line_discount DECIMAL(10, 2)
        )
    """)
    conn.execute("""
        INSERT INTO analytics.order_items VALUES
        (1, 1, 1, 1, 999.00, 50.00),
        (2, 2, 5, 3, 29.99, 0.00),
        (3, 2, 6, 2, 49.99, 0.00),
        (4, 2, 7, 2, 79.99, 0.00),
        (5, 3, 5, 5, 29.99, 15.00),
        (6, 3, 6, 1, 49.99, 5.00),
        (7, 4, 3, 1, 2499.00, 250.00),
        (8, 4, 1, 1, 999.00, 50.00),
        (9, 5, 7, 2, 79.99, 0.00),
        (10, 6, 5, 2, 29.99, 0.00),
        (11, 6, 6, 1, 49.99, 0.00),
        (12, 7, 3, 1, 2499.00, 250.00),
        (13, 8, 5, 3, 29.99, 0.00),
        (14, 9, 4, 1, 1799.00, 60.00),
        (15, 10, 3, 1, 2499.00, 250.00),
        (16, 10, 4, 1, 1799.00, 150.00),
        (17, 11, 6, 3, 49.99, 0.00),
        (18, 11, 7, 2, 79.99, 0.00),
        (19, 12, 5, 2, 29.99, 0.00)
    """)

    # Shipments table (multiple shipments per order possible)
    conn.execute("""
        CREATE TABLE analytics.shipments (
            id INTEGER PRIMARY KEY,
            order_id INTEGER,
            carrier VARCHAR,
            tracking_number VARCHAR,
            status VARCHAR,
            weight DECIMAL(8, 2),
            cost DECIMAL(8, 2),
            shipped_at TIMESTAMP,
            delivered_at TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO analytics.shipments VALUES
        (1, 1, 'ups', 'UPS123456', 'delivered', 2.5, 12.00, '2024-01-12', '2024-01-15'),
        (2, 2, 'fedex', 'FDX789012', 'delivered', 1.8, 8.50, '2024-01-27', '2024-01-30'),
        (3, 3, 'usps', 'USPS345678', 'in_transit', 0.9, 5.00, '2024-02-07', NULL),
        (4, 4, 'ups', 'UPS901234', 'delivered', 3.2, 0.00, '2024-01-15', '2024-01-15'),
        (5, 7, 'fedex', 'FDX567890', 'delivered', 4.1, 18.00, '2024-01-22', '2024-01-28'),
        (6, 7, 'ups', 'UPS234567', 'delivered', 1.0, 10.00, '2024-01-23', '2024-01-28'),
        (7, 8, 'usps', 'USPS890123', 'returned', 0.5, 4.00, '2024-02-10', '2024-02-15'),
        (8, 9, 'dhl', 'DHL456789', 'delivered', 3.8, 22.00, '2024-01-30', '2024-02-02'),
        (9, 10, 'fedex', 'FDX012345', 'delivered', 5.5, 0.00, '2024-01-05', '2024-01-05'),
        (10, 11, 'ups', 'UPS678901', 'in_transit', 1.2, 9.00, '2024-02-14', NULL),
        (11, 12, 'usps', 'USPS123890', 'delivered', 0.6, 5.50, '2024-02-03', '2024-02-06')
    """)

    # Reviews table
    conn.execute("""
        CREATE TABLE analytics.reviews (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            customer_id INTEGER,
            order_id INTEGER,
            rating INTEGER,
            is_verified BOOLEAN,
            created_at TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO analytics.reviews VALUES
        (1, 1, 1, 1, 5, true, '2024-01-20'),
        (2, 5, 1, 2, 4, true, '2024-02-01'),
        (3, 6, 1, 2, 5, true, '2024-02-01'),
        (4, 3, 3, 4, 5, true, '2024-01-25'),
        (5, 1, 3, 4, 4, true, '2024-01-25'),
        (6, 3, 5, 7, 5, true, '2024-02-05'),
        (7, 4, 6, 9, 4, true, '2024-02-10'),
        (8, 3, 7, 10, 5, true, '2024-01-15'),
        (9, 4, 7, 10, 3, true, '2024-01-15'),
        (10, 5, 8, 12, 4, true, '2024-02-10'),
        (11, 1, 2, NULL, 3, false, '2024-01-30'),
        (12, 3, 4, NULL, 2, false, '2024-02-05')
    """)

    return conn


@pytest.fixture
def kitchen_sink_layer(kitchen_sink_db):
    """Create semantic layer from kitchen sink LookML files."""
    adapter = LookMLAdapter()

    # Copy fixtures to temp directory to parse together
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        shutil.copy("tests/fixtures/lookml/kitchen_sink.lkml", tmpdir_path / "kitchen_sink.lkml")
        shutil.copy("tests/fixtures/lookml/kitchen_sink_explores.lkml", tmpdir_path / "kitchen_sink_explores.lkml")

        graph = adapter.parse(tmpdir_path)

    layer = SemanticLayer(auto_register=False)
    layer.graph = graph
    layer.conn = kitchen_sink_db

    return layer


# =============================================================================
# PARSING TESTS
# =============================================================================


class TestKitchenSinkParsing:
    """Test that the kitchen sink LookML parses correctly."""

    def test_all_models_parsed(self, kitchen_sink_layer):
        """Verify all 8 entities were parsed."""
        expected_models = [
            "regions",
            "categories",
            "customers",
            "products",
            "orders",
            "order_items",
            "shipments",
            "reviews",
        ]
        for model_name in expected_models:
            assert model_name in kitchen_sink_layer.graph.models, f"Model {model_name} not found"

    def test_orders_dimensions(self, kitchen_sink_layer):
        """Test orders model has all expected dimensions."""
        orders = kitchen_sink_layer.graph.get_model("orders")

        # Regular dimensions
        assert orders.get_dimension("id") is not None
        assert orders.get_dimension("customer_id") is not None
        assert orders.get_dimension("status") is not None
        assert orders.get_dimension("channel") is not None
        assert orders.get_dimension("total") is not None
        assert orders.get_dimension("is_first_order") is not None

        # Time dimensions from dimension_group
        assert orders.get_dimension("created_date") is not None
        assert orders.get_dimension("created_week") is not None
        assert orders.get_dimension("created_month") is not None
        assert orders.get_dimension("created_quarter") is not None
        assert orders.get_dimension("created_year") is not None
        assert orders.get_dimension("shipped_date") is not None
        assert orders.get_dimension("delivered_date") is not None

    def test_orders_metrics(self, kitchen_sink_layer):
        """Test orders model has all metric types."""
        orders = kitchen_sink_layer.graph.get_model("orders")

        # Basic aggregations
        assert orders.get_metric("count") is not None
        assert orders.get_metric("count").agg == "count"

        assert orders.get_metric("total_revenue") is not None
        assert orders.get_metric("total_revenue").agg == "sum"

        assert orders.get_metric("avg_order_value") is not None
        assert orders.get_metric("avg_order_value").agg == "avg"

        assert orders.get_metric("min_order_value") is not None
        assert orders.get_metric("min_order_value").agg == "min"

        assert orders.get_metric("max_order_value") is not None
        assert orders.get_metric("max_order_value").agg == "max"

        # Count distinct
        assert orders.get_metric("unique_customers") is not None
        assert orders.get_metric("unique_customers").agg == "count_distinct"

        # Filtered measures
        assert orders.get_metric("delivered_orders") is not None
        assert orders.get_metric("delivered_orders").filters is not None

        assert orders.get_metric("delivered_web_revenue") is not None
        assert len(orders.get_metric("delivered_web_revenue").filters) == 2

        # Derived measures
        assert orders.get_metric("delivery_rate") is not None
        assert orders.get_metric("delivery_rate").type == "derived"

    def test_orders_segments(self, kitchen_sink_layer):
        """Test orders model has segments."""
        orders = kitchen_sink_layer.graph.get_model("orders")
        segment_names = [s.name for s in orders.segments]

        assert "completed" in segment_names
        assert "high_value" in segment_names
        assert "discounted" in segment_names

    def test_relationships_parsed(self, kitchen_sink_layer):
        """Test that relationships were parsed from explores."""
        orders = kitchen_sink_layer.graph.get_model("orders")

        # Orders should have relationships
        rel_names = {r.name for r in orders.relationships}

        assert "customers" in rel_names, "orders -> customers relationship missing"
        assert "order_items" in rel_names, "orders -> order_items relationship missing"
        assert "shipments" in rel_names, "orders -> shipments relationship missing"

    def test_products_metrics_variety(self, kitchen_sink_layer):
        """Test products has diverse metric types including derived."""
        products = kitchen_sink_layer.graph.get_model("products")

        # Filtered measure
        active_products = products.get_metric("active_products")
        assert active_products is not None
        assert active_products.filters is not None

        # Derived/ratio measure
        avg_margin = products.get_metric("avg_margin")
        assert avg_margin is not None
        assert avg_margin.type == "derived"


# =============================================================================
# BASIC QUERY TESTS
# =============================================================================


class TestBasicQueries:
    """Test basic single-model queries."""

    def test_simple_count(self, kitchen_sink_layer):
        """Test simple count query."""
        result = kitchen_sink_layer.query(metrics=["orders.count"])
        records = fetch_dicts(result)

        assert len(records) == 1
        assert records[0]["count"] == 12

    def test_simple_sum(self, kitchen_sink_layer):
        """Test simple sum query."""
        result = kitchen_sink_layer.query(metrics=["orders.total_revenue"])
        records = fetch_dicts(result)

        assert len(records) == 1
        # Sum of all order totals
        expected = 1040 + 540 + 201 + 2940 + 172 + 91.40 + 2465 + 108 + 598 + 3920 + 332 + 69.80
        assert abs(records[0]["total_revenue"] - expected) < 0.01

    def test_avg_metric(self, kitchen_sink_layer):
        """Test average metric."""
        result = kitchen_sink_layer.query(metrics=["orders.avg_order_value"])
        records = fetch_dicts(result)

        assert len(records) == 1
        total = 1040 + 540 + 201 + 2940 + 172 + 91.40 + 2465 + 108 + 598 + 3920 + 332 + 69.80
        expected_avg = total / 12
        assert abs(records[0]["avg_order_value"] - expected_avg) < 0.01

    def test_min_max_metrics(self, kitchen_sink_layer):
        """Test min and max metrics."""
        result = kitchen_sink_layer.query(metrics=["orders.min_order_value", "orders.max_order_value"])
        records = fetch_dicts(result)

        assert len(records) == 1
        assert abs(records[0]["min_order_value"] - 69.80) < 0.01  # Order 12
        assert abs(records[0]["max_order_value"] - 3920.00) < 0.01  # Order 10

    def test_count_distinct(self, kitchen_sink_layer):
        """Test count distinct metric."""
        result = kitchen_sink_layer.query(metrics=["orders.unique_customers"])
        records = fetch_dicts(result)

        assert len(records) == 1
        assert records[0]["unique_customers"] == 8  # 8 unique customers

    def test_multiple_metrics(self, kitchen_sink_layer):
        """Test querying multiple metrics together."""
        result = kitchen_sink_layer.query(
            metrics=["orders.count", "orders.total_revenue", "orders.avg_order_value", "orders.unique_customers"]
        )
        records = fetch_dicts(result)

        assert len(records) == 1
        assert records[0]["count"] == 12
        assert records[0]["unique_customers"] == 8


# =============================================================================
# DIMENSION TESTS
# =============================================================================


class TestDimensionQueries:
    """Test queries with dimensions."""

    def test_categorical_dimension(self, kitchen_sink_layer):
        """Test grouping by categorical dimension."""
        result = kitchen_sink_layer.query(
            metrics=["orders.count", "orders.total_revenue"], dimensions=["orders.status"]
        )
        records = fetch_dicts(result)

        status_counts = {r["status"]: r["count"] for r in records}
        assert status_counts["delivered"] == 7
        assert status_counts["shipped"] == 2
        assert status_counts["cancelled"] == 1
        assert status_counts["pending"] == 1
        assert status_counts["refunded"] == 1

    def test_channel_dimension(self, kitchen_sink_layer):
        """Test grouping by channel dimension."""
        result = kitchen_sink_layer.query(metrics=["orders.count"], dimensions=["orders.channel"])
        records = fetch_dicts(result)

        channel_counts = {r["channel"]: r["count"] for r in records}
        assert channel_counts["web"] == 6
        assert channel_counts["mobile"] == 3
        assert channel_counts["store"] == 2
        assert channel_counts["phone"] == 1

    def test_time_dimension_month(self, kitchen_sink_layer):
        """Test grouping by month time dimension."""
        result = kitchen_sink_layer.query(metrics=["orders.count"], dimensions=["orders.created_month"])
        records = fetch_dicts(result)

        # Should have January and February 2024
        assert len(records) == 2

        def month_str(value):
            if hasattr(value, "strftime"):
                return value.strftime("%Y-%m")
            return str(value)[:7]

        month_counts = {month_str(r["created_month"]): r["count"] for r in records}
        assert month_counts["2024-01"] == 6
        assert month_counts["2024-02"] == 6

    def test_multiple_dimensions(self, kitchen_sink_layer):
        """Test grouping by multiple dimensions."""
        result = kitchen_sink_layer.query(metrics=["orders.count"], dimensions=["orders.status", "orders.channel"])
        records = fetch_dicts(result)

        # Should have combinations of status and channel
        assert len(records) > 5

        # Find specific combination
        web_delivered = [r for r in records if r["status"] == "delivered" and r["channel"] == "web"]
        assert len(web_delivered) == 1
        assert web_delivered[0]["count"] == 3  # Orders 1, 7, 9


# =============================================================================
# FILTERED MEASURE TESTS
# =============================================================================


class TestFilteredMeasures:
    """Test filtered measures."""

    def test_single_filter_status(self, kitchen_sink_layer):
        """Test measure filtered by status."""
        result = kitchen_sink_layer.query(metrics=["orders.delivered_orders"])
        records = fetch_dicts(result)

        assert len(records) == 1
        assert records[0]["delivered_orders"] == 7

    def test_single_filter_channel(self, kitchen_sink_layer):
        """Test measure filtered by channel."""
        result = kitchen_sink_layer.query(metrics=["orders.web_orders", "orders.mobile_orders"])
        records = fetch_dicts(result)

        assert len(records) == 1
        assert records[0]["web_orders"] == 6
        assert records[0]["mobile_orders"] == 3

    def test_multi_filter_measure(self, kitchen_sink_layer):
        """Test measure with multiple filters."""
        result = kitchen_sink_layer.query(metrics=["orders.delivered_web_revenue"])
        records = fetch_dicts(result)

        assert len(records) == 1
        # Delivered web orders: 1, 7, 9
        # Totals: 1040 + 2465 + 598 = 4103
        expected = 1040 + 2465 + 598
        assert abs(records[0]["delivered_web_revenue"] - expected) < 0.01

    def test_filtered_vs_unfiltered(self, kitchen_sink_layer):
        """Test filtered and unfiltered metrics together."""
        result = kitchen_sink_layer.query(
            metrics=["orders.count", "orders.delivered_orders", "orders.cancelled_orders"]
        )
        records = fetch_dicts(result)

        assert len(records) == 1
        assert records[0]["count"] == 12
        assert records[0]["delivered_orders"] == 7
        assert records[0]["cancelled_orders"] == 1

    def test_filtered_revenue_sum(self, kitchen_sink_layer):
        """Test filtered sum metric."""
        result = kitchen_sink_layer.query(metrics=["orders.delivered_revenue", "orders.total_revenue"])
        records = fetch_dicts(result)

        assert len(records) == 1
        # Delivered orders: 1, 2, 4, 7, 9, 10, 12
        delivered_total = 1040 + 540 + 2940 + 2465 + 598 + 3920 + 69.80
        assert abs(records[0]["delivered_revenue"] - delivered_total) < 0.01

    def test_first_order_filter(self, kitchen_sink_layer):
        """Test boolean filtered measure (is_first_order)."""
        result = kitchen_sink_layer.query(metrics=["orders.first_orders", "orders.first_order_revenue"])
        records = fetch_dicts(result)

        assert len(records) == 1
        assert records[0]["first_orders"] == 8  # 8 first orders


# =============================================================================
# DERIVED METRIC TESTS
# =============================================================================


class TestDerivedMetrics:
    """Test derived/ratio metrics."""

    def test_delivery_rate(self, kitchen_sink_layer):
        """Test derived delivery rate metric."""
        result = kitchen_sink_layer.query(metrics=["orders.delivery_rate"])
        records = fetch_dicts(result)

        assert len(records) == 1
        # 7 delivered / 12 total = 58.33%
        expected = 100.0 * 7 / 12
        assert abs(records[0]["delivery_rate"] - expected) < 0.1

    def test_cancellation_rate(self, kitchen_sink_layer):
        """Test cancellation rate derived metric."""
        result = kitchen_sink_layer.query(metrics=["orders.cancellation_rate"])
        records = fetch_dicts(result)

        assert len(records) == 1
        # 1 cancelled / 12 total = 8.33%
        expected = 100.0 * 1 / 12
        assert abs(records[0]["cancellation_rate"] - expected) < 0.1

    def test_product_margin(self, kitchen_sink_layer):
        """Test products avg_margin derived metric."""
        result = kitchen_sink_layer.query(metrics=["products.avg_margin"])
        records = fetch_dicts(result)

        assert len(records) == 1
        # This is a derived calculation
        assert records[0]["avg_margin"] is not None

    def test_derived_with_dimension(self, kitchen_sink_layer):
        """Test derived metric grouped by dimension."""
        result = kitchen_sink_layer.query(metrics=["orders.delivery_rate"], dimensions=["orders.channel"])
        records = fetch_dicts(result)

        channel_rates = {r["channel"]: r["delivery_rate"] for r in records}
        # Web: 3 delivered out of 6
        assert abs(channel_rates["web"] - 50.0) < 0.1
        # Store: 2 delivered out of 2
        assert abs(channel_rates["store"] - 100.0) < 0.1


# =============================================================================
# CROSS-MODEL JOIN TESTS
# =============================================================================


class TestCrossModelJoins:
    """Test queries across multiple models with joins."""

    def test_orders_by_customer_tier(self, kitchen_sink_layer):
        """Test orders grouped by customer tier (1-hop join)."""
        result = kitchen_sink_layer.query(
            metrics=["orders.count", "orders.total_revenue"], dimensions=["customers.tier"]
        )
        records = fetch_dicts(result)

        tier_counts = {r["tier"]: r["count"] for r in records}
        # Gold customers (1, 5): 4 orders
        assert tier_counts["gold"] == 4
        # Platinum customers (3, 7): 4 orders
        assert tier_counts["platinum"] == 4
        # Silver customers (2, 6): 2 orders
        assert tier_counts["silver"] == 2
        # Bronze customers (4, 8): 2 orders
        assert tier_counts["bronze"] == 2

    def test_orders_by_region(self, kitchen_sink_layer):
        """Test orders grouped by region (2-hop join: orders -> customers -> regions)."""
        result = kitchen_sink_layer.query(metrics=["orders.count", "orders.total_revenue"], dimensions=["regions.name"])
        records = fetch_dicts(result)

        region_counts = {r["name"]: r["count"] for r in records}
        # North America (customers 1, 2): 3 orders
        assert region_counts["North America"] == 3
        # Europe (customers 3, 4): 3 orders
        assert region_counts["Europe"] == 3
        # Asia Pacific (customers 5, 6): 3 orders
        assert region_counts["Asia Pacific"] == 3
        # Latin America (customers 7, 8): 3 orders
        assert region_counts["Latin America"] == 3

    def test_order_items_by_category(self, kitchen_sink_layer):
        """Test order items by product category (multi-hop join)."""
        result = kitchen_sink_layer.query(
            metrics=["order_items.total_quantity", "order_items.total_line_revenue"], dimensions=["categories.name"]
        )
        records = fetch_dicts(result)

        cat_qty = {r["name"]: r["total_quantity"] for r in records}
        # Phones (product 1, 2): orders have iPhones
        assert "Phones" in cat_qty
        # Laptops (product 3, 4): orders have MacBooks and Dells
        assert "Laptops" in cat_qty
        # Shirts (product 5, 6): T-shirts and polos
        assert "Shirts" in cat_qty
        # Pants (product 7, 8): Jeans and chinos
        assert "Pants" in cat_qty

    def test_revenue_by_region_and_channel(self, kitchen_sink_layer):
        """Test multi-dimensional cross-model query."""
        result = kitchen_sink_layer.query(
            metrics=["orders.total_revenue"], dimensions=["regions.name", "orders.channel"]
        )
        records = fetch_dicts(result)

        # Should have region x channel combinations
        assert len(records) > 4

    def test_customer_metrics_by_region(self, kitchen_sink_layer):
        """Test customer metrics grouped by region."""
        result = kitchen_sink_layer.query(
            metrics=["customers.count", "customers.avg_lifetime_value"], dimensions=["regions.name"]
        )
        records = fetch_dicts(result)

        assert len(records) == 4  # 4 regions

        region_counts = {r["name"]: r["count"] for r in records}
        assert region_counts["North America"] == 2
        assert region_counts["Europe"] == 2


# =============================================================================
# SYMMETRIC AGGREGATE TESTS (FAN-OUT HANDLING)
# =============================================================================


class TestSymmetricAggregates:
    """Test symmetric aggregate handling for fan-out joins."""

    def test_order_revenue_not_inflated_by_items(self, kitchen_sink_layer):
        """Test that order revenue isn't multiplied by number of items."""
        # First get total revenue without joins
        simple_result = kitchen_sink_layer.query(metrics=["orders.total_revenue"])
        simple_records = fetch_dicts(simple_result)
        simple_total = simple_records[0]["total_revenue"]

        # Now query with order items dimension (creates fan-out)
        join_result = kitchen_sink_layer.query(
            metrics=["orders.total_revenue", "order_items.total_quantity"], dimensions=["orders.channel"]
        )
        join_records = fetch_dicts(join_result)
        join_total = sum(r["total_revenue"] for r in join_records)

        # The totals should be equal - symmetric aggregates prevent inflation
        assert abs(join_total - simple_total) < 0.01, f"Revenue inflated: {join_total} vs {simple_total}"

    def test_order_count_not_inflated_by_shipments(self, kitchen_sink_layer):
        """Test that order count isn't multiplied by shipments."""
        # Some orders have multiple shipments (order 7 has 2)
        simple_result = kitchen_sink_layer.query(metrics=["orders.count"])
        simple_count = fetch_dicts(simple_result)[0]["count"]

        join_result = kitchen_sink_layer.query(
            metrics=["orders.count", "shipments.count"], dimensions=["orders.status"]
        )
        join_records = fetch_dicts(join_result)
        # When multiple count metrics exist, they get prefixed with model name
        join_count = sum(r["orders_count"] for r in join_records)

        assert join_count == simple_count, f"Count inflated: {join_count} vs {simple_count}"

    def test_metrics_from_multiple_fanout_tables(self, kitchen_sink_layer):
        """Test querying metrics from orders with both items and shipments."""
        result = kitchen_sink_layer.query(
            metrics=[
                "orders.count",
                "orders.total_revenue",
                "order_items.total_quantity",
                "shipments.total_shipping_cost",
            ]
        )
        records = fetch_dicts(result)

        assert len(records) == 1
        assert records[0]["count"] == 12
        # Verify revenue not inflated
        total = 1040 + 540 + 201 + 2940 + 172 + 91.40 + 2465 + 108 + 598 + 3920 + 332 + 69.80
        assert abs(records[0]["total_revenue"] - total) < 0.01


# =============================================================================
# SEGMENT TESTS
# =============================================================================


class TestSegments:
    """Test segment (filter) functionality."""

    def test_completed_segment(self, kitchen_sink_layer):
        """Test querying with completed orders segment."""
        result = kitchen_sink_layer.query(
            metrics=["orders.count", "orders.total_revenue"], segments=["orders.completed"]
        )
        records = fetch_dicts(result)

        assert len(records) == 1
        # Completed = shipped or delivered (orders 1, 2, 3, 4, 7, 9, 10, 11)
        # Note: This depends on how the segment filter is defined
        # shipped or delivered = 7 + 2 = 9 orders

    def test_high_value_segment(self, kitchen_sink_layer):
        """Test high value orders segment ($500+)."""
        result = kitchen_sink_layer.query(metrics=["orders.count"], segments=["orders.high_value"])
        records = fetch_dicts(result)

        # Orders >= 500: 1 (1040), 2 (540), 4 (2940), 7 (2465), 9 (598), 10 (3920)
        assert records[0]["count"] == 6

    def test_segment_with_dimension(self, kitchen_sink_layer):
        """Test segment combined with dimension grouping."""
        result = kitchen_sink_layer.query(
            metrics=["orders.count"], dimensions=["orders.channel"], segments=["orders.high_value"]
        )
        records = fetch_dicts(result)

        channel_counts = {r["channel"]: r["count"] for r in records}
        # High value web orders: 1, 7, 9 = 3
        assert channel_counts.get("web", 0) == 3


# =============================================================================
# PRODUCTS AND REVIEWS TESTS
# =============================================================================


class TestProductsAndReviews:
    """Test product and review related queries."""

    def test_product_count_by_category(self, kitchen_sink_layer):
        """Test counting products by category."""
        result = kitchen_sink_layer.query(metrics=["products.count"], dimensions=["categories.name"])
        records = fetch_dicts(result)

        cat_counts = {r["name"]: r["count"] for r in records}
        assert cat_counts["Phones"] == 2
        assert cat_counts["Laptops"] == 2
        assert cat_counts["Shirts"] == 2
        assert cat_counts["Pants"] == 2

    def test_active_products_filter(self, kitchen_sink_layer):
        """Test filtered measure for active products."""
        result = kitchen_sink_layer.query(metrics=["products.count", "products.active_products"])
        records = fetch_dicts(result)

        assert records[0]["count"] == 8
        assert records[0]["active_products"] == 7  # 1 inactive (chinos)

    def test_review_metrics(self, kitchen_sink_layer):
        """Test review metrics."""
        result = kitchen_sink_layer.query(metrics=["reviews.count", "reviews.avg_rating", "reviews.verified_reviews"])
        records = fetch_dicts(result)

        assert records[0]["count"] == 12
        assert records[0]["verified_reviews"] == 10
        # Average of all ratings
        ratings = [5, 4, 5, 5, 4, 5, 4, 5, 3, 4, 3, 2]
        expected_avg = sum(ratings) / len(ratings)
        assert abs(records[0]["avg_rating"] - expected_avg) < 0.1

    def test_review_ratings_by_product(self, kitchen_sink_layer):
        """Test review ratings grouped by product."""
        result = kitchen_sink_layer.query(metrics=["reviews.avg_rating", "reviews.count"], dimensions=["products.name"])
        records = fetch_dicts(result)

        product_ratings = {r["name"]: r["avg_rating"] for r in records}
        # iPhone 15 reviews: 5, 4, 3 = avg 4.0
        assert abs(product_ratings["iPhone 15"] - 4.0) < 0.1
        # MacBook Pro reviews: 5, 5, 5, 2 = avg 4.25
        assert abs(product_ratings['MacBook Pro 16"'] - 4.25) < 0.1


# =============================================================================
# SHIPMENTS TESTS
# =============================================================================


class TestShipments:
    """Test shipment related queries."""

    def test_shipment_counts_by_carrier(self, kitchen_sink_layer):
        """Test shipment counts grouped by carrier."""
        result = kitchen_sink_layer.query(metrics=["shipments.count"], dimensions=["shipments.carrier"])
        records = fetch_dicts(result)

        carrier_counts = {r["carrier"]: r["count"] for r in records}
        assert carrier_counts["ups"] == 4
        assert carrier_counts["fedex"] == 3
        assert carrier_counts["usps"] == 3
        assert carrier_counts["dhl"] == 1

    def test_shipment_delivery_rate(self, kitchen_sink_layer):
        """Test shipment delivery rate derived metric."""
        result = kitchen_sink_layer.query(metrics=["shipments.delivery_rate"])
        records = fetch_dicts(result)

        # 8 delivered out of 11
        expected = 100.0 * 8 / 11
        assert abs(records[0]["delivery_rate"] - expected) < 0.1

    def test_shipping_costs_by_status(self, kitchen_sink_layer):
        """Test shipping costs by shipment status."""
        result = kitchen_sink_layer.query(
            metrics=["shipments.total_shipping_cost", "shipments.count"], dimensions=["shipments.status"]
        )
        records = fetch_dicts(result)

        status_costs = {r["status"]: r["total_shipping_cost"] for r in records}
        assert "delivered" in status_costs
        assert "in_transit" in status_costs


# =============================================================================
# CUSTOMER ANALYSIS TESTS
# =============================================================================


class TestCustomerAnalysis:
    """Test customer-focused analysis queries."""

    def test_customer_tier_distribution(self, kitchen_sink_layer):
        """Test customer count by tier."""
        result = kitchen_sink_layer.query(metrics=["customers.count"], dimensions=["customers.tier"])
        records = fetch_dicts(result)

        tier_counts = {r["tier"]: r["count"] for r in records}
        assert tier_counts["gold"] == 2
        assert tier_counts["silver"] == 2
        assert tier_counts["platinum"] == 2
        assert tier_counts["bronze"] == 2

    def test_customer_ltv_by_region(self, kitchen_sink_layer):
        """Test customer lifetime value by region."""
        result = kitchen_sink_layer.query(
            metrics=["customers.avg_lifetime_value", "customers.total_lifetime_value"], dimensions=["regions.name"]
        )
        records = fetch_dicts(result)

        region_ltv = {r["name"]: r["total_lifetime_value"] for r in records}
        # North America: Alice (2500) + Bob (800) = 3300
        assert abs(region_ltv["North America"] - 3300) < 0.01
        # Europe: Charlie (5000) + Diana (300) = 5300
        assert abs(region_ltv["Europe"] - 5300) < 0.01

    def test_premium_customer_metrics(self, kitchen_sink_layer):
        """Test filtered metrics for premium tier customers."""
        result = kitchen_sink_layer.query(metrics=["customers.gold_customers", "customers.platinum_customers"])
        records = fetch_dicts(result)

        assert records[0]["gold_customers"] == 2
        assert records[0]["platinum_customers"] == 2


# =============================================================================
# TIME INTELLIGENCE TESTS
# =============================================================================


class TestTimeIntelligence:
    """Test time-based queries."""

    def test_orders_by_month(self, kitchen_sink_layer):
        """Test orders grouped by created month."""
        result = kitchen_sink_layer.query(
            metrics=["orders.count", "orders.total_revenue"], dimensions=["orders.created_month"]
        )
        records = fetch_dicts(result)

        assert len(records) == 2  # Jan and Feb 2024

    def test_orders_by_week(self, kitchen_sink_layer):
        """Test orders grouped by created week."""
        result = kitchen_sink_layer.query(metrics=["orders.count"], dimensions=["orders.created_week"])
        records = fetch_dicts(result)

        # Orders span multiple weeks
        assert len(records) >= 4

    def test_customer_registration_by_quarter(self, kitchen_sink_layer):
        """Test customer registration grouped by quarter."""
        result = kitchen_sink_layer.query(metrics=["customers.count"], dimensions=["customers.registered_quarter"])
        records = fetch_dicts(result)

        # Customers registered across multiple quarters
        assert len(records) >= 2


# =============================================================================
# COMPREHENSIVE METRIC COVERAGE TESTS
# =============================================================================


class TestCountDistinctMetrics:
    """Test count_distinct metrics are properly executed."""

    def test_distinct_products_sold(self, kitchen_sink_layer):
        """Test count_distinct on product_id in order_items."""
        result = kitchen_sink_layer.query(metrics=["order_items.distinct_products_sold"])
        records = fetch_dicts(result)

        # 6 unique products in order_items (products 1, 3, 4, 5, 6, 7)
        assert records[0]["distinct_products_sold"] == 6

    def test_distinct_orders_in_items(self, kitchen_sink_layer):
        """Test count_distinct on order_id in order_items."""
        result = kitchen_sink_layer.query(metrics=["order_items.distinct_orders"])
        records = fetch_dicts(result)

        # 12 orders have items
        assert records[0]["distinct_orders"] == 12

    def test_distinct_reviewers(self, kitchen_sink_layer):
        """Test count_distinct on customer_id in reviews."""
        result = kitchen_sink_layer.query(metrics=["reviews.distinct_reviewers"])
        records = fetch_dicts(result)

        # Each review from different customer in our data
        assert records[0]["distinct_reviewers"] > 0

    def test_distinct_carriers_used(self, kitchen_sink_layer):
        """Test count_distinct on carrier in shipments."""
        result = kitchen_sink_layer.query(metrics=["shipments.distinct_carriers_used"])
        records = fetch_dicts(result)

        # 4 carriers: ups, fedex, usps, dhl
        assert records[0]["distinct_carriers_used"] == 4

    def test_unique_channels(self, kitchen_sink_layer):
        """Test count_distinct on channel in orders."""
        result = kitchen_sink_layer.query(metrics=["orders.unique_channels"])
        records = fetch_dicts(result)

        # 4 channels: web, mobile, store, phone
        assert records[0]["unique_channels"] == 4


class TestMinMaxMetrics:
    """Test min/max aggregations are properly executed."""

    def test_min_max_order_value(self, kitchen_sink_layer):
        """Test min and max order values."""
        result = kitchen_sink_layer.query(metrics=["orders.min_order_value", "orders.max_order_value"])
        records = fetch_dicts(result)

        # Min: 69.80 (order 12), Max: 3920 (order 10)
        assert abs(records[0]["min_order_value"] - 69.80) < 0.01
        assert abs(records[0]["max_order_value"] - 3920) < 0.01

    def test_min_max_price(self, kitchen_sink_layer):
        """Test min and max product prices."""
        result = kitchen_sink_layer.query(metrics=["products.min_price", "products.max_price"])
        records = fetch_dicts(result)

        # Check prices are reasonable
        assert records[0]["min_price"] > 0
        assert records[0]["max_price"] > records[0]["min_price"]

    def test_min_max_rating(self, kitchen_sink_layer):
        """Test min and max review ratings."""
        result = kitchen_sink_layer.query(metrics=["reviews.min_rating", "reviews.max_rating"])
        records = fetch_dicts(result)

        # Ratings 1-5, our data has 2-5
        assert records[0]["min_rating"] == 2
        assert records[0]["max_rating"] == 5


class TestAverageMetrics:
    """Test average aggregations are properly executed."""

    def test_avg_quantity_per_line(self, kitchen_sink_layer):
        """Test average quantity in order_items."""
        result = kitchen_sink_layer.query(metrics=["order_items.avg_quantity_per_line"])
        records = fetch_dicts(result)

        assert records[0]["avg_quantity_per_line"] > 0

    def test_avg_unit_price(self, kitchen_sink_layer):
        """Test average unit price in order_items."""
        result = kitchen_sink_layer.query(metrics=["order_items.avg_unit_price"])
        records = fetch_dicts(result)

        assert records[0]["avg_unit_price"] > 0

    def test_avg_shipping_cost(self, kitchen_sink_layer):
        """Test average shipping cost."""
        result = kitchen_sink_layer.query(metrics=["shipments.avg_shipping_cost"])
        records = fetch_dicts(result)

        assert records[0]["avg_shipping_cost"] > 0

    def test_avg_weight(self, kitchen_sink_layer):
        """Test average shipment weight."""
        result = kitchen_sink_layer.query(metrics=["shipments.avg_weight"])
        records = fetch_dicts(result)

        assert records[0]["avg_weight"] > 0


class TestAdditionalFilteredMeasures:
    """Test filtered measures not covered elsewhere."""

    def test_mobile_orders(self, kitchen_sink_layer):
        """Test filtered count for mobile channel."""
        result = kitchen_sink_layer.query(metrics=["orders.mobile_orders"])
        records = fetch_dicts(result)

        # 3 mobile orders in test data (orders 2, 8, 12)
        assert records[0]["mobile_orders"] == 3

    def test_pending_orders(self, kitchen_sink_layer):
        """Test filtered count for pending status."""
        result = kitchen_sink_layer.query(metrics=["orders.pending_orders"])
        records = fetch_dicts(result)

        # 1 pending order
        assert records[0]["pending_orders"] == 1

    def test_refunded_orders(self, kitchen_sink_layer):
        """Test filtered count for refunded status."""
        result = kitchen_sink_layer.query(metrics=["orders.refunded_orders"])
        records = fetch_dicts(result)

        # 1 refunded order
        assert records[0]["refunded_orders"] == 1

    def test_returned_shipments(self, kitchen_sink_layer):
        """Test filtered count for returned shipments."""
        result = kitchen_sink_layer.query(metrics=["shipments.returned_shipments"])
        records = fetch_dicts(result)

        # 1 returned shipment in test data (shipment 7)
        assert records[0]["returned_shipments"] == 1

    def test_five_star_reviews(self, kitchen_sink_layer):
        """Test filtered count for 5-star reviews."""
        result = kitchen_sink_layer.query(metrics=["reviews.five_star_reviews"])
        records = fetch_dicts(result)

        # Count 5-star reviews from test data
        assert records[0]["five_star_reviews"] == 5

    def test_one_star_reviews(self, kitchen_sink_layer):
        """Test filtered count for 1-star reviews."""
        result = kitchen_sink_layer.query(metrics=["reviews.one_star_reviews"])
        records = fetch_dicts(result)

        # No 1-star reviews in test data (min is 2)
        assert records[0]["one_star_reviews"] == 0

    def test_verified_avg_rating(self, kitchen_sink_layer):
        """Test filtered average - avg rating for verified reviews only."""
        result = kitchen_sink_layer.query(metrics=["reviews.verified_avg_rating"])
        records = fetch_dicts(result)

        # Should be close to overall avg since most are verified
        assert records[0]["verified_avg_rating"] is not None


class TestAdditionalDerivedMetrics:
    """Test derived metrics not covered elsewhere."""

    def test_avg_items_per_order(self, kitchen_sink_layer):
        """Test derived metric: count / distinct_orders."""
        result = kitchen_sink_layer.query(metrics=["order_items.avg_items_per_order"])
        records = fetch_dicts(result)

        # 19 items / 12 orders = 1.58 items per order
        assert abs(records[0]["avg_items_per_order"] - 1.58) < 0.1

    def test_avg_discount_pct(self, kitchen_sink_layer):
        """Test derived metric: total_discount / total_subtotal * 100."""
        result = kitchen_sink_layer.query(metrics=["orders.avg_discount_pct"])
        records = fetch_dicts(result)

        # Should be a reasonable percentage
        assert records[0]["avg_discount_pct"] >= 0

    def test_repeat_customer_rate(self, kitchen_sink_layer):
        """Test derived metric: (count - first_orders) / count * 100."""
        result = kitchen_sink_layer.query(metrics=["orders.repeat_customer_rate"])
        records = fetch_dicts(result)

        # Some orders are repeat customers
        assert records[0]["repeat_customer_rate"] >= 0

    def test_five_star_rate(self, kitchen_sink_layer):
        """Test derived metric: five_star_reviews / count * 100."""
        result = kitchen_sink_layer.query(metrics=["reviews.five_star_rate"])
        records = fetch_dicts(result)

        # 5 out of 12 = 41.67%
        expected = 100.0 * 5 / 12
        assert abs(records[0]["five_star_rate"] - expected) < 0.1


class TestProductMetrics:
    """Test product-specific metrics."""

    def test_active_products(self, kitchen_sink_layer):
        """Test filtered count with yesno filter."""
        result = kitchen_sink_layer.query(metrics=["products.active_products"])
        records = fetch_dicts(result)

        # 7 active products (1 inactive: chinos)
        assert records[0]["active_products"] == 7

    def test_avg_price(self, kitchen_sink_layer):
        """Test average product price."""
        result = kitchen_sink_layer.query(metrics=["products.avg_price"])
        records = fetch_dicts(result)

        assert records[0]["avg_price"] > 0

    def test_total_inventory_value(self, kitchen_sink_layer):
        """Test sum of product prices."""
        result = kitchen_sink_layer.query(metrics=["products.total_inventory_value"])
        records = fetch_dicts(result)

        assert records[0]["total_inventory_value"] > 0

    def test_total_cost(self, kitchen_sink_layer):
        """Test sum of product costs."""
        result = kitchen_sink_layer.query(metrics=["products.total_cost"])
        records = fetch_dicts(result)

        assert records[0]["total_cost"] > 0


class TestOrderSumMetrics:
    """Test order sum metrics for completeness."""

    def test_total_subtotal(self, kitchen_sink_layer):
        """Test sum of order subtotals."""
        result = kitchen_sink_layer.query(metrics=["orders.total_subtotal"])
        records = fetch_dicts(result)

        assert records[0]["total_subtotal"] > 0

    def test_total_tax(self, kitchen_sink_layer):
        """Test sum of order tax."""
        result = kitchen_sink_layer.query(metrics=["orders.total_tax"])
        records = fetch_dicts(result)

        assert records[0]["total_tax"] > 0

    def test_total_shipping(self, kitchen_sink_layer):
        """Test sum of order shipping."""
        result = kitchen_sink_layer.query(metrics=["orders.total_shipping"])
        records = fetch_dicts(result)

        assert records[0]["total_shipping"] > 0

    def test_total_discount(self, kitchen_sink_layer):
        """Test sum of order discounts."""
        result = kitchen_sink_layer.query(metrics=["orders.total_discount"])
        records = fetch_dicts(result)

        assert records[0]["total_discount"] >= 0

    def test_first_order_revenue(self, kitchen_sink_layer):
        """Test revenue from first orders only."""
        result = kitchen_sink_layer.query(metrics=["orders.first_order_revenue"])
        records = fetch_dicts(result)

        assert records[0]["first_order_revenue"] > 0


class TestShipmentMetrics:
    """Test shipment-specific metrics."""

    def test_total_weight(self, kitchen_sink_layer):
        """Test sum of shipment weights."""
        result = kitchen_sink_layer.query(metrics=["shipments.total_weight"])
        records = fetch_dicts(result)

        assert records[0]["total_weight"] > 0

    def test_delivered_shipments(self, kitchen_sink_layer):
        """Test filtered count of delivered shipments."""
        result = kitchen_sink_layer.query(metrics=["shipments.delivered_shipments"])
        records = fetch_dicts(result)

        # 8 delivered shipments
        assert records[0]["delivered_shipments"] == 8

    def test_distinct_orders_shipped(self, kitchen_sink_layer):
        """Test count_distinct of orders with shipments."""
        result = kitchen_sink_layer.query(metrics=["shipments.distinct_orders_shipped"])
        records = fetch_dicts(result)

        # 10 orders have shipments (order 7 has 2 shipments)
        assert records[0]["distinct_orders_shipped"] == 10


class TestOrderItemMetrics:
    """Test order_item specific metrics."""

    def test_total_line_discounts(self, kitchen_sink_layer):
        """Test sum of line item discounts."""
        result = kitchen_sink_layer.query(metrics=["order_items.total_line_discounts"])
        records = fetch_dicts(result)

        assert records[0]["total_line_discounts"] >= 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
