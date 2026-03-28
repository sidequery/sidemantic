"""Kitchen sink integration test for Malloy adapter.

Validates end-to-end functionality with a multi-entity data model backed by
DuckDB. Covers:

- Multi-entity data model (regions, customers, products, orders, order_items)
- All measure types: count, sum, avg, min, max, count_distinct
- Filtered measures
- Derived measures
- Pick/when/else -> CASE transforms
- Apply-pick with partial comparisons
- Null coalescing (?? -> COALESCE)
- Date literals (@YYYY-MM-DD)
- Time dimensions with granularity (.month, .year, DATE_TRUNC)
- Boolean dimensions from comparisons
- Rename statements
- Segments (source-level where)
- SQL-based sources
- Source inheritance (extends)
- Join types (join_one with, join_many on)
- Join direction modifiers (LEFT)
- Dot-method aggregation (field.sum())
- Backtick-quoted identifiers
- Annotations (## and # desc:)
"""

from pathlib import Path

import duckdb
import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.malloy import MalloyAdapter
from tests.utils import fetch_dicts


@pytest.fixture
def kitchen_sink_db():
    """Create comprehensive test database with realistic e-commerce data."""
    conn = duckdb.connect(":memory:")

    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")

    # Regions
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
        (3, 'Asia Pacific', 'APAC')
    """)

    # Customers
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
        (1, 1, 'alice@example.com', 'Alice', 'platinum', 5000.00, '2023-01-15 10:00:00'),
        (2, 1, 'bob@example.com', 'Bob', 'gold', 2500.00, '2023-03-20 14:30:00'),
        (3, 2, 'charlie@example.com', 'Charlie', 'silver', 800.00, '2023-06-10 09:15:00'),
        (4, 2, 'diana@example.com', NULL, 'bronze', 300.00, '2023-09-01 16:45:00'),
        (5, 3, 'eve@example.com', 'Eve', 'gold', 3000.00, '2023-02-28 11:20:00'),
        (6, 3, 'frank@example.com', 'Frank', 'silver', 950.00, '2023-04-15 08:00:00')
    """)

    # Products
    conn.execute("""
        CREATE TABLE analytics.products (
            id INTEGER PRIMARY KEY,
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
        (1, 'Widget A', 'WA-001', 100.00, 60.00, true, '2023-01-01'),
        (2, 'Widget B', 'WB-002', 200.00, 120.00, true, '2023-02-15'),
        (3, 'Gadget X', 'GX-003', 500.00, 300.00, true, '2023-03-01'),
        (4, 'Gadget Y', 'GY-004', 50.00, 30.00, false, '2022-06-15')
    """)

    # Orders
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
        (1, 1, 'delivered', 'web', 500.00, 40.00, 10.00, 50.00, 500.00, true, '2024-01-10 09:00:00', '2024-01-12', '2024-01-15'),
        (2, 1, 'delivered', 'mobile', 200.00, 16.00, 0.00, 0.00, 216.00, false, '2024-01-25 14:30:00', '2024-01-27', '2024-01-30'),
        (3, 2, 'shipped', 'web', 100.00, 8.00, 5.00, 10.00, 103.00, true, '2024-02-05 11:00:00', '2024-02-07', NULL),
        (4, 3, 'delivered', 'web', 1000.00, 80.00, 0.00, 100.00, 980.00, true, '2024-01-15 10:00:00', '2024-01-15', '2024-01-15'),
        (5, 3, 'cancelled', 'web', 50.00, 4.00, 5.00, 0.00, 59.00, false, '2024-02-01 16:00:00', NULL, NULL),
        (6, 4, 'pending', 'phone', 80.00, 6.40, 5.00, 0.00, 91.40, true, '2024-02-10 09:30:00', NULL, NULL),
        (7, 5, 'delivered', 'web', 600.00, 48.00, 15.00, 60.00, 603.00, true, '2024-01-20 13:00:00', '2024-01-22', '2024-01-28'),
        (8, 5, 'refunded', 'mobile', 100.00, 8.00, 0.00, 0.00, 108.00, false, '2024-02-08 15:45:00', '2024-02-10', '2024-02-12'),
        (9, 6, 'delivered', 'web', 300.00, 24.00, 10.00, 30.00, 304.00, true, '2024-01-28 10:30:00', '2024-01-30', '2024-02-02'),
        (10, 6, 'shipped', 'mobile', 150.00, 12.00, 8.00, 0.00, 170.00, false, '2024-02-12 14:00:00', '2024-02-14', NULL)
    """)

    # Order items
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
        (1, 1, 1, 3, 100.00, 20.00),
        (2, 1, 2, 1, 200.00, 30.00),
        (3, 2, 1, 2, 100.00, 0.00),
        (4, 3, 1, 1, 100.00, 10.00),
        (5, 4, 3, 2, 500.00, 100.00),
        (6, 5, 4, 1, 50.00, 0.00),
        (7, 7, 3, 1, 500.00, 50.00),
        (8, 7, 1, 1, 100.00, 10.00),
        (9, 8, 4, 2, 50.00, 0.00),
        (10, 9, 2, 1, 200.00, 20.00),
        (11, 9, 1, 1, 100.00, 10.00),
        (12, 10, 2, 1, 200.00, 0.00)
    """)

    return conn


@pytest.fixture
def kitchen_sink_layer(kitchen_sink_db):
    """Create semantic layer from kitchen sink Malloy file."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/kitchen_sink.malloy"))

    layer = SemanticLayer(auto_register=False)
    layer.graph = graph
    layer.conn = kitchen_sink_db

    return layer


@pytest.fixture
def kitchen_sink_graph():
    """Parse kitchen sink for structure-only tests (no DB needed)."""
    adapter = MalloyAdapter()
    return adapter.parse(Path("tests/fixtures/malloy/kitchen_sink.malloy"))


# =============================================================================
# PARSING / STRUCTURE TESTS
# =============================================================================


class TestModelStructure:
    """Test that all models and their properties parsed correctly."""

    def test_all_models_parsed(self, kitchen_sink_graph):
        expected = [
            "regions",
            "customers",
            "products",
            "orders",
            "order_items",
            "order_summary",
            "vip_customers",
            "directed_orders",
        ]
        for name in expected:
            assert name in kitchen_sink_graph.models, f"Model {name} not found"

    def test_customers_description(self, kitchen_sink_graph):
        customers = kitchen_sink_graph.get_model("customers")
        assert customers.description is not None
        assert "Customer" in customers.description

    def test_customers_primary_key(self, kitchen_sink_graph):
        customers = kitchen_sink_graph.get_model("customers")
        assert customers.primary_key == "id"

    def test_customers_dimension_types(self, kitchen_sink_graph):
        customers = kitchen_sink_graph.get_model("customers")

        # pick/when -> CASE dimension
        tier_label = customers.get_dimension("tier_label")
        assert tier_label is not None
        assert "CASE" in tier_label.sql.upper()

        # Boolean from comparison
        is_hv = customers.get_dimension("is_high_value")
        assert is_hv is not None
        assert is_hv.type == "boolean"

        # Null coalescing -> COALESCE
        display = customers.get_dimension("display_name")
        assert display is not None
        assert "COALESCE" in display.sql.upper()

        # Time with Malloy dot syntax
        reg_month = customers.get_dimension("registered_month")
        assert reg_month is not None
        assert "month" in reg_month.sql

        reg_year = customers.get_dimension("registered_year")
        assert reg_year is not None
        assert "year" in reg_year.sql

        # ::date cast
        reg_date = customers.get_dimension("registered_date")
        assert reg_date is not None
        assert reg_date.type == "time"

    def test_customers_measures(self, kitchen_sink_graph):
        customers = kitchen_sink_graph.get_model("customers")

        assert customers.get_metric("customer_count").agg == "count"
        assert customers.get_metric("unique_emails").agg == "count_distinct"
        assert customers.get_metric("avg_lifetime_value").agg == "avg"
        assert customers.get_metric("total_lifetime_value").agg == "sum"
        assert customers.get_metric("max_lifetime_value").agg == "max"
        assert customers.get_metric("min_lifetime_value").agg == "min"

        # Filtered measure
        hv = customers.get_metric("high_value_count")
        assert hv.agg == "count"
        assert hv.filters is not None

    def test_customers_join(self, kitchen_sink_graph):
        customers = kitchen_sink_graph.get_model("customers")
        rels = {r.name: r for r in customers.relationships}
        assert "regions" in rels
        assert rels["regions"].type == "many_to_one"
        assert rels["regions"].foreign_key == "region_id"

    def test_products_dot_method_measure(self, kitchen_sink_graph):
        products = kitchen_sink_graph.get_model("products")
        total_cost = products.get_metric("total_cost")
        assert total_cost.agg == "sum"
        assert total_cost.sql == "cost"

    def test_products_computed_dimension(self, kitchen_sink_graph):
        products = kitchen_sink_graph.get_model("products")
        label = products.get_dimension("product_label")
        assert label is not None
        assert "concat" in label.sql.lower()

    def test_products_arithmetic_dimension(self, kitchen_sink_graph):
        products = kitchen_sink_graph.get_model("products")
        margin = products.get_dimension("margin")
        assert margin is not None
        assert margin.type == "numeric"

    def test_orders_apply_pick(self, kitchen_sink_graph):
        orders = kitchen_sink_graph.get_model("orders")
        order_size = orders.get_dimension("order_size")
        assert order_size is not None
        assert "CASE" in order_size.sql.upper()

    def test_orders_date_literal(self, kitchen_sink_graph):
        orders = kitchen_sink_graph.get_model("orders")
        is_recent = orders.get_dimension("is_recent")
        assert is_recent is not None
        assert "DATE" in is_recent.sql.upper()
        assert "2024-02-01" in is_recent.sql

    def test_orders_segment(self, kitchen_sink_graph):
        orders = kitchen_sink_graph.get_model("orders")
        assert len(orders.segments) >= 1
        segment_sqls = [s.sql for s in orders.segments]
        assert any("status" in sql for sql in segment_sqls)

    def test_orders_derived_measure(self, kitchen_sink_graph):
        orders = kitchen_sink_graph.get_model("orders")
        delivery_rate = orders.get_metric("delivery_rate")
        assert delivery_rate is not None
        assert delivery_rate.type == "derived"

    def test_orders_rename(self, kitchen_sink_graph):
        orders = kitchen_sink_graph.get_model("orders")
        order_status = orders.get_dimension("order_status")
        assert order_status is not None
        assert order_status.sql == "status"

    def test_orders_joins(self, kitchen_sink_graph):
        orders = kitchen_sink_graph.get_model("orders")
        rels = {r.name: r for r in orders.relationships}
        assert "customers" in rels
        assert rels["customers"].type == "many_to_one"
        assert "order_items" in rels
        assert rels["order_items"].type == "one_to_many"

    def test_sql_source(self, kitchen_sink_graph):
        summary = kitchen_sink_graph.get_model("order_summary")
        assert summary is not None
        assert summary.sql is not None
        assert summary.table is None
        assert "customer_id" in summary.sql

    def test_source_inheritance(self, kitchen_sink_graph):
        vip = kitchen_sink_graph.get_model("vip_customers")
        assert vip is not None
        assert vip.extends == "customers"

    def test_join_direction(self, kitchen_sink_graph):
        directed = kitchen_sink_graph.get_model("directed_orders")
        rels = {r.name: r for r in directed.relationships}
        assert "customers" in rels
        assert rels["customers"].metadata is not None
        assert rels["customers"].metadata.get("join_direction") == "left"


# =============================================================================
# BASIC QUERY TESTS
# =============================================================================


class TestBasicMetrics:
    """Test basic single-model queries against DuckDB."""

    def test_simple_count(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["orders.order_count"])
        records = fetch_dicts(result)
        assert len(records) == 1
        assert records[0]["order_count"] == 10

    def test_simple_sum(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["orders.total_revenue"])
        records = fetch_dicts(result)
        assert len(records) == 1
        expected = 500 + 216 + 103 + 980 + 59 + 91.40 + 603 + 108 + 304 + 170
        assert abs(records[0]["total_revenue"] - expected) < 0.01

    def test_avg_metric(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["orders.avg_order_value"])
        records = fetch_dicts(result)
        assert len(records) == 1
        total = 500 + 216 + 103 + 980 + 59 + 91.40 + 603 + 108 + 304 + 170
        assert abs(records[0]["avg_order_value"] - total / 10) < 0.01

    def test_min_max(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["orders.min_order_value", "orders.max_order_value"])
        records = fetch_dicts(result)
        assert len(records) == 1
        assert abs(records[0]["min_order_value"] - 59.00) < 0.01
        assert abs(records[0]["max_order_value"] - 980.00) < 0.01

    def test_count_distinct(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["orders.unique_customers"])
        records = fetch_dicts(result)
        assert len(records) == 1
        assert records[0]["unique_customers"] == 6

    def test_customer_count(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["customers.customer_count"])
        records = fetch_dicts(result)
        assert len(records) == 1
        assert records[0]["customer_count"] == 6

    def test_product_count(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["products.product_count"])
        records = fetch_dicts(result)
        assert len(records) == 1
        assert records[0]["product_count"] == 4


# =============================================================================
# DIMENSION QUERY TESTS
# =============================================================================


class TestDimensionQueries:
    """Test queries with dimension groupings."""

    def test_categorical_dimension(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["orders.order_count"], dimensions=["orders.status"])
        records = fetch_dicts(result)
        status_counts = {r["status"]: r["order_count"] for r in records}
        assert status_counts["delivered"] == 5
        assert status_counts["shipped"] == 2
        assert status_counts["cancelled"] == 1
        assert status_counts["pending"] == 1
        assert status_counts["refunded"] == 1

    def test_channel_dimension(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["orders.order_count"], dimensions=["orders.channel"])
        records = fetch_dicts(result)
        channel_counts = {r["channel"]: r["order_count"] for r in records}
        # Orders: web(1,3,4,5,7,9)=6, mobile(2,8,10)=3, phone(6)=1
        assert channel_counts["web"] == 6
        assert channel_counts["mobile"] == 3
        assert channel_counts["phone"] == 1

    def test_time_dimension_month(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(
            metrics=["orders.order_count"],
            dimensions=["orders.created_month"],
        )
        records = fetch_dicts(result)
        assert len(records) == 2  # Jan and Feb 2024


# =============================================================================
# FILTERED MEASURE TESTS
# =============================================================================


class TestFilteredMeasures:
    """Test measures with { where: ... } filters."""

    def test_delivered_count(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["orders.delivered_count"])
        records = fetch_dicts(result)
        assert len(records) == 1
        assert records[0]["delivered_count"] == 5

    def test_delivered_revenue(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["orders.delivered_revenue"])
        records = fetch_dicts(result)
        assert len(records) == 1
        # Delivered orders: 1(500), 2(216), 4(980), 7(603), 9(304)
        expected = 500 + 216 + 980 + 603 + 304
        assert abs(records[0]["delivered_revenue"] - expected) < 0.01

    def test_web_orders(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["orders.web_orders"])
        records = fetch_dicts(result)
        assert len(records) == 1
        # Orders with channel='web': 1, 3, 4, 5, 7, 9
        assert records[0]["web_orders"] == 6

    def test_filtered_vs_unfiltered(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["orders.order_count", "orders.delivered_count", "orders.web_orders"])
        records = fetch_dicts(result)
        assert records[0]["order_count"] == 10
        assert records[0]["delivered_count"] == 5
        assert records[0]["web_orders"] == 6

    def test_customer_filtered_measure(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["customers.high_value_count"])
        records = fetch_dicts(result)
        assert len(records) == 1
        # Customers with lifetime_value > 2000: Alice(5000), Bob(2500), Eve(3000)
        assert records[0]["high_value_count"] == 3

    def test_product_filtered_measure(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["products.active_count"])
        records = fetch_dicts(result)
        assert len(records) == 1
        assert records[0]["active_count"] == 3


# =============================================================================
# CROSS-MODEL JOIN TESTS
# =============================================================================


class TestJoinQueries:
    """Test queries across joined models."""

    def test_orders_by_customer_tier(self, kitchen_sink_layer):
        """1-hop join: orders -> customers."""
        result = kitchen_sink_layer.query(
            metrics=["orders.order_count"],
            dimensions=["customers.tier"],
        )
        records = fetch_dicts(result)
        tier_counts = {r["tier"]: r["order_count"] for r in records}
        # Platinum (customer 1): orders 1, 2
        assert tier_counts["platinum"] == 2
        # Gold (customer 2, 5): orders 3, 7, 8
        assert tier_counts["gold"] == 3

    def test_orders_by_region(self, kitchen_sink_layer):
        """2-hop join: orders -> customers -> regions."""
        result = kitchen_sink_layer.query(
            metrics=["orders.order_count"],
            dimensions=["regions.name"],
        )
        records = fetch_dicts(result)
        region_counts = {r["name"]: r["order_count"] for r in records}
        # NA (customers 1,2): 3 orders
        assert region_counts["North America"] == 3
        # Europe (customers 3,4): 3 orders
        assert region_counts["Europe"] == 3
        # APAC (customers 5,6): 4 orders
        assert region_counts["Asia Pacific"] == 4

    def test_order_items_total(self, kitchen_sink_layer):
        result = kitchen_sink_layer.query(metrics=["order_items.total_quantity"])
        records = fetch_dicts(result)
        assert len(records) == 1
        # Sum all quantities: 3+1+2+1+2+1+1+1+2+1+1+1 = 17
        assert records[0]["total_quantity"] == 17


# =============================================================================
# SEGMENT TESTS
# =============================================================================


class TestSegments:
    """Test source-level where clauses applied as segments."""

    def test_orders_default_segment(self, kitchen_sink_layer):
        """The orders source has where: status != 'test', which should
        already be applied (no test orders in our data anyway)."""
        result = kitchen_sink_layer.query(
            metrics=["orders.order_count"],
            segments=["orders.default_filter"],
        )
        records = fetch_dicts(result)
        assert len(records) == 1
        # All 10 orders pass since none have status='test'
        assert records[0]["order_count"] == 10
