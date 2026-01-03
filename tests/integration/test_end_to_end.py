"""End-to-end integration tests for the sidemantic semantic layer.

These tests exercise the full workflow from model definition through query execution,
including YAML/BSL loading, SQL compilation, and DuckDB execution with real data.
"""

import tempfile
from pathlib import Path

import duckdb
import pytest

from sidemantic import (
    Dimension,
    Metric,
    Model,
    PreAggregation,
    Relationship,
    Segment,
    SemanticLayer,
)
from sidemantic.adapters.bsl import BSLAdapter
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.preagg_matcher import PreAggregationMatcher
from sidemantic.validation import QueryValidationError
from tests.utils import fetch_dicts, fetch_rows

# =============================================================================
# Fixtures for DuckDB test data
# =============================================================================


@pytest.fixture
def sample_db():
    """Create a DuckDB database with sample e-commerce data."""
    conn = duckdb.connect(":memory:")

    # Create customers table
    conn.execute("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            region VARCHAR,
            tier VARCHAR,
            created_at DATE
        )
    """)
    conn.execute("""
        INSERT INTO customers VALUES
        (1, 'Alice', 'alice@example.com', 'US', 'Gold', '2023-01-15'),
        (2, 'Bob', 'bob@example.com', 'EU', 'Silver', '2023-02-20'),
        (3, 'Charlie', 'charlie@example.com', 'US', 'Gold', '2023-03-10'),
        (4, 'Diana', 'diana@example.com', 'APAC', 'Bronze', '2023-04-05'),
        (5, 'Eve', 'eve@example.com', 'EU', 'Silver', '2023-05-12')
    """)

    # Create orders table
    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            order_amount DECIMAL(10, 2),
            discount DECIMAL(10, 2),
            status VARCHAR,
            created_at DATE,
            shipped_at DATE
        )
    """)
    conn.execute("""
        INSERT INTO orders VALUES
        (1, 1, 100.00, 10.00, 'completed', '2024-01-15', '2024-01-17'),
        (2, 1, 200.00, 0.00, 'completed', '2024-01-20', '2024-01-22'),
        (3, 2, 150.00, 15.00, 'pending', '2024-01-25', NULL),
        (4, 3, 300.00, 30.00, 'completed', '2024-02-01', '2024-02-03'),
        (5, 2, 75.00, 0.00, 'cancelled', '2024-02-05', NULL),
        (6, 4, 500.00, 50.00, 'completed', '2024-02-10', '2024-02-12'),
        (7, 5, 125.00, 12.50, 'completed', '2024-02-15', '2024-02-17'),
        (8, 1, 175.00, 17.50, 'pending', '2024-02-20', NULL),
        (9, 3, 250.00, 25.00, 'completed', '2024-03-01', '2024-03-03'),
        (10, 4, 400.00, 40.00, 'pending', '2024-03-05', NULL)
    """)

    # Create order_items table
    conn.execute("""
        CREATE TABLE order_items (
            item_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            unit_price DECIMAL(10, 2)
        )
    """)
    conn.execute("""
        INSERT INTO order_items VALUES
        (1, 1, 101, 2, 50.00),
        (2, 2, 102, 4, 50.00),
        (3, 3, 101, 3, 50.00),
        (4, 4, 103, 2, 150.00),
        (5, 5, 101, 1, 75.00),
        (6, 6, 104, 5, 100.00),
        (7, 7, 102, 5, 25.00),
        (8, 8, 105, 7, 25.00),
        (9, 9, 103, 1, 250.00),
        (10, 10, 104, 4, 100.00)
    """)

    # Create products table
    conn.execute("""
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            name VARCHAR,
            category VARCHAR,
            unit_cost DECIMAL(10, 2)
        )
    """)
    conn.execute("""
        INSERT INTO products VALUES
        (101, 'Widget A', 'Electronics', 25.00),
        (102, 'Widget B', 'Electronics', 20.00),
        (103, 'Gadget X', 'Appliances', 100.00),
        (104, 'Gadget Y', 'Appliances', 75.00),
        (105, 'Accessory Z', 'Accessories', 15.00)
    """)

    return conn


@pytest.fixture
def orders_model():
    """Create an orders model with standard dimensions and metrics."""
    return Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        relationships=[
            Relationship(name="customers", type="many_to_one", foreign_key="customer_id"),
        ],
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="created_at", type="time", granularity="day"),
            Dimension(name="shipped_at", type="time", granularity="day"),
            Dimension(name="order_amount", type="numeric"),  # For segment filtering
        ],
        metrics=[
            Metric(name="order_count", agg="count"),
            Metric(name="revenue", agg="sum", sql="order_amount"),
            Metric(name="total_discount", agg="sum", sql="discount"),
            Metric(name="avg_order_value", agg="avg", sql="order_amount"),
            Metric(name="unique_customers", agg="count_distinct", sql="customer_id"),
            Metric(
                name="completed_revenue",
                agg="sum",
                sql="order_amount",
                filters=["{model}.status = 'completed'"],
            ),
        ],
        segments=[
            # Use {model} template for proper column resolution
            Segment(name="high_value", sql="{model}.order_amount > 200"),
            Segment(name="completed", sql="{model}.status = 'completed'"),
        ],
    )


@pytest.fixture
def customers_model():
    """Create a customers model."""
    return Model(
        name="customers",
        table="customers",
        primary_key="customer_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
            Dimension(name="tier", type="categorical"),
            Dimension(name="name", type="categorical"),
            Dimension(name="created_at", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="customer_count", agg="count_distinct", sql="customer_id"),
        ],
    )


@pytest.fixture
def order_items_model():
    """Create an order_items model."""
    return Model(
        name="order_items",
        table="order_items",
        primary_key="item_id",
        relationships=[
            Relationship(name="orders", type="many_to_one", foreign_key="order_id"),
            Relationship(name="products", type="many_to_one", foreign_key="product_id"),
        ],
        dimensions=[
            Dimension(name="quantity", type="numeric"),
        ],
        metrics=[
            Metric(name="item_count", agg="count"),
            Metric(name="total_quantity", agg="sum", sql="quantity"),
            Metric(name="item_revenue", agg="sum", sql="quantity * unit_price"),
        ],
    )


@pytest.fixture
def products_model():
    """Create a products model."""
    return Model(
        name="products",
        table="products",
        primary_key="product_id",
        dimensions=[
            Dimension(name="name", type="categorical"),
            Dimension(name="category", type="categorical"),
        ],
        metrics=[
            Metric(name="product_count", agg="count_distinct", sql="product_id"),
            Metric(name="avg_unit_cost", agg="avg", sql="unit_cost"),
        ],
    )


# =============================================================================
# Test 1: Full workflow tests - YAML loading
# =============================================================================


class TestYAMLWorkflow:
    """Test full workflow from YAML model files through query execution."""

    def test_load_yaml_model_and_query(self, sample_db):
        """Load YAML model file, compile query, execute against DuckDB, verify results."""
        yaml_content = """
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: order_amount
      - name: order_count
        agg: count
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            yaml_path = f.name

        try:
            layer = SemanticLayer.from_yaml(yaml_path)
            layer.conn = sample_db

            result = layer.query(
                metrics=["orders.revenue", "orders.order_count"],
                dimensions=["orders.status"],
            )
            rows = fetch_dicts(result)

            # Verify results
            by_status = {row["status"]: row for row in rows}
            assert "completed" in by_status
            assert "pending" in by_status
            assert "cancelled" in by_status

            # completed: 100 + 200 + 300 + 500 + 125 + 250 = 1475
            assert float(by_status["completed"]["revenue"]) == 1475.0
            assert by_status["completed"]["order_count"] == 6

            # pending: 150 + 175 + 400 = 725
            assert float(by_status["pending"]["revenue"]) == 725.0
            assert by_status["pending"]["order_count"] == 3

            # cancelled: 75
            assert float(by_status["cancelled"]["revenue"]) == 75.0
            assert by_status["cancelled"]["order_count"] == 1

        finally:
            Path(yaml_path).unlink()

    def test_yaml_with_relationships(self, sample_db):
        """Test YAML model with relationships and cross-model queries."""
        yaml_content = """
models:
  - name: orders
    table: orders
    primary_key: order_id
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
    metrics:
      - name: revenue
        agg: sum
        sql: order_amount

  - name: customers
    table: customers
    primary_key: customer_id
    dimensions:
      - name: region
        type: categorical
      - name: tier
        type: categorical
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            yaml_path = f.name

        try:
            layer = SemanticLayer.from_yaml(yaml_path)
            layer.conn = sample_db

            result = layer.query(
                metrics=["orders.revenue"],
                dimensions=["customers.region"],
            )
            rows = fetch_dicts(result)

            by_region = {row["region"]: float(row["revenue"]) for row in rows}

            # US customers (Alice=1, Charlie=3): orders 1,2,4,8,9 = 100+200+300+175+250 = 1025
            assert by_region["US"] == 1025.0

            # EU customers (Bob=2, Eve=5): orders 3,5,7 = 150+75+125 = 350
            assert by_region["EU"] == 350.0

            # APAC customers (Diana=4): orders 6,10 = 500+400 = 900
            assert by_region["APAC"] == 900.0

        finally:
            Path(yaml_path).unlink()


# =============================================================================
# Test 2: Full workflow tests - BSL loading
# =============================================================================


class TestBSLWorkflow:
    """Test full workflow from BSL model files through query execution."""

    def test_load_bsl_model_and_query(self, sample_db):
        """Load BSL model file, compile query, execute against DuckDB, verify results."""
        # BSL format uses is_entity to mark the primary key dimension
        bsl_content = """
orders:
  table: orders
  description: "Order transactions"

  dimensions:
    order_id:
      expr: _.order_id
      is_entity: true
    status:
      expr: _.status
    created_at:
      expr: _.created_at
      is_time_dimension: true
      smallest_time_grain: "TIME_GRAIN_DAY"

  measures:
    count:
      expr: _.count()
    revenue:
      expr: _.order_amount.sum()
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(bsl_content)
            bsl_path = f.name

        try:
            adapter = BSLAdapter()
            graph = adapter.parse(bsl_path)

            layer = SemanticLayer()
            layer.conn = sample_db
            layer.graph = graph

            result = layer.query(
                metrics=["orders.revenue"],
                dimensions=["orders.status"],
            )
            rows = fetch_dicts(result)

            by_status = {row["status"]: float(row["revenue"]) for row in rows}
            assert by_status["completed"] == 1475.0
            assert by_status["pending"] == 725.0
            assert by_status["cancelled"] == 75.0

        finally:
            Path(bsl_path).unlink()

    def test_bsl_with_joins(self, sample_db):
        """Test BSL model with joins and cross-model queries."""
        bsl_content = """
orders:
  table: orders
  description: "Order transactions"

  dimensions:
    order_id:
      expr: _.order_id
      is_entity: true
    status: _.status
    customer_id: _.customer_id

  measures:
    revenue:
      expr: _.order_amount.sum()

  joins:
    customers:
      model: customers
      type: one
      left_on: customer_id
      right_on: customer_id

customers:
  table: customers
  description: "Customer data"

  dimensions:
    customer_id:
      expr: _.customer_id
      is_entity: true
    region: _.region
    tier: _.tier
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(bsl_content)
            bsl_path = f.name

        try:
            adapter = BSLAdapter()
            graph = adapter.parse(bsl_path)

            layer = SemanticLayer()
            layer.conn = sample_db
            layer.graph = graph

            result = layer.query(
                metrics=["orders.revenue"],
                dimensions=["customers.tier"],
            )
            rows = fetch_dicts(result)

            by_tier = {row["tier"]: float(row["revenue"]) for row in rows}

            # Gold: Alice(1) + Charlie(3) = orders 1,2,4,8,9 = 100+200+300+175+250 = 1025
            assert by_tier["Gold"] == 1025.0

            # Silver: Bob(2) + Eve(5) = orders 3,5,7 = 150+75+125 = 350
            assert by_tier["Silver"] == 350.0

            # Bronze: Diana(4) = orders 6,10 = 500+400 = 900
            assert by_tier["Bronze"] == 900.0

        finally:
            Path(bsl_path).unlink()


# =============================================================================
# Test 3: Multi-model join queries
# =============================================================================


class TestMultiModelJoins:
    """Test queries spanning multiple models with joins."""

    def test_two_model_join(self, sample_db, orders_model, customers_model):
        """Test query spanning 2 models with join."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)
        layer.add_model(customers_model)

        result = layer.query(
            metrics=["orders.revenue"],
            dimensions=["customers.region", "orders.status"],
        )
        rows = fetch_dicts(result)

        # Verify join SQL was generated correctly
        sql = layer.compile(
            metrics=["orders.revenue"],
            dimensions=["customers.region", "orders.status"],
        )
        assert "LEFT JOIN" in sql or "JOIN" in sql
        assert "customers" in sql.lower()

        # Verify data
        assert len(rows) > 0
        for row in rows:
            assert "region" in row
            assert "status" in row
            assert "revenue" in row

    def test_three_model_join_chain(self, sample_db, orders_model, customers_model, order_items_model, products_model):
        """Test query spanning 3+ models with join chain."""
        # Add relationship from orders to order_items (reverse direction)
        orders_model.relationships.append(Relationship(name="order_items", type="one_to_many", foreign_key="order_id"))

        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)
        layer.add_model(customers_model)
        layer.add_model(order_items_model)
        layer.add_model(products_model)

        # Query across orders -> customers (for region) and orders -> order_items (for quantity)
        result = layer.query(
            metrics=["order_items.total_quantity"],
            dimensions=["customers.region"],
        )
        rows = fetch_dicts(result)

        by_region = {row["region"]: int(row["total_quantity"]) for row in rows}

        # US customers (1, 3): orders 1,2,4,8,9 -> items with quantities 2,4,2,7,1 = 16
        assert by_region["US"] == 16

        # EU customers (2, 5): orders 3,5,7 -> items with quantities 3,1,5 = 9
        assert by_region["EU"] == 9

        # APAC customers (4): orders 6,10 -> items with quantities 5,4 = 9
        assert by_region["APAC"] == 9

    def test_join_sql_generation(self, sample_db, orders_model, customers_model):
        """Verify correct join SQL is generated for multi-model queries."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)
        layer.add_model(customers_model)

        sql = layer.compile(
            metrics=["orders.revenue"],
            dimensions=["customers.region"],
        )

        # Verify JOIN is present
        assert "JOIN" in sql.upper()

        # Verify both tables are referenced
        assert "orders" in sql.lower()
        assert "customers" in sql.lower()

        # Verify join condition uses foreign key
        assert "customer_id" in sql


# =============================================================================
# Test 4: Derived metrics with filters
# =============================================================================


class TestDerivedMetricsWithFilters:
    """Test derived metrics with metric-level and query-level filters."""

    def test_derived_metric_basic(self, sample_db, orders_model):
        """Test basic derived metric calculation."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        # Add derived metric (ratio)
        revenue_per_order = Metric(
            name="revenue_per_order",
            type="ratio",
            numerator="orders.revenue",
            denominator="orders.order_count",
        )
        layer.add_metric(revenue_per_order)

        result = layer.query(metrics=["revenue_per_order"])
        rows = fetch_dicts(result)

        # Total revenue: 2275, Total orders: 10
        # revenue_per_order = 2275 / 10 = 227.5
        assert len(rows) == 1
        assert float(rows[0]["revenue_per_order"]) == 227.5

    def test_derived_metric_with_metric_level_filter(self, sample_db, orders_model):
        """Test that ratio metrics properly apply metric-level filters to referenced measures.

        When a ratio metric references measures that have their own filters defined,
        those filters should be applied when computing the ratio. This ensures that:
        - completed_revenue (with filter status='completed') computes only completed orders
        - revenue (with no filter) computes all orders
        - The ratio correctly reflects completed_revenue / total_revenue
        """
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        # Query the base metrics directly to verify their individual behavior
        result_completed = layer.query(metrics=["orders.completed_revenue"])
        completed = fetch_dicts(result_completed)
        # completed_revenue should be 1475 (with filter applied)
        assert float(completed[0]["completed_revenue"]) == 1475.0

        result_total = layer.query(metrics=["orders.revenue"])
        total = fetch_dicts(result_total)
        # total revenue should be 2275
        assert float(total[0]["revenue"]) == 2275.0

        # Create ratio metric
        completion_rate = Metric(
            name="completion_rate",
            type="ratio",
            numerator="orders.completed_revenue",
            denominator="orders.revenue",
        )
        layer.add_metric(completion_rate)

        result = layer.query(metrics=["completion_rate"])
        rows = fetch_dicts(result)

        # The ratio should correctly apply the metric-level filter on completed_revenue
        # completion_rate = completed_revenue / revenue = 1475 / 2275 ≈ 0.648
        assert len(rows) == 1
        rate = float(rows[0]["completion_rate"])
        expected_rate = 1475.0 / 2275.0  # ≈ 0.6483516483516484
        assert abs(rate - expected_rate) < 0.001, f"Expected {expected_rate}, got {rate}"

    def test_query_filter_combined_with_metric_filter(self, sample_db, orders_model):
        """Test query-level filter combined with metric-level filter."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        # Query completed_revenue (which filters status='completed')
        # with additional query filter on created_at
        sql = layer.compile(
            metrics=["orders.completed_revenue"],
            filters=["orders.created_at >= '2024-02-01'"],
        )

        # Both filters should be in the SQL
        assert "completed" in sql
        assert "2024-02-01" in sql

        result = layer.query(
            metrics=["orders.completed_revenue"],
            filters=["orders.created_at >= '2024-02-01'"],
        )
        rows = fetch_dicts(result)

        # Completed orders after 2024-02-01: orders 4,6,7,9
        # = 300 + 500 + 125 + 250 = 1175
        assert float(rows[0]["completed_revenue"]) == 1175.0

    def test_filter_sql_correctness(self, sample_db, orders_model):
        """Verify filter SQL is generated correctly."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        # Test metric-level filter appears in SQL
        sql = layer.compile(metrics=["orders.completed_revenue"])
        # The filter uses {model} template which expands to orders_cte
        assert "status = 'completed'" in sql

    def test_multiple_filtered_metrics_together(self, sample_db):
        """Test querying multiple metrics with different filters in a single query.

        Each metric's filter should only affect that specific metric, not other
        metrics in the same query. This is achieved by applying metric-level filters
        via CASE WHEN inside each aggregation, rather than in the WHERE clause.
        """
        model = Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            dimensions=[
                Dimension(name="status", type="categorical"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="order_amount"),
                Metric(
                    name="completed_revenue",
                    agg="sum",
                    sql="order_amount",
                    filters=["{model}.status = 'completed'"],
                ),
                Metric(
                    name="pending_revenue",
                    agg="sum",
                    sql="order_amount",
                    filters=["{model}.status = 'pending'"],
                ),
            ],
        )

        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(model)

        # Query metrics individually to verify filters work correctly
        result_revenue = layer.query(metrics=["orders.revenue"])
        rows_revenue = fetch_dicts(result_revenue)
        assert float(rows_revenue[0]["revenue"]) == 2275.0

        result_completed = layer.query(metrics=["orders.completed_revenue"])
        rows_completed = fetch_dicts(result_completed)
        assert float(rows_completed[0]["completed_revenue"]) == 1475.0

        result_pending = layer.query(metrics=["orders.pending_revenue"])
        rows_pending = fetch_dicts(result_pending)
        assert float(rows_pending[0]["pending_revenue"]) == 725.0

        # Query unfiltered and filtered metrics together
        # Each metric should compute independently with its own filters
        result_both = layer.query(metrics=["orders.revenue", "orders.completed_revenue"])
        rows_both = fetch_dicts(result_both)
        assert float(rows_both[0]["completed_revenue"]) == 1475.0  # Only completed orders
        assert float(rows_both[0]["revenue"]) == 2275.0  # All orders (NOT filtered)

        # Query all three metrics together - each should have independent filters
        result_all = layer.query(metrics=["orders.revenue", "orders.completed_revenue", "orders.pending_revenue"])
        rows_all = fetch_dicts(result_all)
        assert float(rows_all[0]["revenue"]) == 2275.0  # All orders
        assert float(rows_all[0]["completed_revenue"]) == 1475.0  # Completed only
        assert float(rows_all[0]["pending_revenue"]) == 725.0  # Pending only


# =============================================================================
# Test 5: Pre-aggregation matching
# =============================================================================


class TestPreAggregationMatching:
    """Test pre-aggregation definition and matching logic."""

    def test_preagg_definition_and_match(self):
        """Test pre-aggregation definitions match correctly."""
        model = Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            dimensions=[
                Dimension(name="status", type="categorical"),
                Dimension(name="region", type="categorical"),
                Dimension(name="created_at", type="time", granularity="day"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="order_amount"),
                Metric(name="order_count", agg="count"),
            ],
            pre_aggregations=[
                PreAggregation(
                    name="daily_by_status",
                    measures=["revenue", "order_count"],
                    dimensions=["status"],
                    time_dimension="created_at",
                    granularity="day",
                )
            ],
        )

        matcher = PreAggregationMatcher(model)

        # Exact match
        preagg = matcher.find_matching_preagg(
            metrics=["revenue"],
            dimensions=["status"],
            time_granularity="day",
        )
        assert preagg is not None
        assert preagg.name == "daily_by_status"

        # Coarser granularity (month) should match
        preagg = matcher.find_matching_preagg(
            metrics=["revenue"],
            dimensions=["status"],
            time_granularity="month",
        )
        assert preagg is not None

        # Finer granularity (hour) should NOT match
        preagg = matcher.find_matching_preagg(
            metrics=["revenue"],
            dimensions=["status"],
            time_granularity="hour",
        )
        assert preagg is None

    def test_preagg_subset_dimensions_match(self):
        """Test pre-agg matches when query uses subset of dimensions."""
        model = Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            dimensions=[
                Dimension(name="status", type="categorical"),
                Dimension(name="region", type="categorical"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="order_amount"),
            ],
            pre_aggregations=[
                PreAggregation(
                    name="by_status_region",
                    measures=["revenue"],
                    dimensions=["status", "region"],
                )
            ],
        )

        matcher = PreAggregationMatcher(model)

        # Query with only status should match (subset of pre-agg dimensions)
        preagg = matcher.find_matching_preagg(
            metrics=["revenue"],
            dimensions=["status"],
        )
        assert preagg is not None

        # Query with dimension not in pre-agg should NOT match
        model.dimensions.append(Dimension(name="customer_id", type="categorical"))
        preagg = matcher.find_matching_preagg(
            metrics=["revenue"],
            dimensions=["customer_id"],
        )
        assert preagg is None

    def test_preagg_sql_generation(self, sample_db):
        """Test SQL generation uses pre-aggregation table when enabled."""
        model = Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            dimensions=[
                Dimension(name="status", type="categorical", sql="status"),
                Dimension(name="created_at", type="time", sql="created_at", granularity="day"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="order_amount"),
            ],
            pre_aggregations=[
                PreAggregation(
                    name="daily_by_status",
                    measures=["revenue"],
                    dimensions=["status"],
                    time_dimension="created_at",
                    granularity="day",
                )
            ],
        )

        layer = SemanticLayer(use_preaggregations=True)
        layer.conn = sample_db
        layer.add_model(model)

        sql = layer.compile(
            metrics=["orders.revenue"],
            dimensions=["orders.status", "orders.created_at__day"],
        )

        # Should reference pre-aggregation table
        assert "orders_preagg_daily_by_status" in sql

    def test_preagg_disabled_by_default(self, sample_db):
        """Test that pre-aggregations are not used when disabled."""
        model = Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            dimensions=[
                Dimension(name="status", type="categorical", sql="status"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="order_amount"),
            ],
            pre_aggregations=[
                PreAggregation(
                    name="by_status",
                    measures=["revenue"],
                    dimensions=["status"],
                )
            ],
        )

        layer = SemanticLayer()  # use_preaggregations defaults to False
        layer.conn = sample_db
        layer.add_model(model)

        sql = layer.compile(
            metrics=["orders.revenue"],
            dimensions=["orders.status"],
        )

        # Should NOT reference pre-aggregation table
        assert "orders_preagg_" not in sql
        # Should use normal CTE approach
        assert "orders_cte" in sql


# =============================================================================
# Test 6: Time intelligence integration
# =============================================================================


class TestTimeIntelligence:
    """Test time intelligence features (YoY, MoM, etc.) in full query context."""

    def test_time_dimension_granularity(self, sample_db, orders_model):
        """Test time dimension with different granularities."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        # Query by month
        result = layer.query(
            metrics=["orders.revenue"],
            dimensions=["orders.created_at__month"],
        )
        rows = fetch_dicts(result)

        # Should have 3 months: Jan, Feb, Mar 2024
        assert len(rows) == 3

        # Verify monthly totals
        monthly = {}
        for row in rows:
            month_val = row["created_at__month"]
            # Handle both date objects and strings
            if hasattr(month_val, "strftime"):
                month_key = month_val.strftime("%Y-%m")
            else:
                month_key = str(month_val)[:7]
            monthly[month_key] = float(row["revenue"])

        # January: orders 1,2,3 = 100+200+150 = 450
        assert monthly["2024-01"] == 450.0

        # February: orders 4,5,6,7,8 = 300+75+500+125+175 = 1175
        assert monthly["2024-02"] == 1175.0

        # March: orders 9,10 = 250+400 = 650
        assert monthly["2024-03"] == 650.0

    def test_time_dimension_with_filters(self, sample_db, orders_model):
        """Test time dimension queries with filters."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        result = layer.query(
            metrics=["orders.revenue"],
            dimensions=["orders.created_at__month"],
            filters=["orders.status = 'completed'"],
        )
        rows = fetch_dicts(result)

        # Should only include completed orders by month
        monthly = {}
        for row in rows:
            month_val = row["created_at__month"]
            if hasattr(month_val, "strftime"):
                month_key = month_val.strftime("%Y-%m")
            else:
                month_key = str(month_val)[:7]
            monthly[month_key] = float(row["revenue"])

        # January completed: orders 1,2 = 100+200 = 300
        assert monthly["2024-01"] == 300.0

        # February completed: orders 4,6,7 = 300+500+125 = 925
        assert monthly["2024-02"] == 925.0

        # March completed: order 9 = 250
        assert monthly["2024-03"] == 250.0

    def test_multiple_time_granularities(self, sample_db, orders_model):
        """Test querying with different time granularities in same session."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        # Day granularity
        day_result = layer.query(
            metrics=["orders.order_count"],
            dimensions=["orders.created_at__day"],
        )
        day_rows = fetch_rows(day_result)
        assert len(day_rows) == 10  # 10 unique order dates

        # Month granularity
        month_result = layer.query(
            metrics=["orders.order_count"],
            dimensions=["orders.created_at__month"],
        )
        month_rows = fetch_rows(month_result)
        assert len(month_rows) == 3  # 3 months

        # Verify aggregation is correct at month level
        total_orders = sum(row[1] for row in month_rows)
        assert total_orders == 10


# =============================================================================
# Test 7: Error scenarios
# =============================================================================


class TestErrorScenarios:
    """Test error handling for invalid queries and configurations."""

    def test_invalid_model_reference(self, sample_db, orders_model):
        """Test error when referencing non-existent model."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        with pytest.raises(QueryValidationError) as exc_info:
            layer.compile(
                metrics=["nonexistent_model.revenue"],
                dimensions=[],
            )
        assert "not found" in str(exc_info.value).lower()

    def test_invalid_metric_reference(self, sample_db, orders_model):
        """Test error when referencing non-existent metric."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        with pytest.raises(QueryValidationError) as exc_info:
            layer.compile(
                metrics=["orders.nonexistent_metric"],
                dimensions=[],
            )
        assert "not found" in str(exc_info.value).lower()

    def test_invalid_dimension_reference(self, sample_db, orders_model):
        """Test error when referencing non-existent dimension."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        with pytest.raises(QueryValidationError) as exc_info:
            layer.compile(
                metrics=["orders.revenue"],
                dimensions=["orders.nonexistent_dimension"],
            )
        assert "not found" in str(exc_info.value).lower()

    def test_invalid_time_granularity(self, sample_db, orders_model):
        """Test error when using invalid time granularity."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        with pytest.raises(QueryValidationError) as exc_info:
            layer.compile(
                metrics=["orders.revenue"],
                dimensions=["orders.created_at__invalid"],
            )
        assert "granularity" in str(exc_info.value).lower()

    def test_missing_join_path(self, sample_db):
        """Test error when no join path exists between models."""
        orders = Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            # No relationships defined
            metrics=[Metric(name="revenue", agg="sum", sql="order_amount")],
        )
        customers = Model(
            name="customers",
            table="customers",
            primary_key="customer_id",
            dimensions=[Dimension(name="region", type="categorical")],
        )

        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders)
        layer.add_model(customers)

        with pytest.raises(QueryValidationError) as exc_info:
            layer.compile(
                metrics=["orders.revenue"],
                dimensions=["customers.region"],
            )
        assert "join path" in str(exc_info.value).lower() or "no join" in str(exc_info.value).lower()

    def test_self_referencing_metric_detection(self, sample_db, orders_model):
        """Test that self-referencing derived metrics are detected.

        Note: The validation catches direct self-references at metric validation time.
        For more complex circular dependencies (A->B->A), detection happens at query time
        when the dependencies are resolved.
        """
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        from sidemantic.validation import MetricValidationError

        # Direct self-reference should be caught
        self_ref_metric = Metric(
            name="self_ref",
            type="derived",
            sql="self_ref * 2",
        )

        with pytest.raises(MetricValidationError) as exc_info:
            layer.add_metric(self_ref_metric)
        assert "cannot reference itself" in str(exc_info.value).lower()

    def test_derived_metric_missing_dependency(self, sample_db, orders_model):
        """Test that querying a derived metric with missing dependency fails."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        # Create metric referencing non-existent dependency
        bad_metric = Metric(
            name="bad_metric",
            type="derived",
            sql="nonexistent_metric * 2",
        )
        # Adding the metric succeeds (validation is lazy)
        layer.add_metric(bad_metric)

        # But querying it should fail during compilation/execution
        with pytest.raises(Exception):  # Could be QueryValidationError or other
            layer.compile(metrics=["bad_metric"])


# =============================================================================
# Test 8: Real data scenarios with numeric verification
# =============================================================================


class TestRealDataScenarios:
    """Test with real DuckDB data and verify numeric results."""

    def test_aggregation_accuracy(self, sample_db, orders_model):
        """Test that aggregations produce accurate numeric results."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        result = layer.query(
            metrics=[
                "orders.revenue",
                "orders.order_count",
                "orders.avg_order_value",
                "orders.unique_customers",
            ]
        )
        rows = fetch_dicts(result)

        assert len(rows) == 1
        row = rows[0]

        # Verify each aggregation
        # Total revenue: sum of all order_amounts
        expected_revenue = 100 + 200 + 150 + 300 + 75 + 500 + 125 + 175 + 250 + 400
        assert float(row["revenue"]) == expected_revenue  # 2275

        # Order count
        assert row["order_count"] == 10

        # Average order value
        expected_avg = expected_revenue / 10
        assert float(row["avg_order_value"]) == expected_avg  # 227.5

        # Unique customers (1,2,3,4,5)
        assert row["unique_customers"] == 5

    def test_grouping_accuracy(self, sample_db, orders_model, customers_model):
        """Test that groupings produce correct results."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)
        layer.add_model(customers_model)

        result = layer.query(
            metrics=["orders.revenue", "orders.order_count"],
            dimensions=["orders.status"],
        )
        rows = fetch_dicts(result)

        by_status = {row["status"]: row for row in rows}

        # Completed orders: 1,2,4,6,7,9
        assert float(by_status["completed"]["revenue"]) == 100 + 200 + 300 + 500 + 125 + 250
        assert by_status["completed"]["order_count"] == 6

        # Pending orders: 3,8,10
        assert float(by_status["pending"]["revenue"]) == 150 + 175 + 400
        assert by_status["pending"]["order_count"] == 3

        # Cancelled orders: 5
        assert float(by_status["cancelled"]["revenue"]) == 75
        assert by_status["cancelled"]["order_count"] == 1

    def test_filter_accuracy(self, sample_db, orders_model):
        """Test that filters produce correct results."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        # Filter by order amount
        result = layer.query(
            metrics=["orders.revenue", "orders.order_count"],
            filters=["orders.order_amount > 200"],
        )
        rows = fetch_dicts(result)

        # Orders > 200: 4(300), 6(500), 9(250), 10(400) = 1450
        assert float(rows[0]["revenue"]) == 1450.0
        assert rows[0]["order_count"] == 4

    def test_segment_filtering(self, sample_db, orders_model):
        """Test that segments filter correctly."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        # Use high_value segment (order_amount > 200)
        result = layer.query(
            metrics=["orders.revenue"],
            segments=["orders.high_value"],
        )
        rows = fetch_dicts(result)

        # High value orders: 4(300), 6(500), 9(250), 10(400)
        assert float(rows[0]["revenue"]) == 1450.0

    def test_combined_segment_and_filter(self, sample_db, orders_model):
        """Test combining segment with additional filter."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        result = layer.query(
            metrics=["orders.revenue"],
            segments=["orders.completed"],  # status = 'completed'
            filters=["orders.order_amount > 200"],  # > 200
        )
        rows = fetch_dicts(result)

        # Completed AND > 200: orders 4(300), 6(500), 9(250) = 1050
        assert float(rows[0]["revenue"]) == 1050.0

    def test_order_by_and_limit(self, sample_db, orders_model, customers_model):
        """Test ORDER BY and LIMIT clauses."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)
        layer.add_model(customers_model)

        result = layer.query(
            metrics=["orders.revenue"],
            dimensions=["customers.region"],
            order_by=["orders.revenue DESC"],
            limit=2,
        )
        rows = fetch_dicts(result)

        assert len(rows) == 2
        # Should be ordered by revenue descending
        # US=1025, APAC=900, EU=350
        assert rows[0]["region"] == "US"
        assert rows[1]["region"] == "APAC"

    def test_count_distinct_accuracy(self, sample_db):
        """Test COUNT DISTINCT produces accurate results."""
        conn = sample_db

        # Create a model with count_distinct
        model = Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            dimensions=[
                Dimension(name="status", type="categorical"),
            ],
            metrics=[
                Metric(name="unique_customers", agg="count_distinct", sql="customer_id"),
            ],
        )

        layer = SemanticLayer()
        layer.conn = conn
        layer.add_model(model)

        result = layer.query(
            metrics=["orders.unique_customers"],
            dimensions=["orders.status"],
        )
        rows = fetch_dicts(result)

        by_status = {row["status"]: row["unique_customers"] for row in rows}

        # Completed: customers 1,2,3,4,5 (all 5)
        # Wait, let me recalculate: completed orders are 1,2,4,6,7,9
        # Customers: 1,1,3,4,5,3 -> unique: 1,3,4,5 = 4
        assert by_status["completed"] == 4

        # Pending: orders 3,8,10 -> customers 2,1,4 -> unique: 1,2,4 = 3
        assert by_status["pending"] == 3

        # Cancelled: order 5 -> customer 2 = 1
        assert by_status["cancelled"] == 1


# =============================================================================
# Test 9: SQL rewriter integration
# =============================================================================


class TestSQLRewriterIntegration:
    """Test SQL rewriter functionality with semantic layer."""

    def test_simple_sql_rewrite(self, sample_db, orders_model):
        """Test that simple SQL queries get rewritten correctly."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)

        # Use the sql() method which rewrites and executes
        result = layer.sql("SELECT orders.revenue, orders.status FROM orders")
        rows = fetch_dicts(result)

        by_status = {row["status"]: float(row["revenue"]) for row in rows}
        assert by_status["completed"] == 1475.0
        assert by_status["pending"] == 725.0
        assert by_status["cancelled"] == 75.0


# =============================================================================
# Test 10: Sidemantic native adapter integration
# =============================================================================


class TestSidemanticAdapterIntegration:
    """Test the native Sidemantic YAML adapter."""

    def test_full_yaml_with_metrics(self, sample_db):
        """Test loading full YAML with models and graph-level metrics."""
        yaml_content = """
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: order_amount
      - name: order_count
        agg: count

metrics:
  - name: avg_order_value
    type: ratio
    numerator: orders.revenue
    denominator: orders.order_count
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            yaml_path = f.name

        try:
            adapter = SidemanticAdapter()
            graph = adapter.parse(yaml_path)

            layer = SemanticLayer()
            layer.conn = sample_db
            layer.graph = graph

            # Query the graph-level derived metric
            result = layer.query(metrics=["avg_order_value"])
            rows = fetch_dicts(result)

            # 2275 / 10 = 227.5
            assert float(rows[0]["avg_order_value"]) == 227.5

        finally:
            Path(yaml_path).unlink()

    def test_yaml_roundtrip(self, sample_db, orders_model, customers_model):
        """Test exporting and re-importing YAML produces equivalent model."""
        layer = SemanticLayer()
        layer.conn = sample_db
        layer.add_model(orders_model)
        layer.add_model(customers_model)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml_path = f.name

        try:
            # Export
            layer.to_yaml(yaml_path)

            # Re-import
            layer2 = SemanticLayer.from_yaml(yaml_path)
            layer2.conn = sample_db

            # Verify same models exist
            assert set(layer.list_models()) == set(layer2.list_models())

            # Verify query produces same results
            # Note: Must fetch results immediately after each query because
            # DuckDB's Python API shares a cursor across the same connection.
            result1 = layer.query(metrics=["orders.revenue"], dimensions=["orders.status"])
            rows1 = sorted(fetch_rows(result1), key=lambda x: x[0])

            result2 = layer2.query(metrics=["orders.revenue"], dimensions=["orders.status"])
            rows2 = sorted(fetch_rows(result2), key=lambda x: x[0])

            assert rows1 == rows2

        finally:
            Path(yaml_path).unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
