"""Tests for semantic layer coverage analyzer."""

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.core.migrator import Migrator


def test_coverage_analyzer_basic():
    """Test basic coverage analysis."""
    # Create a simple semantic layer
    layer = SemanticLayer(auto_register=False)

    orders = Model(
        name="orders",
        table="orders_table",
        dimensions=[
            Dimension(name="order_id", sql="order_id", type="categorical"),
            Dimension(name="customer_id", sql="customer_id", type="categorical"),
            Dimension(name="status", sql="status", type="categorical"),
        ],
        metrics=[
            Metric(name="total_revenue", agg="sum", sql="amount"),
            Metric(name="order_count", agg="count", sql="*"),
        ],
    )

    layer.graph.add_model(orders)

    # Create analyzer
    analyzer = Migrator(layer)

    # Analyze a query that can be fully rewritten
    queries = [
        """
        SELECT status, SUM(amount) as revenue
        FROM orders_table
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)

    assert report.total_queries == 1
    assert report.parseable_queries == 1
    assert report.rewritable_queries == 1
    assert report.coverage_percentage == 100.0

    # Check analysis
    analysis = report.query_analyses[0]
    assert analysis.can_rewrite
    assert "orders_table" in analysis.tables
    assert ("sum", "amount", "orders_table") in analysis.aggregations  # table name is inferred from FROM clause
    assert ("", "status") in analysis.group_by_columns  # table name is empty when not qualified
    assert analysis.suggested_rewrite is not None
    assert "orders.status" in analysis.suggested_rewrite
    assert "orders.total_revenue" in analysis.suggested_rewrite


def test_coverage_analyzer_missing_model():
    """Test coverage analysis with missing model."""
    # Create a semantic layer without the orders model
    layer = SemanticLayer(auto_register=False)

    # Create analyzer
    analyzer = Migrator(layer)

    # Analyze a query with a missing table
    queries = [
        """
        SELECT status, SUM(amount) as revenue
        FROM orders_table
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)

    assert report.total_queries == 1
    assert report.parseable_queries == 1
    assert report.rewritable_queries == 0
    assert "orders_table" in report.missing_models

    # Check analysis
    analysis = report.query_analyses[0]
    assert not analysis.can_rewrite
    assert "orders_table" in analysis.missing_models


def test_coverage_analyzer_missing_dimension():
    """Test coverage analysis with missing dimension."""
    # Create a semantic layer with partial coverage
    layer = SemanticLayer(auto_register=False)

    orders = Model(
        name="orders",
        table="orders_table",
        dimensions=[
            Dimension(name="order_id", sql="order_id", type="categorical"),
        ],
        metrics=[
            Metric(name="total_revenue", agg="sum", sql="amount"),
        ],
    )

    layer.graph.add_model(orders)

    # Create analyzer
    analyzer = Migrator(layer)

    # Analyze a query with a missing dimension
    queries = [
        """
        SELECT status, SUM(amount) as revenue
        FROM orders_table
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)

    assert report.total_queries == 1
    assert report.parseable_queries == 1
    assert report.rewritable_queries == 0

    # Check analysis
    analysis = report.query_analyses[0]
    assert not analysis.can_rewrite
    assert ("orders", "status") in analysis.missing_dimensions  # uses model name, not table name


def test_coverage_analyzer_missing_metric():
    """Test coverage analysis with missing metric."""
    # Create a semantic layer with partial coverage
    layer = SemanticLayer(auto_register=False)

    orders = Model(
        name="orders",
        table="orders_table",
        dimensions=[
            Dimension(name="status", sql="status", type="categorical"),
        ],
        metrics=[
            # No metrics defined
        ],
    )

    layer.graph.add_model(orders)

    # Create analyzer
    analyzer = Migrator(layer)

    # Analyze a query with a missing metric
    queries = [
        """
        SELECT status, SUM(amount) as revenue
        FROM orders_table
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)

    assert report.total_queries == 1
    assert report.parseable_queries == 1
    assert report.rewritable_queries == 0

    # Check analysis
    analysis = report.query_analyses[0]
    assert not analysis.can_rewrite
    assert ("orders", "sum", "amount") in analysis.missing_metrics  # uses model name, not table name


def test_coverage_analyzer_multiple_queries():
    """Test coverage analysis with multiple queries."""
    # Create a semantic layer
    layer = SemanticLayer(auto_register=False)

    orders = Model(
        name="orders",
        table="orders_table",
        dimensions=[
            Dimension(name="status", sql="status", type="categorical"),
            Dimension(name="region", sql="region", type="categorical"),
        ],
        metrics=[
            Metric(name="total_revenue", agg="sum", sql="amount"),
            Metric(name="order_count", agg="count", sql="*"),
        ],
    )

    layer.graph.add_model(orders)

    # Create analyzer
    analyzer = Migrator(layer)

    # Analyze multiple queries
    queries = [
        "SELECT status, SUM(amount) FROM orders_table GROUP BY status",  # Can rewrite
        "SELECT region, COUNT(*) FROM orders_table GROUP BY region",  # Can rewrite
        "SELECT country, SUM(amount) FROM orders_table GROUP BY country",  # Missing dimension
        "SELECT status, AVG(amount) FROM orders_table GROUP BY status",  # Missing metric
    ]

    report = analyzer.analyze_queries(queries)

    assert report.total_queries == 4
    assert report.parseable_queries == 4
    assert report.rewritable_queries == 2
    assert report.coverage_percentage == 50.0


def test_coverage_analyzer_parse_error():
    """Test coverage analysis with unparseable query."""
    # Create a semantic layer
    layer = SemanticLayer(auto_register=False)

    # Create analyzer
    analyzer = Migrator(layer)

    # Analyze an invalid query
    queries = ["SELECT FROM WHERE"]

    report = analyzer.analyze_queries(queries)

    assert report.total_queries == 1
    assert report.parseable_queries == 0
    assert report.rewritable_queries == 0

    # Check analysis
    analysis = report.query_analyses[0]
    assert not analysis.can_rewrite
    assert analysis.parse_error is not None


def test_coverage_analyzer_with_execution():
    """Test coverage analyzer with actual query execution."""
    # Create a semantic layer with DuckDB
    layer = SemanticLayer(auto_register=False)

    # Create test data
    layer.conn.execute("CREATE TABLE orders (id INT, status VARCHAR, amount DECIMAL)")
    layer.conn.execute("INSERT INTO orders VALUES (1, 'completed', 100), (2, 'pending', 50)")

    orders = Model(
        name="orders",
        table="orders",
        dimensions=[
            Dimension(name="status", sql="status", type="categorical"),
        ],
        metrics=[
            Metric(name="total_revenue", agg="sum", sql="amount"),
        ],
    )

    layer.graph.add_model(orders)

    # Create analyzer
    analyzer = Migrator(layer)

    # Analyze a query
    queries = [
        """
        SELECT status, SUM(amount) as revenue
        FROM orders
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)

    assert report.total_queries == 1
    assert report.rewritable_queries == 1

    # Check that the suggested rewrite would work
    analysis = report.query_analyses[0]
    assert analysis.can_rewrite
    assert analysis.suggested_rewrite is not None

    # Verify the rewrite is correct
    assert "orders.status" in analysis.suggested_rewrite
    assert "orders.total_revenue" in analysis.suggested_rewrite
