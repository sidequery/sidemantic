"""Tests for coverage analyzer model and query generation."""

from sidemantic import SemanticLayer
from sidemantic.core.coverage_analyzer import CoverageAnalyzer


def test_generate_models_from_queries():
    """Test generating model definitions from queries."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT status, region, SUM(amount), COUNT(*)
        FROM orders
        GROUP BY status, region
        """,
        """
        SELECT category, AVG(price), COUNT(DISTINCT product_id)
        FROM products
        GROUP BY category
        """,
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    # Should generate 2 models
    assert len(models) == 2
    assert "orders" in models
    assert "products" in models

    # Check orders model
    orders = models["orders"]
    assert orders["model"]["name"] == "orders"
    assert orders["model"]["table"] == "orders"

    # Check orders dimensions
    assert len(orders["dimensions"]) == 2
    dim_names = {d["name"] for d in orders["dimensions"]}
    assert "status" in dim_names
    assert "region" in dim_names

    # Check orders metrics
    assert len(orders["metrics"]) == 2
    metric_names = {m["name"] for m in orders["metrics"]}
    assert "sum_amount" in metric_names
    assert "count" in metric_names

    # Check products model
    products = models["products"]
    assert products["model"]["name"] == "products"

    # Check products dimensions
    assert len(products["dimensions"]) == 1
    assert products["dimensions"][0]["name"] == "category"

    # Check products metrics
    assert len(products["metrics"]) == 2
    metric_names = {m["name"] for m in products["metrics"]}
    assert "avg_price" in metric_names
    assert "product_id_count" in metric_names


def test_generate_models_count_distinct():
    """Test COUNT(DISTINCT col) generates correct metric."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT status, COUNT(DISTINCT customer_id)
        FROM orders
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    orders = models["orders"]
    metrics = {m["name"]: m for m in orders["metrics"]}

    assert "customer_id_count" in metrics
    assert metrics["customer_id_count"]["agg"] == "count_distinct"
    assert metrics["customer_id_count"]["sql"] == "customer_id"


def test_generate_models_no_duplicate_metrics():
    """Test that duplicate metrics are not generated."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        "SELECT status, SUM(amount) FROM orders GROUP BY status",
        "SELECT region, SUM(amount) FROM orders GROUP BY region",
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    orders = models["orders"]
    metric_names = [m["name"] for m in orders["metrics"]]

    # sum_amount should only appear once
    assert metric_names.count("sum_amount") == 1


def test_generate_rewritten_queries():
    """Test generating rewritten queries."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT status, SUM(amount), COUNT(*)
        FROM orders
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)

    # Should generate 1 rewritten query
    assert len(rewritten) == 1
    assert "query_1" in rewritten

    sql = rewritten["query_1"]

    # Check it's SQL format
    assert "SELECT" in sql
    assert "FROM orders" in sql

    # Check it uses semantic layer syntax (model.dimension, model.metric)
    assert "orders.status" in sql
    assert "orders.count" in sql
    assert "orders.sum_amount" in sql


def test_generate_rewritten_queries_with_filter():
    """Test generating rewritten queries with WHERE clause."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT status, SUM(amount)
        FROM orders
        WHERE status = 'completed'
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)

    sql = rewritten["query_1"]

    # Check it includes WHERE clause
    assert "WHERE" in sql
    assert "status = 'completed'" in sql or "status='completed'" in sql


def test_generate_rewritten_queries_skips_unparseable():
    """Test that unparseable queries are skipped."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        "SELECT FROM WHERE",  # Invalid
        "SELECT status, COUNT(*) FROM orders GROUP BY status",  # Valid
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)

    # Should only generate 1 query (skip the invalid one)
    assert len(rewritten) == 1


def test_write_model_files(tmp_path):
    """Test writing model files to disk."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        "SELECT status, SUM(amount) FROM orders GROUP BY status",
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    output_dir = tmp_path / "models"
    analyzer.write_model_files(models, str(output_dir))

    # Check file was created
    orders_file = output_dir / "orders.yml"
    assert orders_file.exists()

    # Check file contents
    import yaml

    with open(orders_file) as f:
        data = yaml.safe_load(f)

    assert data["model"]["name"] == "orders"
    assert len(data["dimensions"]) == 1
    assert len(data["metrics"]) == 1


def test_write_rewritten_queries(tmp_path):
    """Test writing rewritten queries to disk."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        "SELECT status, COUNT(*) FROM orders GROUP BY status",
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)

    output_dir = tmp_path / "queries"
    analyzer.write_rewritten_queries(rewritten, str(output_dir))

    # Check file was created
    query_file = output_dir / "query_1.sql"
    assert query_file.exists()

    # Check file contents
    content = query_file.read_text()
    assert "SELECT" in content
    assert "FROM orders" in content
    assert "orders.status" in content
    assert "orders.count" in content


def test_generate_models_multiple_aggregations_same_column():
    """Test handling multiple aggregation types on same column."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            status,
            SUM(amount),
            AVG(amount),
            MIN(amount),
            MAX(amount)
        FROM orders
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    orders = models["orders"]
    metric_names = {m["name"] for m in orders["metrics"]}

    assert "sum_amount" in metric_names
    assert "avg_amount" in metric_names
    assert "min_amount" in metric_names
    assert "max_amount" in metric_names
