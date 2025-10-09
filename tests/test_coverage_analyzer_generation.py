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


def test_generate_models_with_date_trunc():
    """Test extracting time dimensions from DATE_TRUNC."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            DATE_TRUNC('month', order_date) as month,
            COUNT(*) as count
        FROM orders
        GROUP BY DATE_TRUNC('month', order_date)
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    orders = models["orders"]

    # Should have order_date as time dimension
    assert "dimensions" in orders
    dims = {d["name"]: d for d in orders["dimensions"]}
    assert "order_date" in dims
    assert dims["order_date"]["type"] == "time"


def test_generate_rewritten_query_with_date_trunc():
    """Test rewriting queries with DATE_TRUNC to use granularity syntax."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            DATE_TRUNC('month', order_date),
            SUM(amount)
        FROM orders
        GROUP BY DATE_TRUNC('month', order_date)
        """
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)

    sql = rewritten["query_1"]

    # Should use semantic layer granularity syntax
    assert "orders.order_date__month" in sql
    assert "orders.sum_amount" in sql


def test_generate_rewritten_query_with_having():
    """Test rewriting queries with HAVING clause."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            category,
            SUM(revenue)
        FROM products
        GROUP BY category
        HAVING SUM(revenue) > 10000
        """
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)

    sql = rewritten["query_1"]

    # Should preserve HAVING clause
    assert "HAVING" in sql
    assert "SUM(revenue) > 10000" in sql


def test_generate_rewritten_query_with_order_by_limit():
    """Test rewriting queries with ORDER BY and LIMIT."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            status,
            COUNT(*)
        FROM orders
        GROUP BY status
        ORDER BY COUNT(*) DESC
        LIMIT 10
        """
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)

    sql = rewritten["query_1"]

    # Should preserve ORDER BY and LIMIT
    assert "ORDER BY" in sql
    assert "LIMIT 10" in sql


def test_generate_rewritten_query_multi_table():
    """Test rewriting multi-table queries with JOINs."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            c.region,
            COUNT(o.order_id)
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.region
        """
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)

    sql = rewritten["query_1"]

    # Should preserve JOIN with aliases
    assert "FROM customers c" in sql
    assert "JOIN orders o" in sql
    assert "ON c.customer_id = o.customer_id" in sql

    # Should resolve aliases to real table names in SELECT
    assert "customers.region" in sql
    assert "orders.order_id_count" in sql


def test_generate_rewritten_query_left_join():
    """Test rewriting LEFT JOIN queries."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            c.name,
            COUNT(o.id) as order_count
        FROM customers c
        LEFT JOIN orders o ON c.id = o.customer_id
        GROUP BY c.name
        """
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)

    sql = rewritten["query_1"]

    # Should preserve LEFT JOIN
    assert "LEFT JOIN orders o" in sql
    assert "ON c.id = o.customer_id" in sql


def test_generate_rewritten_query_multiple_joins():
    """Test rewriting queries with multiple JOINs."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            c.region,
            p.category,
            SUM(o.amount)
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        JOIN products p ON o.product_id = p.id
        GROUP BY c.region, p.category
        """
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)

    sql = rewritten["query_1"]

    # Should preserve both JOINs
    assert "FROM customers c" in sql
    assert "JOIN orders o" in sql
    assert "JOIN products p" in sql


def test_extract_having_clause():
    """Test that HAVING clauses are extracted."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT category, SUM(amount)
        FROM products
        GROUP BY category
        HAVING SUM(amount) > 1000
        """
    ]

    report = analyzer.analyze_queries(queries)
    analysis = report.query_analyses[0]

    assert len(analysis.having_clauses) == 1
    assert "SUM(amount) > 1000" in analysis.having_clauses[0]


def test_extract_order_by():
    """Test that ORDER BY is extracted."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT status, COUNT(*)
        FROM orders
        GROUP BY status
        ORDER BY COUNT(*) DESC, status ASC
        """
    ]

    report = analyzer.analyze_queries(queries)
    analysis = report.query_analyses[0]

    assert len(analysis.order_by) == 2


def test_extract_limit():
    """Test that LIMIT is extracted."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT status, COUNT(*)
        FROM orders
        GROUP BY status
        LIMIT 20
        """
    ]

    report = analyzer.analyze_queries(queries)
    analysis = report.query_analyses[0]

    assert analysis.limit == 20


def test_extract_derived_metrics():
    """Test that derived metrics are extracted from expressions."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            status,
            SUM(total_amount) / COUNT(*) as avg_order_value,
            SUM(revenue) / COUNT(DISTINCT customer_id) as revenue_per_customer,
            COUNT(*) * 100 as count_pct
        FROM orders
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)
    analysis = report.query_analyses[0]

    # Should extract derived metrics
    assert len(analysis.derived_metrics) == 3
    metric_names = {m[0] for m in analysis.derived_metrics}
    assert "avg_order_value" in metric_names
    assert "revenue_per_customer" in metric_names
    assert "count_pct" in metric_names

    # Should still have base aggregations for model generation
    assert len(analysis.aggregations) > 0

    # Base aggregations should be marked as part of derived metrics
    assert len(analysis.aggregations_in_derived) > 0


def test_generate_models_with_derived_metrics():
    """Test that models include derived metrics."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            status,
            SUM(total_amount) / COUNT(*) as avg_order_value
        FROM orders
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    orders = models["orders"]

    # Should have both base metrics and derived metrics
    metrics = {m["name"]: m for m in orders["metrics"]}

    # Base metrics needed for derived calculation
    assert "sum_total_amount" in metrics
    assert "count" in metrics

    # Derived metric
    assert "avg_order_value" in metrics
    assert metrics["avg_order_value"]["type"] == "derived"
    assert "SUM(total_amount)" in metrics["avg_order_value"]["sql"]


def test_extract_relationships_from_joins():
    """Test that relationships are extracted from JOIN ON conditions."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT c.region, COUNT(o.order_id)
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.region
        """
    ]

    report = analyzer.analyze_queries(queries)
    analysis = report.query_analyses[0]

    # Should extract relationships
    assert len(analysis.relationships) == 2

    # Check relationships
    rels_by_table = {}
    for from_model, to_model, rel_type, fk_col, pk_col in analysis.relationships:
        rels_by_table[from_model] = (to_model, rel_type, fk_col, pk_col)

    # orders has many_to_one to customers
    assert "orders" in rels_by_table
    assert rels_by_table["orders"][0] == "customers"  # to_model
    assert rels_by_table["orders"][1] == "many_to_one"  # rel_type
    assert rels_by_table["orders"][2] == "customer_id"  # fk_col

    # customers has one_to_many to orders
    assert "customers" in rels_by_table
    assert rels_by_table["customers"][0] == "orders"  # to_model
    assert rels_by_table["customers"][1] == "one_to_many"  # rel_type


def test_generate_models_with_relationships():
    """Test that generated models include relationships."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT c.name, COUNT(o.order_id)
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        GROUP BY c.name
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    # Check customers model
    customers = models["customers"]
    assert "relationships" in customers
    cust_rels = {r["name"]: r for r in customers["relationships"]}
    assert "orders" in cust_rels
    assert cust_rels["orders"]["type"] == "one_to_many"
    assert "foreign_key" not in cust_rels["orders"]  # FK is on orders side

    # Check orders model
    orders = models["orders"]
    assert "relationships" in orders
    orders_rels = {r["name"]: r for r in orders["relationships"]}
    assert "customers" in orders_rels
    assert orders_rels["customers"]["type"] == "many_to_one"
    assert orders_rels["customers"]["foreign_key"] == "customer_id"


def test_generate_models_with_multiple_joins():
    """Test relationship extraction with multiple JOINs."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT c.region, p.category, SUM(o.amount)
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        JOIN products p ON o.product_id = p.id
        GROUP BY c.region, p.category
        """
    ]

    report = analyzer.analyze_queries(queries)
    models = analyzer.generate_models(report)

    # orders should have relationships to both customers and products
    orders = models["orders"]
    assert "relationships" in orders
    orders_rels = {r["name"]: r for r in orders["relationships"]}

    assert "customers" in orders_rels
    assert orders_rels["customers"]["type"] == "many_to_one"
    assert orders_rels["customers"]["foreign_key"] == "customer_id"

    assert "products" in orders_rels
    assert orders_rels["products"]["type"] == "many_to_one"
    assert orders_rels["products"]["foreign_key"] == "product_id"


def test_rewrite_query_with_derived_metrics():
    """Test that derived metrics appear in rewritten queries without base metrics."""
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer)

    queries = [
        """
        SELECT
            status,
            SUM(revenue) / COUNT(*) as avg_revenue
        FROM orders
        GROUP BY status
        """
    ]

    report = analyzer.analyze_queries(queries)
    rewritten = analyzer.generate_rewritten_queries(report)
    sql = rewritten["query_1"]

    # Should include dimension and derived metric
    assert "orders.status" in sql
    assert "orders.avg_revenue" in sql

    # Should NOT include base metrics in SELECT
    assert "orders.sum_revenue" not in sql
    assert "orders.count" not in sql


def test_information_schema_relationship_detection():
    """Test that relationships are detected using information_schema."""
    import duckdb

    # Create in-memory database with FK constraints
    con = duckdb.connect(":memory:")

    con.execute("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            region VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount DECIMAL,
            status VARCHAR,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        )
    """)

    # Create analyzer with connection
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer, connection=con)

    # Verify schema metadata was loaded
    assert "customers" in analyzer.primary_keys
    assert "id" in analyzer.primary_keys["customers"]
    assert ("orders", "customer_id") in analyzer.foreign_keys
    assert analyzer.foreign_keys[("orders", "customer_id")] == ("customers", "id")

    # Analyze a query
    queries = [
        """
        SELECT c.region, COUNT(o.id)
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        GROUP BY c.region
        """
    ]

    report = analyzer.analyze_queries(queries)
    analysis = report.query_analyses[0]

    # Should extract relationships using information_schema
    assert len(analysis.relationships) == 2

    # Verify correct FK/PK detection
    rels_by_table = {}
    for from_model, to_model, rel_type, fk_col, pk_col in analysis.relationships:
        if rel_type == "many_to_one":
            rels_by_table[from_model] = (to_model, rel_type, fk_col, pk_col)

    # orders has many_to_one to customers with correct PK
    assert "orders" in rels_by_table
    assert rels_by_table["orders"][0] == "customers"
    assert rels_by_table["orders"][2] == "customer_id"
    assert rels_by_table["orders"][3] == "id"  # PK from information_schema, not inferred

    con.close()


def test_information_schema_column_inference():
    """Test unqualified column inference using information_schema."""
    import duckdb

    # Create in-memory database
    con = duckdb.connect(":memory:")

    con.execute("""
        CREATE TABLE customers (
            id INTEGER,
            name VARCHAR,
            region VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE orders (
            id INTEGER,
            customer_id INTEGER,
            amount DECIMAL
        )
    """)

    # Create analyzer with connection
    layer = SemanticLayer(auto_register=False)
    analyzer = CoverageAnalyzer(layer, connection=con)

    # Verify column metadata was loaded
    assert "region" in analyzer.table_columns["customers"]
    assert "amount" in analyzer.table_columns["orders"]

    # Analyze query with unqualified columns
    queries = [
        """
        SELECT region, SUM(amount)
        FROM customers
        JOIN orders ON customers.id = orders.customer_id
        GROUP BY region
        """
    ]

    report = analyzer.analyze_queries(queries)
    analysis = report.query_analyses[0]

    # Should infer that 'region' belongs to 'customers'
    assert "customers" in analysis.columns
    assert "region" in analysis.columns["customers"]

    # Should infer that 'amount' belongs to 'orders'
    assert "orders" in analysis.columns
    assert "amount" in analysis.columns["orders"]

    con.close()
