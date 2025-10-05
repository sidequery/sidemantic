"""Performance tests for query rewriting and SQL generation.

These tests measure execution time for common operations to catch performance regressions.
"""

import time

import duckdb
import pytest

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_layer import SemanticLayer


@pytest.fixture
def performance_layer():
    """Create a semantic layer with multiple models for performance testing."""
    conn = duckdb.connect(":memory:")

    # Create test data
    conn.execute("""
        CREATE TABLE orders AS
        SELECT
            id::INTEGER AS id,
            customer_id::INTEGER AS customer_id,
            product_id::INTEGER AS product_id,
            amount::DECIMAL(10,2) AS amount,
            status::VARCHAR AS status,
            order_date::DATE AS order_date
        FROM (VALUES
            (1, 1, 1, 100.00, 'completed', '2024-01-01'),
            (2, 1, 2, 150.00, 'completed', '2024-01-02'),
            (3, 2, 1, 200.00, 'pending', '2024-01-03'),
            (4, 2, 3, 75.00, 'completed', '2024-01-04'),
            (5, 3, 2, 300.00, 'completed', '2024-01-05')
        ) AS t(id, customer_id, product_id, amount, status, order_date)
    """)

    conn.execute("""
        CREATE TABLE customers AS
        SELECT
            id::INTEGER AS id,
            name::VARCHAR AS name,
            region::VARCHAR AS region,
            created_at::DATE AS created_at
        FROM (VALUES
            (1, 'Alice', 'US', '2023-01-01'),
            (2, 'Bob', 'EU', '2023-02-01'),
            (3, 'Charlie', 'US', '2023-03-01')
        ) AS t(id, name, region, created_at)
    """)

    conn.execute("""
        CREATE TABLE products AS
        SELECT
            id::INTEGER AS id,
            name::VARCHAR AS name,
            category::VARCHAR AS category,
            price::DECIMAL(10,2) AS price
        FROM (VALUES
            (1, 'Widget', 'Electronics', 50.00),
            (2, 'Gadget', 'Electronics', 75.00),
            (3, 'Tool', 'Hardware', 25.00)
        ) AS t(id, name, category, price)
    """)

    # Define models
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        relationships=[
            Relationship(name="customers", type="many_to_one", foreign_key="customer_id"),
            Relationship(name="products", type="many_to_one", foreign_key="product_id"),
        ],
        dimensions=[
            Dimension(name="id", sql="id", type="categorical"),
            Dimension(name="status", sql="status", type="categorical"),
            Dimension(name="order_date", sql="order_date", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="order_count", agg="count", sql="id"),
            Metric(name="avg_order_value", agg="avg", sql="amount"),
        ],
    )

    customers = Model(
        name="customers",
        table="customers",
        primary_key="id",
        dimensions=[
            Dimension(name="id", sql="id", type="categorical"),
            Dimension(name="name", sql="name", type="categorical"),
            Dimension(name="region", sql="region", type="categorical"),
        ],
        metrics=[
            Metric(name="customer_count", agg="count", sql="id"),
        ],
    )

    products = Model(
        name="products",
        table="products",
        primary_key="id",
        dimensions=[
            Dimension(name="id", sql="id", type="categorical"),
            Dimension(name="name", sql="name", type="categorical"),
            Dimension(name="category", sql="category", type="categorical"),
        ],
        metrics=[
            Metric(name="avg_price", agg="avg", sql="price"),
        ],
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.conn = conn  # Use existing connection with test data
    layer.add_model(orders)
    layer.add_model(customers)
    layer.add_model(products)

    return layer


def test_simple_rewrite_performance(performance_layer):
    """Measure performance of simple query rewriting."""
    from sidemantic.sql.query_rewriter import QueryRewriter

    sql = "SELECT revenue FROM orders"
    rewriter = QueryRewriter(performance_layer.graph, dialect="duckdb")

    iterations = 1000
    start = time.perf_counter()

    for _ in range(iterations):
        rewriter.rewrite(sql)

    elapsed = time.perf_counter() - start
    avg_ms = (elapsed / iterations) * 1000

    print(f"\nSimple rewrite: {avg_ms:.3f}ms per query ({iterations} iterations)")
    assert avg_ms < 10.0, f"Simple rewrite too slow: {avg_ms:.3f}ms"


def test_complex_rewrite_performance(performance_layer):
    """Measure performance of complex query rewriting with joins and filters."""
    from sidemantic.sql.query_rewriter import QueryRewriter

    sql = """
        SELECT
            orders.revenue,
            customers.region,
            products.category
        FROM orders
        WHERE customers.region = 'US'
          AND orders.status = 'completed'
          AND orders.order_date >= '2024-01-01'
    """
    rewriter = QueryRewriter(performance_layer.graph, dialect="duckdb")

    iterations = 1000
    start = time.perf_counter()

    for _ in range(iterations):
        rewriter.rewrite(sql)

    elapsed = time.perf_counter() - start
    avg_ms = (elapsed / iterations) * 1000

    print(f"\nComplex rewrite: {avg_ms:.3f}ms per query ({iterations} iterations)")
    assert avg_ms < 15.0, f"Complex rewrite too slow: {avg_ms:.3f}ms"


def test_sql_generation_performance(performance_layer):
    """Measure performance of SQL generation."""
    from sidemantic.sql.generator_v2 import SQLGenerator

    generator = SQLGenerator(performance_layer.graph, dialect="duckdb")

    iterations = 1000
    start = time.perf_counter()

    for _ in range(iterations):
        generator.generate(
            metrics=["orders.revenue"],
            dimensions=["orders.status"],
            filters=["orders.status = 'completed'"],
        )

    elapsed = time.perf_counter() - start
    avg_ms = (elapsed / iterations) * 1000

    print(f"\nSQL generation: {avg_ms:.3f}ms per query ({iterations} iterations)")
    assert avg_ms < 15.0, f"SQL generation too slow: {avg_ms:.3f}ms"


def test_multi_join_generation_performance(performance_layer):
    """Measure performance of SQL generation with multiple joins."""
    from sidemantic.sql.generator_v2 import SQLGenerator

    generator = SQLGenerator(performance_layer.graph, dialect="duckdb")

    iterations = 1000
    start = time.perf_counter()

    for _ in range(iterations):
        generator.generate(
            metrics=["orders.revenue", "customers.customer_count", "products.avg_price"],
            dimensions=["customers.region", "products.category"],
            filters=["customers.region = 'US'"],
        )

    elapsed = time.perf_counter() - start
    avg_ms = (elapsed / iterations) * 1000

    print(f"\nMulti-join generation: {avg_ms:.3f}ms per query ({iterations} iterations)")
    assert avg_ms < 20.0, f"Multi-join generation too slow: {avg_ms:.3f}ms"


def test_end_to_end_execution_performance(performance_layer):
    """Measure end-to-end performance including SQL execution."""
    sql = """
        SELECT orders.revenue, customers.region
        FROM orders
        WHERE customers.region = 'US'
    """

    iterations = 100  # Fewer iterations since this includes DB execution
    start = time.perf_counter()

    for _ in range(iterations):
        result = performance_layer.sql(sql)
        result.fetchdf()

    elapsed = time.perf_counter() - start
    avg_ms = (elapsed / iterations) * 1000

    print(f"\nEnd-to-end execution: {avg_ms:.3f}ms per query ({iterations} iterations)")
    assert avg_ms < 50.0, f"End-to-end execution too slow: {avg_ms:.3f}ms"


def test_filter_parsing_performance(performance_layer):
    """Measure performance of filter parsing and transformation."""
    from sidemantic.sql.generator_v2 import SQLGenerator

    generator = SQLGenerator(performance_layer.graph, dialect="duckdb")

    filters = [
        "orders.status = 'completed'",
        "orders.order_date >= '2024-01-01'",
        "customers.region IN ('US', 'EU')",
        "orders.revenue > 100",
    ]

    iterations = 1000
    start = time.perf_counter()

    for _ in range(iterations):
        generator.generate(
            metrics=["orders.revenue"],
            dimensions=["orders.status"],
            filters=filters,
        )

    elapsed = time.perf_counter() - start
    avg_ms = (elapsed / iterations) * 1000

    print(f"\nFilter parsing: {avg_ms:.3f}ms per query ({iterations} iterations)")
    assert avg_ms < 20.0, f"Filter parsing too slow: {avg_ms:.3f}ms"


def test_query_rewriter_warm_vs_cold(performance_layer):
    """Compare performance of first query (cold) vs subsequent queries (warm)."""
    from sidemantic.sql.query_rewriter import QueryRewriter

    rewriter = QueryRewriter(performance_layer.graph, dialect="duckdb")

    sql = """
        SELECT orders.revenue, customers.region
        FROM orders
        WHERE customers.region = 'US'
    """

    # Cold run
    start = time.perf_counter()
    rewriter.rewrite(sql)
    cold_time = (time.perf_counter() - start) * 1000

    # Warm runs
    iterations = 100
    start = time.perf_counter()
    for _ in range(iterations):
        rewriter.rewrite(sql)
    warm_time = ((time.perf_counter() - start) / iterations) * 1000

    print(f"\nCold run: {cold_time:.3f}ms")
    print(f"Warm run: {warm_time:.3f}ms (avg of {iterations})")
    print(f"Speedup: {cold_time / warm_time:.1f}x")

    # Warm runs should be reasonably fast
    assert warm_time < 15.0, f"Warm runs too slow: {warm_time:.3f}ms"


def test_parameter_substitution_performance(performance_layer):
    """Measure performance with parameter substitution."""
    from sidemantic.sql.generator_v2 import SQLGenerator

    generator = SQLGenerator(performance_layer.graph, dialect="duckdb")

    parameters = {
        "start_date": "2024-01-01",
        "status": "completed",
    }

    iterations = 1000
    start = time.perf_counter()

    for _ in range(iterations):
        generator.generate(
            metrics=["orders.revenue"],
            dimensions=[],
            filters=[
                "orders.order_date >= {{ start_date }}",
                "orders.status = {{ status }}",
            ],
            parameters=parameters,
        )

    elapsed = time.perf_counter() - start
    avg_ms = (elapsed / iterations) * 1000

    print(f"\nParameter substitution: {avg_ms:.3f}ms per query ({iterations} iterations)")
    assert avg_ms < 20.0, f"Parameter substitution too slow: {avg_ms:.3f}ms"
