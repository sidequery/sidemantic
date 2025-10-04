"""Test multi-hop join discovery and execution.

Multi-hop joins enable queries across models that aren't directly connected.
Example: orders -> customers -> regions (2 hops)
"""

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer


@pytest.fixture
def three_table_chain():
    """Create 3-table chain: orders -> customers -> regions."""
    conn = duckdb.connect(":memory:")

    conn.execute("""CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, order_amount DECIMAL(10, 2))""")
    conn.execute("""CREATE TABLE customers (customer_id INTEGER, region_id INTEGER, customer_name VARCHAR)""")
    conn.execute("""CREATE TABLE regions (region_id INTEGER, region_name VARCHAR)""")

    conn.execute("""INSERT INTO orders VALUES (1, 101, 150.00), (2, 102, 200.00), (3, 101, 100.00), (4, 103, 300.00)""")
    conn.execute("""INSERT INTO customers VALUES (101, 1, 'Alice'), (102, 2, 'Bob'), (103, 1, 'Charlie')""")
    conn.execute("""INSERT INTO regions VALUES (1, 'North America'), (2, 'Europe')""")

    return conn


def test_two_hop_join(three_table_chain):
    """Test 2-hop join path discovery."""
    # TODO: Implement test for orders -> customers -> regions
    # Expected: Correct SQL with 2 LEFT JOINs, all CTEs included
    pass


def test_join_path_discovery(three_table_chain):
    """Test join path algorithm finds multi-hop paths."""
    # TODO: Implement test for graph.find_join_path("orders", "regions")
    # Expected: Returns 2-element list of JoinPath objects
    pass


def test_intermediate_model_included():
    """Test that intermediate models are included in CTEs."""
    # TODO: Implement test verifying customers_cte exists even though only orders and regions are in SELECT
    # Expected: All 3 CTEs present in generated SQL
    pass


def test_query_execution(three_table_chain):
    """Test multi-hop query executes and returns correct results."""
    # TODO: Implement end-to-end test with query execution
    # Expected: Revenue grouped by region_name works correctly
    pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
