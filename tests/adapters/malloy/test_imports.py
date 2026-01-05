"""Tests for Malloy adapter - import support."""

from pathlib import Path

from sidemantic.adapters.malloy import MalloyAdapter


def test_simple_import():
    """Test simple import (import all from file)."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/imports/orders.malloy"))

    # Should have imported customers and products from base.malloy
    assert "customers" in graph.models
    assert "products" in graph.models
    # Plus the orders model from orders.malloy
    assert "orders" in graph.models

    # Verify customers model
    customers = graph.get_model("customers")
    assert customers.table == "customers.parquet"
    assert customers.primary_key == "customer_id"
    assert len(customers.dimensions) == 4
    assert len(customers.metrics) == 1

    # Verify orders model references customers
    orders = graph.get_model("orders")
    assert orders.table == "orders.parquet"
    customer_rel = next((r for r in orders.relationships if r.name == "customers"), None)
    assert customer_rel is not None
    assert customer_rel.type == "many_to_one"
    assert customer_rel.foreign_key == "customer_id"


def test_named_import():
    """Test named import (only import specific sources)."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/imports/named_import.malloy"))

    # Should have imported only customers (not products)
    assert "customers" in graph.models
    assert "products" not in graph.models
    # Plus the customer_orders model
    assert "customer_orders" in graph.models


def test_aliased_import():
    """Test aliased import (import source with different name)."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/imports/aliased_import.malloy"))

    # Should have crm_customers (alias) not customers (original name)
    assert "crm_customers" in graph.models
    assert "customers" not in graph.models
    # Plus the sales model
    assert "sales" in graph.models

    # Verify the alias worked - crm_customers should have customers' structure
    crm_customers = graph.get_model("crm_customers")
    assert crm_customers.table == "customers.parquet"
    assert crm_customers.primary_key == "customer_id"

    # Verify sales references the aliased model
    sales = graph.get_model("sales")
    crm_rel = next((r for r in sales.relationships if r.name == "crm_customers"), None)
    assert crm_rel is not None


def test_multi_level_import():
    """Test multi-level imports (A imports B which imports C)."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/imports/multi_level.malloy"))

    # Should have all models from the import chain:
    # multi_level.malloy -> orders.malloy -> base.malloy
    assert "customers" in graph.models  # From base.malloy
    assert "products" in graph.models  # From base.malloy
    assert "orders" in graph.models  # From orders.malloy
    assert "order_analytics" in graph.models  # From multi_level.malloy

    # Verify the chain is complete
    orders = graph.get_model("orders")
    assert orders.table == "orders.parquet"

    order_analytics = graph.get_model("order_analytics")
    orders_rel = next((r for r in order_analytics.relationships if r.name == "orders"), None)
    assert orders_rel is not None


def test_circular_import_protection():
    """Test that circular imports don't cause infinite loops."""
    adapter = MalloyAdapter()

    # This should not hang or crash - circular imports are detected
    graph = adapter.parse(Path("tests/fixtures/malloy/imports/circular_a.malloy"))

    # Both models should be parsed (each file parsed once)
    assert "model_a" in graph.models
    assert "model_b" in graph.models


def test_import_missing_file():
    """Test graceful handling of missing import files."""
    import tempfile

    # Create a temp file that imports a non-existent file
    with tempfile.NamedTemporaryFile(suffix=".malloy", delete=False, mode="w") as f:
        f.write("""
import 'non_existent.malloy'

source: test is duckdb.table('test.parquet') extend {
  primary_key: id
  dimension: id is id
  measure: count_all is count()
}
""")
        temp_path = Path(f.name)

    try:
        adapter = MalloyAdapter()
        graph = adapter.parse(temp_path)

        # Should still parse the main file even if import is missing
        assert "test" in graph.models
    finally:
        temp_path.unlink()


def test_import_directory_with_imports():
    """Test parsing a directory where files have import relationships."""
    adapter = MalloyAdapter()

    # Parse the entire imports directory
    graph = adapter.parse(Path("tests/fixtures/malloy/imports"))

    # Should have all unique models (no duplicates from import chains)
    model_names = set(graph.models.keys())

    # Expected models from all files
    assert "customers" in model_names
    assert "products" in model_names
    assert "orders" in model_names
    assert "customer_orders" in model_names
    assert "sales" in model_names
    assert "order_analytics" in model_names
    assert "model_a" in model_names
    assert "model_b" in model_names

    # crm_customers is an alias - check it exists
    assert "crm_customers" in model_names
