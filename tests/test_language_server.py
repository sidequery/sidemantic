"""Tests for SQL Language Server functionality."""

import tempfile
from pathlib import Path

import pytest

# Only run tests if pygls is installed
pytest.importorskip("pygls")

from sidemantic.language_server import SidemanticLanguageServer


@pytest.fixture
def demo_models_dir():
    """Create a temporary directory with demo model files."""
    tmpdir = tempfile.mkdtemp()
    tmpdir_path = Path(tmpdir)

    # Create a simple model file
    model_yaml = """
models:
  - name: orders
    table: orders_table
    dimensions:
      - name: order_id
        sql: order_id
        type: categorical
        description: Unique identifier for each order
      - name: customer_name
        sql: customer_name
        type: categorical
        description: Name of the customer
      - name: order_date
        sql: order_date
        type: time
        granularity: day
        description: Date when the order was placed
    metrics:
      - name: total_revenue
        agg: sum
        sql: amount
        description: Total revenue from all orders
      - name: order_count
        agg: count_distinct
        sql: order_id
        description: Total number of orders
"""
    model_file = tmpdir_path / "orders.yml"
    model_file.write_text(model_yaml)

    yield tmpdir_path


def test_language_server_initialization():
    """Test that the language server can be initialized."""
    from pygls.protocol import LanguageServerProtocol, default_converter

    server = SidemanticLanguageServer(protocol_cls=LanguageServerProtocol, converter_factory=default_converter)
    assert server is not None
    assert server.semantic_layer is None


def test_language_server_load_semantic_layer(demo_models_dir):
    """Test loading semantic layer from configuration."""
    from pygls.protocol import LanguageServerProtocol, default_converter

    server = SidemanticLanguageServer(protocol_cls=LanguageServerProtocol, converter_factory=default_converter)
    server.load_semantic_layer(demo_models_dir)

    assert server.semantic_layer is not None
    assert "orders" in server.semantic_layer.graph.models
    assert len(server.semantic_layer.graph.models["orders"].dimensions) == 3
    assert len(server.semantic_layer.graph.models["orders"].metrics) == 2


def test_language_server_validates_models(demo_models_dir):
    """Test that loaded models have correct structure."""
    from pygls.protocol import LanguageServerProtocol, default_converter

    server = SidemanticLanguageServer(protocol_cls=LanguageServerProtocol, converter_factory=default_converter)
    server.load_semantic_layer(demo_models_dir)

    orders_model = server.semantic_layer.graph.models["orders"]

    # Check dimensions
    dimension_names = [d.name for d in orders_model.dimensions]
    assert "order_id" in dimension_names
    assert "customer_name" in dimension_names
    assert "order_date" in dimension_names

    # Check metrics
    metric_names = [m.name for m in orders_model.metrics]
    assert "total_revenue" in metric_names
    assert "order_count" in metric_names


def test_language_server_cli_import():
    """Test that the language server can be imported from CLI."""
    try:
        from sidemantic.language_server import start_language_server

        assert start_language_server is not None
    except ImportError:
        pytest.skip("pygls not installed")


def test_language_server_table_metadata(demo_models_dir):
    """Test that table metadata is loaded from the database."""
    from pygls.protocol import LanguageServerProtocol, default_converter

    # Create a test table in the semantic layer's database
    server = SidemanticLanguageServer(protocol_cls=LanguageServerProtocol, converter_factory=default_converter)
    server.load_semantic_layer(demo_models_dir)

    # Create a test table
    if server.semantic_layer:
        server.semantic_layer.conn.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER,
                name VARCHAR,
                created_at TIMESTAMP
            )
        """)

        # Reload metadata
        server._load_table_metadata()

        # Check that table metadata was loaded
        assert "test_table" in server.table_metadata
        table_meta = server.table_metadata["test_table"]
        assert table_meta.name == "test_table"
        assert len(table_meta.columns) >= 3
        assert "id" in table_meta.columns
        assert "name" in table_meta.columns
        assert "created_at" in table_meta.columns


def test_table_metadata_class():
    """Test the TableMetadata class."""
    from sidemantic.language_server import TableMetadata

    table = TableMetadata("users", "public", {"id": "INTEGER", "email": "VARCHAR"})
    assert table.name == "users"
    assert table.schema == "public"
    assert len(table.columns) == 2
    assert table.columns["id"] == "INTEGER"
    assert table.columns["email"] == "VARCHAR"
