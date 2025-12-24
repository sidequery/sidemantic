"""Test import/export/roundtrip for BSL adapter."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.bsl import BSLAdapter
from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter


def test_import_real_bsl_example():
    """Test importing a real BSL schema file."""
    adapter = BSLAdapter()
    graph = adapter.parse("tests/fixtures/bsl/orders.yml")

    # Verify models loaded
    assert "orders" in graph.models
    orders = graph.models["orders"]

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "status" in dim_names
    assert "created_at" in dim_names
    assert "customer_id" in dim_names

    # Verify measures
    measure_names = [m.name for m in orders.metrics]
    assert "count" in measure_names
    assert "revenue" in measure_names
    assert "avg_order_value" in measure_names

    # Verify measure types
    revenue = next(m for m in orders.metrics if m.name == "revenue")
    assert revenue.agg == "sum"
    assert revenue.sql == "amount"


def test_bsl_to_sidemantic_to_bsl_roundtrip():
    """Test that BSL -> Sidemantic -> BSL preserves structure."""
    # Import from BSL
    bsl_adapter = BSLAdapter()
    graph = bsl_adapter.parse("tests/fixtures/bsl/orders.yml")

    # Export back to BSL
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        bsl_adapter.export(graph, temp_path)

        # Re-import and verify
        graph2 = bsl_adapter.parse(temp_path)

        # Verify model preserved
        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify key fields preserved
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names
        assert "created_at" in dim_names

        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names
        assert "count" in measure_names

        # Verify time dimension preserved
        created_at = next(d for d in orders.dimensions if d.name == "created_at")
        assert created_at.type == "time"
        assert created_at.granularity == "day"

    finally:
        temp_path.unlink(missing_ok=True)


def test_bsl_to_cube_conversion():
    """Test converting BSL format to Cube format."""
    # Import from BSL
    bsl_adapter = BSLAdapter()
    graph = bsl_adapter.parse("tests/fixtures/bsl/orders.yml")

    # Export to Cube
    cube_adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        cube_adapter.export(graph, temp_path)

        # Re-import as Cube and verify structure
        graph2 = cube_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_to_bsl_conversion():
    """Test converting Cube format to BSL format."""
    # Import from Cube
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("tests/fixtures/cube/orders.yml")

    # Export to BSL
    bsl_adapter = BSLAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        bsl_adapter.export(graph, temp_path)

        # Re-import as BSL and verify structure
        graph2 = bsl_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_bsl_to_metricflow_conversion():
    """Test converting BSL format to MetricFlow format."""
    # Import from BSL
    bsl_adapter = BSLAdapter()
    graph = bsl_adapter.parse("tests/fixtures/bsl/orders.yml")

    # Export to MetricFlow
    mf_adapter = MetricFlowAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        mf_adapter.export(graph, temp_path)

        # Re-import as MetricFlow and verify structure
        graph2 = mf_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_query_imported_bsl_example():
    """Test that we can compile queries from imported BSL schema."""
    from sidemantic import SemanticLayer

    adapter = BSLAdapter()
    graph = adapter.parse("tests/fixtures/bsl/orders.yml")

    layer = SemanticLayer()
    layer.graph = graph

    # Test basic metric query
    sql = layer.compile(metrics=["orders.revenue"])
    assert "SUM" in sql.upper()

    # Test with dimension
    sql = layer.compile(metrics=["orders.revenue", "orders.count"], dimensions=["orders.status"])
    assert "GROUP BY" in sql.upper()
    assert "status" in sql.lower()


def test_query_with_time_dimension_bsl():
    """Test querying time dimensions from BSL import."""
    from sidemantic import SemanticLayer

    adapter = BSLAdapter()
    graph = adapter.parse("tests/fixtures/bsl/orders.yml")

    layer = SemanticLayer()
    layer.graph = graph

    # Query with time dimension
    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.created_at"])
    assert "created_at" in sql.lower()
    assert "GROUP BY" in sql.upper()


def test_roundtrip_real_bsl_example():
    """Test BSL example roundtrip using the actual example file."""
    adapter = BSLAdapter()

    # Import original
    graph1 = adapter.parse("tests/fixtures/bsl/orders.yml")

    # Export
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)

        # Import exported version
        graph2 = adapter.parse(temp_path)

        # Verify models match
        assert set(graph1.models.keys()) == set(graph2.models.keys())

        # Verify dimensions count preserved
        orders1 = graph1.models["orders"]
        orders2 = graph2.models["orders"]
        assert len(orders1.dimensions) == len(orders2.dimensions)

        # Verify metrics count preserved
        assert len(orders1.metrics) == len(orders2.metrics)

    finally:
        temp_path.unlink(missing_ok=True)


def test_bsl_flights_with_joins():
    """Test BSL flights example with joins roundtrips correctly."""
    adapter = BSLAdapter()

    # Import original
    graph1 = adapter.parse("tests/fixtures/bsl/flights.yml")

    # Verify joins imported
    flights = graph1.models["flights"]
    assert len(flights.relationships) == 1
    assert flights.relationships[0].name == "carriers"

    # Export and re-import
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)
        graph2 = adapter.parse(temp_path)

        # Verify joins preserved
        flights2 = graph2.models["flights"]
        assert len(flights2.relationships) == 1
        assert flights2.relationships[0].name == "carriers"
        assert flights2.relationships[0].type == "many_to_one"

    finally:
        temp_path.unlink(missing_ok=True)


def test_bsl_order_items_date_extraction():
    """Test that date extraction dimensions roundtrip correctly."""
    adapter = BSLAdapter()
    graph = adapter.parse("tests/fixtures/bsl/order_items.yml")

    order_items = graph.models["order_items"]

    # Verify date extraction dimensions
    created_year = next(d for d in order_items.dimensions if d.name == "created_year")
    assert created_year.type == "categorical"
    assert "EXTRACT(YEAR FROM" in created_year.sql


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
