"""Tests for Hex adapter - parsing, export, and roundtrip."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.hex import HexAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from tests.adapters.helpers import (
    assert_dimension_equivalent,
    assert_graph_equivalent,
    assert_metric_equivalent,
)

# =============================================================================
# PARSING TESTS
# =============================================================================


def test_import_real_hex_example():
    """Test importing real Hex semantic model files."""
    adapter = HexAdapter()
    graph = adapter.parse("tests/fixtures/hex/")

    # Verify models loaded
    assert "orders" in graph.models
    assert "users" in graph.models
    assert "organizations" in graph.models

    orders = graph.models["orders"]

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "id" in dim_names
    assert "customer_id" in dim_names
    assert "amount" in dim_names
    assert "status" in dim_names
    assert "is_completed" in dim_names

    # Verify primary key from unique dimension
    assert orders.primary_key == "id"

    # Verify measures
    measure_names = [m.name for m in orders.metrics]
    assert "order_count" in measure_names
    assert "revenue" in measure_names
    assert "avg_order_value" in measure_names
    assert "completed_revenue" in measure_names

    # Verify measure with filter
    completed_revenue = next(m for m in orders.metrics if m.name == "completed_revenue")
    assert completed_revenue.filters is not None
    assert len(completed_revenue.filters) > 0

    # Verify custom func_sql measure
    conversion_rate = next(m for m in orders.metrics if m.name == "conversion_rate")
    assert conversion_rate.type == "derived"

    # Verify relationships
    rel_names = [r.name for r in orders.relationships]
    assert "customers" in rel_names
    customers_rel = next(r for r in orders.relationships if r.name == "customers")
    assert customers_rel.type == "many_to_one"


def test_import_hex_with_relations():
    """Test that Hex relations are properly imported."""
    adapter = HexAdapter()
    graph = adapter.parse("tests/fixtures/hex/")

    users = graph.models["users"]
    orgs = graph.models["organizations"]

    # Verify many_to_one from users to organizations
    user_rels = [r.name for r in users.relationships]
    assert "organizations" in user_rels

    # Verify one_to_many from organizations to users
    org_rels = [r.name for r in orgs.relationships]
    assert "users" in org_rels
    users_rel = next(r for r in orgs.relationships if r.name == "users")
    assert users_rel.type == "one_to_many"


def test_import_hex_calculated_dimensions():
    """Test that Hex calculated dimensions (expr_sql) are imported."""
    adapter = HexAdapter()
    graph = adapter.parse("tests/fixtures/hex/users.yml")

    users = graph.models["users"]

    # Find the calculated dimension
    annual_price = next(d for d in users.dimensions if d.name == "annual_seat_price")
    assert annual_price.sql is not None
    assert "IF" in annual_price.sql


# =============================================================================
# ROUNDTRIP TESTS
# =============================================================================


def test_hex_to_sidemantic_to_hex_roundtrip():
    """Test that Hex -> Sidemantic -> Hex preserves structure."""
    # Import from Hex
    hex_adapter = HexAdapter()
    graph1 = hex_adapter.parse("tests/fixtures/hex/orders.yml")

    # Export back to Hex
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        hex_adapter.export(graph1, temp_path)

        # Re-import and verify
        graph2 = hex_adapter.parse(temp_path)

        # Verify semantic equivalence
        # NOTE: Hex doesn't have native relationships or segments
        assert_graph_equivalent(graph1, graph2, check_relationships=False, check_segments=False)

    finally:
        temp_path.unlink(missing_ok=True)


def test_roundtrip_real_hex_example():
    """Test Hex example roundtrip using actual example files."""
    adapter = HexAdapter()

    # Import original
    graph1 = adapter.parse("tests/fixtures/hex/orders.yml")

    # Export
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)

        # Import exported version
        graph2 = adapter.parse(temp_path)

        # Verify semantic equivalence
        assert_graph_equivalent(graph1, graph2, check_relationships=False, check_segments=False)

    finally:
        temp_path.unlink(missing_ok=True)


def test_hex_roundtrip_dimension_properties():
    """Test that dimension properties survive Hex roundtrip."""
    adapter = HexAdapter()
    graph1 = adapter.parse("tests/fixtures/hex/orders.yml")
    orders1 = graph1.models["orders"]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)
        graph2 = adapter.parse(temp_path)
        orders2 = graph2.models["orders"]

        for dim1 in orders1.dimensions:
            dim2 = orders2.get_dimension(dim1.name)
            assert dim2 is not None, f"Dimension {dim1.name} missing after roundtrip"
            assert_dimension_equivalent(dim1, dim2)

    finally:
        temp_path.unlink(missing_ok=True)


def test_hex_roundtrip_metric_properties():
    """Test that metric properties survive Hex roundtrip."""
    adapter = HexAdapter()
    graph1 = adapter.parse("tests/fixtures/hex/orders.yml")
    orders1 = graph1.models["orders"]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)
        graph2 = adapter.parse(temp_path)
        orders2 = graph2.models["orders"]

        for m1 in orders1.metrics:
            m2 = orders2.get_metric(m1.name)
            assert m2 is not None, f"Metric {m1.name} missing after roundtrip"
            assert_metric_equivalent(m1, m2)

    finally:
        temp_path.unlink(missing_ok=True)


# =============================================================================
# CROSS-FORMAT CONVERSION TESTS
# =============================================================================


def test_hex_to_cube_conversion():
    """Test converting Hex format to Cube format."""
    # Import from Hex
    hex_adapter = HexAdapter()
    graph = hex_adapter.parse("tests/fixtures/hex/orders.yml")

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


def test_cube_to_hex_conversion():
    """Test converting Cube format to Hex format."""
    # Import from Cube
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("tests/fixtures/cube/orders.yml")

    # Export to Hex
    hex_adapter = HexAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        hex_adapter.export(graph, temp_path)

        # Re-import as Hex and verify structure
        graph2 = hex_adapter.parse(temp_path)

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


def test_hex_to_metricflow_conversion():
    """Test converting Hex format to MetricFlow format."""
    # Import from Hex
    hex_adapter = HexAdapter()
    graph = hex_adapter.parse("tests/fixtures/hex/orders.yml")

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


def test_hex_to_lookml_conversion():
    """Test converting Hex format to LookML format."""
    # Import from Hex
    hex_adapter = HexAdapter()
    graph = hex_adapter.parse("tests/fixtures/hex/orders.yml")

    # Export to LookML
    lookml_adapter = LookMLAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        lookml_adapter.export(graph, temp_path)

        # Re-import as LookML and verify structure
        graph2 = lookml_adapter.parse(temp_path)

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


# =============================================================================
# QUERY TESTS
# =============================================================================


def test_query_imported_hex_example():
    """Test that we can compile queries from imported Hex schema."""
    from sidemantic import SemanticLayer

    adapter = HexAdapter()
    graph = adapter.parse("tests/fixtures/hex/orders.yml")

    layer = SemanticLayer()
    layer.graph = graph

    # Test basic metric query
    sql = layer.compile(metrics=["orders.revenue"])
    assert "SUM" in sql.upper()

    # Test with dimension
    sql = layer.compile(metrics=["orders.revenue", "orders.order_count"], dimensions=["orders.status"])
    assert "GROUP BY" in sql.upper()
    assert "status" in sql.lower()

    # Test with filter
    sql = layer.compile(metrics=["orders.revenue"], filters=["orders.status = 'completed'"])
    assert "WHERE" in sql.upper()
    assert "completed" in sql.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
