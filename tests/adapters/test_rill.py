"""Tests for Rill adapter - parsing, export, and roundtrip."""

import tempfile
from pathlib import Path

import pytest
import yaml

from sidemantic import Dimension, Metric, Model
from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.omni import OmniAdapter
from sidemantic.adapters.rill import RillAdapter
from sidemantic.adapters.superset import SupersetAdapter
from sidemantic.core.semantic_graph import SemanticGraph
from tests.adapters.helpers import (
    assert_dimension_equivalent,
    assert_graph_equivalent,
    assert_metric_equivalent,
)

# =============================================================================
# PARSING TESTS
# =============================================================================


def test_import_real_rill_example():
    """Test importing a real Rill metrics view YAML file."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/orders.yaml")

    # Verify models loaded
    assert "orders" in graph.models
    orders = graph.models["orders"]

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "status" in dim_names
    assert "customer_id" in dim_names
    assert "country" in dim_names
    assert "product_category" in dim_names

    # Verify measures
    measure_names = [m.name for m in orders.metrics]
    assert "total_orders" in measure_names
    assert "total_revenue" in measure_names
    assert "avg_order_value" in measure_names
    assert "completed_orders" in measure_names

    # Verify timeseries dimension was created
    # Should have timeseries as a time dimension
    time_dims = [d for d in orders.dimensions if d.type == "time"]
    assert len(time_dims) > 0


def test_import_rill_with_derived_measures():
    """Test importing Rill metrics view with derived measures."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/users.yaml")

    assert "users" in graph.models
    users = graph.models["users"]

    # Verify derived measures were detected
    derived_metrics = [m for m in users.metrics if m.type == "derived"]
    assert len(derived_metrics) == 2

    derived_names = [m.name for m in derived_metrics]
    assert "avg_revenue_per_user" in derived_names
    assert "activation_rate" in derived_names


def test_import_rill_with_table_reference():
    """Test importing Rill metrics view that references a table."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/sales.yaml")

    assert "sales" in graph.models
    sales = graph.models["sales"]

    # Verify table reference was captured
    assert sales.table == "public.sales"

    # Verify dimensions
    dim_names = [d.name for d in sales.dimensions]
    assert "store_id" in dim_names
    assert "product_id" in dim_names
    assert "sales_rep" in dim_names
    assert "region" in dim_names


def test_import_rill_with_window_function():
    """Test importing Rill metrics view with window function definition."""
    # Create a Rill YAML with window function
    rill_yaml = {
        "type": "metrics_view",
        "name": "bids",  # Explicit name for the metrics view
        "model": "bids_model",
        "timeseries": "__time",
        "smallest_time_grain": "hour",
        "dimensions": [
            {"name": "advertiser", "column": "advertiser_name"},
        ],
        "measures": [
            {"name": "total_bids", "expression": "SUM(bid_cnt)"},
            {
                "name": "bids_7day_rolling_avg",
                "expression": "AVG(total_bids)",
                "requires": ["total_bids"],
                "window": {
                    "order": "__time",
                    "frame": "RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW",
                },
            },
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(rill_yaml, f)
        temp_path = Path(f.name)

    try:
        adapter = RillAdapter()
        graph = adapter.parse(temp_path)

        assert "bids" in graph.models
        model = graph.models["bids"]

        # Verify window function metric was parsed
        metric_names = [m.name for m in model.metrics]
        assert "bids_7day_rolling_avg" in metric_names

        # Find the window function metric
        window_metric = next(m for m in model.metrics if m.name == "bids_7day_rolling_avg")

        # Verify it was parsed as cumulative with window fields
        assert window_metric.type == "cumulative"
        assert window_metric.agg == "avg"
        assert window_metric.window_order == "__time"
        assert window_metric.window_frame == "RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW"
    finally:
        temp_path.unlink()


def test_import_rill_format_preset():
    """Test importing Rill metrics view with format_preset mapping."""
    # Create a Rill YAML with format presets
    rill_yaml = {
        "type": "metrics_view",
        "name": "sales",
        "model": "sales_model",
        "timeseries": "sale_date",
        "dimensions": [
            {"name": "region", "column": "region"},
        ],
        "measures": [
            {"name": "revenue", "expression": "SUM(amount)", "format_preset": "currency_usd"},
            {"name": "order_count", "expression": "COUNT(*)", "format_preset": "humanize"},
            {"name": "conversion_rate", "expression": "SUM(conversions)/SUM(visits)", "format_preset": "percentage"},
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(rill_yaml, f)
        temp_path = Path(f.name)

    try:
        adapter = RillAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["sales"]

        # Check format mapping
        revenue = next(m for m in model.metrics if m.name == "revenue")
        assert revenue.value_format_name == "usd"

        order_count = next(m for m in model.metrics if m.name == "order_count")
        assert order_count.value_format_name == "decimal_0"

        conversion_rate = next(m for m in model.metrics if m.name == "conversion_rate")
        assert conversion_rate.value_format_name == "percent"

        # Check default_time_dimension was set
        assert model.default_time_dimension == "sale_date"
    finally:
        temp_path.unlink()


def test_export_rill_with_window_function():
    """Test exporting Sidemantic model with window function to Rill format."""
    # Create model with window function metric
    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        default_time_dimension="order_date",
        default_grain="day",
        dimensions=[
            Dimension(name="order_date", type="time", sql="order_date", granularity="day"),
            Dimension(name="status", type="categorical", sql="status"),
        ],
        metrics=[
            Metric(name="daily_revenue", agg="sum", sql="amount"),
            Metric(
                name="rolling_avg_revenue",
                type="cumulative",
                agg="avg",
                sql="daily_revenue",
                window_order="order_date",
                window_frame="RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW",
            ),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter = RillAdapter()
        adapter.export(graph, tmpdir)

        # Read exported file
        output_file = Path(tmpdir) / "orders.yaml"
        with open(output_file) as f:
            exported = yaml.safe_load(f)

        # Verify window function was exported
        measures = exported.get("measures", [])
        rolling_measure = next((m for m in measures if m["name"] == "rolling_avg_revenue"), None)

        assert rolling_measure is not None
        assert "window" in rolling_measure
        assert rolling_measure["window"]["order"] == "order_date"
        assert rolling_measure["window"]["frame"] == "RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW"


# =============================================================================
# ROUNDTRIP TESTS
# =============================================================================


def test_rill_to_sidemantic_to_rill_roundtrip():
    """Test Rill roundtrip conversion."""
    adapter = RillAdapter()

    # Import original
    graph1 = adapter.parse("tests/fixtures/rill/orders.yaml")

    # Export
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)

        # Import exported version
        graph2 = adapter.parse(output_path / "orders.yaml")

        # Verify semantic equivalence
        # NOTE: Rill doesn't have native relationships or segments
        assert_graph_equivalent(graph1, graph2, check_relationships=False, check_segments=False)


def test_roundtrip_real_rill_example():
    """Test Rill example roundtrip using actual example files."""
    adapter = RillAdapter()

    # Import original
    graph1 = adapter.parse("tests/fixtures/rill/orders.yaml")

    # Export
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)

        # Import exported version
        graph2 = adapter.parse(output_path / "orders.yaml")

        # Verify semantic equivalence
        assert_graph_equivalent(graph1, graph2, check_relationships=False, check_segments=False)


def test_rill_roundtrip_dimension_properties():
    """Test that dimension properties survive Rill roundtrip."""
    adapter = RillAdapter()
    graph1 = adapter.parse("tests/fixtures/rill/orders.yaml")
    orders1 = graph1.models["orders"]

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)
        graph2 = adapter.parse(output_path / "orders.yaml")
        orders2 = graph2.models["orders"]

        for dim1 in orders1.dimensions:
            dim2 = orders2.get_dimension(dim1.name)
            assert dim2 is not None, f"Dimension {dim1.name} missing after roundtrip"
            assert_dimension_equivalent(dim1, dim2)


def test_rill_roundtrip_metric_properties():
    """Test that metric properties survive Rill roundtrip."""
    adapter = RillAdapter()
    graph1 = adapter.parse("tests/fixtures/rill/orders.yaml")
    orders1 = graph1.models["orders"]

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)
        graph2 = adapter.parse(output_path / "orders.yaml")
        orders2 = graph2.models["orders"]

        for m1 in orders1.metrics:
            m2 = orders2.get_metric(m1.name)
            assert m2 is not None, f"Metric {m1.name} missing after roundtrip"
            assert_metric_equivalent(m1, m2)


# =============================================================================
# CROSS-FORMAT CONVERSION TESTS
# =============================================================================


def test_rill_to_cube_conversion():
    """Test converting Rill format to Cube format."""
    # Import from Rill
    rill_adapter = RillAdapter()
    graph = rill_adapter.parse("tests/fixtures/rill/orders.yaml")

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
        assert "total_revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_to_rill_conversion():
    """Test converting Cube format to Rill format."""
    # Import from Cube
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("tests/fixtures/cube/orders.yml")

    # Export to Rill
    rill_adapter = RillAdapter()
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        rill_adapter.export(graph, output_path)

        # Re-import as Rill and verify structure
        graph2 = rill_adapter.parse(output_path / "orders.yaml")

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names


def test_rill_to_metricflow_conversion():
    """Test converting Rill format to MetricFlow format."""
    # Import from Rill
    rill_adapter = RillAdapter()
    graph = rill_adapter.parse("tests/fixtures/rill/orders.yaml")

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
        assert "total_revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_rill_to_lookml_conversion():
    """Test converting Rill format to LookML format."""
    # Import from Rill
    rill_adapter = RillAdapter()
    graph = rill_adapter.parse("tests/fixtures/rill/orders.yaml")

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
        assert "total_revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_superset_to_rill_conversion():
    """Test converting Superset dataset to Rill."""
    superset_adapter = SupersetAdapter()
    rill_adapter = RillAdapter()

    # Import from Superset
    graph = superset_adapter.parse("tests/fixtures/superset/orders.yaml")

    # Export to Rill
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        rill_adapter.export(graph, output_path)

        # Verify file created
        assert (output_path / "orders.yaml").exists()


def test_omni_to_rill_conversion():
    """Test converting Omni view to Rill."""
    omni_adapter = OmniAdapter()
    rill_adapter = RillAdapter()

    # Import from Omni
    graph = omni_adapter.parse("tests/fixtures/omni/")

    # Export to Rill
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        rill_adapter.export(graph, output_path)

        # Verify file created
        assert (output_path / "orders.yaml").exists()


# =============================================================================
# QUERY TESTS
# =============================================================================


def test_query_imported_rill_example():
    """Test that we can compile queries from imported Rill schema."""
    from sidemantic import SemanticLayer

    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/orders.yaml")

    layer = SemanticLayer()
    layer.graph = graph

    # Simple metric query
    sql = layer.compile(metrics=["orders.total_orders"])
    assert "COUNT" in sql.upper()

    # Query with dimension
    sql = layer.compile(metrics=["orders.total_revenue"], dimensions=["orders.status"])
    assert "GROUP BY" in sql.upper()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
