"""Tests for Omni adapter parsing."""

import pytest

from sidemantic.adapters.omni import OmniAdapter


def test_import_real_omni_example():
    """Test importing real Omni view files."""
    adapter = OmniAdapter()
    graph = adapter.parse("tests/fixtures/omni/")

    # Verify models loaded
    assert "orders" in graph.models
    assert "customers" in graph.models

    # Verify orders view
    orders = graph.models["orders"]
    assert orders.table == "public.orders"
    assert orders.description == "Customer order transactions"

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "id" in dim_names
    assert "customer_id" in dim_names
    assert "created_at" in dim_names
    assert "status" in dim_names
    assert "amount" in dim_names

    # Verify time dimension
    created_at = next(d for d in orders.dimensions if d.name == "created_at")
    assert created_at.type == "time"
    assert created_at.label == "Order Date"

    # Verify measures
    metric_names = [m.name for m in orders.metrics]
    assert "count" in metric_names
    assert "total_revenue" in metric_names
    assert "avg_order_value" in metric_names
    assert "completed_revenue" in metric_names

    # Verify measure with filter
    completed_revenue = next(m for m in orders.metrics if m.name == "completed_revenue")
    assert completed_revenue.filters is not None
    assert len(completed_revenue.filters) > 0

    # Verify relationships
    rel_names = [r.name for r in orders.relationships]
    assert "customers" in rel_names
    customers_rel = next(r for r in orders.relationships if r.name == "customers")
    assert customers_rel.type == "many_to_one"


def test_import_omni_with_timeframes():
    """Test that Omni timeframes are properly imported."""
    adapter = OmniAdapter()
    graph = adapter.parse("tests/fixtures/omni/views/orders.yaml")

    orders = graph.models["orders"]

    # Verify time dimension has granularity from timeframes
    created_at = next(d for d in orders.dimensions if d.name == "created_at")
    assert created_at.type == "time"
    assert created_at.granularity is not None


def test_import_omni_model_relationships():
    """Test that Omni model.yaml files with relationships are properly imported."""
    adapter = OmniAdapter()
    graph = adapter.parse("tests/fixtures/omni/")

    # Verify models loaded
    assert "orders" in graph.models
    assert "customers" in graph.models

    # Verify relationships from model.yaml were parsed
    orders = graph.models["orders"]
    rel_names = [r.name for r in orders.relationships]
    assert "customers" in rel_names

    # Check relationship properties
    customers_rel = next(r for r in orders.relationships if r.name == "customers")
    assert customers_rel.type == "many_to_one"
    assert customers_rel.foreign_key == "customer_id"


def test_omni_time_comparison_import():
    """Test importing Omni time comparison measures (date_offset_from_query)."""
    import tempfile
    from pathlib import Path

    import yaml

    # Create a test Omni view with time comparison measure
    view_def = {
        "name": "sales",
        "schema": "public",
        "table_name": "sales",
        "dimensions": {
            "revenue": {"type": "number", "sql": "${TABLE}.revenue"},
            "created_at": {"type": "timestamp", "sql": "${TABLE}.created_at", "timeframes": ["date"]},
        },
        "measures": {
            "total_revenue": {"aggregate_type": "sum", "sql": "${sales.revenue}"},
            "revenue_yoy": {
                "aggregate_type": "sum",
                "sql": "${sales.revenue}",
                "filters": {"created_at": {"date_offset_from_query": "1 year", "cancel_query_filter": True}},
            },
            "revenue_mom": {
                "aggregate_type": "sum",
                "sql": "${sales.revenue}",
                "filters": {"created_at": {"date_offset_from_query": "1 month", "cancel_query_filter": True}},
            },
        },
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        views_dir = tmpdir_path / "views"
        views_dir.mkdir()

        view_file = views_dir / "sales.yaml"
        with open(view_file, "w") as f:
            yaml.dump(view_def, f)

        # Import
        adapter = OmniAdapter()
        graph = adapter.parse(tmpdir_path)

        # Verify sales model
        assert "sales" in graph.models
        sales = graph.models["sales"]

        # Verify time comparison metrics were imported
        metric_names = [m.name for m in sales.metrics]
        assert "revenue_yoy" in metric_names
        assert "revenue_mom" in metric_names

        # Check revenue_yoy properties
        revenue_yoy = next(m for m in sales.metrics if m.name == "revenue_yoy")
        assert revenue_yoy.type == "time_comparison"
        assert revenue_yoy.comparison_type == "yoy"
        assert revenue_yoy.time_offset == "1 year"
        assert revenue_yoy.calculation == "difference"

        # Check revenue_mom properties
        revenue_mom = next(m for m in sales.metrics if m.name == "revenue_mom")
        assert revenue_mom.type == "time_comparison"
        assert revenue_mom.comparison_type == "mom"
        assert revenue_mom.time_offset == "1 month"


def test_omni_time_comparison_export():
    """Test exporting time_comparison metrics to Omni format."""
    import tempfile
    from pathlib import Path

    import yaml

    from sidemantic.core.dimension import Dimension
    from sidemantic.core.metric import Metric
    from sidemantic.core.model import Model
    from sidemantic.core.semantic_graph import SemanticGraph

    # Create a model with time_comparison metric
    sales = Model(
        name="sales",
        table="public.sales",
        dimensions=[
            Dimension(name="revenue", sql="revenue", type="numeric"),
            Dimension(name="created_at", sql="created_at", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="total_revenue", agg="sum", sql="revenue"),
            Metric(
                name="revenue_yoy",
                type="time_comparison",
                base_metric="sales.total_revenue",
                comparison_type="yoy",
                calculation="percent_change",
            ),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(sales)

    # Export
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        adapter = OmniAdapter()
        adapter.export(graph, tmpdir_path)

        # Read exported file
        view_file = tmpdir_path / "views" / "sales.yaml"
        assert view_file.exists()

        with open(view_file) as f:
            exported = yaml.safe_load(f)

        # Verify time comparison measure was exported
        assert "measures" in exported
        assert "revenue_yoy" in exported["measures"]

        revenue_yoy = exported["measures"]["revenue_yoy"]

        # Should have filters with date_offset_from_query
        assert "filters" in revenue_yoy
        filters = revenue_yoy["filters"]

        # Should have at least one time field with offset
        has_offset = False
        for field, conditions in filters.items():
            if isinstance(conditions, dict):
                if "date_offset_from_query" in conditions:
                    assert conditions["date_offset_from_query"] == "1 year"
                    assert conditions.get("cancel_query_filter") is True
                    has_offset = True

        assert has_offset, "Expected date_offset_from_query filter in exported measure"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
