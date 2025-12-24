"""Tests for Rill adapter parsing."""

import tempfile
from pathlib import Path

import yaml

from sidemantic.adapters.rill import RillAdapter


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
    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

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
