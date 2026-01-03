"""Tests for Rill adapter parsing."""

import tempfile
from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.rill import RillAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph


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


# =============================================================================
# ERROR HANDLING TESTS
# =============================================================================


def test_rill_parse_empty_file():
    """Test parsing empty YAML file returns empty graph."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("")
        temp_path = Path(f.name)

    try:
        adapter = RillAdapter()
        graph = adapter.parse(temp_path)
        assert len(graph.models) == 0
    finally:
        temp_path.unlink()


def test_rill_parse_non_metrics_view():
    """Test parsing file that is not a metrics_view type is skipped."""
    rill_yaml = {
        "type": "model",  # Not a metrics_view
        "name": "test",
        "model": "test_model",
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(rill_yaml, f)
        temp_path = Path(f.name)

    try:
        adapter = RillAdapter()
        graph = adapter.parse(temp_path)
        assert len(graph.models) == 0
    finally:
        temp_path.unlink()


def test_rill_parse_nonexistent_directory():
    """Test parsing nonexistent directory raises FileNotFoundError."""
    adapter = RillAdapter()
    with pytest.raises(FileNotFoundError, match="Path does not exist"):
        adapter.parse(Path("/nonexistent/path/"))


def test_rill_parse_nonexistent_file():
    """Test parsing nonexistent file raises FileNotFoundError."""
    adapter = RillAdapter()
    with pytest.raises(FileNotFoundError, match="Path does not exist"):
        adapter.parse(Path("/nonexistent/file.yaml"))


def test_rill_parse_empty_dimensions():
    """Test parsing metrics view with empty dimensions section."""
    rill_yaml = {
        "type": "metrics_view",
        "name": "test",
        "model": "test_model",
        "dimensions": None,
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(rill_yaml, f)
        temp_path = Path(f.name)

    try:
        adapter = RillAdapter()
        graph = adapter.parse(temp_path)
        assert "test" in graph.models
        assert len(graph.models["test"].dimensions) == 0
    finally:
        temp_path.unlink()


def test_rill_parse_empty_measures():
    """Test parsing metrics view with empty measures section."""
    rill_yaml = {
        "type": "metrics_view",
        "name": "test",
        "model": "test_model",
        "measures": None,
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(rill_yaml, f)
        temp_path = Path(f.name)

    try:
        adapter = RillAdapter()
        graph = adapter.parse(temp_path)
        assert "test" in graph.models
        assert len(graph.models["test"].metrics) == 0
    finally:
        temp_path.unlink()


def test_rill_dimension_without_name():
    """Test dimension without name is skipped."""
    rill_yaml = {
        "type": "metrics_view",
        "name": "test",
        "model": "test_model",
        "dimensions": [{"column": "status"}],  # Missing name
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(rill_yaml, f)
        temp_path = Path(f.name)

    try:
        adapter = RillAdapter()
        graph = adapter.parse(temp_path)
        assert len(graph.models["test"].dimensions) == 0
    finally:
        temp_path.unlink()


def test_rill_measure_without_name():
    """Test measure without name is skipped."""
    rill_yaml = {
        "type": "metrics_view",
        "name": "test",
        "model": "test_model",
        "measures": [{"expression": "COUNT(*)"}],  # Missing name
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(rill_yaml, f)
        temp_path = Path(f.name)

    try:
        adapter = RillAdapter()
        graph = adapter.parse(temp_path)
        assert len(graph.models["test"].metrics) == 0
    finally:
        temp_path.unlink()


def test_rill_measure_without_expression():
    """Test measure without expression is skipped."""
    rill_yaml = {
        "type": "metrics_view",
        "name": "test",
        "model": "test_model",
        "measures": [{"name": "count"}],  # Missing expression
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(rill_yaml, f)
        temp_path = Path(f.name)

    try:
        adapter = RillAdapter()
        graph = adapter.parse(temp_path)
        assert len(graph.models["test"].metrics) == 0
    finally:
        temp_path.unlink()


# =============================================================================
# TIME GRAIN MAPPING TESTS
# =============================================================================


def test_rill_time_grain_mapping():
    """Test Rill time grain to granularity mapping."""
    rill_yaml = {
        "type": "metrics_view",
        "name": "test",
        "model": "test_model",
        "timeseries": "event_time",
        "smallest_time_grain": "hour",
        "dimensions": [{"name": "event_time", "column": "event_time"}],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(rill_yaml, f)
        temp_path = Path(f.name)

    try:
        adapter = RillAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["test"]
        assert model.default_time_dimension == "event_time"
        assert model.default_grain == "hour"
    finally:
        temp_path.unlink()


def test_rill_time_grain_all_values():
    """Test all Rill time grain values map correctly."""
    grains = ["millisecond", "second", "minute", "hour", "day", "week", "month", "quarter", "year"]
    expected = ["hour", "hour", "hour", "hour", "day", "week", "month", "quarter", "year"]

    for grain, expected_granularity in zip(grains, expected):
        rill_yaml = {
            "type": "metrics_view",
            "name": "test",
            "model": "test_model",
            "timeseries": "ts",
            "smallest_time_grain": grain,
            "dimensions": [{"name": "ts", "column": "ts"}],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(rill_yaml, f)
            temp_path = Path(f.name)

        try:
            adapter = RillAdapter()
            graph = adapter.parse(temp_path)

            model = graph.models["test"]
            assert model.default_grain == expected_granularity, f"Failed for grain {grain}"
        finally:
            temp_path.unlink()


# =============================================================================
# AGGREGATION TYPE DETECTION TESTS
# =============================================================================


def test_rill_aggregation_detection():
    """Test that aggregation types are detected from expressions."""
    rill_yaml = {
        "type": "metrics_view",
        "name": "test",
        "model": "test_model",
        "measures": [
            {"name": "total_count", "expression": "COUNT(*)"},
            {"name": "total_revenue", "expression": "SUM(amount)"},
            {"name": "avg_value", "expression": "AVG(price)"},
            {"name": "min_price", "expression": "MIN(price)"},
            {"name": "max_price", "expression": "MAX(price)"},
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(rill_yaml, f)
        temp_path = Path(f.name)

    try:
        adapter = RillAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["test"]

        assert model.get_metric("total_count").agg == "count"
        assert model.get_metric("total_revenue").agg == "sum"
        assert model.get_metric("avg_value").agg == "avg"
        assert model.get_metric("min_price").agg == "min"
        assert model.get_metric("max_price").agg == "max"
    finally:
        temp_path.unlink()


def test_rill_aggregation_sql_extraction():
    """Test that inner SQL is extracted from aggregation expressions."""
    rill_yaml = {
        "type": "metrics_view",
        "name": "test",
        "model": "test_model",
        "measures": [
            {"name": "revenue", "expression": "SUM(order_total)"},
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(rill_yaml, f)
        temp_path = Path(f.name)

    try:
        adapter = RillAdapter()
        graph = adapter.parse(temp_path)

        metric = graph.models["test"].get_metric("revenue")
        assert metric.agg == "sum"
        assert metric.sql == "order_total"  # Extracted from SUM(order_total)
    finally:
        temp_path.unlink()


# =============================================================================
# EXPORT TESTS
# =============================================================================


def test_rill_export_simple_model():
    """Test exporting a simple model to Rill format."""
    model = Model(
        name="test_model",
        table="test_table",
        description="Test model",
        default_time_dimension="created_at",
        default_grain="day",
        dimensions=[
            Dimension(name="id", type="numeric", sql="id"),
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="created_at", type="time", sql="created_at", granularity="day"),
        ],
        metrics=[
            Metric(name="count", agg="count"),
            Metric(name="total", agg="sum", sql="amount"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)

    adapter = RillAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        # Read back and verify
        output_file = Path(tmpdir) / "test_model.yaml"
        assert output_file.exists()

        with open(output_file) as f:
            data = yaml.safe_load(f)

        assert data["type"] == "metrics_view"
        assert data["model"] == "test_table"  # Simple table name uses model
        assert data["timeseries"] == "created_at"
        assert data["smallest_time_grain"] == "day"
        assert len(data["dimensions"]) == 3
        assert len(data["measures"]) == 2


def test_rill_export_metric_expression():
    """Test exporting metrics generates proper expression strings."""
    model = Model(
        name="test_model",
        table="test_table",
        metrics=[
            Metric(name="count", agg="count"),
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="avg_value", agg="avg", sql="value"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)

    adapter = RillAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        with open(Path(tmpdir) / "test_model.yaml") as f:
            data = yaml.safe_load(f)

        measures = {m["name"]: m for m in data["measures"]}

        assert measures["count"]["expression"] == "COUNT(*)"
        assert measures["revenue"]["expression"] == "SUM(amount)"
        assert measures["avg_value"]["expression"] == "AVG(value)"


def test_rill_export_derived_metric():
    """Test exporting derived metrics preserves SQL expression."""
    model = Model(
        name="test_model",
        table="test_table",
        metrics=[
            Metric(name="custom_calc", type="derived", sql="SUM(a) / NULLIF(SUM(b), 0)"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)

    adapter = RillAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        with open(Path(tmpdir) / "test_model.yaml") as f:
            data = yaml.safe_load(f)

        measure = data["measures"][0]
        assert measure["expression"] == "SUM(a) / NULLIF(SUM(b), 0)"
        assert measure["type"] == "derived"


def test_rill_export_with_labels():
    """Test exporting dimensions and metrics with labels."""
    model = Model(
        name="test",
        table="test",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status", label="Order Status"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount", label="Total Revenue"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)

    adapter = RillAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        with open(Path(tmpdir) / "test.yaml") as f:
            data = yaml.safe_load(f)

        status_dim = next(d for d in data["dimensions"] if d["name"] == "status")
        assert status_dim["display_name"] == "Order Status"

        revenue_measure = next(m for m in data["measures"] if m["name"] == "revenue")
        assert revenue_measure["display_name"] == "Total Revenue"


def test_rill_export_format_preset():
    """Test exporting metrics with value_format_name to format_preset."""
    model = Model(
        name="test",
        table="test",
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount", value_format_name="usd"),
            Metric(name="count", agg="count", value_format_name="decimal_0"),
            Metric(name="rate", agg="avg", sql="rate", value_format_name="percent"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)

    adapter = RillAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        with open(Path(tmpdir) / "test.yaml") as f:
            data = yaml.safe_load(f)

        measures = {m["name"]: m for m in data["measures"]}

        assert measures["revenue"]["format_preset"] == "currency_usd"
        assert measures["count"]["format_preset"] == "humanize"
        assert measures["rate"]["format_preset"] == "percentage"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
