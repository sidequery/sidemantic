"""Tests for Superset adapter - parsing."""

import tempfile
from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.superset import SupersetAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph

# =============================================================================
# BASIC PARSING TESTS
# =============================================================================


def test_import_real_superset_example():
    """Test importing real Superset dataset files."""
    adapter = SupersetAdapter()
    graph = adapter.parse("tests/fixtures/superset/")

    # Verify models loaded
    assert "orders" in graph.models
    assert "sales_summary" in graph.models

    # Verify orders dataset
    orders = graph.models["orders"]
    assert orders.table == "public.orders"
    assert orders.description == "Customer orders dataset"

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "id" in dim_names
    assert "customer_id" in dim_names
    assert "created_at" in dim_names
    assert "status" in dim_names
    assert "amount" in dim_names

    # Verify main datetime column
    created_at = next(d for d in orders.dimensions if d.name == "created_at")
    assert created_at.type == "time"
    assert created_at.label == "Order Date"

    # Verify metrics
    metric_names = [m.name for m in orders.metrics]
    assert "count" in metric_names
    assert "total_revenue" in metric_names
    assert "avg_order_value" in metric_names

    # Verify metric types
    count_metric = next(m for m in orders.metrics if m.name == "count")
    assert count_metric.agg == "count"

    revenue_metric = next(m for m in orders.metrics if m.name == "total_revenue")
    assert revenue_metric.agg == "sum"
    assert revenue_metric.label == "Total Revenue"


def test_import_superset_virtual_dataset():
    """Test that Superset virtual datasets (SQL-based) are imported."""
    adapter = SupersetAdapter()
    graph = adapter.parse("tests/fixtures/superset/sales_summary.yaml")

    sales = graph.models["sales_summary"]

    # Verify it has SQL (virtual dataset)
    assert sales.sql is not None
    assert "SELECT" in sales.sql
    assert sales.table is None  # Virtual datasets don't have table

    # Verify derived metric without aggregation type
    revenue_per_order = next(m for m in sales.metrics if m.name == "revenue_per_order")
    assert revenue_per_order.type == "derived"
    assert revenue_per_order.agg is None


# =============================================================================
# COLUMN TYPE MAPPING TESTS
# =============================================================================


def test_superset_column_type_varchar():
    """Test Superset VARCHAR column type maps to categorical."""
    superset_def = {
        "table_name": "test",
        "columns": [{"column_name": "status", "type": "VARCHAR"}],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)

        dim = graph.models["test"].get_dimension("status")
        assert dim.type == "categorical"
    finally:
        temp_path.unlink()


def test_superset_column_type_numeric():
    """Test Superset NUMERIC column type maps to numeric."""
    superset_def = {
        "table_name": "test",
        "columns": [
            {"column_name": "amount", "type": "NUMERIC"},
            {"column_name": "price", "type": "FLOAT"},
            {"column_name": "quantity", "type": "INT"},
            {"column_name": "total", "type": "DOUBLE"},
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["test"]
        assert model.get_dimension("amount").type == "numeric"
        assert model.get_dimension("price").type == "numeric"
        assert model.get_dimension("quantity").type == "numeric"
        assert model.get_dimension("total").type == "numeric"
    finally:
        temp_path.unlink()


def test_superset_column_type_datetime():
    """Test Superset datetime column types map to time."""
    superset_def = {
        "table_name": "test",
        "main_dttm_col": "event_date",
        "columns": [
            {"column_name": "event_date", "type": "DATE", "is_dttm": True},
            {"column_name": "created_at", "type": "TIMESTAMP", "is_dttm": True},
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["test"]

        event_date = model.get_dimension("event_date")
        assert event_date.type == "time"
        assert event_date.granularity == "day"  # DATE type

        created_at = model.get_dimension("created_at")
        assert created_at.type == "time"
        assert created_at.granularity == "hour"  # TIMESTAMP type
    finally:
        temp_path.unlink()


def test_superset_column_type_boolean():
    """Test Superset BOOLEAN column type."""
    superset_def = {
        "table_name": "test",
        "columns": [{"column_name": "is_active", "type": "BOOLEAN"}],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)

        dim = graph.models["test"].get_dimension("is_active")
        assert dim.type == "boolean"
    finally:
        temp_path.unlink()


# =============================================================================
# METRIC TYPE MAPPING TESTS
# =============================================================================


def test_superset_metric_aggregation_types():
    """Test all Superset aggregation types are properly mapped."""
    superset_def = {
        "table_name": "test",
        "metrics": [
            {"metric_name": "total_count", "metric_type": "count", "expression": "COUNT(*)"},
            {"metric_name": "unique_users", "metric_type": "count_distinct", "expression": "COUNT(DISTINCT user_id)"},
            {"metric_name": "total_sum", "metric_type": "sum", "expression": "SUM(amount)"},
            {"metric_name": "total_avg", "metric_type": "avg", "expression": "AVG(amount)"},
            {"metric_name": "total_min", "metric_type": "min", "expression": "MIN(amount)"},
            {"metric_name": "total_max", "metric_type": "max", "expression": "MAX(amount)"},
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["test"]

        assert model.get_metric("total_count").agg == "count"
        assert model.get_metric("unique_users").agg == "count_distinct"
        assert model.get_metric("total_sum").agg == "sum"
        assert model.get_metric("total_avg").agg == "avg"
        assert model.get_metric("total_min").agg == "min"
        assert model.get_metric("total_max").agg == "max"
    finally:
        temp_path.unlink()


def test_superset_metric_sql_extraction():
    """Test that inner SQL is extracted from aggregation expressions."""
    superset_def = {
        "table_name": "test",
        "metrics": [
            {"metric_name": "revenue", "metric_type": "sum", "expression": "SUM(amount)"},
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)

        metric = graph.models["test"].get_metric("revenue")
        assert metric.agg == "sum"
        assert metric.sql == "amount"  # Should extract inner expression
    finally:
        temp_path.unlink()


def test_superset_metric_count_star():
    """Test COUNT(*) metric is handled correctly."""
    superset_def = {
        "table_name": "test",
        "metrics": [
            {"metric_name": "count", "metric_type": "count", "expression": "COUNT(*)"},
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)

        metric = graph.models["test"].get_metric("count")
        assert metric.agg == "count"
        assert metric.sql is None  # COUNT(*) has no inner column
    finally:
        temp_path.unlink()


def test_superset_derived_metric():
    """Test Superset metrics without metric_type are parsed as derived."""
    superset_def = {
        "table_name": "test",
        "metrics": [
            {"metric_name": "custom_calc", "metric_type": None, "expression": "SUM(a) / SUM(b)"},
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)

        metric = graph.models["test"].get_metric("custom_calc")
        assert metric.type == "derived"
        assert metric.agg is None
        assert "SUM" in metric.sql
    finally:
        temp_path.unlink()


# =============================================================================
# SCHEMA AND TABLE HANDLING TESTS
# =============================================================================


def test_superset_table_with_schema():
    """Test that schema and table_name are combined correctly."""
    superset_def = {
        "table_name": "orders",
        "schema": "public",
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["orders"]
        assert model.table == "public.orders"
    finally:
        temp_path.unlink()


def test_superset_table_without_schema():
    """Test table_name without schema."""
    superset_def = {
        "table_name": "orders",
        "schema": None,
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["orders"]
        assert model.table == "orders"
    finally:
        temp_path.unlink()


def test_superset_virtual_dataset_no_table():
    """Test virtual datasets have SQL but no table."""
    superset_def = {
        "table_name": "my_view",
        "schema": "analytics",
        "sql": "SELECT * FROM orders WHERE status = 'completed'",
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["my_view"]
        assert model.table is None  # Virtual datasets don't have table
        assert model.sql is not None
        assert "SELECT" in model.sql
    finally:
        temp_path.unlink()


# =============================================================================
# ERROR HANDLING TESTS
# =============================================================================


def test_superset_parse_empty_file():
    """Test parsing empty YAML file returns empty graph."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("")
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)
        assert len(graph.models) == 0
    finally:
        temp_path.unlink()


def test_superset_parse_file_without_table_name():
    """Test parsing file without table_name field is skipped."""
    superset_def = {"schema": "public", "columns": [{"column_name": "id", "type": "INT"}]}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)
        assert len(graph.models) == 0
    finally:
        temp_path.unlink()


def test_superset_parse_nonexistent_file():
    """Test parsing nonexistent file raises FileNotFoundError."""
    adapter = SupersetAdapter()
    with pytest.raises(FileNotFoundError):
        adapter.parse("/nonexistent/path/file.yaml")


def test_superset_parse_empty_columns():
    """Test parsing dataset with empty columns section."""
    superset_def = {"table_name": "test", "columns": None}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)
        assert "test" in graph.models
        assert len(graph.models["test"].dimensions) == 0
    finally:
        temp_path.unlink()


def test_superset_parse_empty_metrics():
    """Test parsing dataset with empty metrics section."""
    superset_def = {"table_name": "test", "metrics": None}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)
        assert "test" in graph.models
        assert len(graph.models["test"].metrics) == 0
    finally:
        temp_path.unlink()


def test_superset_column_without_name():
    """Test column without column_name is skipped."""
    superset_def = {
        "table_name": "test",
        "columns": [{"type": "VARCHAR"}],  # Missing column_name
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)
        assert len(graph.models["test"].dimensions) == 0
    finally:
        temp_path.unlink()


def test_superset_metric_without_name():
    """Test metric without metric_name is skipped."""
    superset_def = {
        "table_name": "test",
        "metrics": [{"metric_type": "count", "expression": "COUNT(*)"}],  # Missing metric_name
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(superset_def, f)
        temp_path = Path(f.name)

    try:
        adapter = SupersetAdapter()
        graph = adapter.parse(temp_path)
        assert len(graph.models["test"].metrics) == 0
    finally:
        temp_path.unlink()


# =============================================================================
# EXPORT TESTS
# =============================================================================


def test_superset_export_simple_model():
    """Test exporting a simple model to Superset format."""
    model = Model(
        name="test_model",
        table="public.test_table",
        description="Test model",
        primary_key="id",
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

    adapter = SupersetAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        # Read back and verify
        with open(Path(tmpdir) / "test_model.yaml") as f:
            data = yaml.safe_load(f)

        assert data["table_name"] == "test_table"  # Extracted from model.table
        assert data["schema"] == "public"
        assert data["main_dttm_col"] == "created_at"  # First time dimension
        assert len(data["columns"]) == 3
        assert len(data["metrics"]) == 2

        # Verify column export
        id_col = next(c for c in data["columns"] if c["column_name"] == "id")
        assert id_col["type"] == "NUMERIC"

        status_col = next(c for c in data["columns"] if c["column_name"] == "status")
        assert status_col["type"] == "VARCHAR"

        created_col = next(c for c in data["columns"] if c["column_name"] == "created_at")
        assert created_col["type"] == "TIMESTAMP WITHOUT TIME ZONE"
        assert created_col["is_dttm"] is True


def test_superset_export_metric_expression():
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

    adapter = SupersetAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        with open(Path(tmpdir) / "test_model.yaml") as f:
            data = yaml.safe_load(f)

        metrics = {m["metric_name"]: m for m in data["metrics"]}

        # COUNT(*) for count metric
        assert metrics["count"]["expression"] == "COUNT(*)"
        assert metrics["count"]["metric_type"] == "count"

        # SUM(column) for sum metric
        assert metrics["revenue"]["expression"] == "SUM(amount)"
        assert metrics["revenue"]["metric_type"] == "sum"

        # AVG(column) for avg metric
        assert metrics["avg_value"]["expression"] == "AVG(value)"
        assert metrics["avg_value"]["metric_type"] == "avg"


def test_superset_export_derived_metric():
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

    adapter = SupersetAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        with open(Path(tmpdir) / "test_model.yaml") as f:
            data = yaml.safe_load(f)

        metric = data["metrics"][0]
        assert metric["expression"] == "SUM(a) / NULLIF(SUM(b), 0)"
        assert metric["metric_type"] is None


def test_superset_export_to_single_file():
    """Test exporting to a single file."""
    model = Model(name="orders", table="orders", primary_key="id")

    graph = SemanticGraph()
    graph.add_model(model)

    adapter = SupersetAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph, temp_path)

        with open(temp_path) as f:
            data = yaml.safe_load(f)

        assert data["table_name"] == "orders"
    finally:
        temp_path.unlink()


def test_superset_export_multiple_models():
    """Test exporting multiple models to directory creates separate files."""
    model1 = Model(name="orders", table="orders", primary_key="id")
    model2 = Model(name="customers", table="customers", primary_key="id")

    graph = SemanticGraph()
    graph.add_model(model1)
    graph.add_model(model2)

    adapter = SupersetAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        assert (Path(tmpdir) / "orders.yaml").exists()
        assert (Path(tmpdir) / "customers.yaml").exists()


def test_superset_export_with_labels():
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

    adapter = SupersetAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        with open(Path(tmpdir) / "test.yaml") as f:
            data = yaml.safe_load(f)

        col = next(c for c in data["columns"] if c["column_name"] == "status")
        assert col["verbose_name"] == "Order Status"

        metric = next(m for m in data["metrics"] if m["metric_name"] == "revenue")
        assert metric["verbose_name"] == "Total Revenue"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
