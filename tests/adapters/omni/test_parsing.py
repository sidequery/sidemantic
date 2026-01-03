"""Tests for Omni adapter - parsing."""

import tempfile
from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.omni import OmniAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph


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


# =============================================================================
# DIMENSION TYPE MAPPING TESTS
# =============================================================================


def test_omni_dimension_type_string():
    """Test Omni string dimension type maps to categorical."""
    view_def = {
        "name": "test",
        "table_name": "test_table",
        "dimensions": {"status": {"type": "string", "sql": "${TABLE}.status"}},
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(view_def, f)
        temp_path = Path(f.name)

    try:
        adapter = OmniAdapter()
        graph = adapter.parse(temp_path)

        dim = graph.models["test"].get_dimension("status")
        assert dim.type == "categorical"
    finally:
        temp_path.unlink()


def test_omni_dimension_type_number():
    """Test Omni number dimension type maps to numeric."""
    view_def = {
        "name": "test",
        "table_name": "test_table",
        "dimensions": {"amount": {"type": "number", "sql": "${TABLE}.amount"}},
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(view_def, f)
        temp_path = Path(f.name)

    try:
        adapter = OmniAdapter()
        graph = adapter.parse(temp_path)

        dim = graph.models["test"].get_dimension("amount")
        assert dim.type == "numeric"
    finally:
        temp_path.unlink()


def test_omni_dimension_type_timestamp():
    """Test Omni timestamp dimension type maps to time."""
    view_def = {
        "name": "test",
        "table_name": "test_table",
        "dimensions": {
            "created_at": {"type": "timestamp", "sql": "${TABLE}.created_at", "timeframes": ["date", "week"]},
            "event_date": {"type": "date", "sql": "${TABLE}.event_date"},
        },
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(view_def, f)
        temp_path = Path(f.name)

    try:
        adapter = OmniAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["test"]

        created_at = model.get_dimension("created_at")
        assert created_at.type == "time"
        assert created_at.granularity == "day"  # First timeframe is "date" -> "day"

        event_date = model.get_dimension("event_date")
        assert event_date.type == "time"
    finally:
        temp_path.unlink()


def test_omni_dimension_type_yesno():
    """Test Omni yesno dimension type maps to boolean."""
    view_def = {
        "name": "test",
        "table_name": "test_table",
        "dimensions": {"is_active": {"type": "yesno", "sql": "${TABLE}.is_active"}},
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(view_def, f)
        temp_path = Path(f.name)

    try:
        adapter = OmniAdapter()
        graph = adapter.parse(temp_path)

        dim = graph.models["test"].get_dimension("is_active")
        assert dim.type == "boolean"
    finally:
        temp_path.unlink()


# =============================================================================
# MEASURE AGGREGATION TYPE TESTS
# =============================================================================


def test_omni_measure_aggregation_types():
    """Test all Omni aggregation types are properly mapped."""
    view_def = {
        "name": "test",
        "table_name": "test_table",
        "measures": {
            "total_count": {"aggregate_type": "count"},
            "unique_count": {"aggregate_type": "count_distinct", "sql": "${test.id}"},
            "total_sum": {"aggregate_type": "sum", "sql": "${test.amount}"},
            "total_avg": {"aggregate_type": "average", "sql": "${test.amount}"},
            "total_min": {"aggregate_type": "min", "sql": "${test.amount}"},
            "total_max": {"aggregate_type": "max", "sql": "${test.amount}"},
        },
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(view_def, f)
        temp_path = Path(f.name)

    try:
        adapter = OmniAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["test"]

        assert model.get_metric("total_count").agg == "count"
        assert model.get_metric("unique_count").agg == "count_distinct"
        assert model.get_metric("total_sum").agg == "sum"
        assert model.get_metric("total_avg").agg == "avg"
        assert model.get_metric("total_min").agg == "min"
        assert model.get_metric("total_max").agg == "max"
    finally:
        temp_path.unlink()


def test_omni_measure_sql_reference_cleanup():
    """Test that ${view.field} SQL references are cleaned up during parse."""
    view_def = {
        "name": "test",
        "table_name": "test_table",
        "measures": {
            "total": {"aggregate_type": "sum", "sql": "${test.amount}"},
        },
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(view_def, f)
        temp_path = Path(f.name)

    try:
        adapter = OmniAdapter()
        graph = adapter.parse(temp_path)

        metric = graph.models["test"].get_metric("total")
        assert metric.sql == "amount"  # ${test.amount} -> amount
    finally:
        temp_path.unlink()


# =============================================================================
# ERROR HANDLING TESTS
# =============================================================================


def test_omni_parse_empty_file():
    """Test parsing empty YAML file returns empty graph."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("")
        temp_path = Path(f.name)

    try:
        adapter = OmniAdapter()
        graph = adapter.parse(temp_path)
        assert len(graph.models) == 0
    finally:
        temp_path.unlink()


def test_omni_parse_model_file_skipped():
    """Test that model.yaml files are skipped for view parsing."""
    model_def = {
        "name": "ecommerce",
        "type": "model",
        "relationships": [],
    }

    # Create a temp directory with a model.yaml file
    with tempfile.TemporaryDirectory() as tmpdir:
        model_file = Path(tmpdir) / "model.yaml"
        with open(model_file, "w") as f:
            yaml.dump(model_def, f)

        adapter = OmniAdapter()
        graph = adapter.parse(tmpdir)
        # Model file should not create any models
        assert len(graph.models) == 0


def test_omni_parse_nonexistent_file():
    """Test parsing nonexistent file raises FileNotFoundError."""
    adapter = OmniAdapter()
    with pytest.raises(FileNotFoundError):
        adapter.parse("/nonexistent/path/file.yaml")


def test_omni_parse_empty_dimensions():
    """Test parsing view with empty dimensions section."""
    view_def = {"name": "test", "table_name": "test_table", "dimensions": None}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(view_def, f)
        temp_path = Path(f.name)

    try:
        adapter = OmniAdapter()
        graph = adapter.parse(temp_path)
        assert "test" in graph.models
        assert len(graph.models["test"].dimensions) == 0
    finally:
        temp_path.unlink()


def test_omni_parse_empty_measures():
    """Test parsing view with empty measures section."""
    view_def = {"name": "test", "table_name": "test_table", "measures": None}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(view_def, f)
        temp_path = Path(f.name)

    try:
        adapter = OmniAdapter()
        graph = adapter.parse(temp_path)
        assert "test" in graph.models
        assert len(graph.models["test"].metrics) == 0
    finally:
        temp_path.unlink()


def test_omni_dimension_with_null_def():
    """Test dimension with null definition uses defaults."""
    view_def = {
        "name": "test",
        "table_name": "test_table",
        "dimensions": {"status": None},  # Null definition
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(view_def, f)
        temp_path = Path(f.name)

    try:
        adapter = OmniAdapter()
        graph = adapter.parse(temp_path)
        assert "test" in graph.models
        dim = graph.models["test"].get_dimension("status")
        assert dim is not None
        assert dim.sql == "status"  # Defaults to dimension name
    finally:
        temp_path.unlink()


def test_omni_measure_with_null_def():
    """Test measure with null definition uses defaults."""
    view_def = {
        "name": "test",
        "table_name": "test_table",
        "measures": {"count": None},  # Null definition
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(view_def, f)
        temp_path = Path(f.name)

    try:
        adapter = OmniAdapter()
        graph = adapter.parse(temp_path)
        assert "test" in graph.models
        metric = graph.models["test"].get_metric("count")
        assert metric is not None
    finally:
        temp_path.unlink()


# =============================================================================
# EXPORT TESTS
# =============================================================================


def test_omni_export_simple_model():
    """Test exporting a simple model to Omni format."""
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

    adapter = OmniAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        # Verify views directory created
        views_dir = Path(tmpdir) / "views"
        assert views_dir.exists()

        # Read back and verify
        view_file = views_dir / "test_model.yaml"
        assert view_file.exists()

        with open(view_file) as f:
            data = yaml.safe_load(f)

        assert data["name"] == "test_model"
        assert data["schema"] == "public"
        assert data["table_name"] == "test_table"
        assert len(data["dimensions"]) == 3
        assert len(data["measures"]) == 2

        # Verify dimension export
        assert "id" in data["dimensions"]
        assert data["dimensions"]["id"]["type"] == "number"
        assert data["dimensions"]["id"]["primary_key"] is True

        assert "status" in data["dimensions"]
        assert data["dimensions"]["status"]["type"] == "string"

        # Verify time dimension has timeframes
        assert "created_at" in data["dimensions"]
        assert data["dimensions"]["created_at"]["type"] == "timestamp"
        assert "timeframes" in data["dimensions"]["created_at"]


def test_omni_export_with_relationships():
    """Test exporting models with relationships creates model.yaml."""
    from sidemantic.core.relationship import Relationship

    model1 = Model(
        name="orders",
        table="orders",
        primary_key="id",
        relationships=[
            Relationship(name="customers", type="many_to_one", foreign_key="customer_id"),
        ],
    )
    model2 = Model(name="customers", table="customers", primary_key="id")

    graph = SemanticGraph()
    graph.add_model(model1)
    graph.add_model(model2)

    adapter = OmniAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        # Verify model.yaml with relationships was created
        model_file = Path(tmpdir) / "model.yaml"
        assert model_file.exists()

        with open(model_file) as f:
            data = yaml.safe_load(f)

        assert "relationships" in data
        assert len(data["relationships"]) >= 1

        rel = data["relationships"][0]
        assert rel["join_from_view"] == "orders"
        assert rel["join_to_view"] == "customers"
        assert rel["relationship_type"] == "many_to_one"


def test_omni_export_dimension_type_mapping():
    """Test dimension type mapping during export."""
    model = Model(
        name="test",
        table="test",
        dimensions=[
            Dimension(name="cat_dim", type="categorical", sql="cat_dim"),
            Dimension(name="num_dim", type="numeric", sql="num_dim"),
            Dimension(name="time_dim", type="time", sql="time_dim", granularity="month"),
            Dimension(name="bool_dim", type="boolean", sql="bool_dim"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)

    adapter = OmniAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        with open(Path(tmpdir) / "views" / "test.yaml") as f:
            data = yaml.safe_load(f)

        dims = data["dimensions"]

        assert dims["cat_dim"]["type"] == "string"
        assert dims["num_dim"]["type"] == "number"
        assert dims["time_dim"]["type"] == "timestamp"
        assert dims["bool_dim"]["type"] == "yesno"


def test_omni_export_measure_aggregation_mapping():
    """Test measure aggregation type mapping during export."""
    model = Model(
        name="test",
        table="test",
        metrics=[
            Metric(name="m_count", agg="count"),
            Metric(name="m_count_distinct", agg="count_distinct", sql="id"),
            Metric(name="m_sum", agg="sum", sql="amount"),
            Metric(name="m_avg", agg="avg", sql="amount"),
            Metric(name="m_min", agg="min", sql="amount"),
            Metric(name="m_max", agg="max", sql="amount"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)

    adapter = OmniAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        with open(Path(tmpdir) / "views" / "test.yaml") as f:
            data = yaml.safe_load(f)

        measures = data["measures"]

        assert measures["m_count"]["aggregate_type"] == "count"
        assert measures["m_count_distinct"]["aggregate_type"] == "count_distinct"
        assert measures["m_sum"]["aggregate_type"] == "sum"
        assert measures["m_avg"]["aggregate_type"] == "average"  # avg -> average
        assert measures["m_min"]["aggregate_type"] == "min"
        assert measures["m_max"]["aggregate_type"] == "max"


def test_omni_export_with_labels():
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

    adapter = OmniAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        with open(Path(tmpdir) / "views" / "test.yaml") as f:
            data = yaml.safe_load(f)

        assert data["dimensions"]["status"]["label"] == "Order Status"
        assert data["measures"]["revenue"]["label"] == "Total Revenue"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
