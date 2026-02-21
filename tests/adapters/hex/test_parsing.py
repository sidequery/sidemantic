"""Tests for Hex adapter - parsing."""

import tempfile
from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.hex import HexAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph

# =============================================================================
# BASIC PARSING TESTS
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
# DIMENSION TYPE MAPPING TESTS
# =============================================================================


def test_hex_dimension_type_string():
    """Test Hex string dimension type maps to categorical."""
    hex_def = {
        "id": "test_model",
        "base_sql_table": "test_table",
        "dimensions": [{"id": "status", "type": "string"}],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)

    try:
        adapter = HexAdapter()
        graph = adapter.parse(temp_path)

        dim = graph.models["test_model"].get_dimension("status")
        assert dim.type == "categorical"
    finally:
        temp_path.unlink()


def test_hex_dimension_type_number():
    """Test Hex number dimension type maps to numeric."""
    hex_def = {
        "id": "test_model",
        "base_sql_table": "test_table",
        "dimensions": [{"id": "amount", "type": "number"}],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)

    try:
        adapter = HexAdapter()
        graph = adapter.parse(temp_path)

        dim = graph.models["test_model"].get_dimension("amount")
        assert dim.type == "numeric"
    finally:
        temp_path.unlink()


def test_hex_dimension_type_timestamp():
    """Test Hex timestamp dimension types map to time."""
    hex_def = {
        "id": "test_model",
        "base_sql_table": "test_table",
        "dimensions": [
            {"id": "created_at", "type": "timestamp_tz"},
            {"id": "updated_at", "type": "timestamp_naive"},
            {"id": "event_date", "type": "date"},
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)

    try:
        adapter = HexAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["test_model"]

        # All timestamp types should map to time
        created_at = model.get_dimension("created_at")
        assert created_at.type == "time"
        assert created_at.granularity == "hour"  # Default for timestamps

        updated_at = model.get_dimension("updated_at")
        assert updated_at.type == "time"

        event_date = model.get_dimension("event_date")
        assert event_date.type == "time"
        assert event_date.granularity == "day"  # Default for dates
    finally:
        temp_path.unlink()


def test_hex_dimension_type_boolean():
    """Test Hex boolean dimension type maps to categorical."""
    hex_def = {
        "id": "test_model",
        "base_sql_table": "test_table",
        "dimensions": [{"id": "is_active", "type": "boolean"}],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)

    try:
        adapter = HexAdapter()
        graph = adapter.parse(temp_path)

        dim = graph.models["test_model"].get_dimension("is_active")
        assert dim.type == "categorical"
    finally:
        temp_path.unlink()


# =============================================================================
# MEASURE AGGREGATION TYPE TESTS
# =============================================================================


def test_hex_measure_aggregation_types():
    """Test all Hex aggregation types are properly mapped."""
    hex_def = {
        "id": "test_model",
        "base_sql_table": "test_table",
        "dimensions": [{"id": "amount", "type": "number"}],
        "measures": [
            {"id": "total_count", "func": "count"},
            {"id": "unique_count", "func": "count_distinct", "of": "id"},
            {"id": "total_sum", "func": "sum", "of": "amount"},
            {"id": "total_avg", "func": "avg", "of": "amount"},
            {"id": "total_min", "func": "min", "of": "amount"},
            {"id": "total_max", "func": "max", "of": "amount"},
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)

    try:
        adapter = HexAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["test_model"]

        assert model.get_metric("total_count").agg == "count"
        assert model.get_metric("unique_count").agg == "count_distinct"
        assert model.get_metric("total_sum").agg == "sum"
        assert model.get_metric("total_avg").agg == "avg"
        assert model.get_metric("total_min").agg == "min"
        assert model.get_metric("total_max").agg == "max"
    finally:
        temp_path.unlink()


def test_hex_measure_with_custom_sql():
    """Test Hex measures with func_sql are parsed as derived."""
    hex_def = {
        "id": "test_model",
        "base_sql_table": "test_table",
        "measures": [
            {"id": "custom_calc", "func_sql": "SUM(a) / NULLIF(SUM(b), 0)"},
            {"id": "custom_calc2", "func_calc": "revenue / order_count"},
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)

    try:
        adapter = HexAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["test_model"]

        custom_calc = model.get_metric("custom_calc")
        assert custom_calc.type == "derived"
        assert "SUM" in custom_calc.sql

        custom_calc2 = model.get_metric("custom_calc2")
        assert custom_calc2.type == "derived"
    finally:
        temp_path.unlink()


# =============================================================================
# RELATIONSHIP PARSING TESTS
# =============================================================================


def test_hex_relationship_types():
    """Test Hex relation type mapping."""
    hex_def = {
        "id": "test_model",
        "base_sql_table": "test_table",
        "relations": [
            {"id": "parent", "type": "many_to_one", "join_sql": "parent_id = ${parent}.id"},
            {"id": "children", "type": "one_to_many", "join_sql": "id = ${children}.parent_id"},
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)

    try:
        adapter = HexAdapter()
        graph = adapter.parse(temp_path)

        model = graph.models["test_model"]

        parent_rel = next(r for r in model.relationships if r.name == "parent")
        assert parent_rel.type == "many_to_one"
        assert parent_rel.foreign_key == "parent_id"

        children_rel = next(r for r in model.relationships if r.name == "children")
        assert children_rel.type == "one_to_many"
    finally:
        temp_path.unlink()


# =============================================================================
# ERROR HANDLING TESTS
# =============================================================================


def test_hex_parse_empty_file():
    """Test parsing empty YAML file returns empty graph."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write("")
        temp_path = Path(f.name)

    try:
        adapter = HexAdapter()
        graph = adapter.parse(temp_path)
        assert len(graph.models) == 0
    finally:
        temp_path.unlink()


def test_hex_parse_file_without_id():
    """Test parsing file without id field is skipped."""
    hex_def = {"base_sql_table": "test_table", "dimensions": [{"id": "col", "type": "string"}]}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)

    try:
        adapter = HexAdapter()
        graph = adapter.parse(temp_path)
        assert len(graph.models) == 0
    finally:
        temp_path.unlink()


def test_hex_parse_nonexistent_file():
    """Test parsing nonexistent file raises FileNotFoundError."""
    adapter = HexAdapter()
    with pytest.raises(FileNotFoundError):
        adapter.parse("/nonexistent/path/file.yml")


def test_hex_parse_empty_dimensions():
    """Test parsing model with empty dimensions section."""
    hex_def = {"id": "test_model", "base_sql_table": "test_table", "dimensions": None}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)

    try:
        adapter = HexAdapter()
        graph = adapter.parse(temp_path)
        assert "test_model" in graph.models
        assert len(graph.models["test_model"].dimensions) == 0
    finally:
        temp_path.unlink()


def test_hex_parse_empty_measures():
    """Test parsing model with empty measures section."""
    hex_def = {"id": "test_model", "base_sql_table": "test_table", "measures": None}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)

    try:
        adapter = HexAdapter()
        graph = adapter.parse(temp_path)
        assert "test_model" in graph.models
        assert len(graph.models["test_model"].metrics) == 0
    finally:
        temp_path.unlink()


def test_hex_dimension_without_id():
    """Test dimension without id is skipped."""
    hex_def = {
        "id": "test_model",
        "base_sql_table": "test_table",
        "dimensions": [{"type": "string"}],  # Missing id
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)

    try:
        adapter = HexAdapter()
        graph = adapter.parse(temp_path)
        assert len(graph.models["test_model"].dimensions) == 0
    finally:
        temp_path.unlink()


def test_hex_measure_without_id():
    """Test measure without id is skipped."""
    hex_def = {
        "id": "test_model",
        "base_sql_table": "test_table",
        "measures": [{"func": "count"}],  # Missing id
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)

    try:
        adapter = HexAdapter()
        graph = adapter.parse(temp_path)
        assert len(graph.models["test_model"].metrics) == 0
    finally:
        temp_path.unlink()


# =============================================================================
# EXPORT TESTS
# =============================================================================


def test_hex_export_simple_model():
    """Test exporting a simple model to Hex format."""
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

    adapter = HexAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph, temp_path)

        # Read back and verify
        with open(temp_path) as f:
            data = yaml.safe_load(f)

        assert data["id"] == "test_model"
        assert data["base_sql_table"] == "public.test_table"
        assert len(data["dimensions"]) == 3
        assert len(data["measures"]) == 2

        # Verify dimension export
        id_dim = next(d for d in data["dimensions"] if d["id"] == "id")
        assert id_dim["type"] == "number"
        assert id_dim.get("unique") is True  # Primary key marked unique

        status_dim = next(d for d in data["dimensions"] if d["id"] == "status")
        assert status_dim["type"] == "string"

        created_dim = next(d for d in data["dimensions"] if d["id"] == "created_at")
        assert created_dim["type"] == "date"  # Day granularity maps to date
    finally:
        temp_path.unlink()


def test_hex_export_ratio_metric():
    """Test exporting ratio metrics to Hex format."""
    model = Model(
        name="test_model",
        table="test_table",
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="order_count", agg="count"),
            Metric(name="aov", type="ratio", numerator="revenue", denominator="order_count"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)

    adapter = HexAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph, temp_path)

        with open(temp_path) as f:
            data = yaml.safe_load(f)

        # Ratio metric should be exported as func_sql
        aov_measure = next(m for m in data["measures"] if m["id"] == "aov")
        assert "func_sql" in aov_measure
        assert "revenue" in aov_measure["func_sql"]
        assert "order_count" in aov_measure["func_sql"]
    finally:
        temp_path.unlink()


def test_hex_export_to_directory():
    """Test exporting multiple models to directory creates separate files."""
    model1 = Model(name="orders", table="orders", primary_key="id")
    model2 = Model(name="customers", table="customers", primary_key="id")

    graph = SemanticGraph()
    graph.add_model(model1)
    graph.add_model(model2)

    adapter = HexAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        assert (Path(tmpdir) / "orders.yml").exists()
        assert (Path(tmpdir) / "customers.yml").exists()


def test_hex_export_with_filters():
    """Test exporting measures with filters."""
    model = Model(
        name="test_model",
        table="test_table",
        metrics=[
            Metric(
                name="completed_count",
                agg="count",
                filters=["status = 'completed'"],
            ),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)

    adapter = HexAdapter()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph, temp_path)

        with open(temp_path) as f:
            data = yaml.safe_load(f)

        measure = data["measures"][0]
        assert "filters" in measure
        assert len(measure["filters"]) == 1
    finally:
        temp_path.unlink()


def test_hex_median_maps_to_median():
    """Median should map to agg='median', not fall through to default."""
    hex_def = {
        "id": "test_model",
        "base_sql_table": "test_table",
        "dimensions": [{"id": "val", "type": "number"}],
        "measures": [
            {"id": "med_val", "func": "median", "of": "val"},
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)

    try:
        adapter = HexAdapter()
        graph = adapter.parse(temp_path)
        model = graph.models["test_model"]
        assert model.get_metric("med_val").agg == "median"
    finally:
        temp_path.unlink()


def test_hex_statistical_funcs_map_to_native_agg():
    """stddev/variance functions should map to native agg types, not silently become count."""
    hex_def = {
        "id": "test_model",
        "base_sql_table": "test_table",
        "dimensions": [{"id": "val", "type": "number"}],
        "measures": [
            {"id": "std_val", "func": "stddev", "of": "val"},
            {"id": "std_pop_val", "func": "stddev_pop", "of": "val"},
            {"id": "var_val", "func": "variance", "of": "val"},
            {"id": "var_pop_val", "func": "variance_pop", "of": "val"},
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)

    try:
        adapter = HexAdapter()
        graph = adapter.parse(temp_path)
        model = graph.models["test_model"]

        assert model.get_metric("std_val").agg == "stddev"
        assert model.get_metric("std_pop_val").agg == "stddev_pop"
        assert model.get_metric("var_val").agg == "variance"
        assert model.get_metric("var_pop_val").agg == "variance_pop"

        # All should have the column reference as sql
        assert model.get_metric("std_val").sql == "val"
        assert model.get_metric("var_val").sql == "val"
    finally:
        temp_path.unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
