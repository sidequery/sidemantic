"""Tests for Tableau adapter - parsing."""

import shutil
import zipfile
from pathlib import Path

import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.tableau import TableauAdapter
from sidemantic.loaders import load_from_directory

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "tableau"


@pytest.fixture
def adapter():
    return TableauAdapter()


# =============================================================================
# BASIC PARSING TESTS
# =============================================================================


def test_parse_single_table_datasource(adapter):
    """Parse orders.tds: dimensions, metrics, table name."""
    graph = adapter.parse(FIXTURES / "orders.tds")

    assert "orders" in graph.models
    model = graph.models["orders"]
    assert model.table == "public.orders"

    # Dimensions
    id_dim = model.get_dimension("id")
    assert id_dim is not None
    assert id_dim.type == "numeric"
    assert id_dim.label == "Order ID"

    order_date = model.get_dimension("order_date")
    assert order_date is not None
    assert order_date.type == "time"
    assert order_date.granularity == "day"

    # Metrics
    amount = model.get_metric("amount")
    assert amount is not None
    assert amount.agg == "sum"

    order_count = model.get_metric("order_count")
    assert order_count is not None
    assert order_count.agg == "count"


def test_parse_calculated_fields(adapter):
    """Parse sales_calcs.tds: formula translation."""
    graph = adapter.parse(FIXTURES / "sales_calcs.tds")

    assert "sales_calcs" in graph.models
    model = graph.models["sales_calcs"]

    # calc_revenue: [price] * [quantity] -> price * quantity
    revenue = model.get_metric("calc_revenue")
    assert revenue is not None
    assert revenue.agg == "sum"
    assert "price" in revenue.sql
    assert "quantity" in revenue.sql

    # calc_safe_discount: ZN([discount]) -> COALESCE(discount, 0)
    discount = model.get_metric("calc_safe_discount")
    assert discount is not None
    assert "COALESCE" in discount.sql


def test_parse_drill_path_hierarchy(adapter):
    """Parse sales_calcs.tds: drill-path creates parent chain."""
    graph = adapter.parse(FIXTURES / "sales_calcs.tds")
    model = graph.models["sales_calcs"]

    country = model.get_dimension("country")
    state = model.get_dimension("state")
    city = model.get_dimension("city")

    assert country is not None
    assert state is not None
    assert city is not None

    assert country.parent is None
    assert state.parent == "country"
    assert city.parent == "state"


def test_parse_workbook(adapter):
    """Parse embedded.twb: extracts embedded datasource."""
    graph = adapter.parse(FIXTURES / "embedded.twb")

    assert "orders" in graph.models
    model = graph.models["orders"]
    assert model.table == "public.orders"

    amount = model.get_metric("amount")
    assert amount is not None
    assert amount.agg == "sum"


def test_parse_tdsx_zip(adapter, tmp_path):
    """Parse packaged .tdsx: unzips and parses inner .tds."""
    # Create a .tdsx from orders.tds
    tdsx_path = tmp_path / "orders.tdsx"
    with zipfile.ZipFile(tdsx_path, "w") as zf:
        zf.write(FIXTURES / "orders.tds", "orders.tds")

    graph = adapter.parse(tdsx_path)
    assert "orders" in graph.models
    assert graph.models["orders"].table == "public.orders"


def test_parse_twbx_zip(adapter, tmp_path):
    """Parse packaged .twbx: unzips and parses inner .twb."""
    twbx_path = tmp_path / "embedded.twbx"
    with zipfile.ZipFile(twbx_path, "w") as zf:
        zf.write(FIXTURES / "embedded.twb", "embedded.twb")

    graph = adapter.parse(twbx_path)
    assert "orders" in graph.models


# =============================================================================
# TYPE MAPPING TESTS
# =============================================================================


def test_type_mapping_all_types(adapter):
    """Kitchen sink fixture covers all Tableau datatypes."""
    graph = adapter.parse(FIXTURES / "kitchen_sink.tds")
    model = graph.models["kitchen_sink"]

    # integer -> numeric
    assert model.get_dimension("id").type == "numeric"

    # string -> categorical
    assert model.get_dimension("category").type == "categorical"

    # date -> time with granularity=day
    event_date = model.get_dimension("event_date")
    assert event_date.type == "time"
    assert event_date.granularity == "day"

    # datetime -> time with granularity=hour
    created_at = model.get_dimension("created_at")
    assert created_at.type == "time"
    assert created_at.granularity == "hour"

    # boolean -> boolean
    assert model.get_dimension("is_active").type == "boolean"

    # real -> numeric
    assert model.get_dimension("score").type == "numeric"


# =============================================================================
# AGGREGATION MAPPING TESTS
# =============================================================================


def test_aggregation_mapping_all_aggs(adapter):
    """Kitchen sink fixture covers all aggregation types."""
    graph = adapter.parse(FIXTURES / "kitchen_sink.tds")
    model = graph.models["kitchen_sink"]

    assert model.get_metric("amount").agg == "sum"
    assert model.get_metric("avg_amount").agg == "avg"
    assert model.get_metric("event_count").agg == "count"
    assert model.get_metric("unique_users").agg == "count_distinct"
    assert model.get_metric("min_amount").agg == "min"
    assert model.get_metric("max_amount").agg == "max"
    assert model.get_metric("median_amount").agg == "median"

    # attr -> derived (no sidemantic equivalent)
    attr_metric = model.get_metric("attr_amount")
    assert attr_metric is not None
    assert attr_metric.type == "derived"


# =============================================================================
# LOD AND SPECIAL FORMULA TESTS
# =============================================================================


def test_lod_expression_preserved(adapter):
    """LOD expressions are not translated; raw formula stored in metadata, hidden from queries."""
    graph = adapter.parse(FIXTURES / "kitchen_sink.tds")
    model = graph.models["kitchen_sink"]

    lod = model.get_metric("calc_lod")
    assert lod is not None
    assert lod.type == "derived"
    assert lod.metadata is not None
    assert "tableau_formula" in lod.metadata
    assert "{FIXED" in lod.metadata["tableau_formula"]
    # Untranslatable formulas should be hidden and use safe SQL
    assert lod.public is False
    assert lod.sql == "NULL"


# =============================================================================
# HIDDEN FIELD TESTS
# =============================================================================


def test_hidden_fields(adapter):
    """Hidden columns become public=False."""
    graph = adapter.parse(FIXTURES / "sales_calcs.tds")
    model = graph.models["sales_calcs"]

    hidden_cost = model.get_metric("hidden_cost")
    assert hidden_cost is not None
    assert hidden_cost.public is False

    # Non-hidden fields should be public
    price = model.get_metric("price")
    assert price is not None
    assert price.public is True


def test_raw_dimension_names_with_special_chars_are_quoted(adapter):
    """Raw Tableau dimension names fall back to valid quoted SQL identifiers."""
    dimension = adapter._build_dimension("Country/Region", "string", None, None, False, None)
    assert dimension.sql == '"Country/Region"'


def test_raw_metric_names_with_special_chars_are_quoted(adapter):
    """Raw Tableau metric names fall back to valid quoted SQL identifiers."""
    metric = adapter._build_metric("Profit Ratio (%)", "sum", None, None, False, True, None, None)
    assert metric.sql == '"Profit Ratio (%)"'


# =============================================================================
# MULTI-TABLE JOIN TESTS
# =============================================================================


def test_multi_table_join(adapter):
    """Parse multi_join.tds: reconstructed SQL, relationship extracted."""
    graph = adapter.parse(FIXTURES / "multi_join.tds")

    assert "multi_join" in graph.models
    model = graph.models["multi_join"]

    # Should have SQL with JOIN
    assert model.sql is not None
    assert "JOIN" in model.sql

    # Should have a relationship
    assert len(model.relationships) >= 1
    rel = model.relationships[0]
    assert rel.name == "customers"
    assert rel.type == "many_to_one"


# =============================================================================
# GROUP SEGMENTS TESTS
# =============================================================================


def test_groups_as_segments(adapter):
    """Kitchen sink groups become segments."""
    graph = adapter.parse(FIXTURES / "kitchen_sink.tds")
    model = graph.models["kitchen_sink"]

    assert len(model.segments) >= 1
    seg = next((s for s in model.segments if s.name == "Category Group"), None)
    assert seg is not None
    assert "IN" in seg.sql
    assert "'Tech'" in seg.sql
    assert "'Science'" in seg.sql


# =============================================================================
# LOADER AUTO-DETECTION
# =============================================================================


def test_tableau_auto_detect_tds(tmp_path):
    """Test .tds auto-detection in loaders.py."""
    shutil.copy(FIXTURES / "orders.tds", tmp_path / "orders.tds")

    layer = SemanticLayer()
    load_from_directory(layer, str(tmp_path))

    assert "orders" in layer.graph.models


def test_tableau_auto_detect_twb(tmp_path):
    """Test .twb auto-detection in loaders.py."""
    shutil.copy(FIXTURES / "embedded.twb", tmp_path / "embedded.twb")

    layer = SemanticLayer()
    load_from_directory(layer, str(tmp_path))

    assert "orders" in layer.graph.models


# =============================================================================
# DIRECTORY PARSING
# =============================================================================


def test_parse_directory(adapter, tmp_path):
    """Parse a directory containing multiple .tds files."""
    shutil.copy(FIXTURES / "orders.tds", tmp_path / "orders.tds")
    shutil.copy(FIXTURES / "sales_calcs.tds", tmp_path / "sales_calcs.tds")

    graph = adapter.parse(tmp_path)
    assert "orders" in graph.models
    assert "sales_calcs" in graph.models


# =============================================================================
# EDGE CASES
# =============================================================================


def test_empty_datasource(adapter, tmp_path):
    """Empty datasource produces no models."""
    empty_tds = tmp_path / "empty.tds"
    empty_tds.write_text("<?xml version='1.0' encoding='utf-8' ?>\n<datasource version='18.1' />\n")
    graph = adapter.parse(empty_tds)
    assert len(graph.models) == 0
