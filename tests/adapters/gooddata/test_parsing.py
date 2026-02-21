"""Tests for GoodData adapter - parsing."""

import json
import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.gooddata import GoodDataAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph

# =============================================================================
# BASIC PARSING TESTS
# =============================================================================


def test_import_cloud_ldm_fixture():
    """Test importing GoodData cloud LDM JSON fixture."""
    adapter = GoodDataAdapter()
    graph = adapter.parse("tests/fixtures/gooddata/cloud_ldm.json")

    assert "orders" in graph.models
    assert "customers" in graph.models
    assert "date" in graph.models

    orders = graph.models["orders"]
    assert orders.table == "public.orders"
    assert orders.primary_key == "order_id"
    assert orders.metadata["gooddata"]["data_source_id"] == "demo"

    order_id = orders.get_dimension("order_id")
    assert order_id.type == "numeric"

    order_status = orders.get_dimension("order_status")
    assert order_status.type == "categorical"
    assert order_status.metadata["gooddata"]["labels"]

    order_date = orders.get_dimension("order_date")
    assert order_date.type == "time"
    assert order_date.granularity == "day"

    amount = orders.get_metric("amount")
    assert amount.agg == "sum"
    assert amount.sql == "amount"

    rel = next(r for r in orders.relationships if r.name == "customers")
    assert rel.type == "many_to_one"
    assert rel.foreign_key == "customer_id"
    assert rel.primary_key == "customer_id"

    date_rel = next(r for r in orders.relationships if r.name == "date")
    assert date_rel.type == "many_to_one"

    date_model = graph.models["date"]
    assert date_model.primary_key == "date"
    assert date_model.dimensions[0].supported_granularities == ["day", "month", "year"]


def test_import_legacy_project_model():
    """Test importing legacy GoodData projectModel JSON."""
    adapter = GoodDataAdapter()
    graph = adapter.parse("tests/fixtures/gooddata/legacy_project_model.json")

    assert "dataset.orders" in graph.models
    assert "date" in graph.models

    orders = graph.models["dataset.orders"]
    assert orders.primary_key == "order_id"

    anchor_dim = orders.get_dimension("attr.orders.id")
    assert anchor_dim is not None
    assert anchor_dim.sql == "order_id"

    rel = next(r for r in orders.relationships if r.name == "date")
    assert rel.type == "many_to_one"


def test_cloud_kitchen_sink_metadata_preserved():
    """Test GoodData cloud kitchen sink payload preserves metadata."""
    adapter = GoodDataAdapter()
    graph = adapter.parse("tests/fixtures/gooddata/cloud_kitchen_sink.json")

    orders = graph.models["orders"]
    gd_meta = orders.metadata["gooddata"]
    assert gd_meta["data_source_id"] == "demo"
    assert gd_meta["grain"] == ["order_id", "order_line_id"]
    assert gd_meta["extra"]["customField"]["owner"] == "analytics-team"

    status_dim = orders.get_dimension("order_status")
    assert status_dim.metadata["gooddata"]["labels"]

    discount_metric = orders.get_metric("discount_rate")
    assert discount_metric.agg == "avg"
    assert discount_metric.metadata["gooddata"]["extra"]["format"] == "0.0%"

    products_rel = next(r for r in orders.relationships if r.name == "products")
    assert products_rel.type == "many_to_many"
    assert products_rel.metadata["gooddata"]["multivalue"] is True

    date_model = graph.models["date"]
    assert date_model.dimensions[0].supported_granularities == ["day", "week", "month", "quarter", "year"]
    date_meta = date_model.metadata["gooddata"]
    assert date_meta["granularitiesFormatting"]["titleBase"] == "Date"
    assert date_meta["extra"]["calendarType"] == "GREGORIAN"


def test_legacy_kitchen_sink_metadata_preserved():
    """Test GoodData legacy kitchen sink payload preserves metadata."""
    adapter = GoodDataAdapter()
    graph = adapter.parse("tests/fixtures/gooddata/legacy_kitchen_sink.json")

    sales = graph.models["dataset.sales"]
    gd_meta = sales.metadata["gooddata"]
    assert gd_meta["extra"]["customField"]["owner"] == "legacy-team"

    region_dim = sales.get_dimension("attr.sales.region")
    assert region_dim.metadata["gooddata"]["labels"] == ["label.sales.region", "label.sales.region_code"]

    date_model = graph.models["date"]
    assert date_model.metadata["gooddata"]["extra"]["customField"]["calendarType"] == "GREGORIAN"


def test_gooddata_default_label_source_column():
    """Test that defaultView label drives dimension SQL."""
    ldm = {
        "ldm": {
            "datasets": [
                {
                    "id": "users",
                    "dataSourceTableId": "public.users",
                    "grain": [{"id": "user_id", "type": "attribute"}],
                    "attributes": [
                        {
                            "id": "user_id",
                            "title": "User Id",
                            "sourceColumn": "user_id",
                            "labels": [
                                {
                                    "id": "user_id_label",
                                    "title": "User Id",
                                    "sourceColumn": "user_id",
                                    "dataType": "NUMERIC",
                                },
                                {
                                    "id": "user_id_display",
                                    "title": "User Id Display",
                                    "sourceColumn": "user_id_display",
                                    "dataType": "TEXT",
                                },
                            ],
                            "defaultView": {"id": "user_id_display", "type": "label"},
                        }
                    ],
                }
            ]
        }
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(ldm, f)
        temp_path = Path(f.name)

    try:
        adapter = GoodDataAdapter()
        graph = adapter.parse(temp_path)

        dim = graph.models["users"].get_dimension("user_id")
        assert dim.sql == "user_id_display"
        assert dim.type == "categorical"
    finally:
        temp_path.unlink(missing_ok=True)


# =============================================================================
# FACTS AND AGGREGATION TESTS
# =============================================================================


def test_gooddata_fact_aggregation_override():
    """Test that explicit aggregation on facts is respected."""
    ldm = {
        "ldm": {
            "datasets": [
                {
                    "id": "events",
                    "dataSourceTableId": "events",
                    "facts": [
                        {
                            "id": "avg_latency",
                            "title": "Average Latency",
                            "sourceColumn": "latency_ms",
                            "dataType": "NUMERIC",
                            "aggregation": "avg",
                        }
                    ],
                }
            ]
        }
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(ldm, f)
        temp_path = Path(f.name)

    try:
        adapter = GoodDataAdapter()
        graph = adapter.parse(temp_path)

        metric = graph.models["events"].get_metric("avg_latency")
        assert metric.agg == "avg"
        assert metric.sql == "latency_ms"
    finally:
        temp_path.unlink(missing_ok=True)


# =============================================================================
# EXPORT TESTS
# =============================================================================


def test_gooddata_export_basic_model():
    """Test exporting a basic model to GoodData LDM JSON."""
    model = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="order_id", type="numeric", sql="order_id"),
            Dimension(name="status", type="categorical", sql="status", label="Status"),
        ],
        metrics=[
            Metric(name="amount", agg="sum", sql="amount", label="Amount"),
        ],
        relationships=[
            Relationship(name="customers", type="many_to_one", foreign_key="customer_id"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)

    adapter = GoodDataAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)

        export_path = Path(tmpdir) / "ldm.json"
        assert export_path.exists()

        with open(export_path) as f:
            data = json.load(f)

        datasets = data["ldm"]["datasets"]
        assert len(datasets) == 1

        dataset = datasets[0]
        assert dataset["id"] == "orders"
        assert dataset["dataSourceTableId"] == "public.orders"
        assert dataset["grain"][0]["id"] == "order_id"

        attributes = {attr["id"]: attr for attr in dataset["attributes"]}
        assert "status" in attributes
        assert attributes["status"]["labels"][0]["dataType"] == "TEXT"

        facts = {fact["id"]: fact for fact in dataset["facts"]}
        assert facts["amount"]["sourceColumn"] == "amount"

        references = dataset["references"]
        assert references[0]["identifier"]["id"] == "customers"


# =============================================================================
# LOADER AUTO-DETECTION
# =============================================================================


def test_gooddata_loader_auto_detect():
    """Test GoodData LDM auto-detection in load_from_directory."""
    from sidemantic import SemanticLayer
    from sidemantic.loaders import load_from_directory

    with tempfile.TemporaryDirectory() as tmpdir:
        cloud_path = Path(tmpdir) / "cloud_ldm.json"
        with open("tests/fixtures/gooddata/cloud_ldm.json") as src:
            cloud_path.write_text(src.read())

        legacy_path = Path(tmpdir) / "legacy_project_model.json"
        with open("tests/fixtures/gooddata/legacy_project_model.json") as src:
            legacy_path.write_text(src.read())

        layer = SemanticLayer()
        load_from_directory(layer, tmpdir)

        assert "orders" in layer.graph.models
        assert "dataset.orders" in layer.graph.models


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


# =============================================================================
# REAL-WORLD FIXTURE TESTS (permissively licensed)
# =============================================================================
# Source: gooddata/gooddata-public-demos (BSD-3-Clause)
# Source: gooddata/gooddata-python-sdk (MIT)


def test_ecommerce_demo_ldm():
    """Test parsing ecommerce demo LDM (BSD-3, gooddata/gooddata-public-demos).

    6 datasets, 5 date dimensions, GEO labels, returns + inventory models.
    """
    adapter = GoodDataAdapter()
    graph = adapter.parse("tests/fixtures/gooddata/ecommerce_demo_ldm.json")

    # 6 datasets + 5 date instances = 11 models
    assert len(graph.models) == 11

    # Datasets
    assert "customer" in graph.models
    assert "order_lines" in graph.models
    assert "product" in graph.models
    assert "returns" in graph.models
    assert "monthlyinventory" in graph.models
    assert "orders" in graph.models

    # Date dimensions
    assert "date" in graph.models
    assert "order_date" in graph.models
    assert "return_date" in graph.models
    assert "customer_created_date" in graph.models
    assert "inventory_month" in graph.models

    # Customer has GEO labels
    customer = graph.models["customer"]
    assert len(customer.dimensions) == 5
    assert len(customer.relationships) == 1
    geo_labels_found = False
    for dim in customer.dimensions:
        labels = (dim.metadata or {}).get("gooddata", {}).get("labels", [])
        for label in labels:
            vt = label.get("valueType", "")
            if "GEO" in vt:
                geo_labels_found = True
                break
    assert geo_labels_found, "Expected GEO_LATITUDE/GEO_LONGITUDE labels on customer"

    # order_lines has facts and references
    order_lines = graph.models["order_lines"]
    assert len(order_lines.metrics) == 4
    assert len(order_lines.relationships) == 5

    # returns model
    returns = graph.models["returns"]
    assert len(returns.metrics) == 3
    assert len(returns.relationships) == 5

    # monthlyinventory model
    inventory = graph.models["monthlyinventory"]
    assert len(inventory.metrics) == 2
    assert len(inventory.relationships) == 3

    # Date dimensions have rich granularities
    date_model = graph.models["date"]
    date_dim = date_model.dimensions[0]
    assert "day" in date_dim.supported_granularities
    assert "month" in date_dim.supported_granularities
    assert "quarter" in date_dim.supported_granularities
    assert "year" in date_dim.supported_granularities


def test_ecommerce_demo_ldm_product_facts():
    """Test product dataset rating fact (sourceColumnDataType, no dataType)."""
    adapter = GoodDataAdapter()
    graph = adapter.parse("tests/fixtures/gooddata/ecommerce_demo_ldm.json")

    product = graph.models["product"]
    assert len(product.metrics) == 1
    metric = product.metrics[0]
    assert metric.name == "rating"
    assert metric.sql == "rating"
    # sourceColumnDataType=NUMERIC but no dataType, so agg inference returns None
    assert metric.agg is None


def test_ecommerce_demo_ldm_relationships_resolve():
    """Test that cross-model references resolve primary keys."""
    adapter = GoodDataAdapter()
    graph = adapter.parse("tests/fixtures/gooddata/ecommerce_demo_ldm.json")

    # order_lines references order_date, date, customer, product, orders
    order_lines = graph.models["order_lines"]
    ref_names = {r.name for r in order_lines.relationships}
    assert len(ref_names) == 5


def test_sdk_declarative_analytics_model_not_ldm():
    """Analytics model files are not LDM and should raise GoodDataParseError.

    Source: gooddata/gooddata-python-sdk (MIT). Contains 24 MAQL metrics.
    """
    from sidemantic.adapters.gooddata import GoodDataParseError

    adapter = GoodDataAdapter()
    with pytest.raises(GoodDataParseError, match="does not look like GoodData LDM"):
        adapter.parse("tests/fixtures/gooddata/sdk_declarative_analytics_model.json")


def test_ecommerce_demo_analytics_not_ldm():
    """Ecommerce analytics file is not LDM and should raise GoodDataParseError.

    Source: gooddata/gooddata-public-demos (BSD-3). Contains 60 MAQL metrics.
    """
    from sidemantic.adapters.gooddata import GoodDataParseError

    adapter = GoodDataAdapter()
    with pytest.raises(GoodDataParseError, match="does not look like GoodData LDM"):
        adapter.parse("tests/fixtures/gooddata/ecommerce_demo_analytics.json")


def test_sdk_declarative_ldm_dict_sql_field():
    """SDK declarative LDM has dict-style sql field that adapter doesn't yet handle.

    Source: gooddata/gooddata-python-sdk (MIT). 6 datasets, star schema,
    GEO labels, aggregatedFacts, 1 SQL dataset with dict-style sql.
    The sql field is {"dataSourceId": ..., "statement": ...} instead of a string.
    """
    adapter = GoodDataAdapter()
    # Currently raises ValidationError because sql is a dict not a string.
    # When adapter gains dict-style sql support, update this test to verify parsing.
    with pytest.raises(Exception):
        adapter.parse("tests/fixtures/gooddata/sdk_declarative_ldm.json")


def test_sdk_declarative_ldm_with_sql_dataset():
    """SDK LDM with SQL datasets has dict-style sql field.

    Source: gooddata/gooddata-python-sdk (MIT). 7 datasets, newer ref format,
    isNullable facts, 2 SQL datasets with dict-style sql.
    """
    adapter = GoodDataAdapter()
    # Currently raises ValidationError because sql is a dict not a string.
    with pytest.raises(Exception):
        adapter.parse("tests/fixtures/gooddata/sdk_declarative_ldm_with_sql_dataset.json")
