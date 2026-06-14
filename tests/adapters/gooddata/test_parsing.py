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
    # sourceColumnDataType=NUMERIC is read (newer name for dataType), so a numeric
    # fact infers a sum aggregation.
    assert metric.agg == "sum"
    assert metric.metadata["gooddata"]["data_type"] == "NUMERIC"


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
    """SDK declarative LDM with object-style sql field and aggregatedFacts.

    Source: gooddata/gooddata-python-sdk (MIT). 6 datasets, star schema,
    GEO labels, aggregatedFacts, 1 SQL dataset with object-style sql.
    The sql field is {"dataSourceId": ..., "statement": ...} instead of a string.
    """
    adapter = GoodDataAdapter()
    graph = adapter.parse("tests/fixtures/gooddata/sdk_declarative_ldm.json")

    # 6 datasets + 1 date instance = 7 models
    assert "campaign_channels" in graph.models
    assert "campaign_channels_per_category" in graph.models
    assert "campaigns" in graph.models
    assert "customers" in graph.models
    assert "order_lines" in graph.models
    assert "products" in graph.models
    assert "date" in graph.models

    # Object-style dataSourceTableId -> path joined into a table name.
    campaign_channels = graph.models["campaign_channels"]
    assert campaign_channels.table == "demo.campaign_channels"
    assert campaign_channels.metadata["gooddata"]["data_source_id"] == "demo-test-ds"
    # Original object form is preserved in metadata.
    assert campaign_channels.metadata["gooddata"]["data_source_table_id"]["path"] == ["demo", "campaign_channels"]

    # sources-array reference yields the join foreign key.
    rel = next(r for r in campaign_channels.relationships if r.name == "campaigns")
    assert rel.type == "many_to_one"
    assert rel.foreign_key == "campaign_id"

    # sourceColumnDataType (renamed from dataType) drives attribute/fact typing.
    channel_id = campaign_channels.get_dimension("campaign_channel_id")
    assert channel_id.type == "categorical"
    budget = campaign_channels.get_metric("budget")
    assert budget.agg == "sum"
    assert budget.metadata["gooddata"]["data_type"] == "NUMERIC"

    # SQL-backed dataset: object sql {dataSourceId, statement} -> Model.sql statement.
    per_category = graph.models["campaign_channels_per_category"]
    assert per_category.sql == "SELECT category, SUM(budget) FROM campaign_channels GROUP BY category"
    assert per_category.table is None
    assert per_category.metadata["gooddata"]["data_source_id"] == "demo-test-ds"

    # aggregatedFacts (aggregate awareness): source fact + SUM operation.
    assert len(per_category.metrics) == 1
    budget_agg = per_category.get_metric("budget_agg")
    assert budget_agg.agg == "sum"
    assert budget_agg.sql == "budget"
    agg_meta = budget_agg.metadata["gooddata"]
    assert agg_meta["aggregated_fact"] is True
    assert agg_meta["operation"] == "SUM"
    assert agg_meta["source_fact"] == "budget"


def test_sdk_declarative_ldm_with_sql_dataset():
    """SDK LDM with SQL datasets, object sql, sources refs, and WDF columns.

    Source: gooddata/gooddata-python-sdk (MIT). 7 datasets, newer ref format,
    isNullable facts, 2 SQL datasets with object-style sql.
    """
    adapter = GoodDataAdapter()
    graph = adapter.parse("tests/fixtures/gooddata/sdk_declarative_ldm_with_sql_dataset.json")

    # SQL-backed dataset with workspaceDataFilterColumns.
    sql_ds = graph.models["Customers_sql_dataset_with_WDF"]
    assert sql_ds.sql == "SELECT * FROM v_wdf_customers"
    assert sql_ds.table is None
    assert sql_ds.metadata["gooddata"]["data_source_id"] == "demo-test-ds"
    wdf = sql_ds.metadata["gooddata"]["workspace_data_filter_columns"]
    assert wdf == [{"dataType": "STRING", "name": "wdf__region"}]

    # Second SQL dataset still references date via a sources-array reference.
    dup = graph.models["Order_lines_duplicate_sql_dataset"]
    assert dup.sql == "SELECT * FROM order_lines"
    date_rel = next(r for r in dup.relationships if r.name == "date")
    assert date_rel.foreign_key == "date"

    # Table-backed dataset: object dataSourceTableId + sources refs + WDF columns.
    order_lines = graph.models["order_lines"]
    assert order_lines.table == "demo.order_lines"
    ref_fks = {r.name: r.foreign_key for r in order_lines.relationships}
    assert ref_fks == {
        "campaigns": "campaign_id",
        "customers": "customer_id",
        "date": "date",
        "products": "product_id",
    }
    assert order_lines.metadata["gooddata"]["workspace_data_filter_columns"] == [
        {"dataType": "STRING", "name": "wdf__region"},
        {"dataType": "STRING", "name": "wdf__state"},
    ]


def test_sdk_declarative_ldm_sql_dataset_export_roundtrip():
    """Object sql, aggregatedFacts, and WDF columns survive an export round-trip."""
    import json as _json
    import tempfile as _tempfile
    from pathlib import Path as _Path

    adapter = GoodDataAdapter()
    graph = adapter.parse("tests/fixtures/gooddata/sdk_declarative_ldm.json")

    with _tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)
        data = _json.loads((_Path(tmpdir) / "ldm.json").read_text())

    datasets = {d["id"]: d for d in data["ldm"]["datasets"]}

    # Object dataSourceTableId preserved verbatim.
    assert datasets["campaign_channels"]["dataSourceTableId"]["path"] == ["demo", "campaign_channels"]

    # SQL dataset keeps its statement and emits aggregatedFacts.
    per_category = datasets["campaign_channels_per_category"]
    # Object-form SQL round-trips back to the SDK shape {dataSourceId, statement},
    # not a bare string, and the data source stays nested (no top-level dataSourceId).
    assert per_category["sql"] == {
        "dataSourceId": "demo-test-ds",
        "statement": "SELECT category, SUM(budget) FROM campaign_channels GROUP BY category",
    }
    assert "dataSourceId" not in per_category
    assert "facts" not in per_category or not per_category["facts"]
    agg_facts = per_category["aggregatedFacts"]
    assert len(agg_facts) == 1
    assert agg_facts[0]["id"] == "budget_agg"
    assert agg_facts[0]["sourceFactReference"] == {
        "operation": "SUM",
        "reference": {"id": "budget", "type": "fact"},
    }

    # Re-parsing the export preserves the aggregated fact + sql.
    with _tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)
        reparsed = adapter.parse(_Path(tmpdir) / "ldm.json")
    reparsed_agg = reparsed.models["campaign_channels_per_category"].get_metric("budget_agg")
    assert reparsed_agg.agg == "sum"
    assert reparsed_agg.metadata["gooddata"]["operation"] == "SUM"


def test_sdk_sql_dataset_object_sql_export_roundtrip():
    """SQL-backed datasets re-emit the SDK ``sql`` object shape on export.

    Object-form ``sql`` ({dataSourceId, statement}) must survive a parse/export
    round-trip without collapsing to a bare statement string or hoisting the
    data source to a top-level ``dataSourceId``.
    """
    adapter = GoodDataAdapter()
    graph = adapter.parse("tests/fixtures/gooddata/sdk_declarative_ldm_with_sql_dataset.json")

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)
        data = json.loads((Path(tmpdir) / "ldm.json").read_text())

    datasets = {d["id"]: d for d in data["ldm"]["datasets"]}

    sql_ds = datasets["Customers_sql_dataset_with_WDF"]
    assert sql_ds["sql"] == {"dataSourceId": "demo-test-ds", "statement": "SELECT * FROM v_wdf_customers"}
    assert "dataSourceId" not in sql_ds
    assert "dataSourceTableId" not in sql_ds

    dup = datasets["Order_lines_duplicate_sql_dataset"]
    assert dup["sql"] == {"dataSourceId": "demo-test-ds", "statement": "SELECT * FROM order_lines"}
    assert "dataSourceId" not in dup

    # Table-backed cloud datasets still emit a top-level dataSourceId string.
    cloud = adapter.parse("tests/fixtures/gooddata/cloud_ldm.json")
    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(cloud, tmpdir)
        cloud_data = json.loads((Path(tmpdir) / "ldm.json").read_text())
    cloud_datasets = {d["id"]: d for d in cloud_data["ldm"]["datasets"]}
    assert cloud_datasets["customers"]["dataSourceId"] == "demo"
    assert "sql" not in cloud_datasets["customers"]

    # Re-parsing the SQL export recovers the statement and nested data source.
    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)
        reparsed = adapter.parse(Path(tmpdir) / "ldm.json")
    reparsed_sql = reparsed.models["Customers_sql_dataset_with_WDF"]
    assert reparsed_sql.sql == "SELECT * FROM v_wdf_customers"
    assert reparsed_sql.table is None
    assert reparsed_sql.metadata["gooddata"]["data_source_id"] == "demo-test-ds"


def test_gooddata_composite_sources_reference_foreign_key():
    """A multi-column ``sources`` reference sets the full composite foreign key.

    Single-column refs unwrap to a plain string; composite refs keep the list so
    the SQL planner joins on every column instead of falling back to ``<target>_id``.
    """
    ldm = {
        "ldm": {
            "datasets": [
                {
                    "id": "orders",
                    "title": "Orders",
                    "dataSourceTableId": "orders",
                    "grain": [{"id": "order_id", "type": "attribute"}],
                    "references": [
                        {
                            "identifier": {"id": "customers", "type": "dataset"},
                            "multivalue": False,
                            "sources": [
                                {"column": "region", "target": {"id": "region", "type": "attribute"}},
                                {"column": "customer_code", "target": {"id": "customer_code", "type": "attribute"}},
                            ],
                        },
                        {
                            "identifier": {"id": "products", "type": "dataset"},
                            "multivalue": False,
                            "sources": [
                                {"column": "product_id", "target": {"id": "product_id", "type": "attribute"}},
                            ],
                        },
                    ],
                },
                {"id": "customers", "title": "Customers", "dataSourceTableId": "customers"},
                {"id": "products", "title": "Products", "dataSourceTableId": "products"},
            ]
        }
    }

    adapter = GoodDataAdapter()
    with tempfile.TemporaryDirectory() as tmpdir:
        src = Path(tmpdir) / "ldm.json"
        src.write_text(json.dumps(ldm))
        graph = adapter.parse(src)

    orders = graph.models["orders"]
    customers_rel = next(r for r in orders.relationships if r.name == "customers")
    # Composite key: full column list preserved (not collapsed, not defaulted).
    assert customers_rel.foreign_key == ["region", "customer_code"]
    assert customers_rel.foreign_key_columns == ["region", "customer_code"]

    products_rel = next(r for r in orders.relationships if r.name == "products")
    # Single column unwraps to a plain string.
    assert products_rel.foreign_key == "product_id"

    # Export preserves the composite sources array verbatim.
    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph, tmpdir)
        exported = json.loads((Path(tmpdir) / "ldm.json").read_text())
    exported_orders = next(d for d in exported["ldm"]["datasets"] if d["id"] == "orders")
    cust_ref = next(r for r in exported_orders["references"] if r["identifier"]["id"] == "customers")
    assert [s["column"] for s in cust_ref["sources"]] == ["region", "customer_code"]
