"""Format-specific export behavior behind the round-trip matrix in test_export_roundtrip."""

import pytest
import yaml

from sidemantic import Dimension, Metric, Model, Relationship
from sidemantic.adapters.atscale_sml import AtScaleSMLAdapter
from sidemantic.adapters.base import filtered_measure_sql
from sidemantic.adapters.bsl import BSLAdapter
from sidemantic.adapters.hex import HexAdapter
from sidemantic.adapters.holistics import HolisticsAdapter
from sidemantic.adapters.omni import OmniAdapter
from sidemantic.adapters.rill import RillAdapter
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.adapters.snowflake import SnowflakeAdapter
from sidemantic.adapters.superset import SupersetAdapter
from sidemantic.adapters.thoughtspot import ThoughtSpotAdapter
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.fidelity import ConversionFidelityError, capture_export_report
from sidemantic.formats import export_semantic_graph


def graph_of(*models: Model) -> SemanticGraph:
    graph = SemanticGraph()
    for model in models:
        graph.add_model(model)
    return graph


def orders(*metrics: Metric, **fields) -> Model:
    return Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
        metrics=list(metrics),
        **fields,
    )


@pytest.mark.parametrize(
    "metric,expected",
    [
        (
            Metric(name="m", agg="sum", sql="amount", filters=["status = 'complete'"]),
            "CASE WHEN status = 'complete' THEN amount END",
        ),
        (
            Metric(name="m", agg="count", filters=["{model}.status = 'complete'", "amount > 0"]),
            "CASE WHEN (status = 'complete') AND (amount > 0) THEN 1 END",
        ),
        (
            Metric(name="m", agg="sum", sql="{model}.amount", filters=["orders.status = 'complete'"]),
            "CASE WHEN status = 'complete' THEN amount END",
        ),
        # Another model's column cannot be evaluated on this model's rows.
        (Metric(name="m", agg="sum", sql="amount", filters=["customers.region = 'EU'"]), None),
        (Metric(name="m", agg="sum", sql="amount", filters=["id IN (SELECT id FROM vip)"]), None),
        (Metric(name="m", agg="sum", sql="amount"), None),
    ],
)
def test_filters_fold_only_when_they_read_the_metrics_own_rows(metric, expected):
    assert filtered_measure_sql(metric, "orders") == expected


def test_bsl_reads_and_writes_measure_filters_as_ibis_where(tmp_path):
    source = tmp_path / "orders.yml"
    source.write_text(
        yaml.safe_dump(
            {
                "orders": {
                    "table": "orders",
                    "measures": {
                        "completed": "_.amount.sum(where=(_.status == 'complete') & (_.amount > 0))",
                        "completed_count": "_.count(where=_.status == 'complete')",
                    },
                }
            }
        )
    )
    model = BSLAdapter().parse(source).models["orders"]
    completed, completed_count = model.get_metric("completed"), model.get_metric("completed_count")
    assert (completed.agg, completed.sql) == ("sum", "amount")
    assert completed.filters == ["(status = 'complete') AND (amount > 0)"]
    assert (completed_count.agg, completed_count.sql, completed_count.filters) == (
        "count",
        None,
        ["status = 'complete'"],
    )

    native = orders(
        Metric(name="completed", agg="sum", sql="amount", filters=["status = 'complete'", "amount > 0"]),
        Metric(name="completed_count", agg="count", filters=["status IN ('complete', 'shipped')"]),
    )
    BSLAdapter().export(graph_of(native), tmp_path / "out.yml")
    measures = yaml.safe_load((tmp_path / "out.yml").read_text())["orders"]["measures"]
    assert measures["completed"] == "_.amount.sum(where=(_.status == 'complete') & (_.amount > 0))"
    assert measures["completed_count"] == "_.count(where=_.status.isin(['complete', 'shipped']))"
    reread = BSLAdapter().parse(tmp_path / "out.yml").models["orders"]
    assert reread.get_metric("completed_count").filters == ["status IN ('complete', 'shipped')"]


def test_bsl_rejects_a_where_it_cannot_translate(tmp_path):
    source = tmp_path / "orders.yml"
    source.write_text("orders:\n  table: orders\n  measures:\n    m: _.amount.sum(where=_.status.startswith('c'))\n")
    with pytest.raises(ValueError, match="Cannot translate BSL measure filter"):
        BSLAdapter().parse(source)


@pytest.mark.parametrize(
    "target,output,unwritable_filter",
    [
        # The BSL importer reads comparisons, isin/between and null checks, not LIKE.
        ("bsl", "out.yml", "status LIKE 'c%'"),
        # The SQL-expression formats fold filters over the metric's own rows only.
        ("snowflake", "out.yaml", "customers.region = 'EU'"),
        ("holistics", "out.aml", "customers.region = 'EU'"),
        ("rill", "out", "customers.region = 'EU'"),
        ("superset", "out.yaml", "customers.region = 'EU'"),
    ],
)
def test_filter_that_cannot_be_written_is_reported_not_dropped(tmp_path, target, output, unwritable_filter):
    model = orders(Metric(name="some_revenue", agg="sum", sql="amount", filters=[unwritable_filter]))
    destination = tmp_path / output
    with capture_export_report() as report, pytest.raises(ConversionFidelityError, match="some_revenue"):
        export_semantic_graph(graph_of(model), destination, target_format=target)
    assert report.is_blocked
    assert not destination.exists()


def test_snowflake_writes_filtered_aggregates_as_metric_expressions(tmp_path):
    model = orders(
        Metric(name="revenue", agg="sum", sql="amount"),
        Metric(name="completed_revenue", agg="sum", sql="amount", filters=["status = 'complete'"]),
        Metric(name="completed_buyers", agg="count_distinct", sql="customer_id", filters=["status = 'complete'"]),
    )
    SnowflakeAdapter().export(graph_of(model), tmp_path / "model.yaml")
    table = yaml.safe_load((tmp_path / "model.yaml").read_text())["tables"][0]
    assert [fact["name"] for fact in table["facts"]] == ["revenue"]
    assert {metric["name"]: metric["expr"] for metric in table["metrics"]} == {
        "completed_revenue": "SUM(CASE WHEN status = 'complete' THEN amount END)",
        "completed_buyers": "COUNT(DISTINCT CASE WHEN status = 'complete' THEN customer_id END)",
    }
    reread = SnowflakeAdapter().parse(tmp_path / "model.yaml").models["orders"]
    assert reread.get_metric("completed_buyers").agg == "count_distinct"


def test_superset_single_file_refuses_to_drop_models(tmp_path):
    customers = Model(name="customers", table="customers")
    with pytest.raises(ValueError, match="holds one model"):
        SupersetAdapter().export(graph_of(orders(), customers), tmp_path / "dataset.yaml")
    assert not (tmp_path / "dataset.yaml").exists()


def test_superset_inlines_ratio_operands_and_keeps_unmapped_aggregates(tmp_path):
    model = orders(
        Metric(name="revenue", agg="sum", sql="amount"),
        Metric(name="completed_revenue", agg="sum", sql="amount", filters=["status = 'complete'"]),
        Metric(name="completion_rate", type="ratio", numerator="completed_revenue", denominator="revenue"),
        Metric(name="median_amount", agg="median", sql="amount"),
    )
    SupersetAdapter().export(graph_of(model), tmp_path / "out")
    metrics = {m["metric_name"]: m for m in yaml.safe_load((tmp_path / "out" / "orders.yaml").read_text())["metrics"]}
    assert metrics["completion_rate"]["expression"] == (
        "SUM(CASE WHEN status = 'complete' THEN amount END) / NULLIF(SUM(amount), 0)"
    )
    # ``median`` has no Superset metric_type; labelling it ``count`` would re-import as COUNT(MEDIAN(amount)).
    assert (metrics["median_amount"]["metric_type"], metrics["median_amount"]["expression"]) == (
        None,
        "MEDIAN(amount)",
    )


def test_rill_measures_are_valid_sql_aggregates(tmp_path):
    model = orders(
        Metric(name="buyers", agg="count_distinct", sql="customer_id"),
        Metric(name="completed_orders", agg="count", filters=["status = 'complete'"]),
    )
    RillAdapter().export(graph_of(model), tmp_path)
    measures = {m["name"]: m["expression"] for m in yaml.safe_load((tmp_path / "orders.yaml").read_text())["measures"]}
    assert measures == {
        "buyers": "COUNT(DISTINCT customer_id)",
        "completed_orders": "COUNT(CASE WHEN status = 'complete' THEN 1 END)",
    }
    reread = RillAdapter().parse(tmp_path).models["orders"]
    assert reread.get_metric("buyers").agg == "count_distinct"


@pytest.mark.parametrize(
    "adapter,output,key_of",
    [
        (HexAdapter(), "out", lambda path: yaml.safe_load((path / "orders.yml").read_text())["dimensions"][-1]),
        (
            OmniAdapter(),
            "out",
            lambda path: yaml.safe_load((path / "views" / "orders.view.yaml").read_text())["dimensions"]["order_id"],
        ),
    ],
)
def test_undeclared_primary_key_is_exported_as_a_hidden_key_dimension(tmp_path, adapter, output, key_of):
    adapter.export(graph_of(orders(Metric(name="revenue", agg="sum", sql="amount"))), tmp_path / output)
    key = key_of(tmp_path / output)
    assert key.get("unique") or key.get("primary_key")
    assert key.get("visibility") == "internal" or key.get("hidden") is True
    assert type(adapter)().parse(tmp_path / output).models["orders"].primary_key == "order_id"


def test_holistics_exports_undeclared_primary_key(tmp_path):
    HolisticsAdapter().export(graph_of(orders()), tmp_path / "orders.aml")
    assert (
        "dimension order_id {\n    type: 'text'\n    hidden: true\n    primary_key: true"
        in (tmp_path / "orders.aml").read_text()
    )
    assert HolisticsAdapter().parse(tmp_path / "orders.aml").models["orders"].primary_key == "order_id"


@pytest.mark.parametrize(
    "adapter,name,content",
    [
        (HexAdapter(), "orders.yml", "id: orders\nbase_sql_table: orders\ndimensions:\n- id: status\n  type: string\n"),
        (OmniAdapter(), "orders.view.yaml", "table_name: orders\ndimensions:\n  status:\n    type: string\n"),
        (
            HolisticsAdapter(),
            "orders.model.aml",
            "Model orders {\n  type: 'table'\n  table_name: 'orders'\n  dimension status {\n    type: 'text'\n  }\n}\n",
        ),
        (
            ThoughtSpotAdapter(),
            "orders.table.tml",
            "table:\n  name: orders\n  db_table: orders\n  columns:\n  - name: status\n    db_column_name: status\n"
            "    data_type: VARCHAR\n    properties:\n      column_type: ATTRIBUTE\n",
        ),
    ],
)
def test_import_does_not_invent_an_id_primary_key(tmp_path, adapter, name, content):
    (tmp_path / name).write_text(content)
    assert adapter.parse(tmp_path / name).models["orders"].primary_key is None
    (tmp_path / name).write_text(content.replace("status", "id"))
    assert adapter.parse(tmp_path / name).models["orders"].primary_key == "id"


def test_omni_join_uses_the_related_views_primary_key(tmp_path):
    fact = orders(relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")])
    customers = Model(name="customers", table="customers", primary_key="customer_id")
    OmniAdapter().export(graph_of(fact, customers), tmp_path)
    joins = yaml.safe_load((tmp_path / "relationships.yaml").read_text())
    assert joins[0]["on_sql"] == "${orders.customer_id} = ${customers.customer_id}"


def test_atscale_counts_the_key_and_writes_ratio_operands(tmp_path):
    model = orders(
        Metric(name="order_count", agg="count"),
        Metric(name="revenue", agg="sum", sql="amount"),
        Metric(name="average_order", type="ratio", numerator="orders.revenue", denominator="orders.order_count"),
    )
    AtScaleSMLAdapter().export(graph_of(model), tmp_path)
    count = yaml.safe_load((tmp_path / "metrics" / "order_count.yml").read_text())
    assert (count["object_type"], count["calculation_method"], count["column"]) == (
        "metric",
        "count non-null",
        "order_id",
    )
    ratio = yaml.safe_load((tmp_path / "metrics" / "average_order.yml").read_text())
    assert ratio["expression"] == "revenue / NULLIF(order_count, 0)"

    reread = AtScaleSMLAdapter().parse(tmp_path).models["orders"]
    assert reread.primary_key == "order_id"
    assert (reread.get_metric("order_count").agg, reread.get_metric("order_count").sql) == ("count", "order_id")


def test_atscale_refuses_metric_names_shared_across_models(tmp_path):
    left = orders(Metric(name="row_count", agg="count"))
    right = Model(
        name="customers", table="customers", primary_key="customer_id", metrics=[Metric(name="row_count", agg="count")]
    )
    with pytest.raises(ValueError, match="'customers.row_count' shares its name with 'orders.row_count'"):
        AtScaleSMLAdapter().export(graph_of(left, right), tmp_path)


def test_thoughtspot_joins_on_the_related_models_key_and_counts_the_key(tmp_path):
    fact = orders(
        Metric(name="order_count", agg="count"),
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )
    customers = Model(
        name="customers",
        table="customers",
        primary_key="customer_id",
        metrics=[Metric(name="customer_count", agg="count")],
    )
    ThoughtSpotAdapter().export(graph_of(fact, customers), tmp_path)
    worksheet = yaml.safe_load((tmp_path / "orders.worksheet.tml").read_text())["worksheet"]
    assert worksheet["joins"][0]["on"] == "[orders::customer_id] = [customers::customer_id]"
    order_count = next(column for column in worksheet["worksheet_columns"] if column["name"] == "order_count")
    assert order_count["column_id"] == "orders::order_id"
    table = yaml.safe_load((tmp_path / "customers.table.tml").read_text())["table"]
    assert table["columns"][-1]["db_column_name"] == "customer_id"


def test_native_export_keeps_model_owned_ratio_operands(tmp_path):
    model = orders(
        Metric(name="revenue", agg="sum", sql="amount"),
        Metric(name="order_count", agg="count"),
        Metric(name="average_order", type="ratio", numerator="revenue", denominator="order_count"),
    )
    SidemanticAdapter().export(graph_of(model), tmp_path / "models.yml")
    ratio = SidemanticAdapter().parse(tmp_path / "models.yml").models["orders"].get_metric("average_order")
    assert (ratio.type, ratio.numerator, ratio.denominator) == ("ratio", "revenue", "order_count")
