"""Window wrappers preserve independent source grains and grouped output names."""

import pytest
import sqlglot
from sqlglot import exp

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from tests.duckdb_compat import date_bucket


@pytest.mark.parametrize("dialect", ["duckdb", "postgres", "tsql", "mysql", "bigquery"])
@pytest.mark.parametrize("metric_name", ["order", "daily amount"])
@pytest.mark.parametrize("day_name", ["day", "order day"])
def test_native_window_expression_preserves_quoted_input_across_dialects(dialect, metric_name, day_name):
    pytest.importorskip("sidemantic_rs")
    # Authored expressions use the layer's input dialect, including its quoting.
    window_input = exp.column(metric_name, table="base", quoted=True).sql(dialect=dialect)
    with SemanticLayer(engine="rust", fallback=False, dialect=dialect, auto_register=False) as layer:
        layer.add_model(
            Model(
                name="events",
                table="events",
                dimensions=[Dimension(name=day_name, type="time", granularity="day")],
                metrics=[
                    Metric(name=metric_name, agg="sum", sql="amount"),
                    Metric(name="running amount", type="cumulative", window_expression=f"SUM({window_input})"),
                ],
            )
        )
        sql = layer.compile(metrics=["events.running amount"], dimensions=[f"events.{day_name}"])
        parsed = sqlglot.parse_one(sql, read=dialect)
        window = next(parsed.find_all(exp.Window))
        columns = list(window.this.find_all(exp.Column))
        assert [(column.table, column.name) for column in columns] == [("base", metric_name)]
        assert window.parent.alias == "running amount"
        assert window.args["order"].expressions[0].this.name == day_name
        assert layer.last_engine_selection == {"engine": "rust", "reason": None}


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Cross-source window acceptance requires the real Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    try:
        yield layer
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("population", ["populated", "empty_facts", "empty_accounts", "both_empty"])
@pytest.mark.parametrize("reverse", [False, True])
def test_cross_source_calculation_beside_cumulative_preserves_leaf_grains(layer, population, reverse):
    layer.add_model(
        Model(
            name="facts",
            table="window_facts",
            primary_key="id",
            dimensions=[Dimension(name="day", type="time", granularity="day")],
            metrics=[
                Metric(name="total", agg="sum", sql="amount", fill_nulls_with=0),
                Metric(name="running", type="cumulative", sql="total"),
            ],
            relationships=[Relationship(name="accounts", type="many_to_one", foreign_key="account_id")],
        )
    )
    layer.add_model(
        Model(
            name="accounts",
            table="window_accounts",
            primary_key="id",
            metrics=[Metric(name="quota", agg="sum", sql="quota")],
        )
    )
    layer.add_metric(Metric(name="combined", type="derived", sql="facts.total + accounts.quota"))
    layer.adapter.execute("""
        create table window_facts(id integer, account_id integer, day date, amount integer);
        create table window_accounts(id integer, quota integer);
    """)
    if population not in {"empty_facts", "both_empty"}:
        layer.adapter.execute("""
            insert into window_facts values
                (1, 1, '2024-01-01', 2), (2, 1, '2024-01-01', 3),
                (3, 1, '2024-01-02', 7), (4, 2, '2024-01-02', 11);
        """)
    if population not in {"empty_accounts", "both_empty"}:
        layer.adapter.execute("insert into window_accounts values (1, 10), (2, 20)")

    metrics = ["combined", "facts.running"]
    if reverse:
        metrics.reverse()
    sql = layer.compile(metrics=metrics, dimensions=["facts.day"], order_by=["facts.day"])
    assert layer.last_engine_selection["engine"] == layer.engine
    result = layer.adapter.execute(sql)
    columns = [column[0] for column in result.description]
    leaves = ["total", "quota"] if reverse else ["quota", "total"]
    assert columns == ["day", *leaves, "combined", "running"]
    rows = result.fetchall()
    records = [dict(zip(columns, row, strict=True)) for row in rows]
    actual = [(r["day"], r["quota"], r["total"], r["combined"], r["running"]) for r in records]
    expected = {
        "populated": [(date_bucket(2024, 1, 1), 10, 5, 15, 5), (date_bucket(2024, 1, 2), 30, 18, 48, 23)],
        "empty_facts": [(None, 30, 0, 30, 0)],
        "empty_accounts": [(date_bucket(2024, 1, 1), None, 5, None, 5), (date_bucket(2024, 1, 2), None, 18, None, 23)],
        "both_empty": [],
    }
    assert actual == expected[population]


@pytest.mark.parametrize("window_kind", ["running", "expression", "lag"])
def test_window_partitions_bind_both_colliding_sibling_dimensions(layer, window_kind):
    metrics = [Metric(name="total", agg="sum", sql="amount")]
    if window_kind == "lag":
        metrics.append(
            Metric(
                name="change",
                type="time_comparison",
                base_metric="total",
                comparison_type="dod",
                calculation="difference",
            )
        )
        selected = "facts.change"
    else:
        controls = {"window_expression": "SUM(base.total)"} if window_kind == "expression" else {"sql": "total"}
        metrics.append(Metric(name="running", type="cumulative", **controls))
        selected = "facts.running"
    layer.add_model(
        Model(
            name="facts",
            table="collision_facts",
            primary_key="id",
            dimensions=[Dimension(name="day", type="time", granularity="day")],
            metrics=metrics,
            relationships=[
                Relationship(name="items", type="one_to_many", foreign_key="fact_id"),
                Relationship(name="refunds", type="one_to_many", foreign_key="fact_id"),
            ],
        )
    )
    for name in ["items", "refunds"]:
        layer.add_model(
            Model(
                name=name,
                table=f"collision_{name}",
                primary_key="id",
                dimensions=[Dimension(name="label", type="categorical")],
            )
        )
    layer.adapter.execute("""
        create table collision_facts(id integer, day date, amount integer);
        create table collision_items(id integer, fact_id integer, label varchar);
        create table collision_refunds(id integer, fact_id integer, label varchar);
        insert into collision_facts values
            (1, '2024-01-01', 2), (2, '2024-01-02', 3),
            (3, '2024-01-01', 10), (4, '2024-01-02', 20);
        insert into collision_items values
            (1, 1, 'a'), (2, 1, 'a'), (3, 2, 'a'), (4, 3, 'b'), (5, 4, 'b');
        insert into collision_refunds values
            (1, 1, 'x'), (2, 1, 'x'), (3, 1, 'y'), (4, 2, 'x'),
            (5, 2, 'y'), (6, 3, 'x'), (7, 4, 'x');
    """)
    sql = layer.compile(
        metrics=[selected],
        dimensions=["facts.day", "items.label", "refunds.label"],
        order_by=["items.label", "refunds.label", "facts.day"],
    )
    assert layer.last_engine_selection["engine"] == layer.engine
    result = layer.adapter.execute(sql)
    columns = [column[0] for column in result.description]
    assert columns[:4] == ["day", "items_label", "refunds_label", "total"]
    rows = result.fetchall()
    expected_windows = [None, 1, None, 1, None, 10] if window_kind == "lag" else [2, 5, 2, 5, 10, 30]
    expected_groups = [
        (date_bucket(2024, 1, 1), "a", "x", 2),
        (date_bucket(2024, 1, 2), "a", "x", 3),
        (date_bucket(2024, 1, 1), "a", "y", 2),
        (date_bucket(2024, 1, 2), "a", "y", 3),
        (date_bucket(2024, 1, 1), "b", "x", 10),
        (date_bucket(2024, 1, 2), "b", "x", 20),
    ]
    assert rows == [(*group, window) for group, window in zip(expected_groups, expected_windows, strict=True)]


@pytest.mark.parametrize("collision_kind", ["metric", "dimension"])
@pytest.mark.parametrize("window_kind", ["running", "expression", "lag", "ratio"])
def test_window_inputs_keep_qualified_metric_identity(layer, collision_kind, window_kind):
    account_metric = "total" if collision_kind == "metric" else "quota"
    if window_kind == "lag":
        window = Metric(
            name="windowed",
            type="time_comparison",
            base_metric="total",
            comparison_type="dod",
            calculation="difference",
        )
    elif window_kind == "ratio":
        window = Metric(name="windowed", type="ratio", numerator="total", denominator="total", offset_window="1 day")
    else:
        controls = {"window_expression": "SUM(base.total)"} if window_kind == "expression" else {"sql": "total"}
        window = Metric(name="windowed", type="cumulative", window_order="facts.total", **controls)
    layer.add_model(
        Model(
            name="facts",
            table="alias_facts",
            primary_key="id",
            dimensions=[Dimension(name="day", type="time", granularity="day")],
            metrics=[Metric(name="total", agg="sum", sql="amount"), window],
            relationships=[Relationship(name="accounts", type="many_to_one", foreign_key="account_id")],
        )
    )
    layer.add_model(
        Model(
            name="accounts",
            table="alias_accounts",
            primary_key="id",
            dimensions=[Dimension(name="total", type="categorical", sql="category")]
            if collision_kind == "dimension"
            else [],
            metrics=[Metric(name=account_metric, agg="sum", sql="quota")],
        )
    )
    layer.add_metric(Metric(name="combined", type="derived", sql=f"facts.total + accounts.{account_metric}"))
    layer.adapter.execute("""
        create table alias_facts(id integer, account_id integer, day date, amount integer);
        create table alias_accounts(id integer, category varchar, quota integer);
        insert into alias_accounts values (1, 'retail', 10);
        insert into alias_facts values
            (1, 1, '2024-01-01', 2), (2, 1, '2024-01-01', 3), (3, 1, '2024-01-02', 7);
    """)
    dimensions = ["facts.day"]
    if collision_kind == "dimension":
        dimensions.append("accounts.total")
    sql = layer.compile(metrics=["combined", "facts.windowed"], dimensions=dimensions, order_by=["facts.total DESC"])
    assert layer.last_engine_selection["engine"] == layer.engine
    result = layer.adapter.execute(sql)
    columns = [column[0] for column in result.description]
    records = [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
    window_alias = "facts.windowed" if "facts.windowed" in columns else "windowed"
    expected_window = {
        "running": [12, 5],
        "expression": [12, 5],
        "lag": [2, None],
        "ratio": [1.4, None],
    }[window_kind]
    assert [(row["facts_total"], row["combined"], row[window_alias]) for row in records] == [
        (7, 17, expected_window[0]),
        (5, 15, expected_window[1]),
    ]
    assert [row["accounts_total"] for row in records] == (
        [10, 10] if collision_kind == "metric" else ["retail", "retail"]
    )


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
@pytest.mark.parametrize("include_calculation", [False, True])
def test_deferred_calculation_keeps_unqueryable_window_owner_validation(layer, include_calculation):
    layer.add_model(
        Model(
            name="facts",
            table="guard_facts",
            primary_key="id",
            dimensions=[Dimension(name="day", type="time", granularity="day")],
            metrics=[Metric(name="total", agg="sum", sql="amount")],
            relationships=[
                Relationship(name="accounts", type="many_to_one", foreign_key="account_id"),
                Relationship(name="unavailable", type="many_to_one", foreign_key="unavailable_id"),
            ],
        )
    )
    layer.add_model(
        Model(
            name="accounts",
            table="guard_accounts",
            primary_key="id",
            metrics=[Metric(name="quota", agg="sum", sql="quota")],
        )
    )
    layer.add_model(
        Model(
            name="unavailable",
            source_uri="s3://warehouse/unavailable.parquet",
            primary_key="id",
            metrics=[Metric(name="running", type="cumulative", sql="facts.total")],
        )
    )
    layer.add_metric(Metric(name="combined", type="derived", sql="facts.total + accounts.quota"))
    metrics = ["unavailable.running"]
    if include_calculation:
        metrics.insert(0, "combined")
    with pytest.raises(Exception, match="unavailable.*source_uri|source_uri.*unavailable"):
        layer.compile(metrics=metrics, dimensions=["facts.day"])
