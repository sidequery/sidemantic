"""Execute native query regressions against DuckDB, rather than comparing SQL text."""

import duckdb
import pytest
import yaml

native = pytest.importorskip("sidemantic_rs")


def execute(models, **query):
    sql = native.compile_with_yaml(yaml.safe_dump({"models": models}), yaml.safe_dump(query))
    with duckdb.connect() as conn:
        return conn.execute(sql).fetchall()


def facts(metrics, dimensions=None, source=None):
    return {
        "name": "facts",
        "primary_key": "id",
        "sql": source or "SELECT * FROM (VALUES (1, 10, 999), (2, 20, 999)) t(id, amount, revenue_raw)",
        "metrics": metrics,
        "dimensions": dimensions or [],
    }


@pytest.mark.parametrize("declare_collision", [False, True])
def test_physical_columns_cannot_shadow_native_measure_inputs(declare_collision):
    model = facts(
        [
            {"name": "revenue", "agg": "sum", "sql": "amount"},
            {"name": "double_revenue", "type": "derived", "sql": "revenue * 2"},
        ],
        [{"name": "revenue_raw", "type": "categorical"}] if declare_collision else [],
    )
    query = {"metrics": ["facts.revenue", "facts.double_revenue"]}
    if declare_collision:
        query["dimensions"] = ["facts.revenue_raw"]
    assert execute([model], **query) == ([(999, 30, 60)] if declare_collision else [(30, 60)])


@pytest.mark.parametrize("customer_dimension", [False, True])
@pytest.mark.parametrize("expression", [None, "CAST(created_at AS TIMESTAMP)"])
def test_joined_time_inputs_bind_to_their_source(customer_dimension, expression):
    time = {"name": "created_at", "type": "time", "granularity": "day"}
    if expression:
        time["sql"] = expression
    orders = {
        "name": "orders",
        "primary_key": "id",
        "sql": "SELECT 1 id, 1 customer_id, DATE '2024-01-01' created_at, 10 amount",
        "dimensions": [time],
        "metrics": [{"name": "revenue", "agg": "sum", "sql": "amount"}],
        "relationships": [{"name": "customers", "type": "many_to_one", "foreign_key": "customer_id"}],
    }
    customers = {
        "name": "customers",
        "primary_key": "id",
        "sql": "SELECT 1 id, 'a' AS name, DATE '2023-01-01' created_at",
        "dimensions": [{"name": "name", "type": "categorical"}, time],
    }
    dimensions = ["orders.created_at"] + (["customers.name"] if customer_dimension else [])
    rows = execute(
        [orders, customers],
        metrics=["orders.revenue"],
        dimensions=dimensions,
        filters=[] if customer_dimension else ["customers.name = 'a'"],
    )
    assert len(rows) == 1
    assert str(rows[0][0]).startswith("2024-01-01")
    assert rows[0][-1] == 10


@pytest.mark.parametrize(
    "predicate",
    [
        "facts.revenue > 10 AND facts.category = 'b'",
        "((facts.revenue > 10) AND (facts.category = 'b' OR facts.category = 'c'))",
    ],
)
def test_mixed_conjunctive_filters_keep_row_and_aggregate_stages(predicate):
    model = facts(
        [{"name": "revenue", "agg": "sum", "sql": "amount"}],
        [{"name": "category", "type": "categorical"}],
        "SELECT * FROM (VALUES (1, 'a', 5), (2, 'b', 15)) t(id, category, amount)",
    )
    assert execute([model], metrics=["facts.revenue"], dimensions=["facts.category"], filters=[predicate]) == [
        ("b", 15)
    ]


def test_reverse_direct_many_to_many_retains_fanout_risk():
    a = {
        "name": "a",
        "primary_key": "id",
        "sql": "SELECT * FROM (VALUES (1, 'shared', 'group'), (2, 'shared', 'group')) t(id, code, label)",
        "dimensions": [{"name": "label", "type": "categorical"}],
        "relationships": [{"name": "b", "type": "many_to_many", "foreign_key": "code", "primary_key": "code"}],
    }
    b = {
        "name": "b",
        "primary_key": "id",
        "sql": "SELECT * FROM (VALUES (10, 'shared', 100), (11, 'shared', 200)) t(id, code, amount)",
        "metrics": [{"name": "revenue", "agg": "sum", "sql": "amount"}],
    }
    assert execute([a, b], dimensions=["a.label"], metrics=["b.revenue"]) == [("group", 300)]


@pytest.mark.parametrize("window_order", [None, "revenue"])
def test_cumulative_temporal_cohort_is_a_partition(window_order):
    model = facts(
        [
            {"name": "revenue", "agg": "sum", "sql": "amount"},
            {"name": "running", "type": "cumulative", "sql": "revenue", "window_order": window_order},
        ],
        [{"name": name, "type": "time", "granularity": "day"} for name in ("day", "cohort")],
        "SELECT * FROM (VALUES (1, DATE '2024-01-01', DATE '2023-12-01', 10), "
        "(2, DATE '2024-01-01', DATE '2023-12-02', 100), "
        "(3, DATE '2024-01-02', DATE '2023-12-01', 20), "
        "(4, DATE '2024-01-02', DATE '2023-12-02', 200)) t(id, day, cohort, amount)",
    )
    rows = execute(
        [model],
        dimensions=["facts.day", "facts.cohort"],
        metrics=["facts.running"],
        order_by=["facts.day", "facts.cohort"],
    )
    assert [row[-1] for row in rows] == [10, 100, 30, 300]


@pytest.mark.parametrize("base", ["revenue", "double_revenue"])
def test_filtered_cumulative_inputs_do_not_change_sibling_populations(base):
    model = facts(
        [
            {"name": "revenue", "agg": "sum", "sql": "amount", "filters": ["amount > 0"]},
            {"name": "double_revenue", "type": "derived", "sql": "revenue * 2"},
            {"name": "running", "type": "cumulative", "sql": "revenue"},
            {"name": "paid_running", "type": "cumulative", "sql": base, "filters": ["category = 'a'"]},
            {"name": "other_running", "type": "cumulative", "sql": "revenue", "filters": ["category = 'b'"]},
        ],
        [{"name": "day", "type": "time", "granularity": "day"}, {"name": "category", "type": "categorical"}],
        "SELECT * FROM (VALUES (1, DATE '2024-01-01', 'a', 5), (2, DATE '2024-01-01', 'b', 100), "
        "(3, DATE '2024-01-02', 'a', 15), (4, DATE '2024-01-02', 'b', 200), "
        "(5, DATE '2024-01-02', 'a', -50)) t(id, day, category, amount)",
    )
    query = {
        "dimensions": ["facts.day"],
        "metrics": ["facts.revenue", "facts.running", "facts.paid_running", "facts.other_running"],
        "order_by": ["facts.day"],
    }
    expected = [(105, 105, 5, 100), (215, 320, 20, 300)]
    if base == "double_revenue":
        expected = [(a, b, c * 2, d) for a, b, c, d in expected]
    # Native YAML compilation may retain intermediate window inputs.
    # Inspect named outputs so these assertions also cover the native API.
    sql = native.compile_with_yaml(yaml.safe_dump({"models": [model]}), yaml.safe_dump(query))
    with duckdb.connect() as conn:
        result = conn.execute(sql)
        names = [column[0] for column in result.description]
        requested = [names.index(name) for name in ("revenue", "running", "paid_running", "other_running")]
        actual = [tuple(row[index] for index in requested) for row in result.fetchall()]
    assert actual == expected


@pytest.mark.parametrize("expression, expected", [("SUM(DISTINCT amount)", 30), ("AVG(DISTINCT amount)", 15)])
def test_native_inline_aggregate_preserves_distinct(expression, expected):
    model = facts(
        [{"name": "value", "sql": expression}],
        source="SELECT * FROM (VALUES (1, 10), (2, 10), (3, 20)) t(id, amount)",
    )
    assert execute([model], metrics=["facts.value"]) == [(expected,)]


def test_distinct_cannot_be_silently_discarded_from_a_row_input():
    model = facts([{"name": "value", "agg": "sum", "sql": "DISTINCT amount"}])
    with pytest.raises((ValueError, RuntimeError), match="scalar expression"):
        execute([model], metrics=["facts.value"])


@pytest.mark.parametrize("input_kind", ["simple", "derived", "ratio", "complete", "window_expression"])
@pytest.mark.parametrize("graph_owner", [False, True])
def test_filtered_cumulative_semantic_handoff_preserves_model_state(input_kind, graph_owner):
    from sidemantic import Dimension, Metric, Model, SemanticLayer

    base = {
        "simple": "revenue",
        "derived": "double_revenue",
        "ratio": "average",
        "complete": "distinct_revenue",
        "window_expression": None,
    }[input_kind]
    with SemanticLayer(engine="rust", fallback=False, auto_register=False) as layer:
        model = Model(
            name="facts",
            primary_key="id",
            sql="SELECT * FROM (VALUES (1, DATE '2024-01-01', 'a', 5), (2, DATE '2024-01-01', 'b', 100), "
            "(3, DATE '2024-01-02', 'a', 15), (4, DATE '2024-01-02', 'b', 200)) t(id, day, category, amount)",
            dimensions=[
                Dimension(name="day", type="time", granularity="day"),
                Dimension(name="category", type="categorical"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="count", agg="count"),
                Metric(name="double_revenue", type="derived", sql="revenue * 2"),
                Metric(name="average", type="ratio", numerator="revenue", denominator="count"),
                Metric(name="distinct_revenue", sql="SUM(DISTINCT amount)", sql_is_complete=True),
                Metric(name="running", type="cumulative", sql="revenue"),
            ],
        )
        wrapper = Metric(
            name="paid_running",
            type="cumulative",
            sql=f"facts.{base}" if graph_owner and base else base,
            window_expression="SUM(base.revenue)" if input_kind == "window_expression" else None,
            filters=["facts.category = 'a'"],
        )
        if not graph_owner:
            model.metrics.append(wrapper)
        layer.add_model(model)
        if graph_owner:
            layer.add_metric(wrapper)
        before = model.model_dump()
        metric = "paid_running" if graph_owner else "facts.paid_running"
        for _ in range(2):
            sql = layer.compile(metrics=["facts.running", metric], dimensions=["facts.day"], order_by=["facts.day"])
            result = layer.adapter.execute(sql)
            names = [column[0] for column in result.description]
            indexes = [names.index(name) for name in ("running", "paid_running")]
            rows = [tuple(row[index] for index in indexes) for row in result.fetchall()]
            assert rows == ([(105, 10), (320, 40)] if input_kind == "derived" else [(105, 5), (320, 20)])
            assert not any(name.startswith("__sidemantic_filtered") for name in names)
            assert model.model_dump() == before
            assert layer.last_engine_selection == {"engine": "rust", "reason": None}


def test_private_cumulative_input_name_is_safe_as_a_caller_alias():
    from sidemantic import Dimension, Metric, Model, SemanticLayer

    with SemanticLayer(engine="rust", fallback=False, auto_register=False) as layer:
        layer.add_model(
            Model(
                name="facts",
                sql="SELECT 1 id, DATE '2024-01-01' AS day, 10 amount, 'a' category",
                primary_key="id",
                dimensions=[Dimension(name="day", type="time", granularity="day")],
                metrics=[
                    Metric(name="revenue", agg="sum", sql="amount"),
                    Metric(name="running", type="cumulative", sql="revenue", filters=["category = 'a'"]),
                ],
            )
        )
        # The first private input uses suffix 2, the number of model metrics.
        sql = layer.compile(
            metrics=["facts.running"],
            dimensions=["facts.day"],
            aliases={"facts.running": "__sidemantic_filtered_2"},
        )
        result = layer.adapter.execute(sql)
        assert [column[0] for column in result.description] == ["day", "__sidemantic_filtered_2"]
        assert result.fetchall()[0][-1] == 10


@pytest.mark.parametrize(
    "subquery",
    [
        "SELECT 1 FROM labels facts_cte WHERE facts_cte.extra = 7",
        "SELECT 1 FROM (SELECT extra FROM labels) facts_cte WHERE facts_cte.extra = 7",
        "WITH facts_cte AS (SELECT extra FROM labels) SELECT 1 FROM facts_cte WHERE facts_cte.extra = 7",
    ],
)
def test_nested_filter_aliases_do_not_add_outer_source_columns(subquery):
    from sidemantic import Metric, Model, SemanticLayer

    with SemanticLayer(engine="rust", fallback=False, auto_register=False) as layer:
        layer.add_model(
            Model(name="facts", table="source_facts", metrics=[Metric(name="revenue", agg="sum", sql="amount")])
        )
        layer.adapter.execute("CREATE TABLE source_facts(amount INT); INSERT INTO source_facts VALUES (10), (20)")
        layer.adapter.execute("CREATE TABLE labels(extra INT); INSERT INTO labels VALUES (7)")
        assert layer.sql(f"SELECT facts.revenue FROM facts WHERE EXISTS ({subquery})").fetchall() == [(30,)]


@pytest.mark.parametrize("qualified", [False, True])
@pytest.mark.parametrize(
    "base_kind",
    [
        "sum",
        "count",
        "derived",
        "ratio",
        "derived_count",
        "count_fill",
        "ratio_fill",
        "complete_count_filter",
        "complete_nullif",
    ],
)
def test_cumulative_filter_keeps_its_owner_when_base_belongs_to_another_model(qualified, base_kind):
    from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer

    with SemanticLayer(engine="rust", fallback=False, auto_register=False) as layer:
        predicate = "facts.category = 'a'" if qualified else "category = 'a'"
        running = Metric(name="running", type="cumulative", sql="other.value", filters=[predicate])
        layer.add_model(
            Model(
                name="facts",
                primary_key="id",
                sql="SELECT * FROM (VALUES (1, DATE '2024-01-01', 'b'), (2, DATE '2024-01-02', 'a'), "
                "(3, DATE '2024-01-03', 'b')) t(id, day, category)",
                dimensions=[
                    Dimension(name="day", type="time", granularity="day"),
                    Dimension(name="category", type="categorical"),
                ],
                relationships=[Relationship(name="other", type="many_to_one", foreign_key="id")],
                metrics=[running],
            )
        )
        layer.add_model(
            Model(
                name="other",
                primary_key="id",
                sql="SELECT * FROM (VALUES (1, 'a', 100), (2, 'b', 5), (3, 'a', 200)) t(id, category, amount)",
                dimensions=[Dimension(name="category", type="categorical")],
                metrics=[
                    Metric(name="total", agg="sum", sql="amount"),
                    Metric(name="count", agg="count"),
                    Metric(
                        name="value",
                        **{
                            "sum": {"agg": "sum", "sql": "amount"},
                            "count": {"agg": "count"},
                            "derived": {"type": "derived", "sql": "total * 2"},
                            "ratio": {"type": "ratio", "numerator": "total", "denominator": "count"},
                            "derived_count": {"type": "derived", "sql": "count + 1"},
                            "count_fill": {"agg": "count", "fill_nulls_with": 99},
                            "ratio_fill": {
                                "type": "ratio",
                                "numerator": "total",
                                "denominator": "count",
                                "fill_nulls_with": 99,
                            },
                            "complete_count_filter": {
                                "sql": "COUNT(*) FILTER (WHERE amount > 0)",
                                "sql_is_complete": True,
                            },
                            "complete_nullif": {"sql": "NULLIF(COUNT(*), 1)", "sql_is_complete": True},
                        }[base_kind],
                    ),
                ],
            )
        )
        reference = "facts.running"
        sql = layer.compile(metrics=["other.value", reference], dimensions=["facts.day"], order_by=["facts.day"])
        result = layer.adapter.execute(sql)
        names = [column[0] for column in result.description]
        indexes = [names.index(name) for name in ("value", "running")]
        actual = [tuple(row[index] for index in indexes) for row in result.fetchall()]
        assert (
            actual
            == {
                "sum": [(100, None), (5, 5), (200, 5)],
                "count": [(1, 0), (1, 1), (1, 1)],
                "derived": [(200, None), (10, 10), (400, 10)],
                "ratio": [(100, None), (5, 5), (200, 5)],
                "derived_count": [(2, 1), (2, 3), (2, 4)],
                "count_fill": [(1, 0), (1, 1), (1, 1)],
                "ratio_fill": [(100, 99), (5, 104), (200, 203)],
                "complete_count_filter": [(1, 0), (1, 1), (1, 1)],
                "complete_nullif": [(None, 0), (None, 0), (None, 0)],
            }[base_kind]
        )


def test_native_inline_input_preserves_model_names_ending_in_cte():
    model = {
        "name": "facts_cte",
        "sql": "SELECT 10 amount",
        "metrics": [{"name": "revenue", "sql": "SUM(DISTINCT amount)"}],
    }
    assert execute([model], metrics=["facts_cte.revenue"]) == [(10,)]


@pytest.mark.parametrize("column", ["category", "{model}.category"])
def test_cumulative_filter_preserves_placeholder_text_in_literals(column):
    from sidemantic import Dimension, Metric, Model, SemanticLayer

    with SemanticLayer(engine="rust", fallback=False, auto_register=False) as layer:
        layer.add_model(
            Model(
                name="facts",
                primary_key="id",
                sql="SELECT 1 id, DATE '2024-01-01' AS day, '{model}' AS category, 5 amount",
                dimensions=[
                    Dimension(name="day", type="time", granularity="day"),
                    Dimension(name="category", type="categorical"),
                ],
                metrics=[
                    Metric(name="revenue", agg="sum", sql="amount"),
                    Metric(name="running", type="cumulative", sql="revenue", filters=[f"{column} = '{{model}}'"]),
                ],
            )
        )
        result = layer.query(metrics=["facts.running"], dimensions=["facts.day"]).fetchall()
        assert result[0][-1] == 5
