"""Execute both compilers against the independent, existing row processor."""

import json
from pathlib import Path

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.core.table_calculation import TableCalculation
from sidemantic.sql.selected_table_calculations import wrap_table_calculations
from sidemantic.sql.table_calc_processor import TableCalculationProcessor

FIXTURE = Path(__file__).parents[2] / "sidemantic-rs/tests/fixtures/selected_calculations.json"
DATA = json.loads(FIXTURE.read_text())


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Requires real native extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="samples",
            table="samples",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric"), Dimension(name="bucket", type="categorical")],
            metrics=[Metric(name="value", agg="sum", sql="amount")],
        )
    )
    for definition in DATA["source"]["table_calculations"]:
        layer.graph.add_table_calculation(TableCalculation(**definition))
    layer.adapter.execute(DATA["seed"])
    yield layer
    layer.adapter.close()


def assert_rows(actual, expected):
    assert len(actual) == len(expected)
    for actual_row, expected_row in zip(actual, expected):
        assert len(actual_row) == len(expected_row)
        for actual_value, expected_value in zip(actual_row, expected_row):
            if isinstance(expected_value, (int, float)):
                assert actual_value == pytest.approx(expected_value)
            else:
                assert actual_value == expected_value


@pytest.mark.parametrize(
    "population",
    [
        {},
        {"limit": 4, "offset": 2},
        {"filters": ["samples.id < 0"]},
        {"filters": ["samples.id IN (2,4)"]},
        {"filters": ["samples.id IN (3,5)"]},
    ],
)
def test_all_calculations_match_independent_processor(layer, population):
    query = {**DATA["query"], **population}
    names = query.pop("table_calculations")
    base = layer.query(**query)
    original = base.fetchall()
    columns = [column[0] for column in base.description]
    expected, _ = TableCalculationProcessor([layer.graph.table_calculations[n] for n in names]).process(
        original, columns
    )
    actual = layer.query(**query, table_calculations=names)
    assert_rows(actual.fetchall(), expected)
    assert [column[0] for column in actual.description] == columns + names
    assert layer.last_engine_selection["engine"] == layer.engine


def test_postprocess_keeps_paginated_calculations(layer):
    query = {**DATA["query"], "limit": 3, "offset": 2}
    actual = layer.query(**query, post_process="SELECT * FROM ({inner}) AS computed").fetchall()
    assert_rows(actual, layer.query(**query).fetchall())


@pytest.mark.parametrize("layer", ["python"], indirect=True)
@pytest.mark.parametrize("alias", ["gross", "Gross Revenue"])
@pytest.mark.parametrize("qualified", [True, False])
def test_sequential_calculations_order_by_custom_output_aliases(layer, alias, qualified):
    calculations = [
        TableCalculation(name="running", type="running_total", field=alias),
        TableCalculation(name="ranked", type="rank", field=alias),
        TableCalculation(name="sequence", type="row_number"),
        TableCalculation(name="moving", type="moving_average", field=alias, window_size=2),
        TableCalculation(name="previous", type="percent_of_previous", field=alias),
    ]
    for calculation in calculations:
        layer.graph.table_calculations[calculation.name] = calculation
    query = {
        "metrics": ["samples.value"],
        "dimensions": ["samples.id"],
        "aliases": {"samples.value": alias, "samples.id": "row_id"},
        "order_by": ["samples.value DESC", "samples.id ASC"] if qualified else ["value DESC", "id ASC"],
        "limit": 5,
        "offset": 1,
    }
    base = layer.adapter.execute(layer.compile(**query))
    columns = [column[0] for column in base.description]
    expected, expected_columns = TableCalculationProcessor(calculations).process(base.fetchall(), columns)
    actual = layer.adapter.execute(
        layer.compile(**query, table_calculations=[calculation.name for calculation in calculations])
    )
    assert [column[0] for column in actual.description] == expected_columns
    assert_rows(actual.fetchall(), expected)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "later",
        "duplicate",
        "collision",
        "no_order",
        "partition",
        "order",
        "window",
        "percentile",
        "formula",
        "synthetic_ref",
        "infinite",
    ],
)
def test_invalid_selections_fail_before_execution(layer, mutation):
    query = dict(DATA["query"])
    if mutation == "missing":
        query["table_calculations"] = ["absent"]
    elif mutation == "later":
        query["table_calculations"] = ["dependent", "running"]
    elif mutation == "duplicate":
        query["table_calculations"] = ["running", "running"]
    elif mutation == "collision":
        layer.graph.add_table_calculation(TableCalculation(name="value", type="row_number"))
        query["table_calculations"] = ["value"]
    elif mutation == "no_order":
        query["order_by"] = []
    elif mutation in {"partition", "order"}:
        setattr(layer.graph.table_calculations["running"], mutation + "_by", ["id"])
    elif mutation == "window":
        layer.graph.table_calculations["moving"].window_size = 0
    elif mutation == "percentile":
        layer.graph.table_calculations["median"].percentile = 2
    else:
        layer.graph.table_calculations["formula"].expression = {
            "formula": "${value} ** 2",
            "synthetic_ref": "${value} + _ref0",
            "infinite": "1e309",
        }[mutation]
    with pytest.raises(ValueError):
        layer.compile(**query)


def test_helper_names_do_not_shadow_physical_tables(layer):
    layer.adapter.execute('CREATE TABLE "__SIDEMANTIC_CALC_base" AS SELECT * FROM samples')
    layer.graph.models["samples"].table = "__SIDEMANTIC_CALC_base"
    query = dict(DATA["query"])
    actual = layer.query(**query).fetchall()
    assert_rows(actual, DATA["expected"])


def test_python_wrapper_has_no_hidden_columns_or_numeric_identifier_injection():
    calc = TableCalculation(name="n", type="formula", expression="${value} + _ref0")
    with pytest.raises(ValueError, match="formulas"):
        wrap_table_calculations("select 2 as value", {"n": calc}, ["n"], [], "duckdb")


@pytest.mark.parametrize("mutation", [None, "unknown", "dependency", "formula", "unknown_option"])
def test_canonical_validation_checks_active_calculations(mutation):
    runtime = pytest.importorskip("sidemantic_rs", reason="Requires real native extension")
    source = json.loads(json.dumps(DATA["source"]))
    query = dict(DATA["query"])
    if mutation == "unknown":
        query["table_calculations"] = ["missing"]
    elif mutation == "dependency":
        query["table_calculations"] = ["dependent"]
    elif mutation == "formula":
        source["table_calculations"][0]["expression"] = "${value} ** 2"
    elif mutation == "unknown_option":
        source["table_calculations"][0]["unknown_window"] = "future"
    if mutation is None:
        assert runtime.validate_with_semantic_input(json.dumps(source), json.dumps(query)) == []
    else:
        error = runtime.UnsupportedSemanticFeaturesError if mutation == "formula" else ValueError
        with pytest.raises(error):
            runtime.validate_with_semantic_input(json.dumps(source), json.dumps(query))
        with pytest.raises(error):
            runtime.compile_with_semantic_input(json.dumps(source), json.dumps(query))


@pytest.mark.parametrize(
    "population",
    [{}, {"limit": 4, "offset": 2}, {"filters": ["samples.id IN (2,4)"]}, {"filters": ["samples.id IN (3,5)"]}],
)
def test_postgres_calculation_results_match_processor(layer, population):
    import os

    dsn = os.environ.get("SIDEMANTIC_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("PostgreSQL execution requires SIDEMANTIC_TEST_POSTGRES_DSN")
    import psycopg

    query = {**DATA["query"], **population, "dialect": "postgres"}
    names = query.pop("table_calculations")
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(DATA["seed"].replace("create table samples", "create temporary table samples"))
        base = connection.execute(layer.compile(**query))
        columns = [column.name for column in base.description]
        expected, _ = TableCalculationProcessor([layer.graph.table_calculations[n] for n in names]).process(
            base.fetchall(), columns
        )
        actual = connection.execute(layer.compile(**query, table_calculations=names))
        assert_rows(actual.fetchall(), expected)
        assert [column.name for column in actual.description] == columns + names


@pytest.mark.parametrize("formula", ["(" * 64 + "1" + ")" * 64, "-" * 64 + "1", "1e309", "01", "${value} % 2"])
def test_bounded_formula_contract(layer, formula):
    layer.graph.table_calculations["formula"].expression = formula
    with pytest.raises(ValueError):
        layer.compile(**DATA["query"])


def test_calculations_use_only_authorized_population(layer):
    from sidemantic import SecurityPolicy
    from sidemantic.core.semantic_layer import SecurityError
    from sidemantic.semantic_handoff import graph_to_semantic_input

    layer.graph.models["samples"].security = SecurityPolicy(row_filters=["id <= {{ user.max_id }}"])
    layer.graph.models["samples"].invariant_filters = ["id != 1"]
    before = graph_to_semantic_input(layer.graph)
    with pytest.raises(SecurityError):
        layer.compile(**DATA["query"])
    query = {**DATA["query"], "user_attributes": {"max_id": 4}}
    names = query.pop("table_calculations")
    base = layer.query(**query)
    expected, _ = TableCalculationProcessor([layer.graph.table_calculations[n] for n in names]).process(
        base.fetchall(), [c[0] for c in base.description]
    )
    assert_rows(layer.query(**query, table_calculations=names).fetchall(), expected)
    assert graph_to_semantic_input(layer.graph) == before
