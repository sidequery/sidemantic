"""One result contract across the Python frontend, public Rust, and native import.

Expected results come from fixed inputs and independent SQL, never from choosing
one of the Sidemantic engines as the oracle for the other.
"""

import json
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from sidemantic import SemanticLayer
from sidemantic.cli import app
from sidemantic.interchange.ossie import OssieParseOptions, lower_ossie_document, parse_ossie_document
from tests.rust_layer_adapter import RustRuntimeSemanticLayer, _rust_request

ROUTES = ["python", "public_rust", "native_rust"]
PORTABLE_CASES = json.loads((Path(__file__).parent / "fixtures/portable_expressions.json").read_text())


def _field(name, sql=None, dialect="ANSI_SQL"):
    return {"name": name, "expression": {"dialects": [{"dialect": dialect, "expression": sql or name}]}}


def _document(datasets, metrics=(), relationships=(), *, shape="current"):
    model = {
        "name": "commerce",
        "datasets": datasets,
        "metrics": list(metrics),
        "relationships": list(relationships),
    }
    if shape == "legacy":
        return {"version": "0.2.0.dev0", "semantic_model": [model]}
    return {"version": "0.2.0.dev0", **model}


def _query(document, route, query, ddl=""):
    content = json.dumps(document)
    if route == "native_rust":
        response = _rust_request(
            {
                "action": "ossie_compile",
                "content": content,
                "serialization": "json",
                "target": "DUCKDB",
                "dialect": "duckdb",
                **query,
            }
        )
        assert response["status"] == "ok", response
        with duckdb.connect() as connection:
            if ddl:
                connection.execute(ddl)
            return connection.execute(response["sql"]).fetchall()

    parsed = parse_ossie_document(
        content.encode(), options=OssieParseOptions(target_dialect="duckdb", validate_schema=True)
    )
    lowered = lower_ossie_document(parsed)
    assert lowered.valid, lowered.diagnostics
    if route == "public_rust":
        pytest.importorskip("sidemantic_rs", reason="Requires a source-built Rust extension")
        layer = RustRuntimeSemanticLayer.from_catalog(lowered.catalog, auto_register=False)
    else:
        layer = SemanticLayer.from_catalog(lowered.catalog, engine="python", fallback=False, auto_register=False)
    try:
        if ddl:
            layer.adapter.execute(ddl)
        rows = layer.query(**query).fetchall()
        assert layer.last_engine_selection == {"engine": "rust" if route == "public_rust" else "python", "reason": None}
        return rows
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("shape", ["current", "legacy"])
@pytest.mark.parametrize("name,reference", [("amount", "amount"), ("Amount", "amount"), ("gross", "gross")])
def test_logical_aggregate_inputs_have_one_meaning(route, shape, name, reference):
    document = _document(
        [
            {
                "name": "orders",
                "source": "select 10 as amount union all select 20 as amount",
                "fields": [_field(name, "amount * 2")],
            }
        ],
        [_field("doubled", "TOTAL * 2"), _field("total", f"SUM(orders.{reference})")],
        shape=shape,
    )
    assert _query(document, route, {"metrics": ["total", "doubled"]}) == [(60, 120)]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("shape", ["current", "legacy"])
def test_undeclared_physical_aggregate_input_is_preserved(route, shape):
    document = _document(
        [{"name": "orders", "source": "select 10 as amount union all select 20 as amount"}],
        [_field("total", "SUM(orders.amount)")],
        shape=shape,
    )
    assert _query(document, route, {"metrics": ["total"]}) == [(30,)]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("shape", ["current", "legacy"])
def test_quoted_and_regular_names_preserve_separate_identity(route, shape):
    document = _document(
        [
            {
                "name": name,
                "source": f"select {amount} as amount",
                "fields": [_field('"gross amount"', "amount * 2")],
            }
            for name, amount in [("Orders", 10), ('"Orders"', 20), ('"order ""details"""', 30)]
        ],
        [
            _field("regular", 'SUM(orders."gross amount")'),
            _field("quoted", 'SUM("Orders"."gross amount")'),
            _field("escaped", 'SUM("order ""details"""."gross amount")'),
        ],
        shape=shape,
    )
    for metric, expected in [("regular", 20), ("quoted", 40), ("escaped", 60)]:
        assert _query(document, route, {"metrics": [metric]}) == [(expected,)]


def _joined_document(shape):
    return _document(
        [
            {
                "name": "orders",
                "source": "orders",
                "primary_key": ["id"],
                "fields": [_field("id"), _field("customer_ref", "customer_id + 1"), _field("amount")],
            },
            {
                "name": "customers",
                "source": "customers",
                "primary_key": ["id"],
                "fields": [_field("id"), _field("name"), _field("budget")],
            },
        ],
        [
            _field(name, sql)
            for name, sql in {
                "revenue": "SUM(orders.amount)",
                "budget": "SUM(customers.budget)",
                "customer_count": "COUNT(customers.id)",
                "average": "AVG(customers.budget)",
                "ratio": "SUM(orders.amount) / SUM(customers.budget)",
                "weighted": "SUM(orders.amount * customers.budget)",
                "combined": "weighted + budget",
            }.items()
        ],
        [
            {
                "name": "customer",
                "from": "orders",
                "to": "customers",
                "from_columns": ["customer_ref"],
                "to_columns": ["id"],
            }
        ],
        shape=shape,
    )


JOINED_DDL = """
create table orders(id int, customer_id int, amount int);
insert into orders values (1,0,10),(2,0,20),(3,1,30);
create table customers(id int, name varchar, budget int);
insert into customers values (1,'A',100),(2,'B',200);
"""


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("shape", ["current", "legacy"])
def test_joined_population_and_independent_grains(route, shape):
    document = _joined_document(shape)
    for metrics, expected in [
        (["budget", "customer_count", "average"], [(300, 2, 150)]),
        (["revenue", "budget", "customer_count", "average", "ratio"], [(60, 300, 2, 150, 0.2)]),
        (["weighted", "revenue", "budget", "combined"], [(9000, 60, 300, 9300)]),
    ]:
        assert _query(document, route, {"metrics": metrics}, JOINED_DDL) == expected
    assert _query(
        document,
        route,
        {
            "metrics": ["weighted", "revenue", "budget"],
            "dimensions": ["customers.name"],
            "order_by": ["customers.name"],
        },
        JOINED_DDL,
    ) == [("A", 3000, 30, 100), ("B", 6000, 30, 200)]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("case", PORTABLE_CASES, ids=lambda case: case["expression"])
def test_portable_required_expression_results(route, case):
    expression = case["expression"]
    source = (
        f"select row_number() over () as row_id, x from {case['from_sql']}"
        if "from_sql" in case
        else "select 1 as row_id"
    )
    fields = [_field("row_id")]
    if "from_sql" in case:
        fields.append(_field("x"))
    metrics = []
    if "from_sql" in case and "OVER" not in expression.upper():
        metrics.append(_field("result", expression, "OSSIE_SQL_2026"))
        # COUNT(*) has no field reference; this explicit population dimension
        # gives it the same dataset context as all other aggregate cases.
        fields.append(_field("population", "1"))
        query = {"metrics": ["result"], "dimensions": ["samples.population"]}
    else:
        fields.append(_field("result", expression, "OSSIE_SQL_2026"))
        # Keep a row identity so repeated window values remain separate rows.
        query = {"dimensions": ["samples.row_id", "samples.result"], "order_by": ["samples.row_id"]}
    document = _document([{"name": "samples", "source": source, "primary_key": ["row_id"], "fields": fields}], metrics)
    actual = [row[-1] for row in _query(document, route, query)]
    actual = [
        value.isoformat()
        if isinstance(value, (datetime, date, time))
        else float(value)
        if isinstance(value, Decimal)
        else value
        for value in actual
    ]
    if case.get("sort_results"):
        actual.sort()
    assert len(actual) == len(case["expected"]), actual
    for value, expected in zip(actual, case["expected"], strict=True):
        assert value == (pytest.approx(expected) if isinstance(expected, float) else expected), (expression, actual)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "expression",
    ["CURRENT_DATE", "CURRENT_DATE()", "CURRENT_TIME", "CURRENT_TIME()", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()"],
)
def test_portable_current_date_and_time_results(route, expression):
    document = _document(
        [{"name": "clock", "source": "select 1 as id", "fields": [_field("result", expression, "OSSIE_SQL_2026")]}]
    )
    reference_expression = expression.removesuffix("()")
    if reference_expression == "CURRENT_TIME":
        reference_expression = "cast(current_time as time)"
    with duckdb.connect() as reference:
        before = reference.execute(f"select {reference_expression}").fetchone()[0]
        actual = _query(document, route, {"dimensions": ["clock.result"]})[0][0]
        after = reference.execute(f"select {reference_expression}").fetchone()[0]
    assert type(actual) is type(before)
    if isinstance(before, time) and before > after:
        assert actual >= before or actual <= after
    else:
        assert before <= actual <= after


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("command", ["query", "rewrite"])
@pytest.mark.parametrize("shape", ["current", "legacy"])
def test_cli_ossie_query_and_rewrite_execute_same_results(engine, command, shape, tmp_path):
    if engine == "rust":
        pytest.importorskip("sidemantic_rs", reason="Requires a source-built Rust extension")
    models = tmp_path / "models"
    models.mkdir()
    document = _document(
        [{"name": "orders", "source": "orders", "fields": [_field("Amount", "amount * 2")]}],
        [_field("total", "SUM(orders.amount)")],
        shape=shape,
    )
    (models / "current.ossie.json").write_text(json.dumps(document))
    database = tmp_path / "data.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("create table orders(amount int); insert into orders values (10),(20)")
    arguments = [command, "select total from metrics", "--models", str(models), "--engine", engine, "--no-fallback"]
    if command == "query":
        arguments += ["--connection", f"duckdb:///{database}", "--output", str(tmp_path / "result.csv")]
    result = CliRunner().invoke(app, arguments)
    assert result.exit_code == 0, result.output
    if command == "query":
        assert (tmp_path / "result.csv").read_text().splitlines() == ["total", "60"]
    else:
        with duckdb.connect(str(database)) as connection:
            assert connection.execute(result.stdout).fetchall() == [(60,)]
