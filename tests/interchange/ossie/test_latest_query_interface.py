"""Execute the unchanged upstream A01 case through every Ossie query route."""

import json
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from sidemantic import SemanticLayer
from sidemantic.cli import app
from sidemantic.interchange.ossie import OssieParseOptions, lower_ossie_document, parse_ossie_document
from sidemantic.sql.query_rewriter import QueryRewriter
from tests.rust_layer_adapter import _rust_request

FIXTURES = Path(__file__).resolve().parents[2] / "ossie-fixtures/latest-query-172a3e5"
ROUTES = ["python", "public_rust", "native_rust"]


def test_physical_window_query_keeps_its_native_session_semantics():
    parsed = parse_ossie_document((FIXTURES / "scott/scott_model.ossie.yaml").read_bytes())
    lowered = lower_ossie_document(parsed, target_dialect="duckdb")
    graph = next(iter(lowered.catalog.scopes.values())).graph
    rewriter = QueryRewriter(graph, use_rust_rewriter=False)
    query = "SELECT ROW_NUMBER() OVER (ORDER BY x) FROM (VALUES (NULL), (1)) AS unrelated(x)"
    assert rewriter.rewrite(query) == query
    assert rewriter.explain(query).rewritten_sql == query


def _execute(route, query, *, empty=False, document=None, null_order=None):
    content = json.dumps(document) if document else (FIXTURES / "scott/scott_model.ossie.yaml").read_text()
    ddl = (FIXTURES / "scott/scott.sql").read_text()
    if empty:
        ddl += "\nDELETE FROM scott.emp;"
    if null_order:
        ddl += f"\nSET default_null_order='{null_order}';"
    if route == "native_rust":
        result = _rust_request(
            {
                "action": "ossie_compile",
                "content": content,
                "serialization": "yaml",
                "target": "DUCKDB",
                "dialect": "duckdb",
                "sql": query,
            }
        )
        if result["status"] != "ok":
            raise ValueError(result["error"])
        with duckdb.connect() as connection:
            connection.execute(ddl)
            return connection.execute(result["sql"]).fetchall()
    parsed = parse_ossie_document(content.encode(), options=OssieParseOptions(validate_schema=True))
    lowered = lower_ossie_document(parsed, target_dialect="duckdb")
    assert lowered.valid, lowered.diagnostics
    engine = "rust" if route == "public_rust" else "python"
    layer = SemanticLayer.from_catalog(lowered.catalog, engine=engine, fallback=False, auto_register=False)
    try:
        layer.adapter.execute(ddl)
        rows = layer.sql(query).fetchall()
        assert layer.last_engine_selection == {"engine": engine, "reason": None}
        return rows
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("route", ROUTES)
def test_upstream_a01_grand_total(route):
    query = (FIXTURES / "a01_grand_total/query.ossie.sql").read_text()
    with duckdb.connect() as connection:
        expected = connection.execute((FIXTURES / "a01_grand_total/expected.sql").read_text()).fetchall()
    assert _execute(route, query) == expected


@pytest.mark.parametrize("route", ROUTES)
def test_empty_relation_preserves_sum_null_and_count_zero(route):
    assert _execute(route, "SELECT AGG(sum_sal), AGG(emp_count) FROM scott_model.emp", empty=True) == [(None, 0)]


@pytest.mark.parametrize("route", ROUTES)
def test_relation_alias_and_regular_identifier_case(route):
    assert _execute(route, "SELECT AGG(E.SUM_SAL) AS salary, AGG(E.EMP_COUNT) AS employees FROM SCOTT_MODEL.EMP E") == [
        (29025, 14)
    ]


@pytest.mark.parametrize("route", ROUTES)
def test_comments_between_measure_function_and_arguments(route):
    assert _execute(route, "SELECT AGG /* measure */ (sum_sal) AS total FROM scott_model.emp") == [(29025,)]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "query",
    [
        "SELECT AGG(sal) FROM scott_model.emp",
        "SELECT AGG(sum_sal) FROM wrong_namespace.emp",
        "SELECT AGG(x.sum_sal) FROM scott_model.emp e",
        "SELECT AGG(wrong_namespace.emp.sum_sal) FROM scott_model.emp",
        "SELECT AGG(sum_sal + 1) FROM scott_model.emp",
        "SELECT AGG(DISTINCT sum_sal) FROM scott_model.emp",
        "SELECT AGG(sum_sal), deptno FROM scott_model.emp",
        "SELECT SUM(AGG(sum_sal)) FROM scott_model.emp",
        "SELECT AGG(sum_sal) FROM scott_model.emp; SELECT 1",
    ],
)
def test_unsupported_measure_queries_fail_before_execution(route, query):
    with pytest.raises(ValueError, match="Ossie AGG|Multiple statements"):
        _execute(route, query)


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("command", ["query", "rewrite"])
def test_upstream_a01_through_cli(engine, command, tmp_path):
    source = FIXTURES / "scott/scott_model.ossie.yaml"
    database = tmp_path / "data.duckdb"
    output = tmp_path / "result.csv"
    with duckdb.connect(str(database)) as connection:
        connection.execute((FIXTURES / "scott/scott.sql").read_text())
    query = (FIXTURES / "a01_grand_total/query.ossie.sql").read_text()
    arguments = [command, "--models", str(source), "--engine", engine, "--no-fallback"]
    if command == "query":
        arguments += ["--connection", f"duckdb:///{database}", "--output", str(output)]
    arguments += ["--", query]
    result = CliRunner().invoke(app, arguments)
    assert result.exit_code == 0, result.output
    if command == "query":
        lines = output.read_text().splitlines()
        assert lines[0] == "sum_sal,emp_count"
        assert lines[1].split(",") == ["29025.00", "14"]
    else:
        with duckdb.connect(str(database)) as connection:
            assert connection.execute(result.stdout).fetchall() == [(29025, 14)]


@pytest.mark.parametrize("route", ROUTES)
def test_quoted_dataset_and_metric_names(route):
    document = {
        "version": "0.2.0.dev0",
        "name": "scope",
        "datasets": [
            {
                "name": '"order facts"',
                "source": "SELECT 10 AS amount UNION ALL SELECT 20 AS amount",
                "fields": [{"name": "amount", "expression": {"sql": "amount"}}],
                "metrics": [{"name": '"total.amount"', "expression": {"sql": "SUM(amount)"}}],
            }
        ],
    }
    assert _execute(route, 'SELECT AGG("total.amount") AS total FROM scope."order facts"', document=document) == [(30,)]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("null_order", ["NULLS_FIRST", "NULLS_LAST"])
@pytest.mark.parametrize(
    "ordering,expected",
    [
        ("ASC NULLS LAST", [(5, 1), (10, 2), (None, 3)]),
        ("DESC", [(None, 1), (10, 2), (5, 3)]),
    ],
)
def test_sql_rewrite_preserves_portable_window_order(route, null_order, ordering, expected):
    document = {
        "version": "0.2.0.dev0",
        "name": "window_scope",
        "datasets": [
            {
                "name": "orders",
                "source": "SELECT 10 AS amount UNION ALL SELECT NULL UNION ALL SELECT 5",
                "fields": [
                    {"name": "amount", "expression": {"sql": "amount"}},
                    {"name": "rn", "expression": {"sql": f"ROW_NUMBER() OVER (ORDER BY amount {ordering})"}},
                ],
            }
        ],
    }
    assert (
        _execute(route, "SELECT amount, rn FROM orders ORDER BY rn", document=document, null_order=null_order)
        == expected
    )
