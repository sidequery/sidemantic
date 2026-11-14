"""Runtime checks for the latest portable aggregate and window contracts."""

import json

import duckdb
import pytest
import sqlglot

from sidemantic import SemanticLayer
from sidemantic.interchange.ossie import OssieParseOptions, lower_ossie_document, parse_ossie_document
from sidemantic.interchange.ossie.expression_validation import scalar_sql_expression_error
from sidemantic.interchange.ossie.portable import lower_ossie_sql, parse_portable_expression


def _field(name, sql):
    return {"name": name, "expression": {"dialects": [{"dialect": "OSSIE_SQL_2026", "expression": sql}]}}


def _layer(fields, metrics=()):
    document = {
        "version": "0.2.0.dev0",
        "name": "portable",
        "datasets": [{"name": "orders", "source": "orders", "fields": fields}],
        "metrics": list(metrics),
    }
    parsed = parse_ossie_document(
        json.dumps(document).encode(), options=OssieParseOptions(validate_schema=True, target_dialect="duckdb")
    )
    lowered = lower_ossie_document(parsed)
    assert lowered.valid, lowered.diagnostics
    return SemanticLayer.from_catalog(lowered.catalog, engine="python", fallback=False, auto_register=False)


@pytest.mark.parametrize("session_order", ["NULLS_FIRST", "NULLS_LAST"])
@pytest.mark.parametrize(
    "ordering,expected",
    [
        ("amount", [(5, 1), (10, 2), (None, 3)]),
        ("amount DESC", [(None, 1), (10, 2), (5, 3)]),
        ("amount ASC NULLS LAST", [(5, 1), (10, 2), (None, 3)]),
        ("amount ASC NULLS FIRST", [(None, 1), (5, 2), (10, 3)]),
        ("amount DESC NULLS FIRST", [(None, 1), (10, 2), (5, 3)]),
        ("amount DESC NULLS LAST", [(10, 1), (5, 2), (None, 3)]),
    ],
)
def test_window_null_order_survives_runtime_compilation(session_order, ordering, expected):
    layer = _layer([_field("amount", "amount"), _field("rn", f"ROW_NUMBER() OVER (ORDER BY {ordering})")])
    try:
        layer.adapter.execute("CREATE TABLE orders(amount INTEGER); INSERT INTO orders VALUES (10), (NULL), (5)")
        layer.adapter.execute(f"SET default_null_order = '{session_order}'")
        assert layer.query(dimensions=["orders.amount", "orders.rn"], order_by=["orders.rn"]).fetchall() == expected
    finally:
        layer.adapter.close()


def test_filter_aggregates_keep_empty_groups_and_null_semantics():
    layer = _layer(
        [_field("category", "category"), _field("amount", "amount")],
        [
            _field("total", "SUM(orders.amount) FILTER (WHERE orders.amount > 10)"),
            _field("count", "COUNT(*) FILTER (WHERE orders.amount > 10)"),
            _field("distinct", "COUNT(DISTINCT orders.amount) FILTER (WHERE orders.amount > 10)"),
            _field("average", "AVG(orders.amount) FILTER (WHERE orders.amount > 10)"),
        ],
    )
    try:
        layer.adapter.execute(
            "CREATE TABLE orders(category VARCHAR, amount INTEGER); "
            "INSERT INTO orders VALUES ('a', 20), ('a', 20), ('a', NULL), ('b', 5), ('b', NULL)"
        )
        assert layer.query(
            dimensions=["orders.category"],
            metrics=["total", "count", "distinct", "average"],
            order_by=["orders.category"],
        ).fetchall() == [("a", 40, 2, 1, 20.0), ("b", None, 0, 0, None)]
    finally:
        layer.adapter.close()


@pytest.mark.parametrize(
    "expression",
    ["WHERE x > 0", "ROUND(x) FILTER (WHERE x > 0)", "SUM(x) FILTER (WHERE x IN (SELECT x FROM y))"],
)
def test_filter_does_not_relax_scalar_structure(expression):
    with pytest.raises(ValueError):
        parse_portable_expression(expression)
    assert scalar_sql_expression_error(expression, sqlglot_dialect="duckdb") is not None


def test_filter_remains_an_aggregate_for_dataset_field_validation():
    assert "cannot contain aggregates" in scalar_sql_expression_error(
        "SUM(x) FILTER (WHERE x > 0)", sqlglot_dialect="duckdb", row_level=True
    )


@pytest.mark.parametrize("target", ["duckdb", "postgres", "snowflake", "bigquery", "databricks"])
def test_filter_translates_for_each_supported_target(target):
    expression = lower_ossie_sql("COUNT(DISTINCT x) FILTER (WHERE x > 0)", target)
    assert scalar_sql_expression_error(expression, sqlglot_dialect=target) is None


@pytest.mark.parametrize("session_order", ["NULLS_FIRST", "NULLS_LAST"])
def test_explicit_null_order_preserves_range_peer_groups(session_order):
    expression = lower_ossie_sql(
        "COUNT(*) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", "duckdb"
    )
    with duckdb.connect() as connection:
        connection.execute(f"SET default_null_order = '{session_order}'")
        assert connection.execute(
            f"SELECT x, {expression} AS n FROM (VALUES (1),(1),(2),(NULL),(NULL)) t(x) ORDER BY n"
        ).fetchall() == [(1, 2), (1, 2), (2, 3), (None, 5), (None, 5)]


@pytest.mark.parametrize("session_order", ["NULLS_FIRST", "NULLS_LAST"])
def test_bounded_range_preserves_order_arity_through_runtime(session_order):
    layer = _layer(
        [
            _field("amount", "amount"),
            _field("rolling", "SUM(amount) OVER (ORDER BY amount RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)"),
        ]
    )
    try:
        layer.adapter.execute("CREATE TABLE orders(amount INTEGER); INSERT INTO orders VALUES (1), (2), (4), (NULL)")
        layer.adapter.execute(f"SET default_null_order = '{session_order}'")
        sql = layer.compile(dimensions=["orders.amount", "orders.rolling"])
        assert "CASE WHEN" not in sql
        assert "NULLS LAST" in sql
        tree = sqlglot.parse_one(sql, read="duckdb")
        assert all(len(window.args["order"].expressions) == 1 for window in tree.find_all(sqlglot.exp.Window))
        assert layer.query(dimensions=["orders.amount", "orders.rolling"], order_by=["orders.rolling"]).fetchall() == [
            (None, None),
            (1, 1),
            (2, 3),
            (4, 4),
        ]
    finally:
        layer.adapter.close()
