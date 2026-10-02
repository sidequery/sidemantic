from __future__ import annotations

import json

import pytest

from sidemantic import SemanticLayer
from sidemantic.interchange.ossie import OssieParseOptions, lower_ossie_document, parse_ossie_document


def _expression(sql: str, dialect: str = "ANSI_SQL") -> dict:
    return {"dialects": [{"dialect": dialect, "expression": sql}]}


def _lower(
    *,
    sql: str = "SUM(orders.amount)",
    field_sql: str = "amount * 2",
    flat: bool = False,
    dataset: str = "Orders",
    field: str = "Amount",
    extra_metrics: list | None = None,
    source_sql: str = "SELECT 10 AS amount UNION ALL SELECT 20 AS amount",
):
    scope = {
        "name": "commerce",
        "datasets": [
            {
                "name": dataset,
                "source": source_sql,
                "fields": [{"name": field, "expression": _expression(field_sql)}],
            }
        ],
        "metrics": [{"name": "total", "expression": _expression(sql)}, *(extra_metrics or [])],
    }
    source = {"version": "0.2.0.dev0", **scope} if flat else {"version": "0.2.0.dev0", "semantic_model": [scope]}
    parsed = parse_ossie_document(json.dumps(source).encode(), options=OssieParseOptions(validate_schema=True))
    return lower_ossie_document(parsed, target_dialect="duckdb")


@pytest.mark.parametrize("flat", [False, True])
@pytest.mark.parametrize("sql", ["SUM(orders.amount)", 'SUM("ORDERS"."AMOUNT")', "SUM(Orders.Amount)"])
def test_metric_references_bind_computed_fields_before_physical_fallback(sql, flat):
    result = _lower(sql=sql, flat=flat)
    assert result.valid, result.diagnostics
    layer = SemanticLayer.from_catalog(result.catalog, engine="python", fallback=False, auto_register=False)
    assert layer.query(metrics=["total"]).fetchall() == [(60,)]


def test_quoted_declarations_bind_without_losing_their_identity():
    result = _lower(dataset='"orders"', field='"amount"', sql='SUM("orders"."amount")')
    assert result.valid, result.diagnostics
    layer = SemanticLayer.from_catalog(result.catalog, engine="python", fallback=False, auto_register=False)
    assert layer.query(metrics=["total"]).fetchall() == [(60,)]


@pytest.mark.parametrize("name", ["order items", "orders.total", "2orders", '"broken', '"bad"quote"', '""'])
@pytest.mark.parametrize("flat", [False, True])
def test_malformed_identifier_declarations_fail_before_compilation(name, flat):
    result = _lower(dataset=name, flat=flat)
    assert not result.valid
    assert any(d.code == "ossie.semantic.identifier.invalid" for d in result.diagnostics)


@pytest.mark.parametrize("source", ["SELECT", "SELECT FROM orders"])
@pytest.mark.parametrize("flat", [False, True])
def test_empty_query_source_projection_is_rejected(source, flat):
    result = _lower(source_sql=source, flat=flat)
    assert not result.valid


def test_quoted_dataset_does_not_match_regular_declaration():
    result = _lower(sql='SUM("orders".amount)')
    assert not result.valid
    assert not result.catalog.scope_ids
    assert any(d.code == "ossie.lowering.metric_unexecutable" for d in result.diagnostics)


@pytest.mark.parametrize("field_sql", ["SUM(amount)", "AVG(amount)", "COUNT(*)", "amount AS renamed", "*"])
@pytest.mark.parametrize("flat", [False, True])
def test_invalid_row_expressions_are_rejected_before_querying(field_sql, flat):
    result = _lower(field_sql=field_sql, flat=flat)
    assert not result.valid
    assert not result.catalog.scope_ids
    diagnostic = next(d for d in result.diagnostics if d.code == "ossie.lowering.expression_invalid")
    assert diagnostic.json_pointer == ("" if flat else "/semantic_model/0") + "/datasets/0/fields/0/expression"


def test_metric_dependencies_expand_after_normalized_lookup():
    result = _lower(sql="BASE * 2", extra_metrics=[{"name": "base", "expression": _expression("SUM(orders.amount)")}])
    assert result.valid, result.diagnostics
    layer = SemanticLayer.from_catalog(result.catalog, engine="python", fallback=False, auto_register=False)
    assert layer.query(metrics=["total"]).fetchall() == [(120,)]


def test_metric_cycles_are_rejected():
    result = _lower(sql="base * 2", extra_metrics=[{"name": "base", "expression": _expression("total / 2")}])
    assert not result.valid
    assert any("Cyclic metric reference" in d.message for d in result.diagnostics)


def test_quoted_and_regular_dataset_names_do_not_merge_runtime_identity():
    source = {
        "version": "0.2.0.dev0",
        "name": "commerce",
        "datasets": [
            {
                "name": name,
                "source": f"SELECT {amount} AS amount",
                "fields": [{"name": "amount", "expression": _expression("amount")}],
            }
            for name, amount in [("Orders", 10), ('"Orders"', 20)]
        ],
        "metrics": [
            {"name": "regular", "expression": _expression("SUM(orders.amount)")},
            {"name": "quoted", "expression": _expression('SUM("Orders".amount)')},
        ],
    }
    result = lower_ossie_document(parse_ossie_document(json.dumps(source).encode()), target_dialect="duckdb")
    assert result.valid, result.diagnostics
    graph = result.catalog["commerce"].graph
    assert len(graph.models) == 2
    assert {model.metadata["ossie_source_name"] for model in graph.models.values()} == {"Orders", '"Orders"'}
    layer = SemanticLayer.from_catalog(result.catalog, engine="python", fallback=False, auto_register=False)
    assert layer.query(metrics=["regular"]).fetchall() == [(10,)]
    assert layer.query(metrics=["quoted"]).fetchall() == [(20,)]
