"""Regression tests for Snowflake expression parsing boundaries."""

from sidemantic.adapters.snowflake import SnowflakeAdapter, _qualify_columns


def test_qualify_columns_only_rewrites_real_snowflake_columns():
    assert _qualify_columns("CAST(amount AS NUMBER)") == "CAST({model}.amount AS NUMBER)"
    assert _qualify_columns("amount::NUMBER") == "{model}.amount::NUMBER"
    assert _qualify_columns("EXTRACT(YEAR FROM created_at)") == "EXTRACT(YEAR FROM {model}.created_at)"
    assert _qualify_columns("CURRENT_DATE") == "CURRENT_DATE"
    assert _qualify_columns('"order amount" + tax') == '{model}."order amount" + {model}.tax'
    assert _qualify_columns("amount /* tax */ + fee") == "{model}.amount /* tax */ + {model}.fee"
    assert _qualify_columns("note = $$amount$$") == "{model}.note = $$amount$$"
    assert _qualify_columns("note = '{model}' AND amount > 0") == ("{model}.note = '{model}' AND {model}.amount > 0")
    assert _qualify_columns("{model}.amount + tax") == "{model}.amount + {model}.tax"
    assert _qualify_columns("${TABLE}.amount + tax") == "${TABLE}.amount + {model}.tax"
    assert _qualify_columns("(SELECT MAX(amount) FROM audit)") == "(SELECT MAX(amount) FROM audit)"


def test_simple_aggregate_detection_uses_the_parsed_outer_expression():
    adapter = SnowflakeAdapter()
    nested = adapter._parse_metric({"name": "net", "expr": "SUM(COALESCE(amount, 0) - discount)"})
    quoted = adapter._parse_metric({"name": "labeled", "expr": 'SUM("order amount")'})
    aggregate_text = adapter._parse_metric(
        {"name": "literal_text", "expr": "IFF(note = 'SUM(amount)', SUM(amount), 0)"}
    )

    assert nested.agg == "sum" and nested.sql == "COALESCE(amount, 0) - discount"
    assert quoted.agg == "sum" and quoted.sql == '"order amount"'
    assert aggregate_text.type == "derived"
    assert aggregate_text.sql == "IFF({model}.note = 'SUM(amount)', SUM({model}.amount), 0)"
