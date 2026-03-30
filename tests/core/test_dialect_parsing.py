"""Tests for Sidemantic SQL dialect parsing."""

from sidemantic.core.dialect import (
    is_definition,
    is_dimension_def,
    is_metric_def,
    is_model_def,
    is_relationship_def,
    is_segment_def,
    parse,
)


def _extract_props(defn):
    props = {}
    for expr in defn.expressions:
        key = expr.this.this
        value = expr.expression.this
        props[key] = value
    return props


def test_parse_definitions_and_properties():
    sql = """
    MODEL (
        name orders,
        table orders,
        primary_key order_id
    );

    DIMENSION (
        name status,
        type categorical,
        sql status
    );

    METRIC (
        name revenue,
        expression COALESCE(amount, 0) + 1,
        description 'Total, revenue'
    );

    RELATIONSHIP (
        name customers,
        type many_to_one,
        foreign_key customer_id
    );

    SEGMENT (
        name active,
        expression status = 'active'
    );
    """

    statements = parse(sql)
    assert len(statements) == 5

    assert is_model_def(statements[0])
    assert is_dimension_def(statements[1])
    assert is_metric_def(statements[2])
    assert is_relationship_def(statements[3])
    assert is_segment_def(statements[4])

    # All are definitions
    for stmt in statements:
        assert is_definition(stmt)

    metric_props = _extract_props(statements[2])
    assert metric_props["name"] == "revenue"
    assert metric_props["expression"].replace(" ", "") == "COALESCE(amount,0)+1"
    assert metric_props["description"] == "'Total, revenue'"


def test_parse_property_aliases():
    sql = """
    METRIC (
        name orders_count,
        aggregation count
    );
    """

    statements = parse(sql)
    metric_props = _extract_props(statements[0])
    assert metric_props["aggregation"] == "count"
