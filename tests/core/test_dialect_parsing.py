"""Tests for Sidemantic SQL dialect parsing."""

from sidemantic.core.dialect import DimensionDef, MetricDef, ModelDef, RelationshipDef, SegmentDef, parse


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

    assert isinstance(statements[0], ModelDef)
    assert isinstance(statements[1], DimensionDef)
    assert isinstance(statements[2], MetricDef)
    assert isinstance(statements[3], RelationshipDef)
    assert isinstance(statements[4], SegmentDef)

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
