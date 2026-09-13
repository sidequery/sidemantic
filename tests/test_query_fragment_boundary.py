"""Request expressions cannot add physical reads or escape their SQL clause."""

import pytest

from sidemantic import Dimension, Metric, Model, Parameter, SemanticLayer
from sidemantic.core.parameter import ParameterSet
from sidemantic.sql.fragment import parse_query_fragment


@pytest.fixture
def query_layer():
    layer = SemanticLayer()
    layer.conn.execute("create table events_raw(id integer, user_id integer, event_type varchar, timestamp timestamp)")
    layer.conn.execute("insert into events_raw values (1, 1, 'signup', '2024-01-01'), (2, 1, 'purchase', '2024-01-02')")
    layer.add_model(
        Model(
            name="events",
            table="events_raw",
            primary_key="id",
            dimensions=[
                Dimension(name="user_id", type="numeric"),
                Dimension(name="event_type", type="categorical"),
                Dimension(name="timestamp", type="time", granularity="day"),
            ],
            metrics=[
                Metric(name="count", agg="count"),
                Metric(name="running", type="cumulative", sql="events.count"),
                Metric(
                    name="funnel",
                    type="conversion",
                    entity="user_id",
                    steps=["event_type = 'signup'", "event_type = 'purchase'"],
                ),
                Metric(
                    name="retention",
                    type="retention",
                    entity="user_id",
                    cohort_event="event_type = 'signup'",
                    activity_event="event_type = 'purchase'",
                    periods=3,
                    retention_granularity="day",
                ),
                Metric(
                    name="conversion",
                    type="conversion",
                    entity="user_id",
                    base_event="signup",
                    conversion_event="purchase",
                    conversion_window="30 days",
                ),
            ],
        )
    )
    return layer


@pytest.mark.parametrize(
    "predicate",
    [
        "EXISTS (SELECT 1 FROM secret_table)",
        "events.user_id IN (SELECT user_id FROM events_raw)",
        "EXISTS (SELECT 1 FROM read_csv('/tmp/private.csv'))",
        "1 = 1 UNION SELECT 1",
        "1 = 1 ORDER BY 1",
        "1 = 1; SELECT 2",
        "(SELECT pg_read_file('/tmp/private')) IS NOT NULL",
        "readfile('/tmp/private') IS NOT NULL",
        "lo_get(123) IS NOT NULL",
        "custom_schema.abs('/tmp/private') IS NOT NULL",
    ],
)
def test_request_filters_reject_new_sql_sources(query_layer, predicate):
    with pytest.raises(ValueError):
        query_layer.query(metrics=["events.count"], filters=[predicate])


@pytest.mark.parametrize(
    "order",
    [
        "conversion; SELECT 2",
        "conversion DESC LIMIT 1",
        "(SELECT user_id FROM events_raw)",
        "random()",
        "missing",
        "conversion, user_id",
    ],
)
def test_specialized_order_rejects_sql_and_nonoutputs(query_layer, order):
    with pytest.raises(ValueError):
        query_layer.query(metrics=["events.conversion"], order_by=[order])


@pytest.mark.parametrize("metric,field", [("retention", "retention_pct"), ("funnel", "funnel"), ("running", "running")])
def test_other_specialized_paths_reject_escaped_orders(query_layer, metric, field):
    dimensions = ["events.timestamp"] if metric == "running" else []
    with pytest.raises(ValueError):
        query_layer.query(metrics=["events." + metric], dimensions=dimensions, order_by=[field + "; select 2"])
    with pytest.raises(ValueError):
        query_layer.query(metrics=["events." + metric], dimensions=dimensions, order_by=["random()"])
    assert query_layer.query(
        metrics=["events." + metric], dimensions=dimensions, order_by=[field + " DESC NULLS LAST"]
    ).fetchall()


def test_legitimate_specialized_order_and_filter(query_layer):
    rows = query_layer.query(
        metrics=["events.conversion"],
        order_by=["events.conversion DESC NULLS LAST"],
        filters=["coalesce(events.user_id, 0) >= 1"],
    ).fetchall()
    assert rows == [(1.0,)]


def test_scalar_subquery_and_literal_delimiters_remain_expressions(query_layer):
    assert query_layer.query(
        metrics=["events.count"],
        filters=[
            "EXISTS (SELECT 1 WHERE 2 > 1)",
            "events.event_type != '; -- FROM secret_table'",
        ],
    ).fetchall() == [(2,)]
    parse_query_fragment("CASE WHEN events.user_id > 0 THEN events.user_id ELSE 0 END DESC", "duckdb", order_by=True)


@pytest.mark.parametrize("quoted", [False, True])
@pytest.mark.parametrize("kind", ["string", "date"])
def test_control_flow_emits_literal_values(query_layer, quoted, kind):
    value = "2024-01-01' OR 1=1 --"
    params = {"value": Parameter(name="value", type=kind), "enabled": Parameter(name="enabled", type="yesno")}
    placeholder = "'{{ value }}'" if quoted else "{{ value }}"
    rendered = ParameterSet(params, {"value": value, "enabled": True}).interpolate(
        "{% if enabled %}" + placeholder + "{% else %}'disabled'{% endif %}"
    )
    assert query_layer.conn.execute("select " + rendered).fetchone() == (value,)
    assert (
        ParameterSet(params, {"value": value, "enabled": False}).interpolate(
            "{% if enabled %}" + placeholder + "{% else %}'disabled'{% endif %}"
        )
        == "'disabled'"
    )


def test_loop_outputs_and_numeric_comparisons_keep_values(query_layer):
    params = {
        "values": Parameter(name="values", type="unquoted"),
        "threshold": Parameter(name="threshold", type="number"),
    }
    rendered = ParameterSet(params, {"values": ["O'Reilly", "'); SELECT 1; --"], "threshold": 2}).interpolate(
        "{% if threshold > 1 %}select {% for item in values %}'{{ item }}'{% if not loop.last %}, {% endif %}{% endfor %}{% endif %}"
    )
    assert query_layer.conn.execute(rendered).fetchone() == ("O'Reilly", "'); SELECT 1; --")
    with pytest.raises(ValueError):
        ParameterSet(params, {"threshold": "0 OR 1=1"}).interpolate("{# comment #}{{ threshold }}")


def test_date_simple_interpolation_is_one_literal(query_layer):
    value = "2024-01-01' OR 1=1 --"
    params = {"date": Parameter(name="date", type="date")}
    sql = ParameterSet(params, {"date": value}).interpolate("select {{ date }}")
    assert query_layer.conn.execute(sql).fetchone() == (value,)


@pytest.mark.parametrize("kind", ["date", "string"])
@pytest.mark.parametrize("dialect", ["duckdb", "postgres", "mysql"])
@pytest.mark.parametrize("template", ["{{ value }}", "{# c #}{{ value }}", "{# c #}'{{ value }}'"])
def test_parameter_backslashes_preserve_dialect_literal(query_layer, kind, dialect, template):
    import sqlglot
    from sqlglot import exp

    value = "\\' OR 1=1 -- "
    params = {"value": Parameter(name="value", type=kind)}
    rendered = ParameterSet(params, {"value": value}, dialect=dialect).interpolate(template)
    parsed = sqlglot.parse_one(rendered, read=dialect)
    assert isinstance(parsed, exp.Literal)
    assert parsed.this == value
    if dialect == "duckdb":
        assert query_layer.conn.execute("select " + rendered).fetchone() == (value,)


@pytest.mark.parametrize("kind", ["date", "string"])
@pytest.mark.parametrize("value", ["\\' OR 1=1 -- ", "O'Reilly\\folder", "backslash\\", "plain"])
def test_escape_string_parameter_values_roundtrip(query_layer, kind, value):
    import sqlglot
    from sqlglot import exp

    params = {"value": Parameter(name="value", type=kind)}
    for dialect in ("duckdb", "postgres"):
        rendered = ParameterSet(params, {"value": value}, dialect=dialect).interpolate("{# c #}E'{{ value }}'")
        assert isinstance(sqlglot.parse_one(rendered, read=dialect), exp.Literal)
        assert query_layer.conn.execute("select " + rendered).fetchone() == (value,)
    with pytest.raises(ValueError, match="unpaired SQL escape"):
        ParameterSet(params, {"value": value}, dialect="duckdb").interpolate("{# c #}E'\\{{ value }}'")


def test_rewriter_cte_scope_does_not_authorize_structured_or_later_calls(query_layer):
    from sidemantic.sql.query_rewriter import QueryRewriter

    rewriter = QueryRewriter(query_layer.graph, dialect="duckdb", use_rust_rewriter=False)
    predicate = "events.user_id IN (SELECT user_id FROM allowed)"
    sql = "WITH allowed AS (SELECT 1 AS user_id) SELECT events.count FROM events WHERE " + predicate
    rewritten = rewriter.rewrite(sql)
    assert query_layer.conn.execute(rewritten).fetchall() == [(2,)]
    with pytest.raises(ValueError, match="physical data sources"):
        rewriter.generator.generate(metrics=["events.count"], filters=[predicate])
    with pytest.raises(ValueError, match="physical data sources"):
        rewriter.rewrite("SELECT events.count FROM events WHERE " + predicate)
    with pytest.raises(ValueError, match="physical data sources"):
        rewriter.rewrite(sql.replace("FROM allowed)", "FROM main.allowed)"))


def test_cte_scope_allows_expression_plan_and_identifier_folding(query_layer):
    from sidemantic.sql.query_rewriter import QueryRewriter

    rewriter = QueryRewriter(query_layer.graph, dialect="duckdb", use_rust_rewriter=False)
    sql = """WITH Allowed AS (SELECT 1 AS user_id)
        SELECT events.count + 1 AS result FROM events
        WHERE events.user_id IN (SELECT user_id FROM allowed)"""
    assert query_layer.conn.execute(rewriter.rewrite(sql)).fetchall() == [(3,)]


@pytest.mark.parametrize("kind", ["date", "string"])
@pytest.mark.parametrize("use_segment", [False, True])
def test_escape_string_filters_preserve_scope_through_query_pipeline(query_layer, kind, use_segment):
    from sidemantic.core.segment import Segment

    query_layer.graph.add_parameter(Parameter(name="value", type=kind))
    predicate = "{# c #}events.event_type = E'{{ value }}'"
    if use_segment:
        query_layer.graph.models["events"].segments.append(Segment(name="matching_event", sql=predicate))
        query_args = {"segments": ["events.matching_event"]}
    else:
        query_args = {"filters": [predicate]}
    assert query_layer.query(metrics=["events.count"], parameters={"value": "signup"}, **query_args).fetchall() == [
        (1,)
    ]
    assert query_layer.query(
        metrics=["events.count"], parameters={"value": "\\' OR 1=1 -- "}, **query_args
    ).fetchall() == [(0,)]
    literal_value = "O'Reilly\\folder"
    query_layer.conn.execute("insert into events_raw values (3, 1, ?, '2024-01-03')", [literal_value])
    assert query_layer.query(
        metrics=["events.count"], parameters={"value": literal_value}, **query_args
    ).fetchall() == [(1,)]


@pytest.mark.parametrize("trusted_source", ["segment", "security"])
def test_window_recursion_preserves_trusted_filter_provenance(query_layer, trusted_source):
    from sidemantic.core.security import SecurityPolicy
    from sidemantic.core.segment import Segment

    query_layer.conn.execute("create table allowed_users(user_id integer)")
    query_layer.conn.execute("insert into allowed_users values (1)")
    query_layer.conn.execute("insert into events_raw values (3, 2, 'signup', '2024-01-01')")
    predicate = "user_id IN (SELECT user_id FROM allowed_users)"
    if trusted_source == "segment":
        query_layer.graph.models["events"].segments.append(Segment(name="allowed", sql=predicate))
        query_args = {"segments": ["events.allowed"]}
    else:
        query_layer.graph.models["events"].security = SecurityPolicy(row_filters=[predicate])
        query_args = {"user_attributes": {}}
    rows = query_layer.query(
        metrics=["events.running"], dimensions=["events.timestamp"], order_by=["events.timestamp"], **query_args
    ).fetchall()
    assert len(rows) == 2
    assert rows[-1][-1] == 2
    with pytest.raises(ValueError, match="physical data sources"):
        query_layer.query(
            metrics=["events.running"], dimensions=["events.timestamp"], filters=[predicate], user_attributes={}
        )


def test_multimodel_recursion_preserves_trusted_segment_filter(query_layer):
    from sidemantic.core.relationship import Relationship
    from sidemantic.core.segment import Segment

    query_layer.conn.execute("create table users_raw(user_id integer, region varchar)")
    query_layer.conn.execute("insert into users_raw values (1, 'US'), (2, 'EU')")
    query_layer.conn.execute("insert into events_raw values (3, 2, 'signup', '2024-01-01')")
    query_layer.conn.execute("create table allowed_regions(region varchar)")
    query_layer.conn.execute("insert into allowed_regions values ('US')")
    query_layer.add_model(
        Model(
            name="users",
            table="users_raw",
            primary_key="user_id",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[Metric(name="users_count", agg="count")],
            relationships=[Relationship(name="events", type="one_to_many", sql="user_id", foreign_key="user_id")],
            segments=[Segment(name="allowed", sql="region IN (SELECT region FROM allowed_regions)")],
        )
    )
    sql = query_layer.compile(
        metrics=["events.count", "users.users_count"], dimensions=["users.region"], segments=["users.allowed"]
    )
    assert "events_preagg" in sql and "users_preagg" in sql
    assert query_layer.conn.execute(sql).fetchall() == [("US", 2, 1)]
    with pytest.raises(ValueError, match="physical data sources"):
        query_layer.compile(
            metrics=["events.count", "users.users_count"],
            dimensions=["users.region"],
            filters=["users.region IN (SELECT region FROM allowed_regions)"],
        )


@pytest.mark.parametrize("dialect,prefix", [("duckdb", "E"), ("postgres", "E"), ("mysql", "")])
@pytest.mark.parametrize("value", [" OR 1=1 -- ", "ok", "O'Reilly\\folder"])
def test_escaped_static_quote_before_parameter_keeps_literal_boundary(query_layer, dialect, prefix, value):
    import sqlglot
    from sqlglot import exp

    params = {"value": Parameter(name="value", type="string")}
    template = "{# c #}'safe' = " + prefix + "'prefix\\'{{ value }}'"
    rendered = ParameterSet(params, {"value": value}, dialect=dialect).interpolate(template)
    expression = sqlglot.parse_one(rendered, read=dialect)
    assert isinstance(expression, exp.EQ)
    assert isinstance(expression.expression, exp.Literal)
    assert expression.expression.this == "prefix'" + value
    if dialect == "duckdb":
        assert query_layer.conn.execute("select 1 where " + rendered).fetchall() == []


@pytest.mark.parametrize("use_segment", [False, True])
def test_escaped_static_quote_before_parameter_through_query(query_layer, use_segment):
    from sidemantic.core.segment import Segment

    query_layer.graph.add_parameter(Parameter(name="value", type="string"))
    predicate = "{# c #}events.event_type = E'prefix\\'{{ value }}'"
    query_layer.conn.execute("insert into events_raw values (3, 1, ?, '2024-01-03')", ["prefix'ok"])
    if use_segment:
        query_layer.graph.models["events"].segments.append(Segment(name="prefixed", sql=predicate))
        query_args = {"segments": ["events.prefixed"]}
    else:
        query_args = {"filters": [predicate]}
    assert query_layer.query(metrics=["events.count"], parameters={"value": "ok"}, **query_args).fetchall() == [(1,)]
    assert query_layer.query(
        metrics=["events.count"], parameters={"value": " OR 1=1 -- "}, **query_args
    ).fetchall() == [(0,)]
