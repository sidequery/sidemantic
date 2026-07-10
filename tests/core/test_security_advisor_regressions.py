"""Regression tests for issues found in the advisor review of the security/semi-additive work.

Covers:
- P0-1: row-filter SQL injection via the unquoted template form.
- P0-2: semi-additive (non_additive_dimension) correctness at a coarser time grain.
- P1-1: enforce_visibility must also cover fields referenced only in filters/order_by.
"""

import datetime

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.core.security import SecurityPolicy, render_row_filter
from sidemantic.core.semantic_layer import SecurityError

# --- P0-1: injection via unquoted row-filter templates ---------------------------------


def test_unquoted_row_filter_neutralizes_string_injection():
    # The docstring's canonical form is unquoted; a string value must still not break out.
    rendered = render_row_filter("tenant_id = {{ user.tenant_id }}", {"tenant_id": "1 OR 1=1"})
    # The value renders as a single quoted string literal, not a boolean condition.
    assert rendered == "tenant_id = '1 OR 1=1'"
    assert " OR " not in rendered.replace("'1 OR 1=1'", "")


def test_quoted_row_filter_still_works_and_is_safe():
    assert render_row_filter("region = '{{ user.region }}'", {"region": "US"}) == "region = 'US'"
    # Quote-breakout attempt stays inside one escaped literal.
    out = render_row_filter("email = '{{ user.email }}'", {"email": "x' OR '1'='1"})
    assert out == "email = 'x'' OR ''1''=''1'"


def test_row_filter_typed_literals():
    assert render_row_filter("n = {{ user.n }}", {"n": 42}) == "n = 42"
    assert render_row_filter("f = {{ user.f }}", {"f": 1.5}) == "f = 1.5"
    assert render_row_filter("b = {{ user.b }}", {"b": True}) == "b = TRUE"
    assert render_row_filter("x = {{ user.x }}", {"x": None}) == "x = NULL"


def test_row_filter_rejects_unsupported_attribute_type():
    with pytest.raises(SecurityError):
        render_row_filter("x = {{ user.x }}", {"x": object()})


def test_row_filter_injection_is_enforced_end_to_end():
    """A malicious attribute value cannot widen a scoped query's result."""
    layer = SemanticLayer()
    con = layer.adapter.conn
    con.execute("CREATE TABLE t (tenant INTEGER, v INTEGER)")
    con.execute("INSERT INTO t VALUES (1, 10), (2, 50)")
    layer.add_model(
        Model(
            name="t",
            table="t",
            primary_key="tenant",
            dimensions=[Dimension(name="tenant", type="numeric")],
            metrics=[Metric(name="total", agg="sum", sql="v")],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
        )
    )
    # Legit scoped user sees only their tenant.
    assert layer.query(metrics=["t.total"], user_attributes={"tenant": 1}).fetchall() == [(10,)]
    # Injection string is neutralized: it renders as a quoted literal compared to the int
    # column, so the query fails closed (conversion error) or returns no all-tenants row --
    # never the naive unscoped total of 60.
    try:
        rows = layer.query(metrics=["t.total"], user_attributes={"tenant": "1 OR 1=1"}).fetchall()
    except Exception:
        rows = None  # fail-closed (conversion error) is an acceptable, safe outcome
    assert rows != [(60,)]


# --- P0-2: semi-additive correctness at a coarse grain ---------------------------------


def _balance_layer():
    layer = SemanticLayer()
    con = layer.adapter.conn
    con.execute("CREATE TABLE bal (account VARCHAR, day DATE, balance INTEGER)")
    con.execute(
        """INSERT INTO bal VALUES
        ('A','2026-01-10',100),('A','2026-01-31',110),
        ('B','2026-01-10',200),('B','2026-01-31',210)"""
    )
    layer.add_model(
        Model(
            name="bal",
            table="bal",
            primary_key="account",
            dimensions=[
                Dimension(name="account", type="categorical"),
                Dimension(name="day", type="time", granularity="day"),
            ],
            metrics=[Metric(name="total_balance", agg="sum", sql="balance", non_additive_dimension="day")],
        )
    )
    return layer


def test_semi_additive_month_grain_uses_last_snapshot():
    layer = _balance_layer()
    sql = layer.compile(metrics=["bal.total_balance"], dimensions=["bal.day__month"])
    assert "QUALIFY" in sql, "coarse grain must keep the semi-additive QUALIFY"
    # Correct: last day-of-month per account, summed = 110 + 210 = 320 (NOT naive 620).
    assert layer.query(metrics=["bal.total_balance"], dimensions=["bal.day__month"]).fetchall() == [
        (datetime.date(2026, 1, 1), 320)
    ]


def test_semi_additive_raw_grain_is_additive_no_qualify():
    layer = _balance_layer()
    sql = layer.compile(metrics=["bal.total_balance"], dimensions=["bal.day"])
    # Grouping by the raw grain is already one snapshot per bucket: no QUALIFY needed.
    assert "QUALIFY" not in sql


def test_semi_additive_by_entity_last_value():
    layer = _balance_layer()
    rows = dict(layer.query(metrics=["bal.total_balance"], dimensions=["bal.account"]).fetchall())
    assert rows == {"A": 110, "B": 210}


# --- P1-1: enforce_visibility covers filters and order_by ------------------------------


def _visibility_layer():
    layer = SemanticLayer(enforce_visibility=True)
    con = layer.adapter.conn
    con.execute("CREATE TABLE orders (id INTEGER, region VARCHAR, margin INTEGER)")
    con.execute("INSERT INTO orders VALUES (1,'US',50),(2,'EU',150)")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[
                Dimension(name="region", type="categorical"),
                Dimension(name="margin", type="numeric", public=False),
            ],
            metrics=[Metric(name="cnt", agg="count")],
        )
    )
    return layer


def test_visibility_blocks_hidden_field_in_filter():
    layer = _visibility_layer()
    with pytest.raises(SecurityError, match="margin"):
        layer.compile(metrics=["orders.cnt"], filters=["orders.margin > 100"])


def test_visibility_blocks_hidden_field_in_order_by():
    layer = _visibility_layer()
    with pytest.raises(SecurityError, match="margin"):
        layer.compile(metrics=["orders.cnt"], dimensions=["orders.region"], order_by=["orders.margin"])


def test_visibility_allows_public_fields():
    layer = _visibility_layer()
    # Order-independent: the query has no ORDER BY, so row order is not guaranteed.
    rows = layer.query(metrics=["orders.cnt"], dimensions=["orders.region"]).fetchall()
    assert dict(rows) == {"US": 1, "EU": 1}


def test_sql_first_path_denied_for_secured_model():
    """P1: SemanticLayer.sql() (SQL-first, used by the CLI) cannot scope rows, so it must
    refuse when any model declares a security policy rather than returning unfiltered rows."""
    layer = SemanticLayer()
    con = layer.adapter.conn
    con.execute("CREATE TABLE t (tenant INTEGER, v INTEGER)")
    con.execute("INSERT INTO t VALUES (1, 10), (2, 50)")
    layer.add_model(
        Model(
            name="t",
            table="t",
            primary_key="tenant",
            dimensions=[Dimension(name="tenant", type="numeric")],
            metrics=[Metric(name="total", agg="sum", sql="v")],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
        )
    )
    with pytest.raises(SecurityError, match="sql"):
        layer.sql("SELECT total FROM t")


def test_row_filter_boolean_control_flow_preserves_truthiness():
    """PR review P1: a false boolean attribute must not render the admin/bypass branch.

    The finalize-based renderer keeps raw values for {% if %}/comparisons while still quoting
    interpolated {{ }} output, so wrapping no longer makes FALSE look truthy to Jinja.
    """
    tmpl = "{% if user.is_admin %}1=1{% else %}tenant_id = {{ user.tenant_id }}{% endif %}"
    assert render_row_filter(tmpl, {"is_admin": False, "tenant_id": 7}) == "tenant_id = 7"
    assert render_row_filter(tmpl, {"is_admin": True, "tenant_id": 7}) == "1=1"
    # Comparisons in control flow use the raw value, not a SQL literal.
    cmp_tmpl = "{% if user.role == 'admin' %}1=1{% else %}region = {{ user.region }}{% endif %}"
    assert render_row_filter(cmp_tmpl, {"role": "analyst", "region": "US"}) == "region = 'US'"
    # Injection is still neutralized because the interpolated OUTPUT is finalized to a literal.
    assert render_row_filter("tid = {{ user.tid }}", {"tid": "1 OR 1=1"}) == "tid = '1 OR 1=1'"


def test_visibility_blocks_non_public_segment():
    """enforce_visibility must reject a public=False segment referenced in a query."""
    from sidemantic.core.segment import Segment

    layer = SemanticLayer(enforce_visibility=True)
    con = layer.adapter.conn
    con.execute("CREATE TABLE orders (id INTEGER, status VARCHAR)")
    con.execute("INSERT INTO orders VALUES (1,'internal'),(2,'shipped')")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="cnt", agg="count")],
            segments=[Segment(name="internal", sql="status = 'internal'", public=False)],
        )
    )
    with pytest.raises(SecurityError, match="internal"):
        layer.compile(metrics=["orders.cnt"], segments=["orders.internal"])


def test_pg_server_refuses_user_attrs_map_without_auth():
    """PR review P1: a user-attrs map without password auth lets clients spoof usernames."""
    import pytest as _pytest

    _pytest.importorskip("riffq")
    from sidemantic.server.server import start_server

    layer = SemanticLayer()
    layer.add_model(Model(name="orders", table="orders", primary_key="id", metrics=[Metric(name="cnt", agg="count")]))
    with _pytest.raises(ValueError, match="requires authentication"):
        start_server(layer, user_attrs_map={"admin": {"role": "admin"}})


def test_segment_only_secured_query_forces_python_and_enforces():
    """PR review P1: a secured model referenced only via segments must not bypass enforcement."""
    from sidemantic.core.segment import Segment

    layer = SemanticLayer()
    con = layer.adapter.conn
    con.execute("CREATE TABLE orders (id INTEGER, region VARCHAR)")
    con.execute("INSERT INTO orders VALUES (1,'US'),(2,'EU')")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[Metric(name="cnt", agg="count")],
            segments=[Segment(name="us_only", sql="region = 'US'")],
            security=SecurityPolicy(row_filters=["region = '{{ user.region }}'"]),
        )
    )
    # touches-secured must see the model via the segment reference.
    assert layer._query_touches_secured_model(["orders.cnt"], None, None, ["orders.us_only"]) is True
    # deny-by-default still fires for a segment-only query with no attributes.
    with pytest.raises(SecurityError):
        layer.compile(metrics=["orders.cnt"], segments=["orders.us_only"])


def test_row_filter_subquery_scopes_correctly():
    """PR review P2: a row filter with a subquery keeps the inner columns unqualified."""
    layer = SemanticLayer()
    con = layer.adapter.conn
    con.execute("CREATE TABLE t (id INTEGER, v INTEGER); CREATE TABLE allowed (id INTEGER)")
    con.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30); INSERT INTO allowed VALUES (1),(3)")
    layer.add_model(
        Model(
            name="t",
            table="t",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric")],
            metrics=[Metric(name="tot", agg="sum", sql="v")],
            security=SecurityPolicy(row_filters=["id IN (SELECT id FROM allowed)"]),
        )
    )
    assert layer.query(metrics=["t.tot"], user_attributes={}).fetchall() == [(40,)]


def test_rewriter_threads_user_attributes():
    """PR review P2: SQL-first rewrite evaluates the access gate against the caller's attributes."""
    from sidemantic.sql.query_rewriter import QueryRewriter

    layer = SemanticLayer()
    con = layer.adapter.conn
    con.execute("CREATE TABLE orders (id INTEGER, amount INTEGER)")
    con.execute("INSERT INTO orders VALUES (1,10),(2,20)")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[Metric(name="total", agg="sum", sql="amount")],
            security=SecurityPolicy(access="user.role == 'analyst'"),
        )
    )
    # No attributes -> deny-by-default even on the SQL-first path.
    with pytest.raises(SecurityError):
        QueryRewriter(layer.graph, dialect=layer.dialect).rewrite("SELECT total FROM orders", strict=False)
    # Authorized attributes -> access-only model rewrites fine.
    sql = QueryRewriter(layer.graph, dialect=layer.dialect).rewrite(
        "SELECT total FROM orders", strict=False, user_attributes={"role": "analyst"}
    )
    assert "orders" in sql
    # Unauthorized -> denied by the access gate.
    with pytest.raises(SecurityError):
        QueryRewriter(layer.graph, dialect=layer.dialect).rewrite(
            "SELECT total FROM orders", strict=False, user_attributes={"role": "guest"}
        )
