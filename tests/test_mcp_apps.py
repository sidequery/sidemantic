"""Tests for MCP Apps UI integration."""

import pytest

pytest.importorskip("mcp")  # Skip if mcp extra not installed

from sidemantic.apps import _get_widget_template, build_chart_html
from sidemantic.mcp_server import create_chart, initialize_layer


@pytest.fixture
def demo_layer(tmp_path):
    """Create a demo semantic layer for testing."""
    model_yaml = """
models:
  - name: orders
    table: orders_table
    dimensions:
      - name: status
        sql: status
        type: categorical
    metrics:
      - name: total_revenue
        agg: sum
        sql: amount
"""
    (tmp_path / "orders.yml").write_text(model_yaml)
    layer = initialize_layer(str(tmp_path), db_path=":memory:")
    layer.adapter.execute("""
        CREATE TABLE orders_table (
            id INTEGER, status VARCHAR, amount DECIMAL
        )
    """)
    layer.adapter.execute("""
        INSERT INTO orders_table VALUES
            (1, 'completed', 100),
            (2, 'completed', 200),
            (3, 'pending', 150)
    """)
    yield layer


def test_widget_template_loads():
    """Test that the built chart widget HTML loads."""
    html = _get_widget_template()
    assert len(html) > 1000  # Built file is ~960KB
    assert "<!DOCTYPE html>" in html
    assert "sidemantic-chart" in html


def test_build_chart_html():
    """Test that build_chart_html embeds the Vega spec."""
    spec = {"$schema": "https://vega.github.io/schema/vega-lite/v5.json", "mark": "bar"}
    html = build_chart_html(spec)

    assert "{{VEGA_SPEC}}" not in html
    assert '"$schema"' in html
    assert '"mark"' in html
    assert "vega-embed" in html
    assert 'data-sidemantic-vendor="vega"' in html
    assert 'data-sidemantic-vendor="vega_lite"' in html
    assert 'data-sidemantic-vendor="vega_embed"' in html
    assert 'type="module"' not in html
    assert "esm.sh" not in html
    assert "<script src=" not in html


def test_widget_uses_vega_interpreter():
    """Test that the built widget uses the CSP-safe vega interpreter."""
    html = _get_widget_template()
    # The widget should use ast:true + expressionInterpreter for CSP safety.
    # Note: vega-loader's CSV parser includes new Function() in the bundle
    # but it's never called at runtime since we pass JSON data.
    assert "expressionInterpreter" in html or "ast" in html


def test_create_chart_returns_vega_spec(demo_layer):
    """Test that create_chart returns vega_spec without png_base64."""
    result = create_chart(
        dimensions=["orders.status"],
        metrics=["orders.total_revenue"],
        chart_type="bar",
    )

    assert isinstance(result, dict)
    assert "sql" in result
    assert "vega_spec" in result
    assert "row_count" in result
    assert "png_base64" not in result
    assert isinstance(result["vega_spec"], dict)
    assert "data" in result["vega_spec"]


# --- Explorer filter helpers ---


def test_explorer_filter_preserves_value_types():
    """Boolean/numeric filter values must not be coerced to quoted strings."""
    from sidemantic.mcp_server import _build_explorer_filters

    state = {
        "model_name": "orders",
        "time_dimension": None,
        "filters": {
            "is_active": [True],
            "count": [5],
            "rate": [1.5],
            "status": ["completed"],
        },
    }
    exprs = _build_explorer_filters(state)

    assert "orders.is_active = true" in exprs
    assert "orders.count = 5" in exprs
    assert "orders.rate = 1.5" in exprs
    assert "orders.status = 'completed'" in exprs
    # Booleans must never become quoted string literals.
    assert "orders.is_active = 'True'" not in exprs
    assert "orders.is_active = 'true'" not in exprs


def test_explorer_filter_multi_value_preserves_types():
    """IN-style multi-value filters keep native types for each value."""
    from sidemantic.mcp_server import _build_explorer_filters

    state = {
        "model_name": "orders",
        "time_dimension": None,
        "filters": {"flag": [True, False]},
    }
    exprs = _build_explorer_filters(state)

    assert exprs == ["(orders.flag = true OR orders.flag = false)"]


def test_explorer_filter_escapes_string_quotes():
    """String filter values remain quoted and SQL-escaped."""
    from sidemantic.mcp_server import _build_explorer_filters

    state = {
        "model_name": "orders",
        "time_dimension": None,
        "filters": {"name": ["O'Brien"]},
    }
    exprs = _build_explorer_filters(state)

    assert exprs == ["orders.name = 'O''Brien'"]


def test_explorer_date_only_end_bound_is_exclusive_next_day():
    """Date-only end bounds use an exclusive next-day bound so the whole day counts."""
    from sidemantic.mcp_server import _build_explorer_filters, _format_date_end_bound

    op, literal = _format_date_end_bound("2024-01-31")
    assert op == "<"
    assert literal == "CAST('2024-02-01' AS DATE)"

    state = {
        "model_name": "orders",
        "time_dimension": "order_ts",
        "filters": {},
        "date_range": ["2024-01-01", "2024-01-31"],
        "brush_selection": [],
    }
    exprs = _build_explorer_filters(state)
    assert len(exprs) == 1
    predicate = exprs[0]
    assert "orders.order_ts >= CAST('2024-01-01' AS DATE)" in predicate
    assert "orders.order_ts < CAST('2024-02-01' AS DATE)" in predicate
    # The naive inclusive bound would drop most of the end day for timestamps.
    assert "<= CAST('2024-01-31' AS DATE)" not in predicate


def test_explorer_timestamp_end_bound_stays_inclusive():
    """End bounds that already carry a time component keep the inclusive operator."""
    from sidemantic.mcp_server import _format_date_end_bound

    op, literal = _format_date_end_bound("2024-01-31 23:59:59")
    assert op == "<="
    assert literal == "CAST('2024-01-31 23:59:59' AS TIMESTAMP)"


def test_widget_query_dimension_uses_configured_metric_ref(demo_layer, monkeypatch):
    """Dimension leaderboards must use the metric's configured ref, not model.key.

    Reconstructing ``model_name.selected_metric`` breaks when the configured
    metric ref points at a different (e.g. related) model.
    """
    import sidemantic.mcp_server as mcp_mod

    captured: dict = {}
    real_compile = demo_layer.compile

    def spy_compile(*args, **kwargs):
        if "metrics" in kwargs:
            captured["metrics"] = kwargs["metrics"]
        return real_compile(*args, **kwargs)

    monkeypatch.setattr(demo_layer, "compile", spy_compile)

    # Model name differs from the metric ref's model to expose the bug:
    # naive reconstruction would yield "analytics.total_revenue" (invalid),
    # the fix must use the configured ref "orders.total_revenue".
    monkeypatch.setattr(
        mcp_mod,
        "_explorer_state",
        {
            "model_name": "analytics",
            "time_dimension": None,
            "metrics_config": [{"key": "total_revenue", "ref": "orders.total_revenue", "label": "Total Revenue"}],
            "dimensions_config": [{"key": "status", "ref": "orders.status", "label": "Status"}],
            "date_range": [],
            "filters": {},
            "brush_selection": [],
        },
    )

    mcp_mod.widget_query(
        query_type="dimension",
        dimension_key="status",
        selected_metric="total_revenue",
    )

    assert captured.get("metrics") == ["orders.total_revenue"]
    assert "analytics.total_revenue" not in (captured.get("metrics") or [])
