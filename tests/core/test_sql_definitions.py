"""Test SQL-based metric and segment definitions."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.sql_definitions import parse_sql_definitions, parse_sql_file_with_frontmatter
from sidemantic.core.semantic_layer import SemanticLayer


def test_parse_simple_metric():
    """Test parsing simple aggregation metric."""
    sql = """
    METRIC (
        name revenue,
        agg sum,
        sql amount
    );
    """

    metrics, segments = parse_sql_definitions(sql)

    assert len(metrics) == 1
    assert len(segments) == 0

    metric = metrics[0]
    assert metric.name == "revenue"
    assert metric.agg == "sum"
    assert metric.sql == "amount"


def test_parse_metric_with_expression_alias():
    """Test that 'expression' is an alias for 'sql'."""
    sql = """
    METRIC (
        name revenue,
        expression SUM(amount)
    );
    """

    metrics, _ = parse_sql_definitions(sql)

    assert len(metrics) == 1
    assert metrics[0].name == "revenue"
    assert metrics[0].sql == "SUM(amount)"


def test_parse_metric_all_fields():
    """Test parsing metric with all supported fields."""
    sql = """
    METRIC (
        name revenue,
        agg sum,
        sql amount,
        description 'Total revenue',
        label 'Revenue',
        format '$#,##0.00',
        filters status = 'completed',
        fill_nulls_with 0,
        non_additive_dimension time,
        default_time_dimension order_date,
        default_grain day
    );
    """

    metrics, _ = parse_sql_definitions(sql)

    metric = metrics[0]
    assert metric.name == "revenue"
    assert metric.agg == "sum"
    assert metric.sql == "amount"
    assert metric.description == "Total revenue"
    assert metric.label == "Revenue"
    assert metric.format == "$#,##0.00"
    assert metric.filters == ["status = 'completed'"]
    assert metric.fill_nulls_with == 0
    assert metric.non_additive_dimension == "time"
    assert metric.default_time_dimension == "order_date"
    assert metric.default_grain == "day"


def test_parse_ratio_metric():
    """Test parsing ratio metric."""
    sql = """
    METRIC (
        name conversion_rate,
        type ratio,
        numerator completed_orders,
        denominator total_orders
    );
    """

    metrics, _ = parse_sql_definitions(sql)

    metric = metrics[0]
    assert metric.name == "conversion_rate"
    assert metric.type == "ratio"
    assert metric.numerator == "completed_orders"
    assert metric.denominator == "total_orders"


def test_parse_cumulative_metric():
    """Test parsing cumulative metric."""
    sql = """
    METRIC (
        name running_total,
        type cumulative,
        sql revenue,
        window 7 days
    );
    """

    metrics, _ = parse_sql_definitions(sql)

    metric = metrics[0]
    assert metric.name == "running_total"
    assert metric.type == "cumulative"
    assert metric.sql == "revenue"
    assert metric.window == "7 days"


def test_parse_time_comparison_metric():
    """Test parsing time comparison metric."""
    sql = """
    METRIC (
        name yoy_growth,
        type time_comparison,
        base_metric revenue,
        comparison_type yoy,
        calculation percent_change
    );
    """

    metrics, _ = parse_sql_definitions(sql)

    metric = metrics[0]
    assert metric.name == "yoy_growth"
    assert metric.type == "time_comparison"
    assert metric.base_metric == "revenue"
    assert metric.comparison_type == "yoy"
    assert metric.calculation == "percent_change"


def test_parse_conversion_metric():
    """Test parsing conversion metric."""
    sql = """
    METRIC (
        name signup_to_purchase,
        type conversion,
        entity user_id,
        base_event event_type = 'signup',
        conversion_event event_type = 'purchase',
        conversion_window 30 days
    );
    """

    metrics, _ = parse_sql_definitions(sql)

    metric = metrics[0]
    assert metric.name == "signup_to_purchase"
    assert metric.type == "conversion"
    assert metric.entity == "user_id"
    assert metric.base_event == "event_type = 'signup'"
    assert metric.conversion_event == "event_type = 'purchase'"
    assert metric.conversion_window == "30 days"


def test_parse_segment():
    """Test parsing segment definition."""
    sql = """
    SEGMENT (
        name active_users,
        expression status = 'active',
        description 'Active users only'
    );
    """

    metrics, segments = parse_sql_definitions(sql)

    assert len(metrics) == 0
    assert len(segments) == 1

    segment = segments[0]
    assert segment.name == "active_users"
    assert segment.sql == "status = 'active'"
    assert segment.description == "Active users only"


def test_parse_multiple_definitions():
    """Test parsing multiple metrics and segments."""
    sql = """
    METRIC (
        name revenue,
        agg sum,
        sql amount
    );

    METRIC (
        name order_count,
        agg count
    );

    SEGMENT (
        name completed,
        expression status = 'completed'
    );
    """

    metrics, segments = parse_sql_definitions(sql)

    assert len(metrics) == 2
    assert len(segments) == 1
    assert metrics[0].name == "revenue"
    assert metrics[1].name == "order_count"
    assert segments[0].name == "completed"


def test_parse_sql_file_with_frontmatter():
    """Test parsing .sql file with YAML frontmatter."""
    sql_content = """---
name: orders
table: orders
primary_key: order_id
dimensions:
  - name: status
    type: categorical
    sql: status
---

METRIC (
    name revenue,
    agg sum,
    sql amount
);

SEGMENT (
    name completed,
    expression status = 'completed'
);
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql_content)
        temp_path = Path(f.name)

    try:
        frontmatter, metrics, segments = parse_sql_file_with_frontmatter(temp_path)

        # Check frontmatter
        assert frontmatter["name"] == "orders"
        assert frontmatter["table"] == "orders"
        assert frontmatter["primary_key"] == "order_id"
        assert len(frontmatter["dimensions"]) == 1

        # Check SQL definitions
        assert len(metrics) == 1
        assert metrics[0].name == "revenue"

        assert len(segments) == 1
        assert segments[0].name == "completed"

    finally:
        temp_path.unlink(missing_ok=True)


def test_adapter_parse_sql_file():
    """Test SidemanticAdapter parsing .sql file."""
    sql_content = """---
name: orders
table: orders
primary_key: order_id
---

METRIC (
    name revenue,
    agg sum,
    sql amount
);
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql_content)
        temp_path = Path(f.name)

    try:
        adapter = SidemanticAdapter()
        graph = adapter.parse(temp_path)

        assert len(graph.models) == 1
        assert "orders" in graph.models

        orders = graph.models["orders"]
        assert len(orders.metrics) == 1
        assert orders.metrics[0].name == "revenue"

    finally:
        temp_path.unlink(missing_ok=True)


def test_yaml_with_embedded_sql_metrics():
    """Test YAML file with embedded sql_metrics field."""
    yaml_content = """
models:
  - name: orders
    table: orders
    primary_key: order_id
    sql_metrics: |
      METRIC (
        name revenue,
        agg sum,
        sql amount
      );

      METRIC (
        name order_count,
        agg count
      );
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        temp_path = Path(f.name)

    try:
        adapter = SidemanticAdapter()
        graph = adapter.parse(temp_path)

        assert "orders" in graph.models
        orders = graph.models["orders"]
        assert len(orders.metrics) == 2
        assert orders.metrics[0].name == "revenue"
        assert orders.metrics[1].name == "order_count"

    finally:
        temp_path.unlink(missing_ok=True)


def test_yaml_with_embedded_sql_segments():
    """Test YAML file with embedded sql_segments field."""
    yaml_content = """
models:
  - name: orders
    table: orders
    primary_key: order_id
    sql_segments: |
      SEGMENT (
        name completed,
        expression status = 'completed'
      );
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        temp_path = Path(f.name)

    try:
        adapter = SidemanticAdapter()
        graph = adapter.parse(temp_path)

        orders = graph.models["orders"]
        assert len(orders.segments) == 1
        assert orders.segments[0].name == "completed"

    finally:
        temp_path.unlink(missing_ok=True)


def test_mixed_yaml_and_sql_metrics():
    """Test mixing YAML and SQL metric definitions in same model."""
    yaml_content = """
models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: yaml_metric
        agg: sum
        sql: yaml_amount
    sql_metrics: |
      METRIC (
        name sql_metric,
        agg count
      );
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        temp_path = Path(f.name)

    try:
        adapter = SidemanticAdapter()
        graph = adapter.parse(temp_path)

        orders = graph.models["orders"]
        assert len(orders.metrics) == 2

        metric_names = {m.name for m in orders.metrics}
        assert "yaml_metric" in metric_names
        assert "sql_metric" in metric_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_graph_level_sql_metrics():
    """Test graph-level SQL metrics in YAML."""
    yaml_content = """
sql_metrics: |
  METRIC (
    name total_revenue,
    sql orders.revenue
  );
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        temp_path = Path(f.name)

    try:
        adapter = SidemanticAdapter()
        graph = adapter.parse(temp_path)

        assert len(graph.metrics) == 1
        assert "total_revenue" in graph.metrics

    finally:
        temp_path.unlink(missing_ok=True)


def test_semantic_layer_from_sql():
    """Test loading SQL file into SemanticLayer."""
    sql_content = """---
name: orders
table: orders
primary_key: order_id
---

METRIC (
    name revenue,
    agg sum,
    sql amount
);
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql_content)
        temp_path = Path(f.name)

    try:
        sl = SemanticLayer.from_yaml(temp_path)  # from_yaml handles .sql too

        assert "orders" in sl.list_models()
        model = sl.graph.models["orders"]
        assert len(model.metrics) == 1

    finally:
        temp_path.unlink(missing_ok=True)


def test_parse_pure_sql_model():
    """Test parsing complete model in pure SQL."""
    sql_content = """
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

DIMENSION (
    name order_date,
    type time,
    sql created_at,
    granularity day
);

RELATIONSHIP (
    name customer,
    type many_to_one,
    foreign_key customer_id
);

METRIC (
    name revenue,
    agg sum,
    sql amount
);

SEGMENT (
    name completed,
    expression status = 'completed'
);
"""

    from sidemantic.core.sql_definitions import parse_sql_model

    model = parse_sql_model(sql_content)

    assert model is not None
    assert model.name == "orders"
    assert model.table == "orders"
    assert model.primary_key == "order_id"

    assert len(model.dimensions) == 2
    assert model.dimensions[0].name == "status"
    assert model.dimensions[1].name == "order_date"
    assert model.dimensions[1].granularity == "day"

    assert len(model.relationships) == 1
    assert model.relationships[0].name == "customer"
    assert model.relationships[0].type == "many_to_one"

    assert len(model.metrics) == 1
    assert model.metrics[0].name == "revenue"

    assert len(model.segments) == 1
    assert model.segments[0].name == "completed"


def test_adapter_parse_pure_sql_file():
    """Test SidemanticAdapter parsing pure SQL file without frontmatter."""
    sql_content = """
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
    agg sum,
    sql amount
);
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql_content)
        temp_path = Path(f.name)

    try:
        adapter = SidemanticAdapter()
        graph = adapter.parse(temp_path)

        assert len(graph.models) == 1
        assert "orders" in graph.models

        orders = graph.models["orders"]
        assert orders.table == "orders"
        assert len(orders.dimensions) == 1
        assert len(orders.metrics) == 1

    finally:
        temp_path.unlink(missing_ok=True)


def test_semantic_layer_from_pure_sql():
    """Test loading pure SQL file into SemanticLayer."""
    sql_content = """
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
    agg sum,
    sql amount
);
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql_content)
        temp_path = Path(f.name)

    try:
        sl = SemanticLayer.from_yaml(temp_path)

        assert "orders" in sl.list_models()
        model = sl.graph.models["orders"]
        assert len(model.dimensions) == 1
        assert len(model.metrics) == 1

    finally:
        temp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
