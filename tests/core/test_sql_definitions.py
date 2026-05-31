"""Test SQL-based metric and segment definitions."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.dialect import (
    MetricDef,
    TableBlockFieldDef,
    TableBlockJoinDef,
    TableBlockModelDef,
)
from sidemantic.core.dialect import (
    parse as parse_sidemantic_sql,
)
from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.core.sql_definitions import (
    parse_sql_definitions,
    parse_sql_file_with_frontmatter,
    parse_sql_graph_definitions,
    parse_sql_model,
    parse_sql_models,
)


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
    """Test that 'expression' is an alias for 'sql' and parses aggregation."""
    sql = """
    METRIC (
        name revenue,
        expression SUM(amount)
    );
    """

    metrics, _ = parse_sql_definitions(sql)

    assert len(metrics) == 1
    assert metrics[0].name == "revenue"
    # Aggregation is now automatically parsed from the expression
    assert metrics[0].agg == "sum"
    assert metrics[0].sql == "amount"


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
        non_additive_dimension time
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


def test_parse_list_literals():
    """Test parsing list literals in SQL definitions."""
    sql = """
    METRIC (
        name revenue,
        agg sum,
        sql amount,
        filters ['status = completed', 'status = pending'],
        drill_fields [order_id, status]
    );
    """

    metrics, _ = parse_sql_definitions(sql)

    metric = metrics[0]
    assert metric.filters == ["status = completed", "status = pending"]
    assert metric.drill_fields == ["order_id", "status"]


def test_parse_sql_parameter_definitions():
    """Test parsing PARAMETER definitions in SQL."""
    sql = """
    PARAMETER (
        name region,
        type string,
        allowed_values [us, eu],
        default_value 'us'
    );
    """

    _, _, parameters = parse_sql_graph_definitions(sql)

    assert len(parameters) == 1
    param = parameters[0]
    assert param.name == "region"
    assert param.type == "string"
    assert param.allowed_values == ["us", "eu"]
    assert param.default_value == "us"


def test_parse_pre_aggregation_definition():
    """Test parsing PRE_AGGREGATION definition in SQL."""
    sql = """
    MODEL (name orders, table orders);

    PRE_AGGREGATION (
        name daily_rollup,
        measures [order_count, revenue],
        dimensions [status],
        time_dimension order_date,
        granularity day,
        partition_granularity month,
        scheduled_refresh false,
        refresh_key { every '1 hour', incremental true, update_window '7 day' },
        indexes [{ name idx_status, columns [status], type regular }]
    );
    """

    model = parse_sql_model(sql)

    assert model is not None
    assert len(model.pre_aggregations) == 1
    preagg = model.pre_aggregations[0]
    assert preagg.name == "daily_rollup"
    assert preagg.measures == ["order_count", "revenue"]
    assert preagg.dimensions == ["status"]
    assert preagg.time_dimension == "order_date"
    assert preagg.granularity == "day"
    assert preagg.partition_granularity == "month"
    assert preagg.scheduled_refresh is False
    assert preagg.refresh_key is not None
    assert preagg.refresh_key.every == "1 hour"
    assert preagg.refresh_key.incremental is True
    assert preagg.refresh_key.update_window == "7 day"
    assert preagg.indexes is not None
    assert preagg.indexes[0].name == "idx_status"
    assert preagg.indexes[0].columns == ["status"]


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


def test_yaml_parameters():
    """Test parsing parameters from YAML."""
    yaml_content = """
parameters:
  - name: region
    type: string
    allowed_values: [us, eu]
    default_value: us
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        temp_path = Path(f.name)

    try:
        adapter = SidemanticAdapter()
        graph = adapter.parse(temp_path)

        assert "region" in graph.parameters
        param = graph.parameters["region"]
        assert param.allowed_values == ["us", "eu"]
        assert param.default_value == "us"
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


def test_parse_table_block_sql_model():
    """Test parsing compact SQL-ish model blocks."""
    sql_content = """
model orders from orders (
  primary key (order_id)
  default time order_date grain day

  status
  date_trunc('day', created_at) as order_date : time grain day
  status = 'completed' as is_complete : boolean
  amount - discount as net_amount : numeric

  segment completed as status = 'completed'

  join one customers on customer_id = customers.id
  join many order_items on order_id = order_items.order_id and store_id = order_items.store_id

  revenue / order_count as average_order_value
  sum(amount) as revenue
  count(*) as order_count
)
"""

    model = parse_sql_model(sql_content)

    assert model is not None
    assert model.name == "orders"
    assert model.table == "orders"
    assert model.primary_key == "order_id"
    assert model.default_time_dimension == "order_date"
    assert model.default_grain == "day"

    dimensions = {dimension.name: dimension for dimension in model.dimensions}
    assert dimensions["status"].type == "categorical"
    assert dimensions["status"].sql is None
    assert dimensions["order_date"].type == "time"
    assert dimensions["order_date"].sql == "date_trunc('day', created_at)"
    assert dimensions["order_date"].granularity == "day"
    assert dimensions["is_complete"].type == "boolean"
    assert dimensions["is_complete"].sql == "status = 'completed'"
    assert dimensions["net_amount"].type == "numeric"
    assert dimensions["net_amount"].sql == "amount - discount"

    assert len(model.relationships) == 2
    relationships = {relationship.name: relationship for relationship in model.relationships}
    assert relationships["customers"].type == "many_to_one"
    assert relationships["customers"].foreign_key == "customer_id"
    assert relationships["customers"].primary_key == "id"
    assert relationships["order_items"].type == "one_to_many"
    assert relationships["order_items"].foreign_key == ["order_id", "store_id"]
    assert relationships["order_items"].primary_key == ["order_id", "store_id"]

    metrics = {metric.name: metric for metric in model.metrics}
    assert list(metrics) == ["average_order_value", "revenue", "order_count"]
    assert metrics["revenue"].agg == "sum"
    assert metrics["revenue"].sql == "amount"
    assert metrics["order_count"].agg == "count"
    assert metrics["order_count"].sql == "*"
    assert metrics["average_order_value"].type == "derived"
    assert metrics["average_order_value"].sql == "revenue / order_count"

    assert len(model.segments) == 1
    assert model.segments[0].name == "completed"
    assert model.segments[0].sql == "status = 'completed'"


def test_dialect_parses_table_block_sql_model():
    """Test compact model blocks are parsed by the Sidemantic SQL dialect."""
    statements = parse_sidemantic_sql(
        """
model orders from orders (
  primary key (order_id)
  status
  join one customers on customer_id = customers.id
  sum(amount) as revenue
)
"""
    )

    assert len(statements) == 1
    model_def = statements[0]
    assert isinstance(model_def, TableBlockModelDef)
    assert model_def.this.name == "orders"
    assert model_def.args["table"] == "orders"
    assert any(isinstance(expression, TableBlockFieldDef) for expression in model_def.expressions)
    assert any(isinstance(expression, TableBlockJoinDef) for expression in model_def.expressions)


def test_dialect_parses_table_block_with_graph_definition():
    """Test compact model blocks can be followed by graph-level definitions."""
    statements = parse_sidemantic_sql(
        """
model orders from orders (
  primary key (order_id)
  sum(amount) as revenue
)

METRIC (
  name total_revenue,
  sql orders.revenue
)
"""
    )

    assert len(statements) == 2
    assert isinstance(statements[0], TableBlockModelDef)
    assert isinstance(statements[1], MetricDef)


def test_parse_graph_definitions_after_table_block():
    """Test graph definitions after compact model blocks are not dropped."""
    sql_content = """
model orders from orders (
  primary key (order_id)
  sum(amount) as revenue
)

METRIC (
  name total_revenue,
  sql orders.revenue
)
"""

    metrics, segments, parameters = parse_sql_graph_definitions(sql_content)

    assert len(metrics) == 1
    assert metrics[0].name == "total_revenue"
    assert segments == []
    assert parameters == []


def test_parse_graph_definitions_rejects_plain_sql():
    """Graph definition blocks should not silently ignore unsupported SQL."""
    with pytest.raises(ValueError, match="Unsupported SQL definition statement: Select"):
        parse_sql_graph_definitions("SELECT 1;")


def test_parse_sql_definitions_propagates_parse_errors():
    """Malformed embedded definition syntax should surface as a parse failure."""
    with pytest.raises(Exception):
        parse_sql_definitions("NOT_A_DEF (name x);")


def test_parse_table_block_multiline_field_expression():
    """Test compact field expressions can span lines before their alias."""
    sql_content = """
model orders from orders (
  primary key (order_id)
  amount
    - discount as net_amount : numeric
  sum(amount) as revenue
)
"""

    model = parse_sql_model(sql_content)

    assert model is not None
    dimensions = {dimension.name: dimension for dimension in model.dimensions}
    assert dimensions["net_amount"].type == "numeric"
    assert "amount" in dimensions["net_amount"].sql
    assert "- discount" in dimensions["net_amount"].sql


def test_adapter_parse_table_block_sql_file():
    """Test SidemanticAdapter parsing compact SQL-ish model blocks."""
    sql_content = """
model orders from orders (
  primary key (order_id)
  status
  join one customers on customer_id = customers.id
  sum(amount) as revenue
)

model customers from public.customers (
  primary key (id)
  region
)
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql_content)
        temp_path = Path(f.name)

    try:
        adapter = SidemanticAdapter()
        graph = adapter.parse(temp_path)

        assert len(graph.models) == 2
        assert "orders" in graph.models
        assert "customers" in graph.models
        orders = graph.models["orders"]
        assert orders.primary_key == "order_id"
        assert orders.dimensions[0].name == "status"
        assert orders.metrics[0].name == "revenue"
        assert orders.metrics[0].agg == "sum"
        assert orders.relationships[0].name == "customers"
        assert graph.models["customers"].table == "public.customers"

    finally:
        temp_path.unlink(missing_ok=True)


def test_parse_table_block_sql_models():
    """Test parsing multiple compact SQL-ish model blocks."""
    sql_content = """
model orders from orders (
  primary key (order_id)
  sum(amount) as revenue
)

model customers from customers (
  primary key (id)
  region
)
"""

    models = parse_sql_models(sql_content)

    assert [model.name for model in models] == ["orders", "customers"]
    assert models[0].metrics[0].name == "revenue"
    assert models[1].dimensions[0].name == "region"


def test_parse_table_block_parenthesized_composite_join():
    """Test compact composite joins can wrap ON predicates in parentheses."""
    sql_content = """
model orders from orders (
  primary key (order_id, store_id)
  join many order_items on (order_id = order_items.order_id and store_id = order_items.store_id)
  sum(amount) as revenue
)
"""

    model = parse_sql_model(sql_content)

    assert model is not None
    assert len(model.relationships) == 1
    relationship = model.relationships[0]
    assert relationship.name == "order_items"
    assert relationship.type == "one_to_many"
    assert relationship.foreign_key == ["order_id", "store_id"]
    assert relationship.primary_key == ["order_id", "store_id"]


def test_parse_sql_models_mixed_table_block_and_model_statement():
    """Test compact and MODEL() definitions can coexist in one SQL file."""
    sql_content = """
model orders from orders (
  primary key (order_id)
  sum(amount) as revenue
)

MODEL (
  name customers,
  table customers,
  primary_key id
);

DIMENSION (
  name region,
  type categorical,
  sql region
);
"""

    models = parse_sql_models(sql_content)

    assert [model.name for model in models] == ["orders", "customers"]
    assert models[0].metrics[0].name == "revenue"
    assert models[1].dimensions[0].name == "region"


def test_parse_sql_models_multiple_legacy_models():
    """Test multiple legacy MODEL() blocks retain their own fields."""
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

MODEL (
  name customers,
  table customers,
  primary_key id
);

DIMENSION (
  name region,
  type categorical,
  sql region
);
"""

    models = parse_sql_models(sql_content)

    assert [model.name for model in models] == ["orders", "customers"]
    assert models[0].dimensions[0].name == "status"
    assert models[0].metrics[0].name == "revenue"
    assert models[1].dimensions[0].name == "region"
    assert models[1].metrics == []


def test_parse_table_block_derived_sql_source():
    """Test compact model blocks can use derived SQL sources."""
    sql_content = """
model completed_orders from (
  select *
  from raw.orders
  where status = 'completed'
) (
  primary key (order_id)
  created_at as order_date : time grain day
  sum(amount) as revenue
)
"""

    model = parse_sql_model(sql_content)

    assert model is not None
    assert model.name == "completed_orders"
    assert model.table is None
    assert model.sql == "select *\n  from raw.orders\n  where status = 'completed'"
    assert model.dimensions[0].name == "order_date"
    assert model.dimensions[0].type == "time"
    assert model.dimensions[0].granularity == "day"
    assert model.metrics[0].name == "revenue"


def test_table_block_requires_from_source():
    """Test table-block models require source in the header."""
    sql_content = """
model orders (
  primary key (order_id)
  sum(amount) as revenue
)
"""

    with pytest.raises(ValueError, match="must use `model orders from <table>"):
        parse_sql_model(sql_content)


def test_table_block_rejects_table_statement():
    """Test table-block model sources are declared with from, not table statements."""
    sql_content = """
model orders from orders (
  table orders
  primary key (order_id)
)
"""

    with pytest.raises(ValueError, match="use `model orders from <table>"):
        parse_sql_model(sql_content)


def test_table_block_rejects_bad_join():
    """Test table-block joins fail instead of being ignored."""
    sql_content = """
model orders from orders (
  primary key (order_id)
  join one customers on customer_id = 1
)
"""

    with pytest.raises(ValueError, match="must compare model columns"):
        parse_sql_model(sql_content)


def test_table_block_rejects_unknown_statement():
    """Test table-block unknown statements fail instead of being ignored."""
    sql_content = """
model orders from orders (
  primary key (order_id)
  description 'Orders table'
)
"""

    with pytest.raises(ValueError, match="Unrecognized statement"):
        parse_sql_model(sql_content)


def test_table_block_rejects_duplicate_field():
    """Test table-block duplicate fields fail clearly."""
    sql_content = """
model orders from orders (
  primary key (order_id)
  status
  status = 'completed' as status
)
"""

    with pytest.raises(ValueError, match="defines field 'status' more than once"):
        parse_sql_model(sql_content)


def test_table_block_rejects_empty_primary_key():
    """Test table-block primary key requires at least one column."""
    sql_content = """
model orders from orders (
  primary key ()
  sum(amount) as revenue
)
"""

    with pytest.raises(ValueError, match="Primary key requires at least one column"):
        parse_sql_model(sql_content)


def test_table_block_rejects_invalid_field_annotation():
    """Test compact field annotations fail clearly."""
    sql_content = """
model orders from orders (
  primary key (order_id)
  amount as net_amount : numeric grain day
)
"""

    with pytest.raises(ValueError, match="cannot use grain with type 'numeric'"):
        parse_sql_model(sql_content)


def test_table_block_rejects_default_time_for_non_time_dimension():
    """Test default time must reference a time dimension."""
    sql_content = """
model orders from orders (
  primary key (order_id)
  default time status
  status
)
"""

    with pytest.raises(ValueError, match="must be a time dimension"):
        parse_sql_model(sql_content)


def test_table_block_rejects_duplicate_segment():
    """Test duplicate compact segments fail clearly."""
    sql_content = """
model orders from orders (
  primary key (order_id)
  segment completed as status = 'completed'
  segment completed as completed_at is not null
)
"""

    with pytest.raises(ValueError, match="defines segment 'completed' more than once"):
        parse_sql_model(sql_content)


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
