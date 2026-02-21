"""Kitchen sink parity tests across Python, YAML, and SQL definitions."""

import tempfile
import warnings
from pathlib import Path

from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.parameter import Parameter
from sidemantic.core.pre_aggregation import Index, PreAggregation, RefreshKey
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment
from sidemantic.core.semantic_graph import SemanticGraph


def _normalize_sql_text(value: object) -> object:
    if not isinstance(value, str):
        return value
    try:
        import sqlglot

        parsed = sqlglot.parse_one(value, read="duckdb")
        return parsed.sql(dialect="duckdb")
    except Exception:
        return value


def _normalize_model(model: Model) -> dict:
    data = model.model_dump()

    for key in ("dimensions", "metrics", "segments", "relationships", "pre_aggregations"):
        items = data.get(key)
        if items:
            data[key] = sorted(items, key=lambda item: item["name"])

    for preagg in data.get("pre_aggregations") or []:
        if preagg.get("indexes"):
            preagg["indexes"] = sorted(preagg["indexes"], key=lambda item: item["name"])
        if preagg.get("build_range_start"):
            preagg["build_range_start"] = _normalize_sql_text(preagg["build_range_start"])
        if preagg.get("build_range_end"):
            preagg["build_range_end"] = _normalize_sql_text(preagg["build_range_end"])

    for dim in data.get("dimensions") or []:
        if dim.get("sql"):
            dim["sql"] = _normalize_sql_text(dim["sql"])

    for metric in data.get("metrics") or []:
        if metric.get("sql"):
            metric["sql"] = _normalize_sql_text(metric["sql"])
        if metric.get("window_expression"):
            metric["window_expression"] = _normalize_sql_text(metric["window_expression"])
        if metric.get("filters"):
            metric["filters"] = [_normalize_sql_text(item) for item in metric["filters"]]

    return data


def _normalize_graph(graph: SemanticGraph) -> dict:
    return {
        "models": {name: _normalize_model(model) for name, model in graph.models.items()},
        "metrics": {
            name: {
                **metric.model_dump(),
                "sql": _normalize_sql_text(metric.sql) if metric.sql else None,
            }
            for name, metric in graph.metrics.items()
        },
        "parameters": {name: param.model_dump() for name, param in graph.parameters.items()},
    }


def _merge_graphs(graphs: list[SemanticGraph]) -> SemanticGraph:
    merged = SemanticGraph()
    for graph in graphs:
        for model in graph.models.values():
            merged.add_model(model)
        for metric in graph.metrics.values():
            if metric.name not in merged.metrics:
                merged.add_metric(metric)
        for param in graph.parameters.values():
            if param.name not in merged.parameters:
                merged.add_parameter(param)
    return merged


def _build_python_graph() -> SemanticGraph:
    model = Model(
        name="orders",
        table="orders",
        source_uri="s3://warehouse/orders",
        description="Orders model",
        extends="base_orders",
        primary_key="order_id",
        default_time_dimension="order_date",
        default_grain="day",
        relationships=[
            Relationship(
                name="customers",
                type="many_to_one",
                foreign_key="customer_id",
                primary_key="id",
            )
        ],
        dimensions=[
            Dimension(
                name="status",
                type="categorical",
                sql="status",
                description="Order status",
                label="Status",
            ),
            Dimension(
                name="order_date",
                type="time",
                sql="order_date",
                granularity="day",
                supported_granularities=["day", "week", "month"],
                description="Order date",
            ),
            Dimension(
                name="is_priority",
                type="boolean",
                sql="is_priority",
                description="Priority flag",
            ),
            Dimension(
                name="amount",
                type="numeric",
                sql="amount",
                format="$#,##0.00",
                value_format_name="usd",
            ),
        ],
        metrics=[
            Metric(
                name="revenue",
                agg="sum",
                sql="amount",
                filters=["status = completed", "status = pending"],
                fill_nulls_with=0,
                description="Total revenue",
                label="Revenue",
                format="$#,##0.00",
                value_format_name="usd",
                drill_fields=["order_id", "status"],
                non_additive_dimension="order_date",
            ),
            Metric(
                name="order_count",
                agg="count",
                sql="order_id",
            ),
            Metric(
                name="conversion_rate",
                type="ratio",
                numerator="completed_orders",
                denominator="order_count",
                offset_window="1 month",
            ),
            Metric(
                name="profit_margin",
                type="derived",
                sql="(revenue - cost) / revenue",
            ),
            Metric(
                name="running_revenue",
                type="cumulative",
                sql="revenue",
                window="7 days",
                grain_to_date="month",
                window_expression="SUM(revenue)",
                window_frame="RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW",
                window_order="order_date",
            ),
            Metric(
                name="revenue_yoy",
                type="time_comparison",
                base_metric="revenue",
                comparison_type="yoy",
                calculation="percent_change",
                time_offset="1 year",
            ),
            Metric(
                name="signup_to_purchase",
                type="conversion",
                entity="user_id",
                base_event="event_type = signup",
                conversion_event="event_type = purchase",
                conversion_window="30 days",
            ),
        ],
        segments=[
            Segment(
                name="completed",
                sql="status = completed",
                description="Completed orders only",
                public=False,
            )
        ],
        pre_aggregations=[
            PreAggregation(
                name="daily_rollup",
                type="rollup",
                measures=["order_count", "revenue"],
                dimensions=["status", "is_priority"],
                time_dimension="order_date",
                granularity="day",
                partition_granularity="month",
                refresh_key=RefreshKey(every="1 hour", incremental=True, update_window="7 day"),
                scheduled_refresh=False,
                indexes=[Index(name="idx_status", columns=["status"], type="regular")],
                build_range_start="date_trunc('month',current_date - interval '6 month')",
                build_range_end="current_date",
            )
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)

    graph_metric = Metric(
        name="total_revenue_graph",
        type="derived",
        sql="orders.revenue",
        description="Graph-level revenue",
    )
    graph.add_metric(graph_metric)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        graph.add_parameter(
            Parameter(
                name="region",
                type="string",
                allowed_values=["us", "eu"],
                default_value="us",
                description="Region filter",
            )
        )

    return graph


def _parse_yaml_graph() -> SemanticGraph:
    model_yaml = """
models:
  - name: orders
    table: orders
    source_uri: s3://warehouse/orders
    description: Orders model
    extends: base_orders
    primary_key: order_id
    default_time_dimension: order_date
    default_grain: day
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
        primary_key: id
    dimensions:
      - name: status
        type: categorical
        sql: status
        description: Order status
        label: Status
      - name: order_date
        type: time
        sql: order_date
        granularity: day
        supported_granularities: [day, week, month]
        description: Order date
      - name: is_priority
        type: boolean
        sql: is_priority
        description: Priority flag
      - name: amount
        type: numeric
        sql: amount
        format: "$#,##0.00"
        value_format_name: usd
    metrics:
      - name: revenue
        agg: sum
        sql: amount
        filters: [status = completed, status = pending]
        fill_nulls_with: 0
        description: Total revenue
        label: Revenue
        format: "$#,##0.00"
        value_format_name: usd
        drill_fields: [order_id, status]
        non_additive_dimension: order_date
      - name: order_count
        agg: count
        sql: order_id
      - name: conversion_rate
        type: ratio
        numerator: completed_orders
        denominator: order_count
        offset_window: 1 month
      - name: profit_margin
        type: derived
        sql: "(revenue - cost) / revenue"
      - name: running_revenue
        type: cumulative
        sql: revenue
        window: 7 days
        grain_to_date: month
        window_expression: SUM(revenue)
        window_frame: RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW
        window_order: order_date
      - name: revenue_yoy
        type: time_comparison
        base_metric: revenue
        comparison_type: yoy
        calculation: percent_change
        time_offset: 1 year
      - name: signup_to_purchase
        type: conversion
        entity: user_id
        base_event: event_type = signup
        conversion_event: event_type = purchase
        conversion_window: 30 days
    segments:
      - name: completed
        sql: status = completed
        description: Completed orders only
        public: false
    pre_aggregations:
      - name: daily_rollup
        type: rollup
        measures: [order_count, revenue]
        dimensions: [status, is_priority]
        time_dimension: order_date
        granularity: day
        partition_granularity: month
        refresh_key:
          every: 1 hour
          incremental: true
          update_window: 7 day
        scheduled_refresh: false
        indexes:
          - name: idx_status
            columns: [status]
            type: regular
        build_range_start: "date_trunc('month',current_date - interval '6 month')"
        build_range_end: current_date
"""

    graph_yaml = """
metrics:
  - name: total_revenue_graph
    type: derived
    sql: orders.revenue
    description: Graph-level revenue

parameters:
  - name: region
    type: string
    allowed_values: [us, eu]
    default_value: us
    description: Region filter
"""

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        model_path = tmpdir_path / "orders.yml"
        graph_path = tmpdir_path / "graph.yml"
        model_path.write_text(model_yaml)
        graph_path.write_text(graph_yaml)

        adapter = SidemanticAdapter()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            model_graph = adapter.parse(model_path)
            graph_graph = adapter.parse(graph_path)

        return _merge_graphs([model_graph, graph_graph])


def _parse_sql_graph() -> SemanticGraph:
    model_sql = """
MODEL (
  name orders,
  table orders,
  source_uri 's3://warehouse/orders',
  description 'Orders model',
  extends base_orders,
  primary_key order_id,
  default_time_dimension order_date,
  default_grain day
);

RELATIONSHIP (
  name customers,
  type many_to_one,
  foreign_key customer_id,
  primary_key id
);

DIMENSION (
  name status,
  type categorical,
  sql status,
  description 'Order status',
  label 'Status'
);

DIMENSION (
  name order_date,
  type time,
  sql order_date,
  granularity day,
  supported_granularities [day, week, month],
  description 'Order date'
);

DIMENSION (
  name is_priority,
  type boolean,
  sql is_priority,
  description 'Priority flag'
);

DIMENSION (
  name amount,
  type numeric,
  sql amount,
  format '$#,##0.00',
  value_format_name usd
);

METRIC (
  name revenue,
  agg sum,
  sql amount,
  filters ['status = completed', 'status = pending'],
  fill_nulls_with 0,
  description 'Total revenue',
  label 'Revenue',
  format '$#,##0.00',
  value_format_name usd,
  drill_fields [order_id, status],
  non_additive_dimension order_date
);

METRIC (
  name order_count,
  agg count,
  sql order_id
);

METRIC (
  name conversion_rate,
  type ratio,
  numerator completed_orders,
  denominator order_count,
  offset_window '1 month'
);

METRIC (
  name profit_margin,
  type derived,
  sql (revenue - cost) / revenue
);

METRIC (
  name running_revenue,
  type cumulative,
  sql revenue,
  window '7 days',
  grain_to_date month,
  window_expression SUM(revenue),
  window_frame 'RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW',
  window_order order_date
);

METRIC (
  name revenue_yoy,
  type time_comparison,
  base_metric revenue,
  comparison_type yoy,
  calculation percent_change,
  time_offset '1 year'
);

METRIC (
  name signup_to_purchase,
  type conversion,
  entity user_id,
  base_event event_type = signup,
  conversion_event event_type = purchase,
  conversion_window '30 days'
);

SEGMENT (
  name completed,
  expression status = completed,
  description 'Completed orders only',
  public false
);

PRE_AGGREGATION (
  name daily_rollup,
  type rollup,
  measures [order_count, revenue],
  dimensions [status, is_priority],
  time_dimension order_date,
  granularity day,
  partition_granularity month,
  refresh_key { every '1 hour', incremental true, update_window '7 day' },
  scheduled_refresh false,
  indexes [{ name idx_status, columns [status], type regular }],
  build_range_start date_trunc('month',current_date - interval '6 month'),
  build_range_end current_date
);
"""

    graph_sql = """
METRIC (
  name total_revenue_graph,
  type derived,
  sql orders.revenue,
  description 'Graph-level revenue'
);

PARAMETER (
  name region,
  type string,
  allowed_values [us, eu],
  default_value 'us',
  description 'Region filter'
);
"""

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        model_path = tmpdir_path / "orders.sql"
        graph_path = tmpdir_path / "graph.sql"
        model_path.write_text(model_sql)
        graph_path.write_text(graph_sql)

        adapter = SidemanticAdapter()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            model_graph = adapter.parse(model_path)
            graph_graph = adapter.parse(graph_path)

        return _merge_graphs([model_graph, graph_graph])


def test_kitchen_sink_parity_roundtrip():
    expected_graph = _build_python_graph()
    yaml_graph = _parse_yaml_graph()
    sql_graph = _parse_sql_graph()

    expected = _normalize_graph(expected_graph)
    yaml_result = _normalize_graph(yaml_graph)
    sql_result = _normalize_graph(sql_graph)

    assert yaml_result == expected
    assert sql_result == expected
