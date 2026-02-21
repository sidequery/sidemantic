"""Widget example coverage for auto models and notebook-style metrics."""

from __future__ import annotations

import pytest

from sidemantic import Dimension, Metric, Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator


def test_notebook_metric_sql_parses_aggregations():
    """Notebook example metrics should parse into simple aggregations."""
    bid_requests = Metric(name="bid_requests", sql="sum(bid_request_cnt)")
    bid_floor_requests = Metric(name="bid_floor_requests", sql="sum(has_bid_floor_cnt)")
    avg_bid_floor = Metric(name="avg_bid_floor", sql="avg(bid_floor)")

    assert bid_requests.agg == "sum"
    assert bid_requests.sql == "bid_request_cnt"
    assert bid_floor_requests.agg == "sum"
    assert bid_floor_requests.sql == "has_bid_floor_cnt"
    assert avg_bid_floor.agg == "avg"
    assert avg_bid_floor.sql == "bid_floor"


def test_notebook_metric_sql_generates():
    """SQL generation should succeed for notebook-style metrics."""
    model = Model(
        name="auctions",
        table="auctions",
        primary_key="id",
        default_time_dimension="__time",
        dimensions=[
            Dimension(name="__time", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="bid_requests", sql="sum(bid_request_cnt)"),
            Metric(name="bid_floor_requests", sql="sum(has_bid_floor_cnt)"),
            Metric(name="avg_bid_floor", sql="avg(bid_floor)"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)
    generator = SQLGenerator(graph, dialect="duckdb")

    sql = generator.generate(
        metrics=["auctions.bid_requests", "auctions.bid_floor_requests", "auctions.avg_bid_floor"],
        dimensions=["auctions.__time__day"],
        order_by=["auctions.__time__day"],
        skip_default_time_dimensions=True,
    )

    assert "SELECT" in sql
    assert "FROM" in sql


def test_auto_model_metrics_generate_without_dependency_errors():
    """Auto-model style metrics should generate without missing-metric errors."""
    model = Model(
        name="widget_data",
        table="widget_data",
        primary_key="rowid",
        dimensions=[
            Dimension(name="__time", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="row_count", agg="count"),
            Metric(name="sum_has_bid_floor_cnt", agg="sum", sql="has_bid_floor_cnt"),
            Metric(name="avg_has_bid_floor_cnt", agg="avg", sql="has_bid_floor_cnt"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(model)
    generator = SQLGenerator(graph, dialect="duckdb")

    sql = generator.generate(
        metrics=[
            "widget_data.row_count",
            "widget_data.sum_has_bid_floor_cnt",
            "widget_data.avg_has_bid_floor_cnt",
        ],
        dimensions=["widget_data.__time__day"],
        order_by=["widget_data.__time__day"],
        skip_default_time_dimensions=True,
    )

    assert "SELECT" in sql
    assert "FROM" in sql


def test_build_auto_model_sets_simple_aggs():
    """Auto-model should create simple agg metrics (not inline SQL)."""
    pa = pytest.importorskip("pyarrow")
    from sidemantic.widget._auto_model import build_auto_model

    schema = pa.schema(
        [
            pa.field("__time", pa.timestamp("us")),
            pa.field("has_bid_floor_cnt", pa.int64()),
        ]
    )

    graph, time_dim = build_auto_model(schema, table_name="widget_data")
    model = graph.get_model("widget_data")

    assert time_dim == "__time"

    sum_metric = model.get_metric("sum_has_bid_floor_cnt")
    avg_metric = model.get_metric("avg_has_bid_floor_cnt")

    assert sum_metric.agg == "sum"
    assert sum_metric.sql == "has_bid_floor_cnt"
    assert avg_metric.agg == "avg"
    assert avg_metric.sql == "has_bid_floor_cnt"
