"""Tests for cohort metric type (two-level aggregation with HAVING)."""

import duckdb

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator
from tests.utils import df_rows


def _make_events_model():
    return Model(
        name="events",
        sql="""
            SELECT 1 AS user_id, 'web' AS platform, '2024-01-01'::DATE AS ts
            UNION ALL SELECT 1, 'mobile', '2024-01-02'::DATE
            UNION ALL SELECT 1, 'web', '2024-01-03'::DATE
            UNION ALL SELECT 2, 'web', '2024-01-01'::DATE
            UNION ALL SELECT 2, 'web', '2024-01-02'::DATE
            UNION ALL SELECT 3, 'mobile', '2024-01-01'::DATE
        """,
        primary_key="user_id",
        dimensions=[
            Dimension(name="user_id", sql="user_id", type="categorical"),
            Dimension(name="platform", sql="platform", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
    )


def _make_multi_platform_metric():
    """Cohort metric: count users active on 2+ platforms."""
    return Metric(
        name="multi_platform_users",
        type="cohort",
        entity="user_id",
        inner_metrics=[{"name": "platform_count", "agg": "count_distinct", "sql": "platform"}],
        having="platform_count >= 2",
        agg="count",
    )


def test_cohort_basic():
    """Basic cohort metric: count users on 2+ platforms."""
    events = _make_events_model()
    metric = _make_multi_platform_metric()
    events.metrics.append(metric)

    graph = SemanticGraph()
    graph.add_model(events)

    gen = SQLGenerator(graph)
    sql = gen.generate(metrics=["events.multi_platform_users"], dimensions=[])

    conn = duckdb.connect(":memory:")
    rows = df_rows(conn.execute(sql))
    # Only user 1 has both web and mobile
    assert rows[0][0] == 1


def test_cohort_graph_level_metric():
    """Graph-level cohort metric (added via graph.add_metric) resolves correctly."""
    events = _make_events_model()
    metric = _make_multi_platform_metric()

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(metric)

    gen = SQLGenerator(graph)
    # Unqualified name: should resolve via graph.get_metric, then find model by entity
    sql = gen.generate(metrics=["multi_platform_users"], dimensions=[])

    conn = duckdb.connect(":memory:")
    rows = df_rows(conn.execute(sql))
    assert rows[0][0] == 1


def test_cohort_with_dimension():
    """Cohort metric with a query-level dimension unpacks tuples correctly."""
    events = Model(
        name="events",
        sql="""
            SELECT 1 AS user_id, 'web' AS platform, 'US' AS region, '2024-01-01'::DATE AS ts
            UNION ALL SELECT 1, 'mobile', 'US', '2024-01-02'::DATE
            UNION ALL SELECT 2, 'web', 'US', '2024-01-01'::DATE
            UNION ALL SELECT 3, 'mobile', 'EU', '2024-01-01'::DATE
            UNION ALL SELECT 3, 'web', 'EU', '2024-01-02'::DATE
        """,
        primary_key="user_id",
        dimensions=[
            Dimension(name="user_id", sql="user_id", type="categorical"),
            Dimension(name="platform", sql="platform", type="categorical"),
            Dimension(name="region", sql="region", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[_make_multi_platform_metric()],
    )

    graph = SemanticGraph()
    graph.add_model(events)

    gen = SQLGenerator(graph)
    # This previously raised AttributeError: 'tuple' object has no attribute 'get'
    sql = gen.generate(
        metrics=["events.multi_platform_users"],
        dimensions=["events.region"],
    )

    conn = duckdb.connect(":memory:")
    rows = df_rows(conn.execute(sql))
    # user 1 (US, 2 platforms), user 3 (EU, 2 platforms)
    result = {r[0]: r[1] for r in rows}
    assert result["US"] == 1
    assert result["EU"] == 1
