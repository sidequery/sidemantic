"""Tests for cohort metric type (two-level aggregation with HAVING)."""

import duckdb
import pytest

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


def test_cohort_model_scoped_unqualified():
    """Model-scoped cohort metric with unqualified name resolves correctly."""
    events = _make_events_model()
    metric = _make_multi_platform_metric()
    events.metrics.append(metric)

    graph = SemanticGraph()
    graph.add_model(events)

    gen = SQLGenerator(graph)
    # Unqualified name for a model-scoped metric (not added via graph.add_metric)
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


def test_cohort_outer_agg_without_sql_raises():
    """Non-count outer agg without sql should raise, not emit SUM(*)."""
    events = _make_events_model()
    metric = Metric(
        name="bad_cohort",
        type="cohort",
        entity="user_id",
        inner_metrics=[{"name": "cnt", "agg": "count"}],
        having="cnt >= 2",
        agg="avg",
        # no sql field
    )
    events.metrics.append(metric)

    graph = SemanticGraph()
    graph.add_model(events)

    gen = SQLGenerator(graph)
    with pytest.raises(ValueError, match="requires a 'sql' field"):
        gen.generate(metrics=["events.bad_cohort"], dimensions=[])


def test_cohort_inner_metric_agg_without_sql_raises():
    """Non-count inner metric agg without sql should raise at construction time."""
    with pytest.raises(ValueError, match="requires a 'sql' field"):
        Metric(
            name="bad_inner",
            type="cohort",
            entity="user_id",
            inner_metrics=[{"name": "total", "agg": "sum"}],  # no sql
            having="total > 0",
            agg="count",
        )


def test_cohort_inner_metric_missing_name_raises():
    """inner_metrics entry without a name should raise at construction time."""
    with pytest.raises(ValueError, match="must be a dict with at least a 'name' key"):
        Metric(
            name="bad_shape",
            type="cohort",
            entity="user_id",
            inner_metrics=[{}],
            having="cnt >= 1",
            agg="count",
        )


def test_cohort_inner_count_distinct_without_sql_raises():
    """count_distinct without sql would emit COUNT(DISTINCT *), which is invalid."""
    with pytest.raises(ValueError, match="requires a 'sql' field"):
        Metric(
            name="bad_cd",
            type="cohort",
            entity="user_id",
            inner_metrics=[{"name": "uniq", "agg": "count_distinct"}],
            having="uniq >= 2",
            agg="count",
        )


def test_cohort_unknown_dimension_raises():
    """Referencing a nonexistent dimension should raise, not silently drop it."""
    events = _make_events_model()
    metric = _make_multi_platform_metric()
    events.metrics.append(metric)

    graph = SemanticGraph()
    graph.add_model(events)

    gen = SQLGenerator(graph)
    with pytest.raises(ValueError, match="Dimension 'nonexistent' not found"):
        gen.generate(
            metrics=["events.multi_platform_users"],
            dimensions=["events.nonexistent"],
        )


def test_cohort_outer_sql_references_subquery():
    """Cohort metric with explicit sql should reference cohort_sub, not inner alias."""
    events = Model(
        name="events",
        sql="""
            SELECT 1 AS user_id, 'web' AS platform, 10 AS score
            UNION ALL SELECT 1, 'mobile', 20
            UNION ALL SELECT 2, 'web', 5
            UNION ALL SELECT 2, 'mobile', 15
            UNION ALL SELECT 3, 'mobile', 30
        """,
        primary_key="user_id",
        dimensions=[
            Dimension(name="user_id", sql="user_id", type="categorical"),
            Dimension(name="platform", sql="platform", type="categorical"),
            Dimension(name="score", sql="score", type="numeric"),
        ],
        metrics=[
            Metric(
                name="avg_total_score",
                type="cohort",
                entity="user_id",
                inner_metrics=[{"name": "total_score", "agg": "sum", "sql": "score"}],
                having="total_score > 0",
                agg="avg",
                sql="cohort_sub.total_score",
            ),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(events)

    gen = SQLGenerator(graph)
    sql = gen.generate(metrics=["events.avg_total_score"], dimensions=[])

    # Should not contain "t.total_score" in the outer query
    assert "t.total_score" not in sql.split("cohort_sub")[0] or "cohort_sub.total_score" in sql

    conn = duckdb.connect(":memory:")
    rows = df_rows(conn.execute(sql))
    # user1: 30, user2: 20, user3: 30 -> avg = (30+20+30)/3 = 26.67
    assert len(rows) == 1
    assert abs(rows[0][0] - 26.667) < 1


def test_cohort_ambiguous_unqualified_raises():
    """Same cohort metric name on two models should raise ambiguity error."""
    events1 = Model(
        name="events1",
        sql="SELECT 1 AS user_id, 'web' AS platform",
        primary_key="user_id",
        dimensions=[
            Dimension(name="user_id", sql="user_id", type="categorical"),
            Dimension(name="platform", sql="platform", type="categorical"),
        ],
        metrics=[_make_multi_platform_metric()],
    )
    events2 = Model(
        name="events2",
        sql="SELECT 1 AS user_id, 'mobile' AS platform",
        primary_key="user_id",
        dimensions=[
            Dimension(name="user_id", sql="user_id", type="categorical"),
            Dimension(name="platform", sql="platform", type="categorical"),
        ],
        metrics=[_make_multi_platform_metric()],
    )

    graph = SemanticGraph()
    graph.add_model(events1)
    graph.add_model(events2)

    gen = SQLGenerator(graph)
    with pytest.raises(ValueError, match="Ambiguous metric"):
        gen.generate(metrics=["multi_platform_users"], dimensions=[])


def test_cohort_reserved_word_dimension():
    """Dimension named with a reserved word should be quoted in generated SQL."""
    events = Model(
        name="events",
        sql="""
            SELECT 1 AS user_id, 'web' AS platform, 'active' AS "order"
            UNION ALL SELECT 1, 'mobile', 'active'
            UNION ALL SELECT 2, 'web', 'pending'
        """,
        primary_key="user_id",
        dimensions=[
            Dimension(name="user_id", sql="user_id", type="categorical"),
            Dimension(name="platform", sql="platform", type="categorical"),
            Dimension(name="order", sql='"order"', type="categorical"),
        ],
        metrics=[_make_multi_platform_metric()],
    )

    graph = SemanticGraph()
    graph.add_model(events)

    gen = SQLGenerator(graph)
    sql = gen.generate(
        metrics=["events.multi_platform_users"],
        dimensions=["events.order"],
    )

    # Reserved word 'order' must be quoted in the SQL
    assert '"order"' in sql

    conn = duckdb.connect(":memory:")
    rows = df_rows(conn.execute(sql))
    # user 1 has both platforms, order='active'
    assert len(rows) == 1
    assert rows[0][0] == "active"


def test_cohort_mixed_with_conversion_raises():
    """Mixing cohort + conversion metrics should raise, not silently drop one."""
    events = _make_events_model()
    cohort = _make_multi_platform_metric()
    events.metrics.append(cohort)

    funnel = Metric(
        name="signup_funnel",
        type="conversion",
        entity="user_id",
        steps=["platform = 'web'", "platform = 'mobile'"],
    )
    events.metrics.append(funnel)

    graph = SemanticGraph()
    graph.add_model(events)

    gen = SQLGenerator(graph)
    with pytest.raises(ValueError, match="cannot be combined"):
        gen.generate(
            metrics=["events.multi_platform_users", "events.signup_funnel"],
            dimensions=[],
        )
