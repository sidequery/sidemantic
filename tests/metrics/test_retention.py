"""Test retention metric type: cohort retention analysis."""

import duckdb
import pytest

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator
from tests.utils import df_rows


def test_retention_metric_validation():
    """Test that retention metric validates required fields."""
    # Valid retention metric
    m = Metric(
        name="install_retention",
        type="retention",
        entity="person_id",
        cohort_event="event = 'Application Installed'",
        activity_event="event IS NOT NULL",
        periods=28,
        retention_granularity="day",
    )
    assert m.type == "retention"
    assert m.entity == "person_id"
    assert m.cohort_event == "event = 'Application Installed'"
    assert m.activity_event == "event IS NOT NULL"
    assert m.periods == 28
    assert m.retention_granularity == "day"


def test_retention_metric_missing_entity():
    """Test that retention metric requires entity."""
    with pytest.raises(ValueError, match="retention metric requires 'entity' field"):
        Metric(
            name="bad_retention",
            type="retention",
            cohort_event="event = 'install'",
        )


def test_retention_metric_missing_cohort_event():
    """Test that retention metric requires cohort_event."""
    with pytest.raises(ValueError, match="retention metric requires 'cohort_event' field"):
        Metric(
            name="bad_retention",
            type="retention",
            entity="user_id",
        )


def test_retention_metric_defaults():
    """Test retention metric defaults for optional fields."""
    m = Metric(
        name="retention",
        type="retention",
        entity="user_id",
        cohort_event="event = 'signup'",
    )
    assert m.activity_event is None
    assert m.periods is None
    assert m.retention_granularity is None


def test_retention_sql_generation():
    """Test retention metric generates correct multi-CTE SQL."""
    events = Model(
        name="events",
        sql="""
            SELECT 1 AS user_id, 'install' AS event, '2024-01-01'::DATE AS ts
            UNION ALL SELECT 1, 'active', '2024-01-01'::DATE
            UNION ALL SELECT 1, 'active', '2024-01-02'::DATE
            UNION ALL SELECT 1, 'active', '2024-01-03'::DATE
            UNION ALL SELECT 2, 'install', '2024-01-01'::DATE
            UNION ALL SELECT 2, 'active', '2024-01-01'::DATE
            UNION ALL SELECT 2, 'active', '2024-01-02'::DATE
            UNION ALL SELECT 3, 'install', '2024-01-02'::DATE
            UNION ALL SELECT 3, 'active', '2024-01-02'::DATE
        """,
        primary_key="user_id",
        dimensions=[
            Dimension(name="user_id", sql="user_id", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[],
    )

    retention = Metric(
        name="install_retention",
        type="retention",
        entity="user_id",
        cohort_event="event = 'install'",
        activity_event="event IS NOT NULL",
        periods=3,
        retention_granularity="day",
    )

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(retention)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["install_retention"], dimensions=[])

    # Verify the SQL contains expected CTE structure
    assert "cohorts" in sql.lower()
    assert "activity" in sql.lower()
    assert "retention" in sql.lower()
    assert "cohort_sizes" in sql.lower()
    assert "retention_pct" in sql.lower()

    # Execute and check results
    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)

    # Columns: cohort_date, days_since, active_users, cohort_size, retention_pct
    assert len(rows) > 0

    # All rows should have 5 columns
    for row in rows:
        assert len(row) == 5

    # Day 0 retention should always be 100%
    day0_rows = [r for r in rows if r[1] == 0]
    for r in day0_rows:
        assert r[4] == 100.0


def test_retention_day_granularity_results():
    """Test retention metric produces correct day-level retention percentages."""
    events = Model(
        name="events",
        sql="""
            SELECT 1 AS uid, 'signup' AS event, '2024-01-01'::DATE AS ts
            UNION ALL SELECT 1, 'login', '2024-01-01'::DATE
            UNION ALL SELECT 1, 'login', '2024-01-02'::DATE
            UNION ALL SELECT 1, 'login', '2024-01-03'::DATE
            UNION ALL SELECT 2, 'signup', '2024-01-01'::DATE
            UNION ALL SELECT 2, 'login', '2024-01-01'::DATE
            UNION ALL SELECT 3, 'signup', '2024-01-01'::DATE
            UNION ALL SELECT 3, 'login', '2024-01-01'::DATE
            UNION ALL SELECT 3, 'login', '2024-01-03'::DATE
        """,
        primary_key="uid",
        dimensions=[
            Dimension(name="uid", sql="uid", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[],
    )

    retention = Metric(
        name="signup_retention",
        type="retention",
        entity="uid",
        cohort_event="event = 'signup'",
        activity_event="TRUE",
        periods=3,
        retention_granularity="day",
    )

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(retention)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["signup_retention"], dimensions=[])

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)

    # All 3 users signed up on 2024-01-01
    # Day 0: all 3 active (signup events) -> 100%
    # Day 1: user 1 active -> 1/3 -> 33.3%
    # Day 2: users 1 and 3 active -> 2/3 -> 66.7%
    cohort_rows = {r[1]: r for r in rows}

    assert cohort_rows[0][4] == 100.0  # Day 0: 100%
    assert cohort_rows[1][4] == 33.3  # Day 1: 1/3
    assert cohort_rows[2][4] == 66.7  # Day 2: 2/3


def test_retention_default_activity_event():
    """Test retention metric with no explicit activity_event defaults to TRUE."""
    events = Model(
        name="events",
        sql="""
            SELECT 1 AS uid, 'signup' AS event, '2024-01-01'::DATE AS ts
            UNION ALL SELECT 1, 'login', '2024-01-02'::DATE
        """,
        primary_key="uid",
        dimensions=[
            Dimension(name="uid", sql="uid", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[],
    )

    retention = Metric(
        name="retention",
        type="retention",
        entity="uid",
        cohort_event="event = 'signup'",
        periods=2,
    )

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(retention)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["retention"], dimensions=[])

    # Should use TRUE as the activity filter when activity_event is not set
    assert "TRUE" in sql

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)
    assert len(rows) > 0


def test_retention_model_level_metric():
    """Test retention metric defined at model level."""
    events = Model(
        name="events",
        sql="""
            SELECT 1 AS uid, 'install' AS event, '2024-01-01'::DATE AS ts
            UNION ALL SELECT 1, 'open', '2024-01-02'::DATE
        """,
        primary_key="uid",
        dimensions=[
            Dimension(name="uid", sql="uid", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[
            Metric(
                name="app_retention",
                type="retention",
                entity="uid",
                cohort_event="event = 'install'",
                activity_event="TRUE",
                periods=1,
                retention_granularity="day",
            )
        ],
    )

    graph = SemanticGraph()
    graph.add_model(events)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["events.app_retention"], dimensions=[])

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)
    assert len(rows) > 0


def test_retention_week_granularity():
    """Test retention metric with weekly granularity."""
    events = Model(
        name="events",
        sql="""
            SELECT 1 AS uid, 'signup' AS event, '2024-01-01'::DATE AS ts
            UNION ALL SELECT 1, 'login', '2024-01-08'::DATE
            UNION ALL SELECT 1, 'login', '2024-01-15'::DATE
            UNION ALL SELECT 2, 'signup', '2024-01-01'::DATE
            UNION ALL SELECT 2, 'login', '2024-01-08'::DATE
        """,
        primary_key="uid",
        dimensions=[
            Dimension(name="uid", sql="uid", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[],
    )

    retention = Metric(
        name="weekly_retention",
        type="retention",
        entity="uid",
        cohort_event="event = 'signup'",
        activity_event="TRUE",
        periods=2,
        retention_granularity="week",
    )

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(retention)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["weekly_retention"], dimensions=[])

    assert "weeks_since" in sql

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)

    # Week 0: both users active -> 100%
    # Week 1: both users active -> 100%
    # Week 2: only user 1 -> 50%
    week_data = {r[1]: r for r in rows}
    assert week_data[0][4] == 100.0
    assert week_data[1][4] == 100.0
    assert week_data[2][4] == 50.0


def test_retention_model_placeholder_expansion_sql_model():
    """Test that {model} placeholders in cohort_event/activity_event are expanded to table alias."""
    events = Model(
        name="events",
        sql="""
            SELECT 1 AS uid, 'signup' AS event, '2024-01-01'::DATE AS ts
            UNION ALL SELECT 1, 'login', '2024-01-02'::DATE
        """,
        primary_key="uid",
        dimensions=[
            Dimension(name="uid", sql="uid", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[],
    )

    retention = Metric(
        name="retention",
        type="retention",
        entity="uid",
        cohort_event="{model}.event = 'signup'",
        activity_event="{model}.event IS NOT NULL",
        periods=1,
        retention_granularity="day",
    )

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(retention)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["retention"], dimensions=[])

    # {model} should be replaced with 't' (SQL subquery alias)
    assert "{model}" not in sql
    assert "t.event = 'signup'" in sql
    assert "t.event IS NOT NULL" in sql

    # Should still execute correctly
    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)
    assert len(rows) > 0


def test_retention_model_placeholder_expansion_table_model():
    """Test that {model} placeholders are stripped for table-backed models."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE test_events AS
        SELECT 1 AS uid, 'signup' AS event, '2024-01-01'::DATE AS ts
        UNION ALL SELECT 1, 'login', '2024-01-02'::DATE
    """)

    events = Model(
        name="events",
        table="test_events",
        primary_key="uid",
        dimensions=[
            Dimension(name="uid", sql="uid", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[],
    )

    retention = Metric(
        name="retention",
        type="retention",
        entity="uid",
        cohort_event="{model}.event = 'signup'",
        activity_event="{model}.event IS NOT NULL",
        periods=1,
        retention_granularity="day",
    )

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(retention)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["retention"], dimensions=[])

    # {model}. should be stripped for table-backed models
    assert "{model}" not in sql
    assert "event = 'signup'" in sql

    result = conn.execute(sql)
    rows = df_rows(result)
    assert len(rows) > 0


def test_retention_periods_zero_raises_validation_error():
    """Test that periods=0 raises a validation error instead of silently becoming 28."""
    events = Model(
        name="events",
        sql="""
            SELECT 1 AS uid, 'signup' AS event, '2024-01-01'::DATE AS ts
        """,
        primary_key="uid",
        dimensions=[
            Dimension(name="uid", sql="uid", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[],
    )

    retention = Metric(
        name="retention",
        type="retention",
        entity="uid",
        cohort_event="event = 'signup'",
        periods=0,
    )

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(retention)

    generator = SQLGenerator(graph)
    with pytest.raises(ValueError, match="Invalid periods value"):
        generator.generate(metrics=["retention"], dimensions=[])


def test_retention_yaml_retention_granularity_key():
    """Test that YAML with retention_granularity: week parses correctly."""
    import os
    import tempfile

    from sidemantic.adapters.sidemantic import SidemanticAdapter

    yaml_content = """
models:
  - name: events
    table: events
    dimensions:
      - name: user_id
        type: categorical
      - name: ts
        type: time
    metrics:
      - name: weekly_retention
        type: retention
        entity: user_id
        cohort_event: "event = 'signup'"
        retention_granularity: week
        periods: 4
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        tmp_path = f.name

    try:
        adapter = SidemanticAdapter()
        graph = adapter.parse(tmp_path)
        model = graph.get_model("events")
        metric = model.get_metric("weekly_retention")
        assert metric.retention_granularity == "week"
        assert metric.periods == 4
    finally:
        os.unlink(tmp_path)


def test_retention_yaml_granularity_fallback():
    """Test that YAML with granularity: month also parses for retention metrics."""
    import os
    import tempfile

    from sidemantic.adapters.sidemantic import SidemanticAdapter

    yaml_content = """
metrics:
  - name: monthly_retention
    type: retention
    entity: user_id
    cohort_event: "event = 'signup'"
    granularity: month
    periods: 12
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        tmp_path = f.name

    try:
        adapter = SidemanticAdapter()
        graph = adapter.parse(tmp_path)
        metric = graph.get_metric("monthly_retention")
        assert metric.retention_granularity == "month"
        assert metric.periods == 12
    finally:
        os.unlink(tmp_path)


def test_retention_export_roundtrip_retention_granularity():
    """Test that export uses retention_granularity key and roundtrips correctly."""
    import os
    import tempfile

    from sidemantic.adapters.sidemantic import SidemanticAdapter

    # Create a graph with a retention metric
    graph = SemanticGraph()
    retention = Metric(
        name="weekly_retention",
        type="retention",
        entity="user_id",
        cohort_event="event = 'signup'",
        retention_granularity="week",
        periods=4,
    )
    graph.add_metric(retention)

    # Export
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        tmp_path = f.name

    try:
        adapter = SidemanticAdapter()
        adapter.export(graph, tmp_path)

        # Re-parse and verify
        graph2 = adapter.parse(tmp_path)
        metric = graph2.get_metric("weekly_retention")
        assert metric.retention_granularity == "week"
        assert metric.periods == 4
    finally:
        os.unlink(tmp_path)


def test_retention_model_placeholder_in_time_dimension():
    """Test that {model} placeholder in time dimension sql_expr is resolved."""
    events = Model(
        name="events",
        sql="""
            SELECT 1 AS uid, 'signup' AS event, '2024-01-01'::DATE AS created_at
            UNION ALL SELECT 1, 'login', '2024-01-02'::DATE
        """,
        primary_key="uid",
        dimensions=[
            Dimension(name="uid", sql="uid", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="{model}.created_at", type="time"),
        ],
        metrics=[],
    )

    retention = Metric(
        name="retention",
        type="retention",
        entity="uid",
        cohort_event="event = 'signup'",
        periods=1,
        retention_granularity="day",
    )

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(retention)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["retention"], dimensions=[])

    # {model} should be replaced with 't' for SQL-backed models
    assert "{model}" not in sql
    assert "t.created_at" in sql

    # Should execute correctly
    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)
    assert len(rows) > 0
    # Day 0 retention should be 100%
    day0 = [r for r in rows if r[1] == 0]
    assert day0[0][4] == 100.0


def test_retention_aliased_entity_dimension():
    """Test that aliased entity dimension (name != sql) generates correct SQL."""
    events = Model(
        name="events",
        sql="""
            SELECT 1 AS person_id, 'signup' AS event, '2024-01-01'::DATE AS ts
            UNION ALL SELECT 1, 'login', '2024-01-02'::DATE
            UNION ALL SELECT 2, 'signup', '2024-01-01'::DATE
            UNION ALL SELECT 2, 'login', '2024-01-01'::DATE
        """,
        primary_key="person_id",
        dimensions=[
            Dimension(name="user_id", sql="person_id", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[],
    )

    retention = Metric(
        name="retention",
        type="retention",
        entity="user_id",
        cohort_event="event = 'signup'",
        periods=1,
        retention_granularity="day",
    )

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(retention)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["retention"], dimensions=[])

    # Should use person_id (sql_expr) in SELECT from raw table, aliased as user_id
    assert "person_id AS user_id" in sql
    # Downstream CTEs should reference user_id (alias)
    assert "c.user_id" in sql
    assert "a.user_id" in sql

    # Should execute correctly
    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)
    assert len(rows) > 0
    day0 = [r for r in rows if r[1] == 0]
    assert day0[0][4] == 100.0


def test_retention_metric_level_filters():
    """Test that metric.filters are included in retention CTE predicates."""
    events = Model(
        name="events",
        sql="""
            SELECT 1 AS uid, 'signup' AS event, '2024-01-01'::DATE AS ts, 'US' AS country
            UNION ALL SELECT 1, 'login', '2024-01-02'::DATE, 'US'
            UNION ALL SELECT 2, 'signup', '2024-01-01'::DATE, 'UK'
            UNION ALL SELECT 2, 'login', '2024-01-02'::DATE, 'UK'
            UNION ALL SELECT 3, 'signup', '2024-01-01'::DATE, 'US'
            UNION ALL SELECT 3, 'login', '2024-01-01'::DATE, 'US'
        """,
        primary_key="uid",
        dimensions=[
            Dimension(name="uid", sql="uid", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
            Dimension(name="country", sql="country", type="categorical"),
        ],
        metrics=[],
    )

    # Metric-level filter scopes to US only
    retention = Metric(
        name="us_retention",
        type="retention",
        entity="uid",
        cohort_event="event = 'signup'",
        activity_event="TRUE",
        periods=1,
        retention_granularity="day",
        filters=["country = 'US'"],
    )

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(retention)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["us_retention"], dimensions=[])

    # Metric filter should appear in the SQL
    assert "country = 'US'" in sql

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)

    # Only US users (uid 1 and 3) should be in cohort
    assert len(rows) > 0
    day0 = [r for r in rows if r[1] == 0]
    assert day0[0][3] == 2  # cohort_size = 2 (US users only)
    # Day 0: both active -> 100%
    assert day0[0][4] == 100.0
    # Day 1: only user 1 active -> 50%
    day1 = [r for r in rows if r[1] == 1]
    assert day1[0][4] == 50.0


def test_retention_multiple_retention_metrics_raises():
    """Test that querying two retention metrics raises ValueError."""
    events = Model(
        name="events",
        sql="SELECT 1 AS uid, 'signup' AS event, '2024-01-01'::DATE AS ts",
        primary_key="uid",
        dimensions=[
            Dimension(name="uid", sql="uid", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[],
    )

    ret1 = Metric(
        name="retention_a",
        type="retention",
        entity="uid",
        cohort_event="event = 'signup'",
        periods=1,
    )
    ret2 = Metric(
        name="retention_b",
        type="retention",
        entity="uid",
        cohort_event="event = 'signup'",
        periods=2,
    )

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(ret1)
    graph.add_metric(ret2)

    generator = SQLGenerator(graph)
    with pytest.raises(ValueError, match="Only one retention metric can be queried at a time"):
        generator.generate(metrics=["retention_a", "retention_b"], dimensions=[])


def test_retention_mixed_with_regular_metric_raises():
    """Test that mixing retention + regular metric raises ValueError."""
    events = Model(
        name="events",
        sql="SELECT 1 AS uid, 'signup' AS event, '2024-01-01'::DATE AS ts, 10 AS revenue",
        primary_key="uid",
        dimensions=[
            Dimension(name="uid", sql="uid", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[
            Metric(name="total_revenue", agg="sum", sql="revenue"),
        ],
    )

    retention = Metric(
        name="retention",
        type="retention",
        entity="uid",
        cohort_event="event = 'signup'",
        periods=1,
    )

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(retention)

    generator = SQLGenerator(graph)
    with pytest.raises(ValueError, match="Retention metrics cannot be combined with other metrics"):
        generator.generate(metrics=["retention", "events.total_revenue"], dimensions=[])


def test_retention_offset_in_sql():
    """Test that offset parameter is included in retention SQL output."""
    events = Model(
        name="events",
        sql="""
            SELECT 1 AS uid, 'signup' AS event, '2024-01-01'::DATE AS ts
            UNION ALL SELECT 1, 'login', '2024-01-02'::DATE
            UNION ALL SELECT 2, 'signup', '2024-01-01'::DATE
            UNION ALL SELECT 2, 'login', '2024-01-01'::DATE
        """,
        primary_key="uid",
        dimensions=[
            Dimension(name="uid", sql="uid", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[],
    )

    retention = Metric(
        name="retention",
        type="retention",
        entity="uid",
        cohort_event="event = 'signup'",
        periods=2,
        retention_granularity="day",
    )

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(retention)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["retention"], dimensions=[], limit=5, offset=10)

    assert "LIMIT 5" in sql
    assert "OFFSET 10" in sql

    # Without offset, OFFSET should not appear
    sql_no_offset = generator.generate(metrics=["retention"], dimensions=[], limit=5)
    assert "OFFSET" not in sql_no_offset


def test_retention_ambiguous_model_raises():
    """Test that graph-level retention metric with entity in multiple models raises ValueError."""
    orders = Model(
        name="orders",
        sql="SELECT 1 AS user_id, 'purchase' AS event, '2024-01-01'::DATE AS ts",
        primary_key="user_id",
        dimensions=[
            Dimension(name="user_id", sql="user_id", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[],
    )

    sessions = Model(
        name="sessions",
        sql="SELECT 1 AS user_id, 'visit' AS event, '2024-01-01'::DATE AS ts",
        primary_key="user_id",
        dimensions=[
            Dimension(name="user_id", sql="user_id", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[],
    )

    retention = Metric(
        name="retention",
        type="retention",
        entity="user_id",
        cohort_event="event = 'purchase'",
        periods=7,
        retention_granularity="day",
    )

    graph = SemanticGraph()
    graph.add_model(orders)
    graph.add_model(sessions)
    graph.add_metric(retention)

    generator = SQLGenerator(graph)
    with pytest.raises(ValueError, match="Ambiguous model for retention metric") as exc_info:
        generator.generate(metrics=["retention"], dimensions=[])

    # Verify error mentions both model names
    err_msg = str(exc_info.value)
    assert "orders" in err_msg
    assert "sessions" in err_msg


def test_retention_graph_metric_does_not_bind_to_same_name_model_metric():
    """Graph-level retention metric must not bind to a model-level metric with the same name."""
    # Model A: model-level metric named "retention" with cohort_event = 'install'
    model_a = Model(
        name="app_events",
        sql="""
            SELECT 1 AS uid, 'install' AS event, '2024-01-01'::DATE AS ts
            UNION ALL SELECT 1, 'open', '2024-01-02'::DATE
            UNION ALL SELECT 2, 'install', '2024-01-01'::DATE
        """,
        primary_key="uid",
        dimensions=[
            Dimension(name="uid", sql="uid", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[
            Metric(
                name="retention",
                type="retention",
                entity="uid",
                cohort_event="event = 'install'",
                periods=2,
                retention_granularity="day",
            )
        ],
    )

    # Model B: model-level metric named "retention" with cohort_event = 'purchase'
    model_b = Model(
        name="shop_events",
        sql="""
            SELECT 10 AS uid, 'purchase' AS event, '2024-01-01'::DATE AS ts
            UNION ALL SELECT 10, 'browse', '2024-01-02'::DATE
        """,
        primary_key="uid",
        dimensions=[
            Dimension(name="uid", sql="uid", type="categorical"),
            Dimension(name="event", sql="event", type="categorical"),
            Dimension(name="ts", sql="ts", type="time"),
        ],
        metrics=[
            Metric(
                name="retention",
                type="retention",
                entity="uid",
                cohort_event="event = 'purchase'",
                periods=2,
                retention_granularity="day",
            )
        ],
    )

    # Graph-level retention metric: same name but different config
    graph_retention = Metric(
        name="retention",
        type="retention",
        entity="uid",
        cohort_event="event = 'signup'",
        activity_event="TRUE",
        periods=1,
        retention_granularity="day",
    )

    graph = SemanticGraph()
    graph.add_model(model_a)
    graph.add_model(model_b)
    # Insert graph-level metric directly (retention type is not auto-registered)
    graph.metrics["retention"] = graph_retention

    generator = SQLGenerator(graph)

    # The resolved metric object (graph_retention) is NOT in either model's
    # metrics list.  The fallback should proceed to entity-dimension matching,
    # find uid in both models, and raise an ambiguity error.  Before the fix,
    # m.get_metric("retention") would match by name alone, incorrectly binding
    # the graph-level metric to whichever model was iterated first.
    with pytest.raises(ValueError, match="Ambiguous model for retention metric"):
        generator.generate(metrics=["retention"], dimensions=[])
