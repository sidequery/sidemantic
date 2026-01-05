"""Tests for Malloy adapter - SQL-based sources."""

from pathlib import Path

from sidemantic.adapters.malloy import MalloyAdapter


def test_sql_source_parsing():
    """Test that SQL-based sources are parsed correctly."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/sql_sources.malloy"))

    # Check all models were parsed
    assert "active_users" in graph.models
    assert "all_users" in graph.models
    assert "user_summary" in graph.models


def test_sql_source_has_sql_not_table():
    """Test that SQL sources have sql property, not table."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/sql_sources.malloy"))

    active_users = graph.get_model("active_users")
    assert active_users is not None
    # Should have sql, not table
    assert active_users.sql is not None
    assert "SELECT" in active_users.sql
    assert "FROM users" in active_users.sql
    assert active_users.table is None or active_users.table == ""


def test_sql_source_preserves_query():
    """Test that SQL query content is preserved."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/sql_sources.malloy"))

    active_users = graph.get_model("active_users")
    # Check key parts of the query are preserved
    assert "status = 'active'" in active_users.sql
    assert "deleted_at IS NULL" in active_users.sql

    user_summary = graph.get_model("user_summary")
    # Check complex query parts
    assert "LEFT JOIN orders" in user_summary.sql or "LEFT JOIN" in user_summary.sql
    assert "GROUP BY" in user_summary.sql


def test_sql_source_dimensions_and_measures():
    """Test that dimensions and measures are parsed from SQL sources."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/sql_sources.malloy"))

    active_users = graph.get_model("active_users")
    assert active_users.primary_key == "id"

    # Check dimensions
    dim_names = {d.name for d in active_users.dimensions}
    assert "id" in dim_names
    assert "email" in dim_names
    assert "name" in dim_names

    # Check measures
    metric_names = {m.name for m in active_users.metrics}
    assert "user_count" in metric_names
    assert "unique_users" in metric_names

    # Check aggregation types
    user_count = active_users.get_metric("user_count")
    assert user_count.agg == "count"

    unique_users = active_users.get_metric("unique_users")
    assert unique_users.agg == "count_distinct"


def test_table_source_for_comparison():
    """Test that table-based sources still work alongside SQL sources."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/sql_sources.malloy"))

    all_users = graph.get_model("all_users")
    assert all_users is not None
    # Should have table, not sql
    assert all_users.table == "users.parquet"
    assert all_users.sql is None or all_users.sql == ""


def test_sql_source_export_roundtrip():
    """Test that SQL sources can be exported and re-parsed."""
    import tempfile

    adapter = MalloyAdapter()
    graph1 = adapter.parse(Path("tests/fixtures/malloy/sql_sources.malloy"))

    # Export
    with tempfile.NamedTemporaryFile(suffix=".malloy", delete=False) as f:
        output_path = Path(f.name)

    try:
        adapter.export(graph1, output_path)

        # Re-parse
        graph2 = adapter.parse(output_path)

        # Check models exist
        assert "active_users" in graph2.models
        assert "all_users" in graph2.models

        # Check SQL was preserved
        active_users2 = graph2.get_model("active_users")
        assert active_users2.sql is not None
        assert "SELECT" in active_users2.sql

        # Check table was preserved
        all_users2 = graph2.get_model("all_users")
        assert all_users2.table == "users.parquet"

    finally:
        output_path.unlink()


def test_sql_source_descriptions():
    """Test that SQL sources preserve descriptions."""
    adapter = MalloyAdapter()
    graph = adapter.parse(Path("tests/fixtures/malloy/sql_sources.malloy"))

    active_users = graph.get_model("active_users")
    assert active_users.description is not None
    assert "SQL" in active_users.description or "inline" in active_users.description

    user_summary = graph.get_model("user_summary")
    assert user_summary.description is not None
    assert "SQL" in user_summary.description or "complex" in user_summary.description
