"""Shared query-history parameter validation coverage."""

import pytest

from sidemantic.db.base import validate_query_history_params
from sidemantic.db.bigquery import BigQueryAdapter
from sidemantic.db.clickhouse import ClickHouseAdapter
from sidemantic.db.databricks import DatabricksAdapter
from sidemantic.db.snowflake import SnowflakeAdapter


def test_validate_query_history_params_coerces_safe_integer_strings():
    assert validate_query_history_params("4", "9") == (4, 9)


@pytest.mark.parametrize(
    ("days_back", "limit", "match"),
    [
        ("1; DROP TABLE jobs", 10, "days_back must be a positive integer"),
        (-1, 10, "days_back must be a positive integer"),
        (0, 10, "days_back must be a positive integer"),
        (True, 10, "days_back must be a positive integer"),
        (366, 10, "days_back must be <= 365"),
        (7, "10; DROP TABLE jobs", "limit must be a positive integer"),
        (7, -1, "limit must be a positive integer"),
        (7, 0, "limit must be a positive integer"),
        (7, True, "limit must be a positive integer"),
        (7, 10_001, "limit must be <= 10000"),
    ],
)
def test_validate_query_history_params_rejects_unsafe_values(days_back, limit, match):
    with pytest.raises(ValueError, match=match):
        validate_query_history_params(days_back, limit)


@pytest.mark.parametrize(
    "adapter",
    [
        BigQueryAdapter.__new__(BigQueryAdapter),
        ClickHouseAdapter.__new__(ClickHouseAdapter),
        DatabricksAdapter.__new__(DatabricksAdapter),
        SnowflakeAdapter.__new__(SnowflakeAdapter),
    ],
)
def test_query_history_adapters_reject_interpolated_values_before_execution(adapter):
    with pytest.raises(ValueError, match="days_back must be a positive integer"):
        adapter.get_query_history(days_back="1; DROP TABLE query_log", limit=10)

    with pytest.raises(ValueError, match="limit must be a positive integer"):
        adapter.get_query_history(days_back=1, limit="10; DROP TABLE query_log")


def test_snowflake_query_history_enforces_information_schema_lookback_limit():
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)

    with pytest.raises(ValueError, match="days_back must be <= 7"):
        adapter.get_query_history(days_back=8, limit=10)


class _CapturedResult:
    def fetchall(self):
        return []


def _capture_sql(adapter):
    captured = {}

    def execute(sql):
        captured["sql"] = sql
        return _CapturedResult()

    adapter.execute = execute
    return captured


def test_snowflake_query_history_requests_max_result_limit():
    """QUERY_HISTORY applies RESULT_LIMIT (default 100) before the outer WHERE/LIMIT.

    Without an explicit RESULT_LIMIT the import silently caps at 100 rows no
    matter what limit the caller asked for.
    """
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    captured = _capture_sql(adapter)

    adapter.get_query_history(days_back=7, limit=1000)

    assert "RESULT_LIMIT => 10000" in captured["sql"]
    assert "LIMIT 1000" in captured["sql"]


def test_databricks_query_history_uses_execution_status_column():
    """system.query.history has execution_status, not status."""
    adapter = DatabricksAdapter.__new__(DatabricksAdapter)
    captured = _capture_sql(adapter)

    adapter.get_query_history(days_back=7, limit=100)

    assert "execution_status = 'FINISHED'" in captured["sql"]
    assert " status = " not in captured["sql"]


def test_clickhouse_query_history_filters_initial_queries():
    """Distributed subqueries must not pollute the import on clusters."""
    from types import SimpleNamespace

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    captured = {}

    def query(sql):
        captured["sql"] = sql
        return SimpleNamespace(result_rows=[])

    adapter.client = SimpleNamespace(query=query)

    adapter.get_query_history(days_back=7, limit=100)

    assert "is_initial_query = 1" in captured["sql"]


def test_bigquery_query_history_lowercases_region_qualifier():
    """INFORMATION_SCHEMA region qualifiers are lowercase (region-us)."""
    from types import SimpleNamespace

    adapter = BigQueryAdapter.__new__(BigQueryAdapter)
    adapter.project_id = "my-project"
    adapter.client = SimpleNamespace(location="US")
    captured = _capture_sql(adapter)

    adapter.get_query_history(days_back=7, limit=100)

    assert "`my-project.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`" in captured["sql"]
