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
