"""Tests for time intelligence helpers."""

import pytest

from sidemantic.core.time_intelligence import TimeComparison, TrailingPeriod, generate_time_comparison_sql


def test_time_comparison_default_offsets():
    assert TimeComparison(type="dod", metric="m").offset_interval == (1, "day")
    assert TimeComparison(type="wow", metric="m").offset_interval == (1, "week")
    assert TimeComparison(type="mom", metric="m").offset_interval == (1, "month")
    assert TimeComparison(type="qoq", metric="m").offset_interval == (1, "quarter")
    assert TimeComparison(type="yoy", metric="m").offset_interval == (1, "year")


def test_time_comparison_custom_offset():
    comparison = TimeComparison(type="prior_period", metric="m", offset=7, offset_unit="day")
    assert comparison.offset_interval == (7, "day")
    assert comparison.get_sql_offset() == "INTERVAL '7 day'"


def test_trailing_period_interval():
    trailing = TrailingPeriod(amount=3, unit="month")
    assert trailing.get_sql_interval() == "INTERVAL '3 month'"


def test_generate_time_comparison_sql_modes():
    comparison = TimeComparison(type="mom", metric="revenue", calculation="difference")
    sql = generate_time_comparison_sql(comparison, "SUM(amount)", "order_date")
    assert "LAG(" in sql
    assert "SUM(amount)" in sql

    comparison = TimeComparison(type="mom", metric="revenue", calculation="percent_change")
    sql = generate_time_comparison_sql(comparison, "SUM(amount)", "order_date")
    assert "/ NULLIF" in sql

    comparison = TimeComparison(type="mom", metric="revenue", calculation="ratio")
    sql = generate_time_comparison_sql(comparison, "SUM(amount)", "order_date")
    assert "/ NULLIF" in sql


def test_generate_time_comparison_sql_invalid():
    comparison = TimeComparison(type="mom", metric="revenue", calculation="difference")
    comparison.calculation = "bad"  # type: ignore[assignment]
    with pytest.raises(ValueError, match="Unknown calculation type"):
        generate_time_comparison_sql(comparison, "SUM(amount)", "order_date")
