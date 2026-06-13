"""Tests for advanced LookML measure types and dimension_group timeframes.

Covers:
  - Distinct aggregate measures: sum_distinct, average_distinct,
    median_distinct, percentile_distinct (honoring sql_distinct_key).
  - Post-SQL / table-calculation measures: running_total, percent_of_total,
    percent_of_previous.
  - Non-standard / fiscal dimension_group timeframes: fiscal_quarter,
    fiscal_month_num, day_of_week, day_of_week_index, month_name, month_num,
    week, week_of_year, quarter_of_year, hour_of_day, etc.
"""

from pathlib import Path

import pytest

from sidemantic.adapters.lookml import LookMLAdapter

FIXTURES_DIR = Path("tests/fixtures/lookml")


@pytest.fixture
def graph():
    adapter = LookMLAdapter()
    return adapter.parse(FIXTURES_DIR / "advanced_measure_types.lkml")


# =============================================================================
# DISTINCT AGGREGATE MEASURES
# =============================================================================


class TestDistinctMeasures:
    def test_sum_distinct(self, graph):
        m = graph.get_model("order_lines").get_metric("total_order_amount")
        assert m is not None
        assert m.type == "derived"
        assert m.sql.startswith("SUM(DISTINCT ")
        assert "{model}.order_amount" in m.sql
        # sql_distinct_key preserved in meta, resolved to row-level SQL.
        assert m.meta["distinct"] is True
        assert "{model}.order_id" in m.meta["sql_distinct_key"]

    def test_average_distinct(self, graph):
        m = graph.get_model("order_lines").get_metric("avg_order_amount")
        assert m.type == "derived"
        assert m.sql.startswith("AVG(DISTINCT ")
        assert "{model}.order_amount" in m.sql
        assert "{model}.order_id" in m.meta["sql_distinct_key"]

    def test_median_distinct(self, graph):
        m = graph.get_model("order_lines").get_metric("median_order_amount")
        assert m.type == "derived"
        assert m.sql.startswith("MEDIAN(DISTINCT ")
        assert "{model}.order_amount" in m.sql

    def test_percentile_distinct(self, graph):
        m = graph.get_model("order_lines").get_metric("p90_order_amount")
        assert m.type == "derived"
        # percentile: 90 -> fraction 0.9
        assert "PERCENTILE_CONT(0.9)" in m.sql
        assert "WITHIN GROUP (ORDER BY DISTINCT" in m.sql
        assert "{model}.order_amount" in m.sql

    def test_distinct_without_sql_distinct_key(self, graph):
        m = graph.get_model("order_lines").get_metric("sum_distinct_line_amount")
        assert m.type == "derived"
        assert m.sql.startswith("SUM(DISTINCT ")
        assert "{model}.line_amount" in m.sql
        # No sql_distinct_key key present, but still flagged distinct.
        assert m.meta["distinct"] is True
        assert "sql_distinct_key" not in m.meta


# =============================================================================
# POST-SQL / TABLE-CALCULATION MEASURES
# =============================================================================


class TestPostSqlMeasures:
    def test_running_total(self, graph):
        m = graph.get_model("order_lines").get_metric("running_line_amount")
        assert m is not None
        # running_total maps to a cumulative metric over the base measure.
        assert m.type == "cumulative"
        assert m.sql == "total_line_amount"
        assert m.meta["table_calculation"] == "running_total"

    def test_percent_of_total(self, graph):
        m = graph.get_model("order_lines").get_metric("pct_of_total_line_amount")
        assert m.type == "derived"
        assert m.sql == "total_line_amount / NULLIF(SUM(total_line_amount) OVER (), 0)"
        assert m.meta["table_calculation"] == "percent_of_total"

    def test_percent_of_previous(self, graph):
        m = graph.get_model("order_lines").get_metric("pct_of_previous_line_amount")
        assert m.type == "derived"
        assert "LAG(total_line_amount) OVER ()" in m.sql
        assert m.meta["table_calculation"] == "percent_of_previous"


# =============================================================================
# DIMENSION_GROUP TIMEFRAMES (non-standard / fiscal)
# =============================================================================


class TestDimensionGroupTimeframes:
    def test_time_truncation_timeframes_are_time(self, graph):
        model = graph.get_model("events_calendar")
        for tf, gran in [
            ("occurred_time", "hour"),
            ("occurred_date", "day"),
            ("occurred_week", "week"),
            ("occurred_month", "month"),
            ("occurred_quarter", "quarter"),
            ("occurred_year", "year"),
        ]:
            dim = model.get_dimension(tf)
            assert dim is not None, f"missing {tf}"
            assert dim.type == "time"
            assert dim.granularity == gran

    def test_fiscal_truncation_timeframes(self, graph):
        model = graph.get_model("events_calendar")
        # fiscal_quarter / fiscal_year truncate to a calendar grain.
        assert model.get_dimension("occurred_fiscal_quarter").type == "time"
        assert model.get_dimension("occurred_fiscal_quarter").granularity == "quarter"
        assert model.get_dimension("occurred_fiscal_year").type == "time"
        assert model.get_dimension("occurred_fiscal_year").granularity == "year"

    def test_numeric_extracted_parts(self, graph):
        model = graph.get_model("events_calendar")
        checks = {
            "occurred_month_num": "MONTH",
            "occurred_week_of_year": "WEEK",
            "occurred_quarter_of_year": "QUARTER",
            "occurred_hour_of_day": "HOUR",
            "occurred_day_of_month": "DAY",
            "occurred_day_of_year": "DOY",
            "occurred_day_of_week_index": "ISODOW",
        }
        for name, fn in checks.items():
            dim = model.get_dimension(name)
            assert dim is not None, f"missing {name}"
            assert dim.type == "numeric", f"{name} should be numeric"
            assert fn in dim.sql, f"{name} sql should use {fn}: {dim.sql}"
            assert "{model}.occurred_at" in dim.sql

    def test_string_extracted_parts(self, graph):
        model = graph.get_model("events_calendar")
        dow = model.get_dimension("occurred_day_of_week")
        assert dow.type == "categorical"
        assert "STRFTIME" in dow.sql and "%A" in dow.sql

        mname = model.get_dimension("occurred_month_name")
        assert mname.type == "categorical"
        assert "STRFTIME" in mname.sql and "%B" in mname.sql

    def test_fiscal_extracted_parts(self, graph):
        model = graph.get_model("events_calendar")
        fmn = model.get_dimension("occurred_fiscal_month_num")
        assert fmn is not None
        assert fmn.type == "numeric"
        assert "MONTH" in fmn.sql

        fqoy = model.get_dimension("occurred_fiscal_quarter_of_year")
        assert fqoy is not None
        assert fqoy.type == "numeric"

    def test_raw_timeframe_skipped(self, graph):
        model = graph.get_model("events_calendar")
        assert model.get_dimension("occurred_raw") is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
