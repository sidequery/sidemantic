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
        # A sql_distinct_key dedupes by the key entity, not the value: this uses a
        # symmetric aggregate keyed on order_id rather than SUM(DISTINCT value),
        # so two distinct orders with the same amount are both counted.
        assert "SUM(DISTINCT" in m.sql
        assert "HASH({model}.order_id)" in m.sql or "HASH(({model}.order_id))" in m.sql
        assert "{model}.order_amount" in m.sql
        # sql_distinct_key preserved in meta, resolved to row-level SQL.
        assert m.meta["distinct"] is True
        assert "{model}.order_id" in m.meta["sql_distinct_key"]

    def test_average_distinct(self, graph):
        m = graph.get_model("order_lines").get_metric("avg_order_amount")
        assert m.type == "derived"
        # average_distinct keyed on order_id: symmetric keyed sum / distinct keys.
        assert "SUM(DISTINCT" in m.sql
        assert "COUNT(DISTINCT" in m.sql
        assert "{model}.order_amount" in m.sql
        assert "{model}.order_id" in m.meta["sql_distinct_key"]

    def test_median_distinct(self, graph):
        m = graph.get_model("order_lines").get_metric("median_order_amount")
        assert m.type == "derived"
        # With a sql_distinct_key the ordered-set quantile must dedupe by key
        # before computing the median, so it collapses (key, value) pairs to one
        # value per key and takes the 0.5 quantile of that list. An ordered-set
        # MEDIAN over the fanned-out rows would weight repeated values per row.
        assert "LIST_AGGREGATE(" in m.sql
        assert "'quantile_cont', 0.5)" in m.sql
        assert "{model}.order_id" in m.sql  # dedupe key
        assert "{model}.order_amount" in m.sql

    def test_percentile_distinct(self, graph):
        m = graph.get_model("order_lines").get_metric("p90_order_amount")
        assert m.type == "derived"
        # percentile: 90 -> fraction 0.9. With a sql_distinct_key the value is
        # deduplicated by key (one value per distinct order) before the quantile,
        # so fan-out rows do not skew the result. `ORDER BY DISTINCT` (which
        # SQLGlot rejects) is never emitted.
        assert "LIST_AGGREGATE(" in m.sql
        assert "'quantile_cont', 0.9)" in m.sql
        assert "ORDER BY DISTINCT" not in m.sql
        assert "{model}.order_id" in m.sql  # dedupe key
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
        # The base measure ref is qualified ({model}) and aggregated with its own
        # aggregate (SUM) so the generator resolves it to the base measure's _raw
        # column instead of an out-of-scope bare `total_line_amount` column.
        assert m.sql == "SUM({model}.total_line_amount) / NULLIF(SUM(SUM({model}.total_line_amount)) OVER (), 0)"
        assert m.meta["table_calculation"] == "percent_of_total"

    def test_percent_of_previous(self, graph):
        m = graph.get_model("order_lines").get_metric("pct_of_previous_line_amount")
        assert m.type == "derived"
        assert "SUM({model}.total_line_amount)" in m.sql
        assert "LAG(SUM({model}.total_line_amount)) OVER ()" in m.sql
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
        # fiscal_quarter / fiscal_year are time dimensions truncated at the
        # matching grain; with the fixture's default offset (0) the SQL is the
        # bare timestamp (offset shifting is exercised separately).
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


# =============================================================================
# END-TO-END QUERYABILITY / CORRECTNESS (regression for P1 fixes)
# =============================================================================


class TestAdvancedMeasuresQueryable:
    """The distinct-key and post-SQL measures must compile to SQL that both runs
    and returns the correct fan-out-safe result."""

    def _layer(self, graph):
        from sidemantic import SemanticLayer

        layer = SemanticLayer()
        for model in graph.models.values():
            layer.add_model(model)
        return layer

    def _orders_con(self):
        import duckdb

        con = duckdb.connect()
        con.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        con.execute(
            "CREATE TABLE analytics.order_lines (id INT, order_id INT, order_amount DOUBLE, line_amount DOUBLE)"
        )
        # order 1 fans out to two lines (order_amount=10 on both); order 2 also has
        # order_amount=10. A correct keyed sum_distinct must return 20, not 10.
        con.execute("INSERT INTO analytics.order_lines VALUES (1,1,10,4),(2,1,10,6),(3,2,10,5)")
        return con

    def test_sum_distinct_counts_each_key(self, graph):
        con = self._orders_con()
        layer = self._layer(graph)
        sql = layer.compile(metrics=["order_lines.total_order_amount"])
        (total,) = con.execute(sql).fetchone()
        assert float(total) == 20.0

    def test_average_distinct_is_per_key(self, graph):
        con = self._orders_con()
        layer = self._layer(graph)
        sql = layer.compile(metrics=["order_lines.avg_order_amount"])
        (avg,) = con.execute(sql).fetchone()
        assert float(avg) == 10.0

    def test_percent_of_total_resolves_and_runs(self, graph):
        con = self._orders_con()
        layer = self._layer(graph)
        # line totals: order 1 = 4 + 6 = 10, order 2 = 5; total = 15.
        sql = layer.compile(
            metrics=["order_lines.pct_of_total_line_amount"],
            dimensions=["order_lines.order_id"],
        )
        rows = dict(con.execute(sql).fetchall())
        assert rows[1] == pytest.approx(10 / 15)
        assert rows[2] == pytest.approx(5 / 15)

    def _fanned_orders_con(self):
        # Five distinct orders with amounts 100..500; order 1 fans out to 3 lines.
        # Distinct-by-key values are [100,200,300,400,500]: median 300, p90 460.
        # Without keyed dedup, the repeated 100 skews both (median 200, p90 440).
        import duckdb

        con = duckdb.connect()
        con.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        con.execute(
            "CREATE TABLE analytics.order_lines (id INT, order_id INT, order_amount DOUBLE, line_amount DOUBLE)"
        )
        con.executemany(
            "INSERT INTO analytics.order_lines VALUES (?,?,?,?)",
            [
                (1, 1, 100, 1),
                (2, 1, 100, 1),
                (3, 1, 100, 1),
                (4, 2, 200, 1),
                (5, 3, 300, 1),
                (6, 4, 400, 1),
                (7, 5, 500, 1),
            ],
        )
        return con

    def test_percentile_distinct_dedupes_by_key(self, graph):
        # Regression: percentile_distinct previously emitted `ORDER BY DISTINCT`
        # inside PERCENTILE_CONT (unparseable), and then a plain ordered-set
        # PERCENTILE_CONT that ignored sql_distinct_key, so fan-out rows skewed the
        # result. It must now compile, run, and dedupe by key: p90 over the five
        # distinct order amounts is 460, not the fan-out-skewed 440.
        con = self._fanned_orders_con()
        layer = self._layer(graph)
        sql = layer.compile(metrics=["order_lines.p90_order_amount"])
        (val,) = con.execute(sql).fetchone()
        assert float(val) == pytest.approx(460.0)

    def test_median_distinct_dedupes_by_key(self, graph):
        # median_distinct with a sql_distinct_key must also dedupe by key before
        # taking the median: median of [100,200,300,400,500] is 300, not the
        # fan-out-skewed 200.
        con = self._fanned_orders_con()
        layer = self._layer(graph)
        sql = layer.compile(metrics=["order_lines.median_order_amount"])
        (val,) = con.execute(sql).fetchone()
        assert float(val) == pytest.approx(300.0)

    def test_keyed_sum_distinct_does_not_overflow(self, graph):
        # Regression: the keyed distinct offset (HASH(key) scaled into DECIMAL)
        # overflowed once a query accumulated ~100 distinct keys. A high-key-count
        # query must run and return the correct fan-out-safe keyed sum.
        import duckdb

        con = duckdb.connect()
        con.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        con.execute(
            "CREATE TABLE analytics.order_lines (id INT, order_id INT, order_amount DOUBLE, line_amount DOUBLE)"
        )
        rows = []
        expected = 0.0
        idx = 0
        for order_id in range(1, 501):  # 500 distinct keys, well past the old ~100 ceiling
            amount = float(order_id)
            expected += amount
            for _ in range(3):  # fan out each order to 3 lines
                idx += 1
                rows.append((idx, order_id, amount, 1.0))
        con.executemany("INSERT INTO analytics.order_lines VALUES (?,?,?,?)", rows)
        layer = self._layer(graph)
        sql = layer.compile(metrics=["order_lines.total_order_amount"])
        (total,) = con.execute(sql).fetchone()
        assert float(total) == pytest.approx(expected)

    def test_percent_of_total_over_count_distinct_base(self):
        # Regression: percent_of_total / percent_of_previous referencing a
        # count_distinct base measure had no entry in the aggregate lookup, so the
        # base stayed a raw id column instead of COUNT(DISTINCT ...), producing
        # invalid SQL / percentages over raw ids.
        import tempfile

        import duckdb

        lkml = """
view: visits {
  sql_table_name: analytics.visits ;;
  dimension: id { type: number primary_key: yes sql: ${TABLE}.id ;; }
  dimension: country { type: string sql: ${TABLE}.country ;; }
  dimension: user_id { type: number sql: ${TABLE}.user_id ;; }
  measure: unique_users { type: count_distinct sql: ${user_id} ;; }
  measure: pct_unique_users { type: percent_of_total sql: ${unique_users} ;; }
}
"""
        with tempfile.NamedTemporaryFile("w", suffix=".lkml", delete=False) as f:
            f.write(lkml)
            path = f.name
        graph = LookMLAdapter().parse(path)
        m = graph.get_model("visits").get_metric("pct_unique_users")
        # The base ref must be wrapped in COUNT(DISTINCT ...), not left as a raw id.
        assert "COUNT(DISTINCT {model}.unique_users)" in m.sql

        layer = self._layer(graph)
        con = duckdb.connect()
        con.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        con.execute("CREATE TABLE analytics.visits (id INT, country VARCHAR, user_id INT)")
        # US has 3 distinct users, CA has 1; shares are 3/4 and 1/4.
        con.execute("INSERT INTO analytics.visits VALUES (1,'US',1),(2,'US',2),(3,'US',3),(4,'CA',4),(5,'CA',4)")
        sql = layer.compile(metrics=["visits.pct_unique_users"], dimensions=["visits.country"])
        rows = dict(con.execute(sql).fetchall())
        assert rows["US"] == pytest.approx(3 / 4)
        assert rows["CA"] == pytest.approx(1 / 4)

    def test_fiscal_offset_buckets_by_fiscal_period(self):
        import tempfile

        import duckdb

        lkml = """
view: ev {
  sql_table_name: analytics.ev ;;
  dimension: id { type: number primary_key: yes sql: ${TABLE}.id ;; }
  dimension_group: occurred {
    type: time
    timeframes: [fiscal_year]
    fiscal_month_offset: 3
    sql: ${TABLE}.occurred_at ;;
  }
  measure: count { type: count }
}
"""
        with tempfile.NamedTemporaryFile("w", suffix=".lkml", delete=False) as f:
            f.write(lkml)
            path = f.name
        graph = LookMLAdapter().parse(path)
        # Offset shifts the timestamp so the calendar truncation lands on fiscal
        # boundaries rather than ignoring the offset.
        assert "INTERVAL (3) MONTH" in graph.get_model("ev").get_dimension("occurred_fiscal_year").sql

        layer = self._layer(graph)
        con = duckdb.connect()
        con.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        con.execute("CREATE TABLE analytics.ev (id INT, occurred_at DATE)")
        # April fiscal-year start: 2024-03-31 is the prior fiscal year; 2024-04-01
        # and 2024-06-15 are the next one.
        con.execute(
            "INSERT INTO analytics.ev VALUES (1, DATE '2024-03-31'),(2, DATE '2024-04-01'),(3, DATE '2024-06-15')"
        )
        sql = layer.compile(metrics=["ev.count"], dimensions=["ev.occurred_fiscal_year"])
        counts = sorted(c for _, c in con.execute(sql).fetchall())
        assert counts == [1, 2]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
