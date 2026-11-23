import pytest

from sqlctx import SemanticLayer
from sqlctx.adapters.yardstick import YardstickAdapter
from tests.queries.yardstick_replay_runtime import replay_runtime


@pytest.mark.parametrize("source_columns", ["*", "year, region, amount, s"])
def test_single_yardstick_source_join_does_not_multiply_or_hide_visible_rows(source_columns):
    layer = SemanticLayer(connection="duckdb:///:memory:")
    try:
        runtime = replay_runtime(layer, YardstickAdapter())
        # The physical column s also matches the source alias. A bare s in
        # DuckDB would bind to the column, not to the whole source row.
        runtime.execute(
            "CREATE TABLE sales(year INT, region VARCHAR, amount INT, s INT); "
            "INSERT INTO sales VALUES (2023,'North',10,1), (2023,'North',10,1), "
            "(2023,'South',20,1), (2024,'North',30,1), (2024,NULL,40,1), (2024,'East',5,1)"
        )
        runtime.execute(
            f"CREATE VIEW sales_v AS SELECT {source_columns}, COUNT(*) AS MEASURE tally, "
            "SUM(amount) AS MEASURE revenue FROM sales"
        )
        query = (
            "WITH thresholds AS (SELECT 'North' AS region, 8 AS cutoff UNION ALL "
            "SELECT 'North', 9) "
            "SELECT year, AGGREGATE(tally), AGGREGATE(revenue) FROM sales_v s "
            "JOIN (SELECT region, cutoff FROM thresholds) r "
            "ON r.region = s.region WHERE {predicate} GROUP BY year ORDER BY year"
        )
        assert runtime.execute(query.format(predicate="amount > 7")) == [(2023, 3, 40), (2024, 2, 70)]
        assert runtime.execute(query.format(predicate="s.amount > r.cutoff")) == [(2023, 2, 20), (2024, 1, 30)]
    finally:
        layer.adapter.close()


def test_joined_grouping_does_not_assign_a_right_hand_column_to_the_measure_source():
    layer = SemanticLayer(connection="duckdb:///:memory:")
    try:
        runtime = replay_runtime(layer, YardstickAdapter())
        runtime.execute(
            "CREATE TABLE raw_sales(region VARCHAR, amount INT); "
            "INSERT INTO raw_sales VALUES ('North',10), ('North',11), ('South',20); "
            "CREATE VIEW raw_sales_v AS SELECT *, COUNT(*) AS MEASURE tally FROM raw_sales"
        )
        assert runtime.execute(
            "SELECT report_year, AGGREGATE(tally) FROM raw_sales_v s "
            "JOIN (SELECT 2026 AS report_year, 'North' AS region) r ON r.region = s.region "
            "WHERE s.amount > 0 GROUP BY report_year"
        ) == [(2026, 3)]
    finally:
        layer.adapter.close()


@pytest.mark.parametrize(
    ("year", "join", "where", "expected"),
    [
        ("y", "JOIN (VALUES (2024,'N')) allowed(y,r) USING(y,r)", "y=2024", 30),
        ("s.y", "JOIN (VALUES (2024,'N')) allowed(y,r) ON s.y=allowed.y AND s.r=allowed.r", "s.y=2024", 30),
        ("y", "JOIN (VALUES ('N',1),('N',1),('S',0)) allowed(r,keep) USING(r)", "y=2024 AND keep=1", 10),
    ],
)
def test_joined_visible_set_replaces_year_without_filtering_against_join(year, join, where, expected):
    layer = SemanticLayer(connection="duckdb:///:memory:")
    try:
        runtime = replay_runtime(layer, YardstickAdapter())
        runtime.execute(
            "CREATE TABLE sales(y INT, r VARCHAR, amount INT); "
            "INSERT INTO sales VALUES (2023,'N',10), (2023,'S',20), (2024,'N',40), (2024,'S',80); "
            "CREATE VIEW sales_v AS SELECT y, r, SUM(amount) AS MEASURE revenue FROM sales"
        )
        assert runtime.execute(
            f"SELECT {year}, AGGREGATE(revenue) AT (VISIBLE SET y=y-1) FROM sales_v s {join} WHERE {where}"
        ) == [(2024, expected)]
    finally:
        layer.adapter.close()
