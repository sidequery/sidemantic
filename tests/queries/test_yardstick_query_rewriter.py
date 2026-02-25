"""Yardstick SQL query rewrite tests (`SEMANTIC`, `AGGREGATE`, `AT`)."""

from __future__ import annotations

import pytest

from sidemantic import SemanticLayer
from sidemantic.loaders import load_from_directory
from sidemantic.sql.query_rewriter import QueryRewriter
from tests.utils import fetch_dicts


@pytest.fixture
def yardstick_layer(tmp_path):
    sql_file = tmp_path / "sales.sql"
    sql_file.write_text(
        """
CREATE VIEW sales_v AS
SELECT
    year,
    region,
    SUM(amount) AS MEASURE revenue
FROM sales;

CREATE VIEW sales_yearly AS
SELECT
    year,
    SUM(amount) AS MEASURE revenue
FROM sales;

CREATE VIEW financials_v AS
SELECT
    year,
    SUM(revenue) AS MEASURE revenue,
    SUM(cost) AS MEASURE cost,
    revenue - cost AS MEASURE profit
FROM financials;

CREATE VIEW daily_orders_v AS
SELECT
    order_date,
    SUM(amount) AS MEASURE revenue
FROM daily_orders;

CREATE VIEW fact_orders_v AS
SELECT
    year,
    region,
    SUM(amount) AS MEASURE revenue
FROM fact_orders;

CREATE VIEW fact_returns_v AS
SELECT
    year,
    region,
    SUM(return_amount) AS MEASURE refunds
FROM fact_returns;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE sales (year INT, region TEXT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO sales VALUES
    (2022, 'US', 100), (2022, 'EU', 50),
    (2023, 'US', 150), (2023, 'EU', 75);
"""
    )
    layer.adapter.execute("CREATE TABLE financials (year INT, revenue DOUBLE, cost DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO financials VALUES
    (2022, 100, 60), (2022, 150, 80),
    (2023, 200, 100), (2023, 250, 120);
"""
    )
    layer.adapter.execute("CREATE TABLE daily_orders (order_date DATE, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO daily_orders VALUES
    ('2023-01-15', 100), ('2023-01-20', 150),
    ('2023-02-10', 200), ('2023-02-25', 120),
    ('2023-03-05', 180), ('2023-03-15', 90);
"""
    )
    layer.adapter.execute("CREATE TABLE fact_orders (year INT, region TEXT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO fact_orders VALUES
    (2022, 'US', 100), (2022, 'EU', 50),
    (2023, 'US', 150), (2023, 'EU', 75);
"""
    )
    layer.adapter.execute("CREATE TABLE fact_returns (year INT, region TEXT, return_amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO fact_returns VALUES
    (2022, 'US', 10), (2022, 'EU', 5),
    (2023, 'US', 20), (2023, 'EU', 8);
"""
    )
    load_from_directory(layer, tmp_path)
    return layer


@pytest.fixture
def yardstick_paper_layer(tmp_path):
    sql_file = tmp_path / "paper.sql"
    sql_file.write_text(
        """
CREATE VIEW paper_orders_v AS
SELECT *, SUM(revenue) AS MEASURE sumRevenue
FROM paper_orders;

CREATE VIEW enhanced_customers_paper AS
SELECT *, AVG(custAge) AS MEASURE avgAge
FROM paper_customers;

CREATE VIEW paper_orders_l12_v AS
SELECT prodName, orderDate, revenue, AVG(revenue) AS MEASURE avgRevenue
FROM paper_orders_l12;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE paper_orders (prodName TEXT, custName TEXT, order_date DATE, revenue INT)")
    layer.adapter.execute(
        """
INSERT INTO paper_orders VALUES
    ('Happy', 'Var Bob', '2024-01-01', 4),
    ('Happy', 'Alice', '2024-01-02', 6),
    ('Happy', 'Alice', '2024-01-03', 7),
    ('Whizz', 'Alice', '2024-01-04', 3);
"""
    )

    layer.adapter.execute("CREATE TABLE paper_customers (custName TEXT, custAge INT)")
    layer.adapter.execute(
        """
INSERT INTO paper_customers VALUES
    ('Alice', 30), ('Var Bob', 16), ('Carol', 40);
"""
    )

    layer.adapter.execute("CREATE TABLE paper_order_customers (prodName TEXT, custName TEXT)")
    layer.adapter.execute(
        """
INSERT INTO paper_order_customers VALUES
    ('Happy', 'Alice'),
    ('Happy', 'Var Bob'),
    ('Whizz', 'Carol');
"""
    )

    layer.adapter.execute("CREATE TABLE paper_orders_l12 (prodName TEXT, orderDate DATE, revenue INT)")
    layer.adapter.execute(
        """
INSERT INTO paper_orders_l12 VALUES
    ('Happy', '2024-01-01', 4),
    ('Happy', '2024-01-02', 6),
    ('Happy', '2024-01-03', 7),
    ('Whizz', '2024-01-04', 3);
"""
    )

    load_from_directory(layer, tmp_path)
    return layer


def test_yardstick_semantic_aggregate_identity(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AS revenue
FROM sales_v
"""
    )
    rows = fetch_dicts(result)
    values = {(row["year"], row["region"]): float(row["revenue"]) for row in rows}
    assert values == {
        (2022, "EU"): 50.0,
        (2022, "US"): 100.0,
        (2023, "EU"): 75.0,
        (2023, "US"): 150.0,
    }


def test_yardstick_schema_qualified_aggregate_function(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    schema.AGGREGATE(revenue) AS revenue
FROM sales_v
GROUP BY year
"""
    )
    rows = fetch_dicts(result)
    assert [(row["year"], float(row["revenue"])) for row in rows] == [(2022, 150.0), (2023, 225.0)]


def test_yardstick_at_all_dimension(yardstick_layer):
    result = yardstick_layer.sql(
        """
SELECT
    year,
    region,
    AGGREGATE(revenue) AT (ALL region) AS year_total
FROM sales_v
"""
    )
    rows = fetch_dicts(result)
    values = {(row["year"], row["region"]): float(row["year_total"]) for row in rows}
    assert values == {
        (2022, "EU"): 150.0,
        (2022, "US"): 150.0,
        (2023, "EU"): 225.0,
        (2023, "US"): 225.0,
    }


def test_yardstick_at_all(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AT (ALL) AS grand_total
FROM sales_v
"""
    )
    rows = fetch_dicts(result)
    assert {float(row["grand_total"]) for row in rows} == {375.0}


def test_yardstick_at_where_and_qualified_reference(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    s.year,
    s.region,
    AGGREGATE(revenue) AT (WHERE sales_v.region = 'US') AS us_revenue
FROM sales_v AS s
"""
    )
    rows = fetch_dicts(result)
    assert {float(row["us_revenue"]) for row in rows} == {250.0}


def test_yardstick_at_where_without_semantic_prefix(yardstick_layer):
    result = yardstick_layer.sql(
        """
SELECT
    year,
    region,
    AGGREGATE(revenue) AT (WHERE region = 'US') AS us_revenue
FROM sales_v
"""
    )
    rows = fetch_dicts(result)
    assert {float(row["us_revenue"]) for row in rows} == {250.0}


def test_yardstick_at_set_and_current_keyword(yardstick_layer):
    prior_result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    AGGREGATE(revenue) AT (SET year = year - 1) AS prior_year
FROM sales_yearly
"""
    )
    prior_rows = fetch_dicts(prior_result)
    prior_values = {
        row["year"]: (None if row["prior_year"] is None else float(row["prior_year"])) for row in prior_rows
    }
    assert prior_values == {2022: None, 2023: 150.0}

    current_result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    AGGREGATE(revenue) AT (SET year = CURRENT year - 1) AS prior_year
FROM sales_yearly
"""
    )
    current_rows = fetch_dicts(current_result)
    current_values = {
        row["year"]: (None if row["prior_year"] is None else float(row["prior_year"])) for row in current_rows
    }
    assert current_values == {2022: None, 2023: 150.0}


def test_yardstick_current_keyword_ambiguous_context_returns_null(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    region,
    AGGREGATE(revenue) AT (SET year = CURRENT year - 1) AS prior_from_current
FROM sales_v
GROUP BY region
"""
    )
    rows = fetch_dicts(result)
    assert {row["region"]: row["prior_from_current"] for row in rows} == {"EU": None, "US": None}


def test_yardstick_current_keyword_single_valued_where_context(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    AGGREGATE(revenue) AT (SET year = CURRENT year - 1) AS prior_from_current
FROM sales_yearly
WHERE year = 2023
"""
    )
    rows = fetch_dicts(result)
    assert [float(row["prior_from_current"]) for row in rows] == [150.0]


def test_yardstick_at_visible(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AT (VISIBLE) AS visible_revenue
FROM sales_v
WHERE region = 'US'
"""
    )
    rows = fetch_dicts(result)
    values = {(row["year"], row["region"]): float(row["visible_revenue"]) for row in rows}
    assert values == {(2022, "US"): 100.0, (2023, "US"): 150.0}


def test_yardstick_measure_at_visible_without_aggregate(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    revenue AT (VISIBLE) AS visible_revenue
FROM sales_v
WHERE region = 'US'
GROUP BY year
"""
    )
    rows = fetch_dicts(result)
    values = {row["year"]: float(row["visible_revenue"]) for row in rows}
    assert values == {2022: 100.0, 2023: 150.0}


def test_yardstick_measure_at_where_in_predicate(tmp_path):
    sql_file = tmp_path / "orders.sql"
    sql_file.write_text(
        """
CREATE VIEW orders_v AS
SELECT
    prod_name,
    order_date,
    revenue,
    AVG(revenue) AS MEASURE avg_revenue
FROM orders;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE orders (prod_name TEXT, order_date DATE, revenue DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO orders VALUES
    ('A', '2024-01-01', 10), ('A', '2024-01-02', 20),
    ('B', '2024-01-01', 30), ('B', '2024-01-02', 40);
"""
    )
    load_from_directory(layer, tmp_path)

    result = layer.sql(
        """
SEMANTIC SELECT
    o.prod_name,
    o.order_date,
    o.revenue
FROM orders_v AS o
WHERE o.revenue > o.avg_revenue AT (WHERE prod_name = o.prod_name)
"""
    )
    rows = fetch_dicts(result)
    values = {(row["prod_name"], row["order_date"].isoformat(), float(row["revenue"])) for row in rows}
    assert values == {("A", "2024-01-02", 20.0), ("B", "2024-01-02", 40.0)}


def test_yardstick_plain_measure_reference_with_where_context(yardstick_layer):
    rows = fetch_dicts(
        yardstick_layer.sql(
            """
SELECT
    year,
    revenue AS plain_revenue
FROM sales_v
WHERE region = 'US'
GROUP BY year
ORDER BY year
"""
        )
    )
    assert [(row["year"], float(row["plain_revenue"])) for row in rows] == [
        (2022, 150.0),
        (2023, 225.0),
    ]


def test_yardstick_curly_measure_reference_without_semantic_prefix(yardstick_layer):
    rows = fetch_dicts(
        yardstick_layer.sql(
            """
SELECT
    year,
    {revenue} AS revenue
FROM sales_v
WHERE region = 'US'
GROUP BY year
ORDER BY year
"""
        )
    )
    assert [(row["year"], float(row["revenue"])) for row in rows] == [
        (2022, 150.0),
        (2023, 225.0),
    ]


def test_yardstick_mixed_non_semantic_at_routing(yardstick_layer):
    result = yardstick_layer.sql(
        """
SELECT
    year,
    AGGREGATE(revenue) AS agg_revenue,
    revenue AT (VISIBLE) AS visible_revenue,
    revenue AS plain_revenue
FROM sales_v
WHERE region = 'US'
GROUP BY year
ORDER BY year
"""
    )
    rows = fetch_dicts(result)
    assert [
        (row["year"], float(row["agg_revenue"]), float(row["visible_revenue"]), float(row["plain_revenue"]))
        for row in rows
    ] == [
        (2022, 100.0, 100.0, 150.0),
        (2023, 150.0, 150.0, 225.0),
    ]


def test_yardstick_listing8_rollup_parity(yardstick_paper_layer):
    rows = fetch_dicts(
        yardstick_paper_layer.sql(
            """
SELECT
    o.prodName,
    COUNT(*) AS c,
    AGGREGATE(o.sumRevenue) AS rAgg,
    o.sumRevenue AT (VISIBLE) AS rViz,
    o.sumRevenue AS r
FROM paper_orders_v o
WHERE o.custName <> 'Var Bob'
GROUP BY ROLLUP(o.prodName)
ORDER BY o.prodName
"""
        )
    )
    assert [
        (
            row["prodName"],
            int(row["c"]),
            None if row["rAgg"] is None else float(row["rAgg"]),
            None if row["rViz"] is None else float(row["rViz"]),
            None if row["r"] is None else float(row["r"]),
        )
        for row in rows
    ] == [
        ("Happy", 2, 13.0, 13.0, 17.0),
        ("Whizz", 1, 3.0, 3.0, 3.0),
        (None, 3, None, None, None),
    ]


def test_yardstick_listing9_join_parity(yardstick_paper_layer):
    rows = fetch_dicts(
        yardstick_paper_layer.sql(
            """
SELECT
    o.prodName,
    COUNT(*) AS orderCount,
    AVG(c.custAge) AS weightedAvgAge,
    c.avgAge AS avgAge,
    c.avgAge AT (VISIBLE) AS visibleAvgAge
FROM paper_order_customers o
JOIN enhanced_customers_paper c USING (custName)
WHERE c.custAge >= 18
GROUP BY o.prodName
ORDER BY o.prodName
"""
        )
    )
    assert [
        (
            row["prodName"],
            int(row["orderCount"]),
            float(row["weightedAvgAge"]),
            float(row["avgAge"]),
            float(row["visibleAvgAge"]),
        )
        for row in rows
    ] == [
        ("Happy", 1, 30.0, 28.666666666666668, 35.0),
        ("Whizz", 1, 40.0, 28.666666666666668, 35.0),
    ]


def test_yardstick_listing12_wrapperless_measure_at_where(yardstick_paper_layer):
    rows = fetch_dicts(
        yardstick_paper_layer.sql(
            """
SELECT
    o.prodName,
    o.orderDate
FROM paper_orders_l12_v o
WHERE o.revenue > o.avgRevenue AT (WHERE prodName = o.prodName)
ORDER BY o.prodName, o.orderDate
"""
        )
    )
    assert [(row["prodName"], row["orderDate"].isoformat()) for row in rows] == [
        ("Happy", "2024-01-02"),
        ("Happy", "2024-01-03"),
    ]


def test_yardstick_chained_at(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    AGGREGATE(revenue) AT (ALL year) AT (ALL region) AS grand_total
FROM sales_v
"""
    )
    rows = fetch_dicts(result)
    values = {row["year"]: float(row["grand_total"]) for row in rows}
    assert values == {2022: 375.0, 2023: 375.0}


def test_yardstick_single_clause_all_multiple_dimensions_matches_chained(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AT (ALL year region) AS single_all,
    AGGREGATE(revenue) AT (ALL year) AT (ALL region) AS chained_all
FROM sales_v
"""
    )
    rows = fetch_dicts(result)
    for row in rows:
        assert float(row["single_all"]) == pytest.approx(375.0)
        assert float(row["single_all"]) == pytest.approx(float(row["chained_all"]))


def test_yardstick_multiple_aggregate_calls_in_expression(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) / AGGREGATE(revenue) AT (ALL region) AS share_of_year
FROM sales_v
"""
    )
    rows = fetch_dicts(result)
    values = {(row["year"], row["region"]): float(row["share_of_year"]) for row in rows}
    assert values == {
        (2022, "EU"): pytest.approx(1.0 / 3.0),
        (2022, "US"): pytest.approx(2.0 / 3.0),
        (2023, "EU"): pytest.approx(1.0 / 3.0),
        (2023, "US"): pytest.approx(2.0 / 3.0),
    }


def test_yardstick_at_all_year(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AT (ALL year) AS region_total
FROM sales_v
"""
    )
    rows = fetch_dicts(result)
    values = {(row["year"], row["region"]): float(row["region_total"]) for row in rows}
    assert values == {
        (2022, "EU"): 125.0,
        (2022, "US"): 250.0,
        (2023, "EU"): 125.0,
        (2023, "US"): 250.0,
    }


def test_yardstick_set_constant_correlates_on_remaining_dimensions(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AT (SET year = 2022) AS fixed_2022
FROM sales_v
"""
    )
    rows = fetch_dicts(result)
    values = {(row["year"], row["region"]): float(row["fixed_2022"]) for row in rows}
    assert values == {
        (2022, "EU"): 50.0,
        (2022, "US"): 100.0,
        (2023, "EU"): 50.0,
        (2023, "US"): 100.0,
    }


def test_yardstick_set_reaches_beyond_outer_where(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    AGGREGATE(revenue) AT (SET year = year - 1) AS prior_year
FROM sales_yearly
WHERE year = 2023
"""
    )
    rows = fetch_dicts(result)
    assert rows == [{"year": 2023, "prior_year": 150.0}]


def test_yardstick_set_in_predicate_form(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    AGGREGATE(revenue) AS total_revenue,
    AGGREGATE(revenue) AT (SET region IN ('US')) AS us_only
FROM sales_v
"""
    )
    rows = fetch_dicts(result)
    values = {row["year"]: (float(row["total_revenue"]), float(row["us_only"])) for row in rows}
    assert values == {
        2022: (150.0, 100.0),
        2023: (225.0, 150.0),
    }


def test_yardstick_set_then_all_overrides_set(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    AGGREGATE(revenue) AT (SET year = year - 1) AT (ALL year) AS prior_grand
FROM sales_yearly
"""
    )
    rows = fetch_dicts(result)
    values = {row["year"]: float(row["prior_grand"]) for row in rows}
    assert values == {2022: 375.0, 2023: 375.0}


def test_yardstick_where_then_all_keeps_remaining_correlation(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AT (WHERE region = 'US') AT (ALL region) AS us_year_total
FROM sales_v
"""
    )
    rows = fetch_dicts(result)
    values = {(row["year"], row["region"]): float(row["us_year_total"]) for row in rows}
    assert values == {
        (2022, "EU"): 100.0,
        (2022, "US"): 100.0,
        (2023, "EU"): 150.0,
        (2023, "US"): 150.0,
    }


def test_yardstick_compound_at_modifiers_set_visible(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    AGGREGATE(revenue) AT (SET year = year - 1 VISIBLE) AS prior_year
FROM sales_yearly
WHERE year = 2023
"""
    )
    rows = fetch_dicts(result)
    assert rows == [{"year": 2023, "prior_year": 150.0}]


def test_yardstick_at_visible_without_where_is_identity(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AT (VISIBLE) AS same_as_base
FROM sales_v
"""
    )
    rows = fetch_dicts(result)
    values = {(row["year"], row["region"]): float(row["same_as_base"]) for row in rows}
    assert values == {
        (2022, "EU"): 50.0,
        (2022, "US"): 100.0,
        (2023, "EU"): 75.0,
        (2023, "US"): 150.0,
    }


def test_yardstick_aggregate_without_at_requires_semantic(yardstick_layer):
    with pytest.raises(ValueError, match="requires the SEMANTIC prefix"):
        yardstick_layer.sql("SELECT AGGREGATE(revenue) AS revenue FROM sales_v")


def test_yardstick_scalar_aggregate_without_group_by(yardstick_layer):
    result = yardstick_layer.sql("SEMANTIC SELECT AGGREGATE(revenue) AS total FROM sales_v")
    rows = fetch_dicts(result)
    assert rows == [{"total": 375.0}]


def test_yardstick_doubled_aggregate_expression(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    2 * AGGREGATE(revenue) AS doubled
FROM sales_yearly
"""
    )
    rows = fetch_dicts(result)
    values = {row["year"]: float(row["doubled"]) for row in rows}
    assert values == {2022: 300.0, 2023: 450.0}


def test_yardstick_literal_constant_not_grouped(yardstick_layer):
    result = yardstick_layer.sql("SEMANTIC SELECT 1000 AS marker, AGGREGATE(revenue) AS total FROM sales_v")
    rows = fetch_dicts(result)
    assert rows == [{"marker": 1000, "total": 375.0}]


def test_yardstick_ad_hoc_dimension_all_expression(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    MONTH(order_date) AS month_num,
    AGGREGATE(revenue) AS month_revenue,
    AGGREGATE(revenue) AT (ALL MONTH(order_date)) AS total
FROM daily_orders_v
"""
    )
    rows = fetch_dicts(result)
    values = {int(row["month_num"]): (float(row["month_revenue"]), float(row["total"])) for row in rows}
    assert values == {1: (250.0, 840.0), 2: (320.0, 840.0), 3: (270.0, 840.0)}


def test_yardstick_ad_hoc_dimension_set_expression(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    MONTH(order_date) AS month_num,
    AGGREGATE(revenue) AS month_revenue,
    AGGREGATE(revenue) AT (SET MONTH(order_date) = 2) AS feb_revenue
FROM daily_orders_v
"""
    )
    rows = fetch_dicts(result)
    values = {int(row["month_num"]): (float(row["month_revenue"]), float(row["feb_revenue"])) for row in rows}
    assert values == {1: (250.0, 320.0), 2: (320.0, 320.0), 3: (270.0, 320.0)}


def test_yardstick_derived_measure_profit(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    AGGREGATE(profit) AS profit
FROM financials_v
"""
    )
    rows = fetch_dicts(result)
    values = {row["year"]: float(row["profit"]) for row in rows}
    assert values == {2022: 110.0, 2023: 230.0}


def test_yardstick_multi_fact_join(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    o.year,
    o.region,
    AGGREGATE(revenue) AS revenue,
    AGGREGATE(refunds) AS refunds
FROM fact_orders_v o
JOIN fact_returns_v r ON o.year = r.year AND o.region = r.region
"""
    )
    rows = fetch_dicts(result)
    values = {(row["year"], row["region"]): (float(row["revenue"]), float(row["refunds"])) for row in rows}
    assert values == {
        (2022, "EU"): (50.0, 5.0),
        (2022, "US"): (100.0, 10.0),
        (2023, "EU"): (75.0, 8.0),
        (2023, "US"): (150.0, 20.0),
    }


def test_yardstick_multi_fact_join_with_at_all_dimension(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    o.year,
    o.region,
    AGGREGATE(revenue) AT (ALL region) AS year_total
FROM fact_orders_v o
JOIN fact_returns_v r ON o.year = r.year AND o.region = r.region
"""
    )
    rows = fetch_dicts(result)
    values = {(row["year"], row["region"]): float(row["year_total"]) for row in rows}
    assert values == {
        (2022, "EU"): 150.0,
        (2022, "US"): 150.0,
        (2023, "EU"): 225.0,
        (2023, "US"): 225.0,
    }


def test_yardstick_percent_of_total(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AS revenue,
    100.0 * AGGREGATE(revenue) / AGGREGATE(revenue) AT (ALL) AS pct_of_total
FROM sales_v
"""
    )
    rows = fetch_dicts(result)
    values = {(row["year"], row["region"]): (float(row["revenue"]), float(row["pct_of_total"])) for row in rows}
    assert values == {
        (2022, "EU"): (50.0, pytest.approx(13.333333333333334)),
        (2022, "US"): (100.0, pytest.approx(26.666666666666668)),
        (2023, "EU"): (75.0, pytest.approx(20.0)),
        (2023, "US"): (150.0, pytest.approx(40.0)),
    }


def test_yardstick_yoy_change_arithmetic(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    AGGREGATE(revenue) AS revenue,
    AGGREGATE(revenue) - AGGREGATE(revenue) AT (SET year = year - 1) AS yoy_change
FROM sales_yearly
"""
    )
    rows = fetch_dicts(result)
    values = {
        row["year"]: (float(row["revenue"]), None if row["yoy_change"] is None else float(row["yoy_change"]))
        for row in rows
    }
    assert values == {
        2022: (150.0, None),
        2023: (225.0, 75.0),
    }


def test_yardstick_combining_base_with_all_variants(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AS base,
    AGGREGATE(revenue) AT (ALL region) AS year_total,
    AGGREGATE(revenue) AT (ALL year) AS region_total,
    AGGREGATE(revenue) AT (ALL) AS grand_total
FROM sales_v
"""
    )
    rows = fetch_dicts(result)
    values = {
        (row["year"], row["region"]): (
            float(row["base"]),
            float(row["year_total"]),
            float(row["region_total"]),
            float(row["grand_total"]),
        )
        for row in rows
    }
    assert values == {
        (2022, "EU"): (50.0, 150.0, 125.0, 375.0),
        (2022, "US"): (100.0, 150.0, 250.0, 375.0),
        (2023, "EU"): (75.0, 225.0, 125.0, 375.0),
        (2023, "US"): (150.0, 225.0, 250.0, 375.0),
    }


def test_yardstick_set_with_future_year(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    AGGREGATE(revenue) AT (SET year = year + 1) AS next_year
FROM sales_yearly
"""
    )
    rows = fetch_dicts(result)
    values = {row["year"]: (None if row["next_year"] is None else float(row["next_year"])) for row in rows}
    assert values == {2022: 225.0, 2023: None}


def test_yardstick_table_alias_modifiers(yardstick_layer):
    set_result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    s.year,
    AGGREGATE(revenue) AT (SET year = year - 1) AS prior_year
FROM sales_yearly s
"""
    )
    set_rows = fetch_dicts(set_result)
    set_values = {row["year"]: (None if row["prior_year"] is None else float(row["prior_year"])) for row in set_rows}
    assert set_values == {2022: None, 2023: 150.0}

    all_result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    s.year,
    AGGREGATE(revenue) AT (ALL year) AS grand_total
FROM sales_yearly AS s
"""
    )
    all_rows = fetch_dicts(all_result)
    all_values = {row["year"]: float(row["grand_total"]) for row in all_rows}
    assert all_values == {2022: 375.0, 2023: 375.0}

    visible_result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    s.year,
    s.region,
    AGGREGATE(revenue) AT (VISIBLE) AS visible_rev
FROM sales_v AS s
WHERE s.region = 'US'
"""
    )
    visible_rows = fetch_dicts(visible_result)
    visible_values = {(row["year"], row["region"]): float(row["visible_rev"]) for row in visible_rows}
    assert visible_values == {(2022, "US"): 100.0, (2023, "US"): 150.0}


def test_yardstick_multiple_measures_same_view(tmp_path):
    sql_file = tmp_path / "orders.sql"
    sql_file.write_text(
        """
CREATE VIEW orders_v AS
SELECT
    year,
    SUM(amount) AS MEASURE total_revenue,
    COUNT(*) AS MEASURE order_count,
    AVG(amount) AS MEASURE avg_order
FROM sales;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE sales (year INT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO sales VALUES
    (2022, 100), (2022, 50),
    (2023, 150), (2023, 75);
"""
    )
    load_from_directory(layer, tmp_path)

    result = layer.sql(
        """
SEMANTIC SELECT
    year,
    AGGREGATE(total_revenue) AS total_revenue,
    AGGREGATE(avg_order) AS avg_order
FROM orders_v
"""
    )
    rows = fetch_dicts(result)
    values = {row["year"]: (float(row["total_revenue"]), float(row["avg_order"])) for row in rows}
    assert values == {2022: (150.0, 75.0), 2023: (225.0, 112.5)}


def test_yardstick_count_measure_behavior(tmp_path):
    sql_file = tmp_path / "orders.sql"
    sql_file.write_text(
        """
CREATE VIEW orders_v AS
SELECT
    year,
    COUNT(*) AS MEASURE order_count
FROM sales;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE sales (year INT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO sales VALUES
    (2022, 100), (2022, 50),
    (2023, 150), (2023, 75);
"""
    )
    load_from_directory(layer, tmp_path)

    by_year = fetch_dicts(layer.sql("SEMANTIC SELECT year, AGGREGATE(order_count) AS order_count FROM orders_v"))
    by_year_values = {row["year"]: int(row["order_count"]) for row in by_year}
    assert by_year_values == {2022: 2, 2023: 2}

    all_years = fetch_dicts(
        layer.sql("SEMANTIC SELECT year, AGGREGATE(order_count) AT (ALL) AS order_count FROM orders_v")
    )
    all_values = {row["year"]: int(row["order_count"]) for row in all_years}
    assert all_values == {2022: 4, 2023: 4}


def test_yardstick_semantic_with_cte(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC WITH a AS (
    SELECT year, region, AGGREGATE(revenue) AS revenue
    FROM sales_v
)
SELECT * FROM a
"""
    )
    rows = fetch_dicts(result)
    values = {(row["year"], row["region"]): float(row["revenue"]) for row in rows}
    assert values == {
        (2022, "EU"): 50.0,
        (2022, "US"): 100.0,
        (2023, "EU"): 75.0,
        (2023, "US"): 150.0,
    }


def test_yardstick_index_to_base_period(yardstick_layer):
    result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    AGGREGATE(revenue) AS revenue,
    AGGREGATE(revenue) / AGGREGATE(revenue) AT (SET year = 2022) AS index_to_2022
FROM sales_yearly
"""
    )
    rows = fetch_dicts(result)
    values = {row["year"]: (float(row["revenue"]), float(row["index_to_2022"])) for row in rows}
    assert values == {
        2022: (150.0, 1.0),
        2023: (225.0, 1.5),
    }


def test_yardstick_where_complex_conditions(yardstick_layer):
    and_result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AT (WHERE year = 2023 AND region = 'US') AS us_2023
FROM sales_v
"""
    )
    and_rows = fetch_dicts(and_result)
    assert {float(row["us_2023"]) for row in and_rows} == {150.0}

    or_result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AT (WHERE region = 'US' OR year = 2022) AS filtered
FROM sales_v
"""
    )
    or_rows = fetch_dicts(or_result)
    assert {float(row["filtered"]) for row in or_rows} == {300.0}

    numeric_result = yardstick_layer.sql(
        """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AT (WHERE year > 2022) AS recent_sales
FROM sales_v
"""
    )
    numeric_rows = fetch_dicts(numeric_result)
    assert {float(row["recent_sales"]) for row in numeric_rows} == {225.0}


def test_yardstick_three_dimensional_all_semantics(tmp_path):
    sql_file = tmp_path / "products.sql"
    sql_file.write_text(
        """
CREATE VIEW products_v AS
SELECT
    year,
    region,
    category,
    SUM(amount) AS MEASURE revenue
FROM products;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE products (year INT, region TEXT, category TEXT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO products VALUES
    (2022, 'US', 'A', 100), (2022, 'US', 'B', 50),
    (2022, 'EU', 'A', 80), (2022, 'EU', 'B', 40),
    (2023, 'US', 'A', 120), (2023, 'US', 'B', 60),
    (2023, 'EU', 'A', 100), (2023, 'EU', 'B', 50);
"""
    )
    load_from_directory(layer, tmp_path)

    grand_total = fetch_dicts(
        layer.sql(
            """
SEMANTIC SELECT
    year,
    region,
    category,
    AGGREGATE(revenue) AT (ALL) AS grand_total
FROM products_v
"""
        )
    )
    assert {float(row["grand_total"]) for row in grand_total} == {600.0}

    by_year_total = fetch_dicts(
        layer.sql(
            """
SEMANTIC SELECT
    year,
    region,
    category,
    AGGREGATE(revenue) AT (ALL region) AT (ALL category) AS year_total
FROM products_v
"""
        )
    )
    by_year_values = {(row["year"], row["region"], row["category"]): float(row["year_total"]) for row in by_year_total}
    assert by_year_values == {
        (2022, "EU", "A"): 270.0,
        (2022, "EU", "B"): 270.0,
        (2022, "US", "A"): 270.0,
        (2022, "US", "B"): 270.0,
        (2023, "EU", "A"): 330.0,
        (2023, "EU", "B"): 330.0,
        (2023, "US", "A"): 330.0,
        (2023, "US", "B"): 330.0,
    }

    by_category_total = fetch_dicts(
        layer.sql(
            """
SEMANTIC SELECT
    year,
    region,
    category,
    AGGREGATE(revenue) AT (ALL year) AT (ALL region) AS category_total
FROM products_v
"""
        )
    )
    by_category_values = {
        (row["year"], row["region"], row["category"]): float(row["category_total"]) for row in by_category_total
    }
    assert by_category_values == {
        (2022, "EU", "A"): 400.0,
        (2022, "EU", "B"): 200.0,
        (2022, "US", "A"): 400.0,
        (2022, "US", "B"): 200.0,
        (2023, "EU", "A"): 400.0,
        (2023, "EU", "B"): 200.0,
        (2023, "US", "A"): 400.0,
        (2023, "US", "B"): 200.0,
    }


def test_yardstick_difference_from_average(tmp_path):
    sql_file = tmp_path / "quarterly.sql"
    sql_file.write_text(
        """
CREATE VIEW quarterly AS
SELECT
    year,
    quarter,
    SUM(amount) AS MEASURE revenue
FROM quarter_data;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE quarter_data (year INT, quarter INT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO quarter_data VALUES
    (2022, 1, 100), (2022, 2, 120), (2022, 3, 90), (2022, 4, 140);
"""
    )
    load_from_directory(layer, tmp_path)

    result = layer.sql(
        """
SEMANTIC SELECT
    year,
    quarter,
    AGGREGATE(revenue) - (AGGREGATE(revenue) AT (ALL quarter) / 4.0) AS diff_from_avg
FROM quarterly
"""
    )
    rows = fetch_dicts(result)
    values = {(row["year"], row["quarter"]): float(row["diff_from_avg"]) for row in rows}
    assert values == {
        (2022, 1): -12.5,
        (2022, 2): 7.5,
        (2022, 3): -22.5,
        (2022, 4): 27.5,
    }


def test_yardstick_group_by_alias_expression_dimension(tmp_path):
    sql_file = tmp_path / "monthly_sales.sql"
    sql_file.write_text(
        """
CREATE VIEW monthly_sales_v AS
SELECT
    DATE_TRUNC('month', order_date) AS month,
    region,
    SUM(amount) AS MEASURE revenue
FROM monthly_sales
GROUP BY DATE_TRUNC('month', order_date), region;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE monthly_sales (order_date DATE, region TEXT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO monthly_sales VALUES
    ('2023-01-05', 'US', 100), ('2023-01-12', 'EU', 50),
    ('2023-02-03', 'US', 200), ('2023-02-20', 'EU', 20);
"""
    )

    load_from_directory(layer, tmp_path)

    result = layer.sql(
        """
SEMANTIC SELECT
    month,
    region,
    AGGREGATE(revenue) AS revenue,
    AGGREGATE(revenue) AT (ALL region) AS month_total
FROM monthly_sales_v
"""
    )
    rows = fetch_dicts(result)
    values = {(str(row["month"]), row["region"]): (float(row["revenue"]), float(row["month_total"])) for row in rows}
    assert values == {
        ("2023-01-01", "EU"): (50.0, 150.0),
        ("2023-01-01", "US"): (100.0, 150.0),
        ("2023-02-01", "EU"): (20.0, 220.0),
        ("2023-02-01", "US"): (200.0, 220.0),
    }


def test_yardstick_null_dimensions_all_modifiers(tmp_path):
    sql_file = tmp_path / "sales_nulls.sql"
    sql_file.write_text(
        """
CREATE VIEW sales_nulls_v AS
SELECT
    year,
    region,
    SUM(amount) AS MEASURE revenue
FROM sales_nulls;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE sales_nulls (year INT, region TEXT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO sales_nulls VALUES
    (2022, 'US', 100), (2022, NULL, 50),
    (2023, 'US', 150), (2023, NULL, 75),
    (NULL, 'US', 10), (NULL, NULL, 5);
"""
    )

    load_from_directory(layer, tmp_path)

    base = fetch_dicts(layer.sql("SEMANTIC SELECT year, region, AGGREGATE(revenue) AS revenue FROM sales_nulls_v"))
    base_values = {(row["year"], row["region"]): float(row["revenue"]) for row in base}
    assert base_values == {
        (2022, "US"): 100.0,
        (2022, None): 50.0,
        (2023, "US"): 150.0,
        (2023, None): 75.0,
        (None, "US"): 10.0,
        (None, None): 5.0,
    }

    all_region = fetch_dicts(
        layer.sql("SEMANTIC SELECT year, region, AGGREGATE(revenue) AT (ALL region) AS year_total FROM sales_nulls_v")
    )
    all_region_values = {(row["year"], row["region"]): float(row["year_total"]) for row in all_region}
    assert all_region_values == {
        (2022, "US"): 150.0,
        (2022, None): 150.0,
        (2023, "US"): 225.0,
        (2023, None): 225.0,
        (None, "US"): 15.0,
        (None, None): 15.0,
    }

    all_year = fetch_dicts(
        layer.sql("SEMANTIC SELECT year, region, AGGREGATE(revenue) AT (ALL year) AS region_total FROM sales_nulls_v")
    )
    all_year_values = {(row["year"], row["region"]): float(row["region_total"]) for row in all_year}
    assert all_year_values == {
        (2022, "US"): 260.0,
        (2022, None): 130.0,
        (2023, "US"): 260.0,
        (2023, None): 130.0,
        (None, "US"): 260.0,
        (None, None): 130.0,
    }

    grand_total = fetch_dicts(layer.sql("SEMANTIC SELECT AGGREGATE(revenue) AS total FROM sales_nulls_v"))
    assert grand_total == [{"total": 390.0}]


def test_yardstick_from_detection_with_lowercase_and_comments(yardstick_layer):
    lower = fetch_dicts(
        yardstick_layer.sql(
            """
SEMANTIC SELECT year, region, AGGREGATE(revenue) AT (ALL region) AS year_total
from
    sales_v
"""
        )
    )
    assert {(row["year"], row["region"]): float(row["year_total"]) for row in lower} == {
        (2022, "EU"): 150.0,
        (2022, "US"): 150.0,
        (2023, "EU"): 225.0,
        (2023, "US"): 225.0,
    }

    line_comment = fetch_dicts(
        yardstick_layer.sql(
            """
SEMANTIC SELECT year, region, AGGREGATE(revenue) AT (ALL region) AS year_total
-- from sales_v
FROM sales_v
"""
        )
    )
    assert {(row["year"], row["region"]): float(row["year_total"]) for row in line_comment} == {
        (2022, "EU"): 150.0,
        (2022, "US"): 150.0,
        (2023, "EU"): 225.0,
        (2023, "US"): 225.0,
    }

    block_comment = fetch_dicts(
        yardstick_layer.sql(
            """
SEMANTIC SELECT year, region, AGGREGATE(revenue) AT (ALL region) AS year_total
/* from sales_v */
FROM sales_v
"""
        )
    )
    assert {(row["year"], row["region"]): float(row["year_total"]) for row in block_comment} == {
        (2022, "EU"): 150.0,
        (2022, "US"): 150.0,
        (2023, "EU"): 225.0,
        (2023, "US"): 225.0,
    }


def test_yardstick_group_by_extra_spaces(yardstick_layer):
    result = fetch_dicts(
        yardstick_layer.sql(
            """
SEMANTIC SELECT year, region, AGGREGATE(revenue) AT (ALL region) AS year_total
FROM sales_v
GROUP   BY year, region
"""
        )
    )
    assert {(row["year"], row["region"]): float(row["year_total"]) for row in result} == {
        (2022, "EU"): 150.0,
        (2022, "US"): 150.0,
        (2023, "EU"): 225.0,
        (2023, "US"): 225.0,
    }


def test_yardstick_group_by_positional_ordinal(yardstick_layer):
    result = fetch_dicts(
        yardstick_layer.sql(
            """
SEMANTIC SELECT
    year,
    AGGREGATE(revenue) AS revenue
FROM sales_v
GROUP BY 1
ORDER BY 1
"""
        )
    )
    assert [(row["year"], float(row["revenue"])) for row in result] == [
        (2022, 150.0),
        (2023, 225.0),
    ]


def test_yardstick_multiple_measures_with_different_modifiers(tmp_path):
    sql_file = tmp_path / "orders.sql"
    sql_file.write_text(
        """
CREATE VIEW orders_v AS
SELECT
    year,
    SUM(amount) AS MEASURE total_revenue,
    AVG(amount) AS MEASURE avg_order
FROM sales;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE sales (year INT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO sales VALUES
    (2022, 100), (2022, 50),
    (2023, 150), (2023, 75);
"""
    )
    load_from_directory(layer, tmp_path)

    result = fetch_dicts(
        layer.sql(
            """
SEMANTIC SELECT
    year,
    AGGREGATE(total_revenue) AS revenue,
    AGGREGATE(total_revenue) AT (ALL year) AS grand_total,
    AGGREGATE(avg_order) AT (SET year = year - 1) AS prev_avg
FROM orders_v
"""
        )
    )
    assert {row["year"]: (float(row["revenue"]), float(row["grand_total"]), row["prev_avg"]) for row in result} == {
        2022: (150.0, 375.0, None),
        2023: (225.0, 375.0, 75.0),
    }


def test_yardstick_yoy_growth_percent(yardstick_layer):
    result = fetch_dicts(
        yardstick_layer.sql(
            """
SEMANTIC SELECT
    year,
    AGGREGATE(revenue) AS revenue,
    100.0 * (AGGREGATE(revenue) - AGGREGATE(revenue) AT (SET year = year - 1))
        / AGGREGATE(revenue) AT (SET year = year - 1) AS yoy_pct
FROM sales_yearly
"""
        )
    )
    assert {
        row["year"]: (float(row["revenue"]), None if row["yoy_pct"] is None else float(row["yoy_pct"]))
        for row in result
    } == {
        2022: (150.0, None),
        2023: (225.0, 50.0),
    }


def test_yardstick_min_max_and_negative_values(tmp_path):
    sql_file = tmp_path / "measures.sql"
    sql_file.write_text(
        """
CREATE VIEW sales_minmax AS
SELECT year, region, MIN(amount) AS MEASURE min_sale, MAX(amount) AS MEASURE max_sale
FROM sales;

CREATE VIEW adj_v AS
SELECT year, region, SUM(amount) AS MEASURE adjustment
FROM adjustments;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE sales (year INT, region TEXT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO sales VALUES
    (2022, 'US', 100), (2022, 'EU', 50),
    (2023, 'US', 150), (2023, 'EU', 75);
"""
    )
    layer.adapter.execute("CREATE TABLE adjustments (year INT, region TEXT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO adjustments VALUES
    (2022, 'US', -20), (2022, 'EU', 10),
    (2023, 'US', 0), (2023, 'EU', -5);
"""
    )
    load_from_directory(layer, tmp_path)

    minmax = fetch_dicts(
        layer.sql(
            "SEMANTIC SELECT year, region, AGGREGATE(min_sale) AS min_sale, AGGREGATE(max_sale) AS max_sale FROM sales_minmax"
        )
    )
    assert {(row["year"], row["region"]): (float(row["min_sale"]), float(row["max_sale"])) for row in minmax} == {
        (2022, "EU"): (50.0, 50.0),
        (2022, "US"): (100.0, 100.0),
        (2023, "EU"): (75.0, 75.0),
        (2023, "US"): (150.0, 150.0),
    }

    adjustments = fetch_dicts(
        layer.sql("SEMANTIC SELECT year, region, AGGREGATE(adjustment) AT (ALL) AS total_adj FROM adj_v")
    )
    assert {(row["year"], row["region"]): float(row["total_adj"]) for row in adjustments} == {
        (2022, "EU"): -15.0,
        (2022, "US"): -15.0,
        (2023, "EU"): -15.0,
        (2023, "US"): -15.0,
    }


def test_yardstick_filtered_aggregate_measure(tmp_path):
    sql_file = tmp_path / "filtered.sql"
    sql_file.write_text(
        """
CREATE VIEW filtered_agg_v AS
SELECT
    year,
    SUM(amount) AS MEASURE total_revenue,
    SUM(amount) FILTER (WHERE region = 'US') AS MEASURE us_revenue
FROM sales;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE sales (year INT, region TEXT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO sales VALUES
    (2022, 'US', 100), (2022, 'EU', 50),
    (2023, 'US', 150), (2023, 'EU', 75);
"""
    )
    load_from_directory(layer, tmp_path)

    result = fetch_dicts(
        layer.sql(
            "SEMANTIC SELECT year, AGGREGATE(total_revenue) AS total_revenue, AGGREGATE(us_revenue) AS us_revenue FROM filtered_agg_v"
        )
    )
    assert {row["year"]: (float(row["total_revenue"]), float(row["us_revenue"])) for row in result} == {
        2022: (150.0, 100.0),
        2023: (225.0, 150.0),
    }


def test_yardstick_count_distinct_and_median_recomputation(tmp_path):
    sql_file = tmp_path / "non_decomposable.sql"
    sql_file.write_text(
        """
CREATE VIEW distinct_count_v AS
SELECT year, COUNT(DISTINCT region) AS MEASURE unique_regions
FROM dup_regions;

CREATE VIEW distinct_count_cte_v AS
WITH base AS (SELECT * FROM dup_regions)
SELECT year, COUNT(DISTINCT region) AS MEASURE unique_regions
FROM base;

CREATE VIEW median_v AS
SELECT category, MEDIAN(value) AS MEASURE med_value
FROM median_test;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE dup_regions (year INT, region TEXT, amount DOUBLE)")
    layer.adapter.execute("INSERT INTO dup_regions VALUES (2023, 'US', 100), (2023, 'US', 100), (2023, 'EU', 50)")
    layer.adapter.execute("CREATE TABLE median_test (category TEXT, value DOUBLE)")
    layer.adapter.execute("INSERT INTO median_test VALUES ('A', 10), ('A', 20), ('A', 30), ('B', 100), ('B', 100)")
    load_from_directory(layer, tmp_path)

    distinct_base = fetch_dicts(
        layer.sql("SEMANTIC SELECT year, AGGREGATE(unique_regions) AS unique_regions FROM distinct_count_v")
    )
    assert distinct_base == [{"year": 2023, "unique_regions": 2}]

    distinct_all = fetch_dicts(
        layer.sql("SEMANTIC SELECT year, AGGREGATE(unique_regions) AT (ALL) AS unique_regions FROM distinct_count_v")
    )
    assert distinct_all == [{"year": 2023, "unique_regions": 2}]

    distinct_cte = fetch_dicts(
        layer.sql("SEMANTIC SELECT year, AGGREGATE(unique_regions) AS unique_regions FROM distinct_count_cte_v")
    )
    assert distinct_cte == [{"year": 2023, "unique_regions": 2}]

    median_by_category = fetch_dicts(
        layer.sql("SEMANTIC SELECT category, AGGREGATE(med_value) AS med_value FROM median_v")
    )
    assert {(row["category"], float(row["med_value"])) for row in median_by_category} == {("A", 20.0), ("B", 100.0)}

    median_all = fetch_dicts(layer.sql("SEMANTIC SELECT AGGREGATE(med_value) AT (ALL) AS med_value FROM median_v"))
    assert median_all == [{"med_value": 30.0}]


def test_yardstick_constants_and_cast(yardstick_layer):
    int_literal = fetch_dicts(
        yardstick_layer.sql("SEMANTIC SELECT 1000 AS marker, AGGREGATE(revenue) AS total FROM sales_v")
    )
    assert int_literal == [{"marker": 1000, "total": 375.0}]

    str_literal = fetch_dicts(
        yardstick_layer.sql("SEMANTIC SELECT 'hello' AS marker, AGGREGATE(revenue) AS total FROM sales_v")
    )
    assert str_literal == [{"marker": "hello", "total": 375.0}]

    null_literal = fetch_dicts(
        yardstick_layer.sql("SEMANTIC SELECT NULL AS marker, AGGREGATE(revenue) AS total FROM sales_v")
    )
    assert null_literal == [{"marker": None, "total": 375.0}]

    with_dimension = fetch_dicts(
        yardstick_layer.sql("SEMANTIC SELECT year, 1000 AS marker, AGGREGATE(revenue) AS total FROM sales_v")
    )
    assert {row["year"]: (row["marker"], float(row["total"])) for row in with_dimension} == {
        2022: (1000, 150.0),
        2023: (1000, 225.0),
    }

    cast_result = fetch_dicts(
        yardstick_layer.sql("SEMANTIC SELECT year, AGGREGATE(revenue)::INTEGER AS revenue_int FROM sales_yearly")
    )
    assert cast_result == [{"year": 2022, "revenue_int": 150}, {"year": 2023, "revenue_int": 225}]


def test_yardstick_join_with_extra_dimension_from_second_table(tmp_path):
    sql_file = tmp_path / "join_dims.sql"
    sql_file.write_text(
        """
CREATE VIEW sales_v AS
SELECT year, region, SUM(amount) AS MEASURE revenue
FROM sales;

CREATE VIEW salesdetails_v AS
SELECT year, region, product, SUM(amount) AS MEASURE quantity
FROM salesdetails;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE sales (year INT, region TEXT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO sales VALUES
    (2022, 'US', 100), (2022, 'EU', 50),
    (2023, 'US', 150), (2023, 'EU', 75);
"""
    )
    layer.adapter.execute("CREATE TABLE salesdetails (year INT, region TEXT, product TEXT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO salesdetails VALUES
    (2022, 'US', 'Shoes', 2), (2022, 'US', 'Cars', 1),
    (2022, 'EU', 'Shoes', 3),
    (2023, 'US', 'Shoes', 4), (2023, 'US', 'Cars', 2),
    (2023, 'EU', 'Cars', 5);
"""
    )
    load_from_directory(layer, tmp_path)

    result = fetch_dicts(
        layer.sql(
            """
SEMANTIC SELECT
    s.year,
    s.region,
    sd.product,
    AGGREGATE(revenue) AS year_sales_revenue,
    AGGREGATE(revenue) AT (ALL year) AS region_total,
    AGGREGATE(quantity) AS product_qty
FROM sales_v s
JOIN salesdetails_v sd ON s.year = sd.year AND s.region = sd.region
"""
        )
    )
    values = {
        (row["year"], row["region"], row["product"]): (
            float(row["year_sales_revenue"]),
            float(row["region_total"]),
            float(row["product_qty"]),
        )
        for row in result
    }
    assert values == {
        (2022, "EU", "Shoes"): (50.0, 125.0, 3.0),
        (2022, "US", "Cars"): (100.0, 250.0, 1.0),
        (2022, "US", "Shoes"): (100.0, 250.0, 2.0),
        (2023, "EU", "Cars"): (75.0, 125.0, 5.0),
        (2023, "US", "Cars"): (150.0, 250.0, 2.0),
        (2023, "US", "Shoes"): (150.0, 250.0, 4.0),
    }


def test_yardstick_moving_total_with_set(tmp_path):
    sql_file = tmp_path / "yearly.sql"
    sql_file.write_text(
        """
CREATE VIEW yearly_v AS
SELECT year, SUM(amount) AS MEASURE revenue
FROM yearly_data;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE yearly_data (year INT, amount DOUBLE)")
    layer.adapter.execute("INSERT INTO yearly_data VALUES (2020, 100), (2021, 120), (2022, 150), (2023, 180)")
    load_from_directory(layer, tmp_path)

    result = fetch_dicts(
        layer.sql(
            """
SEMANTIC SELECT
    year,
    AGGREGATE(revenue) AS current,
    AGGREGATE(revenue) + AGGREGATE(revenue) AT (SET year = year - 1) AS two_year_total
FROM yearly_v
"""
        )
    )
    assert {
        row["year"]: (float(row["current"]), None if row["two_year_total"] is None else float(row["two_year_total"]))
        for row in result
    } == {
        2020: (100.0, None),
        2021: (120.0, 220.0),
        2022: (150.0, 270.0),
        2023: (180.0, 330.0),
    }


def test_yardstick_case_expression_measure(tmp_path):
    sql_file = tmp_path / "case_measure.sql"
    sql_file.write_text(
        """
CREATE OR REPLACE VIEW case_measure AS
SELECT year, CASE WHEN SUM(amount) > 150 THEN 1 ELSE 0 END AS MEASURE high_value
FROM case_data;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE case_data (year INT, amount DOUBLE)")
    layer.adapter.execute("INSERT INTO case_data VALUES (2022, 100), (2022, 50), (2023, 200), (2023, 100)")
    load_from_directory(layer, tmp_path)

    result = fetch_dicts(layer.sql("SEMANTIC SELECT year, AGGREGATE(high_value) AS high_value FROM case_measure"))
    assert result == [{"year": 2022, "high_value": 0}, {"year": 2023, "high_value": 1}]


def test_yardstick_ordered_set_aggregates(tmp_path):
    sql_file = tmp_path / "ordered_set.sql"
    sql_file.write_text(
        """
CREATE VIEW ordered_set_v AS
SELECT
    category,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS MEASURE p50,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS MEASURE p50d,
    QUANTILE_CONT(value, 0.5) AS MEASURE q50,
    QUANTILE_DISC(value, 0.5) AS MEASURE q50d,
    MODE(value) AS MEASURE mode_value
FROM ordered_set_test;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE ordered_set_test (category TEXT, value INT)")
    layer.adapter.execute(
        """
INSERT INTO ordered_set_test VALUES
    ('A', 1), ('A', 1), ('A', 2), ('A', 3), ('A', 4),
    ('B', 10), ('B', 10), ('B', 20);
"""
    )
    load_from_directory(layer, tmp_path)

    p50 = fetch_dicts(layer.sql("SEMANTIC SELECT category, AGGREGATE(p50) AS p50 FROM ordered_set_v"))
    assert {(row["category"], float(row["p50"])) for row in p50} == {("A", 2.0), ("B", 10.0)}

    p50d = fetch_dicts(layer.sql("SEMANTIC SELECT category, AGGREGATE(p50d) AS p50d FROM ordered_set_v"))
    assert {(row["category"], int(row["p50d"])) for row in p50d} == {("A", 2), ("B", 10)}

    q50 = fetch_dicts(layer.sql("SEMANTIC SELECT category, AGGREGATE(q50) AS q50 FROM ordered_set_v"))
    assert {(row["category"], float(row["q50"])) for row in q50} == {("A", 2.0), ("B", 10.0)}

    q50d = fetch_dicts(layer.sql("SEMANTIC SELECT category, AGGREGATE(q50d) AS q50d FROM ordered_set_v"))
    assert {(row["category"], int(row["q50d"])) for row in q50d} == {("A", 2), ("B", 10)}

    mode_value = fetch_dicts(
        layer.sql("SEMANTIC SELECT category, AGGREGATE(mode_value) AS mode_value FROM ordered_set_v")
    )
    assert {(row["category"], int(row["mode_value"])) for row in mode_value} == {("A", 1), ("B", 10)}


def test_yardstick_generate_series_source(tmp_path):
    sql_file = tmp_path / "series.sql"
    sql_file.write_text(
        """
CREATE VIEW series_v AS
SELECT x, SUM(x) AS MEASURE total
FROM generate_series(1, 5) AS t(x);
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    load_from_directory(layer, tmp_path)

    by_x = fetch_dicts(layer.sql("SEMANTIC SELECT x, AGGREGATE(total) AS total FROM series_v"))
    assert {(row["x"], int(row["total"])) for row in by_x} == {(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)}

    grand = fetch_dicts(layer.sql("SEMANTIC SELECT AGGREGATE(total) AS total FROM series_v"))
    assert grand == [{"total": 15}]


def test_yardstick_duckdb_scalar_functions(tmp_path):
    sql_file = tmp_path / "duckdb_functions.sql"
    sql_file.write_text(
        """
CREATE VIEW dated_sales_v AS
SELECT sale_date, SUM(amount) AS MEASURE revenue
FROM dated_sales;

CREATE VIEW products_str_v AS
SELECT category, subcategory, SUM(amount) AS MEASURE revenue
FROM products_str;

CREATE VIEW nullable_sales_v AS
SELECT region, SUM(amount) AS MEASURE revenue
FROM nullable_sales;

CREATE VIEW tagged_items_v AS
SELECT tags, SUM(amount) AS MEASURE revenue
FROM tagged_items;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE dated_sales (sale_date DATE, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO dated_sales VALUES
    ('2023-01-15', 100), ('2023-01-20', 150),
    ('2023-02-10', 200), ('2023-03-05', 80),
    ('2024-01-08', 250), ('2024-02-14', 180);
"""
    )
    layer.adapter.execute("CREATE TABLE products_str (category TEXT, subcategory TEXT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO products_str VALUES
    ('Electronics', 'Phones', 500),
    ('Electronics', 'Laptops', 800),
    ('Clothing', 'Shirts', 100),
    ('Clothing', 'Pants', 150);
"""
    )
    layer.adapter.execute("CREATE TABLE nullable_sales (region TEXT, amount DOUBLE)")
    layer.adapter.execute("INSERT INTO nullable_sales VALUES ('US', 100), (NULL, 50), ('EU', 75), (NULL, 25)")
    layer.adapter.execute("CREATE TABLE tagged_items (tags TEXT[], amount DOUBLE)")
    layer.adapter.execute("INSERT INTO tagged_items VALUES (['a', 'b'], 100), (['b', 'c'], 150), (['a'], 80)")
    load_from_directory(layer, tmp_path)

    year_month = fetch_dicts(
        layer.sql(
            "SEMANTIC SELECT YEAR(sale_date) AS yr, MONTH(sale_date) AS mo, AGGREGATE(revenue) AS revenue FROM dated_sales_v"
        )
    )
    assert {(row["yr"], row["mo"], float(row["revenue"])) for row in year_month} == {
        (2023, 1, 250.0),
        (2023, 2, 200.0),
        (2023, 3, 80.0),
        (2024, 1, 250.0),
        (2024, 2, 180.0),
    }

    category_upper = fetch_dicts(
        layer.sql("SEMANTIC SELECT UPPER(category) AS cat, AGGREGATE(revenue) AS revenue FROM products_str_v")
    )
    assert {(row["cat"], float(row["revenue"])) for row in category_upper} == {
        ("CLOTHING", 250.0),
        ("ELECTRONICS", 1300.0),
    }

    region_coalesce = fetch_dicts(
        layer.sql(
            "SEMANTIC SELECT COALESCE(region, 'Unknown') AS region_bucket, AGGREGATE(revenue) AS revenue FROM nullable_sales_v"
        )
    )
    assert {(row["region_bucket"], float(row["revenue"])) for row in region_coalesce} == {
        ("EU", 75.0),
        ("US", 100.0),
        ("Unknown", 75.0),
    }

    tag_len = fetch_dicts(
        layer.sql("SEMANTIC SELECT ARRAY_LENGTH(tags) AS tag_len, AGGREGATE(revenue) AS revenue FROM tagged_items_v")
    )
    assert {(int(row["tag_len"]), float(row["revenue"])) for row in tag_len} == {(1, 80.0), (2, 250.0)}

    by_year_trunc = fetch_dicts(
        layer.sql(
            "SEMANTIC SELECT DATE_TRUNC('year', sale_date) AS yr, AGGREGATE(revenue) AS revenue FROM dated_sales_v"
        )
    )
    assert {(str(row["yr"]), float(row["revenue"])) for row in by_year_trunc} == {
        ("2023-01-01", 530.0),
        ("2024-01-01", 430.0),
    }

    category_left = fetch_dicts(
        layer.sql("SEMANTIC SELECT LEFT(category, 4) AS cat_prefix, AGGREGATE(revenue) AS revenue FROM products_str_v")
    )
    assert {(row["cat_prefix"], float(row["revenue"])) for row in category_left} == {
        ("Clot", 250.0),
        ("Elec", 1300.0),
    }


def test_yardstick_window_measure_direct_and_at_where(tmp_path):
    sql_file = tmp_path / "window.sql"
    sql_file.write_text(
        """
CREATE VIEW running_v AS
SELECT
    year,
    SUM(amount) OVER (ORDER BY year) AS MEASURE running_total
FROM window_sales;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE window_sales (year INT, amount DOUBLE)")
    layer.adapter.execute("INSERT INTO window_sales VALUES (2022, 100), (2023, 150), (2024, 200)")
    load_from_directory(layer, tmp_path)

    direct_rows = fetch_dicts(
        layer.sql(
            """
SEMANTIC SELECT
    year,
    AGGREGATE(running_total) AS running_total
FROM running_v
GROUP BY year
"""
        )
    )
    assert {row["year"]: float(row["running_total"]) for row in direct_rows} == {
        2022: 100.0,
        2023: 250.0,
        2024: 450.0,
    }

    where_rows = fetch_dicts(
        layer.sql(
            """
SEMANTIC SELECT
    AGGREGATE(running_total) AT (WHERE year = 2024) AS running_total_2024
FROM running_v
"""
        )
    )
    assert [float(row["running_total_2024"]) for row in where_rows] == [200.0]


def test_yardstick_window_measure_at_all_raises_on_ambiguous_context(tmp_path):
    sql_file = tmp_path / "window.sql"
    sql_file.write_text(
        """
CREATE VIEW running_v AS
SELECT
    year,
    SUM(amount) OVER (ORDER BY year) AS MEASURE running_total
FROM window_sales;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE window_sales (year INT, amount DOUBLE)")
    layer.adapter.execute("INSERT INTO window_sales VALUES (2022, 100), (2023, 150), (2024, 200)")
    load_from_directory(layer, tmp_path)

    with pytest.raises(Exception):
        layer.sql(
            """
SEMANTIC SELECT
    year,
    AGGREGATE(running_total) AT (ALL) AS running_total_all
FROM running_v
GROUP BY year
"""
        )


def test_yardstick_derived_measure_with_at_all(yardstick_layer):
    result = fetch_dicts(
        yardstick_layer.sql(
            """
SEMANTIC SELECT
    year,
    AGGREGATE(profit) AS profit,
    AGGREGATE(profit) AT (ALL) AS total_profit
FROM financials_v
"""
        )
    )
    assert {row["year"]: (float(row["profit"]), float(row["total_profit"])) for row in result} == {
        2022: (110.0, 340.0),
        2023: (230.0, 340.0),
    }


def test_yardstick_multi_fact_derived_expression(yardstick_layer):
    result = fetch_dicts(
        yardstick_layer.sql(
            """
SEMANTIC SELECT
    o.year,
    o.region,
    AGGREGATE(revenue) AS revenue,
    AGGREGATE(refunds) AS refunds,
    AGGREGATE(revenue) - AGGREGATE(refunds) AS net_revenue
FROM fact_orders_v o
JOIN fact_returns_v r ON o.year = r.year AND o.region = r.region
"""
        )
    )
    assert {
        (row["year"], row["region"]): (float(row["revenue"]), float(row["refunds"]), float(row["net_revenue"]))
        for row in result
    } == {
        (2022, "EU"): (50.0, 5.0, 45.0),
        (2022, "US"): (100.0, 10.0, 90.0),
        (2023, "EU"): (75.0, 8.0, 67.0),
        (2023, "US"): (150.0, 20.0, 130.0),
    }


def test_yardstick_visible_with_year_filter(yardstick_layer):
    result = fetch_dicts(
        yardstick_layer.sql(
            """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AT (VISIBLE) AS visible_2023
FROM sales_v
WHERE year = 2023
"""
        )
    )
    assert {(row["year"], row["region"], float(row["visible_2023"])) for row in result} == {
        (2023, "EU", 75.0),
        (2023, "US", 150.0),
    }


def test_yardstick_scalar_at_all_without_group_by(yardstick_layer):
    result = fetch_dicts(yardstick_layer.sql("SEMANTIC SELECT AGGREGATE(revenue) AT (ALL) AS total FROM sales_v"))
    assert result == [{"total": 375.0}]


def test_yardstick_all_year_on_single_dimension_view(yardstick_layer):
    result = fetch_dicts(
        yardstick_layer.sql(
            """
SEMANTIC SELECT
    year,
    AGGREGATE(revenue) AT (ALL year) AS grand_total
FROM sales_yearly
"""
        )
    )
    assert {row["year"]: float(row["grand_total"]) for row in result} == {2022: 375.0, 2023: 375.0}


def test_yardstick_percent_of_year_total(yardstick_layer):
    result = fetch_dicts(
        yardstick_layer.sql(
            """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AS revenue,
    100.0 * AGGREGATE(revenue) / AGGREGATE(revenue) AT (ALL region) AS pct_of_year
FROM sales_v
"""
        )
    )
    assert {(row["year"], row["region"]): (float(row["revenue"]), float(row["pct_of_year"])) for row in result} == {
        (2022, "EU"): (50.0, pytest.approx(33.33333333333333)),
        (2022, "US"): (100.0, pytest.approx(66.66666666666667)),
        (2023, "EU"): (75.0, pytest.approx(33.33333333333333)),
        (2023, "US"): (150.0, pytest.approx(66.66666666666667)),
    }


def test_yardstick_set_correlates_vs_base_value(yardstick_layer):
    fixed = fetch_dicts(
        yardstick_layer.sql(
            """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AT (SET year = 2022) AS fixed_2022
FROM sales_v
"""
        )
    )
    assert {(row["year"], row["region"]): float(row["fixed_2022"]) for row in fixed} == {
        (2022, "EU"): 50.0,
        (2022, "US"): 100.0,
        (2023, "EU"): 50.0,
        (2023, "US"): 100.0,
    }

    own = fetch_dicts(
        yardstick_layer.sql(
            """
SEMANTIC SELECT
    year,
    region,
    AGGREGATE(revenue) AS own_revenue
FROM sales_v
"""
        )
    )
    assert {(row["year"], row["region"]): float(row["own_revenue"]) for row in own} == {
        (2022, "EU"): 50.0,
        (2022, "US"): 100.0,
        (2023, "EU"): 75.0,
        (2023, "US"): 150.0,
    }


def test_yardstick_direct_query_baseline_for_distinct_and_median(tmp_path):
    sql_file = tmp_path / "baseline.sql"
    sql_file.write_text(
        """
CREATE VIEW distinct_count_v AS
SELECT year, COUNT(DISTINCT region) AS MEASURE unique_regions
FROM dup_regions;

CREATE VIEW median_v AS
SELECT category, MEDIAN(value) AS MEASURE med_value
FROM median_test;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE dup_regions (year INT, region TEXT, amount DOUBLE)")
    layer.adapter.execute("INSERT INTO dup_regions VALUES (2023, 'US', 100), (2023, 'US', 100), (2023, 'EU', 50)")
    layer.adapter.execute("CREATE TABLE median_test (category TEXT, value DOUBLE)")
    layer.adapter.execute("INSERT INTO median_test VALUES ('A', 10), ('A', 20), ('A', 30), ('B', 100), ('B', 100)")
    load_from_directory(layer, tmp_path)

    distinct_direct = fetch_dicts(layer.sql("SELECT year, unique_regions FROM distinct_count_v"))
    assert distinct_direct == [{"year": 2023, "unique_regions": 2}]

    median_direct = fetch_dicts(layer.sql("SELECT category, med_value FROM median_v"))
    assert {(row["category"], float(row["med_value"])) for row in median_direct} == {("A", 20.0), ("B", 100.0)}


def test_yardstick_no_semantic_at_where_with_qualified_column(yardstick_layer):
    result = fetch_dicts(
        yardstick_layer.sql(
            """
SELECT
    year,
    region,
    AGGREGATE(revenue) AT (WHERE sales_v.region = 'US') AS us_revenue
FROM sales_v
"""
        )
    )
    assert {(row["year"], row["region"], float(row["us_revenue"])) for row in result} == {
        (2022, "EU", 250.0),
        (2022, "US", 250.0),
        (2023, "EU", 250.0),
        (2023, "US", 250.0),
    }


def test_yardstick_unnest_source_relation(tmp_path):
    sql_file = tmp_path / "unnest.sql"
    sql_file.write_text(
        """
CREATE VIEW tag_sales_v AS
SELECT
    tag,
    SUM(amount) AS MEASURE revenue
FROM (
    SELECT UNNEST(tags) AS tag, amount
    FROM tagged_items
) AS exploded;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE tagged_items (tags TEXT[], amount DOUBLE)")
    layer.adapter.execute("INSERT INTO tagged_items VALUES (['a', 'b'], 100), (['b', 'c'], 150), (['a'], 80)")
    load_from_directory(layer, tmp_path)

    base = fetch_dicts(layer.sql("SEMANTIC SELECT tag, AGGREGATE(revenue) AS revenue FROM tag_sales_v"))
    assert {(row["tag"], float(row["revenue"])) for row in base} == {
        ("a", 180.0),
        ("b", 250.0),
        ("c", 150.0),
    }

    all_tags = fetch_dicts(layer.sql("SEMANTIC SELECT tag, AGGREGATE(revenue) AT (ALL tag) AS total FROM tag_sales_v"))
    assert {(row["tag"], float(row["total"])) for row in all_tags} == {
        ("a", 580.0),
        ("b", 580.0),
        ("c", 580.0),
    }


def test_yardstick_generate_series_rewrite_uses_subquery_source(tmp_path):
    sql_file = tmp_path / "series.sql"
    sql_file.write_text(
        """
CREATE VIEW series_v AS
SELECT x, SUM(x) AS MEASURE total
FROM generate_series(1, 5) AS t(x);
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    load_from_directory(layer, tmp_path)

    rewriter = QueryRewriter(layer.graph, dialect=layer.adapter.dialect)
    rewritten = rewriter.rewrite("SEMANTIC SELECT x, AGGREGATE(total) AS total FROM series_v")

    # Guardrail: table-function sources must be wrapped as relation SQL, not re-aliased table names.
    assert "FROM (SELECT * FROM GENERATE_SERIES(1, 5) AS T(X)) AS _INNER" in rewritten.upper()


def test_yardstick_expression_dimension_rewrite_uses_projection_alias(tmp_path):
    sql_file = tmp_path / "coalesce.sql"
    sql_file.write_text(
        """
CREATE VIEW nullable_sales_v AS
SELECT region, SUM(amount) AS MEASURE revenue
FROM nullable_sales;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE nullable_sales (region TEXT, amount DOUBLE)")
    layer.adapter.execute("INSERT INTO nullable_sales VALUES ('US', 100), (NULL, 50), ('EU', 75), (NULL, 25)")
    load_from_directory(layer, tmp_path)

    query = "SEMANTIC SELECT COALESCE(region, 'Unknown') AS region_bucket, AGGREGATE(revenue) AS revenue FROM nullable_sales_v"
    rewriter = QueryRewriter(layer.graph, dialect=layer.adapter.dialect)
    rewritten = rewriter.rewrite(query)

    # Guardrail: expression-dimension correlation should bind against outer projection alias.
    assert "IS NOT DISTINCT FROM (region_bucket)" in rewritten


def test_yardstick_default_alias_not_applied_with_non_semantic_join(tmp_path):
    sql_file = tmp_path / "sales.sql"
    sql_file.write_text(
        """
CREATE VIEW sales_v AS
SELECT year, region, SUM(amount) AS MEASURE revenue
FROM sales;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE sales (year INT, region TEXT, amount DOUBLE)")
    layer.adapter.execute(
        """
INSERT INTO sales VALUES
    (2022, 'US', 100), (2022, 'EU', 50),
    (2023, 'US', 150), (2023, 'EU', 75);
"""
    )
    layer.adapter.execute("CREATE TABLE region_labels (region TEXT, label TEXT)")
    layer.adapter.execute("INSERT INTO region_labels VALUES ('US', 'Core'), ('EU', 'Edge')")
    load_from_directory(layer, tmp_path)

    query = """
SEMANTIC SELECT
    year,
    label,
    AGGREGATE(revenue) AS revenue
FROM sales_v AS s
JOIN region_labels AS l ON s.region = l.region
"""
    rewriter = QueryRewriter(layer.graph, dialect=layer.adapter.dialect)
    rewritten = rewriter.rewrite(query)

    # Guardrail: unaliased non-semantic columns should not be forced onto the semantic alias.
    assert "S.LABEL" not in rewritten.upper()


def test_yardstick_subquery_placeholder_rewrite_stays_in_inner_scope(yardstick_layer):
    rows = fetch_dicts(
        yardstick_layer.sql(
            """
SEMANTIC SELECT * FROM (
    SELECT AGGREGATE(revenue) AS total_revenue
    FROM sales_v
) AS scoped
"""
        )
    )

    assert len(rows) == 1
    assert rows[0]["total_revenue"] == pytest.approx(375.0)
