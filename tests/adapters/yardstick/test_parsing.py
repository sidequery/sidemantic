"""Tests for Yardstick adapter parsing."""

from pathlib import Path

from sidemantic import SemanticLayer
from sidemantic.adapters.yardstick import YardstickAdapter
from sidemantic.loaders import load_from_directory
from tests.utils import fetch_dicts


def test_import_yardstick_view(tmp_path):
    sql_file = tmp_path / "sales.sql"
    sql_file.write_text(
        """
CREATE VIEW sales_v AS
SELECT
    year,
    region,
    SUM(amount) AS MEASURE revenue,
    COUNT(*) AS MEASURE order_count
FROM sales;
"""
    )

    adapter = YardstickAdapter()
    graph = adapter.parse(sql_file)

    assert "sales_v" in graph.models
    model = graph.models["sales_v"]

    assert model.table == "sales"
    assert [dim.name for dim in model.dimensions] == ["year", "region"]
    assert model.primary_key == "year"

    revenue = model.get_metric("revenue")
    assert revenue is not None
    assert revenue.agg == "sum"
    assert revenue.sql == "amount"

    order_count = model.get_metric("order_count")
    assert order_count is not None
    assert order_count.agg == "count"
    assert order_count.sql == "*"


def test_import_yardstick_derived_measure(tmp_path):
    sql_file = tmp_path / "financials.sql"
    sql_file.write_text(
        """
CREATE VIEW financials_v AS
SELECT
    year,
    SUM(revenue) AS MEASURE revenue,
    SUM(cost) AS MEASURE cost,
    revenue - cost AS MEASURE profit
FROM financials;
"""
    )

    adapter = YardstickAdapter()
    graph = adapter.parse(sql_file)

    model = graph.models["financials_v"]
    profit = model.get_metric("profit")
    assert profit is not None
    assert profit.type == "derived"
    assert profit.sql == "revenue - cost"


def test_import_yardstick_count_distinct_from_base_table(tmp_path):
    sql_file = tmp_path / "distinct_count.sql"
    sql_file.write_text(
        """
CREATE VIEW distinct_count_v AS
SELECT
    year,
    COUNT(DISTINCT region) AS MEASURE unique_regions
FROM dup_regions;
"""
    )

    adapter = YardstickAdapter()
    graph = adapter.parse(sql_file)

    model = graph.models["distinct_count_v"]
    assert model.table == "dup_regions"
    unique_regions = model.get_metric("unique_regions")
    assert unique_regions is not None
    assert unique_regions.agg == "count_distinct"
    assert unique_regions.sql == "region"


def test_import_yardstick_filtered_aggregate_measure(tmp_path):
    sql_file = tmp_path / "filtered_agg.sql"
    sql_file.write_text(
        """
CREATE VIEW filtered_agg_v AS
SELECT
    year,
    SUM(amount) FILTER (WHERE region = 'US') AS MEASURE us_revenue
FROM sales;
"""
    )

    adapter = YardstickAdapter()
    graph = adapter.parse(sql_file)

    model = graph.models["filtered_agg_v"]
    assert model.table == "sales"
    us_revenue = model.get_metric("us_revenue")
    assert us_revenue is not None
    assert us_revenue.agg == "sum"
    assert us_revenue.sql == "amount"
    assert us_revenue.filters == ["region = 'US'"]


def test_import_yardstick_nonstandard_aggregate_metric_keeps_sql_expression(tmp_path):
    sql_file = tmp_path / "ordered_set.sql"
    sql_file.write_text(
        """
CREATE VIEW ordered_set_v AS
SELECT
    category,
    MODE(value) AS MEASURE mode_value
FROM ordered_set_test;
"""
    )

    adapter = YardstickAdapter()
    graph = adapter.parse(sql_file)

    model = graph.models["ordered_set_v"]
    mode_value = model.get_metric("mode_value")
    assert mode_value is not None
    assert mode_value.agg is None
    assert mode_value.type is None
    assert mode_value.sql == "MODE(value)"


def test_import_yardstick_view_with_where_uses_model_sql_base_relation(tmp_path):
    sql_file = tmp_path / "paid_sales.sql"
    sql_file.write_text(
        """
CREATE VIEW paid_sales_v AS
SELECT
    year,
    SUM(amount) AS MEASURE revenue
FROM sales
WHERE status = 'paid';
"""
    )

    adapter = YardstickAdapter()
    graph = adapter.parse(sql_file)

    model = graph.models["paid_sales_v"]
    assert model.table is None
    assert model.sql is not None
    assert "FROM sales" in model.sql
    assert "WHERE status = 'paid'" in model.sql


def test_import_directory_of_yardstick_sql(tmp_path):
    first = tmp_path / "sales.sql"
    second = tmp_path / "orders.sql"
    first.write_text(
        """
CREATE VIEW sales_v AS
SELECT region, SUM(amount) AS MEASURE revenue
FROM sales;
"""
    )
    second.write_text(
        """
CREATE VIEW orders_v AS
SELECT status, COUNT(*) AS MEASURE order_count
FROM orders;
"""
    )

    adapter = YardstickAdapter()
    graph = adapter.parse(Path(tmp_path))

    assert {"sales_v", "orders_v"} == set(graph.models)


def test_import_yardstick_forward_measure_reference(tmp_path):
    sql_file = tmp_path / "financials_forward.sql"
    sql_file.write_text(
        """
CREATE VIEW financials_v AS
SELECT
    year,
    SUM(revenue) AS MEASURE revenue,
    revenue - cost AS MEASURE profit,
    SUM(cost) AS MEASURE cost
FROM financials;
"""
    )

    adapter = YardstickAdapter()
    graph = adapter.parse(sql_file)

    model = graph.models["financials_v"]
    profit = model.get_metric("profit")
    assert profit is not None
    assert profit.type == "derived"
    assert profit.sql == "revenue - cost"


def test_yardstick_mode_measure_executes_via_query(tmp_path):
    sql_file = tmp_path / "ordered_set.sql"
    sql_file.write_text(
        """
CREATE VIEW ordered_set_v AS
SELECT
    category,
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

    result = layer.query(
        dimensions=["ordered_set_v.category"],
        metrics=["ordered_set_v.mode_value"],
    )
    rows = fetch_dicts(result)

    assert {row["category"]: row["mode_value"] for row in rows} == {"A": 1, "B": 10}


def test_yardstick_percentile_within_group_executes_via_query(tmp_path):
    sql_file = tmp_path / "ordered_set.sql"
    sql_file.write_text(
        """
CREATE VIEW ordered_set_v AS
SELECT
    category,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS MEASURE p50
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

    result = layer.query(
        dimensions=["ordered_set_v.category"],
        metrics=["ordered_set_v.p50"],
    )
    rows = fetch_dicts(result)
    by_category = {row["category"]: float(row["p50"]) for row in rows}

    assert by_category == {"A": 2.0, "B": 10.0}


def test_yardstick_derived_measure_over_mode_executes_via_query(tmp_path):
    sql_file = tmp_path / "ordered_set.sql"
    sql_file.write_text(
        """
CREATE VIEW ordered_set_v AS
SELECT
    category,
    MODE(value) AS MEASURE mode_value,
    mode_value + 1 AS MEASURE mode_plus_one
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

    result = layer.query(
        dimensions=["ordered_set_v.category"],
        metrics=["ordered_set_v.mode_plus_one"],
    )
    rows = fetch_dicts(result)

    assert {row["category"]: row["mode_plus_one"] for row in rows} == {"A": 2, "B": 11}


def test_yardstick_case_measure_executes_via_query(tmp_path):
    sql_file = tmp_path / "case_measure.sql"
    sql_file.write_text(
        """
CREATE OR REPLACE VIEW case_measure AS
SELECT
    year,
    CASE WHEN SUM(amount) > 150 THEN 1 ELSE 0 END AS MEASURE high_value
FROM case_data;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE case_data (year INT, amount INT)")
    layer.adapter.execute(
        """
INSERT INTO case_data VALUES
    (2022, 100), (2022, 50),
    (2023, 200), (2023, 100);
"""
    )

    load_from_directory(layer, tmp_path)

    result = layer.query(
        dimensions=["case_measure.year"],
        metrics=["case_measure.high_value"],
    )
    rows = fetch_dicts(result)

    assert {row["year"]: row["high_value"] for row in rows} == {2022: 0, 2023: 1}


def test_yardstick_count_distinct_executes_via_query(tmp_path):
    sql_file = tmp_path / "distinct_count.sql"
    sql_file.write_text(
        """
CREATE VIEW distinct_count_v AS
SELECT
    year,
    COUNT(DISTINCT region) AS MEASURE unique_regions
FROM dup_regions;
"""
    )

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE dup_regions (year INT, region TEXT)")
    layer.adapter.execute(
        """
INSERT INTO dup_regions VALUES
    (2023, 'US'), (2023, 'US'), (2023, 'EU'),
    (2024, 'US'), (2024, 'APAC'), (2024, 'APAC');
"""
    )

    load_from_directory(layer, tmp_path)

    result = layer.query(
        dimensions=["distinct_count_v.year"],
        metrics=["distinct_count_v.unique_regions"],
    )
    rows = fetch_dicts(result)

    assert {row["year"]: row["unique_regions"] for row in rows} == {2023: 2, 2024: 2}


def test_import_yardstick_with_postgres_dialect(tmp_path):
    """Postgres dialect: TRY_CAST becomes plain CAST, measure detection still works."""
    sql_file = tmp_path / "sales.sql"
    sql_file.write_text(
        """
CREATE VIEW sales_v AS
SELECT
    TRY_CAST(year_str AS INT) AS year,
    region,
    SUM(amount) AS MEASURE revenue,
    COUNT(*) AS MEASURE order_count
FROM sales;
"""
    )

    adapter = YardstickAdapter(dialect="postgres")
    graph = adapter.parse(sql_file)

    model = graph.models["sales_v"]
    assert model.table == "sales"

    # Postgres has no TRY_CAST; sqlglot downgrades it to CAST
    year_dim = model.get_dimension("year")
    assert year_dim is not None
    assert "CAST" in year_dim.sql
    assert "TRY_CAST" not in year_dim.sql

    revenue = model.get_metric("revenue")
    assert revenue is not None
    assert revenue.agg == "sum"
    assert revenue.sql == "amount"

    order_count = model.get_metric("order_count")
    assert order_count is not None
    assert order_count.agg == "count"


def test_bigquery_dialect_rewrites_types_and_functions(tmp_path):
    """BigQuery dialect: TEXT becomes STRING, TRY_CAST becomes SAFE_CAST."""
    sql_file = tmp_path / "bq.sql"
    sql_file.write_text(
        """
CREATE VIEW bq_v AS
SELECT
    CAST(category AS TEXT) AS category,
    TRY_CAST(score AS INT) AS score,
    SUM(amount) AS MEASURE revenue
FROM events;
"""
    )

    adapter = YardstickAdapter(dialect="bigquery")
    graph = adapter.parse(sql_file)

    model = graph.models["bq_v"]
    assert model.table == "events"

    category_dim = model.get_dimension("category")
    assert category_dim is not None
    # BigQuery uses STRING instead of TEXT
    assert "STRING" in category_dim.sql
    assert "TEXT" not in category_dim.sql

    score_dim = model.get_dimension("score")
    assert score_dim is not None
    # BigQuery uses SAFE_CAST instead of TRY_CAST, INT64 instead of INT
    assert "SAFE_CAST" in score_dim.sql
    assert "INT64" in score_dim.sql

    revenue = model.get_metric("revenue")
    assert revenue is not None
    assert revenue.agg == "sum"


def test_dialect_affects_filter_serialization(tmp_path):
    """Filter expressions inside FILTER (WHERE ...) are serialized in the target dialect."""
    sql_file = tmp_path / "filtered.sql"
    sql_file.write_text(
        """
CREATE VIEW filtered_v AS
SELECT
    region,
    SUM(amount) FILTER (WHERE TRY_CAST(flag AS INT) = 1) AS MEASURE flagged_revenue
FROM sales;
"""
    )

    # DuckDB keeps TRY_CAST
    duckdb_adapter = YardstickAdapter(dialect="duckdb")
    duckdb_model = duckdb_adapter.parse(sql_file).models["filtered_v"]
    duckdb_metric = duckdb_model.get_metric("flagged_revenue")
    assert duckdb_metric is not None
    assert duckdb_metric.filters is not None
    assert "TRY_CAST" in duckdb_metric.filters[0]

    # Postgres downgrades TRY_CAST to CAST in the filter
    pg_adapter = YardstickAdapter(dialect="postgres")
    pg_model = pg_adapter.parse(sql_file).models["filtered_v"]
    pg_metric = pg_model.get_metric("flagged_revenue")
    assert pg_metric is not None
    assert pg_metric.filters is not None
    assert "TRY_CAST" not in pg_metric.filters[0]
    assert "CAST" in pg_metric.filters[0]


def test_dialect_affects_base_relation_sql(tmp_path):
    """Complex base relations (JOIN/WHERE) are serialized in the target dialect."""
    sql_file = tmp_path / "joined.sql"
    sql_file.write_text(
        """
CREATE VIEW joined_v AS
SELECT
    a.region,
    SUM(a.amount) AS MEASURE revenue
FROM sales a
JOIN dim_region b ON a.region_id = b.id
WHERE TRY_CAST(a.active AS INT) = 1;
"""
    )

    duckdb_adapter = YardstickAdapter(dialect="duckdb")
    duckdb_model = duckdb_adapter.parse(sql_file).models["joined_v"]
    assert duckdb_model.sql is not None
    assert "TRY_CAST" in duckdb_model.sql

    pg_adapter = YardstickAdapter(dialect="postgres")
    pg_model = pg_adapter.parse(sql_file).models["joined_v"]
    assert pg_model.sql is not None
    assert "TRY_CAST" not in pg_model.sql
    assert "CAST" in pg_model.sql


def test_dialect_affects_view_sql_metadata(tmp_path):
    """The view_sql stored in metadata is serialized in the target dialect."""
    sql_file = tmp_path / "meta.sql"
    sql_file.write_text(
        """
CREATE VIEW meta_v AS
SELECT
    CAST(name AS TEXT) AS name,
    SUM(amount) AS MEASURE revenue
FROM items;
"""
    )

    bq_adapter = YardstickAdapter(dialect="bigquery")
    bq_model = bq_adapter.parse(sql_file).models["meta_v"]
    view_sql = bq_model.metadata["yardstick"]["view_sql"]
    # BigQuery serialization uses STRING not TEXT
    assert "STRING" in view_sql
    assert "TEXT" not in view_sql


def test_dialect_default_is_duckdb(tmp_path):
    """Adapter defaults to duckdb when no dialect is specified."""
    sql_file = tmp_path / "default.sql"
    sql_file.write_text(
        """
CREATE VIEW default_v AS
SELECT
    TRY_CAST(x AS INT) AS val,
    SUM(amount) AS MEASURE revenue
FROM t;
"""
    )

    default_adapter = YardstickAdapter()
    explicit_adapter = YardstickAdapter(dialect="duckdb")

    default_model = default_adapter.parse(sql_file).models["default_v"]
    explicit_model = explicit_adapter.parse(sql_file).models["default_v"]

    # Both should produce identical dimension SQL (TRY_CAST preserved)
    assert default_model.get_dimension("val").sql == explicit_model.get_dimension("val").sql
    assert "TRY_CAST" in default_model.get_dimension("val").sql
