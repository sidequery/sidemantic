# /// script
# dependencies = ["pytest"]
# ///
"""Named semantic selections retain measures across grain and catalog changes."""

import os
from decimal import Decimal

import pytest
from test_catalog_host import execute as execute_host
from test_catalog_host import literal


def execute(*args, **kwargs):
    results = execute_host(*args, **kwargs)
    for rows in results:
        for row in rows:
            for key in ("gross", "paid", "aov", "revenue", "total"):
                if isinstance(row.get(key), str):
                    row[key] = Decimal(row[key])
    return results


DEFINITIONS = """
models:
  - name: orders
    table: raw_orders
    primary_key: id
    dimensions:
      - {name: status, type: categorical}
      - {name: country, type: categorical}
    metrics:
      - {name: revenue, agg: sum, sql: amount}
      - {name: paid, agg: sum, sql: amount, filters: ["status = 'paid'"]}
      - {name: rows, agg: count}
      - {name: average, type: ratio, numerator: revenue, denominator: rows}
"""

SETUP = f"""
create table raw_orders as select * from (values
    (1, 'paid', 'US', 10), (2, 'pending', 'US', 30), (3, 'paid', 'CA', 80)
) t(id, status, country, amount);
select * from sqlctx_load({literal(DEFINITIONS)});
create semantic view sales as select country, revenue as gross, paid, average as aov from orders;
"""


def test_named_view_regrains_ratios_and_filtered_metrics():
    rows = execute(
        """
        select gross, paid, aov from sales;
        select country, gross, paid, aov from sales order by country;
        select country, aov from semantic.sales where country = 'US';
        select gross as total from sales order by total;
        """,
        setup=SETUP,
    )
    assert rows[0] == [{"gross": 120, "paid": 90, "aov": 40}]
    assert rows[1] == [
        {"country": "CA", "gross": 80, "paid": 80, "aov": 80},
        {"country": "US", "gross": 40, "paid": 10, "aov": 20},
    ]
    assert rows[2] == [{"country": "US", "aov": 20}]
    assert rows[3] == [{"total": 120}]


def test_named_view_nested_selection_and_star():
    rows = execute(
        """
        create semantic view report as select country as region, gross as total, aov from sales;
        select total, aov from report;
        select * from report order by region;
        with sales as (select 7 as gross) select gross from sales;
        """,
        setup=SETUP,
    )
    assert rows[-3] == [{"total": 120, "aov": 40}]
    assert rows[-2] == [
        {"region": "CA", "total": 80, "aov": 80},
        {"region": "US", "total": 40, "aov": 20},
    ]
    assert rows[-1] == [{"gross": 7}]


def test_view_aliases_keep_the_grain_and_bind_in_scalar_subqueries():
    rows = execute(
        """
        create semantic view report as select country as region, gross from sales;
        select gross as region from report order by region;
        select (select gross from report) as total from main.raw_orders;
        """,
        setup=SETUP,
    )
    assert [int(row["region"]) for row in rows[-2]] == [120]
    assert rows[-1] == [{"total": 120}] * 3


def test_view_wildcards_preserve_selection_order_and_filtered_ratio():
    rows = execute(
        """
        create metric orders.paid_average (type ratio, numerator paid, denominator rows);
        create semantic view paid_report as select paid_average as aov, country, paid from orders;
        create semantic view copied_report as select * from paid_report;
        select aov, paid from copied_report;
        select * from copied_report order by country;
        """,
        setup=SETUP,
    )
    assert rows[-2] == [{"aov": 30, "paid": 90}]
    assert list(rows[-1][0]) == ["aov", "country", "paid"]
    assert rows[-1] == [
        {"aov": 80, "country": "CA", "paid": 80},
        {"aov": 5, "country": "US", "paid": 10},
    ]


def test_prepared_view_query_rebinds_and_rollback_restores_definition():
    rows = execute(
        """
        prepare total as select gross from sales;
        execute total;
        begin;
        create or replace metric orders.revenue as sum(amount * 2);
        execute total;
        rollback;
        execute total;
        begin;
        create or replace semantic view sales as select paid as gross from orders;
        execute total;
        rollback;
        execute total;
        """,
        setup=SETUP,
    )
    assert [result for result in rows if result and "gross" in result[0]] == [
        [{"gross": 120}],
        [{"gross": 240}],
        [{"gross": 120}],
        [{"gross": 90}],
        [{"gross": 120}],
    ]


def test_view_reopens_and_failed_replacement_preserves_catalog(tmp_path):
    database = tmp_path / "views.duckdb"
    execute("select gross from sales;", setup=SETUP, database=database)
    assert execute("select aov from sales;", setup="", database=database) == [[{"aov": 40}]]
    execute(
        "create or replace model orders (table raw_orders);",
        setup="",
        database=database,
        error="semantic view 'sales' dependency",
    )
    assert execute("select gross from sales;", setup="", database=database) == [[{"gross": 120}]]


@pytest.mark.parametrize(
    "sql",
    [
        "drop model orders;",
        "drop metric orders.revenue;",
        "drop dimension orders.country;",
        "create semantic view nested as select gross from sales; drop semantic view sales;",
    ],
)
def test_views_restrict_dependency_removal(sql):
    execute(sql, setup=SETUP, error="RESTRICT")


def test_view_ddl_is_preparable_transactional_and_distinct_from_sql_views():
    rows = execute(
        """
        prepare define_view as create semantic view prepared_sales as select revenue from orders;
        begin;
        execute define_view;
        select revenue from prepared_sales;
        rollback;
        execute define_view;
        select revenue from prepared_sales;
        drop semantic view prepared_sales;
        drop semantic view if exists prepared_sales;
        create view ordinary as select gross from sales;
        select gross from ordinary;
        drop view ordinary;
        """,
        setup=SETUP,
    )
    assert [result for result in rows if result and "revenue" in result[0]] == [
        [{"revenue": 120}],
        [{"revenue": 120}],
    ]
    assert rows[-1] == [{"gross": 120}]


def test_view_visibility_across_live_connections_and_reader_transactions(tmp_path):
    duckdb = pytest.importorskip("duckdb", reason="requires the matching DuckDB Python host")
    database = str(tmp_path / "view-connections.duckdb")
    with duckdb.connect(database, config={"allow_unsigned_extensions": "true"}) as writer:
        writer.execute(f"load {literal(os.environ['SQLCTX_DUCKDB_EXTENSION'])}")
        writer.execute(SETUP)
        with writer.cursor() as reader:
            reader.execute("prepare totals as select gross from sales")
            reader.execute("begin")
            assert reader.execute("execute totals").fetchall() == [(120,)]
            writer.execute("begin")
            writer.execute("create or replace semantic view sales as select paid as gross from orders")
            assert writer.execute("select gross from sales").fetchall() == [(90,)]
            assert reader.execute("execute totals").fetchall() == [(120,)]
            writer.execute("rollback")
            assert writer.execute("select gross from sales").fetchall() == [(120,)]
            writer.execute("create or replace semantic view sales as select paid as gross from orders")
            # A committed writer does not replace an existing reader snapshot.
            assert reader.execute("execute totals").fetchall() == [(120,)]
            reader.execute("commit")
            assert reader.execute("execute totals").fetchall() == [(90,)]
            writer.execute("create or replace metric orders.paid as sum(amount * 2)")
            assert reader.execute("execute totals").fetchall() == [(240,)]
