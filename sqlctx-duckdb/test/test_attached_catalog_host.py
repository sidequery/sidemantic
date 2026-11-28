# /// script
# dependencies = ["pytest"]
# ///
"""Attached semantic catalogs use their own definitions and physical sources."""

import pytest
from test_catalog_host import execute, literal

FINANCE_SETUP = """
create table sales(id integer, region varchar, amount integer);
insert into sales values (1, 'west', 10), (2, 'west', 30), (3, 'east', 5);
MODEL sales FROM sales (
  primary key(id)
  region
  sum(amount) AS revenue
);
"""

ATTACHED_SETUP = (
    """
attach ':memory:' as finance;
use finance;
"""
    + FINANCE_SETUP
    + """
use memory;
create table sales(id integer, region varchar, amount integer);
insert into sales values (1, 'current', 999);
"""
)


def test_attached_catalog_qualification_aliases_and_source_ownership():
    results = execute(
        """
        select revenue from finance.semantic.sales;
        select finance.semantic.sales.revenue from finance.semantic.sales;
        select s.region, s.revenue from finance.semantic.sales s order by s.region;
        select * from finance.semantic.sales where region = 'west';
        with sales as (select 123 as revenue) select revenue from finance.semantic.sales;
        select amount from memory.main.sales;
        use finance;
        select revenue from finance.semantic.sales;
        """,
        setup=ATTACHED_SETUP,
    )
    assert results == [
        [{"revenue": "45"}],
        [{"revenue": "45"}],
        [{"region": "east", "revenue": "5"}, {"region": "west", "revenue": "40"}],
        [{"region": "west", "revenue": "40"}],
        [{"revenue": "45"}],
        [{"amount": 999}],
        [{"revenue": "45"}],
    ]


def test_attached_catalog_prepared_definition_and_data_rollback():
    results = execute(
        """
        prepare revenue_by_region as select s.revenue from finance.semantic.sales s where s.region = $1;
        execute revenue_by_region('west');
        begin;
        use finance;
        insert into sales values (4, 'west', 20);
        create or replace metric sales.revenue as count(*);
        use memory;
        execute revenue_by_region('west');
        rollback;
        execute revenue_by_region('west');
        """,
        setup=ATTACHED_SETUP,
    )
    totals = [int(rows[0]["revenue"]) for rows in results if rows and "revenue" in rows[0]]
    assert totals == [40, 3, 40]


def test_same_model_in_multiple_catalogs_and_metadata():
    results = execute(
        """
        select f.revenue as finance_revenue, m.revenue as current_revenue
        from finance.semantic.sales f cross join memory.semantic.sales m;
        select table_catalog, table_name from information_schema.tables
        where table_schema = 'semantic' order by table_catalog, table_name;
        select table_catalog, column_name, data_type from information_schema.columns
        where table_schema = 'semantic' and column_name = 'revenue' order by table_catalog;
        select database_name, view_name from duckdb_views()
        where schema_name = 'semantic' order by database_name, view_name;
        """,
        setup=ATTACHED_SETUP
        + """
        MODEL sales FROM sales (
          primary key(id)
          region
          count(*) AS revenue
        );
        """,
    )
    assert results == [
        [{"finance_revenue": "45", "current_revenue": 1}],
        [{"table_catalog": "finance", "table_name": "sales"}, {"table_catalog": "memory", "table_name": "sales"}],
        [
            {"table_catalog": "finance", "column_name": "revenue", "data_type": "HUGEINT"},
            {"table_catalog": "memory", "column_name": "revenue", "data_type": "BIGINT"},
        ],
        [{"database_name": "finance", "view_name": "sales"}, {"database_name": "memory", "view_name": "sales"}],
    ]


def test_attached_physical_semantic_namespace_collision_is_catalog_local():
    results = execute(
        """
        create schema semantic;
        create table semantic.sales(revenue integer);
        insert into semantic.sales values (777);
        select revenue from memory.semantic.sales;
        select revenue from finance.semantic.sales;
        begin;
        create schema finance.semantic;
        create table finance.semantic.sales(revenue integer);
        insert into finance.semantic.sales values (888);
        select revenue from finance.semantic.sales;
        rollback;
        select revenue from finance.semantic.sales;
        """,
        setup=ATTACHED_SETUP,
    )
    assert [int(rows[0]["revenue"]) for rows in results if rows and "revenue" in rows[0]] == [777, 45, 888, 45]


def test_attached_catalog_readonly_reopen(tmp_path):
    database = tmp_path / "finance.duckdb"
    execute("select revenue from semantic.sales;", setup=FINANCE_SETUP, database=database)
    results = execute(
        """
        select revenue from finance.semantic.sales;
        select table_catalog, column_name from information_schema.columns
        where table_catalog = 'finance' and table_schema = 'semantic' order by ordinal_position;
        """,
        setup=f"attach {literal(str(database))} as finance (read_only);",
    )
    assert results == [
        [{"revenue": "45"}],
        [{"table_catalog": "finance", "column_name": "region"}, {"table_catalog": "finance", "column_name": "revenue"}],
    ]


def test_attached_catalog_sql_sources_keep_ctes_and_explicit_catalogs():
    definitions = """
models:
  - name: cte_sales
    sql: "with sales as (select * from sales) select * from sales"
    primary_key: id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
  - name: explicitly_current
    table: memory.main.sales
    primary_key: id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"""
    results = execute(
        """
        select revenue from finance.semantic.cte_sales;
        select revenue from finance.semantic.explicitly_current;
        """,
        setup=ATTACHED_SETUP + f"use finance; select * from sqlctx_load({literal(definitions)}); use memory;",
    )
    assert results == [[{"revenue": "45"}], [{"revenue": "999"}]]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v", "-o", "addopts="]))
