# /// script
# dependencies = ["pytest"]
# ///
"""Deferred catalog binding and live physical source changes."""

import pytest
from test_catalog_host import execute


def test_prepared_discovery_rebinds_physical_types_and_missing_sources():
    results = execute("""
        prepare field_types as select column_name, data_type from information_schema.columns
          where table_schema = 'semantic' and table_name = 'orders' order by ordinal_position;
        prepare metric_types as show metrics from orders;
        execute field_types;
        execute metric_types;
        begin;
        alter table raw_orders alter amount type double;
        execute field_types;
        execute metric_types;
        rollback;
        execute field_types;
        drop table raw_orders;
        execute field_types;
        select is_bound from duckdb_views() where schema_name = 'semantic' and view_name = 'orders';
        create table raw_orders(id integer, status varchar, amount bigint);
        execute field_types;
        select is_bound from duckdb_views() where schema_name = 'semantic' and view_name = 'orders';
    """)
    fields = [rows for rows in results if rows and "column_name" in rows[0]]
    assert [rows[1]["data_type"] for rows in fields] == ["DECIMAL(38,2)", "DOUBLE", "DECIMAL(38,2)", None, "HUGEINT"]
    metrics = [rows for rows in results if rows and "kind" in rows[0]]
    assert [rows[0]["data_type"] for rows in metrics] == ["DECIMAL(38,2)", "DOUBLE"]
    assert [rows[0]["is_bound"] for rows in results if rows and "is_bound" in rows[0]] == [False, True]


def test_discovery_projection_and_restrictions_preserve_results():
    results = execute("""
        select count(*) as n from duckdb_columns() where schema_name = 'semantic';
        select column_name from duckdb_columns()
          where schema_name = 'semantic' and table_name = 'orders' and column_name = 'revenue';
        select table_name, column_name, data_type from duckdb_columns()
          where schema_name = 'semantic' and (table_name = 'orders' or table_name = 'unrelated')
          order by table_name, column_index;
        select view_name from duckdb_views() where schema_name = 'semantic' and view_name = 'unrelated';
        select data_type from duckdb_columns()
          where schema_name = 'semantic' and table_name = 'orders' and data_type = 'VARCHAR';
        select count(*) as n from duckdb_columns()
          where schema_name = 'not_semantic' and table_name = 'orders';
    """)
    assert results == [
        [{"n": 4}],
        [{"column_name": "revenue"}],
        [
            {"table_name": "orders", "column_name": "status", "data_type": "VARCHAR"},
            {"table_name": "orders", "column_name": "revenue", "data_type": "DECIMAL(38,2)"},
            {"table_name": "unrelated", "column_name": "name", "data_type": None},
        ],
        [{"view_name": "unrelated"}],
        [{"data_type": "VARCHAR"}],
        [{"n": 0}],
    ]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v", "-o", "addopts="]))
