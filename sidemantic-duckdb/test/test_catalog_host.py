# /// script
# dependencies = ["pytest"]
# ///
"""Exercise semantic catalog SQL through each built DuckDB shell."""

import json
import os
import subprocess
from decimal import Decimal
from pathlib import Path

import pytest


def literal(value):
    return "'" + value.replace("'", "''") + "'"


DEFINITIONS = """
models:
  - name: orders
    table: raw_orders
    primary_key: id
    label: Orders
    description: Order facts
    dimensions:
      - name: status
        type: categorical
        description: Fulfilment status
    metrics:
      - name: revenue
        agg: sum
        sql: amount
        label: Revenue
        description: Gross revenue
    segments:
      - name: completed
        sql: status = 'complete'
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
  - name: customers
    table: raw_customers
    primary_key: id
    dimensions:
      - name: country
        type: categorical
  - name: unrelated
    table: raw_unrelated
    primary_key: id
    dimensions:
      - name: name
        type: categorical
parameters:
  - name: region
    type: string
    default_value: US
metadata:
  owner: finance
"""

SETUP = f"""
create table raw_orders(id integer, customer_id integer, status varchar, amount decimal(10,2));
insert into raw_orders values (1, 1, 'complete', 10), (2, 1, 'complete', 30), (3, 2, 'pending', 5);
create table raw_customers(id integer, country varchar);
insert into raw_customers values (1, 'US'), (2, 'CA');
select * from sidemantic_load({literal(DEFINITIONS)});
"""


def execute(sql, *, setup=SETUP, database=":memory:", readonly=False, error=None):
    binary = Path(os.environ["SIDEMANTIC_DUCKDB_BINARY"]).resolve()
    extension = Path(os.environ["SIDEMANTIC_DUCKDB_EXTENSION"]).resolve()
    args = [str(binary), "-unsigned", "-no-init", "-json", "-bail"]
    if readonly:
        args.append("-readonly")
    args.append(str(database))
    result = subprocess.run(
        args,
        input=f".output /dev/null\nset threads = 2;\nload {literal(str(extension))};\n{setup}\n.output stdout\n{sql}\n",
        text=True,
        capture_output=True,
        timeout=30,
    )
    if error:
        assert result.returncode != 0, result.stdout
        assert "INTERNAL Error" not in result.stderr, result.stderr
        assert error in result.stderr, result.stderr
        return []
    assert result.returncode == 0, result.stderr
    decoder = json.JSONDecoder()
    results = []
    remaining = result.stdout.strip()
    while remaining:
        rows, end = decoder.raw_decode(remaining)
        results.append(rows)
        remaining = remaining[end:].strip()
    return results


def test_catalog_discovery_and_select_combinations():
    models, metrics, dimensions, relationships, segments, description, compatible, rows = execute("""
        show models;
        show metrics from ORDERS;
        show dimensions from orders;
        show relationships from orders;
        show segments from orders;
        describe model orders;
        show dimensions for ORDERS.REVENUE;
        select customers.country, orders.revenue from orders order by customers.country;
    """)
    assert {row["name"] for row in models} == {"orders", "customers", "unrelated"}
    metric = metrics[0]
    assert (metric["qualified_name"], metric["label"], metric["description"]) == (
        "orders.revenue",
        "Revenue",
        "Gross revenue",
    )
    assert metric["aggregation"] == "sum"
    assert metric["is_public"] is True
    assert json.loads(metric["definition"])["sql"] == "amount"
    assert dimensions[0]["description"] == "Fulfilment status"
    assert dimensions[0]["type"] == "categorical"
    assert relationships[0]["target_model"] == "customers"
    assert relationships[0]["relationship_type"] == "many_to_one"
    assert segments[0]["sql"] == "status = 'complete'"
    assert {row["kind"] for row in description} == {"model", "metric", "dimension", "relationship", "segment"}
    assert [row["qualified_name"] for row in compatible] == ["customers.country", "orders.status"]
    assert [(row["country"], Decimal(str(row["revenue"]))) for row in rows] == [
        ("CA", Decimal("5.00")),
        ("US", Decimal("40.00")),
    ]


@pytest.mark.parametrize(
    ("sql", "error"),
    [
        ("show metrics from missing;", "model 'missing' not found"),
        ("show dimensions for orders.missing;", "metric 'orders.missing' not found"),
        ("select orders.revenue, unrelated.name from orders;", "Sidemantic"),
        ("import semantic catalog '{\"version\":99}';", "unsupported catalog snapshot version"),
    ],
)
def test_invalid_catalog_requests_surface_errors(sql, error):
    execute(sql, error=error)


def test_prepared_catalog_reads_and_rollback():
    results = execute(
        """
        prepare inspect_metrics as show metrics from orders;
        execute inspect_metrics;
        begin;
        create metric orders.order_count as count(*);
        execute inspect_metrics;
        rollback;
        execute inspect_metrics;
        """,
        setup=SETUP + "explain drop metric orders.revenue;",
    )
    metric_results = [rows for rows in results if rows and rows[0].get("kind") == "metric"]
    assert [[row["name"] for row in rows] for rows in metric_results] == [
        ["revenue"],
        ["order_count", "revenue"],
        ["revenue"],
    ]


def test_export_import_roundtrip_and_readonly(tmp_path):
    source = tmp_path / "source.duckdb"
    snapshot = execute("export semantic catalog;", database=source)[0][0]["definition"]
    decoded = json.loads(snapshot)
    assert decoded["version"] == 1
    assert decoded["metadata"]["owner"] == "finance"
    assert decoded["parameters"][0]["name"] == "region"
    destination = tmp_path / "destination.duckdb"
    imported = execute(
        "export semantic catalog;",
        setup=f"import semantic catalog {literal(snapshot)};",
        database=destination,
    )[0][0]["definition"]
    assert json.loads(imported) == decoded
    restarted = execute("export semantic catalog;", setup="", database=destination, readonly=True)
    assert json.loads(restarted[0][0]["definition"]) == decoded
    execute("drop metric orders.revenue;", setup="", database=destination, readonly=True, error="read-only")


def test_prefixless_physical_collision_and_prepared_catalog_changes():
    results = execute(
        """
        select status, revenue from orders order by id;
        select semantic_total from orders where status = 'paid';
        prepare by_status as select semantic_total from orders where status = $1;
        execute by_status('paid');
        begin;
        create or replace metric orders.semantic_total as count(*);
        execute by_status('paid');
        rollback;
        execute by_status('paid');
        """,
        setup="""
        create table orders(id integer, status varchar, amount integer, revenue integer);
        insert into orders values (1, 'paid', 10, 999), (2, 'paid', 30, 999), (3, 'pending', 5, 999);
        MODEL orders FROM orders (
          primary key(id)
          status
          sum(amount) AS revenue
          sum(amount) AS semantic_total
        );
        """,
    )
    # Columns of the same-name table keep their native meaning.
    assert [(row["status"], int(row["revenue"])) for row in results[0]] == [
        ("paid", 999),
        ("paid", 999),
        ("pending", 999),
    ]
    assert int(results[1][0]["semantic_total"]) == 40
    totals = [int(rows[0]["semantic_total"]) for rows in results[2:] if rows and "semantic_total" in rows[0]]
    assert totals == [40, 2, 40]


def test_standard_catalog_discovery():
    tables, columns, attributes, schema, rows, stars = execute("""
        select table_name, table_type from information_schema.tables
        where table_schema = 'semantic' order by table_name;
        select column_name, data_type, numeric_precision, numeric_scale
        from information_schema.columns where table_schema = 'semantic' and table_name = 'orders'
        order by ordinal_position;
        select c.relname, a.attname, a.atttypid, a.attnum
        from pg_catalog.pg_class c join pg_catalog.pg_namespace n on c.relnamespace = n.oid
        join pg_catalog.pg_attribute a on c.oid = a.attrelid
        where n.nspname = 'semantic' and c.relname = 'orders' order by a.attnum;
        select schema_name from information_schema.schemata where schema_name = 'semantic';
        select status, revenue from semantic.orders order by status;
        select * from semantic.orders order by status;
    """)
    assert tables == [{"table_name": name, "table_type": "VIEW"} for name in ("customers", "orders", "unrelated")]
    assert columns == [
        {"column_name": "status", "data_type": "VARCHAR", "numeric_precision": None, "numeric_scale": None},
        {"column_name": "revenue", "data_type": "DECIMAL(38,2)", "numeric_precision": 38, "numeric_scale": 2},
    ]
    assert [(row["attname"], row["attnum"]) for row in attributes] == [("status", 1), ("revenue", 2)]
    assert all(row["atttypid"] is not None for row in attributes)
    assert schema == [{"schema_name": "semantic"}]
    assert [(row["status"], Decimal(str(row["revenue"]))) for row in rows] == [
        ("complete", Decimal("40")),
        ("pending", Decimal("5")),
    ]
    assert list(stars[0]) == [column["column_name"] for column in columns]
    assert stars == rows


def test_standard_catalog_subset_counts_unknown_types_and_prepared_changes():
    results = execute("""
        select count(*) as n from information_schema.tables where table_schema = 'semantic';
        select column_name, data_type from information_schema.columns
        where table_schema = 'semantic' and table_name = 'unrelated';
        select is_bound from duckdb_views() where schema_name = 'semantic' and view_name = 'unrelated';
        prepare inspect_fields as select column_name, data_type from information_schema.columns
          where table_schema = 'semantic' and table_name = 'orders' order by ordinal_position;
        execute inspect_fields;
        begin;
        create metric orders.order_count as count(*);
        execute inspect_fields;
        create or replace metric orders.revenue as count(*);
        execute inspect_fields;
        rollback;
        execute inspect_fields;
    """)
    assert results[0] == [{"n": 3}]
    assert results[1] == [{"column_name": "name", "data_type": None}]
    assert results[2] == [{"is_bound": False}]
    fields = [rows for rows in results[3:] if rows and "column_name" in rows[0]]
    assert [[row["column_name"] for row in rows] for rows in fields] == [
        ["status", "revenue"],
        ["status", "revenue", "order_count"],
        ["status", "revenue", "order_count"],
        ["status", "revenue"],
    ]
    assert next(row["data_type"] for row in fields[2] if row["column_name"] == "revenue") == "BIGINT"
    assert fields[0] == fields[-1]
    assert execute("""
        select a.attname, a.atttypid from pg_catalog.pg_attribute a
        join pg_catalog.pg_class c on a.attrelid = c.oid
        join pg_catalog.pg_namespace n on c.relnamespace = n.oid
        where n.nspname = 'semantic' and c.relname = 'unrelated';
    """)[0] == [{"attname": "name", "atttypid": None}]


def test_standard_catalog_physical_namespace_collision_and_rollback():
    results = execute("""
        create schema semantic;
        select count(*) as n from information_schema.schemata where schema_name = 'semantic';
        begin;
        create table semantic.orders(marker integer);
        insert into semantic.orders values (123);
        select column_name from information_schema.columns
          where table_schema = 'semantic' and table_name = 'orders';
        select * from semantic.orders;
        select table_type from information_schema.tables
          where table_schema = 'semantic' and table_name = 'orders';
        rollback;
        select column_name from information_schema.columns
          where table_schema = 'semantic' and table_name = 'orders' order by ordinal_position;
        select revenue from semantic.orders;
    """)
    assert results[:-1] == [
        [{"n": 1}],
        [{"column_name": "marker"}],
        [{"marker": 123}],
        [{"table_type": "BASE TABLE"}],
        [{"column_name": "status"}, {"column_name": "revenue"}],
    ]
    assert Decimal(str(results[-1][0]["revenue"])) == Decimal("45")


def test_standard_catalog_persistent_oids_and_readonly(tmp_path):
    database = tmp_path / "metadata.duckdb"
    sql = """
        select c.oid, c.relname, n.oid as namespace_oid from pg_catalog.pg_class c
        join pg_catalog.pg_namespace n on c.relnamespace = n.oid
        where n.nspname = 'semantic' order by c.relname;
    """
    first = execute(sql, database=database)
    second = execute(sql, setup="", database=database, readonly=True)
    assert first == second
    assert len(first[0]) == 3
    assert all(0 < row["oid"] <= 2**31 - 1 and 0 < row["namespace_oid"] <= 2**31 - 1 for row in first[0])


def test_semantic_namespace_qualification_and_cte_shadowing():
    results = execute("""
        select semantic.orders.revenue from semantic.orders;
        with orders as (select 999 as revenue) select revenue from semantic.orders;
        with orders as (select 999 as revenue) select revenue from orders;
        select o.revenue from semantic.orders o;
    """)
    assert [Decimal(str(rows[0]["revenue"])) for rows in results] == [45, 45, 999, 45]
    execute("select wrong_db.semantic.orders.revenue from semantic.orders;", error="Semantic column qualifier")
    execute("select wrong_schema.orders.revenue from semantic.orders;", error="Column qualifier does not match")


def test_physical_column_qualifiers_preserve_native_resolution():
    results = execute(
        """
        select main.orders.amount from orders order by id;
        select main.orders.status from orders order by id;
    """,
        setup="""
        create table orders(id integer, status varchar, amount integer);
        insert into orders values (1, 'paid', 10), (2, 'paid', 30), (3, 'pending', 5);
        MODEL orders FROM orders (
          primary key(id)
          status
          sum(amount) AS revenue
        );
    """,
    )
    assert results == [
        [{"amount": 10}, {"amount": 30}, {"amount": 5}],
        [{"status": "paid"}, {"status": "paid"}, {"status": "pending"}],
    ]


def test_standard_catalog_numeric_metadata_matches_host():
    columns = [
        ("u8", "utinyint"),
        ("u16", "usmallint"),
        ("u32", "uinteger"),
        ("u64", "ubigint"),
        ("u128", "uhugeint"),
        ("i8", "tinyint"),
        ("i16", "smallint"),
        ("i32", "integer"),
        ("i64", "bigint"),
        ("i128", "hugeint"),
        ("f32", "float"),
        ("f64", "double"),
        ("dec", "decimal(12,3)"),
    ]
    fields = ", ".join(f"{name} {kind}" for name, kind in columns)
    dimensions = "\n".join(f"    - name: {name}\n      type: numeric" for name, _ in columns)
    definitions = f"models:\n- name: numbers\n  table: raw_numbers\n  primary_key: i32\n  dimensions:\n{dimensions}\n"
    setup = f"create table raw_numbers({fields}); select * from sidemantic_load({literal(definitions)});"
    native, semantic = execute(
        """
        select column_name, data_type, numeric_precision, numeric_precision_radix, numeric_scale
        from information_schema.columns where table_schema = 'main' and table_name = 'raw_numbers'
        order by column_name;
        select column_name, data_type, numeric_precision, numeric_precision_radix, numeric_scale
        from information_schema.columns where table_schema = 'semantic' and table_name = 'numbers'
        order by column_name;
    """,
        setup=setup,
    )
    assert native == semantic


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v", "-o", "addopts="]))
