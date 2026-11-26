# /// script
# dependencies = ["pytest"]
# ///
"""Exercise semantic catalog SQL through each built DuckDB shell."""

import hashlib
import json
import os
import re
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
select * from sqlctx_load({literal(DEFINITIONS)});
"""


def execute(sql, *, setup=SETUP, database=":memory:", readonly=False, error=None):
    binary = Path(os.environ["SQLCTX_DUCKDB_BINARY"]).resolve()
    extension = Path(os.environ["SQLCTX_DUCKDB_EXTENSION"]).resolve()
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


def test_default_shell_grammar_composes_with_semantic_statements():
    settings = execute("select value from duckdb_settings() where name = 'active_grammar_extensions';")[0]
    if settings != [{"value": "[shell]"}]:
        pytest.skip("Host does not enable the DuckDB 2.0 shell grammar by default")
    before, expression, metrics, after = execute("""
        select current_setting('active_grammar_extensions') as grammars;
        40 + 2 as answer;
        show metrics from orders;
        select current_setting('active_grammar_extensions') as grammars;
    """)
    assert expression == [{"answer": 42}]
    assert [row["qualified_name"] for row in metrics] == ["orders.revenue"]
    assert before == after == [{"grammars": ["shell"]}]


@pytest.mark.parametrize(
    ("sql", "error"),
    [
        ("show metrics from missing;", "model 'missing' not found"),
        ("show dimensions for orders.missing;", "metric 'orders.missing' not found"),
        ("select orders.revenue, unrelated.name from orders;", "sqlctx"),
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


def test_legacy_catalog_snapshot_is_read_and_mutations_write_sqlctx_snapshot(tmp_path):
    database = tmp_path / "legacy.duckdb"
    [[row]] = execute("select __sqlctx_catalog() as payload;", database=database)
    execute(
        f"create macro __sidemantic_catalog() as {literal(row['payload'])}; drop macro __sqlctx_catalog;",
        setup="",
        database=database,
    )
    query = "select orders.revenue from semantic.orders;"
    [[row]] = execute(query, setup="", database=database, readonly=True)
    assert Decimal(str(row["revenue"])) == Decimal("45")

    execute("create or replace metric orders.revenue as count(*);", setup="", database=database)
    [[row]] = execute(query, setup="", database=database, readonly=True)
    assert row["revenue"] == 3


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
    setup = f"create table raw_numbers({fields}); select * from sqlctx_load({literal(definitions)});"
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


def test_prefixless_metrics_namespace_respects_duckdb_tables_and_ctes():
    totals = execute("select orders.revenue from metrics;")
    assert totals == [[{"revenue": "45.00"}]]
    rows = execute("""
        create temp table metrics as select 7 as revenue;
        select revenue from metrics;
        with metrics as (select 9 as revenue) select revenue from metrics;
        select revenue from metrics union all select orders.revenue from orders order by revenue;
    """)
    assert rows[-3:] == [[{"revenue": 7}], [{"revenue": 9}], [{"revenue": "7.00"}, {"revenue": "45.00"}]]


TPCDS_SETUP = """
create schema public;
create table public.store_sales(
    ss_item_sk integer, ss_ticket_number integer, ss_customer_sk integer,
    ss_store_sk integer, ss_sold_date_sk integer, ss_quantity integer,
    ss_sales_price decimal(10,2), ss_ext_sales_price decimal(10,2), ss_net_profit decimal(10,2)
);
insert into public.store_sales values
    (1, 100, 1, 1, 1, 2, 10, 20, 4),
    (1, 101, 1, 1, 1, 3, 10, 30, 6),
    (2, 102, 2, 2, 2, 1, 15, 15, -2);
create table public.item(
    i_item_sk integer, i_item_id varchar, i_item_desc varchar,
    i_brand varchar, i_category varchar, i_current_price decimal(10,2)
);
insert into public.item values
    (1, 'A', 'First item', 'Acme', 'Home', 10),
    (2, 'B', 'Second item', 'Beta', 'Garden', 15);
create table public.customer(
    c_customer_sk integer, c_customer_id varchar, c_first_name varchar,
    c_last_name varchar, c_email_address varchar
);
insert into public.customer values
    (1, 'C1', 'Ada', 'Lovelace', 'ada@example.test'),
    (2, 'C2', 'Grace', 'Hopper', 'grace@example.test');
create table public.store(
    s_store_sk integer, s_store_id varchar, s_store_name varchar,
    s_city varchar, s_state varchar, s_number_employees integer
);
insert into public.store values (1, 'S1', 'North', 'Seattle', 'WA', 5), (2, 'S2', 'South', 'Portland', 'OR', 3);
create table public.date_dim(
    d_date_sk integer, d_date date, d_year integer, d_quarter_name varchar,
    d_month_name varchar, d_moy integer
);
insert into public.date_dim values
    (1, '2026-01-01', 2026, '2026Q1', 'January', 1),
    (2, '2026-02-01', 2026, '2026Q1', 'February', 2);
"""


@pytest.mark.parametrize(
    ("pin", "checksum", "dataset_metric"),
    [
        ("faf5810", "1c8c8e1a41d6b1b4d957ae5934b883b46ca07ad3901927ec6ce0ecff55d2cb6b", False),
        ("d27a6be", "72a98b17eda7daa6bcb777125ea44b1cbbb7788bdd6167430e0f58ea7e06a1ce", True),
    ],
    ids=["upstream-0.1.1", "upstream-0.3.0.dev0"],
)
def test_upstream_ossie_file_metrics_and_reopen(tmp_path, pin, checksum, dataset_metric):
    fixture = (
        Path(__file__).resolve().parents[2]
        / "tests/ossie-fixtures/upstream"
        / pin
        / "examples/tpcds_semantic_model.yaml"
    )
    # Keep the upstream bytes unchanged, including physical source catalog names.
    assert hashlib.sha256(fixture.read_bytes()).hexdigest() == checksum
    database = tmp_path / "tpcds.duckdb"
    query = """
        select total_sales, total_profit from metrics;
        select item.i_brand, sales_by_brand from metrics order by item.i_brand;
        select total_sales from metrics where store_sales.ss_quantity > 1;
        select customer_lifetime_value, store_productivity from metrics;
    """
    if dataset_metric:
        query += "select agg(s.total_quantity) as total_quantity from store_sales s; show metrics from store_sales;"
        documentation = (Path(__file__).resolve().parents[2] / "docs/duckdb-extension.md").read_text()
        window_section = documentation.split("#### Windowed metrics in plain SQL", 1)[1].split(
            "### Native SQL definitions", 1
        )[0]
        window_queries = re.findall(r"```sql\n(.*?)```", window_section, re.DOTALL)
        assert len(window_queries) == 3
        query += "\n".join(window_queries)
    results = execute(
        query,
        setup=TPCDS_SETUP + f"select * from sqlctx_load_file({literal(str(fixture))});",
        database=database,
    )
    assert results[0] == [{"total_sales": "65.00", "total_profit": "8.00"}]
    assert results[1] == [
        {"i_brand": "Acme", "sales_by_brand": "50.00"},
        {"i_brand": "Beta", "sales_by_brand": "15.00"},
    ]
    assert results[2] == [{"total_sales": "50.00"}]
    assert results[3] == [{"customer_lifetime_value": 32.5, "store_productivity": 8.125}]
    if dataset_metric:
        assert results[4] == [{"total_quantity": "6"}]
        assert [row["qualified_name"] for row in results[5]] == ["store_sales.total_quantity"]
        assert results[6] == [
            {"sale_date": "2026-01-01", "cumulative_sales": "50.00"},
            {"sale_date": "2026-02-01", "cumulative_sales": "65.00"},
        ]
        assert results[7] == [
            {"store_id": 1, "brand": "Acme", "brand_rank_in_store": 1},
            {"store_id": 2, "brand": "Beta", "brand_rank_in_store": 1},
        ]
        assert results[8] == [
            {"sale_year": 2026, "sale_month": 1, "monthly_sales_change": None},
            {"sale_year": 2026, "sale_month": 2, "monthly_sales_change": "-35.00"},
        ]
        for metric in ["cumulative_sales", "brand_rank_in_store", "monthly_sales_change"]:
            execute(f"select {metric} from metrics;", setup="", database=database, error="E_DEFERRED_WINDOWED_METRIC")
    # Reopen with no definitions loaded and no write permission: both physical
    # data and the imported semantic catalog must survive the shell process.
    assert execute(query, setup="", database=database, readonly=True) == results


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v", "-o", "addopts="]))
