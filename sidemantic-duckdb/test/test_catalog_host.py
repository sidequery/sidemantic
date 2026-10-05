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
        input=f".output /dev/null\nload {literal(str(extension))};\n{setup}\n.output stdout\n{sql}\n",
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
        select status, revenue from orders order by status;
        select semantic_total from orders where status = 'paid';
        prepare by_status as select revenue from orders where status = $1;
        execute by_status('paid');
        begin;
        create or replace metric orders.revenue as count(*);
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
    assert [(row["status"], int(row["revenue"])) for row in results[0]] == [("paid", 40), ("pending", 5)]
    assert int(results[1][0]["semantic_total"]) == 40
    totals = [int(rows[0]["revenue"]) for rows in results[2:] if rows and "revenue" in rows[0]]
    assert totals == [40, 2, 40]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v", "-o", "addopts="]))
