"""Exercise the actual Python client and shell against matching host artifacts."""

import os
import subprocess
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

SETUP = """
create table orders(id integer, status varchar, amount integer, revenue integer);
insert into orders values (1, 'paid', 10, 999), (2, 'paid', 30, 999), (3, 'pending', 5, 999);
MODEL orders FROM orders (
  primary key(id)
  status
  sum(amount) AS revenue
  sum(amount) AS semantic_total
);
"""


@pytest.fixture
def connection():
    extension = Path(os.environ["SQLCTX_DUCKDB_EXTENSION"]).resolve()
    with duckdb.connect(config={"allow_unsigned_extensions": "true", "threads": "2"}) as con:
        con.execute("load '" + str(extension).replace("'", "''") + "'")
        con.execute(SETUP)
        yield con


@pytest.mark.parametrize("source", ["orders", "semantic.orders"])
def test_sql_relation_fetch_show_and_composition(connection, source, capfd):
    relation = connection.sql(f"select status, semantic_total from {source} order by status")
    assert relation.columns == ["status", "semantic_total"]
    assert relation.fetchall() == [("paid", 40), ("pending", 5)]
    relation.show()
    output = capfd.readouterr().out
    assert "semantic_total" in output and "paid" in output and "40" in output
    assert relation.filter("status = 'paid'").project("semantic_total * 2 as doubled").fetchall() == [(80,)]


def test_physical_shadowing_and_native_scope(connection):
    assert connection.sql("select status, revenue from orders order by id").fetchall() == [
        ("paid", 999),
        ("paid", 999),
        ("pending", 999),
    ]
    assert connection.sql("select revenue from semantic.orders").fetchall() == [(45,)]
    assert connection.sql("select main.orders.amount from orders order by id").fetchall() == [(10,), (30,), (5,)]
    assert connection.sql(
        "with orders as (select 7 as semantic_total) select semantic_total from orders"
    ).fetchall() == [(7,)]
    assert connection.sql("select o.semantic_total as total from semantic.orders o").fetchall() == [(45,)]
    with pytest.raises(duckdb.Error, match="both table"):
        connection.sql("select amount, semantic_total from orders").fetchall()


def test_parameters_prepare_views_and_transaction_snapshots(connection):
    query = "select semantic_total from orders where status = $status"
    assert connection.sql(query, params={"status": "paid"}).fetchall() == [(40,)]
    connection.execute("prepare totals as select semantic_total from orders where status = $1")
    connection.execute("begin")
    connection.execute("create or replace view selected_totals as select semantic_total from orders")
    assert connection.sql("select * from selected_totals").fetchall() == [(45,)]
    connection.execute("create or replace metric orders.semantic_total as count(*)")
    assert connection.execute("execute totals('paid')").fetchall() == [(2,)]
    assert connection.sql(query, params={"status": "paid"}).fetchall() == [(2,)]
    connection.execute("rollback")
    assert connection.execute("execute totals('paid')").fetchall() == [(40,)]
    assert connection.sql(query, params={"status": "paid"}).fetchall() == [(40,)]
    assert connection.execute("select count(*) from duckdb_views() where view_name = 'selected_totals'").fetchone() == (
        0,
    )


def test_ordinary_queries_keep_values_types_aliases_and_order(connection):
    query = """
        with picked as (select id, status, amount from orders where amount > 5)
        select p.id as order_id, p.status, p.amount::decimal(8, 2) as amount,
               (select max(amount) from orders) as largest
        from picked p order by p.id desc limit 1
    """
    assert connection.execute(query).fetchall() == [(2, "paid", 30, 30)]
    relation = connection.sql(query)
    assert relation.columns == ["order_id", "status", "amount", "largest"]
    assert relation.fetchall() == [(2, "paid", 30, 30)]
    assert str(relation.types[2]) == "DECIMAL(8,2)"
    result = connection.execute("select 1 as x, 2 as x from orders limit 1")
    assert [column[0] for column in result.description] == ["x", "x"]
    assert result.fetchall() == [(1, 2)]
    query = "select * from orders a cross join orders b where a.id = 1 and b.id = 2"
    expected_names = ["id", "status", "amount", "revenue"] * 2
    result = connection.execute(query)
    assert [column[0] for column in result.description] == expected_names
    assert result.fetchall() == [(1, "paid", 10, 999, 2, "paid", 30, 999)]
    assert connection.sql(query).columns == expected_names
    result = connection.execute("select unnest({'x': 1, 'y': 2}), 3 as x from orders limit 1")
    assert [column[0] for column in result.description] == ["x", "y", "x"]
    assert result.fetchall() == [(1, 2, 3)]


def test_relation_created_view_survives_reopen(tmp_path):
    database = tmp_path / "client.duckdb"
    extension = str(Path(os.environ["SQLCTX_DUCKDB_EXTENSION"]).resolve()).replace("'", "''")
    for initialize in (True, False):
        with duckdb.connect(str(database), config={"allow_unsigned_extensions": "true", "threads": "2"}) as con:
            con.execute(f"load '{extension}'")
            if initialize:
                con.execute(SETUP)
                con.sql("select status, semantic_total from semantic.orders").create_view("saved_totals", replace=True)
                con.execute("set integer_division = true")
                con.sql("select id, amount / 3 as quotient from orders").create_view("saved_quotients", replace=True)
                con.execute("set preserve_identifier_case = false")
                con.sql("select amount as MiXeD from orders").create_view("saved_case", replace=True)
            assert con.sql("select * from saved_totals order by status").fetchall() == [("paid", 40), ("pending", 5)]
            assert con.sql("select quotient from saved_quotients order by id").fetchall() == [(3,), (10,), (1,)]
            assert con.sql("select * from saved_case").columns == ["mixed"]


def test_private_carrier_cannot_discard_user_predicates_or_decode_arbitrary_data(connection):
    assert connection.execute("select 7 as kept from __sqlctx_bind_query('select 1 as x') where false").fetchall() == []
    assert connection.execute("select 7 as kept from __sqlctx_bind_query('select 1 as x')").fetchall() == [(7,)]
    assert connection.execute("select count(*) from __sqlctx_bind_query('show models')").fetchone() == (1,)
    for sql in [
        "select * from __sqlctx_bind_query()",
        "select * from __sqlctx_bind_query(NULL)",
        "select * from __sqlctx_bind_query('not valid SQL')",
        "select * from __sqlctx_bind_query('drop table orders')",
        "select * from __sqlctx_bind_query('select 1; drop table orders')",
        "select * from __sqlctx_bind_query('select 1', 'invalid settings')",
        "select * from __sqlctx_bind_query('select 1', NULL)",
        "select * from orders, __sqlctx_bind_query(status)",
    ]:
        with pytest.raises(duckdb.Error) as error:
            connection.execute(sql)
        assert "INTERNAL" not in str(error.value)
    assert connection.execute("select count(*) from orders").fetchone() == (3,)


def test_agent_shell_create_or_replace_view_in_transaction():
    binary = Path(os.environ["SQLCTX_DUCKDB_BINARY"]).resolve()
    extension = Path(os.environ["SQLCTX_DUCKDB_EXTENSION"]).resolve()
    version = subprocess.run([str(binary), "--version"], capture_output=True, text=True, check=True).stdout
    if not version.startswith("v2."):
        pytest.skip("Agent mode is provided by the DuckDB 2.x shell")
    sql = (
        f"load '{str(extension).replace(chr(39), chr(39) * 2)}';\n"
        + SETUP
        + """
        begin;
        create or replace view selected_totals as select semantic_total from orders;
        select * from selected_totals;
        rollback;
        select semantic_total from semantic.orders;
    """
    )
    result = subprocess.run(
        [str(binary), "-unsigned", "-no-init", "-agent", "-bail", ":memory:"],
        input=sql,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "45" in result.stdout
    assert "INTERNAL" not in result.stderr
