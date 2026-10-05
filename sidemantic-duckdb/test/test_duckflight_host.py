# /// script
# dependencies = ["psycopg[binary]>=3.2,<4", "pytest"]
# ///
"""Exercise Sidemantic through the real DuckFlight extension's PostgreSQL listener."""

import hashlib
import json
import os
import secrets
import shutil
import socket
import subprocess
import time
from decimal import Decimal
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


@pytest.fixture
def postgres(tmp_path):
    required = ("DUCKFLIGHT_EXTENSION", "SIDEMANTIC_DUCKDB_EXTENSION")
    if any(not os.environ.get(name) for name in (*required, "SIDEMANTIC_DUCKDB_BINARY")):
        pytest.skip("Set SIDEMANTIC_DUCKDB_BINARY, SIDEMANTIC_DUCKDB_EXTENSION, and DUCKFLIGHT_EXTENSION")
    password = secrets.token_urlsafe(24)
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 10000).hex()
    config = tmp_path / "users.toml"
    config.write_text(f'[users.smoke]\npassword_hash = "{digest}"\nsalt = {list(salt)}\niterations = 10000\n')
    # Bind-check within the project's permitted local port range before starting.
    for port in range(5400, 5500):
        with socket.socket() as probe:
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                continue
            break
    else:
        pytest.fail("No available localhost port in 5400-5499")
    address = f"127.0.0.1:{port}"
    binary = Path(os.environ["SIDEMANTIC_DUCKDB_BINARY"]).resolve()
    assert binary.is_file(), binary
    setup = ""
    for name in required:
        path = Path(os.environ[name]).resolve()
        assert path.is_file(), path
        if name == "SIDEMANTIC_DUCKDB_EXTENSION":
            # Keep a concurrent native rebuild from replacing a loaded library.
            snapshot = tmp_path / "sidemantic.duckdb_extension"
            shutil.copyfile(path, snapshot)
            path = snapshot
        setup += f"load {literal(path)};\n"
    setup += f"select * from duckflight_pg_serve({literal(address)}, {literal(config)});\n"
    with (tmp_path / "host.log").open("w+") as log:
        host = subprocess.Popen(
            [str(binary), "-unsigned", "-no-init", "-bail", ":memory:"],
            stdin=subprocess.PIPE,
            stdout=log,
            stderr=log,
            text=True,
        )
        try:
            host.stdin.write(setup)
            host.stdin.flush()
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if host.poll() is not None:
                    log.seek(0)
                    pytest.fail(log.read())
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                pytest.fail("DuckFlight listener startup timed out")
            with psycopg.connect(
                host="127.0.0.1",
                port=port,
                user="smoke",
                password=password,
                dbname="memory",
                sslmode="disable",
                connect_timeout=10,
                autocommit=True,
            ) as connection:
                yield connection
        finally:
            if host.poll() is None:
                host.stdin.write(f"select * from duckflight_stop('pgwire', {literal(address)});\n.quit\n")
                host.stdin.flush()
                try:
                    host.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    host.kill()
                    host.wait()
            log.seek(0)
            assert host.returncode == 0, log.read()


@pytest.fixture
def orders(postgres):
    postgres.execute("create table orders(order_id integer, status varchar, amount decimal(10,2))")
    postgres.execute("insert into orders values (1, 'complete', 100), (2, 'complete', 50), (3, 'pending', 75)")
    postgres.execute("""
        MODEL orders FROM orders (
          primary key(order_id)
          status
          sum(amount) AS revenue
        )
    """)
    return postgres


def test_semantic_queries_and_postgres_metadata(orders):
    postgres = orders
    query = "select orders.status, orders.revenue from orders order by orders.status"
    for prepared in (False, True):
        result = postgres.execute(query, prepare=prepared)
        assert [column.name for column in result.description] == ["status", "revenue"]
        assert result.fetchall() == [("complete", Decimal("150.00")), ("pending", Decimal("75.00"))]
    assert postgres.execute(
        "select column_name from information_schema.columns where table_name = 'orders' order by ordinal_position"
    ).fetchall() == [("order_id",), ("status",), ("amount",)]


@pytest.mark.parametrize("prepared", [False, True])
def test_discovery_and_catalog_transactions(orders, prepared):
    metrics = orders.execute("SHOW SEMANTIC METRICS FROM orders", prepare=prepared)
    names = [column.name for column in metrics.description]
    rows = [dict(zip(names, row)) for row in metrics.fetchall()]
    assert [row["name"] for row in rows] == ["revenue"]
    dimensions = orders.execute("SHOW SEMANTIC DIMENSIONS FROM orders", prepare=prepared)
    names = [column.name for column in dimensions.description]
    assert [dict(zip(names, row))["name"] for row in dimensions.fetchall()] == ["status"]
    assert orders.execute("SHOW SEMANTIC RELATIONSHIPS FROM orders", prepare=prepared).fetchall() == []
    assert orders.execute("SHOW SEMANTIC SEGMENTS FROM orders", prepare=prepared).fetchall() == []
    assert orders.execute("DESCRIBE MODEL orders", prepare=prepared).fetchall()
    snapshot = orders.execute("EXPORT SEMANTIC CATALOG", prepare=prepared).fetchone()[0]
    assert isinstance(json.loads(snapshot), dict)
    orders.execute("begin")
    try:
        orders.execute("drop model orders")
        assert orders.execute("SHOW SEMANTIC MODELS").fetchall() == []
    finally:
        orders.execute("rollback")
    assert orders.execute("SHOW SEMANTIC MODELS").fetchall()
    orders.execute("drop model orders")
    orders.execute(f"IMPORT SEMANTIC CATALOG {literal(snapshot)}", prepare=prepared)
    assert orders.execute("select revenue from orders").fetchall() == [(Decimal("225.00"),)]


@pytest.mark.parametrize("prepared", [False, True])
def test_prefixless_bound_predicate_and_result_metadata(orders, prepared):
    result = orders.execute(
        "select orders.status, orders.revenue from orders where orders.status = %s",
        ("pending",),
        prepare=prepared,
    )
    assert [column.name for column in result.description] == ["status", "revenue"]
    assert [column.type_code for column in result.description] == [25, 1700]
    assert result.fetchall() == [("pending", Decimal("75.00"))]


def test_native_model_prepare_and_describe_do_not_mutate(orders):
    statement = b"""
        MODEL prepared_orders FROM orders (
          primary key(order_id)
          status
          sum(amount) AS revenue
        )
    """
    result = orders.pgconn.prepare(b"native_model", statement)
    assert result.status == psycopg.pq.ExecStatus.COMMAND_OK, result.error_message.decode()
    models = orders.execute("SHOW SEMANTIC MODELS")
    names = [column.name for column in models.description]
    assert "prepared_orders" not in [dict(zip(names, row))["name"] for row in models.fetchall()]
    result = orders.pgconn.exec_prepared(b"native_model", [])
    assert result.status in (psycopg.pq.ExecStatus.COMMAND_OK, psycopg.pq.ExecStatus.TUPLES_OK), (
        result.error_message.decode()
    )
    assert orders.execute("select revenue from prepared_orders").fetchall() == [(Decimal("225.00"),)]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v", "-o", "addopts="]))
