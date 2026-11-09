"""End-to-end tests for `sidemantic server postgres` with a real PostgreSQL client.

riffq's event loop blocks until the process is signalled, so each server runs in
a subprocess started through the CLI and psycopg talks to it on an ephemeral
port. The module is skipped when riffq or psycopg is not installed.
"""

import datetime
import socket
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path

import pytest

riffq = pytest.importorskip("riffq")
pytest.importorskip("pyarrow")
psycopg = pytest.importorskip("psycopg")

if not hasattr(riffq, "Server"):
    # Other test modules install a stub `riffq` when the real one is missing.
    pytest.skip("requires the real riffq package", allow_module_level=True)

REPO_ROOT = Path(__file__).resolve().parents[2]
STARTUP_TIMEOUT_SECONDS = 60

MODELS_YAML = """
models:
  - name: orders
    table: orders
    primary_key: id
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
    dimensions:
      - name: status
        type: categorical
      - name: created_at
        type: time
        granularity: day
    metrics:
      - name: order_count
        agg: count
      - name: revenue
        agg: sum
        sql: amount
      - name: discount_total
        agg: sum
        sql: discount
  - name: customers
    table: customers
    primary_key: id
    dimensions:
      - name: region
        type: categorical
    metrics:
      - name: customer_count
        agg: count
"""


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class PgServer:
    def __init__(self, port: int, user: str = "analyst", password: str = "any"):
        self.port = port
        self.user = user
        self.password = password

    def connect(self, **overrides):
        options = {
            "host": "127.0.0.1",
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "dbname": "sidemantic",
            "autocommit": True,
            "connect_timeout": 10,
        }
        options.update(overrides)
        return psycopg.connect(**options)


def _start_server(tmp_path: Path, arguments: list[str], **credentials):
    """Start the CLI server and wait until a client can connect."""
    port = _free_port()
    log_path = tmp_path / f"pg-server-{port}.log"
    command = [sys.executable, "-c", "from sidemantic.cli import app; app()", "server", "postgres"]
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [*command, *arguments, "--port", str(port)], cwd=REPO_ROOT, stdout=log, stderr=subprocess.STDOUT
        )
    server = PgServer(port, **credentials)
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    while True:
        if process.poll() is not None:
            pytest.fail(f"PostgreSQL server exited with {process.returncode} during startup:\n{log_path.read_text()}")
        try:
            server.connect(connect_timeout=2).close()
            return process, server
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                _stop_server(process)
                pytest.fail(f"PostgreSQL server did not accept connections:\n{log_path.read_text()}")
            time.sleep(0.1)


def _stop_server(process: subprocess.Popen) -> None:
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _write_project(root: Path) -> Path:
    """Write a project whose database has several tables per schema, like any real one."""
    import duckdb

    (root / "models").mkdir()
    (root / "models" / "models.yml").write_text(MODELS_YAML)
    database = duckdb.connect(str(root / "warehouse.duckdb"))
    database.execute("CREATE TABLE customers (id INTEGER, region VARCHAR)")
    database.execute("INSERT INTO customers VALUES (1, 'East'), (2, 'West')")
    database.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DECIMAL(10,2), discount DECIMAL(10,2), "
        "status VARCHAR, created_at TIMESTAMP)"
    )
    database.execute(
        "INSERT INTO orders VALUES "
        "(1, 1, 10.25, NULL, 'completed', '2026-01-05 10:30:00'), "
        "(2, 1, 20.50, NULL, 'completed', '2026-01-06 11:00:00'), "
        "(3, 2, 5.10, NULL, 'pending', '2026-02-01 09:15:00')"
    )
    database.execute("CREATE TABLE products (id INTEGER, name VARCHAR)")
    database.execute("CREATE SCHEMA staging")
    database.execute("CREATE TABLE staging.events (id INTEGER)")
    database.execute("CREATE TABLE staging.sessions (id INTEGER)")
    database.close()
    return root


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    project = _write_project(tmp_path_factory.mktemp("pg_wire_project"))
    process, server = _start_server(project, [str(project / "models"), "--db", str(project / "warehouse.duckdb")])
    yield server
    _stop_server(process)


@pytest.fixture(params=["simple", "extended"])
def connection(server, request):
    """A connection speaking the simple or the extended query protocol."""
    cursor_factory = psycopg.ClientCursor if request.param == "simple" else psycopg.Cursor
    with server.connect(cursor_factory=cursor_factory) as connection:
        yield connection


def test_starts_with_several_tables_per_schema(server):
    """riffq >= 0.4 refuses duplicate schemas; registering one per table crashed startup."""
    with server.connect() as connection:
        tables = connection.execute(
            "SELECT table_schema, table_name FROM information_schema.tables ORDER BY 1, 2"
        ).fetchall()
    assert tables == [
        ("main", "customers"),
        ("main", "orders"),
        ("main", "products"),
        ("semantic_layer", "customers"),
        ("semantic_layer", "orders"),
        ("staging", "events"),
        ("staging", "sessions"),
    ]


def test_semantic_query_returns_typed_values(connection):
    assert connection.execute("SELECT 1").fetchall() == [(1,)]

    rows = connection.execute(
        "SELECT orders.status, orders.revenue, orders.order_count, orders.discount_total "
        "FROM orders ORDER BY orders.status"
    ).fetchall()
    assert rows == [("completed", Decimal("30.75"), 2, None), ("pending", Decimal("5.10"), 1, None)]

    rows = connection.execute("SELECT orders.created_at__month, orders.revenue FROM orders ORDER BY 1").fetchall()
    assert rows == [(datetime.date(2026, 1, 1), Decimal("30.75")), (datetime.date(2026, 2, 1), Decimal("5.10"))]

    rows = connection.execute("SELECT customers.region, orders.revenue FROM orders ORDER BY 1").fetchall()
    assert rows == [("East", Decimal("30.75")), ("West", Decimal("5.10"))]

    rows = connection.execute("SELECT max(created_at), CAST(1.5 AS DECIMAL(10,2)) FROM main.orders").fetchall()
    assert rows == [(datetime.datetime(2026, 2, 1, 9, 15), Decimal("1.50"))]


def test_values_riffq_cannot_encode_arrive_as_text(connection):
    row = connection.execute(
        "SELECT CAST('12:34:56' AS TIME), CAST('x' AS ENUM('x', 'y')), {'a': 1}, CAST(NULL AS TIME)"
    ).fetchone()
    assert row == ("12:34:56", "x", '{"a": 1}', None)


@pytest.mark.parametrize(
    ("sql", "error"),
    [
        ("SELECT orders.no_such_metric FROM orders", "UndefinedColumn"),
        ("SELECT * FROM no_such_table", "UndefinedTable"),
        ("SELEC nonsense", "SyntaxError"),
    ],
)
def test_bad_queries_raise_postgres_errors(connection, sql, error):
    with pytest.raises(getattr(psycopg.errors, error)):
        connection.execute(sql)
    # The error is an ErrorResponse, not a dropped connection.
    assert connection.execute("SELECT 1").fetchall() == [(1,)]


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE pwn (x INTEGER)",
        "DROP TABLE orders",
        "INSERT INTO orders (id) VALUES (99)",
        "UPDATE orders SET status = 'x'",
        "DELETE FROM orders",
        "COPY orders TO 'orders.csv'",
        "WITH gone AS (DELETE FROM orders RETURNING *) SELECT * FROM gone",
    ],
)
def test_statements_that_change_the_database_are_rejected(connection, sql):
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        connection.execute(sql)
    assert connection.execute("SELECT count(*) FROM main.orders").fetchall() == [(3,)]
    assert connection.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'pwn'").fetchall() == [
        (0,)
    ]


def test_parameterized_queries(server):
    """Drivers and BI tools bind parameters through the extended protocol."""
    with server.connect() as connection:
        query = "SELECT orders.status, orders.order_count FROM orders WHERE orders.status = %s"
        assert connection.execute(query, ("completed",)).fetchall() == [("completed", 2)]
        assert connection.execute(query, ("pending",), prepare=True).fetchall() == [("pending", 1)]
        assert connection.execute(query, ("it's; --",)).fetchall() == []

        row = connection.execute(
            "SELECT %s::bigint, %s::double, %s::boolean, %s::varchar", (12345678, 1.5, True, None)
        ).fetchone()
        assert row == (12345678, 1.5, True, None)

        rows = connection.execute(
            "SELECT count(*) FROM main.orders WHERE created_at >= %s", (datetime.datetime(2026, 2, 1),)
        ).fetchall()
        assert rows == [(1,)]

        rows = connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY 1", ("semantic_layer",)
        ).fetchall()
        assert rows == [("customers",), ("orders",)]

        with pytest.raises(psycopg.errors.UndefinedColumn):
            connection.execute("SELECT orders.nope FROM orders WHERE orders.status = %s", ("completed",))
        assert connection.execute("SELECT 1").fetchall() == [(1,)]


def test_catalog_lists_semantic_models_with_columns(connection):
    columns = connection.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'semantic_layer' AND table_name = 'orders' ORDER BY ordinal_position"
    ).fetchall()
    assert columns == [
        ("status", "VARCHAR"),
        ("created_at", "TIMESTAMP"),
        ("order_count", "DOUBLE"),
        ("revenue", "DOUBLE"),
        ("discount_total", "DOUBLE"),
    ]

    # Requested columns, joins and filters are honored rather than answered with fixed rows.
    relations = connection.execute(
        "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE c.relkind = 'r' AND n.nspname = 'semantic_layer' ORDER BY 2"
    ).fetchall()
    assert relations == [("semantic_layer", "customers"), ("semantic_layer", "orders")]

    # JDBC drivers list columns by joining pg_attribute to pg_type on the type OID.
    attributes = connection.execute(
        "SELECT a.attname, t.typname, pg_catalog.format_type(a.atttypid, a.atttypmod) "
        "FROM pg_catalog.pg_attribute a "
        "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_catalog.pg_type t ON t.oid = a.atttypid "
        "WHERE n.nspname = 'semantic_layer' AND c.relname = 'orders' AND a.attnum > 0 ORDER BY a.attnum"
    ).fetchall()
    assert attributes == [
        ("status", "varchar", "character varying"),
        ("created_at", "timestamp", "timestamp without time zone"),
        ("order_count", "float8", "double precision"),
        ("revenue", "float8", "double precision"),
        ("discount_total", "float8", "double precision"),
    ]

    assert connection.execute("SELECT current_database()").fetchall() == [("sidemantic",)]
    assert connection.execute("SELECT pg_catalog.version()").fetchone()[0].startswith("PostgreSQL ")


def test_session_statements_from_drivers(connection):
    for statement in ("SET extra_float_digits = 3", "BEGIN", "COMMIT", "ROLLBACK", "DISCARD ALL"):
        connection.execute(statement)
    assert connection.execute("SHOW transaction isolation level").fetchall() == [("read committed",)]
    assert connection.execute("SHOW server_version").fetchone()[0][0].isdigit()


def test_client_with_implicit_transactions(server):
    """psycopg's default mode sends BEGIN before the first statement."""
    with server.connect(autocommit=False) as connection:
        assert connection.execute("SELECT orders.order_count FROM orders").fetchall() == [(3,)]
        connection.commit()
        assert connection.execute("SELECT 1").fetchall() == [(1,)]


def test_password_authentication(tmp_path):
    # A server holds its DuckDB file open, so this one gets its own project.
    project = _write_project(tmp_path)
    password_file = tmp_path / "pg-password"
    password_file.write_text("s3cret\n")
    process, server = _start_server(
        tmp_path,
        [
            str(project / "models"),
            "--db",
            str(project / "warehouse.duckdb"),
            "--username",
            "analyst",
            "--password-file",
            str(password_file),
        ],
        user="analyst",
        password="s3cret",
    )
    try:
        with server.connect() as connection:
            assert connection.execute("SELECT orders.order_count FROM orders").fetchall() == [(3,)]
        with pytest.raises(psycopg.OperationalError):
            server.connect(password="wrong")
        with pytest.raises(psycopg.OperationalError):
            server.connect(user="intruder")
    finally:
        _stop_server(process)


def test_demo_mode_starts(tmp_path):
    pytest.importorskip("lkml")  # The bundled demo includes a LookML model.
    process, server = _start_server(tmp_path, ["--demo"])
    try:
        with server.connect() as connection:
            models = connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'semantic_layer' ORDER BY 1"
            ).fetchall()
            assert ("orders",) in models
            assert connection.execute("SELECT orders.order_count FROM orders").fetchone()[0] > 0
    finally:
        _stop_server(process)
