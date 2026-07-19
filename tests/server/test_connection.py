"""Tests for PostgreSQL wire protocol connection handling."""

import threading
import time

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer


def test_handle_auth():
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:")

    conn = SemanticLayerConnection(connection_id=1, executor=None, layer=layer, username="user", password="pass")

    auth_results = []

    def callback(result):
        auth_results.append(result)

    conn.handle_auth("user", "pass", "localhost", callback=callback)
    conn.handle_auth("user", "wrong", "localhost", callback=callback)

    assert auth_results == [True, False]


def test_handle_auth_partial_config_fails_closed():
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:")
    conn = SemanticLayerConnection(connection_id=1, executor=None, layer=layer, username="user", password=None)

    auth_results = []

    def callback(result):
        auth_results.append(result)

    conn.handle_auth("user", "anything", "localhost", callback=callback)

    assert auth_results == [False]


def test_handle_system_queries():
    pytest.importorskip("riffq")
    pa = pytest.importorskip("pyarrow")
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", sql="status", type="categorical")],
            metrics=[Metric(name="order_count", agg="count")],
        )
    )

    layer.conn.execute("CREATE TABLE orders (id INTEGER, status VARCHAR)")

    captured = {}

    def send_reader(reader, callback):
        captured["reader"] = reader
        callback(True)

    conn = SemanticLayerConnection(connection_id=1, executor=None, layer=layer)
    conn.send_reader = send_reader

    cursor = layer.adapter.cursor()

    assert (
        conn._try_handle_system_query(
            "SELECT * FROM information_schema.tables",
            "select * from information_schema.tables",
            lambda *_: None,
            cursor,
        )
        is True
    )

    table = captured["reader"].read_all()
    rows = table.to_pylist()
    assert any(r["table_schema"] == "semantic_layer" for r in rows)

    captured.clear()
    assert (
        conn._try_handle_system_query(
            "SELECT * FROM pg_catalog.pg_namespace",
            "select * from pg_catalog.pg_namespace",
            lambda *_: None,
            cursor,
        )
        is True
    )
    assert isinstance(captured["reader"], pa.RecordBatchReader)


def test_dml_passthrough():
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:")

    captured = {}

    def send_reader(reader, callback):
        captured["reader"] = reader
        callback(True)

    conn = SemanticLayerConnection(connection_id=1, executor=None, layer=layer)
    conn.send_reader = send_reader

    conn._handle_query("SET search_path TO public", lambda *_: None)

    assert "reader" in captured


@pytest.mark.parametrize("statement", ["SHOW TABLES", "SHOW TABLES;"])
def test_show_passthrough_is_not_wrapped_as_select(statement):
    from tests.optional_dep_stubs import ensure_fake_riffq

    ensure_fake_riffq()
    pytest.importorskip("pyarrow")
    from unittest.mock import MagicMock

    from sidemantic.server.connection import SemanticLayerConnection

    reader = MagicMock()
    cursor = MagicMock()
    cursor.execute.return_value.fetch_record_batch.return_value = reader
    layer = MagicMock()
    layer.adapter.cursor.return_value = cursor

    conn = SemanticLayerConnection.__new__(SemanticLayerConnection)
    conn.layer = layer
    conn.user_attrs_map = {}
    conn.session_user = None
    conn.send_reader = MagicMock()
    callback = MagicMock()

    conn._handle_query(statement, callback)

    cursor.execute.assert_called_once_with("SHOW TABLES")
    conn.send_reader.assert_called_once_with(reader, callback)


@pytest.mark.parametrize("statement", ["BEGIN", "BEGIN TRANSACTION;", "COMMIT", "ROLLBACK WORK"])
def test_transaction_control_is_not_wrapped_as_select(statement):
    from tests.optional_dep_stubs import ensure_fake_riffq

    ensure_fake_riffq()
    pytest.importorskip("pyarrow")
    from unittest.mock import MagicMock

    from sidemantic.server.connection import SemanticLayerConnection

    layer = MagicMock()

    conn = SemanticLayerConnection.__new__(SemanticLayerConnection)
    conn.layer = layer
    conn.user_attrs_map = {}
    conn.session_user = None
    conn.send_reader = MagicMock()
    callback = MagicMock()

    conn._handle_query(statement, callback)

    layer.adapter.cursor.assert_not_called()
    reader, sent_callback = conn.send_reader.call_args.args
    assert reader.read_all().num_rows == 0
    assert sent_callback is callback


def test_pg_wire_timeout_returns_before_uncancellable_worker_finishes(monkeypatch):
    from tests.optional_dep_stubs import ensure_fake_riffq

    ensure_fake_riffq()
    pytest.importorskip("pyarrow")
    from unittest.mock import MagicMock

    from sidemantic.core.query_telemetry import QueryTelemetry
    from sidemantic.db.base import CancellationOutcome
    from sidemantic.server.connection import SemanticLayerConnection
    from sidemantic.server.query_execution import QueryAdmission, QueryLimits

    worker_started = threading.Event()
    release_worker = threading.Event()

    def blocking_execute(*args, **kwargs):
        kwargs["control"].register(args[0].adapter, kwargs["cursor"])
        worker_started.set()
        assert release_worker.wait(timeout=2)
        raise RuntimeError("worker released")

    monkeypatch.setattr("sidemantic.server.connection.execute_bounded", blocking_execute)
    monkeypatch.setattr("sidemantic.server.connection.QueryRewriter.rewrite", lambda *args, **kwargs: "SELECT 1")

    cursor = MagicMock()
    layer = MagicMock()
    layer.adapter.cursor.return_value = cursor
    layer.adapter.cancel.return_value = CancellationOutcome(
        supported=False,
        cancelled=False,
        diagnostic="test adapter cannot cancel an in-flight query; warehouse work may continue",
    )
    layer.graph.models = {}
    layer.dialect = "duckdb"
    layer.query_telemetry = QueryTelemetry()

    conn = SemanticLayerConnection.__new__(SemanticLayerConnection)
    conn.layer = layer
    conn.user_attrs_map = {}
    conn.session_user = None
    conn.connection_id = 1
    conn.query_limits = QueryLimits(execution_timeout_seconds=0.05)
    conn.query_admission = QueryAdmission(1, 0)
    conn._active_controls = set()
    conn._active_controls_lock = threading.Lock()

    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError, match="warehouse work may continue"):
            conn._handle_query("SELECT 1", MagicMock())
        assert worker_started.is_set()
        assert time.monotonic() - started < 0.5
    finally:
        release_worker.set()

    deadline = time.monotonic() + 1
    while conn.query_admission.stats()["active"] and time.monotonic() < deadline:
        time.sleep(0.001)
    assert conn.query_admission.stats()["active"] == 0
    cursor.close.assert_called_once()


def test_query_error_raises_exception():
    """_handle_query should raise on errors, not return error as data rows."""
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from unittest.mock import MagicMock

    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.server.connection import SemanticLayerConnection

    mock_layer = MagicMock()
    # Query work now runs on a per-request cursor obtained from the adapter.
    mock_layer.adapter.cursor.return_value.execute.side_effect = Exception("test error")
    mock_layer.graph = SemanticGraph()
    mock_layer.dialect = "duckdb"

    conn = SemanticLayerConnection.__new__(SemanticLayerConnection)
    conn.layer = mock_layer

    callback = MagicMock()

    with pytest.raises(Exception, match="test error"):
        conn._handle_query("SELECT invalid_col FROM nonexistent", callback)

    callback.assert_not_called()


def test_user_attributes_lookup_by_session_user():
    """The connecting username is mapped to its security user attributes."""
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:")
    attrs_map = {"alice": {"tenant_id": 1}, "bob": {"tenant_id": 2}}
    conn = SemanticLayerConnection(connection_id=1, executor=None, layer=layer, user_attrs_map=attrs_map)

    # Before auth, no session user -> None.
    assert conn._user_attributes() is None

    conn.handle_auth("alice", "", "localhost", callback=lambda _r: None)
    assert conn._user_attributes() == {"tenant_id": 1}

    conn.handle_auth("bob", "", "localhost", callback=lambda _r: None)
    assert conn._user_attributes() == {"tenant_id": 2}

    # Unknown user with a configured map -> None (deny-by-default upstream).
    conn.handle_auth("mallory", "", "localhost", callback=lambda _r: None)
    assert conn._user_attributes() is None


def test_enforce_pg_access_denies_secured_model_without_attrs():
    """A secured model touched with no user attributes is denied."""
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")

    from sidemantic import SecurityPolicy
    from sidemantic.core.semantic_layer import SecurityError
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", sql="status", type="categorical")],
            metrics=[Metric(name="order_count", agg="count")],
            security=SecurityPolicy(access="{{ user.role == 'admin' }}"),
        )
    )
    conn = SemanticLayerConnection(
        connection_id=1, executor=None, layer=layer, user_attrs_map={"alice": {"role": "admin"}}
    )

    # No attributes for a secured model -> denied.
    with pytest.raises(SecurityError):
        conn._enforce_pg_access("SELECT * FROM orders", None)

    # Non-admin -> access gate falsy -> denied.
    with pytest.raises(SecurityError):
        conn._enforce_pg_access("SELECT * FROM orders", {"role": "viewer"})

    # Admin -> allowed (no raise).
    conn._enforce_pg_access("SELECT * FROM orders", {"role": "admin"})

    # A query not touching the secured model is unaffected.
    conn._enforce_pg_access("SELECT 1", None)
