"""Integration tests for the HTTP API server."""

# ruff: noqa: E402

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("fastapi")
pa = pytest.importorskip("pyarrow")
pytest.importorskip("httpx")

import duckdb
from fastapi.testclient import TestClient

from sidemantic import Dimension, Metric, Model, SemanticLayer, load_from_directory
from sidemantic.api_server import create_app
from sidemantic.db.base import BaseDatabaseAdapter
from sidemantic.server.common import ARROW_STREAM_MEDIA_TYPE


def _write_models(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "models.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: status
        sql: status
        type: categorical
      - name: created_at
        sql: created_at
        type: time
        granularity: day
    metrics:
      - name: order_count
        agg: count
      - name: total_amount
        agg: sum
        sql: amount
"""
    )


def _build_test_client(tmp_path: Path, auth_token: str | None = "secret", max_body_bytes: int = 1024 * 1024):
    models_dir = tmp_path / "models"
    _write_models(models_dir)

    db_path = tmp_path / "warehouse.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        create table orders (
            id integer,
            status varchar,
            amount double,
            created_at timestamp
        )
        """
    )
    conn.executemany(
        "insert into orders values (?, ?, ?, ?)",
        [
            (1, "completed", 10.0, "2024-01-01 10:00:00"),
            (2, "completed", 20.0, "2024-01-02 11:00:00"),
            (3, "pending", 5.0, "2024-01-03 12:00:00"),
        ],
    )
    conn.close()

    layer = SemanticLayer(connection=f"duckdb:///{db_path}", auto_register=False)
    load_from_directory(layer, str(models_dir))
    app = create_app(layer, auth_token=auth_token, max_request_body_bytes=max_body_bytes)
    return TestClient(app)


def _auth_headers(token: str = "secret") -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_health_requires_auth(tmp_path):
    client = _build_test_client(tmp_path)

    response = client.get("/health")

    assert response.status_code == 401


def test_readyz_is_public(tmp_path):
    client = _build_test_client(tmp_path)

    response = client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_and_models_endpoints(tmp_path):
    client = _build_test_client(tmp_path)

    health_response = client.get("/health", headers=_auth_headers())
    models_response = client.get("/models", headers=_auth_headers())
    graph_response = client.get("/graph", headers=_auth_headers())

    assert health_response.status_code == 200
    assert health_response.json()["dialect"] == "duckdb"

    assert models_response.status_code == 200
    assert models_response.json()[0]["name"] == "orders"

    assert graph_response.status_code == 200
    assert graph_response.json()["models"][0]["name"] == "orders"


def test_compile_and_query_json_endpoints(tmp_path):
    client = _build_test_client(tmp_path)

    compile_response = client.post(
        "/compile",
        headers=_auth_headers(),
        json={
            "dimensions": ["orders.status"],
            "metrics": ["orders.total_amount"],
            "order_by": ["orders.status"],
        },
    )
    query_response = client.post(
        "/query",
        headers=_auth_headers(),
        json={
            "dimensions": ["orders.status"],
            "metrics": ["orders.total_amount", "orders.order_count"],
            "order_by": ["orders.status"],
        },
    )

    assert compile_response.status_code == 200
    assert "sum" in compile_response.json()["sql"].lower()

    assert query_response.status_code == 200
    payload = query_response.json()
    assert payload["row_count"] == 2
    assert payload["rows"] == [
        {"status": "completed", "total_amount": 30.0, "order_count": 2},
        {"status": "pending", "total_amount": 5.0, "order_count": 1},
    ]


def test_query_arrow_endpoint(tmp_path):
    client = _build_test_client(tmp_path)

    response = client.post(
        "/query?format=arrow",
        headers=_auth_headers(),
        json={
            "dimensions": ["orders.status"],
            "metrics": ["orders.total_amount"],
            "order_by": ["orders.status"],
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(ARROW_STREAM_MEDIA_TYPE)
    table = pa.ipc.open_stream(BytesIO(response.content)).read_all()
    assert table.to_pylist() == [
        {"status": "completed", "total_amount": 30.0},
        {"status": "pending", "total_amount": 5.0},
    ]


def test_query_arrow_respects_accept_header(tmp_path):
    client = _build_test_client(tmp_path)

    response = client.post(
        "/query",
        headers={**_auth_headers(), "Accept": ARROW_STREAM_MEDIA_TYPE},
        json={"metrics": ["orders.order_count"]},
    )

    assert response.status_code == 200
    table = pa.ipc.open_stream(BytesIO(response.content)).read_all()
    assert table.to_pylist() == [{"order_count": 3}]


def test_sql_compile_and_sql_query_endpoints(tmp_path):
    client = _build_test_client(tmp_path)

    compile_response = client.post(
        "/sql/compile",
        headers=_auth_headers(),
        json={"query": "SELECT status, total_amount FROM orders ORDER BY status"},
    )
    query_response = client.post(
        "/sql",
        headers=_auth_headers(),
        json={"query": "SELECT status, total_amount FROM orders ORDER BY status"},
    )
    arrow_response = client.post(
        "/sql?format=arrow",
        headers=_auth_headers(),
        json={"query": "SELECT status, total_amount FROM orders ORDER BY status"},
    )

    assert compile_response.status_code == 200
    assert "sum" in compile_response.json()["sql"].lower()

    assert query_response.status_code == 200
    assert query_response.json()["rows"] == [
        {"status": "completed", "total_amount": 30.0},
        {"status": "pending", "total_amount": 5.0},
    ]

    table = pa.ipc.open_stream(BytesIO(arrow_response.content)).read_all()
    assert table.to_pylist() == [
        {"status": "completed", "total_amount": 30.0},
        {"status": "pending", "total_amount": 5.0},
    ]


def test_invalid_filter_returns_400(tmp_path):
    client = _build_test_client(tmp_path)

    response = client.post(
        "/query",
        headers=_auth_headers(),
        json={
            "metrics": ["orders.order_count"],
            "where": "1=1; DROP TABLE orders",
        },
    )

    assert response.status_code == 400
    assert "disallowed sql" in response.json()["error"].lower()


def test_request_size_limit_returns_413(tmp_path):
    client = _build_test_client(tmp_path, max_body_bytes=16)

    response = client.post(
        "/query",
        headers=_auth_headers(),
        json={"metrics": ["orders.order_count"]},
    )

    assert response.status_code == 413


class _ArrowOnlyResult:
    def __init__(self):
        self._table = pa.table({"order_count": [7]})
        self.description = [("order_count", None)]

    def fetch_record_batch(self):
        return pa.RecordBatchReader.from_batches(self._table.schema, self._table.to_batches())

    def fetchall(self):
        raise AssertionError("HTTP API should use Arrow readers, not fetchall()")


class _ArrowOnlyAdapter(BaseDatabaseAdapter):
    def execute(self, sql: str) -> Any:
        self.last_sql = sql
        return _ArrowOnlyResult()

    def executemany(self, sql: str, params: list) -> Any:
        raise NotImplementedError

    def fetchone(self, result: Any) -> tuple | None:
        return result.fetchone()

    def fetch_record_batch(self, result: Any) -> Any:
        return result.fetch_record_batch()

    def get_tables(self) -> list[dict]:
        return []

    def get_columns(self, table_name: str, schema: str | None = None) -> list[dict]:
        return []

    def close(self) -> None:
        return None

    @property
    def dialect(self) -> str:
        return "duckdb"

    @property
    def raw_connection(self) -> Any:
        return None


def test_sql_with_semicolon_in_string_literal(tmp_path):
    client = _build_test_client(tmp_path)

    response = client.post(
        "/sql/compile",
        headers=_auth_headers(),
        json={"query": "SELECT order_count FROM orders WHERE status = ';'"},
    )

    assert response.status_code == 200, response.json()
    assert "sql" in response.json()


def test_sql_multi_statement_rejected(tmp_path):
    client = _build_test_client(tmp_path)

    response = client.post(
        "/sql/compile",
        headers=_auth_headers(),
        json={"query": "SELECT 1; DROP TABLE orders"},
    )

    assert response.status_code == 400
    assert "multiple" in response.json()["error"].lower()


def test_raw_select_returns_results(tmp_path):
    client = _build_test_client(tmp_path)

    response = client.post(
        "/raw",
        headers=_auth_headers(),
        json={"query": "SELECT 1 AS n"},
    )

    assert response.status_code == 200
    assert response.json()["rows"] == [{"n": 1}]


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 1 AS n UNION SELECT 2 AS n",
        "SELECT 1 AS n UNION ALL SELECT 2 AS n",
        "SELECT 1 AS n INTERSECT SELECT 1 AS n",
        "SELECT 1 AS n EXCEPT SELECT 2 AS n",
        "(SELECT 1 AS n)",
    ],
)
def test_raw_allows_set_operations(tmp_path, query):
    client = _build_test_client(tmp_path)

    response = client.post(
        "/raw",
        headers=_auth_headers(),
        json={"query": query},
    )

    assert response.status_code == 200
    assert len(response.json()["rows"]) > 0


@pytest.mark.parametrize(
    "stmt",
    [
        "INSERT INTO orders VALUES (99, 'x', 1.0, '2024-01-01')",
        "UPDATE orders SET status = 'x'",
        "DELETE FROM orders",
        "DROP TABLE orders",
        "CREATE TABLE hack (id INT)",
    ],
)
def test_raw_rejects_non_select(tmp_path, stmt):
    client = _build_test_client(tmp_path)

    response = client.post(
        "/raw",
        headers=_auth_headers(),
        json={"query": stmt},
    )

    assert response.status_code == 400
    assert "only select" in response.json()["error"].lower()


def test_raw_rejects_dml_in_cte(tmp_path):
    client = _build_test_client(tmp_path)

    response = client.post(
        "/raw",
        headers=_auth_headers(),
        json={"query": "WITH changed AS (DELETE FROM orders RETURNING id) SELECT * FROM changed"},
    )

    assert response.status_code == 400
    assert "only select" in response.json()["error"].lower()


def test_json_responses_use_arrow_reader_for_generic_adapters():
    adapter = _ArrowOnlyAdapter()
    layer = SemanticLayer(connection=adapter, auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", sql="status", type="categorical")],
            metrics=[Metric(name="order_count", agg="count")],
        )
    )

    client = TestClient(create_app(layer))
    response = client.post("/query", json={"metrics": ["orders.order_count"]})

    assert response.status_code == 200
    assert response.json()["rows"] == [{"order_count": 7}]
