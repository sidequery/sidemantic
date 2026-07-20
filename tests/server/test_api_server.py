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

from sidemantic import DashboardDocument, Dimension, Metric, Model, SemanticLayer, load_from_directory
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


def _build_test_client(
    tmp_path: Path, auth_token: str | None = "secret", max_body_bytes: int = 1024 * 1024, serve_ui: bool = False
):
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
    app = create_app(layer, auth_token=auth_token, max_request_body_bytes=max_body_bytes, serve_ui=serve_ui)
    return TestClient(app)


def _build_test_client_with_cache(tmp_path: Path, result_cache_mb: int = 16, result_cache_ttl: float = 60.0):
    """Build a client with the result cache enabled, returning (client, app)."""
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
    app = create_app(
        layer,
        auth_token="secret",
        result_cache_mb=result_cache_mb,
        result_cache_ttl=result_cache_ttl,
    )
    return TestClient(app), app


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


def test_dashboard_endpoint_is_optional_and_authenticated(tmp_path):
    models_dir = tmp_path / "models"
    _write_models(models_dir)
    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    load_from_directory(layer, models_dir)
    dashboard = DashboardDocument.from_dict(
        {
            "schema": "sidemantic.dashboard.v1",
            "title": "Orders overview",
            "tabs": [
                {
                    "id": "overview",
                    "charts": [
                        {
                            "id": "orders",
                            "query": {
                                "metrics": ["orders.order_count"],
                                "dimensions": ["orders.created_at__day", "orders.status"],
                            },
                            "encoding": {"x": "orders.created_at__day", "y": "orders.order_count"},
                        },
                        {
                            "id": "orders_kpi",
                            "type": "kpi",
                            "query": {"metrics": ["orders.order_count"]},
                        },
                    ],
                }
            ],
        }
    )
    configured = TestClient(create_app(layer, auth_token="secret", dashboard=dashboard))
    unconfigured = TestClient(create_app(layer))

    assert configured.get("/dashboard").status_code == 401
    response = configured.get("/dashboard", headers=_auth_headers())
    assert response.status_code == 200
    assert response.json()["title"] == "Orders overview"
    assert [chart["id"] for chart in response.json()["tabs"][0]["charts"]] == ["orders", "orders_kpi"]
    assert unconfigured.get("/dashboard").status_code == 404


def test_describe_endpoint(tmp_path):
    client = _build_test_client(tmp_path)

    assert client.get("/describe").status_code == 401

    response = client.get("/describe", headers=_auth_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload["dialect"] == "duckdb"

    orders = next(model for model in payload["models"] if model["name"] == "orders")
    dimensions = {dimension["name"]: dimension for dimension in orders["dimensions"]}
    assert dimensions["created_at"]["type"] == "time"
    assert dimensions["created_at"]["granularity"] == "day"
    assert dimensions["status"]["type"] == "categorical"
    metric_names = {metric["name"] for metric in orders["metrics"]}
    assert {"order_count", "total_amount"} <= metric_names


def test_result_cache_serves_repeated_query(tmp_path):
    client, app = _build_test_client_with_cache(tmp_path)

    body = {
        "dimensions": ["orders.status"],
        "metrics": ["orders.total_amount", "orders.order_count"],
        "order_by": ["orders.status"],
    }

    first = client.post("/query", headers=_auth_headers(), json=body)
    second = client.post("/query", headers=_auth_headers(), json=body)

    assert first.status_code == 200
    assert second.status_code == 200
    # Identical results served both times.
    assert first.json()["rows"] == second.json()["rows"]

    stats = app.state.result_cache.stats()
    assert stats["hits"] == 1
    assert stats["misses"] == 1
    assert stats["entries"] == 1

    # A different query is a distinct cache entry (another miss).
    other = client.post(
        "/query",
        headers=_auth_headers(),
        json={"dimensions": ["orders.status"], "metrics": ["orders.order_count"]},
    )
    assert other.status_code == 200
    stats2 = app.state.result_cache.stats()
    assert stats2["misses"] == 2
    assert stats2["entries"] == 2


def test_result_cache_disabled_by_default(tmp_path):
    client = _build_test_client(tmp_path)
    assert client.app.state.result_cache is None


def test_serve_ui_serves_spa_and_keeps_api_gated(tmp_path):
    from sidemantic.api_server import ui_static_dir

    if not ui_static_dir().joinpath("index.html").exists():
        import pytest

        pytest.skip("web UI bundle not built (run scripts/build_webapp.py)")

    client = _build_test_client(tmp_path, serve_ui=True)

    # SPA shell is public (no auth) at the root and as a fallback for unknown paths.
    root = client.get("/")
    assert root.status_code == 200
    assert "text/html" in root.headers["content-type"]
    assert client.get("/some/deep/link").status_code == 200

    # API routes still resolve and stay auth-gated.
    assert client.get("/health").status_code == 401
    assert client.get("/health", headers=_auth_headers()).status_code == 200
    assert client.get("/describe", headers=_auth_headers()).status_code == 200


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


def test_compile_accepts_timezone(tmp_path):
    # The optional timezone field on the structured-query request threads into
    # layer.compile so time-dimension truncation happens in the requested zone.
    client = _build_test_client(tmp_path)

    without_tz = client.post(
        "/compile",
        headers=_auth_headers(),
        json={"dimensions": ["orders.created_at__day"], "metrics": ["orders.order_count"]},
    )
    with_tz = client.post(
        "/compile",
        headers=_auth_headers(),
        json={
            "dimensions": ["orders.created_at__day"],
            "metrics": ["orders.order_count"],
            "timezone": "America/New_York",
        },
    )

    assert without_tz.status_code == 200
    assert with_tz.status_code == 200
    tz_sql = with_tz.json()["sql"]
    # The zone shows up in the compiled SQL and the timezone-aware SQL differs from the UTC default.
    assert "America/New_York" in tz_sql
    assert tz_sql != without_tz.json()["sql"]


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


def _build_unbuilt_rollup_client(tmp_path: Path, preagg_strict: bool = False) -> TestClient:
    """Client whose layer routes to a pre-aggregation that was never materialized."""
    from sidemantic.core.pre_aggregation import PreAggregation

    db_path = tmp_path / "preagg-warehouse.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("create table orders (id integer, status varchar, amount double)")
    conn.executemany(
        "insert into orders values (?, ?, ?)",
        [(1, "completed", 10.0), (2, "completed", 20.0), (3, "pending", 5.0)],
    )
    conn.close()

    layer = SemanticLayer(
        connection=f"duckdb:///{db_path}",
        auto_register=False,
        use_preaggregations=True,
        preagg_strict=preagg_strict,
    )
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", sql="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            pre_aggregations=[PreAggregation(name="by_status", measures=["revenue"], dimensions=["status"])],
        )
    )
    return TestClient(create_app(layer))


def test_query_falls_back_to_raw_when_rollup_missing(tmp_path):
    """HTTP /query matches the Python API: a routed-but-unbuilt rollup falls back to raw tables."""
    client = _build_unbuilt_rollup_client(tmp_path)

    response = client.post("/query", json={"metrics": ["orders.revenue"], "dimensions": ["orders.status"]})

    assert response.status_code == 200
    payload = response.json()
    rows = sorted((row["status"], row["revenue"]) for row in payload["rows"])
    assert rows == [("completed", 30.0), ("pending", 5.0)]
    # The response reports the SQL that actually produced the rows (the raw fallback).
    assert "used_preagg=true" not in payload["sql"]


def test_query_strict_mode_returns_409_when_rollup_missing(tmp_path):
    """Rollup-only mode over HTTP maps PreaggregationStrictError to 409, not a 500."""
    client = _build_unbuilt_rollup_client(tmp_path, preagg_strict=True)

    response = client.post("/query", json={"metrics": ["orders.revenue"], "dimensions": ["orders.status"]})

    assert response.status_code == 409
    assert "not built" in response.json()["error"]


def test_query_strict_override_via_payload(tmp_path):
    """preagg_strict can be requested per-query even when the layer default is lenient."""
    client = _build_unbuilt_rollup_client(tmp_path)

    response = client.post(
        "/query",
        json={"metrics": ["orders.revenue"], "dimensions": ["orders.status"], "preagg_strict": True},
    )

    assert response.status_code == 409


def test_sql_endpoint_falls_back_to_raw_when_rollup_missing(tmp_path):
    """HTTP /sql honors configured rollup routing and falls back when the rollup is unbuilt."""
    client = _build_unbuilt_rollup_client(tmp_path)

    response = client.post(
        "/sql",
        json={"query": "SELECT orders.revenue, orders.status FROM orders"},
    )

    assert response.status_code == 200
    rows = sorted((row["status"], row["revenue"]) for row in response.json()["rows"])
    assert rows == [("completed", 30.0), ("pending", 5.0)]
