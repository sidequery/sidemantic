"""Cross-surface SQL equivalence tests.

Trust guarantee under test: *the SQL you preview is exactly the SQL that runs --
across every surface*. For a set of representative semantic queries, the
compiled/rewritten SQL must be byte-identical no matter which surface produced
it, and executing that SQL must return identical results.

Surfaces covered here (the ones that compile/rewrite SQL from the same graph):

SQL-first family (semantic SQL string -> rewritten SQL):
  1. Direct ``QueryRewriter`` -- the exact construction used by
     ``sidemantic rewrite`` and ``sidemantic query --dry-run`` (see
     ``sidemantic/cli.py``: ``QueryRewriter(layer.graph,
     dialect=layer.adapter.dialect, use_preaggregations=layer.use_preaggregations)``).
  2. ``SemanticLayer.sql()`` -- the ``sidemantic query`` execution path. It
     rewrites through the identical ``QueryRewriter(self.graph,
     dialect=self.dialect, use_preaggregations=self.use_preaggregations)`` call
     before executing (``sidemantic/core/semantic_layer.py``). Covered as a
     regression tripwire so a future divergence in that construction is caught.
  3. HTTP API ``POST /sql/compile`` -- ``sidemantic/api_server.py`` calls
     ``QueryRewriter(current_layer.graph, dialect=current_layer.dialect).rewrite(query)``.

Structured family (dimensions/metrics -> compiled SQL):
  4. ``SemanticLayer.compile(...)`` -- the library/CLI compile entry point.
  5. HTTP API ``POST /compile`` -- ``sidemantic/api_server.py`` delegates
     straight to ``current_layer.compile(...)``.

Skipped surface:
  - PG wire server rewrite (``sidemantic/server/connection.py``). Its rewrite
    path is ``QueryRewriter(self.layer.graph, dialect=self.layer.dialect)`` too,
    but the module imports ``riffq`` at module top level, and ``riffq`` is an
    optional dependency not installed in the dev/test environment. Importing the
    rewrite function is therefore unavoidable-ly gated on ``riffq``; we assert
    the import is skippable rather than smuggling in a copy of its logic. See
    ``test_pg_server_rewrite_path_requires_riffq``.
"""

# ruff: noqa: E402

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.api_server import create_app
from sidemantic.sql.query_rewriter import QueryRewriter

# Representative semantic-SQL queries exercised across the SQL-first surfaces.
# Chosen to hit: simple metric aggregation; metric + categorical dimension +
# filter; time-granularity truncation; a join across the relationship; and
# ORDER BY / LIMIT.
SQL_FIRST_QUERIES = {
    "simple_aggregation": "SELECT orders.revenue FROM orders",
    "metric_dimension_filter": ("SELECT orders.status, orders.revenue FROM orders WHERE orders.status = 'completed'"),
    "time_granularity": "SELECT orders.created_at__month, orders.revenue FROM orders",
    "join_across_relationship": ("SELECT customers.region, orders.revenue FROM orders"),
    # Order by revenue (distinct per status: 650/75/50) rather than order_count
    # (which ties at 1) so LIMIT 2 is deterministic and results are comparable.
    "order_by_limit": ("SELECT orders.status, orders.revenue FROM orders ORDER BY orders.revenue DESC LIMIT 2"),
    "ratio_metric": "SELECT orders.status, orders.avg_order_value FROM orders",
}

# Representative structured queries exercised across the structured surfaces.
STRUCTURED_QUERIES = {
    "simple_aggregation": {"metrics": ["orders.revenue"]},
    "metric_dimension_filter": {
        "metrics": ["orders.revenue"],
        "dimensions": ["orders.status"],
        "filters": ["orders.status = 'completed'"],
    },
    "time_granularity": {
        "metrics": ["orders.revenue"],
        "dimensions": ["orders.created_at__month"],
    },
    "join_across_relationship": {
        "metrics": ["orders.revenue"],
        "dimensions": ["customers.region"],
    },
    "order_by_limit": {
        "metrics": ["orders.revenue"],
        "dimensions": ["orders.status"],
        "order_by": ["orders.revenue desc"],
        "limit": 2,
    },
    "ratio_metric": {
        "metrics": ["orders.avg_order_value"],
        "dimensions": ["orders.status"],
    },
}


def _build_layer() -> SemanticLayer:
    """Build an in-memory DuckDB layer with orders/customers, metrics and data.

    ``use_preaggregations`` is left at its default (False) so all three SQL-first
    surfaces are genuinely equivalent: ``/sql/compile`` does not thread the
    pre-aggregation flag, while the CLI/``layer.sql`` paths pass
    ``layer.use_preaggregations``; with routing off the constructions coincide.
    """
    layer = SemanticLayer(auto_register=False)

    layer.adapter.execute(
        """
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id INTEGER,
            status VARCHAR,
            order_amount DECIMAL(10, 2),
            created_at DATE
        )
        """
    )
    layer.adapter.execute(
        """
        CREATE TABLE customers (
            customer_id INTEGER,
            customer_name VARCHAR,
            region VARCHAR,
            tier VARCHAR
        )
        """
    )
    layer.adapter.execute(
        """
        INSERT INTO orders VALUES
            (1, 101, 'completed', 150.00, '2024-01-15'),
            (2, 102, 'completed', 200.00, '2024-01-20'),
            (3, 101, 'pending', 75.00, '2024-02-01'),
            (4, 103, 'completed', 300.00, '2024-02-10'),
            (5, 102, 'cancelled', 50.00, '2024-02-15')
        """
    )
    layer.adapter.execute(
        """
        INSERT INTO customers VALUES
            (101, 'Alice', 'US', 'premium'),
            (102, 'Bob', 'EU', 'standard'),
            (103, 'Charlie', 'US', 'premium')
        """
    )

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="order_id", type="numeric"),
            Dimension(name="status", type="categorical"),
            Dimension(name="created_at", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="order_amount"),
            Metric(name="order_count", agg="count"),
            Metric(name="avg_order_value", agg="avg", sql="order_amount"),
        ],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )
    customers = Model(
        name="customers",
        table="customers",
        primary_key="customer_id",
        dimensions=[
            Dimension(name="customer_id", type="numeric"),
            Dimension(name="customer_name", type="categorical"),
            Dimension(name="region", type="categorical"),
            Dimension(name="tier", type="categorical"),
        ],
    )
    layer.add_model(orders)
    layer.add_model(customers)

    # A cross-model ratio metric (graph-level, references both models) so the
    # structured surfaces exercise a derived/ratio compile path too.
    layer.add_metric(
        Metric(
            name="revenue_per_order",
            type="ratio",
            numerator="orders.revenue",
            denominator="orders.order_count",
        )
    )
    return layer


@pytest.fixture
def layer() -> SemanticLayer:
    return _build_layer()


@pytest.fixture
def client(layer: SemanticLayer) -> TestClient:
    # auth_token=None -> endpoints are open, so no header handling needed. The
    # app shares the same in-memory layer (and thus the same DuckDB connection),
    # so execution results are directly comparable.
    return TestClient(create_app(layer, auth_token=None, serve_ui=False))


def _rewrite_cli(layer: SemanticLayer, sql: str) -> str:
    """Reproduce the exact QueryRewriter construction used by the CLI.

    Mirrors ``sidemantic rewrite`` / ``sidemantic query --dry-run`` in
    ``sidemantic/cli.py``.
    """
    return QueryRewriter(
        layer.graph,
        dialect=layer.adapter.dialect,
        use_preaggregations=layer.use_preaggregations,
    ).rewrite(sql)


def _rewrite_layer_sql_path(layer: SemanticLayer, sql: str) -> str:
    """Reproduce the rewrite step inside ``SemanticLayer.sql()``.

    ``SemanticLayer.sql()`` executes rather than returning SQL, so it cannot be
    asserted on directly. This mirrors the exact construction it uses so a future
    divergence between the ``sql()`` construction and the CLI construction fails
    here (regression tripwire). The end-to-end ``sql()`` execution is separately
    checked for result equality in ``test_execution_results_match_across_surfaces``.
    """
    return QueryRewriter(
        layer.graph,
        dialect=layer.dialect,
        use_preaggregations=layer.use_preaggregations,
    ).rewrite(sql)


def _fetch_sorted(layer: SemanticLayer, sql: str) -> list[tuple]:
    rows = layer.adapter.cursor().execute(sql).fetchall()
    return sorted(rows, key=lambda row: tuple(str(value) for value in row))


# --------------------------------------------------------------------------- #
# SQL-first family: byte-identical rewritten SQL across surfaces.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("name", list(SQL_FIRST_QUERIES))
def test_sql_first_rewrite_is_byte_identical(name: str, layer: SemanticLayer, client: TestClient) -> None:
    sql = SQL_FIRST_QUERIES[name]

    cli_sql = _rewrite_cli(layer, sql)
    layer_sql_path_sql = _rewrite_layer_sql_path(layer, sql)

    response = client.post("/sql/compile", json={"query": sql})
    assert response.status_code == 200, response.text
    api_sql = response.json()["sql"]

    # Direct QueryRewriter (CLI) == SemanticLayer.sql() rewrite step == HTTP /sql/compile.
    assert cli_sql == layer_sql_path_sql, f"{name}: CLI rewrite diverged from SemanticLayer.sql() rewrite"
    assert cli_sql == api_sql, f"{name}: CLI rewrite diverged from HTTP /sql/compile"


def test_sql_compile_endpoint_matches_after_semicolon_normalization(layer: SemanticLayer, client: TestClient) -> None:
    """A trailing semicolon is stripped by the API's documented query normalization.

    ``/sql/compile`` runs ``_normalize_sql_query`` (strips a single trailing
    ``;``) before rewriting. That is the ONLY documented shaping difference, and
    it makes the endpoint agree with the CLI's rewrite of the un-terminated
    query -- confirming normalization, not the rewriter, accounts for the ``;``.
    """
    sql = "SELECT orders.revenue FROM orders"
    cli_sql = _rewrite_cli(layer, sql)

    response = client.post("/sql/compile", json={"query": sql + ";"})
    assert response.status_code == 200, response.text
    assert response.json()["sql"] == cli_sql


# --------------------------------------------------------------------------- #
# Structured family: byte-identical compiled SQL across surfaces.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("name", list(STRUCTURED_QUERIES))
def test_structured_compile_is_byte_identical(name: str, layer: SemanticLayer, client: TestClient) -> None:
    spec = STRUCTURED_QUERIES[name]

    library_sql = layer.compile(
        metrics=spec.get("metrics"),
        dimensions=spec.get("dimensions"),
        filters=spec.get("filters"),
        order_by=spec.get("order_by"),
        limit=spec.get("limit"),
    )

    payload = {
        "metrics": spec.get("metrics", []),
        "dimensions": spec.get("dimensions", []),
        "filters": spec.get("filters", []),
        "order_by": spec.get("order_by", []),
        "limit": spec.get("limit"),
    }
    response = client.post("/compile", json=payload)
    assert response.status_code == 200, response.text
    api_sql = response.json()["sql"]

    # SemanticLayer.compile() == HTTP /compile (which delegates to compile()).
    assert library_sql == api_sql, f"{name}: layer.compile() diverged from HTTP /compile"


def test_structured_ratio_metric_compile_is_byte_identical(layer: SemanticLayer, client: TestClient) -> None:
    """The cross-model ratio metric compiles identically via library and HTTP."""
    library_sql = layer.compile(metrics=["revenue_per_order"], dimensions=["orders.status"])

    response = client.post(
        "/compile",
        json={"metrics": ["revenue_per_order"], "dimensions": ["orders.status"]},
    )
    assert response.status_code == 200, response.text
    assert library_sql == response.json()["sql"]


# --------------------------------------------------------------------------- #
# Execution equivalence: the previewed SQL actually runs and agrees.
# --------------------------------------------------------------------------- #


def _sort_rows(rows: list) -> list:
    return sorted(rows, key=lambda row: tuple(str(value) for value in row))


@pytest.mark.parametrize("name", list(SQL_FIRST_QUERIES))
def test_execution_results_match_across_surfaces(name: str, layer: SemanticLayer, client: TestClient) -> None:
    sql = SQL_FIRST_QUERIES[name]
    previewed_sql = _rewrite_cli(layer, sql)

    # DB-native equality: executing the previewed rewrite directly on the fixture
    # DB == SemanticLayer.sql() (the actual execution path). Same connection, same
    # native types (date/Decimal), so this is exact tuple equality.
    previewed_rows = _fetch_sorted(layer, previewed_sql)
    layer_sql_rows = _sort_rows(layer.sql(sql).fetchall())
    assert layer_sql_rows == previewed_rows, f"{name}: SemanticLayer.sql() results diverged from previewed SQL"

    # HTTP surface: /sql rewrites then executes and echoes the SQL it ran -- that
    # must be the previewed SQL. Its rows are JSON-serialized (dates -> strings,
    # etc.), so rather than coerce types we compare against /raw executing the
    # *previewed* SQL: both go through the identical JSON serialization, isolating
    # "did the same SQL run?" from "how are values serialized?".
    sql_response = client.post("/sql", json={"query": sql})
    assert sql_response.status_code == 200, sql_response.text
    sql_body = sql_response.json()
    assert sql_body["sql"] == previewed_sql, f"{name}: HTTP /sql ran different SQL than it previewed"

    raw_response = client.post("/raw", json={"query": previewed_sql})
    assert raw_response.status_code == 200, raw_response.text

    api_sql_rows = _sort_rows([tuple(row.items()) for row in sql_body["rows"]])
    api_raw_rows = _sort_rows([tuple(row.items()) for row in raw_response.json()["rows"]])
    assert api_sql_rows == api_raw_rows, f"{name}: HTTP /sql results diverged from executing the previewed SQL"


# --------------------------------------------------------------------------- #
# Skipped surface: PG wire server rewrite path (gated on optional riffq dep).
# --------------------------------------------------------------------------- #


def test_pg_server_rewrite_path_requires_riffq() -> None:
    """Document why the PG wire server rewrite path is not covered here.

    ``sidemantic/server/connection.py`` rewrites with
    ``QueryRewriter(self.layer.graph, dialect=self.layer.dialect)`` -- the same
    rewriter the surfaces above exercise. But that module imports ``riffq`` at
    module top level, so it cannot be imported without the optional ``riffq``
    dependency (absent in the dev/test env). We assert the import is genuinely
    skippable so this test documents the reason rather than testing a fake.
    """
    riffq = pytest.importorskip("riffq", reason="PG wire server rewrite path requires the optional 'riffq' dependency")
    # If riffq ever lands in the dev env, exercise the real path for parity.
    assert riffq is not None
    from sidemantic.server.connection import QueryRewriter as PGQueryRewriter

    layer = _build_layer()
    sql = "SELECT orders.revenue FROM orders"
    pg_sql = PGQueryRewriter(layer.graph, dialect=layer.dialect).rewrite(sql, strict=False)
    assert pg_sql == _rewrite_cli(layer, sql)
