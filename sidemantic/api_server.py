"""HTTP API server for Sidemantic."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Literal

try:
    from fastapi import Depends, FastAPI, HTTPException, Query, Request, Security
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse, Response
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    from pydantic import BaseModel, ConfigDict, Field
    from starlette.middleware.base import BaseHTTPMiddleware
except ImportError as exc:
    raise ImportError("HTTP API support requires FastAPI. Install with: uv add 'sidemantic[api]'") from exc

from sidemantic import SemanticLayer, __version__
from sidemantic.loaders import load_from_directory
from sidemantic.server.common import (
    ARROW_STREAM_MEDIA_TYPE,
    record_batch_reader_to_table,
    result_to_record_batch_reader,
    table_to_arrow_bytes,
    table_to_json_rows,
    validate_filter_expression,
)
from sidemantic.sql.query_rewriter import QueryRewriter

ARROW_FORMAT = "arrow"
JSON_FORMAT = "json"


class StructuredQueryRequest(BaseModel):
    """HTTP request model for semantic queries."""

    model_config = ConfigDict(extra="forbid")

    dimensions: list[str] = Field(default_factory=list)
    metrics: list[str] = Field(default_factory=list)
    where: str | None = None
    filters: list[str] = Field(default_factory=list)
    segments: list[str] = Field(default_factory=list)
    order_by: list[str] = Field(default_factory=list)
    limit: int | None = Field(default=None, ge=0)
    offset: int | None = Field(default=None, ge=0)
    ungrouped: bool = False
    parameters: dict[str, Any] | None = None
    use_preaggregations: bool | None = None
    timezone: str | None = None

    def resolved_filters(self) -> list[str]:
        filters = list(self.filters)
        if self.where:
            filters.append(self.where)
        return filters


class SQLRequest(BaseModel):
    """HTTP request model for raw SQL rewrite and execution."""

    model_config = ConfigDict(extra="forbid")

    query: str = Field(min_length=1)


class MaxRequestBodyMiddleware(BaseHTTPMiddleware):
    """Reject request bodies above a configured byte limit."""

    def __init__(self, app, max_body_bytes: int):
        super().__init__(app)
        self.max_body_bytes = max_body_bytes

    async def dispatch(self, request: Request, call_next):
        if request.method in {"POST", "PUT", "PATCH"}:
            content_length = request.headers.get("content-length")
            if content_length and int(content_length) > self.max_body_bytes:
                return JSONResponse(
                    {"error": f"Request body exceeds {self.max_body_bytes} bytes"},
                    status_code=413,
                )
            # Read incrementally so we can reject before buffering the full
            # payload (protects against chunked uploads without Content-Length).
            chunks: list[bytes] = []
            total = 0
            async for chunk in request.stream():
                total += len(chunk)
                if total > self.max_body_bytes:
                    return JSONResponse(
                        {"error": f"Request body exceeds {self.max_body_bytes} bytes"},
                        status_code=413,
                    )
                chunks.append(chunk)
            # Starlette caches the body once read; inject the reassembled bytes
            # so downstream handlers can still use request.body() / request.json().
            request._body = b"".join(chunks)
        return await call_next(request)


def initialize_layer(directory: str, connection: str | None = None) -> SemanticLayer:
    """Initialize a semantic layer for the HTTP API."""
    layer = SemanticLayer(connection=connection) if connection else SemanticLayer()
    load_from_directory(layer, directory)
    return layer


def start_api_server(
    layer: SemanticLayer,
    host: str = "127.0.0.1",
    port: int = 4400,
    auth_token: str | None = None,
    cors_origins: list[str] | None = None,
    max_request_body_bytes: int = 1024 * 1024,
    serve_ui: bool = True,
    result_cache_mb: int = 0,
    result_cache_ttl: float = 60.0,
) -> None:
    """Start the HTTP API server."""
    try:
        import uvicorn
    except ImportError as exc:
        raise ImportError("HTTP API support requires uvicorn. Install with: uv add 'sidemantic[api]'") from exc

    app = create_app(
        layer,
        auth_token=auth_token,
        cors_origins=cors_origins,
        max_request_body_bytes=max_request_body_bytes,
        serve_ui=serve_ui,
        result_cache_mb=result_cache_mb,
        result_cache_ttl=result_cache_ttl,
    )
    uvicorn.run(app, host=host, port=port, log_level="info")


def ui_static_dir() -> Path:
    """Directory of the embedded web UI bundle (present when built/committed)."""
    return Path(__file__).parent / "ui" / "static"


def create_app(
    layer: SemanticLayer,
    auth_token: str | None = None,
    cors_origins: list[str] | None = None,
    max_request_body_bytes: int = 1024 * 1024,
    serve_ui: bool = False,
    result_cache_mb: int = 0,
    result_cache_ttl: float = 60.0,
) -> FastAPI:
    """Create a FastAPI app for a loaded semantic layer.

    When ``result_cache_mb`` > 0, read-only query handlers serve identical
    repeated queries from a content-keyed Arrow result cache (opt-in; the
    library ``SemanticLayer.query()`` is never cached by default).
    """
    app = FastAPI(title="Sidemantic API", version=__version__)
    app.state.layer = layer
    # Reentrant lock reserved for layer-MUTATION endpoints (model registration,
    # config reload). Read-only query/compile/metadata handlers do NOT take it:
    # they read the immutable in-memory graph and execute queries on a fresh
    # per-request adapter.cursor(), so HTTP reads run concurrently.
    app.state.lock = threading.RLock()
    app.state.auth_token = auth_token

    if result_cache_mb and result_cache_mb > 0:
        from sidemantic.core.result_cache import ResultCache

        app.state.result_cache = ResultCache(
            max_bytes=result_cache_mb * 1024 * 1024,
            ttl_seconds=result_cache_ttl,
        )
    else:
        app.state.result_cache = None

    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    app.add_middleware(MaxRequestBodyMiddleware, max_body_bytes=max_request_body_bytes)

    security = HTTPBearer(auto_error=False)

    def require_auth(credentials: HTTPAuthorizationCredentials | None = Security(security)) -> None:
        expected = app.state.auth_token
        if not expected:
            return
        if credentials is None or credentials.scheme.lower() != "bearer" or credentials.credentials != expected:
            raise HTTPException(
                status_code=401,
                detail="Invalid or missing bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    @app.exception_handler(ValueError)
    async def handle_value_error(_request: Request, exc: ValueError):
        return JSONResponse({"error": str(exc)}, status_code=400)

    @app.get("/readyz")
    def readyz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health", dependencies=[Depends(require_auth)])
    def health() -> dict[str, Any]:
        # Read-only metadata: reads the in-memory graph, never mutates layer
        # state and never touches the database, so it needs no lock.
        current_layer = app.state.layer
        return {
            "status": "ok",
            "version": __version__,
            "dialect": current_layer.dialect,
            "model_count": len(current_layer.graph.models),
        }

    @app.get("/models", dependencies=[Depends(require_auth)])
    def list_models() -> list[dict[str, Any]]:
        # Read-only metadata over the in-memory graph.
        current_layer = app.state.layer
        models = []
        for model_name, model in current_layer.graph.models.items():
            models.append(
                {
                    "name": model_name,
                    "table": model.table,
                    "dimensions": [d.name for d in model.dimensions],
                    "metrics": [m.name for m in model.metrics],
                    "relationships": len(model.relationships),
                }
            )
        return models

    @app.get("/graph", dependencies=[Depends(require_auth)])
    def graph() -> dict[str, Any]:
        # Read-only metadata over the in-memory graph.
        current_layer = app.state.layer
        graph_obj = current_layer.graph

        models = []
        for model_name, model in graph_obj.models.items():
            model_info = {
                "name": model_name,
                "table": model.table,
                "dimensions": [d.name for d in model.dimensions],
                "metrics": [m.name for m in model.metrics],
                "relationships": [{"name": rel.name, "type": rel.type} for rel in model.relationships],
            }
            if model.segments:
                model_info["segments"] = [segment.name for segment in model.segments]
            models.append(model_info)

        graph_metrics = []
        for metric_name, metric in graph_obj.metrics.items():
            metric_info = {"name": metric_name}
            if metric.type:
                metric_info["type"] = metric.type
            if metric.description:
                metric_info["description"] = metric.description
            if metric.base_metric:
                metric_info["base_metric"] = metric.base_metric
            graph_metrics.append(metric_info)

        joinable_pairs = []
        model_names = list(graph_obj.models.keys())
        for index, left_name in enumerate(model_names):
            for right_name in model_names[index + 1 :]:
                try:
                    path = graph_obj.find_relationship_path(left_name, right_name)
                except (KeyError, ValueError):
                    continue
                joinable_pairs.append({"from": left_name, "to": right_name, "hops": len(path)})

        payload: dict[str, Any] = {"models": models, "joinable_pairs": joinable_pairs}
        if graph_metrics:
            payload["graph_metrics"] = graph_metrics
        return payload

    @app.get("/describe", dependencies=[Depends(require_auth)])
    def describe() -> dict[str, Any]:
        """Rich UI/FFI model metadata (dimension types, granularities, labels, formats)."""
        # Read-only metadata over the in-memory graph.
        current_layer = app.state.layer
        payload = current_layer.describe_models()
        payload["dialect"] = current_layer.dialect
        return payload

    @app.post("/compile", dependencies=[Depends(require_auth)])
    def compile_query(payload: StructuredQueryRequest) -> dict[str, str]:
        # Pure CPU: compiles SQL from the in-memory graph, no DB access, no lock.
        current_layer = app.state.layer
        filters = payload.resolved_filters()
        for filter_str in filters:
            validate_filter_expression(filter_str, dialect=current_layer.dialect)
        sql = current_layer.compile(
            dimensions=payload.dimensions,
            metrics=payload.metrics,
            filters=filters,
            segments=payload.segments or None,
            order_by=payload.order_by or None,
            limit=payload.limit,
            offset=payload.offset,
            ungrouped=payload.ungrouped,
            parameters=payload.parameters,
            use_preaggregations=payload.use_preaggregations,
            timezone=payload.timezone,
        )
        return {"sql": sql}

    @app.post("/query", dependencies=[Depends(require_auth)])
    def run_query(
        payload: StructuredQueryRequest,
        request: Request,
        format: Literal["json", "arrow"] | None = Query(default=None),
    ):
        # Read-only query: compile (pure CPU) then execute on a fresh per-request
        # cursor so concurrent reads do not serialize on one connection. Execution
        # flows through _query_table so the opt-in result cache can serve repeats.
        current_layer = app.state.layer
        filters = payload.resolved_filters()
        for filter_str in filters:
            validate_filter_expression(filter_str, dialect=current_layer.dialect)
        sql = current_layer.compile(
            dimensions=payload.dimensions,
            metrics=payload.metrics,
            filters=filters,
            segments=payload.segments or None,
            order_by=payload.order_by or None,
            limit=payload.limit,
            offset=payload.offset,
            ungrouped=payload.ungrouped,
            parameters=payload.parameters,
            use_preaggregations=payload.use_preaggregations,
            timezone=payload.timezone,
        )
        table = _query_table(app, current_layer, sql)
        return _build_query_response(request, current_layer, table, sql=sql, format_override=format)

    @app.post("/sql/compile", dependencies=[Depends(require_auth)])
    def compile_sql(payload: SQLRequest) -> dict[str, str]:
        # Pure CPU: rewrites SQL from the in-memory graph, no DB access, no lock.
        current_layer = app.state.layer
        query = _normalize_sql_query(payload.query)
        rewritten_sql = QueryRewriter(current_layer.graph, dialect=current_layer.dialect).rewrite(query)
        return {"sql": rewritten_sql}

    @app.post("/sql", dependencies=[Depends(require_auth)])
    def run_sql(
        payload: SQLRequest,
        request: Request,
        format: Literal["json", "arrow"] | None = Query(default=None),
    ):
        # Read-only query: rewrite (pure CPU) then execute on a fresh per-request
        # cursor, routed through _query_table for opt-in result caching.
        current_layer = app.state.layer
        query = _normalize_sql_query(payload.query)
        rewritten_sql = QueryRewriter(current_layer.graph, dialect=current_layer.dialect).rewrite(query)
        table = _query_table(app, current_layer, rewritten_sql)
        return _build_query_response(
            request,
            current_layer,
            table,
            sql=rewritten_sql,
            original_sql=query,
            format_override=format,
        )

    @app.post("/raw", dependencies=[Depends(require_auth)])
    def run_raw_sql(
        payload: SQLRequest,
        request: Request,
        format: Literal["json", "arrow"] | None = Query(default=None),
    ):
        """Execute raw SQL directly on the underlying database, bypassing the semantic rewriter."""
        # Read-only query (SELECT enforced) on a fresh per-request cursor,
        # routed through _query_table for opt-in result caching.
        current_layer = app.state.layer
        query = _normalize_sql_query(payload.query)
        _require_select_statement(query)
        table = _query_table(app, current_layer, query)
        return _build_query_response(
            request,
            current_layer,
            table,
            sql=query,
            format_override=format,
        )

    if serve_ui:
        ui_dir = ui_static_dir()
        index_file = ui_dir / "index.html"
        if index_file.exists():
            from fastapi.responses import FileResponse
            from fastapi.staticfiles import StaticFiles

            assets_dir = ui_dir / "assets"
            if assets_dir.is_dir():
                app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="ui-assets")

            # UI routes are intentionally public (no auth) so the static shell loads; the data
            # endpoints above stay token-gated and the SPA sends the token on its own requests.
            # Registered last so the API routes take precedence.
            @app.get("/", include_in_schema=False)
            def _ui_root():
                return FileResponse(index_file)

            @app.get("/{spa_path:path}", include_in_schema=False)
            def _ui_spa(spa_path: str):
                candidate = ui_dir / spa_path
                if candidate.is_file() and candidate.resolve().is_relative_to(ui_dir.resolve()):
                    return FileResponse(candidate)
                return FileResponse(index_file)

    return app


def _execute_to_table(layer: SemanticLayer, sql: str) -> Any:
    """Execute SQL on the layer's adapter and materialize a PyArrow table.

    Caching operates on fully-materialized tables (rather than single-use
    RecordBatchReaders) so a cached result can be served to many requests.

    Executes on a fresh per-request cursor rather than the shared connection so
    concurrent reads run in parallel; the result cache's singleflight then dedups
    identical concurrent queries into a single underlying execute.
    """
    result = layer.adapter.cursor().execute(sql)
    reader = result_to_record_batch_reader(result, layer.adapter)
    return record_batch_reader_to_table(reader)


def _query_table(app: FastAPI, layer: SemanticLayer, sql: str) -> Any:
    """Return the Arrow table for ``sql``, served from the result cache if enabled.

    The cache is opt-in (``result_cache_mb`` > 0). The key covers the compiled
    SQL, dialect + connection fingerprint, layer generation, and user attributes
    (None today; A2 will populate them). Singleflight in ResultCache ensures a
    duplicate in-flight query runs the underlying execute exactly once.
    """
    cache = getattr(app.state, "result_cache", None)
    if cache is None:
        return _execute_to_table(layer, sql)

    key = layer.build_result_key(sql, user_attributes=None)
    return cache.get_or_compute(key, lambda: _execute_to_table(layer, sql))


def _build_query_response(
    request: Request,
    layer: SemanticLayer,
    table: Any,
    sql: str,
    format_override: Literal["json", "arrow"] | None,
    original_sql: str | None = None,
):
    response_format = _resolve_response_format(request, format_override)

    if response_format == ARROW_FORMAT:
        body = table_to_arrow_bytes(table)
        return Response(
            content=body,
            media_type=ARROW_STREAM_MEDIA_TYPE,
            headers={
                "X-Sidemantic-Row-Count": str(table.num_rows),
                "X-Sidemantic-Dialect": layer.dialect,
            },
        )

    payload: dict[str, Any] = {
        "sql": sql,
        "rows": table_to_json_rows(table),
        "row_count": table.num_rows,
    }
    if original_sql is not None:
        payload["original_sql"] = original_sql
    return JSONResponse(payload)


def _normalize_sql_query(query: str) -> str:
    normalized = query.strip()
    if not normalized:
        raise ValueError("Query cannot be empty")
    if normalized.endswith(";"):
        normalized = normalized[:-1].strip()
    # Use sqlglot to detect multiple statements so semicolons inside string
    # literals (e.g. SELECT ';') are not rejected.
    import sqlglot

    try:
        statements = sqlglot.parse(normalized)
    except Exception as exc:
        raise ValueError(f"Failed to parse SQL: {exc}") from exc
    if len(statements) > 1:
        raise ValueError("Multiple SQL statements are not supported")
    return normalized


_DML_TYPES: tuple[type, ...] | None = None


def _get_dml_types() -> tuple[type, ...]:
    global _DML_TYPES
    if _DML_TYPES is None:
        from sqlglot import exp

        _DML_TYPES = (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Alter, exp.Command)
    return _DML_TYPES


def _require_select_statement(query: str) -> None:
    """Reject non-SELECT statements to prevent mutations via /raw.

    Also inspects CTEs so that DML hidden inside a WITH clause
    (e.g., WITH x AS (DELETE ... RETURNING ...) SELECT * FROM x)
    is caught.
    """
    import sqlglot
    from sqlglot import exp

    try:
        parsed = sqlglot.parse_one(query)
    except Exception:
        # If parsing fails, let it through to get a proper DB error
        return
    query_types = (exp.Select, exp.Union, exp.Intersect, exp.Except, exp.Subquery)
    if not isinstance(parsed, query_types):
        raise ValueError("Only SELECT statements are allowed on the /raw endpoint")
    # Walk the full AST to catch DML buried in CTEs or subqueries
    dml_types = _get_dml_types()
    for node in parsed.walk():
        if isinstance(node, dml_types):
            raise ValueError("Only SELECT statements are allowed on the /raw endpoint")


def _resolve_response_format(
    request: Request,
    format_override: Literal["json", "arrow"] | None,
) -> Literal["json", "arrow"]:
    if format_override:
        return format_override
    accept_header = request.headers.get("accept", "")
    if ARROW_STREAM_MEDIA_TYPE in accept_header:
        return ARROW_FORMAT
    return JSON_FORMAT
