"""HTTP API server for Sidemantic."""

from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
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
    table_to_arrow_bytes,
    table_to_json_rows,
    validate_filter_expression,
)
from sidemantic.server.query_execution import (
    QueryAdmission,
    QueryExecutionControl,
    QueryLimits,
    QueryResponseTooLargeError,
    QueryRowLimitExceededError,
    execute_bounded,
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
    max_rows: int = 10_000,
    max_response_bytes: int = 16 * 1024 * 1024,
    execution_timeout_seconds: float = 30.0,
    max_concurrent_queries: int = 4,
    max_queued_queries: int = 16,
    queue_timeout_seconds: float = 5.0,
    query_history_size: int = 1000,
    require_user_attrs: bool = False,
    enforce_visibility: bool = False,
    user_header: str = "X-Sidemantic-User",
    dashboard: Any | None = None,
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
        max_rows=max_rows,
        max_response_bytes=max_response_bytes,
        execution_timeout_seconds=execution_timeout_seconds,
        max_concurrent_queries=max_concurrent_queries,
        max_queued_queries=max_queued_queries,
        queue_timeout_seconds=queue_timeout_seconds,
        query_history_size=query_history_size,
        require_user_attrs=require_user_attrs,
        enforce_visibility=enforce_visibility,
        user_header=user_header,
        dashboard=dashboard,
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
    max_rows: int = 10_000,
    max_response_bytes: int = 16 * 1024 * 1024,
    execution_timeout_seconds: float = 30.0,
    max_concurrent_queries: int = 4,
    max_queued_queries: int = 16,
    queue_timeout_seconds: float = 5.0,
    query_history_size: int = 1000,
    require_user_attrs: bool = False,
    enforce_visibility: bool = False,
    user_header: str = "X-Sidemantic-User",
    dashboard: Any | None = None,
) -> FastAPI:
    """Create a FastAPI app for a loaded semantic layer.

    When ``result_cache_mb`` > 0, read-only query handlers serve identical
    repeated queries from a content-keyed Arrow result cache (opt-in; the
    library ``SemanticLayer.query()`` is never cached by default).

    Security integration:
    - Per-request user attributes are read from the trusted ``user_header``
      (default ``X-Sidemantic-User``) whose value is a JSON object. They are
      threaded into the layer's compile/query path so access gates and row
      filters are enforced, and into the result-cache key so cached results
      never leak across users.
    - ``require_user_attrs`` rejects data-endpoint requests lacking a valid
      header with HTTP 400 before executing.
    - ``enforce_visibility`` is applied to the layer so requesting a non-public
      field is rejected.
    """
    app = FastAPI(title="Sidemantic API", version=__version__)
    # enforce_visibility is a SemanticLayer.__init__ arg; the layer is passed in
    # pre-built here, so set it on the instance (least invasive) rather than
    # reconstructing the layer and re-loading its models.
    if enforce_visibility:
        layer.enforce_visibility = True
    app.state.layer = layer
    app.state.require_user_attrs = require_user_attrs
    app.state.user_header = user_header
    # Reentrant lock reserved for layer-MUTATION endpoints (model registration,
    # config reload). Read-only query/compile/metadata handlers do NOT take it:
    # they read the immutable in-memory graph and execute queries on a fresh
    # per-request adapter.cursor(), so HTTP reads run concurrently.
    app.state.lock = threading.RLock()
    app.state.auth_token = auth_token
    app.state.dashboard = dashboard.to_dict() if hasattr(dashboard, "to_dict") else dashboard
    app.state.query_limits = QueryLimits(
        max_rows=max_rows,
        max_response_bytes=max_response_bytes,
        execution_timeout_seconds=execution_timeout_seconds,
        max_concurrent_queries=max_concurrent_queries,
        max_queued_queries=max_queued_queries,
        queue_timeout_seconds=queue_timeout_seconds,
    )
    app.state.query_admission = QueryAdmission(max_concurrent_queries, max_queued_queries)
    layer.query_telemetry.resize(query_history_size)

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

    from sidemantic.core.semantic_layer import SecurityError

    @app.exception_handler(SecurityError)
    async def handle_security_error(_request: Request, exc: SecurityError):
        # A secured model was queried without sufficient user attributes (or an
        # access gate denied the request). Map to 403 Forbidden.
        return JSONResponse({"error": str(exc)}, status_code=403)

    def resolve_user_attributes(request: Request) -> dict | None:
        """Parse per-request user attributes from the trusted user header.

        The header value is a JSON object bound to the ``user`` namespace when
        enforcing security policies. Returns ``None`` when the header is absent.
        Raises HTTP 400 when ``require_user_attrs`` is set and the header is
        missing, or whenever the header is present but not a JSON object.
        """
        header_name = app.state.user_header
        raw = request.headers.get(header_name)
        if raw is None or raw.strip() == "":
            if app.state.require_user_attrs:
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing required user-attributes header {header_name!r}",
                )
            return None
        try:
            parsed = json.loads(raw)
        except (ValueError, TypeError) as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Malformed JSON in {header_name!r} header: {exc}",
            ) from exc
        if not isinstance(parsed, dict):
            raise HTTPException(
                status_code=400,
                detail=f"{header_name!r} header must be a JSON object",
            )
        return parsed

    def deny_free_sql_if_secured() -> None:
        """Deny the free-form SQL endpoints when any model declares a security policy.

        ``/sql`` (semantic rewrite) and ``/raw`` (direct passthrough) cannot apply
        per-user row filters -- the rewriter/raw paths do not thread user attributes,
        and ``/raw`` reads the underlying table directly, bypassing the model entirely.
        Rather than silently return unscoped rows for a secured model, refuse these
        endpoints outright when security is in play and point callers at ``/query``,
        which enforces access gates and row filters.
        """
        layer = app.state.layer
        if any(getattr(model, "security", None) is not None for model in layer.graph.models.values()):
            raise SecurityError(
                "The /sql and /raw endpoints cannot enforce row-level security and are "
                "disabled because a model declares a security policy. Use the structured "
                "/query endpoint, which applies access gates and row filters per user."
            )

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

    def _visible_dimensions(layer, model) -> list:
        if getattr(layer, "enforce_visibility", False):
            return [d for d in model.dimensions if getattr(d, "public", True)]
        return list(model.dimensions)

    def _visible_metrics(layer, model) -> list:
        if getattr(layer, "enforce_visibility", False):
            return [m for m in model.metrics if getattr(m, "public", True)]
        return list(model.metrics)

    @app.get("/models", dependencies=[Depends(require_auth)])
    def list_models() -> list[dict[str, Any]]:
        # Read-only metadata over the in-memory graph. Non-public fields are omitted
        # when the layer enforces visibility, so this catalog cannot enumerate them.
        current_layer = app.state.layer
        models = []
        for model_name, model in current_layer.graph.models.items():
            models.append(
                {
                    "name": model_name,
                    "table": model.table,
                    "dimensions": [d.name for d in _visible_dimensions(current_layer, model)],
                    "metrics": [m.name for m in _visible_metrics(current_layer, model)],
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
                "dimensions": [d.name for d in _visible_dimensions(current_layer, model)],
                "metrics": [m.name for m in _visible_metrics(current_layer, model)],
                "relationships": [{"name": rel.name, "type": rel.type} for rel in model.relationships],
            }
            if model.segments:
                model_info["segments"] = [segment.name for segment in model.segments]
            models.append(model_info)

        graph_metrics = []
        for metric_name, metric in graph_obj.metrics.items():
            if getattr(current_layer, "enforce_visibility", False) and not getattr(metric, "public", True):
                continue
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

    @app.get("/dashboard", dependencies=[Depends(require_auth)])
    def dashboard_spec() -> dict[str, Any]:
        """Return the declarative dashboard loaded for the canonical web UI."""
        payload = app.state.dashboard
        if payload is None:
            raise HTTPException(status_code=404, detail="No dashboard configured")
        return payload

    @app.post("/compile", dependencies=[Depends(require_auth)])
    def compile_query(payload: StructuredQueryRequest, request: Request) -> dict[str, str]:
        # Pure CPU: compiles SQL from the in-memory graph, no DB access, no lock.
        # User attributes are threaded in so access gates and row filters are
        # baked into the compiled SQL (a secured model with no attrs raises
        # SecurityError -> 403).
        current_layer = app.state.layer
        user_attributes = resolve_user_attributes(request)
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
            user_attributes=user_attributes,
            timezone=payload.timezone,
        )
        return {"sql": sql}

    @app.post("/query", dependencies=[Depends(require_auth)])
    async def run_query(
        payload: StructuredQueryRequest,
        request: Request,
        format: Literal["json", "arrow"] | None = Query(default=None),
    ):
        # Read-only query: compile (pure CPU) then execute on a fresh per-request
        # cursor so concurrent reads do not serialize on one connection. Execution
        # flows through _query_table so the opt-in result cache can serve repeats.
        current_layer = app.state.layer
        limits = app.state.query_limits
        if payload.limit is not None and payload.limit > limits.max_rows:
            raise ValueError(f"limit must be <= the server maximum of {limits.max_rows}")
        user_attributes = resolve_user_attributes(request)
        filters = payload.resolved_filters()
        for filter_str in filters:
            validate_filter_expression(filter_str, dialect=current_layer.dialect)
        sql = current_layer.compile(
            dimensions=payload.dimensions,
            metrics=payload.metrics,
            filters=filters,
            segments=payload.segments or None,
            order_by=payload.order_by or None,
            limit=payload.limit if payload.limit is not None else limits.max_rows + 1,
            offset=payload.offset,
            ungrouped=payload.ungrouped,
            parameters=payload.parameters,
            use_preaggregations=payload.use_preaggregations,
            user_attributes=user_attributes,
            timezone=payload.timezone,
        )
        return await _execute_http_query(
            app,
            request,
            current_layer,
            sql,
            format_override=format,
            user_attributes=user_attributes,
        )

    @app.post("/sql/compile", dependencies=[Depends(require_auth)])
    def compile_sql(payload: SQLRequest) -> dict[str, str]:
        # Pure CPU: rewrites SQL from the in-memory graph, no DB access, no lock.
        current_layer = app.state.layer
        query = _normalize_sql_query(payload.query)
        rewritten_sql = QueryRewriter(current_layer.graph, dialect=current_layer.dialect).rewrite(query)
        return {"sql": rewritten_sql}

    @app.post("/sql", dependencies=[Depends(require_auth)])
    async def run_sql(
        payload: SQLRequest,
        request: Request,
        format: Literal["json", "arrow"] | None = Query(default=None),
    ):
        # Read-only query: rewrite (pure CPU) then execute on a fresh per-request
        # cursor, routed through _query_table for opt-in result caching.
        current_layer = app.state.layer
        user_attributes = resolve_user_attributes(request)
        deny_free_sql_if_secured()
        query = _normalize_sql_query(payload.query)
        rewritten_sql = QueryRewriter(current_layer.graph, dialect=current_layer.dialect).rewrite(query)
        limited_sql = _limit_query_sql(rewritten_sql, app.state.query_limits.max_rows)
        return await _execute_http_query(
            app,
            request,
            current_layer,
            limited_sql,
            original_sql=query,
            format_override=format,
            response_sql=rewritten_sql,
            user_attributes=user_attributes,
        )

    @app.post("/raw", dependencies=[Depends(require_auth)])
    async def run_raw_sql(
        payload: SQLRequest,
        request: Request,
        format: Literal["json", "arrow"] | None = Query(default=None),
    ):
        """Execute raw SQL directly on the underlying database, bypassing the semantic rewriter."""
        # Read-only query (SELECT enforced) on a fresh per-request cursor,
        # routed through _query_table for opt-in result caching.
        current_layer = app.state.layer
        user_attributes = resolve_user_attributes(request)
        deny_free_sql_if_secured()
        query = _normalize_sql_query(payload.query)
        _require_select_statement(query)
        limited_sql = _limit_query_sql(query, app.state.query_limits.max_rows)
        return await _execute_http_query(
            app,
            request,
            current_layer,
            limited_sql,
            format_override=format,
            response_sql=query,
            user_attributes=user_attributes,
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


def _execute_to_table(
    layer: SemanticLayer,
    sql: str,
    limits: QueryLimits,
    control: QueryExecutionControl,
) -> Any:
    """Execute SQL and consume at most the configured bounded Arrow result."""
    return execute_bounded(layer, sql, limits=limits, control=control).table


def _query_table(
    app: FastAPI,
    layer: SemanticLayer,
    sql: str,
    control: QueryExecutionControl,
    user_attributes: dict | None = None,
) -> tuple[Any, bool]:
    """Return a bounded Arrow table and whether it was already cached."""
    cache = getattr(app.state, "result_cache", None)
    if cache is None:
        return _execute_to_table(layer, sql, app.state.query_limits, control), False

    key = layer.build_result_key(sql, user_attributes=user_attributes)
    return cache.get_or_compute_with_status(
        key,
        lambda: _execute_to_table(layer, sql, app.state.query_limits, control),
    )


def _build_query_response(
    request: Request,
    layer: SemanticLayer,
    table: Any,
    sql: str,
    format_override: Literal["json", "arrow"] | None,
    max_response_bytes: int,
    query_id: str,
    request_id: str,
    original_sql: str | None = None,
) -> tuple[Response, int]:
    """Serialize a bounded table and enforce the exact transport byte ceiling."""
    response_format = _resolve_response_format(request, format_override)
    headers = {
        "X-Sidemantic-Query-ID": query_id,
        "X-Sidemantic-Request-ID": request_id,
        "X-Sidemantic-Row-Count": str(table.num_rows),
        "X-Sidemantic-Dialect": layer.dialect,
    }

    if response_format == ARROW_FORMAT:
        body = table_to_arrow_bytes(table)
        if len(body) > max_response_bytes:
            raise QueryResponseTooLargeError(
                f"Arrow response exceeds the configured maximum of {max_response_bytes} bytes"
            )
        headers["X-Sidemantic-Response-Bytes"] = str(len(body))
        return Response(content=body, media_type=ARROW_STREAM_MEDIA_TYPE, headers=headers), len(body)

    payload: dict[str, Any] = {
        "sql": sql,
        "rows": table_to_json_rows(table),
        "row_count": table.num_rows,
    }
    if original_sql is not None:
        payload["original_sql"] = original_sql
    body = json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    if len(body) > max_response_bytes:
        raise QueryResponseTooLargeError(f"JSON response exceeds the configured maximum of {max_response_bytes} bytes")
    headers["X-Sidemantic-Response-Bytes"] = str(len(body))
    return Response(content=body, media_type="application/json", headers=headers), len(body)


async def _execute_http_query(
    app: FastAPI,
    request: Request,
    layer: SemanticLayer,
    sql: str,
    *,
    format_override: Literal["json", "arrow"] | None,
    user_attributes: dict | None,
    response_sql: str | None = None,
    original_sql: str | None = None,
) -> Response:
    """Run one HTTP query under admission, deadline, cancellation, and telemetry."""
    from sidemantic.core.query_telemetry import QueryEvent, sanitize_sql

    limits: QueryLimits = app.state.query_limits
    query_id = uuid.uuid4().hex
    request_id = (request.headers.get("X-Request-ID") or uuid.uuid4().hex)[:128]
    started = time.monotonic()
    sanitized_sql, fingerprint = sanitize_sql(sql, layer.dialect)
    used_preaggregation = "used_preagg=true" in sql

    def record(
        *,
        row_count: int | None = None,
        response_bytes: int | None = None,
        cache_hit: bool = False,
        cancelled: bool = False,
        timed_out: bool = False,
        error: str | None = None,
        cancellation_diagnostic: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        layer.query_telemetry.record(
            QueryEvent(
                query_id=query_id,
                request_id=request_id,
                duration_ms=(time.monotonic() - started) * 1000,
                dialect=layer.dialect,
                row_count=row_count,
                response_bytes=response_bytes,
                cache_hit=cache_hit,
                used_preaggregation=used_preaggregation,
                cancelled=cancelled,
                timed_out=timed_out,
                error=error,
                sql=sanitized_sql,
                sql_fingerprint=fingerprint,
                plan_metadata={"source": "http", **(metadata or {})},
                cancellation_diagnostic=cancellation_diagnostic,
            )
        )

    admission = app.state.query_admission
    queue_started = time.monotonic()
    admission_status = await asyncio.to_thread(admission.acquire, limits.queue_timeout_seconds)
    queue_ms = (time.monotonic() - queue_started) * 1000
    if admission_status == "full":
        record(error="QueryQueueFull", metadata={"queue_duration_ms": queue_ms})
        raise HTTPException(
            status_code=429,
            detail=f"Query queue is full (maximum {limits.max_queued_queries} waiting)",
            headers={"X-Sidemantic-Query-ID": query_id, "X-Sidemantic-Request-ID": request_id},
        )
    if admission_status == "timeout":
        record(error="QueryQueueTimeout", metadata={"queue_duration_ms": queue_ms})
        raise HTTPException(
            status_code=503,
            detail=f"Query waited more than {limits.queue_timeout_seconds:g}s for an execution slot",
            headers={"X-Sidemantic-Query-ID": query_id, "X-Sidemantic-Request-ID": request_id},
        )

    control = QueryExecutionControl()

    def worker() -> tuple[Any, bool]:
        try:
            return _query_table(app, layer, sql, control, user_attributes=user_attributes)
        finally:
            # A timed-out uncancellable driver keeps its slot until the worker
            # actually stops, preventing hidden concurrency-limit overruns.
            admission.release()

    task = asyncio.create_task(asyncio.to_thread(worker))
    deadline = time.monotonic() + limits.execution_timeout_seconds
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                outcome = await asyncio.to_thread(control.cancel)
                task.add_done_callback(_consume_background_task)
                record(
                    cancelled=outcome.cancelled,
                    timed_out=True,
                    error="QueryExecutionTimeout",
                    cancellation_diagnostic=outcome.diagnostic,
                    metadata={"queue_duration_ms": queue_ms, "timeout": control.timeout_diagnostic},
                )
                raise HTTPException(
                    status_code=504,
                    detail=(
                        f"Query exceeded the {limits.execution_timeout_seconds:g}s execution deadline. "
                        f"{outcome.diagnostic}"
                    ),
                    headers={"X-Sidemantic-Query-ID": query_id, "X-Sidemantic-Request-ID": request_id},
                )
            try:
                table, cache_hit = await asyncio.wait_for(asyncio.shield(task), timeout=min(0.1, remaining))
                break
            except TimeoutError:
                if await request.is_disconnected():
                    outcome = await asyncio.to_thread(control.cancel)
                    task.add_done_callback(_consume_background_task)
                    record(
                        cancelled=outcome.cancelled,
                        error="ClientDisconnected",
                        cancellation_diagnostic=outcome.diagnostic,
                        metadata={"queue_duration_ms": queue_ms, "timeout": control.timeout_diagnostic},
                    )
                    raise HTTPException(
                        status_code=499,
                        detail=f"Client disconnected. {outcome.diagnostic}",
                        headers={"X-Sidemantic-Query-ID": query_id, "X-Sidemantic-Request-ID": request_id},
                    )

        response, response_bytes = _build_query_response(
            request,
            layer,
            table,
            sql=response_sql or sql,
            format_override=format_override,
            max_response_bytes=limits.max_response_bytes,
            query_id=query_id,
            request_id=request_id,
            original_sql=original_sql,
        )
        record(
            row_count=table.num_rows,
            response_bytes=response_bytes,
            cache_hit=cache_hit,
            metadata={
                "queue_duration_ms": queue_ms,
                "timeout": control.timeout_diagnostic,
                "response_format": _resolve_response_format(request, format_override),
            },
        )
        return response
    except (QueryRowLimitExceededError, QueryResponseTooLargeError) as exc:
        outcome = control.cancellation_outcome
        record(
            cancelled=bool(outcome and outcome.cancelled),
            error=type(exc).__name__,
            cancellation_diagnostic=outcome.diagnostic if outcome else None,
            metadata={"queue_duration_ms": queue_ms, "timeout": control.timeout_diagnostic},
        )
        status_code = 413 if isinstance(exc, QueryResponseTooLargeError) else 422
        raise HTTPException(
            status_code=status_code,
            detail=str(exc),
            headers={"X-Sidemantic-Query-ID": query_id, "X-Sidemantic-Request-ID": request_id},
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:
        record(
            error=type(exc).__name__,
            metadata={"queue_duration_ms": queue_ms, "timeout": control.timeout_diagnostic},
        )
        raise


def _consume_background_task(task: asyncio.Task) -> None:
    """Retrieve a detached timed-out worker's eventual exception."""
    try:
        task.exception()
    except (asyncio.CancelledError, Exception):
        pass


def _limit_query_sql(sql: str, max_rows: int) -> str:
    """Apply an outer ``LIMIT max_rows + 1`` so overflow is detectable."""
    # The rewriter's instrumentation is a trailing ``--`` comment, so the
    # closing parenthesis must begin on a fresh line rather than be commented.
    return f"SELECT * FROM ({sql}\n) AS _sidemantic_bounded LIMIT {max_rows + 1}"


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
