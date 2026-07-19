"""HTTP API server for Sidemantic."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Literal

try:
    from fastapi import Depends, FastAPI, HTTPException, Query, Request, Security
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse, Response
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    from pydantic import BaseModel, ConfigDict, Field, model_validator
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
    explore: str | None = None
    saved_query: str | None = None

    def optional_list(self, field_name: str) -> list[str] | None:
        """Preserve an explicitly supplied empty list while treating omission as unset."""
        if field_name not in self.model_fields_set:
            return None
        return getattr(self, field_name)

    def resolved_filters(self) -> list[str] | None:
        filters = list(self.filters)
        supplied = "filters" in self.model_fields_set
        if self.where:
            filters.append(self.where)
            supplied = True
        return filters if supplied else None

    @model_validator(mode="after")
    def validate_saved_query_contract(self) -> StructuredQueryRequest:
        explicit_list_overrides = self.model_fields_set & {
            "dimensions",
            "metrics",
            "filters",
            "segments",
            "order_by",
        }
        if self.saved_query and (
            explicit_list_overrides
            or self.metrics
            or self.where is not None
            or self.filters
            or self.segments
            or self.order_by
            or self.limit is not None
            or self.offset is not None
            or self.ungrouped
            or self.parameters is not None
        ):
            raise ValueError("saved_query cannot be combined with structured query overrides")
        return self


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
            return [m for m in model.metrics if getattr(m, "public", True) and m.visibility == "public"]
        return list(model.metrics)

    @app.get("/models", dependencies=[Depends(require_auth)])
    def list_models() -> list[dict[str, Any]]:
        # Read-only metadata over the in-memory graph. Non-public fields are omitted
        # when the layer enforces visibility, so this catalog cannot enumerate them.
        current_layer = app.state.layer
        models = []
        for model_name, model in current_layer.graph.models.items():
            if current_layer.enforce_visibility and model.visibility != "public":
                continue
            models.append(
                {
                    "name": model_name,
                    "table": model.table,
                    "dimensions": [d.name for d in _visible_dimensions(current_layer, model)],
                    "metrics": [m.name for m in _visible_metrics(current_layer, model)],
                    "relationships": len(model.relationships),
                    "owner": model.owner,
                    "domain": model.domain,
                    "status": model.status,
                    "certification": model.certification,
                    "visibility": model.visibility,
                }
            )
        return models

    @app.get("/graph", dependencies=[Depends(require_auth)])
    def graph() -> dict[str, Any]:
        # Read-only metadata over the in-memory graph.
        from sidemantic.core.consumption import graph_metric_is_public, serialize_consumption_contract

        current_layer = app.state.layer
        graph_obj = current_layer.graph
        visible_model_names = {
            model_name
            for model_name, model in graph_obj.models.items()
            if not current_layer.enforce_visibility or model.visibility == "public"
        }

        models = []
        for model_name, model in graph_obj.models.items():
            if model_name not in visible_model_names:
                continue
            model_info = {
                "name": model_name,
                "table": model.table,
                "dimensions": [d.name for d in _visible_dimensions(current_layer, model)],
                "metrics": [m.name for m in _visible_metrics(current_layer, model)],
                "relationships": [
                    {"name": rel.name, "type": rel.type}
                    for rel in model.relationships
                    if rel.name in visible_model_names
                ],
            }
            if model.segments:
                model_info["segments"] = [segment.name for segment in model.segments]
            models.append(model_info)

        graph_metrics = []
        for metric_name, metric in graph_obj.metrics.items():
            if current_layer.enforce_visibility and not graph_metric_is_public(metric, graph_obj):
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
        model_names = [model_name for model_name in graph_obj.models if model_name in visible_model_names]
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
        payload["explores"] = [
            serialized
            for value in graph_obj.explores.values()
            if (
                serialized := serialize_consumption_contract(
                    value, graph_obj, enforce_visibility=current_layer.enforce_visibility
                )
            )
            is not None
        ]
        payload["saved_queries"] = [
            serialized
            for value in graph_obj.saved_queries.values()
            if (
                serialized := serialize_consumption_contract(
                    value, graph_obj, enforce_visibility=current_layer.enforce_visibility
                )
            )
            is not None
        ]
        return payload

    @app.get("/explores", dependencies=[Depends(require_auth)])
    def list_explores() -> list[dict[str, Any]]:
        from sidemantic.core.consumption import serialize_consumption_contract

        current_layer = app.state.layer
        return [
            serialized
            for value in current_layer.graph.explores.values()
            if (
                serialized := serialize_consumption_contract(
                    value, current_layer.graph, enforce_visibility=current_layer.enforce_visibility
                )
            )
            is not None
        ]

    @app.get("/saved-queries", dependencies=[Depends(require_auth)])
    def list_saved_queries() -> list[dict[str, Any]]:
        from sidemantic.core.consumption import serialize_consumption_contract

        current_layer = app.state.layer
        return [
            serialized
            for value in current_layer.graph.saved_queries.values()
            if (
                serialized := serialize_consumption_contract(
                    value, current_layer.graph, enforce_visibility=current_layer.enforce_visibility
                )
            )
            is not None
        ]

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
        for filter_str in filters or []:
            validate_filter_expression(filter_str, dialect=current_layer.dialect)
        sql = current_layer.compile(
            dimensions=payload.optional_list("dimensions"),
            metrics=payload.optional_list("metrics"),
            filters=filters,
            segments=payload.optional_list("segments"),
            order_by=payload.optional_list("order_by"),
            limit=payload.limit,
            offset=payload.offset,
            ungrouped=payload.ungrouped,
            parameters=payload.parameters,
            use_preaggregations=payload.use_preaggregations,
            user_attributes=user_attributes,
            timezone=payload.timezone,
            explore=payload.explore,
            saved_query=payload.saved_query,
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
        user_attributes = resolve_user_attributes(request)
        filters = payload.resolved_filters()
        for filter_str in filters or []:
            validate_filter_expression(filter_str, dialect=current_layer.dialect)
        sql = current_layer.compile(
            dimensions=payload.optional_list("dimensions"),
            metrics=payload.optional_list("metrics"),
            filters=filters,
            segments=payload.optional_list("segments"),
            order_by=payload.optional_list("order_by"),
            limit=payload.limit,
            offset=payload.offset,
            ungrouped=payload.ungrouped,
            parameters=payload.parameters,
            use_preaggregations=payload.use_preaggregations,
            user_attributes=user_attributes,
            timezone=payload.timezone,
            explore=payload.explore,
            saved_query=payload.saved_query,
        )
        table = _query_table(app, current_layer, sql, user_attributes=user_attributes)
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
        user_attributes = resolve_user_attributes(request)
        deny_free_sql_if_secured()
        query = _normalize_sql_query(payload.query)
        rewritten_sql = QueryRewriter(current_layer.graph, dialect=current_layer.dialect).rewrite(query)
        table = _query_table(app, current_layer, rewritten_sql, user_attributes=user_attributes)
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
        user_attributes = resolve_user_attributes(request)
        deny_free_sql_if_secured()
        query = _normalize_sql_query(payload.query)
        _require_select_statement(query)
        table = _query_table(app, current_layer, query, user_attributes=user_attributes)
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


def _query_table(app: FastAPI, layer: SemanticLayer, sql: str, user_attributes: dict | None = None) -> Any:
    """Return the Arrow table for ``sql``, served from the result cache if enabled.

    The cache is opt-in (``result_cache_mb`` > 0). The key covers the compiled
    SQL, dialect + connection fingerprint, layer generation, and the caller's
    security-scoped ``user_attributes`` so cached results never leak across
    users. Singleflight in ResultCache ensures a duplicate in-flight query runs
    the underlying execute exactly once.
    """
    cache = getattr(app.state, "result_cache", None)
    if cache is None:
        return _execute_to_table(layer, sql)

    key = layer.build_result_key(sql, user_attributes=user_attributes)
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
