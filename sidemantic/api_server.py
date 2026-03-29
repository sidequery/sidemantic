"""HTTP API server for Sidemantic."""

from __future__ import annotations

import threading
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
    reader_to_arrow_bytes,
    record_batch_reader_to_table,
    result_to_record_batch_reader,
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
            body = await request.body()
            if len(body) > self.max_body_bytes:
                return JSONResponse(
                    {"error": f"Request body exceeds {self.max_body_bytes} bytes"},
                    status_code=413,
                )
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
    )
    uvicorn.run(app, host=host, port=port, log_level="info")


def create_app(
    layer: SemanticLayer,
    auth_token: str | None = None,
    cors_origins: list[str] | None = None,
    max_request_body_bytes: int = 1024 * 1024,
) -> FastAPI:
    """Create a FastAPI app for a loaded semantic layer."""
    app = FastAPI(title="Sidemantic API", version=__version__)
    app.state.layer = layer
    app.state.lock = threading.RLock()
    app.state.auth_token = auth_token

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
        with app.state.lock:
            current_layer = app.state.layer
            return {
                "status": "ok",
                "version": __version__,
                "dialect": current_layer.dialect,
                "model_count": len(current_layer.graph.models),
            }

    @app.get("/models", dependencies=[Depends(require_auth)])
    def list_models() -> list[dict[str, Any]]:
        with app.state.lock:
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
        with app.state.lock:
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

    @app.post("/compile", dependencies=[Depends(require_auth)])
    def compile_query(payload: StructuredQueryRequest) -> dict[str, str]:
        with app.state.lock:
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
            )
            return {"sql": sql}

    @app.post("/query", dependencies=[Depends(require_auth)])
    def run_query(
        payload: StructuredQueryRequest,
        request: Request,
        format: Literal["json", "arrow"] | None = Query(default=None),
    ):
        with app.state.lock:
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
            )
            result = current_layer.adapter.execute(sql)
            return _build_query_response(request, current_layer, result, sql=sql, format_override=format)

    @app.post("/sql/compile", dependencies=[Depends(require_auth)])
    def compile_sql(payload: SQLRequest) -> dict[str, str]:
        with app.state.lock:
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
        with app.state.lock:
            current_layer = app.state.layer
            query = _normalize_sql_query(payload.query)
            rewritten_sql = QueryRewriter(current_layer.graph, dialect=current_layer.dialect).rewrite(query)
            result = current_layer.adapter.execute(rewritten_sql)
            return _build_query_response(
                request,
                current_layer,
                result,
                sql=rewritten_sql,
                original_sql=query,
                format_override=format,
            )

    return app


def _build_query_response(
    request: Request,
    layer: SemanticLayer,
    result: Any,
    sql: str,
    format_override: Literal["json", "arrow"] | None,
    original_sql: str | None = None,
):
    reader = result_to_record_batch_reader(result, layer.adapter)
    response_format = _resolve_response_format(request, format_override)

    if response_format == ARROW_FORMAT:
        body, row_count = reader_to_arrow_bytes(reader)
        return Response(
            content=body,
            media_type=ARROW_STREAM_MEDIA_TYPE,
            headers={
                "X-Sidemantic-Row-Count": str(row_count),
                "X-Sidemantic-Dialect": layer.dialect,
            },
        )

    table = record_batch_reader_to_table(reader)
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

    statements = sqlglot.parse(normalized)
    if len(statements) > 1:
        raise ValueError("Multiple SQL statements are not supported")
    return normalized


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
