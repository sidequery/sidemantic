#!/usr/bin/env python3
"""Inspect a Sidemantic model directory and emit a webapp-oriented JSON spec."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


def _ref(model_name: str, field_name: str) -> str:
    return f"{model_name}.{field_name}"


def _field_summary(field: Any, *, model_name: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": field.name,
        "ref": _ref(model_name, field.name),
    }
    for attr in (
        "type",
        "agg",
        "sql",
        "granularity",
        "supported_granularities",
        "description",
        "format",
        "filters",
        "label",
        "value_format_name",
    ):
        value = getattr(field, attr, None)
        if value not in (None, "", []):
            payload[attr] = value
    return payload


def _relationship_summary(rel: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for attr in ("name", "type", "model", "foreign_key", "primary_key", "through"):
        value = getattr(rel, attr, None)
        if value not in (None, "", []):
            payload[attr] = value
    return payload


def _is_identifier_dimension(model: Any, dim: Any) -> bool:
    name = getattr(dim, "name", "")
    sql = (getattr(dim, "sql", None) or "").strip()
    primary_key = getattr(model, "primary_key", None)
    identifier_names = {"id", "uuid", "guid", "key"}
    if primary_key and name == primary_key:
        return True
    if name in identifier_names or name.endswith("_id") or name.endswith("_uuid") or name.endswith("_key"):
        return True
    if sql and (sql == primary_key or sql in identifier_names or sql.endswith("_id")):
        return True
    return False


def _output_name(ref: str) -> str:
    return ref.split(".", 1)[1] if "." in ref else ref


def _output_aliases(metrics: list[str], dimensions: list[str]) -> dict[str, str]:
    return {ref: _output_name(ref) for ref in [*dimensions, *metrics]}


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _execute_sample(layer: Any, sql: str, *, sample_rows: int) -> dict[str, Any]:
    result = layer.adapter.execute(sql)
    columns = [desc[0] for desc in result.description]
    rows = result.fetchmany(sample_rows)
    return {
        "columns": columns,
        "sample_rows": [
            {column: _json_value(value) for column, value in zip(columns, row, strict=False)} for row in rows
        ],
        "sample_row_count": len(rows),
    }


def _try_compile(
    generator: Any,
    *,
    layer: Any | None = None,
    execute: bool = False,
    sample_rows: int = 5,
    metrics: list[str],
    dimensions: list[str],
    filters: list[str] | None = None,
    order_by: list[str] | None = None,
    limit: int | None = None,
    ungrouped: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "metrics": metrics,
        "dimensions": dimensions,
        "filters": filters or [],
        "order_by": order_by or [],
        "limit": limit,
        "ungrouped": ungrouped,
        "output_aliases": _output_aliases(metrics, dimensions),
    }
    try:
        payload["sql"] = generator.generate(
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            order_by=order_by,
            limit=limit,
            ungrouped=ungrouped,
            skip_default_time_dimensions=True,
        )
        if execute and layer is not None:
            payload["result"] = _execute_sample(layer, payload["sql"], sample_rows=sample_rows)
    except Exception as exc:  # noqa: BLE001 - tool output should report model-specific failures.
        if "sql" in payload:
            payload["execution_error"] = str(exc)
        else:
            payload["error"] = str(exc)
    return payload


def _grain_for(model: Any, time_dim: Any) -> str:
    default = getattr(model, "default_grain", None)
    if default:
        return default
    return getattr(time_dim, "granularity", None) or "day"


def _matches_dimension(value: str, *, model_name: str, dim: Any) -> bool:
    return value in {getattr(dim, "name", ""), _ref(model_name, getattr(dim, "name", ""))}


def _sort_leaderboard_dimensions(dimensions: list[Any]) -> list[Any]:
    preferred_names = (
        "status",
        "state",
        "category",
        "product_area",
        "region",
        "country",
        "channel",
        "source",
        "type",
        "segment",
    )
    rank = {name: index for index, name in enumerate(preferred_names)}

    def score(dim: Any) -> tuple[int, int, str]:
        name = getattr(dim, "name", "")
        type_penalty = 0 if getattr(dim, "type", None) == "categorical" else 1
        return (rank.get(name, len(rank)), type_penalty, name)

    return sorted(dimensions, key=score)


def _leaderboard_dimension_summary(dim: Any, *, model: Any, model_name: str) -> dict[str, Any]:
    payload = {
        "name": getattr(dim, "name", ""),
        "ref": _ref(model_name, getattr(dim, "name", "")),
        "type": getattr(dim, "type", None),
    }
    if _is_identifier_dimension(model, dim):
        payload["identifier_like"] = True
    return payload


def _candidate_for_model(
    generator: Any,
    layer: Any,
    model_name: str,
    model: Any,
    *,
    max_metrics: int,
    max_dimensions: int,
    execute: bool,
    sample_rows: int,
    leaderboard_dimension: str | None,
) -> dict[str, Any]:
    metrics = list(getattr(model, "metrics", None) or [])
    dimensions = list(getattr(model, "dimensions", None) or [])
    time_dimensions = [dim for dim in dimensions if getattr(dim, "type", None) == "time"]
    all_leaderboard_dimensions = [dim for dim in dimensions if getattr(dim, "type", None) in ("categorical", "boolean")]
    preferred_leaderboard_dimensions = [
        dim for dim in all_leaderboard_dimensions if not _is_identifier_dimension(model, dim)
    ]
    candidate_leaderboard_dimensions = _sort_leaderboard_dimensions(
        preferred_leaderboard_dimensions or all_leaderboard_dimensions
    )
    if leaderboard_dimension:
        requested = [
            dim
            for dim in candidate_leaderboard_dimensions
            if _matches_dimension(leaderboard_dimension, model_name=model_name, dim=dim)
        ]
        if requested:
            candidate_leaderboard_dimensions = [
                *requested,
                *[dim for dim in candidate_leaderboard_dimensions if dim not in requested],
            ]
    leaderboard_dimensions = candidate_leaderboard_dimensions[:max_dimensions]

    metric_refs = [_ref(model_name, metric.name) for metric in metrics[:max_metrics]]
    selected_metric = metric_refs[0] if metric_refs else None

    time_dim_name = getattr(model, "default_time_dimension", None)
    time_dim = next((dim for dim in time_dimensions if dim.name == time_dim_name), None)
    if time_dim is None and time_dimensions:
        time_dim = time_dimensions[0]
    grain = _grain_for(model, time_dim) if time_dim is not None else None
    time_ref = f"{model_name}.{time_dim.name}__{grain}" if time_dim is not None else None

    queries: dict[str, Any] = {}
    if metric_refs and time_ref:
        queries["metric_series"] = _try_compile(
            generator,
            layer=layer,
            execute=execute,
            sample_rows=sample_rows,
            metrics=metric_refs,
            dimensions=[time_ref],
            order_by=[time_ref],
            limit=500,
        )
    if metric_refs:
        queries["metric_totals"] = _try_compile(
            generator,
            layer=layer,
            execute=execute,
            sample_rows=sample_rows,
            metrics=metric_refs,
            dimensions=[],
        )
    if selected_metric and leaderboard_dimensions:
        dim_ref = _ref(model_name, leaderboard_dimensions[0].name)
        queries["dimension_leaderboard"] = _try_compile(
            generator,
            layer=layer,
            execute=execute,
            sample_rows=sample_rows,
            metrics=[selected_metric],
            dimensions=[dim_ref],
            order_by=[f"{selected_metric} DESC"],
            limit=6,
        )
    if dimensions:
        preview_dims = [_ref(model_name, dim.name) for dim in dimensions[: min(max_dimensions, 8)]]
        queries["preview_rows"] = _try_compile(
            generator,
            layer=layer,
            execute=execute,
            sample_rows=sample_rows,
            metrics=[],
            dimensions=preview_dims,
            limit=50,
            ungrouped=True,
        )

    return {
        "model": model_name,
        "table": getattr(model, "table", None),
        "recommended_metrics": metric_refs,
        "recommended_dimensions": [_ref(model_name, dim.name) for dim in leaderboard_dimensions],
        "available_leaderboard_dimensions": [
            _leaderboard_dimension_summary(dim, model=model, model_name=model_name)
            for dim in candidate_leaderboard_dimensions
        ],
        "default_leaderboard_dimension": _ref(model_name, leaderboard_dimensions[0].name)
        if leaderboard_dimensions
        else None,
        "time_dimension": _ref(model_name, time_dim.name) if time_dim is not None else None,
        "time_grain": grain,
        "queries": queries,
    }


def inspect_layer(args: argparse.Namespace) -> dict[str, Any]:
    from sidemantic import SemanticLayer, load_from_directory
    from sidemantic.sql.generator import SQLGenerator

    connection = args.connection
    if args.db:
        connection = f"duckdb:///{Path(args.db).resolve()}"

    layer = SemanticLayer(connection=connection) if connection else SemanticLayer()
    load_from_directory(layer, str(args.models))
    generator = SQLGenerator(
        layer.graph,
        dialect=layer.dialect,
        preagg_database=getattr(layer, "preagg_database", None),
        preagg_schema=getattr(layer, "preagg_schema", None),
    )

    models = []
    candidates = []
    warnings = []
    execute = bool(args.execute or args.require_execute)
    if args.execute and not connection:
        execute = False
        warnings.append("--execute was requested without --db or --connection, so sample query execution was skipped.")
    if args.require_execute and not connection:
        execute = False
        warnings.append(
            "--require-execute was requested without --db or --connection, so sample query execution could not run."
        )
    for model_name, model in sorted(layer.graph.models.items()):
        dimensions = list(getattr(model, "dimensions", None) or [])
        metrics = list(getattr(model, "metrics", None) or [])
        models.append(
            {
                "name": model_name,
                "table": getattr(model, "table", None),
                "primary_key": getattr(model, "primary_key", None),
                "default_time_dimension": getattr(model, "default_time_dimension", None),
                "default_grain": getattr(model, "default_grain", None),
                "dimensions": [_field_summary(dim, model_name=model_name) for dim in dimensions],
                "metrics": [_field_summary(metric, model_name=model_name) for metric in metrics],
                "relationships": [_relationship_summary(rel) for rel in (getattr(model, "relationships", None) or [])],
            }
        )
        candidates.append(
            _candidate_for_model(
                generator,
                layer,
                model_name,
                model,
                max_metrics=args.max_metrics,
                max_dimensions=args.max_dimensions,
                execute=execute,
                sample_rows=args.sample_rows,
                leaderboard_dimension=args.leaderboard_dimension,
            )
        )

    graph_metrics = []
    for metric_name, metric in sorted(layer.graph.metrics.items()):
        graph_metrics.append(_field_summary(metric, model_name="metrics") | {"name": metric_name, "ref": metric_name})

    return {
        "models_path": str(args.models.resolve()),
        "connection": connection,
        "dialect": layer.dialect,
        "model_count": len(models),
        "models": models,
        "graph_metrics": graph_metrics,
        "app_candidates": candidates,
        "warnings": warnings,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("models", type=Path, help="Directory containing Sidemantic model files")
    parser.add_argument("--connection", help="Sidemantic connection string")
    parser.add_argument("--db", type=Path, help="DuckDB database path, shorthand for duckdb:///...")
    parser.add_argument("--output", "-o", type=Path, help="Write JSON spec to this path")
    parser.add_argument("--max-metrics", type=int, default=6, help="Max metrics per app candidate")
    parser.add_argument("--max-dimensions", type=int, default=12, help="Max dimensions per app candidate")
    parser.add_argument(
        "--leaderboard-dimension",
        help="Preferred default leaderboard dimension name or model.dimension reference when present",
    )
    parser.add_argument("--execute", action="store_true", help="Execute compiled app queries and include sample rows")
    parser.add_argument(
        "--require-execute",
        action="store_true",
        help="Require all generated app queries to execute successfully; exits nonzero on missing execution",
    )
    parser.add_argument("--sample-rows", type=int, default=5, help="Sample rows to include per executed query")
    args = parser.parse_args()

    if not args.models.exists():
        print(f"Models directory not found: {args.models}", file=sys.stderr)
        return 2

    payload = inspect_layer(args)
    text = json.dumps(payload, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
        print(f"Wrote {args.output}", file=sys.stderr)
    else:
        print(text)
    if args.require_execute:
        failures: list[str] = []
        for candidate in payload["app_candidates"]:
            for query_name, query in candidate["queries"].items():
                if "result" not in query:
                    failures.append(f"{candidate['model']}.{query_name}: missing executed result")
                if query.get("execution_error"):
                    failures.append(f"{candidate['model']}.{query_name}: {query['execution_error']}")
                if query.get("error"):
                    failures.append(f"{candidate['model']}.{query_name}: {query['error']}")
        if failures:
            print("Execution validation failed:", file=sys.stderr)
            for failure in failures:
                print(f"- {failure}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
