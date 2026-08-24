#!/usr/bin/env python3
"""Run one differential fixture through the local Sidemantic DuckDB path.

The Bun runner owns the official Malloy process. This small process is kept
separate so the comparison uses the same fixture files and a real Python
Sidemantic query without adding a runtime network dependency.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _seed(workspace: Path, seed_file: Path) -> None:
    import duckdb

    (workspace / "data").mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(workspace / "fixture.duckdb")) as connection:
        connection.execute(seed_file.read_text())


def _run_query(workspace: Path, source_file: Path, query: dict[str, Any]) -> dict[str, Any]:
    from sidemantic import Explore, SemanticLayer
    from sidemantic.adapters.malloy import MalloyAdapter

    graph = MalloyAdapter(strict=True).parse(source_file)
    model_name = query.get("model")
    if not model_name:
        raise ValueError("fixture query is missing its manifest model")
    if model_name not in graph.models:
        raise ValueError(f"fixture model {model_name!r} was not imported")

    explore_name = f"__malloy_differential_root_{model_name}"
    suffix = 2
    while explore_name in graph.explores:
        explore_name = f"__malloy_differential_root_{model_name}_{suffix}"
        suffix += 1
    layer = SemanticLayer(
        connection=f"duckdb:///{workspace / 'fixture.duckdb'}",
        auto_register=False,
        engine="python",
    )
    # Exercise the normal public registration path so intrinsic physical
    # dimensions are discovered before the differential query is validated.
    for model in graph.models.values():
        layer.add_model(model)
    layer.add_explore(Explore(name=explore_name, model=model_name))
    result = layer.query(
        metrics=query["metrics"],
        dimensions=query["dimensions"],
        filters=query.get("filters"),
        segments=query.get("segments"),
        order_by=query.get("order_by"),
        limit=query.get("limit"),
        explore=explore_name,
    )
    columns = [column[0] for column in result.description]
    rows = [{column: _json_value(value) for column, value in zip(columns, row)} for row in result.fetchall()]
    return {"schema": columns, "rows": rows}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--query", type=json.loads, required=True)
    args = parser.parse_args()

    workspace = args.workspace.resolve()
    os.chdir(workspace)
    _seed(workspace, args.seed.resolve())
    query = dict(args.query)
    query["model"] = args.model
    print(json.dumps(_run_query(workspace, args.source.resolve(), query), separators=(",", ":")))


if __name__ == "__main__":
    main()
