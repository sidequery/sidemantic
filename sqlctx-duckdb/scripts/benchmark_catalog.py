# /// script
# requires-python = ">=3.11"
# ///
"""Reproduce catalog and routing costs with a matching DuckDB shell/extension.

uv run --no-project sqlctx-duckdb/scripts/benchmark_catalog.py --binary /path/duckdb \
    --extension /path/sqlctx.duckdb_extension

Times come from the shell, excluding process startup/model loading. CPU samples
retain sub-millisecond precision where the shell rounds wall time to milliseconds.
Run the same command with an earlier extension to compare identical workloads.
"""

import argparse
import json
import re
import statistics
import subprocess
from decimal import Decimal
from pathlib import Path


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def setup(models, metrics):
    declarations = []
    for model in range(models):
        fields = "\n".join(f"sum(amount) AS metric_{metric}" for metric in range(metrics))
        declarations.append(f"MODEL model_{model} FROM facts (\nprimary key(id)\ncategory\n{fields}\n);")
    return "\n".join(declarations)


def validate_results(results, models, metrics):
    if results["ordinary_constant"] != [{"42": 42}]:
        raise RuntimeError(f"Incorrect ordinary constant: {results['ordinary_constant']}")
    for name in ("ordinary_aggregate", "one_metric"):
        if name not in results:
            continue
        rows = results[name]
        if len(rows) != 1 or Decimal(str(next(iter(rows[0].values())))) != Decimal(499500):
            raise RuntimeError(f"Incorrect {name}: {rows}")
    if "list_views" not in results:
        return
    expected_models = [{"table_name": name} for name in sorted(f"model_{i}" for i in range(models))]
    expected_columns = [{"column_name": "category", "data_type": "VARCHAR"}] + [
        {"column_name": f"metric_{i}", "data_type": "DECIMAL(38,2)"} for i in range(metrics)
    ]
    expected_names = [{"column_name": row["column_name"]} for row in expected_columns]
    for name, expected in (
        ("list_views", expected_models),
        ("one_model_columns", expected_columns),
        ("one_model_column_names", expected_names),
    ):
        if results[name] != expected:
            raise RuntimeError(f"Incorrect {name}: expected {expected}, received {results[name]}")
    expected_metrics = sorted((f"metric_{i}", "DECIMAL(38,2)", "sum") for i in range(metrics))
    actual_metrics = sorted((row["name"], row["data_type"], row["aggregation"]) for row in results["show_metrics"])
    if actual_metrics != expected_metrics:
        raise RuntimeError(f"Incorrect show_metrics: expected {expected_metrics}, received {actual_metrics}")


def run(binary, extension, models, metrics, repeats, warmups):
    queries = {
        "ordinary_constant": "select 42;",
        "ordinary_aggregate": "select sum(amount) from facts;",
    }
    if extension:
        queries.update(
            list_views="select table_name from information_schema.tables "
            "where table_schema = 'semantic' order by table_name;",
            one_model_columns="select column_name, data_type from information_schema.columns "
            "where table_schema = 'semantic' and table_name = 'model_0' order by ordinal_position;",
            one_model_column_names="select column_name from information_schema.columns "
            "where table_schema = 'semantic' and table_name = 'model_0' order by ordinal_position;",
            one_metric="select metric_0 from semantic.model_0;",
            show_metrics="show metrics from model_0;",
        )
    statements = [
        ".output /dev/null",
        "set threads = 1;",
        "create table facts as select i as id, (i % 4)::varchar as category, i::decimal(12,2) as amount "
        "from range(1000) t(i);",
    ]
    if extension:
        statements.extend([f"load {literal(extension)};", setup(models, metrics)])
    # Execute the exact timed queries with visible results in the same session.
    # A truncated metadata scan must fail validation, not count as a speedup.
    statements.extend([".mode json", ".output stdout", *queries.values(), ".output /dev/null"])
    # Round-robin samples reduce bias from CPU frequency/load drift. Warm every
    # shape before recording; preserve every sample instead of only a headline.
    statements.extend(query for _ in range(warmups) for query in queries.values())
    statements.append(".timer on")
    statements.extend(query for _ in range(repeats) for query in queries.values())
    result = subprocess.run(
        [str(binary), "-unsigned", "-no-init", "-batch", "-bail", ":memory:"],
        input="\n".join(statements) + "\n",
        capture_output=True,
        text=True,
        timeout=180,
    )
    if result.returncode:
        raise RuntimeError(result.stderr)
    decoder = json.JSONDecoder()
    remaining = result.stdout.lstrip()
    results = {}
    for name in queries:
        rows, end = decoder.raw_decode(remaining)
        results[name] = rows
        remaining = remaining[end:].lstrip()
    validate_results(results, models, metrics)
    timings = re.findall(r"Run Time \(s\): real ([\d.]+) user ([\d.]+) sys ([\d.]+)", remaining)
    if len(timings) != repeats * len(queries):
        raise RuntimeError(f"Expected {repeats * len(queries)} timings, got {len(timings)}: {result.stdout}")
    report = {}
    for index, (name, query) in enumerate(queries.items()):
        samples = timings[index :: len(queries)]
        wall = [float(real) * 1000 for real, _, _ in samples]
        cpu = [(float(user) + float(system)) * 1000 for _, user, system in samples]
        report[name] = {
            "sql": query,
            "validated_row_count": len(results[name]),
            "validated_result": results[name]
            if name != "show_metrics"
            else [{key: row[key] for key in ("name", "data_type", "aggregation")} for row in results[name]],
            "wall_ms": wall,
            "cpu_ms": cpu,
            "median_wall_ms": statistics.median(wall),
            "median_cpu_ms": statistics.median(cpu),
        }
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--extension", type=Path, required=True)
    parser.add_argument("--models", type=int, default=25)
    parser.add_argument("--metrics", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--warmups", type=int, default=2)
    args = parser.parse_args()
    version = subprocess.check_output([str(args.binary), "--version"], text=True).strip()
    print(
        json.dumps(
            {
                "host": version,
                "extension": str(args.extension.resolve()),
                "models": args.models,
                "metrics_per_model": args.metrics,
                "repeats": args.repeats,
                "warmups": args.warmups,
                "without_extension": run(args.binary, None, args.models, args.metrics, args.repeats, args.warmups),
                "with_extension": run(
                    args.binary, args.extension, args.models, args.metrics, args.repeats, args.warmups
                ),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
