#!/usr/bin/env python3
"""Validate a Sidemantic YAML model file.

Usage:
    uv run python skills/sidemantic-modeler/scripts/validate-model.py models.yml
    uv run python skills/sidemantic-modeler/scripts/validate-model.py models/ --recursive

Parses the file with SidemanticAdapter, runs all validation checks,
and reports issues in a clear format. Exit code 0 = valid, 1 = errors found.
"""

import sys
from pathlib import Path

from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.validation import validate_metric, validate_model


def validate_graph(graph) -> list[str]:
    """Validate a parsed SemanticGraph. Returns list of errors."""
    errors = []

    # Validate models
    for model_name, model in graph.models.items():
        model_errors = validate_model(model)
        for err in model_errors:
            errors.append(f"[{model_name}] {err}")

    # Validate graph-level metrics
    for metric_name, metric in graph.metrics.items():
        metric_errors = validate_metric(metric, graph)
        for err in metric_errors:
            errors.append(f"[metric:{metric_name}] {err}")

    # Check relationship targets exist
    for model_name, model in graph.models.items():
        for rel in model.relationships:
            if rel.name not in graph.models:
                errors.append(f"[{model_name}] Relationship target '{rel.name}' not found in graph")

    # Check for duplicate dimension/metric names within models
    for model_name, model in graph.models.items():
        seen_dims = set()
        for dim in model.dimensions:
            if dim.name in seen_dims:
                errors.append(f"[{model_name}] Duplicate dimension '{dim.name}'")
            seen_dims.add(dim.name)

        seen_metrics = set()
        for m in model.metrics:
            if m.name in seen_metrics:
                errors.append(f"[{model_name}] Duplicate metric '{m.name}'")
            seen_metrics.add(m.name)

    # Check time dimensions have granularity
    for model_name, model in graph.models.items():
        for dim in model.dimensions:
            if dim.type == "time" and not dim.granularity:
                errors.append(f"[{model_name}] Time dimension '{dim.name}' missing granularity")

    return errors


def collect_files(path: Path, recursive: bool = False) -> list[Path]:
    """Collect YAML files from a path (file or directory)."""
    if path.is_file():
        return [path]
    if path.is_dir():
        pattern = "**/*.yml" if recursive else "*.yml"
        files = list(path.glob(pattern))
        pattern2 = "**/*.yaml" if recursive else "*.yaml"
        files.extend(path.glob(pattern2))
        return sorted(files)
    return []


def main():
    if len(sys.argv) < 2:
        print("Usage: validate-model.py <file_or_dir> [--recursive]")
        sys.exit(1)

    target = Path(sys.argv[1])
    recursive = "--recursive" in sys.argv

    if not target.exists():
        print(f"Error: {target} does not exist")
        sys.exit(1)

    files = collect_files(target, recursive)
    if not files:
        print(f"No YAML files found at {target}")
        sys.exit(1)

    total_errors = 0
    total_models = 0
    total_metrics = 0

    for f in files:
        print(f"\n--- {f} ---")

        try:
            adapter = SidemanticAdapter()
            graph = adapter.parse(f)
            total_models += len(graph.models)
            total_metrics += len(graph.metrics)
        except Exception as e:
            print(f"  PARSE ERROR: {e}")
            total_errors += 1
            continue

        errors = validate_graph(graph)

        if errors:
            total_errors += len(errors)
            for err in errors:
                print(f"  ERROR: {err}")
        else:
            models = ", ".join(graph.models.keys()) or "(none)"
            print(f"  OK: {len(graph.models)} models [{models}], {len(graph.metrics)} graph-level metrics")

    print(f"\n{'=' * 50}")
    print(f"Files: {len(files)}")
    print(f"Models: {total_models}")
    print(f"Graph-level metrics: {total_metrics}")
    print(f"Errors: {total_errors}")

    if total_errors > 0:
        print("\nValidation FAILED")
        sys.exit(1)
    else:
        print("\nValidation PASSED")
        sys.exit(0)


if __name__ == "__main__":
    main()
