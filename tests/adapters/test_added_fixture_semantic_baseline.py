import json
from pathlib import Path

from tests.adapters.test_added_fixture_coverage import (
    ADDED_FIXTURE_CASES,
    ADDED_FIXTURE_EXPECTED_FAILURE_CASES,
    _pick_compile_query,
)

BASELINE_PATH = Path("tests/adapters/added_fixture_semantic_baseline.json")


def _compute_fixture_semantic_baseline() -> dict[str, dict[str, int | bool]]:
    failure_paths = {path for _, path, _ in ADDED_FIXTURE_EXPECTED_FAILURE_CASES}
    observed: dict[str, dict[str, int | bool]] = {}

    for adapter_cls, fixture_path in ADDED_FIXTURE_CASES:
        if fixture_path in failure_paths:
            continue

        graph = adapter_cls().parse(fixture_path)
        observed[fixture_path] = {
            "models": len(graph.models),
            "model_dimensions": sum(len(model.dimensions) for model in graph.models.values()),
            "model_metrics": sum(len(model.metrics) for model in graph.models.values()),
            "model_relationships": sum(len(model.relationships) for model in graph.models.values()),
            "model_segments": sum(len(model.segments) for model in graph.models.values()),
            "graph_metrics": len(graph.metrics),
            "has_compile_candidate": _pick_compile_query(graph) is not None,
        }

    return dict(sorted(observed.items()))


def test_added_fixture_semantic_baseline_stable():
    expected = json.loads(BASELINE_PATH.read_text())
    observed = _compute_fixture_semantic_baseline()

    expected_keys = set(expected)
    observed_keys = set(observed)
    assert observed_keys == expected_keys, (
        "Fixture set drifted.\n"
        f"Missing from observed: {sorted(expected_keys - observed_keys)}\n"
        f"New in observed: {sorted(observed_keys - expected_keys)}"
    )

    mismatches = []
    for fixture_path in sorted(expected):
        if observed[fixture_path] != expected[fixture_path]:
            mismatches.append((fixture_path, expected[fixture_path], observed[fixture_path]))

    assert not mismatches, "Fixture semantic baseline drifted.\n" + "\n".join(
        f"{path}\n  expected={expected_stats}\n  observed={observed_stats}"
        for path, expected_stats, observed_stats in mismatches
    )
