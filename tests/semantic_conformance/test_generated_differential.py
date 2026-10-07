"""Seeded differential campaigns and replay of minimized JSON fixtures.

Configuration is environment-scoped to this module; no global pytest options
or engine defaults are changed. See differential/README.md for volume runs.
"""

import hashlib
import json
import os
from collections import Counter
from pathlib import Path
from time import perf_counter

import pytest

from tests.semantic_conformance.differential.generator import generate_case, population_cases
from tests.semantic_conformance.differential.harness import Case, DifferentialRunner, outcome_dict, runtime_provenance
from tests.semantic_conformance.differential.minimize import minimize


def positive_env(name, default, *, allow_zero=False):
    value = int(os.environ.get(name, default))
    if value < (0 if allow_zero else 1):
        raise ValueError(f"{name} must be {'nonnegative' if allow_zero else 'positive'}")
    return value


@pytest.fixture
def runner():
    if os.environ.get("SIDEMANTIC_DIFFERENTIAL_REQUIRE_RUST", "0") == "1":
        __import__("sidemantic_rs")
    else:
        pytest.importorskip("sidemantic_rs", reason="Differential execution requires the real Rust extension")
    instance = DifferentialRunner()
    yield instance
    instance.close()


def test_seeded_differential_campaign(runner, tmp_path, record_property):
    seed = int(os.environ.get("SIDEMANTIC_DIFFERENTIAL_SEED", "20261005"))
    count = positive_env("SIDEMANTIC_DIFFERENTIAL_CASES", "52")
    variants = positive_env("SIDEMANTIC_DIFFERENTIAL_DATA_VARIANTS", "1")
    start = positive_env("SIDEMANTIC_DIFFERENTIAL_START", "0", allow_zero=True)
    example_limit = positive_env("SIDEMANTIC_DIFFERENTIAL_EXAMPLES", "12", allow_zero=True)
    reduction_budget = positive_env("SIDEMANTIC_DIFFERENTIAL_REDUCTIONS", "100", allow_zero=True)
    output = Path(os.environ.get("SIDEMANTIC_DIFFERENTIAL_OUTPUT", str(tmp_path / "differential")))
    output.mkdir(parents=True, exist_ok=True)
    provenance = runtime_provenance()
    started = perf_counter()
    classes, fingerprints, families, features = Counter(), Counter(), Counter(), Counter()
    timing = Counter()
    selections, execution_successes = Counter(), Counter()
    compile_attempts, compile_successes, cache_hits = Counter(), Counter(), Counter()
    compiler_inputs = set()
    examples = []
    reduction_seconds = 0
    checked = 0

    def report():
        return {
            "runtime": provenance,
            "seed": seed,
            "start": start,
            "requested": count * variants,
            "requested_model_query_cases": count,
            "data_variants_per_input": variants,
            "unique_compiler_inputs": len(compiler_inputs),
            "actual_compile_attempts": dict(compile_attempts),
            "actual_successful_compilations": dict(compile_successes),
            "compilation_cache_hits": dict(cache_hits),
            "checked": checked,
            "checked_populations": checked,
            "passed": checked - sum(classes.values()),
            "failures": dict(classes),
            "fingerprints": dict(fingerprints),
            "families": dict(families),
            "features": dict(features),
            "timing_seconds": dict(timing),
            "reduction_seconds": reduction_seconds,
            "engine_selections": dict(selections),
            "successful_executions": dict(execution_successes),
            "elapsed_seconds": perf_counter() - started,
            "populations_per_second_excluding_reduction": checked
            / max(1e-9, perf_counter() - started - reduction_seconds),
            "examples": examples,
        }

    def cases():
        for index in range(start, start + count):
            yield from population_cases(generate_case(seed, index), variants)

    try:
        for case in cases():
            if case.data_variant == 0:
                compiler_inputs.add(hashlib.sha256(runner.compilation_key(case).encode()).hexdigest())
            outcome = runner.evaluate(case, reuse_compilation=variants > 1)
            checked += 1
            families[case.family] += 1
            features.update(case.features)
            for engine in ["python", "rust"]:
                result = getattr(outcome, engine)
                if result:
                    timing[f"{engine}_compile"] += result.compile_seconds
                    timing[f"{engine}_execute"] += result.execute_seconds
                    if result.compilation_reused:
                        cache_hits[engine] += 1
                    else:
                        compile_attempts[engine] += 1
                    if result.selection and not result.compilation_reused:
                        selections[f"{engine}:{result.selection['engine']}"] += 1
                        if result.stage not in {"compile", "selection"}:
                            compile_successes[engine] += 1
                    if result.stage == "complete":
                        execution_successes[engine] += 1
            if outcome.failure_class:
                classes[outcome.failure_class] += 1
                fingerprint = outcome.fingerprint(case)
                fingerprints[fingerprint] += 1
                if fingerprints[fingerprint] == 1 and len(examples) < example_limit:
                    prefix = f"{seed}-{case.index}-v{case.data_variant}-{fingerprint}"
                    case.write(output / f"{prefix}.original.json")
                    original_outcome = outcome_dict(outcome)
                    (output / f"{prefix}.evidence.json").write_text(
                        json.dumps({"original": original_outcome, "reduction_pending": True}, indent=2) + "\n"
                    )
                    (output / "report.json").write_text(json.dumps(report(), indent=2) + "\n")
                    reduction = None
                    if outcome.failure_class != "invalid_case" and reduction_budget:
                        reduction_started = perf_counter()
                        reduction = minimize(case, runner.evaluate, max_attempts=reduction_budget)
                        reduction_seconds += perf_counter() - reduction_started
                        reduction.case.write(output / f"{prefix}.case.json")
                    else:
                        case.write(output / f"{prefix}.case.json")
                    evidence = {
                        "original": original_outcome,
                        "minimized": outcome_dict(reduction.outcome) if reduction else None,
                        "attempts": reduction.attempts if reduction else 0,
                        "accepted": reduction.accepted if reduction else 0,
                        "budget_exhausted": reduction.exhausted if reduction else False,
                    }
                    (output / f"{prefix}.evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
                    examples.append(
                        {"fingerprint": fingerprint, "class": outcome.failure_class, "fixture": f"{prefix}.case.json"}
                    )
            # Checkpoint long runs so interruption retains useful accounting.
            if checked % 100 == 0:
                (output / "report.json").write_text(json.dumps(report(), indent=2) + "\n")
    finally:
        result = report()
        (output / "report.json").write_text(json.dumps(result, indent=2) + "\n")
        record_property("differential_report", str(output / "report.json"))
        record_property("differential_cases", checked)
    assert not classes, (
        f"Differential failures: {dict(classes)}; {len(fingerprints)} fingerprints; evidence: {output / 'report.json'}"
    )


def replay_paths():
    supplied = os.environ.get("SIDEMANTIC_DIFFERENTIAL_REPLAY")
    if supplied:
        path = Path(supplied)
        return sorted(path.glob("*.case.json")) if path.is_dir() else [path]
    return sorted((Path(__file__).parent / "differential" / "fixtures").glob("*.case.json"))


REPLAY_PATHS = replay_paths()
if REPLAY_PATHS:

    @pytest.mark.parametrize("path", REPLAY_PATHS, ids=lambda path: path.name)
    def test_replay_differential_fixture(runner, path):
        case = Case.read(path)
        outcome = runner.evaluate(case)
        assert outcome.failure_class is None, json.dumps(outcome_dict(outcome), indent=2)

elif os.environ.get("SIDEMANTIC_DIFFERENTIAL_REPLAY"):

    def test_replay_path_contains_fixtures():
        pytest.fail("SIDEMANTIC_DIFFERENTIAL_REPLAY did not contain any *.case.json fixtures")
