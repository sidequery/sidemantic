"""Behavioral contracts for generation, comparison, classification and reduction."""

import json
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal

import pytest

from .generator import FAMILIES, data_variant, generate_case, population_cases
from .harness import Case, DifferentialRunner, EngineResult, InvalidCaseError, Outcome, rows_equal, validate_case
from .minimize import minimize


@pytest.mark.parametrize(
    "left,right,ordered,expected",
    [
        ([(None, 1), ("a", 2)], [("a", 2), (None, 1)], False, True),
        ([(None, 1), ("a", 2)], [("a", 2), (None, 1)], True, False),
        ([(1,), (1,)], [(1,), (2,)], False, False),
        ([(Decimal("1.25"),)], [(1.2500000001,)], False, True),
        ([(1.25,)], [(1.2501,)], False, False),
        ([(2**60,)], [(2**60 + 1,)], False, False),
        ([(None,)], [(0,)], False, False),
        ([(True,)], [(1,)], False, False),
        ([(float("nan"),)], [(float("nan"),)], False, True),
        ([(float("inf"),)], [(-float("inf"),)], False, False),
        ([(date(2024, 1, 1),)], [(datetime(2024, 1, 1),)], False, True),
        ([(date(2024, 1, 1),)], [(datetime(2024, 1, 1, 1),)], False, False),
        ([(1,)], [(1, 2)], False, False),
        ([], [(1,)], False, False),
    ],
)
def test_comparison_preserves_values_order_and_multiplicity(left, right, ordered, expected):
    assert rows_equal(left, right, ordered=ordered) is expected


def test_tolerant_multiset_matching_reassigns_an_earlier_match():
    # The first left row matches either right row, but the second only matches
    # the first. A greedy matcher would incorrectly reject equal multisets.
    assert rows_equal([(0.1,), (0.0,)], [(0.0,), (0.2,)], ordered=False, abs_tol=0.11, rel_tol=0)


def test_seeded_generation_is_valid_replayable_and_varies_definitions(tmp_path):
    cases = [generate_case(20261005, index) for index in range(120)]
    assert {case.family for case in cases} == set(FAMILIES)
    assert len({json.dumps(case.models, sort_keys=True) for case in cases}) > 90
    joined_metrics = {metric for case in cases[:52] if len(case.models) > 1 for metric in case.query["metrics"]}
    assert {"facts.filtered", "facts.derived", "facts.ratio", "facts.running", "facts.nested"} <= joined_metrics
    for case in cases:
        if case.family == "default_time":
            assert case.models[0]["default_time_dimension"] == "day"
            assert case.query["dimensions"] == []
            assert not {"order_by", "limit", "offset"} & case.query.keys()
    for case in cases:
        validate_case(case)
        assert case.to_dict() == generate_case(case.seed, case.index).to_dict()
        assert case.to_dict() != generate_case(case.seed + 1, case.index).to_dict()
    path = tmp_path / "replay.case.json"
    cases[7].write(path)
    assert Case.read(path).to_dict() == cases[7].to_dict()


def small_case():
    return Case(
        seed=1,
        index=0,
        family="injected_sum_regression",
        models=[
            {
                "name": "facts",
                "table": "generated_facts",
                "primary_key": "id",
                "dimensions": [{"name": "category", "type": "categorical"}],
                "metrics": [{"name": "total", "agg": "sum", "sql": "amount"}, {"name": "unused", "agg": "count"}],
            },
            {
                "name": "unused",
                "table": "generated_unused",
                "primary_key": "id",
                "dimensions": [{"name": "id", "type": "numeric"}],
            },
        ],
        metrics=[],
        query={
            "metrics": ["facts.total"],
            "dimensions": ["facts.category"],
            "order_by": ["facts.category"],
            "filters": ["facts.category = 'a'"],
        },
        tables=[
            {
                "name": "generated_facts",
                "columns": [["id", "integer"], ["category", "varchar"], ["amount", "integer"]],
                "rows": [[1, "a", 2], [2, "a", 3], [3, "a", 0]],
            },
            {"name": "generated_unused", "columns": [["id", "integer"]], "rows": [[9]]},
        ],
    )


def test_minimizer_preserves_real_result_failure_and_emits_replay(tmp_path):
    runner = DifferentialRunner()

    def injected_aggregate_regression(case):
        # Execute genuine Python-compiled SQL, then inject SUM -> MAX into a
        # second executable query. This creates a known observable discrepancy
        # without depending on any outstanding production compiler defect.
        runner._load(case)
        result = runner._engine(case, "python")
        if result.error:
            return Outcome("python_error", result)
        faulty = result.sql.replace("SUM(", "MAX(")
        cursor = runner.adapter.execute(faulty)
        wrong = EngineResult(columns=[c[0] for c in cursor.description], rows=cursor.fetchall(), sql=faulty)
        return Outcome(None if rows_equal(result.rows, wrong.rows, ordered=case.ordered) else "rows", result, wrong)

    try:
        original = small_case()
        first = minimize(original, injected_aggregate_regression, max_attempts=250)
        second = minimize(original, injected_aggregate_regression, max_attempts=250)
        assert first.case.to_dict() == second.case.to_dict()
        assert first.outcome.failure_class == "rows"
        assert first.accepted > 0
        assert len(first.case.tables) == len(first.case.models) == 1
        assert len(first.case.tables[0]["rows"]) == 2
        assert first.case.tables[0]["columns"] == [["id", "integer"], ["category", "varchar"], ["amount", "integer"]]
        assert first.case.query == {"metrics": ["facts.total"], "dimensions": ["facts.category"], "filters": []}
        assert [m["name"] for m in first.case.models[0]["metrics"]] == ["total"]
        validate_case(first.case)
        path = tmp_path / "reduced.case.json"
        first.case.write(path)
        assert injected_aggregate_regression(Case.read(path)).failure_class == "rows"
        assert original.to_dict() == small_case().to_dict()
        # Removing either necessary row eliminates the injected failure.
        for index in range(2):
            candidate = deepcopy(first.case)
            candidate.tables[0]["rows"].pop(index)
            assert injected_aggregate_regression(candidate).failure_class is None
    finally:
        runner.close()


def test_minimizer_respects_budget_and_rejects_changed_failure_class():
    case = small_case()

    def failure(candidate):
        return Outcome("rows" if candidate.to_dict() == case.to_dict() else "rust:compile:ValueError")

    reduction = minimize(case, failure, max_attempts=3)
    assert reduction.attempts == 3 and reduction.exhausted
    assert reduction.case.to_dict() == case.to_dict()


def test_minimizer_does_not_exchange_one_binder_failure_for_another():
    case = small_case()

    def failure(candidate):
        diagnostic = (
            'Referenced table "facts" not found'
            if candidate.to_dict() == case.to_dict()
            else 'Referenced column "missing" not found'
        )
        return Outcome(
            "rust:execute:BinderException",
            rust=EngineResult(error=diagnostic, error_type="BinderException", stage="execute"),
        )

    reduction = minimize(case, failure, max_attempts=15)
    assert reduction.accepted == 0
    assert reduction.case.to_dict() == case.to_dict()


@pytest.mark.parametrize("change", ["cardinality", "order_only", "column", "witness", "different_metric"])
def test_minimizer_cannot_substitute_an_unrelated_result_discrepancy(change):
    case = small_case()

    def failure(candidate):
        columns = ["total", "unused"]
        left, right = [(5, 10)], [(3, 10)]
        if candidate.to_dict() != case.to_dict():
            if change == "cardinality":
                left = [(5, 10), (9, 10)]
            elif change == "order_only":
                left, right = [(5, 10), (3, 10)], [(3, 10), (5, 10)]
            elif change == "column":
                columns = ["another_metric", "unused"]
            elif change == "witness":
                left, right = [(7, 10)], [(4, 10)]
            else:
                right = [(5, 7)]
        return Outcome("rows", EngineResult(columns=columns, rows=left), EngineResult(columns=columns, rows=right))

    reduction = minimize(case, failure, max_attempts=20)
    assert reduction.accepted == 0
    assert reduction.case.to_dict() == case.to_dict()


def test_result_witness_is_stable_under_unordered_row_permutations():
    first = Outcome(
        "rows",
        EngineResult(columns=["total"], rows=[(1,), (5,), (3,)]),
        EngineResult(columns=["total"], rows=[(1,), (6,), (3,)]),
    )
    permuted = deepcopy(first)
    permuted.python.rows.reverse()
    permuted.rust.rows = [(6,), (3,), (1,)]
    assert first.failure_identity() == permuted.failure_identity()
    assert first.failure_identity()[1] == "values"


@pytest.mark.parametrize("engines", [{"python"}, {"rust"}, {"python", "rust"}])
def test_one_or_both_engine_errors_are_failures(monkeypatch, engines):
    runner = DifferentialRunner()

    def execute(case, engine):
        if engine in engines:
            return EngineResult(error="deliberate compiler failure", error_type="ValueError", stage="compile")
        return EngineResult(columns=["total"], rows=[(5,)], stage="complete")

    monkeypatch.setattr(runner, "_engine", execute)
    try:
        outcome = runner.evaluate(small_case())
        assert outcome.failure_class is not None
        assert outcome.failure_class != "invalid_case"
        for engine in engines:
            assert f"{engine}:compile:ValueError" in outcome.failure_class
    finally:
        runner.close()


def test_engine_planning_cannot_mutate_the_other_request_or_fixture(monkeypatch):
    case = small_case()
    original = deepcopy(case.to_dict())
    seen = []
    from sidemantic import SemanticLayer

    def compile_mutating_request(self, **query):
        seen.append(deepcopy(query))
        query["dimensions"].append("mutated")
        self.last_engine_selection = {"engine": "python", "reason": None}
        return "SELECT 1 AS total"

    monkeypatch.setattr(SemanticLayer, "compile", compile_mutating_request)
    runner = DifferentialRunner()
    try:
        for _ in range(2):
            assert runner._engine(case, "python").error is None
        assert seen == [original["query"], original["query"]]
        assert case.to_dict() == original
    finally:
        runner.close()


def test_independently_seeded_populations_preserve_immutable_inputs():
    for index in range(len(FAMILIES)):
        base = generate_case(20261005, index)
        original = deepcopy(base.to_dict())
        variants = list(population_cases(base, 12))
        assert len({json.dumps(case.tables, sort_keys=True) for case in variants}) == 12
        assert {DifferentialRunner.compilation_key(case) for case in variants} == {
            DifferentialRunner.compilation_key(base)
        }
        for case in variants:
            validate_case(case)
            assert data_variant(base, case.data_variant).to_dict() == case.to_dict()
        assert base.to_dict() == original


def test_compile_reuse_still_executes_new_population_and_default_bypasses_cache(monkeypatch):
    runner = DifferentialRunner()
    original_compile = runner._compile
    calls = []

    def compile(case, engine):
        calls.append(engine)
        return original_compile(case, engine)

    monkeypatch.setattr(runner, "_compile", compile)
    try:
        first = small_case()
        runner._load(first)
        initial = runner._engine(first, "python", reuse_compilation=True)
        second = deepcopy(first)
        second.tables[0]["rows"][0][2] = 4
        runner._load(second)
        changed = runner._engine(second, "python", reuse_compilation=True)
        assert not initial.compilation_reused and changed.compilation_reused
        assert initial.rows == [("a", 5)] and changed.rows == [("a", 7)]
        assert calls == ["python"]
        assert changed.selection == {"engine": "python", "reason": None}
        # Replay and minimization omit reuse_compilation and must compile again.
        replayed = runner._engine(second, "python")
        assert not replayed.compilation_reused
        assert replayed.rows == changed.rows
        assert calls == ["python", "python"]
    finally:
        runner.close()


@pytest.mark.parametrize("change", ["model", "query", "schema", "graph_metric"])
def test_compile_reuse_rejects_any_changed_compiler_input(monkeypatch, change):
    runner = DifferentialRunner()
    original_compile = runner._compile
    calls = []

    def compile(case, engine):
        calls.append(engine)
        return original_compile(case, engine)

    monkeypatch.setattr(runner, "_compile", compile)
    try:
        case = small_case()
        runner._load(case)
        assert runner._engine(case, "python", reuse_compilation=True).error is None
        changed = deepcopy(case)
        if change == "model":
            changed.models[0]["metrics"][0]["agg"] = "max"
        elif change == "query":
            changed.query["filters"] = []
        elif change == "schema":
            changed.tables[0]["columns"][2][1] = "double"
        else:
            changed.metrics = [{"name": "global", "type": "derived", "sql": "facts.total + 1"}]
        validate_case(changed)
        runner._load(changed)
        result = runner._engine(changed, "python", reuse_compilation=True)
        assert result.error is None
        assert not result.compilation_reused
        assert calls == ["python", "python"]
    finally:
        runner.close()


@pytest.mark.parametrize("damage", ["row_width", "duplicate_key", "null_key", "type", "model", "schema", "version"])
def test_validation_reuse_checks_every_population_and_invalidates_changed_definitions(monkeypatch, damage):
    runner = DifferentialRunner()
    executions = []

    def execute(case, engine, **kwargs):
        executions.append(engine)
        return EngineResult(columns=["total"], rows=[(5,)], stage="complete")

    monkeypatch.setattr(runner, "_engine", execute)
    try:
        case = small_case()
        assert runner.evaluate(case, reuse_compilation=True).failure_class is None
        changed = deepcopy(case)
        changed.tables[0]["rows"][0][2] = 8
        assert runner.evaluate(changed, reuse_compilation=True).failure_class is None
        assert runner.definition_validations == 1
        assert runner.definition_validation_cache_hits == 1
        damaged = deepcopy(changed)
        if damage == "row_width":
            damaged.tables[0]["rows"][0].pop()
        elif damage == "duplicate_key":
            damaged.tables[0]["rows"][1][0] = damaged.tables[0]["rows"][0][0]
        elif damage == "null_key":
            damaged.tables[0]["rows"][0][0] = None
        elif damage == "type":
            damaged.tables[0]["rows"][0][2] = "not a number"
        elif damage == "model":
            damaged.models[0]["metrics"][0]["sql"] = "missing_physical_column"
        elif damage == "schema":
            damaged.tables[0]["columns"][2][0] = "changed_column"
        else:
            damaged.version = 2
        outcome = runner.evaluate(damaged, reuse_compilation=True)
        assert outcome.failure_class == "invalid_case"
        assert len(executions) == 4
        # Replay/minimization always repeat full definition validation too.
        assert runner.evaluate(changed).failure_class is None
        assert runner.definition_validations == 2
    finally:
        runner.close()


@pytest.mark.parametrize("damage", ["reference", "schema", "order", "primary_key"])
def test_invalid_generator_cases_are_separate(damage):
    case = small_case()
    if damage == "reference":
        case.query["metrics"] = ["facts.missing"]
    elif damage == "schema":
        case.tables[0]["rows"][0].pop()
    elif damage == "order":
        case.query["order_by"] = ["facts.total"]
    else:
        case.tables[0]["rows"][1][0] = 1
    with pytest.raises(InvalidCaseError):
        validate_case(case)
    runner = DifferentialRunner()
    try:
        outcome = runner.evaluate(case)
        assert outcome.failure_class == "invalid_case"
        assert outcome.python is outcome.rust is None
    finally:
        runner.close()
