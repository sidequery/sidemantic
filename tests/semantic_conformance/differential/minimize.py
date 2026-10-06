"""Deterministic bounded delta reduction preserving a concrete failure class."""

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass

from .harness import Case, InvalidCaseError, Outcome, validate_case


@dataclass
class Reduction:
    case: Case
    outcome: Outcome
    attempts: int
    accepted: int
    exhausted: bool


def minimize(case: Case, evaluate: Callable[[Case], Outcome], *, max_attempts: int = 150) -> Reduction:
    """Reduce query, graph definitions, rows, and non-key values to a fixed point.

    Accepted candidates must pass schema/reference validation and reproduce the
    original failure class and normalized exception diagnostic or result
    discrepancy kind, public columns, and concrete unmatched-row witness. A
    budget bounds expensive failure collection. Exhaustion is reported, not
    described as global minimality.
    """
    validate_case(case)
    current = deepcopy(case)
    outcome = evaluate(current)
    if not outcome.failure_class or outcome.failure_class == "invalid_case":
        raise ValueError("Reducer requires a valid case with a reproducible engine failure")
    target = outcome.failure_identity()
    attempts = accepted = 0

    def attempt(candidate: Case) -> bool:
        nonlocal current, outcome, attempts, accepted
        if attempts >= max_attempts:
            return False
        attempts += 1
        try:
            validate_case(candidate)
        except InvalidCaseError:
            return False
        candidate_outcome = evaluate(candidate)
        if candidate_outcome.failure_identity() != target:
            return False
        current, outcome = candidate, candidate_outcome
        accepted += 1
        return True

    def repair_order(candidate: Case) -> None:
        query = candidate.query
        if query.get("order_by"):
            selected = query.get("metrics", []) + query.get("dimensions", [])
            query["order_by"] = [item for item in query["order_by"] if item.split()[0] in selected]
            if not query["order_by"]:
                query.pop("order_by")
                query.pop("limit", None)
                query.pop("offset", None)

    def reduce_list(get_values, remove) -> None:
        # Classic complement-based ddmin, followed by individual deletions.
        granularity = 2
        while attempts < max_attempts:
            length = len(get_values(current))
            if length == 0:
                return
            chunk = max(1, (length + granularity - 1) // granularity)
            changed = False
            for start in range(0, length, chunk):
                candidate = deepcopy(current)
                remove(candidate, start, min(start + chunk, length))
                if attempt(candidate):
                    granularity = max(2, granularity - 1)
                    changed = True
                    break
                if attempts >= max_attempts:
                    return
            if changed:
                continue
            if chunk == 1:
                return
            granularity = min(length, granularity * 2)

    # Data reduction first keeps later compiler probes cheap and rapidly yields
    # a small reproducer even when the failure is independent of physical rows.
    for table_index in range(len(current.tables)):

        def remove_rows(candidate, start, stop, ti=table_index):
            del candidate.tables[ti]["rows"][start:stop]

        reduce_list(lambda item, ti=table_index: item.tables[ti]["rows"], remove_rows)

    previous = -1
    while attempts < max_attempts and accepted != previous:
        previous = accepted
        for field in ["limit", "offset", "order_by"]:
            if field in current.query:
                candidate = deepcopy(current)
                candidate.query.pop(field)
                attempt(candidate)
        for field in ["filters", "segments", "metrics", "dimensions"]:

            def remove_query(candidate, start, stop, name=field):
                del candidate.query[name][start:stop]
                repair_order(candidate)

            reduce_list(lambda item, name=field: item.query.get(name, []), remove_query)

        def remove_models(candidate, start, stop):
            removed = {model["name"] for model in candidate.models[start:stop]}
            del candidate.models[start:stop]
            for model in candidate.models:
                model["relationships"] = [
                    r for r in model.get("relationships", []) if r.get("target_model", r["name"]) not in removed
                ]
            tables = {model["table"] for model in candidate.models}
            candidate.tables = [table for table in candidate.tables if table["name"] in tables]

        reduce_list(lambda item: item.models, remove_models)

        def remove_metrics(candidate, start, stop):
            del candidate.metrics[start:stop]

        reduce_list(lambda item: item.metrics, remove_metrics)

        for model_index in range(len(current.models)):
            for field in ["metrics", "segments", "relationships", "dimensions", "invariant_filters"]:

                def remove_fields(candidate, start, stop, mi=model_index, name=field):
                    model = candidate.models[mi]
                    del model[name][start:stop]
                    if name == "dimensions" and model.get("default_time_dimension") not in {
                        d["name"] for d in model[name]
                    }:
                        model.pop("default_time_dimension", None)

                reduce_list(lambda item, mi=model_index, name=field: item.models[mi].get(name, []), remove_fields)

    for table_index in range(len(current.tables)):

        def remove_columns(candidate, start, stop, ti=table_index):
            table = candidate.tables[ti]
            del table["columns"][start:stop]
            for row in table["rows"]:
                del row[start:stop]

        reduce_list(lambda item, ti=table_index: item.tables[ti]["columns"], remove_columns)

    # Keep key columns intact. Changing foreign keys is valid SQL but makes a
    # reduced relational population harder to understand and can defeat fanout.
    for table_index, table in enumerate(current.tables):
        key_columns = set()
        for model in current.models:
            if model["table"] == table["name"]:
                keys = model.get("primary_key") or []
                key_columns.update([keys] if isinstance(keys, str) else keys)
            for relationship in model.get("relationships", []):
                target = next(
                    (m for m in current.models if m["name"] == relationship.get("target_model", relationship["name"])),
                    None,
                )
                owner = model if relationship["type"] == "many_to_one" else target
                if owner and owner["table"] == table["name"]:
                    keys = relationship.get("foreign_key") or []
                    key_columns.update([keys] if isinstance(keys, str) else keys)
        for row_index, row in enumerate(table["rows"]):
            for column_index, value in enumerate(row):
                if attempts >= max_attempts:
                    break
                column, dtype = table["columns"][column_index]
                if column in key_columns or value is None:
                    continue
                simple = "2000-01-01" if dtype == "date" else "" if dtype == "varchar" else 0
                for replacement in [None, simple]:
                    if value == replacement:
                        continue
                    candidate = deepcopy(current)
                    candidate.tables[table_index]["rows"][row_index][column_index] = replacement
                    if attempt(candidate):
                        break
    return Reduction(current, outcome, attempts, accepted, attempts >= max_attempts)
