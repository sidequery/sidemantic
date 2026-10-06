"""Strict engine execution, multiset comparison, and replayable case validation."""

import hashlib
import importlib
import inspect
import json
import math
import re
import sys
from copy import deepcopy
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any

import duckdb
import sqlglot
from sqlglot import exp

from sidemantic import Metric, Model, SemanticLayer
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.db.duckdb import DuckDBAdapter
from sidemantic.validation import validate_model, validate_query


@dataclass
class Case:
    seed: int
    index: int
    family: str
    models: list[dict]
    metrics: list[dict]
    query: dict
    tables: list[dict]
    features: list[str] = field(default_factory=list)
    version: int = 1
    data_variant: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    def write(self, path: Path) -> None:
        path.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n")

    @classmethod
    def read(cls, path: Path) -> "Case":
        return cls(**json.loads(path.read_text()))

    @property
    def ordered(self) -> bool:
        return bool(self.query.get("order_by"))


class InvalidCaseError(ValueError):
    """Generator/schema/definition errors, distinct from engine failures."""


def make_graph(case: Case) -> SemanticGraph:
    graph = SemanticGraph()
    for definition in case.models:
        graph.add_model(Model.model_validate(definition))
    for definition in case.metrics:
        graph.add_metric(Metric.model_validate(definition))
    return graph


def _keys(value: str | list[str] | None) -> list[str]:
    return [value] if isinstance(value, str) else (value or [])


def validate_population(case: Case) -> None:
    """Validate every population, even when its immutable definition is cached."""
    try:
        if case.version != 1:
            raise ValueError(f"Unsupported fixture version {case.version}")
        tables = {table["name"]: table for table in case.tables}
        for table in case.tables:
            if any(len(row) != len(table["columns"]) for row in table["rows"]):
                raise ValueError("Data does not match table schema")
        for model in case.models:
            keys = _keys(model.get("primary_key"))
            if not keys:
                continue
            table = tables[model["table"]]
            names = [column[0] for column in table["columns"]]
            positions = [names.index(key) for key in keys]
            values = [tuple(row[position] for position in positions) for row in table["rows"]]
            if any(None in value for value in values) or len(set(values)) != len(values):
                raise ValueError("Primary key is nullable or duplicated")
    except (ValueError, KeyError, TypeError, IndexError) as exc:
        raise InvalidCaseError(str(exc)) from exc


def validate_case(case: Case) -> SemanticGraph:
    """Check definitions and references without asking either compiler to work.

    Engine compile/execute failures remain failures even when both engines raise.
    This intentionally validates the generated grammar rather than claiming to
    validate every possible Sidemantic feature.
    """
    try:
        validate_population(case)
        graph = make_graph(case)
        tables = {table["name"]: table for table in case.tables}
        if len(tables) != len(case.tables) or len(graph.models) != len(case.models):
            raise ValueError("Duplicate table or model")
        schemas = {}
        for table in case.tables:
            names = [column[0] for column in table["columns"]]
            if len(set(names)) != len(names) or not names:
                raise ValueError("Duplicate or empty schema")
            for name in [table["name"], *names]:
                if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
                    raise ValueError(f"Invalid generated identifier: {name}")
            if any(not re.fullmatch(r"integer|double|varchar|date|decimal\(12,2\)", c[1]) for c in table["columns"]):
                raise ValueError("Unsupported generated column type")
            schemas[table["name"]] = set(names)

        def expression(sql: str, owner: str | None, physical: bool = False) -> None:
            parsed = sqlglot.parse_one(sql.replace("{model}", owner or ""), read="duckdb")
            for column in parsed.find_all(exp.Column):
                model_name = column.table or owner
                name = column.name.split("__", 1)[0]
                if not column.table and name in graph.metrics:
                    continue
                if model_name not in graph.models:
                    raise ValueError(f"Unknown expression owner: {column.sql()}")
                model = graph.models[model_name]
                available = {d.name for d in model.dimensions} | {m.name for m in model.metrics}
                if physical:
                    available = schemas[model.table]
                if name not in available:
                    raise ValueError(f"Unknown expression field: {column.sql()}")

        for model in graph.models.values():
            errors = validate_model(model)
            if errors:
                raise ValueError("; ".join(errors))
            if model.table not in tables:
                raise ValueError(f"Missing table {model.table}")
            schema = schemas[model.table]
            key_names = _keys(model.primary_key)
            if not set(key_names) <= schema:
                raise ValueError("Primary key not in schema")
            for dimension in model.dimensions:
                expression(dimension.sql or dimension.name, model.name, physical=True)
                if not dimension.sql and dimension.name not in schema:
                    raise ValueError(f"Dimension {dimension.name} missing physical column")
            for segment in model.segments:
                expression(segment.sql, model.name, physical=True)
            for predicate in model.invariant_filters or []:
                expression(predicate, model.name, physical=True)
            for relationship in model.relationships:
                target = graph.models.get(relationship.related_model)
                if target is None:
                    raise ValueError("Missing relationship target")
                target_schema = schemas[target.table]
                foreign = relationship.foreign_key_columns
                primary = relationship.primary_key_columns or _keys(
                    target.primary_key if relationship.type == "many_to_one" else model.primary_key
                )
                foreign_schema, primary_schema = (
                    (schema, target_schema) if relationship.type == "many_to_one" else (target_schema, schema)
                )
                if (
                    len(foreign) != len(primary)
                    or not set(foreign) <= foreign_schema
                    or not set(primary) <= primary_schema
                ):
                    raise ValueError("Invalid relationship keys")
            for metric in model.metrics:
                for sql in [metric.sql, metric.numerator, metric.denominator, *(metric.filters or [])]:
                    if sql:
                        expression(sql, model.name, physical=metric.type is None or sql in (metric.filters or []))
        for metric in graph.metrics.values():
            for sql in [metric.sql, metric.numerator, metric.denominator]:
                if sql:
                    expression(sql, None)
        if not case.query.get("metrics") and not case.query.get("dimensions"):
            raise ValueError("Empty query")
        errors = validate_query(case.query.get("metrics", []), case.query.get("dimensions", []), graph)
        if errors:
            raise ValueError("; ".join(errors))
        for predicate in case.query.get("filters", []):
            expression(predicate, None)
        for segment in case.query.get("segments", []):
            owner, name = segment.split(".")
            if owner not in graph.models or name not in {s.name for s in graph.models[owner].segments}:
                raise ValueError("Unknown query segment")
        order = [re.sub(r"\s+(ASC|DESC)$", "", item, flags=re.I) for item in case.query.get("order_by", [])]
        selected = case.query.get("metrics", []) + case.query.get("dimensions", [])
        if any(item not in selected for item in order):
            raise ValueError("Order expression must be selected")
        if order and not set(case.query.get("dimensions", [])) <= set(order):
            raise ValueError("Order must contain every group key as a tie breaker")
        if (case.query.get("limit") is not None or case.query.get("offset") is not None) and not order:
            raise ValueError("Pagination requires deterministic ordering")
        # A selected cumulative metric requires a time grouping. This also
        # prevents reducers from turning a semantic mismatch into invalid input.
        for reference in case.query.get("metrics", []):
            _, metric = graph.resolve_metric_reference(reference)
            if metric.type == "cumulative" and not any(
                graph.models[ref.split(".")[0]].get_dimension(ref.split(".")[1].split("__")[0]).type == "time"
                for ref in case.query.get("dimensions", [])
            ):
                raise ValueError("Cumulative query requires a time dimension")
        return graph
    except (ValueError, KeyError, TypeError, AttributeError, sqlglot.errors.ParseError) as exc:
        raise InvalidCaseError(str(exc)) from exc


def _value_equal(left: Any, right: Any, *, rel_tol: float, abs_tol: float) -> bool:
    if left is None or right is None:
        return left is right
    # DATE and midnight TIMESTAMP are equal semantic time buckets. Other time
    # components remain observable; strings are never coerced into dates.
    if isinstance(left, date) and isinstance(right, date):

        def timestamp(value):
            return value if isinstance(value, datetime) else datetime.combine(value, datetime.min.time())

        return timestamp(left) == timestamp(right)
    numeric = (int, float, Decimal)
    if isinstance(left, numeric) and isinstance(right, numeric):
        if isinstance(left, bool) or isinstance(right, bool):
            return type(left) is type(right) and left == right
        if isinstance(left, int) and isinstance(right, int):
            return left == right
        if math.isnan(float(left)) or math.isnan(float(right)):
            return math.isnan(float(left)) and math.isnan(float(right))
        return math.isclose(left, right, rel_tol=rel_tol, abs_tol=abs_tol)
    return type(left) is type(right) and left == right


def rows_equal(left: list, right: list, *, ordered: bool, rel_tol=1e-9, abs_tol=1e-9) -> bool:
    """Compare multisets with duplicate multiplicity and tolerant numeric values.

    Tolerance is not transitive. A bipartite matching avoids the false negatives
    produced by greedy matching or sorting near-equal numeric rows.
    """
    if len(left) != len(right):
        return False

    if ordered:
        return all(_row_equal(a, b, rel_tol=rel_tol, abs_tol=abs_tol) for a, b in zip(left, right))
    return len(_multiset_matches(left, right, rel_tol=rel_tol, abs_tol=abs_tol)) == len(left)


def _row_equal(left, right, *, rel_tol=1e-9, abs_tol=1e-9):
    return len(left) == len(right) and all(
        _value_equal(x, y, rel_tol=rel_tol, abs_tol=abs_tol) for x, y in zip(left, right)
    )


def _multiset_matches(left, right, *, rel_tol=1e-9, abs_tol=1e-9):
    edges = [[j for j, b in enumerate(right) if _row_equal(a, b, rel_tol=rel_tol, abs_tol=abs_tol)] for a in left]
    matched = {}

    def augment(i, seen):
        for j in edges[i]:
            if j in seen:
                continue
            seen.add(j)
            if j not in matched or augment(matched[j], seen):
                matched[j] = i
                return True
        return False

    for i in range(len(left)):
        augment(i, set())
    return matched


def _canonical_row(row):
    def value(item):
        if item is None:
            return "null", ""
        if isinstance(item, bool):
            return "bool", str(item)
        if isinstance(item, (int, float, Decimal)):
            return "number", str(Decimal(str(item)).normalize())
        if isinstance(item, date):
            timestamp = item if isinstance(item, datetime) else datetime.combine(item, datetime.min.time())
            return "time", timestamp.isoformat()
        return type(item).__name__, str(item)

    return tuple(value(item) for item in row)


@dataclass
class EngineResult:
    columns: list[str] = field(default_factory=list)
    rows: list = field(default_factory=list)
    sql: str | None = None
    error: str | None = None
    error_type: str | None = None
    stage: str = "compile"
    selection: dict | None = None
    compile_seconds: float = 0
    execute_seconds: float = 0
    compilation_reused: bool = False


@dataclass
class Outcome:
    failure_class: str | None
    python: EngineResult | None = None
    rust: EngineResult | None = None
    detail: str | None = None

    def failure_identity(self) -> tuple:
        """Preserve the diagnostic or public-result discrepancy being reduced.

        Result identities intentionally retain all public columns and a concrete
        witness. This can prevent otherwise useful reductions, but cannot trade
        a missing group for an unrelated ordering/metric discrepancy.
        """
        if self.failure_class in {"rows", "columns"} and self.python and self.rust:
            columns = tuple(self.python.columns), tuple(self.rust.columns)
            if self.failure_class == "columns":
                return "columns", columns
            left, right = self.python.rows, self.rust.rows
            if len(left) == len(right) and rows_equal(left, right, ordered=False):
                witness = next(
                    (_canonical_row(a), _canonical_row(b)) for a, b in zip(left, right) if not _row_equal(a, b)
                )
                return "rows", "order", columns, witness
            left = sorted(left, key=_canonical_row)
            right = sorted(right, key=_canonical_row)
            matched = _multiset_matches(left, right)
            matched_left = set(matched.values())
            unmatched_left = [_canonical_row(row) for i, row in enumerate(left) if i not in matched_left]
            unmatched_right = [_canonical_row(row) for i, row in enumerate(right) if i not in matched]
            witness = (
                unmatched_left[0] if unmatched_left else None,
                unmatched_right[0] if unmatched_right else None,
            )
            if len(left) != len(right):
                return "rows", "cardinality", columns, len(left) > len(right), witness
            return "rows", "values", columns, witness
        diagnostics = []
        for result in [self.python, self.rust]:
            if result and result.error:
                # Retain referenced owner/column names and multi-line semantic
                # validation detail; remove SQL excerpts and variable locations.
                message = re.split(r"\n(?:LINE \d+:|\s*\^)|\n\n", result.error, maxsplit=1)[0]
                message = re.sub(r"\b\d+(?:\.\d+)?\b", "#", message)
                diagnostics.append(" ".join(message.split()))
            else:
                diagnostics.append(None)
        return self.failure_class, *diagnostics

    def fingerprint(self, case: Case) -> str:
        parts = list(self.failure_identity())
        if self.failure_class == "rows" and self.python and self.rust:
            # Collection groups similar failures across data populations; the
            # reducer alone pins the concrete witness within each saved case.
            parts.pop()
        if self.failure_class == "invalid_case":
            parts.append(self.detail)
        if self.failure_class in {"rows", "columns"}:
            parts.append(case.family)
        return hashlib.sha256(json.dumps(parts, sort_keys=True).encode()).hexdigest()[:16]


class DifferentialRunner:
    """Reuse one single-thread DuckDB connection; recreate graphs for every case."""

    def __init__(self):
        self.adapter = DuckDBAdapter(config={"threads": 1})
        self.table_names: list[str] = []
        # Retain only the last immutable input per engine. Volume campaigns
        # process all populations together; memory does not grow with the run.
        self.compiled: dict[str, tuple[str, EngineResult]] = {}
        self.validated_key: str | None = None
        self.definition_validations = 0
        self.definition_validation_cache_hits = 0

    def close(self):
        self.adapter.close()

    def _load(self, case: Case):
        for name in self.table_names:
            self.adapter.execute(f'DROP TABLE "{name}"')
        self.table_names = []
        for table in case.tables:
            name = table["name"]
            columns = ", ".join(f'"{column}" {dtype}' for column, dtype in table["columns"])
            self.adapter.execute(f'CREATE TABLE "{name}" ({columns})')
            self.table_names.append(name)
            if table["rows"]:
                placeholders = ", ".join("?" for _ in table["columns"])
                self.adapter.executemany(f'INSERT INTO "{name}" VALUES ({placeholders})', table["rows"])

    @staticmethod
    def compilation_key(case: Case) -> str:
        return json.dumps(
            {
                "version": case.version,
                "models": case.models,
                "metrics": case.metrics,
                "query": case.query,
                "schemas": [{"name": t["name"], "columns": t["columns"]} for t in case.tables],
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def _compile(self, case: Case, engine: str) -> EngineResult:
        result = EngineResult()
        started = perf_counter()
        try:
            layer = SemanticLayer(connection=self.adapter, engine=engine, fallback=False, auto_register=False)
            layer.graph = make_graph(case)
            if engine == "rust":

                def forbidden_python(*args, **kwargs):
                    raise AssertionError("Rust compilation entered the Python fallback")

                layer._compile_with_python = forbidden_python
            # Some compiler planning paths expand mutable selection lists. Give
            # each engine an independent request, preserving the replay fixture.
            result.sql = layer.compile(**deepcopy(case.query))
            result.compile_seconds = perf_counter() - started
            result.selection = layer.last_engine_selection
            result.stage = "selection"
            if result.selection != {"engine": engine, "reason": None}:
                raise AssertionError(f"Expected strict {engine} selection, got {result.selection}")
            result.stage = "execute"
        except Exception as exc:
            result.error_type = type(exc).__name__
            result.error = str(exc)
            if result.stage == "compile":
                result.compile_seconds = perf_counter() - started
        return result

    def _engine(self, case: Case, engine: str, *, reuse_compilation: bool = False) -> EngineResult:
        key = self.compilation_key(case) if reuse_compilation else None
        cached = self.compiled.get(engine) if reuse_compilation else None
        if cached and cached[0] == key:
            result = replace(cached[1], compile_seconds=0, compilation_reused=True)
        else:
            result = self._compile(case, engine)
            # Never cache a failed compile or unexpected engine selection.
            if reuse_compilation and result.error is None:
                self.compiled[engine] = (key, replace(result))
        if result.error is not None:
            return result
        started = perf_counter()
        try:
            cursor = self.adapter.execute(result.sql)
            result.columns = [column[0] for column in cursor.description]
            result.rows = cursor.fetchall()
            result.stage = "complete"
        except Exception as exc:
            result.error_type = type(exc).__name__
            result.error = str(exc)
        result.execute_seconds = perf_counter() - started
        return result

    def evaluate(self, case: Case, *, reuse_compilation: bool = False) -> Outcome:
        try:
            key = self.compilation_key(case) if reuse_compilation else None
            if reuse_compilation and key == self.validated_key:
                validate_population(case)
                self.definition_validation_cache_hits += 1
            else:
                validate_case(case)
                self.definition_validations += 1
                if reuse_compilation:
                    self.validated_key = key
            self._load(case)
        except Exception as exc:
            return Outcome("invalid_case", detail=f"{type(exc).__name__}: {exc}")
        if reuse_compilation:
            python = self._engine(case, "python", reuse_compilation=True)
            rust = self._engine(case, "rust", reuse_compilation=True)
        else:
            python = self._engine(case, "python")
            rust = self._engine(case, "rust")
        errors = [
            f"{engine}:{result.stage}:{result.error_type}"
            for engine, result in [("python", python), ("rust", rust)]
            if result.error is not None
        ]
        if errors:
            return Outcome("|".join(errors), python, rust)
        if python.columns != rust.columns:
            return Outcome("columns", python, rust)
        if not rows_equal(python.rows, rust.rows, ordered=case.ordered):
            return Outcome("rows", python, rust)
        return Outcome(None, python, rust)


def outcome_dict(outcome: Outcome) -> dict:
    """Diagnostic JSON conversion; fixtures themselves retain exact source data."""
    return json.loads(json.dumps(asdict(outcome), default=str))


def runtime_provenance() -> dict:
    """Identify the actual source package and loaded native artifact in reports."""
    native = importlib.import_module("sidemantic_rs.sidemantic_rs")
    native_path = Path(native.__file__).resolve()
    with native_path.open("rb") as binary:
        digest = hashlib.file_digest(binary, "sha256").hexdigest()
    return {
        "python_executable": sys.executable,
        "semantic_layer_source": inspect.getfile(SemanticLayer),
        "rust_extension": str(native_path),
        "rust_extension_sha256": digest,
        "duckdb_version": duckdb.__version__,
        "duckdb_threads": 1,
    }
