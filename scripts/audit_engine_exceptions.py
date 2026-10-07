"""Inventory pytest exceptions and engine routing without importing or running tests.

Run with ``uv run --no-project scripts/audit_engine_exceptions.py``. Optionally
supply ``--collected-nodeids FILE`` containing one pytest node ID per line to
expand static scopes to actual parametrized cases. Classification is deliberately
conservative: a pinned behavioral contract is a coverage gap until demonstrated
otherwise, not evidence that the Rust implementation itself is defective.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import tomllib
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENGINE_ENV = re.compile(r"SIDEMANTIC_(?:ENGINE|RS_|OSI_BACKEND)")
HELPERS = {
    "SQLGenerator",
    "QueryRewriter",
    "RustRuntimeSemanticLayer",
    "RustSemanticLayerAdapter",
    "RustSQLGeneratorAdapter",
    "RustQueryRewriterAdapter",
    "RustSemanticGraphDirectAdapter",
    "LegacyOSIAdapter",
}
CONTRACT_FILES = {
    "tests/core/test_runtime_defaults.py",
    "tests/core/test_rust_engine_mode.py",
    "tests/core/test_rust_strict_sql_generator.py",
    "tests/core/test_rust_query_validation.py",
    "tests/test_cli_engine_routing.py",
    "tests/core/test_rust_adapter_forwarding.py",
}
PRIVATE_CONTRACTS = {
    "test_complete_row_count_generator_reuse_observes_graph_changes": "Python SQLGenerator object reuse and graph identity contract.",
    "test_complete_row_count_role_queries_preserve_caller_caches": "Python graph private cache identity and SQLGenerator non-mutation contract.",
    "test_owned_count_generator_sees_live_graph_changes": "Python SQLGenerator object reuse and graph identity contract.",
}


def classify(path, scope, kind, source, context):
    text = (source + " " + context).lower()
    if kind == "importorskip":
        return "optional-host", "Dependency availability gate; native Rust must be installed for acceptance."
    if kind in {"skip", "skipif", "xfail"}:
        if any(
            s in text
            for s in ("date functions test", "format_date", "cross-model segments", "reparses", "physical keys")
        ):
            return "real-gap", "Behavioral or emulator limitation; do not count the omitted contract as parity."
        if (
            any(
                s in text
                for s in (
                    "postgres_dsn",
                    "_test",
                    "databricks_url",
                    "dax_online",
                    "dax_engine_conformance",
                    "yardstick_upstream",
                    "adbc_db",
                    "postgres_url",
                    "power bi",
                    "dax formatter",
                )
            )
            and "sqlite" not in text
        ):
            return "external-service", "Explicit external database, network oracle, or integration environment gate."
        if any(
            s in text
            for s in (
                "not installed",
                "not available",
                "not present",
                "not built",
                "sqlite_tests",
                "no working adbc",
                "fakesnow not compatible",
                "uv is required",
                "file not found",
                "could not import",
            )
        ):
            return "optional-host", "Optional local dependency, artifact, fixture, or emulator compatibility gate."
        if "native" in text or "strict rust" in text:
            return (
                "intentional-Python-only",
                "Engine-asymmetric contract: skips the Python parameter; Rust still runs. Not a Rust-default blocker by itself.",
            )
        if "postgresql adapter available" in text:
            return "external-service", "Connection contract delegated to the integration suite."
        return "real-gap", "Unconditional or conditional behavioral omission requiring explicit review."
    if kind == "integration-marker":
        return (
            "external-service",
            "Deselected by default addopts; may additionally have dependency or behavioral skips.",
        )
    if kind == "engine-matrix":
        if "'python'" in source or '"python"' in source:
            if "'rust'" not in source and '"rust"' not in source:
                if scope.split("::")[-1] in PRIVATE_CONTRACTS:
                    return "intentional-Python-only", PRIVATE_CONTRACTS[scope.split("::")[-1]]
                return (
                    "real-gap",
                    "Python-only parameter selection: adjudicate behavioral coverage separately from private implementation contracts.",
                )
        return None, "Explicit engine matrix, including Rust; not a Python-only exclusion."
    if kind == "implementation-import":
        if "rust" in source.lower():
            return (
                None,
                "Explicit Rust helper/adapter route, recorded for completeness; this is not a Python exclusion.",
            )
        return (
            "real-gap",
            "Direct implementation helper bypasses SemanticLayer engine selection; shared-suite success alone is not Rust coverage.",
        )
    if kind == "public-layer-import":
        return None, "Public SemanticLayer import alias is engine-aware; not a Python pin."
    if kind == "legacy-adapter-import":
        if path.endswith(("conftest.py", "test_rust_parity.py")):
            return None, "Legacy adapter constants or explicit Python side of OSI differential oracle."
        return (
            "real-gap",
            "Explicit LegacyOSIAdapter alias bypasses canonical native OSI adapter; legacy compatibility coverage needs separate Rust evidence.",
        )
    if kind == "engine-control":
        if "SIDEMANTIC_OSI_BACKEND" in source and '"python"' in source:
            return (
                "real-gap",
                "OSI legacy adapter shim defaults to Python independently of --test-engine; separate backend override required.",
            )
        python_pin = bool(re.search(r"engine\s*=\s*['\"]python['\"]", source)) or bool(
            re.search(r"setenv\(['\"]SIDEMANTIC_ENGINE['\"],\s*['\"]python['\"]", source)
        )
        if not python_pin:
            return None, "Engine route control or default/reset observation; not a Python exclusion."
        if path in CONTRACT_FILES or any(
            s in scope for s in ("legacy_python", "rust_modes", "python_engine", "python_mode")
        ):
            return "intentional-Python-only", "Explicit engine selection, legacy fallback, or routing contract."
        if python_pin:
            if "ossie_engine_conformance" in path:
                return None, "Python side of explicit Python/Rust differential oracle."
            return (
                "real-gap",
                "Behavioral test explicitly pins Python; remove pin or establish equivalent Rust coverage.",
            )
        return (
            None,
            "Engine route control (Rust, dynamic matrix, environment reset, or test double); inspect source and conditions.",
        )
    return "external-service", "Selection rule or CI boundary; see exact source."


def replay_contracts():
    """Reconstruct the replay selector from AST, without importing native modules."""
    path = ROOT / "tests/core/test_pure_rust_python_test_parity.py"
    tree = ast.parse(path.read_text())
    imports = {
        n.asname: n.name for statement in tree.body if isinstance(statement, ast.Import) for n in statement.names
    }
    values = {}
    for statement in tree.body:
        if isinstance(statement, ast.Assign) and isinstance(statement.targets[0], ast.Name):
            try:
                values[statement.targets[0].id] = ast.literal_eval(statement.value)
            except (ValueError, TypeError):
                if statement.targets[0].id == "PYTHON_TEST_PARITY_MODULES":
                    values["modules"] = {imports[n.id] for n in statement.value.elts}
    return values


def scan_file(path, replay):
    relative = path.relative_to(ROOT).as_posix()
    source = path.read_text()
    source_lines = source.splitlines(keepends=True)
    tree = ast.parse(source)
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    functions = [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]

    def ancestors(node):
        result = []
        while node in parents:
            node = parents[node]
            result.append(node)
        return result

    def scope_of(node):
        owners = [
            n.name
            for n in reversed(ancestors(node))
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        ]
        return "::".join(owners) or "<module>"

    def function_id(node):
        prefix = scope_of(node)
        return node.name if prefix == "<module>" else prefix + "::" + node.name

    tests = {function_id(n): n for n in functions if n.name.startswith("test_")}
    dependencies = {}
    for function in functions:
        dependencies[function.name] = {n.id for n in ast.walk(function) if isinstance(n, ast.Name)} | {
            a.arg for a in function.args.args
        }
    autouse = {
        f.name
        for f in functions
        for d in f.decorator_list
        if isinstance(d, ast.Call)
        and any(k.arg == "autouse" and isinstance(k.value, ast.Constant) and k.value.value is True for k in d.keywords)
    }

    def affected(scope, symbols=None):
        if symbols is None and (scope == "<module>" or scope.split("::")[-1] in autouse):
            return sorted(tests)
        if scope in tests:
            return [scope]
        target = scope.split("::")[-1]
        result = []
        for name, node in tests.items():
            pending = [node.name]
            seen = set()
            while pending:
                current = pending.pop()
                if current in seen:
                    continue
                seen.add(current)
                pending.extend(dependencies.get(current, set()) - seen)
            if (symbols is not None and symbols & seen) or (
                symbols is None and (target in seen or name.startswith(scope + "::"))
            ):
                result.append(name)
        return sorted(result)

    def assess_helper(test):
        function = tests[test]
        reviewed = replay.get("helper_evidence", {}).get(relative + "::" + test)
        if reviewed is not None:
            detail = reviewed["behavior"]
            if reviewed.get("counterparts"):
                detail += " Counterparts: " + "; ".join(reviewed["counterparts"])
            if reviewed.get("limitation"):
                detail += " Limitation: " + reviewed["limitation"]
            return {
                "test": relative + "::" + test,
                "category": reviewed["category"],
                "evidence": "docs/rust-parity-helper-evidence.json#" + reviewed["id"] + ": " + detail,
            }
        module = relative.removesuffix(".py").replace("/", ".")
        nodeid = module + "::" + test
        signature = tuple(a.arg for a in function.args.args if a.arg != "self")
        if function.name in PRIVATE_CONTRACTS:
            return {
                "test": relative + "::" + test,
                "category": "intentional-Python-only",
                "evidence": PRIVATE_CONTRACTS[function.name],
            }
        full_replays = {
            "tests/queries/test_sql_rewriter.py": "tests/queries/test_rust_sql_rewriter_parity.py::test_semantic_sql_contract",
            "tests/queries/test_yardstick_query_rewriter.py": "tests/queries/test_rust_yardstick_parity.py::test_yardstick_result_contract",
            "tests/queries/test_yardstick_warnings.py": "tests/queries/test_rust_yardstick_parity.py::test_yardstick_result_contract",
        }
        if relative in full_replays and "::" not in test:
            return {
                "test": relative + "::" + test,
                "category": None,
                "evidence": full_replays[relative] + "[" + test + "]",
            }
        if relative == "tests/queries/test_semantic_sql_planner.py":
            return {
                "test": relative + "::" + test,
                "category": "intentional-Python-only",
                "evidence": "Python optimizer plan/rule contract (module docstring); engine-selected SemanticLayer execution assertions remain active.",
            }
        if relative in {
            "tests/semantic_conformance/test_parameterized_segments.py",
            "tests/semantic_conformance/test_yardstick_source_dialects.py",
        }:
            return {
                "test": relative + "::" + test,
                "category": None,
                "evidence": "Engine-parametrized layer fixture and result/rows helper branch between Rust bridge and Python helper in this module.",
            }
        replayed = module in replay["modules"] and (
            signature in replay["SUPPORTED_SIGNATURES"]
            or not signature
            and nodeid
            in replay["DIRECT_RUST_GRAPH_PARITY_NODEIDS"] | replay["DIRECT_RUST_SQL_GENERATOR_PARITY_NODEIDS"]
            or module in replay["DIRECT_RUST_FUNCTION_PARITY_MODULES"]
            and signature in {(), ("conn",)}
        )
        if replayed:
            return {
                "test": relative + "::" + test,
                "category": None,
                "evidence": "tests/core/test_pure_rust_python_test_parity.py::test_existing_python_layer_contract_matches_pure_rust["
                + nodeid
                + "]",
            }
        calls = [n for n in ast.walk(function) if isinstance(n, ast.Call)]
        private = [
            ast.unparse(n.func) for n in calls if isinstance(n.func, ast.Attribute) and n.func.attr.startswith("_")
        ]
        public = [
            n
            for n in calls
            if isinstance(n.func, ast.Attribute)
            and n.func.attr in {"generate", "rewrite", "compile", "query", "explain"}
        ]
        if private and not public:
            return {
                "test": relative + "::" + test,
                "category": "intentional-Python-only",
                "evidence": "Direct private implementation-unit calls: " + ", ".join(sorted(set(private))),
            }
        if "engine" in signature and "sidemantic_rs" in source:
            return {
                "test": relative + "::" + test,
                "category": None,
                "evidence": "Co-located engine-parametrized native/reference oracle; inspect test and Rust import.",
            }
        if any(
            any(
                k.arg == "use_rust_rewriter" and isinstance(k.value, ast.Constant) and k.value.value is True
                for k in n.keywords
            )
            for n in calls
        ):
            return {
                "test": relative + "::" + test,
                "category": None,
                "evidence": "Explicit use_rust_rewriter=True route in this test.",
            }
        return {
            "test": relative + "::" + test,
            "category": "real-gap",
            "evidence": "Direct helper behavioral coverage has no established replay counterpart; requires native coverage adjudication, not a confirmed Rust defect.",
        }

    records = []
    for node in ast.walk(tree):
        kind = None
        if not isinstance(
            node, (ast.Call, ast.Attribute, ast.ImportFrom, ast.Assign, ast.AnnAssign, ast.For, ast.List, ast.Tuple)
        ):
            continue
        segment_lines = source_lines[node.lineno - 1 : node.end_lineno]
        segment = "".join(segment_lines)
        if len(segment_lines) == 1:
            segment = segment[node.col_offset : node.end_col_offset]
        else:
            segment = segment[node.col_offset : len(segment) - len(segment_lines[-1]) + node.end_col_offset]
        if isinstance(node, ast.Call):
            name = ast.unparse(node.func)
            if (
                name in {"pytest.skip", "pytest.xfail", "pytest.importorskip", "_pytest.importorskip"}
                or name.startswith("pytest.mark.")
                and name.rsplit(".", 1)[-1] in {"skip", "skipif", "xfail"}
            ):
                kind = name.rsplit(".", 1)[-1]
            elif any(k.arg in {"engine", "use_rust_rewriter", "rust_no_fallback"} for k in node.keywords):
                kind = "engine-control"
            elif (
                name == "pytest.mark.parametrize"
                and len(node.args) >= 2
                and any(isinstance(v, ast.Constant) and v.value in {"python", "rust"} for v in ast.walk(node.args[1]))
            ):
                kind = "engine-matrix"
            elif name == "pytest.fixture" and any(
                k.arg == "params"
                and any(isinstance(v, ast.Constant) and v.value in {"python", "rust"} for v in ast.walk(k.value))
                for k in node.keywords
            ):
                kind = "engine-matrix"
            elif name.endswith(".addoption") and "--test-engine" in segment:
                kind = "engine-control"
            elif name == "os.environ.get" and "SIDEMANTIC_OSI_BACKEND" in segment:
                kind = "engine-control"
            elif name.endswith((".setenv", ".delenv", ".setattr")) and (
                ENGINE_ENV.search(segment)
                or any(s in segment for s in HELPERS | {"SemanticLayer", "_compile_with_python", "_compile_with_rust"})
            ):
                kind = "engine-control"
        elif isinstance(node, ast.Attribute) and ast.unparse(node) == "pytest.mark.integration":
            kind = "integration-marker"
        elif isinstance(node, ast.ImportFrom):
            if node.module == "sidemantic.adapters.osi" and any(a.name == "LegacyOSIAdapter" for a in node.names):
                kind = "legacy-adapter-import"
            elif (
                node.module
                and (node.module.startswith("sidemantic.sql.") or node.module == "tests.rust_layer_adapter")
                and any(a.name in HELPERS for a in node.names)
            ):
                kind = "implementation-import"
            elif node.module == "sidemantic.core.semantic_layer" and any(a.name == "SemanticLayer" for a in node.names):
                kind = "public-layer-import"
        elif isinstance(node, (ast.Assign, ast.AnnAssign)) and (
            re.search(r"\[['\"]engine['\"]\]", segment)
            or re.search(r"\._(?:use_rust|strict_rust|rust_module)\w*\s*=", segment)
            or (ENGINE_ENV.search(segment) and not isinstance(getattr(node, "value", None), ast.Call))
        ):
            kind = "engine-control"
        elif isinstance(node, ast.For) and ENGINE_ENV.search(ast.unparse(node.iter)):
            kind = "engine-control"
        elif isinstance(node, (ast.List, ast.Tuple)) and any(
            isinstance(value, ast.Constant) and value.value == "--engine" for value in node.elts
        ):
            kind = "engine-control"
        if kind is None:
            continue
        scope = scope_of(node)
        # A decorator belongs to the decorated test, not merely its surrounding module.
        for owner in ancestors(node):
            if isinstance(owner, (ast.FunctionDef, ast.AsyncFunctionDef)):
                scope = function_id(owner)
                break
        conditions = [ast.unparse(a.test) for a in reversed(ancestors(node)) if isinstance(a, ast.If)]
        context = " ".join(conditions)
        category, rationale = classify(relative, scope, kind, segment, context)
        symbols = (
            {a.asname or a.name for a in node.names if a.name in HELPERS} if kind == "implementation-import" else None
        )
        affected_tests = affected(scope, symbols)
        assessments = []
        if kind == "implementation-import" and "rust" not in segment.lower():
            assessments = [assess_helper(test) for test in affected_tests]
            categories = {item["category"] for item in assessments}
            category = (
                "real-gap"
                if "real-gap" in categories
                else "intentional-Python-only"
                if "intentional-Python-only" in categories
                else None
            )
            rationale = "Per-test adjudication below distinguishes replay coverage, private units, and unresolved behavioral coverage."
        is_exception = category is not None
        direction = "none"
        if is_exception:
            direction = "both-engines"
            if kind in {"engine-control", "implementation-import", "engine-matrix", "legacy-adapter-import"}:
                direction = "rust-coverage-exclusion"
            elif (
                "engine == 'python'" in context
                or 'engine == "python"' in context
                or "Native" in segment
                or "Strict Rust" in segment
                or "reparses" in segment
                or "physical keys" in segment
            ):
                direction = "python-only-exclusion"
        records.append(
            {
                "path": relative,
                "line": node.lineno,
                "scope": scope,
                "kind": kind,
                "category": category,
                "is_exception": is_exception,
                "exclusion_direction": direction,
                "rationale": rationale,
                "conditions": conditions,
                "source": segment,
                "declared_tests": [relative + "::" + name for name in affected_tests],
                "test_assessments": assessments,
            }
        )
    return records, [relative + "::" + name for name in tests]


def compact_inventory(result):
    """Keep actual exceptions; share repeated scope expansions and explanations."""
    scopes, scope_ids, explanations, explanation_ids = {}, {}, {}, {}

    def explanation(value):
        if value not in explanation_ids:
            key = f"reason-{len(explanations) + 1}"
            explanation_ids[value] = key
            explanations[key] = value
        return explanation_ids[value]

    exceptions, helpers = [], {}
    for record in result["exceptions"]:
        for assessment in record["test_assessments"]:
            path, test = assessment["test"].split("::", 1)
            helpers.setdefault(path, {})[test] = {
                "category": assessment["category"],
                "evidence": explanation(assessment["evidence"]),
            }
        if not record["is_exception"]:
            continue
        compact = {
            key: value
            for key, value in record.items()
            if key not in {"is_exception", "declared_tests", "test_assessments", "rationale", "collected_tests"}
        }
        compact["rationale"] = explanation(record["rationale"])
        if not compact["conditions"]:
            compact.pop("conditions")
        tests = [test.removeprefix(record["path"] + "::") for test in record["declared_tests"]]
        if record["path"].endswith("/conftest.py") and "collected_tests" not in record:
            compact["affected_scope"] = "subtree"
            compact["declared_test_count"] = len(tests)
            exceptions.append(compact)
            continue
        if tests == [record["scope"]] and "collected_tests" not in record:
            compact["affected_scope"] = "self"
            exceptions.append(compact)
            continue
        if (
            record["scope"] == "<module>"
            and record["kind"] != "implementation-import"
            and "collected_tests" not in record
        ):
            compact["affected_scope"] = "module"
            compact["declared_test_count"] = len(tests)
            exceptions.append(compact)
            continue
        scope_key = (record["path"], tuple(tests), tuple(record.get("collected_tests", [])))
        if scope_key not in scope_ids:
            key = f"scope-{len(scopes) + 1}"
            scope_ids[scope_key] = key
            scopes[key] = {"path": record["path"], "declared_tests": tests}
            if "collected_tests" in record:
                scopes[key]["collected_tests"] = record["collected_tests"]
        compact["affected_scope"] = scope_ids[scope_key]
        exceptions.append(compact)
    compact_result = {
        **result,
        "exceptions": exceptions,
        "affected_scopes": scopes,
        "helper_test_assessments": helpers,
        "explanations": explanations,
    }
    if "execution_evidence" in result:
        evidence = dict(result["execution_evidence"])
        groups = {}
        for item in evidence.pop("omissions"):
            group = groups.setdefault(item["reason"], {"category": item["category"], "type": item["type"], "tests": []})
            group["tests"].append(item["classname"] + "::" + item["name"])
        evidence["omissions_by_reason"] = groups
        compact_result["execution_evidence"] = evidence
    return compact_result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collected-nodeids", type=Path)
    parser.add_argument(
        "--junit",
        type=Path,
        help="Optional historical/runtime JUnit evidence; never conflated with static scope counts",
    )
    parser.add_argument(
        "--evidence-label", default="Unspecified execution; caller must record revision and environment"
    )
    parser.add_argument("--output", type=Path, default=ROOT / "docs/rust-parity-exceptions.json")
    parser.add_argument("--markdown", type=Path, default=ROOT / "docs/rust-parity-exceptions.md")
    args = parser.parse_args()
    records, declared = [], []
    replay = replay_contracts()
    evidence_path = ROOT / "docs/rust-parity-helper-evidence.json"
    if evidence_path.exists():
        evidence = json.loads(evidence_path.read_text())
        replay["helper_evidence"] = {}
        for group in evidence["groups"]:
            for test in group["source_tests"]:
                if test in replay["helper_evidence"]:
                    raise ValueError(f"Duplicate reviewed helper evidence: {test}")
                replay["helper_evidence"][test] = group
    for path in sorted((ROOT / "tests").rglob("*.py")):
        found, tests = scan_file(path, replay)
        records.extend(found)
        declared.extend(tests)
    # Root autouse fixtures select every test, not zero tests in conftest itself.
    for record in records:
        if record["path"].endswith("/conftest.py") and not record["declared_tests"]:
            directory = record["path"].removesuffix("conftest.py")
            record["declared_tests"] = [test for test in declared if test.startswith(directory)]
    config = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["pytest"]["ini_options"]
    selection = [{"path": "pyproject.toml", "source": config.get("addopts", ""), "category": "external-service"}]
    for path in sorted((ROOT / ".github/workflows").glob("*.yml")):
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            if re.search(r"pytest|test-engine|SIDEMANTIC_ENGINE|engine:|addopts=|--deselect|--ignore", line):
                selection.append({"path": path.relative_to(ROOT).as_posix(), "line": lineno, "source": line.strip()})
    collected = None
    if args.collected_nodeids:
        collected = [
            line.strip()
            for line in args.collected_nodeids.read_text().splitlines()
            if line.startswith("tests/") and "::" in line
        ]
        for record in records:
            bases = set(record["declared_tests"])
            record["collected_tests"] = [node for node in collected if node.split("[", 1)[0] in bases]
    records.sort(key=lambda item: (item["path"], item["line"], item["kind"]))
    summary = {
        "source_records": len(records),
        "by_kind": dict(sorted(Counter(r["kind"] for r in records).items())),
        "exception_records": sum(r["is_exception"] for r in records),
        "observation_records": sum(not r["is_exception"] for r in records),
        "exceptions_by_category": dict(sorted(Counter(r["category"] for r in records if r["is_exception"]).items())),
        "exceptions_by_direction": dict(
            sorted(Counter(r["exclusion_direction"] for r in records if r["is_exception"]).items())
        ),
        "declared_test_functions": len(declared),
        "collected_nodeids": None if collected is None else len(collected),
    }
    helper_assessments = {item["test"]: item for record in records for item in record["test_assessments"]}
    summary["direct_helper_tests"] = {
        "native_counterpart_or_route": sum(item["category"] is None for item in helper_assessments.values()),
        "intentional_python_implementation": sum(
            item["category"] == "intentional-Python-only" for item in helper_assessments.values()
        ),
        "unresolved_native_coverage": sum(item["category"] == "real-gap" for item in helper_assessments.values()),
    }
    result = {
        "schema_version": 3,
        "summary": summary,
        "limitations": [
            "Static source inventory, not execution results or proof of product defects.",
            "Declared scope expansion follows local fixture/helper references conservatively; dynamic imports, cross-module fixtures, and generated parity cases require collection/runtime evidence.",
            "Engine-aware imports and explicit Rust/differential routing are observations with is_exception=false and category=null.",
            "real-gap includes Python-only omissions and unproven Rust coverage, not only confirmed Rust failures.",
            "Module dependency gates and integration deselection can overlap behavioral omissions; records are not additive test counts.",
            "Only actual exclusions appear in exceptions. affected_scope=self means the source test; module means its module; subtree means the conftest directory (conservative); other values reference affected_scopes. rationale references explanations. Routing observations contribute counts only. Empty conditions are omitted. Helper assessments retain native counterpart evidence.",
        ],
        "selection_rules": selection,
        "exceptions": records,
    }
    if args.junit:
        xml = ET.parse(args.junit).getroot()
        cases = list(xml.iter("testcase"))
        omissions = []
        for case in cases:
            for skip in case.findall("skipped"):
                path = case.attrib.get("classname", "").replace(".", "/")
                message = skip.attrib.get("message", "")
                category, rationale = classify(path, case.attrib.get("name", ""), "skip", message, "")
                omissions.append(
                    {
                        "classname": case.attrib.get("classname"),
                        "name": case.attrib.get("name"),
                        "type": skip.attrib.get("type"),
                        "reason": message,
                        "category": category,
                        "rationale": rationale,
                    }
                )
        result["execution_evidence"] = {
            "label": args.evidence_label,
            "testcases": len(cases),
            "skipped": len(omissions),
            "failures": sum(len(case.findall("failure")) for case in cases),
            "errors": sum(len(case.findall("error")) for case in cases),
            "skip_reasons": dict(sorted(Counter(item["reason"] for item in omissions).items())),
            "omissions": omissions,
            "limitations": "JUnit does not enumerate deselected tests. Historical results do not validate current edits.",
        }
    args.output.write_text(json.dumps(compact_inventory(result), indent=2) + "\n")
    lines = [
        "# Rust parity exception inventory",
        "",
        "Generated by `uv run --no-project scripts/audit_engine_exceptions.py`.",
        "",
        "This is an auditable source inventory, not a passing-test claim. The JSON contains every matched source expression, enclosing condition, qualified scope, and statically affected test function.",
        "",
        "## Counts",
        "",
        "```json",
        json.dumps(summary, indent=2),
        "```",
        "",
        "## Interpretation",
        "",
        *["- " + item for item in result["limitations"]],
        "",
        "- Missing native Rust is an `optional-host` gate operationally, but unacceptable for a Rust-readiness run.",
        "- `--test-engine rust` selects the public layer. Importing that layer from its implementation module does not pin Python. Direct `SQLGenerator` and `QueryRewriter` use must be assessed separately.",
        "- CI runs Python 3.11–3.14 for Python and 3.12 for Rust. Both shared lanes exclude `integration`; integration.yml supplies separate engine runs. Inspect `selection_rules` for all commands and environment controls.",
        "- Zero executable xfail records means the prose mention of xfail in LookML fixtures is not an active exemption.",
        "",
        "## Behavioral omissions and Python pins",
        "",
        "| Source scope | Kind | Reason |",
        "| --- | --- | --- |",
    ]
    for record in records:
        if record["category"] == "real-gap" and record["kind"] != "implementation-import":
            ref = record["path"] + "::" + record["scope"]
            lines.append(f"| `{ref}` | {record['kind']} | {record['rationale']} |")
    lines += [
        "",
        "## Direct implementation coverage",
        "",
        "The JSON enumerates direct helper imports and their scopes. These are coverage warnings, not assertions that every helper test should be rewritten: implementation-unit contracts can remain Python-specific when their public behavior has independent native coverage. The parity replay in `tests/core/test_pure_rust_python_test_parity.py` selects modules, signatures, and explicit node IDs; it does not replay every Python test. Its monkeypatches and `tests/rust_layer_adapter.py` routing are included in the inventory.",
        "",
        "To expand to parametrized IDs, save the output of an authorized collect-only run to a file, then pass `--collected-nodeids FILE`. The report does not import tests, install packages, or compile extensions.",
    ]
    helper_modules = {}
    for item in helper_assessments.values():
        counts = helper_modules.setdefault(item["test"].split("::", 1)[0], Counter())
        counts[item["category"] or "native-counterpart"] += 1
    lines += [
        "",
        "Each count below is a unique declared function, not parametrized cases. Native counterparts are identified by exact replay selection or a co-located explicit native route; unresolved means the static audit has not established coverage.",
        "",
        "| Helper consumer | Native counterpart / route | Private Python contract | Unresolved native coverage |",
        "| --- | ---: | ---: | ---: |",
    ]
    for path, counts in sorted(helper_modules.items()):
        lines.append(
            f"| `{path}` | {counts['native-counterpart']} | {counts['intentional-Python-only']} | {counts['real-gap']} |"
        )
    if args.junit:
        evidence = result["execution_evidence"]
        lines += [
            "",
            "## Execution evidence",
            "",
            evidence["label"],
            "",
            f"JUnit: {evidence['testcases']} cases, {evidence['skipped']} skipped, {evidence['failures']} failures, {evidence['errors']} errors.",
            "",
            evidence["limitations"],
            "",
            "| Skip reason | Cases |",
            "| --- | ---: |",
        ]
        lines += [f"| {reason.replace('|', '/')} | {count} |" for reason, count in evidence["skip_reasons"].items()]
    args.markdown.write_text("\n".join(lines) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
