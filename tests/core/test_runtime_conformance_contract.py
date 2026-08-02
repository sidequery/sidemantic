"""Machine checks for runtime claims and generated compatibility summaries."""

import json
from pathlib import Path

import yaml

from scripts.generate_compatibility_docs import generated_documents
from sidemantic.formats import semantic_formats

ROOT = Path(__file__).parents[2]
CONTRACT_PATH = ROOT / "docs" / "runtime-conformance.yml"
FIXTURE_MANIFEST = ROOT / "tests" / "native-fixtures" / "manifest.yml"


def test_runtime_contract_has_complete_evidenced_rows():
    contract = yaml.safe_load(CONTRACT_PATH.read_text())
    runtime_keys = set(contract["runtimes"])
    allowed_statuses = set(contract["status_legend"])
    fixture_manifest = yaml.safe_load(FIXTURE_MANIFEST.read_text())
    fixture_names = {fixture["name"] for fixture in fixture_manifest["fixtures"]}

    assert contract["schema_version"] == 1
    assert runtime_keys == {"python", "rust", "duckdb_extension", "wasm"}
    assert contract["runtimes"]["python"]["lifecycle"] == "stable"
    assert all(
        contract["runtimes"][runtime]["lifecycle"] == "experimental" for runtime in ("rust", "duckdb_extension", "wasm")
    )

    for capability_name, capability in contract["capabilities"].items():
        assert runtime_keys <= set(capability), f"{capability_name} is missing a runtime"
        for runtime in runtime_keys:
            record = capability[runtime]
            assert record["status"] in allowed_statuses
            if record["status"] != "unsupported":
                assert record.get("evidence"), f"{capability_name}.{runtime} has no evidence"
            for evidence in record.get("evidence", []):
                assert (ROOT / evidence).exists(), f"missing evidence: {evidence}"
            fixtures = record.get("fixtures")
            if isinstance(fixtures, list):
                assert set(fixtures) <= fixture_names
            elif fixtures is not None:
                assert fixtures in {"all", "all_valid"}


def test_rust_strict_matrix_points_to_experimental_runtime_contract():
    matrix = json.loads((ROOT / "docs" / "rust-parity-matrix.json").read_text())

    assert matrix["runtime_contract"] == str(CONTRACT_PATH.relative_to(ROOT))
    assert matrix["runtime_lifecycle"] == "experimental"
    assert matrix["subsystems"]["semantic_sql_rewriter"]["status"] == "rust_backed_opt_in"


def test_generated_compatibility_documents_are_current():
    for path, expected in generated_documents().items():
        assert path.read_text() == expected, f"regenerate {path.relative_to(ROOT)}"


def test_adapter_summary_covers_the_complete_format_registry():
    summary = (ROOT / "docs" / "compatibility" / "index.md").read_text()

    for spec in semantic_formats():
        assert f"| `{spec.name}` |" in summary
