from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
CHECK_PATH = REPO_ROOT / "scripts" / "check_malloy_grammar_drift.py"


def _drift_check_module():
    spec = importlib.util.spec_from_file_location("malloy_grammar_drift_check", CHECK_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_bundled_grammar_matches_recorded_upstream_pin() -> None:
    drift_check = _drift_check_module()
    provenance = drift_check._read_provenance()

    assert provenance["version"] == "v0.0.407"
    assert provenance["commit"] == "cf1f6a6449562bd9b74fc14936f47af294c3a325"
    assert provenance["antlr_version"] == "4.13.2"
    drift_check.check_local_bundle(provenance)


def test_generated_artifact_inventory_must_be_complete() -> None:
    drift_check = _drift_check_module()
    provenance = drift_check._read_provenance()
    provenance["generated_files"].pop("MalloyParser.py")

    with pytest.raises(RuntimeError, match="unrecorded generated files: MalloyParser.py"):
        drift_check.check_local_bundle(provenance)
