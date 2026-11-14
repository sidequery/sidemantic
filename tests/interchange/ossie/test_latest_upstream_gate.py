"""Run latest-profile documents through the unmodified pinned Apache validator."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from sidemantic import Dimension, Metric, Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.interchange.ossie import (
    LATEST_OSSIE_SCHEMA_COMMIT,
    serialize_ossie_document,
    synthesize_ossie_document,
)

ROOT = Path(__file__).parents[2] / "ossie-fixtures/upstream/172a3e5"


def validate(path: Path, family: str = "logical") -> subprocess.CompletedProcess:
    schema = {
        "logical": "core-spec/ossie-schema.json",
        "ontology": "ontology/ontology.json",
        "mapping": "ontology/mapping.json",
    }[family]
    return subprocess.run(
        [sys.executable, str(ROOT / "validation/validate.py"), "--schema", str(ROOT / schema), str(path)],
        cwd=ROOT,
        env={key: value for key, value in os.environ.items() if key not in {"PYTHONPATH", "OSSIE_SCHEMA"}},
        capture_output=True,
        text=True,
        check=False,
    )


def test_latest_validator_and_core_schema_are_exact_pinned_assets():
    expected = {
        "validation/validate.py": "4dea25c854e1b9d4da74cd568304133cdc4b679dd59594c426cf4370ddadcab7",
        "python/src/ossie/_loading.py": "e74ed8fc142a6d790b94ce09f81380912bbf6b855bc207feb916f4ee7d2e36ee",
        "core-spec/ossie-schema.json": "ea513794167c8d67e29fb7a7c537c9e2c33e718fb1e50becbe1ac389884ae662",
        "ontology/ontology.json": "228b589ad950c557199f9049eb087b0c9cf8e6acbd6dab70e1b408dd1061fde2",
        "ontology/mapping.json": "8db74be12cda94e23240cc502bb2ac0580b69a05431fda35ee0c9e8b7e6f096b",
    }
    for path, digest in expected.items():
        assert hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == digest


@pytest.mark.parametrize("family", ["logical", "ontology", "mapping"])
def test_latest_upstream_examples_pass_their_official_family_validator(family):
    filename = "semantic_model" if family == "logical" else family
    result = validate(ROOT / f"examples/flights.{filename}.yaml", family)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Validation PASSED" in result.stdout


def test_latest_native_graph_export_expands_derived_metric_references(tmp_path):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            dimensions=[Dimension(name="category", type="categorical")],
            metrics=[
                Metric(name="total", agg="sum", sql="amount"),
                Metric(name="double_total", type="derived", sql="total * 2"),
            ],
        )
    )
    result = synthesize_ossie_document(graph, scope_name="sales", expression_dialect="ANSI_SQL", portable_only=True)
    assert result.valid, result.diagnostics
    assert result.document.schema_revision == LATEST_OSSIE_SCHEMA_COMMIT
    output = tmp_path / "native.ossie.json"
    output.write_bytes(serialize_ossie_document(result.document, "json").data)
    checked = validate(output)
    assert checked.returncode == 0, checked.stdout + checked.stderr
    assert "Warning" not in checked.stdout, checked.stdout


@pytest.mark.parametrize("invalid", [False, True])
def test_latest_dataset_scoping_and_schema_are_enforced(tmp_path, invalid):
    document = {
        "version": "0.2.0.dev0",
        "name": "sales",
        "datasets": [
            {
                "name": "orders",
                "source": "orders",
                "fields": [{"name": "amount", "expression": {"sql": "amount * 2"}}],
                "metrics": [{"name": "total", "expression": {"sql": "SUM(amount)"}}],
            }
        ],
    }
    if invalid:
        document["datasets"][0]["metrics"][0]["expression"]["sql"] = "SUM(undeclared)"
    output = tmp_path / "scope.ossie.json"
    output.write_text(json.dumps(document))
    checked = validate(output)
    assert (checked.returncode == 0) is (not invalid), checked.stdout + checked.stderr
    if invalid:
        assert "undeclared" in checked.stdout
