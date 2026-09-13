from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.interchange.ossie import (
    OssieSerialization,
    require_synthesized_document,
    serialize_ossie_document,
    synthesize_ossie_document,
)

_REPOSITORY_ROOT = Path(__file__).parents[3]
_UPSTREAM_ROOT = _REPOSITORY_ROOT / "tests" / "ossie-fixtures" / "upstream"
_VALIDATOR = _UPSTREAM_ROOT / "validation" / "validate.py"
_SCHEMAS = _UPSTREAM_ROOT / "schemas" / "logical"


def test_current_dialects_and_vendors_pass_pinned_apache_validator() -> None:
    fixture = _UPSTREAM_ROOT.parent / "cases" / "logical-0.2-current-dialects-vendors" / "document.json"

    completed = _run_pinned_validator(fixture, schema_version="0.2.0.dev0")

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "Validation PASSED" in completed.stdout


def _graph(*, include_datatypes: bool) -> SemanticGraph:
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="analytics.orders",
            primary_key="id",
            dimensions=[
                Dimension(
                    name="id",
                    type="numeric",
                    sql="id",
                    logical_data_type="Integer" if include_datatypes else None,
                ),
                Dimension(
                    name="order_date",
                    type="time",
                    sql="order_date",
                    logical_data_type="Date" if include_datatypes else None,
                ),
            ],
            metrics=[
                Metric(
                    name="revenue",
                    agg="sum",
                    sql="amount",
                    logical_data_type="Decimal" if include_datatypes else None,
                )
            ],
        )
    )
    return graph


def _run_pinned_validator(output_path: Path, *, schema_version: str) -> subprocess.CompletedProcess[str]:
    schema_path = _SCHEMAS / schema_version / "schema.json"
    environment = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    return subprocess.run(
        [sys.executable, str(_VALIDATOR), str(output_path), "--schema", str(schema_path)],
        cwd=_UPSTREAM_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize(
    ("schema_version", "serialization"),
    [("0.1.1", OssieSerialization.JSON), ("0.2.0.dev0", OssieSerialization.YAML)],
)
def test_canonical_sidemantic_export_passes_exact_pinned_apache_validator(
    tmp_path: Path,
    schema_version: str,
    serialization: OssieSerialization,
) -> None:
    synthesis = synthesize_ossie_document(
        _graph(include_datatypes=schema_version == "0.2.0.dev0"),
        scope_name="commerce",
        expression_dialect="ANSI_SQL",
        schema_version=schema_version,
        serialization=serialization,
    )
    document = require_synthesized_document(synthesis)
    output = tmp_path / f"canonical-{schema_version}.{serialization.value}"
    serialized = serialize_ossie_document(document, serialization)
    output.write_bytes(serialized.data)

    completed = _run_pinned_validator(output, schema_version=schema_version)

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert f"Validation PASSED: {output.name}" in completed.stdout


def test_pinned_apache_validator_rejects_deliberately_invalid_input() -> None:
    invalid = _UPSTREAM_ROOT / "cases" / "invalid-semantic-model.yaml"

    completed = _run_pinned_validator(invalid, schema_version="0.2.0.dev0")

    assert completed.returncode != 0
    combined = completed.stdout + completed.stderr
    assert "Validation FAILED" in combined
    assert "semantic_model" in combined


def test_gate_uses_local_validator_and_schema_paths_only(tmp_path: Path) -> None:
    output = tmp_path / "empty.json"
    output.write_text(json.dumps({"version": "0.1.1", "semantic_model": []}) + "\n")

    completed = _run_pinned_validator(output, schema_version="0.1.1")

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert str(_VALIDATOR) not in completed.stdout
    assert yaml.safe_load(output.read_text())["version"] == "0.1.1"
