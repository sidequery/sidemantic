"""Tests for YAML schema generation."""

import json

from sidemantic.schema import export_schema, generate_yaml_schema


def test_generate_yaml_schema_structure():
    schema = generate_yaml_schema()

    assert schema["title"] == "Sidemantic Semantic Layer"
    assert schema["type"] == "object"
    assert "models" in schema["properties"]
    assert "metrics" in schema["properties"]
    assert "parameters" in schema["properties"]
    assert schema["required"] == ["models"]

    defs = schema["$defs"]
    assert "Dimension" in defs
    assert "Metric" in defs
    assert "Relationship" in defs
    assert "Parameter" in defs


def test_export_schema_writes_file(tmp_path):
    output_path = tmp_path / "schema.json"
    export_schema(output_path)

    data = json.loads(output_path.read_text())
    assert data["title"] == "Sidemantic Semantic Layer"
    assert "models" in data["properties"]
