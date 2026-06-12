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
    assert "Freshness" in defs
    assert "Metric" in defs
    assert "Relationship" in defs
    assert "Segment" in defs
    assert "Parameter" in defs
    assert "freshness" in schema["properties"]["models"]["items"]["properties"]


def test_generate_yaml_schema_refs_resolve_to_root_defs():
    schema = generate_yaml_schema()
    defs = schema["$defs"]

    def walk(value):
        if isinstance(value, dict):
            ref = value.get("$ref")
            if isinstance(ref, str) and ref.startswith("#/$defs/"):
                assert ref.removeprefix("#/$defs/") in defs
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(schema)


def test_generate_yaml_schema_includes_dax_authoring_fields():
    schema = generate_yaml_schema()

    model_props = schema["properties"]["models"]["items"]["properties"]
    top_metric_props = schema["properties"]["metrics"]["items"]["properties"]
    dimension_props = schema["$defs"]["Dimension"]["properties"]
    metric_props = schema["$defs"]["Metric"]["properties"]

    for props in (model_props, top_metric_props, dimension_props, metric_props):
        assert props["dax"]["anyOf"][0] == {"type": "string"}
        assert props["expression_language"]["anyOf"][0] == {"enum": ["sql", "dax"], "type": "string"}


def test_export_schema_writes_file(tmp_path):
    output_path = tmp_path / "schema.json"
    export_schema(output_path)

    data = json.loads(output_path.read_text())
    assert data["title"] == "Sidemantic Semantic Layer"
    assert "models" in data["properties"]
