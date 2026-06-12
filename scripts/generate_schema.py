#!/usr/bin/env python
"""Generate JSON Schema from Pydantic models for YAML editor support."""

import json
from copy import deepcopy
from pathlib import Path

from sidemantic import Dimension, Metric, Model, Parameter, Relationship, Segment


def add_native_relationship_aliases(schema: dict) -> dict:
    """Expose native YAML relationship aliases that map to Python API fields."""
    properties = schema.setdefault("properties", {})

    if "foreign_key" in properties and "foreign_key_columns" not in properties:
        foreign_key_columns = deepcopy(properties["foreign_key"])
        foreign_key_columns["title"] = "Foreign Key Columns"
        foreign_key_columns["description"] = "Explicit source-column list (alias for foreign_key)"
        properties["foreign_key_columns"] = foreign_key_columns

    if "primary_key" in properties and "primary_key_columns" not in properties:
        primary_key_columns = deepcopy(properties["primary_key"])
        primary_key_columns["title"] = "Primary Key Columns"
        primary_key_columns["description"] = "Explicit target-column list (alias for primary_key)"
        properties["primary_key_columns"] = primary_key_columns

    if "sql" not in properties:
        properties["sql"] = {
            "anyOf": [{"type": "string"}, {"type": "null"}],
            "default": None,
            "description": "Custom join SQL using {from} and {to} runtime placeholders",
            "title": "Sql",
        }

    return schema


def patch_relationship_schemas(schema: dict) -> None:
    """Patch every embedded Relationship schema emitted by Pydantic."""
    if not isinstance(schema, dict):
        return
    if schema.get("title") == "Relationship":
        add_native_relationship_aliases(schema)
    for value in schema.values():
        if isinstance(value, dict):
            patch_relationship_schemas(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    patch_relationship_schemas(item)


def generate_schema() -> dict:
    """Generate JSON Schema for sidemantic YAML files."""
    # Get schemas from pydantic models
    model_schema = Model.model_json_schema()
    metric_schema = Metric.model_json_schema()
    parameter_schema = Parameter.model_json_schema()

    # Build the full schema
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Sidemantic Semantic Layer",
        "description": "Schema for Sidemantic semantic layer YAML configuration",
        "type": "object",
        "properties": {
            "models": {
                "type": "array",
                "description": "Model definitions",
                "items": model_schema,
            },
            "metrics": {
                "type": "array",
                "description": "Top-level metric definitions (optional - can also define in models)",
                "items": metric_schema,
            },
            "parameters": {
                "type": "array",
                "description": "Parameter definitions for dynamic queries",
                "items": parameter_schema,
            },
        },
        "required": ["models"],
        # Collect all $defs from sub-schemas
        "$defs": {
            **model_schema.get("$defs", {}),
            **metric_schema.get("$defs", {}),
            **parameter_schema.get("$defs", {}),
            "Dimension": Dimension.model_json_schema(),
            "Metric": Metric.model_json_schema(),
            "Relationship": Relationship.model_json_schema(),
            "Segment": Segment.model_json_schema(),
            "Parameter": Parameter.model_json_schema(),
        },
    }

    patch_relationship_schemas(schema)

    return schema


if __name__ == "__main__":
    schema = generate_schema()
    root = Path(__file__).parent.parent

    output_path = root / "sidemantic-schema.json"
    with open(output_path, "w") as f:
        json.dump(schema, f, indent=2, sort_keys=True)
    print(f"Generated {output_path}")
