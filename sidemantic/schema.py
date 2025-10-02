"""Generate JSON Schema from Pydantic models for YAML editor completion."""

import json
from pathlib import Path

from sidemantic.core.dimension import Dimension
from sidemantic.core.entity import Entity
from sidemantic.core.join import Join
from sidemantic.core.measure import Measure
from sidemantic.core.model import Model
from sidemantic.core.parameter import Parameter


def generate_yaml_schema() -> dict:
    """Generate JSON Schema for Sidemantic YAML format.

    Returns:
        JSON Schema dict compatible with YAML Language Server
    """
    # Get schemas from Pydantic models
    model_schema = Model.model_json_schema()
    dimension_schema = Dimension.model_json_schema()
    measure_schema = Measure.model_json_schema()
    join_schema = Join.model_json_schema()
    entity_schema = Entity.model_json_schema()
    parameter_schema = Parameter.model_json_schema()

    # Build complete schema
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Sidemantic Semantic Layer",
        "description": "Schema for Sidemantic semantic layer YAML configuration",
        "type": "object",
        "properties": {
            "models": {
                "type": "array",
                "description": "Model definitions",
                "items": model_schema
            },
            "metrics": {
                "type": "array",
                "description": "Top-level measure definitions (optional - can also define in models)",
                "items": measure_schema
            },
            "parameters": {
                "type": "array",
                "description": "Parameter definitions for dynamic queries",
                "items": parameter_schema
            }
        },
        "required": ["models"],
        "$defs": {
            "Dimension": dimension_schema,
            "Measure": measure_schema,
            "Join": join_schema,
            "Entity": entity_schema,
            "Parameter": parameter_schema
        }
    }

    return schema


def export_schema(output_path: str | Path = "sidemantic-schema.json"):
    """Export JSON Schema to file for editor completion.

    Args:
        output_path: Where to write the schema file

    Usage:
        In your YAML file, add at the top:
        # yaml-language-server: $schema=./sidemantic-schema.json
    """
    schema = generate_yaml_schema()

    output_path = Path(output_path)
    with output_path.open("w") as f:
        json.dump(schema, f, indent=2)

    print(f"✓ JSON Schema exported to: {output_path}")
    print(f"\nAdd this to the top of your YAML files:")
    print(f"# yaml-language-server: $schema=./{output_path.name}")


if __name__ == "__main__":
    export_schema()
