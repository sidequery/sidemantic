"""Generate JSON Schema from Pydantic models for YAML editor completion."""

import json
from pathlib import Path

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.parameter import Parameter
from sidemantic.core.relationship import Relationship


def generate_yaml_schema() -> dict:
    """Generate JSON Schema for Sidemantic YAML format.

    Returns:
        JSON Schema dict compatible with YAML Language Server
    """
    # Get schemas from Pydantic models
    model_schema = Model.model_json_schema()
    dimension_schema = Dimension.model_json_schema()
    metric_schema = Metric.model_json_schema()
    relationship_schema = Relationship.model_json_schema()
    parameter_schema = Parameter.model_json_schema()

    # Build complete schema
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Sidemantic Semantic Layer",
        "description": "Schema for Sidemantic semantic layer YAML configuration",
        "type": "object",
        "properties": {
            "models": {"type": "array", "description": "Model definitions", "items": model_schema},
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
        "$defs": {
            "Dimension": dimension_schema,
            "Metric": metric_schema,
            "Relationship": relationship_schema,
            "Parameter": parameter_schema,
        },
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
    print("\nAdd this to the top of your YAML files:")
    print(f"# yaml-language-server: $schema=./{output_path.name}")


if __name__ == "__main__":
    export_schema()
