#!/usr/bin/env python
"""Generate JSON Schema from Pydantic models for YAML editor support."""

import json
from pathlib import Path

from sidemantic import Dimension, Metric, Model, Parameter, Relationship, Segment


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

    return schema


if __name__ == "__main__":
    schema = generate_schema()
    root = Path(__file__).parent.parent

    output_path = root / "sidemantic-schema.json"
    with open(output_path, "w") as f:
        json.dump(schema, f, indent=2, sort_keys=True)
    print(f"Generated {output_path}")
