"""Auto-discovery loaders for semantic layer definitions."""

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sidemantic.core.semantic_layer import SemanticLayer


def load_from_directory(layer: "SemanticLayer", directory: str | Path) -> None:
    """Load all semantic layer definitions from a directory.

    Automatically detects and parses Cube, Hex, LookML, and other formats.
    Infers relationships based on foreign key naming conventions.

    Args:
        layer: SemanticLayer to add models to
        directory: Directory containing semantic layer files

    Example:
        >>> layer = SemanticLayer()
        >>> load_from_directory(layer, "semantic_models/")
        >>> # All models loaded and ready to query
    """
    from sidemantic.adapters.bsl import BSLAdapter
    from sidemantic.adapters.cube import CubeAdapter
    from sidemantic.adapters.hex import HexAdapter
    from sidemantic.adapters.lookml import LookMLAdapter
    from sidemantic.adapters.malloy import MalloyAdapter
    from sidemantic.adapters.metricflow import MetricFlowAdapter
    from sidemantic.adapters.omni import OmniAdapter
    from sidemantic.adapters.rill import RillAdapter
    from sidemantic.adapters.sidemantic import SidemanticAdapter
    from sidemantic.adapters.snowflake import SnowflakeAdapter
    from sidemantic.adapters.superset import SupersetAdapter

    directory = Path(directory)
    if not directory.exists():
        raise ValueError(f"Directory {directory} does not exist")

    # Collect all models first (so we can infer relationships after)
    all_models = {}

    # Find and parse all files
    for file_path in directory.rglob("*"):
        if not file_path.is_file():
            continue

        # Detect format and parse
        adapter = None
        suffix = file_path.suffix.lower()

        if suffix == ".lkml":
            adapter = LookMLAdapter()
        elif suffix == ".malloy":
            adapter = MalloyAdapter()
        elif suffix == ".sql":
            # Sidemantic SQL files (pure SQL or with YAML frontmatter)
            adapter = SidemanticAdapter()
        elif suffix in (".yml", ".yaml"):
            # Try to detect which format by reading the file
            content = file_path.read_text()
            # Check for Sidemantic format first (explicit models: key)
            if "models:" in content:
                adapter = SidemanticAdapter()
            elif "cubes:" in content or "views:" in content and "measures:" in content:
                adapter = CubeAdapter()
            elif "semantic_models:" in content or "metrics:" in content and "type: " in content:
                adapter = MetricFlowAdapter()
            elif "base_sql_table:" in content and "measures:" in content:
                adapter = HexAdapter()
            elif "tables:" in content and "base_table:" in content:
                # Snowflake Cortex Semantic Model format
                adapter = SnowflakeAdapter()
            elif "_." in content and ("dimensions:" in content or "measures:" in content):
                # BSL format uses _.column syntax for expressions
                adapter = BSLAdapter()
            elif "type: metrics_view" in content:
                adapter = RillAdapter()
            elif "table_name:" in content and "columns:" in content and "metrics:" in content:
                adapter = SupersetAdapter()
            elif (
                "measures:" in content
                and "dimensions:" in content
                and ("table_name:" in content or "table:" in content or "schema:" in content)
            ):
                adapter = OmniAdapter()

        if adapter:
            try:
                graph = adapter.parse(str(file_path))
                # Track source format for each model
                adapter_name = adapter.__class__.__name__.replace("Adapter", "")
                for model in graph.models.values():
                    if not hasattr(model, "_source_format"):
                        model._source_format = adapter_name
                    if not hasattr(model, "_source_file"):
                        model._source_file = str(file_path.relative_to(directory))
                all_models.update(graph.models)
            except Exception as e:
                # Skip files that fail to parse
                print(f"Warning: Could not parse {file_path}: {e}")

    # Infer cross-model relationships based on naming conventions
    _infer_relationships(all_models)

    # Add all models to the layer (now with relationships)
    for model in all_models.values():
        if model.name not in layer.graph.models:
            layer.add_model(model)

    # Rebuild adjacency graph to recognize all inferred relationships
    layer.graph.build_adjacency()


def _infer_relationships(models: dict) -> None:
    """Infer relationships between models based on foreign key naming conventions.

    Looks for patterns like:
    - orders.customer_id -> customers.id
    - line_items.order_id -> orders.id
    - products.category_id -> categories.id
    """
    from sidemantic.core.relationship import Relationship

    for model_name, model in models.items():
        # Look at all dimensions to find potential foreign keys
        for dimension in model.dimensions:
            dim_name = dimension.name.lower()

            # Check if this looks like a foreign key (ends with _id)
            if not dim_name.endswith("_id"):
                continue

            # Extract the referenced table name (e.g., customer_id -> customer)
            referenced_table = dim_name[:-3]  # Remove _id

            # Try both singular and plural forms
            potential_targets = [
                referenced_table,
                referenced_table + "s",  # customer -> customers
                referenced_table[:-1] if referenced_table.endswith("s") else referenced_table + "s",
            ]

            # Find if any of these tables exist
            for target in potential_targets:
                if target in models and target != model_name:
                    # Check if this relationship already exists
                    existing = [r for r in model.relationships if r.name == target]
                    if not existing:
                        # Add many_to_one relationship
                        model.relationships.append(
                            Relationship(name=target, type="many_to_one", foreign_key=dimension.name)
                        )

                        # Add reverse one_to_many relationship
                        target_model = models[target]
                        reverse_existing = [r for r in target_model.relationships if r.name == model_name]
                        if not reverse_existing:
                            target_model.relationships.append(
                                Relationship(name=model_name, type="one_to_many", foreign_key=dimension.name)
                            )
                    break
