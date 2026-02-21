"""Auto-discovery loaders for semantic layer definitions."""

import logging
import runpy
import sys
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
    from sidemantic.adapters.gooddata import GoodDataAdapter
    from sidemantic.adapters.hex import HexAdapter
    from sidemantic.adapters.lookml import LookMLAdapter
    from sidemantic.adapters.metricflow import MetricFlowAdapter
    from sidemantic.adapters.omni import OmniAdapter
    from sidemantic.adapters.osi import OSIAdapter
    from sidemantic.adapters.rill import RillAdapter
    from sidemantic.adapters.sidemantic import SidemanticAdapter
    from sidemantic.adapters.snowflake import SnowflakeAdapter
    from sidemantic.adapters.superset import SupersetAdapter
    from sidemantic.adapters.thoughtspot import ThoughtSpotAdapter

    directory = Path(directory)
    if not directory.exists():
        raise ValueError(f"Directory {directory} does not exist")

    # Collect parsed definitions first, then register in dependency order.
    all_models = {}
    all_metrics = {}
    all_parameters = {}

    # Check for SML repository (catalog.yml/atscale.yml or object_type files)
    if _try_load_sml(layer, directory, all_models):
        return

    # Find and parse all files
    for file_path in directory.rglob("*"):
        if not file_path.is_file():
            continue

        if _try_load_python_file(file_path, directory, all_models):
            continue

        # Detect format and parse
        adapter = None
        suffix = file_path.suffix.lower()

        if suffix == ".lkml":
            adapter = LookMLAdapter()
        elif suffix == ".malloy":
            from sidemantic.adapters.malloy import MalloyAdapter

            adapter = MalloyAdapter()
        elif suffix == ".sql":
            # Sidemantic SQL files (pure SQL or with YAML frontmatter)
            adapter = SidemanticAdapter()
        elif suffix == ".json":
            content = file_path.read_text()
            if '"ldm"' in content and '"datasets"' in content:
                adapter = GoodDataAdapter()
            elif '"projectModel"' in content:
                adapter = GoodDataAdapter()
            elif '"dateInstances"' in content or '"date_instances"' in content or '"dateDimensions"' in content:
                adapter = GoodDataAdapter()
            elif '"datasets"' in content and ('"dataSourceTableId"' in content or '"data_source_table_id"' in content):
                adapter = GoodDataAdapter()
        elif suffix == ".aml":
            from sidemantic.adapters.holistics import HolisticsAdapter

            adapter = HolisticsAdapter()
        elif suffix == ".tml":
            adapter = ThoughtSpotAdapter()
        elif suffix in (".yml", ".yaml"):
            # Try to detect which format by reading the file
            content = file_path.read_text()
            # Check for Sidemantic format first (explicit models: key)
            if "models:" in content:
                adapter = SidemanticAdapter()
            elif "semantic_model:" in content and "datasets:" in content:
                adapter = OSIAdapter()
            elif "cubes:" in content or "views:" in content and "measures:" in content:
                adapter = CubeAdapter()
            elif "semantic_models:" in content or "metrics:" in content and "type: " in content:
                adapter = MetricFlowAdapter()
            elif "base_sql_table:" in content and "measures:" in content:
                adapter = HexAdapter()
            elif "table:" in content and "db_table:" in content and "columns:" in content:
                adapter = ThoughtSpotAdapter()
            elif "worksheet:" in content and "worksheet_columns:" in content:
                adapter = ThoughtSpotAdapter()
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
                all_metrics.update(graph.metrics)
                all_parameters.update(graph.parameters)
            except Exception as e:
                # Skip files that fail to parse
                logging.warning("Could not parse %s: %s", file_path, e)

    # Infer cross-model relationships based on naming conventions
    _infer_relationships(all_models)

    # Add all models to the layer (now with relationships)
    for model in all_models.values():
        if model.name not in layer.graph.models:
            layer.add_model(model)

    # Register graph-level metrics and parameters after models.
    for metric in all_metrics.values():
        if metric.name not in layer.graph.metrics:
            layer.add_metric(metric)

    for parameter in all_parameters.values():
        if parameter.name not in layer.graph.parameters:
            layer.graph.add_parameter(parameter)

    # Rebuild adjacency graph to recognize all inferred relationships
    layer.graph.build_adjacency()


def _load_sml_directory(layer: "SemanticLayer", directory: Path, all_models: dict) -> None:
    """Parse an SML directory and load all models into the layer."""
    from sidemantic.adapters.atscale_sml import AtScaleSMLAdapter

    adapter = AtScaleSMLAdapter()
    graph = adapter.parse(str(directory))
    adapter_name = adapter.__class__.__name__.replace("Adapter", "")
    for model in graph.models.values():
        if not hasattr(model, "_source_format"):
            model._source_format = adapter_name
        if not hasattr(model, "_source_file"):
            model._source_file = str(directory)
    all_models.update(graph.models)
    _infer_relationships(all_models)
    for model in all_models.values():
        if model.name not in layer.graph.models:
            layer.add_model(model)
    layer.graph.build_adjacency()


def _looks_like_python_semantic_definition(file_path: Path) -> bool:
    """Return True if a Python file appears to contain semantic definitions."""
    name = file_path.name.lower()
    if name == "sidemantic.py" or name.endswith(".sidemantic.py"):
        return True

    if file_path.suffix.lower() != ".py":
        return False

    try:
        content = file_path.read_text()
    except Exception:
        return False

    if "sidemantic" not in content.lower():
        return False

    return any(
        token in content
        for token in (
            "Model(",
            "SemanticLayer(",
            "SemanticGraph(",
            "Dimension(",
            "Metric(",
        )
    )


def _extract_models_from_python_namespace(namespace: dict, fallback_models: dict) -> dict:
    """Extract model definitions from executed Python globals."""
    from sidemantic.core.model import Model
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.core.semantic_layer import SemanticLayer

    extracted = dict(fallback_models)
    visited: set[int] = set()

    def collect(candidate: object) -> None:
        candidate_id = id(candidate)
        if candidate_id in visited:
            return
        visited.add(candidate_id)

        if isinstance(candidate, Model):
            extracted[candidate.name] = candidate
            return
        if isinstance(candidate, SemanticLayer):
            extracted.update(candidate.graph.models)
            return
        if isinstance(candidate, SemanticGraph):
            extracted.update(candidate.models)
            return
        if isinstance(candidate, dict):
            for nested in candidate.values():
                collect(nested)
            return
        if isinstance(candidate, (list, tuple, set)):
            for nested in candidate:
                collect(nested)

    for key, value in namespace.items():
        if key.startswith("__"):
            continue
        collect(value)

    return extracted


def _try_load_python_file(file_path: Path, directory: Path, all_models: dict) -> bool:
    """Load semantic definitions from a Python file if it looks like Sidemantic code."""
    if not _looks_like_python_semantic_definition(file_path):
        return False

    from sidemantic.core.semantic_layer import SemanticLayer

    captured_layer = SemanticLayer(auto_register=True)
    namespace: dict = {}

    script_dir = str(file_path.parent)
    sys.path.insert(0, script_dir)
    try:
        with captured_layer:
            namespace = runpy.run_path(str(file_path))
    except Exception as e:
        logging.warning("Could not parse %s: %s", file_path, e)
        return False
    finally:
        if sys.path and sys.path[0] == script_dir:
            sys.path.pop(0)

    models = _extract_models_from_python_namespace(namespace, captured_layer.graph.models)
    if not models:
        return False

    for model in models.values():
        if not hasattr(model, "_source_format"):
            model._source_format = "Python"
        if not hasattr(model, "_source_file"):
            model._source_file = str(file_path.relative_to(directory))

    all_models.update(models)
    return True


def _try_load_sml(layer: "SemanticLayer", directory: Path, all_models: dict) -> bool:
    """Detect and load an AtScale SML repository. Returns True if SML was found."""
    for catalog_name in ("catalog.yml", "catalog.yaml", "atscale.yml", "atscale.yaml"):
        candidate = directory / catalog_name
        if candidate.exists():
            catalog_text = candidate.read_text()
            if "object_type" in catalog_text and "catalog" in catalog_text:
                _load_sml_directory(layer, directory, all_models)
                return True

    for sml_file in list(directory.rglob("*.yml")) + list(directory.rglob("*.yaml")):
        try:
            content = sml_file.read_text()
        except Exception:
            continue
        if "object_type" in content and "unique_name" in content:
            if any(
                token in content
                for token in (
                    "object_type: dataset",
                    "object_type: dimension",
                    "object_type: metric",
                    "object_type: metric_calc",
                    "object_type: model",
                    "object_type: composite_model",
                    "object_type: connection",
                )
            ):
                _load_sml_directory(layer, directory, all_models)
                return True

    return False


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
