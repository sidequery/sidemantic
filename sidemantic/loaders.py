"""Auto-discovery loaders for semantic layer definitions."""

import copy
import logging
import runpy
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from sidemantic.core.semantic_layer import SemanticLayer


def load_from_directory(layer: "SemanticLayer", directory: str | Path, *, strict: bool = True) -> None:
    """Load all semantic layer definitions from a directory.

    Automatically detects and parses Cube, Hex, LookML, and other formats.
    Infers relationships based on foreign key naming conventions.

    Args:
        layer: SemanticLayer to add models to
        directory: Directory containing semantic layer files
        strict: If True, fail on parse errors in detected semantic files. If
            False, log parse errors and continue loading other files.

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
    from sidemantic.adapters.tmdl import TMDLAdapter
    from sidemantic.adapters.yardstick import YardstickAdapter

    directory = Path(directory)
    if not directory.exists():
        raise ValueError(f"Directory {directory} does not exist")

    # Collect parsed definitions first, then register in dependency order.
    all_models = {}
    all_metrics = {}
    all_parameters = {}
    # Snowflake table-scoped metrics whose table lives in another file, held as
    # (table_name, Metric) pairs so same-named scoped metrics never collide.
    all_pending_table_metrics: list = []
    # Snowflake relationship definitions whose tables live in other files.
    all_pending_relationships: list = []
    import_warnings: list[dict[str, object]] = []

    # Check for SML repository (catalog.yml/atscale.yml or object_type files)
    if _try_load_sml(layer, directory, all_models):
        return

    # TMDL projects are folder-based. Parse a project root once instead of
    # treating each .tmdl file as an independent model.
    tmdl_root = None
    definition_dir = directory / "definition"
    if definition_dir.is_dir() and list(definition_dir.rglob("*.tmdl")):
        tmdl_root = definition_dir
    elif list(directory.rglob("*.tmdl")):
        tmdl_root = directory

    if tmdl_root:
        try:
            graph = TMDLAdapter().parse(tmdl_root)
            _merge_graph_passthrough_metadata(layer.graph, graph)
            _extend_import_warnings(import_warnings, graph)
            for model in graph.models.values():
                if not hasattr(model, "_source_format"):
                    model._source_format = "TMDL"
                if not hasattr(model, "_source_file"):
                    model._source_file = str(tmdl_root.relative_to(directory))
            all_models.update(graph.models)
            all_metrics.update(graph.metrics)
            all_parameters.update(graph.parameters)
        except Exception as e:
            _append_import_warning(
                import_warnings,
                code="tmdl_parse_error",
                message=str(e),
                source_format="TMDL",
                source_file=str(tmdl_root.relative_to(directory)),
            )
            _handle_parse_error(tmdl_root, e, strict=strict)
            logging.warning("Could not parse TMDL models in %s: %s", tmdl_root, e)

    _load_graphene_project(directory, all_models, all_metrics, all_parameters, strict=strict)

    # Find and parse all files
    for file_path in directory.rglob("*"):
        if not file_path.is_file():
            continue

        if _try_load_python_file(file_path, directory, all_models, import_warnings, strict=strict):
            continue

        # Detect format and parse
        adapter = None
        suffix = file_path.suffix.lower()

        if suffix == ".tmdl":
            if tmdl_root:
                continue
            adapter = TMDLAdapter()
        elif suffix == ".lkml":
            adapter = LookMLAdapter()
        elif suffix == ".malloy":
            from sidemantic.adapters.malloy import MalloyAdapter

            adapter = MalloyAdapter()
        elif suffix == ".gsql":
            continue
        elif suffix == ".sql":
            content = file_path.read_text()
            if _looks_like_yardstick_sql(content):
                adapter = YardstickAdapter(dialect=layer.dialect or "duckdb")
            else:
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
            elif (
                '"semantic_model"' in content
                and '"datasets"' in content
                and _is_under_osi_tree(file_path, directory)
                and not _is_generated_artifact(file_path, directory)
            ):
                # Released-spec OSI profile (dbt OSI consumer) ships as JSON in an
                # OSI/ directory at the project root. Mirror the YAML detection
                # (semantic_model + datasets), but only inside that OSI/ tree:
                # dbt's OSI consumer scans only ``<project_root>/OSI/``, so an
                # archived or scratch OSI .json elsewhere under the project must
                # not add stale models or collide with the real sources.
                # Skip dbt-generated copies (e.g. target/osi_document.json) so a
                # `dbt compile` artifact never shadows the real OSI/ sources.
                try:
                    is_osi = _looks_like_osi_json(content)
                except ValueError as e:
                    # The file textually looks like OSI (semantic_model + datasets)
                    # but is malformed JSON. Surface it as a parse error instead of
                    # silently skipping, mirroring the malformed-YAML handling above.
                    _handle_parse_error(file_path, e, strict=strict)
                    continue
                if is_osi:
                    adapter = OSIAdapter()
        elif suffix == ".aml":
            from sidemantic.adapters.holistics import HolisticsAdapter

            adapter = HolisticsAdapter()
        elif suffix == ".tml":
            adapter = ThoughtSpotAdapter()
        elif suffix in (".tds", ".twb", ".tdsx", ".twbx"):
            from sidemantic.adapters.tableau import TableauAdapter

            adapter = TableauAdapter()
        elif suffix in (".yml", ".yaml"):
            # Try to detect which format by reading the file
            content = file_path.read_text()
            try:
                yaml_data = _load_yaml_mapping(content)
            except Exception as e:
                # Current Hex Semantic Authoring files are multi-document YAML
                # (``---``-separated, typed resources). ``yaml.safe_load`` rejects
                # those before any single-document detection runs, so check for
                # Hex explicitly here before treating the file as unparseable.
                if _looks_like_hex_yaml(content):
                    adapter = HexAdapter()
                elif _looks_like_semantic_yaml_text(content):
                    _handle_parse_error(file_path, e, strict=strict)
                    continue
                else:
                    continue
                yaml_data = None
            # Check for MetricFlow before Sidemantic native since
            # "semantic_models:" contains "models:" as a substring
            if yaml_data is None:
                # Format already resolved on the multi-document fallback path.
                pass
            elif _yaml_has_top_level_key(yaml_data, "semantic_models"):
                adapter = MetricFlowAdapter()
            elif _yaml_has_top_level_key(yaml_data, "semantic_model") and _yaml_has_top_level_key(
                yaml_data, "datasets"
            ):
                adapter = OSIAdapter()
            elif _yaml_has_top_level_key(yaml_data, "cubes") or (
                _yaml_has_top_level_key(yaml_data, "views") and _contains_yaml_key(yaml_data, "measures")
            ):
                adapter = CubeAdapter()
            # Check for Sidemantic native format (explicit models: key)
            elif _yaml_has_top_level_key(yaml_data, "models"):
                adapter = SidemanticAdapter()
            elif _looks_like_native_sidemantic_yaml(yaml_data):
                adapter = SidemanticAdapter()
            elif _yaml_has_top_level_key(yaml_data, "tables") and _contains_yaml_key(yaml_data, "base_table"):
                # Snowflake Cortex Semantic Model format. Checked before the generic
                # MetricFlow `metrics:` + `type:` heuristic because a Cortex file may
                # carry top-level `metrics:` and `data_type:` while `base_table` is a
                # Snowflake-only signal MetricFlow never has.
                adapter = SnowflakeAdapter()
            elif _looks_like_snowflake_metrics_file(yaml_data):
                # Cortex top-level metrics split into their own file (table + expr,
                # no tables section). Route to Snowflake so the metrics defer and
                # attach to tables defined in sibling files.
                adapter = SnowflakeAdapter()
            elif _yaml_has_top_level_key(yaml_data, "metrics") and "type: " in content:
                adapter = MetricFlowAdapter()
            elif _is_hex_resource_mapping(yaml_data):
                # Single-document Hex (legacy ``base_sql_table``/``measures`` form
                # or a current typed ``type: model``/``type: view`` resource).
                adapter = HexAdapter()
            elif (
                _contains_yaml_key(yaml_data, "table")
                and _contains_yaml_key(yaml_data, "db_table")
                and _contains_yaml_key(yaml_data, "columns")
            ):
                adapter = ThoughtSpotAdapter()
            elif _contains_yaml_key(yaml_data, "worksheet") and _contains_yaml_key(yaml_data, "worksheet_columns"):
                adapter = ThoughtSpotAdapter()
            elif (
                _contains_yaml_key(yaml_data, "model")
                and _contains_yaml_key(yaml_data, "model_tables")
                and _contains_yaml_key(yaml_data, "columns")
            ):
                # ThoughtSpot TML Model object (export_schema_version v2)
                adapter = ThoughtSpotAdapter()
            elif _looks_like_bsl_yaml(yaml_data):
                # BSL format uses _.column syntax for expressions
                adapter = BSLAdapter()
            elif "type: metrics_view" in content:
                adapter = RillAdapter()
            elif (
                _contains_yaml_key(yaml_data, "table_name")
                and _contains_yaml_key(yaml_data, "columns")
                and _contains_yaml_key(yaml_data, "metrics")
            ):
                adapter = SupersetAdapter()
            elif (
                _contains_yaml_key(yaml_data, "measures")
                and _contains_yaml_key(yaml_data, "dimensions")
                and (
                    _contains_yaml_key(yaml_data, "table_name")
                    or _contains_yaml_key(yaml_data, "table")
                    or _contains_yaml_key(yaml_data, "schema")
                )
            ):
                adapter = OmniAdapter()

        if adapter:
            adapter_name = adapter.__class__.__name__.replace("Adapter", "")
            try:
                graph = _parse_adapter_without_auto_registration(adapter, file_path)
                _merge_graph_passthrough_metadata(layer.graph, graph)
                _extend_import_warnings(import_warnings, graph)
                # Track source format for each model
                for model in graph.models.values():
                    if not hasattr(model, "_source_format"):
                        model._source_format = adapter_name
                    if not hasattr(model, "_source_file"):
                        model._source_file = str(file_path.relative_to(directory))
                for metric in graph.metrics.values():
                    if not hasattr(metric, "_source_format"):
                        metric._source_format = adapter_name
                    if not hasattr(metric, "_source_file"):
                        metric._source_file = str(file_path.relative_to(directory))
                all_models.update(graph.models)
                all_metrics.update(graph.metrics)
                all_parameters.update(graph.parameters)
                all_pending_table_metrics.extend(getattr(graph, "_pending_table_metrics", []))
                all_pending_relationships.extend(getattr(graph, "_pending_relationships", []))
            except Exception as e:
                _append_import_warning(
                    import_warnings,
                    code="adapter_parse_error",
                    message=str(e),
                    source_format=adapter_name,
                    source_file=str(file_path.relative_to(directory)),
                )
                _handle_parse_error(file_path, e, strict=strict)

    _resolve_native_model_inheritance(all_models, strict=strict)
    _resolve_native_metric_inheritance(all_metrics, strict=strict)

    # BSL files are parsed one at a time during auto-discovery. Finalize join
    # aliases after all files have been loaded so aliases can target models
    # declared in separate files.
    _finalize_bsl_join_aliases(all_models)

    # Attach Snowflake top-level metrics whose referenced table was defined in a
    # different file (each Snowflake file is parsed separately, so the table may
    # not have been known when the metric file was parsed).
    _resolve_snowflake_pending_table_metrics(all_models, all_metrics, all_pending_table_metrics)

    # Apply Snowflake relationships declared in a separate file before FK inference
    # so an explicit Cortex join takes precedence over a guessed one for the same
    # table pair.
    _apply_snowflake_pending_relationships(all_models, all_pending_relationships)

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

    _merge_import_warnings(layer.graph, import_warnings)

    # Rebuild adjacency graph to recognize all inferred relationships
    layer.graph.build_adjacency()


def _load_graphene_project(
    directory: Path,
    all_models: dict,
    all_metrics: dict,
    all_parameters: dict,
    *,
    strict: bool,
) -> None:
    """Parse Graphene `.gsql` files together so project-level links resolve."""
    from sidemantic.adapters.graphene import GrapheneAdapter

    if not any(directory.rglob("*.gsql")):
        return

    adapter = GrapheneAdapter()
    try:
        graph = adapter.parse(str(directory))
    except Exception as e:
        _handle_parse_error(directory, e, strict=strict)
        return

    adapter_name = adapter.__class__.__name__.replace("Adapter", "")
    for model in graph.models.values():
        if not hasattr(model, "_source_format"):
            model._source_format = adapter_name
        if not hasattr(model, "_source_file"):
            model._source_file = str(directory)
    all_models.update(graph.models)
    all_metrics.update(graph.metrics)
    all_parameters.update(graph.parameters)


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


def _finalize_bsl_join_aliases(all_models: dict) -> None:
    """Add BSL join alias models once directory-level loading has all models."""
    if not all_models:
        return

    from sidemantic.adapters.bsl import BSLAdapter
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    for model in all_models.values():
        graph.add_model(model)

    existing_alias_models = {
        name for name, model in all_models.items() if model.metadata and model.metadata.get("bsl_alias_of")
    }
    BSLAdapter()._add_join_alias_models(graph)
    for name in existing_alias_models:
        if name not in graph.models:
            all_models.pop(name, None)
    all_models.update(graph.models)


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


def _load_yaml_mapping(content: str) -> dict:
    """Parse YAML content and return a mapping, or an empty mapping for scalar/list YAML."""
    data = yaml.safe_load(content)
    return data if isinstance(data, dict) else {}


def _looks_like_osi_json(content: str) -> bool:
    """Return True for a released-spec OSI JSON document (dbt OSI consumer).

    Released OSI ships as JSON with a top-level ``semantic_model`` list whose
    entries contain ``datasets``. This mirrors the YAML OSI detection and avoids
    routing unrelated JSON (e.g. GoodData) to the OSI adapter.

    Raises ``ValueError`` when ``content`` is not valid JSON so callers that have
    already confirmed the OSI text markers can surface a parse error instead of
    silently skipping a malformed OSI document.
    """
    import json

    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}") from e
    if not isinstance(data, dict) or "semantic_model" not in data:
        return False
    models = data.get("semantic_model")
    if isinstance(models, dict):
        models = [models]
    if not isinstance(models, list):
        return False
    return any(isinstance(model, dict) and "datasets" in model for model in models)


# Directories that hold generated/compiled artifacts rather than source models.
# dbt writes a copy of the OSI document to ``target/`` on ``dbt compile``; routing
# those to the OSI adapter would resurrect deleted or stale models, so skip them.
_GENERATED_ARTIFACT_DIRS = frozenset({"target", "dbt_packages"})


def _is_generated_artifact(file_path: "Path", directory: "Path") -> bool:
    """Return True when ``file_path`` lives under a generated-artifact directory.

    Only path components *below* ``directory`` are considered so that loading a
    directory literally named ``target`` still works.
    """
    try:
        relative_parts = file_path.relative_to(directory).parts
    except ValueError:
        relative_parts = file_path.parts
    return any(part in _GENERATED_ARTIFACT_DIRS for part in relative_parts[:-1])


# dbt's OSI consumer (dbt Core 1.12+) only ingests released ``.json`` documents
# placed in an ``OSI/`` directory at the project root. Mirroring that scope keeps
# an archived or scratch OSI document under some other folder (e.g.
# ``backups/old_osi.json``) from quietly adding stale models or colliding with
# the real sources during ``sidemantic validate .``.
_OSI_TREE_DIR = "OSI"


def _is_under_osi_tree(file_path: "Path", directory: "Path") -> bool:
    """Return True when ``file_path`` lives under the project-root ``OSI/`` tree.

    Two layouts count as "inside the OSI tree":

    * A top-level ``OSI/`` child of the loaded project root (``validate .`` from
      the project root). Only the first relative path component is checked
      (case-insensitively, to match dbt accepting ``OSI`` regardless of
      filesystem case-folding).
    * The loaded ``directory`` *itself* being the ``OSI/`` directory (``validate
      OSI/`` pointed straight at the folder dbt users are told to drop these
      files in). The whole loaded tree is then the OSI tree, so any descendant at
      any depth counts -- matching how ``validate <project>`` (which rglobs and
      only checks the leading ``OSI/`` component) loads the same files.

    A JSON file sitting directly at a non-``OSI/`` project root or under any
    non-``OSI/`` subfolder is rejected even when it is OSI-shaped.
    """
    try:
        relative_parts = file_path.relative_to(directory).parts
    except ValueError:
        return False
    # ``validate OSI/`` points the loader root straight at the OSI directory. The
    # whole loaded tree is then the OSI tree, so accept every descendant at any
    # depth (a file directly in it, or under a subfolder of it). This keeps the
    # two documented entrypoints in agreement: loading the parent project accepts
    # the same files by their leading ``OSI/`` component.
    #
    # Resolve the loader root before reading its name so the OSI directory is
    # recognized even when it is the current working directory: ``cd OSI &&
    # sidemantic validate`` (or ``load_from_directory(layer, ".")`` after
    # ``chdir``) passes ``Path(".")``, whose raw ``.name`` is empty.
    if directory.resolve().name.casefold() == _OSI_TREE_DIR.casefold():
        return True
    # Otherwise require a top-level ``OSI/`` directory plus the file name.
    if len(relative_parts) < 2:
        return False
    return relative_parts[0].casefold() == _OSI_TREE_DIR.casefold()


def _is_hex_resource_mapping(data: object) -> bool:
    """Return True when a single YAML mapping is a Hex Semantic Authoring resource.

    Covers both the legacy single-document form (``base_sql_table``/
    ``base_sql_query`` + ``measures``) and the current typed form where each
    resource carries a ``type: model`` / ``type: view`` discriminator alongside
    an ``id``.
    """
    if not isinstance(data, dict):
        return False
    if data.get("type") in ("model", "view") and "id" in data:
        return True
    # ``HexAdapter`` accepts query-backed models (``base_sql_query``) in addition
    # to table-backed ones; both must be recognized so directory auto-discovery
    # does not silently skip query-backed Hex models on the CLI/MCP path.
    if not _contains_yaml_key(data, "measures"):
        return False
    return _contains_yaml_key(data, "base_sql_table") or _contains_yaml_key(data, "base_sql_query")


def _looks_like_hex_yaml(content: str) -> bool:
    """Detect Hex YAML, including multi-document (``---``-separated) files.

    Current Hex Semantic Authoring projects emit multiple typed resources in one
    file separated by ``---``. ``yaml.safe_load`` rejects those, so this helper
    uses ``safe_load_all`` and returns True when any document is a Hex resource.
    """
    try:
        documents = list(yaml.safe_load_all(content))
    except Exception:
        return False
    return any(_is_hex_resource_mapping(doc) for doc in documents)


def _looks_like_semantic_yaml_text(content: str) -> bool:
    """Return True when malformed YAML text contains a known semantic-layer key."""
    semantic_keys = (
        "base_sql_table",
        "cubes",
        "datasets",
        "dimensions",
        "measures",
        "metrics",
        "models",
        "semantic_model",
        "semantic_models",
        "table_name",
        "tables",
        "views",
        "worksheet",
    )
    prefixes = tuple(f"{key}:" for key in semantic_keys)
    return any(line.lstrip().startswith(prefixes) for line in content.splitlines())


def _looks_like_native_sidemantic_yaml(data: dict) -> bool:
    """Return True for explicit native Sidemantic YAML files without models."""
    from sidemantic.adapters.sidemantic import METRIC_FIELDS, NATIVE_FORMAT_VERSION, ROOT_FIELDS

    if not isinstance(data, dict):
        return False
    # A native file may carry only passthrough ``metadata`` (no metrics/parameters/
    # SQL) -- e.g. a CLI-first project that splits graph-level metadata into its own
    # sidecar. Recognize it as native when it declares the native ``version`` plus a
    # ``metadata`` block so the directory loader routes (not silently drops) it.
    if (
        data.get("version") == NATIVE_FORMAT_VERSION
        and isinstance(data.get("metadata"), dict)
        and set(data) <= ROOT_FIELDS
    ):
        return True
    if not any(_yaml_has_top_level_key(data, key) for key in ("metrics", "parameters", "sql_metrics", "sql_segments")):
        return False
    if data.get("version") == NATIVE_FORMAT_VERSION:
        return True
    if data.get("version") is not None:
        return False

    # The version key is optional in the native format. Unversioned files count
    # as native when their root keys match the native schema and metric entries
    # use flat native fields (MetricFlow nests details under type_params).
    if not set(data) <= ROOT_FIELDS:
        return False
    metrics = data.get("metrics") or []
    if not isinstance(metrics, list):
        return False
    return all(isinstance(metric_def, dict) and set(metric_def) <= METRIC_FIELDS for metric_def in metrics)


def _yaml_has_top_level_key(data: dict, key: str) -> bool:
    """Return True when a YAML mapping has an exact top-level key."""
    return isinstance(data, dict) and key in data


_SNOWFLAKE_TOP_LEVEL_SECTIONS = ("verified_queries", "custom_instructions", "module_custom_instructions")
# Per-metric keys that only Snowflake Cortex uses (not in the native METRIC_FIELDS).
_SNOWFLAKE_METRIC_KEYS = (
    "table",
    "access_modifier",
    "labels",
    "tags",
    "non_additive_dimensions",
    "using_relationships",
)


def _looks_like_snowflake_relationships(data: dict) -> bool:
    """Return True when a file's top-level ``relationships`` are Snowflake-shaped."""
    relationships = data.get("relationships")
    if not isinstance(relationships, list) or not relationships:
        return False
    return all(
        isinstance(rel, dict) and "left_table" in rel and "right_table" in rel and "relationship_columns" in rel
        for rel in relationships
    )


def _looks_like_snowflake_metrics_file(data: dict) -> bool:
    """Detect a split Snowflake Cortex sidecar without a ``tables`` section.

    Cortex projects may split top-level ``metrics:``, ``relationships:`` and/or the
    Snowflake-only sections (verified_queries / custom instructions) into their own
    file. Route such a file to the Snowflake adapter when it carries a Cortex-only
    signal:

    - a Snowflake-only top-level section (verified_queries / custom instructions),
      even when no ``metrics`` are present (instruction-only sidecar),
    - Snowflake-shaped top-level ``relationships`` (relationship-only sidecar), or
    - top-level ``metrics`` carrying a Snowflake-only metric key (``table`` or per-
      metric ``access_modifier``/``labels``/``tags``/``non_additive_dimensions``/
      ``using_relationships``), or
    - a root ``name`` alongside Cortex-shaped ``metrics`` -- a tableless view-metric
      sidecar whose only Cortex signal is the root ``name`` the native format rejects.

    Any present metrics must be Cortex-shaped (``expr`` with no MetricFlow
    ``type_params``/``measure`` markers). A tableless metrics file with no root
    ``name`` and none of these signals is left to native detection.
    """
    if not isinstance(data, dict) or "tables" in data:
        return False

    metrics = data.get("metrics")
    has_snowflake_metric_key = False
    has_cortex_metrics = False
    if metrics is not None:
        if not isinstance(metrics, list):
            return False
        # An empty ``metrics: []`` placeholder means "no metrics", not a
        # disqualifier: the sidecar may still carry a Snowflake section or
        # relationships signal below. Only validate Cortex shape when metrics
        # are actually present.
        if metrics:
            for metric in metrics:
                if not isinstance(metric, dict):
                    return False
                if "expr" not in metric:
                    return False
                if "type_params" in metric or "measure" in metric:
                    return False
                if any(key in metric for key in _SNOWFLAKE_METRIC_KEYS):
                    has_snowflake_metric_key = True
            has_cortex_metrics = True

    has_snowflake_section = any(section in data for section in _SNOWFLAKE_TOP_LEVEL_SECTIONS)
    # A tableless Cortex sidecar may carry only a root ``name`` plus view-level
    # metrics (no per-metric Snowflake key, no Snowflake sections). The root
    # ``name`` is a Cortex semantic-model field the native format rejects, so its
    # presence alongside Cortex-shaped metrics is a reliable Snowflake signal --
    # without it the file is dropped by both native and Snowflake detection.
    has_snowflake_root_name = has_cortex_metrics and isinstance(data.get("name"), str)
    return (
        has_snowflake_metric_key
        or has_snowflake_section
        or has_snowflake_root_name
        or _looks_like_snowflake_relationships(data)
    )


def _contains_yaml_key(value: object, key: str) -> bool:
    """Return True when a parsed YAML object contains an exact key anywhere."""
    if isinstance(value, dict):
        if key in value:
            return True
        return any(_contains_yaml_key(nested, key) for nested in value.values())
    if isinstance(value, list):
        return any(_contains_yaml_key(item, key) for item in value)
    return False


def _contains_bsl_expr(value: object) -> bool:
    """Return True when a YAML object contains a BSL deferred expression string."""
    if isinstance(value, str):
        return "_." in value
    if isinstance(value, dict):
        return any(_contains_bsl_expr(nested) for nested in value.values())
    if isinstance(value, list):
        return any(_contains_bsl_expr(item) for item in value)
    return False


def _looks_like_bsl_yaml(data: dict) -> bool:
    """Detect Boring Semantic Layer YAML without substring false positives."""
    if not isinstance(data, dict):
        return False

    model_section_keys = {
        "calculated_measures",
        "database",
        "dimensions",
        "filter",
        "joins",
        "measures",
        "primary_key",
        "time_dimension",
    }

    for model_name, model_def in data.items():
        if model_name == "profile":
            continue
        if not isinstance(model_def, dict) or "table" not in model_def:
            continue
        if model_section_keys.intersection(model_def) or _contains_bsl_expr(model_def):
            return True

    return False


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


def _handle_parse_error(file_path: Path, error: Exception, *, strict: bool) -> None:
    if strict:
        raise ValueError(f"Could not parse {file_path}: {error}") from error
    logging.warning("Could not parse %s: %s", file_path, error)


def _parse_adapter_without_auto_registration(adapter, file_path: Path):
    return _run_without_auto_registration(adapter.parse, str(file_path))


def _run_without_auto_registration(callback, *args):
    from sidemantic.core.registry import get_current_layer, set_current_layer

    previous_layer = get_current_layer()
    set_current_layer(None)
    try:
        return callback(*args)
    finally:
        set_current_layer(previous_layer)


def _copy_source_attrs(source, target) -> None:
    for attr in ("_source_format", "_source_file"):
        if hasattr(source, attr):
            setattr(target, attr, getattr(source, attr))


def _resolve_native_model_inheritance(all_models: dict, *, strict: bool) -> None:
    """Resolve Sidemantic-native model inheritance after directory-wide parsing."""
    native_children = {
        name: model
        for name, model in all_models.items()
        if getattr(model, "_source_format", None) == "Sidemantic" and model.extends
    }
    if not native_children:
        return

    from sidemantic.core.inheritance import merge_model, resolve_model_metric_inheritance

    resolved = {}
    resolving = set()

    def fail(message: str):
        if strict:
            raise ValueError(message)
        logging.warning(message)
        return None

    def resolve(name: str):
        if name in resolved:
            return resolved[name]

        model = all_models.get(name)
        if model is None:
            return fail(f"Native model '{name}' not found")

        if name in resolving:
            return fail(f"Circular native model inheritance detected for model '{name}'")

        if not model.extends:
            resolved[name] = model
            return model

        parent = all_models.get(model.extends)
        if parent is None:
            return fail(f"Native model '{name}' extends unknown model '{model.extends}'")

        resolving.add(name)
        try:
            if getattr(parent, "_source_format", None) == "Sidemantic" and parent.extends:
                parent = resolve(model.extends)
        finally:
            resolving.remove(name)

        if parent is None:
            return None

        merged = _run_without_auto_registration(merge_model, model, parent)
        _run_without_auto_registration(resolve_model_metric_inheritance, merged)
        _copy_source_attrs(model, merged)
        resolved[name] = merged
        all_models[name] = merged
        return merged

    for name in native_children:
        resolve(name)


def _resolve_native_metric_inheritance(all_metrics: dict, *, strict: bool) -> None:
    """Resolve Sidemantic-native graph metric inheritance after directory-wide parsing."""
    native_children = {
        name: metric
        for name, metric in all_metrics.items()
        if getattr(metric, "_source_format", None) == "Sidemantic" and metric.extends
    }
    if not native_children:
        return

    from sidemantic.core.inheritance import merge_metric

    resolved = {}
    resolving = set()

    def fail(message: str):
        if strict:
            raise ValueError(message)
        logging.warning(message)
        return None

    def resolve(name: str):
        if name in resolved:
            return resolved[name]

        metric = all_metrics.get(name)
        if metric is None:
            return fail(f"Native metric '{name}' not found")

        if name in resolving:
            return fail(f"Circular native metric inheritance detected for metric '{name}'")

        if not metric.extends:
            resolved[name] = metric
            return metric

        parent = all_metrics.get(metric.extends)
        if parent is None:
            return fail(f"Native metric '{name}' extends unknown metric '{metric.extends}'")

        resolving.add(name)
        try:
            if parent.extends:
                parent = resolve(metric.extends)
        finally:
            resolving.remove(name)

        if parent is None:
            return None

        merged = _run_without_auto_registration(merge_metric, metric, parent)
        _copy_source_attrs(metric, merged)
        resolved[name] = merged
        all_metrics[name] = merged
        return merged

    for name in native_children:
        resolve(name)


def _try_load_python_file(
    file_path: Path,
    directory: Path,
    all_models: dict,
    import_warnings: list[dict[str, object]],
    *,
    strict: bool,
) -> bool:
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
        _append_import_warning(
            import_warnings,
            code="python_parse_error",
            message=str(e),
            source_format="Python",
            source_file=str(file_path.relative_to(directory)),
        )
        _handle_parse_error(file_path, e, strict=strict)
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


def _extend_import_warnings(target: list[dict[str, object]], graph: object) -> None:
    warnings = getattr(graph, "import_warnings", None)
    if not isinstance(warnings, list):
        return
    for warning in warnings:
        if isinstance(warning, dict):
            target.append(dict(warning))


def _append_import_warning(
    target: list[dict[str, object]],
    *,
    code: str,
    message: str,
    source_format: str,
    source_file: str,
    context: str = "loader",
) -> None:
    target.append(
        {
            "code": code,
            "context": context,
            "source_format": source_format,
            "source_file": source_file,
            "message": message,
        }
    )


def _merge_import_warnings(graph: object, warnings: list[dict[str, object]]) -> None:
    existing = getattr(graph, "import_warnings", [])
    merged: list[dict[str, object]] = []
    if isinstance(existing, list):
        for warning in existing:
            if isinstance(warning, dict):
                merged.append(dict(warning))
    merged.extend(warnings)
    graph.import_warnings = merged


def _resolve_snowflake_pending_table_metrics(all_models: dict, all_metrics: dict, pending: list) -> None:
    """Re-attach Snowflake top-level metrics to tables defined in other files."""
    if not pending:
        return
    from sidemantic.adapters.snowflake import SnowflakeAdapter

    SnowflakeAdapter.resolve_pending_table_metrics(all_models, pending)
    # Any metric whose table is still unknown falls back to a graph-level metric
    # so it is not silently dropped.
    for _table_name, metric in pending:
        all_metrics.setdefault(metric.name, metric)
    pending.clear()


def _apply_snowflake_pending_relationships(all_models: dict, pending: list) -> None:
    """Apply Snowflake relationship definitions whose tables live in other files."""
    if not pending:
        return
    from sidemantic.adapters.snowflake import SnowflakeAdapter

    SnowflakeAdapter().apply_pending_relationships(pending, all_models)
    pending.clear()


def _deep_merge_metadata(target: dict, source: dict) -> None:
    """Recursively merge ``source`` into ``target``.

    Nested dicts are merged, list values are appended (deduplicated by value),
    and scalars from ``source`` overwrite. This keeps multi-file payloads such as
    Snowflake Cortex ``verified_queries`` from clobbering one another when several
    files are loaded from a directory.
    """
    for key, value in source.items():
        existing = target.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            _deep_merge_metadata(existing, value)
        elif isinstance(existing, list) and isinstance(value, list):
            for item in value:
                if item not in existing:
                    existing.append(copy.deepcopy(item))
        else:
            target[key] = copy.deepcopy(value)


def _merge_graph_passthrough_metadata(target_graph: object, source_graph: object) -> None:
    for name, value in vars(source_graph).items():
        if not name.startswith("_tmdl_"):
            continue
        setattr(target_graph, name, copy.deepcopy(value))

    # Merge graph-level metadata (e.g. Snowflake Cortex top-level sections) so the
    # CLI-first load -> export-native path round-trips them. Deep-merge so multiple
    # files in a directory each contribute their sections instead of overwriting.
    source_metadata = getattr(source_graph, "metadata", None)
    if isinstance(source_metadata, dict) and source_metadata:
        target_metadata = getattr(target_graph, "metadata", None)
        if not isinstance(target_metadata, dict):
            target_metadata = {}
            target_graph.metadata = target_metadata
        _deep_merge_metadata(target_metadata, source_metadata)

    # Carry over Snowflake dynamic top-level attributes set by the adapter. Lists
    # (verified_queries) accumulate across files; scalars take the latest value.
    for attr in ("verified_queries", "custom_instructions", "module_custom_instructions"):
        value = getattr(source_graph, attr, None)
        if not value:
            continue
        existing = getattr(target_graph, attr, None)
        if isinstance(existing, list) and isinstance(value, list):
            for item in value:
                if item not in existing:
                    existing.append(copy.deepcopy(item))
        elif isinstance(existing, dict) and isinstance(value, dict):
            # Dict-valued attrs (module_custom_instructions) must accumulate keys
            # across split files, not get overwritten by the last file.
            _deep_merge_metadata(existing, value)
        else:
            setattr(target_graph, attr, copy.deepcopy(value))


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


def _looks_like_yardstick_sql(content: str) -> bool:
    """Return True when SQL contains Yardstick `AS MEASURE <alias>` syntax."""
    if "measure" not in content.lower():
        return False

    from sqlglot import tokenize
    from sqlglot.tokens import TokenType

    try:
        tokens = tokenize(content, read="duckdb")
    except Exception:
        return False

    token_count = len(tokens)

    for i in range(token_count - 2):
        if tokens[i].token_type != TokenType.ALIAS:
            continue
        if tokens[i + 1].text.upper() != "MEASURE":
            continue
        if tokens[i + 2].token_type in (TokenType.VAR, TokenType.IDENTIFIER, TokenType.STRING):
            return True

    return False
