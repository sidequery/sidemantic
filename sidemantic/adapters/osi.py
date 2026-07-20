"""OSI (Open Semantic Interchange) adapter for importing and exporting OSI files.

OSI is a vendor-agnostic semantic model specification designed to enable
interoperability between data analytics, AI, and BI tools.

Two profiles are supported:

- The in-development profile (default): the ``0.2.0.dev0`` schema serialized as
  YAML (``.yml``/``.yaml``).
- The released interop profile: a ``0.1.x`` document serialized as JSON
  (``.json``), matching what dbt's OSI consumer (dbt Core 1.12+) ingests from an
  ``OSI/`` directory at a project root.

Both profiles share the same ``version`` + ``semantic_model`` structure, so a
single parser/exporter handles them; only the serialization and version string
differ. Import auto-detects the format from the file extension.

Spec: https://github.com/open-semantic-interchange/OSI
dbt consumer: https://docs.getdbt.com/docs/build/osi-semantic-models
"""

import json
from pathlib import Path
from typing import Any

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph

# Directories that hold generated/compiled artifacts rather than source models.
# dbt writes a copy of the OSI document to ``target/`` on ``dbt compile``; parsing
# those would duplicate or resurrect deleted/stale models, so they are skipped
# when an OSI directory (e.g. a dbt project root) is parsed.
_GENERATED_ARTIFACT_DIRS = frozenset({"target", "dbt_packages"})


def _is_generated_artifact(file_path: Path, directory: Path) -> bool:
    """Return True when ``file_path`` lives under a generated-artifact directory.

    Only path components *below* ``directory`` are considered so that parsing a
    directory literally named ``target`` still works.
    """
    try:
        relative_parts = file_path.relative_to(directory).parts
    except ValueError:
        relative_parts = file_path.parts
    return any(part in _GENERATED_ARTIFACT_DIRS for part in relative_parts[:-1])


class OSIAdapter(BaseAdapter):
    """Adapter for importing/exporting OSI (Open Semantic Interchange) YAML files.

    Transforms OSI definitions into Sidemantic format:
    - OSI semantic_model → SemanticGraph
    - OSI datasets → Models
    - OSI fields → Dimensions
    - OSI metrics → Metrics (graph-level)
    - OSI relationships → Relationships
    """

    OSI_VERSION = "0.2.0.dev0"

    # Released OSI versions accepted by downstream consumers such as dbt's OSI
    # consumer (dbt Core 1.12+). dbt only ingests released 0.1.x ``.json`` files
    # placed in an ``OSI/`` directory and raises on any other version string.
    RELEASED_OSI_VERSION = "0.1.1"
    RELEASED_OSI_VERSIONS = ("0.1.0", "0.1.1")

    # The released 0.1.x JSON Schema constrains custom_extensions[].vendor_name to
    # this enum (core-spec/osi-schema.json $defs/Vendor). dbt's OSI consumer
    # validates against it, so a released JSON export must not emit any other
    # vendor (e.g. the local "SIDEMANTIC" wrapper) or the document fails parsing.
    RELEASED_OSI_VENDORS = ("COMMON", "SNOWFLAKE", "SALESFORCE", "DBT", "DATABRICKS", "GOODDATA")
    # Vendor used to carry Sidemantic-owned / unknown-vendor extension payloads in
    # released JSON. COMMON is the OSI-blessed cross-vendor bucket.
    RELEASED_OSI_FALLBACK_VENDOR = "COMMON"

    # Output formats supported by export().
    SUPPORTED_EXPORT_FORMATS = ("yaml", "json")

    # OSI dialect preference order for extracting SQL expressions
    DIALECT_PREFERENCE = ["ANSI_SQL", "SNOWFLAKE", "DATABRICKS", "MAQL", "TABLEAU", "MDX"]

    # Dialects we can safely emit from SQL expressions. The current OSI schema
    # also allows MDX/TABLEAU/MAQL, but those are not SQL dialects sqlglot can
    # transpile from Sidemantic SQL without changing semantics.
    SUPPORTED_EXPORT_DIALECTS = ["ANSI_SQL", "SNOWFLAKE", "DATABRICKS"]
    _SQLGLOT_DIALECTS = {
        "SNOWFLAKE": "snowflake",
        "DATABRICKS": "databricks",
    }

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse OSI YAML files into semantic graph.

        Args:
            source: Path to OSI YAML file or directory

        Returns:
            Semantic graph with imported models and metrics

        Raises:
            FileNotFoundError: If the source path does not exist
        """
        source_path = Path(source)

        if not source_path.exists():
            raise FileNotFoundError(f"Path does not exist: {source_path}")

        graph = SemanticGraph()

        if source_path.is_dir():
            # Accept both the in-development YAML profile (.yml/.yaml) and the
            # released JSON profile (.json) consumed by dbt's OSI consumer.
            for pattern in ("*.yml", "*.yaml", "*.json"):
                for osi_file in source_path.rglob(pattern):
                    # Skip dbt-generated copies (e.g. target/osi_document.json) so
                    # a `dbt compile` artifact never duplicates or resurrects the
                    # real OSI/ sources when a project root is parsed directly.
                    if _is_generated_artifact(osi_file, source_path):
                        continue
                    # A dbt project root can contain unrelated JSON (config files,
                    # JSON arrays, etc.) outside target/. Only feed JSON that looks
                    # like an OSI document to the parser so unrelated files do not
                    # raise or overwrite real OSI metadata. This mirrors the
                    # directory loader's ``_looks_like_osi_json`` shape check.
                    if osi_file.suffix.lower() == ".json" and not self._looks_like_osi_json(osi_file):
                        continue
                    self._parse_file(osi_file, graph)
        else:
            self._parse_file(source_path, graph)

        # Rebuild adjacency graph after all models are added
        graph.build_adjacency()

        return graph

    @staticmethod
    def _looks_like_osi_json(file_path: Path) -> bool:
        """Return True when ``file_path`` is a released-spec OSI JSON document.

        Released OSI ships as JSON with a top-level ``semantic_model`` list whose
        entries contain ``datasets``. A dbt project root can hold unrelated JSON
        (config files, JSON arrays) outside ``target/``; this shape check keeps
        those out of the parser so they neither raise nor overwrite real OSI
        metadata. Mirrors the directory loader's ``_looks_like_osi_json``.

        Unreadable or invalid JSON is treated as non-OSI so unrelated files are
        silently skipped rather than aborting a directory parse.
        """
        try:
            text = file_path.read_text()
        except OSError:
            return False
        if not text.strip():
            return False
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return False
        if not isinstance(data, dict) or "semantic_model" not in data:
            return False
        models = data.get("semantic_model")
        if isinstance(models, dict):
            models = [models]
        if not isinstance(models, list):
            return False
        return any(isinstance(model, dict) and "datasets" in model for model in models)

    def _parse_file(self, file_path: Path, graph: SemanticGraph) -> None:
        """Parse a single OSI YAML file.

        Args:
            file_path: Path to YAML file
            graph: Semantic graph to add models/metrics to
        """
        with open(file_path) as f:
            if file_path.suffix.lower() == ".json":
                # Released-spec OSI profile (dbt consumer) ships as JSON.
                text = f.read()
                data = json.loads(text) if text.strip() else None
            else:
                # In-development OSI profile ships as YAML (the default).
                data = yaml.safe_load(f)

        if not data:
            return

        osi_meta = self._ensure_osi_metadata(graph)
        if data.get("version"):
            osi_meta["version"] = data["version"]
        if data.get("ontology"):
            osi_meta["ontology"] = data["ontology"]

        for sm_def, source, mapping_def in self._iter_semantic_models(data):
            self._remember_semantic_model_metadata(osi_meta, sm_def, source, mapping_def)
            self._parse_semantic_model(sm_def, graph)

    def _parse_semantic_model(self, sm_def: dict, graph: SemanticGraph) -> None:
        """Parse one OSI semantic model object into the graph."""
        # Parse datasets (equivalent to models)
        datasets = sm_def.get("datasets") or []
        for dataset_def in datasets:
            model = self._parse_dataset(dataset_def)
            if model:
                graph.add_model(model)

        # Parse relationships and attach to models
        relationships = sm_def.get("relationships") or []
        for rel_def in relationships:
            self._add_relationship_to_model(rel_def, graph)

        # Parse metrics (graph-level)
        metrics = sm_def.get("metrics") or []
        for metric_def in metrics:
            metric = self._parse_metric(metric_def)
            if metric:
                graph.add_metric(metric)

    def _iter_semantic_models(self, data: dict) -> list[tuple[dict, str, dict | None]]:
        """Return top-level and ontology-mapped semantic model definitions.

        The core OSI schema uses ``semantic_model`` as a top-level list. The new
        ontology spec embeds a single semantic model under each
        ``ontology_mappings[].semantic_model``. Those logical models can map
        cleanly to Sidemantic models, while ontology concepts themselves are
        preserved as metadata.
        """
        result: list[tuple[dict, str, dict | None]] = []

        semantic_models = data.get("semantic_model") or []
        if isinstance(semantic_models, dict):
            semantic_models = [semantic_models]

        for sm_def in semantic_models:
            if isinstance(sm_def, dict):
                result.append((sm_def, "semantic_model", None))

        ontology_mappings = data.get("ontology_mappings") or []
        for index, mapping_def in enumerate(ontology_mappings):
            if not isinstance(mapping_def, dict):
                continue
            sm_def = mapping_def.get("semantic_model")
            if isinstance(sm_def, dict):
                result.append((sm_def, f"ontology_mappings[{index}].semantic_model", mapping_def))

        return result

    def _ensure_osi_metadata(self, graph: SemanticGraph) -> dict[str, Any]:
        """Get/create the graph metadata section used by the OSI adapter."""
        return graph.metadata.setdefault("osi", {"semantic_models": []})

    def _remember_semantic_model_metadata(
        self,
        osi_meta: dict[str, Any],
        sm_def: dict,
        source: str,
        mapping_def: dict | None,
    ) -> None:
        """Preserve semantic-model-level OSI fields that do not map to models."""
        sm_meta: dict[str, Any] = {"source": source}
        for key in ("name", "description", "ai_context", "custom_extensions"):
            if key in sm_def:
                sm_meta[key] = sm_def[key]
        if mapping_def:
            mapping_meta = {
                key: mapping_def[key] for key in ("name", "description", "concept_mappings") if key in mapping_def
            }
            if mapping_meta:
                sm_meta["ontology_mapping"] = mapping_meta
        osi_meta.setdefault("semantic_models", []).append(sm_meta)

    def _parse_dataset(self, dataset_def: dict) -> Model | None:
        """Parse OSI dataset into Sidemantic Model.

        Args:
            dataset_def: Dataset definition dictionary

        Returns:
            Model instance or None
        """
        name = dataset_def.get("name")
        if not name:
            return None

        # Source is the table reference
        source = dataset_def.get("source")

        # Primary key - preserve full list for multi-column keys
        primary_key_list = dataset_def.get("primary_key") or []
        if len(primary_key_list) == 0:
            primary_key: str | list[str] | None = None
        elif len(primary_key_list) == 1:
            primary_key = primary_key_list[0]
        else:
            primary_key = primary_key_list

        # Unique keys - list of column lists
        unique_keys = dataset_def.get("unique_keys")

        # Parse fields (dimensions)
        dimensions = []
        for field_def in dataset_def.get("fields") or []:
            dim = self._parse_field(field_def)
            if dim:
                dimensions.append(dim)

        # Determine default time dimension
        default_time_dimension = None
        for dim in dimensions:
            if dim.type == "time":
                default_time_dimension = dim.name
                break

        # Build meta from ai_context and custom_extensions
        meta = None
        ai_context = dataset_def.get("ai_context")
        custom_extensions = self._decode_custom_extensions(dataset_def.get("custom_extensions"))
        if "ai_context" in dataset_def or custom_extensions is not None:
            meta = {}
            if "ai_context" in dataset_def:
                meta["ai_context"] = ai_context
            if custom_extensions is not None:
                meta["custom_extensions"] = custom_extensions

        return Model(
            name=name,
            table=source,
            description=dataset_def.get("description"),
            primary_key=primary_key,
            unique_keys=unique_keys,
            dimensions=dimensions,
            default_time_dimension=default_time_dimension,
            meta=meta,
        )

    def _parse_field(self, field_def: dict) -> Dimension | None:
        """Parse OSI field into Sidemantic Dimension.

        Args:
            field_def: Field definition dictionary

        Returns:
            Dimension instance or None
        """
        name = field_def.get("name")
        if not name:
            return None

        # Extract SQL expression from dialects (prefer ANSI_SQL)
        sql = self._extract_expression(field_def.get("expression"))

        # Determine dimension type from dimension.is_time
        dimension_meta = field_def.get("dimension") or {}
        is_time = dimension_meta.get("is_time", False)
        dim_type = "time" if is_time else "categorical"

        # Build meta from ai_context and custom_extensions
        meta = None
        ai_context = field_def.get("ai_context")
        custom_extensions = self._decode_custom_extensions(field_def.get("custom_extensions"))
        if "ai_context" in field_def or custom_extensions is not None:
            meta = {}
            if "ai_context" in field_def:
                meta["ai_context"] = ai_context
            if custom_extensions is not None:
                meta["custom_extensions"] = custom_extensions

        return Dimension(
            name=name,
            type=dim_type,
            sql=sql,
            description=field_def.get("description"),
            label=field_def.get("label"),
            granularity="day" if is_time else None,
            meta=meta,
        )

    def _parse_metric(self, metric_def: dict) -> Metric | None:
        """Parse OSI metric into Sidemantic Metric.

        OSI metrics contain full aggregate expressions like "SUM(dataset.field)".
        We parse these to extract the aggregation type and inner expression.

        Args:
            metric_def: Metric definition dictionary

        Returns:
            Metric instance or None
        """
        name = metric_def.get("name")
        if not name:
            return None

        # Extract SQL expression from dialects
        expression = self._extract_expression(metric_def.get("expression"))

        if not expression:
            return None

        # Build meta from ai_context and custom_extensions
        meta = None
        ai_context = metric_def.get("ai_context")
        custom_extensions = self._decode_custom_extensions(metric_def.get("custom_extensions"))
        if "ai_context" in metric_def or custom_extensions is not None:
            meta = {}
            if "ai_context" in metric_def:
                meta["ai_context"] = ai_context
            if custom_extensions is not None:
                meta["custom_extensions"] = custom_extensions

        # Let the Metric class handle aggregation parsing via its model_validator.
        # This properly handles complex expressions like SUM(x) / SUM(y) and
        # COUNT(DISTINCT col) using sqlglot.
        return Metric(
            name=name,
            sql=expression,
            description=metric_def.get("description"),
            meta=meta,
        )

    def _extract_expression(self, expression_def: dict | None) -> str | None:
        """Extract SQL expression from OSI expression definition.

        OSI expressions have a "dialects" array with dialect-specific expressions.
        We prefer ANSI_SQL but fall back to other dialects.

        Args:
            expression_def: Expression definition with dialects

        Returns:
            SQL expression string or None
        """
        if not expression_def:
            return None

        dialects = expression_def.get("dialects") or []

        # Build a map of dialect -> expression
        dialect_map = {}
        for d in dialects:
            dialect_name = d.get("dialect")
            expr = d.get("expression")
            if dialect_name and expr:
                dialect_map[dialect_name] = expr

        # Return first available in preference order
        for preferred in self.DIALECT_PREFERENCE:
            if preferred in dialect_map:
                return dialect_map[preferred]

        # Fallback to first available
        if dialects and dialects[0].get("expression"):
            return dialects[0]["expression"]

        return None

    def _add_relationship_to_model(self, rel_def: dict, graph: SemanticGraph) -> None:
        """Parse OSI relationship and add to the appropriate model.

        OSI relationships define:
        - from: dataset on the "many" side
        - to: dataset on the "one" side
        - from_columns: foreign key columns (can be multi-column)
        - to_columns: primary/unique key columns (can be multi-column)

        Args:
            rel_def: Relationship definition dictionary
            graph: Semantic graph with models
        """
        from_model = rel_def.get("from")
        to_model = rel_def.get("to")

        if not from_model or not to_model:
            return

        # Get the "from" model to add the relationship
        model = graph.models.get(from_model)
        if not model:
            return

        # Extract foreign key columns - preserve full list for multi-column keys
        from_columns = rel_def.get("from_columns") or []
        to_columns = rel_def.get("to_columns") or []

        # Normalize to appropriate type (str for single, list for multi)
        if len(from_columns) == 0:
            foreign_key: str | list[str] | None = None
        elif len(from_columns) == 1:
            foreign_key = from_columns[0]
        else:
            foreign_key = from_columns

        if len(to_columns) == 0:
            primary_key: str | list[str] | None = None
        elif len(to_columns) == 1:
            primary_key = to_columns[0]
        else:
            primary_key = to_columns

        # Create many_to_one relationship (from many -> to one)
        metadata = {}
        if rel_def.get("name"):
            metadata["osi_name"] = rel_def["name"]
        if "ai_context" in rel_def:
            metadata["ai_context"] = rel_def.get("ai_context")
        custom_extensions = self._decode_custom_extensions(rel_def.get("custom_extensions"))
        if custom_extensions is not None:
            metadata["custom_extensions"] = custom_extensions

        relationship = Relationship(
            name=to_model,
            type="many_to_one",
            foreign_key=foreign_key,
            primary_key=primary_key,
            metadata=metadata or None,
        )

        model.relationships.append(relationship)

    def export(
        self,
        graph: SemanticGraph,
        output_path: str | Path,
        dialects: list[str] | None = None,
        format: str | None = None,
        version: str | None = None,
    ) -> None:
        """Export semantic graph to OSI format.

        By default this emits the in-development OSI profile: the
        ``0.2.0.dev0`` schema as YAML. Passing ``format="json"`` (or a ``.json``
        output path) emits the released-spec interop profile consumed by dbt's
        OSI consumer (dbt Core 1.12+): a ``0.1.x`` document written as JSON,
        suitable for an ``OSI/`` directory at a dbt project root.

        Args:
            graph: Semantic graph to export
            output_path: Path to output file. ``.json`` extensions default the
                         format to JSON; otherwise YAML is used.
            dialects: List of OSI dialects to generate SQL expressions for.
                      Default is ["ANSI_SQL"]. Options: ANSI_SQL, SNOWFLAKE, DATABRICKS.
                      When multiple dialects specified, sqlglot is used for transpilation.
            format: Output format, ``"yaml"`` (default) or ``"json"``. When
                    omitted it is inferred from the output path extension.
            version: OSI schema version string to emit. Defaults to
                     ``0.2.0.dev0`` for YAML and the released ``0.1.1`` for JSON.
                     Released JSON exports must use a ``0.1.x`` version.
        """
        output_path = Path(output_path)

        format = self._resolve_export_format(format, output_path)
        version = self._resolve_export_version(version, format, graph)

        if not dialects:
            dialects = ["ANSI_SQL"]
        unsupported = [dialect for dialect in dialects if dialect not in self.SUPPORTED_EXPORT_DIALECTS]
        if unsupported:
            supported = ", ".join(self.SUPPORTED_EXPORT_DIALECTS)
            raise ValueError(f"Unsupported OSI export dialect(s): {', '.join(unsupported)}. Supported: {supported}")

        # Store dialects for use in export methods
        self._export_dialects = dialects
        # Released JSON validates against the 0.1.x enum-constrained schema; flag
        # it so extension export can coerce non-enum vendors to a released vendor.
        self._export_released_json = format == "json"

        # Resolve inheritance first
        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        # Build OSI semantic model
        semantic_model = self._export_semantic_model(resolved_models, graph)

        data = {"version": version, "semantic_model": [semantic_model]}

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            if format == "json":
                json.dump(data, f, indent=2, sort_keys=False)
            else:
                yaml.dump(data, f, sort_keys=False, default_flow_style=False)

    def _resolve_export_format(self, format: str | None, output_path: Path) -> str:
        """Determine the export format, inferring from the path extension."""
        if format is None:
            format = "json" if output_path.suffix.lower() == ".json" else "yaml"
        format = format.lower()
        if format not in self.SUPPORTED_EXPORT_FORMATS:
            supported = ", ".join(self.SUPPORTED_EXPORT_FORMATS)
            raise ValueError(f"Unsupported OSI export format: {format}. Supported: {supported}")
        return format

    def _resolve_export_version(self, version: str | None, format: str, graph: SemanticGraph) -> str:
        """Determine the OSI version string to emit for a given format."""
        if version is not None:
            if format == "json" and version not in self.RELEASED_OSI_VERSIONS:
                supported = ", ".join(self.RELEASED_OSI_VERSIONS)
                raise ValueError(
                    f"Released OSI JSON export requires a released version ({supported}); got {version!r}."
                )
            return version
        if format == "json":
            # Released-spec JSON: emit the latest released version dbt accepts.
            return self.RELEASED_OSI_VERSION
        return self._export_version(graph)

    def _generate_dialect_expressions(self, sql_expr: str) -> list[dict[str, str]]:
        """Generate expressions for multiple SQL dialects using sqlglot.

        Args:
            sql_expr: SQL expression in DuckDB/ANSI SQL format

        Returns:
            List of dialect expression dictionaries for OSI format
        """
        dialects = getattr(self, "_export_dialects", ["ANSI_SQL"])
        result = []

        for dialect in dialects:
            if dialect == "ANSI_SQL":
                result.append({"dialect": dialect, "expression": sql_expr})
            else:
                # Use sqlglot for transpilation
                import sqlglot

                target = self._SQLGLOT_DIALECTS.get(dialect)
                if target:
                    try:
                        transpiled = sqlglot.transpile(sql_expr, read="duckdb", write=target)[0]
                        result.append({"dialect": dialect, "expression": transpiled})
                    except Exception:
                        # Fallback to original expression if transpilation fails
                        result.append({"dialect": dialect, "expression": sql_expr})
                else:
                    result.append({"dialect": dialect, "expression": sql_expr})

        return result

    def _export_semantic_model(self, models: dict[str, Model], graph: SemanticGraph) -> dict[str, Any]:
        """Export models to OSI semantic model definition.

        Args:
            models: Resolved models dictionary
            graph: Original semantic graph (for graph-level metrics)

        Returns:
            OSI semantic model definition dictionary
        """
        result: dict[str, Any] = {
            "name": "semantic_model",
            "description": "Semantic model exported from Sidemantic",
        }
        osi_meta = (graph.metadata or {}).get("osi", {})
        semantic_models_meta = osi_meta.get("semantic_models") or []
        semantic_model_meta = semantic_models_meta[0] if semantic_models_meta else {}
        for key in ("name", "description", "ai_context"):
            if semantic_model_meta.get(key) is not None:
                result[key] = semantic_model_meta[key]
        custom_extensions = self._normalize_custom_extensions_for_export(semantic_model_meta.get("custom_extensions"))
        if custom_extensions:
            result["custom_extensions"] = custom_extensions

        # Export datasets
        datasets = []
        for model in models.values():
            dataset = self._export_dataset(model)
            datasets.append(dataset)
        result["datasets"] = datasets

        # Export relationships
        relationships = []
        for model in models.values():
            for rel in model.relationships:
                rel_def = self._export_relationship(model.name, rel, models)
                if rel_def:
                    relationships.append(rel_def)
        if relationships:
            result["relationships"] = relationships

        # Export graph-level metrics
        metrics = []
        for metric in graph.metrics.values():
            metric_def = self._export_metric(metric, models)
            if metric_def:
                metrics.append(metric_def)
        # Also export model-level metrics as graph-level (OSI style)
        for model in models.values():
            for metric in model.metrics:
                metric_def = self._export_metric(metric, models, model.name)
                if metric_def:
                    metrics.append(metric_def)
        if metrics:
            result["metrics"] = metrics

        return result

    def _export_dataset(self, model: Model) -> dict[str, Any]:
        """Export model to OSI dataset definition.

        Args:
            model: Model to export

        Returns:
            OSI dataset definition dictionary
        """
        dataset: dict[str, Any] = {"name": model.name}

        if model.sql:
            dataset["source"] = f"({model.sql})"
        elif model.table:
            dataset["source"] = model.table

        # Export primary_key as list (multi-column support)
        if model.primary_key:
            dataset["primary_key"] = model.primary_key_columns

        # Export unique_keys if present
        if model.unique_keys:
            dataset["unique_keys"] = model.unique_keys

        if model.description:
            dataset["description"] = model.description

        # Export fields (dimensions)
        if model.dimensions:
            fields = []
            for dim in model.dimensions:
                field = self._export_field(dim)
                fields.append(field)
            dataset["fields"] = fields

        # Export meta as ai_context and custom_extensions
        if model.meta:
            if "ai_context" in model.meta:
                dataset["ai_context"] = model.meta["ai_context"]
            if "custom_extensions" in model.meta:
                custom_extensions = self._normalize_custom_extensions_for_export(model.meta["custom_extensions"])
                if custom_extensions:
                    dataset["custom_extensions"] = custom_extensions

        return dataset

    def _export_field(self, dim: Dimension) -> dict[str, Any]:
        """Export dimension to OSI field definition.

        Args:
            dim: Dimension to export

        Returns:
            OSI field definition dictionary
        """
        field: dict[str, Any] = {"name": dim.name}

        # Build expression with dialect support
        sql_expr = dim.sql or dim.name
        field["expression"] = {"dialects": self._generate_dialect_expressions(sql_expr)}

        # Set dimension.is_time for time dimensions
        if dim.type == "time":
            field["dimension"] = {"is_time": True}

        if dim.description:
            field["description"] = dim.description

        if dim.label:
            field["label"] = dim.label

        # Export meta as ai_context and custom_extensions
        if dim.meta:
            if "ai_context" in dim.meta:
                field["ai_context"] = dim.meta["ai_context"]
            if "custom_extensions" in dim.meta:
                custom_extensions = self._normalize_custom_extensions_for_export(dim.meta["custom_extensions"])
                if custom_extensions:
                    field["custom_extensions"] = custom_extensions

        return field

    def _export_relationship(
        self, from_model: str, rel: Relationship, models: dict[str, Model]
    ) -> dict[str, Any] | None:
        """Export relationship to OSI relationship definition.

        Args:
            from_model: Name of the model containing the relationship
            rel: Relationship to export
            models: Resolved models dict, used to look up the related model's actual PK

        Returns:
            OSI relationship definition dictionary or None
        """
        if rel.type != "many_to_one":
            return None  # OSI only supports many-to-one style relationships

        # Use the related model's actual primary key when rel.primary_key is unset
        if rel.primary_key is None:
            related_model = models.get(rel.name)
            to_columns = related_model.primary_key_columns if related_model else []
        else:
            to_columns = rel.primary_key_columns

        result: dict[str, Any] = {
            "name": (rel.metadata or {}).get("osi_name") or f"{from_model}_to_{rel.name}",
            "from": from_model,
            "to": rel.name,
            "from_columns": rel.foreign_key_columns,
            "to_columns": to_columns,
        }
        if rel.metadata:
            if "ai_context" in rel.metadata:
                result["ai_context"] = rel.metadata["ai_context"]
            if "custom_extensions" in rel.metadata:
                custom_extensions = self._normalize_custom_extensions_for_export(rel.metadata["custom_extensions"])
                if custom_extensions:
                    result["custom_extensions"] = custom_extensions
        return result

    def _export_metric(
        self, metric: Metric, models: dict[str, Model], model_name: str | None = None
    ) -> dict[str, Any] | None:
        """Export metric to OSI metric definition.

        OSI metrics use full aggregate expressions like "SUM(dataset.field)".

        Args:
            metric: Metric to export
            models: Resolved models for context
            model_name: Model name for model-level metrics (for qualifying field refs)

        Returns:
            OSI metric definition dictionary or None
        """
        result: dict[str, Any] = {"name": metric.name}

        # Build the full expression
        expression = self._build_metric_expression(metric, model_name)
        if not expression:
            return None

        result["expression"] = {"dialects": self._generate_dialect_expressions(expression)}

        if metric.description:
            result["description"] = metric.description

        # Export meta as ai_context and custom_extensions
        if metric.meta:
            if "ai_context" in metric.meta:
                result["ai_context"] = metric.meta["ai_context"]
            if "custom_extensions" in metric.meta:
                custom_extensions = self._normalize_custom_extensions_for_export(metric.meta["custom_extensions"])
                if custom_extensions:
                    result["custom_extensions"] = custom_extensions

        return result

    def _export_version(self, graph: SemanticGraph) -> str:
        """Return the OSI version to emit."""
        osi_meta = (graph.metadata or {}).get("osi", {})
        version = osi_meta.get("version")
        if version == self.OSI_VERSION:
            return version
        return self.OSI_VERSION

    def _normalize_custom_extensions_for_export(self, custom_extensions: Any) -> list[dict[str, str]] | None:
        """Normalize permissive local extension metadata to the OSI schema shape."""
        if custom_extensions is None:
            return None

        if isinstance(custom_extensions, list):
            normalized = []
            for item in custom_extensions:
                if not isinstance(item, dict):
                    normalized.append({"vendor_name": "SIDEMANTIC", "data": self._extension_data_to_string(item)})
                    continue
                vendor_name = item.get("vendor_name") or item.get("vendor") or "SIDEMANTIC"
                data = item.get("data")
                if data is None:
                    data = {key: value for key, value in item.items() if key not in {"vendor_name", "vendor"}}
                normalized.append({"vendor_name": str(vendor_name), "data": self._extension_data_to_string(data)})
            return self._coerce_extension_vendors_for_export(normalized)

        if isinstance(custom_extensions, dict) and {"vendor_name", "data"} <= set(custom_extensions):
            return self._coerce_extension_vendors_for_export(
                [
                    {
                        "vendor_name": str(custom_extensions["vendor_name"]),
                        "data": self._extension_data_to_string(custom_extensions["data"]),
                    }
                ]
            )

        return self._coerce_extension_vendors_for_export(
            [{"vendor_name": "SIDEMANTIC", "data": self._extension_data_to_string(custom_extensions)}]
        )

    def _coerce_extension_vendors_for_export(self, extensions: list[dict[str, str]]) -> list[dict[str, str]]:
        """Map non-enum vendors to a released vendor when emitting released JSON.

        The released 0.1.x JSON Schema constrains ``vendor_name`` to
        :attr:`RELEASED_OSI_VENDORS`, so dbt's OSI consumer rejects any other
        vendor (notably the local ``SIDEMANTIC`` wrapper). For released JSON we
        relabel unsupported vendors to the cross-vendor ``COMMON`` bucket while
        preserving the original vendor inside ``data`` so a round-trip can
        restore it. The in-development YAML profile is left untouched.
        """
        if not getattr(self, "_export_released_json", False):
            return extensions

        coerced = []
        for ext in extensions:
            vendor = ext.get("vendor_name")
            if vendor in self.RELEASED_OSI_VENDORS:
                coerced.append(ext)
                continue
            payload = {"original_vendor_name": vendor, "data": ext.get("data")}
            coerced.append(
                {
                    "vendor_name": self.RELEASED_OSI_FALLBACK_VENDOR,
                    "data": self._extension_data_to_string(payload),
                }
            )
        return coerced

    def _decode_custom_extensions(self, custom_extensions: Any) -> Any:
        """Decode Sidemantic-owned extension wrappers while preserving standard OSI lists."""
        # Released JSON export relabels non-enum vendors to COMMON and stashes the
        # original under ``original_vendor_name``. Undo that first so the original
        # vendor/payload is restored before the SIDEMANTIC unwrap below.
        custom_extensions = self._restore_coerced_extension_vendors(custom_extensions)

        if (
            isinstance(custom_extensions, list)
            and len(custom_extensions) == 1
            and isinstance(custom_extensions[0], dict)
            and custom_extensions[0].get("vendor_name") == "SIDEMANTIC"
        ):
            data = custom_extensions[0].get("data")
            if isinstance(data, str):
                try:
                    return json.loads(data)
                except json.JSONDecodeError:
                    return data
            return data
        return custom_extensions

    def _restore_coerced_extension_vendors(self, custom_extensions: Any) -> Any:
        """Reverse :meth:`_coerce_extension_vendors_for_export` on import.

        A coerced extension is a ``COMMON`` entry whose ``data`` decodes to a dict
        carrying ``original_vendor_name``. Restore the original ``vendor_name`` and
        inner ``data`` so the released JSON path round-trips identically to YAML.
        """
        if not isinstance(custom_extensions, list):
            return custom_extensions

        restored = []
        changed = False
        for ext in custom_extensions:
            if (
                isinstance(ext, dict)
                and ext.get("vendor_name") == self.RELEASED_OSI_FALLBACK_VENDOR
                and isinstance(ext.get("data"), str)
            ):
                try:
                    payload = json.loads(ext["data"])
                except json.JSONDecodeError:
                    payload = None
                if isinstance(payload, dict) and "original_vendor_name" in payload:
                    restored.append(
                        {
                            "vendor_name": payload.get("original_vendor_name"),
                            "data": payload.get("data"),
                        }
                    )
                    changed = True
                    continue
            restored.append(ext)
        return restored if changed else custom_extensions

    def _extension_data_to_string(self, data: Any) -> str:
        """Convert custom extension data to the string required by OSI."""
        if isinstance(data, str):
            return data
        return json.dumps(data, sort_keys=True)

    def _build_metric_expression(self, metric: Metric, model_name: str | None) -> str | None:
        """Build full OSI metric expression from Sidemantic metric.

        Args:
            metric: Metric to convert
            model_name: Model name for qualifying field references

        Returns:
            Full SQL expression or None
        """
        if metric.type == "ratio":
            # Ratio: numerator / denominator
            num = metric.numerator or ""
            denom = metric.denominator or ""
            return f"{num} / NULLIF({denom}, 0)"

        if metric.type == "derived":
            # Derived: use sql expression as-is
            return metric.sql

        # Simple aggregation
        if metric.agg:
            inner = metric.sql or "*"
            agg_upper = metric.agg.upper()

            # Qualify with model name if provided
            if model_name and inner != "*" and "." not in inner:
                inner = f"{model_name}.{inner}"

            if metric.agg == "count_distinct":
                return f"COUNT(DISTINCT {inner})"
            return f"{agg_upper}({inner})"

        # No aggregation, just SQL
        return metric.sql
