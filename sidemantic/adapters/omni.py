"""Omni adapter for importing/exporting Omni Analytics semantic models."""

import re
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml
from sqlglot import exp

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.fidelity import record_export_feature, record_import_note
from sidemantic.paths import output_child
from sidemantic.sql.fragment import parse_sql_fragment
from sidemantic.yaml_compat import safe_load as _yaml_safe_load


class OmniAdapter(BaseAdapter):
    """Adapter for importing/exporting Omni Analytics view definitions.

    Transforms Omni definitions into Sidemantic format:
    - Views → Models
    - Dimensions → Dimensions
    - Measures → Metrics
    - Relationships (from model file) → Relationships
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse Omni view files into semantic graph.

        Args:
            source: Path to view .yaml file, views directory, or model directory

        Returns:
            Semantic graph with imported models

        The Omni export layout this handles:

        - ``views/*.view.yaml`` (current Omni) or ``views/*.yaml`` (older exports)
          define views (models). When ``views/`` is absent, view files are
          discovered recursively.
        - ``topics/*.topic.yaml`` define topics (a base view + nested joins). Topics
          are recorded on ``graph.topics`` and their joins are realized as
          relationships.
        - ``relationships.yaml``/``relationships.yml`` is a bare top-level *list* of
          joins (current Omni). A nested ``relationships:`` key inside
          ``model.yaml`` is also still supported (older exports).
        """
        graph = SemanticGraph()
        # Topics are an Omni concept with no native graph slot; expose as attribute.
        graph.topics = []
        source_path = Path(source)
        project = graph.metadata.setdefault("omni", {"view_paths": {}, "topics": {}})

        # Collect view, topic and relationship files.
        view_files: list[Path] = []
        topic_files: list[Path] = []
        relationships_files: list[Path] = []
        model_files: list[Path] = []

        if source_path.is_dir():
            candidate_views = sorted(
                path
                for path in source_path.rglob("*")
                if path.is_file()
                and path.suffix in {".yaml", ".yml", ".view"}
                and self._belongs_to_project(path, source_path)
            )
            topic_files = self._glob_topics(source_path)

            # Relationships and model files live at the project root.
            for candidate in ("relationships.yaml", "relationships.yml"):
                candidate_path = source_path / candidate
                if candidate_path.exists():
                    relationships_files.append(candidate_path)
            for candidate in ("model.yaml", "model.yml"):
                candidate_path = source_path / candidate
                if candidate_path.exists():
                    model_files.append(candidate_path)

            topic_set = {p.resolve() for p in topic_files}
            rel_set = {p.resolve() for p in relationships_files}
            model_set = {p.resolve() for p in model_files}
            for candidate in candidate_views:
                resolved = candidate.resolve()
                if resolved in topic_set or resolved in rel_set or resolved in model_set:
                    continue
                if self._is_model_or_relationships_file(candidate):
                    continue
                view_files.append(candidate)
        else:
            # Single file - dispatch by suffix.
            if self._is_topic_file(source_path):
                topic_files = [source_path]
            elif source_path.name in ("relationships.yaml", "relationships.yml"):
                relationships_files = [source_path]
            elif source_path.name in ("model.yaml", "model.yml"):
                model_files = [source_path]
            else:
                view_files = [source_path]

        # Parse all views first so relationships/topics can attach to them.
        for view_file in view_files:
            model = self._parse_view(view_file)
            if model:
                if model.name in graph.models:
                    raise ValueError(f"Duplicate Omni view name {model.name!r}")
                graph.add_model(model)
                project["view_paths"][model.name] = (
                    str(view_file.relative_to(source_path)) if source_path.is_dir() else view_file.name
                )

        # Parse a global relationships file (bare list of joins).
        for relationships_file in relationships_files:
            self._parse_relationships_list(self._load_relationships_list(relationships_file), graph)

        # Parse relationships nested inside a model file (older Omni layout).
        for model_file in model_files:
            with model_file.open() as handle:
                project["model"] = _yaml_safe_load(handle) or {}
            self._parse_relationships(model_file, graph)

        # Parse topics (base view + nested joins).
        for topic_file in topic_files:
            topic_path = str(topic_file.relative_to(source_path)) if source_path.is_dir() else topic_file.name
            with topic_file.open() as handle:
                project["topics"][topic_path] = _yaml_safe_load(handle)
            self._parse_topic(topic_file, graph)
            record_import_note(
                "topic_configuration",
                "Omni topic configuration is retained for native export; query curation is not enforced.",
                severity="approximated",
                source="omni",
                location=topic_path,
            )

        return graph

    @staticmethod
    def _belongs_to_project(path: Path, root: Path) -> bool:
        """A nested directory with its own model config is a separate project."""
        return not any(
            (parent / "model.yaml").exists() or (parent / "model.yml").exists()
            for parent in path.parents
            if parent != root and root in parent.parents
        )

    @staticmethod
    def _is_topic_file(path: Path) -> bool:
        """Whether a path is an Omni topic file (``*.topic.yaml``/``*.topic.yml``)."""
        name = path.name.lower()
        return name.endswith((".topic.yaml", ".topic.yml", ".topic"))

    @classmethod
    def _glob_topics(cls, directory: Path) -> list[Path]:
        """Find all topic files under a directory."""
        return sorted(
            path
            for path in directory.rglob("*")
            if path.is_file() and cls._is_topic_file(path) and cls._belongs_to_project(path, directory)
        )

    @classmethod
    def _is_model_or_relationships_file(cls, path: Path) -> bool:
        """Whether a candidate view file is actually a model/relationships file."""
        name = path.name.lower()
        if name in ("model.yaml", "model.yml", "relationships.yaml", "relationships.yml"):
            return True
        # Topic files are handled separately; never treat them as views.
        return cls._is_topic_file(path)

    @staticmethod
    def _load_relationships_list(relationships_file: Path) -> list[dict[str, Any]]:
        """Load a bare top-level list of joins from a relationships file."""
        with open(relationships_file) as f:
            data = _yaml_safe_load(f)

        if data is None:
            return []
        # Current Omni: bare list. Be tolerant of a wrapping ``relationships:`` key.
        if isinstance(data, dict):
            data = data.get("relationships") or []
        if not isinstance(data, list):
            return []
        return [rel for rel in data if isinstance(rel, dict)]

    def _parse_view(self, file_path: Path) -> Model | None:
        """Parse Omni view YAML into Sidemantic model.

        Args:
            file_path: Path to view YAML file

        Returns:
            Model instance or None
        """
        text = file_path.read_text()
        view = _yaml_safe_load(text)

        if not view or not isinstance(view, dict):
            return None

        if "query" in view or ("extends" in view and not any(view.get(key) for key in ("sql", "table", "table_name"))):
            detail = "Omni query and extends-only views require vendor query semantics; materialize the view or provide explicit SQL"
            record_import_note("query_view", detail, severity="unsupported", source="omni", location=str(file_path))
            raise ValueError(detail)

        # Get table reference
        schema = view.get("schema")
        table_name = view.get("table_name") or view.get("table")

        if schema and table_name:
            table = f"{schema}.{table_name}"
        elif table_name:
            table = table_name
        else:
            table = None

        # Derive the view name. Omni references a view as ``{schema}__{table_name}``
        # when it is scoped to a schema (see the "Reference this view as ..." header
        # Omni emits), otherwise by its file stem. An explicit ``name:`` always wins.
        # For schema-less views the file stem is the identifier that relationships and
        # topics reference, so it must be used even when ``table_name`` differs from it
        # (e.g. ``views/orders.yaml`` with ``table_name: fact_orders`` is the ``orders``
        # view, not ``fact_orders``). Using ``table_name`` there would silently drop any
        # ``join_from_view: orders`` relationship.
        header = re.search(r"^\s*#\s*Reference this view as\s+[`\"']?([\w]+)", text, re.MULTILINE)
        name = view.get("name") or (header.group(1) if header else None)
        if not name:
            if schema and table_name:
                name = f"{schema}__{table_name}"
            else:
                # Strip the ``.view`` suffix Omni adds to view filenames.
                stem = file_path.name
                for suffix in (".view.yaml", ".view.yml", ".yaml", ".yml", ".view"):
                    if stem.lower().endswith(suffix):
                        stem = stem[: -len(suffix)]
                        break
                name = stem

        if table is None and schema:
            implicit_table = re.sub(r"\.view(?:\.ya?ml)?$|\.ya?ml$", "", file_path.name)
            table = f"{schema}.{implicit_table}"

        # Get SQL for query-based views
        sql = view.get("sql")

        # Parse dimensions
        dimensions = []
        primary_key = "id"  # default

        for dim_name, dim_def in (view.get("dimensions") or {}).items():
            if dim_def is None:
                dim_def = {}

            dim = self._parse_dimension(dim_name, dim_def)
            if dim:
                dimensions.append(dim)

                # Check for primary key
                if dim_def.get("primary_key") is True:
                    primary_key = dim.name

        # Parse measures
        metrics = []
        for measure_name, measure_def in (view.get("measures") or {}).items():
            if measure_def is None:
                measure_def = {}

            metric = self._parse_measure(measure_name, measure_def)
            if metric:
                metrics.append(metric)

        # Use description if available, otherwise use label
        description = view.get("description")
        if not description:
            description = view.get("label")

        return Model(
            name=name,
            table=table,
            sql=sql,
            description=description,
            primary_key=primary_key,
            dimensions=dimensions,
            metrics=metrics,
            metadata={"omni_view": deepcopy(view)},
        )

    def _parse_dimension(self, name: str, dim_def: dict[str, Any]) -> Dimension | None:
        """Parse Omni dimension definition.

        Args:
            name: Dimension name
            dim_def: Dimension definition dict

        Returns:
            Dimension instance or None
        """
        # Map Omni types to Sidemantic types
        dim_type_str = dim_def.get("type", "string")
        type_mapping = {
            "string": "categorical",
            "number": "numeric",
            "date": "time",
            "timestamp": "time",
            "yesno": "boolean",
        }

        dim_type = type_mapping.get(dim_type_str, "categorical")

        # Get SQL - replace ${TABLE} or ${view.field} references with {model}
        sql = dim_def.get("sql", name)
        if sql:
            sql = sql.replace("${TABLE}", "{model}")
            # Simplify ${view.field} to just field for now
            import re

            sql = re.sub(r"\$\{[^.]+\.([^}]+)\}", r"\1", sql)

        # Handle timeframes for time dimensions. Omni allows multiple timeframes per
        # time dimension; map the first to the base granularity and keep the full
        # list as supported_granularities.
        timeframe_mapping = {
            "date": "day",
            "day": "day",
            "week": "week",
            "month": "month",
            "quarter": "quarter",
            "year": "year",
            "hour": "hour",
            "minute": "minute",
            "second": "second",
        }
        timeframes = dim_def.get("timeframes")
        granularity = None
        supported_granularities = None
        if dim_type == "time" and timeframes:
            if not isinstance(timeframes, list):
                timeframes = [timeframes]
            mapped = [timeframe_mapping[tf] for tf in timeframes if tf in timeframe_mapping]
            if mapped:
                granularity = mapped[0]
                # De-duplicate while preserving order.
                supported_granularities = list(dict.fromkeys(mapped))

        # Preserve Omni-specific dimension metadata that has no first-class field.
        metadata: dict[str, Any] = {}
        for key in ("synonyms", "all_values", "sample_values", "suggestion_list", "bin_boundaries"):
            if key in dim_def and dim_def[key] is not None:
                metadata[key] = dim_def[key]
        if dim_def.get("order_by_field") is not None:
            metadata["order_by_field"] = dim_def["order_by_field"]
        if timeframes:
            metadata["timeframes"] = timeframes
            metadata["omni_timeframes"] = {
                "source": deepcopy(dim_def["timeframes"]),
                "granularity": granularity,
                "supported_granularities": deepcopy(supported_granularities),
            }

        return Dimension(
            name=name,
            type=dim_type,
            sql=sql,
            label=dim_def.get("label"),
            granularity=granularity,
            supported_granularities=supported_granularities,
            description=dim_def.get("description"),
            format=dim_def.get("format"),
            metadata=metadata or None,
        )

    def _parse_measure(self, name: str, measure_def: dict[str, Any]) -> Metric | None:
        """Parse Omni measure definition.

        Args:
            name: Measure name
            measure_def: Measure definition dict

        Returns:
            Metric instance or None
        """
        # Check for time comparison pattern first (date_offset_from_query + cancel_query_filter)
        filter_defs = measure_def.get("filters", {})
        if filter_defs:
            for field, conditions in filter_defs.items():
                if isinstance(conditions, dict):
                    has_offset = "date_offset_from_query" in conditions
                    has_cancel = conditions.get("cancel_query_filter") is True

                    if has_offset and has_cancel:
                        # This is a time comparison measure
                        offset_str = conditions["date_offset_from_query"]

                        # Parse offset to determine comparison type
                        # e.g., "2 years" -> yoy, "1 month" -> mom, "1 week" -> wow
                        comparison_type = self._parse_time_offset_to_comparison(offset_str)

                        # Extract base measure name - typically the measure name minus suffix
                        # e.g., "count_signups_same_time_two_years_previously" -> "count_signups"
                        base_metric = self._extract_base_metric_name(name)

                        return Metric(
                            name=name,
                            type="time_comparison",
                            base_metric=base_metric,
                            comparison_type=comparison_type,
                            time_offset=offset_str,
                            calculation="difference",  # Omni defaults to difference
                            label=measure_def.get("label"),
                            description=measure_def.get("description"),
                            metadata={
                                "omni_time_comparison": {
                                    "definition": deepcopy(measure_def),
                                    "time_offset": offset_str,
                                    "comparison_type": comparison_type,
                                    "base_metric": base_metric,
                                }
                            },
                        )

        # Map Omni aggregate types to Sidemantic aggregations. Omni has several
        # aggregate types that Sidemantic does not model natively; map them to the
        # closest supported aggregation and preserve the original in metadata.
        agg_type_str = measure_def.get("aggregate_type", "")
        type_mapping = {
            "count": "count",
            "count_distinct": "count_distinct",
            "sum": "sum",
            "average": "avg",
            "avg": "avg",
            "min": "min",
            "max": "max",
            "median": "median",
            # Omni percentile/list and the *_distinct_on variants have no direct
            # Sidemantic aggregation. The distinct-on measures deduplicate rows by a
            # custom primary key before aggregating; collapsing them to a plain
            # sum/avg/median would silently drop the dedup and overcount whenever the
            # source rows fan out. Leave agg unset so they parse as derived/custom-SQL
            # measures (with the original aggregate_type and custom_primary_key_sql in
            # metadata) rather than mislabeling the aggregation.
        }

        agg = type_mapping.get(agg_type_str)

        # Get SQL - replace ${view.field} references
        sql = measure_def.get("sql")
        if sql:
            import re

            # Replace ${view.field} with just field
            sql = re.sub(r"\$\{[^.]+\.([^}]+)\}", r"\1", sql)

        # Parse regular filters
        filters = []
        if filter_defs:
            for field, conditions in filter_defs.items():
                if isinstance(conditions, dict):
                    # Skip time comparison filters
                    if "date_offset_from_query" in conditions and conditions.get("cancel_query_filter") is True:
                        continue

                    for operator, value in conditions.items():
                        rendered = self._render_filter(field, operator, value)
                        if rendered:
                            filters.append(rendered)

        # Preserve Omni-specific measure metadata that has no first-class field.
        metadata: dict[str, Any] = {}
        if agg_type_str:
            metadata["aggregate_type"] = agg_type_str
        if filter_defs:
            metadata["omni_filters"] = {"source": deepcopy(filter_defs), "lowered": filters or None}
        if measure_def.get("synonyms") is not None:
            metadata["synonyms"] = measure_def["synonyms"]
        if measure_def.get("percentile") is not None:
            metadata["percentile"] = measure_def["percentile"]
        if measure_def.get("custom_primary_key_sql") is not None:
            metadata["custom_primary_key_sql"] = measure_def["custom_primary_key_sql"]

        # Determine metric type
        metric_type = None
        if not agg and sql:
            # Custom SQL without standard aggregation
            metric_type = "derived"

        return Metric(
            name=name,
            type=metric_type,
            agg=agg,
            sql=sql,
            filters=filters if filters else None,
            label=measure_def.get("label"),
            description=measure_def.get("description"),
            format=measure_def.get("format"),
            metadata=metadata or None,
        )

    @staticmethod
    def _render_filter(field: str, operator: str, value: Any) -> str | None:
        """Render an Omni filter condition into a SQL WHERE fragment.

        Supports the documented Omni filter operators. Unknown operators are
        skipped (returns ``None``).
        """

        def quote(val: Any) -> str:
            # Numbers and booleans are emitted bare; everything else is quoted.
            if isinstance(val, bool):
                return "TRUE" if val else "FALSE"
            if isinstance(val, (int, float)):
                return str(val)
            return f"'{val}'"

        if operator == "is":
            return f"{field} = {quote(value)}"
        if operator in ("is_not", "not"):
            return f"{field} != {quote(value)}"
        if operator == "greater_than":
            return f"{field} > {quote(value)}"
        if operator == "greater_than_or_equal_to":
            return f"{field} >= {quote(value)}"
        if operator == "less_than":
            return f"{field} < {quote(value)}"
        if operator == "less_than_or_equal_to":
            return f"{field} <= {quote(value)}"
        if operator == "contains":
            return f"{field} LIKE '%{value}%'"
        if operator == "starts_with":
            return f"{field} LIKE '{value}%'"
        if operator == "ends_with":
            return f"{field} LIKE '%{value}'"
        if operator == "between" and isinstance(value, (list, tuple)) and len(value) == 2:
            return f"{field} BETWEEN {quote(value[0])} AND {quote(value[1])}"
        return None

    def _parse_time_offset_to_comparison(self, offset: str) -> str:
        """Parse Omni time offset string to comparison_type.

        Args:
            offset: Offset string like "2 years", "1 month", "1 week"

        Returns:
            Comparison type: yoy, mom, wow, dod, qoq, or prior_period
        """
        offset_lower = offset.lower().strip()

        # Check for common patterns
        if "year" in offset_lower:
            return "yoy"
        elif "month" in offset_lower:
            return "mom"
        elif "week" in offset_lower:
            return "wow"
        elif "day" in offset_lower:
            return "dod"
        elif "quarter" in offset_lower:
            return "qoq"
        else:
            # Default to prior_period for custom offsets
            return "prior_period"

    def _extract_base_metric_name(self, comparison_name: str) -> str:
        """Extract base metric name from time comparison metric name.

        Args:
            comparison_name: Name like "revenue_yoy" or "count_signups_same_time_two_years_previously"

        Returns:
            Base metric name (best guess)
        """
        # Common suffixes to remove
        suffixes = [
            "_yoy",
            "_mom",
            "_wow",
            "_dod",
            "_qoq",
            "_same_time_two_years_previously",
            "_same_time_one_year_previously",
            "_same_time_last_month",
            "_same_time_last_week",
            "_previous_period",
            "_prior_period",
        ]

        name = comparison_name
        for suffix in suffixes:
            if name.endswith(suffix):
                return name[: -len(suffix)]

        # If no known suffix, return as-is and let user fix it
        return comparison_name

    def _parse_relationships(self, model_file: Path, graph: SemanticGraph) -> None:
        """Parse relationships nested inside an Omni model file (older layout).

        Args:
            model_file: Path to model.yaml file
            graph: Semantic graph to add relationships to
        """
        with open(model_file) as f:
            model_def = _yaml_safe_load(f)

        if not model_def or not isinstance(model_def, dict):
            return

        relationships_list = model_def.get("relationships") or []
        self._parse_relationships_list(relationships_list, graph)

    def _parse_relationships_list(self, relationships_list: list[dict[str, Any]], graph: SemanticGraph) -> None:
        """Parse a list of Omni join definitions into relationships.

        Args:
            relationships_list: List of join dicts (``join_from_view``,
                ``join_to_view``, ``relationship_type``, ``on_sql``, ...).
            graph: Semantic graph to add relationships to
        """
        # Omni cardinalities mapped to Sidemantic relationship types.
        # ``assumed_many_to_one`` is Omni's auto-inferred variant of many_to_one.
        type_mapping = {
            "one_to_one": "one_to_one",
            "many_to_one": "many_to_one",
            "assumed_many_to_one": "many_to_one",
            "one_to_many": "one_to_many",
            "many_to_many": "many_to_many",
        }

        for rel_def in relationships_list:
            if not isinstance(rel_def, dict):
                continue

            from_view = rel_def.get("join_from_view")
            to_view = rel_def.get("join_to_view")

            if not from_view or not to_view:
                continue

            rel_type_str = rel_def.get("relationship_type", "many_to_one")
            rel_type = type_mapping.get(rel_type_str, "many_to_one")

            # Extract foreign/primary keys from on_sql: ${from.col} = ${to.col}
            on_sql = rel_def.get("on_sql", "")
            foreign_key, primary_key = self._keys_from_on_sql(on_sql, from_view, to_view, rel_type)
            join_sql = self._join_sql(on_sql, from_view, to_view) if on_sql else None

            # Preserve Omni join metadata with no first-class field.
            metadata: dict[str, Any] = {}
            if rel_def.get("join_type") is not None:
                metadata["omni_join_type"] = rel_def["join_type"]
                join_types = {
                    "always_left": "left",
                    "left": "left",
                    "inner": "inner",
                    "full_outer": "full",
                    "right": "right",
                }
                if rel_def["join_type"] not in join_types:
                    raise ValueError(f"Unsupported Omni join_type {rel_def['join_type']!r}")
                metadata["bsl_how"] = join_types[rel_def["join_type"]]
            if rel_def.get("reversible") is not None:
                if not isinstance(rel_def["reversible"], bool):
                    raise ValueError("Omni relationship reversible must be a boolean")
                metadata["reversible"] = rel_def["reversible"]
            if rel_type_str == "assumed_many_to_one":
                metadata["assumed"] = True

            if from_view in graph.models:
                relationship = Relationship(
                    name=to_view,
                    type=rel_type,
                    foreign_key=foreign_key,
                    primary_key=primary_key,
                    sql=join_sql if foreign_key is None or primary_key is None else None,
                    metadata=metadata or None,
                )
                graph.models[from_view].relationships.append(relationship)

    @staticmethod
    def _keys_from_on_sql(
        on_sql: str, from_view: str, to_view: str, rel_type: str = "many_to_one"
    ) -> tuple[str | None, str | None]:
        """Extract ``(foreign_key, primary_key)`` from an Omni ``on_sql`` join.

        The relationship is always attached to ``from_view`` with ``name=to_view``,
        but Sidemantic's interpretation of ``foreign_key``/``primary_key`` depends on
        the cardinality (see ``Relationship``):

        - ``many_to_one`` (from_view holds the FK): ``foreign_key`` is the from_view
          column and ``primary_key`` is the to_view column.
        - ``one_to_many`` / ``one_to_one`` (to_view holds the FK): ``foreign_key`` is
          the to_view column (the FK in the related model) and ``primary_key`` is the
          from_view column (the local key). Assigning these the other way around
          reverses the join and produces invalid SQL.
        """
        from_column = None
        to_column = None
        match = re.fullmatch(r"\s*\$\{([^}.]+)\.([^}]+)\}\s*=\s*\$\{([^}.]+)\.([^}]+)\}\s*", on_sql)
        if match:
            columns = {match[1]: match[2], match[3]: match[4]}
            from_column, to_column = columns.get(from_view), columns.get(to_view)

        if rel_type in ("one_to_many", "one_to_one"):
            # to_view (related model) holds the foreign key; from_view contributes its key.
            return to_column, from_column
        # many_to_one (and default): from_view holds the foreign key.
        return from_column, to_column

    @staticmethod
    def _join_sql(on_sql: str, from_view: str, to_view: str) -> str:
        """Translate the full predicate, including non-equi and compound joins."""
        if "{{" in on_sql:
            raise ValueError("Templated Omni join predicates cannot be imported as executable SQL")
        normalized = re.sub(
            r"('(?:''|[^'])*')|\$\{([^}]+)\}",
            lambda match: match[0] if match[1] else match[2],
            on_sql,
        )
        predicate = parse_sql_fragment(normalized)
        if predicate is None:
            raise ValueError(f"Invalid Omni on_sql: {on_sql}")

        def rewrite(node):
            if isinstance(node, exp.Column):
                if node.table not in (from_view, to_view):
                    raise ValueError(f"Unsupported Omni join reference {node.sql()!r}")
                side = "{from}" if node.table == from_view else "{to}"
                return exp.Var(this=f"{side}.{node.this.sql()}")
            return node

        return predicate.transform(rewrite).sql()

    @staticmethod
    def _project_path(directory: Path, relative: str) -> Path:
        """Restore nested source paths without accepting traversal or symlink escapes."""
        parts = Path(relative).parts
        if not parts or Path(relative).is_absolute():
            raise ValueError(f"Unsafe Omni output path: {relative!r}")
        path = directory
        for part in parts:
            path = output_child(path, part)
        return path

    def _parse_topic(self, topic_file: Path, graph: SemanticGraph) -> None:
        """Parse an Omni topic file (base view + nested joins).

        Topics are recorded on ``graph.topics`` and their joins are realized as
        ``many_to_one`` relationships from each parent view to its joined views
        (mirroring how Omni traverses joins from a base view).

        Args:
            topic_file: Path to ``*.topic.yaml`` file
            graph: Semantic graph to add topic + relationships to
        """
        with open(topic_file) as f:
            topic_def = _yaml_safe_load(f)

        if not topic_def or not isinstance(topic_def, dict):
            return

        base_view = topic_def.get("base_view")
        if not base_view:
            return

        # Flatten the nested joins map into (parent_view, joined_view) edges.
        joins = topic_def.get("joins") or {}
        joined_views: list[str] = []
        edges: list[tuple[str, str]] = []

        def walk(parent: str, joins_map: Any) -> None:
            if not isinstance(joins_map, dict):
                return
            for joined_view, nested in joins_map.items():
                joined_views.append(joined_view)
                edges.append((parent, joined_view))
                walk(joined_view, nested)

        walk(base_view, joins)

        topic_record = {
            "name": re.sub(r"\.topic(?:\.ya?ml)?$", "", topic_file.name, flags=re.IGNORECASE).lower(),
            "label": topic_def.get("label"),
            "description": topic_def.get("description"),
            "base_view": base_view,
            "joined_views": joined_views,
        }
        graph.topics.append(topic_record)

        # Realize the join graph as relationships (skip duplicates already present).
        for parent, joined_view in edges:
            parent_model = graph.models.get(parent)
            if parent_model is None:
                continue
            if any(rel.related_model == joined_view for rel in parent_model.relationships):
                continue
            parent_model.relationships.append(
                Relationship(
                    name=joined_view,
                    type="many_to_one",
                    metadata={"source": "topic", "topic": topic_record["name"]},
                )
            )

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to Omni view format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output directory
        """
        output_path = Path(output_path)

        # Resolve inheritance first
        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        local_metrics = [metric for model in resolved_models.values() for metric in model.metrics]
        graph_only = [name for name, metric in graph.metrics.items() if metric not in local_metrics]
        if graph_only:
            detail = "Graph-level metrics have no omni model placement: " + ", ".join(graph_only)
            record_export_feature("graph_metrics", "rejected", source="omni", detail=detail)
            raise ValueError(detail)

        for model in resolved_models.values():
            for rel in model.relationships:
                if rel.name != rel.related_model:
                    raise ValueError(
                        "Omni export cannot represent relationship role "
                        f"'{rel.name}' independently of target view '{rel.related_model}'"
                    )

        project = graph.metadata.get("omni", {})
        self._validate_topics(project.get("topics", {}), resolved_models)

        output_path.mkdir(parents=True, exist_ok=True)

        # Create views directory
        views_dir = output_child(output_path, "views")
        views_dir.mkdir(exist_ok=True)

        # Export each model as a view file
        for model in resolved_models.values():
            view = self._export_view(model)
            relative = project.get("view_paths", {}).get(model.name)
            file_path = (
                self._project_path(output_path, relative)
                if relative
                else output_child(views_dir, f"{model.name}.view.yaml")
            )
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "w") as f:
                yaml.dump(view, f, default_flow_style=False, sort_keys=False)

        # Keep model-wide configuration and topics independent of join definitions.
        model_config = deepcopy(project.get("model", {}))
        model_config.pop("relationships", None)
        if model_config:
            output_child(output_path, "model.yaml").write_text(yaml.safe_dump(model_config, sort_keys=False))
        for relative, topic in project.get("topics", {}).items():
            topic_path = self._project_path(output_path, relative)
            topic_path.parent.mkdir(parents=True, exist_ok=True)
            topic_path.write_text(yaml.safe_dump(topic, sort_keys=False))
        if any(model.relationships for model in resolved_models.values()):
            self._export_relationships(resolved_models, output_path)

    @staticmethod
    def _validate_topics(topics: dict, models: dict[str, Model]) -> None:
        """Do not replay topic references that conflict with edited graph topology."""

        def reject(path: str, detail: str) -> None:
            message = f"Omni topic {path!r} {detail}; update or remove retained topic metadata before exporting"
            record_export_feature("topic_configuration", "rejected", source="omni", detail=message)
            raise ValueError(message)

        def walk(path: str, parent: str, joins: Any) -> None:
            if not isinstance(joins, dict):
                return
            for target, nested in joins.items():
                if target not in models:
                    reject(path, f"references missing view {target!r}")
                if not any(rel.active and rel.related_model == target for rel in models[parent].relationships):
                    reject(path, f"references removed or inactive relationship {parent}.{target}")
                walk(path, target, nested)

        for path, topic in topics.items():
            if not isinstance(topic, dict) or not topic.get("base_view"):
                continue
            base = topic["base_view"]
            if base not in models:
                reject(path, f"references missing base view {base!r}")
            walk(path, base, topic.get("joins"))

    def _export_view(self, model: Model) -> dict[str, Any]:
        """Export model to Omni view definition.

        Args:
            model: Model to export

        Returns:
            View definition dictionary
        """
        view: dict[str, Any] = deepcopy((model.metadata or {}).get("omni_view", {}))
        view["name"] = model.name
        original_dimensions = view.pop("dimensions", {}) or {}
        original_measures = view.pop("measures", {}) or {}

        if model.description:
            if "description" in view:
                view["description"] = model.description
            elif "label" not in view:
                view["label"] = model.description

        for key in ("schema", "catalog", "table_name", "table", "sql"):
            view.pop(key, None)
        if not model.description:
            view.pop("description", None)
            view.pop("label", None)

        # Extract schema and table
        if model.table and "." in model.table:
            parts = model.table.split(".", 1)
            view["schema"] = parts[0]
            view["table_name"] = parts[1]
        elif model.table:
            view["table_name"] = model.table

        if model.sql:
            view["sql"] = model.sql

        # Don't add separate description field since we used it for label

        # Export dimensions
        dimensions: dict[str, Any] = {}
        for dim in model.dimensions:
            dim_def: dict[str, Any] = deepcopy(original_dimensions.get(dim.name) or {})
            for key in ("type", "sql", "label", "description", "format", "primary_key", "timeframes"):
                dim_def.pop(key, None)

            # Map Sidemantic types to Omni types
            type_mapping = {
                "categorical": "string",
                "numeric": "number",
                "time": "timestamp",
                "boolean": "yesno",
            }
            if dim.type in type_mapping:
                dim_def["type"] = type_mapping[dim.type]

            # Add SQL - convert {model} to ${TABLE}
            if dim.sql and dim.sql != dim.name:
                sql = dim.sql.replace("{model}", "${TABLE}")
                dim_def["sql"] = sql

            if dim.label:
                dim_def["label"] = dim.label

            if dim.description:
                dim_def["description"] = dim.description
            if dim.format:
                dim_def["format"] = dim.format
            for key in (
                "synonyms",
                "all_values",
                "sample_values",
                "suggestion_list",
                "bin_boundaries",
                "order_by_field",
            ):
                if key in (dim.metadata or {}):
                    dim_def[key] = deepcopy(dim.metadata[key])

            # Only replay source timeframes while the native time properties match.
            if dim.type == "time":
                granularity_mapping = {
                    "second": "second",
                    "minute": "minute",
                    "hour": "hour",
                    "day": "date",
                    "week": "week",
                    "month": "month",
                    "quarter": "quarter",
                    "year": "year",
                }
                original = (dim.metadata or {}).get("omni_timeframes")
                if original and all(
                    original[key] == getattr(dim, key) for key in ("granularity", "supported_granularities")
                ):
                    dim_def["timeframes"] = deepcopy(original["source"])
                else:
                    grains = list(dim.supported_granularities or [])
                    if dim.granularity:
                        if grains and dim.granularity not in grains:
                            raise ValueError(
                                f"Omni dimension {model.name}.{dim.name} granularity must be included in supported_granularities"
                            )
                        grains = [dim.granularity, *(grain for grain in grains if grain != dim.granularity)]
                    unsupported = set(grains) - granularity_mapping.keys()
                    if unsupported:
                        raise ValueError(f"Unsupported Omni time granularities: {sorted(unsupported)!r}")
                    if grains:
                        dim_def["timeframes"] = [granularity_mapping[grain] for grain in grains]

            # Mark primary key
            if dim.name == model.primary_key:
                dim_def["primary_key"] = True

            dimensions[dim.name] = dim_def

        if dimensions:
            view["dimensions"] = dimensions

        # Export measures
        measures: dict[str, Any] = {}
        for metric in model.metrics:
            measure_def: dict[str, Any] = deepcopy(original_measures.get(metric.name) or {})
            for key in ("aggregate_type", "sql", "label", "description", "format", "filters"):
                measure_def.pop(key, None)

            # Handle time_comparison metrics specially
            if metric.type == "time_comparison":
                original = (metric.metadata or {}).get("omni_time_comparison")
                if original and all(
                    original[key] == getattr(metric, key) for key in ("time_offset", "comparison_type", "base_metric")
                ):
                    measure_def.update(deepcopy(original["definition"]))
                    measure_def.pop("label", None)
                    measure_def.pop("description", None)
                else:
                    base = model.get_metric((metric.base_metric or "").split(".")[-1])
                    if base is None or base.agg is None:
                        raise ValueError(f"Omni time comparison {metric.name!r} requires a local aggregate base metric")
                    measure_def["aggregate_type"] = "average" if base.agg == "avg" else base.agg
                    if base.sql:
                        measure_def["sql"] = base.sql.replace("{model}", "${TABLE}")
                    time_offset = metric.time_offset or self._comparison_type_to_offset(metric.comparison_type)
                    time_field = model.default_time_dimension or "created_at"
                    measure_def["filters"] = {
                        time_field: {"date_offset_from_query": time_offset, "cancel_query_filter": True}
                    }

                if metric.label:
                    measure_def["label"] = metric.label

                if metric.description:
                    measure_def["description"] = metric.description

                measures[metric.name] = measure_def
                continue

            # Map aggregation type
            if metric.agg:
                type_mapping = {
                    "count": "count",
                    "count_distinct": "count_distinct",
                    "sum": "sum",
                    "avg": "average",
                    "min": "min",
                    "max": "max",
                    "median": "median",
                    "stddev": "stddev",
                    "stddev_pop": "stddev_pop",
                    "variance": "variance",
                    "variance_pop": "variance_pop",
                }
                if metric.agg not in type_mapping:
                    raise ValueError(f"Unsupported Omni aggregation {metric.agg!r}")
                measure_def["aggregate_type"] = type_mapping[metric.agg]

            elif (metric.metadata or {}).get("aggregate_type") not in (
                None,
                "",
                "count",
                "count_distinct",
                "sum",
                "average",
                "avg",
                "min",
                "max",
                "median",
            ):
                measure_def["aggregate_type"] = metric.metadata["aggregate_type"]

            if metric.sql:
                # Convert {model} references
                sql = metric.sql.replace("{model}", "${TABLE}")
                measure_def["sql"] = sql

            if metric.label:
                measure_def["label"] = metric.label

            if metric.description:
                measure_def["description"] = metric.description

            if metric.format:
                measure_def["format"] = metric.format
            for key in ("synonyms", "percentile", "custom_primary_key_sql"):
                if key in (metric.metadata or {}):
                    measure_def[key] = deepcopy(metric.metadata[key])

            # Preserve the original structured filters for an unedited imported measure.
            original_filters = (metric.metadata or {}).get("omni_filters")
            if original_filters and metric.filters == original_filters["lowered"]:
                measure_def["filters"] = deepcopy(original_filters["source"])
            # Export filters
            elif metric.filters:
                filters_dict = {}
                operators = {
                    exp.EQ: "is",
                    exp.NEQ: "is_not",
                    exp.GT: "greater_than",
                    exp.GTE: "greater_than_or_equal_to",
                    exp.LT: "less_than",
                    exp.LTE: "less_than_or_equal_to",
                }
                for filter_str in metric.filters:
                    normalized = re.sub(
                        r"('(?:''|[^'])*')|\{model\}\.",
                        lambda match: match[0] if match[1] else "",
                        filter_str,
                    )
                    predicate = parse_sql_fragment(normalized)
                    if type(predicate) not in operators or not isinstance(predicate.left, exp.Column):
                        raise ValueError(f"Unsupported Omni metric filter {filter_str!r}")
                    value = predicate.right
                    if isinstance(value, exp.Boolean):
                        value = value.this
                    elif isinstance(value, exp.Literal):
                        value = (
                            value.this
                            if value.is_string
                            else float(value.this)
                            if "." in value.this
                            else int(value.this)
                        )
                    elif (
                        isinstance(value, exp.Neg) and isinstance(value.this, exp.Literal) and not value.this.is_string
                    ):
                        value = -float(value.this.this)
                    else:
                        raise ValueError(f"Unsupported Omni metric filter value in {filter_str!r}")
                    field = predicate.left.sql()
                    operator = operators[type(predicate)]
                    if operator in filters_dict.get(field, {}):
                        raise ValueError(f"Repeated Omni metric filter operator for {field!r}")
                    filters_dict.setdefault(field, {})[operator] = value
                measure_def["filters"] = filters_dict

            measures[metric.name] = measure_def

        if measures:
            view["measures"] = measures

        return view

    def _comparison_type_to_offset(self, comparison_type: str | None) -> str:
        """Convert comparison_type to Omni time offset string.

        Args:
            comparison_type: Comparison type (yoy, mom, wow, etc.)

        Returns:
            Offset string like "1 year", "1 month", etc.
        """
        if not comparison_type:
            return "1 year"

        mapping = {
            "yoy": "1 year",
            "mom": "1 month",
            "wow": "1 week",
            "dod": "1 day",
            "qoq": "1 quarter",
        }
        return mapping.get(comparison_type, "1 year")

    def _export_relationships(self, models: dict[str, Model], output_dir: Path) -> None:
        """Export relationships to model.yaml file.

        Args:
            models: Dictionary of resolved models
            output_dir: Output directory path
        """
        relationships = []

        for model in models.values():
            for rel in model.relationships:
                metadata = rel.metadata or {}
                join_type = metadata.get("omni_join_type", "always_left")
                runtime_type = metadata.get("bsl_how") or metadata.get("join_type")
                if runtime_type is not None:
                    join_types = {
                        "left": "always_left",
                        "left_outer": "always_left",
                        "inner": "inner",
                        "full": "full_outer",
                        "outer": "full_outer",
                        "full_outer": "full_outer",
                        "right": "right",
                    }
                    if runtime_type not in join_types:
                        raise ValueError(f"Unsupported Omni join type {runtime_type!r}")
                    mapped_type = join_types[runtime_type]
                    if not (join_type == "left" and mapped_type == "always_left"):
                        join_type = mapped_type
                rel_def = {
                    "join_from_view": model.name,
                    "join_to_view": rel.name,
                    "join_type": join_type,
                }

                if (rel.metadata or {}).get("source") == "topic" and not rel.sql and not rel.foreign_key:
                    # Topic curation supplies no join predicate; do not invent one.
                    continue
                if "reversible" in (rel.metadata or {}):
                    rel_def["reversible"] = rel.metadata["reversible"]

                # Map relationship type
                type_mapping = {
                    "one_to_one": "one_to_one",
                    "many_to_one": "many_to_one",
                    "one_to_many": "one_to_many",
                    "many_to_many": "many_to_many",
                }
                rel_def["relationship_type"] = type_mapping.get(rel.type, "many_to_one")

                # Build on_sql. Which side holds the foreign vs. primary key
                # depends on cardinality (see Relationship): for many_to_one the
                # from_view holds the FK; for one_to_many / one_to_one the to_view
                # (related model) holds the FK and the from_view contributes its key.
                if rel.type in ("one_to_many", "one_to_one"):
                    from_key = rel.primary_key or "id"
                    to_key = rel.foreign_key or f"{model.name}_id"
                else:
                    from_key = rel.foreign_key or f"{rel.name}_id"
                    to_key = rel.primary_key or "id"
                if rel.sql:
                    normalized = re.sub(
                        r"('(?:''|[^'])*')|\{(from|to)\}",
                        lambda match: match[0] if match[1] else f"__omni_{match[2]}",
                        rel.sql,
                    )
                    predicate = parse_sql_fragment(normalized)
                    if predicate is None:
                        raise ValueError(f"Invalid Omni relationship SQL on {rel.name!r}")

                    def rewrite(node):
                        if isinstance(node, exp.Column):
                            names = {"__omni_from": model.name, "__omni_to": rel.related_model}
                            if node.table not in names:
                                raise ValueError(f"Unsupported Omni join reference {node.sql()!r}")
                            return exp.Var(this=f"${{{names[node.table]}.{node.name}}}")
                        return node

                    rel_def["on_sql"] = predicate.transform(rewrite).sql()
                else:
                    from_keys = from_key if isinstance(from_key, list) else [from_key]
                    to_keys = to_key if isinstance(to_key, list) else [to_key]
                    if len(from_keys) != len(to_keys):
                        raise ValueError(f"Mismatched Omni join key counts on {rel.name!r}")
                    rel_def["on_sql"] = " AND ".join(
                        f"${{{model.name}.{left}}} = ${{{rel.name}.{right}}}" for left, right in zip(from_keys, to_keys)
                    )

                relationships.append(rel_def)

        if relationships:
            model_file = output_child(output_dir, "relationships.yaml")
            with open(model_file, "w") as f:
                yaml.safe_dump(relationships, f, default_flow_style=False, sort_keys=False)
