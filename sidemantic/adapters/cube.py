"""Cube adapter for importing Cube.js semantic models."""

import re
from pathlib import Path

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.pre_aggregation import Index, PreAggregation, RefreshKey
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph


def _normalize_cube_sql(sql: str | None, cube_name: str | None = None) -> str | None:
    """Normalize Cube.js SQL syntax to Sidemantic format.

    Handles:
    - ${CUBE} -> {model} placeholder
    - ${cube_name} -> {model} placeholder
    - {CUBE} -> {model} placeholder (variant without dollar sign)

    Note: ${measure_ref} references are handled separately in _parse_measure()
    for derived metrics.

    Args:
        sql: SQL expression string or None
        cube_name: Name of the cube (used to replace ${cube_name} references)

    Returns:
        Normalized SQL string or None
    """
    if sql is None:
        return None

    # Replace ${CUBE} and {CUBE} variants with {model}
    result = sql.replace("${CUBE}", "{model}")
    result = result.replace("{CUBE}", "{model}")

    # Replace ${cube_name} with {model} if cube_name is provided
    if cube_name:
        result = result.replace(f"${{{cube_name}}}", "{model}")
        result = result.replace(f"{{{cube_name}}}", "{model}")

    return result


class CubeAdapter(BaseAdapter):
    """Adapter for importing/exporting Cube.js YAML semantic models.

    Transforms Cube.js definitions into Sidemantic format:
    - Cubes -> Models
    - Dimensions -> Dimensions
    - Measures -> Measures
    - Joins -> Inferred from relationships
    - Views -> Composite Models
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse Cube YAML files into semantic graph.

        Args:
            source: Path to Cube YAML file or directory

        Returns:
            Semantic graph with imported models
        """
        graph = SemanticGraph()
        source_path = Path(source)
        pending_views: list[dict] = []
        pending_hierarchies: dict[str, list[dict]] = {}

        if source_path.is_dir():
            # Parse all YAML files in directory
            for yaml_file in source_path.rglob("*.yml"):
                self._parse_file(yaml_file, graph, pending_views, pending_hierarchies)
            for yaml_file in source_path.rglob("*.yaml"):
                self._parse_file(yaml_file, graph, pending_views, pending_hierarchies)
        else:
            # Parse single file
            self._parse_file(source_path, graph, pending_views, pending_hierarchies)

        # Resolve extends (inheritance) after all cubes are parsed
        from sidemantic.core.inheritance import resolve_model_inheritance

        if any(m.extends for m in graph.models.values()):
            graph.models = resolve_model_inheritance(graph.models)

        # Apply hierarchies after inheritance so inherited dimensions are available
        for model_name, hierarchy_defs in pending_hierarchies.items():
            model = graph.models.get(model_name)
            if not model:
                continue
            for h_def in hierarchy_defs:
                levels = h_def.get("levels", [])
                for i in range(1, len(levels)):
                    child_name = levels[i]
                    parent_name = levels[i - 1]
                    if "." not in parent_name and "." not in child_name:
                        child_dim = model.get_dimension(child_name)
                        if child_dim:
                            child_dim.parent = parent_name

        # Parse views after all cubes are loaded and inheritance resolved
        for view_def in pending_views:
            model = self._parse_view(view_def, graph)
            if model:
                graph.add_model(model)

        return graph

    def _parse_file(
        self,
        file_path: Path,
        graph: SemanticGraph,
        pending_views: list[dict],
        pending_hierarchies: dict[str, list[dict]],
    ) -> None:
        """Parse a single Cube YAML file.

        Args:
            file_path: Path to YAML file
            graph: Semantic graph to add models to
            pending_views: List to accumulate view definitions for deferred parsing
            pending_hierarchies: Dict to accumulate hierarchy definitions per cube for deferred application
        """
        with open(file_path) as f:
            data = yaml.safe_load(f)

        if not data:
            return

        # Cube YAML has "cubes" key with list of cube definitions
        cubes = data.get("cubes") or []

        for cube_def in cubes:
            model = self._parse_cube(cube_def)
            if model:
                graph.add_model(model)
                # Collect hierarchies for deferred application (after inheritance)
                h_defs = cube_def.get("hierarchies")
                if h_defs:
                    pending_hierarchies[model.name] = h_defs

        # Collect views for deferred parsing (need all cubes loaded first)
        for view_def in data.get("views") or []:
            pending_views.append(view_def)

    def _extract_fk_from_join_sql(self, join_sql: str, relationship_type: str, join_name: str) -> str | None:
        """Extract foreign key column from Cube join SQL.

        Parses join SQL to extract the foreign key column name based on relationship type:
        - many_to_one: Extract from ${CUBE}.column (e.g., "${CUBE}.company_id = ${companies.id}" -> "company_id")
        - one_to_many: Extract from ${join_name.column} (e.g., "${CUBE}.id = ${project_assignments.project_id}" -> "project_id")

        Args:
            join_sql: Join SQL expression from Cube definition
            relationship_type: Type of relationship (many_to_one or one_to_many)
            join_name: Name of the joined model

        Returns:
            Foreign key column name, or None if parsing fails
        """
        if relationship_type == "one_to_many":
            # For one_to_many, extract from ${join_name.column}
            # Example: "${CUBE}.id = ${project_assignments.project_id}" -> "project_id"
            match = re.search(rf"\$\{{{re.escape(join_name)}\.(\w+)\}}", join_sql)
            if match:
                return match.group(1)
        else:
            # For many_to_one (default), extract from ${CUBE}.column
            # Example: "${CUBE}.company_id = ${companies.id}" -> "company_id"
            match = re.search(r"\$\{CUBE\}\.(\w+)", join_sql)
            if match:
                return match.group(1)

        return None

    def _parse_cube(self, cube_def: dict) -> Model | None:
        """Parse a Cube definition into a Model.

        Args:
            cube_def: Cube definition dictionary

        Returns:
            Model instance or None if parsing fails
        """
        name = cube_def.get("name")
        if not name:
            return None

        # Get table name and extends
        table = cube_def.get("sql_table")
        sql = cube_def.get("sql")
        extends = cube_def.get("extends")
        cube_meta = cube_def.get("meta")

        # Parse dimensions and find primary key
        dimensions = []
        primary_key = None  # Only set if explicitly declared

        for dim_def in cube_def.get("dimensions") or []:
            dim = self._parse_dimension(dim_def, name)
            if dim:
                dimensions.append(dim)

                # Check if this is a primary key
                if dim_def.get("primary_key"):
                    primary_key = dim.name

        # Parse measures
        measures = []
        for measure_def in cube_def.get("measures") or []:
            measure = self._parse_measure(measure_def, name)
            if measure:
                measures.append(measure)

        # Parse segments
        from sidemantic.core.segment import Segment

        segments = []
        for segment_def in cube_def.get("segments") or []:
            segment_name = segment_def.get("name")
            segment_sql = segment_def.get("sql")
            if segment_name and segment_sql:
                # Normalize ${CUBE}/{CUBE} to {model} placeholder
                segment_sql = _normalize_cube_sql(segment_sql, name)
                segment_public = segment_def.get("shown", segment_def.get("public", True))
                segments.append(
                    Segment(
                        name=segment_name,
                        sql=segment_sql,
                        description=segment_def.get("description"),
                        public=segment_public,
                    )
                )

        # Parse joins to create relationships
        relationships = []
        for join_def in cube_def.get("joins") or []:
            join_name = join_def.get("name")
            if join_name:
                # Get relationship type from join definition, default to many_to_one
                rel_type = join_def.get("relationship", "many_to_one")

                # Extract foreign key from join SQL, fallback to convention
                join_sql = join_def.get("sql", "")
                fk_column = self._extract_fk_from_join_sql(join_sql, rel_type, join_name)
                if not fk_column:
                    # Fallback to conventional naming if parsing fails
                    fk_column = f"{join_name}_id"

                relationships.append(Relationship(name=join_name, type=rel_type, foreign_key=fk_column))

        # Parse pre-aggregations (handle None from empty YAML section)
        pre_aggregations = []
        for preagg_def in cube_def.get("pre_aggregations") or []:
            preagg = self._parse_preaggregation(preagg_def, name)
            if preagg:
                pre_aggregations.append(preagg)

        # Build kwargs, omitting None table/sql/primary_key so inheritance can provide them
        model_kwargs = {
            "name": name,
            "relationships": relationships,
            "dimensions": dimensions,
            "metrics": measures,
            "segments": segments,
            "pre_aggregations": pre_aggregations,
        }
        if primary_key is not None:
            model_kwargs["primary_key"] = primary_key
        if table is not None:
            model_kwargs["table"] = table
        if sql is not None:
            model_kwargs["sql"] = sql
        if extends is not None:
            model_kwargs["extends"] = extends
        if cube_meta is not None:
            model_kwargs["meta"] = cube_meta
        desc = cube_def.get("description")
        if desc is not None:
            model_kwargs["description"] = desc

        return Model(**model_kwargs)

    def _parse_dimension(self, dim_def: dict, cube_name: str) -> Dimension | None:
        """Parse Cube dimension into Sidemantic dimension.

        Args:
            dim_def: Dimension definition dictionary
            cube_name: Name of the parent cube (for SQL normalization)

        Returns:
            Dimension instance or None
        """
        name = dim_def.get("name")
        if not name:
            return None

        dim_type = dim_def.get("type", "string")

        # Map Cube types to Sidemantic types
        type_mapping = {
            "string": "categorical",
            "number": "numeric",
            "time": "time",
            "boolean": "categorical",
        }

        sidemantic_type = type_mapping.get(dim_type, "categorical")

        # For time dimensions, extract granularity
        granularity = None
        if dim_type == "time":
            granularity = "day"  # Default granularity

        # Custom granularities on time dimensions
        supported_granularities = None
        custom_grans = dim_def.get("granularities")
        if custom_grans and isinstance(custom_grans, list):
            supported_granularities = [g.get("name") for g in custom_grans if g.get("name")]

        # Normalize SQL to replace ${CUBE}/{CUBE} with {model}
        dim_sql = _normalize_cube_sql(dim_def.get("sql"), cube_name)

        # Convert case/when/else blocks to SQL CASE expressions
        case_def = dim_def.get("case")
        if case_def and not dim_sql:
            whens = case_def.get("when", [])
            else_clause = case_def.get("else", {})
            parts = []
            for w in whens:
                cond = _normalize_cube_sql(w.get("sql"), cube_name)
                lbl = w.get("label", "").replace("'", "''")
                parts.append(f"WHEN {cond} THEN '{lbl}'")
            if else_clause:
                else_label = else_clause.get("label", "Unknown").replace("'", "''")
                parts.append(f"ELSE '{else_label}'")
            dim_sql = "CASE " + " ".join(parts) + " END"

        # Read additional metadata fields
        label = dim_def.get("title")
        meta = dim_def.get("meta")
        public = dim_def.get("shown", dim_def.get("public", True))

        return Dimension(
            name=name,
            type=sidemantic_type,
            sql=dim_sql,
            granularity=granularity,
            supported_granularities=supported_granularities,
            description=dim_def.get("description"),
            format=dim_def.get("format"),
            label=label,
            meta=meta,
            public=public,
        )

    def _parse_measure(self, measure_def: dict, cube_name: str) -> Metric | None:
        """Parse Cube measure into Sidemantic measure.

        Args:
            measure_def: Metric definition dictionary
            cube_name: Name of the parent cube (for SQL normalization)

        Returns:
            Measure instance or None
        """
        name = measure_def.get("name")
        if not name:
            return None

        measure_type = measure_def.get("type", "count")

        # Map Cube measure types to Sidemantic aggregation types
        type_mapping = {
            "count": "count",
            "count_distinct": "count_distinct",
            "count_distinct_approx": "count_distinct",
            "sum": "sum",
            "avg": "avg",
            "min": "min",
            "max": "max",
            "number": None,  # Calculated measures - determine type from context
        }

        # Read additional metadata fields early (may be modified by rank handling)
        label = measure_def.get("title")
        meta = measure_def.get("meta")
        drill_fields = measure_def.get("drill_members")
        public = measure_def.get("shown", measure_def.get("public", True))

        agg_type = type_mapping.get(measure_type)
        metric_type = None

        # Handle unknown measure types explicitly
        if agg_type is None and measure_type not in ("number",):
            if measure_type == "rank":
                # Rank measures: use count as executable fallback, store rank
                # metadata so consumers can handle them specially
                agg_type = "count"
                meta = meta.copy() if meta else {}
                meta["cube_type"] = "rank"
                meta["order_by"] = measure_def.get("order_by")
                meta["reduce_by"] = measure_def.get("reduce_by")
            else:
                # Truly unknown types fall back to count
                agg_type = "count"

        # Parse filters and normalize ${CUBE}/{CUBE} references
        filters = []
        for filter_def in measure_def.get("filters") or []:
            if isinstance(filter_def, dict):
                sql_filter = filter_def.get("sql")
                if sql_filter:
                    filters.append(_normalize_cube_sql(sql_filter, cube_name))

        # Check for rolling_window (cumulative metric)
        rolling_window = measure_def.get("rolling_window")
        window = None
        grain_to_date = None
        if rolling_window:
            metric_type = "cumulative"
            window = rolling_window.get("trailing")
            # Handle type: to_date with granularity (e.g., YTD, MTD)
            rw_type = rolling_window.get("type")
            rw_granularity = rolling_window.get("granularity")
            if rw_type == "to_date" and rw_granularity:
                grain_to_date = rw_granularity

        # For calculated measures (type=number), treat as derived with SQL expression
        if measure_type == "number" and not rolling_window:
            sql_expr = measure_def.get("sql", "")
            if sql_expr:
                metric_type = "derived"

        # Normalize SQL to replace ${CUBE}/{CUBE} with {model}
        measure_sql = _normalize_cube_sql(measure_def.get("sql"), cube_name)

        # Check for time_shift (period-over-period comparison)
        # Only convert to time_comparison if we can extract a base_metric reference
        base_metric = None
        comparison_type = None
        time_offset = None
        time_shift_def = measure_def.get("time_shift")
        if time_shift_def and isinstance(time_shift_def, list) and len(time_shift_def) > 0:
            ts = time_shift_def[0]
            ts_interval = ts.get("interval")
            ts_type = ts.get("type")
            if ts_type == "prior" and ts_interval and measure_sql:
                # Extract base_metric from sql like "{measure_name}"
                base_match = re.match(r"^\s*\{(\w+)\}\s*$", measure_sql)
                if base_match:
                    base_metric = f"{cube_name}.{base_match.group(1)}"
                    measure_sql = None  # Clear sql, base_metric carries the reference
                    metric_type = "time_comparison"
                    time_offset = str(ts_interval)
                    comparison_map = {
                        "1 year": "yoy",
                        "1 month": "mom",
                        "1 week": "wow",
                        "1 day": "dod",
                        "1 quarter": "qoq",
                    }
                    comparison_type = comparison_map.get(ts_interval, "prior_period")

        # Convert ${measure_name} references to model_name.measure_name format
        # This is needed for derived metrics that reference other measures
        numerator = None
        denominator = None
        if measure_sql and metric_type == "derived":
            # Check if this is a simple ratio pattern: ${measure1} / ${measure2}
            # This is a common pattern in Cube for ratio metrics
            ratio_pattern = (
                r"^\s*\$\{(\w+)\}(?:::[\w\s]+)?\s*/\s*(?:NULLIF\()?\$\{(\w+)\}(?:::[\w\s]+)?(?:,\s*0\))?\s*$"
            )
            ratio_match = re.match(ratio_pattern, measure_sql, re.IGNORECASE)

            if ratio_match:
                # This is a simple ratio - convert to ratio metric type
                num_measure = ratio_match.group(1)
                denom_measure = ratio_match.group(2)
                metric_type = "ratio"
                numerator = f"{cube_name}.{num_measure}"
                denominator = f"{cube_name}.{denom_measure}"
                measure_sql = None  # Ratio metrics don't use sql field
            else:
                # Check if SQL contains inline aggregations (COUNT, SUM, AVG, etc.)
                # These are "SQL expression metrics" that already contain aggregation
                has_inline_agg = any(agg in measure_sql.upper() for agg in ["COUNT(", "SUM(", "AVG(", "MIN(", "MAX("])

                if has_inline_agg:
                    # This is a SQL expression metric with inline aggregations
                    # Don't try to replace measure references - use SQL as-is
                    # Set agg=None to signal this is a complete SQL expression
                    agg_type = None
                else:
                    # Complex derived metric - replace measure references
                    def replace_measure_ref(match):
                        measure_ref = match.group(1)
                        # Don't replace if it's already been normalized to {model}
                        if measure_ref == "model":
                            return "{model}"
                        # Convert ${measure_name} to cube_name.measure_name
                        return f"{cube_name}.{measure_ref}"

                    measure_sql = re.sub(r"\$\{(\w+)\}", replace_measure_ref, measure_sql)

        return Metric(
            name=name,
            type=metric_type,
            agg=agg_type,
            sql=measure_sql,
            numerator=numerator,
            denominator=denominator,
            window=window,
            grain_to_date=grain_to_date,
            base_metric=base_metric,
            comparison_type=comparison_type,
            time_offset=time_offset,
            filters=filters if filters else None,
            description=measure_def.get("description"),
            format=measure_def.get("format"),
            label=label,
            meta=meta,
            drill_fields=drill_fields,
            public=public,
        )

    def _parse_preaggregation(self, preagg_def: dict, cube_name: str) -> PreAggregation | None:
        """Parse Cube pre-aggregation into Sidemantic pre-aggregation.

        Args:
            preagg_def: Pre-aggregation definition dictionary
            cube_name: Name of the parent cube

        Returns:
            PreAggregation instance or None
        """
        name = preagg_def.get("name")
        if not name:
            return None

        preagg_type = preagg_def.get("type", "rollup")

        # Normalize Cube camelCase type names to Sidemantic snake_case
        preagg_type_mapping = {
            "rollupJoin": "rollup_join",
            "rollupLambda": "lambda",
            "rollup_join": "rollup_join",
            "original_sql": "original_sql",
            "rollup": "rollup",
            "lambda": "lambda",
        }
        preagg_type = preagg_type_mapping.get(preagg_type, preagg_type)

        # Extract measures - strip CUBE prefix if present
        measures = []
        for measure_ref in preagg_def.get("measures") or []:
            if isinstance(measure_ref, str):
                # Remove CUBE. or {cube_name}. prefix
                measure_name = measure_ref.replace("CUBE.", "").replace(f"{cube_name}.", "")
                measures.append(measure_name)

        # Extract dimensions - strip CUBE prefix if present
        dimensions = []
        for dim_ref in preagg_def.get("dimensions") or []:
            if isinstance(dim_ref, str):
                # Remove CUBE. or {cube_name}. prefix
                dim_name = dim_ref.replace("CUBE.", "").replace(f"{cube_name}.", "")
                dimensions.append(dim_name)

        # Parse time dimension
        time_dimension = preagg_def.get("time_dimension")
        if time_dimension:
            # Remove CUBE. prefix if present
            time_dimension = time_dimension.replace("CUBE.", "").replace(f"{cube_name}.", "")

        # Parse granularity
        granularity = preagg_def.get("granularity")

        # Parse partition granularity
        partition_granularity = preagg_def.get("partition_granularity")

        # Parse refresh key
        refresh_key_def = preagg_def.get("refresh_key")
        refresh_key = None
        if refresh_key_def:
            refresh_key = RefreshKey(
                every=refresh_key_def.get("every"),
                sql=refresh_key_def.get("sql"),
                incremental=refresh_key_def.get("incremental", False),
                update_window=refresh_key_def.get("update_window"),
            )

        # Parse scheduled refresh
        scheduled_refresh = preagg_def.get("scheduled_refresh", True)

        # Parse indexes
        indexes = []
        for index_def in preagg_def.get("indexes") or []:
            if isinstance(index_def, dict):
                index_name = index_def.get("name", f"idx_{len(indexes)}")
                index_columns = index_def.get("columns") or []

                # Strip CUBE prefix from column names
                cleaned_columns = [col.replace("CUBE.", "").replace(f"{cube_name}.", "") for col in index_columns]

                indexes.append(
                    Index(
                        name=index_name,
                        columns=cleaned_columns,
                        type=index_def.get("type", "regular"),
                    )
                )

        # Parse build range
        build_range_start = preagg_def.get("build_range_start", {}).get("sql")
        build_range_end = preagg_def.get("build_range_end", {}).get("sql")

        return PreAggregation(
            name=name,
            type=preagg_type,
            measures=measures if measures else None,
            dimensions=dimensions if dimensions else None,
            time_dimension=time_dimension,
            granularity=granularity,
            partition_granularity=partition_granularity,
            refresh_key=refresh_key,
            scheduled_refresh=scheduled_refresh,
            indexes=indexes if indexes else None,
            build_range_start=build_range_start,
            build_range_end=build_range_end,
        )

    def _parse_view(self, view_def: dict, graph: SemanticGraph) -> Model | None:
        """Parse a Cube view into a composite Model.

        Views project and rename members from existing cubes via join_path,
        includes, excludes, prefix, and alias.

        Args:
            view_def: View definition dictionary
            graph: Semantic graph with already-parsed cubes

        Returns:
            Model instance or None
        """
        name = view_def.get("name")
        if not name:
            return None

        dimensions = []
        metrics = []

        for cube_spec in view_def.get("cubes", []):
            join_path = cube_spec.get("join_path", "")
            # Resolve target cube: last segment of join_path
            target_name = join_path.split(".")[-1] if join_path else None
            target = graph.models.get(target_name) if target_name else None
            if not target:
                continue

            includes = cube_spec.get("includes", [])
            excludes = set(cube_spec.get("excludes") or [])
            prefix = cube_spec.get("prefix", False)
            cube_alias = cube_spec.get("alias")
            prefix_str = f"{cube_alias or target_name}_" if prefix else ""

            if includes == "*":
                dims = [d for d in target.dimensions if d.name not in excludes]
                mets = [m for m in target.metrics if m.name not in excludes]
            elif isinstance(includes, list):
                dims, mets = [], []
                for inc in includes:
                    if isinstance(inc, str):
                        d = target.get_dimension(inc)
                        if d and d.name not in excludes:
                            dims.append(d)
                        m = target.get_metric(inc)
                        if m and m.name not in excludes:
                            mets.append(m)
                    elif isinstance(inc, dict):
                        orig = inc.get("name", "")
                        alias = inc.get("alias", orig)
                        d = target.get_dimension(orig)
                        if d:
                            dims.append(d.model_copy(update={"name": alias}))
                        m = target.get_metric(orig)
                        if m:
                            mets.append(m.model_copy(update={"name": alias}))
            else:
                continue

            if prefix_str:
                # Prefix names and update dependent references (parent, drill_fields)
                prefixed_dims = []
                for d in dims:
                    updates: dict = {"name": f"{prefix_str}{d.name}"}
                    if d.parent:
                        updates["parent"] = f"{prefix_str}{d.parent}"
                    prefixed_dims.append(d.model_copy(update=updates))
                dims = prefixed_dims

                prefixed_mets = []
                for m in mets:
                    updates = {"name": f"{prefix_str}{m.name}"}
                    if m.drill_fields:
                        updates["drill_fields"] = [f"{prefix_str}{f}" for f in m.drill_fields]
                    prefixed_mets.append(m.model_copy(update=updates))
                mets = prefixed_mets

            dimensions.extend(dims)
            metrics.extend(mets)

        # Only create a model if the view resolved at least some members
        if not dimensions and not metrics:
            return None

        return Model(
            name=name,
            dimensions=dimensions,
            metrics=metrics,
            meta={"cube_type": "view"},
        )

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to Cube YAML format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output YAML file
        """
        output_path = Path(output_path)

        # Resolve inheritance first
        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        # Convert models to cubes (skip view-type models)
        cubes = []
        for model in resolved_models.values():
            if model.meta and model.meta.get("cube_type") == "view":
                continue
            cube = self._export_cube(model, resolved_models)
            cubes.append(cube)

        data = {"cubes": cubes}

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            yaml.dump(data, f, sort_keys=False, default_flow_style=False)

    def _export_cube(self, model: Model, resolved_models: dict[str, Model]) -> dict:
        """Export model to Cube definition.

        Args:
            model: Model to export
            resolved_models: Resolved (inheritance-applied) models dict for join target lookup

        Returns:
            Cube definition dictionary
        """
        cube = {"name": model.name}

        if model.table:
            cube["sql_table"] = model.table
        elif model.sql:
            cube["sql"] = model.sql

        if model.description:
            cube["description"] = model.description

        if model.meta:
            cube["meta"] = model.meta

        # Export dimensions
        dimensions = []
        drill_members = []  # Track dimensions with drill-down support

        for dim in model.dimensions:
            dim_def = {"name": dim.name}

            # Map Sidemantic types to Cube types
            type_mapping = {
                "categorical": "string",
                "numeric": "number",
                "time": "time",
                "boolean": "boolean",
            }
            dim_def["type"] = type_mapping.get(dim.type, "string")

            if dim.sql:
                dim_def["sql"] = dim.sql

            if dim.description:
                dim_def["description"] = dim.description

            # Add metadata fields
            if dim.format:
                dim_def["format"] = dim.format

            if dim.label:
                dim_def["title"] = dim.label

            if dim.meta:
                dim_def["meta"] = dim.meta

            if not dim.public:
                dim_def["shown"] = False

            # Mark primary key dimension
            if model.primary_key and dim.name == model.primary_key:
                dim_def["primary_key"] = True

            # Track hierarchies - collect all dimensions for drill_members
            if dim.parent or any(other.parent == dim.name for other in model.dimensions):
                drill_members.append(dim.name)

            dimensions.append(dim_def)

        # Add primary key dimension if not already in dimensions
        if model.primary_key:
            dim_names = [d["name"] for d in dimensions]
            if model.primary_key not in dim_names:
                dimensions.append(
                    {
                        "name": model.primary_key,
                        "type": "number",
                        "sql": model.primary_key,
                        "primary_key": True,
                    }
                )

        if dimensions:
            cube["dimensions"] = dimensions

        # Export measures
        measures = []
        for measure in model.metrics:
            measure_def = {"name": measure.name}

            # Handle different metric types
            if measure.type == "ratio":
                # Ratio metrics become calculated measures with ${measure} references
                measure_def["type"] = "number"
                if measure.numerator and measure.denominator:
                    # Convert model.measure to ${measure} format for Cube
                    num_ref = measure.numerator.split(".")[-1] if "." in measure.numerator else measure.numerator
                    denom_ref = (
                        measure.denominator.split(".")[-1] if "." in measure.denominator else measure.denominator
                    )
                    measure_def["sql"] = f"${{{num_ref}}}::float / NULLIF(${{{denom_ref}}}, 0)"
            elif measure.type == "derived":
                # Derived metrics become calculated measures
                measure_def["type"] = "number"
                if measure.sql:
                    measure_def["sql"] = measure.sql
            elif measure.type == "cumulative":
                # Cumulative metrics become rolling window measures (Cube has rolling_window)
                measure_def["type"] = "number"
                if measure.sql:
                    measure_def["sql"] = measure.sql
                if measure.window:
                    measure_def["rolling_window"] = {"trailing": measure.window}
                elif measure.grain_to_date:
                    measure_def["rolling_window"] = {
                        "type": "to_date",
                        "granularity": measure.grain_to_date,
                    }
            elif measure.type == "time_comparison":
                # Time comparison - use Cube's time dimension features
                measure_def["type"] = "number"
                if measure.base_metric:
                    # Convert qualified name (cube.metric) to Cube reference (${metric})
                    base_ref = measure.base_metric.split(".")[-1] if "." in measure.base_metric else measure.base_metric
                    measure_def["sql"] = f"${{{base_ref}}}"
                    measure_def["description"] = (measure.description or "") + f" (Time comparison of {base_ref})"
            else:
                # Regular aggregation measure
                type_mapping = {
                    "count": "count",
                    "count_distinct": "count_distinct",
                    "sum": "sum",
                    "avg": "avg",
                    "min": "min",
                    "max": "max",
                }
                measure_def["type"] = type_mapping.get(measure.agg, "count")

                if measure.sql:
                    measure_def["sql"] = measure.sql

            if measure.filters:
                measure_def["filters"] = [{"sql": f} for f in measure.filters]

            if measure.description:
                measure_def["description"] = measure.description

            # Add metadata fields
            if measure.format:
                measure_def["format"] = measure.format

            if measure.label:
                measure_def["title"] = measure.label

            if measure.meta:
                measure_def["meta"] = measure.meta

            if not measure.public:
                measure_def["shown"] = False

            # Add drill fields if specified
            if measure.drill_fields and drill_members:
                # Only include drill fields that exist in this model
                valid_drill = [f for f in measure.drill_fields if f in [d.name for d in model.dimensions]]
                if valid_drill:
                    measure_def["drill_members"] = valid_drill
            elif measure.drill_fields:
                measure_def["drill_members"] = measure.drill_fields
            elif drill_members:
                # Default to hierarchy dimensions
                measure_def["drill_members"] = drill_members

            measures.append(measure_def)

        if measures:
            cube["measures"] = measures

        # Export segments
        if model.segments:
            cube["segments"] = []
            for segment in model.segments:
                segment_def = {"name": segment.name}
                if segment.sql:
                    # Replace {model} placeholder with CUBE reference
                    segment_sql = segment.sql.replace("{model}", "${CUBE}")
                    segment_def["sql"] = segment_sql
                if segment.description:
                    segment_def["description"] = segment.description
                if not segment.public:
                    segment_def["shown"] = False
                cube["segments"].append(segment_def)

        # Export joins (all relationship types)
        joins = []
        for relationship in model.relationships:
            # Skip many_to_many (needs junction table info)
            if relationship.type == "many_to_many":
                continue

            # Find target model from resolved models (inheritance-applied)
            target_model = resolved_models.get(relationship.name)
            if target_model:
                # Use ${CUBE} for local side and ${target.col} for remote side
                # so _extract_fk_from_join_sql can re-parse the exported SQL
                if relationship.type in ("many_to_one", "one_to_one"):
                    local_key = relationship.sql_expr or relationship.foreign_key
                    remote_key = relationship.primary_key or target_model.primary_key
                    if isinstance(remote_key, list):
                        remote_key = remote_key[0]
                    join_sql = f"${{CUBE}}.{local_key} = ${{{relationship.name}}}.{remote_key}"
                else:
                    # one_to_many: ${CUBE}.pk = ${target.fk}
                    local_key = model.primary_key if isinstance(model.primary_key, str) else model.primary_key[0]
                    remote_key = relationship.sql_expr or relationship.foreign_key
                    join_sql = f"${{CUBE}}.{local_key} = ${{{relationship.name}}}.{remote_key}"

                join_def = {
                    "name": relationship.name,
                    "sql": join_sql,
                    "relationship": relationship.type,
                }
                joins.append(join_def)

        if joins:
            cube["joins"] = joins

        # Export pre-aggregations
        if model.pre_aggregations:
            cube["pre_aggregations"] = []
            for preagg in model.pre_aggregations:
                preagg_def = {"name": preagg.name, "type": preagg.type}
                if preagg.measures:
                    preagg_def["measures"] = [f"CUBE.{m}" for m in preagg.measures]
                if preagg.dimensions:
                    preagg_def["dimensions"] = [f"CUBE.{d}" for d in preagg.dimensions]
                if preagg.time_dimension:
                    preagg_def["time_dimension"] = preagg.time_dimension
                if preagg.granularity:
                    preagg_def["granularity"] = preagg.granularity
                if preagg.partition_granularity:
                    preagg_def["partition_granularity"] = preagg.partition_granularity
                if preagg.refresh_key:
                    rk = {}
                    if preagg.refresh_key.every:
                        rk["every"] = preagg.refresh_key.every
                    if preagg.refresh_key.sql:
                        rk["sql"] = preagg.refresh_key.sql
                    if preagg.refresh_key.incremental:
                        rk["incremental"] = True
                    if preagg.refresh_key.update_window:
                        rk["update_window"] = preagg.refresh_key.update_window
                    if rk:
                        preagg_def["refresh_key"] = rk
                if not preagg.scheduled_refresh:
                    preagg_def["scheduled_refresh"] = False
                if preagg.indexes:
                    preagg_def["indexes"] = [
                        {"name": idx.name, "columns": idx.columns, "type": idx.type} for idx in preagg.indexes
                    ]
                if preagg.build_range_start:
                    preagg_def["build_range_start"] = {"sql": preagg.build_range_start}
                if preagg.build_range_end:
                    preagg_def["build_range_end"] = {"sql": preagg.build_range_end}
                cube["pre_aggregations"].append(preagg_def)

        return cube
