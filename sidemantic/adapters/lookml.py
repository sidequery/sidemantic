"""LookML adapter for importing Looker semantic models."""

import re
from pathlib import Path

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph


def _import_lkml():
    """Lazily import lkml, raising a clear error if not installed."""
    try:
        import lkml
    except ImportError:
        raise ImportError('LookML support requires lkml. Install with: pip install "sidemantic[lookml]"') from None
    return lkml


class LookMLAdapter(BaseAdapter):
    """Adapter for importing/exporting LookML view definitions.

    Transforms LookML definitions into Sidemantic format:
    - Views -> Models
    - Dimensions -> Dimensions
    - Measures -> Metrics
    - dimension_group (time) -> Time dimensions
    - derived_table -> Model with SQL
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse LookML files into semantic graph.

        Args:
            source: Path to .lkml file or directory

        Returns:
            Semantic graph with imported models
        """
        graph = SemanticGraph()
        source_path = Path(source)

        # Collect all .lkml files
        lkml_files = []
        if source_path.is_dir():
            lkml_files = list(source_path.rglob("*.lkml"))
        else:
            lkml_files = [source_path]

        # First pass: parse all views, collecting refinements separately
        refinements: list[Model] = []
        for lkml_file in lkml_files:
            self._parse_views_from_file(lkml_file, graph, refinements)

        # Apply refinements: merge each refinement into its base view
        from sidemantic.core.inheritance import merge_model, resolve_model_inheritance

        for refinement in refinements:
            base_name = refinement.name.lstrip("+")
            if base_name in graph.models:
                # Create a copy with the base name for merging
                refinement_for_merge = refinement.model_copy(update={"name": base_name})
                merged = merge_model(refinement_for_merge, graph.models[base_name])
                graph.models[base_name] = merged

        # Resolve extends chains. Pre-filter to models whose full chain
        # is present so one broken/missing parent doesn't block valid ones.
        def _chain_resolvable(name: str, visited: set[str] | None = None) -> bool:
            if visited is None:
                visited = set()
            if name in visited:
                return False  # circular
            model = graph.models.get(name)
            if not model:
                return False
            if not model.extends:
                return True
            visited.add(name)
            return _chain_resolvable(model.extends, visited)

        resolvable = {n: m for n, m in graph.models.items() if _chain_resolvable(n)}
        unresolvable = {n: m for n, m in graph.models.items() if n not in resolvable}

        if resolvable:
            resolved = resolve_model_inheritance(resolvable)
            resolved.update(unresolvable)
            graph.models = resolved

        # Second pass: parse explores and add relationships
        for lkml_file in lkml_files:
            self._parse_explores_from_file(lkml_file, graph)

        # Rebuild adjacency graph now that relationships have been added
        graph.build_adjacency()

        return graph

    def _parse_views_from_file(
        self, file_path: Path, graph: SemanticGraph, refinements: list[Model] | None = None
    ) -> None:
        """Parse views from a single LookML file.

        Args:
            file_path: Path to .lkml file
            graph: Semantic graph to add models to
            refinements: Optional list to collect refinement models into
        """
        lkml = _import_lkml()

        with open(file_path) as f:
            content = f.read()

        parsed = lkml.load(content)

        if not parsed:
            return

        # Parse views
        for view_def in parsed.get("views") or []:
            model = self._parse_view(view_def)
            if model:
                if model.name.startswith("+"):
                    # Refinement: collect separately for merging after all views parsed
                    if refinements is not None:
                        refinements.append(model)
                else:
                    graph.add_model(model)

    def _parse_explores_from_file(self, file_path: Path, graph: SemanticGraph) -> None:
        """Parse explores from a single LookML file and add relationships.

        Args:
            file_path: Path to .lkml file
            graph: Semantic graph to add relationships to
        """
        lkml = _import_lkml()

        with open(file_path) as f:
            content = f.read()

        parsed = lkml.load(content)

        if not parsed:
            return

        # Parse explores
        for explore_def in parsed.get("explores") or []:
            self._parse_explore(explore_def, graph)

    def _resolve_dimension_references(self, sql: str, dimension_sql_lookup: dict[str, str], max_depth: int = 10) -> str:
        """Resolve ${dimension_name} references in SQL expressions.

        This handles LookML's dimension reference syntax where measures and dimensions
        can reference other dimensions using ${dimension_name}. It handles recursive
        resolution when a dimension references another dimension.

        Args:
            sql: SQL expression that may contain ${dimension_name} references
            dimension_sql_lookup: Dict mapping dimension names to their SQL expressions
            max_depth: Maximum recursion depth to prevent infinite loops

        Returns:
            SQL with all dimension references resolved
        """
        if not sql or max_depth <= 0:
            return sql

        # Pattern to match ${name} but NOT ${TABLE} or ${model.field}
        pattern = r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}"

        def replace_ref(match: re.Match) -> str:
            ref_name = match.group(1)
            if ref_name == "TABLE":
                # Keep ${TABLE} as-is, it's handled separately
                return match.group(0)
            if ref_name in dimension_sql_lookup:
                # Return the dimension's SQL, wrapped in parentheses for safety
                return f"({dimension_sql_lookup[ref_name]})"
            # Unknown reference, keep as-is
            return match.group(0)

        resolved = re.sub(pattern, replace_ref, sql)

        # If we made changes and there are still references, recurse
        if resolved != sql and re.search(pattern, resolved):
            # Check if remaining refs are just ${TABLE} or unknown
            remaining_refs = re.findall(pattern, resolved)
            if any(ref != "TABLE" and ref in dimension_sql_lookup for ref in remaining_refs):
                return self._resolve_dimension_references(resolved, dimension_sql_lookup, max_depth - 1)

        return resolved

    def _convert_lookml_filter_to_sql(self, field: str, value: str) -> str:
        """Convert a LookML filter value to SQL condition.

        Handles LookML filter syntax:
        - "value" -> field = 'value'
        - "val1,val2,val3" -> field IN ('val1', 'val2', 'val3')
        - "-value" -> field != 'value' (negation)
        - "-val1,-val2" -> field NOT IN ('val1', 'val2')
        - "yes"/"no" -> field = true/false (for yesno dimensions)
        - ">100", ">=50", "<10", "<=5", "!=0" -> numeric comparisons
        - "%pattern%" -> field LIKE '%pattern%' (wildcards)
        - "NULL" -> field IS NULL
        - "-NULL" -> field IS NOT NULL
        - "EMPTY" -> field = ''
        - "-EMPTY" -> field != ''

        Args:
            field: The field name
            value: The LookML filter value

        Returns:
            SQL condition string
        """
        # Handle NULL special values
        if value.upper() == "NULL":
            return f"{{model}}.{field} IS NULL"
        if value.upper() == "-NULL":
            return f"{{model}}.{field} IS NOT NULL"

        # Handle EMPTY special values
        if value.upper() == "EMPTY":
            return f"{{model}}.{field} = ''"
        if value.upper() == "-EMPTY":
            return f"{{model}}.{field} != ''"

        # Handle yes/no boolean values
        if value.lower() == "yes":
            return f"{{model}}.{field} = true"
        if value.lower() == "no":
            return f"{{model}}.{field} = false"

        # Check if this is a comma-separated list of values (OR condition)
        # But be careful: ">100,<200" is two comparison operators, not a list
        if "," in value:
            parts = [p.strip() for p in value.split(",")]

            # Check if all parts are negations (NOT IN)
            if all(p.startswith("-") for p in parts):
                # Remove the - prefix from each
                clean_parts = [p[1:] for p in parts]
                # Check if they're all simple strings (not operators)
                if all(not re.match(r"^(>=|<=|!=|<>|>|<)", p) for p in clean_parts):
                    quoted = ", ".join(f"'{p}'" for p in clean_parts)
                    return f"{{model}}.{field} NOT IN ({quoted})"

            # Check if all parts are simple values (no operators) -> IN clause
            if all(not p.startswith("-") and not re.match(r"^(>=|<=|!=|<>|>|<)", p) for p in parts):
                # Check if all parts are numeric
                if all(p.replace(".", "").replace("-", "").isdigit() for p in parts):
                    # Numeric IN clause (no quotes)
                    return f"{{model}}.{field} IN ({', '.join(parts)})"
                else:
                    # String IN clause (with quotes)
                    quoted = ", ".join(f"'{p}'" for p in parts)
                    return f"{{model}}.{field} IN ({quoted})"

            # Mixed operators - this is actually multiple filter conditions
            # LookML doesn't really support this in a single filter value
            # Fall through to single value handling (will be slightly wrong but safer)

        # Handle negation prefix for single values
        if value.startswith("-") and not re.match(r"^-(>=|<=|!=|<>|>|<|\d)", value):
            negated_value = value[1:]
            if negated_value.replace(".", "").replace("-", "").isdigit():
                return f"{{model}}.{field} != {negated_value}"
            else:
                return f"{{model}}.{field} != '{negated_value}'"

        # Handle comparison operators: ">1000", "<=100", ">=5", "<10", "!=0"
        if match := re.match(r"^(>=|<=|!=|<>|>|<)(.+)$", value):
            operator, operand = match.groups()
            operand = operand.strip()
            # Normalize <> to !=
            if operator == "<>":
                operator = "!="
            # Check if operand is numeric
            if operand.replace(".", "").replace("-", "").isdigit():
                return f"{{model}}.{field} {operator} {operand}"
            else:
                return f"{{model}}.{field} {operator} '{operand}'"

        # Handle wildcard patterns (LIKE)
        if "%" in value or "_" in value:
            return f"{{model}}.{field} LIKE '{value}'"

        # Handle numeric values
        if value.replace(".", "").replace("-", "").isdigit():
            return f"{{model}}.{field} = {value}"

        # Default: string equality
        return f"{{model}}.{field} = '{value}'"

    def _parse_view(self, view_def: dict) -> Model | None:
        """Parse LookML view into Sidemantic model.

        Args:
            view_def: View definition dictionary (after parsing)

        Returns:
            Model instance or None
        """
        name = view_def.get("name")
        if not name:
            return None

        # Get table name
        table = view_def.get("sql_table_name")

        # Parse derived table SQL
        sql = None
        derived_table = view_def.get("derived_table")
        if derived_table:
            sql = derived_table.get("sql")
            # Handle native derived tables with explore_source
            if not sql and "explore_source" in derived_table:
                sql = self._convert_explore_source_to_sql(derived_table)

        # First pass: build a lookup dict of dimension SQL expressions
        # This is used to resolve ${dimension_name} references
        dimension_sql_lookup: dict[str, str] = {}
        dimension_defs = view_def.get("dimensions") or []

        # Get raw SQL for all dimensions (before resolving inter-dimension references)
        for dim_def in dimension_defs:
            dim_name = dim_def.get("name")
            dim_sql = dim_def.get("sql")
            if dim_name and dim_sql:
                # Replace ${TABLE} with {model} placeholder
                dim_sql = dim_sql.replace("${TABLE}", "{model}")
                dimension_sql_lookup[dim_name] = dim_sql

        # Also add dimension_group dimensions to the lookup
        for dim_group_def in view_def.get("dimension_groups") or []:
            group_name = dim_group_def.get("name")
            group_sql = dim_group_def.get("sql")
            if group_name and group_sql:
                group_sql = group_sql.replace("${TABLE}", "{model}")
                timeframes = dim_group_def.get("timeframes", ["date"])
                for timeframe in timeframes:
                    if timeframe != "raw":
                        dimension_sql_lookup[f"{group_name}_{timeframe}"] = group_sql

        # Resolve any dimension-to-dimension references in the lookup
        # (e.g., line_total references quantity, unit_price, line_discount)
        resolved_dimension_sql: dict[str, str] = {}
        for dim_name, dim_sql in dimension_sql_lookup.items():
            resolved_sql = self._resolve_dimension_references(dim_sql, dimension_sql_lookup)
            resolved_dimension_sql[dim_name] = resolved_sql

        # Parse dimensions with resolved SQL
        dimensions = []
        primary_key = "id"  # default

        for dim_def in dimension_defs:
            dim = self._parse_dimension(dim_def, resolved_dimension_sql)
            if dim:
                dimensions.append(dim)

                # Check for primary key
                if dim_def.get("primary_key") in ("yes", True):
                    primary_key = dim.name

        # Parse dimension_group (time dimensions)
        for dim_group_def in view_def.get("dimension_groups") or []:
            dims = self._parse_dimension_group(dim_group_def, resolved_dimension_sql)
            dimensions.extend(dims)

        # Build a set of dimension names for measure reference resolution
        dimension_names = {d.name for d in dimensions}

        # Collect measure names + their base aggregation up front so post-SQL
        # measures (running_total / percent_of_total / ...) can recognize a
        # ${ref} as a base measure, qualify it with {model} (which the generator
        # resolves to the measure's _raw column) and wrap it in the base
        # measure's own aggregate function.
        measure_names: set[str] = set()
        measure_agg_lookup: dict[str, str] = {}
        for m in view_def.get("measures") or []:
            m_name = m.get("name")
            if not m_name:
                continue
            measure_names.add(m_name)
            agg_template = self._SQL_AGG_FUNC.get(m.get("type", "count"))
            if agg_template:
                measure_agg_lookup[m_name] = agg_template

        # Parse measures with dimension SQL lookup for reference resolution
        measures = []
        for measure_def in view_def.get("measures") or []:
            measure = self._parse_measure(
                measure_def, dimension_names, resolved_dimension_sql, measure_names, measure_agg_lookup
            )
            if measure:
                measures.append(measure)

        # Parse segments
        from sidemantic.core.segment import Segment

        segments = []
        for segment_def in view_def.get("filters") or []:
            # LookML filters at view level can be used as segments
            segment_name = segment_def.get("name")
            segment_sql = segment_def.get("sql")
            if segment_name and segment_sql:
                # Replace ${TABLE} with {model} placeholder
                segment_sql = segment_sql.replace("${TABLE}", "{model}")
                segments.append(
                    Segment(
                        name=segment_name,
                        sql=segment_sql,
                        description=segment_def.get("description"),
                    )
                )

        # Build model-level meta from LookML properties
        model_meta = {}
        if view_def.get("extension") == "required":
            model_meta["extension_required"] = True
        if view_def.get("label"):
            model_meta["label"] = view_def["label"]
        if view_def.get("hidden") in ("yes", True):
            model_meta["hidden"] = True
        if view_def.get("tags"):
            model_meta["tags"] = view_def["tags"]

        # Extract extends (lkml parses as list, e.g. ["base_view"])
        extends_list = view_def.get("extends") or view_def.get("extends__all")
        extends = None
        if extends_list:
            if isinstance(extends_list, list):
                # Flatten nested lists from extends__all format
                flat = extends_list
                while flat and isinstance(flat[0], list):
                    flat = flat[0]
                extends = flat[0] if flat else None
            elif isinstance(extends_list, str):
                extends = extends_list

        # Build kwargs conditionally so that unset scalars don't appear in
        # model_fields_set. This matters for refinements: merge_model treats
        # every field in model_fields_set as an explicit child override, so
        # passing table=None or primary_key="id" would erase the base view's
        # real values.
        model_kwargs: dict = {
            "name": name,
            "dimensions": dimensions,
            "metrics": measures,
            "segments": segments,
        }
        if table is not None:
            model_kwargs["table"] = table
        if sql is not None:
            model_kwargs["sql"] = sql
        desc = view_def.get("description")
        if desc is not None:
            model_kwargs["description"] = desc
        if extends is not None:
            model_kwargs["extends"] = extends
        if primary_key != "id":
            model_kwargs["primary_key"] = primary_key
        if model_meta:
            model_kwargs["meta"] = model_meta

        return Model(**model_kwargs)

    def _parse_dimension(self, dim_def: dict, dimension_sql_lookup: dict[str, str] | None = None) -> Dimension | None:
        """Parse LookML dimension.

        Args:
            dim_def: Dimension definition
            dimension_sql_lookup: Optional dict of dimension names to resolved SQL

        Returns:
            Dimension instance or None
        """
        name = dim_def.get("name")
        if not name:
            return None

        dim_type = dim_def.get("type", "string")

        # Map LookML types to Sidemantic types
        type_mapping = {
            "string": "categorical",
            "number": "numeric",
            "yesno": "categorical",
            "tier": "categorical",
        }

        sidemantic_type = type_mapping.get(dim_type, "categorical")

        # Get SQL from the resolved lookup if available, otherwise parse directly
        if dimension_sql_lookup and name in dimension_sql_lookup:
            sql = dimension_sql_lookup[name]
        else:
            sql = dim_def.get("sql")
            if sql:
                sql = sql.replace("${TABLE}", "{model}")

        # Build meta dict from LookML-specific display properties
        meta = {}
        if dim_def.get("hidden") in ("yes", True):
            meta["hidden"] = True
        if dim_def.get("group_label"):
            meta["group_label"] = dim_def["group_label"]
        if dim_def.get("tags"):
            meta["tags"] = dim_def["tags"]
        if dim_def.get("order_by_field"):
            meta["order_by_field"] = dim_def["order_by_field"]
        if dim_def.get("can_filter") in ("no", False):
            meta["can_filter"] = False

        return Dimension(
            name=name,
            type=sidemantic_type,
            sql=sql,
            description=dim_def.get("description"),
            label=dim_def.get("label"),
            value_format_name=dim_def.get("value_format_name"),
            format=dim_def.get("value_format"),
            meta=meta or None,
        )

    def _parse_dimension_group(
        self, dim_group_def: dict, dimension_sql_lookup: dict[str, str] | None = None
    ) -> list[Dimension]:
        """Parse LookML dimension_group (time dimensions).

        Args:
            dim_group_def: Dimension group definition
            dimension_sql_lookup: Optional dict of dimension names to resolved SQL

        Returns:
            List of time dimensions with different granularities
        """
        group_name = dim_group_def.get("name")
        if not group_name:
            return []

        group_type = dim_group_def.get("type", "time")

        # Handle duration type separately
        if group_type == "duration":
            return self._parse_duration_group(group_name, dim_group_def)

        if group_type != "time":
            return []

        timeframes = dim_group_def.get("timeframes", ["date"])

        # Get SQL from the resolved lookup if available
        first_timeframe_name = f"{group_name}_{timeframes[0]}" if timeframes else None
        if dimension_sql_lookup and first_timeframe_name and first_timeframe_name in dimension_sql_lookup:
            base_sql = dimension_sql_lookup[first_timeframe_name]
        else:
            base_sql = dim_group_def.get("sql")
            if base_sql:
                base_sql = base_sql.replace("${TABLE}", "{model}")

        # Create a dimension for each timeframe
        dimensions = []
        for timeframe in timeframes:
            if timeframe == "raw":
                continue  # Skip raw timeframe

            dim = self._build_timeframe_dimension(group_name, timeframe, base_sql, dim_group_def)
            if dim is not None:
                dimensions.append(dim)

        return dimensions

    # Timeframes that truncate a timestamp to a coarser time grain. These keep
    # type="time" with a Sidemantic granularity so they behave as time dimensions.
    _TIME_GRANULARITY_TIMEFRAMES = {
        "time": "hour",
        "time_of_day": "hour",
        "hour": "hour",
        "minute": "minute",
        "minute15": "minute",
        "minute30": "minute",
        "second": "second",
        "millisecond": "second",
        "microsecond": "second",
        "date": "day",
        "week": "week",
        "month": "month",
        "quarter": "quarter",
        "year": "year",
        # NOTE: fiscal_quarter / fiscal_year are intentionally NOT mapped here.
        # A plain calendar truncation ignores fiscal_month_offset and buckets
        # non-calendar fiscal years incorrectly, so they are handled as offset
        # aware truncations in _timeframe_part_sql instead.
    }

    # SQL aggregate wrapper for a base measure type, used by post-SQL measures
    # (percent_of_total / percent_of_previous) to aggregate the referenced base
    # measure before applying the window calculation. Each entry is a format
    # template with a single ``{0}`` placeholder for the column reference, so
    # count_distinct (which needs ``COUNT(DISTINCT col)``) is expressed correctly
    # rather than being silently dropped from the lookup.
    _SQL_AGG_FUNC = {
        "sum": "SUM({0})",
        "count": "COUNT({0})",
        "count_distinct": "COUNT(DISTINCT {0})",
        "average": "AVG({0})",
        "min": "MIN({0})",
        "max": "MAX({0})",
        "median": "MEDIAN({0})",
    }

    def _build_timeframe_dimension(
        self, group_name: str, timeframe: str, base_sql: str | None, dim_group_def: dict
    ) -> Dimension | None:
        """Build a single dimension for one dimension_group timeframe.

        Handles both time-truncation timeframes (``date``, ``week``, ``month`` ...)
        which become ``type=time`` dimensions, and non-standard "extracted part"
        timeframes (``day_of_week``, ``month_name``, ``month_num``, ``fiscal_quarter`` ...)
        which become numeric or categorical dimensions with an extraction SQL
        expression derived from the base timestamp.

        Args:
            group_name: Name of the dimension_group.
            timeframe: A single LookML timeframe.
            base_sql: The base timestamp SQL ({model}-substituted, refs resolved).
            dim_group_def: The dimension_group definition (for label/description).

        Returns:
            A Dimension, or None if the timeframe is unrecognized and unusable.
        """
        name = f"{group_name}_{timeframe}"
        label = dim_group_def.get("label")
        description = dim_group_def.get("description")

        # Time-truncation timeframes -> time dimension with granularity.
        granularity = self._TIME_GRANULARITY_TIMEFRAMES.get(timeframe)
        if granularity is not None:
            return Dimension(
                name=name,
                type="time",
                sql=base_sql,
                granularity=granularity,
                label=label,
                description=description,
            )

        # Fiscal quarter/year truncations honoring fiscal_month_offset. The base
        # timestamp is shifted back by the offset so the generator's calendar
        # DATE_TRUNC at the matching grain buckets dates into the correct fiscal
        # periods (each distinct fiscal quarter/year maps to a distinct value),
        # instead of ignoring the offset and grouping by calendar boundaries.
        if timeframe in ("fiscal_quarter", "fiscal_year"):
            fiscal_offset = dim_group_def.get("fiscal_month_offset")
            shifted_sql, grain = self._fiscal_shifted_sql(timeframe, base_sql, fiscal_offset)
            return Dimension(
                name=name,
                type="time",
                sql=shifted_sql,
                granularity=grain,
                label=label,
                description=description,
            )

        # Non-standard / fiscal "extracted part" timeframes. These return a
        # number or a string, not a truncated timestamp, so we emit a
        # numeric/categorical dimension with an EXTRACT/strftime-style SQL.
        fiscal_offset = dim_group_def.get("fiscal_month_offset")
        sql, dim_type = self._timeframe_part_sql(timeframe, base_sql, fiscal_offset)
        if sql is None:
            return None
        return Dimension(
            name=name,
            type=dim_type,
            sql=sql,
            label=label,
            description=description,
        )

    @staticmethod
    def _fiscal_shifted_sql(timeframe: str, base_sql: str | None, fiscal_offset=None) -> tuple[str, str]:
        """Build offset-shifted SQL + calendar grain for a fiscal timeframe.

        ``fiscal_month_offset`` is the number of months the fiscal year starts
        after January (e.g. an April fiscal-year start is offset 3). The base
        timestamp is shifted back by the offset so that a subsequent calendar
        DATE_TRUNC at the returned grain (applied by the SQL generator) lands on
        fiscal-period boundaries. Offset 0 leaves the timestamp unchanged.

        Returns ``(sql, grain)`` where grain is ``quarter`` or ``year``.
        """
        expr = base_sql if base_sql is not None else "{model}"
        grain = "quarter" if timeframe == "fiscal_quarter" else "year"
        try:
            offset = int(fiscal_offset) if fiscal_offset is not None else 0
        except (TypeError, ValueError):
            offset = 0
        if offset == 0:
            return expr, grain
        return f"(({expr}) - INTERVAL ({offset}) MONTH)", grain

    @staticmethod
    def _timeframe_part_sql(timeframe: str, base_sql: str | None, fiscal_offset=None):
        """Map a non-truncation LookML timeframe to (sql_expression, dimension_type).

        Uses portable, DuckDB-compatible date functions. ``base_sql`` is the base
        timestamp expression. Returns (None, type) if the timeframe is unknown.
        """
        expr = base_sql if base_sql is not None else "{model}"

        # Numeric extracted parts (integers).
        numeric_parts = {
            "hour_of_day": f"EXTRACT(HOUR FROM {expr})",
            "day_of_month": f"EXTRACT(DAY FROM {expr})",
            "day_of_year": f"EXTRACT(DOY FROM {expr})",
            # LookML day_of_week_index: Monday=0 .. Sunday=6
            "day_of_week_index": f"(EXTRACT(ISODOW FROM {expr}) - 1)",
            "month_num": f"EXTRACT(MONTH FROM {expr})",
            "week_of_year": f"EXTRACT(WEEK FROM {expr})",
            "quarter_of_year": f"EXTRACT(QUARTER FROM {expr})",
        }
        if timeframe in numeric_parts:
            return numeric_parts[timeframe], "numeric"

        # String/categorical extracted parts.
        if timeframe == "day_of_week":
            return f"STRFTIME({expr}, '%A')", "categorical"
        if timeframe == "month_name":
            return f"STRFTIME({expr}, '%B')", "categorical"

        # Fiscal "month number" honoring fiscal_month_offset (months the fiscal
        # year starts after the calendar year). Default offset 0 == calendar.
        try:
            offset = int(fiscal_offset) if fiscal_offset is not None else 0
        except (TypeError, ValueError):
            offset = 0
        if timeframe == "fiscal_month_num":
            return f"(((EXTRACT(MONTH FROM {expr}) - 1 - {offset}) % 12) + 1)", "numeric"
        if timeframe == "fiscal_quarter_of_year":
            return f"(FLOOR(((EXTRACT(MONTH FROM {expr}) - 1 - {offset}) % 12) / 3) + 1)", "numeric"

        return None, "categorical"

    def _convert_explore_source_to_sql(self, derived_table: dict) -> str:
        """Convert a native derived table (explore_source) to a SQL representation.

        Native derived tables in LookML use explore_source to define the query
        declaratively. We convert this to a SQL comment documenting the source,
        since the actual SQL is generated by Looker at runtime.

        Args:
            derived_table: The derived_table definition containing explore_source

        Returns:
            A SQL comment describing the explore_source
        """
        explore_source = derived_table.get("explore_source")
        if not explore_source:
            return "-- Native derived table (explore_source)"

        # explore_source can be a string (explore name) or a dict with config
        if isinstance(explore_source, str):
            explore_name = explore_source
            columns = []
            filters = []
        else:
            # It's a dict with explore name as key
            # lkml parses it as: {"explore_name": {...config...}}
            if isinstance(explore_source, dict):
                explore_name = list(explore_source.keys())[0] if explore_source else "unknown"
                config = explore_source.get(explore_name, {})
                if isinstance(config, dict):
                    columns = config.get("columns") or config.get("column") or []
                    filters = config.get("filters") or config.get("filter") or []
                else:
                    columns = []
                    filters = []
            else:
                explore_name = str(explore_source)
                columns = []
                filters = []

        # Build a descriptive SQL comment
        sql_parts = [f"-- Native Derived Table from explore: {explore_name}"]

        if columns:
            col_names = []
            for col in columns if isinstance(columns, list) else [columns]:
                if isinstance(col, dict):
                    col_name = col.get("name") or col.get("column")
                    if col_name:
                        col_names.append(col_name)
            if col_names:
                sql_parts.append(f"-- Columns: {', '.join(col_names)}")

        if filters:
            sql_parts.append("-- Has filters applied")

        sql_parts.append(f"SELECT * FROM {explore_name}")

        return "\n".join(sql_parts)

    def _parse_duration_group(self, group_name: str, dim_group_def: dict) -> list[Dimension]:
        """Parse LookML dimension_group with type: duration.

        Duration dimension groups calculate the difference between two timestamps
        in various intervals (seconds, minutes, hours, days, weeks, months, years).

        Args:
            group_name: Name of the dimension group
            dim_group_def: Dimension group definition

        Returns:
            List of duration dimensions
        """
        intervals = dim_group_def.get("intervals", ["day"])
        sql_start = dim_group_def.get("sql_start", "")
        sql_end = dim_group_def.get("sql_end", "")

        if sql_start:
            sql_start = sql_start.replace("${TABLE}", "{model}")
        if sql_end:
            sql_end = sql_end.replace("${TABLE}", "{model}")

        # If no sql_start/sql_end, we can't create duration dimensions
        if not sql_start or not sql_end:
            return []

        dimensions = []
        for interval in intervals:
            # Create a dimension for each interval
            # The SQL calculates the difference between start and end
            # Note: The exact SQL depends on the database dialect
            dim_name = f"{group_name}_{interval}s" if interval != "second" else f"{group_name}_seconds"

            # Generate appropriate SQL for duration calculation
            # This uses a generic DATE_DIFF pattern that works in most SQL dialects
            duration_sql = f"DATE_DIFF({sql_end}, {sql_start}, {interval.upper()})"

            dimensions.append(
                Dimension(
                    name=dim_name,
                    type="numeric",
                    sql=duration_sql,
                    description=f"Duration in {interval}s between start and end",
                )
            )

        return dimensions

    def _parse_measure(
        self,
        measure_def: dict,
        dimension_names: set[str] | None = None,
        dimension_sql_lookup: dict[str, str] | None = None,
        measure_names: set[str] | None = None,
        measure_agg_lookup: dict[str, str] | None = None,
    ) -> Metric | None:
        """Parse LookML measure.

        Args:
            measure_def: Metric definition
            dimension_names: Set of dimension names in this view (for reference resolution)
            dimension_sql_lookup: Dict mapping dimension names to their resolved SQL
            measure_names: Set of measure names in this view (for base-measure resolution)
            measure_agg_lookup: Dict mapping base measure names to their SQL aggregate template

        Returns:
            Metric instance or None
        """
        name = measure_def.get("name")
        if not name:
            return None

        dimension_names = dimension_names or set()
        dimension_sql_lookup = dimension_sql_lookup or {}

        # Check if type is explicitly set
        has_explicit_type = "type" in measure_def
        measure_type = measure_def.get("type", "count")

        # Handle period_over_period type (time comparisons)
        if measure_type == "period_over_period":
            based_on = measure_def.get("based_on")
            period = measure_def.get("period", "year")
            kind = measure_def.get("kind", "relative_change")

            # Map period to comparison_type
            period_mapping = {
                "year": "yoy",
                "month": "mom",
                "week": "wow",
                "day": "dod",
                "quarter": "qoq",
            }
            comparison_type = period_mapping.get(period, "yoy")

            # Map kind to calculation
            kind_mapping = {
                "difference": "difference",
                "relative_change": "percent_change",
                "ratio": "ratio",
            }
            calculation = kind_mapping.get(kind, "percent_change")

            return Metric(
                name=name,
                type="time_comparison",
                base_metric=based_on,
                comparison_type=comparison_type,
                calculation=calculation,
                description=measure_def.get("description"),
            )

        # Handle percentile type with proper SQL generation
        if measure_type == "percentile":
            sql = measure_def.get("sql")
            if not sql:
                return None  # Skip placeholder percentile measures without SQL
            sql = sql.replace("${TABLE}", "{model}")
            sql = self._resolve_dimension_references(sql, dimension_sql_lookup or {})
            percentile_value = measure_def.get("percentile", 50)
            fraction = float(percentile_value) / 100.0
            percentile_sql = f"PERCENTILE_CONT({fraction}) WITHIN GROUP (ORDER BY {sql})"
            meta = {}
            if measure_def.get("hidden") in ("yes", True):
                meta["hidden"] = True
            return Metric(
                name=name,
                type="derived",
                sql=percentile_sql,
                description=measure_def.get("description"),
                label=measure_def.get("label"),
                value_format_name=measure_def.get("value_format_name"),
                format=measure_def.get("value_format"),
                meta=meta or None,
            )

        # Handle list type with STRING_AGG
        if measure_type == "list":
            sql = measure_def.get("sql")
            if sql:
                sql = sql.replace("${TABLE}", "{model}")
                sql = self._resolve_dimension_references(sql, dimension_sql_lookup or {})
                list_sql = f"STRING_AGG(DISTINCT {sql}, ', ')"
                meta = {}
                if measure_def.get("hidden") in ("yes", True):
                    meta["hidden"] = True
                return Metric(
                    name=name,
                    type="derived",
                    sql=list_sql,
                    description=measure_def.get("description"),
                    label=measure_def.get("label"),
                    value_format_name=measure_def.get("value_format_name"),
                    format=measure_def.get("value_format"),
                    meta=meta or None,
                )
            # No SQL for list measure - skip it (placeholder)
            return None

        # Handle distinct aggregate measure types. These dedup repeated values
        # (e.g. caused by join fanout) using sql_distinct_key when present.
        # Looker: sum_distinct, average_distinct, median_distinct, percentile_distinct.
        if measure_type in ("sum_distinct", "average_distinct", "median_distinct", "percentile_distinct"):
            return self._parse_distinct_measure(name, measure_type, measure_def, dimension_sql_lookup)

        # Handle post-SQL / table-calculation measure types. These reference
        # another numeric measure and compute a column-wise calculation.
        # Looker: running_total, percent_of_total, percent_of_previous.
        if measure_type in ("running_total", "percent_of_total", "percent_of_previous"):
            return self._parse_post_sql_measure(
                name,
                measure_type,
                measure_def,
                dimension_sql_lookup,
                measure_names or set(),
                measure_agg_lookup or {},
            )

        # Map LookML measure types to sidemantic aggregation types
        # Only include types supported by Metric.agg: sum, count, count_distinct, avg, min, max, median
        type_mapping = {
            "count": "count",
            "count_distinct": "count_distinct",
            "sum": "sum",
            "average": "avg",
            "min": "min",
            "max": "max",
            "median": "median",
            # Treated as derived:
            "date": None,
            "number": None,  # Calculated/derived measures
            "string": None,  # String measures are derived
            "yesno": None,  # Boolean measures are derived
        }

        agg_type = type_mapping.get(measure_type)

        # Parse filters - lkml parses these as filters__all
        # There are TWO different filter syntaxes in LookML:
        # 1. Shorthand: filters: [status: "completed"]
        #    -> lkml returns [[{'status': 'completed'}]]
        # 2. Block syntax: filters: { field: x value: y }
        #    -> lkml returns [{'field': 'flight_length', 'value': '>120'}]
        # We need to handle both formats.
        filters = []
        filters_all = measure_def.get("filters__all") or []
        if filters_all:
            for item in filters_all:
                if isinstance(item, list):
                    # Format 1: Shorthand syntax - list of dicts with field:value pairs
                    for filter_dict in item:
                        if isinstance(filter_dict, dict):
                            for field, value in filter_dict.items():
                                filter_sql = self._convert_lookml_filter_to_sql(field, value)
                                if filter_sql:
                                    filters.append(filter_sql)
                elif isinstance(item, dict):
                    # Format 2: Block syntax - dict with 'field' and 'value' keys
                    field = item.get("field")
                    value = item.get("value")
                    if field and value:
                        filter_sql = self._convert_lookml_filter_to_sql(field, value)
                        if filter_sql:
                            filters.append(filter_sql)

        # Replace ${TABLE} and resolve ${dimension_ref} placeholders in SQL
        sql = measure_def.get("sql")
        if sql:
            sql = sql.replace("${TABLE}", "{model}")

            if measure_type == "number":
                # For derived measures (type: number), convert ${measure_name} references
                # to plain measure_name for sidemantic's dependency resolution.
                # We need to distinguish measure references from dimension references:
                # - ${measure_name} where measure_name is NOT a dimension -> plain measure_name
                # - ${dimension_name} -> resolved SQL from dimension
                def resolve_reference(match):
                    ref_name = match.group(1)
                    if ref_name in dimension_sql_lookup:
                        # It's a dimension reference - use the resolved SQL
                        return f"({dimension_sql_lookup[ref_name]})"
                    else:
                        # It's a measure reference - use plain measure_name
                        # The dependency analyzer will resolve this
                        return ref_name

                sql = re.sub(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}", resolve_reference, sql)
            else:
                # For regular aggregation measures (sum, avg, count_distinct, etc.),
                # resolve dimension references to their SQL expressions
                sql = self._resolve_dimension_references(sql, dimension_sql_lookup)

        # Determine if this is a derived/ratio metric
        metric_type = None
        if measure_type == "number":
            # type: number is a derived measure, but it requires SQL
            # If no SQL, this is likely a placeholder in an abstract/template view
            if sql:
                metric_type = "derived"
            else:
                # Skip placeholder measures with no SQL
                return None
        # If there's SQL but no explicit type, treat as derived measure
        elif sql and not has_explicit_type:
            metric_type = "derived"
            agg_type = None  # No aggregation type for derived measures

        # Build meta dict from LookML-specific display properties
        meta = {}
        if measure_def.get("hidden") in ("yes", True):
            meta["hidden"] = True
        if measure_def.get("group_label"):
            meta["group_label"] = measure_def["group_label"]
        if measure_def.get("tags"):
            meta["tags"] = measure_def["tags"]

        return Metric(
            name=name,
            type=metric_type,
            agg=agg_type,
            sql=sql,
            filters=filters if filters else None,
            description=measure_def.get("description"),
            label=measure_def.get("label"),
            value_format_name=measure_def.get("value_format_name"),
            format=measure_def.get("value_format"),
            drill_fields=measure_def.get("drill_fields"),
            meta=meta or None,
        )

    def _measure_meta(self, measure_def: dict, extra: dict | None = None) -> dict | None:
        """Build the common measure meta dict (hidden/group_label/tags) plus extras."""
        meta: dict = {}
        if measure_def.get("hidden") in ("yes", True):
            meta["hidden"] = True
        if measure_def.get("group_label"):
            meta["group_label"] = measure_def["group_label"]
        if measure_def.get("tags"):
            meta["tags"] = measure_def["tags"]
        if extra:
            meta.update(extra)
        return meta or None

    def _parse_distinct_measure(
        self,
        name: str,
        measure_type: str,
        measure_def: dict,
        dimension_sql_lookup: dict[str, str],
    ) -> Metric | None:
        """Parse a distinct aggregate measure (sum/average/median/percentile_distinct).

        These deduplicate the aggregated field across the unique entities defined
        by ``sql_distinct_key`` (used to avoid double counting when joins fan out).
        We emit a derived measure with an explicit DISTINCT aggregation. When a
        ``sql_distinct_key`` is provided it is preserved in ``meta`` so the exact
        de-duplication entity is not lost.

        Args:
            name: Measure name.
            measure_type: One of sum_distinct/average_distinct/median_distinct/percentile_distinct.
            measure_def: Raw measure definition.
            dimension_sql_lookup: Resolved dimension SQL for ${ref} resolution.

        Returns:
            A derived Metric, or None if required SQL is missing.
        """
        sql = measure_def.get("sql")
        if not sql:
            # No field to aggregate -> placeholder in an abstract view, skip.
            return None
        sql = sql.replace("${TABLE}", "{model}")
        sql = self._resolve_dimension_references(sql, dimension_sql_lookup)

        sql_distinct_key = measure_def.get("sql_distinct_key")
        if sql_distinct_key:
            sql_distinct_key = sql_distinct_key.replace("${TABLE}", "{model}")
            sql_distinct_key = self._resolve_dimension_references(sql_distinct_key, dimension_sql_lookup)

        # With a sql_distinct_key, Looker dedupes by the *key entity*, not by the
        # aggregated value: two distinct orders that both have amount 10 must
        # contribute 20, not collapse to 10. `SUM(DISTINCT value)` deduplicates
        # by value and corrupts exactly that case, so sum/average distinct keyed
        # measures use a symmetric aggregate (HASH(key)-based) which is the
        # fan-out-safe form for keyed deduplication.
        if sql_distinct_key and measure_type in ("sum_distinct", "average_distinct"):
            agg_sql = self._keyed_distinct_aggregate_sql(measure_type, sql, sql_distinct_key)
        elif sql_distinct_key and measure_type in ("median_distinct", "percentile_distinct"):
            # Ordered-set aggregates (median / percentile) are skewed by fan-out:
            # a value repeated across joined rows is counted once per row, so the
            # plain ordered-set form computes the quantile over the duplicated
            # distribution rather than one value per distinct key. There is no
            # fan-out-safe ordered-set form via WITHIN GROUP (an ORDER BY DISTINCT
            # is rejected by SQLGlot and standard SQL). Instead collapse to one
            # value per distinct key first, then take the quantile of that list.
            if measure_type == "median_distinct":
                fraction = 0.5
            else:
                fraction = float(measure_def.get("percentile", 50)) / 100.0
            agg_sql = self._keyed_distinct_quantile_sql(sql, sql_distinct_key, fraction)
        elif measure_type == "sum_distinct":
            agg_sql = f"SUM(DISTINCT {sql})"
        elif measure_type == "average_distinct":
            agg_sql = f"AVG(DISTINCT {sql})"
        elif measure_type == "median_distinct":
            # No key: dedupe by value (the same row-collapsing the database does).
            agg_sql = f"MEDIAN(DISTINCT {sql})"
        else:  # percentile_distinct, no key
            percentile_value = measure_def.get("percentile", 50)
            fraction = float(percentile_value) / 100.0
            # `ORDER BY DISTINCT ...` inside PERCENTILE_CONT is rejected by SQLGlot
            # and standard SQL, so the imported metric would fail to parse before
            # reaching the database, making the measure type unusable. Emit the
            # standard parseable ordered-set form (the same one used for the plain
            # `percentile` measure type above), which the generator compiles and
            # runs. Without a key the only available de-duplication is by value,
            # which is what the database's PERCENTILE_CONT already does.
            agg_sql = f"PERCENTILE_CONT({fraction}) WITHIN GROUP (ORDER BY {sql})"

        extra = {"distinct": True}
        if sql_distinct_key:
            extra["sql_distinct_key"] = sql_distinct_key

        return Metric(
            name=name,
            type="derived",
            sql=agg_sql,
            description=measure_def.get("description"),
            label=measure_def.get("label"),
            value_format_name=measure_def.get("value_format_name"),
            format=measure_def.get("value_format"),
            meta=self._measure_meta(measure_def, extra),
        )

    @staticmethod
    def _keyed_distinct_aggregate_sql(measure_type: str, value_sql: str, key_sql: str) -> str:
        """Build a fan-out-safe sum/avg over values deduplicated by a key entity.

        Implements LookML ``sum_distinct`` / ``average_distinct`` with a
        ``sql_distinct_key`` using a symmetric aggregate: each distinct key
        contributes its value exactly once even when joins fan rows out. The
        bounded HASH(key) offset is cast to DECIMAL alongside the value so the
        per-key value stays exact, and the bound keeps the summed offsets within
        DECIMAL(38, 6) range so the aggregate does not overflow at realistic key
        cardinalities. ``{model}`` placeholders are preserved for the SQL
        generator.
        """
        # Per-key offset, cast to DECIMAL so summing alongside the value stays
        # exact; the offset cancels out in the subtraction, leaving the per-key
        # value summed once. HASH is bounded by `% (1 << 61)` so each offset stays
        # below ~2.3e18: summing many of them (thousands of distinct keys) stays
        # well within DECIMAL(38, 6) headroom and never overflows, while the 2^61
        # separation dwarfs realistic measure magnitudes so distinct keys do not
        # collide. The unbounded `HASH * (1 << 40)` form overflowed once a query
        # accumulated ~100 distinct keys.
        offset = f"(HASH({key_sql}) % (1::HUGEINT << 61))::DECIMAL(38, 6)"
        value = f"({value_sql})::DECIMAL(38, 6)"
        keyed_sum = f"(SUM(DISTINCT {offset} + {value}) - SUM(DISTINCT {offset}))"
        if measure_type == "sum_distinct":
            return keyed_sum
        # average_distinct: keyed sum divided by the number of distinct keys.
        return f"({keyed_sum} / NULLIF(COUNT(DISTINCT {key_sql}), 0))"

    @staticmethod
    def _keyed_distinct_quantile_sql(value_sql: str, key_sql: str, fraction: float) -> str:
        """Build a fan-out-safe ordered-set quantile deduplicated by a key entity.

        Implements LookML ``median_distinct`` / ``percentile_distinct`` with a
        ``sql_distinct_key``. A plain ``PERCENTILE_CONT(...) WITHIN GROUP`` over the
        fanned-out rows counts a value once per joined row, skewing the quantile.
        DuckDB forbids ``ORDER BY DISTINCT`` inside an ordered-set aggregate and
        forbids nesting an aggregate inside another aggregate, so instead collect
        the ``(key, value)`` pairs into a single ``LIST`` aggregate, drop duplicate
        keys with scalar ``list_distinct``, project the value, and take the
        continuous quantile of that per-key value list via scalar ``list_aggregate``.
        NULL values are ignored by ``quantile_cont`` (matching ordered-set
        semantics), and an empty group yields NULL. ``{model}`` placeholders are
        preserved for the SQL generator.
        """
        pairs = f"LIST(STRUCT_PACK(k := {key_sql}, v := {value_sql}))"
        per_key_values = f"LIST_TRANSFORM(LIST_DISTINCT({pairs}), x -> x.v)"
        return f"LIST_AGGREGATE({per_key_values}, 'quantile_cont', {fraction})"

    def _resolve_measure_reference_sql(
        self,
        sql: str,
        dimension_sql_lookup: dict[str, str],
        measure_names: set[str] | None = None,
        measure_agg_lookup: dict[str, str] | None = None,
    ) -> str:
        """Resolve ${ref} in a measure-referencing SQL (e.g. running_total sql).

        ${dimension} references resolve to the dimension's SQL. ${measure}
        references resolve to ``{model}.<measure>``; when ``measure_agg_lookup``
        provides the base measure's aggregate template the reference becomes
        ``<AGG>({model}.<measure>)`` (e.g. ``COUNT(DISTINCT {model}.<measure>)``
        for a count_distinct base) so the value is aggregated per group before
        the window calculation. The generator's inline-aggregate path then
        rewrites ``{model}.<measure>`` to the base measure's ``<measure>_raw``
        CTE column. A bare ``<measure>`` would reference a column the model CTE
        never exposes (only ``<measure>_raw`` exists).
        """
        measure_names = measure_names or set()
        measure_agg_lookup = measure_agg_lookup or {}
        sql = sql.replace("${TABLE}", "{model}")

        def _resolve(match: re.Match) -> str:
            ref_name = match.group(1)
            if ref_name == "TABLE":
                return match.group(0)
            if ref_name in dimension_sql_lookup:
                return f"({dimension_sql_lookup[ref_name]})"
            if ref_name in measure_names:
                agg_template = measure_agg_lookup.get(ref_name)
                if agg_template:
                    return agg_template.format(f"{{model}}.{ref_name}")
                return f"{{model}}.{ref_name}"
            return ref_name

        return re.sub(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}", _resolve, sql)

    def _parse_post_sql_measure(
        self,
        name: str,
        measure_type: str,
        measure_def: dict,
        dimension_sql_lookup: dict[str, str],
        measure_names: set[str] | None = None,
        measure_agg_lookup: dict[str, str] | None = None,
    ) -> Metric | None:
        """Parse a post-SQL / table-calculation measure.

        Looker computes running_total/percent_of_total/percent_of_previous after
        the database returns rows, over another numeric measure referenced via the
        ``sql`` parameter. We map:
          - running_total       -> cumulative metric over the base measure
          - percent_of_total    -> derived measure: base / SUM(base) OVER ()
          - percent_of_previous -> derived measure: base / LAG(base) OVER ()

        The base measure reference is aggregated with its own aggregate function
        (via ``measure_agg_lookup``) so percent_of_total / percent_of_previous
        operate on the grouped measure value rather than a raw, ungrouped column.

        Args:
            name: Measure name.
            measure_type: running_total / percent_of_total / percent_of_previous.
            measure_def: Raw measure definition.
            dimension_sql_lookup: Resolved dimension SQL for ${ref} resolution.
            measure_names: Set of base measure names for ${ref} qualification.
            measure_agg_lookup: Base measure name -> SQL aggregate template.

        Returns:
            A Metric, or None if the referenced base measure SQL is missing.
        """
        sql = measure_def.get("sql")
        if not sql:
            # Looker requires sql for these; without it there is nothing to compute.
            return None
        measure_names = measure_names or set()
        measure_agg_lookup = measure_agg_lookup or {}

        if measure_type == "running_total":
            # A running_total maps to a cumulative metric whose `sql` is the base
            # measure; sidemantic resolves that dependency by bare measure name,
            # so leave measure refs unqualified here.
            base = self._resolve_measure_reference_sql(sql, dimension_sql_lookup).strip()
            return Metric(
                name=name,
                type="cumulative",
                sql=base,
                meta=self._measure_meta(measure_def, {"table_calculation": "running_total"}),
                description=measure_def.get("description"),
                label=measure_def.get("label"),
                value_format_name=measure_def.get("value_format_name"),
                format=measure_def.get("value_format"),
            )

        # percent_of_total / percent_of_previous build window aggregates inline,
        # so qualify base measure refs with {model} (for the generator's _raw
        # column rewrite) and wrap them in the base measure's aggregate function.
        base = self._resolve_measure_reference_sql(sql, dimension_sql_lookup, measure_names, measure_agg_lookup).strip()

        common = {
            "description": measure_def.get("description"),
            "label": measure_def.get("label"),
            "value_format_name": measure_def.get("value_format_name"),
            "format": measure_def.get("value_format"),
        }

        if measure_type == "percent_of_total":
            calc_sql = f"{base} / NULLIF(SUM({base}) OVER (), 0)"
            table_calc = "percent_of_total"
        else:  # percent_of_previous
            calc_sql = f"({base} - LAG({base}) OVER ()) / NULLIF(LAG({base}) OVER (), 0)"
            table_calc = "percent_of_previous"

        return Metric(
            name=name,
            type="derived",
            sql=calc_sql,
            meta=self._measure_meta(measure_def, {"table_calculation": table_calc}),
            **common,
        )

    def _parse_explore(self, explore_def: dict, graph: SemanticGraph) -> None:
        """Parse LookML explore and add relationships to models.

        Args:
            explore_def: Explore definition from parsed LookML
            graph: Semantic graph to add relationships to
        """
        explore_name = explore_def.get("name")
        if not explore_name:
            return

        # Handle from: aliasing (explore uses a different view as its base)
        base_model_name = explore_def.get("from", explore_name)
        if base_model_name not in graph.models:
            # Fall back to explore name if from: target not found
            if explore_name not in graph.models:
                return
            base_model_name = explore_name

        base_model = graph.models[base_model_name]

        # Set description from explore if model doesn't already have one
        explore_desc = explore_def.get("description")
        if explore_desc and not base_model.description:
            base_model.description = explore_desc

        # Store explore-level display properties in model meta
        explore_meta = {}
        if explore_def.get("label"):
            explore_meta["explore_label"] = explore_def["label"]
        if explore_def.get("group_label"):
            explore_meta["explore_group_label"] = explore_def["group_label"]
        if explore_meta:
            if base_model.meta:
                base_model.meta.update(explore_meta)
            else:
                base_model.meta = explore_meta

        # Convert sql_always_where to a segment (use explore name for uniqueness)
        from sidemantic.core.segment import Segment

        sql_always_where = explore_def.get("sql_always_where")
        if sql_always_where:
            # Translate LookML ${view.field} references to {model}.field
            sql_always_where = re.sub(r"\$\{(\w+)\.(\w+)\}", r"{model}.\2", sql_always_where)
            segment_name = f"_sql_always_where_{explore_name}"
            # Skip if this exact segment already exists
            existing_names = {s.name for s in base_model.segments}
            if segment_name not in existing_names:
                base_model.segments.append(
                    Segment(
                        name=segment_name,
                        sql=sql_always_where,
                        description=f"Explore filter: {explore_name}",
                    )
                )

        # Convert always_filter to segments
        always_filter = explore_def.get("always_filter")
        if always_filter:
            existing_names = {s.name for s in base_model.segments}
            filter_items = always_filter.get("filters") or always_filter.get("filters__all") or []

            def _add_always_filter_segment(field: str, value: str) -> None:
                # Strip view qualifier (e.g. "fact_orders.created_date" -> "created_date")
                # so _convert_lookml_filter_to_sql doesn't produce {model}.view.col
                bare_field = field.rsplit(".", 1)[-1] if "." in field else field
                filter_sql = self._convert_lookml_filter_to_sql(bare_field, str(value))
                segment_name = f"_always_filter_{explore_name}_{field}"
                if filter_sql and segment_name not in existing_names:
                    base_model.segments.append(
                        Segment(
                            name=segment_name,
                            sql=filter_sql,
                            description=f"Always filter: {field}",
                        )
                    )
                    existing_names.add(segment_name)

            for item in filter_items:
                if isinstance(item, list):
                    for filter_dict in item:
                        if isinstance(filter_dict, dict):
                            for field, value in filter_dict.items():
                                _add_always_filter_segment(field, value)
                elif isinstance(item, dict):
                    field = item.get("field")
                    value = item.get("value")
                    if field and value:
                        _add_always_filter_segment(field, value)

        # Parse joins
        for join_def in explore_def.get("joins") or []:
            relationship = self._parse_join(join_def, base_model_name, explore_name)
            if relationship:
                # Add relationship to the base model
                base_model.relationships.append(relationship)

    def _parse_join(self, join_def: dict, base_model_name: str, explore_name: str | None = None) -> Relationship | None:
        """Parse a join definition into a Relationship.

        Args:
            join_def: Join definition from explore
            base_model_name: Name of the base model in the explore
            explore_name: Optional explore alias (for from: aliased explores where
                sql_on may reference the explore name instead of the view name)

        Returns:
            Relationship or None if parsing fails
        """
        join_name = join_def.get("name")
        if not join_name:
            return None

        # Handle from: aliasing on joins (join alias -> actual view)
        actual_model_name = join_def.get("from", join_name)

        # Get relationship type from LookML
        # LookML uses: one_to_one, one_to_many, many_to_one, many_to_many
        lookml_relationship = join_def.get("relationship", "many_to_one")

        # Map LookML relationship types to Sidemantic types
        relationship_mapping = {
            "many_to_one": "many_to_one",
            "one_to_one": "one_to_one",
            "one_to_many": "one_to_many",
            "many_to_many": "many_to_many",
        }

        relationship_type = relationship_mapping.get(lookml_relationship, "many_to_one")

        # Extract foreign key from sql_on if possible
        # sql_on typically looks like: ${orders.customer_id} = ${customers.id}
        foreign_key = None
        sql_on = join_def.get("sql_on", "")

        # Try to extract foreign key from sql_on
        # For many_to_one: base model has the FK -> extract from base_model
        # For one_to_many: joined model has the FK -> extract from join_name
        if sql_on:
            matches = re.findall(r"\$\{(\w+)\.(\w+)\}", sql_on)
            models_in_sql = {m for m, c in matches}

            # Build set of names that represent the base model in sql_on.
            # With from: aliasing (explore: orders { from: fact_orders }), the
            # sql_on may reference either the view name or the explore alias.
            base_aliases = {base_model_name}
            if explore_name and explore_name != base_model_name:
                base_aliases.add(explore_name)

            # Check if this is a direct relationship between base_model and join_name
            # For many_to_one: base_model must be in sql_on (it has the FK)
            # For one_to_many: join_name must be in sql_on (it has the FK)
            # If the required model isn't present, this is likely a multi-hop join
            # (e.g., orders -> regions via customers.region_id = regions.id where orders isn't present)
            # Skip these as sidemantic will compute the path through intermediate models
            if relationship_type == "many_to_one":
                if not (base_aliases & models_in_sql):
                    return None
                # Base model has the FK (e.g., orders.customer_id -> customers.id)
                for model, column in matches:
                    if model in base_aliases:
                        foreign_key = column
                        break
            elif relationship_type in ("one_to_many", "one_to_one"):
                if join_name not in models_in_sql:
                    return None
                # Joined model has the FK (e.g., customers.id <- orders.customer_id)
                for model, column in matches:
                    if model == join_name:
                        foreign_key = column
                        break

        # Capture LookML join type (left_outer, inner, full_outer, cross)
        metadata = None
        lookml_join_type = join_def.get("type")
        if lookml_join_type:
            metadata = {"join_type": lookml_join_type}

        return Relationship(
            name=actual_model_name,
            type=relationship_type,
            foreign_key=foreign_key,
            metadata=metadata,
        )

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to LookML format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output .lkml file
        """
        output_path = Path(output_path)

        # Resolve inheritance first
        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        # Convert models to views
        views = []
        for model in resolved_models.values():
            view = self._export_view(model, graph)
            views.append(view)

        data = {"views": views}

        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Use lkml to dump to LookML format
        lkml = _import_lkml()
        with open(output_path, "w") as f:
            lookml_str = lkml.dump(data)
            f.write(lookml_str)

    def _export_view(self, model: Model, graph: SemanticGraph) -> dict:
        """Export model to LookML view definition.

        Args:
            model: Model to export
            graph: Semantic graph (for context)

        Returns:
            View definition dictionary
        """
        view = {"name": model.name}

        if model.sql:
            view["derived_table"] = {"sql": model.sql}
        elif model.table:
            view["sql_table_name"] = model.table

        if model.description:
            view["description"] = model.description

        # Export dimensions
        dimensions = []
        for dim in model.dimensions:
            # Skip time dimensions with granularity - they'll be in dimension_groups
            if dim.type == "time" and dim.granularity:
                continue

            dim_def = {"name": dim.name}

            # Map Sidemantic types to LookML types
            type_mapping = {
                "categorical": "string",
                "numeric": "number",
                "boolean": "yesno",
            }
            dim_def["type"] = type_mapping.get(dim.type, "string")

            if dim.sql:
                # Replace {model} with ${TABLE}
                sql = dim.sql.replace("{model}", "${TABLE}")
                dim_def["sql"] = sql

            if dim.description:
                dim_def["description"] = dim.description

            if dim.label:
                dim_def["label"] = dim.label

            if dim.value_format_name:
                dim_def["value_format_name"] = dim.value_format_name

            if dim.format:
                dim_def["value_format"] = dim.format

            # Write meta properties back as LookML fields
            if dim.meta:
                if dim.meta.get("hidden"):
                    dim_def["hidden"] = "yes"
                if dim.meta.get("group_label"):
                    dim_def["group_label"] = dim.meta["group_label"]
                if dim.meta.get("tags"):
                    dim_def["tags"] = dim.meta["tags"]
                if dim.meta.get("order_by_field"):
                    dim_def["order_by_field"] = dim.meta["order_by_field"]

            # Check if primary key
            if dim.name == model.primary_key:
                dim_def["primary_key"] = "yes"

            dimensions.append(dim_def)

        if dimensions:
            view["dimensions"] = dimensions

        # Export dimension_groups (time dimensions)
        # Group time dimensions by base name
        time_dims = [d for d in model.dimensions if d.type == "time" and d.granularity]
        if time_dims:
            # Group by base name and collect all timeframes
            from collections import defaultdict

            base_name_groups = defaultdict(list)

            for dim in time_dims:
                # Extract base name (remove _date, _week, etc suffix)
                base_name = dim.name
                for suffix in ["_date", "_week", "_month", "_quarter", "_year", "_time", "_hour"]:
                    if dim.name.endswith(suffix):
                        base_name = dim.name[: -len(suffix)]
                        break
                base_name_groups[base_name].append(dim)

            dimension_groups = []
            for base_name, dims in base_name_groups.items():
                # Map granularity to timeframe
                granularity_mapping = {
                    "hour": "time",
                    "day": "date",
                    "week": "week",
                    "month": "month",
                    "quarter": "quarter",
                    "year": "year",
                }

                # Collect all timeframes for this base name
                timeframes = []
                sql = None
                for dim in dims:
                    timeframe = granularity_mapping.get(dim.granularity, "date")
                    timeframes.append(timeframe)
                    if dim.sql and not sql:
                        sql = dim.sql

                dim_group_def = {
                    "name": base_name,
                    "type": "time",
                    "timeframes": timeframes,
                }

                if sql:
                    sql = sql.replace("{model}", "${TABLE}")
                    dim_group_def["sql"] = sql

                dimension_groups.append(dim_group_def)

            if dimension_groups:
                view["dimension_groups"] = dimension_groups

        # Export measures
        measures = []
        for metric in model.metrics:
            measure_def = {"name": metric.name}

            # Handle different metric types
            if metric.type == "time_comparison":
                # Export as period_over_period measure
                measure_def["type"] = "period_over_period"

                # Add based_on (base metric)
                if metric.base_metric:
                    # Remove model prefix if present (e.g., "sales.revenue" -> "revenue")
                    based_on = metric.base_metric
                    if "." in based_on:
                        based_on = based_on.split(".")[-1]
                    measure_def["based_on"] = based_on

                # Map comparison_type to period
                if metric.comparison_type:
                    period_mapping = {
                        "yoy": "year",
                        "mom": "month",
                        "wow": "week",
                        "dod": "day",
                        "qoq": "quarter",
                    }
                    period = period_mapping.get(metric.comparison_type, "year")
                    measure_def["period"] = period

                # Map calculation to kind
                if metric.calculation:
                    kind_mapping = {
                        "difference": "difference",
                        "percent_change": "relative_change",
                        "ratio": "ratio",
                    }
                    kind = kind_mapping.get(metric.calculation, "relative_change")
                    measure_def["kind"] = kind

                if metric.description:
                    measure_def["description"] = metric.description

            elif metric.type == "derived":
                measure_def["type"] = "number"
                if metric.sql:
                    sql = metric.sql.replace("{model}", "${TABLE}")
                    measure_def["sql"] = sql
            elif metric.type == "ratio":
                measure_def["type"] = "number"
                if metric.numerator and metric.denominator:
                    measure_def["sql"] = f"1.0 * ${{{metric.numerator}}} / NULLIF(${{{metric.denominator}}}, 0)"
            else:
                # Regular aggregation measure
                type_mapping = {
                    "count": "count",
                    "count_distinct": "count_distinct",
                    "sum": "sum",
                    "avg": "average",
                    "min": "min",
                    "max": "max",
                }
                measure_def["type"] = type_mapping.get(metric.agg, "count")

                if metric.sql:
                    sql = metric.sql.replace("{model}", "${TABLE}")
                    measure_def["sql"] = sql

            # Add filters (skip for time_comparison as they don't use filters)
            if metric.filters and metric.type != "time_comparison":
                filters_all = []
                for filter_str in metric.filters:
                    # Parse SQL-format filters back to LookML format
                    # Input: "{model}.field = 'value'" or "{model}.field = true"
                    # Output: filters__all format for lkml
                    sql_filter = filter_str.replace("{model}.", "")

                    # Parse "field = 'value'" or "field = value" format
                    match = re.match(r"(\w+)\s*=\s*(.+)", sql_filter)
                    if match:
                        field = match.group(1)
                        value = match.group(2).strip()
                        # Remove quotes from value
                        if value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        # Convert boolean to yes/no
                        if value.lower() == "true":
                            value = "yes"
                        elif value.lower() == "false":
                            value = "no"
                        filters_all.append([{field: value}])
                    else:
                        # Fallback: keep as-is in case of complex filters
                        # Try to parse as "field: value" format (legacy)
                        if ":" in filter_str:
                            field, value = filter_str.split(":", 1)
                            field = field.strip()
                            value = value.strip().strip('"')
                            filters_all.append([{field: value}])

                if filters_all:
                    measure_def["filters__all"] = filters_all

            if metric.description and metric.type != "time_comparison":
                measure_def["description"] = metric.description

            if metric.label:
                measure_def["label"] = metric.label

            if metric.value_format_name:
                measure_def["value_format_name"] = metric.value_format_name

            if metric.format:
                measure_def["value_format"] = metric.format

            if metric.drill_fields:
                measure_def["drill_fields"] = metric.drill_fields

            # Write meta properties back as LookML fields
            if metric.meta:
                if metric.meta.get("hidden"):
                    measure_def["hidden"] = "yes"
                if metric.meta.get("group_label"):
                    measure_def["group_label"] = metric.meta["group_label"]
                if metric.meta.get("tags"):
                    measure_def["tags"] = metric.meta["tags"]

            measures.append(measure_def)

        if measures:
            view["measures"] = measures

        # Export segments as view-level filters
        if model.segments:
            filters = []
            for segment in model.segments:
                filter_def = {"name": segment.name}
                if segment.sql:
                    sql = segment.sql.replace("{model}", "${TABLE}")
                    filter_def["sql"] = sql
                if segment.description:
                    filter_def["description"] = segment.description
                filters.append(filter_def)
            view["filters"] = filters

        return view
