"""LookML adapter for importing Looker semantic models."""

import re
from pathlib import Path

import lkml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph


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

        # First pass: parse all views
        for lkml_file in lkml_files:
            self._parse_views_from_file(lkml_file, graph)

        # Second pass: parse explores and add relationships
        for lkml_file in lkml_files:
            self._parse_explores_from_file(lkml_file, graph)

        # Rebuild adjacency graph now that relationships have been added
        graph.build_adjacency()

        return graph

    def _parse_views_from_file(self, file_path: Path, graph: SemanticGraph) -> None:
        """Parse views from a single LookML file.

        Args:
            file_path: Path to .lkml file
            graph: Semantic graph to add models to
        """
        with open(file_path) as f:
            content = f.read()

        parsed = lkml.load(content)

        if not parsed:
            return

        # Parse views
        for view_def in parsed.get("views") or []:
            model = self._parse_view(view_def)
            if model:
                # For refinements (+view_name), skip if already exists
                # In real Looker, multiple refinements would be merged
                if model.name.startswith("+") and model.name in graph.models:
                    continue
                graph.add_model(model)

    def _parse_explores_from_file(self, file_path: Path, graph: SemanticGraph) -> None:
        """Parse explores from a single LookML file and add relationships.

        Args:
            file_path: Path to .lkml file
            graph: Semantic graph to add relationships to
        """
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

        # Parse measures with dimension SQL lookup for reference resolution
        measures = []
        for measure_def in view_def.get("measures") or []:
            measure = self._parse_measure(measure_def, dimension_names, resolved_dimension_sql)
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

        return Model(
            name=name,
            table=table,
            sql=sql,
            description=view_def.get("description"),
            primary_key=primary_key,
            dimensions=dimensions,
            metrics=measures,
            segments=segments,
        )

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

        return Dimension(
            name=name,
            type=sidemantic_type,
            sql=sql,
            description=dim_def.get("description"),
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

            # Map LookML timeframe to granularity
            granularity_mapping = {
                "time": "hour",
                "date": "day",
                "week": "week",
                "month": "month",
                "quarter": "quarter",
                "year": "year",
            }

            granularity = granularity_mapping.get(timeframe, "day")

            dimensions.append(
                Dimension(
                    name=f"{group_name}_{timeframe}",
                    type="time",
                    sql=base_sql,
                    granularity=granularity,
                )
            )

        return dimensions

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
    ) -> Metric | None:
        """Parse LookML measure.

        Args:
            measure_def: Metric definition
            dimension_names: Set of dimension names in this view (for reference resolution)
            dimension_sql_lookup: Dict mapping dimension names to their resolved SQL

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

        # Map LookML measure types to sidemantic aggregation types
        # Only include types supported by Metric.agg: sum, count, count_distinct, avg, min, max, median
        # Unsupported types (percentile, list, date, string, yesno) become derived measures
        type_mapping = {
            "count": "count",
            "count_distinct": "count_distinct",
            "sum": "sum",
            "average": "avg",
            "min": "min",
            "max": "max",
            "median": "median",
            # Unsupported as agg, will be treated as derived:
            "percentile": None,
            "list": None,
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

        return Metric(
            name=name,
            type=metric_type,
            agg=agg_type,
            sql=sql,
            filters=filters if filters else None,
            description=measure_def.get("description"),
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

        # Check if base model exists
        if explore_name not in graph.models:
            return

        base_model = graph.models[explore_name]

        # Parse joins
        for join_def in explore_def.get("joins") or []:
            relationship = self._parse_join(join_def, explore_name)
            if relationship:
                # Add relationship to the base model
                base_model.relationships.append(relationship)

    def _parse_join(self, join_def: dict, base_model_name: str) -> Relationship | None:
        """Parse a join definition into a Relationship.

        Args:
            join_def: Join definition from explore
            base_model_name: Name of the base model in the explore

        Returns:
            Relationship or None if parsing fails
        """
        join_name = join_def.get("name")
        if not join_name:
            return None

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

            # Check if this is a direct relationship between base_model and join_name
            # For many_to_one: base_model must be in sql_on (it has the FK)
            # For one_to_many: join_name must be in sql_on (it has the FK)
            # If the required model isn't present, this is likely a multi-hop join
            # (e.g., orders -> regions via customers.region_id = regions.id where orders isn't present)
            # Skip these as sidemantic will compute the path through intermediate models
            if relationship_type == "many_to_one":
                if base_model_name not in models_in_sql:
                    return None
                # Base model has the FK (e.g., orders.customer_id -> customers.id)
                for model, column in matches:
                    if model == base_model_name:
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

        return Relationship(
            name=join_name,
            type=relationship_type,
            foreign_key=foreign_key,
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

        if model.table:
            view["sql_table_name"] = model.table
        elif model.sql:
            view["derived_table"] = {"sql": model.sql}

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
