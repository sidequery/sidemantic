"""Rill adapter for importing and exporting Rill metrics view YAML files.

Rill separates data loading (Model YAML) from semantic definitions (Metrics View YAML).
This adapter focuses on the Metrics View YAML which defines dimensions and measures.
"""

import re
from pathlib import Path
from typing import Any

import yaml

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph


class RillAdapter:
    """Adapter for Rill metrics view YAML format."""

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse Rill metrics view YAML file(s) into a SemanticGraph.

        Args:
            source: Path to a Rill metrics view YAML file or directory

        Returns:
            SemanticGraph containing the parsed models

        Raises:
            FileNotFoundError: If the source path does not exist
        """
        source_path = Path(source)

        # Check if path exists first - fail loudly on configuration errors
        if not source_path.exists():
            raise FileNotFoundError(f"Path does not exist: {source_path}")

        graph = SemanticGraph()
        if source_path.is_file():
            model = self._parse_file(source_path)
            if model:
                graph.add_model(model)
        else:
            for yaml_file in source_path.glob("**/*.yaml"):
                model = self._parse_file(yaml_file)
                if model:
                    graph.add_model(model)
            for yml_file in source_path.glob("**/*.yml"):
                model = self._parse_file(yml_file)
                if model:
                    graph.add_model(model)

        # Resolve parent/derived metrics views now that every metrics view in the
        # project has been parsed. A derived view inherits its fields (and data
        # source) from its parent, so the selected parent dimensions/measures need
        # to be materialized on the derived model for it to be queryable.
        self._resolve_parents(graph)

        return graph

    def _resolve_parents(self, graph: SemanticGraph) -> None:
        """Resolve parent/derived metrics views against their parsed parents.

        Rill `parent` views inherit fields from a parent metrics view via
        `parent_dimensions` / `parent_measures` selectors (or inherit everything
        when no selectors are given). When the parent metrics view is available in
        the same project, copy the selected dimensions/measures onto the derived
        model and adopt the parent's table so that fields like
        ``derived_view.revenue`` resolve for CLI ``info`` / ``query``.

        The parent linkage stays in metadata; if the parent is not present in the
        parsed graph the derived model is left as-is (still valid via the parent
        name fallback table).
        """
        for model in graph.models.values():
            meta = model.meta or {}
            parent_name = meta.get("rill_parent")
            if not parent_name:
                continue

            parent = graph.models.get(parent_name)
            if parent is None:
                continue

            # Inherit the parent's data source so the derived view points at a real
            # relation rather than just the parent metrics-view name.
            if parent.table:
                model.table = parent.table

            existing_dims = {d.name for d in model.dimensions}
            existing_metrics = {m.name for m in model.metrics}

            dim_selected = self._field_selector(meta.get("rill_parent_dimensions"))
            for dim in parent.dimensions:
                if not dim_selected(dim.name):
                    continue
                if dim.name in existing_dims:
                    continue
                model.dimensions.append(dim.model_copy(deep=True))
                existing_dims.add(dim.name)

            measure_selected = self._field_selector(meta.get("rill_parent_measures"))
            parent_metric_names = {m.name for m in parent.metrics}
            for metric in parent.metrics:
                if not measure_selected(metric.name):
                    continue
                if metric.name in existing_metrics:
                    continue
                model.metrics.append(metric.model_copy(deep=True))
                existing_metrics.add(metric.name)

            # A selected derived/ratio measure may reference other parent measures
            # that were not selected (e.g. `aov = revenue / orders`). Pull those
            # dependencies in as hidden (public=False) measures so the formula
            # resolves on the child without advertising the extra fields.
            for metric in list(model.metrics):
                for dep in metric.get_dependencies(graph, model_context=parent_name):
                    dep_name = dep.split(".")[-1]
                    if dep_name in existing_metrics or dep_name not in parent_metric_names:
                        continue
                    parent_metric = parent.get_metric(dep_name)
                    if parent_metric is None:
                        continue
                    hidden = parent_metric.model_copy(deep=True)
                    hidden.public = False
                    model.metrics.append(hidden)
                    existing_metrics.add(dep_name)

            # Inherit the parent's default time series/grain when the derived view
            # does not define its own, mirroring Rill (derived views inherit the
            # parent's timeseries). Without this, metric-only CLI queries lose the
            # parent's time dimension. Ensure the referenced time dimension is also
            # present on the model.
            if not model.default_time_dimension and parent.default_time_dimension:
                model.default_time_dimension = parent.default_time_dimension
                # Honor a child-only smallest_time_grain override before falling
                # back to the parent's grain.
                grain_override = meta.get("rill_smallest_time_grain")
                child_grain = self._map_time_grain(grain_override) if grain_override else None
                model.default_grain = model.default_grain or child_grain or parent.default_grain
                if parent.default_time_dimension not in existing_dims:
                    parent_time_dim = parent.get_dimension(parent.default_time_dimension)
                    if parent_time_dim is not None:
                        model.dimensions.append(parent_time_dim.model_copy(deep=True))
                        existing_dims.add(parent_time_dim.name)

    @staticmethod
    def _field_selector(selector: Any):
        """Build a predicate deciding whether a parent field name is inherited.

        Normalizes Rill's `parent_dimensions` / `parent_measures` selector forms:
        - ``None`` (omitted) or ``"*"`` -> select all fields
        - a list/tuple/set of names -> select those names
        - a mapping with ``exclude`` (list) -> select all except those names
        - a mapping with ``regex`` (pattern) -> select names matching the pattern
        - a mapping with ``expr`` (e.g. ``"* EXCLUDE (city)"``) -> evaluate the
          DuckDB-style star expression

        Unrecognized forms fall back to select-all so fields are never silently
        dropped (Rill would still expose them).
        """
        if selector is None or selector == "*":
            return lambda _name: True

        if isinstance(selector, str):
            return RillAdapter._parse_expr_selector(selector)

        if isinstance(selector, (list, tuple, set)):
            names = set(selector)
            return lambda name: name in names

        if isinstance(selector, dict):
            if "exclude" in selector:
                excluded = set(selector.get("exclude") or [])
                return lambda name: name not in excluded
            if "regex" in selector:
                import re

                pattern = re.compile(selector["regex"])
                return lambda name: pattern.search(name) is not None
            if "expr" in selector:
                return RillAdapter._parse_expr_selector(selector["expr"])

        # Unknown selector form: inherit everything rather than dropping fields.
        return lambda _name: True

    @staticmethod
    def _parse_expr_selector(expr: Any):
        """Build a predicate for a DuckDB-style star selector expression.

        Supports the common Rill/DuckDB forms ``"*"`` and
        ``"* EXCLUDE (a, b)"`` (case-insensitive). Anything else is treated as
        select-all so fields are not silently dropped.
        """
        if not isinstance(expr, str):
            return lambda _name: True

        import re

        text = expr.strip()
        match = re.match(r"^\*\s*EXCLUDE\s*\((?P<names>[^)]*)\)\s*$", text, re.IGNORECASE)
        if match:
            excluded = {n.strip().strip('"').strip("'") for n in match.group("names").split(",") if n.strip()}
            return lambda name: name not in excluded

        # Bare "*" (or unrecognized expression): inherit everything.
        return lambda _name: True

    def _parse_file(self, file_path: Path) -> Model | None:
        """Parse a single Rill YAML file.

        Args:
            file_path: Path to the YAML file

        Returns:
            Model if the file is a metrics_view, None otherwise
        """
        with open(file_path) as f:
            data = yaml.safe_load(f)

        if not data or data.get("type") != "metrics_view":
            return None

        model_name = data.get("name") or file_path.stem
        description = data.get("description")

        # Parent / derived metrics views: a metrics view can inherit from a parent
        # (parent + parent_dimensions/parent_measures). The parent's fields aren't
        # available here (separate file), so we record the linkage in metadata and
        # parse any explicit selectors so the model still validates and round-trips.
        parent = data.get("parent")

        # Rill rejects a derived view (parent: ...) that defines its own
        # dimensions/measures: a derived view may only *select* inherited parent
        # fields via parent_dimensions/parent_measures. Parsing child-defined
        # fields here would let the importer accept a project that `rill validate`
        # rejects and expose non-existent fields against the parent table, so fail
        # loudly to match Rill's own validation.
        if parent:
            child_fields = [key for key in ("dimensions", "measures") if data.get(key)]
            if child_fields:
                raise ValueError(
                    f"Rill metrics view '{model_name}' sets parent: '{parent}' but also defines its own "
                    f"{' and '.join(child_fields)}. A derived view may only select inherited parent fields "
                    f"via parent_dimensions/parent_measures."
                )

        # Get the source table or model. A derived view has no own data source;
        # in Rill it inherits the parent metrics view's underlying relation, so we
        # fall back to the parent name as the table. This keeps the imported model
        # a valid, queryable representation (otherwise validation rejects a model
        # with no table/sql/dax/source_uri), while meta preserves the linkage.
        table = data.get("table") or data.get("model") or parent

        # Parse dimensions
        dimensions: list[Dimension] = []
        timeseries_column = data.get("timeseries")
        smallest_time_grain = data.get("smallest_time_grain")

        # An unnamed expression dimension whose SQL is the timeseries column gets
        # renamed to the timeseries name so the default time dimension resolves.
        # That rename must not collide with a dimension that *already* owns the
        # timeseries name through Rill's own derivation (an explicit `name`, or a
        # `column`/key matching the timeseries column -- which can appear at any
        # position), nor with an earlier expression dimension that already claimed
        # it. Otherwise two dimensions share a name and validate_model rejects the
        # duplicates. Pre-scan the natural names so the special-case only fires
        # when nothing else claims the name, and gate repeats with a running flag.
        dim_defs = list(data.get("dimensions") or [])
        # A pure unnamed-expression dimension derives to `dimension_<i>`, so it is
        # never a "natural" owner here -- only explicit names and column/key
        # fallbacks that equal the timeseries column count as already claiming it.
        timeseries_name_taken = bool(timeseries_column) and any(
            self._natural_dimension_name(d, i) == timeseries_column for i, d in enumerate(dim_defs)
        )
        for i, dim_def in enumerate(dim_defs):
            dimension = self._parse_dimension(dim_def, i, timeseries_column, smallest_time_grain, timeseries_name_taken)
            if dimension:
                dimensions.append(dimension)
                if timeseries_column and dimension.name == timeseries_column:
                    timeseries_name_taken = True

        # If timeseries is specified but not found in dimensions, create it
        if timeseries_column:
            has_timeseries = any(d.sql == timeseries_column or d.name == timeseries_column for d in dimensions)
            if not has_timeseries:
                time_dim = Dimension(
                    name=timeseries_column,
                    sql=timeseries_column,
                    type="time",
                    granularity=self._map_time_grain(smallest_time_grain),
                )
                dimensions.append(time_dim)

        # Parse measures
        metrics: list[Metric] = []
        for i, measure_def in enumerate(data.get("measures") or []):
            metric = self._parse_measure(measure_def, i)
            if metric:
                metrics.append(metric)

        # Set default_time_dimension from timeseries
        default_time_dimension = None
        default_grain = None
        if timeseries_column:
            default_time_dimension = timeseries_column
            default_grain = self._map_time_grain(smallest_time_grain)

        # Preserve parent/derived-view linkage and selectors in metadata so the
        # relationship survives import (Rill resolves the parent at a separate layer).
        meta: dict[str, Any] | None = None
        if parent:
            meta = {"rill_parent": parent}
            if data.get("parent_dimensions") is not None:
                meta["rill_parent_dimensions"] = data.get("parent_dimensions")
            if data.get("parent_measures") is not None:
                meta["rill_parent_measures"] = data.get("parent_measures")
            # A derived view can override the grain without redefining the
            # timeseries. Record that override so parent resolution keeps the
            # child's coarser/finer grain instead of inheriting the parent's.
            if smallest_time_grain and not timeseries_column:
                meta["rill_smallest_time_grain"] = smallest_time_grain

        security = self._parse_security(data.get("security"))

        return Model(
            name=model_name,
            description=description,
            table=table,
            dimensions=dimensions,
            metrics=metrics,
            default_time_dimension=default_time_dimension,
            default_grain=default_grain,
            meta=meta,
            security=security,
        )

    @staticmethod
    def _translate_user_refs(template: str) -> str:
        """Rewrite Rill/Go-template ``{{ .user.x }}`` refs to sidemantic's Jinja ``{{ user.x }}``."""
        # Rill exposes attributes as `.user.email`; sidemantic's namespace is `user.email`.
        return re.sub(r"\.user\.", "user.", template)

    def _parse_security(self, security_def: Any):
        """Map a Rill metrics-view ``security:`` block to a SecurityPolicy (access + row filter).

        Rill uses Go templates over ``.user.*``; we translate those to sidemantic's ``user.*``
        Jinja namespace. Only the mechanical subset (``access`` and ``row_filter``) is mapped.
        """
        if not isinstance(security_def, dict):
            return None
        from sidemantic.core.security import SecurityPolicy

        access = security_def.get("access")
        row_filter = security_def.get("row_filter")
        kwargs: dict[str, Any] = {}
        if isinstance(access, str) and access.strip():
            kwargs["access"] = self._translate_user_refs(access)
        elif isinstance(access, bool):
            kwargs["access"] = access
        if isinstance(row_filter, str) and row_filter.strip():
            kwargs["row_filters"] = [self._translate_user_refs(row_filter)]
        if not kwargs:
            return None
        return SecurityPolicy(**kwargs)

    @staticmethod
    def _natural_dimension_name(dim_def: dict[str, Any], index: int) -> str | None:
        """Derive the name Rill itself assigns, ignoring the timeseries special-case.

        Mirrors Rill's `name -> column -> dimension_<i>` derivation
        (`property:` is the deprecated alias for `column:`). Returns ``None`` for
        dimensions Rill drops entirely (`ignore: true`, or no resolvable SQL).
        Used to detect when another dimension already owns the timeseries name so
        an unnamed expression dimension does not collide by also claiming it.
        """
        if dim_def.get("ignore"):
            return None

        column = dim_def.get("column") or dim_def.get("property")
        lookup_key_column = dim_def.get("lookup_key_column")
        # Match `_parse_dimension`'s drop condition: no expression/column/key -> None.
        if not (dim_def.get("expression") or column or lookup_key_column):
            return None

        name = dim_def.get("name")
        if name:
            return name
        return column or lookup_key_column or f"dimension_{index}"

    def _parse_dimension(
        self,
        dim_def: dict[str, Any],
        index: int,
        timeseries_column: str | None,
        smallest_time_grain: str | None,
        timeseries_name_taken: bool = False,
    ) -> Dimension | None:
        """Parse a Rill dimension into a Sidemantic Dimension.

        Mirrors Rill's own backwards-compatibility handling
        (runtime/parser/parse_metrics_view.go):
        - `property:` is a deprecated shorthand alias for `column:`.
        - When `name` is missing, derive it from `column`, otherwise fall back
          to `dimension_<i>` (matching Rill's `fmt.Sprintf("dimension_%d", i)`).
        - `label:` is a deprecated alias for `display_name:`.
        - `lookup_table` dimensions resolve their value via a lookup table; we
          keep the keyed column as the SQL expression and record lookup config in
          metadata so the dimension is preserved rather than dropped.

        Args:
            dim_def: Dimension definition from Rill YAML
            index: Position of the dimension in the dimensions list (for name fallback)
            timeseries_column: Name of the timeseries column
            smallest_time_grain: Smallest time grain for time dimensions
            timeseries_name_taken: Whether an earlier unnamed expression dimension
                already claimed the timeseries name (so a repeated match keeps its
                positional `dimension_<i>` name instead of colliding).

        Returns:
            Dimension or None if parsing fails
        """
        # Rill ignores dimensions explicitly marked with `ignore: true`.
        if dim_def.get("ignore"):
            return None

        # `property:` is a deprecated shorthand alias for `column:`.
        column = dim_def.get("column")
        if not column and dim_def.get("property"):
            column = dim_def.get("property")

        expression = dim_def.get("expression")

        # Lookup dimensions resolve a value from a lookup table keyed off a column.
        lookup_table = dim_def.get("lookup_table")
        lookup_key_column = dim_def.get("lookup_key_column")

        # Derive name following Rill's rules: name -> column -> dimension_<i>.
        # When an unnamed dimension's SQL expression *is* the timeseries column
        # (e.g. `dimensions: [{expression: order_date}]` alongside
        # `timeseries: order_date`), name it after the timeseries column rather
        # than the positional `dimension_<i>` fallback. Otherwise the generated
        # time dimension stays addressable only as `dimension_<i>`, the later
        # auto-create check sees the column already present and skips it, and
        # `default_time_dimension` (set to the timeseries column) resolves to no
        # dimension -- causing validate_model to reject the model.
        #
        # Only the *first* such match may claim the timeseries name. Rill keeps
        # repeated unnamed expression dimensions distinct as `dimension_<i>`, so
        # once the name is taken later matches fall back to their positional name
        # to avoid colliding (validate_model rejects duplicate dimension names).
        name = dim_def.get("name")
        if not name:
            sql_expr = expression or column or lookup_key_column
            if timeseries_column and sql_expr == timeseries_column and not timeseries_name_taken:
                name = timeseries_column
            else:
                name = column or lookup_key_column or f"dimension_{index}"

        # `label` is the deprecated alias for `display_name`.
        label = dim_def.get("display_name") or dim_def.get("label")
        description = dim_def.get("description")

        # SQL expression: prefer expression, then column, then the lookup key column.
        sql = expression or column or lookup_key_column

        if not sql:
            return None

        # Determine if this is the timeseries dimension
        is_timeseries = timeseries_column and (sql == timeseries_column or name == timeseries_column)

        meta = None
        if lookup_table:
            meta = {
                "rill_lookup_table": lookup_table,
                "rill_lookup_key_column": lookup_key_column,
                "rill_lookup_value_column": dim_def.get("lookup_value_column"),
            }
            if dim_def.get("lookup_default_expression") is not None:
                meta["rill_lookup_default_expression"] = dim_def.get("lookup_default_expression")

        return Dimension(
            name=name,
            label=label,
            description=description,
            sql=sql,
            type="time" if is_timeseries else "categorical",
            granularity=self._map_time_grain(smallest_time_grain) if is_timeseries else None,
            meta=meta,
        )

    def _parse_measure(self, measure_def: dict[str, Any], index: int) -> Metric | None:
        """Parse a Rill measure into a Sidemantic Metric.

        Mirrors Rill's own backwards-compatibility handling
        (runtime/parser/parse_metrics_view.go):
        - When `name` is missing, fall back to `measure_<i>` (matching Rill's
          `fmt.Sprintf("measure_%d", i)`).
        - `label:` is a deprecated alias for `display_name:`.
        - `type:` accepts simple / derived / time_comparison. An empty type with
          `requires:` or `per:` is treated as derived (Rill's default promotion)
          UNLESS the expression is itself a plain aggregation (e.g. `SUM(amount)`),
          which keeps simple aggregate parsing so it still decomposes correctly.

        Args:
            measure_def: Measure definition from Rill YAML
            index: Position of the measure in the measures list (for name fallback)

        Returns:
            Metric or None if parsing fails
        """
        # Rill ignores measures explicitly marked with `ignore: true`.
        if measure_def.get("ignore"):
            return None

        expression = measure_def.get("expression")
        if not expression:
            return None

        # Derive name following Rill's rule: name -> measure_<i>.
        name = measure_def.get("name") or f"measure_{index}"

        # `label` is the deprecated alias for `display_name`.
        label = measure_def.get("display_name") or measure_def.get("label")
        description = measure_def.get("description")
        measure_type = (measure_def.get("type") or "").lower()
        requires = measure_def.get("requires")
        per = measure_def.get("per")

        # Parse formatting - prefer format_d3 over format_preset
        format_d3 = measure_def.get("format_d3")
        format_preset = measure_def.get("format_preset")
        format_str = format_d3  # Direct d3 format string
        value_format_name = self._map_format_preset(format_preset) if format_preset and not format_d3 else None

        # Check for window function definition (Rill's rolling window syntax)
        window_def = measure_def.get("window")
        window_order = None
        window_frame = None
        metric_type = None
        base_metric = None
        comparison_type = None
        meta: dict[str, Any] | None = None

        if window_def:
            # Rill window syntax:
            # window:
            #   order: "__time"
            #   frame: RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW
            metric_type = "cumulative"
            if isinstance(window_def, dict):
                window_order = window_def.get("order")
                window_frame = window_def.get("frame")
        elif measure_type == "time_comparison":
            # Rill time_comparison measures compute a period-over-period value from a
            # base measure (the expression names that base measure). Map to
            # Sidemantic's native time_comparison so it queries as an actual
            # comparison; treating it as a derived metric would silently resolve to
            # the current-period value of the base measure instead. Rill compares
            # against the immediately prior period, so default to prior_period.
            metric_type = "time_comparison"
            base_metric = expression
            comparison_type = "prior_period"
            meta = {"rill_type": "time_comparison"}
        elif measure_type == "derived":
            # "simple" = basic aggregation (None type), "derived" = calculation
            # referencing other measures.
            metric_type = "derived"
        elif requires or per:
            # An empty type with requires/per is promoted to derived ONLY when the
            # expression is not itself a plain aggregation. A `per` (or `requires`)
            # measure like `SUM(amount)` is still an ordinary aggregation and must
            # keep simple aggregate parsing: forcing it to derived would leave the
            # raw aggregate as the outer formula while the CTE only projects the
            # decomposed column, producing invalid SQL when a source column name
            # collides with a measure name. The `per`/`requires` linkage is still
            # preserved in metadata regardless.
            if not self._is_simple_aggregate_expression(expression):
                metric_type = "derived"

        if per is not None:
            meta = meta or {}
            meta["rill_per"] = per

        # A native time_comparison carries its base measure via base_metric, not
        # sql (the expression is the referenced measure name, not an aggregation).
        metric_sql = None if metric_type == "time_comparison" else expression

        # Let the Metric class handle aggregation parsing via its model_validator.
        # This properly handles complex expressions like SUM(x) / SUM(y) and
        # COUNT(DISTINCT col) using sqlglot.
        return Metric(
            name=name,
            label=label,
            description=description,
            sql=metric_sql,  # Pass full expression, Metric will parse aggregations
            type=metric_type,
            base_metric=base_metric,
            comparison_type=comparison_type,
            format=format_str,
            value_format_name=value_format_name,
            window_order=window_order,
            window_frame=window_frame,
            meta=meta,
        )

    @staticmethod
    def _is_simple_aggregate_expression(expression: Any) -> bool:
        """Whether a measure expression is a single top-level aggregation.

        Mirrors the decomposition the Metric validator performs: a "simple"
        aggregation is one whose *top-level* node is a single aggregate function
        (e.g. ``SUM(amount)``, ``COUNT(*)``, ``COUNT(DISTINCT id)``). Formulas that
        combine multiple aggregations (e.g. ``SUM(x) / SUM(y)``) or reference other
        measures are not simple and remain derived.

        Used to decide whether a ``per`` / ``requires`` measure should keep simple
        aggregate parsing instead of being promoted to a derived formula.
        """
        if not isinstance(expression, str) or not expression.strip():
            return False

        try:
            import sqlglot
            from sqlglot import expressions as exp
            from sqlglot.errors import SqlglotError

            parsed = sqlglot.parse_one(expression, read="duckdb")
        except SqlglotError:
            return False

        # The top-level node itself must be the aggregation. A wrapping operator
        # (arithmetic, etc.) means the aggregate is nested inside a formula and the
        # expression is genuinely derived.
        if isinstance(parsed, exp.AggFunc):
            return True

        # Anonymous function nodes for engine-specific aggregates (e.g. dialect
        # aggregations sqlglot does not model as AggFunc) that the Metric validator
        # still decomposes.
        if isinstance(parsed, exp.Func) and (parsed.name or "").lower() in {
            "sum",
            "avg",
            "min",
            "max",
            "median",
            "stddev",
            "stddev_pop",
            "variance",
            "variance_pop",
            "var_pop",
            "count",
        }:
            return True

        return False

    def _map_time_grain(self, grain: str | None) -> str:
        """Map Rill time grain to Sidemantic granularity.

        Args:
            grain: Rill time grain (millisecond, second, minute, hour, day, week, month, quarter, year)

        Returns:
            Sidemantic granularity (hour, day, week, month, quarter, year)
        """
        if not grain:
            return "day"

        grain_mapping = {
            "millisecond": "hour",
            "second": "hour",
            "minute": "hour",
            "hour": "hour",
            "day": "day",
            "week": "week",
            "month": "month",
            "quarter": "quarter",
            "year": "year",
        }

        return grain_mapping.get(grain, "day")

    def _map_format_preset(self, preset: str | None) -> str | None:
        """Map Rill format_preset to Sidemantic value_format_name.

        Args:
            preset: Rill format preset (humanize, currency_usd, percentage, etc.)

        Returns:
            Sidemantic value_format_name or None
        """
        if not preset:
            return None

        preset_mapping = {
            "humanize": "decimal_0",
            "currency_usd": "usd",
            "currency_eur": "eur",
            "percentage": "percent",
            "interval_ms": "decimal_0",
        }

        return preset_mapping.get(preset, preset)

    def _map_value_format_to_preset(self, value_format: str | None) -> str | None:
        """Map Sidemantic value_format_name to Rill format_preset.

        Args:
            value_format: Sidemantic value_format_name

        Returns:
            Rill format_preset or None
        """
        if not value_format:
            return None

        format_mapping = {
            "decimal_0": "humanize",
            "decimal_2": "humanize",
            "usd": "currency_usd",
            "eur": "currency_eur",
            "percent": "percentage",
        }

        return format_mapping.get(value_format, None)

    def export(
        self,
        graph: SemanticGraph,
        output_path: str | Path,
        project_name: str | None = None,
        full_project: bool = False,
    ) -> None:
        """Export a SemanticGraph to Rill YAML files.

        By default, generates only metrics_view YAML files (one per model).
        Set full_project=True to generate a complete Rill project including:
        - rill.yaml (project config)
        - sources/*.yaml (for models with source_uri)
        - models/*.sql (passthrough SQL)
        - metrics_views/*.yaml (metrics and dimensions)

        Args:
            graph: The semantic graph to export
            output_path: Directory to write the Rill files to
            project_name: Optional project name for rill.yaml (only used with full_project=True)
            full_project: If True, generate full project structure. If False (default),
                         only generate metrics_view files directly in output_path.
        """
        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Resolve inheritance before export
        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        if full_project:
            # Generate rill.yaml
            self._export_project_config(output_dir, project_name)

            # Create subdirectories
            sources_dir = output_dir / "sources"
            models_dir = output_dir / "models"
            metrics_views_dir = output_dir / "metrics_views"

            sources_dir.mkdir(exist_ok=True)
            models_dir.mkdir(exist_ok=True)
            metrics_views_dir.mkdir(exist_ok=True)

            for model in resolved_models.values():
                # Generate source file if model has source_uri
                if model.source_uri:
                    self._export_source(model, sources_dir)

                # Generate model SQL file (passthrough)
                self._export_model_sql(model, models_dir)

                # Generate metrics_view YAML
                self._export_model(model, metrics_views_dir, graph)
        else:
            # Legacy behavior: only export metrics_views to output_path
            for model in resolved_models.values():
                self._export_model(model, output_dir, graph)

    def _export_project_config(self, output_dir: Path, project_name: str | None) -> None:
        """Export rill.yaml project configuration.

        Args:
            output_dir: Directory to write the file to
            project_name: Optional project name
        """
        config: dict[str, Any] = {}

        if project_name:
            config["name"] = project_name

        output_file = output_dir / "rill.yaml"
        with open(output_file, "w") as f:
            yaml.dump(config, f, sort_keys=False, default_flow_style=False)

    def _export_source(self, model: Model, sources_dir: Path) -> None:
        """Export a source YAML file for a model with source_uri.

        Args:
            model: The model with source_uri
            sources_dir: Directory to write source files to
        """
        if not model.source_uri:
            return

        # Determine source type from URI scheme
        uri = model.source_uri
        if uri.startswith("s3://"):
            source_type = "s3"
        elif uri.startswith("gs://"):
            source_type = "gcs"
        elif uri.startswith("http://") or uri.startswith("https://"):
            source_type = "https"
        else:
            source_type = "local"

        source_def: dict[str, Any] = {
            "type": source_type,
        }

        if source_type == "local":
            source_def["path"] = uri
        else:
            source_def["uri"] = uri

        source_name = f"{model.name}_raw"
        output_file = sources_dir / f"{source_name}.yaml"
        with open(output_file, "w") as f:
            yaml.dump(source_def, f, sort_keys=False, default_flow_style=False)

    def _export_model_sql(self, model: Model, models_dir: Path) -> None:
        """Export a SQL model file.

        If the model has custom SQL defined, use it. Otherwise generate a
        passthrough SELECT * FROM the appropriate source.

        Args:
            model: The model to export
            models_dir: Directory to write model files to
        """
        if model.sql:
            # Use the model's custom SQL
            sql = model.sql
            if not sql.endswith("\n"):
                sql += "\n"
        else:
            # Generate passthrough SQL
            # Determine the source to SELECT from
            if model.source_uri:
                # Reference the generated source
                source_name = f"{model.name}_raw"
            elif model.table:
                # Reference the table directly
                source_name = model.table
            else:
                # Default: assume a source with _raw suffix exists
                source_name = f"{model.name}_raw"

            sql = f"SELECT * FROM {source_name}\n"

        output_file = models_dir / f"{model.name}.sql"
        with open(output_file, "w") as f:
            f.write(sql)

    def _export_model(self, model: Model, output_dir: Path, graph: SemanticGraph) -> None:
        """Export a single Model to a Rill metrics view YAML file.

        Args:
            model: The model to export
            output_dir: Directory to write the file to
            graph: The full semantic graph (for context)
        """
        metrics_view: dict[str, Any] = {
            "type": "metrics_view",
        }

        # Model doesn't have display_name, so we skip it

        if model.description:
            metrics_view["description"] = model.description

        # Set the model reference
        # When source_uri is set, we generate models/{name}.sql, so reference that
        if model.source_uri:
            metrics_view["model"] = model.name
        elif model.table:
            # If it looks like a model reference (no dots/schemas), use model field
            if "." not in model.table:
                metrics_view["model"] = model.table
            else:
                metrics_view["table"] = model.table
        else:
            # Default to model name (assumes models/{name}.sql exists)
            metrics_view["model"] = model.name

        # Export dimensions
        dimensions = []
        timeseries_column = None
        smallest_time_grain = None

        for dim in model.dimensions:
            dim_def: dict[str, Any] = {
                "name": dim.name,
            }

            if dim.label:
                dim_def["display_name"] = dim.label

            if dim.description:
                dim_def["description"] = dim.description

            # Use column if SQL is simple column reference, otherwise use expression
            sql = dim.sql or dim.name  # Default to name if no SQL specified
            if sql and (sql.isidentifier() or sql.replace("_", "").isalnum()):
                dim_def["column"] = sql
            else:
                dim_def["expression"] = sql

            # Track timeseries dimension
            if dim.type == "time":
                if not timeseries_column:
                    timeseries_column = dim.sql
                    if dim.granularity:
                        smallest_time_grain = self._map_granularity_to_rill(dim.granularity)

            dimensions.append(dim_def)

        if dimensions:
            metrics_view["dimensions"] = dimensions

        if timeseries_column:
            metrics_view["timeseries"] = timeseries_column

        if smallest_time_grain:
            metrics_view["smallest_time_grain"] = smallest_time_grain

        # Export measures
        measures = []
        for metric in model.metrics:
            # Build expression from agg + sql or just sql
            if metric.agg and metric.sql:
                expression = f"{metric.agg.upper()}({metric.sql})"
            elif metric.agg:
                expression = f"{metric.agg.upper()}(*)"
            else:
                expression = metric.sql or ""

            measure_def: dict[str, Any] = {
                "name": metric.name,
                "expression": expression,
            }

            if metric.label:
                measure_def["display_name"] = metric.label

            if metric.description:
                measure_def["description"] = metric.description

            # Export formatting - prefer format (d3) over value_format_name (preset)
            if metric.format:
                measure_def["format_d3"] = metric.format
            elif metric.value_format_name:
                format_preset = self._map_value_format_to_preset(metric.value_format_name)
                if format_preset:
                    measure_def["format_preset"] = format_preset

            # Map metric type to Rill measure type
            if metric.type == "derived":
                measure_def["type"] = "derived"
            elif metric.type == "cumulative":
                # Export window function definition
                if metric.window_frame or metric.window_order:
                    window_def: dict[str, Any] = {}
                    if metric.window_order:
                        window_def["order"] = metric.window_order
                    if metric.window_frame:
                        window_def["frame"] = metric.window_frame
                    elif metric.window:
                        # Convert simple window to frame
                        window_parts = metric.window.split()
                        if len(window_parts) == 2:
                            num, unit = window_parts
                            window_def["frame"] = (
                                f"RANGE BETWEEN INTERVAL {num} {unit.upper()} PRECEDING AND CURRENT ROW"
                            )
                    measure_def["window"] = window_def
            # else: default is "simple", no need to specify

            measures.append(measure_def)

        if measures:
            metrics_view["measures"] = measures

        # Write to file
        output_file = output_dir / f"{model.name}.yaml"
        with open(output_file, "w") as f:
            yaml.dump(metrics_view, f, sort_keys=False, default_flow_style=False)

    def _map_granularity_to_rill(self, granularity: str) -> str:
        """Map Sidemantic granularity to Rill time grain.

        Args:
            granularity: Sidemantic granularity

        Returns:
            Rill time grain
        """
        # Sidemantic uses: hour, day, week, month, quarter, year
        # Rill uses: millisecond, second, minute, hour, day, week, month, quarter, year
        # Direct mapping for most values
        return granularity
