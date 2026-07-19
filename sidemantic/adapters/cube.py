"""Cube adapter for importing Cube.js semantic models."""

import os
import re
import warnings
from pathlib import Path

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.pre_aggregation import Index, PreAggregation, RefreshKey
from sidemantic.core.relationship import Relationship
from sidemantic.core.security import SecurityPolicy
from sidemantic.core.semantic_graph import SemanticGraph


class CubeImportWarning(UserWarning):
    """A Cube construct was imported but is not executed by sidemantic's query engine.

    The construct is preserved (on a field or on ``meta``) so format round-trips keep it,
    but it has no effect on generated SQL. Filter this category to silence these notices.
    """


def _warn_inert(member: str, feature: str, detail: str) -> None:
    """Warn that an imported Cube construct is preserved but has no query effect."""
    warnings.warn(
        f"{member}: {feature} is preserved for round-trip but not executed by sidemantic ({detail}).",
        CubeImportWarning,
        stacklevel=3,
    )


# Keys the adapter writes into the cube_internal namespace; on export these are consumed and
# stripped, while any other (user-supplied) keys in the namespace are preserved.
_ADAPTER_INTERNAL_KEYS = frozenset(
    {
        "cube_type",
        "order_by",
        "reduce_by",
        "rolling_window_leading",
        "rolling_window_offset",
        "latitude",
        "longitude",
        "top_level",
        "sub_query",
    }
)


def _cube_internal(meta: object) -> dict:
    """Return the adapter-internal marker namespace from a meta value, tolerating a
    user-supplied ``cube_internal`` that isn't a mapping (returns an empty dict)."""
    if isinstance(meta, dict):
        ns = meta.get("cube_internal")
        if isinstance(ns, dict):
            return ns
    return {}


def _set_cube_internal(meta: dict | None, **markers) -> dict:
    """Stash adapter-internal export markers under a dedicated ``meta["cube_internal"]``
    namespace, so they never collide with (or get mistaken for) user-supplied measure meta
    of the same name (e.g. a measure whose own ``meta`` has a ``cube_type`` key). Only
    non-None markers are stored; a non-dict existing value is replaced (not crashed on)."""
    out = dict(meta) if isinstance(meta, dict) else {}
    internal = dict(_cube_internal(out))
    internal.update({k: v for k, v in markers.items() if v is not None})
    if internal:
        out["cube_internal"] = internal
    return out


# Matches a Cube member reference: ${cube.col}, {cube.col}, ${CUBE}.col, {cube}.col, etc.
# group(1) = content inside braces, group(2) = optional trailing ".column".
_CUBE_MEMBER_RE = re.compile(r"\$?\{([^}]+)\}(?:\.(\w+))?")

# Matches a single-quoted string literal OR a Cube member reference. Used to rewrite member
# references while leaving member-looking text inside string literals untouched. The literal
# alternative is non-capturing, so the member groups remain group(1)/group(2); a matched
# literal therefore has group(1) is None.
_CUBE_MEMBER_OR_STRING_RE = re.compile(r"'(?:[^']|'')*'|" + _CUBE_MEMBER_RE.pattern)

# Matches a single-quoted string literal OR a literal ``{model}`` placeholder. Used on export
# to translate ``{model}`` back to ``${CUBE}`` only outside quoted strings, so a ``{model}``
# occurring inside a literal (e.g. ``label = '{model}'``) is preserved verbatim and survives an
# import->export round-trip (import already skips quoted literals when normalizing).
_MODEL_OR_STRING_RE = re.compile(r"'(?:[^']|'')*'|\{model\}")


def _model_placeholder_to_cube(sql: str) -> str:
    """Replace ``{model}`` with ``${CUBE}`` everywhere except inside single-quoted literals."""

    def repl(match: re.Match) -> str:
        return "${CUBE}" if match.group(0) == "{model}" else match.group(0)

    return _MODEL_OR_STRING_RE.sub(repl, sql)


def _split_cube_ref(inner: str, trailing: str | None) -> tuple[str, str | None]:
    """Split a Cube member reference into ``(cube_name, column)``.

    Handles both ``${cube.col}`` / ``{cube.col}`` (column inside the braces) and
    ``${cube}.col`` / ``${CUBE}.col`` (column trailing the braces).
    """
    if trailing is not None:
        return inner, trailing
    if "." in inner:
        head, col = inner.split(".", 1)
        return head, col
    return inner, None


def _normalize_cube_sql(sql: str | None, cube_name: str | None = None) -> str | None:
    """Normalize Cube.js self-references in a SQL expression to Sidemantic's ``{model}``.

    Rewrites references to *this* cube only:
    - ``${CUBE}`` / ``{CUBE}`` / ``${cube_name}`` / ``{cube_name}`` -> ``{model}``
    - dotted column refs ``${CUBE.col}`` / ``{CUBE.col}`` / ``${CUBE}.col`` and the
      ``cube_name`` equivalents -> ``{model}.col``

    References to *other* cubes and bare ``${measure}`` references are left untouched:
    the former cannot be expressed as ``{model}``, and the latter are resolved later in
    ``_parse_measure()`` for derived metrics.

    Args:
        sql: SQL expression string or None
        cube_name: Name of the cube whose self-references should be normalized

    Returns:
        Normalized SQL string or None
    """
    if sql is None:
        return None

    def repl(match: re.Match) -> str:
        if match.group(1) is None:
            return match.group(0)  # single-quoted string literal -> leave untouched
        head, col = _split_cube_ref(match.group(1), match.group(2))
        if head in ("CUBE", cube_name):
            return f"{{model}}.{col}" if col else "{model}"
        # Other-cube / measure / bare references are handled elsewhere.
        return match.group(0)

    return _CUBE_MEMBER_OR_STRING_RE.sub(repl, sql)


# Cube join "relationship" values -> Sidemantic relationship types. Cube accepts both the
# camelCase forms (belongsTo/hasMany/hasOne) and the snake_case forms.
_CUBE_RELATIONSHIP_MAP = {
    "belongsTo": "many_to_one",
    "hasMany": "one_to_many",
    "hasOne": "one_to_one",
    # Legacy snake_case aliases Cube also accepts.
    "belongs_to": "many_to_one",
    "has_many": "one_to_many",
    "has_one": "one_to_one",
    "many_to_one": "many_to_one",
    "one_to_many": "one_to_many",
    "one_to_one": "one_to_one",
}

# Directories pruned when scanning a Cube project root for .js files: a YAML-only adapter
# only needs to warn that JS cubes exist, so it should not descend into dependency/build trees.
_JS_SCAN_SKIP_DIRS = {"node_modules", "dist", "build", ".git", ".venv", "__pycache__"}


def _sql_literal(value: object) -> str:
    """Render a Cube filter value as a SQL literal, escaping single quotes in strings."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _cube_filter_to_sql(member: str, operator: str, values: list) -> str | None:
    """Translate one Cube row_level filter (member/operator/values) to a SQL fragment.

    Returns None for operators that have no mechanical SQL equivalent here (the caller
    records them as unmapped). Member references like ``{CUBE}.status`` are reduced to the
    bare column name; the enforcement layer scopes the fragment to the owning model's CTE.
    """
    # Strip a leading Cube self-reference (``{CUBE}.col`` / ``{cube_name}.col``) and any prefix.
    col = re.sub(r"^\{[^}]*\}\.", "", member or "")
    col = col.split(".")[-1] if "." in col else col
    op = (operator or "").strip()
    # Operators without operands still map (set/notSet); everything else needs LITERAL values.
    # Cube also allows dynamic values (e.g. ``values: security_context.auth.userAttributes.x``),
    # which arrive as a string, not a list. Those cannot be translated to static SQL -- treat the
    # filter as unmapped rather than iterating the string character by character.
    if op in ("set", "notSet"):
        vals: list = []
    elif isinstance(values, (list, tuple)):
        vals = list(values)
    else:
        return None
    if op in ("equals", "in"):
        if len(vals) == 1:
            return f"{col} = {_sql_literal(vals[0])}"
        return f"{col} IN ({', '.join(_sql_literal(v) for v in vals)})" if vals else None
    if op in ("notEquals", "notIn"):
        if len(vals) == 1:
            return f"{col} != {_sql_literal(vals[0])}"
        return f"{col} NOT IN ({', '.join(_sql_literal(v) for v in vals)})" if vals else None
    if op == "contains":
        return " OR ".join(f"{col} LIKE {_sql_literal('%' + str(v) + '%')}" for v in vals) if vals else None
    if op == "notContains":
        return " AND ".join(f"{col} NOT LIKE {_sql_literal('%' + str(v) + '%')}" for v in vals) if vals else None
    if op == "startsWith":
        return " OR ".join(f"{col} LIKE {_sql_literal(str(v) + '%')}" for v in vals) if vals else None
    if op == "endsWith":
        return " OR ".join(f"{col} LIKE {_sql_literal('%' + str(v))}" for v in vals) if vals else None
    if op in ("gt", "gte", "lt", "lte") and vals:
        sql_op = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[op]
        return f"{col} {sql_op} {_sql_literal(vals[0])}"
    if op == "set":
        return f"{col} IS NOT NULL"
    if op == "notSet":
        return f"{col} IS NULL"
    return None


def _access_policy_to_security(access_policy: object) -> tuple[SecurityPolicy | None, set[str]]:
    """Map a Cube ``access_policy`` to a SecurityPolicy, returning (policy, unmapped_constructs).

    Translates the mechanical subset -- ``row_level.filters`` with member/operator/values --
    into ANDed SQL row filters. Role/condition gating and member-level rules have no direct
    equivalent and are reported as unmapped so the caller can warn and preserve them in meta.
    """
    if not isinstance(access_policy, list):
        return None, set()
    row_filters: list[str] = []
    unmapped: set[str] = set()
    for policy in access_policy:
        if not isinstance(policy, dict):
            continue
        if policy.get("conditions"):
            unmapped.add("conditions")
        if policy.get("role") not in (None, "*"):
            unmapped.add("role")
        if policy.get("member_level"):
            unmapped.add("member_level")
        row_level = policy.get("row_level") or {}
        combine = str(row_level.get("filters_type") or "and").lower()
        filters = row_level.get("filters") or []
        fragments: list[str] = []
        for filt in filters:
            if not isinstance(filt, dict):
                continue
            if "and" in filt or "or" in filt:
                unmapped.add("nested_filters")
                continue
            sql = _cube_filter_to_sql(filt.get("member", ""), filt.get("operator", ""), filt.get("values", []))
            if sql is None:
                unmapped.add(f"operator:{filt.get('operator')}")
            else:
                fragments.append(f"({sql})" if " OR " in sql or " AND " in sql else sql)
        if not fragments:
            continue
        # Multiple filters within one policy combine per filters_type (default AND). An OR
        # group MUST be parenthesized: each policy becomes a separate row filter that the
        # generator later ANDs together, and `A OR B AND C` binds as `A OR (B AND C)`, letting
        # rows matching A bypass the other predicates. AND groups need no extra wrapping.
        if len(fragments) == 1:
            row_filters.append(fragments[0])
        elif combine == "or":
            row_filters.append("(" + " OR ".join(fragments) + ")")
        else:
            row_filters.append(" AND ".join(fragments))
    if not row_filters:
        return None, unmapped
    return SecurityPolicy(row_filters=row_filters), unmapped


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
        pending_extends: dict[str, str] = {}  # child_name -> parent_name

        if source_path.is_dir():
            # Parse all YAML files in directory
            for yaml_file in source_path.rglob("*.yml"):
                self._parse_file(yaml_file, graph, pending_views, pending_hierarchies, pending_extends)
            for yaml_file in source_path.rglob("*.yaml"):
                self._parse_file(yaml_file, graph, pending_views, pending_hierarchies, pending_extends)
            # Cube's native model format is JavaScript; this adapter parses YAML only.
            # Scan lazily, pruning dependency/build trees (a Cube project root often has a
            # large node_modules/dist) and stopping at the first match -- just enough to warn
            # without walking or materializing every .js path under the root.
            example_js = None
            for walk_root, walk_dirs, walk_files in os.walk(source_path):
                walk_dirs[:] = [d for d in walk_dirs if d not in _JS_SCAN_SKIP_DIRS]
                example_js = next((f for f in walk_files if f.endswith(".js")), None)
                if example_js is not None:
                    break
            if example_js is not None:
                warnings.warn(
                    f"Cube adapter parses YAML only; skipped JavaScript cube file(s) under "
                    f"{source_path} (e.g. {example_js}).",
                    CubeImportWarning,
                    stacklevel=2,
                )
        else:
            # Parse single file
            self._parse_file(source_path, graph, pending_views, pending_hierarchies, pending_extends)

        # Resolve extends (inheritance) after all cubes are parsed
        from sidemantic.core.inheritance import resolve_model_inheritance

        if any(m.extends for m in graph.models.values()):
            graph.models = resolve_model_inheritance(graph.models)

        # Apply hierarchies after inheritance so inherited dimensions are available.
        # Also propagate parent hierarchies to child cubes via extends_map.
        def _apply_hierarchies(model: Model, h_defs: list[dict]) -> None:
            for h_def in h_defs:
                levels = h_def.get("levels", [])
                for i in range(1, len(levels)):
                    child_name = levels[i]
                    parent_name = levels[i - 1]
                    if "." not in parent_name and "." not in child_name:
                        child_dim = model.get_dimension(child_name)
                        if child_dim and not child_dim.parent:
                            child_dim.parent = parent_name

        # Apply explicit hierarchies
        for model_name, hierarchy_defs in pending_hierarchies.items():
            model = graph.models.get(model_name)
            if model:
                _apply_hierarchies(model, hierarchy_defs)

        # Propagate parent hierarchies to child cubes that inherited dimensions
        for child_name, parent_name in pending_extends.items():
            if parent_name in pending_hierarchies:
                child_model = graph.models.get(child_name)
                if child_model:
                    _apply_hierarchies(child_model, pending_hierarchies[parent_name])

        # Parse views after all cubes are loaded and inheritance resolved.
        # Resolve view-level `extends` in dependency order so a view can inherit
        # members and folders from a parent view defined earlier or later.
        view_defs_by_name = {v["name"]: v for v in pending_views if v.get("name")}
        built_views: dict[str, Model] = {}

        def _build_view(view_name: str, _stack: tuple[str, ...] = ()) -> Model | None:
            if view_name in built_views:
                return built_views[view_name]
            if view_name in _stack:
                # Guard against circular extends chains
                return None
            view_def = view_defs_by_name.get(view_name)
            if view_def is None:
                return None
            parent_name = view_def.get("extends")
            parent_model = None
            if parent_name:
                parent_model = _build_view(parent_name, _stack + (view_name,))
            model = self._parse_view(view_def, graph, parent_model)
            if model:
                built_views[view_name] = model
            return model

        for view_def in pending_views:
            view_name = view_def.get("name")
            if not view_name:
                continue
            model = _build_view(view_name)
            if model and model.name not in graph.models:
                graph.add_model(model)

        return graph

    def _parse_file(
        self,
        file_path: Path,
        graph: SemanticGraph,
        pending_views: list[dict],
        pending_hierarchies: dict[str, list[dict]],
        pending_extends: dict[str, str],
    ) -> None:
        """Parse a single Cube YAML file.

        Args:
            file_path: Path to YAML file
            graph: Semantic graph to add models to
            pending_views: List to accumulate view definitions for deferred parsing
            pending_hierarchies: Dict to accumulate hierarchy definitions per cube for deferred application
            pending_extends: Dict to track extends relationships (child -> parent)
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
                # Track extends for hierarchy propagation
                ext = cube_def.get("extends")
                if ext:
                    pending_extends[model.name] = ext

        # Collect views for deferred parsing (need all cubes loaded first)
        for view_def in data.get("views") or []:
            pending_views.append(view_def)

        # Top-level view_groups: a UI grouping of views. Sidemantic has no
        # first-class equivalent, so preserve the raw definitions on graph
        # metadata for consumers that care about view organization.
        view_groups = data.get("view_groups")
        if view_groups:
            existing = graph.metadata.get("cube_view_groups") or []
            graph.metadata["cube_view_groups"] = existing + list(view_groups)

    def _convert_cube_join(
        self, join_sql: str, self_name: str, join_name: str
    ) -> tuple[str | None, str | None, str | None]:
        """Convert a Cube join condition into Sidemantic form.

        Cube expresses joins as a SQL equality condition referencing members with
        ``${CUBE}.col`` (this cube), ``${target.col}`` (the joined cube), and the
        single-brace ``{cube.col}`` variants. Sidemantic represents the same thing
        with ``{from}`` / ``{to}`` placeholders.

        Returns ``(native_sql, from_col, to_col)`` where:
        - ``native_sql`` is the condition rewritten with ``{from}`` / ``{to}``
          placeholders, or ``None`` if it references a cube other than this one or
          the join target (and therefore cannot be represented faithfully).
        - ``from_col`` / ``to_col`` are the single columns on each side when the
          condition is a plain ``a = b`` equality, otherwise ``None``.
        """
        refs: list[tuple[str, str | None]] = []  # (side, column) in order of appearance
        untranslatable = False

        def repl(match: re.Match) -> str:
            nonlocal untranslatable
            head, col = _split_cube_ref(match.group(1), match.group(2))
            if head in ("CUBE", self_name):
                side = "from"
            elif head == join_name:
                side = "to"
            else:
                # References a third cube: cannot be expressed with {from}/{to}.
                untranslatable = True
                return match.group(0)
            refs.append((side, col))
            return f"{{{side}}}.{col}" if col else f"{{{side}}}"

        native_sql = _CUBE_MEMBER_RE.sub(repl, join_sql)
        if untranslatable or not refs:
            return None, None, None

        # Detect a plain single-column equality: exactly two members (one per side)
        # joined by a single '=' with nothing else around them.
        residual = re.sub(r"\s+", "", _CUBE_MEMBER_RE.sub("@", join_sql))
        is_simple_equality = (
            residual == "@=@"
            and len(refs) == 2
            and {side for side, _ in refs} == {"from", "to"}
            and all(col for _, col in refs)
        )
        if is_simple_equality:
            from_col = next(col for side, col in refs if side == "from")
            to_col = next(col for side, col in refs if side == "to")
            return native_sql, from_col, to_col
        return native_sql, None, None

    def _relationship_from_join(self, join_def: dict, self_name: str) -> Relationship | None:
        """Build a Relationship from a Cube ``joins`` entry."""
        join_name = join_def.get("name")
        if not join_name:
            return None

        raw_relationship = join_def.get("relationship", "many_to_one")
        rel_type = _CUBE_RELATIONSHIP_MAP.get(raw_relationship) if isinstance(raw_relationship, str) else None
        if rel_type is None:
            warnings.warn(
                f"Unknown Cube join relationship {raw_relationship!r} on '{self_name}' -> "
                f"'{join_name}'; defaulting to 'many_to_one'.",
                stacklevel=2,
            )
            rel_type = "many_to_one"
        join_sql = join_def.get("sql", "") or ""

        native_sql, from_col, to_col = self._convert_cube_join(join_sql, self_name, join_name)

        # Plain single-column equality on many_to_one / one_to_many -> structured keys.
        # Both Sidemantic (Python) and the Rust engine agree on the join direction for
        # these, so structured keys round-trip cleanly to Cube's simple join form.
        # one_to_one is deliberately excluded: the two engines interpret its FK/PK
        # direction differently, so we preserve the explicit condition instead.
        if from_col and to_col and rel_type in ("many_to_one", "one_to_many"):
            if rel_type == "many_to_one":
                return Relationship(name=join_name, type=rel_type, foreign_key=from_col, primary_key=to_col)
            # one_to_many: FK lives on the related model, local key on this one.
            return Relationship(name=join_name, type=rel_type, foreign_key=to_col, primary_key=from_col)

        # Composite / non-equality / one_to_one condition -> preserve the full predicate.
        if native_sql:
            return Relationship(name=join_name, type=rel_type, sql=native_sql)

        # Preserve the relationship shape, but leave its key unknown. Structural validation will
        # require the user to supply the real key instead of compiling a guessed join.
        warnings.warn(
            f"Could not parse Cube join '{self_name}' -> '{join_name}' from SQL {join_sql!r}; "
            "leaving the foreign key unknown.",
            stacklevel=2,
        )
        return Relationship(name=join_name, type=rel_type)

    @staticmethod
    def _native_join_sql_to_cube(native_sql: str, join_name: str) -> str:
        """Translate a ``{from}`` / ``{to}`` join condition back to Cube placeholders.

        ``{from}.col`` -> ``${CUBE}.col`` and ``{to}.col`` -> ``${join_name.col}``.
        """
        result = re.sub(r"\{to\}\.(\w+)", rf"${{{join_name}.\1}}", native_sql)
        result = result.replace("{to}", f"${{{join_name}}}")
        result = result.replace("{from}", "${CUBE}")
        return result

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

        # Cube-level governance/visibility fields. Row-level access_policy filters map to an
        # enforced SecurityPolicy; anything we cannot translate is preserved in meta and warned.
        extra_meta = {}
        access_policy = cube_def.get("access_policy")
        security_policy, unmapped_policy = _access_policy_to_security(access_policy)
        if access_policy is not None:
            extra_meta["access_policy"] = access_policy
        if cube_def.get("public") is False or cube_def.get("shown") is False:
            extra_meta["public"] = False
        for cosmetic_key in ("data_source", "title"):
            cosmetic_val = cube_def.get(cosmetic_key)
            if cosmetic_val is not None:
                extra_meta[cosmetic_key] = cosmetic_val
        if security_policy is not None and security_policy.row_filters:
            imported = f"{len(security_policy.row_filters)} row-level filter(s) imported as an enforced SecurityPolicy"
            if unmapped_policy:
                warnings.warn(
                    f"Cube '{name}': {imported}; some access_policy constructs "
                    f"({', '.join(sorted(unmapped_policy))}) could not be translated and are preserved in meta only.",
                    CubeImportWarning,
                    stacklevel=2,
                )
        elif access_policy is not None or extra_meta.get("public") is False:
            warnings.warn(
                f"Cube '{name}' uses access_policy/public visibility controls that Sidemantic "
                f"could not translate; preserved in meta only.",
                CubeImportWarning,
                stacklevel=2,
            )
        if extra_meta:
            # Stash the preserved top-level fields inside the adapter-owned ``cube_internal``
            # namespace (under ``top_level``) so export lifts only these back to real Cube
            # keys. A user's own ``meta.cube_top_level`` is never touched -- it is just ordinary
            # metadata and round-trips verbatim. Merge with any existing adapter top_level.
            existing_top = _cube_internal(cube_meta).get("top_level")
            existing_top = existing_top if isinstance(existing_top, dict) else {}
            cube_meta = _set_cube_internal(cube_meta, top_level={**existing_top, **extra_meta})

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
            relationship = self._relationship_from_join(join_def, name)
            if relationship is not None:
                relationships.append(relationship)

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
        if security_policy is not None and (security_policy.row_filters or security_policy.access is not True):
            model_kwargs["security"] = security_policy

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
            "boolean": "boolean",
            "switch": "categorical",  # enum-like dimension with a predefined values list
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
            supported_granularities = [g.get("name") for g in custom_grans if isinstance(g, dict) and g.get("name")]

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

        # Store Cube-specific params (switch values, mask, currency) on meta so
        # they survive the import even though Sidemantic has no first-class field.
        switch_values = dim_def.get("values") if dim_type == "switch" else None
        mask = dim_def.get("mask")
        currency = dim_def.get("currency")
        if switch_values is not None or mask is not None or currency is not None:
            meta = dict(meta) if isinstance(meta, dict) else {}
            if switch_values is not None:
                meta["switch_values"] = switch_values
            if mask is not None:
                meta["mask"] = mask
            if currency is not None:
                meta["currency"] = currency

        # Cube dimension features with no first-class Sidemantic equivalent: preserve in
        # meta and warn so the loss is visible rather than silent.
        geo_lat = dim_def.get("latitude")
        geo_lon = dim_def.get("longitude")
        if dim_type == "geo" or geo_lat is not None or geo_lon is not None:
            # Store the geo marker + lat/long under the internal namespace so a user dimension
            # whose own meta uses "cube_type"/"latitude"/"longitude" is never misread on export.
            meta = _set_cube_internal(
                meta,
                cube_type="geo",
                latitude=_normalize_cube_sql(geo_lat, cube_name) if isinstance(geo_lat, str) else geo_lat,
                longitude=_normalize_cube_sql(geo_lon, cube_name) if isinstance(geo_lon, str) else geo_lon,
            )
            warnings.warn(
                f"Cube dimension '{cube_name}.{name}' is type geo; Sidemantic has no geo type, so "
                f"latitude/longitude are preserved in meta and it is imported as categorical.",
                CubeImportWarning,
                stacklevel=2,
            )

        sub_query = dim_def.get("sub_query")
        if sub_query is not None:
            # Stash under the adapter-owned namespace so export lifts it back to the top-level
            # Cube `sub_query` property (a plain meta key would round-trip to meta.sub_query and
            # silently demote a measure-as-dimension back to a plain SQL dimension).
            meta = _set_cube_internal(meta, sub_query=sub_query)
            warnings.warn(
                f"Cube dimension '{cube_name}.{name}' uses sub_query, which Sidemantic does not "
                f"support; preserved in meta but not applied.",
                CubeImportWarning,
                stacklevel=2,
            )

        if (
            custom_grans
            and isinstance(custom_grans, list)
            and any(isinstance(g, dict) and ("sql" in g or "interval" in g or "origin" in g) for g in custom_grans)
        ):
            meta = dict(meta) if isinstance(meta, dict) else {}
            meta["custom_granularities"] = custom_grans
            warnings.warn(
                f"Cube dimension '{cube_name}.{name}' defines custom granularities with sql/interval/origin; "
                f"Sidemantic keeps the names but preserves their definitions only in meta.",
                CubeImportWarning,
                stacklevel=2,
            )

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
            "count_distinct_approx": "approx_count_distinct",
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

        # Measure-level case/switch is a multi-stage construct sidemantic does not execute.
        # Preserve the raw definition on meta (for round-trip) and warn, rather than silently
        # dropping the case block (the measure otherwise keeps only its declared/default
        # aggregation, losing the conditional selection entirely).
        case_def = measure_def.get("case")
        if case_def:
            meta = dict(meta) if meta else {}
            meta["case"] = case_def
            _warn_inert(
                f"{cube_name}.{name}",
                "measure-level case/switch",
                "conditional (multi-stage) measure selection is not reproduced",
            )

        agg_type = type_mapping.get(measure_type)
        metric_type = None
        # When True, the sql holds a complete expression that must be preserved
        # verbatim (no auto-extraction of aggregations into agg=).
        sql_is_complete = False

        # Handle unknown measure types explicitly
        if agg_type is None and measure_type not in ("number",):
            if measure_type == "rank":
                # Rank measures: use count as executable fallback, store rank metadata under
                # the internal namespace so consumers (and export) can handle them specially.
                agg_type = "count"
                # Stash rank metadata under the adapter-owned namespace (so export can handle it
                # specially) and warn that this is a lossy COUNT fallback, not a real rank.
                meta = _set_cube_internal(
                    meta,
                    cube_type="rank",
                    order_by=measure_def.get("order_by"),
                    reduce_by=measure_def.get("reduce_by"),
                )
                warnings.warn(
                    f"Cube measure '{cube_name}.{name}' has type 'rank', which is not supported; "
                    f"imported as a non-rank COUNT fallback (agg='count'). The result is a plain "
                    f"count, NOT a rank, and no RANK() SQL is generated.",
                    CubeImportWarning,
                    stacklevel=2,
                )
            elif measure_type == "number_agg":
                # Custom SQL aggregate (e.g., PERCENTILE_CONT). The sql field holds the
                # complete aggregate expression, so leave agg=None and preserve the SQL
                # verbatim. Record the original Cube type under the internal namespace.
                agg_type = None
                sql_is_complete = True
                meta = _set_cube_internal(meta, cube_type="number_agg")
            elif measure_type in ("string", "time", "boolean"):
                # Non-numeric measure types (Tesseract). The sql is a complete expression
                # (often itself an aggregate, e.g. type: time with MAX({CUBE}.created_at)).
                # Preserve it verbatim with agg=None and record the original Cube type.
                agg_type = None
                sql_is_complete = True
                meta = _set_cube_internal(meta, cube_type=measure_type)
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
            # leading/offset have no first-class Sidemantic field. Preserve them in meta
            # (so measures differing only by leading/offset don't collapse) and warn that
            # they are not yet reflected in query results.
            rw_leading = rolling_window.get("leading")
            rw_offset = rolling_window.get("offset")
            if rw_leading is not None or rw_offset is not None:
                meta = _set_cube_internal(meta, rolling_window_leading=rw_leading, rolling_window_offset=rw_offset)
                warnings.warn(
                    f"Cube measure '{cube_name}.{name}' uses rolling_window leading/offset, which "
                    f"Sidemantic does not yet apply; preserved in meta but not reflected in results.",
                    CubeImportWarning,
                    stacklevel=2,
                )

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
                    # This is a SQL expression metric with inline aggregations (e.g. COUNT(*)).
                    # Don't try to replace measure references - use SQL as-is. Set agg=None and
                    # mark the expression complete: the aggregate lives in sql and cannot be
                    # re-derived from a rollup's stored columns, so the matcher must not route a
                    # query for it to a pre-aggregation (the materializer likewise skips agg=None
                    # measures, so routing would otherwise recompute it over rollup rows).
                    agg_type = None
                    sql_is_complete = True
                else:
                    # Complex derived metric referencing other measures. Re-resolve member
                    # references from the ORIGINAL sql so BOTH the ${...} and the no-dollar
                    # {...} forms -- self-cube (${CUBE.m}/{CUBE.m}/${CUBE}.m), cross-cube
                    # (${other.m}/{other.m}), and bare (${m}/{m}) -- become cube.measure
                    # dependency refs, rather than column refs that point at nothing.
                    def _resolve_member(match: re.Match) -> str:
                        inner, trailing = match.group(1), match.group(2)
                        if trailing is not None:
                            # Trailing form ${X}.col / {X}.col references a COLUMN of X's table,
                            # not a member -- keep it as a column ref so it is not mistaken for a
                            # metric dependency.
                            if not inner.isidentifier() or not trailing.isidentifier():
                                return match.group(0)
                            if inner in ("CUBE", cube_name, "model"):
                                return f"{{model}}.{trailing}"
                            # Cross-cube trailing ${other}.col references a column on the joined
                            # cube's table. Sidemantic calculated measures reference members, not
                            # raw joined columns, and leaving the ${other} placeholder in the metric
                            # SQL compiles to invalid SQL (DuckDB reads ${other} as a struct
                            # literal). Translate it to the cross-cube member form other.col -- the
                            # same result as the ${other.col} in-brace form -- so it joins and
                            # resolves to that member.
                            return f"{inner}.{trailing}"
                        # In-brace form ${X.col} / ${X} / ${col} references a MEMBER.
                        head, col = _split_cube_ref(inner, None)
                        if not head.isidentifier():
                            return match.group(0)  # not a member ref (e.g. struct/JSON literal)
                        if col is None:
                            if head in ("CUBE", cube_name, "model"):
                                return "{model}"  # bare cube self-reference -> table placeholder
                            return f"{cube_name}.{head}"  # bare {measure} -> this cube's measure
                        if not col.isidentifier():
                            return match.group(0)
                        if head in ("CUBE", cube_name):
                            return f"{cube_name}.{col}"
                        return f"{head}.{col}"  # cross-cube reference, already qualified

                    # Resolve member refs but never inside single-quoted string literals, so a
                    # literal like '{pending}' is not rewritten into a member reference.
                    measure_sql = _CUBE_MEMBER_OR_STRING_RE.sub(
                        lambda m: m.group(0) if m.group(1) is None else _resolve_member(m),
                        measure_def.get("sql") or "",
                    )

        # Preserve Cube-specific measure params that have no first-class Sidemantic
        # equivalent: mask/currency, and multi-stage group_by/add_group_by used for
        # percent-of-total style calculations.
        mask = measure_def.get("mask")
        currency = measure_def.get("currency")
        group_by = measure_def.get("group_by")
        add_group_by = measure_def.get("add_group_by")
        if any(v is not None for v in (mask, currency, group_by, add_group_by)):
            meta = dict(meta) if isinstance(meta, dict) else {}
            if mask is not None:
                meta["mask"] = mask
            if currency is not None:
                meta["currency"] = currency
            if group_by is not None:
                meta["group_by"] = group_by
            if add_group_by is not None:
                meta["add_group_by"] = add_group_by

        if group_by is not None or add_group_by is not None or measure_def.get("multi_stage"):
            _warn_inert(
                f"{cube_name}.{name}",
                "multi_stage measure (group_by/add_group_by)",
                "percent-of-total / nested-aggregate semantics are not reproduced",
            )

        return Metric(
            name=name,
            type=metric_type,
            agg=agg_type,
            sql=measure_sql,
            sql_is_complete=sql_is_complete,
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
            "originalSql": "original_sql",
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

        # Extract rollups (rollupLambda/rollupJoin constituents) - strip CUBE prefix
        rollups = []
        for rollup_ref in preagg_def.get("rollups") or []:
            if isinstance(rollup_ref, str):
                rollups.append(rollup_ref.replace("CUBE.", "").replace(f"{cube_name}.", ""))

        # Lambda union flag (Cube rollupLambda real-time union); accept snake/camel case.
        union_with_source_data = bool(
            preagg_def.get("union_with_source_data", preagg_def.get("unionWithSourceData", False))
        )

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
        # The key may be present with a null/non-dict value, so guard before .get("sql").
        build_range_start_def = preagg_def.get("build_range_start")
        build_range_start = build_range_start_def.get("sql") if isinstance(build_range_start_def, dict) else None
        build_range_end_def = preagg_def.get("build_range_end")
        build_range_end = build_range_end_def.get("sql") if isinstance(build_range_end_def, dict) else None

        # Fidelity: these pre-agg controls are preserved for round-trip but are not honored
        # by sidemantic's matcher/materializer. Warn so the semantics aren't assumed to work.
        member = f"{cube_name}.{name}"
        if preagg_type in ("rollup_join", "lambda", "original_sql"):
            _warn_inert(member, f"pre-aggregation type '{preagg_type}'", "only 'rollup' is materialized and routed")
        if partition_granularity:
            _warn_inert(member, "partition_granularity", "rollups are materialized unpartitioned")
        if refresh_key and refresh_key.sql:
            _warn_inert(member, "refresh_key.sql", "no refresh scheduler runs the staleness query")
        if indexes:
            _warn_inert(member, "indexes", "no CREATE INDEX is emitted for materialized rollups")
        if build_range_start or build_range_end:
            _warn_inert(member, "build_range_start/end", "the full source is scanned at materialization")
        if preagg_def.get("segments"):
            _warn_inert(member, "pre-aggregation segments", "segment coverage is not checked during matching")

        # original_sql pre-aggregations stage a custom query; preserve it so it is
        # materialized/exported instead of falling back to the model's SQL. Normalize Cube
        # self-references (${CUBE}/{CUBE.col}/...) to the {model} placeholder, as for every
        # other Cube SQL field, so materialization does not send raw ${CUBE} to the database.
        sql = _normalize_cube_sql(preagg_def.get("sql"), cube_name)

        return PreAggregation(
            name=name,
            type=preagg_type,
            sql=sql,
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
            rollups=rollups if rollups else None,
            union_with_source_data=union_with_source_data,
        )

    def _parse_view(self, view_def: dict, graph: SemanticGraph, parent_model: Model | None = None) -> Model | None:
        """Parse a Cube view into a composite Model.

        Views project and rename members from existing cubes via join_path,
        includes, excludes, prefix, and alias.

        Args:
            view_def: View definition dictionary
            graph: Semantic graph with already-parsed cubes
            parent_model: Already-built view model referenced via view-level
                ``extends`` (its members and folders are inherited).

        Returns:
            Model instance or None
        """
        name = view_def.get("name")
        if not name:
            return None

        dimensions = []
        metrics = []
        inherited_folders: list = []

        # View-level extends: seed members and folders from the parent view.
        if parent_model is not None:
            dimensions.extend(d.model_copy() for d in parent_model.dimensions)
            metrics.extend(m.model_copy() for m in parent_model.metrics)
            if parent_model.meta:
                inherited_folders = list(parent_model.meta.get("folders") or [])

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

            # Build alias map for renaming dependent references
            alias_map: dict[str, str] = {}

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
                        if alias != orig:
                            alias_map[orig] = alias
                        d = target.get_dimension(orig)
                        if d:
                            dims.append(d.model_copy(update={"name": alias}))
                        m = target.get_metric(orig)
                        if m:
                            mets.append(m.model_copy(update={"name": alias}))
            else:
                continue

            # Apply alias map to dependent references (parent, drill_fields)
            if alias_map:
                dims = [
                    d.model_copy(update={"parent": alias_map[d.parent]}) if d.parent and d.parent in alias_map else d
                    for d in dims
                ]
                mets = [
                    m.model_copy(update={"drill_fields": [alias_map.get(f, f) for f in m.drill_fields]})
                    if m.drill_fields and any(f in alias_map for f in m.drill_fields)
                    else m
                    for m in mets
                ]

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

        # Deduplicate by name, keeping the last definition so a child view's
        # own members override inherited (extended) ones.
        def _dedupe(items):
            by_name: dict[str, object] = {}
            for item in items:
                by_name[item.name] = item
            return list(by_name.values())

        dimensions = _dedupe(dimensions)
        metrics = _dedupe(metrics)

        # Only create a model if the view resolved at least some members
        if not dimensions and not metrics:
            return None

        meta: dict = {"cube_type": "view"}

        # Folders: nested member grouping for UI organization. Merge the view's
        # own folders onto any inherited from the extended parent.
        own_folders = view_def.get("folders") or []
        folders = inherited_folders + list(own_folders)
        if folders:
            meta["folders"] = folders

        # default_filters: enforced query filters; preserve raw definitions.
        default_filters = view_def.get("default_filters")
        if default_filters:
            meta["default_filters"] = default_filters
            _warn_inert(
                view_def.get("name", "view"),
                "view default_filters",
                "enforced view filters are stored as metadata only and not applied to queries",
            )

        # meta.default_ui_filters: pre-populated (editable) workbench filters.
        view_meta = view_def.get("meta")
        if view_meta:
            default_ui_filters = view_meta.get("default_ui_filters")
            if default_ui_filters is not None:
                meta["default_ui_filters"] = default_ui_filters

        # View-level access_policy (RBAC) has no first-class equivalent; preserve + warn.
        access_policy = view_def.get("access_policy")
        if access_policy is not None:
            meta["access_policy"] = access_policy
            warnings.warn(
                f"Cube view '{name}' defines access_policy (RBAC), which Sidemantic does not "
                f"enforce; preserved in meta only.",
                CubeImportWarning,
                stacklevel=2,
            )
        view_title = view_def.get("title")
        if view_title is not None:
            meta["title"] = view_title

        return Model(
            name=name,
            description=view_def.get("description"),
            dimensions=dimensions,
            metrics=metrics,
            meta=meta,
        )

    @staticmethod
    def _model_sql_to_cube(sql: str | None) -> str | None:
        """Translate Sidemantic's ``{model}`` placeholder back to Cube's ``${CUBE}``."""
        if sql is None:
            return None
        return _model_placeholder_to_cube(sql)

    def _geo_ref_to_cube(self, ref: object) -> object:
        """Translate a preserved geo latitude/longitude ref back to Cube form.

        Handles both the string form and Cube's ``{sql: ...}`` object form, converting any
        ``{model}`` placeholder back to ``${CUBE}``.
        """
        if isinstance(ref, str):
            return self._model_sql_to_cube(ref)
        if isinstance(ref, dict) and "sql" in ref:
            return {**ref, "sql": self._model_sql_to_cube(ref["sql"])}
        return ref

    @staticmethod
    def _exported_meta(meta: object) -> dict:
        """Return a meta dict for export with the adapter-owned cube_internal markers removed
        but any user-supplied keys in that namespace preserved, so an import->export round-trip
        does not drop a user's own ``cube_internal`` metadata."""
        if not isinstance(meta, dict):
            return {}
        out = dict(meta)
        internal = out.get("cube_internal")
        if isinstance(internal, dict):
            user_internal = {k: v for k, v in internal.items() if k not in _ADAPTER_INTERNAL_KEYS}
            if user_internal:
                out["cube_internal"] = user_internal
            else:
                out.pop("cube_internal", None)
        return out

    @staticmethod
    def _derived_sql_to_cube(sql: str | None, known_members: set[str]) -> str | None:
        """Translate a derived-measure SQL back to Cube form.

        Converts ``{model}`` placeholders to ``${CUBE}`` and re-wraps qualified references
        that are *known semantic members* (``cube.measure`` present in ``known_members``)
        into Cube ``${...}`` refs, so cross-cube references round-trip. Arbitrary dotted SQL
        -- non-member ``table.column``, schema-qualified functions, numeric literals, and
        member-looking text inside single-quoted string literals -- is left untouched so it
        is not misread by Cube as a member reference.
        """
        if sql is None:
            return None
        out = _model_placeholder_to_cube(sql)

        # Match a single-quoted string literal OR a qualified member token. Literals are
        # returned verbatim (group(1) is None), so 'orders.total' inside a string is never
        # rewritten; only real member tokens outside quotes are wrapped.
        pattern = re.compile(r"'(?:[^']|'')*'|(?<![\w.${])([A-Za-z_]\w*\.[A-Za-z_]\w*)\b")

        def _wrap(match: re.Match) -> str:
            token = match.group(1)
            if token is None:
                return match.group(0)
            return f"${{{token}}}" if token in known_members else match.group(0)

        return pattern.sub(_wrap, out)

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

        if model.sql:
            cube["sql"] = model.sql
        elif model.table:
            cube["sql_table"] = model.table

        if model.description:
            cube["description"] = model.description

        if model.meta:
            # Top-level Cube fields stashed in the adapter-owned cube_internal.top_level namespace
            # on import are lifted back to real Cube keys; arbitrary user meta -- including a
            # user's own meta.cube_top_level or a key named like a Cube field (e.g. meta.public) --
            # stays under meta and is never promoted.
            top_level = _cube_internal(model.meta).get("top_level")
            if isinstance(top_level, dict):
                for k, v in top_level.items():
                    cube[k] = v
            remaining_meta = self._exported_meta(model.meta)
            if remaining_meta:
                cube["meta"] = remaining_meta

        # Export dimensions
        dimensions = []

        for dim in model.dimensions:
            dim_def = {"name": dim.name}
            internal = _cube_internal(dim.meta)

            # Cube geo dimensions are imported as categorical with the geo marker + lat/long
            # stashed in the internal namespace; re-emit them as a real Cube geo dimension
            # with top-level latitude/longitude rather than leaving them buried in meta.
            if internal.get("cube_type") == "geo" and ("latitude" in internal or "longitude" in internal):
                dim_def["type"] = "geo"
                for geo_key in ("latitude", "longitude"):
                    geo_val = internal.get(geo_key)
                    if geo_val is not None:
                        dim_def[geo_key] = self._geo_ref_to_cube(geo_val)
            else:
                # Map Sidemantic types to Cube types
                type_mapping = {
                    "categorical": "string",
                    "numeric": "number",
                    "time": "time",
                    "boolean": "boolean",
                }
                dim_def["type"] = type_mapping.get(dim.type, "string")

            if dim.sql:
                dim_def["sql"] = self._model_sql_to_cube(dim.sql)

            if dim.description:
                dim_def["description"] = dim.description

            # Add metadata fields
            if dim.format:
                dim_def["format"] = dim.format

            if dim.label:
                dim_def["title"] = dim.label

            # Re-emit an imported measure-as-dimension as the top-level Cube `sub_query` property
            # (stashed in the adapter-owned namespace on import).
            sub_query = internal.get("sub_query")
            if sub_query is not None:
                dim_def["sub_query"] = sub_query

            dim_meta = self._exported_meta(dim.meta)
            if dim_meta:
                dim_def["meta"] = dim_meta

            if not dim.public:
                dim_def["shown"] = False

            # Mark primary key dimension
            if model.primary_key and dim.name == model.primary_key:
                dim_def["primary_key"] = True

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

        # Export measures. Build the set of qualified semantic members so derived-measure
        # SQL only re-wraps real member references, never arbitrary dotted SQL.
        known_members = {
            f"{mname}.{member.name}" for mname, m in resolved_models.items() for member in (*m.metrics, *m.dimensions)
        }
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
                # Derived metrics become calculated measures; re-wrap member references so
                # cross-cube refs (${other.measure}) round-trip instead of becoming plain SQL.
                measure_def["type"] = "number"
                if measure.sql:
                    measure_def["sql"] = self._derived_sql_to_cube(measure.sql, known_members)
            elif measure.type == "cumulative":
                # Cube rolling-window measures keep their aggregation type (sum/count/...),
                # not type: number (which does not aggregate).
                rolling_agg_mapping = {
                    "count": "count",
                    "count_distinct": "count_distinct",
                    "sum": "sum",
                    "avg": "avg",
                    "min": "min",
                    "max": "max",
                }
                measure_def["type"] = rolling_agg_mapping.get(measure.agg, "number")
                if measure.sql:
                    measure_def["sql"] = self._model_sql_to_cube(measure.sql)
                rolling_window = None
                if measure.window:
                    rolling_window = {"trailing": measure.window}
                elif measure.grain_to_date:
                    rolling_window = {"type": "to_date", "granularity": measure.grain_to_date}
                if rolling_window is not None:
                    # Re-attach leading/offset preserved under the internal meta namespace.
                    rw_internal = _cube_internal(measure.meta)
                    if rw_internal.get("rolling_window_leading") is not None:
                        rolling_window["leading"] = rw_internal["rolling_window_leading"]
                    if rw_internal.get("rolling_window_offset") is not None:
                        rolling_window["offset"] = rw_internal["rolling_window_offset"]
                    measure_def["rolling_window"] = rolling_window
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
                    "approx_count_distinct": "count_distinct_approx",
                    "sum": "sum",
                    "avg": "avg",
                    "min": "min",
                    "max": "max",
                }
                internal = _cube_internal(measure.meta)
                cube_type = internal.get("cube_type")
                if cube_type in ("rank", "number_agg", "string", "time", "boolean"):
                    # Measures with a recorded original Cube type: re-emit that type, not the
                    # aggregation. rank in particular carries an executable count fallback in
                    # agg, so checking cube_type first keeps it from exporting as type: count.
                    measure_def["type"] = cube_type
                    if cube_type == "rank":
                        for rank_field in ("order_by", "reduce_by"):
                            if internal.get(rank_field) is not None:
                                measure_def[rank_field] = internal[rank_field]
                elif measure.agg is not None:
                    measure_def["type"] = type_mapping.get(measure.agg, "count")
                else:
                    # agg=None with no recorded type is a complete SQL expression.
                    measure_def["type"] = "number"

                if measure.sql:
                    measure_def["sql"] = self._model_sql_to_cube(measure.sql)

            if measure.filters:
                measure_def["filters"] = [{"sql": self._model_sql_to_cube(f)} for f in measure.filters]

            if measure.description:
                measure_def["description"] = measure.description

            # Add metadata fields
            if measure.format:
                measure_def["format"] = measure.format

            if measure.label:
                measure_def["title"] = measure.label

            if measure.meta:
                # Strip only the adapter-owned cube_internal markers (re-emitted as real Cube
                # fields above); user-supplied meta -- including their own cube_internal keys --
                # is preserved.
                exported_meta = self._exported_meta(measure.meta)
                if exported_meta:
                    measure_def["meta"] = exported_meta

            if not measure.public:
                measure_def["shown"] = False

            # Export drill fields only when the measure actually declared them; do not
            # fabricate them from the model's hierarchy dimensions.
            if measure.drill_fields:
                valid_drill = [f for f in measure.drill_fields if f in [d.name for d in model.dimensions]]
                measure_def["drill_members"] = valid_drill or measure.drill_fields

            measures.append(measure_def)

        if measures:
            cube["measures"] = measures

        # Export segments
        if model.segments:
            cube["segments"] = []
            for segment in model.segments:
                segment_def = {"name": segment.name}
                if segment.sql:
                    segment_def["sql"] = self._model_sql_to_cube(segment.sql)
                if segment.description:
                    segment_def["description"] = segment.description
                if not segment.public:
                    segment_def["shown"] = False
                cube["segments"].append(segment_def)

        # Export joins (all relationship types)
        joins = []
        for relationship in model.relationships:
            # Cube has no representation for many_to_many (needs a junction) or cross
            # joins; omit them with a warning rather than emitting an invalid join.
            if relationship.type in ("many_to_many", "cross"):
                warnings.warn(
                    f"Cube export: relationship '{model.name}' -> '{relationship.name}' is "
                    f"{relationship.type}, which Cube cannot represent; omitting it from the export.",
                    CubeImportWarning,
                    stacklevel=2,
                )
                continue

            # Find target model from resolved models (inheritance-applied)
            target_model = resolved_models.get(relationship.name)
            if target_model:
                if relationship.sql:
                    # Custom predicate (composite keys, one_to_one, non-equality):
                    # preserve it by translating {from}/{to} back to Cube placeholders.
                    join_sql = self._native_join_sql_to_cube(relationship.sql, relationship.name)
                elif relationship.type == "many_to_one":
                    # ${CUBE}.fk = ${target.pk}
                    local_key = relationship.sql_expr or relationship.foreign_key
                    remote_key = relationship.primary_key or target_model.primary_key
                    if isinstance(remote_key, list):
                        remote_key = remote_key[0]
                    join_sql = f"${{CUBE}}.{local_key} = ${{{relationship.name}}}.{remote_key}"
                else:
                    # one_to_many / one_to_one: ${CUBE}.pk = ${target.fk}.
                    # The local key is this model's key (relationship.primary_key when set,
                    # else the model's primary key); the FK lives on the related model.
                    local_key = relationship.primary_key or model.primary_key
                    if isinstance(local_key, list):
                        local_key = local_key[0]
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
                # Emit Cube's camelCase pre-aggregation type names (not the snake_case
                # internal form, which Cube does not accept).
                cube_preagg_type = {
                    "rollup": "rollup",
                    "original_sql": "originalSql",
                    "rollup_join": "rollupJoin",
                    "lambda": "rollupLambda",
                }.get(preagg.type, preagg.type)
                preagg_def = {"name": preagg.name, "type": cube_preagg_type}
                if preagg.sql:
                    # Translate the {model} placeholder back to Cube's ${CUBE} (the originalSql
                    # query was normalized on import); leave any string literals untouched.
                    preagg_def["sql"] = self._model_sql_to_cube(preagg.sql)
                if preagg.rollups:
                    # Only unqualified refs name a rollup on THIS cube and need the CUBE prefix;
                    # an already-qualified cross-cube ref (e.g. ``visitors.for_join``) must stay
                    # as-is, since Cube treats ``CUBE.visitors.for_join`` as a current-cube member.
                    preagg_def["rollups"] = [r if "." in r else f"CUBE.{r}" for r in preagg.rollups]
                if preagg.union_with_source_data:
                    preagg_def["union_with_source_data"] = True
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
