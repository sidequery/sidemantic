"""LookML adapter for importing Looker semantic models."""

import logging
import re
from pathlib import Path

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph

logger = logging.getLogger(__name__)


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

    # Matches ${field} and ${view.field}. ${TABLE} is handled specially.
    _REF_RE = re.compile(r"\$\{(?:([a-zA-Z_]\w*)\.)?([a-zA-Z_]\w*)\}")

    @staticmethod
    def _strip_self_view_qualifiers(view_def: dict, view_name: str) -> dict:
        """Rewrite ``${view_name.field}`` -> ``${field}`` in this view's SQL.

        In LookML a self-qualified reference (``${this_view.field}``) is identical
        to the bare ``${field}``. Normalizing it here (in place) lets the
        bare-reference resolver handle it instead of leaking the literal ``${...}``
        into generated SQL (which the database rejects).
        """
        pat = re.compile(r"\$\{" + re.escape(view_name) + r"\.([a-zA-Z_]\w*)\}")

        def fix(value):
            return pat.sub(r"${\1}", value) if isinstance(value, str) else value

        # `filters` are view-level segments; their `sql` can self-qualify a field the same way
        # (`sql: ${orders.status} = 'completed' ;;`) and must be normalized too, else the leaked
        # ${orders.status} reaches the WHERE clause and the database rejects it.
        for key in ("dimensions", "dimension_groups", "measures", "filters"):
            for item in view_def.get(key) or []:
                if not isinstance(item, dict):
                    continue
                for sql_key in ("sql", "sql_start", "sql_end", "sql_distinct_key"):
                    if sql_key in item:
                        item[sql_key] = fix(item[sql_key])
        derived = view_def.get("derived_table")
        if isinstance(derived, dict) and "sql" in derived:
            derived["sql"] = fix(derived["sql"])
        return view_def

    def _resolve_dimension_references(
        self,
        sql: str,
        dimension_sql_lookup: dict[str, str],
        max_depth: int = 10,
        dimension_names: set[str] | None = None,
    ) -> str:
        """Resolve ``${dimension}`` references in a SQL expression.

        Handles LookML's reference syntax where dimensions/measures reference other
        dimensions via ``${name}``. Resolution is recursive (a dimension may
        reference another dimension) with cycle detection, so acyclic chains of any
        depth resolve fully and circular references terminate instead of either
        looping forever or silently truncating at a fixed depth.

        ``${TABLE}`` is left untouched (handled separately). Self-view qualifiers
        are expected to have been normalized away already (see
        ``_strip_self_view_qualifiers``); any remaining ``${view.field}`` is a
        cross-view reference, which sidemantic cannot represent inline, so it is
        emitted as a qualified column ``view.field`` with a warning rather than
        leaking the literal ``${...}`` (a guaranteed SQL syntax error).

        Args:
            sql: SQL expression that may contain ``${...}`` references.
            dimension_sql_lookup: Map of dimension name -> its SQL expression.
            max_depth: Retained for compatibility; cycle detection is authoritative.

        Returns:
            SQL with references resolved.
        """
        if not sql:
            return sql

        def resolve(text: str, path: frozenset) -> str:
            def replace_ref(match: re.Match) -> str:
                view, name = match.group(1), match.group(2)
                if view is None and name == "TABLE":
                    return match.group(0)
                if view is not None:
                    # Cross-view reference (self-view already normalized away).
                    # Sidemantic cannot represent an inline cross-model column, so
                    # leave the ${view.field} literal and warn rather than emitting a
                    # qualified column that the generator can't join (which would
                    # produce wrong SQL or fail with "no join path").
                    logger.warning(
                        "LookML cross-view reference ${%s.%s} is not supported (sidemantic "
                        "has no inline cross-model column); left unresolved.",
                        view,
                        name,
                    )
                    return match.group(0)
                if name in dimension_sql_lookup:
                    if name in path:
                        # Circular reference: stop expanding to avoid infinite loop.
                        return match.group(0)
                    return f"({resolve(dimension_sql_lookup[name], path | {name})})"
                if dimension_names and name in dimension_names:
                    # Compact dimension (declared with no explicit sql) -> its default
                    # column. Without this the literal ${name} would leak into SQL.
                    return f"({{model}}.{name})"
                # Unknown bare reference: leave as-is.
                return match.group(0)

            return self._REF_RE.sub(replace_ref, text)

        return resolve(sql, frozenset())

    @classmethod
    def _fold_complete_sql_filters(cls, sql: str, filters: list[str], force: bool = False) -> str | None:
        """Fold measure ``filters`` INTO a complete-SQL aggregate when the generator's
        column-nulling can't apply them safely.

        The generator filters an opaque complete-SQL measure by projecting each column the
        SQL references wrapped in ``CASE WHEN <filter> THEN col ELSE NULL END``; the outer
        aggregate then ignores the NULLs. That works for ``SUM(amount)`` but is WRONG in two
        cases: (1) a ZERO-column aggregate like ``COUNT(*)`` has no column to null so the
        filter is silently dropped (and a mix like ``COUNT(*) / COUNT(DISTINCT id)`` filters
        inconsistently); (2) the SQL tests a filter-affected column for NULL (``col IS NULL``
        / ``COALESCE(col, ...)``) -- nulling the column to EXCLUDE a row instead makes
        ``col IS NULL`` true, COUNTING the excluded row. In either case rewrite EVERY
        aggregate to carry the filter via a portable ``CASE WHEN`` inside its argument and
        return the new SQL (caller then clears ``filters``). Returns None to leave the SQL
        and filters untouched -- the common all-columns/no-null-test case (generator handles
        it) or any parse failure (fall back to the existing path).
        """
        import sqlglot
        from sqlglot import expressions as exp

        from sidemantic.sql.aggregation_detection import _ANONYMOUS_AGGREGATE_FUNCTIONS

        def _case(cond, then):
            return exp.Case(ifs=[exp.If(this=cond, true=then)])

        try:
            tree = sqlglot.parse_one(sql.replace("{model}", "__MODEL__"))
            parsed_conds = [sqlglot.parse_one(f.replace("{model}", "__MODEL__")) for f in filters]
        except Exception:
            return None
        # Include recognized ANONYMOUS aggregates (PRODUCT/ENTROPY/WEIGHTED_AVG/... which
        # sqlglot parses as exp.Anonymous, not exp.AggFunc) so a filter is folded into them
        # too -- otherwise a mix like PRODUCT(amount) / COUNT(*) would fold only the COUNT,
        # clear the filters, and leave PRODUCT computed over ALL rows.
        aggs = list(tree.find_all(exp.AggFunc))
        aggs += [n for n in tree.find_all(exp.Anonymous) if (n.name or "").lower() in _ANONYMOUS_AGGREGATE_FUNCTIONS]
        if not aggs or not parsed_conds:
            return None

        # An ordered-set aggregate (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)) keeps its
        # value column in the enclosing WithinGroup's ORDER BY, NOT inside the AggFunc (whose
        # only arg is the percentile constant). Its "aggregate scope" for the column check and
        # for folding is therefore the WithinGroup.
        def _scope(a):
            wg = a.find_ancestor(exp.WithinGroup)
            return wg if wg is not None else a

        # Column-nulling is UNSAFE when nulling a filter-affected column to EXCLUDE a row does
        # NOT actually exclude it: (1) a NULL-test (`col IS NULL` / `COALESCE(col, ...)`) flips
        # to true and counts the row; (2) a keyed symmetric-distinct aggregate hashes the key
        # (`HASH(col)`), and HASH(NULL) is a non-NULL constant, so the row still contributes
        # (garbage). In those cases fold the filter into the aggregate predicate instead.
        # (3) a conditional with a non-NULL DEFAULT branch also survives nulling: nulling the
        # predicate's column only makes the condition false, and the default still yields a
        # non-NULL value, so the aggregate keeps counting the excluded row -- e.g.
        # COUNT(CASE WHEN status='completed' THEN 1 ELSE 0 END) or its IF/IFF spellings return
        # EVERY row rather than the filtered ones. Covers:
        #   - CASE ... ELSE <x>            -> exp.Case with a `default`
        #   - IF(cond, a, b)               -> exp.If with a non-None `false` branch. A CASE's own
        #                                     WHEN clauses are exp.If with false=None, so a plain
        #                                     CASE (no ELSE) is NOT matched here.
        #   - IFF(cond, a, b) (Snowflake)  -> parsed as exp.Anonymous with 3 args
        # (NVL / IFNULL already parse as exp.Coalesce, covered above.)
        case_with_default = any(c.args.get("default") is not None for c in tree.find_all(exp.Case))
        if_with_default = any(n.args.get("false") is not None for n in tree.find_all(exp.If))
        iff_with_default = any(
            (n.name or "").lower() in ("iff", "if") and len(n.expressions) >= 3 for n in tree.find_all(exp.Anonymous)
        )
        # (4) a MULTI-COLUMN DISTINCT -- COUNT(DISTINCT (a, b)) or COUNT(DISTINCT a, b) -- is unsafe
        # to null: nulling the columns of an excluded row yields the tuple (NULL, NULL), which is
        # NOT a NULL value (only its components are), so DISTINCT counts that phantom tuple ONCE and
        # inflates the result by one. A single-column DISTINCT is safe (its NULL is ignored).
        multi_col_distinct = any(
            len(d.expressions) > 1 or any(isinstance(e, exp.Tuple) and len(e.expressions) > 1 for e in d.expressions)
            for d in tree.find_all(exp.Distinct)
        )
        unsafe_nulling = (
            tree.find(exp.Is) is not None
            or tree.find(exp.Coalesce) is not None
            or re.search(r"\bhash\s*\(", sql, re.I) is not None
            or case_with_default
            or if_with_default
            or iff_with_default
            or multi_col_distinct
        )
        # Otherwise, if every aggregate already references a column the generator can null
        # (nulling the value/ORDER-BY column filters it; aggregates ignore NULLs), the
        # existing path is correct AND consistent -> don't rewrite. `force` skips this: a caller
        # INLINING this SQL into another measure has no generator column-nulling step, so the
        # filter must always be baked into a CASE here.
        if not force and not unsafe_nulling and all(any(True for _ in _scope(a).find_all(exp.Column)) for a in aggs):
            return None

        def _combined():
            c = parsed_conds[0].copy()
            for p in parsed_conds[1:]:
                c = exp.And(this=c, expression=p.copy())
            return c

        for a in aggs:
            # Is `a` itself windowed? Its OVER wrapper (exp.Window) is normally the direct parent,
            # but an aggregate FILTER clause (COUNT(*) FILTER (WHERE ...) OVER ()) nests an exp.Filter
            # between the aggregate and the Window, so walk past any Filter wrapper. (A NESTED
            # aggregate like the inner COUNT of SUM(COUNT(*)) OVER () has a non-Filter/-Window parent,
            # so it is correctly NOT treated as windowed and still folds.)
            _windowed = a.parent
            while isinstance(_windowed, exp.Filter):
                _windowed = _windowed.parent
            if isinstance(_windowed, exp.Window):
                # A WINDOWED aggregate (SUM(...) OVER ()) runs AFTER grouping, so wrapping its
                # argument in CASE WHEN <filter> puts the filter column inside the window, ungrouped
                # -- the engine rejects it. When the window wraps a NESTED aggregate
                # (SUM(COUNT(*)) OVER ()), that inner aggregate is also in `aggs` and carries the
                # filter, so skip only the outer one. With no inner aggregate to carry it
                # (SUM(amount) OVER ()), the predicate cannot be applied to this term consistently
                # with the rest -> abort the fold rather than emit inconsistent or invalid SQL.
                inner_aggs = [n for n in a.find_all(exp.AggFunc) if n is not a]
                inner_aggs += [
                    n
                    for n in a.find_all(exp.Anonymous)
                    if n is not a and (n.name or "").lower() in _ANONYMOUS_AGGREGATE_FUNCTIONS
                ]
                if inner_aggs:
                    continue
                return None
            wg = a.find_ancestor(exp.WithinGroup)
            if wg is not None:
                # Ordered-set aggregate: fold into the ORDER BY value(s), never the percentile
                # constant (PERCENTILE_CONT(CASE ...) would be a non-constant parameter).
                for ordered in wg.find_all(exp.Ordered):
                    ordered.set("this", _case(_combined(), ordered.this.copy()))
                continue
            if isinstance(a, exp.Anonymous):
                # Anonymous aggregate: its arguments live in .expressions (not .this, which is
                # the function name), so wrap each argument to carry the filter.
                a.set("expressions", [_case(_combined(), e.copy()) for e in a.expressions])
                continue
            arg = a.this
            if isinstance(arg, exp.Distinct):
                arg.set("expressions", [_case(_combined(), e.copy()) for e in arg.expressions])
            elif arg is None or isinstance(arg, exp.Star):
                a.set("this", _case(_combined(), exp.Literal.number(1)))
            else:
                a.set("this", _case(_combined(), arg.copy()))
        try:
            return tree.sql().replace("__MODEL__", "{model}")
        except Exception:
            return None

    @classmethod
    def _generator_column_nulling_suffices(cls, sql: str) -> bool:
        """True if the generator's column-nulling can faithfully apply a filter to this complete SQL.

        Mirrors the early-return in ``_fold_complete_sql_filters``: nulling the columns each
        aggregate references filters it only when EVERY aggregate has a column to null and no
        unsafe-nulling construct (NULL-test, COALESCE, HASH, CASE/IF/IFF-with-default, multi-column
        DISTINCT) is present. When this is False AND folding also aborts, the filter cannot be
        applied at all -- a zero-column aggregate like ``COUNT(*) FILTER (WHERE ...) OVER ()`` has no
        column to null, so the imported filter would silently count every row.
        """
        import sqlglot
        from sqlglot import expressions as exp

        from sidemantic.sql.aggregation_detection import _ANONYMOUS_AGGREGATE_FUNCTIONS

        try:
            tree = sqlglot.parse_one(sql.replace("{model}", "__M__"))
        except Exception:
            return False
        aggs = list(tree.find_all(exp.AggFunc))
        aggs += [n for n in tree.find_all(exp.Anonymous) if (n.name or "").lower() in _ANONYMOUS_AGGREGATE_FUNCTIONS]
        if not aggs:
            return False
        unsafe = (
            tree.find(exp.Is) is not None
            or tree.find(exp.Coalesce) is not None
            or re.search(r"\bhash\s*\(", sql, re.I) is not None
            or any(c.args.get("default") is not None for c in tree.find_all(exp.Case))
            or any(n.args.get("false") is not None for n in tree.find_all(exp.If))
            or any(
                (n.name or "").lower() in ("iff", "if") and len(n.expressions) >= 3
                for n in tree.find_all(exp.Anonymous)
            )
            or any(
                len(d.expressions) > 1
                or any(isinstance(e, exp.Tuple) and len(e.expressions) > 1 for e in d.expressions)
                for d in tree.find_all(exp.Distinct)
            )
        )
        if unsafe:
            return False

        def _scope(a):
            wg = a.find_ancestor(exp.WithinGroup)
            return wg if wg is not None else a

        return all(any(True for _ in _scope(a).find_all(exp.Column)) for a in aggs)

    @staticmethod
    def _sql_has_list_aggregate(sql: str) -> bool:
        """True if ``sql`` contains a NULL-retaining array collector -- ``LIST(...)`` (sqlglot's
        ``exp.List``) or ``ARRAY_AGG(...)`` (``exp.ArrayAgg``).

        Callers use this to refuse FILTERING such an expression: these collectors keep NULL inputs,
        so a filter can be applied neither by column-nulling (the excluded row's NULL is still an
        element -- ARRAY_LENGTH still counts it) nor by a folded CASE.
        """
        import sqlglot
        from sqlglot import expressions as exp

        try:
            tree = sqlglot.parse_one(sql.replace("{model}", "__m__").replace("${TABLE}", "__m__"))
        except Exception:
            return False
        return any(True for _ in tree.find_all(exp.List)) or any(True for _ in tree.find_all(exp.ArrayAgg))

    # SQL date-part keywords that a date/time function can take UNQUOTED.
    _DATE_PART_KEYWORDS = frozenset(
        {
            "year", "quarter", "month", "week", "day", "hour", "minute", "second",
            "millisecond", "microsecond", "nanosecond", "epoch", "date", "time",
            "dayofweek", "dayofyear", "dow", "doy", "isoweek", "isoyear", "isodow",
            "weekday", "yearofweek", "century", "decade", "millennium",
        }
    )  # fmt: skip
    # Which ARGUMENT of a date/time call is the date part. Position matters: the same keyword can
    # be a real column in another slot -- DATE_TRUNC(date, month) on a model with BOTH a `date`
    # and a `month` dimension means column `date` truncated to part `month`. Keying only on "is a
    # keyword inside a date function" would wrongly protect the `date` COLUMN too.
    #   -1 => the LAST argument (BigQuery DATE_TRUNC(value, part), DATE_DIFF(a, b, part))
    #    0 => the FIRST argument (SQL Server DATETRUNC(part, x) / DATEADD(part, n, x) /
    #         DATEDIFF(part, a, b); also DuckDB's datetrunc(part, x) spelling)
    # NOTE the two TRUNC spellings differ: underscored DATE_TRUNC is BigQuery's (value, part) --
    # Postgres/DuckDB's date_trunc('part', value) quotes the part, so it is already protected as a
    # string literal -- while the unspaced DATETRUNC is SQL Server's (part, value).
    # Functions with NO bare date-part argument (time_bucket takes an INTERVAL, not a keyword) are
    # deliberately absent: listing one would protect whatever sits in that slot, including a real
    # keyword-named column. EXTRACT(part FROM x) and INTERVAL n part have their own position checks.
    _DATE_PART_ARG_POS = {
        "timestamp_trunc": -1, "datetime_trunc": -1, "time_trunc": -1,
        "date_diff": -1, "timestamp_diff": -1, "datetime_diff": -1, "time_diff": -1,
        "datetrunc": 0, "datediff": 0, "dateadd": 0, "datepart": 0, "date_part": 0,
    }  # fmt: skip
    # DATE_TRUNC has NO fixed part position: BigQuery is DATE_TRUNC(value, part) while Snowflake is
    # DATE_TRUNC(part, expr), and the adapter has no dialect context. Disambiguate by CONTENT --
    # whichever argument is a date-part keyword is the part (see _is_date_part_argument).
    _DATE_PART_AMBIGUOUS_TRUNC = frozenset({"date_trunc"})

    @staticmethod
    def _enclosing_call(pre: str) -> tuple[str | None, int, int]:
        """Describe the call a token sits in, given the text ``pre`` before it.

        Returns ``(function_name, arg_index, open_paren_pos)`` by scanning backwards for the first
        unclosed ``(`` and counting the top-level commas after it. ``(None, 0, -1)`` when the token
        is not inside a call.
        """
        depth, commas, i = 0, 0, len(pre) - 1
        while i >= 0:
            ch = pre[i]
            if ch == ")":
                depth += 1
            elif ch == "(":
                if depth == 0:
                    m = re.search(r"(\w+)\s*$", pre[:i])
                    return (m.group(1) if m else None), commas, i
                depth -= 1
            elif ch == "," and depth == 0:
                commas += 1
            i -= 1
        return None, 0, -1

    @classmethod
    def _enclosing_call_arg_texts(cls, pre: str, suf: str, token: str) -> list[str]:
        """Argument texts of the call a token sits in, reconstructed from ``pre``/``token``/``suf``.

        Used to disambiguate a call whose date-part slot is not fixed (see
        ``_DATE_PART_AMBIGUOUS_TRUNC``). Returns [] when the token is not inside a call.
        """
        _, _, open_pos = cls._enclosing_call(pre)
        if open_pos < 0:
            return []
        depth, end = 0, len(suf)
        for i, ch in enumerate(suf):
            if ch in "([":
                depth += 1
            elif ch in ")]":
                if depth == 0:
                    end = i
                    break
                depth -= 1
        call_args = pre[open_pos + 1 :] + token + suf[:end]
        return [a.strip() for a in cls._split_top_level_commas(call_args, quote_aware=True)]

    @classmethod
    def _is_date_part_argument(cls, pre: str, suf: str, token: str) -> bool:
        """True if ``token`` occupies the date-PART argument slot of a date/time call.

        Position-aware on purpose: DATE_TRUNC(date, month) on a model with both a `date` and a
        `month` dimension means column `date` truncated to part `month` -- only the LAST argument
        is the part, so the `date` column must still resolve.
        """
        if token.lower() not in cls._DATE_PART_KEYWORDS:
            return False
        func, arg_index, open_pos = cls._enclosing_call(pre)
        if not func:
            return False
        if func.lower() in cls._DATE_PART_AMBIGUOUS_TRUNC:
            # DATE_TRUNC(a, b) is BigQuery (value, part) OR Snowflake (part, expr) -- same name,
            # opposite orders. Decide by CONTENT: the argument that IS a date-part keyword is the
            # part. When BOTH are keywords (DATE_TRUNC(date, month)) it is genuinely ambiguous, so
            # take the LAST as the part (BigQuery's order) and let the other resolve as a column.
            args = cls._enclosing_call_arg_texts(pre, suf, token)
            if len(args) != 2:
                return False
            if arg_index == 1:
                return True
            return args[1].strip().lower() not in cls._DATE_PART_KEYWORDS
        want = cls._DATE_PART_ARG_POS.get(func.lower())
        if want is None:
            return False
        if want >= 0:
            return arg_index == want
        # want == -1: the part is the LAST argument -- true when no top-level comma follows the
        # token before the call's closing paren.
        depth = 0
        for ch in suf:
            if ch in "([":
                depth += 1
            elif ch in ")]":
                if depth == 0:
                    return True  # reached this call's close with no further top-level comma
                depth -= 1
            elif ch == "," and depth == 0:
                return False
        return False

    @staticmethod
    def _has_top_level_order_by(s: str) -> bool:
        """True if ``s`` has an ``ORDER BY`` at paren depth 0, outside string literals.

        Used to detect an aggregate-local ORDER BY (``SUM(amount ORDER BY created_at)``), which
        belongs to the aggregate CALL rather than the argument expression -- so a filter must not
        be folded into a CASE around it.
        """
        depth, quote, i = 0, None, 0
        while i < len(s):
            ch = s[i]
            if quote is not None:
                if ch == quote:
                    quote = None
                i += 1
                continue
            if ch in "'\"":
                quote = ch
            elif ch in "[(":
                depth += 1
            elif ch in ")]":
                depth = max(0, depth - 1)
            elif (
                depth == 0
                and (i == 0 or not (s[i - 1].isalnum() or s[i - 1] == "_"))
                and re.match(r"(?i)order\s+by\b", s[i:])
            ):
                return True
            i += 1
        return False

    @staticmethod
    def _has_subquery(sql: str) -> bool:
        """True if ``sql`` contains a SELECT outside any quoted token.

        A raw ``\\bselect\\b`` scan also matches the word inside a VALUE
        (``SUM(CASE WHEN status = 'select' THEN amount END)``) or inside a quoted IDENTIFIER for a
        column named after a reserved word (``SUM(${TABLE}."select")``, ``SUM(`select`)``), inside a
        SQL COMMENT (``/* select paid rows */ SUM(amount)``), or inside a LookML ``${...}`` field
        reference to a column named ``select`` (``SUM(${select})``) -- none is a subquery, and all
        are valid inline aggregates. This runs on the RAW SQL before refs are resolved, so blank out
        every quoted form, every comment, AND every ``${...}`` placeholder first -- in one left-to-
        right pass so a comment inside a string (or a quote inside a comment) is consumed by
        whichever opens first, leaving only real SQL keywords.
        """
        stripped = re.sub(
            r"""'(?:[^']|'')*'|"(?:[^"]|"")*"|`[^`]*`|\[[^\]]*\]|--[^\n]*|/\*[\s\S]*?\*/|\$\{[^}]*\}""",
            " ",
            sql or "",
        )
        return bool(re.search(r"(?is)\bselect\b", stripped))

    @classmethod
    def _mixed_is_aggregate_safe(cls, sql: str, is_dim_ref, dim_sql_lookup: dict[str, str] | None = None) -> bool:
        """For a ``type: number`` measure that mixes measure refs with dimension refs,
        return True iff every dimension column ends up INSIDE an aggregate (no raw,
        ungrouped column -- which would be a GROUP BY error).

        Probes with measure refs as aggregate-valued constants (``1``) and dimension refs
        as their RESOLVED SQL (falling back to a raw column ``t.<name>`` for a compact
        dimension with no explicit sql), then checks via sqlglot that no column sits
        outside an aggregate. Using the resolved SQL matters for a CONSTANT-valued
        dimension (``sql: 0.07 ;;``): probing it as ``t.<name>`` would look like a raw
        ungrouped column and wrongly drop a valid measure such as
        ``${total} * ${tax_rate}`` (really ``SUM(amount) * 0.07``).
        Returns False on any parse failure (treat as unsafe).
        """

        def _probe(m):
            v, rn = m.group(1), m.group(2)
            if v is None and rn != "TABLE" and is_dim_ref(rn):
                # Parenthesized so the substituted expression keeps its precedence; the
                # caller's {model} -> t replacement below rewrites any table qualifier.
                dim_sql = (dim_sql_lookup or {}).get(rn)
                return f"({dim_sql})" if dim_sql else f"t.{rn}"
            return "1"

        probe = cls._REF_RE.sub(_probe, sql).replace("{model}", "t")
        try:
            import sqlglot
            from sqlglot import expressions as exp

            from sidemantic.sql.aggregation_detection import _ANONYMOUS_AGGREGATE_FUNCTIONS

            tree = sqlglot.parse_one(probe)
        except Exception:
            return False

        def _is_agg_scope(node) -> bool:
            # A node that groups its argument columns: an aggregate function (incl. an
            # anonymous/engine-specific aggregate), an aggregate FILTER (WHERE ...) predicate
            # (sqlglot nests that under exp.Filter, not exp.AggFunc), an ordered-set aggregate's
            # WITHIN GROUP (ORDER BY ...) (exp.WithinGroup, e.g. PERCENTILE_CONT(0.5) WITHIN
            # GROUP (ORDER BY x)), or a LIST(...) collector (DuckDB's LIST -> exp.List, which
            # aggregation_detection also counts as an aggregate, so this must agree).
            return isinstance(node, (exp.AggFunc, exp.Filter, exp.WithinGroup, exp.List)) or (
                isinstance(node, exp.Anonymous) and (node.name or "").lower() in _ANONYMOUS_AGGREGATE_FUNCTIONS
            )

        def _column_is_grouped(c) -> bool:
            # Walk up from the column to the FIRST aggregate-or-window boundary.
            node = c.parent
            while node is not None:
                if isinstance(node, exp.Window):
                    # Reached a window before any plain aggregate: the column is a raw per-row
                    # argument of the window (SUM(x) OVER (), LAG(x) OVER ()) -- still ungrouped.
                    return False
                if _is_agg_scope(node):
                    # A plain aggregate consumes the column and produces a grouped value. But if
                    # that aggregate is ITSELF windowed (SUM(x) OVER ()), the column is a raw
                    # per-row argument, so only a NON-windowed aggregate groups it. A nested
                    # aggregate inside a window -- SUM(SUM(x)) OVER () -- is reached here at the
                    # inner SUM, whose parent is the outer SUM (not the Window), so it is grouped.
                    # An aggregate FILTER clause nests exp.Filter between the aggregate and its OVER
                    # window (SUM(x) FILTER (WHERE ...) OVER ()), so walk past Filter wrappers to see
                    # the window -- otherwise a raw windowed aggregate reads as grouped.
                    _p = node.parent
                    while isinstance(_p, exp.Filter):
                        _p = _p.parent
                    return not isinstance(_p, exp.Window)
                node = node.parent
            return False

        return all(_column_is_grouped(c) for c in tree.find_all(exp.Column))

    # Plain decimal numeric literal: optional sign, then digits with optional fraction,
    # or a bare fraction. Deliberately excludes Python-/float()-only spellings that this
    # converter (which does not know the field type) cannot safely emit unquoted:
    # nan/inf/Infinity, exponents (1e2), and underscores (1_000) -- a string filter value
    # like "1e2" must stay a quoted literal, not become `= 1e2`.
    _NUMERIC_LITERAL_RE = re.compile(r"[+-]?(\d+(\.\d*)?|\.\d+)")

    @classmethod
    def _filter_is_number(cls, s: str) -> bool:
        """True if ``s`` is a plain decimal numeric literal (signed / fractional)."""
        return bool(cls._NUMERIC_LITERAL_RE.fullmatch(s.strip()))

    @staticmethod
    def _numeric_range_bounds(s: str):
        """Parse a Looker numeric range/interval into ``(lo, lo_incl, hi, hi_incl)``.

        Handles ``a to b`` / ``a to`` / ``to b`` (inclusive both ends) and bracket
        intervals ``[a,b] (a,b) [a,b) (a,b]``. An empty bound -- or an explicit
        ``inf`` / ``-inf`` (Looker's open-ended notation) -- is ``None`` (open).
        Returns ``None`` if ``s`` is not a numeric range.
        """
        s = s.strip()
        num = r"(-?\d*\.?\d*|[+-]?inf)"

        def norm(b):
            b = (b or "").strip()
            return None if b == "" or b.lower().lstrip("+-") == "inf" else b

        m = re.match(rf"(?i)^([\[\(])\s*{num}\s*,\s*{num}\s*([\]\)])$", s)
        if m:
            lb, lo, hi, rb = m.groups()
            lo, hi = norm(lo), norm(hi)
            return None if lo is None and hi is None else (lo, lb == "[", hi, rb == "]")
        m = re.match(rf"(?i)^{num}\s*to\s*{num}$", s)
        if m:
            lo, hi = norm(m.group(1)), norm(m.group(2))
            return None if lo is None and hi is None else (lo, True, hi, True)
        return None

    @staticmethod
    def _range_sql(bounds, col: str) -> str:
        """SQL for an inclusive/exclusive numeric range (``col >= a AND col <= b``)."""
        lo, lo_incl, hi, hi_incl = bounds
        conds = []
        if lo is not None:
            conds.append(f"{col} >{'=' if lo_incl else ''} {lo}")
        if hi is not None:
            conds.append(f"{col} <{'=' if hi_incl else ''} {hi}")
        return conds[0] if len(conds) == 1 else "(" + " AND ".join(conds) + ")"

    @staticmethod
    def _range_sql_negated(bounds, col: str) -> str:
        """SQL for the negation of a numeric range (``col < a OR col > b``)."""
        lo, lo_incl, hi, hi_incl = bounds
        conds = []
        if lo is not None:
            conds.append(f"{col} <{'' if lo_incl else '='} {lo}")
        if hi is not None:
            conds.append(f"{col} >{'' if hi_incl else '='} {hi}")
        return conds[0] if len(conds) == 1 else "(" + " OR ".join(conds) + ")"

    @staticmethod
    def _split_top_level_commas(s: str, quote_aware: bool = False) -> list[str]:
        """Split on commas that are NOT inside ``[...]``/``(...)`` brackets.

        With ``quote_aware`` also ignore commas inside SQL string literals
        (``'...'`` / ``"..."``) -- needed when splitting SQL expressions to count
        aggregate arity (a single-arg ``COUNT(DISTINCT a || ',' || b)`` must not look
        multi-column). It is OFF by default because LookML filter VALUES treat an
        apostrophe as a literal char (``O'Brien, Smith`` is two values, not one).
        """
        out, cur, depth, quote = [], "", 0, None
        for ch in s:
            if quote_aware and quote is not None:
                cur += ch
                if ch == quote:
                    quote = None
                continue
            if quote_aware and ch in "'\"":
                quote = ch
                cur += ch
                continue
            if ch in "[(":
                depth += 1
            elif ch in ")]":
                depth = max(0, depth - 1)
            if ch == "," and depth == 0:
                out.append(cur)
                cur = ""
            else:
                cur += ch
        out.append(cur)
        return [x.strip() for x in out if x.strip() != ""]

    # Tokens that mark a value as a LookML date/interval filter expression
    # (e.g. "last 7 days", "3 months ago", "this year"). These are not yet
    # translated to SQL; we warn rather than silently string-comparing them.
    _DATE_FILTER_RE = re.compile(
        r"(?i)\b(ago|day|days|week|weeks|month|months|year|years|quarter|quarters|"
        r"hour|hours|minute|minutes|second|seconds|today|yesterday|tomorrow|now|fiscal|"
        r"before|after|"
        r"monday|tuesday|wednesday|thursday|friday|saturday|sunday|"
        r"week|month|year)\b"
    )

    def _measure_filter_conds(self, measure_def: dict, view_name: str | None = None) -> tuple[list[str], bool]:
        """Convert a measure's LookML ``filters`` to SQL condition strings.

        Handles both the shorthand list-of-dicts (``filters: [status: "x"]``) and the
        block (``filter: { field: ...  value: ... }``) syntaxes that ``lkml`` collapses
        into ``filters__all``. A SELF-view qualifier (``<this_view>.status``) is stripped
        so the converter builds ``{model}.status``.

        Returns ``(conds, has_cross_view)``: a CROSS-view qualifier (``other_view.field``) cannot be
        represented in the single-table model CTE, so its condition is omitted and the flag is set
        for the caller to drop the whole measure rather than import a broken filter.
        """

        def _bare(field: str) -> str:
            if isinstance(field, str) and view_name and field.startswith(f"{view_name}."):
                rest = field[len(view_name) + 1 :]
                if re.fullmatch(r"\w+", rest):
                    return rest
            return field

        # A filter field that still carries a dot after stripping the self-view qualifier is a
        # CROSS-view reference (customers.active): the converter would build {model}.customers.active,
        # and the model CTE (single base table) has no `customers` alias, so the measure fails to
        # query. Flag it so the caller drops the whole measure, matching how a cross-view inline sql
        # ref is dropped -- rather than importing a measure with a broken filter.
        has_cross_view = False

        def _add(field, value):
            nonlocal has_cross_view
            bare = _bare(field)
            if isinstance(bare, str) and re.search(r"[^\W]+\.[^\W]", bare) and "${" not in bare:
                has_cross_view = True
                return
            fs = self._convert_lookml_filter_to_sql(bare, value)
            if fs:
                conds.append(fs)

        conds: list[str] = []
        for item in measure_def.get("filters__all") or []:
            if isinstance(item, list):
                for filter_dict in item:
                    if isinstance(filter_dict, dict):
                        for field, value in filter_dict.items():
                            _add(field, value)
            elif isinstance(item, dict):
                field = item.get("field")
                value = item.get("value")
                if field and value:
                    _add(field, value)
        return conds, has_cross_view

    def _convert_lookml_filter_to_sql(self, field: str, value: str) -> str:
        """Convert a LookML filter value to a SQL condition.

        Implements the representable parts of Looker's filter expression
        language (https://cloud.google.com/looker/docs/filter-expressions):

        - ``value`` -> ``field = 'value'`` (single quotes escaped)
        - ``a,b,c`` -> ``field IN ('a','b','c')`` (numeric: unquoted)
        - ``-value`` / ``not value`` -> ``field <> 'value'``
        - ``-a,-b`` -> ``field NOT IN ('a','b')``
        - ``-%pat%`` / ``not %pat%`` -> ``field NOT LIKE '%pat%'``
        - ``yes`` / ``no`` -> ``field = true`` / ``field = false``
        - ``>100``, ``>=5``, ``<=5``, ``<10``, ``!=0``, ``<>0`` -> comparisons
        - ``5 to 10`` -> ``field >= 5 AND field <= 10`` (open: ``5 to`` / ``to 10``)
        - ``[1,10]`` ``(1,10)`` ``[1,10)`` ``(1,10]`` -> inclusive/exclusive ranges
        - ``%pat%`` / ``_at`` -> ``field LIKE '%pat%'``
        - ``NULL`` / ``-NULL`` -> ``IS NULL`` / ``IS NOT NULL``
        - ``EMPTY`` -> ``(field IS NULL OR field = '')`` (and the negation)
        - ``before X`` / ``after X`` -> ``field < X`` / ``field > X``
        - mixed comma lists (operators/wildcards) -> includes OR'd, excludes AND'd

        Date/interval expressions (``last 7 days``, ``3 months ago`` ...) are not
        yet translated: they emit a literal equality but log a warning so the
        silent zero-row match is at least surfaced.
        """
        col = f"{{model}}.{field}"

        def q(s: str) -> str:
            return "'" + s.replace("'", "''") + "'"

        is_number = self._filter_is_number

        # Shared numeric grammar (matches _filter_is_number, incl. bare fractions like .5):
        # _num is a plain decimal literal; _cmp is a single comparison "<op> <number>".
        _num = r"[+-]?(?:\d+\.?\d*|\.\d+)"
        _cmp = rf"(?:>=|<=|!=|<>|>|<)\s*{_num}"

        def _is_numeric_and_range(s: str) -> bool:
            """True if ``s`` is an AND-joined chain of numeric comparisons (">1 AND <100")."""
            if not re.search(r"(?i)\sand\s", s):
                return False
            arms = [a.strip() for a in re.split(r"(?i)\s+and\s+", s)]
            return len(arms) > 1 and all(re.fullmatch(_cmp, a) for a in arms)

        def single(v: str) -> str:
            """Convert one (non-list) LookML filter token to a SQL condition."""
            v = v.strip()
            up = v.upper()
            if up == "NULL":
                return f"{col} IS NULL"
            if up == "-NULL":
                return f"{col} IS NOT NULL"
            if up == "EMPTY":
                return f"({col} IS NULL OR {col} = '')"
            if up == "-EMPTY":
                return f"({col} IS NOT NULL AND {col} <> '')"
            if v.lower() == "yes":
                return f"{col} = true"
            if v.lower() == "no":
                return f"{col} = false"

            # Negation. Looker uses "-FOO" for STRING negation; the word "not" is only
            # for the number/null/range filter forms. So a bare string like
            # `not started` is a LITERAL value, not a negation, and must fall through.
            nm = re.match(r"(?i)^not\s+(.+)$", v)
            neg_word = nm.group(1).strip() if nm else None
            neg_dash = (
                v[1:]
                if (
                    v.startswith("-")
                    and len(v) > 1
                    # A negative NUMBER (-5, -.5, -0.5) is a value, not a string exclusion; a
                    # `-<op>` token is a comparison form. Everything else with a leading dash --
                    # incl. dot-prefixed strings like `-.csv` -> exclude '.csv' -- IS a Looker
                    # string negation (check the whole token as numeric, don't block every `-.`).
                    and not self._filter_is_number(v)
                    and not re.match(r"^-(?:>=|<=|!=|<>|>|<)", v)
                )
                else None
            )
            neg = neg_word if neg_word is not None else neg_dash
            if neg is not None:
                # "NOT NULL" / "NOT EMPTY" are null/empty checks, same as -NULL / -EMPTY.
                if neg.upper() == "NULL":
                    return f"{col} IS NOT NULL"
                if neg.upper() == "EMPTY":
                    return f"({col} IS NOT NULL AND {col} <> '')"
                if ("%" in neg or "_" in neg) and neg_dash is not None:
                    # Only the "-%pat%" dash form negates a wildcard. The word form
                    # "not %complete%" is a literal string pattern (Looker negates strings
                    # with "-"), so let it fall through to a positive LIKE below.
                    return f"{col} NOT LIKE {q(neg)}"
                # NOT of an AND-range -> De Morgan: OR of each flipped comparison, e.g.
                # "NOT >1 AND <100" -> (f <= 1 OR f >= 100). Checked before the single
                # comparison flip, which would otherwise read ">1 AND <100" as one operand.
                _flip = {">": "<=", ">=": "<", "<": ">=", "<=": ">", "!=": "=", "<>": "="}
                if _is_numeric_and_range(neg):
                    flipped = []
                    for s in re.split(r"(?i)\s+and\s+", neg):
                        sm = re.match(r"^(>=|<=|!=|<>|>|<)\s*(.+)$", s.strip())
                        flipped.append(f"{col} {_flip[sm.group(1)]} {sm.group(2).strip()}")
                    return "(" + " OR ".join(flipped) + ")"
                # NOT of a comparison -> flip the operator, e.g. "NOT >1" -> "f <= 1".
                ncmp = re.match(r"^(>=|<=|!=|<>|>|<)\s*(.+)$", neg)
                if ncmp:
                    _flip = {">": "<=", ">=": "<", "<": ">=", "<=": ">", "!=": "=", "<>": "="}
                    nop, noperand = ncmp.group(1), ncmp.group(2).strip()
                    return f"{col} {_flip[nop]} {noperand if is_number(noperand) else q(noperand)}"
                # NOT of a numeric range/interval -> the inverted (outside) condition,
                # e.g. "NOT 3 to 80" -> (f < 3 OR f > 80).
                neg_range = self._numeric_range_bounds(neg)
                if neg_range:
                    return self._range_sql_negated(neg_range, col)
                if is_number(neg):
                    return f"{col} != {neg}"
                # Plain string: only "-FOO" negates a string; "not foo" is a literal
                # value, so let it fall through to the default string-equality below.
                if neg_dash is not None:
                    return f"{col} != {q(neg)}"

            # before / after <value>. Per Looker, "before" is exclusive and "after"
            # is inclusive. Only translate ABSOLUTE bounds (a number, or a full Looker
            # absolute date: YYYY, YYYY-MM, or YYYY-MM-DD with zero-padded 2-digit
            # parts, "/" or "-" separators). Truncated/relative operands ("2016-1",
            # "3 days ago", "Monday") aren't absolute and fall through to the
            # date-expression warning rather than a wrong literal comparison.
            bm = re.match(r"(?i)^(before|after)\s+(.+)$", v)
            if bm:
                operand = bm.group(2).strip()
                is_abs_date = re.fullmatch(r"\d{4}([-/]\d{2}([-/]\d{2})?)?", operand)
                if is_number(operand) or is_abs_date:
                    op = "<" if bm.group(1).lower() == "before" else ">="
                    rhs = operand if is_number(operand) else q(operand)
                    return f"{col} {op} {rhs}"

            def _is_numeric_clause(s: str) -> bool:
                # A clause is numeric if it is a comparison, a numeric range, a bare number,
                # or an AND-range whose arms are ALL numeric comparisons. It must NOT count
                # a clause merely because it contains the word "and" ("red and blue" is a
                # string literal, not a numeric AND-range).
                s = s.strip()
                return bool(
                    re.fullmatch(_cmp, s) or self._numeric_range_bounds(s) or is_number(s) or _is_numeric_and_range(s)
                )

            # OR of numeric sub-filters, e.g. ">10 AND <=20 OR 90" or "3 to 10 OR 30 to 100"
            # (OR binds loosest). Range branches must go through _range_sql, not single(),
            # which only handles non-range tokens.
            if re.search(r"(?i)\sor\s", v):
                or_parts = [s.strip() for s in re.split(r"(?i)\s+or\s+", v)]
                if len(or_parts) > 1 and all(_is_numeric_clause(p) for p in or_parts):
                    rendered = []
                    for p in or_parts:
                        pr = self._numeric_range_bounds(p)
                        rendered.append(self._range_sql(pr, col) if pr else single(p))
                    return "(" + " OR ".join(rendered) + ")"

            # Numeric AND range in a single condition, e.g. ">1 AND <100". Each part
            # must FULLY be a numeric comparison, so values like "<=20 OR 90" (which
            # carry their own OR) are not misread as a clean AND range.
            if re.search(r"(?i)\sand\s", v):
                subs = [s.strip() for s in re.split(r"(?i)\s+and\s+", v)]
                if len(subs) > 1 and all(re.fullmatch(_cmp, s) for s in subs):
                    return "(" + " AND ".join(single(s) for s in subs) + ")"

            # comparison operators ">=", "<=", "!=", "<>", ">", "<"
            cm = re.match(r"^(>=|<=|!=|<>|>|<)\s*(.+)$", v)
            if cm:
                operator, operand = cm.group(1), cm.group(2).strip()
                if operator == "<>":
                    operator = "!="
                rhs = operand if is_number(operand) else q(operand)
                return f"{col} {operator} {rhs}"

            # wildcard LIKE
            if "%" in v or "_" in v:
                return f"{col} LIKE {q(v)}"

            # numeric equality (incl. negative numbers)
            if is_number(v):
                return f"{col} = {v}"

            # date/interval expression we cannot translate yet -> warn instead of
            # silently emitting a string equality that matches zero rows.
            if self._DATE_FILTER_RE.search(v):
                logger.warning(
                    "LookML date/interval filter %r on field %r is not translated to SQL; "
                    "emitting a literal equality (will not match as Looker intends).",
                    v,
                    field,
                )

            return f"{col} = {q(v)}"

        value = (value or "").strip()

        def _is_exclusion(p: str) -> bool:
            # A list token is an exclusion only when single() actually negates it -- the
            # "-FOO" dash form, or the word "not" applied to a null/empty/numeric/range/
            # comparison form. Word "not" before a bare STRING ("not started") is a literal
            # value (an include), not an exclusion.
            # Mirror single()'s neg_dash guard EXACTLY so the classifier agrees with what
            # single() actually negates: a leading dash excludes unless the token is a negative
            # NUMBER (-5, -.5) -- a value -- or a `-<op>` comparison form. A dot-prefixed string
            # like `-.csv` (exclude '.csv') IS an exclusion; the old `^-(\d|\.)` guard wrongly
            # rejected it, so the combiner ORed the exclusion in and admitted almost everything.
            if (
                p.startswith("-")
                and len(p) > 1
                and not self._filter_is_number(p)
                and not re.match(r"^-(?:>=|<=|!=|<>|>|<)", p)
            ):
                return True
            nm2 = re.match(r"(?i)^not\s+(.+)$", p)
            if not nm2:
                return False
            operand = nm2.group(1).strip()
            return bool(
                operand.upper() in ("NULL", "EMPTY")
                or is_number(operand)
                or self._numeric_range_bounds(operand)
                # Use the shared _cmp grammar (incl. bare fractions like `.5`) so a fractional
                # NOT-comparison such as `NOT >.5` is recognized as an exclusion, not a match.
                or re.fullmatch(_cmp, operand)
            )

        # Numeric interval / range: "[a,b]", "(a,b)", "a to b", "a to", "to b".
        single_range = self._numeric_range_bounds(value)
        if single_range:
            return self._range_sql(single_range, col)

        # Negated interval / range: "NOT (3,12)" / "NOT 3 to 12" -> inverted condition.
        # Handled here (before comma splitting) so the interval's comma is not split.
        not_range = re.match(r"(?i)^not\s+(.+)$", value)
        if not_range:
            nr = self._numeric_range_bounds(not_range.group(1).strip())
            if nr:
                return self._range_sql_negated(nr, col)

        # Numeric range LIST: bracket intervals ("[0,9],[20,29]") or "to"-ranges
        # ("1 to 10, 20 to 30"), optionally mixed with plain values and exclusions.
        # Split only on top-level commas so commas inside brackets are preserved. A
        # LEADING "NOT" negates the WHOLE list (De Morgan), so defer that to the comma-list
        # leading-NOT branch -- otherwise "NOT [0,10], 20 to 30" would mis-handle here.
        if "," in value and not re.match(r"(?i)^not\s", value):
            segs = self._split_top_level_commas(value)

            def _seg_is_rangeish(s: str) -> bool:
                # A positive interval ("[0,10]") OR a negated one ("NOT [0,10]"). The latter must
                # route through this bracket-aware branch too, else the naive split() fallback
                # below shatters the interval's inner comma into "NOT [0" / "10]" fragments.
                if self._numeric_range_bounds(s):
                    return True
                m = re.match(r"(?i)^not\s+(.+)$", s.strip())
                return bool(m and self._numeric_range_bounds(m.group(1).strip()))

            if any(_seg_is_rangeish(s) for s in segs):
                # OR the positive alternatives (ranges + plain values), AND the exclusions
                # ("NOT 20" / "-x"): Looker numeric lists combine alternatives then exclude.
                includes, excludes = [], []
                for s in segs:
                    r = self._numeric_range_bounds(s)
                    if r:
                        includes.append(self._range_sql(r, col))
                    elif _is_exclusion(s):
                        excludes.append(single(s))
                    else:
                        includes.append(single(s))
                clauses = []
                if includes:
                    clauses.append("(" + " OR ".join(includes) + ")" if len(includes) > 1 else includes[0])
                clauses.extend(excludes)
                return "(" + " AND ".join(clauses) + ")" if len(clauses) > 1 else clauses[0]

        # Comma-separated list
        if "," in value:
            parts = [p.strip() for p in value.split(",") if p.strip() != ""]

            def is_plain(p: str) -> bool:
                return (
                    not p.startswith("-")
                    and not re.match(r"^(>=|<=|!=|<>|>|<)", p)
                    and not re.match(r"(?i)^not\s", p)
                    and "%" not in p
                    and "_" not in p
                )

            def is_neg_plain(p: str) -> bool:
                return p.startswith("-") and "%" not in p[1:] and "_" not in p[1:] and not re.match(r"^-(\d|\.)", p)

            # A single leading "NOT" negates the whole list: "NOT 1, 2, 3" -> NOT IN (1, 2, 3).
            _flip = {">": "<=", ">=": "<", "<": ">=", "<=": ">", "!=": "=", "<>": "="}

            def negate(p: str) -> str | None:
                """SQL for NOT(p) where p is an AND-range, comparison, range, or number."""
                if _is_numeric_and_range(p):
                    # De Morgan: NOT(a AND b) -> (NOT a OR NOT b).
                    flipped = []
                    for s in re.split(r"(?i)\s+and\s+", p):
                        sm = re.match(r"^(>=|<=|!=|<>|>|<)\s*(.+)$", s.strip())
                        flipped.append(f"{col} {_flip[sm.group(1)]} {sm.group(2).strip()}")
                    return "(" + " OR ".join(flipped) + ")"
                cm2 = re.match(r"^(>=|<=|!=|<>|>|<)\s*(.+)$", p)
                if cm2:
                    op, operand = cm2.group(1), cm2.group(2).strip()
                    return f"{col} {_flip[op]} {operand if is_number(operand) else q(operand)}"
                rng = self._numeric_range_bounds(p)
                if rng:
                    return self._range_sql_negated(rng, col)
                if is_number(p):
                    return f"{col} != {p}"
                return None

            lead_not = re.match(r"(?i)^not\s+(.+)$", value)
            if lead_not:
                # Split at top level so an interval's inner comma (e.g. "NOT [0,10],20")
                # is not broken into "[0"/"10]" fragments.
                neg_parts = [p for p in self._split_top_level_commas(lead_not.group(1)) if p != ""]
                # Fast path: NOT IN for a list of plain simple values (no operators,
                # wildcards, or intervals -- those need per-item negation below).
                plain_simple = neg_parts and all(
                    is_plain(p) and "[" not in p and "(" not in p and not self._numeric_range_bounds(p)
                    for p in neg_parts
                )
                if plain_simple:
                    if all(is_number(p) for p in neg_parts):
                        return f"{col} NOT IN ({', '.join(neg_parts)})"
                    # String list: Looker negates strings with "-FOO", not the word
                    # "not", so a leading "not" is part of the first literal value
                    # (mirroring the single-token path). "not started,pending" ->
                    # IN ('not started', 'pending'), not NOT IN ('started','pending').
                    orig_segs = self._split_top_level_commas(value)
                    return f"{col} IN ({', '.join(q(s) for s in orig_segs)})"
                # Leading NOT over comparison/range/interval operands: negate each and
                # AND them (De Morgan), e.g. "NOT >1, [0,10]" -> (<=1 AND (<0 OR >10)).
                negated = [negate(p) for p in neg_parts]
                if neg_parts and all(n is not None for n in negated):
                    # A self-contradictory all-negated numeric list (e.g. "NOT >1, 2, <100" ->
                    # <=1 AND >=100, which no value satisfies) is documented by Looker to select
                    # NULLs instead: it writes `IS NULL`. Detect an EMPTY numeric intersection
                    # across the simple comparison clauses and emit that, else the always-false
                    # AND also excludes NULLs and undercounts. Skip when any clause is a
                    # disjunction (a negated range's `< a OR > b`) -- those need interval-union
                    # logic; keep the AND-join for them.
                    if all(" OR " not in n for n in negated):
                        lo = hi = None  # each: (value, inclusive?)
                        simple = True
                        for n in negated:
                            cm = re.match(rf"^{re.escape(col)}\s+(<=|>=|<|>|!=|<>)\s+(.+)$", n)
                            if not cm or not is_number(cm.group(2)):
                                simple = False
                                break
                            nop, nval = cm.group(1), float(cm.group(2))
                            if nop in (">=", ">"):
                                incl = nop == ">="
                                if lo is None or nval > lo[0] or (nval == lo[0] and not incl):
                                    lo = (nval, incl)
                            elif nop in ("<=", "<"):
                                incl = nop == "<="
                                if hi is None or nval < hi[0] or (nval == hi[0] and not incl):
                                    hi = (nval, incl)
                            # `!= x` excludes a point but does not bound the interval.
                        if simple and lo is not None and hi is not None:
                            empty = lo[0] > hi[0] or (lo[0] == hi[0] and not (lo[1] and hi[1]))
                            if empty:
                                return f"{col} IS NULL"
                    return "(" + " AND ".join(negated) + ")"

            if parts and all(is_plain(p) for p in parts):
                if all(is_number(p) for p in parts):
                    return f"{col} IN ({', '.join(parts)})"
                # If any part is a date/interval expression ("today, 7 days ago"), OR
                # them via single() so each hits the untranslated-date warning path
                # instead of silently emitting IN ('today', '7 days ago').
                if any(self._DATE_FILTER_RE.search(p) for p in parts):
                    return "(" + " OR ".join(single(p) for p in parts) + ")"
                return f"{col} IN ({', '.join(q(p) for p in parts)})"

            if parts and all(is_neg_plain(p) for p in parts):
                clean = [p[1:] for p in parts]
                if all(is_number(p) for p in clean):
                    return f"{col} NOT IN ({', '.join(clean)})"
                return f"{col} NOT IN ({', '.join(q(p) for p in clean)})"

            # Mixed list: OR the includes together, AND the exclusions (see _is_exclusion).
            includes, excludes = [], []
            for p in parts:
                cond = single(p)
                (excludes if _is_exclusion(p) else includes).append(cond)
            clauses = []
            if includes:
                clauses.append("(" + " OR ".join(includes) + ")" if len(includes) > 1 else includes[0])
            clauses.extend(excludes)
            return "(" + " AND ".join(clauses) + ")" if len(clauses) > 1 else clauses[0]

        return single(value)

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

        # Normalize self-view-qualified references (${this_view.field} -> ${field})
        # so they resolve like bare references instead of leaking literal ${...}.
        view_def = self._strip_self_view_qualifiers(view_def, name.lstrip("+"))

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

        # All declared dimension names (including compact dimensions with no explicit
        # sql), so ${ref}s to a compact dimension resolve to its default column rather
        # than leaking the literal ${name}.
        declared_dim_names: set[str] = {d.get("name") for d in dimension_defs if d.get("name")}
        for dim_group_def in view_def.get("dimension_groups") or []:
            group_name = dim_group_def.get("name")
            if group_name:
                for timeframe in dim_group_def.get("timeframes", ["date"]):
                    if timeframe != "raw":
                        declared_dim_names.add(f"{group_name}_{timeframe}")

        # Resolve any dimension-to-dimension references in the lookup
        # (e.g., line_total references quantity, unit_price, line_discount)
        resolved_dimension_sql: dict[str, str] = {}
        for dim_name, dim_sql in dimension_sql_lookup.items():
            resolved_sql = self._resolve_dimension_references(
                dim_sql, dimension_sql_lookup, dimension_names=declared_dim_names
            )
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
            dims = self._parse_dimension_group(dim_group_def, resolved_dimension_sql, declared_dim_names)
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
        # Full resolved aggregate SQL per measure (e.g. total -> "SUM({model}.amount)"),
        # used to expand a measure ref inside an aggregate-safe mixed number measure into
        # its base aggregate over the REAL column (not a phantom {model}.<measure>).
        measure_full_sql_lookup: dict[str, str] = {}
        # Base measures carrying their OWN LookML `filters`. A post-SQL measure (percent_of_total
        # / percent_of_previous) that references one must expand it through the FILTERED aggregate
        # in measure_full_sql_lookup, not the bare `<AGG>({model}.<measure>)` template -- which
        # would drop the filter and compute over every row.
        filtered_base_measures: set[str] = set()

        def _folded_measure_filter(m_def):
            # AND-joined predicate for a base measure's OWN filters, with each filter field
            # resolved through the dimension SQL ({model}.state -> ({model}.status) when the
            # dimension renames the column) so the folded aggregate hits the real column.
            conds, _ = self._measure_filter_conds(m_def, name.lstrip("+"))
            if not conds:
                return None
            resolved = []
            for c in conds:
                c = re.sub(
                    r"\{model\}\.(\w+)",
                    lambda mm: f"({resolved_dimension_sql[mm.group(1)]})"
                    if mm.group(1) in resolved_dimension_sql
                    else mm.group(0),
                    c,
                )
                resolved.append(f"({c})")
            return " AND ".join(resolved)

        for m in view_def.get("measures") or []:
            m_name = m.get("name")
            if not m_name:
                continue
            measure_names.add(m_name)
            m_type = m.get("type", "count")
            agg_template = self._SQL_AGG_FUNC.get(m_type)
            # An approximate count_distinct must aggregate approximately when wrapped
            # by a post-SQL measure (percent_of_total/previous), matching the direct metric.
            if m_type == "count_distinct" and m.get("approximate") in ("yes", True):
                agg_template = "APPROX_COUNT_DISTINCT({0})"
            if agg_template:
                measure_agg_lookup[m_name] = agg_template
                m_sql = m.get("sql")
                if m_sql:
                    col = self._resolve_dimension_references(
                        m_sql.replace("${TABLE}", "{model}"),
                        resolved_dimension_sql,
                        dimension_names=declared_dim_names,
                    )
                    # Fold the base measure's OWN LookML filters into its aggregate so a
                    # mixed-expr expansion of a filtered measure (e.g. completed_total with
                    # filters: [status: "completed"]) keeps the filter, not SUM(amount).
                    joined = _folded_measure_filter(m)
                    if joined:
                        filtered_base_measures.add(m_name)
                    if m_type == "count" and col.strip() == "*":
                        # `type: count sql: * ;;` is the row-count form; `*` can't live inside
                        # a CASE, so route it through the no-sql count path instead of emitting
                        # an invalid COUNT(CASE WHEN ... THEN * END).
                        measure_full_sql_lookup[m_name] = (
                            f"COUNT(CASE WHEN {joined} THEN 1 END)" if joined else "COUNT(*)"
                        )
                    else:
                        if joined:
                            col = f"CASE WHEN {joined} THEN {col} END"
                        measure_full_sql_lookup[m_name] = agg_template.format(col)
                elif m_type == "count":
                    # type: count with no sql -> COUNT(*); fold the measure's own filters
                    # so a filtered count (completed_count) keeps its filter in a mixed-expr
                    # expansion instead of counting all rows.
                    joined = _folded_measure_filter(m)
                    if joined:
                        filtered_base_measures.add(m_name)
                        measure_full_sql_lookup[m_name] = f"COUNT(CASE WHEN {joined} THEN 1 END)"
                    else:
                        measure_full_sql_lookup[m_name] = "COUNT(*)"
            elif m_type in ("sum_distinct", "average_distinct", "median_distinct", "percentile_distinct"):
                # Supported distinct aggregates: reuse _parse_distinct_measure's generated SQL
                # so a complete `type: number` expression that references one (e.g.
                # ${sum_distinct_total} / SUM(${amount})) can EXPAND it instead of being
                # dropped as unexpandable. Only expand UNFILTERED distinct measures: their
                # SQL (esp. the keyed symmetric-aggregate / quantile forms) has no single
                # foldable predicate slot, so a filtered distinct can't be expanded faithfully
                # -- leave it out so the referencing expr is skipped with a warning rather than
                # silently producing an UNfiltered distinct.
                if not self._measure_filter_conds(m, name.lstrip("+"))[0]:
                    dm = self._parse_distinct_measure(m_name, m_type, m, resolved_dimension_sql, declared_dim_names)
                    if dm and dm.sql:
                        measure_full_sql_lookup[m_name] = dm.sql

        # Second pass: also expand `type: number` measures into measure_full_sql_lookup so a
        # LATER number measure that references them can go through the complete-SQL path. Covers
        # (a) inline-aggregate "complete" measures (SUM(${amount}) with filters) and (b) pure
        # derived metric-of-metrics (${revenue} - ${cost}). Without this, referencing them either
        # drops the measure as unexpandable or silently loses a referenced measure's filter.
        # Iterate to a fixpoint so chains (avg_margin -> gross_margin -> revenue/cost) resolve
        # regardless of declaration order. filter_sensitive_measures tracks measures whose folded
        # filter a plain derived reference would drop -- a referencing expr must INLINE them.
        from sidemantic.sql.aggregation_detection import sql_has_aggregate

        number_measure_defs = {
            m["name"]: m
            for m in (view_def.get("measures") or [])
            if m.get("name") and m.get("type") == "number" and m.get("sql")
        }
        filter_sensitive_measures: set[str] = set()

        def _expand_number_measure(m_def):
            """Return (complete_sql, is_filter_sensitive) or None if not (yet) expandable."""
            raw = m_def["sql"].replace("${TABLE}", "{model}")
            refs = [(mm.group(1), mm.group(2)) for mm in self._REF_RE.finditer(raw)]
            # Cross-view refs and subqueries have no inline complete-SQL form.
            if any(v is not None and rn != "TABLE" for v, rn in refs) or self._has_subquery(raw):
                return None
            # Every measure ref must already be resolvable (a dim, a compact dim, or already in
            # the lookup); otherwise retry next round (it may be a later-added number measure).
            for v, rn in refs:
                if v is not None or rn == "TABLE" or rn in resolved_dimension_sql or rn in declared_dim_names:
                    continue
                if rn not in measure_full_sql_lookup:
                    return None

            def _sub(mm):
                v, rn = mm.group(1), mm.group(2)
                if v is None and rn == "TABLE":
                    return mm.group(0)
                if rn in resolved_dimension_sql:
                    return f"({resolved_dimension_sql[rn]})"
                if rn in declared_dim_names:
                    return f"({{model}}.{rn})"
                return f"({measure_full_sql_lookup[rn]})"

            expanded = self._REF_RE.sub(_sub, raw)
            # A valid measure-level expression must contain an aggregate (else it is a row-level
            # dimension expression, handled/skipped by _parse_measure itself).
            if not sql_has_aggregate(expanded.replace("{model}", "x")):
                return None
            # Apply the SAME aggregate-safety check _parse_measure uses, so a measure IT would
            # skip as invalid never lands in the lookup. `bad: ${total} + ${amount}` mixes an
            # aggregate measure with a RAW dimension: _parse_measure drops it, but without this
            # check the prepass would still cache `SUM(amount) + amount`, and a later
            # `outer: ${bad} / NULLIF(COUNT(*), 0)` would inline that raw ungrouped column and
            # fail on grouped queries instead of the unsupported measure being unavailable.
            if not self._mixed_is_aggregate_safe(
                raw,
                lambda rn: rn in resolved_dimension_sql or rn in declared_dim_names,
                resolved_dimension_sql,
            ):
                return None
            joined = _folded_measure_filter(m_def)
            if joined:
                # A filtered LIST(...) has no faithful form -- LIST keeps NULL inputs, so no
                # filtering strategy excludes a row (see _parse_measure, which skips these).
                # Without this the prepass would fold only the OTHER aggregates and cache a
                # PARTIALLY filtered SQL, so a later measure referencing it would inline an
                # unfiltered LIST over a filtered denominator. Keep prepass and parser in step.
                if self._sql_has_list_aggregate(expanded):
                    return None
                # force=True: this SQL will be INLINED into a referencing measure with no
                # generator column-nulling, so the filter must be baked into a CASE unconditionally.
                folded = self._fold_complete_sql_filters(expanded, [joined], force=True)
                if folded is None:
                    return None  # can't fold the filter safely -> leave unexpandable
                return folded, True
            return expanded, False

        _expanding = True
        while _expanding:
            _expanding = False
            for m_name, m_def in number_measure_defs.items():
                if m_name in measure_full_sql_lookup:
                    continue
                result = _expand_number_measure(m_def)
                if result is None:
                    continue
                measure_full_sql_lookup[m_name], _sensitive = result
                if _sensitive:
                    filter_sensitive_measures.add(m_name)
                _expanding = True

        # Parse measures with dimension SQL lookup for reference resolution
        measures = []
        for measure_def in view_def.get("measures") or []:
            measure = self._parse_measure(
                measure_def,
                dimension_names,
                resolved_dimension_sql,
                measure_names,
                measure_agg_lookup,
                measure_full_sql_lookup,
                view_name=name.lstrip("+"),
                filter_sensitive_measures=filter_sensitive_measures,
                filtered_base_measures=filtered_base_measures,
            )
            if measure and self._leaks_cross_view_ref(measure.sql):
                # Like a cross-view dimension, a measure whose SQL references another view
                # inline (${other_view.field}) would leak that literal into the model CTE
                # (SELECT ${customers.balance} ...). Drop it; the fixpoint below then drops any
                # measure that depended on it.
                logger.warning(
                    "LookML measure %r references another view inline (%s), which sidemantic "
                    "cannot represent; dropping the measure instead of importing unqueryable SQL.",
                    measure.name,
                    measure.sql,
                )
                measure = None
            if measure:
                measures.append(measure)

        # Drop measures that reference a measure which did NOT survive parsing. A row-level helper
        # (`bad { sql: ${amount} }`) is skipped above, but a dependent (`outer { sql: ${bad} * 2 }`)
        # still resolved ${bad} to a bare `bad` and imported as `bad * 2`, which compile cannot
        # resolve ("Metric bad not found"). Collect each measure's UNQUALIFIED measure refs from the
        # raw SQL, then iterate to a fixpoint so a dependent of a dependent is dropped too.
        _measure_refs: dict[str, set[str]] = {}
        for measure_def in view_def.get("measures") or []:
            _mn = measure_def.get("name")
            if not _mn:
                continue
            _measure_refs[_mn] = {
                _r.group(2)
                for _r in self._REF_RE.finditer(measure_def.get("sql") or "")
                if _r.group(1) is None and _r.group(2) in measure_names and _r.group(2) != _mn
            }
        _surviving = {m.name for m in measures}
        _changed = True
        while _changed:
            _changed = False
            for _m in list(measures):
                _dead = {r for r in _measure_refs.get(_m.name, set()) if r not in _surviving}
                if _dead:
                    logger.warning(
                        "LookML measure %r references measure(s) %s that were not imported "
                        "(e.g. a skipped row-level helper); skipping it too.",
                        _m.name,
                        ", ".join(sorted(_dead)),
                    )
                    measures.remove(_m)
                    _surviving.discard(_m.name)
                    _changed = True

        # Parse segments
        from sidemantic.core.segment import Segment

        segments = []
        for segment_def in view_def.get("filters") or []:
            # LookML filters at view level can be used as segments
            segment_name = segment_def.get("name")
            segment_sql = segment_def.get("sql")
            if segment_name and segment_sql:
                # Resolve ${field} references (incl. self-qualified ones normalized above) through
                # the dimension SQL, exactly like dimensions/measures -- otherwise a segment such as
                # `${orders.status} = 'completed'` leaks an unresolved ${...} into the WHERE clause.
                segment_sql = self._resolve_dimension_references(
                    segment_sql, resolved_dimension_sql, dimension_names=declared_dim_names
                )
                # Replace ${TABLE} with {model} placeholder
                segment_sql = segment_sql.replace("${TABLE}", "{model}")
                # A cross-view reference cannot be represented inline, so an unresolved
                # ${other_view.field} would leak into the WHERE clause when the segment is used.
                # Drop the segment rather than import an unqueryable one, mirroring how
                # dimensions/measures with the same leak are dropped.
                if self._leaks_cross_view_ref(segment_sql):
                    logger.warning(
                        "LookML segment %r references another view inline (%s), which sidemantic "
                        "cannot represent; dropping the segment instead of importing unqueryable SQL.",
                        segment_name,
                        segment_sql,
                    )
                    continue
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

    # A ${...} reference containing a dot is a cross-view reference (${other_view.field}).
    # ${TABLE} has no dot inside the braces, and self-view qualifiers are normalized away
    # before resolution, so any remaining dotted ${...} is an unresolvable cross-view ref
    # that sidemantic cannot represent inline.
    _CROSS_VIEW_REF_RE = re.compile(r"\$\{[^}]*\.[^}]*\}")

    @classmethod
    def _leaks_cross_view_ref(cls, sql: str | None) -> bool:
        """True if ``sql`` still carries an unresolved cross-view ``${view.field}`` reference.

        A resolved reference is a plain/qualified column or a parenthesized expression; a
        remaining dotted ``${...}`` would leak the literal into generated SQL (e.g.
        ``SELECT ${customers.name} ...``), a guaranteed syntax error.
        """
        return bool(sql) and bool(cls._CROSS_VIEW_REF_RE.search(sql))

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

        # A queryable dimension whose SQL still references another view inline
        # (${other_view.field}) would emit that literal into the model CTE -- e.g.
        # SELECT ${customers.name} AS cust_name FROM orders -- so every query touching it
        # fails with invalid SQL. Sidemantic has no inline cross-model column, so drop the
        # dimension rather than import an unqueryable field.
        if self._leaks_cross_view_ref(sql):
            logger.warning(
                "LookML dimension %r references another view inline (%s), which sidemantic "
                "cannot represent; dropping the dimension instead of importing unqueryable SQL.",
                name,
                sql,
            )
            return None

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
        self,
        dim_group_def: dict,
        dimension_sql_lookup: dict[str, str] | None = None,
        dimension_names: set[str] | None = None,
    ) -> list[Dimension]:
        """Parse LookML dimension_group (time dimensions).

        Args:
            dim_group_def: Dimension group definition
            dimension_sql_lookup: Optional dict of dimension names to resolved SQL
            dimension_names: All declared dimension names, so a ${ref} to a COMPACT dimension (no
                explicit sql) resolves to its default column instead of leaking the literal

        Returns:
            List of time dimensions with different granularities
        """
        group_name = dim_group_def.get("name")
        if not group_name:
            return []

        group_type = dim_group_def.get("type", "time")

        # Handle duration type separately
        if group_type == "duration":
            return self._parse_duration_group(group_name, dim_group_def, dimension_sql_lookup, dimension_names)

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

        # A dimension_group whose base SQL references another view inline (${other.ts})
        # would leak that literal into every generated timeframe field, so drop the whole
        # group rather than import unqueryable time dimensions (see _parse_dimension).
        if self._leaks_cross_view_ref(base_sql):
            logger.warning(
                "LookML dimension_group %r references another view inline (%s), which sidemantic "
                "cannot represent; dropping the group instead of importing unqueryable SQL.",
                group_name,
                base_sql,
            )
            return []

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

    def _parse_duration_group(
        self,
        group_name: str,
        dim_group_def: dict,
        dimension_sql_lookup: dict[str, str] | None = None,
        dimension_names: set[str] | None = None,
    ) -> list[Dimension]:
        """Parse LookML dimension_group with type: duration.

        Duration dimension groups calculate the difference between two timestamps
        in various intervals (seconds, minutes, hours, days, weeks, months, years).

        Args:
            group_name: Name of the dimension group
            dim_group_def: Dimension group definition
            dimension_sql_lookup: Resolved dimension SQL for ${ref} resolution in sql_start/sql_end
            dimension_names: All declared dimension names, so a ${ref} to a COMPACT dimension (no
                explicit sql) resolves to its default column rather than leaking the literal

        Returns:
            List of duration dimensions
        """
        intervals = dim_group_def.get("intervals", ["day"])
        sql_start = dim_group_def.get("sql_start", "")
        sql_end = dim_group_def.get("sql_end", "")

        # Resolve ${dimension} references (self-view refs normalized to bare ${name}) so a
        # sql_start/sql_end like ${started_at} becomes the real column instead of leaking the
        # literal ${...} into DATE_DIFF; ${TABLE} is handled next. Pass dimension_names so a ref to
        # a COMPACT dimension (declared with no sql) resolves to its default column, not a leak.
        # Without this the duration dimension carried an unresolved ${ref} and any query on it
        # emitted invalid SQL.
        if sql_start:
            sql_start = self._resolve_dimension_references(
                sql_start, dimension_sql_lookup or {}, dimension_names=dimension_names
            ).replace("${TABLE}", "{model}")
        if sql_end:
            sql_end = self._resolve_dimension_references(
                sql_end, dimension_sql_lookup or {}, dimension_names=dimension_names
            ).replace("${TABLE}", "{model}")

        # If no sql_start/sql_end, we can't create duration dimensions
        if not sql_start or not sql_end:
            return []

        # A cross-view reference cannot be represented inline, so an unresolved ${other.field} would
        # still leak. Drop the whole group rather than import unqueryable duration dimensions
        # (mirrors _parse_dimension / _parse_dimension_group).
        if self._leaks_cross_view_ref(sql_start) or self._leaks_cross_view_ref(sql_end):
            logger.warning(
                "LookML duration group %r references another view inline (sql_start=%r sql_end=%r), which "
                "sidemantic cannot represent; dropping the group instead of importing unqueryable SQL.",
                group_name,
                sql_start,
                sql_end,
            )
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
        measure_full_sql_lookup: dict[str, str] | None = None,
        view_name: str | None = None,
        filter_sensitive_measures: set[str] | None = None,
        filtered_base_measures: set[str] | None = None,
    ) -> Metric | None:
        """Parse LookML measure.

        Args:
            measure_def: Metric definition
            dimension_names: Set of dimension names in this view (for reference resolution)
            dimension_sql_lookup: Dict mapping dimension names to their resolved SQL
            measure_names: Set of measure names in this view (for base-measure resolution)
            measure_agg_lookup: Dict mapping base measure names to their SQL aggregate template
            filter_sensitive_measures: Measures whose folded filter a plain derived reference
                would drop, so a referencing number measure must INLINE them (complete path).

        Returns:
            Metric instance or None
        """
        name = measure_def.get("name")
        if not name:
            return None

        dimension_names = dimension_names or set()
        dimension_sql_lookup = dimension_sql_lookup or {}
        measure_agg_lookup = measure_agg_lookup or {}
        measure_full_sql_lookup = measure_full_sql_lookup or {}
        filter_sensitive_measures = filter_sensitive_measures or set()
        filtered_base_measures = filtered_base_measures or set()

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
            sql = self._resolve_dimension_references(sql, dimension_sql_lookup or {}, dimension_names=dimension_names)
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
                sql = self._resolve_dimension_references(
                    sql, dimension_sql_lookup or {}, dimension_names=dimension_names
                )
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
            return self._parse_distinct_measure(name, measure_type, measure_def, dimension_sql_lookup, dimension_names)

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
                measure_full_sql_lookup,
                filtered_base_measures,
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

        # Looker's `approximate: yes` on a count_distinct -> approximate distinct count.
        if agg_type == "count_distinct" and measure_def.get("approximate") in ("yes", True):
            agg_type = "approx_count_distinct"

        # Parse filters - lkml parses these as filters__all
        # There are TWO different filter syntaxes in LookML:
        # 1. Shorthand: filters: [status: "completed"]
        #    -> lkml returns [[{'status': 'completed'}]]
        # 2. Block syntax: filters: { field: x value: y }
        #    -> lkml returns [{'field': 'flight_length', 'value': '>120'}]
        # We need to handle both formats.
        filters, _filters_cross_view = self._measure_filter_conds(measure_def, view_name)
        if _filters_cross_view:
            # A filter references another view inline, which the model CTE cannot resolve; drop the
            # measure rather than import one whose filter emits invalid SQL (mirrors the cross-view
            # inline-sql drop).
            logger.warning(
                "LookML measure %r has a filter referencing another view inline, which sidemantic "
                "cannot represent; dropping the measure instead of importing unqueryable SQL.",
                measure_def.get("name"),
            )
            return None

        # Replace ${TABLE} and resolve ${dimension_ref} placeholders in SQL
        number_refs_only_columns = False
        sql = measure_def.get("sql")
        if sql:
            sql = sql.replace("${TABLE}", "{model}")

            if measure_type == "number":
                # For derived measures (type: number), convert ${measure_name} references
                # to plain measure_name for sidemantic's dependency resolution.
                # We need to distinguish measure references from dimension references:
                # - ${measure_name} where measure_name is NOT a dimension -> plain measure_name
                # - ${dimension_name} -> resolved SQL from dimension
                # Pre-scan the refs: a derived metric is a metric-of-metrics, so it can
                # carry measure references OR raw dimension columns. A mix is representable
                # ONLY when every dimension column ends up inside an aggregate (so there is
                # no raw, ungrouped column); then we expand each measure ref to its base
                # aggregate SQL and emit opaque complete SQL. A mix with a raw ungrouped
                # column has no valid SQL form and is skipped with a warning.
                def _is_dim_ref(rn):
                    return rn in dimension_sql_lookup or rn in dimension_names

                referenced_measure = False
                referenced_dimension = False
                for _m in self._REF_RE.finditer(sql):
                    _v, _rn = _m.group(1), _m.group(2)
                    if _v is not None or _rn == "TABLE":
                        continue
                    if _is_dim_ref(_rn):
                        referenced_dimension = True
                    else:
                        referenced_measure = True

                from sidemantic.sql.aggregation_detection import sql_has_aggregate

                # An expression needs OPAQUE/complete SQL (not a metric-of-metrics derived
                # metric) when it mixes a measure ref with a dimension ref, OR it contains
                # an INLINE aggregate (SUM(...)/COUNT(*)/...). In both, the derived-metric
                # path mishandles it: a raw dimension column or an inline aggregate makes
                # the generator skip dependency replacement, leaving bare measure tokens
                # that reference nonexistent columns. So expand measure refs to their base
                # aggregate SQL and mark the whole thing complete. Pure metric-of-metrics
                # ratios (no inline aggregate, no dimension) stay derived.
                # Detect inline aggregates on a ref-NEUTRALIZED copy: raw ${ref} placeholders
                # break sqlglot's parser, forcing the regex fallback, which misses multi-word
                # aggregates like PERCENTILE_CONT(...) WITHIN GROUP (ORDER BY ${x}). Replacing
                # every ${...}/{model} with a plain identifier lets sqlglot see the real agg.
                has_inline_agg = sql_has_aggregate(self._REF_RE.sub("x", sql).replace("{model}", "x"))
                # A reference to a FILTER-SENSITIVE measure (one whose folded filter a plain
                # derived dependency would drop) must be inlined through the complete-SQL path,
                # not left as a bare metric-of-metrics ref -- else e.g. `${completed_sum} * 2`
                # silently expands to SUM(amount)*2 over ALL rows, ignoring completed_sum's filter.
                refs_filter_sensitive = any(
                    _m.group(1) is None and _m.group(2) != "TABLE" and _m.group(2) in filter_sensitive_measures
                    for _m in self._REF_RE.finditer(sql)
                )
                needs_complete = (
                    (referenced_measure and referenced_dimension) or has_inline_agg or refs_filter_sensitive
                )
                expand_measures = needs_complete and referenced_measure
                if needs_complete:
                    if self._has_subquery(sql):
                        # A scalar subquery in the expr can't go through the complete-SQL
                        # path: its builder rewrites EVERY parsed column -- including columns
                        # INSIDE the subquery -- to this measure's CTE raw alias, producing a
                        # wrong correlated query. No faithful form, so skip on import.
                        logger.warning(
                            "LookML number measure %r contains a subquery, which the complete-SQL "
                            "path cannot represent (it would rewrite the subquery's columns); "
                            "skipping on import.",
                            name,
                        )
                        return None
                    cross_view = any(
                        m.group(1) is not None and m.group(2) != "TABLE" for m in self._REF_RE.finditer(sql)
                    )
                    unexpandable = expand_measures and any(
                        m.group(1) is None
                        and m.group(2) != "TABLE"
                        and not _is_dim_ref(m.group(2))
                        and m.group(2) not in measure_full_sql_lookup
                        for m in self._REF_RE.finditer(sql)
                    )
                    if (
                        cross_view
                        or unexpandable
                        or not self._mixed_is_aggregate_safe(sql, _is_dim_ref, dimension_sql_lookup)
                    ):
                        logger.warning(
                            "LookML number measure %r combines a measure/dimension reference with "
                            "a raw ungrouped column or an unsupported reference, which has no valid "
                            "aggregate SQL form; skipping on import.",
                            name,
                        )
                        return None

                def resolve_reference(match):
                    view, ref_name = match.group(1), match.group(2)
                    if view is None and ref_name == "TABLE":
                        return match.group(0)
                    if view is not None:
                        # Cross-view ref (self-view already normalized away): sidemantic
                        # cannot represent an inline cross-model column, so leave the
                        # literal and warn rather than emitting a column the derived
                        # metric builder can't resolve (it would fail with "no join path").
                        logger.warning(
                            "LookML cross-view reference ${%s.%s} is not supported (sidemantic "
                            "has no inline cross-model column); left unresolved.",
                            view,
                            ref_name,
                        )
                        return match.group(0)
                    if ref_name in dimension_sql_lookup:
                        # It's a dimension reference - use the resolved SQL
                        return f"({dimension_sql_lookup[ref_name]})"
                    if ref_name in dimension_names:
                        # Compact dimension (no explicit sql) -> its default column.
                        return f"({{model}}.{ref_name})"
                    if expand_measures:
                        # Complete expr: expand the measure ref to its base aggregate over
                        # the REAL column so the whole thing is valid opaque SQL (e.g.
                        # total -> SUM({model}.amount)).
                        return f"({measure_full_sql_lookup[ref_name]})"
                    # It's a measure reference - use plain measure_name; the
                    # dependency analyzer will resolve this.
                    return ref_name

                sql = self._REF_RE.sub(resolve_reference, sql)

                # Re-check the RESOLVED expression for a SUBQUERY: a dimension ref may have EXPANDED
                # into one, and the pre-resolution guard above only saw the raw ${refs}. The
                # complete-SQL builder rewrites EVERY column -- including those INSIDE the subquery --
                # to this measure's CTE raw aliases, producing wrong correlated SQL, so skip it.
                if needs_complete and self._has_subquery(sql):
                    logger.warning(
                        "LookML number measure %r resolves to a scalar subquery (via a dimension "
                        "reference), which the complete-SQL path cannot represent; skipping on import.",
                        name,
                    )
                    return None

                # A number measure that references ONLY raw dimension columns with NO
                # aggregate (e.g. ${amount} / 2) is a row-level expression, not a valid
                # aggregate measure: as a metric it returns one value per input row, not a
                # scalar. It belongs as a dimension, so skip it with a warning rather than
                # emit a measure with wrong cardinality.
                if not needs_complete and not referenced_measure and referenced_dimension and bool(sql):
                    logger.warning(
                        "LookML number measure %r is a row-level dimension expression (no "
                        "aggregate); it would return one row per input row, so it is skipped "
                        "on import (define it as a dimension instead).",
                        name,
                    )
                    return None
                # Complete exprs (mixed or inline-aggregate, measure refs expanded above)
                # are opaque SQL; pure metric-of-metrics ratios stay derived metrics.
                number_refs_only_columns = needs_complete
            else:
                # For regular aggregation measures (sum, avg, count_distinct, etc.),
                # resolve dimension references to their SQL expressions
                sql = self._resolve_dimension_references(sql, dimension_sql_lookup, dimension_names=dimension_names)

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

        # A filtered LIST(...) aggregate has NO faithful portable form. Unlike every other
        # aggregate, DuckDB's LIST KEEPS NULL inputs, so neither filtering strategy excludes a
        # row: the generator's column-nulling and a folded CASE both leave one NULL element per
        # non-matching row (LIST(CASE WHEN s='x' THEN amount END) over 3 rows -> [1, NULL, 3]),
        # so ARRAY_LENGTH still counts them. Only a dialect-specific FILTER (WHERE ...) clause
        # would work. Skip rather than silently import a measure that ignores its filter.
        if number_refs_only_columns and filters and self._sql_has_list_aggregate(sql):
            logger.warning(
                "LookML number measure %r combines a LIST(...) aggregate with filters, which has "
                "no faithful form (LIST keeps NULL inputs, so neither column-nulling nor a folded "
                "CASE excludes a row); skipping on import.",
                name,
            )
            return None

        # A COMPLETE (opaque) measure's filters are applied by the generator without
        # resolving dimension refs (it just strips {model}), so resolve {model}.<dim> to
        # the dimension's real column SQL here -- a renamed dimension's filter must hit the
        # actual column (status), not the dimension name (state).
        if number_refs_only_columns and filters:
            filters = [
                re.sub(
                    r"\{model\}\.(\w+)",
                    lambda mm: f"({dimension_sql_lookup[mm.group(1)]})"
                    if mm.group(1) in dimension_sql_lookup
                    else mm.group(0),
                    f,
                )
                for f in filters
            ]
            # A filter field that is a local alias for a DROPPED cross-view dimension expands to its
            # leaked ${other_view.field} SQL above, which the generator would emit into the model
            # CTE. The measure.sql leak check does not see filters, so reject the measure here.
            if any(self._leaks_cross_view_ref(f) for f in filters):
                logger.warning(
                    "LookML measure %r has a filter that expands to a cross-view reference, which "
                    "sidemantic cannot represent; dropping the measure instead of importing "
                    "unqueryable SQL.",
                    name,
                )
                return None
            # The generator filters a complete-SQL measure by nulling the raw columns its SQL
            # references; that drops the filter for a zero-column aggregate (COUNT(*)) and
            # corrupts a NULL-test predicate (status IS NULL). Fold into the aggregate instead.
            folded = self._fold_complete_sql_filters(sql, filters)
            if folded is not None:
                sql = folded
                filters = None
            elif not self._generator_column_nulling_suffices(sql):
                # Folding aborted AND column-nulling cannot apply the filter (a zero-column /
                # windowed aggregate has no column to null), so keeping the filter would silently
                # count every row. Drop the measure rather than import ineffective filters.
                logger.warning(
                    "LookML measure %r has filters that can be applied neither by folding nor by "
                    "generator column-nulling (e.g. a zero-column windowed aggregate); dropping it.",
                    name,
                )
                return None

        return Metric(
            name=name,
            type=metric_type,
            agg=agg_type,
            sql=sql,
            sql_is_complete=number_refs_only_columns,
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
        dimension_names: set[str] | None = None,
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
        sql = self._resolve_dimension_references(sql, dimension_sql_lookup, dimension_names=dimension_names)

        sql_distinct_key = measure_def.get("sql_distinct_key")
        if sql_distinct_key:
            sql_distinct_key = sql_distinct_key.replace("${TABLE}", "{model}")
            sql_distinct_key = self._resolve_dimension_references(
                sql_distinct_key, dimension_sql_lookup, dimension_names=dimension_names
            )

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
        measure_full_sql_lookup: dict[str, str] | None = None,
        filtered_base_measures: set[str] | None = None,
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
        measure_full_sql_lookup = measure_full_sql_lookup or {}
        filtered_base_measures = filtered_base_measures or set()
        sql = sql.replace("${TABLE}", "{model}")

        def _resolve(match: re.Match) -> str:
            view, ref_name = match.group(1), match.group(2)
            if view is None and ref_name == "TABLE":
                return match.group(0)
            if view is not None:
                # Cross-view reference (self-view already normalized away): sidemantic
                # has no inline cross-model column, so leave the literal and warn.
                logger.warning(
                    "LookML cross-view reference ${%s.%s} is not supported (sidemantic "
                    "has no inline cross-model column); left unresolved.",
                    view,
                    ref_name,
                )
                return match.group(0)
            if ref_name in dimension_sql_lookup:
                return f"({dimension_sql_lookup[ref_name]})"
            if ref_name in measure_names:
                # A base measure with its OWN LookML `filters` must expand to the FILTERED
                # aggregate built in the first pass (e.g. APPROX_COUNT_DISTINCT(CASE WHEN
                # status='completed' THEN user_id END)). The `<AGG>({model}.<measure>)` template
                # below carries no filter, so a percent_of_total over a filtered base would be
                # computed across every row instead of the filtered population.
                if ref_name in filtered_base_measures and ref_name in measure_full_sql_lookup:
                    return f"({measure_full_sql_lookup[ref_name]})"
                agg_template = measure_agg_lookup.get(ref_name)
                if agg_template:
                    return agg_template.format(f"{{model}}.{ref_name}")
                return f"{{model}}.{ref_name}"
            return ref_name

        return self._REF_RE.sub(_resolve, sql)

    def _parse_post_sql_measure(
        self,
        name: str,
        measure_type: str,
        measure_def: dict,
        dimension_sql_lookup: dict[str, str],
        measure_names: set[str] | None = None,
        measure_agg_lookup: dict[str, str] | None = None,
        measure_full_sql_lookup: dict[str, str] | None = None,
        filtered_base_measures: set[str] | None = None,
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
        measure_full_sql_lookup = measure_full_sql_lookup or {}
        filtered_base_measures = filtered_base_measures or set()

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
        base = self._resolve_measure_reference_sql(
            sql,
            dimension_sql_lookup,
            measure_names,
            measure_agg_lookup,
            measure_full_sql_lookup,
            filtered_base_measures,
        ).strip()

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

    @classmethod
    def _fold_filter_conds(cls, filters: list[str], model: Model) -> str:
        """Resolve ``metric.filters`` into an AND-joined, ``${TABLE}``-qualified SQL
        predicate for folding into an exported aggregate measure.

        Field refs are resolved through the model's dimension SQL so a renamed column is
        used, not a bare name. Three forms are handled: ``{model}.col``, the model's own
        name ``orders.col``, and an UNqualified dimension name used as a column
        (``status = 'done'``, matched only before a comparison operator so string VALUES
        aren't rewritten). Each filter is parenthesized so a filter containing ``OR`` is
        not broken by ``AND``'s higher precedence.
        """
        dim_sql = {d.name: d.sql for d in model.dimensions if d.sql}
        dim_names = {d.name for d in model.dimensions}

        def _qualify(val: str) -> str:
            # Bare column -> qualify with {model}. so it stays unambiguous in joins;
            # any resolved expression is parenthesized to preserve precedence.
            if re.fullmatch(r"\w+", val):
                return f"({{model}}.{val})"
            return f"({val})"

        # Qualified ref (group 1), OR a bare known-dimension name (group 2) used as a
        # column anywhere (incl. inside a function like LOWER(status)). Matching is done
        # only OUTSIDE single-quoted string literals (see _resolve), so a quoted value
        # that happens to equal a dimension name is never rewritten. Both alternatives use a
        # negative lookbehind for `.`/word-char: the bare one so it does NOT match the field
        # of a foreign qualifier (`status` inside `customers.status`), and the model-name one
        # so it does NOT match a schema-qualified ref (`orders.status` inside
        # `schema.orders.status`). The bare alt also has negative lookaheads: for `(` so it does
        # NOT match a function name (e.g. `date(...)`), and for `.` so it does NOT match a table
        # QUALIFIER (`customers` inside `customers.status` on a model that also has a `customers`
        # dimension) -- the lookbehind only guards the field AFTER a dot, not the name before it,
        # which would otherwise emit `(${TABLE}.customer_id).status`.
        names_alt = "|".join(re.escape(n) for n in sorted(dim_names, key=len, reverse=True))
        pattern = rf"(?:\{{model\}}|(?<![\w.]){re.escape(model.name)})\.(\w+)"
        if names_alt:
            pattern += rf"|(?<![\w.])({names_alt})\b(?!\s*\()(?!\s*\.)"
        ref_re = re.compile(pattern)

        def _resolve(fstr: str) -> str:
            def _make_one(base: int):
                # `base` is this segment's absolute offset into fstr, so context checks can look
                # at the FULL predicate -- not just the current split segment. A quoted number
                # (`INTERVAL '7' day`) splits `day` into its own segment, so a segment-local `pre`
                # would miss the leading INTERVAL and wrongly rewrite the unit keyword.
                def _one(m):
                    # The bare-dimension alternative (group 2) only exists when names_alt is
                    # non-empty; with no declared dimensions the pattern has a single group, so
                    # read group 2 defensively (m.group(2) would raise IndexError otherwise).
                    bare = m.group(2) if m.re.groups >= 2 else None
                    if bare is not None:
                        # Bare dimension-name alternative: skip when it sits in a SQL TYPE context
                        # (a cast target), not a column operand -- e.g. CAST(x AS date) or x::date
                        # with a `date` dimension. Rewriting the type token to a column would emit
                        # invalid SQL like CAST(x AS (${TABLE}.order_date)). (Typed literals like
                        # `date '2024-01-01'` are protected earlier, in the split below.)
                        pre = fstr[: base + m.start()]
                        if re.search(r"(?is)\bAS\s+$", pre) or pre.rstrip().endswith("::"):
                            return m.group(0)
                        # Skip a bare token in a DATE-PART / INTERVAL-UNIT keyword position, not a
                        # column operand -- e.g. EXTRACT(day FROM ...) or `INTERVAL 7 day` on a
                        # model with a `day` dimension. Rewriting the keyword to the dimension SQL
                        # emits invalid SQL like EXTRACT((${TABLE}.order_day) FROM ...). Positions:
                        # right after EXTRACT(, immediately before an extract's FROM, or as the unit
                        # following an INTERVAL <number|'literal'>. (Quoted forms like
                        # DATE_TRUNC('day', ...) and INTERVAL '7 day' are already protected as
                        # string literals.)
                        suf = fstr[base + m.end() :]
                        if (
                            re.search(r"(?i)\bextract\s*\(\s*$", pre)
                            or re.match(r"(?is)\s+from\b", suf)
                            or re.search(r"(?i)\binterval\s+(?:[+-]?\d+(?:\.\d+)?|'(?:[^']|'')*')\s*$", pre)
                            # A date-part KEYWORD in the date-part ARGUMENT SLOT of a date/time
                            # call, e.g. BigQuery's DATE_TRUNC(created_at, month) or
                            # DATE_DIFF(a, b, day). Position-aware so a real column in another
                            # slot of the SAME call still resolves -- DATE_TRUNC(date, month) is
                            # column `date` truncated to part `month`.
                            or cls._is_date_part_argument(pre, suf, bare)
                        ):
                            return m.group(0)
                    name = m.group(1) or bare
                    return _qualify(dim_sql.get(name, name))

                return _one

            # Split out (and thus protect from rewriting) SQL TYPED LITERALS whose type keyword
            # equals a dimension name (`date '2024-01-01'`, `timestamp '...'`, `interval '...'`)
            # -- the whole `<type> '...'` unit is kept intact so the leading `date`/`time`/etc.
            # is not mistaken for a column; then single-quoted string literals, double-quoted
            # identifiers (doubled-quote escapes), backtick (BigQuery/MySQL) and [bracket] (SQL
            # Server) quoted identifiers, AND Liquid/Jinja template segments ({{ }} / {% %}).
            # Rewrite refs only in the remaining (even-index) segments so string VALUES, quoted
            # identifiers, typed literals, and template variables are untouched. The template
            # patterns require DOUBLE braces / brace-percent, so the single-brace {model} is safe.
            parts = re.split(
                r"""((?i:\b(?:date|time|timestamp|timestamptz|datetime|interval)\s+'(?:[^']|'')*')"""
                r"""|'(?:[^']|'')*'|"(?:[^"]|"")*"|`[^`]*`|\[[^\]]*\]|\{\{.*?\}\}|\{%.*?%\})""",
                fstr,
            )
            offset = 0
            for i, part in enumerate(parts):
                if i % 2 == 0:
                    parts[i] = ref_re.sub(_make_one(offset), part)
                offset += len(part)
            return "".join(parts)

        return " AND ".join("(" + _resolve(f).replace("{model}", "${TABLE}") + ")" for f in filters)

    @staticmethod
    def _aggregate_references_column(sql: str) -> bool:
        """True if ``sql`` references at least one COLUMN (not just constants/functions).

        A zero-column aggregate (``COUNT(NULL)``, ``COUNT(DISTINCT 1)``, ``SUM(1)``) exported as a
        LookML ``type: number`` re-imports as an opaque complete-SQL metric whose referenced-column
        set is empty, so compiling it builds an empty model CTE (``SELECT ... FROM`` with no select
        list). Callers use this to skip such measures. On a parse failure assume it DOES reference a
        column, so a genuine (unparseable) column expression is not wrongly dropped.

        Liquid/Jinja segments are neutralised to NULL first: a template is not a column, and left
        in place it makes sqlglot fail, which the fallback above would read as "has columns" -- so
        a folded filter that is ONLY a template (``COUNT(CASE WHEN ({{ user_filter }}) THEN 1 END)``)
        would slip past the zero-column guard and export a measure with no real column.
        """
        import sqlglot
        from sqlglot import expressions as exp

        neutralised = re.sub(r"\{\{.*?\}\}|\{%.*?%\}", "NULL", sql or "")
        # sqlglot cannot parse an aggregate's ALL modifier (COUNT(ALL x)), which would send every
        # such expression to the has-columns fallback below instead of a real check. ALL is the
        # default modifier and irrelevant to which columns are referenced, so drop it first.
        # `(` must precede it, so `= ALL (SELECT ...)` is untouched.
        neutralised = re.sub(r"(?i)\(\s*ALL\s+", "(", neutralised)
        try:
            tree = sqlglot.parse_one(neutralised.replace("{model}", "__m__").replace("${TABLE}", "__m__"))
        except Exception:
            return True
        return any(True for _ in tree.find_all(exp.Column))

    @classmethod
    def _fold_filters_into_aggregate(cls, agg_sql: str, filters: list[str], model: Model) -> str | None:
        """Fold ``filters`` into a single-outer-aggregate SQL expression.

        For ``SUM(${TABLE}.amount)`` + ``status='done'`` returns
        ``SUM(CASE WHEN (...) THEN ${TABLE}.amount END)``. Returns ``None`` when the
        expression is not exactly one outer ``FUNC(arg)`` (so the caller can fall back
        rather than mangle a complex expression).
        """
        m = re.match(r"^\s*(\w+)\s*\((.*)\)\s*$", agg_sql, re.S)
        if not m:
            return None
        func, arg = m.group(1), m.group(2)
        # Confirm the parens wrap the WHOLE expression (no premature close, e.g.
        # "SUM(a)/COUNT(b)" must not be treated as one outer SUM(...)). Quote-aware: a paren
        # inside a string literal / quoted identifier (e.g. CONCAT(a, ')')) is not syntax.
        depth = 0
        quote = None
        for ch in arg:
            if quote is not None:
                if ch == quote:
                    quote = None
                continue
            if ch in "'\"`":
                quote = ch
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth < 0:
                    return None
        if depth != 0:
            return None
        arg = arg.strip()
        # The outer FUNC must itself be the aggregate. A scalar wrapper around an aggregate
        # (e.g. ABS(SUM(amount))) has the aggregate in `arg`; folding would push CASE around
        # the inner aggregate (ABS(CASE WHEN ... THEN SUM(amount) END)) -> wrong. Bail so the
        # caller skips rather than emit invalid SQL.
        from sidemantic.sql.aggregation_detection import sql_has_aggregate as _has_agg

        if _has_agg(arg):
            return None
        # An aggregate-local ORDER BY (SUM(amount ORDER BY created_at), ARRAY_AGG(x ORDER BY y))
        # belongs to the aggregate CALL, not to the argument expression. Wrapping the whole arg
        # in a CASE would emit `SUM(CASE WHEN ... THEN amount ORDER BY created_at END)`, which is
        # malformed. Bail so the caller skips rather than export invalid SQL.
        if cls._has_top_level_order_by(arg):
            return None
        conds = cls._fold_filter_conds(filters, model)
        # COUNT(*) -> COUNT(CASE WHEN ... THEN 1 END): "* " can't live inside CASE.
        if arg == "*":
            return f"{func}(CASE WHEN {conds} THEN 1 END)"
        # COUNT(DISTINCT x) -> COUNT(DISTINCT CASE WHEN ... THEN x END): DISTINCT stays
        # outside the CASE (it's part of the aggregate, not the value being filtered). Accept
        # the parenthesized spelling COUNT(DISTINCT(x)) too; the lookahead requires a space or
        # `(` after DISTINCT so an identifier like `DISTINCTION` is not mistaken for it.
        # DISTINCT / ALL are aggregate MODIFIERS, not row expressions: they must stay OUTSIDE
        # the CASE (COUNT(DISTINCT CASE ... END), not COUNT(CASE ... THEN DISTINCT x END)).
        # The lookahead keeps a column actually named `all`/`distinct` (COUNT(all)) a plain arg.
        dm = re.match(r"(?is)^(DISTINCT|ALL)(?=[\s(])\s*(.+)$", arg)
        if dm:
            modifier, mod_arg = dm.group(1).upper(), dm.group(2).strip()
            # A multi-column DISTINCT (COUNT(DISTINCT a, b)) has no single CASE result, so
            # bail and let the caller skip rather than emit malformed `THEN a, b END`.
            # quote_aware: a delimited composite key COUNT(DISTINCT a || ',' || b) is ONE
            # column -- the comma in the string literal must not count as a separator.
            if len(cls._split_top_level_commas(mod_arg, quote_aware=True)) > 1:
                return None
            return f"{func}({modifier} CASE WHEN {conds} THEN {mod_arg} END)"
        # A multi-argument aggregate (WEIGHTED_AVG(price, qty)) has no single CASE result,
        # so bail rather than emit malformed `THEN price, qty END`.
        if len(cls._split_top_level_commas(arg, quote_aware=True)) > 1:
            return None
        return f"{func}(CASE WHEN {conds} THEN {arg} END)"

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
        from sidemantic.sql.aggregation_detection import sql_has_aggregate as _sql_has_aggregate

        measures = []
        for metric in model.metrics:
            measure_def = {"name": metric.name}
            filters_folded = False  # set when filters are folded into the measure SQL

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
                # Any metric.type that reaches here (time_comparison/derived/ratio
                # were handled above) is a complex type. A running_total imported from
                # LookML (type=cumulative + table_calculation meta) round-trips back to
                # a LookML running_total over its base measure; other complex types
                # (cumulative/conversion/retention/cohort) have no LookML equivalent and
                # are skipped rather than exported as a misleading plain aggregation.
                if metric.type is not None:
                    rt_sql = (metric.sql or "").strip()
                    rt_is_running_total = (metric.meta or {}).get("table_calculation") == "running_total" and rt_sql
                    # A LookML running_total's `sql` is a SINGLE base-measure reference.
                    # Accept a bare measure name (-> ${name}) or a string that is EXACTLY one
                    # already-braced ref (an unresolved cross-view ${other.total}, passed
                    # through). An EXPRESSION (e.g. "${other.total} + tax" -- note the local
                    # ref also lost its braces) is not a valid single ref, so fall through to
                    # the skip-with-warning rather than emit malformed `sql: ${other.total} + tax`.
                    if rt_is_running_total and re.fullmatch(r"\$\{[^{}]+\}", rt_sql):
                        measure_def["type"] = "running_total"
                        measure_def["sql"] = rt_sql
                    elif rt_is_running_total and re.fullmatch(r"\w+", rt_sql):
                        measure_def["type"] = "running_total"
                        measure_def["sql"] = f"${{{rt_sql}}}"
                    else:
                        logger.warning(
                            "Metric %r (type=%r) has no LookML equivalent; skipping on export.",
                            metric.name,
                            metric.type,
                        )
                        continue
                else:
                    # Regular aggregation measure.
                    type_mapping = {
                        "count": "count",
                        "count_distinct": "count_distinct",
                        "sum": "sum",
                        "avg": "average",
                        "average": "average",
                        "min": "min",
                        "max": "max",
                        "median": "median",
                    }
                    # Aggregations Looker has no native measure type for: emit as a
                    # type: number with an explicit SQL aggregate.
                    sql_agg_funcs = {
                        "stddev": "STDDEV",
                        "stddev_pop": "STDDEV_POP",
                        "variance": "VAR_SAMP",
                        "variance_pop": "VAR_POP",
                    }
                    col_sql = metric.sql.replace("{model}", "${TABLE}") if metric.sql else None

                    if metric.agg == "approx_count_distinct":
                        # Looker represents this as count_distinct with approximate: yes.
                        measure_def["type"] = "count_distinct"
                        measure_def["approximate"] = "yes"
                        if col_sql:
                            measure_def["sql"] = col_sql
                    elif metric.agg in type_mapping:
                        measure_def["type"] = type_mapping[metric.agg]
                        if col_sql:
                            measure_def["sql"] = col_sql
                    elif metric.agg in sql_agg_funcs and col_sql:
                        measure_def["type"] = "number"
                        agg_sql = f"{sql_agg_funcs[metric.agg]}({col_sql})"
                        if metric.filters:
                            # type: number re-imports as a derived metric whose generator
                            # does not apply LookML `filters`, so fold them into the aggregate
                            # here. Reuse _fold_filters_into_aggregate so DISTINCT stays OUTSIDE
                            # the CASE (STDDEV(DISTINCT amount) -> STDDEV(DISTINCT CASE WHEN ...
                            # THEN amount END), not STDDEV(CASE WHEN ... THEN DISTINCT amount END));
                            # skip if the form can't be folded rather than emit invalid SQL.
                            folded = self._fold_filters_into_aggregate(agg_sql, metric.filters, model)
                            if folded is None:
                                logger.warning(
                                    "Metric %r (agg=%r) has filters over an aggregate form that "
                                    "cannot be folded for LookML export; skipping to avoid invalid SQL.",
                                    metric.name,
                                    metric.agg,
                                )
                                continue
                            measure_def["sql"] = folded
                            filters_folded = True
                        else:
                            measure_def["sql"] = agg_sql
                    elif metric.agg is None and col_sql and _sql_has_aggregate(metric.sql or ""):
                        # An agg-less measure whose SQL is itself an aggregate (a complete
                        # SUM({model}.amount) imported from Cube, or an inline aggregate
                        # expression). Faithfully maps to a LookML type: number with the
                        # aggregate SQL. type: number re-imports as a derived metric that
                        # does NOT apply a separate `filters` block, so any filters must be
                        # folded into the aggregate; if the expression isn't a single
                        # foldable FUNC(arg), skip rather than emit a silently-unfiltered
                        # measure.
                        # A COUNT over any NON-NULL constant counts every row -- it is a native
                        # row count, identical to type: count: `*`, an int/decimal (1, 0, 1.0,
                        # .5), a boolean (TRUE/FALSE), or a string literal ('x'). COUNT(NULL) is
                        # deliberately excluded -- it is always 0, not a row count. An explicit
                        # ALL modifier (COUNT(ALL 1)) is the default and does not change the count,
                        # so accept it too rather than dropping the metric at the zero-column check
                        # below. The trailing \s+ keeps a column literally named `all` (COUNT(all))
                        # a plain argument.
                        _count_const = r"\*|[+-]?(?:\d+\.?\d*|\.\d+)|true|false|'(?:[^']|'')*'"
                        if not metric.filters and re.fullmatch(
                            rf"(?i)count\s*\(\s*(?:all\s+)?(?:{_count_const})\s*\)", col_sql.strip()
                        ):
                            # These reference no column; a type: number would re-import as a
                            # derived metric over an empty CTE (SELECT FROM ...), which the
                            # compiler rejects. Native type: count round-trips cleanly.
                            measure_def["type"] = "count"
                        elif not metric.filters and not self._aggregate_references_column(col_sql):
                            # ANY OTHER zero-column aggregate -- COUNT(NULL), COUNT(DISTINCT 1),
                            # SUM(1), MAX('x') -- has the same fate as a bare constant count: a
                            # type: number re-imports as an opaque complete-SQL metric whose
                            # referenced-column set is empty, so compiling it builds an empty model
                            # CTE (SELECT ... FROM with no select list). Unlike a plain row count it
                            # has no faithful native form, so skip it with a warning. (A FILTERED
                            # one falls through: folding the filter in makes it reference the
                            # filter's columns, e.g. COUNT(*) -> COUNT(CASE WHEN ... THEN 1 END).)
                            logger.warning(
                                "Metric %r has a zero-column aggregate SQL (%r) with no LookML "
                                "equivalent that round-trips; skipping on export.",
                                metric.name,
                                col_sql,
                            )
                            continue
                        else:
                            measure_def["type"] = "number"
                            if metric.filters:
                                folded = self._fold_filters_into_aggregate(col_sql, metric.filters, model)
                                if folded is None:
                                    logger.warning(
                                        "Metric %r has filters over a complex aggregate SQL expression that "
                                        "cannot be folded for LookML export; skipping to avoid an unfiltered measure.",
                                        metric.name,
                                    )
                                    continue
                                # Re-run the zero-column check on the FOLDED SQL: a filter does
                                # not necessarily add a column (COUNT(*) with `1 = 1`, or a pure
                                # template predicate), so the result can still reference none and
                                # would re-import as a metric over an empty model CTE.
                                if not self._aggregate_references_column(folded):
                                    logger.warning(
                                        "Metric %r folds to a zero-column aggregate SQL (%r) with no LookML "
                                        "equivalent that round-trips; skipping on export.",
                                        metric.name,
                                        folded,
                                    )
                                    continue
                                measure_def["sql"] = folded
                                filters_folded = True
                            else:
                                measure_def["sql"] = col_sql
                    else:
                        # agg=None over a NON-aggregate SQL (a plain row-level column /
                        # string / yesno measure), an unknown aggregation, or an opaque
                        # complete *column* expression: Looker measures aggregate, so there
                        # is no faithful measure form. Skip with a warning rather than
                        # forcing a misleading type: number that crashes on re-import.
                        logger.warning(
                            "Metric %r (agg=%r) has no LookML equivalent; skipping on export.",
                            metric.name,
                            metric.agg,
                        )
                        continue

            # Add filters (skip for time_comparison; skip when already folded into SQL)
            if metric.filters and metric.type != "time_comparison" and not filters_folded:
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
