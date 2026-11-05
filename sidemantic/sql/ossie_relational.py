"""Ossie SQL relations with typed, source-row-preserving measure lineage.

The compiler follows the reference strategy in Ossie Layer 2 section 9. Query
joins and filters determine the multiset of rows. Each measure independently
aggregates a distinct set of its original row identities, while ordinary SQL
aggregates continue to see the joined multiset.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations, product

import sqlglot
from sqlglot import exp

from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.interchange.ossie.identifier import normalize_identifier
from sidemantic.sql.rendering import render_sql


class OssieQueryError(ValueError):
    """A relational measure query violates the Ossie type or binding rules."""


def _error(message: str) -> OssieQueryError:
    return OssieQueryError(f"Ossie AGG: {message}")


def _key(identifier: exp.Expression | str) -> str:
    if isinstance(identifier, str):
        return normalize_identifier(identifier)
    if not isinstance(identifier, exp.Identifier):
        raise _error("expected an identifier")
    return identifier.name if identifier.args.get("quoted") else identifier.name.upper()


def _identifier(value: str) -> exp.Identifier:
    if value.startswith('"') and value.endswith('"'):
        return exp.to_identifier(value[1:-1].replace('""', '"'), quoted=True)
    return exp.to_identifier(value)


def _agg(node: exp.Expression) -> bool:
    # Older supported SQLGlot releases parse AGG as Anonymous and have no Agg class.
    return isinstance(node, getattr(exp, "Agg", ())) or isinstance(node, exp.Anonymous) and node.name.upper() == "AGG"


def _definition(node: exp.Expression) -> bool:
    return isinstance(node, exp.Anonymous) and node.name.upper() in {"TO_MEASURE", "MEASURE"}


def _children(node: exp.Expression):
    for value in node.args.values():
        if isinstance(value, exp.Expression):
            yield value
        elif isinstance(value, list):
            yield from (child for child in value if isinstance(child, exp.Expression))


def _walk_scope(node: exp.Expression):
    yield node
    if isinstance(node, (exp.Subquery, exp.Query)):
        return
    for child in _children(node):
        yield from _walk_scope(child)


def _has_aggregate(node: exp.Expression) -> bool:
    if isinstance(node, (exp.Subquery, exp.Query)) or _definition(node):
        return False
    if isinstance(node, exp.Window):
        # The window function itself does not group rows, but an aggregate in
        # its arguments or ordering does: SUM(SUM(amount)) OVER (...) does.
        return any(_has_aggregate(child) for child in _children(node.this)) or any(
            _has_aggregate(child)
            for name, value in node.args.items()
            if name != "this"
            for child in (value if isinstance(value, list) else [value])
            if isinstance(child, exp.Expression)
        )
    return isinstance(node, exp.AggFunc) or _agg(node) or any(_has_aggregate(child) for child in _children(node))


def _copy_transform(node: exp.Expression, replace):
    replacement = replace(node)
    if replacement is not None:
        return replacement
    result = node.copy()
    for name, value in node.args.items():
        if isinstance(value, exp.Expression):
            result.set(name, _copy_transform(value, replace))
        elif isinstance(value, list):
            result.set(
                name,
                [_copy_transform(child, replace) if isinstance(child, exp.Expression) else child for child in value],
            )
    return result


def _rename_expression(expression: exp.Expression, mapping: dict[str, str]) -> exp.Expression:
    return _copy_transform(
        expression,
        lambda node: exp.column(mapping[node.name]) if isinstance(node, exp.Column) and node.name in mapping else None,
    )


@dataclass
class Recipe:
    expression: exp.Expression
    identity: str | None = None
    parts: dict[str, Recipe] = field(default_factory=dict)
    predicate: exp.Expression | None = None

    def slots(self) -> set[str]:
        predicate_slots = (
            {column.name for column in self.predicate.find_all(exp.Column)} if self.predicate is not None else set()
        )
        if self.identity is not None:
            return {self.identity, *(column.name for column in self.expression.find_all(exp.Column)), *predicate_slots}
        return predicate_slots | set().union(*(part.slots() for part in self.parts.values()))

    def renamed(self, mapping: dict[str, str]) -> Recipe:
        if self.identity is not None:
            return Recipe(
                _rename_expression(self.expression, mapping),
                mapping[self.identity],
                predicate=_rename_expression(self.predicate, mapping) if self.predicate is not None else None,
            )
        return Recipe(
            self.expression.copy(),
            parts={name: part.renamed(mapping) for name, part in self.parts.items()},
            predicate=_rename_expression(self.predicate, mapping) if self.predicate is not None else None,
        )


@dataclass
class Column:
    name: exp.Identifier
    physical: str | None = None
    recipe: Recipe | None = None
    data_type: str = "UNKNOWN"

    @property
    def key(self):
        return _key(self.name)


@dataclass
class Relation:
    query: exp.Expression
    columns: list[Column]
    hidden: set[str] = field(default_factory=set)
    unknown: bool = False

    def physical_columns(self) -> set[str]:
        return self.hidden | {column.physical for column in self.columns if column.physical is not None}


@dataclass
class Source:
    name: exp.Identifier
    relation: Relation
    alias: str
    slots: dict[str, str] = field(default_factory=dict)
    suppressed: set[str] = field(default_factory=set)
    values: dict[str, exp.Expression] = field(default_factory=dict)
    synthetic: bool = False

    def value(self, column: Column) -> exp.Expression:
        if column.physical in self.values:
            return self.values[column.physical].copy()
        return exp.column(self.slots.get(column.physical, column.physical), table=self.alias)

    def recipe(self, column: Column) -> Recipe:
        return column.recipe.renamed(self.slots) if self.slots else column.recipe


def _measure_syntax(sql: str, dialect: str) -> str:
    """Normalize the draft's postfix marker using tokenizer source positions."""
    tokens = sqlglot.tokenize(sql, read=dialect)
    depths = []
    depth = 0
    for token in tokens:
        if token.text == ")":
            depth -= 1
        depths.append(depth)
        if token.text == "(":
            depth += 1
    edits = []
    for index, token in enumerate(tokens[:-1]):
        if token.text.upper() != "TO" or tokens[index + 1].text.upper() != "MEASURE":
            continue
        level = depths[index]
        start = index - 1
        while start >= 0:
            if depths[start] == level and (tokens[start].text == "," or tokens[start].text.upper() == "SELECT"):
                break
            start -= 1
        if start < 0:
            raise _error("TO MEASURE must appear in a SELECT projection")
        edits.append((tokens[start + 1].start, tokens[start + 1].start, "TO_MEASURE("))
        edits.append((token.start, tokens[index + 1].end + 1, ")"))
    for start, end, replacement in sorted(edits, reverse=True):
        sql = sql[:start] + replacement + sql[end:]
    return sql


class RelationalCompiler:
    def __init__(self, graph: SemanticGraph, dialect: str):
        self.graph = graph
        self.dialect = dialect
        self.serial = 0
        self.view_stack: set[str] = set()
        self.models = {
            name: model
            for name, model in graph.models.items()
            if (model.metadata or {}).get("ossie_scope_name")
            and (
                (model.metadata or {}).get("ossie_schema_revision") == "172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1"
                or any(
                    (metric.metadata or {}).get("ossie_dataset_metric") and graph.metric_owners.get(metric_name) == name
                    for metric_name, metric in graph.metrics.items()
                )
            )
        }

    def fresh(self, label="slot") -> str:
        self.serial += 1
        return f"__ossie_l2_{label}_{self.serial}"

    def parse(self, sql: str) -> exp.Expression:
        nodes = [
            node for node in sqlglot.parse(_measure_syntax(sql, self.dialect), read=self.dialect) if node is not None
        ]
        if len(nodes) != 1:
            raise _error("Multiple statements are not supported")
        return nodes[0]

    def relevant(self, node: exp.Expression) -> bool:
        views = self.graph.metadata.get("ossie_views", {})
        for table in node.find_all(exp.Table):
            if self.model_for(table) is not None:
                return True
            if self.view_for(table, views) is not None:
                return True
        return any(_definition(item) or (_agg(item) and bool(self.models or views)) for item in node.walk())

    def model_for(self, table: exp.Table):
        matches = [
            model
            for model in self.models.values()
            if _key((model.metadata or {}).get("ossie_source_name", model.name)) == _key(table.this)
        ]
        if table.db:
            matches = [model for model in matches if _key(model.metadata["ossie_scope_name"]) == _key(table.args["db"])]
        if table.catalog:
            matches = []
        if len(matches) > 1:
            raise _error(f"ambiguous model relation {table.sql()}")
        return matches[0] if matches else None

    def view_for(self, table: exp.Table, views):
        from sidemantic.sql.ossie_views import resolve_view_name

        name = resolve_view_name(self.graph, table, self.dialect)
        return (name, views[name]) if name is not None and name in views else None

    def model_relation(self, model) -> Relation:
        expression_dialect = (model.metadata or {}).get("ossie_target_dialect") or self.dialect
        source_dialect = (model.metadata or {}).get("ossie_source_dialect") or expression_dialect
        source = (
            sqlglot.parse_one(model.sql, read=source_dialect)
            if model.sql
            else exp.select("*").from_(sqlglot.parse_one(model.table, read=source_dialect, into=exp.Table))
        )
        source_alias = self.fresh("physical")
        projections = []
        dimensions = {}
        columns = []
        for dimension in model.dimensions:
            slot = self.fresh("field")
            expression = sqlglot.parse_one(dimension.sql_expr, read=expression_dialect)
            expression = expression.transform(
                lambda item: exp.column(item.name, table=source_alias, quoted=bool(item.this.args.get("quoted")))
                if isinstance(item, exp.Column)
                else item
            )
            projections.append(exp.alias_(expression, slot))
            dimensions[dimension.name] = slot
            # Private source/COUNT anchors belong only to measure recipes.
            if dimension.public is not False and "ossie_internal_expression" not in (dimension.metadata or {}):
                columns.append(
                    Column(
                        _identifier((dimension.metadata or {}).get("ossie_source_name", dimension.name)),
                        slot,
                        data_type=str(dimension.logical_data_type or "UNKNOWN"),
                    )
                )
        identity = self.fresh("identity")
        projections.append(exp.alias_(exp.Window(this=exp.RowNumber()), identity))
        hidden = {identity, *dimensions.values()}
        for name, metric in self.graph.metrics.items():
            if (
                not (metric.metadata or {}).get("ossie_dataset_metric")
                or self.graph.metric_owners.get(name) != model.name
            ):
                continue
            formula = sqlglot.parse_one(
                metric.sql, read=(metric.metadata or {}).get("ossie_target_dialect") or expression_dialect
            )
            for item in list(formula.find_all(exp.Column)):
                if item.name not in dimensions:
                    raise _error(f"measure input {item.sql()} is not a field of {model.name}")
                item.replace(exp.column(dimensions[item.name]))
            columns.append(
                Column(
                    _identifier(metric.metadata.get("ossie_source_name", name.rsplit(".", 1)[-1])),
                    recipe=Recipe(formula, identity),
                    data_type=str(metric.logical_data_type or "UNKNOWN"),
                )
            )
        query = exp.select(*projections).from_(source.subquery(source_alias))
        return Relation(query, columns, hidden)

    def rename_columns(self, relation, names):
        if relation.unknown:
            slots = [self.fresh("named_column") for _ in names]
            source = exp.Subquery(
                this=relation.query,
                alias=exp.TableAlias(
                    this=exp.to_identifier(self.fresh("named_relation")),
                    columns=[exp.to_identifier(slot) for slot in slots],
                ),
            )
            return Relation(
                exp.select(*(exp.column(slot) for slot in slots)).from_(source),
                [Column(name.copy(), slot) for name, slot in zip(names, slots, strict=True)],
            )
        if len(names) != len(relation.columns):
            raise _error("relation column alias count does not match its columns")
        return Relation(
            relation.query,
            [
                Column(name.copy(), column.physical, column.recipe, column.data_type)
                for name, column in zip(names, relation.columns, strict=True)
            ],
            relation.hidden,
        )

    def source(self, node, ctes, outer):
        alias_node = node.args.get("alias")
        name = alias_node.this.copy() if alias_node else (node.this.copy() if isinstance(node, exp.Table) else None)
        if name is None:
            name = exp.to_identifier(self.fresh("relation"))
        if isinstance(node, exp.Subquery):
            relation = self.compile(node.this, ctes, outer)
        elif isinstance(node, exp.Table):
            if not node.db and _key(node.this) in ctes:
                relation = ctes[_key(node.this)]
            elif (view := self.view_for(node, self.graph.metadata.get("ossie_views", {}))) is not None:
                view_name, sql = view
                if view_name in self.view_stack:
                    raise _error("recursive measure views are not well-founded")
                self.view_stack.add(view_name)
                try:
                    # A persistent view has its own lexical scope. Caller CTEs
                    # and correlated bindings cannot change its stored recipe.
                    relation = self.compile(self.parse(sql), {}, [])
                finally:
                    self.view_stack.remove(view_name)
            elif (model := self.model_for(node)) is not None:
                relation = self.model_relation(model)
            else:
                raw = node.copy()
                raw.set("alias", None)
                relation = Relation(exp.select("*").from_(raw), [], unknown=True)
        elif isinstance(node, exp.Values):
            names = list(alias_node.args.get("columns", [])) if alias_node else []
            width = len(node.expressions[0].expressions) if node.expressions else len(names)
            if not names:
                names = [exp.to_identifier(f"column{index}") for index in range(width)]
            physical = [self.fresh("value") for _ in range(width)]
            raw = node.copy()
            raw.set(
                "alias",
                exp.TableAlias(
                    this=exp.to_identifier(self.fresh("values")), columns=[exp.to_identifier(p) for p in physical]
                ),
            )
            relation = Relation(
                exp.select(*(exp.column(p) for p in physical)).from_(raw),
                [Column(n.copy(), p) for n, p in zip(names, physical, strict=True)],
            )
            alias_node = None
        else:
            raise _error(f"unsupported relation expression {type(node).__name__}")
        if alias_node and alias_node.args.get("columns"):
            relation = self.rename_columns(relation, alias_node.args["columns"])
        alias = self.fresh("source")
        return Source(name, relation, alias)

    def resolve(self, column: exp.Column, sources: list[Source], outer: list[Source]):
        if column.catalog or column.db:
            raise _error(f"unrecognized qualified column {column.sql()}; use its relation alias")
        candidates = []
        for source in sources:
            if column.table and _key(column.args["table"]) != _key(source.name):
                continue
            if not column.table and _key(column.this) in source.suppressed:
                continue
            matching = [item for item in source.relation.columns if item.key == _key(column.this)]
            if not matching and source.relation.unknown:
                item = Column(column.this.copy(), column.name)
                source.relation.columns.append(item)
                matching = [item]
            candidates.extend((source, item) for item in matching)
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            raise _error(f"ambiguous column {column.sql()}")
        if outer:
            return self.resolve(column, outer, [])
        raise _error(f"column not found: {column.sql()}")

    def bind(self, expression, sources, ctes, outer, *, aggregate=False):
        def replace(node):
            if isinstance(node, exp.Window):
                result = node.copy()
                result.set("this", self.bind(node.this, sources, ctes, outer, aggregate=True))
                for name, value in node.args.items():
                    if name == "this":
                        continue
                    if isinstance(value, exp.Expression):
                        result.set(name, self.bind(value, sources, ctes, outer))
                    elif isinstance(value, list):
                        result.set(
                            name,
                            [
                                self.bind(item, sources, ctes, outer) if isinstance(item, exp.Expression) else item
                                for item in value
                            ],
                        )
                return result
            if isinstance(node, exp.Subquery):
                relation = self.compile(node.this, ctes, [*sources, *outer])
                if any(column.recipe for column in relation.columns):
                    raise _error("a scalar subquery must return scalar fields, not measures")
                return self.finish(relation, allow_measures=False).subquery()
            if isinstance(node, exp.Query):
                relation = self.compile(node, ctes, [*sources, *outer])
                if any(column.recipe for column in relation.columns):
                    raise _error("a scalar subquery must return scalar fields, not measures")
                return self.finish(relation, allow_measures=False)
            if isinstance(node, exp.Column) and not node.is_star:
                source, column = self.resolve(node, sources, outer)
                if column.recipe:
                    raise _error("a measure is only a scalar inside AGG()")
                return source.value(column)
            if _agg(node):
                raise _error("AGG is only valid in an aggregate context")
            if _definition(node):
                raise _error("measure definitions are only valid in an unaggregated projection")
            if isinstance(node, exp.AggFunc) and not aggregate:
                raise _error("an aggregate is not valid in this clause")
            return None

        return _copy_transform(expression, replace)

    def compile(self, query, ctes=None, outer=None) -> Relation:
        ctes = dict(ctes or {})
        outer = list(outer or [])
        query = query.copy()
        with_clause = query.args.pop("with_", None)
        local_ctes = []
        recursive = bool(with_clause and with_clause.args.get("recursive"))
        if with_clause:
            for cte in with_clause.expressions:
                alias = cte.args["alias"]
                internal = self.fresh("cte")
                self_references = recursive and any(
                    not table.db and _key(table.this) == _key(alias.this) for table in cte.this.find_all(exp.Table)
                )
                if self_references:
                    body = cte.this
                    if not isinstance(body, exp.SetOperation):
                        raise _error("recursive CTE requires a nonrecursive seed and a recursive set branch")
                    seed = self.compile(body.this, ctes, outer)
                    if any(column.recipe for column in seed.columns):
                        raise _error("recursive measure definitions are not well-founded")
                    if alias.args.get("columns"):
                        names = alias.args["columns"]
                        if len(names) != len(seed.columns):
                            raise _error("CTE column alias count does not match")
                        seed.columns = [
                            Column(name.copy(), column.physical, data_type=column.data_type)
                            for name, column in zip(names, seed.columns, strict=True)
                        ]
                    ctes[_key(alias.this)] = Relation(exp.select("*").from_(internal), seed.columns)
                    step = self.compile(body.expression, ctes, outer)
                    if any(column.recipe for column in step.columns):
                        raise _error("recursive measure definitions are not well-founded")
                    if len(seed.columns) != len(step.columns):
                        raise _error("recursive CTE inputs have different column counts")
                    step_query = exp.select(
                        *(
                            exp.alias_(exp.column(source.physical), target.physical)
                            for source, target in zip(step.columns, seed.columns, strict=True)
                        )
                    ).from_(step.query.subquery(self.fresh("recursive_step")))
                    union = body.copy()
                    union.set("this", seed.query)
                    union.set("expression", step_query)
                    relation = Relation(union, seed.columns)
                else:
                    relation = self.compile(cte.this, ctes, outer)
                if alias.args.get("columns"):
                    relation = self.rename_columns(relation, alias.args["columns"])
                local_ctes.append((internal, relation.query))
                ctes[_key(alias.this)] = Relation(
                    exp.select("*").from_(internal), relation.columns, relation.hidden, relation.unknown
                )
        if isinstance(query, exp.Subquery):
            return self.compile(query.this, ctes, outer)
        if isinstance(query, exp.SetOperation):
            left = self.compile(query.this, ctes, outer)
            right = self.compile(query.expression, ctes, outer)
            if any(column.recipe for column in [*left.columns, *right.columns]):
                raise _error("set operators cannot carry unevaluated measures")
            if len(left.columns) != len(right.columns):
                raise _error("set inputs have different column counts")
            physical = [self.fresh("set") for _ in left.columns]
            for relation in (left, right):
                relation.query = exp.select(
                    *(exp.alias_(exp.column(c.physical), p) for c, p in zip(relation.columns, physical, strict=True))
                ).from_(relation.query.subquery(self.fresh("set_input")))
            result = query.copy()
            result.set("this", left.query)
            result.set("expression", right.query)
            if result.args.get("order"):
                positions = {column.key: slot for column, slot in zip(left.columns, physical, strict=True)}
                order = result.args["order"].copy()
                for ordered in order.expressions:

                    def bind_set(node):
                        if isinstance(node, exp.Column):
                            if node.table or _key(node.this) not in positions:
                                raise _error(f"set ORDER BY column not found: {node.sql()}")
                            return exp.column(positions[_key(node.this)])
                        return None

                    ordered.set("this", _copy_transform(ordered.this, bind_set))
                result.set("order", order)
            relation = Relation(
                result, [Column(c.name, p, data_type=c.data_type) for c, p in zip(left.columns, physical, strict=True)]
            )
        elif isinstance(query, exp.Select):
            relation = self.select(query, ctes, outer)
        else:
            raise _error("expected a SELECT, subquery, or set expression")
        if local_ctes:
            existing = relation.query.args.get("with_")
            entries = [
                exp.CTE(this=body, alias=exp.TableAlias(this=exp.to_identifier(name)), materialized=True)
                for name, body in local_ctes
            ]
            relation.query.set(
                "with_", exp.With(expressions=entries + (existing.expressions if existing else []), recursive=recursive)
            )
        return relation

    def inline_named_windows(self, query):
        definitions = {}
        for definition in query.args.get("windows") or []:
            name = _key(definition.this)
            if name in definitions:
                raise _error("duplicate named window")
            definitions[name] = definition

        def resolve(window, visiting):
            result = window.copy()
            reference = window.args.get("alias")
            if reference is None:
                return result
            name = _key(reference)
            if name not in definitions:
                raise _error(f"named window not found: {reference.sql()}")
            if name in visiting:
                raise _error("cyclic named window inheritance")
            inherited = resolve(definitions[name], visiting | {name})
            for attribute in ("partition_by", "order", "spec"):
                value = inherited.args.get(attribute)
                if value:
                    if result.args.get(attribute):
                        raise _error(f"cannot override inherited window {attribute}")
                    result.set(attribute, [item.copy() for item in value] if isinstance(value, list) else value.copy())
            result.set("alias", None)
            return result

        def replace(node):
            if isinstance(node, (exp.Subquery, exp.Query)):
                return node.copy()
            if isinstance(node, exp.Window):
                return resolve(node, set())
            return None

        result = query.copy()
        result.set("windows", None)
        for name, value in result.args.items():
            if isinstance(value, exp.Expression):
                result.set(name, _copy_transform(value, replace))
            elif isinstance(value, list):
                result.set(
                    name,
                    [_copy_transform(item, replace) if isinstance(item, exp.Expression) else item for item in value],
                )
        return result

    def grouped_window_query(self, query, projections, sources, ctes, outer):
        """Materialize group expressions before windows, including grouping sets."""
        aliases = {_key(item.args["alias"]): item.this for item in projections if isinstance(item, exp.Alias)}
        source_names = {
            column.key
            for source in sources
            for column in source.relation.columns
            if column.key not in source.suppressed
        }

        def expand(node, visiting=frozenset(), prefer_alias=False):
            def replace(value):
                if isinstance(value, (exp.Subquery, exp.Query)) or _agg(value) or _definition(value):
                    return value.copy()
                if isinstance(value, exp.Column) and not value.table:
                    name = _key(value.this)
                    if name in aliases and (prefer_alias or name not in source_names) and aliases[name] != value:
                        if name in visiting:
                            raise _error("cyclic projection alias")
                        return expand(aliases[name], visiting | {name}, prefer_alias)
                return None

            return _copy_transform(node, replace)

        def has_window(node):
            return any(isinstance(item, exp.Window) for item in _walk_scope(node))

        lifted = {}

        def lift(node):
            node = expand(node)
            if isinstance(node, (exp.Literal, exp.Boolean, exp.Null, exp.Identifier, exp.Star)):
                return node.copy()
            if not has_window(node) and not isinstance(
                node, (exp.Order, exp.Ordered, exp.Qualify, exp.Where, exp.WindowSpec, exp.Distinct)
            ):
                key = node.sql()
                if key not in lifted:
                    lifted[key] = (self.fresh("window_input"), node.copy())
                return exp.column(lifted[key][0])
            result = node.copy()
            for name, value in node.args.items():
                if isinstance(node, exp.Window) and name == "this":
                    # Keep the outer window operator; only its inputs belong
                    # to the preceding aggregate stage.
                    function = value.copy()
                    for argument, child in value.args.items():
                        if isinstance(child, exp.Expression):
                            function.set(argument, lift(child))
                        elif isinstance(child, list):
                            function.set(
                                argument, [lift(item) if isinstance(item, exp.Expression) else item for item in child]
                            )
                    result.set(name, function)
                elif isinstance(value, exp.Expression):
                    result.set(name, lift(value))
                elif isinstance(value, list):
                    result.set(name, [lift(item) if isinstance(item, exp.Expression) else item for item in value])
            return result

        result = exp.Select()
        result.set(
            "expressions",
            [
                exp.Alias(
                    this=lift(item.this if isinstance(item, exp.Alias) else item),
                    alias=item.args["alias"].copy() if isinstance(item, exp.Alias) else self.output_name(item),
                )
                for item in projections
            ],
        )
        if query.args.get("qualify"):
            result.set("qualify", lift(query.args["qualify"]))
        if query.args.get("order"):
            order = query.args["order"].copy()
            for ordered in order.expressions:
                value = ordered.this
                # Bare ORDER BY output names take precedence over source names;
                # window ordering and QUALIFY retain source-name precedence.
                if isinstance(value, exp.Column) and not value.table and _key(value.this) in aliases:
                    value = aliases[_key(value.this)]
                ordered.set("this", lift(value))
            result.set("order", order)
        for name in ("distinct", "limit", "offset"):
            result.set(name, query.args.get(name))

        inner = query.copy()
        for name in ("qualify", "order", "distinct", "limit", "offset", "windows"):
            inner.set(name, None)
        if inner.args.get("having"):
            inner.set("having", expand(inner.args["having"], prefer_alias=True))

        def group_term(term):
            if isinstance(term, (exp.Group, exp.GroupingSets, exp.Rollup, exp.Cube, exp.Tuple, exp.Paren)):
                rewritten = term.copy()
                for name, value in term.args.items():
                    if isinstance(value, exp.Expression):
                        rewritten.set(name, group_term(value))
                    elif isinstance(value, list):
                        rewritten.set(
                            name, [group_term(item) if isinstance(item, exp.Expression) else item for item in value]
                        )
                return rewritten
            if isinstance(term, exp.Literal) and term.is_int:
                index = int(term.this) - 1
                if not 0 <= index < len(projections):
                    raise _error("GROUP BY ordinal is out of range")
                term = projections[index]
                term = term.this if isinstance(term, exp.Alias) else term
            return expand(term)

        if inner.args.get("group"):
            group = inner.args["group"]
            if group.args.get("all"):
                group = exp.Group(
                    expressions=[
                        item.this if isinstance(item, exp.Alias) else item
                        for item in projections
                        if not _has_aggregate(item) and not has_window(item)
                    ]
                )
            inner.set("group", group_term(group))
        inner.set(
            "expressions",
            [exp.alias_(value, name) for name, value in lifted.values()]
            or [exp.alias_(exp.Literal.number(1), self.fresh("group_row"))],
        )
        result = result.from_(inner.subquery(self.fresh("window_groups")))
        return self.compile(result, ctes, outer)

    def select(self, query: exp.Select, ctes, outer) -> Relation:
        query = self.inline_named_windows(query)
        if (
            not outer
            and not self.relevant(query)
            and not any(not table.db and _key(table.this) in ctes for table in query.find_all(exp.Table))
            and any(
                isinstance(projection, exp.Star) or isinstance(projection, exp.Column) and projection.is_star
                for projection in query.expressions
            )
        ):
            return Relation(query.copy(), [], unknown=True)
        sources = []
        from_clause = query.args.get("from_")
        if from_clause:
            sources.append(self.source(from_clause.this, ctes, outer))
        joins = []
        for join in query.args.get("joins", []):
            right = self.source(join.this, ctes, outer)
            copied = join.copy()
            copied.set("this", right.relation.query.subquery(right.alias))
            sources.append(right)
            if join.args.get("using") or join.args.get("method") == "NATURAL":
                names = join.args.get("using")
                if not names:
                    left_keys = {
                        column.key: column.name
                        for source in sources
                        if source is not right
                        for column in source.relation.columns
                    }
                    names = [column.name for column in right.relation.columns if column.key in left_keys]
                conditions = []
                for identifier in names:
                    left_source, left = self.resolve(
                        exp.column(identifier.name, quoted=identifier.args.get("quoted", False)),
                        [item for item in sources if item is not right],
                        [],
                    )
                    right_source, right_column = self.resolve(
                        exp.column(identifier.name, table=right.name.name, quoted=identifier.args.get("quoted", False)),
                        [right],
                        [],
                    )
                    if left.recipe or right_column.recipe:
                        raise _error("a join condition cannot compare measures")
                    left_value, right_value = left_source.value(left), right_source.value(right_column)
                    conditions.append(exp.EQ(this=left_value.copy(), expression=right_value.copy()))
                    merged_value = exp.Coalesce(this=left_value, expressions=[right_value])
                    merged_source = next((item for item in sources if item.synthetic), None)
                    if merged_source is None:
                        merged_source = Source(
                            exp.to_identifier(self.fresh("using")),
                            Relation(exp.select(), []),
                            self.fresh("merged"),
                            synthetic=True,
                        )
                        sources.append(merged_source)
                    merged_column = next(
                        (item for item in merged_source.relation.columns if item.key == _key(identifier)), None
                    )
                    if merged_column is None:
                        merged_column = Column(identifier.copy(), self.fresh("merged_key"), data_type=left.data_type)
                        merged_source.relation.columns.append(merged_column)
                    merged_source.values[merged_column.physical] = merged_value
                    for item in sources:
                        if not item.synthetic:
                            item.suppressed.add(_key(identifier))
                copied.set("on", exp.and_(*conditions) if conditions else exp.true())
                copied.set("using", None)
                copied.set("method", None)
            elif join.args.get("on"):
                copied.set("on", self.bind(join.args["on"], sources, ctes, outer))
            joins.append(copied)
        where = self.bind(query.args["where"].this, sources, ctes, outer) if query.args.get("where") else None
        projections = self.expand_projections(query.expressions, sources)
        stage_expressions = [
            *projections,
            *([query.args["order"]] if query.args.get("order") else []),
            *([query.args["qualify"]] if query.args.get("qualify") else []),
        ]
        grouped = bool(query.args.get("group") or query.args.get("having")) or any(
            _has_aggregate(item) for item in stage_expressions
        )
        if grouped and any(isinstance(node, exp.Window) for item in stage_expressions for node in _walk_scope(item)):
            return self.grouped_window_query(query, projections, sources, ctes, outer)
        # Bind projection/group/order references before building joined columns,
        # so ordinary physical tables may expose explicitly referenced fields.
        for expression in [
            *projections,
            *([query.args["group"]] if query.args.get("group") else []),
            *([query.args["having"]] if query.args.get("having") else []),
            *([query.args["order"]] if query.args.get("order") else []),
            *([query.args["qualify"]] if query.args.get("qualify") else []),
        ]:
            for node in _walk_scope(expression):
                if isinstance(node, exp.Column) and not node.is_star:
                    try:
                        self.resolve(node, sources, outer)
                    except OssieQueryError:
                        # SELECT aliases in GROUP/HAVING/ORDER bind later.
                        if node.table:
                            raise
        joined_name = self.fresh("joined")
        joined_alias = self.fresh("row")
        joined_projections = []
        post_sources = []
        for source in sources:
            mapping = {slot: self.fresh("input") for slot in sorted(source.relation.physical_columns())}
            joined_projections.extend(
                exp.alias_(
                    source.values[slot].copy() if slot in source.values else exp.column(slot, table=source.alias),
                    mapped,
                )
                for slot, mapped in mapping.items()
            )
            post_sources.append(
                Source(
                    source.name,
                    source.relation,
                    joined_alias,
                    mapping,
                    suppressed=source.suppressed,
                    synthetic=source.synthetic,
                )
            )
        definition_identity = self.fresh("defined_identity")
        joined_projections.append(exp.alias_(exp.Window(this=exp.RowNumber()), definition_identity))
        joined = exp.select(*joined_projections)
        if sources:
            joined = joined.from_(sources[0].relation.query.subquery(sources[0].alias))
        if joins:
            joined.set("joins", joins)
        if where is not None:
            joined = joined.where(where)
        grouped = bool(query.args.get("group") or query.args.get("having")) or any(
            _has_aggregate(item)
            for item in [
                *projections,
                *([query.args["order"]] if query.args.get("order") else []),
                *([query.args["qualify"]] if query.args.get("qualify") else []),
            ]
        )
        if grouped:
            relation = self.aggregate(query, projections, post_sources, ctes, outer, joined_name, joined_alias)
        else:
            relation = self.project(
                query, projections, post_sources, ctes, outer, joined_name, joined_alias, definition_identity
            )
        relation.query = relation.query.with_(joined_name, as_=joined, materialized=True)
        return relation

    def expand_projections(self, projections, sources):
        expanded = []
        for projection in projections:
            star = isinstance(projection, exp.Star) or isinstance(projection, exp.Column) and projection.is_star
            if not star:
                expanded.append(projection)
                continue
            modifiers = projection if isinstance(projection, exp.Star) else projection.this
            excluded = {_key(item.this) for item in modifiers.args.get("except_") or []}
            replacements = {_key(item.args["alias"]): item.this for item in modifiers.args.get("replace") or []}
            renames = {_key(item.this.this): item.args["alias"] for item in modifiers.args.get("rename") or []}
            selected = sorted(sources, key=lambda source: not source.synthetic)
            if isinstance(projection, exp.Column) and projection.table:
                selected = [source for source in sources if _key(source.name) == _key(projection.args["table"])]
            for source in selected:
                if source.relation.unknown:
                    raise _error("a wildcard over an undeclared physical relation requires its column schema")
                for column in source.relation.columns:
                    if isinstance(projection, exp.Star) and column.key in source.suppressed:
                        continue
                    if column.key in excluded:
                        continue
                    expression = replacements.get(column.key)
                    if expression is None:
                        expression = exp.Column(this=column.name.copy(), table=source.name.copy())
                    else:
                        expression = expression.copy()
                    name = renames.get(column.key, column.name)
                    if column.key in replacements or column.key in renames:
                        expression = exp.Alias(this=expression, alias=name.copy())
                    expanded.append(expression)
        return expanded

    def measure(self, call, sources, outer):
        arguments = [call.this] if isinstance(call, getattr(exp, "Agg", ())) else call.expressions
        if len(arguments) != 1 or not isinstance(arguments[0], exp.Column) or arguments[0].is_star:
            raise _error("AGG requires exactly one measure-column reference")
        source, column = self.resolve(arguments[0], sources, outer)
        if column.recipe is None:
            raise _error("AGG requires a measure, not an ordinary field")
        return source.recipe(column)

    def define(self, expression, sources, ctes, outer, identity):
        if not _has_aggregate(expression):
            raise _error("a measure definition requires an aggregate expression")
        parts = {}

        def visit(node):
            if isinstance(node, exp.Filter) and _agg(node.this):
                name = self.fresh("part")
                recipe = self.measure(node.this, sources, outer)
                predicate = self.bind(node.expression.this, sources, ctes, outer)
                predicate = predicate.transform(
                    lambda item: exp.column(item.name) if isinstance(item, exp.Column) else item
                )
                parts[name] = Recipe(exp.column(name), parts={name: recipe}, predicate=predicate)
                return exp.column(name)
            if _agg(node):
                name = self.fresh("part")
                parts[name] = self.measure(node, sources, outer)
                return exp.column(name)
            if isinstance(node, (exp.AggFunc, exp.Filter, exp.WithinGroup)) and _has_aggregate(node):
                if any(_agg(child) for child in _walk_scope(node) if child is not node):
                    raise _error("aggregate functions cannot contain another aggregate")
                bound = self.bind(node, sources, ctes, outer, aggregate=True)
                bound = bound.transform(lambda item: exp.column(item.name) if isinstance(item, exp.Column) else item)
                name = self.fresh("part")
                parts[name] = Recipe(bound, identity)
                return exp.column(name)
            if isinstance(node, exp.Column):
                raise _error("a measure definition cannot read an unaggregated field")
            if isinstance(node, (exp.Subquery, exp.Window)):
                raise _error("measure definitions must be Layer 1 aggregate expressions")
            return None

        formula = _copy_transform(expression, visit)
        return Recipe(formula, parts=parts)

    def project(self, query, projections, sources, ctes, outer, joined, alias, identity):
        outputs = []
        select_items = []
        hidden = set()
        aliases = {}
        for projection in projections:
            value = projection.this if isinstance(projection, exp.Alias) else projection
            name = projection.args["alias"].copy() if isinstance(projection, exp.Alias) else self.output_name(value)
            recipe = None
            data_type = "UNKNOWN"
            if _definition(value):
                if len(value.expressions) != 1:
                    raise _error("TO_MEASURE requires one aggregate expression")
                recipe = self.define(value.expressions[0], sources, ctes, outer, identity)
                data_type = self.infer_type(value.expressions[0], sources, outer)
            elif isinstance(value, exp.Column):
                source, column = self.resolve(value, sources, outer)
                data_type = column.data_type
                if column.recipe:
                    recipe = source.recipe(column)
            if recipe:
                outputs.append(Column(name, recipe=recipe, data_type=data_type))
                hidden |= recipe.slots()
            else:
                expression = self.bind(value, sources, ctes, outer)
                physical = self.fresh("projection")
                outputs.append(
                    Column(
                        name,
                        physical,
                        data_type=data_type if data_type != "UNKNOWN" else self.infer_type(value, sources, outer),
                    )
                )
                select_items.append(exp.alias_(expression, physical))
                aliases[_key(name)] = exp.column(physical)
        if query.args.get("distinct") and any(column.recipe for column in outputs):
            raise _error("DISTINCT cannot carry unevaluated measures")
        select_items.extend(exp.column(slot, table=alias) for slot in sorted(hidden))
        if not select_items:
            select_items.append(exp.alias_(exp.Literal.number(1), self.fresh("empty")))
        result = exp.select(*select_items).from_(exp.to_table(joined).as_(alias))
        result.set("distinct", query.args.get("distinct"))
        if query.args.get("qualify"):
            # QUALIFY is evaluated after the projected windows and before
            # DISTINCT/ORDER/LIMIT. Source fields take precedence over output
            # aliases of the same name, matching ordinary DuckDB SQL.
            source_names = {
                column.key
                for source in sources
                for column in source.relation.columns
                if column.key not in source.suppressed
            }
            output_aliases = Source(
                exp.to_identifier(self.fresh("qualify_aliases")),
                Relation(exp.select(), [column for column in outputs if column.key not in source_names]),
                "",
                values={column.physical: exp.column(column.physical) for column in outputs if column.physical},
            )
            predicate = self.bind(query.args["qualify"].this, [*sources, output_aliases], ctes, outer)
            result.set("qualify", exp.Qualify(this=predicate))
        if query.args.get("order"):
            order = query.args["order"].copy()
            for ordered in order.expressions:
                value = ordered.this
                if isinstance(value, exp.Column) and not value.table and _key(value.this) in aliases:
                    ordered.set("this", aliases[_key(value.this)].copy())
                else:
                    ordered.set("this", self.bind(value, sources, ctes, outer))
            result.set("order", order)
        for name in ("limit", "offset"):
            result.set(name, query.args.get(name))
        return Relation(result, outputs, hidden)

    def infer_type(self, expression, sources, outer):
        if isinstance(expression, exp.Alias):
            return self.infer_type(expression.this, sources, outer)
        if isinstance(expression, exp.Column):
            return self.resolve(expression, sources, outer)[1].data_type
        if _agg(expression):
            argument = expression.this if isinstance(expression, getattr(exp, "Agg", ())) else expression.expressions[0]
            return self.resolve(argument, sources, outer)[1].data_type
        if isinstance(expression, exp.Count):
            return "BIGINT"
        if isinstance(expression, (exp.Avg, exp.Div)):
            return "DOUBLE"
        if isinstance(expression, exp.Cast):
            return expression.args["to"].sql(dialect=self.dialect)
        if isinstance(expression, exp.Literal):
            return "VARCHAR" if expression.is_string else "BIGINT" if expression.is_int else "DOUBLE"
        if isinstance(expression, (exp.Sum, exp.Min, exp.Max, exp.Paren, exp.Filter)):
            return self.infer_type(expression.this, sources, outer)
        if isinstance(expression, (exp.Add, exp.Sub, exp.Mul, exp.Mod)):
            types = [
                self.infer_type(expression.this, sources, outer),
                self.infer_type(expression.expression, sources, outer),
            ]
            return "DOUBLE" if "DOUBLE" in types else next((dtype for dtype in types if dtype != "UNKNOWN"), "UNKNOWN")
        return "UNKNOWN"

    @staticmethod
    def output_name(expression):
        if isinstance(expression, exp.Column):
            return expression.this.copy()
        if _agg(expression):
            argument = expression.this if isinstance(expression, getattr(exp, "Agg", ())) else expression.expressions[0]
            if isinstance(argument, exp.Column):
                return argument.this.copy()
        return exp.to_identifier(expression.sql(), quoted=True)

    def grouping_sets(self, query, projections, sources, ctes, outer):
        group = query.args.get("group")
        aliases = {_key(p.args["alias"]): p.this for p in projections if isinstance(p, exp.Alias)}

        def bind(expression):
            if isinstance(expression, exp.Literal) and expression.is_int:
                position = int(expression.this) - 1
                if not 0 <= position < len(projections):
                    raise _error("GROUP BY ordinal is out of range")
                expression = projections[position]
                if isinstance(expression, exp.Alias):
                    expression = expression.this
            if isinstance(expression, exp.Column) and not expression.table and _key(expression.this) in aliases:
                expression = aliases[_key(expression.this)]
            return self.bind(expression, sources, ctes, outer)

        if group is None:
            return [[]], []
        if group.args.get("all"):
            expressions = [p.this if isinstance(p, exp.Alias) else p for p in projections]
            terms = [bind(p) for p in expressions if not _has_aggregate(p)]
            return [terms], terms

        def expand(node):
            if isinstance(node, exp.GroupingSets):
                result = []
                for item in node.expressions:
                    if isinstance(item, exp.Paren):
                        item = item.this
                    values = item.expressions if isinstance(item, exp.Tuple) else [item]
                    result.append([bind(value) for value in values])
                return result
            if isinstance(node, (exp.Rollup, exp.Cube)):
                values = [bind(value) for value in node.expressions]
                if isinstance(node, exp.Rollup):
                    return [values[:end] for end in range(len(values), -1, -1)]
                return [list(items) for count in range(len(values) + 1) for items in combinations(values, count)]
            return [[bind(node)]]

        elements = list(group.expressions)
        for name in ("rollup", "cube", "grouping_sets"):
            elements.extend(group.args.get(name) or [])
        sets = [sum(items, []) for items in product(*(expand(element) for element in elements))] if elements else [[]]
        all_terms = []
        seen = set()
        for terms in sets:
            for term in terms:
                key = term.sql()
                if key not in seen:
                    all_terms.append(term)
                    seen.add(key)
        return sets, all_terms

    def evaluate(self, recipe, joined, alias, terms, group_names, spine, predicate=None, measure_joins=None):
        if recipe.predicate is not None:
            predicate = exp.and_(predicate, recipe.predicate) if predicate is not None else recipe.predicate
        if recipe.identity is None:
            return _copy_transform(
                recipe.expression,
                lambda node: self.evaluate(
                    recipe.parts[node.name], joined, alias, terms, group_names, spine, predicate, measure_joins
                )
                if isinstance(node, exp.Column) and node.name in recipe.parts
                else None,
            )
        inner = self.fresh("measure_rows")
        columns = sorted(recipe.slots())
        rows = (
            exp.select(*(exp.column(name, table=inner) for name in columns))
            .distinct()
            .from_(exp.to_table(joined).as_(inner))
        )
        conditions = [exp.Not(this=exp.Is(this=exp.column(recipe.identity, table=inner), expression=exp.Null()))]
        if predicate is not None:
            conditions.append(
                predicate.transform(
                    lambda node: exp.column(node.name, table=inner)
                    if isinstance(node, exp.Column) and (not node.table or node.table == alias)
                    else node
                )
            )
        if self.dialect == "clickhouse" and terms and measure_joins is not None:
            # ClickHouse cannot decorrelate a scalar aggregate whose source
            # contains identity windows. Aggregate each measure independently,
            # then attach it to the existing group domain with NULL-equal keys.
            groups = []
            for term in terms:
                inside = term.transform(
                    lambda node: exp.column(node.name, table=inner)
                    if isinstance(node, exp.Column) and node.table == alias
                    else node
                )
                name = group_names[term.sql()]
                rows = rows.select(exp.alias_(inside, name), append=True)
                groups.append(name)
            rows = rows.where(exp.and_(*conditions))
            value_name, present_name = self.fresh("measure_value"), self.fresh("measure_present")
            aggregate = (
                exp.select(
                    *(exp.column(name) for name in groups),
                    exp.alias_(recipe.expression.copy(), value_name),
                    exp.alias_(exp.Literal.number(1), present_name),
                )
                .from_(rows.subquery(self.fresh("deduplicated")))
                .group_by(*(exp.column(name) for name in groups))
            )
            result_alias = self.fresh("measure_result")
            condition = exp.and_(
                *(
                    exp.Anonymous(
                        this="isNotDistinctFrom",
                        expressions=[exp.column(name, table=result_alias), exp.column(name, table=spine)],
                    )
                    for name in groups
                )
            )
            measure_joins.append((aggregate.subquery(result_alias), condition))
            empty_rows = rows.copy().where(exp.false())
            empty = exp.select(recipe.expression.copy()).from_(empty_rows.subquery(self.fresh("empty_measure")))
            return (
                exp.Case()
                .when(
                    exp.Is(this=exp.column(present_name, table=result_alias), expression=exp.Null()), empty.subquery()
                )
                .else_(exp.column(value_name, table=result_alias))
            )
        for term in terms:
            inside = term.transform(
                lambda node: exp.column(node.name, table=inner)
                if isinstance(node, exp.Column) and node.table == alias
                else node
            )
            conditions.append(exp.NullSafeEQ(this=inside, expression=exp.column(group_names[term.sql()], table=spine)))
        rows = rows.where(exp.and_(*conditions))
        body = exp.select(recipe.expression.copy()).from_(rows.subquery(self.fresh("deduplicated")))
        return body.subquery()

    def aggregate(self, query, projections, sources, ctes, outer, joined, alias):
        sets, all_terms = self.grouping_sets(query, projections, sources, ctes, outer)
        group_names = {term.sql(): self.fresh("group") for term in all_terms}
        output_names = [
            p.args["alias"].copy() if isinstance(p, exp.Alias) else self.output_name(p) for p in projections
        ]
        output_slots = [self.fresh("result") for _ in projections]
        aliases = {
            _key(name): (p.this if isinstance(p, exp.Alias) else p)
            for name, p in zip(output_names, projections, strict=True)
        }
        order_slots = [self.fresh("order") for _ in query.args.get("order", exp.Order()).expressions]
        branches = []
        for terms in sets:
            active = {term.sql() for term in terms}
            spine = self.fresh("spine")
            raw = {}
            measure_joins = []

            def render(node, *, output_alias=False):
                if isinstance(node, exp.Alias):
                    return render(node.this)
                if output_alias and isinstance(node, exp.Column) and not node.table and _key(node.this) in aliases:
                    return render(aliases[_key(node.this)])
                if _definition(node):
                    raise _error("a measure cannot be defined in an aggregated query")
                if isinstance(node, exp.Filter) and _agg(node.this):
                    predicate = self.bind(node.expression.this, sources, ctes, outer)
                    return self.evaluate(
                        self.measure(node.this, sources, outer),
                        joined,
                        alias,
                        terms,
                        group_names,
                        spine,
                        predicate,
                        measure_joins,
                    )
                if _agg(node):
                    return self.evaluate(
                        self.measure(node, sources, outer),
                        joined,
                        alias,
                        terms,
                        group_names,
                        spine,
                        measure_joins=measure_joins,
                    )
                if isinstance(node, (exp.Grouping, exp.GroupingId)):
                    arguments = ([node.this] if node.this is not None else []) + list(node.expressions)
                    bits = [self.bind(arg, sources, ctes, outer).sql() not in active for arg in arguments]
                    value = sum(int(bit) << (len(bits) - index - 1) for index, bit in enumerate(bits))
                    return exp.Literal.number(value)
                if isinstance(node, (exp.AggFunc, exp.Filter, exp.WithinGroup)) and _has_aggregate(node):
                    if any(_agg(child) for child in _walk_scope(node) if child is not node):
                        raise _error("aggregate functions cannot contain another aggregate")
                    bound = self.bind(node, sources, ctes, outer, aggregate=True)
                    key = bound.sql()
                    if key not in raw:
                        raw[key] = (self.fresh("aggregate"), bound)
                    return exp.column(raw[key][0], table=spine)
                if isinstance(node, exp.Subquery):
                    bound_sources = [
                        Source(
                            source.name,
                            Relation(
                                source.relation.query,
                                [column for column in source.relation.columns if column.recipe is None],
                            ),
                            source.alias,
                            source.slots,
                            suppressed=source.suppressed,
                            synthetic=source.synthetic,
                        )
                        for source in sources
                    ]
                    bound = self.bind(node, bound_sources, ctes, outer)

                    def grouped_reference(expression):
                        key = expression.sql()
                        if key in group_names:
                            return exp.column(group_names[key], table=spine)
                        if isinstance(expression, exp.Column) and expression.table == alias:
                            raise _error(f"correlated field {expression.sql()} must be grouped or aggregated")
                        return None

                    return _copy_transform(bound, grouped_reference)
                if not any(_has_aggregate(child) for child in [node]) and not isinstance(node, (exp.Star, exp.Window)):
                    try:
                        bound = self.bind(node, sources, ctes, outer)
                        if bound.sql() in group_names:
                            return exp.column(group_names[bound.sql()], table=spine)
                    except OssieQueryError:
                        if isinstance(node, exp.Column):
                            raise
                if isinstance(node, exp.Column):
                    raise _error(f"field {node.sql()} must be grouped or aggregated")
                if isinstance(node, exp.Star):
                    raise _error("an aggregate projection cannot contain a bare wildcard")
                result = node.copy()
                for key, value in node.args.items():
                    if isinstance(value, exp.Expression):
                        result.set(key, render(value, output_alias=output_alias))
                    elif isinstance(value, list):
                        result.set(
                            key,
                            [
                                render(child, output_alias=output_alias) if isinstance(child, exp.Expression) else child
                                for child in value
                            ],
                        )
                return result

            values = [render(p) for p in projections]
            having = render(query.args["having"].this, output_alias=True) if query.args.get("having") else None
            order_values = [
                render(item.this, output_alias=True)
                if not isinstance(item.this, exp.Literal) or not item.this.is_int
                else values[int(item.this.this) - 1].copy()
                for item in query.args.get("order", exp.Order()).expressions
            ]

            def group_value(term):
                if term.sql() in active:
                    return term.copy()
                if self.dialect == "bigquery":
                    # A bare NULL becomes INT64 in a BigQuery UNION branch.
                    # Keep the grouping expression's type, including empty input.
                    return exp.AnyValue(
                        this=exp.Case(ifs=[exp.If(this=exp.false(), true=term.copy())], default=exp.Null())
                    )
                return exp.Null()

            spine_items = [exp.alias_(group_value(term), group_names[term.sql()]) for term in all_terms]
            spine_items += [exp.alias_(expression, name) for name, expression in raw.values()]
            spine_items.append(exp.alias_(exp.Count(this=exp.Star()), self.fresh("group_size")))
            spine_query = exp.select(*spine_items).from_(exp.to_table(joined).as_(alias))
            if terms:
                spine_query = spine_query.group_by(*(term.copy() for term in terms))
            branch = exp.select(
                *(exp.alias_(value, slot) for value, slot in zip(values, output_slots, strict=True)),
                *(exp.alias_(value, slot) for value, slot in zip(order_values, order_slots, strict=True)),
            ).from_(spine_query.subquery(spine))
            for measure_relation, condition in measure_joins:
                branch = branch.join(measure_relation, on=condition, join_type="LEFT")
            if having is not None:
                branch = branch.where(having)
            branches.append(branch)
        result = branches[0]
        for branch in branches[1:]:
            result = exp.Union(this=result, expression=branch, distinct=False)
        final_alias = self.fresh("groups")
        final = exp.select(*(exp.column(slot, table=final_alias) for slot in output_slots)).from_(
            result.subquery(final_alias)
        )
        final.set("distinct", query.args.get("distinct"))
        if query.args.get("order"):
            order = query.args["order"].copy()
            for item, slot in zip(order.expressions, order_slots, strict=True):
                item.set("this", exp.column(slot, table=final_alias))
            final.set("order", order)
        for name in ("limit", "offset"):
            final.set(name, query.args.get(name))
        return Relation(
            final,
            [
                Column(name, slot, data_type=self.infer_type(projection, sources, outer))
                for name, slot, projection in zip(output_names, output_slots, projections, strict=True)
            ],
        )

    def finish(self, relation, *, allow_measures=False):
        if not allow_measures and any(column.recipe for column in relation.columns):
            raise _error("top-level unevaluated measures must be evaluated with AGG()")
        alias = self.fresh("output")
        expressions = []
        for column in relation.columns:
            value = exp.Null() if column.recipe else exp.column(column.physical, table=alias)
            if column.recipe and self.dialect == "clickhouse":
                value = exp.Cast(this=value, to=exp.DataType.build("Nullable(String)", dialect="clickhouse"))
            expressions.append(exp.Alias(this=value, alias=column.name.copy()))
        return exp.select(*expressions).from_(relation.query.subquery(alias))


def _render_relational_result(query, dialect):
    if dialect not in {"duckdb", "postgres", "sqlite"}:
        for cte in query.find_all(exp.CTE):
            cte.set("materialized", None)
    if dialect == "clickhouse":

        def null_safe(node):
            if isinstance(node, exp.NullSafeEQ):
                left, right = node.this, node.expression
                return exp.or_(
                    exp.EQ(this=left.copy(), expression=right.copy()),
                    exp.and_(
                        exp.Is(this=left.copy(), expression=exp.Null()),
                        exp.Is(this=right.copy(), expression=exp.Null()),
                    ),
                )
            return node

        query = query.transform(null_safe)
        query.set(
            "settings",
            [
                exp.EQ(this=exp.column("join_use_nulls"), expression=exp.Literal.number(1)),
                exp.EQ(this=exp.column("aggregate_functions_null_for_empty"), expression=exp.Literal.number(1)),
                exp.EQ(this=exp.column("join_default_strictness"), expression=exp.Literal.string("ALL")),
            ],
        )
    return render_sql(query, dialect=dialect)


def compile_ossie_relational_query(sql: str, graph: SemanticGraph, dialect: str = "duckdb") -> str | None:
    compiler = RelationalCompiler(graph, dialect)
    if not compiler.models and not graph.metadata.get("ossie_views") and "MEASURE" not in sql.upper():
        return None
    query = compiler.parse(sql)
    if not compiler.relevant(query):
        return None
    relation = compiler.compile(query)
    zero_rows = isinstance(query, exp.Select) and (
        query.args.get("limit") is not None
        and query.args["limit"].expression == exp.Literal.number(0)
        or query.args.get("where") is not None
        and query.args["where"].this == exp.false()
    )
    finished = compiler.finish(relation, allow_measures=zero_rows)
    if zero_rows and dialect == "duckdb":
        for projection, column in zip(finished.expressions, relation.columns, strict=True):
            if column.recipe is not None:
                dtype = {
                    "STRING": "VARCHAR",
                    "INTEGER": "BIGINT",
                    "DECIMAL": "DECIMAL",
                    "FLOAT": "DOUBLE",
                    "BOOLEAN": "BOOLEAN",
                    "DATE": "DATE",
                    "TIME": "TIME",
                    "DATETIME": "TIMESTAMP",
                    "DATETIMETZ": "TIMESTAMPTZ",
                }.get(column.data_type.upper(), "VARCHAR")
                projection.set(
                    "this",
                    exp.Cast(this=exp.Null(), to=exp.DataType.build(f"STRUCT(MEASURE {dtype})", dialect=dialect)),
                )
    return _render_relational_result(finished, dialect)


def compile_ossie_view_definition(sql: str, graph: SemanticGraph, dialect: str = "duckdb"):
    compiler = RelationalCompiler(graph, dialect)
    relation = compiler.compile(compiler.parse(sql))
    descriptors = [
        {"name": column.name.name, "is_measure": column.recipe is not None, "data_type": column.data_type}
        for column in relation.columns
    ]
    return _render_relational_result(compiler.finish(relation, allow_measures=True), dialect), descriptors
