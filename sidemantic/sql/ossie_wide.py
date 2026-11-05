"""Apache Ossie 172a3e5 wide-table binding and independent population planner.

Each home dataset supplies a branch. Joining and filtering selects a set of
source row identities per grouping tuple; aggregation happens only after those
identities are deduplicated. Branches never restrict one another's populations.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.annotate_types import annotate_types

from sidemantic.interchange.ossie.identifier import (
    identifier_syntax_valid,
    identifier_within_limit,
    normalize_identifier,
)
from sidemantic.interchange.ossie.portable import lower_ossie_sql, parse_portable_expression
from sidemantic.interchange.ossie.profiles import LATEST_OSSIE_SCHEMA_COMMIT
from sidemantic.sql.rendering import render_sql


class OssieWideQueryError(ValueError):
    """A normative Layer 3 error, with a machine-readable stable code."""

    def __init__(self, code: str, message: str, **details):
        self.code = code
        self.details = details
        super().__init__(f"{code}: {message}")


def is_latest_ossie_graph(graph) -> bool:
    return bool(graph.models) and all(
        (model.metadata or {}).get("ossie_schema_revision") == LATEST_OSSIE_SCHEMA_COMMIT
        for model in graph.models.values()
    )


def _fail(code, message, **details):
    raise OssieWideQueryError(code, message, **details)


def _backend_query_semantics(tree, dialect):
    if dialect == "clickhouse":
        # These are query-local requirements: unmatched rows stay NULL and
        # aggregates keep SQL empty-input values, independent of server defaults.
        tree.set(
            "settings",
            [
                exp.EQ(this=exp.column("join_use_nulls"), expression=exp.Literal.number(1)),
                exp.EQ(this=exp.column("aggregate_functions_null_for_empty"), expression=exp.Literal.number(1)),
                exp.EQ(this=exp.column("join_default_strictness"), expression=exp.Literal.string("ALL")),
            ],
        )
    return tree


def _key(identifier):
    return identifier.name if identifier.args.get("quoted") else identifier.name.upper()


def _reference(text):
    if not isinstance(text, str) or not text.strip():
        _fail("E_INVALID_QUERY", "References must be nonempty identifier strings")
    try:
        expressions = sqlglot.parse(text, read="snowflake")
        node = expressions[0] if len(expressions) == 1 else None
    except sqlglot.errors.SqlglotError as exc:
        _fail("E_INVALID_QUERY", f"Malformed reference {text!r}: {exc}")
    if not isinstance(node, exp.Column) or node.is_star or len(node.parts) not in (1, 2):
        _fail("E_INVALID_QUERY", f"Expected a metric or dataset.field reference, got {text!r}")
    if any(not identifier_within_limit(part.sql(dialect="snowflake")) for part in node.parts):
        _fail("E_INVALID_QUERY", "Identifiers cannot exceed 128 characters")
    return node


def _column_key(column):
    return tuple(_key(part) for part in column.parts)


@dataclass
class _Selection:
    reference: str
    column: exp.Column
    output: str
    output_key: str
    value: object = None
    kind: str | None = None


@dataclass
class _Component:
    index: int
    home: str
    expression: exp.Expression


class _Planner:
    def __init__(self, graph, query, dialect, input_dialect=None):
        self.graph = graph
        self.query = query
        self.dialect = dialect
        self.input_dialect = input_dialect or dialect
        self.models = graph.models
        self.fields = {}
        self.metrics = {}
        self.model_keys = {}
        self.field_slots = {}
        for model_index, (name, model) in enumerate(self.models.items()):
            self.model_keys[normalize_identifier((model.metadata or {}).get("ossie_source_name", name))] = name
            self.field_slots[name] = {dimension.name: index for index, dimension in enumerate(model.dimensions)}
            for dimension in model.dimensions:
                if (dimension.metadata or {}).get("ossie_internal_expression") is not None:
                    continue
                key = (
                    normalize_identifier((model.metadata or {}).get("ossie_source_name", name)),
                    normalize_identifier((dimension.metadata or {}).get("ossie_source_name", dimension.name)),
                )
                self.fields[key] = (name, dimension.name)
        for name, metric in graph.metrics.items():
            metadata = metric.metadata or {}
            owner = metadata.get("ossie_dataset_owner") or graph.metric_owners.get(name)
            if metadata.get("ossie_dataset_metric") and owner in self.models:
                model = self.models[owner]
                key = (
                    normalize_identifier((model.metadata or {}).get("ossie_source_name", owner)),
                    normalize_identifier(metadata.get("ossie_source_name", name.rsplit(".", 1)[-1])),
                )
            else:
                key = (normalize_identifier(metadata.get("ossie_source_name", name)),)
            self.metrics[key] = (name, metric, owner if metadata.get("ossie_dataset_metric") else None)
        self.components = []
        self.metric_formulas = []
        self.metric_homes = []
        self.paths = {}
        self.ctes = []
        self.source_names = {name: f"__ow_source_{index}" for index, name in enumerate(self.models)}

    def q(self, text):
        return exp.to_identifier(text, quoted=True).sql(dialect=self.dialect)

    def col(self, table, name):
        return f"{self.q(table)}.{self.q(name)}"

    def selections(self, kind):
        result = []
        singular = "field" if kind == "dimensions" else "metric"
        for item in self.query.get(kind, []):
            alias = None
            if isinstance(item, str):
                reference = item
            elif isinstance(item, Mapping) and set(item) <= {singular, "alias"} and singular in item:
                reference = item[singular]
                if "alias" in item:
                    alias = item["alias"]
                    if (
                        not isinstance(alias, str)
                        or not identifier_syntax_valid(alias)
                        or not identifier_within_limit(alias)
                    ):
                        _fail("E_INVALID_QUERY", "Aliases must be identifiers")
            else:
                _fail("E_INVALID_QUERY", f"Malformed {kind} entry: {item!r}")
            column = _reference(reference)
            output = sqlglot.parse_one(alias, read="snowflake").this if alias is not None else column.this
            result.append(_Selection(reference, column, output.name, _key(output)))
        return result

    def resolve(self, key, reference):
        if key in self.fields:
            return "field", self.fields[key]
        if key in self.metrics:
            return "metric", self.metrics[key]
        _fail("E_NAME_NOT_FOUND", f"Reference {reference!r} does not resolve", reference=reference)

    def validate_shape(self):
        if not isinstance(self.query, Mapping) or set(self.query) - {"dimensions", "metrics", "filter"}:
            _fail("E_INVALID_QUERY", "A wide-table query accepts only dimensions, metrics and filter")
        for kind in ("dimensions", "metrics"):
            if kind in self.query and not isinstance(self.query[kind], list):
                _fail("E_INVALID_QUERY", f"{kind} must be an array")
        self.dimensions = self.selections("dimensions")
        self.selected_metrics = self.selections("metrics")
        self.filter_tree = None
        if "filter" in self.query:
            text = self.query["filter"]
            if not isinstance(text, str) or not text.strip():
                _fail("E_INVALID_QUERY", "filter must be one nonempty expression string")
            try:
                self.filter_tree = parse_portable_expression(text)
            except ValueError as exc:
                _fail("E_INVALID_QUERY", str(exc))
            # Reject statically known nonboolean predicates before name analysis.
            # Unknown field/extension types remain warehouse-resolved; NULL is a
            # valid predicate and removes the row under SQL three-valued logic.
            predicate_type = annotate_types(self.filter_tree.copy()).type
            if predicate_type and predicate_type.this not in {
                exp.DataType.Type.BOOLEAN,
                exp.DataType.Type.UNKNOWN,
                exp.DataType.Type.NULL,
            }:
                _fail("E_INVALID_QUERY", "filter must be a boolean expression")
        # Do not bypass native query-policy enforcement through this separate binding.
        for model in self.models.values():
            if model.security or model.invariant_filters or model.required_filters or model.required_filters_or_groups:
                _fail("E_INVALID_QUERY", "Wide-table queries over native security/filter policies are not supported")
        if not self.dimensions and not self.selected_metrics:
            _fail("E_EMPTY_AGGREGATION_QUERY", "Select at least one dimension or metric")
        for selection in [*self.dimensions, *self.selected_metrics]:
            selection.kind, selection.value = self.resolve(_column_key(selection.column), selection.reference)
        self.filter_fields = {}
        self.filter_has_metric = False
        if self.filter_tree is not None:
            for column in self.filter_tree.find_all(exp.Column):
                kind, value = self.resolve(_column_key(column), column.sql())
                if kind == "metric":
                    self.filter_has_metric = True
                else:
                    self.filter_fields[_column_key(column)] = value
        if any(selection.kind == "metric" for selection in self.dimensions):
            _fail("E_METRIC_IN_DIMENSIONS", "A dimension entry names a metric")
        if any(selection.kind == "field" for selection in self.selected_metrics):
            _fail("E_FIELD_IN_METRICS", "A metric entry names a field")
        if self.filter_tree is not None:
            if self.filter_has_metric or self.filter_tree.find(exp.AggFunc):
                _fail("E_AGGREGATE_IN_WHERE", "The filter cannot reference metrics or contain aggregates")
            if self.filter_tree.find(exp.Window):
                _fail("E_WINDOW_IN_WHERE", "The filter cannot contain window functions")
        filter_models = {value[0] for value in self.filter_fields.values()}
        if len(filter_models) > 1:
            _fail("E_CROSS_DATASET_FILTER", "The filter reads more than one dataset")
        self.filter_home = next(iter(filter_models), None)
        names = [selection.output_key for selection in [*self.dimensions, *self.selected_metrics]]
        if len(names) != len(set(names)):
            _fail("E_NAME_COLLISION", "Output names must be unique; supply aliases")

    def runtime_home(self, column):
        table = column.table
        if table in self.models:
            return table
        return next((name for name in self.models if name.upper() == table.upper()), None)

    def factor_metrics(self):
        unaggregated = []
        windowed = []
        ambiguous = []
        for selection in self.selected_metrics:
            _, metric, dataset_home = selection.value
            try:
                tree = sqlglot.parse_one(
                    metric.sql, read=(metric.metadata or {}).get("ossie_target_dialect") or self.input_dialect
                )
            except sqlglot.errors.SqlglotError as exc:
                _fail("E_INVALID_QUERY", f"Invalid metric expression: {exc}")
            if any(aggregate.find_ancestor(exp.AggFunc) for aggregate in tree.find_all(exp.AggFunc)):
                _fail("E_INVALID_QUERY", f"Metric {selection.reference!r} contains nested aggregate calls")
            if dataset_home:
                aggregate_nodes = set()
                for aggregate in tree.find_all(exp.AggFunc):
                    atom = aggregate
                    while isinstance(atom.parent, (exp.Filter, exp.WithinGroup)) and atom.parent.this is atom:
                        atom = atom.parent
                    aggregate_nodes.update(id(node) for node in atom.walk())
                if (
                    not aggregate_nodes
                    or tree.find(exp.Window)
                    or any(id(column) not in aggregate_nodes for column in tree.find_all(exp.Column))
                ):
                    _fail("E_INVALID_QUERY", f"Dataset metric {selection.reference!r} is not a valid aggregate measure")
                component = _Component(len(self.components), dataset_home, tree)
                self.components.append(component)
                self.metric_formulas.append(exp.column(f"__ow_c{component.index}"))
                self.metric_homes.append({dataset_home})
                continue
            atoms = []
            for aggregate in tree.find_all(exp.AggFunc):
                if aggregate.find_ancestor(exp.AggFunc):
                    continue
                atom = aggregate
                while isinstance(atom.parent, (exp.Filter, exp.WithinGroup)) and atom.parent.this is atom:
                    atom = atom.parent
                atoms.append((aggregate, atom))
            atom_nodes = {id(node) for _, atom in atoms for node in atom.walk()}
            if any(
                id(column) not in atom_nodes and column.find_ancestor(exp.Window) is None
                for column in tree.find_all(exp.Column)
            ):
                unaggregated.append(selection.reference)
            if tree.find(exp.Window):
                windowed.append(selection.reference)
            homes = set()
            for aggregate, atom in atoms:
                argument_expression = aggregate
                while isinstance(argument_expression.parent, exp.WithinGroup):
                    argument_expression = argument_expression.parent
                argument_homes = {self.runtime_home(column) for column in argument_expression.find_all(exp.Column)}
                atom_homes = {self.runtime_home(column) for column in atom.find_all(exp.Column)}
                if len(argument_homes) != 1 or None in argument_homes or atom_homes != argument_homes:
                    ambiguous.append(selection.reference)
                    continue
                home = next(iter(argument_homes))
                homes.add(home)
                component = _Component(len(self.components), home, atom.copy())
                self.components.append(component)
                replacement = exp.column(f"__ow_c{component.index}")
                if atom is tree:
                    tree = replacement
                else:
                    atom.replace(replacement)
            self.metric_formulas.append(tree)
            self.metric_homes.append(homes)
        if unaggregated:
            _fail("E_UNAGGREGATED_FINER_GRAIN_REFERENCE", f"Unaggregated field in metric {unaggregated[0]!r}")
        if windowed:
            _fail("E_DEFERRED_WINDOWED_METRIC", f"Windowed metric {windowed[0]!r} is deferred")
        if ambiguous:
            _fail("E_AMBIGUOUS_MEASURE_GRAIN", f"Cannot determine a single home dataset for metric {ambiguous[0]!r}")

    def find_paths(self, start, end):
        key = (start, end)
        if key in self.paths:
            return self.paths[key]
        result = []

        def visit(current, seen, path):
            if current == end:
                result.append(path)
                return
            for index, relationship in enumerate(self.models[current].relationships):
                target = relationship.related_model
                if (
                    relationship.type != "many_to_one"
                    or not relationship.active
                    or target not in self.models
                    or target in seen
                ):
                    continue
                visit(target, seen | {target}, [*path, (current, index, relationship)])
                if len(result) >= 2:
                    return

        visit(start, {start}, [])
        self.paths[key] = result
        return result

    def check_paths(self):
        self.dimension_homes = [selection.value[0] for selection in self.dimensions]
        failed = []
        for selection, target in zip(self.dimensions, self.dimension_homes, strict=True):
            reached = [all(self.find_paths(home, target) for home in homes) for homes in self.metric_homes]
            if reached and not all(reached):
                code = "E_PARTIAL_DIMENSION_REACHABILITY" if any(reached) else "E_NO_PATH"
                metric = self.selected_metrics[reached.index(False)].reference
                failed.append((code, selection.reference, metric))
        for code in ("E_NO_PATH", "E_PARTIAL_DIMENSION_REACHABILITY"):
            for error, dimension, metric in failed:
                if error == code:
                    _fail(
                        code,
                        f"Metric {metric!r} cannot reach dimension {dimension!r}",
                        dimension=dimension,
                        metric=metric,
                    )
        self.homes = list(dict.fromkeys(component.home for component in self.components))
        self.anchor_ambiguous = False
        if not self.selected_metrics:
            candidates = [
                home
                for home in dict.fromkeys(self.dimension_homes)
                if all(self.find_paths(home, target) for target in self.dimension_homes)
            ]
            if not candidates:
                _fail("E_NO_ANCHOR", "No dimension dataset reaches all other dimensions")
            self.anchor_ambiguous = len(candidates) > 1
            self.homes = [candidates[0]]
        if self.filter_home:
            for home in self.homes:
                if not self.find_paths(home, self.filter_home):
                    metric = next(
                        (
                            selection.reference
                            for selection, homes in zip(self.selected_metrics, self.metric_homes, strict=True)
                            if home in homes
                        ),
                        home,
                    )
                    _fail(
                        "E_UNSUPPORTED_FILTER_PATH",
                        f"Metric or anchor {metric!r} cannot reach filter dataset {self.filter_home!r}",
                        filter=self.query.get("filter"),
                        metric=metric,
                        anchor=home,
                    )
        if self.anchor_ambiguous:
            _fail("E_AMBIGUOUS_PATH", "More than one dimension dataset qualifies as anchor")
        self.join_trees = {}
        for home in self.homes:
            edges = []
            seen = set()
            targets = [*self.dimension_homes, *([self.filter_home] if self.filter_home else [])]
            for target in targets:
                paths = self.find_paths(home, target)
                if len(paths) > 1:
                    _fail(
                        "E_AMBIGUOUS_PATH",
                        f"More than one directed path from {home!r} to {target!r}",
                        home=home,
                        target=target,
                    )
                for source, index, relationship in paths[0]:
                    if (source, index) not in seen:
                        edges.append((source, relationship))
                        seen.add((source, index))
            self.join_trees[home] = edges

    def sql_expression(self, expression):
        return render_sql(expression, dialect=self.dialect)

    def field_column(self, model, field):
        return self.col(model, f"__ow_f{self.field_slots[model][field]}")

    def sources(self):
        required = {name: set() for name in self.models}
        used = set(self.homes)
        # Constant-only model metrics have no home branch. Their grouped domain
        # is empty; these sources supply field types for that empty projection.
        if self.selected_metrics and not self.homes:
            used.update(selection.value[0] for selection in self.dimensions)
        for selection in self.dimensions:
            owner, field = selection.value
            required[owner].add(field)
        for owner, field in self.filter_fields.values():
            required[owner].add(field)
        for component in self.components:
            for column in component.expression.find_all(exp.Column):
                owner = self.runtime_home(column) or component.home
                required[owner].add(column.name)
        for edges in self.join_trees.values():
            for source, relationship in edges:
                target = relationship.related_model
                used.update((source, target))
                required[source].update(relationship.foreign_key_columns)
                required[target].update(relationship.primary_key_columns)
        self.required_fields = required
        for name, model in self.models.items():
            if name not in used:
                continue
            columns = ["ROW_NUMBER() OVER () AS " + self.q("__ow_rid")]
            for index, dimension in enumerate(model.dimensions):
                if dimension.name not in required[name]:
                    continue
                tree = sqlglot.parse_one(
                    dimension.sql_expr.replace("{model}", name),
                    read=(model.metadata or {}).get("ossie_target_dialect") or self.input_dialect,
                )
                for column in tree.find_all(exp.Column):
                    column.set("table", exp.to_identifier("__ow_raw", quoted=True))
                    column.set("db", None)
                    column.set("catalog", None)
                columns.append(f"{self.sql_expression(tree)} AS {self.q(f'__ow_f{index}')}")
            source_dialect = (
                (model.metadata or {}).get("ossie_source_dialect")
                or (model.metadata or {}).get("ossie_target_dialect")
                or self.input_dialect
            )
            if model.sql:
                source = "(" + self.sql_expression(sqlglot.parse_one(model.sql, read=source_dialect)) + ")"
            else:
                source = self.sql_expression(sqlglot.parse_one(model.table, read=source_dialect, into=exp.Table))
            self.ctes.append(
                (self.source_names[name], f"SELECT {', '.join(columns)} FROM {source} AS {self.q('__ow_raw')}")
            )

    def rewritten_filter(self):
        if self.filter_tree is None:
            return ""
        text = lower_ossie_sql(self.query["filter"], self.dialect)
        tree = sqlglot.parse_one(text, read=self.dialect)
        for column in tree.find_all(exp.Column):
            model, field = self.filter_fields[_column_key(column)]
            column.replace(sqlglot.parse_one(self.field_column(model, field), read=self.dialect))
        return " WHERE " + self.sql_expression(tree)

    def component_sql(self, component, table=None):
        tree = component.expression.copy()
        for column in tree.find_all(exp.Column):
            owner = self.runtime_home(column) or component.home
            if owner != component.home or column.name not in self.field_slots[owner]:
                _fail("E_AMBIGUOUS_MEASURE_GRAIN", "A component reads outside its declared home dataset")
            replacement = exp.column(f"__ow_f{self.field_slots[owner][column.name]}", table=table, quoted=True)
            column.replace(replacement)
        return self.sql_expression(tree)

    def branch(self, home, index):
        joins = f"FROM {self.q(self.source_names[home])} AS {self.q(home)}"
        for source, relationship in self.join_trees[home]:
            target = relationship.related_model
            predicate = " AND ".join(
                f"{self.field_column(source, left)} = {self.field_column(target, right)}"
                for left, right in zip(relationship.foreign_key_columns, relationship.primary_key_columns, strict=True)
            )
            joins += f" LEFT JOIN {self.q(self.source_names[target])} AS {self.q(target)} ON {predicate}"
        projections = [
            f"{self.field_column(*selection.value)} AS {self.q(f'__ow_d{i}')}"
            for i, selection in enumerate(self.dimensions)
        ]
        joined_name = f"__ow_joined_{index}"
        components = [component for component in self.components if component.home == home]
        if not components:
            self.ctes.append(
                (joined_name, f"SELECT DISTINCT {', '.join(projections)} {joins}{self.rewritten_filter()}")
            )
            return joined_name
        projections.append(f"{self.col(home, '__ow_rid')} AS {self.q('__ow_rid')}")
        projections.extend(
            f"{self.col(home, f'__ow_f{i}')} AS {self.q(f'__ow_f{i}')}"
            for field, i in self.field_slots[home].items()
            if field in self.required_fields[home]
        )
        self.ctes.append((joined_name, f"SELECT DISTINCT {', '.join(projections)} {joins}{self.rewritten_filter()}"))
        names = [self.q(f"__ow_d{i}") for i in range(len(self.dimensions))]
        aggregates = [
            f"{self.component_sql(component)} AS {self.q(f'__ow_c{component.index}')}" for component in components
        ]
        branch_name = f"__ow_branch_{index}"
        group = " GROUP BY " + ", ".join(names) if names else ""
        self.ctes.append(
            (
                branch_name,
                f"SELECT {', '.join([*names, *aggregates, '1 AS ' + self.q('__ow_present')])} FROM {self.q(joined_name)}{group}",
            )
        )
        empty = ", ".join(
            f"{self.component_sql(component)} AS {self.q(f'__ow_c{component.index}')}" for component in components
        )
        self.ctes.append((f"__ow_empty_{index}", f"SELECT {empty} FROM {self.q(self.source_names[home])} WHERE 1 = 0"))
        return branch_name

    def compile(self):
        self.validate_shape()
        self.factor_metrics()
        self.check_paths()
        self.sources()
        branches = [self.branch(home, index) for index, home in enumerate(self.homes)]
        dimension_keys = [self.q(f"__ow_d{i}") for i in range(len(self.dimensions))]
        if not self.selected_metrics:
            final = f"SELECT {', '.join(f'{key} AS {self.q(selection.output)}' for key, selection in zip(dimension_keys, self.dimensions, strict=True))} FROM {self.q(branches[0])}"
        else:
            if dimension_keys and not branches:
                typed_columns = [
                    f"{self.field_column(*selection.value)} AS {self.q(f'__ow_d{i}')}"
                    for i, selection in enumerate(self.dimensions)
                ]
                typed_sources = " CROSS JOIN ".join(
                    f"{self.q(self.source_names[owner])} AS {self.q(owner)}"
                    for owner in dict.fromkeys(selection.value[0] for selection in self.dimensions)
                )
                spine = f"SELECT {', '.join(typed_columns)} FROM {typed_sources} WHERE FALSE"
            elif dimension_keys:
                spine = " UNION DISTINCT ".join(
                    f"SELECT {', '.join(dimension_keys)} FROM {self.q(branch)}" for branch in branches
                )
            else:
                spine = "SELECT 1 AS " + self.q("__ow_singleton")
            self.ctes.append(("__ow_keys", spine))
            joins = f"FROM {self.q('__ow_keys')} AS {self.q('__ow_keys')}"
            component_values = {}
            for index, (home, branch) in enumerate(zip(self.homes, branches, strict=True)):
                alias = f"__ow_b{index}"
                if dimension_keys:
                    predicate = " AND ".join(
                        f"{self.col(alias, f'__ow_d{i}')} IS NOT DISTINCT FROM {self.col('__ow_keys', f'__ow_d{i}')}"
                        for i in range(len(self.dimensions))
                    )
                    joins += f" LEFT JOIN {self.q(branch)} AS {self.q(alias)} ON {predicate}"
                else:
                    joins += f" CROSS JOIN {self.q(branch)} AS {self.q(alias)}"
                empty_alias = f"__ow_e{index}"
                joins += f" CROSS JOIN {self.q(f'__ow_empty_{index}')} AS {self.q(empty_alias)}"
                for component in self.components:
                    if component.home != home:
                        continue
                    value = f"CASE WHEN {self.col(alias, '__ow_present')} IS NULL THEN {self.col(empty_alias, f'__ow_c{component.index}')} ELSE {self.col(alias, f'__ow_c{component.index}')} END"
                    component_values[f"__ow_c{component.index}"] = sqlglot.parse_one(value, read=self.dialect)
            outputs = [
                f"{self.col('__ow_keys', f'__ow_d{i}')} AS {self.q(selection.output)}"
                for i, selection in enumerate(self.dimensions)
            ]
            for selection, formula in zip(self.selected_metrics, self.metric_formulas, strict=True):
                tree = formula.copy()
                for column in list(tree.find_all(exp.Column)):
                    if column.name in component_values:
                        replacement = component_values[column.name].copy()
                        if column is tree:
                            tree = replacement
                        else:
                            column.replace(replacement)
                outputs.append(f"{self.sql_expression(tree)} AS {self.q(selection.output)}")
            final = f"SELECT {', '.join(outputs)} {joins}"
        ctes = ",\n".join(f"{self.q(name)} AS ({sql})" for name, sql in self.ctes)
        return render_sql(
            _backend_query_semantics(sqlglot.parse_one(f"WITH {ctes}\n{final}", read=self.dialect), self.dialect),
            dialect=self.dialect,
            pretty=True,
        )


def compile_ossie_wide_query(graph, query, dialect="duckdb", *, input_dialect=None) -> str:
    """Compile the strict Layer 3 JSON binding, without native-query extensions."""
    if not is_latest_ossie_graph(graph):
        _fail("E_INVALID_QUERY", "This interface requires one imported latest Ossie scope")
    return _Planner(graph, query, dialect, input_dialect).compile()


def compile_ossie_wide_extended(
    graph,
    *,
    metrics=None,
    dimensions=None,
    filters=None,
    aliases=None,
    order_by=None,
    limit=None,
    offset=None,
    dialect="duckdb",
    **options,
):
    """Existing structured API: explicit aliases, ordering and pagination extensions.

    Output naming follows the established Sidemantic binding. Population and
    directed-path semantics are those of the strict wide-table query.
    """
    if any(value for value in options.values()):
        _fail("E_INVALID_QUERY", "This option is not supported by the Ossie wide-table binding")
    aliases = aliases or {}
    query = {"dimensions": [], "metrics": []}
    output_names = {}
    for kind, values in (("dimensions", dimensions or []), ("metrics", metrics or [])):
        for reference in values:
            column = _reference(reference)
            default = reference if kind == "metrics" else column.name
            name = aliases.get(reference, default)
            output_names[reference] = name
            query[kind].append(
                {"field" if kind == "dimensions" else "metric": reference, "alias": '"' + name.replace('"', '""') + '"'}
            )
    if filters:
        if not isinstance(filters, (list, tuple)) or not all(isinstance(value, str) for value in filters):
            _fail("E_INVALID_QUERY", "filters must be a list of expression strings")
        query["filter"] = " AND ".join(f"({value})" for value in filters)
    sql = compile_ossie_wide_query(graph, query, dialect)
    if not order_by and limit is None and offset is None:
        return sql
    inner = sqlglot.parse_one(sql, read=dialect)
    inner.set("settings", None)
    tree = sqlglot.select("*").from_(inner.subquery("__ow_result"))
    orders = []
    for item in order_by or []:
        try:
            parsed = sqlglot.parse_one("SELECT 1 ORDER BY " + item, read=dialect).args["order"].expressions[0]
        except sqlglot.errors.SqlglotError:
            _fail("E_INVALID_QUERY", "Malformed order_by extension")
        reference = parsed.this
        if not isinstance(reference, exp.Column):
            _fail("E_INVALID_QUERY", "Ordering must name a selected output")
        match = next(
            (value for key, value in output_names.items() if _column_key(_reference(key)) == _column_key(reference)),
            None,
        )
        if match is None and reference.name in output_names.values():
            match = reference.name
        if match is None:
            _fail("E_NAME_NOT_FOUND", f"Ordering field {item!r} is not selected")
        parsed.set("this", exp.column(match, quoted=True))
        orders.append(parsed)
    if orders:
        tree.set("order", exp.Order(expressions=orders))
    if limit is not None:
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
            _fail("E_INVALID_QUERY", "limit must be a nonnegative integer")
        tree = tree.limit(limit)
    if offset is not None:
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            _fail("E_INVALID_QUERY", "offset must be a nonnegative integer")
        tree = tree.offset(offset)
    return render_sql(_backend_query_semantics(tree, dialect), dialect=dialect, pretty=True)
