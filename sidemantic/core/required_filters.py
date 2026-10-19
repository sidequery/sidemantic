"""Caller filter obligations shared by structured and SQL query entrypoints."""

from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.scope import traverse_scope


def has_required_filters(graph) -> bool:
    return any(model.required_filters or model.required_filters_or_groups for model in graph.models.values())


def _parse(expression: str, dialect: str) -> exp.Expression:
    try:
        return parse_one(expression, dialect=dialect)
    except SqlglotError as error:
        raise ValueError(f"Cannot validate required filters: invalid SQL expression {expression!r}") from error


def _field(graph, model_name: str, name: str) -> str:
    model = graph.models.get(model_name)
    # Only strip a grain when it actually identifies a declared time dimension.
    if model and "__" in name and not model.get_dimension(name):
        base = name.rsplit("__", 1)[0]
        dimension = model.get_dimension(base)
        if dimension and dimension.type == "time":
            name = base
    return f"{model_name}.{name}"


def _references(graph, expression, models, aliases=None) -> set[str]:
    """Bind outer columns only; subquery columns belong to a separate query."""
    references = set()
    aliases = aliases or {}
    for node in expression.walk(prune=lambda node: isinstance(node, (exp.Select, exp.Subquery))):
        if not isinstance(node, exp.Column):
            continue
        owner = aliases.get(node.table, node.table)
        if owner.endswith("_cte") and owner not in graph.models:
            owner = owner.removesuffix("_cte")
        if not owner:
            candidates = [
                name
                for name in models
                if name in graph.models
                and (
                    graph.models[name].get_dimension(_field(graph, name, node.name).split(".", 1)[1])
                    or graph.models[name].get_metric(node.name)
                )
            ]
            if len(candidates) != 1:
                continue
            owner = candidates[0]
        if owner in graph.models:
            references.add(_field(graph, owner, node.name))
    return references


def _enforce(graph, models, filtered, grouped):
    for name in sorted(models):
        model = graph.models.get(name)
        if model is None:
            continue
        for field in model.required_filters:
            reference = _field(graph, name, field)
            if reference not in filtered:
                raise ValueError(f"a query on {name} must filter {reference}")
        for field in model.required_filters_or_groups:
            reference = _field(graph, name, field)
            if reference not in filtered | grouped:
                raise ValueError(f"a query on {name} must filter or group by {reference}")


def validate_required_filters(
    graph,
    *,
    metrics=(),
    dimensions=(),
    filters=(),
    segments=(),
    ungrouped=False,
    base_model=None,
    parameters=None,
    dialect="duckdb",
):
    """Check resolved caller filters before selecting a compiler engine."""
    if not has_required_filters(graph):
        return
    from sidemantic.core.parameter import ParameterSet
    from sidemantic.sql.generator import SQLGenerator

    generator = SQLGenerator(graph, dialect=dialect, base_model=base_model)
    parameter_set = ParameterSet(graph.parameters, parameters, dialect=dialect)
    predicates = [parameter_set.interpolate(predicate) for predicate in filters or []]
    predicates += generator._resolve_segments(segments or [], interpolate=parameter_set.interpolate)
    models = generator._participating_models(
        generator._find_required_models(list(metrics or []), list(dimensions or []), predicates)
    )
    filtered = set()
    for predicate in predicates:
        filtered.update(_references(graph, _parse(predicate, dialect), models))
    grouped = set()
    if not ungrouped:
        for reference in dimensions or []:
            grouped.update(_references(graph, _parse(reference, dialect), models))
    _enforce(graph, models, filtered, grouped)


def validate_required_filters_sql(graph, sql, *, dialect="duckdb"):
    """Check each SQL read scope independently, before Rust or Python rewriting.

    WHERE predicates and explicit GROUP BY expressions satisfy obligations. Outer
    filters on an already aggregated subquery do not constrain its source read.
    """
    if not has_required_filters(graph):
        return
    from sidemantic.sql.generator import SQLGenerator

    parsed = _parse(sql, dialect)
    for table in parsed.find_all(exp.Table):
        if isinstance(table.this, exp.Anonymous) and table.this.name.lower() == "yardstick":
            raise ValueError("Required model filters require standard semantic SQL; yardstick() is unsupported")
    for scope in traverse_scope(parsed):
        select = scope.expression
        if not isinstance(select, exp.Select):
            continue
        aliases = {
            alias: source.name
            for alias, source in scope.sources.items()
            if isinstance(source, exp.Table) and source.name in graph.models
        }
        models = list(dict.fromkeys(aliases.values()))
        references = set()
        for column in scope.columns:
            references.update(_references(graph, column, models, aliases))
        metrics = [ref for ref in references if graph.models[ref.split(".")[0]].get_metric(ref.split(".")[1])]
        for column in scope.columns:
            reference = f"{column.table}.{column.name}" if column.table else column.name
            if reference in graph.metrics:
                metrics.append(reference)
        dimensions = [ref for ref in references if ref not in metrics]
        generator = SQLGenerator(graph, dialect=dialect)
        models = generator._participating_models([*models, *generator._find_required_models(metrics, dimensions)])
        where = select.args.get("where")
        filtered = _references(graph, where.this, models, aliases) if where else set()
        grouped = set()
        group = select.args.get("group")
        if group:
            selected_aliases = {item.alias: item.this for item in select.expressions if isinstance(item, exp.Alias)}
            for expression in group.expressions:
                if isinstance(expression, exp.Literal) and expression.is_int:
                    index = int(expression.this) - 1
                    if 0 <= index < len(select.expressions):
                        expression = select.expressions[index]
                elif isinstance(expression, exp.Column) and not expression.table:
                    expression = selected_aliases.get(expression.name, expression)
                grouped.update(_references(graph, expression, models, aliases))
        _enforce(graph, models, filtered, grouped)
