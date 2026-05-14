"""TMDL adapter for importing/exporting Power BI Tabular Model Definition Language files."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from sidemantic.adapters.base import BaseAdapter
from sidemantic.adapters.tmdl_parser import TmdlExpression, TmdlNode, TmdlParser, TmdlProperty, merge_documents
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph

if TYPE_CHECKING:
    from sidemantic_dax.ast import Expr as DaxExpr


TmdlImportWarning = dict[str, Any]
TmdlExportWarning = dict[str, Any]


class DaxRuntimeUnavailableError(RuntimeError):
    """Raised when TMDL contains DAX but the optional DAX parser is unavailable."""


class TMDLAdapter(BaseAdapter):
    """Adapter for importing/exporting Power BI TMDL models."""

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse TMDL files into semantic graph."""
        source_path = Path(source)
        if not source_path.exists():
            raise FileNotFoundError(source)

        tmdl_root, files = _collect_tmdl_files(source_path)
        parser = TmdlParser()
        documents = [parser.parse(path.read_text(), file=_display_tmdl_path(path, tmdl_root)) for path in files]
        merged_nodes = merge_documents(documents)

        graph = SemanticGraph()
        warnings: list[TmdlImportWarning] = []
        database_passthrough_node = _select_database_passthrough_node(merged_nodes)
        model_passthrough_node = _select_model_passthrough_node(merged_nodes)
        relationship_nodes = [node for node in _find_nodes(merged_nodes, {"relationship"}) if not _is_ref_only(node)]
        table_nodes = [
            node for node in _find_nodes(merged_nodes, {"table", "calculatedtable"}) if not _is_ref_only(node)
        ]
        column_sql_by_table, measure_names_by_table, measure_aggs_by_table, time_dimensions_by_table = (
            _collect_table_metadata(table_nodes)
        )

        for table_node in table_nodes:
            model = _table_to_model(
                table_node,
                tmdl_root,
                column_sql_by_table,
                measure_names_by_table,
                measure_aggs_by_table,
                time_dimensions_by_table,
                warnings,
            )
            model._source_format = "TMDL"
            graph.add_model(model)

        _apply_relationships(graph, relationship_nodes, tmdl_root, warnings)

        if database_passthrough_node is not None:
            if database_passthrough_node.name:
                graph._tmdl_database_name = database_passthrough_node.name
            if database_passthrough_node.name_raw:
                graph._tmdl_database_name_raw = database_passthrough_node.name_raw
            if database_passthrough_node.leading_comments:
                graph._tmdl_database_leading_comments = list(database_passthrough_node.leading_comments)
            model_ref_node = next(
                (child for child in database_passthrough_node.children if child.type.lower() == "model" and child.name),
                None,
            )
            model_ref = model_ref_node.name if model_ref_node else None
            if model_ref:
                graph._tmdl_database_model_name = model_ref
            if model_ref_node and model_ref_node.name_raw:
                graph._tmdl_database_model_name_raw = model_ref_node.name_raw
            if database_passthrough_node.description:
                graph._tmdl_database_description = database_passthrough_node.description
            database_props = _node_passthrough_properties(database_passthrough_node, set())
            if database_props:
                graph._tmdl_database_properties = database_props
            database_children = [
                _clone_tmdl_node(child) for child in database_passthrough_node.children if child.type.lower() != "model"
            ]
            if database_children:
                graph._tmdl_database_child_nodes = database_children

        if model_passthrough_node is not None:
            if model_passthrough_node.name:
                graph._tmdl_model_name = model_passthrough_node.name
            if model_passthrough_node.name_raw:
                graph._tmdl_model_name_raw = model_passthrough_node.name_raw
            if model_passthrough_node.leading_comments:
                graph._tmdl_model_leading_comments = list(model_passthrough_node.leading_comments)
            if model_passthrough_node.description:
                graph._tmdl_model_description = model_passthrough_node.description
            model_props = _node_passthrough_properties(model_passthrough_node, set())
            if model_props:
                graph._tmdl_model_properties = model_props
            model_table_refs = [
                (child.name, child.name_raw)
                for child in model_passthrough_node.children
                if child.is_ref and child.type.lower() == "table" and child.name
            ]
            if model_table_refs:
                graph._tmdl_model_table_refs = model_table_refs
            model_relationship_refs = [
                (child.name, child.name_raw)
                for child in model_passthrough_node.children
                if child.is_ref and child.type.lower() == "relationship" and child.name
            ]
            if model_relationship_refs:
                graph._tmdl_model_relationship_refs = model_relationship_refs
            model_children = [
                _clone_tmdl_node(child)
                for child in model_passthrough_node.children
                if not _is_model_ref_node(child)
                and child.type.lower() not in {"table", "calculatedtable", "relationship"}
            ]
            if model_children:
                graph._tmdl_model_child_nodes = model_children

        root_passthrough_nodes = _collect_graph_root_passthrough_nodes(merged_nodes)
        if root_passthrough_nodes:
            graph._tmdl_root_nodes = root_passthrough_nodes

        graph.build_adjacency()
        graph.import_warnings = warnings
        return graph

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to a TMDL folder structure."""
        output_path = Path(output_path)
        if output_path.suffix:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            export_warnings: list[TmdlExportWarning] = []
            output_path.write_text(_export_script(graph, output_path.stem, export_warnings))
            graph.export_warnings = export_warnings
            return

        output_path.mkdir(parents=True, exist_ok=True)
        definition_dir = output_path / "definition"
        definition_dir.mkdir(parents=True, exist_ok=True)
        tables_dir = definition_dir / "tables"
        tables_dir.mkdir(parents=True, exist_ok=True)

        project_name = output_path.name

        (definition_dir / "database.tmdl").write_text(_export_database(graph, project_name))
        (definition_dir / "model.tmdl").write_text(_export_model(graph, project_name))

        for model in graph.models.values():
            table_file = _export_table_file_path(tables_dir, model)
            table_file.write_text(_export_table(model))

        export_warnings: list[TmdlExportWarning] = []
        relationships_text = _export_relationships(graph, export_warnings)
        if relationships_text:
            (definition_dir / "relationships.tmdl").write_text(relationships_text)
        graph.export_warnings = export_warnings


def _collect_tmdl_files(source_path: Path) -> tuple[Path, list[Path]]:
    if source_path.is_file():
        return source_path.parent, [source_path]

    definition_dir = source_path / "definition"
    root = definition_dir if definition_dir.is_dir() else source_path
    files = sorted(root.rglob("*.tmdl"))
    return root, files


def _display_tmdl_path(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _export_table_file_path(tables_dir: Path, model: Model) -> Path:
    source_file = getattr(model, "_source_file", None)
    if isinstance(source_file, str) and source_file:
        source_path = Path(source_file)
        if source_path.suffix.lower() == ".tmdl" and source_path.parent == Path("tables"):
            return tables_dir / source_path.name
    return tables_dir / f"{_safe_filename(model.name)}.tmdl"


def _find_nodes(nodes: list[TmdlNode], types: set[str]) -> list[TmdlNode]:
    found: list[TmdlNode] = []
    for node in nodes:
        if node.type.lower() in types:
            found.append(node)
        if node.children:
            found.extend(_find_nodes(node.children, types))
    return found


def _is_ref_only(node: TmdlNode) -> bool:
    return node.is_ref and not node.properties and not node.children and node.default_property is None


def _is_model_ref_node(node: TmdlNode) -> bool:
    if not node.is_ref:
        return False
    return node.type.lower() in {"table", "relationship"}


def _select_model_passthrough_node(nodes: list[TmdlNode]) -> TmdlNode | None:
    candidates = [node for node in _find_nodes(nodes, {"model"}) if not node.is_ref]
    if not candidates:
        return None

    def score(node: TmdlNode) -> tuple[int, int]:
        rich = int(
            bool(node.properties)
            or bool(node.children)
            or bool(node.description)
            or bool(node.default_property)
            or bool(node.leading_comments)
        )
        size = len(node.properties) + len(node.children)
        return (rich, size)

    return max(candidates, key=score)


def _select_database_passthrough_node(nodes: list[TmdlNode]) -> TmdlNode | None:
    candidates = [node for node in _find_nodes(nodes, {"database"}) if not node.is_ref]
    if not candidates:
        return None

    def score(node: TmdlNode) -> tuple[int, int]:
        rich = int(
            bool(node.properties)
            or bool(node.children)
            or bool(node.description)
            or bool(node.default_property)
            or bool(node.leading_comments)
        )
        size = len(node.properties) + len(node.children)
        return (rich, size)

    return max(candidates, key=score)


def _collect_graph_root_passthrough_nodes(nodes: list[TmdlNode]) -> list[TmdlNode]:
    passthrough: list[TmdlNode] = []
    for node in nodes:
        node_type = node.type.lower()
        if node_type == "createorreplace":
            passthrough.extend(_collect_graph_root_passthrough_nodes(node.children))
            continue
        if node_type in {"database", "model", "table", "calculatedtable", "relationship"}:
            continue
        passthrough.append(_clone_tmdl_node(node))
    return passthrough


def _collect_table_metadata(
    table_nodes: list[TmdlNode],
) -> tuple[
    dict[str, dict[str, str]],
    dict[str, set[str]],
    dict[str, dict[str, str]],
    dict[str, set[str]],
]:
    column_sql_by_table: dict[str, dict[str, str]] = {}
    measure_names_by_table: dict[str, set[str]] = {}
    measure_aggs_by_table: dict[str, dict[str, str]] = {}
    time_dimensions_by_table: dict[str, set[str]] = {}

    for table_node in table_nodes:
        table_name = table_node.name or ""
        column_sql: dict[str, str] = {}
        measure_names: set[str] = set()
        measure_aggs: dict[str, str] = {}
        time_dimensions: set[str] = set()

        for child in table_node.children:
            child_type = child.type.lower()
            if child_type in ("column", "calculatedcolumn"):
                props = _props(child)
                dim_type, _ = _map_data_type(_string_prop(props.get("datatype")))
                expression = _resolve_expression(child, props)
                source_column = _string_prop(props.get("sourcecolumn"))
                sql = source_column or expression or (child.name or "")
                column_sql[child.name or ""] = sql
                if dim_type == "time" and child.name:
                    time_dimensions.add(child.name)
            elif child_type == "measure":
                measure_name = child.name or ""
                measure_names.add(measure_name)
                expr_text = _resolve_expression(child, _props(child))
                if expr_text:
                    agg, _sql = _extract_dax_agg(expr_text, table_name)
                    if agg:
                        measure_aggs[measure_name] = agg

        column_sql_by_table[table_name] = column_sql
        measure_names_by_table[table_name] = measure_names
        measure_aggs_by_table[table_name] = measure_aggs
        time_dimensions_by_table[table_name] = time_dimensions

    return column_sql_by_table, measure_names_by_table, measure_aggs_by_table, time_dimensions_by_table


def _table_to_model(
    node: TmdlNode,
    root: Path,
    column_sql_by_table: dict[str, dict[str, str]],
    measure_names_by_table: dict[str, set[str]],
    measure_aggs_by_table: dict[str, dict[str, str]],
    time_dimensions_by_table: dict[str, set[str]],
    warnings: list[TmdlImportWarning],
) -> Model:
    props = _props(node)
    description = node.description or _string_prop(props.get("description"))
    dimensions: list[Dimension] = []
    metrics: list[Metric] = []
    passthrough_children: list[TmdlNode] = []
    primary_key = None
    original_expression: str | None = None

    for child in node.children:
        child_type = child.type.lower()
        if child_type in ("column", "calculatedcolumn"):
            dim = _column_to_dimension(
                child,
                node.name or "",
                root,
                column_sql_by_table,
                measure_names_by_table,
                time_dimensions_by_table,
                warnings,
            )
            dimensions.append(dim)
            if _is_true(_props(child).get("iskey")):
                primary_key = dim.name
        elif child_type == "measure":
            parsed_metrics = _measure_to_metric(
                child,
                node.name or "",
                root,
                column_sql_by_table,
                measure_names_by_table,
                measure_aggs_by_table,
                time_dimensions_by_table,
                warnings,
            )
            if parsed_metrics:
                metrics.extend(parsed_metrics)
        else:
            passthrough_children.append(_clone_tmdl_node(child))

    model_sql = None
    model_table = node.name or None
    model_dax = None
    model_expression_language = None
    if node.type.lower() == "calculatedtable":
        model_table = None
        expression_obj = _resolve_expression_object(node, props)
        expr_text = expression_obj.text if expression_obj else None
        original_expression = expr_text
        model_dax = expr_text
        model_expression_language = "dax" if expr_text else None
        if expr_text:
            try:
                dax_expr = _parse_dax_expression(expr_text, node, "table")
            except DaxRuntimeUnavailableError as exc:
                _append_import_warning(
                    warnings,
                    node,
                    code="dax_parser_unavailable",
                    context="calculated_table",
                    message=str(exc),
                    model_name=node.name,
                )
                dax_expr = None
            except ValueError as exc:
                _append_import_warning(
                    warnings,
                    node,
                    code="dax_parse_error",
                    context="calculated_table",
                    message=str(exc),
                    model_name=node.name,
                )
                dax_expr = None
            if dax_expr is not None:
                model_sql = None

    model = Model(
        name=node.name or "",
        table=model_table,
        sql=model_sql,
        dax=model_dax,
        expression_language=model_expression_language,
        description=description,
        primary_key=primary_key or "id",
        dimensions=dimensions,
        metrics=metrics,
        default_time_dimension=_find_default_time_dimension(dimensions),
        default_grain=_find_default_grain(dimensions),
    )
    if node.name_raw:
        model._tmdl_name_raw = node.name_raw
    if node.leading_comments:
        model._tmdl_leading_comments = list(node.leading_comments)

    if node.location and node.location.file:
        try:
            model._source_file = str(Path(node.location.file).relative_to(root))
        except ValueError:
            model._source_file = node.location.file
    if node.type.lower() == "calculatedtable":
        model._tmdl_node_type = "calculatedTable"
        if original_expression:
            model._tmdl_expression = original_expression
            model.dax = original_expression
        expression_obj = _resolve_expression_object(node, props)
        if expression_obj is not None:
            model._tmdl_expression_obj = _clone_tmdl_value(expression_obj)
        if "dax_expr" in locals() and dax_expr is not None:
            model._dax_ast = dax_expr
    table_props = _node_passthrough_properties(node, {"description", "expression"})
    if table_props:
        model._tmdl_properties = table_props
    raw_value_props = _node_raw_value_properties(node)
    if raw_value_props:
        model._tmdl_raw_value_properties = raw_value_props
    if passthrough_children:
        model._tmdl_child_nodes = passthrough_children

    return model


def _column_to_dimension(
    node: TmdlNode,
    table_name: str,
    root: Path,
    column_sql_by_table: dict[str, dict[str, str]],
    measure_names_by_table: dict[str, set[str]],
    time_dimensions_by_table: dict[str, set[str]],
    warnings: list[TmdlImportWarning],
) -> Dimension:
    props = _props(node)
    data_type = _string_prop(props.get("datatype"))
    dim_type, granularity = _map_data_type(data_type)

    expression_obj = _resolve_expression_object(node, props)
    expression = expression_obj.text if expression_obj else None

    source_column = _string_prop(props.get("sourcecolumn"))
    sql = source_column or expression
    if expression:
        try:
            dax_expr = _parse_dax_expression(expression, node, "column")
        except DaxRuntimeUnavailableError as exc:
            _append_import_warning(
                warnings,
                node,
                code="dax_parser_unavailable",
                context="column",
                message=str(exc),
                model_name=table_name,
            )
            dax_expr = None
        except ValueError as exc:
            _append_import_warning(
                warnings,
                node,
                code="dax_parse_error",
                context="column",
                message=str(exc),
                model_name=table_name,
            )
            dax_expr = None
    else:
        dax_expr = None
    if expression and not sql:
        sql = source_column or expression
    if not sql:
        sql = node.name or ""

    dimension = Dimension(
        name=node.name or "",
        type=dim_type,
        sql=sql,
        dax=expression,
        granularity=granularity,
        description=node.description or _string_prop(props.get("description")),
        label=_string_prop(props.get("caption")),
        format=_string_prop(props.get("formatstring")),
        public=not _is_true(props.get("ishidden")),
    )
    if expression:
        dimension.expression_language = "dax"
    dimension._source_format = "TMDL"
    if node.location and node.location.file:
        try:
            dimension._source_file = str(Path(node.location.file).relative_to(root))
        except ValueError:
            dimension._source_file = node.location.file
    if dax_expr is not None:
        dimension._dax_ast = dax_expr
    if node.name_raw:
        dimension._tmdl_name_raw = node.name_raw
    if node.leading_comments:
        dimension._tmdl_leading_comments = list(node.leading_comments)
    if expression:
        dimension._tmdl_expression = expression
    if expression_obj is not None:
        dimension._tmdl_expression_obj = _clone_tmdl_value(expression_obj)
    if data_type:
        dimension._tmdl_data_type = data_type
    if node.type:
        dimension._tmdl_node_type = node.type
    column_props = _node_passthrough_properties(
        node,
        {"datatype", "iskey", "caption", "formatstring", "description", "sourcecolumn", "expression", "ishidden"},
    )
    if column_props:
        dimension._tmdl_properties = column_props
    raw_value_props = _node_raw_value_properties(node)
    if raw_value_props:
        dimension._tmdl_raw_value_properties = raw_value_props
    property_order = [prop.name.lower() for prop in node.properties if isinstance(prop.name, str)]
    if property_order:
        dimension._tmdl_property_order = property_order
    if node.children:
        dimension._tmdl_child_nodes = [_clone_tmdl_node(child) for child in node.children]
    return dimension


def _measure_to_metric(
    node: TmdlNode,
    table_name: str,
    root: Path,
    column_sql_by_table: dict[str, dict[str, str]],
    measure_names_by_table: dict[str, set[str]],
    measure_aggs_by_table: dict[str, dict[str, str]],
    time_dimensions_by_table: dict[str, set[str]],
    warnings: list[TmdlImportWarning],
) -> list[Metric]:
    props = _props(node)
    expression_obj = _resolve_expression_object(node, props)
    expression = expression_obj.text if expression_obj else None

    if not expression:
        return []

    try:
        dax_expr = _parse_dax_expression(expression, node, "measure")
    except DaxRuntimeUnavailableError as exc:
        _append_import_warning(
            warnings,
            node,
            code="dax_parser_unavailable",
            context="measure",
            message=str(exc),
            model_name=table_name,
        )
        dax_expr = None
    except ValueError as exc:
        _append_import_warning(
            warnings,
            node,
            code="dax_parse_error",
            context="measure",
            message=str(exc),
            model_name=table_name,
        )
        dax_expr = None
    agg, sql = _extract_dax_agg(expression, table_name, dax_expr)
    metric_type = None if agg else "derived"
    metric = Metric(
        name=node.name or "",
        agg=agg,
        sql=sql or expression if not agg else sql,
        dax=expression,
        type=metric_type,
        description=node.description or _string_prop(_props(node).get("description")),
        label=_string_prop(_props(node).get("caption")),
        format=_string_prop(_props(node).get("formatstring")),
        public=not _is_true(props.get("ishidden")),
    )
    if expression:
        metric.expression_language = "dax"
    metric._source_format = "TMDL"
    if node.location and node.location.file:
        try:
            metric._source_file = str(Path(node.location.file).relative_to(root))
        except ValueError:
            metric._source_file = node.location.file
    if dax_expr is not None:
        metric._dax_ast = dax_expr
    if node.name_raw:
        metric._tmdl_name_raw = node.name_raw
    if node.leading_comments:
        metric._tmdl_leading_comments = list(node.leading_comments)
    metric._tmdl_expression = expression
    if expression_obj is not None:
        metric._tmdl_expression_obj = _clone_tmdl_value(expression_obj)
    measure_props = _node_passthrough_properties(
        node, {"caption", "formatstring", "description", "expression", "ishidden"}
    )
    if measure_props:
        metric._tmdl_properties = measure_props
    raw_value_props = _node_raw_value_properties(node)
    if raw_value_props:
        metric._tmdl_raw_value_properties = raw_value_props
    property_order = [prop.name.lower() for prop in node.properties if isinstance(prop.name, str)]
    if property_order:
        metric._tmdl_property_order = property_order
    if node.children:
        metric._tmdl_child_nodes = [_clone_tmdl_node(child) for child in node.children]
    return [metric]


def _apply_relationships(
    graph: SemanticGraph, nodes: list[TmdlNode], root: Path, warnings: list[TmdlImportWarning] | None = None
) -> None:
    for node in nodes:
        props = _props(node)
        active = not _is_false(props.get("isactive"))

        from_ref = _string_prop(props.get("fromcolumn"))
        to_ref = _string_prop(props.get("tocolumn"))
        from_table, from_column = _parse_column_reference(from_ref)
        to_table, to_column = _parse_column_reference(to_ref)

        if not from_table or not to_table:
            if warnings is not None:
                _append_import_warning(
                    warnings,
                    node,
                    code="relationship_parse_skip",
                    context="relationship",
                    message="Skipping relationship: invalid fromColumn/toColumn reference",
                )
            continue
        if from_table not in graph.models or to_table not in graph.models:
            if warnings is not None:
                _append_import_warning(
                    warnings,
                    node,
                    code="relationship_parse_skip",
                    context="relationship",
                    message=(f"Skipping relationship: unknown model reference from='{from_table}' to='{to_table}'"),
                )
            continue

        from_cardinality = _string_prop(props.get("fromcardinality"))
        to_cardinality = _string_prop(props.get("tocardinality"))
        rel_type = _map_relationship_type(
            from_cardinality,
            to_cardinality,
        )
        if not rel_type:
            if warnings is not None:
                _append_import_warning(
                    warnings,
                    node,
                    code="relationship_parse_skip",
                    context="relationship",
                    message=(
                        "Skipping relationship: unsupported cardinality "
                        f"from='{from_cardinality or ''}' to='{to_cardinality or ''}'"
                    ),
                )
            continue

        if rel_type == "many_to_one":
            foreign_key = from_column
            primary_key = to_column
        elif rel_type in ("one_to_many", "one_to_one"):
            foreign_key = to_column
            primary_key = None
        elif rel_type == "many_to_many":
            foreign_key = from_column
            primary_key = to_column
        else:
            foreign_key = None
            primary_key = None

        relationship_props = _relationship_passthrough_properties(node)
        relationship = Relationship(
            name=to_table,
            type=rel_type,
            foreign_key=foreign_key,
            primary_key=primary_key,
            active=active,
        )
        if "isactive" in props and active:
            relationship._tmdl_is_active_explicit = True
        relationship._tmdl_from_column = from_column
        relationship._tmdl_to_column = to_column
        if node.name:
            relationship._tmdl_relationship_name = node.name
        relationship._source_format = "TMDL"
        if node.location and node.location.file:
            try:
                relationship._source_file = str(Path(node.location.file).relative_to(root))
            except ValueError:
                relationship._source_file = node.location.file
        if node.name_raw:
            relationship._tmdl_relationship_name_raw = node.name_raw
        if node.description:
            relationship._tmdl_description = node.description
        if node.leading_comments:
            relationship._tmdl_leading_comments = list(node.leading_comments)
        if relationship_props:
            relationship._tmdl_relationship_properties = relationship_props
        raw_value_props = _node_raw_value_properties(node)
        if raw_value_props:
            relationship._tmdl_raw_value_properties = raw_value_props
        property_order = [prop.name.lower() for prop in node.properties if isinstance(prop.name, str)]
        if property_order:
            relationship._tmdl_property_order = property_order
        if node.children:
            relationship._tmdl_child_nodes = [_clone_tmdl_node(child) for child in node.children]

        model = graph.models[from_table]
        if not any(
            existing.name == relationship.name
            and existing.type == relationship.type
            and existing.foreign_key == relationship.foreign_key
            and existing.primary_key == relationship.primary_key
            for existing in model.relationships
        ):
            model.relationships.append(relationship)


def _node_passthrough_properties(node: TmdlNode, excluded_keys: set[str]) -> list[dict[str, Any]]:
    passthrough: list[dict[str, Any]] = []
    for prop in node.properties:
        prop_key = prop.name.lower()
        if prop_key in excluded_keys:
            continue

        entry: dict[str, Any] = {"name": prop.name, "kind": prop.kind, "value": _clone_tmdl_value(prop.value)}
        if isinstance(prop.raw, str):
            entry["raw"] = prop.raw
        passthrough.append(entry)
    return passthrough


def _node_raw_value_properties(node: TmdlNode) -> dict[str, str]:
    raw_props: dict[str, str] = {}
    for prop in node.properties:
        if prop.kind != "value":
            continue
        if not isinstance(prop.raw, str):
            continue
        raw_props.setdefault(prop.name.lower(), prop.raw)
    return raw_props


def _relationship_passthrough_properties(node: TmdlNode) -> list[dict[str, Any]]:
    return _node_passthrough_properties(
        node,
        {"fromcolumn", "tocolumn", "fromcardinality", "tocardinality", "isactive"},
    )


def _clone_tmdl_value(value: Any) -> Any:
    if isinstance(value, TmdlExpression):
        return TmdlExpression(
            text=value.text,
            meta=dict(value.meta) if value.meta else None,
            meta_raw=value.meta_raw,
            is_block=value.is_block,
            block_delimiter=value.block_delimiter,
        )
    return value


def _clone_tmdl_node(node: TmdlNode) -> TmdlNode:
    return TmdlNode(
        type=node.type,
        name=node.name,
        name_raw=node.name_raw,
        is_ref=node.is_ref,
        properties=[
            TmdlProperty(
                name=prop.name,
                value=_clone_tmdl_value(prop.value),
                kind=prop.kind,
                raw=prop.raw,
            )
            for prop in node.properties
        ],
        children=[_clone_tmdl_node(child) for child in node.children],
        default_property=_clone_tmdl_value(node.default_property),
        description=node.description,
        leading_comments=list(node.leading_comments),
        location=None,
    )


def _find_default_time_dimension(dimensions: list[Dimension]) -> str | None:
    for dimension in dimensions:
        if dimension.type == "time":
            return dimension.name
    return None


def _find_default_grain(dimensions: list[Dimension]) -> str | None:
    for dimension in dimensions:
        if dimension.type == "time" and dimension.granularity in (
            "hour",
            "day",
            "week",
            "month",
            "quarter",
            "year",
        ):
            return dimension.granularity
    return None


def _props(node: TmdlNode) -> dict[str, Any]:
    return {prop.name.lower(): prop.value for prop in node.properties}


def _string_prop(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _resolve_expression_object(node: TmdlNode, props: dict[str, Any]) -> TmdlExpression | None:
    expr_obj = _coerce_expression(node.default_property)
    prop_expr = _coerce_expression(props.get("expression"))
    if prop_expr is not None:
        expr_obj = prop_expr
    return expr_obj


def _resolve_expression(node: TmdlNode, props: dict[str, Any]) -> str | None:
    expr_obj = _resolve_expression_object(node, props)
    if expr_obj is None:
        return None
    return expr_obj.text


def _coerce_expression(value: Any) -> TmdlExpression | None:
    if isinstance(value, TmdlExpression):
        return value
    if isinstance(value, str):
        return TmdlExpression(text=value, is_block="\n" in value)
    return None


def _dax_expression_for_export(obj: Any) -> TmdlExpression | None:
    for attr in ("_tmdl_expression_obj", "_tmdl_expression", "_dax_expression", "dax"):
        expression = _coerce_expression(getattr(obj, attr, None))
        if expression is not None and expression.text.strip():
            return expression
    return None


def _is_true(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, str):
        return value.lower() == "true"
    return False


def _is_false(value: Any) -> bool:
    if value is False:
        return True
    if isinstance(value, str):
        return value.lower() == "false"
    return False


def _map_data_type(data_type: str | None) -> tuple[str, str | None]:
    if not data_type:
        return "categorical", None

    dt = data_type.lower()
    if "date" in dt or "time" in dt:
        granularity = "day" if "date" in dt and "time" not in dt else "hour"
        return "time", granularity
    if "bool" in dt:
        return "boolean", None
    if any(token in dt for token in ("int", "decimal", "double", "numeric", "currency", "float")):
        return "numeric", None
    return "categorical", None


def _parse_dax_expression(expression: str, node: TmdlNode, context: str) -> Any | None:
    try:
        from sidemantic_dax.ast import parse_expression as parse_dax_expression
    except Exception as exc:
        raise DaxRuntimeUnavailableError(
            "sidemantic_dax is required to parse embedded TMDL DAX. Install sidemantic[dax] and retry."
        ) from exc

    try:
        return parse_dax_expression(expression)
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            raise DaxRuntimeUnavailableError(
                "sidemantic_dax native module is not available. Rebuild or reinstall sidemantic[dax]."
            ) from exc
        raise ValueError(_format_dax_error(node, context, str(exc))) from exc
    except Exception as exc:
        raise ValueError(_format_dax_error(node, context, str(exc))) from exc


def _format_dax_error(node: TmdlNode, context: str, message: str) -> str:
    name = node.name or "<unnamed>"
    if node.location:
        location = f"{node.location.file or '<input>'}:{node.location.line}:{node.location.column}"
        return f"DAX parse error in {context} {name} at {location}: {message}"
    return f"DAX parse error in {context} {name}: {message}"


def _append_import_warning(
    warnings: list[TmdlImportWarning],
    node: TmdlNode,
    *,
    code: str,
    context: str,
    message: str,
    model_name: str | None = None,
) -> None:
    warning: TmdlImportWarning = {
        "code": code,
        "context": context,
        "message": message,
        "name": node.name or "<unnamed>",
    }
    if model_name:
        warning["model"] = model_name
    if node.location:
        warning["file"] = node.location.file
        warning["line"] = node.location.line
        warning["column"] = node.location.column
    warnings.append(warning)


def _append_export_warning(
    warnings: list[TmdlExportWarning],
    *,
    code: str,
    context: str,
    message: str,
    from_model: str | None = None,
    to_model: str | None = None,
) -> None:
    warning: TmdlExportWarning = {
        "code": code,
        "context": context,
        "message": message,
    }
    if from_model:
        warning["from_model"] = from_model
    if to_model:
        warning["to_model"] = to_model
    warnings.append(warning)


def _extract_dax_agg(
    expression: str, table_name: str, dax_expr: DaxExpr | None = None
) -> tuple[str | None, str | None]:
    if dax_expr is not None:
        parsed = _extract_dax_agg_from_ast(dax_expr, table_name)
        if parsed is not None:
            return parsed
    expr = " ".join(part.strip() for part in expression.splitlines() if part.strip())
    if not expr:
        return None, None

    match = _match_single_function(expr)
    if not match:
        return None, None

    func, arg = match
    func_lower = func.lower()
    agg = {
        "sum": "sum",
        "average": "avg",
        "averagea": "avg",
        "avg": "avg",
        "min": "min",
        "mina": "min",
        "max": "max",
        "maxa": "max",
        "minx": "min",
        "maxx": "max",
        "median": "median",
        "medianx": "median",
        "count": "count",
        "countrows": "count",
        "counta": "count",
        "countblank": "count",
        "countx": "count",
        "countax": "count",
        "distinctcount": "count_distinct",
        "distinctcountnoblank": "count_distinct",
        "approximatedistinctcount": "count_distinct",
    }.get(func_lower)

    if not agg:
        return None, None

    if func_lower == "countrows":
        return agg, None

    table, column = _parse_dax_column_ref(arg)
    if not column:
        return None, None

    if table and table.lower() == table_name.lower():
        return agg, column
    if table:
        return agg, f"{table}.{column}"
    return agg, column


def _extract_dax_agg_from_ast(expr: Any, table_name: str) -> tuple[str | None, str | None] | None:
    try:
        from sidemantic_dax import ast as dax_ast
    except Exception:
        return None

    def unwrap(value: Any) -> Any:
        while isinstance(value, dax_ast.Paren):
            value = value.expr
        return value

    expr = unwrap(expr)
    if not isinstance(expr, dax_ast.FunctionCall):
        return None

    func = expr.name.lower()
    agg = {
        "sum": "sum",
        "average": "avg",
        "averagea": "avg",
        "avg": "avg",
        "min": "min",
        "mina": "min",
        "max": "max",
        "maxa": "max",
        "minx": "min",
        "maxx": "max",
        "median": "median",
        "medianx": "median",
        "count": "count",
        "countrows": "count",
        "counta": "count",
        "countblank": "count",
        "countx": "count",
        "countax": "count",
        "distinctcount": "count_distinct",
        "distinctcountnoblank": "count_distinct",
        "approximatedistinctcount": "count_distinct",
    }.get(func)
    if not agg:
        return None

    if func == "countrows":
        return agg, None

    if len(expr.args) != 1:
        return None

    arg = unwrap(expr.args[0])
    if isinstance(arg, dax_ast.TableColumnRef):
        table = arg.table.name
        column = arg.column
    elif isinstance(arg, dax_ast.BracketRef):
        table = None
        column = arg.name
    elif isinstance(arg, dax_ast.Identifier):
        table = None
        column = arg.name
    else:
        return None

    if table and table.lower() == table_name.lower():
        return agg, column
    if table:
        return agg, f"{table}.{column}"
    return agg, column


def _match_single_function(expr: str) -> tuple[str, str] | None:
    depth = 0
    func_name = []
    idx = 0
    while idx < len(expr) and (expr[idx].isalnum() or expr[idx] == "_"):
        func_name.append(expr[idx])
        idx += 1
    if not func_name:
        return None
    while idx < len(expr) and expr[idx].isspace():
        idx += 1
    if idx >= len(expr) or expr[idx] != "(":
        return None
    depth = 1
    idx += 1
    arg_start = idx
    while idx < len(expr):
        char = expr[idx]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                arg = expr[arg_start:idx].strip()
                rest = expr[idx + 1 :].strip()
                if rest:
                    return None
                return "".join(func_name), arg
        idx += 1
    return None


def _parse_dax_column_ref(expression: str) -> tuple[str | None, str | None]:
    expr = expression.strip()
    if not expr:
        return None, None

    if "[" in expr and "]" in expr:
        table_part, column_part = expr.split("[", 1)
        column = column_part.rstrip("]").strip()
        table = table_part.strip()
        table = _unquote_identifier(table) if table else None
        column = _unquote_identifier(column)
        return table, column

    if "." in expr:
        parts = _split_unquoted(expr, ".")
        if len(parts) == 2:
            return _unquote_identifier(parts[0]), _unquote_identifier(parts[1])

    return None, _unquote_identifier(expr)


def _parse_column_reference(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None
    raw = value.strip()
    if not raw:
        return None, None

    if "[" in raw and "]" in raw:
        return _parse_dax_column_ref(raw)

    if "." in raw:
        parts = _split_unquoted(raw, ".")
        if len(parts) == 2:
            return _unquote_identifier(parts[0]), _unquote_identifier(parts[1])

    return None, None


def _split_unquoted(text: str, sep: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False
    idx = 0
    while idx < len(text):
        char = text[idx]
        if not in_double and char == "'":
            if in_single and idx + 1 < len(text) and text[idx + 1] == "'":
                current.append("'")
                idx += 2
                continue
            in_single = not in_single
        elif not in_single and char == '"':
            if in_double and idx + 1 < len(text) and text[idx + 1] == '"':
                current.append('"')
                idx += 2
                continue
            in_double = not in_double
        elif not in_single and not in_double and char == sep:
            parts.append("".join(current))
            current = []
            idx += 1
            continue
        current.append(char)
        idx += 1
    parts.append("".join(current))
    return [part.strip() for part in parts if part.strip()]


def _unquote_identifier(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        inner = value[1:-1]
        if value[0] == "'":
            return inner.replace("''", "'")
        return inner.replace('""', '"')
    return value


def _map_relationship_type(from_cardinality: str | None, to_cardinality: str | None) -> str | None:
    from_card = (from_cardinality or "").lower()
    to_card = (to_cardinality or "").lower()
    if not from_card and not to_card:
        return "many_to_one"
    if from_card == "many" and not to_card:
        return "many_to_one"
    if not from_card and to_card == "one":
        return "many_to_one"
    if from_card == "one" and not to_card:
        return "one_to_one"
    if not from_card and to_card == "many":
        return "one_to_many"
    if from_card == "many" and to_card == "one":
        return "many_to_one"
    if from_card == "one" and to_card == "many":
        return "one_to_many"
    if from_card == "one" and to_card == "one":
        return "one_to_one"
    if from_card == "many" and to_card == "many":
        return "many_to_many"
    return None


def _export_database(graph: SemanticGraph, name: str) -> str:
    database_name = getattr(graph, "_tmdl_database_name", None) or name
    database_name_raw = getattr(graph, "_tmdl_database_name_raw", None)
    model_name = getattr(graph, "_tmdl_database_model_name", None) or getattr(graph, "_tmdl_model_name", None) or name
    model_name_raw = getattr(graph, "_tmdl_database_model_name_raw", None) or getattr(
        graph, "_tmdl_model_name_raw", None
    )

    lines: list[str] = []
    leading_comments = getattr(graph, "_tmdl_database_leading_comments", None)
    if isinstance(leading_comments, list):
        for comment in leading_comments:
            if isinstance(comment, str) and comment.strip():
                lines.append(comment)
    database_description = getattr(graph, "_tmdl_database_description", None)
    if isinstance(database_description, str) and database_description.strip():
        lines.extend(_format_description(database_description))
    lines.append(f"database {_format_identifier_with_raw(database_name, database_name_raw)}")
    _export_passthrough_properties(lines, getattr(graph, "_tmdl_database_properties", None), set(), indent="    ")
    lines.append(f"    model {_format_identifier_with_raw(model_name, model_name_raw)}")
    for node in _coerce_tmdl_nodes(getattr(graph, "_tmdl_database_child_nodes", None)):
        lines.extend(_render_passthrough_node(node, indent="    "))
    return "\n".join(lines) + "\n"


def _export_model(graph: SemanticGraph, name: str) -> str:
    model_name = getattr(graph, "_tmdl_model_name", None) or name
    model_name_raw = getattr(graph, "_tmdl_model_name_raw", None)
    lines: list[str] = []
    leading_comments = getattr(graph, "_tmdl_model_leading_comments", None)
    if isinstance(leading_comments, list):
        for comment in leading_comments:
            if isinstance(comment, str) and comment.strip():
                lines.append(comment)
    model_description = getattr(graph, "_tmdl_model_description", None)
    if isinstance(model_description, str) and model_description.strip():
        lines.extend(_format_description(model_description))

    lines.append(f"model {_format_identifier_with_raw(model_name, model_name_raw)}")
    _export_passthrough_properties(lines, getattr(graph, "_tmdl_model_properties", None), set(), indent="    ")
    for table_name, table_name_raw in _export_model_table_refs(graph):
        lines.append(f"    ref table {_format_identifier_with_raw(table_name, table_name_raw)}")
    for rel_name, rel_name_raw in _export_relationship_refs(graph):
        lines.append(f"    ref relationship {_format_identifier_with_raw(rel_name, rel_name_raw)}")
    for node in _coerce_tmdl_nodes(getattr(graph, "_tmdl_model_child_nodes", None)):
        lines.extend(_render_passthrough_node(node, indent="    "))
    for node in _coerce_tmdl_nodes(getattr(graph, "_tmdl_root_nodes", None)):
        lines.extend(_render_passthrough_node(node, indent=""))
    return "\n".join(lines) + "\n"


def _export_table(model: Model) -> str:
    lines: list[str] = []
    leading_comments = getattr(model, "_tmdl_leading_comments", None)
    if isinstance(leading_comments, list):
        for comment in leading_comments:
            if isinstance(comment, str) and comment.strip():
                lines.append(comment)
    raw_value_props = getattr(model, "_tmdl_raw_value_properties", None)
    raw_description_value = _raw_value_for_key(raw_value_props, "description")
    if model.description and raw_description_value is None:
        lines.extend(_format_description(model.description))

    node_type = str(getattr(model, "_tmdl_node_type", "table")).lower()
    model_name_raw = getattr(model, "_tmdl_name_raw", None)
    model_expression_obj = _dax_expression_for_export(model)
    is_calculated_table = node_type == "calculatedtable" or (
        model_expression_obj is not None and (getattr(model, "expression_language", None) == "dax" or not model.table)
    )
    if is_calculated_table and model_expression_obj and model_expression_obj.text.strip():
        _append_expression_assignment(
            lines,
            f"calculatedTable {_format_identifier_with_raw(model.name, model_name_raw)}",
            model_expression_obj,
            block_indent="    ",
        )
    else:
        lines.append(f"table {_format_identifier_with_raw(model.name, model_name_raw)}")
    emitted_keys: set[str] = set()
    if raw_description_value is not None:
        lines.append(f"    description: {raw_description_value}")
        emitted_keys.add("description")
    elif model.description:
        emitted_keys.add("description")
    if is_calculated_table and model_expression_obj and model_expression_obj.text.strip():
        emitted_keys.add("expression")
    _export_passthrough_properties(lines, getattr(model, "_tmdl_properties", None), emitted_keys, indent="    ")

    for dim in model.dimensions:
        dim_lines = _export_dimension(model, dim)
        lines.extend(["    " + line for line in dim_lines])

    for metric in model.metrics:
        metric_lines = _export_metric(model, metric)
        if metric_lines:
            lines.extend(["    " + line for line in metric_lines])

    for node in _coerce_tmdl_nodes(getattr(model, "_tmdl_child_nodes", None)):
        lines.extend(_render_passthrough_node(node, indent="    "))

    return "\n".join(lines) + "\n"


def _export_dimension(model: Model, dim: Dimension) -> list[str]:
    lines: list[str] = []
    leading_comments = getattr(dim, "_tmdl_leading_comments", None)
    if isinstance(leading_comments, list):
        for comment in leading_comments:
            if isinstance(comment, str) and comment.strip():
                lines.append(comment.lstrip())
    dim_node_type = str(getattr(dim, "_tmdl_node_type", "")).lower()
    expression_obj = _dax_expression_for_export(dim)
    if dim_node_type == "calculatedcolumn":
        kind = "calculatedColumn"
    elif dim_node_type == "column":
        kind = "column"
    else:
        kind = "column"
        if expression_obj is not None or (dim.sql and not _is_simple_identifier(dim.sql)):
            kind = "calculatedColumn"

    expression_sql = dim.sql
    if kind == "calculatedColumn" and expression_obj and expression_obj.text.strip():
        expression_sql = expression_obj.text

    dim_name_raw = getattr(dim, "_tmdl_name_raw", None)
    lines.append(f"{kind} {_format_identifier_with_raw(dim.name, dim_name_raw)}")
    raw_value_props = getattr(dim, "_tmdl_raw_value_properties", None)
    emitted_keys: set[str] = set()

    def _emit_data_type() -> None:
        dim_data_type = getattr(dim, "_tmdl_data_type", None)
        if isinstance(dim_data_type, str) and dim_data_type.strip():
            data_type_value = _raw_value_for_key(raw_value_props, "datatype") or dim_data_type
            lines.append(f"    dataType: {data_type_value}")
        else:
            lines.append(f"    dataType: {_map_dimension_type(dim)}")
        emitted_keys.add("datatype")

    def _emit_is_key() -> None:
        raw_is_key = _raw_value_for_key(raw_value_props, "iskey")
        if raw_is_key is not None:
            lines.append(f"    isKey: {raw_is_key}")
            emitted_keys.add("iskey")
            return
        if dim.name == model.primary_key:
            lines.append("    isKey")
            emitted_keys.add("iskey")

    def _emit_is_hidden() -> None:
        raw_is_hidden = _raw_value_for_key(raw_value_props, "ishidden")
        if raw_is_hidden is not None:
            lines.append(f"    isHidden: {raw_is_hidden}")
            emitted_keys.add("ishidden")
            return
        if not dim.public:
            lines.append("    isHidden: true")
            emitted_keys.add("ishidden")

    def _emit_caption() -> None:
        if not dim.label:
            return
        caption_value = _raw_value_for_key(raw_value_props, "caption") or _format_string(dim.label)
        lines.append(f"    caption: {caption_value}")
        emitted_keys.add("caption")

    def _emit_format() -> None:
        if not dim.format:
            return
        format_value = _raw_value_for_key(raw_value_props, "formatstring") or _format_string(dim.format)
        lines.append(f"    formatString: {format_value}")
        emitted_keys.add("formatstring")

    def _emit_description() -> None:
        if not dim.description:
            return
        description_value = _raw_value_for_key(raw_value_props, "description") or _format_string(dim.description)
        lines.append(f"    description: {description_value}")
        emitted_keys.add("description")

    def _emit_source_or_expression() -> None:
        if not expression_sql:
            return
        raw_source_column = _raw_value_for_key(raw_value_props, "sourcecolumn")
        if kind == "column" and (raw_source_column is not None or _is_simple_identifier(expression_sql)):
            source_column_value = raw_source_column or _format_value(expression_sql)
            lines.append(f"    sourceColumn: {source_column_value}")
            emitted_keys.update({"sourcecolumn", "expression"})
            return
        if expression_obj is not None:
            _append_expression_assignment(
                lines,
                "    expression",
                expression_obj,
                block_indent="        ",
            )
        elif "\n" in expression_sql:
            lines.append("    expression =")
            for expr_line in expression_sql.splitlines():
                lines.append(f"        {expr_line}")
        else:
            lines.append(f"    expression = {expression_sql}")
        emitted_keys.update({"sourcecolumn", "expression"})

    emitters = {
        "datatype": _emit_data_type,
        "iskey": _emit_is_key,
        "ishidden": _emit_is_hidden,
        "caption": _emit_caption,
        "formatstring": _emit_format,
        "description": _emit_description,
        "sourcecolumn": _emit_source_or_expression,
        "expression": _emit_source_or_expression,
    }
    default_order = [
        "datatype",
        "iskey",
        "ishidden",
        "caption",
        "formatstring",
        "description",
        "sourcecolumn",
        "expression",
    ]
    preferred_order = getattr(dim, "_tmdl_property_order", None)
    if not isinstance(preferred_order, list):
        preferred_order = []
    for key in preferred_order:
        key_l = str(key).lower()
        if key_l in emitted_keys:
            continue
        emitter = emitters.get(key_l)
        if emitter is not None:
            emitter()
    for key in default_order:
        if key in emitted_keys:
            continue
        emitter = emitters.get(key)
        if emitter is not None:
            emitter()
    _export_passthrough_properties(lines, getattr(dim, "_tmdl_properties", None), emitted_keys, indent="    ")
    for node in _coerce_tmdl_nodes(getattr(dim, "_tmdl_child_nodes", None)):
        lines.extend(_render_passthrough_node(node, indent="    "))
    return lines


def _export_metric(model: Model, metric: Metric) -> list[str] | None:
    expression_obj = _dax_expression_for_export(metric)
    if expression_obj and expression_obj.text.strip():
        expression = expression_obj.text
    else:
        expression = _metric_to_dax(metric, model.name)
        expression_obj = _coerce_expression(expression)
    if not expression_obj or not expression_obj.text:
        return None

    lines: list[str] = []
    leading_comments = getattr(metric, "_tmdl_leading_comments", None)
    if isinstance(leading_comments, list):
        for comment in leading_comments:
            if isinstance(comment, str) and comment.strip():
                lines.append(comment.lstrip())
    raw_value_props = getattr(metric, "_tmdl_raw_value_properties", None)
    raw_description_value = _raw_value_for_key(raw_value_props, "description")
    emitted_keys: set[str] = {"expression"}
    if metric.description and raw_description_value is None:
        lines.extend(_format_description(metric.description))
        emitted_keys.add("description")
    metric_name_raw = getattr(metric, "_tmdl_name_raw", None)
    _append_expression_assignment(
        lines,
        f"measure {_format_identifier_with_raw(metric.name, metric_name_raw)}",
        expression_obj,
        block_indent="    ",
    )

    def _emit_description() -> None:
        if raw_description_value is None:
            return
        lines.append(f"    description: {raw_description_value}")
        emitted_keys.add("description")

    def _emit_caption() -> None:
        if not metric.label:
            return
        caption_value = _raw_value_for_key(raw_value_props, "caption") or _format_string(metric.label)
        lines.append(f"    caption: {caption_value}")
        emitted_keys.add("caption")

    def _emit_format() -> None:
        if not metric.format:
            return
        format_value = _raw_value_for_key(raw_value_props, "formatstring") or _format_string(metric.format)
        lines.append(f"    formatString: {format_value}")
        emitted_keys.add("formatstring")

    def _emit_is_hidden() -> None:
        raw_is_hidden = _raw_value_for_key(raw_value_props, "ishidden")
        if raw_is_hidden is not None:
            lines.append(f"    isHidden: {raw_is_hidden}")
            emitted_keys.add("ishidden")
            return
        if not metric.public:
            lines.append("    isHidden: true")
            emitted_keys.add("ishidden")

    emitters = {
        "description": _emit_description,
        "caption": _emit_caption,
        "formatstring": _emit_format,
        "ishidden": _emit_is_hidden,
    }
    preferred_order = getattr(metric, "_tmdl_property_order", None)
    if not isinstance(preferred_order, list):
        preferred_order = []
    for key in preferred_order:
        key_l = str(key).lower()
        if key_l in emitted_keys:
            continue
        emitter = emitters.get(key_l)
        if emitter is not None:
            emitter()
    for key in ("description", "caption", "formatstring", "ishidden"):
        if key in emitted_keys:
            continue
        emitter = emitters.get(key)
        if emitter is not None:
            emitter()

    _export_passthrough_properties(lines, getattr(metric, "_tmdl_properties", None), emitted_keys, indent="    ")
    for node in _coerce_tmdl_nodes(getattr(metric, "_tmdl_child_nodes", None)):
        lines.extend(_render_passthrough_node(node, indent="    "))
    return lines


def _export_relationships(graph: SemanticGraph, warnings: list[TmdlExportWarning] | None = None) -> str:
    lines: list[str] = []
    for model in graph.models.values():
        for rel in model.relationships:
            related = graph.models.get(rel.name)
            if not related:
                if warnings is not None:
                    _append_export_warning(
                        warnings,
                        code="relationship_export_skip",
                        context="relationship",
                        message=(
                            f"Skipping relationship export: related model not found from='{model.name}' to='{rel.name}'"
                        ),
                        from_model=model.name,
                        to_model=rel.name,
                    )
                continue

            if rel.type == "many_to_one":
                from_table = model.name
                to_table = related.name
                from_column = rel.foreign_key or rel.sql_expr
                to_column = rel.primary_key or related.primary_key
                from_card, to_card = "many", "one"
            elif rel.type in ("one_to_many", "one_to_one"):
                from_table = model.name
                to_table = related.name
                from_column = _relationship_tmdl_from_column(rel) or model.primary_key
                to_column = rel.foreign_key or rel.sql_expr
                from_card = "one"
                to_card = "many" if rel.type == "one_to_many" else "one"
            elif rel.type == "many_to_many":
                from_table = model.name
                to_table = related.name
                from_column = rel.foreign_key or rel.sql_expr
                to_column = rel.primary_key or related.primary_key
                from_card, to_card = "many", "many"
            else:
                if warnings is not None:
                    _append_export_warning(
                        warnings,
                        code="relationship_export_skip",
                        context="relationship",
                        message=f"Skipping relationship export: unsupported relationship type '{rel.type}'",
                        from_model=model.name,
                        to_model=related.name,
                    )
                continue

            rel_name = _relationship_export_name(from_table, to_table, rel)
            leading_comments = getattr(rel, "_tmdl_leading_comments", None)
            if isinstance(leading_comments, list):
                for comment in leading_comments:
                    if isinstance(comment, str) and comment.strip():
                        lines.append(comment.lstrip())
            rel_description = getattr(rel, "_tmdl_description", None)
            if isinstance(rel_description, str) and rel_description.strip():
                lines.extend(_format_description(rel_description))
            raw_value_props = getattr(rel, "_tmdl_raw_value_properties", None)
            rel_name_raw = getattr(rel, "_tmdl_relationship_name_raw", None)
            lines.append(f"relationship {_format_identifier_with_raw(rel_name, rel_name_raw)}")
            from_column_value = _raw_value_for_key(raw_value_props, "fromcolumn") or _format_column_ref(
                from_table,
                from_column,
            )
            to_column_value = _raw_value_for_key(raw_value_props, "tocolumn") or _format_column_ref(
                to_table,
                to_column,
            )
            from_cardinality_value = _raw_value_for_key(raw_value_props, "fromcardinality") or from_card
            to_cardinality_value = _raw_value_for_key(raw_value_props, "tocardinality") or to_card
            emitted_keys: set[str] = set()

            def _emit_from_column() -> None:
                lines.append(f"    fromColumn: {from_column_value}")
                emitted_keys.add("fromcolumn")

            def _emit_to_column() -> None:
                lines.append(f"    toColumn: {to_column_value}")
                emitted_keys.add("tocolumn")

            def _emit_from_cardinality() -> None:
                lines.append(f"    fromCardinality: {from_cardinality_value}")
                emitted_keys.add("fromcardinality")

            def _emit_to_cardinality() -> None:
                lines.append(f"    toCardinality: {to_cardinality_value}")
                emitted_keys.add("tocardinality")

            def _emit_is_active() -> None:
                raw_is_active = _raw_value_for_key(raw_value_props, "isactive")
                if raw_is_active is not None:
                    lines.append(f"    isActive: {raw_is_active}")
                    emitted_keys.add("isactive")
                    return
                if getattr(rel, "_tmdl_is_active_explicit", False):
                    lines.append("    isActive")
                    emitted_keys.add("isactive")
                    return
                if not rel.active:
                    lines.append("    isActive: false")
                    emitted_keys.add("isactive")

            emitters = {
                "fromcolumn": _emit_from_column,
                "tocolumn": _emit_to_column,
                "fromcardinality": _emit_from_cardinality,
                "tocardinality": _emit_to_cardinality,
                "isactive": _emit_is_active,
            }
            preferred_order = getattr(rel, "_tmdl_property_order", None)
            if not isinstance(preferred_order, list):
                preferred_order = []
            for key in preferred_order:
                key_l = str(key).lower()
                if key_l in emitted_keys:
                    continue
                emitter = emitters.get(key_l)
                if emitter is not None:
                    emitter()
            for key in ("fromcolumn", "tocolumn", "fromcardinality", "tocardinality", "isactive"):
                if key in emitted_keys:
                    continue
                emitter = emitters.get(key)
                if emitter is not None:
                    emitter()
            _export_relationship_passthrough_properties(lines, rel, emitted_keys)
            for node in _coerce_tmdl_nodes(getattr(rel, "_tmdl_child_nodes", None)):
                lines.extend(_render_passthrough_node(node, indent="    "))
            lines.append("")

    if not lines:
        return ""
    if lines[-1] == "":
        lines = lines[:-1]
    return "\n".join(lines) + "\n"


def _export_relationship_passthrough_properties(lines: list[str], rel: Relationship, emitted_keys: set[str]) -> None:
    _export_passthrough_properties(
        lines, getattr(rel, "_tmdl_relationship_properties", None), emitted_keys, indent="    "
    )


def _export_passthrough_properties(
    lines: list[str],
    passthrough_props: Any,
    emitted_keys: set[str],
    *,
    indent: str,
) -> None:
    if not isinstance(passthrough_props, list):
        return

    for prop in passthrough_props:
        if not isinstance(prop, dict):
            continue
        name = prop.get("name")
        if not isinstance(name, str) or not name.strip():
            continue
        key = name.lower()
        if key in emitted_keys:
            continue

        kind = str(prop.get("kind") or "value").lower()
        value = prop.get("value")
        if kind == "expression":
            expression_obj = _coerce_expression(value) or TmdlExpression(text=str(value or ""))
            _append_expression_assignment(lines, f"{indent}{name}", expression_obj, block_indent=f"{indent}    ")
        else:
            raw_value = prop.get("raw")
            if isinstance(raw_value, str):
                lines.append(f"{indent}{name}: {raw_value}")
            else:
                lines.append(f"{indent}{name}: {_format_value(value)}")
        emitted_keys.add(key)


def _append_expression_assignment(lines: list[str], lhs: str, expression: TmdlExpression, *, block_indent: str) -> None:
    meta = _format_expression_meta(expression)
    is_block = expression.is_block or "\n" in expression.text
    if is_block:
        if expression.block_delimiter == "```" and not meta:
            lines.append(f"{lhs} = ```")
            for expr_line in expression.text.splitlines():
                lines.append(f"{block_indent}{expr_line}")
            lines.append(f"{block_indent}```")
            return
        lines.append(f"{lhs} ={meta}")
        for expr_line in expression.text.splitlines():
            lines.append(f"{block_indent}{expr_line}")
        return
    lines.append(f"{lhs} = {expression.text}{meta}")


def _format_expression_meta(expression: TmdlExpression) -> str:
    if isinstance(expression.meta_raw, str):
        return f" meta [{expression.meta_raw}]"
    if not expression.meta:
        return ""
    parts = [f"{key}={_format_value(value)}" for key, value in expression.meta.items()]
    return f" meta [{', '.join(parts)}]"


def _coerce_tmdl_nodes(value: Any) -> list[TmdlNode]:
    if not isinstance(value, list):
        return []
    nodes: list[TmdlNode] = []
    for item in value:
        if isinstance(item, TmdlNode):
            nodes.append(item)
    return nodes


def _render_passthrough_node(node: TmdlNode, *, indent: str) -> list[str]:
    lines: list[str] = []
    for comment in node.leading_comments:
        if isinstance(comment, str) and comment.strip():
            lines.append(f"{indent}{comment}")
    if node.description:
        for desc_line in _format_description(node.description):
            lines.append(f"{indent}{desc_line}")

    declaration = _render_node_declaration(node)
    expr = _coerce_expression(node.default_property)
    if expr is not None:
        _append_expression_assignment(lines, f"{indent}{declaration}", expr, block_indent=f"{indent}    ")
    else:
        lines.append(f"{indent}{declaration}")

    for prop in node.properties:
        _render_passthrough_property(lines, prop, indent=f"{indent}    ")
    for child in node.children:
        lines.extend(_render_passthrough_node(child, indent=f"{indent}    "))
    return lines


def _render_node_declaration(node: TmdlNode) -> str:
    parts: list[str] = []
    if node.is_ref:
        parts.append("ref")
    parts.append(node.type)
    if node.name is not None:
        parts.append(_format_identifier_with_raw(node.name, node.name_raw))
    return " ".join(parts)


def _render_passthrough_property(lines: list[str], prop: TmdlProperty, *, indent: str) -> None:
    if prop.kind == "expression":
        expression_obj = _coerce_expression(prop.value) or TmdlExpression(text=str(prop.value or ""))
        _append_expression_assignment(lines, f"{indent}{prop.name}", expression_obj, block_indent=f"{indent}    ")
        return
    if isinstance(prop.raw, str):
        lines.append(f"{indent}{prop.name}: {prop.raw}")
        return
    lines.append(f"{indent}{prop.name}: {_format_value(prop.value)}")


def _raw_value_for_key(raw_props: Any, key: str) -> str | None:
    if not isinstance(raw_props, dict):
        return None
    value = raw_props.get(key.lower())
    if isinstance(value, str):
        return value
    return None


def _export_model_table_refs(graph: SemanticGraph) -> list[tuple[str, str | None]]:
    refs_by_key: dict[str, tuple[str, str | None]] = {}
    for model in graph.models.values():
        refs_by_key[model.name.lower()] = (model.name, getattr(model, "_tmdl_name_raw", None))

    ordered: list[tuple[str, str | None]] = []
    seen: set[str] = set()
    source_refs = getattr(graph, "_tmdl_model_table_refs", None)
    if isinstance(source_refs, list):
        for ref in source_refs:
            if not isinstance(ref, (tuple, list)) or len(ref) != 2:
                continue
            name_value, raw_value = ref
            if not isinstance(name_value, str) or not name_value.strip():
                continue
            key = name_value.lower()
            current = refs_by_key.get(key)
            if current is None or key in seen:
                continue
            preserved_raw = raw_value if isinstance(raw_value, str) else current[1]
            ordered.append((current[0], preserved_raw))
            seen.add(key)

    for model in graph.models.values():
        key = model.name.lower()
        if key in seen:
            continue
        ordered.append(refs_by_key[key])
        seen.add(key)
    return ordered


def _export_relationship_refs(graph: SemanticGraph) -> list[tuple[str, str | None]]:
    refs_by_key: dict[str, tuple[str, str | None]] = {}
    for model in graph.models.values():
        for rel in model.relationships:
            related = graph.models.get(rel.name)
            if not related:
                continue
            rel_name = _relationship_export_name(model.name, related.name, rel)
            rel_name_raw = getattr(rel, "_tmdl_relationship_name_raw", None)
            key = rel_name.lower()
            existing = refs_by_key.get(key)
            if existing is None:
                refs_by_key[key] = (rel_name, rel_name_raw)
            elif existing[1] is None and rel_name_raw is not None:
                refs_by_key[key] = (existing[0], rel_name_raw)

    ordered: list[tuple[str, str | None]] = []
    seen: set[str] = set()
    source_refs = getattr(graph, "_tmdl_model_relationship_refs", None)
    if isinstance(source_refs, list):
        for ref in source_refs:
            if not isinstance(ref, (tuple, list)) or len(ref) != 2:
                continue
            name_value, raw_value = ref
            if not isinstance(name_value, str) or not name_value.strip():
                continue
            key = name_value.lower()
            current = refs_by_key.get(key)
            if current is None or key in seen:
                continue
            preserved_raw = raw_value if isinstance(raw_value, str) else current[1]
            ordered.append((current[0], preserved_raw))
            seen.add(key)

    for key in sorted(refs_by_key):
        if key in seen:
            continue
        ordered.append(refs_by_key[key])
        seen.add(key)
    return ordered


def _relationship_name(from_table: str, to_table: str) -> str:
    return f"{from_table}_{to_table}"


def _relationship_tmdl_from_column(rel: Relationship) -> str | None:
    value = getattr(rel, "_tmdl_from_column", None)
    if isinstance(value, str) and value.strip():
        return value
    return None


def _relationship_export_name(from_table: str, to_table: str, rel: Relationship) -> str:
    explicit_name = getattr(rel, "_tmdl_relationship_name", None)
    if isinstance(explicit_name, str) and explicit_name.strip():
        return explicit_name
    return _relationship_name(from_table, to_table)


def _metric_to_dax(metric: Metric, table_name: str) -> str | None:
    if metric.type == "derived" and metric.sql:
        return metric.sql

    if metric.agg:
        func = {
            "sum": "SUM",
            "avg": "AVERAGE",
            "min": "MIN",
            "max": "MAX",
            "count": "COUNT",
            "count_distinct": "DISTINCTCOUNT",
            "median": "MEDIAN",
        }.get(metric.agg)
        if not func:
            return metric.sql

        if metric.agg == "count" and not metric.sql:
            return f"COUNTROWS({_format_identifier(table_name)})"

        if metric.sql:
            if _is_simple_identifier(metric.sql):
                return f"{func}({_format_identifier(table_name)}[{_format_identifier(metric.sql)}])"
            return f"{func}({metric.sql})"

    return metric.sql


def _is_simple_identifier(value: str) -> bool:
    if not value:
        return False
    return value.replace("_", "").isalnum()


def _map_dimension_type(dimension: Dimension) -> str:
    if dimension.type == "time":
        if dimension.granularity == "day":
            return "date"
        return "dateTime"
    if dimension.type == "numeric":
        return "double"
    if dimension.type == "boolean":
        return "boolean"
    return "string"


def _format_description(text: str) -> list[str]:
    return [f"/// {line}" if line else "///" for line in text.splitlines()]


def _format_identifier(name: str) -> str:
    if not name:
        return "''"
    if _is_simple_identifier(name):
        return name
    return _format_string(name)


def _format_identifier_with_raw(name: str, raw_name: Any) -> str:
    if isinstance(raw_name, str) and raw_name.strip():
        return raw_name.strip()
    return _format_identifier(name)


def _format_string(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)

    text = str(value)
    table_ref, column_ref = _parse_column_reference(text)
    if table_ref and column_ref:
        return _format_column_ref(table_ref, column_ref)
    if _is_simple_identifier(text):
        return text
    escaped = text.replace('"', '""')
    return f'"{escaped}"'


def _format_column_ref(table: str, column: str | None) -> str:
    column_value = column or "id"
    return f"{_format_identifier(table)}[{_format_identifier(column_value)}]"


def _safe_filename(name: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in ("_", "-", ".") else "_" for ch in name)
    return cleaned.strip("_") or "table"


def _export_script(graph: SemanticGraph, name: str, warnings: list[TmdlExportWarning] | None = None) -> str:
    lines = ["createOrReplace"]
    database_lines = _export_database(graph, name).splitlines()
    lines.extend(["    " + line for line in database_lines if line.strip()])

    model_lines = _export_model(graph, name).splitlines()
    lines.extend(["    " + line for line in model_lines if line.strip()])

    for model in graph.models.values():
        table_lines = _export_table(model).splitlines()
        lines.extend(["    " + line for line in table_lines if line.strip()])
    relationships = _export_relationships(graph, warnings).splitlines()
    if relationships:
        lines.extend(["    " + line for line in relationships if line.strip()])
    return "\n".join(lines) + "\n"
