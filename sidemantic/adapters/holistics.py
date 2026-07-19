"""Holistics AML adapter for importing/exporting AML semantic models."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from antlr4 import CommonTokenStream, InputStream
from antlr4.error.ErrorListener import ErrorListener

from sidemantic.adapters.base import BaseAdapter
from sidemantic.adapters.holistics_grammar import HolisticsLexer, HolisticsParser
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph

_AML_INTERPOLATION_RE = re.compile(r"\{\{\s*(.*?)\s*\}\}")
_TAGGED_BLOCK_RE = re.compile(r"^@(?P<tag>[A-Za-z_][A-Za-z0-9_]*)\s*(?P<body>.*?);;\s*$", re.DOTALL)


@dataclass
class TaggedBlock:
    tag: str
    body: str


@dataclass
class Identifier:
    name: str


@dataclass
class Reference:
    parts: list[str]

    def to_model_field(self) -> tuple[str, str] | None:
        if len(self.parts) < 2:
            return None
        return ".".join(self.parts[:-1]), self.parts[-1]


@dataclass
class FunctionCall:
    name: str
    args: list[AmlValue]


@dataclass
class NamedArg:
    name: str
    value: AmlValue


@dataclass
class TypedBlock:
    type_name: str
    items: list[AmlItem]


@dataclass
class InlineBlock:
    items: list[AmlItem]


@dataclass
class ExtendCall:
    base: AmlValue
    extensions: list[AmlValue]


@dataclass
class BinaryOp:
    left: AmlValue
    op: str
    right: AmlValue


@dataclass
class UnaryOp:
    op: str
    operand: AmlValue


@dataclass
class IfExpression:
    condition: AmlValue
    then_items: list[AmlItem]
    else_items: list[AmlItem] | None


@dataclass
class ConstDeclaration:
    name: str
    value: AmlValue


@dataclass
class Assignment:
    name: str
    value: AmlValue


@dataclass
class ObjectAssignment:
    kind: str
    name: str
    value: AmlValue


@dataclass
class UseItem:
    name: str
    alias: str | None


@dataclass
class UseStatement:
    module_parts: list[str]
    items: list[UseItem]


@dataclass
class ExpressionStatement:
    value: AmlValue


@dataclass
class FuncParam:
    name: str
    type_options: list[str] | None
    default: AmlValue | None


@dataclass
class FuncDeclaration:
    name: str
    params: list[FuncParam]
    return_type: list[str] | None
    body: list[AmlItem]


@dataclass
class AmlProperty:
    key: str
    value: AmlValue
    # Originating file context for this property's definition. Set when a block is
    # composed across modules (e.g. a Dataset extending a PartialDataset declared
    # in another file) so property values such as `relationships` qualify their
    # model/field references against the file that defined them rather than the
    # consuming file.
    context: _FileContext | None = None


@dataclass
class AmlBlock:
    kind: str
    name: str | None
    items: list[AmlItem]
    # Originating file context for this block's definition. Set when a block is
    # composed across modules (e.g. a Dataset extending a PartialDataset declared
    # in another file) so its child fields resolve constants/`use` aliases against
    # the file that defined them rather than the consuming file.
    context: _FileContext | None = None


AmlItem = (
    AmlBlock
    | AmlProperty
    | ConstDeclaration
    | ObjectAssignment
    | Assignment
    | UseStatement
    | FuncDeclaration
    | ExpressionStatement
)
AmlValue = (
    TaggedBlock
    | TypedBlock
    | InlineBlock
    | ExtendCall
    | FunctionCall
    | NamedArg
    | BinaryOp
    | UnaryOp
    | IfExpression
    | Reference
    | Identifier
    | str
    | bool
    | int
    | float
    | None
    | list["AmlValue"]
)


@dataclass
class _AmlRelationship:
    name: str | None
    from_model: str
    to_model: str
    rel_type: str
    from_field: str
    to_field: str


@dataclass
class _RelationshipRef:
    name: str


@dataclass
class _FileContext:
    path: Path
    module_prefix: str | None
    use_map: dict[str, str]


@dataclass
class _AmlDocument:
    context: _FileContext
    items: list[AmlItem]


class _ParserErrorListener(ErrorListener):
    def __init__(self, file_path: Path):
        self.file_path = file_path

    def syntaxError(self, recognizer, offending_symbol, line, column, msg, e):  # noqa: N802
        location = f"{self.file_path}:{line}:{column}"
        raise ValueError(f"AML parse error at {location}: {msg}")


class HolisticsAdapter(BaseAdapter):
    """Adapter for importing/exporting Holistics AML semantic models."""

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse AML files into a semantic graph.

        Args:
            source: Path to AML file or directory

        Returns:
            Semantic graph with imported models
        """
        graph = SemanticGraph()
        source_path = Path(source)

        if not source_path.exists():
            raise FileNotFoundError(f"AML source not found: {source}")

        documents = _load_documents(source_path)

        constants = _collect_constants(documents)
        model_blocks, partial_blocks, assignments = _collect_model_definitions(documents)
        resolved_models = _resolve_models(model_blocks, partial_blocks, assignments, constants)
        for model in resolved_models.values():
            graph.add_model(model)

        # Dataset-level dimensions/metrics, standalone Metric blocks, and
        # PartialDataset blocks composed via .extend(). Datasets surface as
        # models so their cross-model dimensions/metrics are not dropped, and
        # standalone metrics register as graph-level metrics.
        dataset_models, standalone_metrics, assignment_dataset_blocks = _resolve_dataset_artifacts(
            documents, constants, resolved_models
        )
        # Register standalone metrics before dataset metrics so a genuine
        # top-level Metric keeps its bare graph key. A dataset can carry a copy of
        # a standalone metric renamed to a local alias (the
        # `metric name: standalone_metric` shorthand); registering datasets first
        # would let that alias occupy a root metric's key and silently drop the
        # real standalone metric via the `not in graph.metrics` guard below.
        for metric in standalone_metrics.values():
            if metric.name not in graph.metrics:
                graph.add_metric(metric)
        for model in dataset_models.values():
            if model.name not in graph.models:
                graph.add_model(model)
                # Dataset metrics are first-class graph metrics. add_model() only
                # auto-registers time_comparison/conversion types, so surface the
                # rest (derived/simple) at graph scope too for unqualified access.
                for metric in model.metrics:
                    if metric.name not in graph.metrics:
                        graph.add_metric(metric)

        pending_relationships: list[_AmlRelationship] = []
        pending_relationship_refs: list[_RelationshipRef] = []
        relationships_by_name: dict[str, _AmlRelationship] = {}

        for document in documents:
            context = document.context
            for block in _iter_blocks(document.items, "Relationship"):
                rel = _parse_relationship_definition(block, context)
                if rel:
                    pending_relationships.append(rel)
                    if rel.name:
                        relationships_by_name[rel.name] = rel

            for dataset in _iter_blocks(document.items, "Dataset"):
                dataset_relationships, dataset_rel_refs = _parse_dataset_relationships(dataset, context)
                pending_relationships.extend(dataset_relationships)
                pending_relationship_refs.extend(dataset_rel_refs)

        # Assignment-form datasets (Dataset x = Dataset { ... }) are not top-level
        # AmlBlocks, so attach their relationships from the resolved blocks.
        for dataset_block, dataset_context in assignment_dataset_blocks:
            dataset_relationships, dataset_rel_refs = _parse_dataset_relationships(dataset_block, dataset_context)
            pending_relationships.extend(dataset_relationships)
            pending_relationship_refs.extend(dataset_rel_refs)

        for rel_ref in pending_relationship_refs:
            rel = relationships_by_name.get(rel_ref.name)
            if rel:
                pending_relationships.append(rel)

        self._attach_relationships(graph, _dedupe_relationships(pending_relationships))

        return graph

    def _attach_relationships(self, graph: SemanticGraph, relationships: list[_AmlRelationship]) -> None:
        for rel in relationships:
            if rel.from_model not in graph.models or rel.to_model not in graph.models:
                continue

            model = graph.models[rel.from_model]
            if rel.rel_type == "many_to_one":
                foreign_key = rel.from_field
                primary_key = rel.to_field
            elif rel.rel_type == "one_to_many":
                foreign_key = rel.to_field
                primary_key = rel.from_field
            else:
                foreign_key = rel.to_field
                primary_key = rel.from_field

            if any(r.name == rel.to_model and r.type == rel.rel_type for r in model.relationships):
                continue

            model.relationships.append(
                Relationship(
                    name=rel.to_model,
                    type=rel.rel_type,
                    foreign_key=foreign_key,
                    primary_key=primary_key,
                )
            )

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to Holistics AML format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output file or directory
        """
        output_path = Path(output_path)

        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        if output_path.suffix == ".aml":
            if resolved_models:
                model = next(iter(resolved_models.values()))
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text(self._export_model(model))
            return

        output_path.mkdir(parents=True, exist_ok=True)

        for model in resolved_models.values():
            model_file = output_path / f"{model.name}.model.aml"
            model_file.write_text(self._export_model(model))

        relationship_blocks = self._export_relationships(resolved_models)
        if relationship_blocks:
            relationships_file = output_path / "relationships.aml"
            relationships_file.write_text("\n\n".join(relationship_blocks) + "\n")

    def _export_model(self, model: Model) -> str:
        lines: list[str] = [f"Model {model.name} {{"]

        if model.sql:
            lines.append("  type: 'query'")
        else:
            lines.append("  type: 'table'")

        if model.description:
            lines.append(f"  description: '{_escape_single_quotes(model.description)}'")

        if model.sql:
            query = model.sql.strip()
            lines.append("  query: @sql")
            lines.extend([f"    {line}" if line.strip() else "" for line in query.splitlines()])
            lines.append("  ;;")
        elif model.table:
            lines.append(f"  table_name: '{_escape_single_quotes(model.table)}'")

        for dimension in model.dimensions:
            lines.extend(_export_dimension(dimension, model.primary_key))

        for metric in model.metrics:
            lines.extend(_export_measure(metric, model.primary_key))

        lines.append("}")
        return "\n".join(lines) + "\n"

    def _export_relationships(self, models: dict[str, Model]) -> list[str]:
        blocks: list[str] = []
        seen: set[tuple[str, str, str, str, str]] = set()

        for model in models.values():
            for relationship in model.relationships:
                target = models.get(relationship.name)
                if target is None:
                    continue

                if relationship.type == "many_to_one":
                    from_field = relationship.foreign_key or relationship.sql_expr
                    to_field = relationship.primary_key or target.primary_key
                else:
                    from_field = relationship.primary_key or model.primary_key
                    to_field = relationship.foreign_key or relationship.sql_expr

                signature = (model.name, relationship.name, relationship.type, from_field, to_field)
                if signature in seen:
                    continue
                seen.add(signature)

                block = [f"Relationship {model.name}_{relationship.name} {{"]
                block.append(f"  type: '{relationship.type}'")
                block.append(f"  from: r({model.name}.{from_field})")
                block.append(f"  to: r({relationship.name}.{to_field})")
                block.append("}")
                blocks.append("\n".join(block))

        return blocks


def _parse_document(content: str, file_path: Path) -> list[AmlItem]:
    input_stream = InputStream(content)
    lexer = HolisticsLexer(input_stream)
    lexer.removeErrorListeners()
    lexer.addErrorListener(_ParserErrorListener(file_path))

    token_stream = CommonTokenStream(lexer)
    parser = HolisticsParser(token_stream)
    parser.removeErrorListeners()
    parser.addErrorListener(_ParserErrorListener(file_path))

    tree = parser.document()
    return [_parse_statement(statement) for statement in tree.statement()]


def _load_documents(source_path: Path) -> list[_AmlDocument]:
    if source_path.is_dir():
        files = sorted(source_path.rglob("*.aml"))
        root = source_path
    else:
        files = [source_path]
        root = source_path.parent

    documents: list[_AmlDocument] = []
    for file_path in files:
        context = _build_file_context(file_path, root)
        items = _parse_document(file_path.read_text(), file_path)
        context.use_map = _collect_use_map(items)
        documents.append(_AmlDocument(context=context, items=items))
    return documents


def _build_file_context(file_path: Path, root_path: Path) -> _FileContext:
    module_prefix = _module_prefix_from_path(file_path, root_path)
    return _FileContext(path=file_path, module_prefix=module_prefix, use_map={})


def _module_prefix_from_path(file_path: Path, root_path: Path) -> str | None:
    try:
        relative = file_path.relative_to(root_path)
    except ValueError:
        return None

    parts = list(relative.parts)
    if "modules" not in parts:
        return None

    index = parts.index("modules")
    module_parts: list[str] = []
    stop_dirs = {"models", "datasets", "dashboards", "pages"}

    for part in parts[index + 1 :]:
        if part in stop_dirs:
            break
        if part == "modules":
            continue
        if part.endswith(".aml"):
            break
        module_parts.append(part)

    if not module_parts:
        return None
    return ".".join(module_parts)


def _collect_use_map(items: Iterable[AmlItem]) -> dict[str, str]:
    use_map: dict[str, str] = {}
    for item in items:
        if not isinstance(item, UseStatement):
            continue
        module_name = ".".join(item.module_parts)
        if item.items:
            for use_item in item.items:
                alias = use_item.alias or use_item.name
                use_map[alias] = f"{module_name}.{use_item.name}"
        else:
            alias = item.module_parts[-1] if item.module_parts else module_name
            if alias:
                use_map[alias] = module_name
    return use_map


def _collect_constants(documents: Iterable[_AmlDocument]) -> dict[str, AmlValue]:
    constants: dict[str, AmlValue] = {}
    for document in documents:
        prefix = document.context.module_prefix
        for item in document.items:
            if isinstance(item, ConstDeclaration):
                name = _qualify_declared_name(item.name, prefix)
                constants[name] = item.value
    return constants


def _collect_model_definitions(
    documents: Iterable[_AmlDocument],
) -> tuple[
    dict[str, tuple[AmlBlock, _FileContext]],
    dict[str, tuple[AmlBlock, _FileContext]],
    dict[str, tuple[ObjectAssignment, _FileContext]],
]:
    model_blocks: dict[str, tuple[AmlBlock, _FileContext]] = {}
    partial_blocks: dict[str, tuple[AmlBlock, _FileContext]] = {}
    assignments: dict[str, tuple[ObjectAssignment, _FileContext]] = {}

    for document in documents:
        context = document.context
        for item in document.items:
            if isinstance(item, AmlBlock) and item.kind == "Model" and item.name:
                name = _qualify_declared_name(item.name, context.module_prefix)
                model_blocks[name] = (AmlBlock(kind=item.kind, name=name, items=item.items), context)
                continue
            if isinstance(item, AmlBlock) and item.kind == "PartialModel" and item.name:
                name = _qualify_declared_name(item.name, context.module_prefix)
                partial_blocks[name] = (AmlBlock(kind=item.kind, name=name, items=item.items), context)
                continue
            if isinstance(item, ObjectAssignment) and item.kind == "Model":
                name = _qualify_declared_name(item.name, context.module_prefix)
                assignments[name] = (item, context)

    return model_blocks, partial_blocks, assignments


def _resolve_models(
    model_blocks: dict[str, tuple[AmlBlock, _FileContext]],
    partial_blocks: dict[str, tuple[AmlBlock, _FileContext]],
    assignments: dict[str, tuple[ObjectAssignment, _FileContext]],
    constants: dict[str, AmlValue],
) -> dict[str, Model]:
    resolved_blocks: dict[str, AmlBlock] = {}
    resolving: set[str] = set()

    def resolve_named_block(name: str) -> AmlBlock | None:
        if name in resolved_blocks:
            return resolved_blocks[name]
        if name in resolving:
            return None
        if name in model_blocks:
            block, block_ctx = model_blocks[name]
            stamped = _stamp_block_context(block, block_ctx)
            resolved_blocks[name] = stamped
            return stamped
        if name in partial_blocks:
            block, block_ctx = partial_blocks[name]
            return _stamp_block_context(block, block_ctx)
        assignment_entry = assignments.get(name)
        if assignment_entry:
            assignment, context = assignment_entry
            resolving.add(name)
            resolved_value = _resolve_block_from_value(
                assignment.value,
                context,
                resolve_named_block,
            )
            resolving.remove(name)
            if resolved_value:
                resolved_blocks[name] = AmlBlock(kind="Model", name=name, items=resolved_value.items)
                return resolved_blocks[name]
        return None

    models: dict[str, Model] = {}
    for name, (block, context) in model_blocks.items():
        model = _parse_model_block(block, constants, context)
        if model:
            models[name] = model

    for name, (assignment, context) in assignments.items():
        block = resolve_named_block(name)
        if not block:
            continue
        block_with_name = AmlBlock(kind="Model", name=name, items=block.items)
        model = _parse_model_block(block_with_name, constants, context)
        if model:
            models[name] = model

    return models


def _resolve_block_from_value(
    value: AmlValue,
    context: _FileContext,
    resolve_named_block,
) -> AmlBlock | None:
    if isinstance(value, ExtendCall):
        base_block = _resolve_block_from_value(value.base, context, resolve_named_block)
        if not base_block:
            return None
        merged = _as_model_block(base_block)
        for extension in value.extensions:
            extension_block = _resolve_block_from_value(extension, context, resolve_named_block)
            if not extension_block:
                continue
            merged = _merge_blocks(merged, _as_model_block(extension_block))
        return merged

    if isinstance(value, TypedBlock) and value.type_name in {"Model", "PartialModel", "Dataset", "PartialDataset"}:
        # Inlined block body was authored in `context`'s file; stamp child fields
        # so they resolve constants/`use` aliases against that file when this block
        # is merged into a dataset/model defined elsewhere.
        return _stamp_block_context(AmlBlock(kind=value.type_name, name=None, items=value.items), context)

    if isinstance(value, InlineBlock):
        return _stamp_block_context(AmlBlock(kind="Model", name=None, items=value.items), context)

    if isinstance(value, Identifier):
        qualified_name = _qualify_name(value.name, context)
        return resolve_named_block(qualified_name)

    if isinstance(value, Reference):
        qualified_name = _qualify_name(".".join(value.parts), context)
        return resolve_named_block(qualified_name)

    return None


def _as_model_block(block: AmlBlock) -> AmlBlock:
    if block.kind == "Model":
        return block
    return AmlBlock(kind="Model", name=block.name, items=block.items, context=block.context)


def _stamp_block_context(block: AmlBlock, context: _FileContext) -> AmlBlock:
    """Tag a block's child field blocks with their defining context.

    Used when a block is pulled into a cross-module composition (e.g. a Dataset
    extending a PartialDataset from another file). Each child field (dimension /
    measure / metric) records the file it was authored in so its constants and
    `use` aliases resolve against that file, not the consuming dataset's file.
    Properties (e.g. `relationships`) are stamped too so their model/field
    references qualify against the authoring file. Child items that already carry
    a context keep it (the closest origin wins).
    """
    stamped_items: list[AmlItem] = []
    for item in block.items:
        if isinstance(item, AmlBlock):
            stamped_items.append(_stamp_block_context(item, context) if item.context is None else item)
        elif isinstance(item, AmlProperty):
            stamped_items.append(item if item.context is not None else AmlProperty(item.key, item.value, context))
        else:
            stamped_items.append(item)
    return AmlBlock(kind=block.kind, name=block.name, items=stamped_items, context=block.context or context)


_FIELD_KEYWORDS = {"metric", "measure", "dimension"}
_METRIC_MARKER_KEYWORDS = {"metric", "measure"}


def _is_metric_marker(item: AmlItem | None) -> bool:
    """True if `item` is the bare `metric`/`measure` keyword that the grammar emits
    just before a reusable-reference shorthand property (`metric name: ref`)."""
    return (
        isinstance(item, ExpressionStatement)
        and isinstance(item.value, Identifier)
        and item.value.name in _METRIC_MARKER_KEYWORDS
    )


def _field_units(items: list[AmlItem]) -> list[tuple[tuple[str, str] | None, list[AmlItem]]]:
    """Group items into mergeable field units.

    A field unit is `(field_key, [items])`. Named `metric`/`measure`/`dimension`
    blocks and the reusable-reference shorthand (`metric name: ref`, which the
    grammar emits as a bare `metric` keyword ExpressionStatement immediately
    followed by an `AmlProperty(name, ref)`) both map to the same
    `(keyword, name)` field key, so an override authored in either form replaces
    the base field instead of producing a duplicate. Every other item gets a
    `None` key and is carried through unchanged.
    """
    units: list[tuple[tuple[str, str] | None, list[AmlItem]]] = []
    idx = 0
    n = len(items)
    while idx < n:
        item = items[idx]
        if (
            isinstance(item, ExpressionStatement)
            and isinstance(item.value, Identifier)
            and item.value.name in _FIELD_KEYWORDS
            and idx + 1 < n
            and isinstance(items[idx + 1], AmlProperty)
        ):
            # Reference shorthand: keyword marker + `name: ref` property.
            prop = items[idx + 1]
            units.append(((item.value.name, prop.key), [item, items[idx + 1]]))
            idx += 2
            continue
        if isinstance(item, AmlBlock) and item.kind in _FIELD_KEYWORDS and item.name is not None:
            units.append(((item.kind, item.name), [item]))
            idx += 1
            continue
        units.append((None, [item]))
        idx += 1
    return units


def _merge_blocks(base: AmlBlock, extension: AmlBlock) -> AmlBlock:
    merged_units = _field_units(base.items)
    prop_index: dict[str, int] = {}
    field_index: dict[tuple[str, str], int] = {}

    for unit_idx, (field_key, unit_items) in enumerate(merged_units):
        if field_key is not None:
            field_index[field_key] = unit_idx
        elif len(unit_items) == 1 and isinstance(unit_items[0], AmlProperty):
            prop_index[unit_items[0].key] = unit_idx

    for field_key, unit_items in _field_units(extension.items):
        if field_key is not None:
            if field_key in field_index:
                base_unit = merged_units[field_index[field_key]][1]
                # Two named blocks deep-merge their child fields; any pairing that
                # involves the shorthand reference form replaces wholesale (a
                # reference has no inner fields to merge into).
                if len(base_unit) == 1 and isinstance(base_unit[0], AmlBlock) and isinstance(unit_items[0], AmlBlock):
                    merged_units[field_index[field_key]] = (field_key, [_merge_blocks(base_unit[0], unit_items[0])])
                else:
                    merged_units[field_index[field_key]] = (field_key, unit_items)
            else:
                field_index[field_key] = len(merged_units)
                merged_units.append((field_key, unit_items))
            continue

        item = unit_items[0]
        if isinstance(item, AmlProperty):
            if item.key in prop_index:
                merged_units[prop_index[item.key]] = (None, [item])
            else:
                prop_index[item.key] = len(merged_units)
                merged_units.append((None, [item]))
            continue

        merged_units.append((None, [item]))

    merged_items: list[AmlItem] = []
    for _, unit_items in merged_units:
        merged_items.extend(unit_items)

    # Preserve the defining context so child fields composed across modules keep
    # resolving constants/`use` aliases against their authoring file. The
    # extension wins when it overrides the base (its block is the one taking
    # effect), falling back to the base's context.
    return AmlBlock(
        kind=base.kind,
        name=base.name,
        items=merged_items,
        context=extension.context or base.context,
    )


def _qualify_declared_name(name: str, module_prefix: str | None) -> str:
    if module_prefix:
        return f"{module_prefix}.{name}"
    return name


def _qualify_name(name: str, context: _FileContext) -> str:
    parts = name.split(".")
    if parts and parts[0] in context.use_map:
        mapped = context.use_map[parts[0]].split(".") + parts[1:]
        return ".".join(mapped)
    if len(parts) > 1:
        return name
    if context.module_prefix:
        return f"{context.module_prefix}.{name}"
    return name


def _qualify_reference(ref: Reference, context: _FileContext | None) -> Reference:
    if context is None or len(ref.parts) < 2:
        return ref
    model_name = ".".join(ref.parts[:-1])
    qualified_model = _qualify_name(model_name, context)
    return Reference(parts=qualified_model.split(".") + [ref.parts[-1]])


def _parse_statement(ctx) -> AmlItem:
    if ctx.namedBlock():
        return _parse_named_block(ctx.namedBlock())
    if ctx.anonymousBlock():
        return _parse_anonymous_block(ctx.anonymousBlock())
    if ctx.property_():
        return _parse_property(ctx.property_())
    if ctx.constDeclaration():
        return _parse_const_declaration(ctx.constDeclaration())
    if ctx.objectAssignment():
        return _parse_object_assignment(ctx.objectAssignment())
    if ctx.valueAssignment():
        return _parse_assignment(ctx.valueAssignment())
    if ctx.useStatement():
        return _parse_use_statement(ctx.useStatement())
    if ctx.funcDeclaration():
        return _parse_func_declaration(ctx.funcDeclaration())
    if ctx.expressionStatement():
        return _parse_expression_statement(ctx.expressionStatement())
    raise ValueError("Unsupported AML statement")


def _parse_named_block(ctx) -> AmlBlock:
    kind = ctx.blockKeyword().getText()
    name = _parse_identifier(ctx.identifier())
    items = _parse_block(ctx.block())
    return AmlBlock(kind=kind, name=name, items=items)


def _parse_anonymous_block(ctx) -> AmlBlock:
    kind = ctx.blockKeyword().getText()
    items = _parse_block(ctx.block())
    return AmlBlock(kind=kind, name=None, items=items)


def _parse_block(ctx) -> list[AmlItem]:
    return [_parse_statement(statement) for statement in ctx.statement()]


def _parse_property(ctx) -> AmlProperty:
    key = _parse_identifier(ctx.identifier())
    value = _parse_expression(ctx.expression())
    return AmlProperty(key=key, value=value)


def _parse_const_declaration(ctx) -> ConstDeclaration:
    name = _parse_identifier(ctx.identifier())
    value = _parse_expression(ctx.expression())
    return ConstDeclaration(name=name, value=value)


def _parse_object_assignment(ctx) -> ObjectAssignment:
    kind = ctx.blockKeyword().getText()
    name = _parse_identifier(ctx.identifier())
    value = _parse_expression(ctx.expression())
    return ObjectAssignment(kind=kind, name=name, value=value)


def _parse_assignment(ctx) -> Assignment:
    name = _parse_identifier(ctx.identifier())
    value = _parse_expression(ctx.expression())
    return Assignment(name=name, value=value)


def _parse_use_statement(ctx) -> UseStatement:
    path_ctx = ctx.usePath()
    if path_ctx.qualifiedName():
        module_parts = [_parse_identifier(part) for part in path_ctx.qualifiedName().identifier()]
    else:
        module_parts = [_parse_identifier(path_ctx.identifier())]
    items: list[UseItem] = []
    block = ctx.useImportBlock()
    if block:
        for item_ctx in block.useImportItem():
            identifiers = item_ctx.identifier()
            name = _parse_identifier(identifiers[0])
            alias = _parse_identifier(identifiers[1]) if len(identifiers) > 1 else None
            items.append(UseItem(name=name, alias=alias))
    return UseStatement(module_parts=module_parts, items=items)


def _parse_func_declaration(ctx) -> FuncDeclaration:
    name = _parse_identifier(ctx.identifier())
    params: list[FuncParam] = []
    if ctx.paramList():
        for param_ctx in ctx.paramList().param():
            params.append(_parse_param(param_ctx))
    return_type = _parse_type_expr(ctx.typeExpr()) if ctx.typeExpr() else None
    body = _parse_block(ctx.block())
    return FuncDeclaration(name=name, params=params, return_type=return_type, body=body)


def _parse_param(ctx) -> FuncParam:
    ident_ctx = ctx.identifier()
    if isinstance(ident_ctx, list):
        ident_ctx = ident_ctx[0]
    name = _parse_identifier(ident_ctx)
    type_options = _parse_type_expr(ctx.typeExpr()) if ctx.typeExpr() else None
    default = _parse_expression(ctx.expression()) if ctx.expression() else None
    return FuncParam(name=name, type_options=type_options, default=default)


def _parse_type_expr(ctx) -> list[str]:
    options: list[str] = []
    for primary in ctx.typePrimary():
        if primary.identifier():
            options.append(_parse_identifier(primary.identifier()))
        elif primary.string():
            options.append(_unquote(primary.string().getText()))
    return options


def _parse_expression_statement(ctx) -> ExpressionStatement:
    value = _parse_expression(ctx.expression())
    return ExpressionStatement(value=value)


def _parse_expression(ctx) -> AmlValue:
    return _parse_logical_or(ctx.logicalOr())


def _parse_logical_or(ctx) -> AmlValue:
    return _parse_binary_chain(ctx, _parse_logical_and)


def _parse_logical_and(ctx) -> AmlValue:
    return _parse_binary_chain(ctx, _parse_equality)


def _parse_equality(ctx) -> AmlValue:
    return _parse_binary_chain(ctx, _parse_comparison)


def _parse_comparison(ctx) -> AmlValue:
    return _parse_binary_chain(ctx, _parse_additive)


def _parse_additive(ctx) -> AmlValue:
    return _parse_binary_chain(ctx, _parse_multiplicative)


def _parse_multiplicative(ctx) -> AmlValue:
    return _parse_binary_chain(ctx, _parse_unary)


def _parse_binary_chain(ctx, operand_parser) -> AmlValue:
    children = list(ctx.getChildren())
    if not children:
        return None
    result = operand_parser(children[0])
    idx = 1
    while idx + 1 < len(children):
        op = children[idx].getText()
        right = operand_parser(children[idx + 1])
        result = BinaryOp(left=result, op=op, right=right)
        idx += 2
    return result


def _parse_unary(ctx) -> AmlValue:
    if ctx.primary():
        return _parse_primary(ctx.primary())
    op_token = ctx.NOT() or ctx.DASH()
    operand = _parse_unary(ctx.unary())
    return UnaryOp(op=op_token.getText(), operand=operand)


def _parse_primary(ctx) -> AmlValue:
    if ctx.ifExpression():
        return _parse_if_expression(ctx.ifExpression())
    if ctx.extendCall():
        return _parse_extend_call(ctx.extendCall())
    if ctx.functionCall():
        return _parse_function_call(ctx.functionCall())
    if ctx.reference():
        return _parse_reference(ctx.reference())
    if ctx.taggedBlock():
        return _parse_tagged_block(ctx.taggedBlock())
    if ctx.typedBlock():
        return _parse_typed_block(ctx.typedBlock())
    if ctx.blockLiteral():
        return _parse_inline_block(ctx.blockLiteral())
    if ctx.array():
        return _parse_array(ctx.array())
    if ctx.string():
        return _unquote(ctx.string().getText())
    if ctx.number():
        return _parse_number(ctx.number().getText())
    if ctx.boolean():
        return ctx.boolean().getText().lower() == "true"
    if ctx.nullValue():
        return None
    if ctx.identifier():
        return Identifier(name=_parse_identifier(ctx.identifier()))
    if ctx.expression():
        return _parse_expression(ctx.expression())
    return None


def _parse_if_expression(ctx) -> IfExpression:
    condition = _parse_expression(ctx.expression())
    then_items = _parse_block(ctx.block(0))
    else_items = None
    if ctx.ELSE():
        if len(ctx.block()) > 1:
            else_items = _parse_block(ctx.block(1))
        elif ctx.ifExpression():
            else_items = [ExpressionStatement(value=_parse_if_expression(ctx.ifExpression()))]
    return IfExpression(condition=condition, then_items=then_items, else_items=else_items)


def _parse_extend_call(ctx) -> ExtendCall:
    base = _parse_extend_target(ctx.extendTarget())
    extensions = [_parse_expression(arg.expression()) for arg in ctx.extendArg()]
    return ExtendCall(base=base, extensions=extensions)


def _parse_extend_target(ctx) -> AmlValue:
    if ctx.typedBlock():
        return _parse_typed_block(ctx.typedBlock())
    if ctx.functionCall():
        return _parse_function_call(ctx.functionCall())
    if ctx.reference():
        return _parse_reference(ctx.reference())
    if ctx.identifier():
        return Identifier(name=_parse_identifier(ctx.identifier()))
    if ctx.expression():
        return _parse_expression(ctx.expression())
    return None


def _parse_tagged_block(ctx) -> TaggedBlock:
    raw = ctx.getText()
    match = _TAGGED_BLOCK_RE.match(raw)
    if not match:
        return TaggedBlock(tag="sql", body=raw)
    tag = match.group("tag")
    body = match.group("body").strip()
    return TaggedBlock(tag=tag, body=body)


def _parse_array(ctx) -> list[AmlValue]:
    values = []
    for value_ctx in ctx.expression():
        values.append(_parse_expression(value_ctx))
    return values


def _parse_typed_block(ctx) -> TypedBlock:
    type_name = _parse_identifier(ctx.identifier())
    items = _parse_block(ctx.block())
    return TypedBlock(type_name=type_name, items=items)


def _parse_inline_block(ctx) -> InlineBlock:
    return InlineBlock(items=_parse_block(ctx.block()))


def _parse_function_call(ctx) -> FunctionCall:
    name = _parse_identifier(ctx.identifier())
    args: list[AmlValue] = []
    for arg_ctx in ctx.callArg():
        if arg_ctx.callNamedArg():
            args.append(_parse_named_arg(arg_ctx.callNamedArg()))
        else:
            args.append(_parse_expression(arg_ctx.expression()))
    return FunctionCall(name=name, args=args)


def _parse_named_arg(ctx) -> NamedArg:
    name = _parse_identifier(ctx.identifier())
    value = _parse_expression(ctx.expression())
    return NamedArg(name=name, value=value)


def _parse_reference(ctx) -> Reference:
    parts = [_parse_identifier(part) for part in ctx.qualifiedName().identifier()]
    return Reference(parts=parts)


def _parse_identifier(ctx) -> str:
    return ctx.getText()


def _parse_number(text: str) -> int | float:
    if "." in text:
        return float(text)
    return int(text)


def _unquote(text: str) -> str:
    if len(text) < 2:
        return text
    if text[0] == text[-1] and text[0] in {"'", '"', "`"}:
        body = text[1:-1]
        return bytes(body, "utf-8").decode("unicode_escape")
    return text


def _iter_blocks(items: Iterable[AmlItem], kind: str) -> Iterable[AmlBlock]:
    for item in items:
        if isinstance(item, AmlBlock) and item.kind == kind:
            yield item


def _parse_model_block(block: AmlBlock, constants: dict[str, AmlValue], context: _FileContext) -> Model | None:
    if not block.name:
        return None

    properties = _properties_from_items(block.items)

    model_type = _value_as_string(properties.get("type"), constants, context)
    table_name = _value_as_string(properties.get("table_name"), constants, context)
    description = _value_as_string(properties.get("description"), constants, context)

    query_value = properties.get("query")
    query_expr = _normalize_definition(query_value, constants, context)

    dimensions: list[Dimension] = []
    metrics: list[Metric] = []
    primary_key = None

    for item in block.items:
        if isinstance(item, AmlBlock) and item.kind == "dimension":
            # Fields composed from a PartialModel in another file keep their own
            # defining context so their constants/`use` aliases resolve correctly.
            item_context = item.context or context
            dimension, is_primary = _parse_dimension_block(item, constants, item_context)
            if dimension:
                dimensions.append(dimension)
                if is_primary and primary_key is None:
                    primary_key = dimension.name
        elif isinstance(item, AmlBlock) and item.kind in {"measure", "metric"}:
            item_context = item.context or context
            metric = _parse_measure_block(item, constants, item_context)
            if metric:
                metrics.append(metric)

    if model_type is None:
        model_type = "query" if query_expr else "table"

    return Model(
        name=block.name,
        table=table_name if model_type == "table" else None,
        sql=query_expr if model_type == "query" else None,
        description=description,
        primary_key=primary_key,
        dimensions=dimensions,
        metrics=metrics,
    )


def _parse_dimension_block(
    block: AmlBlock, constants: dict[str, AmlValue], context: _FileContext
) -> tuple[Dimension | None, bool]:
    if not block.name:
        return None, False

    properties = _properties_from_items(block.items)
    prop_contexts = _property_contexts_from_items(block.items)

    def ctx_for(key: str) -> _FileContext:
        return prop_contexts.get(key) or context

    dim_type_raw = _value_as_string(properties.get("type"), constants, ctx_for("type"))
    label = _value_as_string(properties.get("label"), constants, ctx_for("label"))
    description = _value_as_string(properties.get("description"), constants, ctx_for("description"))
    fmt = _value_as_string(properties.get("format"), constants, ctx_for("format"))
    is_primary = _value_as_bool(properties.get("primary_key")) is True

    dim_type, granularity = _map_dimension_type(dim_type_raw)
    sql_expr = _normalize_definition(properties.get("definition"), constants, ctx_for("definition"))
    if sql_expr == block.name:
        sql_expr = None

    return (
        Dimension(
            name=block.name,
            type=dim_type,
            sql=sql_expr,
            granularity=granularity,
            label=label,
            description=description,
            format=fmt,
        ),
        is_primary,
    )


def _parse_measure_block(block: AmlBlock, constants: dict[str, AmlValue], context: _FileContext) -> Metric | None:
    if not block.name:
        return None

    properties = _properties_from_items(block.items)
    prop_contexts = _property_contexts_from_items(block.items)

    def ctx_for(key: str) -> _FileContext:
        return prop_contexts.get(key) or context

    label = _value_as_string(properties.get("label"), constants, ctx_for("label"))
    description = _value_as_string(properties.get("description"), constants, ctx_for("description"))
    fmt = _value_as_string(properties.get("format"), constants, ctx_for("format"))

    aggregation_type = _normalize_agg_type(
        _value_as_string(properties.get("aggregation_type"), constants, ctx_for("aggregation_type"))
    )
    expr = _normalize_definition(properties.get("definition"), constants, ctx_for("definition"))

    agg_map = {
        "count": "count",
        "count distinct": "count_distinct",
        "count_distinct": "count_distinct",
        "sum": "sum",
        "avg": "avg",
        "min": "min",
        "max": "max",
        "median": "median",
    }

    if aggregation_type in agg_map:
        agg = agg_map[aggregation_type]
        if agg == "count" and not expr:
            sql_expr = None
        else:
            sql_expr = expr
            if not sql_expr:
                return None
        return Metric(
            name=block.name,
            agg=agg,
            sql=sql_expr,
            label=label,
            description=description,
            format=fmt,
        )

    if not expr:
        return None

    if aggregation_type and aggregation_type != "custom":
        sql_func = _map_agg_to_sql(aggregation_type)
        return Metric(
            name=block.name,
            type="derived",
            sql=f"{sql_func}({expr})",
            label=label,
            description=description,
            format=fmt,
        )

    ratio = _detect_ratio(expr)
    if ratio:
        numerator, denominator = ratio
        return Metric(
            name=block.name,
            type="ratio",
            numerator=numerator,
            denominator=denominator,
            label=label,
            description=description,
            format=fmt,
        )

    return Metric(
        name=block.name,
        type="derived",
        sql=expr,
        label=label,
        description=description,
        format=fmt,
    )


def _collect_dataset_definitions(
    documents: Iterable[_AmlDocument],
) -> tuple[
    dict[str, tuple[AmlBlock, _FileContext]],
    dict[str, tuple[AmlBlock, _FileContext]],
    dict[str, tuple[AmlBlock, _FileContext]],
    dict[str, tuple[ObjectAssignment, _FileContext]],
]:
    """Collect Dataset / PartialDataset / standalone Metric blocks and Dataset assignments.

    Returns (dataset_blocks, partial_dataset_blocks, metric_blocks, dataset_assignments).
    """
    dataset_blocks: dict[str, tuple[AmlBlock, _FileContext]] = {}
    partial_dataset_blocks: dict[str, tuple[AmlBlock, _FileContext]] = {}
    metric_blocks: dict[str, tuple[AmlBlock, _FileContext]] = {}
    dataset_assignments: dict[str, tuple[ObjectAssignment, _FileContext]] = {}

    for document in documents:
        context = document.context
        for item in document.items:
            if isinstance(item, AmlBlock) and item.name:
                if item.kind == "Dataset":
                    name = _qualify_declared_name(item.name, context.module_prefix)
                    dataset_blocks[name] = (AmlBlock(kind="Dataset", name=name, items=item.items), context)
                elif item.kind == "PartialDataset":
                    name = _qualify_declared_name(item.name, context.module_prefix)
                    partial_dataset_blocks[name] = (
                        AmlBlock(kind="PartialDataset", name=name, items=item.items),
                        context,
                    )
                elif item.kind == "Metric":
                    name = _qualify_declared_name(item.name, context.module_prefix)
                    metric_blocks[name] = (AmlBlock(kind="Metric", name=name, items=item.items), context)
                continue
            if isinstance(item, ObjectAssignment) and item.kind in {"Dataset", "PartialDataset"}:
                name = _qualify_declared_name(item.name, context.module_prefix)
                dataset_assignments[name] = (item, context)
            elif isinstance(item, ObjectAssignment) and item.kind == "Metric":
                # Inline assignment form: `Metric x = Metric { ... }`. Materialize
                # the value's items into a metric block, matching block-form metrics.
                value = item.value
                if isinstance(value, (TypedBlock, InlineBlock)):
                    name = _qualify_declared_name(item.name, context.module_prefix)
                    metric_blocks[name] = (AmlBlock(kind="Metric", name=name, items=value.items), context)

    return dataset_blocks, partial_dataset_blocks, metric_blocks, dataset_assignments


def _resolve_dataset_artifacts(
    documents: Iterable[_AmlDocument],
    constants: dict[str, AmlValue],
    existing_models: dict[str, Model],
) -> tuple[dict[str, Model], dict[str, Metric], list[tuple[AmlBlock, _FileContext]]]:
    """Resolve dataset-level fields and standalone metrics into models / graph metrics.

    Also returns the resolved assignment-form Dataset blocks (with their
    relationships property intact) so the relationship pass can attach join
    edges declared inside inline `Dataset x = Dataset { ... }` assignments.
    """
    documents = list(documents)
    dataset_blocks, partial_dataset_blocks, metric_blocks, dataset_assignments = _collect_dataset_definitions(documents)

    # PartialDataset blocks resolve like partial models for .extend() composition.
    resolved_blocks: dict[str, AmlBlock] = {}
    resolving: set[str] = set()

    def resolve_named_block(name: str) -> AmlBlock | None:
        if name in resolved_blocks:
            return resolved_blocks[name]
        if name in resolving:
            return None
        if name in dataset_blocks:
            block, block_ctx = dataset_blocks[name]
            stamped = _stamp_block_context(block, block_ctx)
            resolved_blocks[name] = stamped
            return stamped
        if name in partial_dataset_blocks:
            block, block_ctx = partial_dataset_blocks[name]
            return _stamp_block_context(block, block_ctx)
        assignment_entry = dataset_assignments.get(name)
        if assignment_entry:
            assignment, ctx = assignment_entry
            resolving.add(name)
            resolved_value = _resolve_block_from_value(assignment.value, ctx, resolve_named_block)
            resolving.remove(name)
            if resolved_value:
                resolved_blocks[name] = AmlBlock(kind="Dataset", name=name, items=resolved_value.items)
                return resolved_blocks[name]
        return None

    # Standalone Metric blocks become graph-level metrics. Resolved before
    # datasets so a dataset/partial that references one via the reusable
    # "metric name: standalone_metric" shorthand can pull it onto the dataset.
    standalone_metrics: dict[str, Metric] = {}
    for name, (block, context) in metric_blocks.items():
        metric_block = AmlBlock(kind="metric", name=block.name, items=block.items)
        metric = _parse_measure_block(metric_block, constants, context)
        if metric:
            metric.name = name
            standalone_metrics[name] = metric

    dataset_models: dict[str, Model] = {}

    for name, (block, context) in dataset_blocks.items():
        model = _parse_dataset_block(block, constants, context, standalone_metrics)
        if model and model.name not in existing_models:
            dataset_models[model.name] = model

    # Resolved assignment-form dataset blocks, surfaced so the relationship pass
    # can attach join edges declared inside them.
    assignment_dataset_blocks: list[tuple[AmlBlock, _FileContext]] = []

    for name, (assignment, context) in dataset_assignments.items():
        # PartialDataset assignments are reusable composition fragments (consumed
        # via .extend()), not queryable datasets, mirroring named PartialDataset
        # blocks which never surface as standalone models.
        if assignment.kind == "PartialDataset":
            continue
        block = resolve_named_block(name)
        if not block:
            continue
        block_with_name = AmlBlock(kind="Dataset", name=name, items=block.items)
        assignment_dataset_blocks.append((block_with_name, context))
        model = _parse_dataset_block(block_with_name, constants, context, standalone_metrics)
        if model and model.name not in existing_models:
            dataset_models[model.name] = model

    return dataset_models, standalone_metrics, assignment_dataset_blocks


def _resolve_standalone_metric_reference(
    item: AmlProperty,
    standalone_metrics: dict[str, Metric],
    context: _FileContext,
) -> Metric | None:
    """Resolve a `metric name: standalone_metric` shorthand reference.

    The reusable-metric-store shorthand parses to a property whose value is a
    bare identifier/reference naming a top-level Metric. When it resolves to a
    known standalone metric, return a copy renamed to the property key (the
    dataset-local name); otherwise return None so genuine properties such as
    `label`/`description`/`models` are left untouched.
    """
    if not standalone_metrics:
        return None

    value = item.value
    if isinstance(value, Identifier):
        name = value.name
    elif isinstance(value, Reference):
        name = ".".join(value.parts)
    else:
        return None

    # Prefer the name qualified against the referencing file's module prefix /
    # `use` aliases before the bare name, matching how standalone metrics are
    # keyed and how other resolver paths qualify in context first. This lets a
    # local same-named metric (e.g. finance.revenue) win over a root metric of
    # the same name instead of silently resolving to the global one.
    candidates = [_qualify_name(name, context), name]
    for candidate in candidates:
        referenced = standalone_metrics.get(candidate)
        if referenced is not None:
            resolved = referenced.model_copy(deep=True)
            resolved.name = item.key
            return resolved
    return None


def _parse_dataset_block(
    block: AmlBlock,
    constants: dict[str, AmlValue],
    context: _FileContext,
    standalone_metrics: dict[str, Metric] | None = None,
) -> Model | None:
    """Parse a Dataset block's dataset-level dimension/metric blocks into a Model.

    Dataset dimensions and metrics use cross-model AQL definitions (the only
    style Holistics allows in datasets). They are surfaced on a synthetic model
    named after the dataset so they are not silently dropped.

    `standalone_metrics` lets the reusable-metric-store shorthand
    (`metric name: standalone_metric`) resolve the referenced top-level Metric
    onto the dataset instead of dropping the reference.
    """
    if not block.name:
        return None

    standalone_metrics = standalone_metrics or {}

    properties = _properties_from_items(block.items)
    label = _value_as_string(properties.get("label"), constants, context)
    description = _value_as_string(properties.get("description"), constants, context)

    dimensions: list[Dimension] = []
    metrics: list[Metric] = []

    prev_item: AmlItem | None = None
    for item in block.items:
        # Reusable-metric-store shorthand: `metric name: standalone_metric`
        # parses to a bare `metric` keyword marker (an ExpressionStatement)
        # immediately followed by a property naming a top-level Metric. Resolve the
        # referenced standalone metric onto the dataset (renamed to the local key)
        # only when that marker is present, so an ordinary metadata property such
        # as `label: some_identifier` is never misread as a metric reference even
        # if its value coincidentally matches a standalone metric name.
        if isinstance(item, AmlProperty):
            if _is_metric_marker(prev_item):
                referenced = _resolve_standalone_metric_reference(item, standalone_metrics, item.context or context)
                if referenced is not None:
                    metrics.append(referenced)
            prev_item = item
            continue
        if not isinstance(item, AmlBlock):
            prev_item = item
            continue
        prev_item = item
        # A child field carries its own defining context when the dataset was
        # composed across modules (Dataset extending a PartialDataset from another
        # file); resolve its constants/`use` aliases against that file.
        item_context = item.context or context
        if item.kind == "dimension":
            dimension, _ = _parse_dimension_block(item, constants, item_context)
            if not dimension:
                continue
            # Dataset dimensions usually combine fields across models, but some
            # are authored with an aggregate AQL (e.g. count(...)). An aggregate
            # cannot be a groupable dimension, so surface it as a derived metric.
            if _sql_is_aggregate(dimension.sql):
                metrics.append(
                    Metric(
                        name=dimension.name,
                        type="derived",
                        sql=dimension.sql,
                        label=dimension.label,
                        description=dimension.description,
                        format=dimension.format,
                    )
                )
            else:
                dimensions.append(dimension)
        elif item.kind in {"measure", "metric"}:
            metric = _parse_measure_block(item, constants, item_context)
            if metric:
                metrics.append(metric)

    if not dimensions and not metrics:
        return None

    return Model(
        name=block.name,
        description=description or label,
        primary_key=None,
        dimensions=dimensions,
        metrics=metrics,
    )


def _parse_relationship_definition(block: AmlBlock, context: _FileContext) -> _AmlRelationship | None:
    rel = _parse_relationship_block(block, context)
    if not rel:
        return None
    if block.name:
        rel.name = _qualify_declared_name(block.name, context.module_prefix)
    return rel


def _parse_relationship_block(block: AmlBlock, context: _FileContext) -> _AmlRelationship | None:
    properties = _properties_from_items(block.items)

    rel_type = _value_as_string(properties.get("type"), None, context)
    from_val = properties.get("from")
    to_val = properties.get("to")

    if not rel_type or not from_val or not to_val:
        return None

    rel_type = rel_type.strip().lower()
    if rel_type not in {"many_to_one", "one_to_one", "one_to_many"}:
        return None

    from_ref = _value_as_reference(from_val, context)
    to_ref = _value_as_reference(to_val, context)
    if not from_ref or not to_ref:
        return None

    from_model_field = from_ref.to_model_field()
    to_model_field = to_ref.to_model_field()
    if not from_model_field or not to_model_field:
        return None

    from_model, from_field = from_model_field
    to_model, to_field = to_model_field

    return _AmlRelationship(
        name=None,
        from_model=from_model,
        to_model=to_model,
        rel_type=rel_type,
        from_field=from_field,
        to_field=to_field,
    )


def _parse_dataset_relationships(
    block: AmlBlock, context: _FileContext
) -> tuple[list[_AmlRelationship], list[_RelationshipRef]]:
    relationships: list[_AmlRelationship] = []
    refs: list[_RelationshipRef] = []

    relationships_property: AmlProperty | None = None
    for item in block.items:
        if isinstance(item, AmlProperty) and item.key == "relationships":
            relationships_property = item
    if relationships_property is None:
        return relationships, refs

    relationships_value = relationships_property.value
    if not isinstance(relationships_value, list):
        return relationships, refs

    # When the `relationships` property was contributed by a PartialDataset from
    # another module, it carries that module's context so its model/field
    # references (e.g. `rel(orders.customer_id > customers.id)`) qualify against
    # the authoring file rather than the consuming dataset's file.
    context = relationships_property.context or context

    for value in relationships_value:
        if isinstance(value, TypedBlock):
            if value.type_name == "RelationshipConfig":
                rels, rel_refs = _parse_relationship_config(value, context)
                relationships.extend(rels)
                refs.extend(rel_refs)
                continue
            if value.type_name == "Relationship":
                rel = _parse_relationship_block(AmlBlock(kind="Relationship", name=None, items=value.items), context)
                if rel:
                    relationships.append(rel)
                continue
        if isinstance(value, FunctionCall):
            rels, rel_refs = _parse_relationship_function(value, context)
            relationships.extend(rels)
            refs.extend(rel_refs)
            continue
        if isinstance(value, Identifier):
            refs.append(_RelationshipRef(name=_qualify_name(value.name, context)))
            continue

    return relationships, refs


def _parse_relationship_config(
    config: TypedBlock, context: _FileContext
) -> tuple[list[_AmlRelationship], list[_RelationshipRef]]:
    relationships: list[_AmlRelationship] = []
    refs: list[_RelationshipRef] = []

    properties = _properties_from_items(config.items)
    active = _value_as_bool(properties.get("active"))
    if active is False:
        return relationships, refs

    rel_value = properties.get("rel")
    if isinstance(rel_value, TypedBlock) and rel_value.type_name == "Relationship":
        rel = _parse_relationship_block(AmlBlock(kind="Relationship", name=None, items=rel_value.items), context)
        if rel:
            relationships.append(rel)
        return relationships, refs

    if isinstance(rel_value, Identifier):
        refs.append(_RelationshipRef(name=_qualify_name(rel_value.name, context)))

    return relationships, refs


def _parse_relationship_function(
    func: FunctionCall, context: _FileContext
) -> tuple[list[_AmlRelationship], list[_RelationshipRef]]:
    relationships: list[_AmlRelationship] = []
    refs: list[_RelationshipRef] = []

    func_name = func.name
    if func_name not in {"relationship", "rel"}:
        return relationships, refs

    if func_name == "relationship":
        rel_expr = func.args[0] if func.args else None
        active = _extract_active_flag(func.args)
        if active is False:
            return relationships, refs

        if isinstance(rel_expr, Identifier):
            refs.append(_RelationshipRef(name=_qualify_name(rel_expr.name, context)))
            return relationships, refs

        rel = _relationship_from_expression(rel_expr, context)
        if rel:
            relationships.append(rel)
        return relationships, refs

    if func_name == "rel":
        rel_expr = None
        active = _extract_active_flag(func.args)
        if func.args and not isinstance(func.args[0], NamedArg):
            rel_expr = func.args[0]
        for arg in func.args:
            if isinstance(arg, NamedArg) and arg.name == "rel_expr":
                rel_expr = arg.value
        if active is False:
            return relationships, refs
        rel = _relationship_from_expression(rel_expr, context)
        if rel:
            relationships.append(rel)
        return relationships, refs

    return relationships, refs


def _extract_active_flag(args: list[AmlValue]) -> bool:
    for arg in args:
        if isinstance(arg, NamedArg) and arg.name == "active":
            return _value_as_bool(arg.value) is not False
    if len(args) > 1:
        return _value_as_bool(args[1]) is not False
    return True


def _relationship_from_expression(expr: AmlValue | None, context: _FileContext) -> _AmlRelationship | None:
    if isinstance(expr, BinaryOp):
        if expr.op not in {">", "-"}:
            return None
        left_ref = _value_as_reference(expr.left, context)
        right_ref = _value_as_reference(expr.right, context)
        if not left_ref or not right_ref:
            return None
        left = left_ref.to_model_field()
        right = right_ref.to_model_field()
        if not left or not right:
            return None
        rel_type = "many_to_one" if expr.op == ">" else "one_to_one"
        return _AmlRelationship(
            name=None,
            from_model=left[0],
            to_model=right[0],
            rel_type=rel_type,
            from_field=left[1],
            to_field=right[1],
        )

    return None


def _resolve_const_value(
    value: AmlValue, constants: dict[str, AmlValue] | None, context: _FileContext | None
) -> AmlValue:
    if not constants or not isinstance(value, Identifier):
        return value

    current: AmlValue = value
    seen: set[str] = set()

    while isinstance(current, Identifier):
        name = current.name
        if name in seen:
            break
        seen.add(name)

        if context:
            qualified = _qualify_name(name, context)
            if qualified in constants:
                current = constants[qualified]
                continue

        if name in constants:
            current = constants[name]
            continue

        break

    return current


def _value_as_string(
    value: AmlValue, constants: dict[str, AmlValue] | None = None, context: _FileContext | None = None
) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, Identifier):
        resolved = _resolve_const_value(value, constants, context)
        if isinstance(resolved, str):
            return resolved
        if isinstance(resolved, Identifier):
            return resolved.name
        return value.name
    return None


def _value_as_bool(value: AmlValue) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _value_as_reference(value: AmlValue, context: _FileContext | None = None) -> Reference | None:
    if isinstance(value, Reference):
        ref = value
        return _qualify_reference(ref, context) if context else ref
    if isinstance(value, TypedBlock) and value.type_name in {"FieldRef", "fieldref", "field_ref"}:
        props = _properties_from_items(value.items)
        model_name = _value_as_string(props.get("model"))
        field_name = _value_as_string(props.get("field"))
        if model_name and field_name:
            parts = model_name.split(".") + [field_name]
            ref = Reference(parts=parts)
            return _qualify_reference(ref, context) if context else ref
    if isinstance(value, FunctionCall) and value.name == "r" and value.args:
        arg = value.args[0]
        if isinstance(arg, Reference):
            ref = arg
            return _qualify_reference(ref, context) if context else ref
        if isinstance(arg, Identifier):
            parts = arg.name.split(".")
            if len(parts) > 1:
                ref = Reference(parts=parts)
                return _qualify_reference(ref, context) if context else ref
    if isinstance(value, Identifier) and "." in value.name:
        ref = Reference(parts=value.name.split("."))
        return _qualify_reference(ref, context) if context else ref
    return None


def _properties_from_items(items: Iterable[AmlItem]) -> dict[str, AmlValue]:
    props: dict[str, AmlValue] = {}
    for item in items:
        if isinstance(item, AmlProperty):
            props[item.key] = item.value
    return props


def _property_contexts_from_items(items: Iterable[AmlItem]) -> dict[str, _FileContext | None]:
    """Map each property key to the file context it was authored in.

    When a field block is composed across modules (a PartialDataset metric from
    one file extended/overridden by a partial in another), individual properties
    carry their own origin context. Resolving each property against that context
    keeps constants/`use` aliases (e.g. `definition: rev_def`) bound to the file
    that defined them rather than the file that overrode a sibling property.
    """
    contexts: dict[str, _FileContext | None] = {}
    for item in items:
        if isinstance(item, AmlProperty):
            contexts[item.key] = item.context
    return contexts


def _normalize_definition(
    value: AmlValue, constants: dict[str, AmlValue] | None = None, context: _FileContext | None = None
) -> str | None:
    if value is None:
        return None

    resolved = _resolve_const_value(value, constants, context)

    expr = None
    if isinstance(resolved, TaggedBlock):
        if resolved.tag.lower() == "aql":
            expr = _translate_aql_to_sql(resolved.body)
        else:
            expr = resolved.body
    elif isinstance(resolved, Identifier):
        expr = resolved.name
    elif isinstance(resolved, Reference):
        expr = ".".join(resolved.parts)
    elif isinstance(resolved, str):
        expr = resolved
    elif isinstance(resolved, (BinaryOp, UnaryOp, FunctionCall, list, int, float, bool)):
        expr = _expression_to_string(resolved)

    if not expr:
        return None

    return _strip_aml_interpolations(expr)


def _expression_to_string(value: AmlValue) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, Identifier):
        return value.name
    if isinstance(value, Reference):
        return ".".join(value.parts)
    if isinstance(value, TaggedBlock):
        return value.body
    if isinstance(value, UnaryOp):
        operand = _expression_to_string(value.operand) or ""
        return f"{value.op}{operand}"
    if isinstance(value, BinaryOp):
        left = _expression_to_string(value.left) or ""
        right = _expression_to_string(value.right) or ""
        return f"{left} {value.op} {right}".strip()
    if isinstance(value, FunctionCall):
        args = []
        for arg in value.args:
            if isinstance(arg, NamedArg):
                arg_value = _expression_to_string(arg.value) or ""
                args.append(f"{arg.name}: {arg_value}")
            else:
                args.append(_expression_to_string(arg) or "")
        return f"{value.name}({', '.join(args)})"
    if isinstance(value, list):
        items = ", ".join(filter(None, (_expression_to_string(item) for item in value)))
        return f"[{items}]"
    return None


def _translate_aql_to_sql(expr: str) -> str:
    expr = expr.strip()
    if not expr:
        return expr

    segments = _split_aql_pipeline(expr)
    base = segments[0].strip()

    if len(segments) == 1:
        return _translate_aql_inline(base)

    # Translate the base segment too: it may itself be a function call
    # (e.g. count(orders.id) | of_all(...)), not just a bare field reference.
    current = _translate_aql_inline(base)
    for segment in segments[1:]:
        current = _apply_aql_pipe(current, segment.strip())
    return current


def _split_aql_pipeline(expr: str) -> list[str]:
    segments: list[str] = []
    buf: list[str] = []
    depth = 0
    quote = None
    escape = False

    for ch in expr:
        if escape:
            buf.append(ch)
            escape = False
            continue
        if ch == "\\":
            buf.append(ch)
            escape = True
            continue
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in {"'", '"', "`"}:
            quote = ch
            buf.append(ch)
            continue
        if ch == "(":
            depth += 1
            buf.append(ch)
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            buf.append(ch)
            continue
        if ch == "|" and depth == 0:
            segment = "".join(buf).strip()
            if segment:
                segments.append(segment)
            buf = []
            continue
        buf.append(ch)

    tail = "".join(buf).strip()
    if tail:
        segments.append(tail)
    return segments


def _translate_aql_inline(expr: str) -> str:
    expr = _replace_aql_macros(expr)
    buf: list[str] = []
    idx = 0

    while idx < len(expr):
        match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", expr[idx:])
        if not match:
            buf.append(expr[idx])
            idx += 1
            continue

        name = match.group(0)
        next_idx = idx + len(name)
        if next_idx >= len(expr) or expr[next_idx] != "(":
            buf.append(name)
            idx = next_idx
            continue

        closing = _find_matching_paren(expr, next_idx)
        if closing is None:
            buf.append(name)
            idx = next_idx
            continue

        args_str = expr[next_idx + 1 : closing]
        args = _split_aql_args(args_str)
        replacement = _apply_aql_function(name, args, base=None)
        buf.append(replacement)
        idx = closing + 1

    return "".join(buf)


def _apply_aql_pipe(base: str, segment: str) -> str:
    match = re.match(r"^(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*(?:\((?P<args>.*)\))?\s*$", segment)
    if not match:
        return base
    name = match.group("name")
    args = _split_aql_args(match.group("args") or "")
    return _apply_aql_function(name, args, base=base)


def _find_matching_paren(expr: str, start: int) -> int | None:
    depth = 0
    quote = None
    escape = False

    for idx in range(start, len(expr)):
        ch = expr[idx]
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if quote:
            if ch == quote:
                quote = None
            continue
        if ch in {"'", '"', "`"}:
            quote = ch
            continue
        if ch == "(":
            depth += 1
            continue
        if ch == ")":
            depth -= 1
            if depth == 0:
                return idx
            continue

    return None


# Aggregation functions: map directly to SQL aggregates.
_AQL_AGG_SQL = {
    "count": "COUNT",
    "count_all": "COUNT",
    "sum": "SUM",
    "avg": "AVG",
    "average": "AVG",
    "min": "MIN",
    "max": "MAX",
    "median": "MEDIAN",
}

# Table-shaping functions (filter/group/select rows of a table). At the
# SQL-fragment level the surrounding aggregation produces the value, so these
# are best-effort passed through (the base flows to the next pipe stage).
_AQL_TABLE_FUNCS = {"filter", "group", "select", "where"}

# Metric-modifier functions that change a metric's grouping/period scope.
# We cannot express their windowing at the fragment level, so we preserve the
# inner metric expression rather than dropping it.
_AQL_METRIC_MODIFIERS = {"of_all", "exclude", "keep_grains", "relative_period", "period_to_date", "running_total"}


def _apply_aql_function(name: str, args: list[str], base: str | None) -> str:
    normalized = name.strip().lower()
    cleaned_args = [_replace_aql_macros(arg.strip()) for arg in args if arg.strip()]
    target = cleaned_args[0] if cleaned_args else base or "*"

    if normalized in _AQL_AGG_SQL:
        sql_func = _AQL_AGG_SQL[normalized]
        # Two-arg aggregation form: sum(table, expr) aggregates expr over table.
        if base is None and len(cleaned_args) >= 2:
            return f"{sql_func}({cleaned_args[1]})"
        return f"{sql_func}({target})"
    if normalized in {"count_distinct", "countdistinct"}:
        if base is None and len(cleaned_args) >= 2:
            return f"COUNT(DISTINCT {cleaned_args[1]})"
        return f"COUNT(DISTINCT {target})"
    if normalized in {"count_if", "countif"}:
        condition = cleaned_args[0] if cleaned_args else (base or "")
        if not condition:
            return "COUNT(*)"
        return f"SUM(CASE WHEN {condition} THEN 1 ELSE 0 END)"

    if normalized in _AQL_TABLE_FUNCS:
        # Best-effort: keep the upstream table expression flowing through the
        # pipeline; the aggregation that follows still produces the value.
        if base is not None:
            return base
        if cleaned_args:
            return cleaned_args[0]
        return "*"

    if normalized in _AQL_METRIC_MODIFIERS:
        # Preserve the metric being modified so it is not lost. The scope change
        # (ignore grouping / shift period) cannot be expressed in a fragment.
        inner = base if base is not None else (cleaned_args[0] if cleaned_args else "")
        return inner

    if base:
        combined_args = [base] + cleaned_args if cleaned_args else [base]
    else:
        combined_args = cleaned_args
    return f"{name}({', '.join(combined_args)})"


def _split_aql_args(expr: str) -> list[str]:
    if not expr.strip():
        return []
    args: list[str] = []
    buf: list[str] = []
    depth = 0
    quote = None
    escape = False

    for ch in expr:
        if escape:
            buf.append(ch)
            escape = False
            continue
        if ch == "\\":
            buf.append(ch)
            escape = True
            continue
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in {"'", '"', "`"}:
            quote = ch
            buf.append(ch)
            continue
        if ch == "(":
            depth += 1
            buf.append(ch)
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            buf.append(ch)
            continue
        if ch == "," and depth == 0:
            arg = "".join(buf).strip()
            if arg:
                args.append(arg)
            buf = []
            continue
        buf.append(ch)

    tail = "".join(buf).strip()
    if tail:
        args.append(tail)
    return args


def _replace_aql_macros(expr: str) -> str:
    expr = re.sub(r"@now\b", "CURRENT_TIMESTAMP", expr, flags=re.IGNORECASE)
    expr = re.sub(r"@today\b", "CURRENT_DATE", expr, flags=re.IGNORECASE)
    return expr


def _strip_aml_interpolations(expr: str) -> str:
    def repl(match: re.Match) -> str:
        inner = match.group(1).strip()
        if inner.startswith("#"):
            inner = inner[1:].strip()
        if inner.lower().startswith("source."):
            inner = inner.split(".", 1)[1]
        return inner

    return _AML_INTERPOLATION_RE.sub(repl, expr)


def _normalize_agg_type(value: str | None) -> str | None:
    if not value:
        return None

    agg = value.strip().lower().replace("_", " ").replace("-", " ")
    agg = re.sub(r"\s+", " ", agg)
    return agg


def _map_agg_to_sql(agg: str) -> str:
    mapping = {
        "stdev": "STDDEV_SAMP",
        "stddev": "STDDEV_SAMP",
        "stdevp": "STDDEV_POP",
        "stddev_pop": "STDDEV_POP",
        "stddev pop": "STDDEV_POP",
        "var": "VAR_SAMP",
        "variance": "VAR_SAMP",
        "varp": "VAR_POP",
        "variance_pop": "VAR_POP",
    }
    return mapping.get(agg, agg.replace(" ", "_"))


def _detect_ratio(expr: str) -> tuple[str, str] | None:
    ratio_nullif = re.compile(
        r"^\s*([A-Za-z0-9_.]+)\s*/\s*NULLIF\(\s*([A-Za-z0-9_.]+)\s*,\s*0\s*\)\s*$",
        re.IGNORECASE,
    )
    ratio_simple = re.compile(r"^\s*([A-Za-z0-9_.]+)\s*/\s*([A-Za-z0-9_.]+)\s*$")

    match = ratio_nullif.match(expr)
    if match:
        return match.group(1), match.group(2)

    match = ratio_simple.match(expr)
    if match:
        return match.group(1), match.group(2)

    return None


_AGGREGATE_FUNC_RE = re.compile(
    r"\b(COUNT|SUM|AVG|MIN|MAX|MEDIAN|STDDEV|STDDEV_SAMP|STDDEV_POP|VAR_SAMP|VAR_POP|VARIANCE)\s*\(",
    re.IGNORECASE,
)


def _sql_is_aggregate(sql: str | None) -> bool:
    """Return True if the SQL fragment contains a SQL aggregate function call."""
    if not sql:
        return False
    return bool(_AGGREGATE_FUNC_RE.search(sql))


def _map_dimension_type(dim_type: str | None) -> tuple[str, str | None]:
    if not dim_type:
        return "categorical", None

    dim_type_lower = dim_type.lower()

    if dim_type_lower == "text":
        return "categorical", None
    if dim_type_lower == "number":
        return "numeric", None
    if dim_type_lower == "truefalse":
        return "boolean", None
    if dim_type_lower == "date":
        return "time", "day"
    if dim_type_lower == "datetime":
        return "time", "hour"

    return "categorical", None


def _dedupe_relationships(relationships: Iterable[_AmlRelationship]) -> list[_AmlRelationship]:
    deduped: list[_AmlRelationship] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for rel in relationships:
        signature = (rel.from_model, rel.to_model, rel.rel_type, rel.from_field, rel.to_field)
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(rel)
    return deduped


def _export_dimension(dimension: Dimension, primary_key: str) -> list[str]:
    lines = [f"  dimension {dimension.name} {{"]

    if dimension.label:
        lines.append(f"    label: '{_escape_single_quotes(dimension.label)}'")

    aml_type = _map_dimension_type_to_aml(dimension)
    lines.append(f"    type: '{aml_type}'")

    if dimension.description:
        lines.append(f"    description: '{_escape_single_quotes(dimension.description)}'")

    if dimension.name == primary_key:
        lines.append("    primary_key: true")

    if dimension.format:
        lines.append(f"    format: '{_escape_single_quotes(dimension.format)}'")

    if dimension.sql and dimension.sql != dimension.name:
        lines.append(f"    definition: @sql {dimension.sql};;")

    lines.append("  }")
    return lines


def _export_measure(metric: Metric, primary_key: str) -> list[str]:
    lines = [f"  measure {metric.name} {{"]

    if metric.label:
        lines.append(f"    label: '{_escape_single_quotes(metric.label)}'")

    if metric.description:
        lines.append(f"    description: '{_escape_single_quotes(metric.description)}'")

    if metric.format:
        lines.append(f"    format: '{_escape_single_quotes(metric.format)}'")

    aggregation_type = None
    definition = None

    if metric.type == "ratio" and metric.numerator and metric.denominator:
        aggregation_type = "custom"
        definition = f"{metric.numerator} / NULLIF({metric.denominator}, 0)"
    elif metric.type == "derived" and metric.sql:
        aggregation_type = "custom"
        definition = metric.sql
    elif metric.agg:
        aggregation_type = _map_agg_to_aml(metric.agg)
        if metric.sql:
            definition = metric.sql
    elif metric.sql:
        aggregation_type = "custom"
        definition = metric.sql

    if aggregation_type:
        lines.append(f"    aggregation_type: '{aggregation_type}'")

    if definition:
        lines.append(f"    definition: @sql {definition};;")

    lines.append("  }")
    return lines


def _map_dimension_type_to_aml(dimension: Dimension) -> str:
    if dimension.type == "numeric":
        return "number"
    if dimension.type == "boolean":
        return "truefalse"
    if dimension.type == "time":
        if dimension.granularity == "day":
            return "date"
        return "datetime"
    return "text"


def _map_agg_to_aml(agg: str) -> str:
    mapping = {
        "count": "count",
        "count_distinct": "count distinct",
        "sum": "sum",
        "avg": "avg",
        "min": "min",
        "max": "max",
        "median": "median",
    }
    return mapping.get(agg, "custom")


def _escape_single_quotes(value: str) -> str:
    return value.replace("'", "\\'")
