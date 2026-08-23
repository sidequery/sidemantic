from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any, TypeAlias


class UnaryOp(str, Enum):
    plus = "Plus"
    minus = "Minus"
    not_ = "Not"


class BinaryOp(str, Enum):
    or_ = "Or"
    and_ = "And"
    eq = "Eq"
    strict_eq = "StrictEq"
    neq = "Neq"
    lt = "Lt"
    lte = "Lte"
    gt = "Gt"
    gte = "Gte"
    in_ = "In"
    concat = "Concat"
    add = "Add"
    sub = "Sub"
    mul = "Mul"
    div = "Div"
    pow = "Pow"


class SortDirection(str, Enum):
    asc = "Asc"
    desc = "Desc"


@dataclass(frozen=True, slots=True)
class Span:
    start: int
    end: int


@dataclass(frozen=True, slots=True)
class Dialect:
    allow_semicolon_separators: bool = True
    allow_decimal_comma: bool = False
    allow_dash_dash_comments: bool = True
    allow_double_slash_comments: bool = True
    allow_block_comments: bool = True

    def native_args(self) -> tuple[bool, bool, bool, bool, bool]:
        return (
            self.allow_semicolon_separators,
            self.allow_decimal_comma,
            self.allow_dash_dash_comments,
            self.allow_double_slash_comments,
            self.allow_block_comments,
        )


@dataclass(frozen=True, slots=True)
class TableName:
    name: str
    quoted: bool


@dataclass(frozen=True, slots=True)
class VarDecl:
    name: str
    expr: Expr


@dataclass(frozen=True, slots=True)
class Number:
    value: str


@dataclass(frozen=True, slots=True)
class String:
    value: str


@dataclass(frozen=True, slots=True)
class DateTime:
    value: str


@dataclass(frozen=True, slots=True)
class Boolean:
    value: bool


@dataclass(frozen=True, slots=True)
class Blank:
    pass


@dataclass(frozen=True, slots=True)
class Omitted:
    pass


@dataclass(frozen=True, slots=True)
class Parameter:
    name: str


@dataclass(frozen=True, slots=True)
class Identifier:
    name: str


@dataclass(frozen=True, slots=True)
class TableRef:
    table: TableName


@dataclass(frozen=True, slots=True)
class BracketRef:
    name: str


@dataclass(frozen=True, slots=True)
class TableColumnRef:
    table: TableName
    column: str


@dataclass(frozen=True, slots=True)
class HierarchyRef:
    table: TableName
    column: str
    levels: list[str]


@dataclass(frozen=True, slots=True)
class FunctionCall:
    name: str
    args: list[Expr]


class DataTableType(str, Enum):
    boolean = "Boolean"
    currency = "Currency"
    datetime = "DateTime"
    double = "Double"
    integer = "Integer"
    string = "String"


@dataclass(frozen=True, slots=True)
class DataTableColumn:
    name: str
    data_type: DataTableType


@dataclass(frozen=True, slots=True)
class DataTable:
    columns: list[DataTableColumn]
    rows: list[list[Expr]]


@dataclass(frozen=True, slots=True)
class Unary:
    op: UnaryOp
    expr: Expr


@dataclass(frozen=True, slots=True)
class Binary:
    op: BinaryOp
    left: Expr
    right: Expr


@dataclass(frozen=True, slots=True)
class VarBlock:
    decls: list[VarDecl]
    body: Expr


@dataclass(frozen=True, slots=True)
class TableConstructor:
    rows: list[list[Expr]]


@dataclass(frozen=True, slots=True)
class Paren:
    expr: Expr


@dataclass(frozen=True, slots=True)
class Tuple:
    elements: list[Expr]


Expr: TypeAlias = (
    Number
    | String
    | DateTime
    | Boolean
    | Blank
    | Omitted
    | Parameter
    | Identifier
    | TableRef
    | BracketRef
    | TableColumnRef
    | HierarchyRef
    | FunctionCall
    | DataTable
    | Unary
    | Binary
    | VarBlock
    | TableConstructor
    | Paren
    | Tuple
)


@dataclass(frozen=True, slots=True)
class MeasureDef:
    doc: str | None
    table: TableName | None
    name: str
    expr: Expr


@dataclass(frozen=True, slots=True)
class VarDef:
    doc: str | None
    name: str
    expr: Expr


@dataclass(frozen=True, slots=True)
class TableDef:
    doc: str | None
    name: str
    expr: Expr
    visual_shape: VisualShape | None = None


@dataclass(frozen=True, slots=True)
class VisualShapeColumn:
    name: str


@dataclass(frozen=True, slots=True)
class VisualShapeGroup:
    columns: list[VisualShapeColumn]
    total: VisualShapeColumn


@dataclass(frozen=True, slots=True)
class VisualShapeAxis:
    name: str
    groups: list[VisualShapeGroup]
    order_by: list[VisualShapeColumn]


@dataclass(frozen=True, slots=True)
class VisualShape:
    axes: list[VisualShapeAxis]
    densify: str | None


@dataclass(frozen=True, slots=True)
class ColumnDef:
    doc: str | None
    table: TableName | None
    name: str
    expr: Expr


@dataclass(frozen=True, slots=True)
class FuncParam:
    name: str
    type_hints: list[str]
    default: Expr | None = None


@dataclass(frozen=True, slots=True)
class FunctionDef:
    doc: str | None
    name: str
    params: list[FuncParam]
    body: Expr


Definition: TypeAlias = MeasureDef | VarDef | TableDef | ColumnDef | FunctionDef


@dataclass(frozen=True, slots=True)
class DefineBlock:
    defs: list[Definition]


@dataclass(frozen=True, slots=True)
class OrderKey:
    expr: Expr
    direction: SortDirection


@dataclass(frozen=True, slots=True)
class EvaluateStmt:
    expr: Expr
    order_by: list[OrderKey]
    start_at: list[Expr] | None


@dataclass(frozen=True, slots=True)
class Query:
    define: DefineBlock | None
    evaluates: list[EvaluateStmt]


class AstNodeKind(str, Enum):
    expression = "Expression"
    definition = "Definition"
    define_block = "DefineBlock"
    evaluate = "Evaluate"
    query = "Query"


@dataclass(frozen=True, slots=True)
class AstNodeSpan:
    kind: AstNodeKind
    span: Span


class CommentKind(str, Enum):
    dash_line = "DashLine"
    slash_line = "SlashLine"
    block = "Block"
    doc_line = "DocLine"


@dataclass(frozen=True, slots=True)
class SourceComment:
    kind: CommentKind
    span: Span
    text: str
    previous_node: int | None
    next_node: int | None
    containing_node: int | None


@dataclass(frozen=True, slots=True)
class LosslessParse:
    ast: Expr | Query
    source: str
    span: Span
    nodes: list[AstNodeSpan]
    comments: list[SourceComment]


class RecoveryPhase(str, Enum):
    lex = "Lex"
    parse = "Parse"


@dataclass(frozen=True, slots=True)
class RecoveryDiagnostic:
    phase: RecoveryPhase
    message: str
    span: Span


RecoveredQueryItem: TypeAlias = Definition | EvaluateStmt


@dataclass(frozen=True, slots=True)
class RecoveredItem:
    value: Expr | RecoveredQueryItem
    span: Span


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    items: list[RecoveredItem]
    diagnostics: list[RecoveryDiagnostic]

    @property
    def is_clean(self) -> bool:
        return not self.diagnostics


@dataclass(frozen=True, slots=True)
class ModelTable:
    name: str
    columns: list[str]
    measures: list[str]


@dataclass(frozen=True, slots=True)
class ModelMetadata:
    tables: list[ModelTable]


class ModelValidationCode(str, Enum):
    unknown_table = "UnknownTable"
    unknown_member = "UnknownMember"
    unknown_identifier = "UnknownIdentifier"
    ambiguous_reference = "AmbiguousReference"
    conflicting_variable_name = "ConflictingVariableName"


@dataclass(frozen=True, slots=True)
class ModelValidationIssue:
    code: ModelValidationCode
    message: str
    path: str


class ValidationCode(str, Enum):
    unrecognized_function = "UnrecognizedFunction"
    function_arity = "FunctionArity"
    expected_table = "ExpectedTable"
    expected_scalar = "ExpectedScalar"
    invalid_argument_type = "InvalidArgumentType"
    missing_definition_table = "MissingDefinitionTable"
    invalid_function_name = "InvalidFunctionName"
    invalid_parameter_name = "InvalidParameterName"
    duplicate_parameter = "DuplicateParameter"
    too_many_parameters = "TooManyParameters"
    invalid_type_hint = "InvalidTypeHint"
    invalid_default_expression = "InvalidDefaultExpression"
    invalid_datatable_value = "InvalidDataTableValue"
    invalid_variable_name = "InvalidVariableName"
    duplicate_variable = "DuplicateVariable"
    missing_required_argument = "MissingRequiredArgument"
    duplicate_function = "DuplicateFunction"


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    code: ValidationCode
    message: str
    path: str


@dataclass(frozen=True, slots=True)
class IdentToken:
    value: str


@dataclass(frozen=True, slots=True)
class DocCommentToken:
    value: str


@dataclass(frozen=True, slots=True)
class ParamToken:
    value: str


@dataclass(frozen=True, slots=True)
class NumberToken:
    value: str


@dataclass(frozen=True, slots=True)
class StringToken:
    value: str


@dataclass(frozen=True, slots=True)
class DateTimeToken:
    value: str


@dataclass(frozen=True, slots=True)
class QuotedIdentToken:
    value: str


@dataclass(frozen=True, slots=True)
class BracketIdentToken:
    value: str


@dataclass(frozen=True, slots=True)
class LParen:
    pass


@dataclass(frozen=True, slots=True)
class RParen:
    pass


@dataclass(frozen=True, slots=True)
class LBrace:
    pass


@dataclass(frozen=True, slots=True)
class RBrace:
    pass


@dataclass(frozen=True, slots=True)
class Comma:
    pass


@dataclass(frozen=True, slots=True)
class Semicolon:
    pass


@dataclass(frozen=True, slots=True)
class Colon:
    pass


@dataclass(frozen=True, slots=True)
class Arrow:
    pass


@dataclass(frozen=True, slots=True)
class Plus:
    pass


@dataclass(frozen=True, slots=True)
class Minus:
    pass


@dataclass(frozen=True, slots=True)
class Star:
    pass


@dataclass(frozen=True, slots=True)
class Slash:
    pass


@dataclass(frozen=True, slots=True)
class Caret:
    pass


@dataclass(frozen=True, slots=True)
class Amp:
    pass


@dataclass(frozen=True, slots=True)
class Eq:
    pass


@dataclass(frozen=True, slots=True)
class EqEq:
    pass


@dataclass(frozen=True, slots=True)
class Neq:
    pass


@dataclass(frozen=True, slots=True)
class Lt:
    pass


@dataclass(frozen=True, slots=True)
class Lte:
    pass


@dataclass(frozen=True, slots=True)
class Gt:
    pass


@dataclass(frozen=True, slots=True)
class Gte:
    pass


@dataclass(frozen=True, slots=True)
class Dot:
    pass


@dataclass(frozen=True, slots=True)
class AndAnd:
    pass


@dataclass(frozen=True, slots=True)
class OrOr:
    pass


@dataclass(frozen=True, slots=True)
class Eof:
    pass


TokenKind: TypeAlias = (
    IdentToken
    | DocCommentToken
    | ParamToken
    | NumberToken
    | StringToken
    | DateTimeToken
    | QuotedIdentToken
    | BracketIdentToken
    | LParen
    | RParen
    | LBrace
    | RBrace
    | Comma
    | Semicolon
    | Colon
    | Arrow
    | Plus
    | Minus
    | Star
    | Slash
    | Caret
    | Amp
    | Eq
    | EqEq
    | Neq
    | Lt
    | Lte
    | Gt
    | Gte
    | Dot
    | AndAnd
    | OrOr
    | Eof
)


@dataclass(frozen=True, slots=True)
class Token:
    kind: TokenKind
    span: Span


def parse_expression(text: str, *, dialect: Dialect | None = None) -> Expr:
    raw = _native_parse_expression(text, dialect)
    return from_raw_expr(raw)


def parse_query(text: str, *, dialect: Dialect | None = None) -> Query:
    raw = _native_parse_query(text, dialect)
    return from_raw_query(raw)


def lex(text: str, *, dialect: Dialect | None = None) -> list[Token]:
    raw = _native_lex(text, dialect)
    return from_raw_tokens(raw, source=text)


def format_expression(
    text: str,
    *,
    localized: bool = False,
    dialect: Dialect | None = None,
) -> str:
    native = _native_module()
    return native.format_expression(text, localized, *(dialect or Dialect()).native_args())


def format_query(
    text: str,
    *,
    localized: bool = False,
    dialect: Dialect | None = None,
) -> str:
    native = _native_module()
    return native.format_query(text, localized, *(dialect or Dialect()).native_args())


def parse_expression_lossless(
    text: str,
    *,
    dialect: Dialect | None = None,
) -> LosslessParse:
    native = _native_module()
    raw = json.loads(native.parse_expression_lossless(text, *(dialect or Dialect()).native_args()))
    return _from_raw_lossless(raw, query=False)


def parse_query_lossless(
    text: str,
    *,
    dialect: Dialect | None = None,
) -> LosslessParse:
    native = _native_module()
    raw = json.loads(native.parse_query_lossless(text, *(dialect or Dialect()).native_args()))
    return _from_raw_lossless(raw, query=True)


def recover_expression(
    text: str,
    *,
    dialect: Dialect | None = None,
) -> RecoveryResult:
    native = _native_module()
    raw = json.loads(native.recover_expression(text, *(dialect or Dialect()).native_args()))
    return _from_raw_recovery(raw, query=False, source=text)


def recover_query(
    text: str,
    *,
    dialect: Dialect | None = None,
) -> RecoveryResult:
    native = _native_module()
    raw = json.loads(native.recover_query(text, *(dialect or Dialect()).native_args()))
    return _from_raw_recovery(raw, query=True, source=text)


def validate_expression(
    text: str,
    *,
    dialect: Dialect | None = None,
    report_unrecognized_functions: bool = False,
) -> list[ValidationIssue]:
    native = _native_module()
    raw = json.loads(
        native.validate_expression(
            text,
            *(dialect or Dialect()).native_args(),
            report_unrecognized_functions,
        )
    )
    return _from_raw_validation_issues(raw)


def validate_query(
    text: str,
    *,
    dialect: Dialect | None = None,
    report_unrecognized_functions: bool = False,
) -> list[ValidationIssue]:
    native = _native_module()
    raw = json.loads(
        native.validate_query(
            text,
            *(dialect or Dialect()).native_args(),
            report_unrecognized_functions,
        )
    )
    return _from_raw_validation_issues(raw)


def validate_expression_against_model(
    text: str,
    model: ModelMetadata,
    *,
    dialect: Dialect | None = None,
) -> list[ModelValidationIssue]:
    native = _native_module()
    raw = json.loads(
        native.validate_expression_against_model(
            text,
            _model_json(model),
            *(dialect or Dialect()).native_args(),
        )
    )
    return _from_raw_model_validation_issues(raw)


def validate_query_against_model(
    text: str,
    model: ModelMetadata,
    *,
    dialect: Dialect | None = None,
) -> list[ModelValidationIssue]:
    native = _native_module()
    raw = json.loads(
        native.validate_query_against_model(
            text,
            _model_json(model),
            *(dialect or Dialect()).native_args(),
        )
    )
    return _from_raw_model_validation_issues(raw)


def from_raw_expr(raw: Any) -> Expr:
    if isinstance(raw, str):
        if raw == "Blank":
            return Blank()
        if raw == "Omitted":
            return Omitted()
        raise ValueError(f"Unexpected expr variant: {raw}")
    if not isinstance(raw, dict) or len(raw) != 1:
        raise ValueError(f"Invalid expr payload: {raw!r}")

    key, value = next(iter(raw.items()))
    if key == "Number":
        return Number(value=value)
    if key == "String":
        return String(value=value)
    if key == "DateTime":
        return DateTime(value=value)
    if key == "Boolean":
        return Boolean(value=bool(value))
    if key == "Blank":
        return Blank()
    if key == "Parameter":
        return Parameter(name=value)
    if key == "Identifier":
        return Identifier(name=value)
    if key == "TableRef":
        return TableRef(table=_from_raw_table_name(value))
    if key == "BracketRef":
        return BracketRef(name=value)
    if key == "TableColumnRef":
        return TableColumnRef(table=_from_raw_table_name(value["table"]), column=value["column"])
    if key == "HierarchyRef":
        return HierarchyRef(
            table=_from_raw_table_name(value["table"]),
            column=value["column"],
            levels=list(value.get("levels", [])),
        )
    if key == "FunctionCall":
        return FunctionCall(name=value["name"], args=[from_raw_expr(arg) for arg in value["args"]])
    if key == "DataTable":
        return DataTable(
            columns=[
                DataTableColumn(
                    name=column["name"],
                    data_type=DataTableType(column["data_type"]),
                )
                for column in value["columns"]
            ],
            rows=[[from_raw_expr(expr) for expr in row] for row in value["rows"]],
        )
    if key == "Unary":
        return Unary(op=_to_unary_op(value["op"]), expr=from_raw_expr(value["expr"]))
    if key == "Binary":
        return Binary(
            op=_to_binary_op(value["op"]),
            left=from_raw_expr(value["left"]),
            right=from_raw_expr(value["right"]),
        )
    if key == "VarBlock":
        return VarBlock(
            decls=[_from_raw_var_decl(decl) for decl in value["decls"]],
            body=from_raw_expr(value["body"]),
        )
    if key == "TableConstructor":
        return TableConstructor(rows=[[from_raw_expr(expr) for expr in row] for row in value])
    if key == "Paren":
        return Paren(expr=from_raw_expr(value))
    if key == "Tuple":
        return Tuple(elements=[from_raw_expr(element) for element in value])
    raise ValueError(f"Unknown expr variant: {key}")


def from_raw_query(raw: Any) -> Query:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid query payload: {raw!r}")
    define_raw = raw.get("define")
    defines = _from_raw_define_block(define_raw) if define_raw is not None else None
    evaluates = [_from_raw_evaluate(stmt) for stmt in raw.get("evaluates", [])]
    return Query(define=defines, evaluates=evaluates)


def from_raw_tokens(raw: Any, *, source: str | None = None) -> list[Token]:
    if not isinstance(raw, Iterable):
        raise ValueError(f"Invalid token list: {raw!r}")
    byte_to_codepoint = _utf8_byte_to_codepoint_offsets(source) if source is not None else None
    return [_from_raw_token(token, byte_to_codepoint=byte_to_codepoint) for token in raw]


def _native_parse_expression(text: str, dialect: Dialect | None = None) -> Any:
    native = _native_module()
    return json.loads(native.parse_expression(text, *(dialect or Dialect()).native_args()))


def _native_parse_query(text: str, dialect: Dialect | None = None) -> Any:
    native = _native_module()
    return json.loads(native.parse_query(text, *(dialect or Dialect()).native_args()))


def _native_lex(text: str, dialect: Dialect | None = None) -> Any:
    native = _native_module()
    return json.loads(native.lex(text, *(dialect or Dialect()).native_args()))


def _native_module():
    try:
        from . import _native
    except Exception as exc:  # pragma: no cover - exercised via import in runtime
        raise RuntimeError("sidemantic_dax native module is not available") from exc
    return _native


def _from_raw_validation_issues(raw: Any) -> list[ValidationIssue]:
    if not isinstance(raw, list):
        raise ValueError(f"Invalid validation issue list: {raw!r}")
    return [
        ValidationIssue(
            code=ValidationCode(item["code"]),
            message=item["message"],
            path=item["path"],
        )
        for item in raw
    ]


def _utf8_byte_to_codepoint_offsets(source: str) -> dict[int, int]:
    offsets = {0: 0}
    byte_offset = 0
    for codepoint_offset, character in enumerate(source, start=1):
        byte_offset += len(character.encode("utf-8"))
        offsets[byte_offset] = codepoint_offset
    return offsets


def _span(raw: Any, *, byte_to_codepoint: dict[int, int] | None = None) -> Span:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid span payload: {raw!r}")
    start = int(raw["start"])
    end = int(raw["end"])
    if byte_to_codepoint is None:
        return Span(start=start, end=end)
    try:
        return Span(start=byte_to_codepoint[start], end=byte_to_codepoint[end])
    except KeyError as exc:
        raise ValueError(f"Span offset is not a UTF-8 character boundary: {exc.args[0]}") from exc


def _from_raw_lossless(raw: Any, *, query: bool) -> LosslessParse:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid lossless parse payload: {raw!r}")
    ast = from_raw_query(raw["ast"]) if query else from_raw_expr(raw["ast"])
    source = raw["source"]
    byte_to_codepoint = _utf8_byte_to_codepoint_offsets(source)
    return LosslessParse(
        ast=ast,
        source=source,
        span=_span(raw["span"], byte_to_codepoint=byte_to_codepoint),
        nodes=[
            AstNodeSpan(
                kind=AstNodeKind(node["kind"]),
                span=_span(node["span"], byte_to_codepoint=byte_to_codepoint),
            )
            for node in raw.get("nodes", [])
        ],
        comments=[
            SourceComment(
                kind=CommentKind(comment["kind"]),
                span=_span(comment["span"], byte_to_codepoint=byte_to_codepoint),
                text=comment["text"],
                previous_node=comment.get("previous_node"),
                next_node=comment.get("next_node"),
                containing_node=comment.get("containing_node"),
            )
            for comment in raw.get("comments", [])
        ],
    )


def _from_raw_recovered_query_item(raw: Any) -> RecoveredQueryItem:
    if not isinstance(raw, dict) or len(raw) != 1:
        raise ValueError(f"Invalid recovered query item: {raw!r}")
    key, value = next(iter(raw.items()))
    if key == "Definition":
        return _from_raw_definition(value)
    if key == "Evaluate":
        return _from_raw_evaluate(value)
    raise ValueError(f"Unknown recovered query item: {key}")


def _from_raw_recovery(raw: Any, *, query: bool, source: str) -> RecoveryResult:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid recovery payload: {raw!r}")
    byte_to_codepoint = _utf8_byte_to_codepoint_offsets(source)
    return RecoveryResult(
        items=[
            RecoveredItem(
                value=(_from_raw_recovered_query_item(item["value"]) if query else from_raw_expr(item["value"])),
                span=_span(item["span"], byte_to_codepoint=byte_to_codepoint),
            )
            for item in raw.get("items", [])
        ],
        diagnostics=[
            RecoveryDiagnostic(
                phase=RecoveryPhase(diagnostic["phase"]),
                message=diagnostic["message"],
                span=_span(diagnostic["span"], byte_to_codepoint=byte_to_codepoint),
            )
            for diagnostic in raw.get("diagnostics", [])
        ],
    )


def _model_json(model: ModelMetadata) -> str:
    return json.dumps(
        {
            "tables": [
                {
                    "name": table.name,
                    "columns": table.columns,
                    "measures": table.measures,
                }
                for table in model.tables
            ]
        }
    )


def _from_raw_model_validation_issues(raw: Any) -> list[ModelValidationIssue]:
    if not isinstance(raw, list):
        raise ValueError(f"Invalid model validation issue list: {raw!r}")
    return [
        ModelValidationIssue(
            code=ModelValidationCode(item["code"]),
            message=item["message"],
            path=item["path"],
        )
        for item in raw
    ]


def _from_raw_table_name(raw: Any) -> TableName:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid table name payload: {raw!r}")
    return TableName(name=raw["name"], quoted=bool(raw["quoted"]))


def _from_raw_var_decl(raw: Any) -> VarDecl:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid var decl payload: {raw!r}")
    return VarDecl(name=raw["name"], expr=from_raw_expr(raw["expr"]))


def _from_raw_func_param(raw: Any) -> FuncParam:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid func param payload: {raw!r}")
    default = raw.get("default")
    return FuncParam(
        name=raw["name"],
        type_hints=list(raw.get("type_hints", [])),
        default=from_raw_expr(default) if default is not None else None,
    )


def _from_raw_visual_shape_column(raw: Any) -> VisualShapeColumn:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid visual shape column payload: {raw!r}")
    return VisualShapeColumn(name=raw["name"])


def _from_raw_visual_shape_group(raw: Any) -> VisualShapeGroup:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid visual shape group payload: {raw!r}")
    return VisualShapeGroup(
        columns=[_from_raw_visual_shape_column(column) for column in raw.get("columns", [])],
        total=_from_raw_visual_shape_column(raw["total"]),
    )


def _from_raw_visual_shape_axis(raw: Any) -> VisualShapeAxis:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid visual shape axis payload: {raw!r}")
    return VisualShapeAxis(
        name=raw["name"],
        groups=[_from_raw_visual_shape_group(group) for group in raw.get("groups", [])],
        order_by=[_from_raw_visual_shape_column(column) for column in raw.get("order_by", [])],
    )


def _from_raw_visual_shape(raw: Any) -> VisualShape:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid visual shape payload: {raw!r}")
    return VisualShape(
        axes=[_from_raw_visual_shape_axis(axis) for axis in raw.get("axes", [])],
        densify=raw.get("densify"),
    )


def _from_raw_define_block(raw: Any) -> DefineBlock:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid define block payload: {raw!r}")
    return DefineBlock(defs=[_from_raw_definition(defn) for defn in raw.get("defs", [])])


def _from_raw_definition(raw: Any) -> Definition:
    if not isinstance(raw, dict) or len(raw) != 1:
        raise ValueError(f"Invalid definition payload: {raw!r}")
    key, value = next(iter(raw.items()))
    if key == "Measure":
        table = _from_raw_table_name(value["table"]) if value.get("table") is not None else None
        return MeasureDef(doc=value.get("doc"), table=table, name=value["name"], expr=from_raw_expr(value["expr"]))
    if key == "Var":
        return VarDef(doc=value.get("doc"), name=value["name"], expr=from_raw_expr(value["expr"]))
    if key == "Table":
        visual_shape = value.get("visual_shape")
        return TableDef(
            doc=value.get("doc"),
            name=value["name"],
            expr=from_raw_expr(value["expr"]),
            visual_shape=_from_raw_visual_shape(visual_shape) if visual_shape is not None else None,
        )
    if key == "Column":
        table = _from_raw_table_name(value["table"]) if value.get("table") is not None else None
        return ColumnDef(
            doc=value.get("doc"),
            table=table,
            name=value["name"],
            expr=from_raw_expr(value["expr"]),
        )
    if key == "Function":
        params = [_from_raw_func_param(param) for param in value.get("params", [])]
        return FunctionDef(
            doc=value.get("doc"),
            name=value["name"],
            params=params,
            body=from_raw_expr(value["body"]),
        )
    raise ValueError(f"Unknown definition variant: {key}")


def _from_raw_evaluate(raw: Any) -> EvaluateStmt:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid evaluate payload: {raw!r}")
    order_by = [_from_raw_order_key(key) for key in raw.get("order_by", [])]
    start_at = raw.get("start_at")
    parsed_start_at = [from_raw_expr(expr) for expr in start_at] if start_at is not None else None
    return EvaluateStmt(expr=from_raw_expr(raw["expr"]), order_by=order_by, start_at=parsed_start_at)


def _from_raw_order_key(raw: Any) -> OrderKey:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid order key payload: {raw!r}")
    return OrderKey(expr=from_raw_expr(raw["expr"]), direction=_to_sort_direction(raw["direction"]))


def _from_raw_token(raw: Any, *, byte_to_codepoint: dict[int, int] | None = None) -> Token:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid token payload: {raw!r}")
    return Token(
        kind=_from_raw_token_kind(raw["kind"]),
        span=_from_raw_span(raw["span"], byte_to_codepoint=byte_to_codepoint),
    )


def _from_raw_span(raw: Any, *, byte_to_codepoint: dict[int, int] | None = None) -> Span:
    return _span(raw, byte_to_codepoint=byte_to_codepoint)


def _from_raw_token_kind(raw: Any) -> TokenKind:
    if isinstance(raw, str):
        return _unit_token_kind(raw)
    if not isinstance(raw, dict) or len(raw) != 1:
        raise ValueError(f"Invalid token kind payload: {raw!r}")
    key, value = next(iter(raw.items()))
    if key == "DocComment":
        return DocCommentToken(value=value)
    if key == "Param":
        return ParamToken(value=value)
    if key == "Ident":
        return IdentToken(value=value)
    if key == "Number":
        return NumberToken(value=value)
    if key == "String":
        return StringToken(value=value)
    if key == "DateTime":
        return DateTimeToken(value=value)
    if key == "QuotedIdent":
        return QuotedIdentToken(value=value)
    if key == "BracketIdent":
        return BracketIdentToken(value=value)
    return _unit_token_kind(key)


def _unit_token_kind(name: str) -> TokenKind:
    mapping: dict[str, TokenKind] = {
        "LParen": LParen(),
        "RParen": RParen(),
        "LBrace": LBrace(),
        "RBrace": RBrace(),
        "Comma": Comma(),
        "Semicolon": Semicolon(),
        "Colon": Colon(),
        "Arrow": Arrow(),
        "Plus": Plus(),
        "Minus": Minus(),
        "Star": Star(),
        "Slash": Slash(),
        "Caret": Caret(),
        "Amp": Amp(),
        "Eq": Eq(),
        "EqEq": EqEq(),
        "Neq": Neq(),
        "Lt": Lt(),
        "Lte": Lte(),
        "Gt": Gt(),
        "Gte": Gte(),
        "Dot": Dot(),
        "AndAnd": AndAnd(),
        "OrOr": OrOr(),
        "Eof": Eof(),
    }
    if name in mapping:
        return mapping[name]
    raise ValueError(f"Unknown token kind: {name}")


def _to_unary_op(raw: Any) -> UnaryOp:
    if isinstance(raw, UnaryOp):
        return raw
    return UnaryOp(raw)


def _to_binary_op(raw: Any) -> BinaryOp:
    if isinstance(raw, BinaryOp):
        return raw
    return BinaryOp(raw)


def _to_sort_direction(raw: Any) -> SortDirection:
    if isinstance(raw, SortDirection):
        return raw
    return SortDirection(raw)


__all__ = [
    "Amp",
    "AndAnd",
    "Arrow",
    "AstNodeKind",
    "AstNodeSpan",
    "Binary",
    "BinaryOp",
    "Blank",
    "Boolean",
    "BracketIdentToken",
    "BracketRef",
    "Caret",
    "Colon",
    "ColumnDef",
    "Comma",
    "CommentKind",
    "DataTable",
    "DataTableColumn",
    "DataTableType",
    "DefineBlock",
    "Definition",
    "Dialect",
    "DateTime",
    "DateTimeToken",
    "DocCommentToken",
    "Dot",
    "Eof",
    "Eq",
    "EqEq",
    "EvaluateStmt",
    "Expr",
    "FuncParam",
    "FunctionDef",
    "FunctionCall",
    "Gt",
    "Gte",
    "HierarchyRef",
    "IdentToken",
    "Identifier",
    "LBrace",
    "LParen",
    "Lt",
    "Lte",
    "LosslessParse",
    "MeasureDef",
    "ModelMetadata",
    "ModelTable",
    "ModelValidationCode",
    "ModelValidationIssue",
    "Minus",
    "Neq",
    "Number",
    "NumberToken",
    "Omitted",
    "OrOr",
    "OrderKey",
    "Paren",
    "Parameter",
    "ParamToken",
    "Plus",
    "Query",
    "QuotedIdentToken",
    "RBrace",
    "RecoveredItem",
    "RecoveredQueryItem",
    "RecoveryDiagnostic",
    "RecoveryPhase",
    "RecoveryResult",
    "RParen",
    "Semicolon",
    "Slash",
    "SortDirection",
    "SourceComment",
    "Span",
    "Star",
    "String",
    "StringToken",
    "TableColumnRef",
    "TableConstructor",
    "TableDef",
    "TableName",
    "TableRef",
    "Token",
    "TokenKind",
    "Tuple",
    "Unary",
    "UnaryOp",
    "ValidationCode",
    "ValidationIssue",
    "VarBlock",
    "VarDecl",
    "VarDef",
    "VisualShape",
    "VisualShapeAxis",
    "VisualShapeColumn",
    "VisualShapeGroup",
    "from_raw_expr",
    "from_raw_query",
    "from_raw_tokens",
    "format_expression",
    "format_query",
    "lex",
    "parse_expression",
    "parse_expression_lossless",
    "parse_query",
    "parse_query_lossless",
    "recover_expression",
    "recover_query",
    "validate_expression",
    "validate_expression_against_model",
    "validate_query",
    "validate_query_against_model",
]
