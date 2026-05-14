from __future__ import annotations

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
class Boolean:
    value: bool


@dataclass(frozen=True, slots=True)
class Blank:
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


Expr: TypeAlias = (
    Number
    | String
    | Boolean
    | Blank
    | Parameter
    | Identifier
    | TableRef
    | BracketRef
    | TableColumnRef
    | HierarchyRef
    | FunctionCall
    | Unary
    | Binary
    | VarBlock
    | TableConstructor
    | Paren
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


def parse_expression(text: str) -> Expr:
    raw = _native_parse_expression(text)
    return from_raw_expr(raw)


def parse_query(text: str) -> Query:
    raw = _native_parse_query(text)
    return from_raw_query(raw)


def lex(text: str) -> list[Token]:
    raw = _native_lex(text)
    return from_raw_tokens(raw)


def from_raw_expr(raw: Any) -> Expr:
    if isinstance(raw, str):
        if raw == "Blank":
            return Blank()
        raise ValueError(f"Unexpected expr variant: {raw}")
    if not isinstance(raw, dict) or len(raw) != 1:
        raise ValueError(f"Invalid expr payload: {raw!r}")

    key, value = next(iter(raw.items()))
    if key == "Number":
        return Number(value=value)
    if key == "String":
        return String(value=value)
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
    raise ValueError(f"Unknown expr variant: {key}")


def from_raw_query(raw: Any) -> Query:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid query payload: {raw!r}")
    define_raw = raw.get("define")
    defines = _from_raw_define_block(define_raw) if define_raw is not None else None
    evaluates = [_from_raw_evaluate(stmt) for stmt in raw.get("evaluates", [])]
    return Query(define=defines, evaluates=evaluates)


def from_raw_tokens(raw: Any) -> list[Token]:
    if not isinstance(raw, Iterable):
        raise ValueError(f"Invalid token list: {raw!r}")
    return [_from_raw_token(token) for token in raw]


def _native_parse_expression(text: str) -> Any:
    native = _native_module()
    return native.parse_expression(text)


def _native_parse_query(text: str) -> Any:
    native = _native_module()
    return native.parse_query(text)


def _native_lex(text: str) -> Any:
    native = _native_module()
    return native.lex(text)


def _native_module():
    try:
        from . import _native
    except Exception as exc:  # pragma: no cover - exercised via import in runtime
        raise RuntimeError("sidemantic_dax native module is not available") from exc
    return _native


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
    return FuncParam(name=raw["name"], type_hints=list(raw.get("type_hints", [])))


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
        return TableDef(doc=value.get("doc"), name=value["name"], expr=from_raw_expr(value["expr"]))
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


def _from_raw_token(raw: Any) -> Token:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid token payload: {raw!r}")
    return Token(kind=_from_raw_token_kind(raw["kind"]), span=_from_raw_span(raw["span"]))


def _from_raw_span(raw: Any) -> Span:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid span payload: {raw!r}")
    return Span(start=int(raw["start"]), end=int(raw["end"]))


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
    "DefineBlock",
    "Definition",
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
    "MeasureDef",
    "Minus",
    "Neq",
    "Number",
    "NumberToken",
    "OrOr",
    "OrderKey",
    "Paren",
    "Parameter",
    "ParamToken",
    "Plus",
    "Query",
    "QuotedIdentToken",
    "RBrace",
    "RParen",
    "Semicolon",
    "Slash",
    "SortDirection",
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
    "Unary",
    "UnaryOp",
    "VarBlock",
    "VarDecl",
    "VarDef",
    "from_raw_expr",
    "from_raw_query",
    "from_raw_tokens",
    "lex",
    "parse_expression",
    "parse_query",
]
