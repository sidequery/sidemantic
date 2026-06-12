"""Parser for Power BI Tabular Model Definition Language (TMDL)."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TmdlLocation:
    file: str | None
    line: int
    column: int


@dataclass
class TmdlExpression:
    text: str
    meta: dict[str, Any] | None = None
    meta_raw: str | None = field(default=None, compare=False)
    is_block: bool = False
    block_delimiter: str | None = field(default=None, compare=False)


@dataclass
class TmdlProperty:
    name: str
    value: Any
    kind: str
    raw: str | None = field(default=None, compare=False)


@dataclass
class TmdlNode:
    type: str
    name: str | None
    name_raw: str | None = field(default=None, compare=False)
    is_ref: bool = False
    properties: list[TmdlProperty] = field(default_factory=list)
    children: list[TmdlNode] = field(default_factory=list)
    default_property: TmdlExpression | None = None
    description: str | None = None
    leading_comments: list[str] = field(default_factory=list, compare=False)
    location: TmdlLocation | None = None

    def property(self, name: str) -> TmdlProperty | None:
        for prop in self.properties:
            if prop.name == name:
                return prop
        return None

    def property_value(self, name: str) -> Any:
        prop = self.property(name)
        return prop.value if prop else None

    def child_nodes(self, type_name: str) -> list[TmdlNode]:
        return [child for child in self.children if child.type == type_name]


@dataclass
class TmdlDocument:
    nodes: list[TmdlNode]
    file: str | None = None


class TmdlParseError(ValueError):
    def __init__(self, message: str, location: TmdlLocation | None = None):
        if location:
            super().__init__(f"{message} ({location.file or '<input>'}:{location.line}:{location.column})")
        else:
            super().__init__(message)
        self.location = location


@dataclass
class _IndentConfig:
    kind: str
    width: int


@dataclass
class _LineInfo:
    raw: str
    content: str
    indent: int
    indent_width: int
    lineno: int
    is_blank: bool
    is_comment: bool
    is_description: bool


_IDENTIFIER_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"


class TmdlParser:
    def parse(self, text: str, file: str | None = None) -> TmdlDocument:
        lines, indent_config = _prepare_lines(text, file)
        parser = _TmdlParser(lines, indent_config, file)
        nodes = parser.parse_nodes(0)
        return TmdlDocument(nodes=nodes, file=file)


class _TmdlParser:
    def __init__(self, lines: list[_LineInfo], indent_config: _IndentConfig | None, file: str | None):
        self.lines = lines
        self.index = 0
        self.indent_config = indent_config
        self.file = file

    def parse_nodes(self, indent_level: int) -> list[TmdlNode]:
        nodes: list[TmdlNode] = []
        pending_description: str | None = None
        pending_comments: list[str] = []

        while self.index < len(self.lines):
            line = self.lines[self.index]

            if line.is_blank:
                pending_description = None
                pending_comments = []
                self.index += 1
                continue

            if line.indent < indent_level:
                break

            if line.indent > indent_level:
                raise self._error("Unexpected indentation", line)

            if line.is_comment:
                pending_comments.append(line.content)
                self.index += 1
                continue

            if line.is_description:
                pending_description = self._collect_description(indent_level)
                continue

            node = self._parse_object(indent_level, pending_description, pending_comments)
            pending_description = None
            pending_comments = []
            nodes.append(node)

        return nodes

    def _collect_description(self, indent_level: int) -> str:
        parts: list[str] = []
        while self.index < len(self.lines):
            line = self.lines[self.index]
            if line.is_description and line.indent == indent_level:
                parts.append(line.content[3:].lstrip())
                self.index += 1
                continue
            break
        return "\n".join(parts)

    def _parse_object(self, indent_level: int, description: str | None, leading_comments: list[str]) -> TmdlNode:
        line = self.lines[self.index]
        is_ref, obj_type, name, name_raw, expr_text, expr_meta, expr_meta_raw = _parse_object_declaration(
            line,
            self.file,
        )
        node = TmdlNode(
            type=obj_type,
            name=name,
            name_raw=name_raw,
            is_ref=is_ref,
            description=description,
            leading_comments=list(leading_comments),
            location=TmdlLocation(self.file, line.lineno, line.indent_width + 1),
        )
        self.index += 1

        if expr_text is not None:
            node.default_property = self._parse_expression_from_inline(
                expr_text,
                indent_level,
                expr_meta,
                expr_meta_raw,
                stop_before_trailing_properties=True,
            )
            if obj_type.lower() == "expression" and expr_text.strip().lower() in {"m", "dax"}:
                body_expression = self._parse_expression_block(indent_level, stop_before_trailing_properties=True)
                if body_expression.text:
                    node.default_property = body_expression

        properties, children = self._parse_object_body(indent_level + 1)
        node.properties = properties
        node.children = children
        return node

    def _has_nested_body(self, indent_level: int) -> bool:
        line = self.lines[self.index]
        if _split_property_line(line.content)[1] is not None:
            return False

        next_index = self.index + 1
        while next_index < len(self.lines):
            next_line = self.lines[next_index]
            if next_line.is_blank or next_line.is_comment or next_line.is_description:
                next_index += 1
                continue
            return next_line.indent > indent_level
        return False

    def _parse_object_body(self, indent_level: int) -> tuple[list[TmdlProperty], list[TmdlNode]]:
        properties: list[TmdlProperty] = []
        children: list[TmdlNode] = []
        pending_description: str | None = None
        pending_comments: list[str] = []

        while self.index < len(self.lines):
            line = self.lines[self.index]

            if line.is_blank:
                pending_description = None
                pending_comments = []
                self.index += 1
                continue

            if line.indent < indent_level:
                break

            if line.indent > indent_level:
                raise self._error("Unexpected indentation", line)

            if line.is_comment:
                pending_comments.append(line.content)
                self.index += 1
                continue

            if line.is_description:
                pending_description = self._collect_description(indent_level)
                continue

            if _is_object_declaration(line.content) or self._has_nested_body(indent_level):
                child = self._parse_object(indent_level, pending_description, pending_comments)
                children.append(child)
                pending_description = None
                pending_comments = []
                continue

            properties.append(self._parse_property(indent_level))
            pending_description = None
            pending_comments = []

        return properties, children

    def _parse_property(self, indent_level: int) -> TmdlProperty:
        line = self.lines[self.index]
        name, sep, remainder = _split_property_line(line.content)
        self.index += 1

        if sep is None:
            return TmdlProperty(name=name, value=True, kind="value")

        if sep == ":":
            value = _parse_value(remainder)
            return TmdlProperty(name=name, value=value, kind="value", raw=remainder)

        expr_text, meta, meta_raw = _split_meta(remainder)
        expression = self._parse_expression_from_inline(expr_text, indent_level, meta, meta_raw)
        return TmdlProperty(name=name, value=expression, kind="expression")

    def _parse_expression_from_inline(
        self,
        expr_text: str,
        base_indent: int,
        meta: dict[str, Any] | None,
        meta_raw: str | None = None,
        stop_before_trailing_properties: bool = False,
    ) -> TmdlExpression:
        expr_text = expr_text.strip()
        if expr_text == "":
            expression = self._parse_expression_block(base_indent, stop_before_trailing_properties)
        elif expr_text == "```":
            expression = self._parse_backtick_block(opening_consumed=True)
        elif expr_text.startswith("```") and expr_text.endswith("```") and len(expr_text) > 6:
            expression = TmdlExpression(text=expr_text[3:-3], is_block=False)
        else:
            expression = TmdlExpression(text=expr_text, is_block=False)

        if meta is not None:
            expression.meta = meta
        if meta_raw is not None:
            expression.meta_raw = meta_raw
        return expression

    def _parse_expression_block(
        self, base_indent: int, stop_before_trailing_properties: bool = False
    ) -> TmdlExpression:
        if self.index >= len(self.lines):
            raise TmdlParseError("Expected expression block", None)

        while self.index < len(self.lines) and self.lines[self.index].is_blank:
            self.index += 1
        if self.index >= len(self.lines):
            raise TmdlParseError("Expected expression block", None)

        first = self.lines[self.index]
        if first.content.strip() == "```":
            return self._parse_backtick_block(opening_consumed=False)

        expression_lines: list[str] = []
        while self.index < len(self.lines):
            line = self.lines[self.index]
            if line.indent <= base_indent:
                break
            if (
                stop_before_trailing_properties
                and expression_lines
                and line.indent == base_indent + 1
                and _looks_like_block_trailing_property(line.content)
            ):
                break
            expression_lines.append(_strip_indent(line.raw, base_indent + 1, self.indent_config))
            self.index += 1

        if not expression_lines:
            raise self._error("Expected expression block", first)

        return TmdlExpression(text="\n".join(expression_lines), is_block=True)

    def _parse_backtick_block(self, opening_consumed: bool) -> TmdlExpression:
        opening_line: _LineInfo | None = None
        if opening_consumed:
            if self.index > 0:
                opening_line = self.lines[self.index - 1]
        else:
            if self.index >= len(self.lines):
                raise TmdlParseError("Expected expression block", None)
            opening_line = self.lines[self.index]
            self.index += 1
        block_lines: list[str] = []
        while self.index < len(self.lines):
            current = self.lines[self.index]
            if current.content.strip() == "```":
                self.index += 1
                return TmdlExpression(
                    text="\n".join(_strip_common_indent(block_lines)),
                    is_block=True,
                    block_delimiter="```",
                )
            block_lines.append(current.raw)
            self.index += 1

        if opening_line is not None:
            raise self._error("Unterminated backtick expression block", opening_line)
        raise TmdlParseError("Unterminated backtick expression block", None)

    def _error(self, message: str, line: _LineInfo) -> TmdlParseError:
        return TmdlParseError(message, TmdlLocation(self.file, line.lineno, line.indent_width + 1))


def _prepare_lines(text: str, file: str | None) -> tuple[list[_LineInfo], _IndentConfig | None]:
    raw_lines = text.splitlines()
    indent_config = _detect_indent(raw_lines)
    lines: list[_LineInfo] = []

    for lineno, raw in enumerate(raw_lines, start=1):
        if lineno == 1:
            raw = raw.lstrip("\ufeff")
        stripped, indent_width = _split_indent(raw)
        indent = _indent_level(indent_width, indent_config, raw, file, lineno)
        content = stripped
        is_blank = content.strip() == ""
        is_comment = content.startswith("//") or (content.startswith("#") and not content.startswith("###"))
        is_description = content.startswith("///")
        lines.append(
            _LineInfo(
                raw=raw,
                content=content,
                indent=indent,
                indent_width=indent_width,
                lineno=lineno,
                is_blank=is_blank,
                is_comment=is_comment and not is_description,
                is_description=is_description,
            )
        )

    return lines, indent_config


def _detect_indent(lines: list[str]) -> _IndentConfig | None:
    for raw in lines:
        raw = raw.lstrip("\ufeff")
        stripped, indent_width = _split_indent(raw)
        if indent_width == 0:
            continue
        if stripped.strip() == "":
            continue
        if raw[:indent_width].count("\t") and raw[:indent_width].count(" "):
            raise TmdlParseError("Mixed tabs and spaces in indentation", None)
        if "\t" in raw[:indent_width]:
            return _IndentConfig(kind="tabs", width=1)
        return _IndentConfig(kind="spaces", width=indent_width)
    return None


def _split_indent(raw: str) -> tuple[str, int]:
    stripped = raw.lstrip(" \t")
    return stripped, len(raw) - len(stripped)


def _indent_level(indent_width: int, config: _IndentConfig | None, raw: str, file: str | None, lineno: int) -> int:
    if indent_width == 0:
        return 0
    if config is None:
        return 0
    leading = raw[:indent_width]
    if config.kind == "tabs":
        tab_count = len(leading) - len(leading.lstrip("\t"))
        if tab_count:
            return tab_count
        return max(1, (indent_width + 3) // 4)

    if "\t" in leading:
        indent_width = len(leading.expandtabs(config.width))
    return max(1, (indent_width + config.width - 1) // config.width)


def _is_object_declaration(content: str) -> bool:
    stripped = content.strip()
    if stripped.lower().startswith("createorreplace"):
        return True
    if stripped.lower().startswith("ref "):
        return True

    first, remainder = _split_first_token(stripped)
    if ":" in first or "=" in first:
        return False
    if not remainder:
        return False
    remainder = remainder.lstrip()
    if not remainder:
        return False
    if remainder.startswith(":") or remainder.startswith("="):
        return False
    return True


def _looks_like_block_trailing_property(content: str) -> bool:
    name, sep, _remainder = _split_property_line(content)
    return sep == ":" and bool(name.strip())


def _parse_object_declaration(
    line: _LineInfo, file: str | None
) -> tuple[bool, str, str | None, str | None, str | None, dict[str, Any] | None, str | None]:
    content = line.content.strip()
    is_ref = False
    if content.lower().startswith("ref "):
        is_ref = True
        content = content[4:].lstrip()

    obj_type, remainder = _split_first_token(content)
    if obj_type == "":
        raise TmdlParseError("Missing object type", TmdlLocation(file, line.lineno, line.indent_width + 1))

    if obj_type.lower() == "createorreplace":
        return is_ref, obj_type, None, None, None, None, None

    name, remainder, name_raw = _parse_identifier(remainder.strip())
    if not name:
        if remainder.strip() == "":
            return is_ref, obj_type, None, None, None, None, None
        raise TmdlParseError("Missing object name", TmdlLocation(file, line.lineno, line.indent_width + 1))

    expr_text = None
    expr_meta = None
    expr_meta_raw = None

    if remainder:
        eq_index = _find_unquoted(remainder, "=")
        if eq_index != -1:
            expr_raw = remainder[eq_index + 1 :].strip()
            expr_text, expr_meta, expr_meta_raw = _split_meta(expr_raw)
        elif remainder.strip():
            raise TmdlParseError(
                "Unexpected tokens after object name", TmdlLocation(file, line.lineno, line.indent_width + 1)
            )

    return is_ref, obj_type, name, name_raw, expr_text, expr_meta, expr_meta_raw


def _split_first_token(text: str) -> tuple[str, str]:
    text = text.lstrip()
    if not text:
        return "", ""
    for i, ch in enumerate(text):
        if ch.isspace():
            return text[:i], text[i:]
    return text, ""


def _parse_identifier(text: str) -> tuple[str, str, str]:
    text = text.lstrip()
    if not text:
        return "", "", ""

    if text[0] in ("'", '"'):
        token, remainder, raw = _parse_quoted(text)
        return token, remainder, raw

    token = []
    for i, ch in enumerate(text):
        if ch.isspace():
            return "".join(token), text[i:], text[:i]
        token.append(ch)
    return "".join(token), "", text


def _parse_quoted(text: str) -> tuple[str, str, str]:
    quote = text[0]
    token = []
    idx = 1
    while idx < len(text):
        char = text[idx]
        if char == quote:
            if idx + 1 < len(text) and text[idx + 1] == quote:
                token.append(quote)
                idx += 2
                continue
            return "".join(token), text[idx + 1 :], text[: idx + 1]
        token.append(char)
        idx += 1
    return "".join(token), "", text


def _split_property_line(content: str) -> tuple[str, str | None, str]:
    content = content.strip()
    sep_index = _find_unquoted(content, ":=")
    if sep_index == -1:
        return content, None, ""

    sep = content[sep_index]
    name = content[:sep_index].strip()
    remainder = content[sep_index + 1 :].strip()
    return name, sep, remainder


def _find_unquoted(text: str, targets: str) -> int:
    in_single = False
    in_double = False
    in_backtick = False
    idx = 0
    while idx < len(text):
        if text.startswith("```", idx):
            in_backtick = not in_backtick
            idx += 3
            continue
        char = text[idx]
        if not in_double and char == "'":
            if in_single and idx + 1 < len(text) and text[idx + 1] == "'":
                idx += 2
                continue
            in_single = not in_single
            idx += 1
            continue
        if not in_single and char == '"':
            if in_double and idx + 1 < len(text) and text[idx + 1] == '"':
                idx += 2
                continue
            in_double = not in_double
            idx += 1
            continue
        if not in_single and not in_double and not in_backtick and char in targets:
            return idx
        idx += 1
    return -1


def _split_meta(text: str) -> tuple[str, dict[str, Any] | None, str | None]:
    if not text:
        return "", None, None

    meta_index = _find_meta(text)
    if meta_index == -1:
        return text.strip(), None, None

    expr = text[:meta_index].strip()
    meta_text = text[meta_index:].strip()
    meta = _parse_meta(meta_text)
    meta_raw = _extract_meta_raw(meta_text)
    return expr, meta, meta_raw


def _find_meta(text: str) -> int:
    in_single = False
    in_double = False
    in_backtick = False
    idx = 0
    while idx < len(text):
        if text.startswith("```", idx):
            in_backtick = not in_backtick
            idx += 3
            continue
        char = text[idx]
        if not in_double and char == "'":
            if in_single and idx + 1 < len(text) and text[idx + 1] == "'":
                idx += 2
                continue
            in_single = not in_single
            idx += 1
            continue
        if not in_single and char == '"':
            if in_double and idx + 1 < len(text) and text[idx + 1] == '"':
                idx += 2
                continue
            in_double = not in_double
            idx += 1
            continue
        if not in_single and not in_double and not in_backtick:
            if text[idx:].lower().startswith("meta ["):
                return idx
        idx += 1
    return -1


def _parse_meta(text: str) -> dict[str, Any]:
    text = text.strip()
    if not text.lower().startswith("meta"):
        return {}
    bracket_start = text.find("[")
    bracket_end = text.rfind("]")
    if bracket_start == -1 or bracket_end == -1 or bracket_end <= bracket_start:
        return {}
    content = text[bracket_start + 1 : bracket_end].strip()
    if not content:
        return {}
    entries = _split_unquoted(content, ",")
    meta: dict[str, Any] = {}
    for entry in entries:
        if "=" not in entry:
            continue
        key, value = entry.split("=", 1)
        meta[key.strip()] = _parse_value(value.strip())
    return meta


def _extract_meta_raw(text: str) -> str | None:
    bracket_start = text.find("[")
    bracket_end = text.rfind("]")
    if bracket_start == -1 or bracket_end == -1 or bracket_end <= bracket_start:
        return None
    return text[bracket_start + 1 : bracket_end]


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


def _parse_value(raw: str) -> Any:
    if raw == "":
        return ""
    lower = raw.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    if raw[0] in ("'", '"') and raw[-1] == raw[0]:
        token, remainder, _raw = _parse_quoted(raw)
        if remainder.strip() == "":
            return token
    return raw


def _strip_indent(raw: str, level: int, config: _IndentConfig | None) -> str:
    if level <= 0:
        return raw
    if config is None:
        return raw.lstrip(" \t")
    if config.kind == "tabs":
        prefix = "\t" * level
    else:
        prefix = " " * (config.width * level)
    if raw.startswith(prefix):
        return raw[len(prefix) :]
    return raw.lstrip(" \t")


def _strip_common_indent(lines: list[str]) -> list[str]:
    min_indent: int | None = None
    for line in lines:
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" \t"))
        if min_indent is None or indent < min_indent:
            min_indent = indent

    if min_indent is None or min_indent <= 0:
        return lines

    normalized: list[str] = []
    for line in lines:
        if not line.strip():
            normalized.append("")
            continue
        normalized.append(line[min_indent:])
    return normalized


def merge_documents(documents: Iterable[TmdlDocument]) -> list[TmdlNode]:
    def merge_into(scope: list[TmdlNode], incoming: TmdlNode) -> TmdlNode:
        existing = next((node for node in scope if node.type == incoming.type and node.name == incoming.name), None)
        if not existing:
            scope.append(incoming)
            return incoming

        existing.is_ref = existing.is_ref and incoming.is_ref
        if not existing.name_raw and incoming.name_raw:
            existing.name_raw = incoming.name_raw
        if incoming.leading_comments and not existing.leading_comments:
            existing.leading_comments = list(incoming.leading_comments)
        if incoming.description:
            if existing.description and existing.description != incoming.description:
                raise TmdlParseError("Conflicting descriptions while merging", incoming.location)
            existing.description = incoming.description
        if incoming.default_property:
            if existing.default_property and existing.default_property.text != incoming.default_property.text:
                raise TmdlParseError("Conflicting default properties while merging", incoming.location)
            existing.default_property = incoming.default_property

        for prop in incoming.properties:
            existing_prop = existing.property(prop.name)
            if existing_prop:
                if existing_prop.value != prop.value:
                    raise TmdlParseError("Conflicting properties while merging", incoming.location)
                continue
            existing.properties.append(prop)

        for child in incoming.children:
            merge_into(existing.children, child)

        return existing

    roots: list[TmdlNode] = []
    for doc in documents:
        for node in doc.nodes:
            merge_into(roots, node)

    return roots
