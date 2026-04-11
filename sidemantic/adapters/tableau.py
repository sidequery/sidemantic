"""Tableau adapter for importing Tableau .tds/.twb/.tdsx/.twbx datasource definitions."""

import re
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from pathlib import Path

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment
from sidemantic.core.semantic_graph import SemanticGraph

# --- Type mapping ---
_DATATYPE_MAP: dict[str, str] = {
    "string": "categorical",
    "integer": "numeric",
    "real": "numeric",
    "date": "time",
    "datetime": "time",
    "boolean": "boolean",
}

_DATATYPE_GRANULARITY: dict[str, str] = {
    "date": "day",
    "datetime": "hour",
}

# --- Aggregation mapping (case-insensitive via .lower()) ---
_AGGREGATION_MAP: dict[str, str] = {
    "sum": "sum",
    "avg": "avg",
    "count": "count",
    "countd": "count_distinct",
    "min": "min",
    "max": "max",
    "median": "median",
}

_PASSTHROUGH_AGGS: set[str] = {"attr", "none", "user"}

# --- Formula patterns ---
_FIELD_REF_RE = re.compile(r"\[([^\]]+)\]")
_LOD_RE = re.compile(r"\{\s*(?:FIXED|INCLUDE|EXCLUDE)\b", re.IGNORECASE)
_TABLE_CALC_FUNCS: set[str] = {
    "RUNNING_SUM",
    "RUNNING_AVG",
    "RUNNING_COUNT",
    "RUNNING_MIN",
    "RUNNING_MAX",
    "LOOKUP",
    "INDEX",
    "FIRST",
    "LAST",
    "SIZE",
    "WINDOW_SUM",
    "WINDOW_AVG",
    "WINDOW_MIN",
    "WINDOW_MAX",
    "WINDOW_COUNT",
    "WINDOW_MEDIAN",
    "WINDOW_STDEV",
    "WINDOW_VAR",
    "PREVIOUS_VALUE",
    "RANK",
    "RANK_DENSE",
    "RANK_MODIFIED",
    "RANK_PERCENTILE",
    "RANK_UNIQUE",
}

# Regex for function calls: FUNC_NAME(...)
_FUNC_CALL_RE = re.compile(r"\b([A-Z_]+)\s*\(", re.IGNORECASE)

# --- Formula replacement patterns ---
# Each is (pattern, replacement_func_or_str)
_ZN_RE = re.compile(r"\bZN\s*\(", re.IGNORECASE)
_IFNULL_RE = re.compile(r"\bIFNULL\s*\(", re.IGNORECASE)
_IIF_RE = re.compile(r"\bIIF\s*\(", re.IGNORECASE)
_IF_THEN_RE = re.compile(
    r"\bIF\s+(.+?)\s+THEN\s+(.+?)(?:\s+ELSEIF\s+(.+?)\s+THEN\s+(.+?))*\s+(?:ELSE\s+(.+?)\s+)?END\b",
    re.IGNORECASE | re.DOTALL,
)
_CONTAINS_RE = re.compile(r"\bCONTAINS\s*\(", re.IGNORECASE)
_DATETRUNC_RE = re.compile(r"\bDATETRUNC\s*\(", re.IGNORECASE)
_COUNTD_RE = re.compile(r"\bCOUNTD\s*\(", re.IGNORECASE)
_LEN_RE = re.compile(r"\bLEN\s*\(", re.IGNORECASE)
_ISNULL_RE = re.compile(r"\bISNULL\s*\(", re.IGNORECASE)
_COMMENT_RE = re.compile(r"//[^\n]*", re.MULTILINE)
_DATEADD_RE = re.compile(r"\bDATEADD\s*\(", re.IGNORECASE)
_MID_RE = re.compile(r"\bMID\s*\(", re.IGNORECASE)
_FIND_RE = re.compile(r"\bFIND\s*\(", re.IGNORECASE)
_SIMPLE_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Simple function renames (Tableau name -> SQL name)
_SIMPLE_RENAMES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bMID\s*\(", re.IGNORECASE), "SUBSTRING("),
    (re.compile(r"\bFIND\s*\(", re.IGNORECASE), "STRPOS("),
    (re.compile(r"\bSTARTSWITH\s*\(", re.IGNORECASE), "STARTS_WITH("),
    (re.compile(r"\bENDSWITH\s*\(", re.IGNORECASE), "ENDS_WITH("),
    (re.compile(r"\bCHAR\s*\(", re.IGNORECASE), "CHR("),
    (re.compile(r"\bMAKEDATE\s*\(", re.IGNORECASE), "MAKE_DATE("),
    (re.compile(r"\bMAKETIME\s*\(", re.IGNORECASE), "MAKE_TIME("),
    (re.compile(r"\bMAKEDATETIME\s*\(", re.IGNORECASE), "MAKE_TIMESTAMP("),
]

# Tableau-specific functions that need balanced-paren-aware wrapping
_TABLEAU_CAST_FUNCS: dict[str, str] = {
    "INT": "CAST({arg} AS INTEGER)",
    "FLOAT": "CAST({arg} AS DOUBLE)",
    "STR": "CAST({arg} AS VARCHAR)",
}

# Regex to detect Tableau-only functions that have no SQL equivalent
_TABLEAU_ONLY_FUNCS: set[str] = {
    "ISMEMBEROF",
    "USERNAME",
    "USERDOMAIN",
    "FULLNAME",
    "ISFULLDATETIME",
    "RAWSQLAGG_REAL",
    "RAWSQLAGG_STR",
    "RAWSQL_REAL",
    "RAWSQL_STR",
    "RAWSQL_INT",
    "RAWSQL_BOOL",
    "RAWSQL_DATE",
    "RAWSQL_DATETIME",
}


def _has_lod_or_table_calc(formula: str) -> bool:
    """Check if formula contains LOD expressions or table calculations."""
    if _LOD_RE.search(formula):
        return True
    for match in _FUNC_CALL_RE.finditer(formula):
        func_name = match.group(1).upper()
        if func_name in _TABLE_CALC_FUNCS or func_name in _TABLEAU_ONLY_FUNCS:
            return True
    return False


def _find_matching_paren(s: str, open_pos: int) -> int:
    """Find the position of the matching closing paren, handling nesting.

    Args:
        s: The string to search in
        open_pos: Position of the opening '('

    Returns:
        Position of the matching ')' or -1 if not found
    """
    depth = 0
    in_string = False
    string_char = None
    i = open_pos
    while i < len(s):
        c = s[i]
        if in_string:
            if c == string_char:
                # Check for doubled-quote escape ('' or "")
                if i + 1 < len(s) and s[i + 1] == string_char:
                    i += 2  # Skip the escaped pair
                    continue
                in_string = False
        elif c in ("'", '"'):
            in_string = True
            string_char = c
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def _replace_func_balanced(text: str, func_re: re.Pattern, template: str) -> str:
    """Replace a function call using balanced-paren matching.

    template uses {arg} for the extracted argument.
    """
    result = text
    offset = 0
    for m in func_re.finditer(text):
        start = m.start() + offset
        open_paren = start + len(m.group(0)) - 1  # position of '('
        adjusted = result
        close_paren = _find_matching_paren(adjusted, open_paren)
        if close_paren == -1:
            continue
        arg = adjusted[open_paren + 1 : close_paren].strip()
        replacement = template.format(arg=arg)
        result = adjusted[:start] + replacement + adjusted[close_paren + 1 :]
        offset = len(result) - len(text)
        # Re-scan from scratch since positions shifted
        return _replace_func_balanced(result, func_re, template)
    return result


def _replace_field_refs(formula: str) -> str:
    """Replace [FieldName] references with quoted column names, skipping string literals.

    Handles Tableau's qualified names: [table].[column] -> column
    Skips brackets inside string literals (single or double quoted).
    """
    result = []
    i = 0
    in_string = False
    string_char = None

    while i < len(formula):
        c = formula[i]

        if in_string:
            if c == string_char:
                if i + 1 < len(formula) and formula[i + 1] == string_char:
                    # Doubled-quote escape: append both and skip
                    result.append(c)
                    result.append(formula[i + 1])
                    i += 2
                else:
                    result.append(c)
                    in_string = False
                    i += 1
            else:
                result.append(c)
                i += 1
            continue

        if c in ("'", '"'):
            in_string = True
            string_char = c
            result.append(c)
            i += 1
            continue

        if c == "[":
            # Find matching ]
            end = formula.find("]", i + 1)
            if end == -1:
                result.append(c)
                i += 1
                continue
            field_name = formula[i + 1 : end]

            # Check if next char starts another bracket reference (qualified name)
            # e.g. [table].[column]
            if end + 2 < len(formula) and formula[end + 1] == "." and formula[end + 2] == "[":
                end2 = formula.find("]", end + 3)
                if end2 != -1:
                    field_name = formula[end + 3 : end2]
                    i = end2 + 1
                else:
                    i = end + 1
            else:
                i = end + 1

            result.append(_quote_identifier_if_needed(_normalize_column_name(field_name)))
            continue

        result.append(c)
        i += 1

    return "".join(result)


def _convert_double_quotes(text: str) -> str:
    """Convert Tableau double-quoted string literals to SQL single quotes.

    Tableau uses "hello" for strings, SQL uses 'hello'. Double quotes in SQL
    mean identifiers. Must skip brackets (already processed) and single-quoted
    strings.
    """
    result = []
    i = 0
    while i < len(text):
        c = text[i]
        if c == "'":
            # Single-quoted string: pass through as-is
            result.append(c)
            i += 1
            while i < len(text):
                result.append(text[i])
                if text[i] == "'" and (i + 1 >= len(text) or text[i + 1] != "'"):
                    i += 1
                    break
                i += 1
        elif c == '"':
            # Double-quoted string: convert to single quotes
            result.append("'")
            i += 1
            while i < len(text):
                if text[i] == '"':
                    if i + 1 < len(text) and text[i + 1] == '"':
                        # Escaped double quote -> escaped single quote
                        result.append("''")
                        i += 2
                    else:
                        result.append("'")
                        i += 1
                        break
                else:
                    result.append(text[i])
                    i += 1
        else:
            result.append(c)
            i += 1
    return "".join(result)


def _strip_comments(text: str) -> str:
    """Strip // line comments while preserving // inside string literals.

    E.g. '://' in a string is NOT a comment start.
    Handles doubled-quote escapes ('') inside string literals.
    """
    result = []
    i = 0
    while i < len(text):
        c = text[i]
        if c in ("'", '"'):
            # Inside a string literal: pass through until matching quote
            quote = c
            result.append(c)
            i += 1
            while i < len(text):
                if text[i] == quote:
                    if i + 1 < len(text) and text[i + 1] == quote:
                        # Doubled-quote escape: append both and skip
                        result.append(text[i])
                        result.append(text[i + 1])
                        i += 2
                    else:
                        # End of string
                        result.append(text[i])
                        i += 1
                        break
                else:
                    result.append(text[i])
                    i += 1
        elif c == "/" and i + 1 < len(text) and text[i + 1] == "/":
            # Skip until end of line
            while i < len(text) and text[i] != "\n":
                i += 1
        else:
            result.append(c)
            i += 1
    return "".join(result)


def _split_args_balanced(text: str) -> list[str]:
    """Split comma-separated arguments respecting parentheses and string literals."""
    args = []
    depth = 0
    current = []
    in_string = False
    string_char = None

    for c in text:
        if in_string:
            current.append(c)
            if c == string_char:
                in_string = False
        elif c in ("'", '"'):
            in_string = True
            string_char = c
            current.append(c)
        elif c == "(":
            depth += 1
            current.append(c)
        elif c == ")":
            depth -= 1
            current.append(c)
        elif c == "," and depth == 0:
            args.append("".join(current).strip())
            current = []
        else:
            current.append(c)

    if current:
        args.append("".join(current).strip())

    return args


def _translate_iif(text: str) -> str:
    """Translate IIF(cond, then, else) using balanced-paren argument parsing."""
    result = text
    for m in _IIF_RE.finditer(text):
        start = m.start()
        open_paren = m.end() - 1
        close_paren = _find_matching_paren(result, open_paren)
        if close_paren == -1:
            continue
        inner = result[open_paren + 1 : close_paren]
        args = _split_args_balanced(inner)
        if len(args) >= 3:
            cond, then_val, else_val = args[0], args[1], args[2]
            replacement = f"CASE WHEN {cond} THEN {then_val} ELSE {else_val} END"
            result = result[:start] + replacement + result[close_paren + 1 :]
            # Restart since positions shifted
            return _translate_iif(result)
    return result


def _convert_string_concat(text: str) -> str:
    """Convert Tableau's + string concatenation to SQL ||.

    Replaces + with || when at least one adjacent operand is a string literal.
    Uses a simple heuristic: if a + is preceded or followed by a single-quoted
    string (possibly with whitespace), replace it with ||.
    """
    # Match: 'string' + or + 'string'
    result = re.sub(r"('\s*)\+(\s*)", r"\1||\2", text)
    result = re.sub(r"(\s*)\+(\s*')", r"\1||\2", result)
    return result


def _translate_formula(formula: str | None) -> tuple[str | None, bool]:
    """Translate Tableau calc formula to SQL.

    Returns:
        (translated_sql, is_translatable) - if is_translatable is False,
        the raw formula is returned as-is and should be stored in metadata.
    """
    if formula is None:
        return (None, True)

    # Check for untranslatable constructs
    if _has_lod_or_table_calc(formula):
        return (formula, False)

    # Strip // comments before translation (they can contain IF/THEN keywords)
    # Must be string-aware to preserve '://' inside string literals
    result = _strip_comments(formula).strip()

    # Convert Tableau double-quoted string literals to SQL single quotes
    result = _convert_double_quotes(result)

    # Replace [Field] references with quoted column names (string-literal-aware)
    result = _replace_field_refs(result)

    # ZN(x) -> COALESCE(x, 0)
    result = _replace_func_balanced(result, _ZN_RE, "COALESCE({arg}, 0)")

    # IFNULL(x,y) -> COALESCE(x,y)
    result = _IFNULL_RE.sub("COALESCE(", result)

    # ISNULL(x) -> (x IS NULL)
    result = _replace_func_balanced(result, _ISNULL_RE, "({arg} IS NULL)")

    # IIF(c, t, f) -> CASE WHEN c THEN t ELSE f END (balanced-paren aware)
    result = _translate_iif(result)

    # IF c THEN t ELSE e END -> CASE WHEN c THEN t ELSE e END
    result = _IF_THEN_RE.sub(_if_to_case, result)

    # CONTAINS(s, sub) -> s LIKE '%' || sub || '%' (balanced-paren aware)
    result = _translate_contains(result)

    # DATETRUNC('g', d) -> DATE_TRUNC('g', d)
    result = _DATETRUNC_RE.sub("DATE_TRUNC(", result)

    # COUNTD(x) -> COUNT(DISTINCT x)
    result = _replace_func_balanced(result, _COUNTD_RE, "COUNT(DISTINCT {arg})")

    # LEN(s) -> LENGTH(s)
    result = _LEN_RE.sub("LENGTH(", result)

    # INT/FLOAT/STR(x) -> CAST(x AS TYPE) with balanced parens
    for func_name, template in _TABLEAU_CAST_FUNCS.items():
        func_re = re.compile(rf"\b{func_name}\s*\(", re.IGNORECASE)
        result = _replace_func_balanced(result, func_re, template)

    # DATEADD('unit', n, date) -> date_add(date, INTERVAL (n) unit) (balanced-paren aware)
    result = _translate_dateadd(result)

    # Simple function renames (MID->SUBSTRING, FIND->STRPOS, etc.)
    for pattern, replacement in _SIMPLE_RENAMES:
        result = pattern.sub(replacement, result)

    # Tableau uses + for string concatenation; SQL uses ||
    # Convert + to || when adjacent to a string literal ('...')
    result = _convert_string_concat(result)

    return (result, True)


def _translate_contains(text: str) -> str:
    """Translate CONTAINS(s, sub) -> s LIKE '%' || sub || '%' with balanced args."""
    result = text
    for m in _CONTAINS_RE.finditer(text):
        start = m.start()
        open_paren = m.end() - 1
        close_paren = _find_matching_paren(result, open_paren)
        if close_paren == -1:
            continue
        inner = result[open_paren + 1 : close_paren]
        args = _split_args_balanced(inner)
        if len(args) >= 2:
            s, sub = args[0], args[1]
            replacement = f"{s} LIKE '%' || {sub} || '%'"
            result = result[:start] + replacement + result[close_paren + 1 :]
            return _translate_contains(result)
    return result


def _translate_dateadd(text: str) -> str:
    """Translate DATEADD('unit', n, date) -> date_add(date, INTERVAL (n) unit) with balanced args."""
    result = text
    for m in _DATEADD_RE.finditer(text):
        start = m.start()
        open_paren = m.end() - 1
        close_paren = _find_matching_paren(result, open_paren)
        if close_paren == -1:
            continue
        inner = result[open_paren + 1 : close_paren]
        args = _split_args_balanced(inner)
        if len(args) >= 3:
            unit = args[0].strip().strip("'\"").lower()
            amount = args[1].strip()
            date_expr = args[2].strip()
            replacement = f"date_add({date_expr}, INTERVAL ({amount}) {unit})"
            result = result[:start] + replacement + result[close_paren + 1 :]
            return _translate_dateadd(result)
    return result


def _if_to_case(match: re.Match) -> str:
    """Convert IF/THEN/ELSE/END to CASE WHEN."""
    full = match.group(0)
    # Simple IF c THEN t ELSE e END
    # Use a simpler approach: replace IF with CASE WHEN, THEN stays, ELSE stays, END stays
    result = re.sub(r"\bIF\b", "CASE WHEN", full, count=1, flags=re.IGNORECASE)
    result = re.sub(r"\bELSEIF\b", "WHEN", result, flags=re.IGNORECASE)
    return result


def _strip_brackets(name: str) -> str:
    """Strip Tableau bracket notation: [public].[orders] -> public.orders"""
    return name.replace("[", "").replace("]", "")


def _normalize_column_name(name: str) -> str:
    """Normalize Tableau column name.

    [calc_revenue] -> calc_revenue
    [orders].[amount] -> amount (take last part for qualified names)
    [none:Column Name:nk] -> Column Name (extract from colon-qualified format)
    """
    stripped = _strip_brackets(name)
    # Handle Tableau colon-qualified format: aggregation:name:qualifier
    # e.g. "none:Burst Out Set list:nk"
    if ":" in stripped:
        parts = stripped.split(":")
        if len(parts) >= 2:
            # The column name is the middle part(s)
            return ":".join(parts[1:-1]) if len(parts) > 2 else parts[1]
    # For qualified names like orders.amount, take the last part
    if "." in stripped:
        return stripped.rsplit(".", 1)[-1]
    return stripped


def _extract_table_name(relation_elem: ET.Element) -> str | None:
    """Extract qualified table name from a <relation type="table"> element."""
    table_attr = relation_elem.get("table")
    if table_attr:
        return _strip_brackets(table_attr)
    return None


# Namespace prefixes commonly used in Tableau XML files
_TABLEAU_NS_PREFIXES = [
    "user",
    "_.fcp.ObjectModelEncapsulateLegacy",
    "_.fcp.ObjectModelTableType",
    "_.fcp.SchemaViewerObjectModel",
]

# Regex to strip namespace-prefixed attributes (user:foo='bar' -> user_foo='bar')
# Only targets attribute positions (preceded by whitespace)
_NS_ATTR_RE = re.compile(r"(?<=\s)(\w[\w.]*):([\w][\w-]*)(?==)")


def _parse_tableau_xml(xml_path: Path) -> ET.Element:
    """Parse Tableau XML, handling undeclared namespace prefixes.

    Tableau files use namespace-prefixed attributes (e.g. user:ui-builder)
    without always declaring them. This causes ET.parse to fail with
    "unbound prefix". We handle this by injecting namespace declarations
    into the root element on retry.
    """
    try:
        tree = ET.parse(xml_path)
        return tree.getroot()
    except ET.ParseError:
        content = xml_path.read_text(encoding="utf-8")
        # Replace namespace-prefixed attributes with underscored versions
        content = _NS_ATTR_RE.sub(r"\1_\2", content)
        return ET.fromstring(content)


def _is_relation_tag(tag: str) -> bool:
    """Check if an XML tag name represents a relation element.

    Handles plain 'relation', namespace-URI format '{uri}relation',
    and Tableau's dotted format '_.fcp.ObjectModelEncapsulateLegacy.false...relation'.
    """
    if tag == "relation":
        return True
    if tag.endswith("}relation"):
        return True
    # Tableau uses ...relation suffix for legacy/modern variants
    if tag.endswith("relation") and ("." in tag or ":" in tag):
        return True
    return False


def _find_relation_element(connection: ET.Element) -> ET.Element | None:
    """Find the <relation> element inside a connection, handling namespace prefixes.

    Tableau files may use namespace-prefixed relation tags like
    <_.fcp.ObjectModelEncapsulateLegacy.false...relation>. This function
    searches direct children first, preferring the '.false...' variant
    (legacy format), then falls back to any relation with a type attribute.
    """
    # Prefer logical-layer collections when both a physical fallback table and a
    # collection relation are present.
    for child in connection:
        if _is_relation_tag(child.tag) and child.get("type") == "collection":
            return child

    # Direct child first (most common case)
    rel = connection.find("relation")
    if rel is not None:
        return rel

    # Search direct children for namespaced relation elements
    # Prefer the .false... variant (legacy format, more complete)
    candidates = []
    for child in connection:
        if _is_relation_tag(child.tag) and child.get("type"):
            if ".false" in child.tag:
                return child  # Prefer legacy format
            candidates.append(child)

    if candidates:
        return candidates[0]

    return None


def _extract_join_columns(expr: ET.Element) -> list[tuple[str, str]]:
    """Extract all (left, right) column pairs from a join expression.

    Handles simple equality (op='='), compound conditions (op='AND'),
    and nested structures. Returns all predicates so multi-column joins
    are fully preserved.
    """
    op = expr.get("op", "")
    sub_exprs = expr.findall("expression")

    if op == "=" and len(sub_exprs) >= 2:
        left = _strip_brackets(sub_exprs[0].get("op", ""))
        right = _strip_brackets(sub_exprs[1].get("op", ""))
        if left and right:
            return [(left, right)]
        return []

    if op.upper() == "AND" and sub_exprs:
        # Compound condition: collect ALL equality clauses
        pairs = []
        for child in sub_exprs:
            pairs.extend(_extract_join_columns(child))
        return pairs

    return []


@dataclass
class _JoinInfo:
    """Internal representation of a parsed join."""

    right_table: str
    right_table_qualified: str
    join_type: str  # inner, left, right, full, cross
    column_pairs: list[tuple[str, str]]  # [(left_col, right_col), ...]


@dataclass
class _CollectionInfo:
    """Ordered table info for Tableau logical-layer collections."""

    tables: list[tuple[str, str]]

    @property
    def base_table_name(self) -> str | None:
        return self.tables[0][0] if self.tables else None

    @property
    def base_table_qualified(self) -> str | None:
        return self.tables[0][1] if self.tables else None

    @property
    def table_map(self) -> dict[str, str]:
        return dict(self.tables)


@dataclass
class _ObjectGraphJoin:
    """Join edge extracted from a Tableau object-graph."""

    first_table: str
    second_table: str
    first_field: str
    second_field: str


@dataclass
class _ObjectGraphInfo:
    """Structured object-graph output for logical-layer datasources."""

    relationships: list[Relationship]
    joins: list[_ObjectGraphJoin]


def _quote_sql_identifier(identifier: str) -> str:
    """Quote a SQL identifier for generated Tableau-derived SQL."""
    return '"' + identifier.replace('"', '""') + '"'


def _quote_identifier_if_needed(identifier: str) -> str:
    """Quote a raw Tableau field name when it is not a simple SQL identifier."""
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier
    if _SIMPLE_SQL_IDENTIFIER_RE.match(identifier):
        return identifier
    if "." in identifier:
        return _quote_column_reference(identifier)
    return _quote_sql_identifier(identifier)


def _quote_column_reference(column_name: str) -> str:
    """Quote a possibly-qualified column reference."""
    parts = _strip_brackets(column_name).split(".")
    return ".".join(_quote_sql_identifier(part) for part in parts if part)


def _quote_table_reference(table_name: str) -> str:
    """Quote a possibly-qualified table reference."""
    parts = _strip_brackets(table_name).split(".")
    return ".".join(_quote_sql_identifier(part) for part in parts if part)


def _normalize_parent_name(name: str | None) -> str | None:
    """Normalize a Tableau parent-name/table identifier to its logical table name."""
    if not name:
        return None
    stripped = _strip_brackets(name)
    return stripped.rsplit(".", 1)[-1]


class TableauAdapter(BaseAdapter):
    """Adapter for importing Tableau .tds/.twb/.tdsx/.twbx datasource definitions.

    Transforms Tableau definitions into Sidemantic format:
    - Data sources -> Models
    - Columns with role=dimension -> Dimensions
    - Columns with role=measure -> Metrics
    - Drill paths -> Dimension hierarchies
    - Joins -> Relationships
    - Groups -> Segments
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse Tableau files into semantic graph.

        Args:
            source: Path to .tds/.twb/.tdsx/.twbx file or directory

        Returns:
            Semantic graph with imported models
        """
        graph = SemanticGraph()
        source_path = Path(source)

        if source_path.is_dir():
            for file_path in sorted(source_path.rglob("*")):
                if file_path.suffix.lower() in (".tds", ".twb"):
                    file_graph = self._parse_xml(file_path)
                    for model in file_graph.models.values():
                        graph.add_model(model)
                elif file_path.suffix.lower() in (".tdsx", ".twbx"):
                    file_graph = self._unzip_and_parse(file_path)
                    for model in file_graph.models.values():
                        graph.add_model(model)
        elif source_path.suffix.lower() in (".tdsx", ".twbx"):
            graph = self._unzip_and_parse(source_path)
        else:
            graph = self._parse_xml(source_path)

        return graph

    def _parse_xml(self, xml_path: Path) -> SemanticGraph:
        """Parse a .tds or .twb XML file."""
        graph = SemanticGraph()
        root = _parse_tableau_xml(xml_path)

        if root.tag == "datasource":
            model = self._parse_datasource(root)
            if model:
                graph.add_model(model)
        elif root.tag == "workbook":
            datasources = root.find("datasources")
            if datasources is not None:
                for ds_elem in datasources.findall("datasource"):
                    # Skip the Parameters datasource
                    name = ds_elem.get("formatted-name") or ds_elem.get("name") or ""
                    if name.lower() == "parameters":
                        continue
                    model = self._parse_datasource(ds_elem)
                    if model:
                        graph.add_model(model)

        return graph

    def _parse_datasource(self, ds_elem: ET.Element) -> Model | None:
        """Parse a single <datasource> element into a Model."""
        # Extract name
        name = ds_elem.get("formatted-name") or ds_elem.get("name") or ds_elem.get("caption")
        if not name:
            return None

        # Extract table reference and join info
        table = None
        sql = None
        relationships: list[Relationship] = []
        collection_info: _CollectionInfo | None = None
        connection = ds_elem.find("connection")
        if connection is not None:
            relation = _find_relation_element(connection)
            if relation is not None:
                rel_type = relation.get("type")
                if rel_type == "table":
                    table = _extract_table_name(relation)
                elif rel_type == "join":
                    base_table, joins = self._parse_relation_tree(relation)
                    if joins:
                        sql = self._build_join_sql(base_table, joins)
                        relationships = self._extract_relationships(joins)
                    else:
                        table = base_table
                elif rel_type == "text":
                    # Custom SQL
                    sql = relation.text or relation.get("table")
                elif rel_type == "collection":
                    collection_info = self._parse_collection(relation)
                    table = collection_info.base_table_qualified

        # Build metadata lookup from <metadata-records> before object-graph parsing so
        # collection sources can build a projected joined SQL model.
        metadata_lookup = self._build_metadata_lookup(ds_elem)

        # Parse object-graph for relationships (Tableau 2020.2+ data model)
        # The object-graph is a sibling of <connection>, not inside it
        object_graph = self._parse_object_graph(ds_elem)
        if collection_info and object_graph.joins:
            sql = self._build_collection_sql(collection_info, object_graph.joins, metadata_lookup)
            table = None if sql else collection_info.base_table_qualified
            relationships = object_graph.relationships
        elif not relationships and object_graph.relationships:
            relationships = object_graph.relationships

        # Parse columns
        dimensions: list[Dimension] = []
        metrics: list[Metric] = []
        seen_column_names: set[str] = set()

        for col_elem in ds_elem.findall("column"):
            result = self._parse_column(col_elem, metadata_lookup)
            if result is None:
                continue
            seen_column_names.add(result.name)
            if isinstance(result, Dimension):
                dimensions.append(result)
            elif isinstance(result, Metric):
                metrics.append(result)

        # Import orphan columns from metadata-records (physical columns with no
        # explicit <column> element, i.e. never customized by the user in Tableau)
        self._import_orphan_metadata_columns(metadata_lookup, seen_column_names, dimensions, metrics)

        # Apply drill-path hierarchies
        self._apply_drill_paths(ds_elem, dimensions)

        # Parse groups as segments
        segments = self._parse_groups_as_segments(ds_elem)

        # Determine primary key
        primary_key = self._infer_primary_key(dimensions, metrics, metadata_lookup, collection_info)
        if collection_info and sql:
            sql = self._inject_collection_primary_key_sql(
                sql,
                primary_key,
                collection_info,
                metadata_lookup,
            )
            primary_key = "__tableau_pk"

        model = Model(
            name=name,
            table=table,
            sql=sql,
            primary_key=primary_key,
            dimensions=dimensions,
            metrics=metrics,
            relationships=relationships,
            segments=segments,
        )

        return model

    def _parse_column(
        self,
        col_elem: ET.Element,
        metadata_lookup: dict[str, dict],
    ) -> Dimension | Metric | None:
        """Parse a single <column> element into a Dimension or Metric."""
        raw_name = col_elem.get("name")
        if not raw_name:
            return None

        col_name = _normalize_column_name(raw_name)
        role = col_elem.get("role")
        datatype = col_elem.get("datatype")
        caption = col_elem.get("caption")
        hidden = col_elem.get("hidden", "").lower() == "true"
        aggregation = col_elem.get("aggregation")

        # Check for calculated field
        calc_elem = col_elem.find("calculation")
        formula = None
        if calc_elem is not None:
            formula = calc_elem.get("formula")

        # Try metadata lookup for additional type info
        meta_info = metadata_lookup.get(raw_name, {})
        if not datatype:
            datatype = meta_info.get("local_type")
        if not aggregation:
            aggregation = meta_info.get("aggregation")

        # Translate formula if present
        sql_expr = None
        is_translatable = True
        metadata = None
        if formula:
            sql_expr, is_translatable = _translate_formula(formula)
            if not is_translatable:
                metadata = {"tableau_formula": formula}

        if role == "measure":
            return self._build_metric(
                col_name, aggregation, sql_expr, caption, hidden, is_translatable, formula, metadata
            )
        else:
            # Default to dimension
            return self._build_dimension(col_name, datatype, sql_expr, caption, hidden, metadata)

    def _build_dimension(
        self,
        name: str,
        datatype: str | None,
        sql: str | None,
        caption: str | None,
        hidden: bool,
        metadata: dict | None,
    ) -> Dimension:
        """Build a Dimension from parsed column attributes."""
        dim_type = _DATATYPE_MAP.get(datatype or "", "categorical")
        granularity = _DATATYPE_GRANULARITY.get(datatype or "")

        if sql is None:
            sql = _quote_identifier_if_needed(name)

        return Dimension(
            name=name,
            type=dim_type,
            sql=sql,
            granularity=granularity,
            label=caption,
            public=not hidden,
            metadata=metadata,
        )

    def _build_metric(
        self,
        name: str,
        aggregation: str | None,
        sql: str | None,
        caption: str | None,
        hidden: bool,
        is_translatable: bool,
        formula: str | None,
        metadata: dict | None,
    ) -> Metric:
        """Build a Metric from parsed column attributes."""
        agg_lower = (aggregation or "").lower()
        mapped_agg = _AGGREGATION_MAP.get(agg_lower)

        # Tableau's "Number of Records" pattern: formula='1' with no aggregation
        # This is equivalent to COUNT(*)
        # Tableau's "Number of Records" pattern: formula='1' with no aggregation
        # This is equivalent to COUNT(*)
        if formula and formula.strip() == "1" and not mapped_agg and agg_lower not in _PASSTHROUGH_AGGS:
            return Metric(name=name, agg="count", sql=None, label=caption, public=not hidden)

        # For metrics without a formula, sql defaults to the column name.
        if sql is None and not formula:
            sql = _quote_identifier_if_needed(name)

        if agg_lower in _PASSTHROUGH_AGGS or not is_translatable:
            # Passthrough or untranslatable: make derived metric
            return Metric(
                name=name,
                type="derived",
                sql=sql or name,
                label=caption,
                public=not hidden,
                metadata=metadata,
            )

        return Metric(
            name=name,
            agg=mapped_agg,
            sql=sql,
            label=caption,
            public=not hidden,
            metadata=metadata,
        )

    def _build_metadata_lookup(self, ds_elem: ET.Element) -> dict[str, dict]:
        """Build lookup from <metadata-records> for type/agg fallback.

        In real Tableau files, <metadata-records> is typically a child of
        <connection>, not a direct child of <datasource>. Use recursive search.
        """
        lookup: dict[str, dict] = {}
        metadata_records = ds_elem.find(".//metadata-records")
        if metadata_records is None:
            return lookup

        for record in metadata_records.findall("metadata-record"):
            if record.get("class") != "column":
                continue
            local_name_elem = record.find("local-name")
            if local_name_elem is None or local_name_elem.text is None:
                continue
            local_name = local_name_elem.text

            info: dict[str, str] = {}
            local_type_elem = record.find("local-type")
            if local_type_elem is not None and local_type_elem.text:
                info["local_type"] = local_type_elem.text

            agg_elem = record.find("aggregation")
            if agg_elem is not None and agg_elem.text:
                info["aggregation"] = agg_elem.text

            remote_alias_elem = record.find("remote-alias")
            if remote_alias_elem is not None and remote_alias_elem.text:
                info["remote_alias"] = remote_alias_elem.text

            parent_name_elem = record.find("parent-name")
            if parent_name_elem is not None and parent_name_elem.text:
                info["parent_name"] = parent_name_elem.text
                normalized_parent = _normalize_parent_name(parent_name_elem.text)
                if normalized_parent:
                    info["source_table_name"] = normalized_parent

            source_column = info.get("remote_alias") or _normalize_column_name(local_name)
            if source_column:
                info["source_column_name"] = source_column

            lookup[local_name] = info

        return lookup

    def _import_orphan_metadata_columns(
        self,
        metadata_lookup: dict[str, dict],
        seen_column_names: set[str],
        dimensions: list[Dimension],
        metrics: list[Metric],
    ) -> None:
        """Import physical columns that only exist in metadata-records.

        Tableau auto-discovers all columns from the database schema and stores
        them in <metadata-records>. Only columns the user customizes get explicit
        <column> elements. This method imports the uncustomized "orphan" columns
        so the semantic layer sees all available fields.
        """
        measure_aggs = {"sum", "avg", "min", "max", "median"}

        for local_name, info in metadata_lookup.items():
            col_name = _normalize_column_name(local_name)
            if col_name in seen_column_names:
                continue

            local_type = info.get("local_type")
            aggregation = info.get("aggregation", "")
            remote_alias = info.get("remote_alias")
            agg_lower = aggregation.lower() if aggregation else ""

            # Use remote_alias as the SQL expression (actual DB column name)
            sql = _quote_identifier_if_needed(remote_alias or col_name)

            # Role inference based on type and aggregation
            is_measure = agg_lower in measure_aggs and local_type in ("real", "integer")

            if is_measure:
                mapped_agg = _AGGREGATION_MAP.get(agg_lower)
                metrics.append(Metric(name=col_name, agg=mapped_agg, sql=sql))
            else:
                dim_type = _DATATYPE_MAP.get(local_type or "", "categorical")
                granularity = _DATATYPE_GRANULARITY.get(local_type or "")
                dimensions.append(Dimension(name=col_name, type=dim_type, sql=sql, granularity=granularity))

            seen_column_names.add(col_name)

    def _apply_drill_paths(self, ds_elem: ET.Element, dimensions: list[Dimension]) -> None:
        """Set parent attributes on dimensions based on <drill-paths>."""
        drill_paths = ds_elem.find("drill-paths")
        if drill_paths is None:
            return

        # Build name -> dimension lookup
        dim_by_name: dict[str, Dimension] = {d.name: d for d in dimensions}

        for drill_path in drill_paths.findall("drill-path"):
            fields = drill_path.findall("field")
            field_names = [_normalize_column_name(f.text or "") for f in fields if f.text]

            # Set parent chain: field[i+1].parent = field[i]
            for i in range(1, len(field_names)):
                child_name = field_names[i]
                parent_name = field_names[i - 1]
                if child_name in dim_by_name:
                    dim_by_name[child_name].parent = parent_name

    def _unzip_and_parse(self, zip_path: Path) -> SemanticGraph:
        """Extract .tdsx or .twbx ZIP, find inner .tds/.twb, parse it."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(tmpdir)

            # Find the inner .tds or .twb file
            tmpdir_path = Path(tmpdir)
            for inner_file in tmpdir_path.rglob("*"):
                if inner_file.suffix.lower() in (".tds", ".twb"):
                    return self._parse_xml(inner_file)

        return SemanticGraph()

    def _parse_collection(self, relation_elem: ET.Element) -> _CollectionInfo:
        """Parse a <relation type="collection"> element (Tableau 2020.2+ data model)."""
        tables: list[tuple[str, str]] = []

        # Find all table relations inside the collection
        for child in relation_elem:
            child_tag = child.tag if "}" not in child.tag else child.tag.rsplit("}", 1)[-1]
            if child_tag == "relation" and child.get("type") == "table":
                tbl_name = child.get("name", "")
                tbl_qualified = _extract_table_name(child)
                if tbl_name and tbl_qualified:
                    tables.append((tbl_name, tbl_qualified))

        return _CollectionInfo(tables=tables)

    def _parse_object_graph(self, ds_elem: ET.Element) -> _ObjectGraphInfo:
        """Parse object-graph elements for relationships (Tableau 2020.2+ data model).

        The object-graph defines relationships between tables in a collection.
        It contains <objects> (table definitions) and <relationships> (join conditions).
        """
        # Find object-graph element (may have namespace prefix in tag name)
        og_elem = None
        for child in ds_elem:
            tag = child.tag
            if tag == "object-graph" or (tag.endswith("object-graph") and "true" in tag):
                og_elem = child
                break

        if og_elem is None:
            return _ObjectGraphInfo(relationships=[], joins=[])

        # Build object-id -> table-name map from <objects>
        obj_map: dict[str, str] = {}
        objects_elem = og_elem.find("objects")
        if objects_elem is not None:
            for obj in objects_elem.findall("object"):
                obj_id = obj.get("id", "")
                obj_caption = obj.get("caption", "")
                if obj_id:
                    obj_map[obj_id] = obj_caption or obj_id

        # Parse <relationships>
        relationships: list[Relationship] = []
        joins: list[_ObjectGraphJoin] = []
        rels_elem = og_elem.find("relationships")
        if rels_elem is None:
            return _ObjectGraphInfo(relationships=[], joins=[])

        for rel in rels_elem.findall("relationship"):
            # Extract join columns from expression
            expr = rel.find("expression")
            pairs: list[tuple[str, str]] = []
            if expr is not None:
                pairs = _extract_join_columns(expr)

            # Extract endpoint table names
            first_ep = rel.find("first-end-point")
            second_ep = rel.find("second-end-point")
            first_table = obj_map.get(first_ep.get("object-id", ""), "") if first_ep is not None else ""
            second_table = obj_map.get(second_ep.get("object-id", ""), "") if second_ep is not None else ""

            if first_table and second_table and pairs:
                left_col, right_col = pairs[0]
                left_field = left_col.rsplit(".", 1)[-1] if "." in left_col else left_col
                right_field = right_col.rsplit(".", 1)[-1] if "." in right_col else right_col
                joins.append(
                    _ObjectGraphJoin(
                        first_table=first_table,
                        second_table=second_table,
                        first_field=left_field,
                        second_field=right_field,
                    )
                )
                # Extract just the column names (strip table qualifiers)
                fk = left_field
                pk = right_field
                relationships.append(
                    Relationship(
                        name=second_table,
                        type="many_to_one",
                        foreign_key=fk,
                        primary_key=pk,
                    )
                )

        return _ObjectGraphInfo(relationships=relationships, joins=joins)

    def _build_collection_field_sources(self, metadata_lookup: dict[str, dict]) -> dict[str, tuple[str, str]]:
        """Map semantic field names to logical table + physical column sources."""
        sources: dict[str, tuple[str, str]] = {}
        for local_name, info in metadata_lookup.items():
            field_name = _normalize_column_name(local_name)
            table_name = info.get("source_table_name")
            column_name = info.get("source_column_name")
            if not field_name or not table_name or not column_name:
                continue
            sources.setdefault(field_name, (table_name, column_name))
        return sources

    def _build_collection_sql(
        self,
        collection_info: _CollectionInfo,
        joins: list[_ObjectGraphJoin],
        metadata_lookup: dict[str, dict],
    ) -> str | None:
        """Build a projected SQL model for Tableau logical-layer collections."""
        base_table_name = collection_info.base_table_name
        base_table_qualified = collection_info.base_table_qualified
        if not base_table_name or not base_table_qualified:
            return None

        field_sources = self._build_collection_field_sources(metadata_lookup)
        if not field_sources:
            return None

        table_map = collection_info.table_map
        alias_by_table = {table_name: f"j{i}" for i, (table_name, _) in enumerate(collection_info.tables)}
        connected = {base_table_name}
        join_clauses: list[str] = []
        remaining = list(joins)

        while remaining:
            progressed = False
            for join in list(remaining):
                if join.first_table in connected and join.second_table not in connected:
                    join_clauses.append(
                        self._build_collection_join_clause(
                            join.first_table,
                            join.first_field,
                            join.second_table,
                            join.second_field,
                            table_map,
                            alias_by_table,
                            field_sources,
                        )
                    )
                    connected.add(join.second_table)
                    remaining.remove(join)
                    progressed = True
                elif join.second_table in connected and join.first_table not in connected:
                    join_clauses.append(
                        self._build_collection_join_clause(
                            join.second_table,
                            join.second_field,
                            join.first_table,
                            join.first_field,
                            table_map,
                            alias_by_table,
                            field_sources,
                        )
                    )
                    connected.add(join.first_table)
                    remaining.remove(join)
                    progressed = True
                elif join.first_table in connected and join.second_table in connected:
                    remaining.remove(join)
                    progressed = True
            if not progressed:
                break

        for table_name, qualified_table in collection_info.tables:
            if table_name in connected or table_name == base_table_name:
                continue
            join_clauses.append(f"CROSS JOIN {_quote_table_reference(qualified_table)} AS {alias_by_table[table_name]}")
            connected.add(table_name)

        select_clauses = [
            f"{alias_by_table[table_name]}.{_quote_sql_identifier(column_name)} AS {_quote_sql_identifier(field_name)}"
            for field_name, (table_name, column_name) in field_sources.items()
            if table_name in alias_by_table
        ]
        if not select_clauses:
            return None

        parts = [
            "SELECT",
            "  " + ",\n  ".join(select_clauses),
            f"FROM {_quote_table_reference(base_table_qualified)} AS {alias_by_table[base_table_name]}",
        ]
        parts.extend(join_clauses)
        return "\n".join(parts)

    def _build_collection_join_clause(
        self,
        connected_table: str,
        connected_field: str,
        joining_table: str,
        joining_field: str,
        table_map: dict[str, str],
        alias_by_table: dict[str, str],
        field_sources: dict[str, tuple[str, str]],
    ) -> str:
        """Build one LEFT JOIN clause for a logical-layer collection."""
        joining_table_qualified = table_map[joining_table]
        left_expr = self._collection_field_sql(
            connected_table,
            connected_field,
            alias_by_table,
            field_sources,
        )
        right_expr = self._collection_field_sql(
            joining_table,
            joining_field,
            alias_by_table,
            field_sources,
        )
        return (
            f"LEFT JOIN {_quote_table_reference(joining_table_qualified)} AS {alias_by_table[joining_table]} "
            f"ON {left_expr} = {right_expr}"
        )

    def _collection_field_sql(
        self,
        expected_table: str,
        field_name: str,
        alias_by_table: dict[str, str],
        field_sources: dict[str, tuple[str, str]],
    ) -> str:
        """Resolve a logical Tableau field name to an aliased physical column."""
        normalized = _normalize_column_name(field_name)
        table_name, column_name = field_sources.get(normalized, (expected_table, normalized))
        alias = alias_by_table.get(table_name, alias_by_table[expected_table])
        return f"{alias}.{_quote_sql_identifier(column_name)}"

    def _infer_primary_key(
        self,
        dimensions: list[Dimension],
        metrics: list[Metric],
        metadata_lookup: dict[str, dict],
        collection_info: _CollectionInfo | None,
    ) -> str:
        """Infer a primary key from actual imported fields instead of hard-coding id."""
        fields: list[tuple[str, str | None]] = []
        field_sources = self._build_collection_field_sources(metadata_lookup)
        for dimension in dimensions:
            fields.append((dimension.name, field_sources.get(dimension.name, (None, None))[0]))
        for metric in metrics:
            fields.append((metric.name, field_sources.get(metric.name, (None, None))[0]))

        def rank(field_name: str) -> tuple[int, int]:
            lowered = field_name.lower()
            if lowered == "id":
                return (0, 0)
            if lowered in {"row id", "rowid"}:
                return (1, 0)
            if lowered.endswith("_id") or lowered.endswith(" id"):
                return (2, 0)
            if lowered.endswith("_key") or lowered.endswith(" key") or lowered.endswith("key"):
                return (3, 0)
            return (99, 0)

        preferred_table = collection_info.base_table_name if collection_info else None
        preferred_fields = [
            field_name
            for field_name, table_name in fields
            if preferred_table is not None and table_name == preferred_table
        ]
        scored_preferred = [field_name for field_name in preferred_fields if rank(field_name)[0] < 99]
        if scored_preferred:
            return min(scored_preferred, key=rank)

        scored_fields = [field_name for field_name, _ in fields if rank(field_name)[0] < 99]
        if scored_fields:
            return min(scored_fields, key=rank)

        if preferred_fields:
            return preferred_fields[0]
        if fields:
            return fields[0][0]
        return "id"

    def _inject_collection_primary_key_sql(
        self,
        sql: str,
        primary_key: str,
        collection_info: _CollectionInfo,
        metadata_lookup: dict[str, dict],
    ) -> str:
        """Inject a stable projected PK alias into collection SQL."""
        field_sources = self._build_collection_field_sources(metadata_lookup)
        base_table = collection_info.base_table_name
        if not base_table:
            return sql

        source_table, source_column = field_sources.get(primary_key, (base_table, primary_key))
        alias_by_table = {table_name: f"j{i}" for i, (table_name, _) in enumerate(collection_info.tables)}
        pk_expr = (
            f"{alias_by_table.get(source_table, alias_by_table[base_table])}.{_quote_sql_identifier(source_column)}"
        )
        return sql.replace(
            "SELECT\n",
            f"SELECT\n  {pk_expr} AS {_quote_sql_identifier('__tableau_pk')},\n",
            1,
        )

    # --- Phase 2: Multi-table joins ---

    def _parse_relation_tree(self, relation_elem: ET.Element) -> tuple[str | None, list[_JoinInfo]]:
        """Recursively parse <relation> tree into base table + join list."""
        rel_type = relation_elem.get("type")

        if rel_type == "table":
            table_name = _extract_table_name(relation_elem)
            return (table_name, [])

        if rel_type == "text":
            # Custom SQL: wrap as subquery with quoted alias
            name = relation_elem.get("name", "")
            sql_body = (relation_elem.text or "").strip()
            if sql_body and name:
                quoted_name = f'"{name}"' if " " in name or "(" in name else name
                return (f"({sql_body}) AS {quoted_name}", [])
            return (name or sql_body, [])

        if rel_type != "join":
            return (None, [])

        join_type_raw = relation_elem.get("join", "inner").lower()
        join_type_map = {
            "inner": "inner",
            "left": "left",
            "right": "right",
            "full": "full",
            "cross": "cross",
        }
        join_type = join_type_map.get(join_type_raw, "inner")

        # Extract join columns from clause
        column_pairs: list[tuple[str, str]] = []
        clause = relation_elem.find("clause")
        if clause is not None:
            expr = clause.find("expression")
            if expr is not None:
                column_pairs = _extract_join_columns(expr)

        # Parse child relations
        child_relations = relation_elem.findall("relation")
        if len(child_relations) < 2:
            return (None, [])

        # First child is left side, second is right side
        left_table, left_joins = self._parse_relation_tree(child_relations[0])
        right_table, right_joins = self._parse_relation_tree(child_relations[1])

        # The right side is being joined
        right_table_qualified = right_table or ""
        right_table_name = right_table.rsplit(".", 1)[-1] if right_table else ""

        joins = left_joins + right_joins
        joins.append(
            _JoinInfo(
                right_table=right_table_name,
                right_table_qualified=right_table_qualified,
                join_type=join_type,
                column_pairs=column_pairs,
            )
        )

        return (left_table, joins)

    def _build_join_sql(self, base_table: str | None, joins: list[_JoinInfo]) -> str | None:
        """Build SELECT * FROM ... JOIN ... ON ... SQL string."""
        if not base_table or not joins:
            return None

        parts = [f"SELECT * FROM {base_table}"]
        for join in joins:
            join_keyword = join.join_type.upper()
            parts.append(f"{join_keyword} JOIN {join.right_table_qualified}")
            if join.column_pairs:
                on_clauses = [
                    f"{_quote_column_reference(lc)} = {_quote_column_reference(rc)}" for lc, rc in join.column_pairs
                ]
                parts.append(f"ON {' AND '.join(on_clauses)}")

        return "\n".join(parts)

    def _extract_relationships(self, joins: list[_JoinInfo]) -> list[Relationship]:
        """Extract Relationship objects from parsed joins."""
        relationships: list[Relationship] = []
        for join in joins:
            if not join.column_pairs:
                continue

            # Use the first column pair for fk/pk (Relationship supports single pair)
            left_col_full, right_col_full = join.column_pairs[0]
            left_col = left_col_full.rsplit(".", 1)[-1]
            right_col = right_col_full.rsplit(".", 1)[-1]

            rel_type = "many_to_one"
            if join.join_type == "full":
                rel_type = "many_to_many"

            relationships.append(
                Relationship(
                    name=join.right_table,
                    type=rel_type,
                    foreign_key=left_col,
                    primary_key=right_col,
                )
            )

        return relationships

    def _parse_groups_as_segments(self, ds_elem: ET.Element) -> list[Segment]:
        """Convert <group> elements to Segment objects."""
        segments: list[Segment] = []

        for group_elem in ds_elem.findall("group"):
            group_name = group_elem.get("name")
            if not group_name:
                continue

            # Collect member values from groupfilter elements
            members: list[str] = []
            level_col: str | None = None

            for gf in group_elem.iter("groupfilter"):
                if gf.get("function") == "member":
                    member = gf.get("member")
                    if member:
                        members.append(member)
                    if not level_col:
                        level = gf.get("level")
                        if level:
                            level_col = _normalize_column_name(level)

            if members and level_col:
                escaped = [m.replace("'", "''") for m in members]
                quoted_members = ", ".join(f"'{m}'" for m in escaped)
                sql = f"{_quote_identifier_if_needed(level_col)} IN ({quoted_members})"
                segments.append(Segment(name=group_name, sql=sql))

        return segments
