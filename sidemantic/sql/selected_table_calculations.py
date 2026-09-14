"""Selected result calculations, applied after semantic ordering and pagination.

Numeric arithmetic supports +, -, *, /, unary signs and parentheses. Sequential
calculations (including stable rank) require an explicit selected-column order.
Unlike the standalone row processor, empty results retain calculation metadata.
"""

import math
import re

import sqlglot

from sidemantic.core.table_calculation import TableCalculation


def wrap_table_calculations(sql, catalog, names, order_by, dialect, *, aliases=None):
    if not names:
        return sql
    if dialect not in {"duckdb", "postgres", "postgresql"}:
        raise ValueError("Selected table calculations require DuckDB or PostgreSQL")
    columns = sqlglot.parse_one(sql, read="postgres" if dialect == "postgresql" else dialect).named_selects
    if not columns or "*" in columns or len({column.lower() for column in columns}) != len(columns):
        raise ValueError("Table calculations require unique named result columns")
    reserved = "__sidemantic_calc_"
    while reserved in sql.lower() or any(name.lower().startswith(reserved) for name in [*columns, *names]):
        reserved += "_"
    if len({name.lower() for name in names}) != len(names) or {n.lower() for n in names} & {c.lower() for c in columns}:
        raise ValueError("Duplicate or colliding table calculation selection")

    def quote(name):
        return '"' + name.replace('"', '""') + '"'

    available = set(columns)

    def reference(name):
        if name not in available:
            raise ValueError(f"Table calculation reference is not an available result column: {name}")
        return quote(name)

    ordering = []
    for item in order_by or []:
        parts = item.split()
        if not parts:
            raise ValueError("Invalid table calculation result ordering")
        field = parts[0]
        output_alias = (aliases or {}).get(field)
        if output_alias is None and aliases and "." not in field and field not in available:
            matches = {alias for key, alias in aliases.items() if key.rsplit(".", 1)[-1] == field}
            if len(matches) == 1:
                output_alias = matches.pop()
        if output_alias is not None:
            field = output_alias
        elif field not in available:
            field = field.replace(".", "_") if field.replace(".", "_") in available else field.rsplit(".", 1)[-1]
        suffix = " ".join(parts[1:]).upper()
        if not re.fullmatch(r"(?:(?:ASC|DESC)(?: NULLS (?:FIRST|LAST))?|NULLS (?:FIRST|LAST))?", suffix):
            raise ValueError("Table calculations require selected-column ordering")
        ordering.append(reference(field) + (" " + suffix if suffix else ""))
    ordinal = quote(reserved + "ordinal")
    ctes = [f"{reserved}base AS (\n{sql.rstrip().rstrip(';')}\n)"]
    window_order = "ORDER BY " + ", ".join(ordering) if ordering else ""
    ctes.append(f"{reserved}0 AS (SELECT *, ROW_NUMBER() OVER ({window_order}) AS {ordinal} FROM {reserved}base)")
    previous = reserved + "0"
    for index, name in enumerate(names, 1):
        if name not in catalog:
            raise ValueError(f"Unknown table calculation: {name}")
        calc: TableCalculation = catalog[name]
        if calc.order_by or (calc.partition_by and calc.type != "percent_of_column_total"):
            raise ValueError(f"Unsupported table calculation ordering/partition controls: {name}")
        if (
            calc.type in {"running_total", "percent_of_previous", "row_number", "moving_average", "rank"}
            and not ordering
        ):
            raise ValueError(f"Table calculation {name} requires explicit selected-column order_by")
        field = reference(calc.field) if calc.type not in {"formula", "row_number"} else None
        value = f"COALESCE({field}, 0)"
        ordered = f"ORDER BY {ordinal} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
        if calc.type == "formula":
            result = _formula_sql(calc.expression or "", reference)
        elif calc.type in {"percent_of_total", "percent_of_column_total"}:
            partition = (
                "PARTITION BY " + ", ".join(reference(p) for p in calc.partition_by) if calc.partition_by else ""
            )
            total = f"SUM({value}) OVER ({partition})"
            result = f"COALESCE(CAST({value} AS DOUBLE PRECISION) / NULLIF({total}, 0) * 100, 0)"
        elif calc.type == "running_total":
            result = f"SUM({value}) OVER ({ordered})"
        elif calc.type == "percent_of_previous":
            prior = f"LAG({field}) OVER (ORDER BY {ordinal})"
            result = f"CAST(({field} - {prior}) AS DOUBLE PRECISION) / NULLIF({prior}, 0) * 100"
        elif calc.type == "row_number":
            result = ordinal
        elif calc.type == "moving_average":
            if not calc.window_size or calc.window_size < 1:
                raise ValueError("Moving average requires positive window_size")
            result = f"AVG(CAST({value} AS DOUBLE PRECISION)) OVER (ORDER BY {ordinal} ROWS BETWEEN {calc.window_size - 1} PRECEDING AND CURRENT ROW)"
        elif calc.type == "percentile":
            if calc.percentile is None or not 0 <= calc.percentile <= 1:
                raise ValueError("Percentile requires percentile between 0 and 1")
            result = f"(SELECT PERCENTILE_CONT({calc.percentile}) WITHIN GROUP (ORDER BY {field}) FROM {previous})"
        elif calc.type == "rank":
            position, boundary = quote(reserved + "position"), quote(reserved + "boundary")
            rank_order = f"ORDER BY {value} DESC, {ordinal}"
            rank_cte = f"{reserved}rank{index}"
            ctes.append(
                f"{rank_cte} AS (SELECT *, ROW_NUMBER() OVER ({rank_order}) AS {position}, CASE WHEN {field} IS DISTINCT FROM LAG({field}) OVER ({rank_order}) THEN 1 ELSE 0 END AS {boundary} FROM {previous})"
            )
            result = f"COALESCE(MAX(CASE WHEN {boundary} = 1 THEN {position} END) OVER (ORDER BY {position} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 1)"
            previous = rank_cte
        else:
            raise ValueError(f"Unsupported table calculation type: {calc.type}")
        current = reserved + str(index)
        projection = ", ".join(quote(c) for c in columns) + f", {ordinal}"
        ctes.append(f"{current} AS (SELECT {projection}, {result} AS {quote(name)} FROM {previous})")
        previous = current
        columns.append(name)
        available.add(name)
    return (
        "WITH "
        + ",\n".join(ctes)
        + "\nSELECT "
        + ", ".join(quote(c) for c in columns)
        + f" FROM {previous} ORDER BY {ordinal}"
    )


def _formula_sql(source, reference):
    if len(source.encode()) > 8192:
        raise ValueError("Table calculation formula exceeds length limit")
    position = 0

    def whitespace():
        nonlocal position
        while position < len(source) and source[position] in " \t\n\r\v\f":
            position += 1

    def expression(minimum=0, depth=1):
        nonlocal position
        if depth > 64:
            raise ValueError("Table calculation formula exceeds depth limit")
        whitespace()
        token = source[position : position + 1]
        if token in {"+", "-"}:
            position += 1
            left = f"({token}{expression(3, depth + 1)})"
        elif token == "(":
            position += 1
            left = expression(0, depth + 1)
            whitespace()
            if source[position : position + 1] != ")":
                raise ValueError("Table calculation formula missing closing parenthesis")
            position += 1
        elif source[position:].startswith("${"):
            end = source.find("}", position + 2)
            if end < 0:
                raise ValueError("Unclosed table calculation formula reference")
            left = f"COALESCE({reference(source[position + 2 : end])}, 0)"
            position = end + 1
        else:
            number = re.match(r"(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", source[position:])
            if number is None:
                raise ValueError("Table calculation formulas support only numeric +, -, *, / and references")
            left = number[0]
            if not math.isfinite(float(left)) or (
                len(left) > 1 and left[0] == "0" and left[1].isdigit() and not any(c in left for c in ".eE")
            ):
                raise ValueError("Invalid numeric table calculation constant")
            position += len(left)
        while True:
            whitespace()
            operator = source[position : position + 1]
            precedence = {"+": 1, "-": 1, "*": 2, "/": 2}.get(operator, 0)
            if not precedence or precedence < minimum:
                return left
            position += 1
            right = expression(precedence + 1, depth + 1)
            if operator == "/":
                left = f"(CAST({left} AS DOUBLE PRECISION) / NULLIF({right}, 0))"
            else:
                left = f"({left} {operator} {right})"

    result = expression()
    if position != len(source):
        raise ValueError("Unsupported table calculation formula expression")
    return result
