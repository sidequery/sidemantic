from __future__ import annotations

import sidemantic_dax.ast as dax_ast


def test_from_raw_expr_function_call():
    raw = {
        "FunctionCall": {
            "name": "SUM",
            "args": [
                {
                    "TableColumnRef": {
                        "table": {"name": "Sales", "quoted": True},
                        "column": "Amount",
                    }
                }
            ],
        }
    }

    expr = dax_ast.from_raw_expr(raw)
    assert isinstance(expr, dax_ast.FunctionCall)
    assert expr.name == "SUM"
    assert len(expr.args) == 1
    arg = expr.args[0]
    assert isinstance(arg, dax_ast.TableColumnRef)
    assert arg.table.name == "Sales"
    assert arg.column == "Amount"


def test_from_raw_query_define_evaluate():
    raw = {
        "define": {
            "defs": [
                {
                    "Measure": {
                        "doc": None,
                        "table": {"name": "t", "quoted": True},
                        "name": "m",
                        "expr": {"Number": "1"},
                    }
                }
            ]
        },
        "evaluates": [
            {
                "expr": {"TableRef": {"name": "t", "quoted": True}},
                "order_by": [{"expr": {"BracketRef": "m"}, "direction": "Desc"}],
                "start_at": [{"Number": "5"}],
            }
        ],
    }

    query = dax_ast.from_raw_query(raw)
    assert query.define is not None
    assert len(query.define.defs) == 1
    definition = query.define.defs[0]
    assert isinstance(definition, dax_ast.MeasureDef)
    assert definition.name == "m"
    assert isinstance(query.evaluates[0].order_by[0].direction, dax_ast.SortDirection)


def test_from_raw_expr_parameter():
    expr = dax_ast.from_raw_expr({"Parameter": "p"})
    assert isinstance(expr, dax_ast.Parameter)
    assert expr.name == "p"


def test_from_raw_expr_hierarchy_ref():
    expr = dax_ast.from_raw_expr(
        {
            "HierarchyRef": {
                "table": {"name": "Fact", "quoted": False},
                "column": "Date",
                "levels": ["Year", "Month"],
            }
        }
    )
    assert isinstance(expr, dax_ast.HierarchyRef)
    assert expr.table.name == "Fact"
    assert expr.column == "Date"
    assert expr.levels == ["Year", "Month"]


def test_from_raw_definition_function():
    raw = {
        "Function": {
            "doc": "adds",
            "name": "sumtwo",
            "params": [{"name": "a", "type_hints": []}, {"name": "b", "type_hints": ["numeric"]}],
            "body": {"Identifier": "a"},
        }
    }
    definition = dax_ast._from_raw_definition(raw)
    assert isinstance(definition, dax_ast.FunctionDef)
    assert definition.name == "sumtwo"
    assert len(definition.params) == 2


def test_from_raw_tokens():
    raw = [
        {"kind": {"Ident": "sum"}, "span": {"start": 0, "end": 3}},
        {"kind": "LParen", "span": {"start": 3, "end": 4}},
        {"kind": {"Number": "1"}, "span": {"start": 4, "end": 5}},
        {"kind": "RParen", "span": {"start": 5, "end": 6}},
        {"kind": "Eof", "span": {"start": 6, "end": 6}},
    ]

    tokens = dax_ast.from_raw_tokens(raw)
    assert len(tokens) == 5
    assert isinstance(tokens[0].kind, dax_ast.IdentToken)
    assert isinstance(tokens[-1].kind, dax_ast.Eof)
