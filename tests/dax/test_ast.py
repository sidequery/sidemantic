from __future__ import annotations

import importlib

import pytest

dax_ast = pytest.importorskip("sidemantic_dax.ast")


def test_public_exports_include_all_python_api_families():
    sidemantic_dax = importlib.import_module("sidemantic_dax")
    expected = {
        "Dialect",
        "ValidationCode",
        "ValidationIssue",
        "format_expression",
        "format_query",
        "LosslessParse",
        "AstNodeSpan",
        "SourceComment",
        "parse_expression_lossless",
        "parse_query_lossless",
        "RecoveryResult",
        "RecoveryDiagnostic",
        "recover_expression",
        "recover_query",
        "ModelMetadata",
        "ModelTable",
        "ModelValidationIssue",
        "validate_expression_against_model",
        "validate_query_against_model",
    }

    assert len(dax_ast.__all__) == len(set(dax_ast.__all__))
    assert expected <= set(dax_ast.__all__)
    assert expected <= set(sidemantic_dax.__all__)
    assert all(hasattr(dax_ast, name) for name in dax_ast.__all__)
    assert all(hasattr(sidemantic_dax, name) for name in sidemantic_dax.__all__)


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


def test_from_raw_expr_omitted_is_distinct_from_blank():
    omitted = dax_ast.from_raw_expr("Omitted")
    blank = dax_ast.from_raw_expr("Blank")
    assert isinstance(omitted, dax_ast.Omitted)
    assert isinstance(blank, dax_ast.Blank)
    assert omitted != blank


def test_from_raw_expr_tuple():
    expr = dax_ast.from_raw_expr({"Tuple": [{"Number": "1"}, {"String": "x"}]})
    assert isinstance(expr, dax_ast.Tuple)
    assert expr.elements == [dax_ast.Number(value="1"), dax_ast.String(value="x")]


def test_from_raw_expr_datetime_literal():
    expr = dax_ast.from_raw_expr({"DateTime": "2020-12-15T12:30:59"})
    assert isinstance(expr, dax_ast.DateTime)
    assert expr.value == "2020-12-15T12:30:59"


def test_from_raw_expr_datatable():
    expr = dax_ast.from_raw_expr(
        {
            "DataTable": {
                "columns": [
                    {"name": "Name", "data_type": "String"},
                    {"name": "Amount", "data_type": "Currency"},
                ],
                "rows": [[{"String": "A"}, {"Number": "1.5"}]],
            }
        }
    )
    assert isinstance(expr, dax_ast.DataTable)
    assert expr.columns[0] == dax_ast.DataTableColumn(name="Name", data_type=dax_ast.DataTableType.string)
    assert expr.rows == [[dax_ast.String(value="A"), dax_ast.Number(value="1.5")]]


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
            "params": [
                {"name": "a", "type_hints": []},
                {"name": "b", "type_hints": ["numeric"], "default": {"Number": "0.1"}},
            ],
            "body": {"Identifier": "a"},
        }
    }
    definition = dax_ast._from_raw_definition(raw)
    assert isinstance(definition, dax_ast.FunctionDef)
    assert definition.name == "sumtwo"
    assert len(definition.params) == 2
    assert definition.params[0].default is None
    assert definition.params[1].default == dax_ast.Number(value="0.1")


def test_from_raw_definition_table_visual_shape():
    raw = {
        "Table": {
            "doc": None,
            "name": "data",
            "expr": {"Identifier": "T"},
            "visual_shape": {
                "axes": [
                    {
                        "name": "rows",
                        "groups": [
                            {
                                "columns": [{"name": "Year"}],
                                "total": {"name": "IsYearTotal"},
                            }
                        ],
                        "order_by": [{"name": "Year"}],
                    }
                ],
                "densify": "IsDensified",
            },
        }
    }

    definition = dax_ast._from_raw_definition(raw)
    assert isinstance(definition, dax_ast.TableDef)
    assert definition.visual_shape is not None
    assert definition.visual_shape.axes[0].name == "rows"
    assert definition.visual_shape.axes[0].groups[0].columns[0].name == "Year"
    assert definition.visual_shape.axes[0].groups[0].total.name == "IsYearTotal"
    assert definition.visual_shape.densify == "IsDensified"


def test_from_raw_definition_table_without_visual_shape():
    definition = dax_ast._from_raw_definition(
        {
            "Table": {
                "doc": None,
                "name": "data",
                "expr": {"Identifier": "T"},
            }
        }
    )
    assert isinstance(definition, dax_ast.TableDef)
    assert definition.visual_shape is None


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


def test_from_raw_datetime_token():
    raw = [
        {
            "kind": {"DateTime": "2020-12-15T12:30:59"},
            "span": {"start": 0, "end": 23},
        }
    ]

    token = dax_ast.from_raw_tokens(raw)[0]
    assert isinstance(token.kind, dax_ast.DateTimeToken)
    assert token.kind.value == "2020-12-15T12:30:59"


def test_datetime_literal_native_round_trip():
    try:
        expr = dax_ast.parse_expression('dt"2020-12-15T12:30:59"')
        token = dax_ast.lex('dt"2020-12-15T12:30:59"')[0]
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise

    assert isinstance(expr, dax_ast.DateTime)
    assert expr.value == "2020-12-15T12:30:59"
    assert isinstance(token.kind, dax_ast.DateTimeToken)
    assert token.span == dax_ast.Span(start=0, end=23)


def test_function_parameter_default_native_round_trip():
    query_text = """
        define function addtax = (
            amount: numeric,
            taxrate: numeric = 0.1,
            scale = divide(1 + 2, 3) * 4
        ) => amount + amount * taxrate * scale
        evaluate { addtax(100) }
    """
    try:
        query = dax_ast.parse_query(query_text)
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise

    assert query.define is not None
    definition = query.define.defs[0]
    assert isinstance(definition, dax_ast.FunctionDef)
    assert definition.params[0].default is None
    assert definition.params[1].default == dax_ast.Number(value="0.1")
    assert isinstance(definition.params[2].default, dax_ast.Binary)
    assert isinstance(definition.params[2].default.left, dax_ast.FunctionCall)


def test_visual_shape_native_round_trip():
    query_text = """
        define table data = summarizecolumns(
            rollupaddissubtotal(T[Year], "IsYearTotal"),
            rollupaddissubtotal(T[Product], "IsProductTotal"),
            "Measure", sum(T[SalesAmount])
        )
        with visual shape
            axis rows group [Year] total [IsYearTotal] order by [Year]
            axis columns group [Product] total [IsProductTotal] order by [Product]
            densify "IsDensified"
        evaluate data
    """
    try:
        query = dax_ast.parse_query(query_text)
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise

    assert query.define is not None
    definition = query.define.defs[0]
    assert isinstance(definition, dax_ast.TableDef)
    assert definition.visual_shape is not None
    assert [axis.name for axis in definition.visual_shape.axes] == ["rows", "columns"]
    assert definition.visual_shape.axes[0].groups[0].total.name == "IsYearTotal"
    assert definition.visual_shape.densify == "IsDensified"


def test_omitted_function_arguments_native_round_trip():
    try:
        index = dax_ast.parse_expression("INDEX(1, , -1)")
        window = dax_ast.parse_expression("WINDOW(0, ABS, 0, REL,, -1)")
        udf_call = dax_ast.parse_expression("f(1,,3)")
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise

    assert isinstance(index, dax_ast.FunctionCall)
    assert isinstance(index.args[1], dax_ast.Omitted)
    assert isinstance(window, dax_ast.FunctionCall)
    assert isinstance(window.args[4], dax_ast.Omitted)
    assert isinstance(udf_call, dax_ast.FunctionCall)
    assert isinstance(udf_call.args[1], dax_ast.Omitted)


def test_start_at_native_accepts_constants_and_parameters_only():
    query_text = """
        evaluate 'T'
        order by [a], [b], [c]
        start at -1, dt"2020-12-15T12:30:59", @cursor
    """
    try:
        query = dax_ast.parse_query(query_text)
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise

    values = query.evaluates[0].start_at
    assert values is not None
    assert isinstance(values[0], dax_ast.Unary)
    assert isinstance(values[1], dax_ast.DateTime)
    assert values[2] == dax_ast.Parameter(name="cursor")

    with pytest.raises(ValueError, match="literal constant or @parameter"):
        dax_ast.parse_query("evaluate 'T' order by [a] start at 1 + 2")
    with pytest.raises(ValueError, match="literal constant or @parameter"):
        dax_ast.parse_query("evaluate 'T' order by [a] start at BLANK()")


def test_multi_column_in_native_round_trip():
    try:
        expr = dax_ast.parse_expression("('Product'[Color], 'Product'[Brand]) IN {(\"Red\", \"Contoso\")}")
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise

    assert isinstance(expr, dax_ast.Binary)
    assert expr.op is dax_ast.BinaryOp.in_
    assert isinstance(expr.left, dax_ast.Tuple)
    assert len(expr.left.elements) == 2
    assert isinstance(expr.right, dax_ast.TableConstructor)
    assert expr.right.rows[0] == [dax_ast.String(value="Red"), dax_ast.String(value="Contoso")]


def test_datatable_native_round_trip_and_validation():
    try:
        expr = dax_ast.parse_expression('DATATABLE("Name", STRING, "Amount", DECIMAL, {{"A", 1.5}, {"B",}})')
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise

    assert isinstance(expr, dax_ast.DataTable)
    assert expr.columns[1].data_type is dax_ast.DataTableType.currency
    assert isinstance(expr.rows[1][1], dax_ast.Omitted)

    with pytest.raises(ValueError, match="schema defines 2 columns"):
        dax_ast.parse_expression('DATATABLE("A", INTEGER, "B", STRING, {{1}})')
    with pytest.raises(ValueError, match="must be constant expressions"):
        dax_ast.parse_expression('DATATABLE("A", INTEGER, {{[Measure]}})')


def test_static_validation_native_api():
    try:
        expression_issues = dax_ast.validate_expression("SUM() + FUTURE_DAX(1)")
        strict_issues = dax_ast.validate_expression("SUM() + FUTURE_DAX(1)", report_unrecognized_functions=True)
        query_issues = dax_ast.validate_query("EVALUATE 1")
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise

    assert [issue.code for issue in expression_issues] == [dax_ast.ValidationCode.function_arity]
    assert {issue.code for issue in strict_issues} == {
        dax_ast.ValidationCode.function_arity,
        dax_ast.ValidationCode.unrecognized_function,
    }
    assert query_issues == [
        dax_ast.ValidationIssue(
            code=dax_ast.ValidationCode.expected_table,
            message="EVALUATE requires a table expression",
            path="$.evaluates[0].expr",
        )
    ]


def test_python_dialect_configuration():
    try:
        localized = dax_ast.parse_expression(
            "DIVIDE(1,5; 2)",
            dialect=dax_ast.Dialect(
                allow_semicolon_separators=True,
                allow_decimal_comma=True,
            ),
        )
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise

    assert isinstance(localized, dax_ast.FunctionCall)
    assert localized.args == [dax_ast.Number(value="1,5"), dax_ast.Number(value="2")]

    tokens = dax_ast.lex(
        "1 -- comment\n + 2",
        dialect=dax_ast.Dialect(allow_dash_dash_comments=False),
    )
    assert sum(isinstance(token.kind, dax_ast.Minus) for token in tokens) == 2

    validation_issues = dax_ast.validate_expression(
        "SUM(1,5)",
        dialect=dax_ast.Dialect(
            allow_semicolon_separators=True,
            allow_decimal_comma=True,
        ),
    )
    assert [issue.code for issue in validation_issues] == [dax_ast.ValidationCode.invalid_argument_type]


def test_datetime_literal_native_lexical_validation():
    try:
        date = dax_ast.parse_expression('dt"2015-1-9"')
        datetime_t = dax_ast.parse_expression('dt"2015-1-9T02:30:00"')
        datetime_space = dax_ast.parse_expression('dt"2015-1-9 02:30:00"')
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise

    assert date == dax_ast.DateTime(value="2015-1-9")
    assert datetime_t == dax_ast.DateTime(value="2015-1-9T02:30:00")
    assert datetime_space == dax_ast.DateTime(value="2015-1-9 02:30:00")

    for source in [
        'dt""',
        'dt"2020-1-1T02:30"',
        'dt"2020-1-1T02:30:00.1"',
        'dt"2020-1-1T02:30:00Z"',
    ]:
        with pytest.raises(ValueError, match=r"invalid datetime literal.*at 0\.\."):
            dax_ast.parse_expression(source)


def test_formatter_native_canonical_and_localized_round_trip():
    canonical = dax_ast.format_expression("DIVIDE(1.5,2)")
    localized = dax_ast.format_expression("DIVIDE(1.5,2)", localized=True)

    assert canonical == "DIVIDE(1.5, 2)"
    assert localized == "DIVIDE(1,5; 2)"
    assert (
        dax_ast.format_expression(
            localized,
            dialect=dax_ast.Dialect(allow_decimal_comma=True),
        )
        == canonical
    )

    query = dax_ast.format_query("EVALUATE {1,2}")
    assert query == "EVALUATE\n    {1, 2}"
    assert dax_ast.parse_query(query) == dax_ast.parse_query("EVALUATE {1,2}")


def test_lossless_native_spans_and_comments_round_trip():
    source = "/// docs\nSUM(/* inside */ 1, -- next\n 2 + 3) // tail"
    parsed = dax_ast.parse_expression_lossless(source)

    assert isinstance(parsed.ast, dax_ast.FunctionCall)
    assert parsed.source == source
    assert source[parsed.span.start : parsed.span.end].startswith("SUM(")
    assert parsed.nodes[-1].kind is dax_ast.AstNodeKind.expression
    assert [comment.kind for comment in parsed.comments] == [
        dax_ast.CommentKind.doc_line,
        dax_ast.CommentKind.block,
        dax_ast.CommentKind.dash_line,
        dax_ast.CommentKind.slash_line,
    ]
    assert parsed.comments[1].containing_node is not None

    query = dax_ast.parse_query_lossless("DEFINE MEASURE 'T'[M] = 1\nEVALUATE { [M] }")
    assert isinstance(query.ast, dax_ast.Query)
    assert query.nodes[-1].kind is dax_ast.AstNodeKind.query


def test_native_spans_use_python_string_indices_after_unicode():
    source = "/*é*/SUM(1)"
    parsed = dax_ast.parse_expression_lossless(source)

    assert source[parsed.span.start : parsed.span.end] == "SUM(1)"
    assert source[parsed.comments[0].span.start : parsed.comments[0].span.end] == "/*é*/"
    assert {source[node.span.start : node.span.end] for node in parsed.nodes} == {"1", "SUM(1)"}

    assert [source[token.span.start : token.span.end] for token in dax_ast.lex(source)] == [
        "SUM",
        "(",
        "1",
        ")",
        "",
    ]


def test_recovery_native_returns_typed_items_and_diagnostics():
    expression = dax_ast.recover_expression("SUM(1 +, 2) #, 3")
    assert not expression.is_clean
    assert any(item.value == dax_ast.Number(value="2") for item in expression.items)
    assert any(item.value == dax_ast.Number(value="3") for item in expression.items)
    assert {diagnostic.phase for diagnostic in expression.diagnostics} == {
        dax_ast.RecoveryPhase.lex,
        dax_ast.RecoveryPhase.parse,
    }

    query = dax_ast.recover_query("DEFINE MEASURE [Bad] = 1 + ; MEASURE [Good] = 2; EVALUATE {1, }; EVALUATE {2}")
    assert any(isinstance(item.value, dax_ast.MeasureDef) and item.value.name == "Good" for item in query.items)
    assert any(isinstance(item.value, dax_ast.EvaluateStmt) for item in query.items)


def test_recovery_spans_use_python_string_indices_after_unicode():
    source = "/*é*/SUM(1 +, 2) #, 3"
    recovered = dax_ast.recover_expression(source)

    assert [source[item.span.start : item.span.end] for item in recovered.items] == ["2", "3"]
    assert {source[diagnostic.span.start : diagnostic.span.end] for diagnostic in recovered.diagnostics} == {
        ",",
        ")",
        "#",
    }


def test_model_validation_native_uses_typed_metadata_and_issues():
    model = dax_ast.ModelMetadata(
        tables=[
            dax_ast.ModelTable(
                name="Sales",
                columns=["Amount", "ProductKey"],
                measures=["Revenue"],
            ),
            dax_ast.ModelTable(
                name="Product",
                columns=["ProductKey", "Color"],
                measures=[],
            ),
        ]
    )

    assert (
        dax_ast.validate_expression_against_model(
            "SUM(Sales[Amount]) + [Revenue]",
            model,
        )
        == []
    )
    issues = dax_ast.validate_expression_against_model(
        "Missing[Value] + Sales[Missing] + [ProductKey] + UnknownVariable",
        model,
    )
    assert {issue.code for issue in issues} == {
        dax_ast.ModelValidationCode.unknown_table,
        dax_ast.ModelValidationCode.unknown_member,
        dax_ast.ModelValidationCode.unknown_identifier,
        dax_ast.ModelValidationCode.ambiguous_reference,
    }

    query_issues = dax_ast.validate_query_against_model(
        "EVALUATE { Sales[Amount], [Revenue] }",
        model,
    )
    assert query_issues == []

    conflict = dax_ast.validate_expression_against_model(
        "VAR sales = 1 RETURN sales",
        model,
    )
    assert [issue.code for issue in conflict] == [dax_ast.ModelValidationCode.conflicting_variable_name]


def test_completed_static_validation_codes_are_exposed():
    cases = {
        'DATATABLE("A", INTEGER, {{"bad"}})': dax_ast.ValidationCode.invalid_datatable_value,
        "VAR _bad = 1 RETURN 1": dax_ast.ValidationCode.invalid_variable_name,
        "VAR a = 1 VAR A = 2 RETURN a": dax_ast.ValidationCode.duplicate_variable,
        "SUM(1)": dax_ast.ValidationCode.invalid_argument_type,
    }
    for source, expected in cases.items():
        assert expected in {issue.code for issue in dax_ast.validate_expression(source)}
