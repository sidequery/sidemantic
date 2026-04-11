"""Tests for Tableau formula translation."""

from sidemantic.adapters.tableau import _translate_formula


def test_field_reference():
    sql, ok = _translate_formula("[Amount]")
    assert ok
    assert sql == "Amount"


def test_multiple_field_references():
    sql, ok = _translate_formula("[price] * [quantity]")
    assert ok
    assert "price" in sql
    assert "quantity" in sql
    assert "*" in sql


def test_zn():
    sql, ok = _translate_formula("ZN([discount])")
    assert ok
    assert "COALESCE" in sql
    assert "0" in sql


def test_ifnull():
    sql, ok = _translate_formula("IFNULL([x], 0)")
    assert ok
    assert "COALESCE" in sql


def test_iif():
    sql, ok = _translate_formula("IIF([x] > 0, [x], 0)")
    assert ok
    assert "CASE WHEN" in sql


def test_if_then_else():
    sql, ok = _translate_formula("IF [x] > 0 THEN 'yes' ELSE 'no' END")
    assert ok
    assert "CASE WHEN" in sql


def test_contains():
    sql, ok = _translate_formula("CONTAINS([name], 'test')")
    assert ok
    assert "LIKE" in sql


def test_datetrunc():
    sql, ok = _translate_formula("DATETRUNC('month', [order_date])")
    assert ok
    assert "DATE_TRUNC" in sql


def test_countd():
    sql, ok = _translate_formula("COUNTD([user_id])")
    assert ok
    assert "COUNT(DISTINCT" in sql


def test_len():
    sql, ok = _translate_formula("LEN([name])")
    assert ok
    assert "LENGTH" in sql


def test_int_cast():
    sql, ok = _translate_formula("INT([x])")
    assert ok
    assert "CAST" in sql
    assert "INTEGER" in sql


def test_float_cast():
    sql, ok = _translate_formula("FLOAT([x])")
    assert ok
    assert "CAST" in sql
    assert "DOUBLE" in sql


def test_str_cast():
    sql, ok = _translate_formula("STR([x])")
    assert ok
    assert "CAST" in sql
    assert "VARCHAR" in sql


def test_lod_not_translated():
    sql, ok = _translate_formula("{FIXED [customer_id] : SUM([amount])}")
    assert not ok


def test_table_calc_not_translated():
    sql, ok = _translate_formula("RUNNING_SUM(SUM([amount]))")
    assert not ok


def test_nested_formula():
    sql, ok = _translate_formula("ZN(IFNULL([x], [y]))")
    assert ok
    assert "COALESCE" in sql


def test_none_input():
    assert _translate_formula(None) == (None, True)


def test_plain_arithmetic():
    sql, ok = _translate_formula("[price] * [quantity]")
    assert ok
    assert "price" in sql
    assert "quantity" in sql


def test_lod_include():
    sql, ok = _translate_formula("{INCLUDE [region] : AVG([sales])}")
    assert not ok


def test_lod_exclude():
    sql, ok = _translate_formula("{EXCLUDE [region] : SUM([sales])}")
    assert not ok


def test_window_calc_not_translated():
    sql, ok = _translate_formula("WINDOW_AVG(SUM([amount]), -3, 0)")
    assert not ok


def test_lookup_not_translated():
    sql, ok = _translate_formula("LOOKUP(SUM([sales]), -1)")
    assert not ok


def test_lod_with_space():
    """LOD with space between { and FIXED."""
    sql, ok = _translate_formula("{ FIXED [customer_id] : SUM([amount]) }")
    assert not ok


def test_int_nested_parens():
    """INT() with nested function call."""
    sql, ok = _translate_formula("INT(ROUND([x]))")
    assert ok
    assert sql == "CAST(ROUND(x) AS INTEGER)"


def test_str_nested_parens():
    """STR() with nested function call."""
    sql, ok = _translate_formula("STR(NOW())")
    assert ok
    assert sql == "CAST(NOW() AS VARCHAR)"


def test_float_nested_parens():
    """FLOAT() with nested function call."""
    sql, ok = _translate_formula("FLOAT(ABS([x]))")
    assert ok
    assert sql == "CAST(ABS(x) AS DOUBLE)"


def test_field_ref_inside_string_literal():
    """Brackets inside string literals are NOT field references."""
    sql, ok = _translate_formula("REGEXP_REPLACE(STR(NOW()), '[^a-zA-Z0-9]', '')")
    assert ok
    assert "'[^a-zA-Z0-9]'" in sql


def test_qualified_field_ref():
    """[table].[column] extracts just column."""
    sql, ok = _translate_formula("[orders].[amount] + 1")
    assert ok
    assert sql == "amount + 1"


def test_field_ref_with_spaces_quoted():
    """Field refs with spaces are quoted as identifiers."""
    sql, ok = _translate_formula("[Extracts Incremented At]")
    assert ok
    assert sql == '"Extracts Incremented At"'


def test_parameter_field_ref_with_spaces_quoted():
    """Qualified field refs keep the leaf name and quote it when needed."""
    sql, ok = _translate_formula("[Parameters].[Parameter 1]")
    assert ok
    assert sql == '"Parameter 1"'


def test_countd_nested():
    """COUNTD with nested expression."""
    sql, ok = _translate_formula("COUNTD(IF [status] = 'active' THEN [user_id] END)")
    assert ok
    assert "COUNT(DISTINCT" in sql


def test_ismemberof_not_translated():
    """ISMEMBEROF is Tableau-only, should be flagged as untranslatable."""
    sql, ok = _translate_formula("ISMEMBEROF('Admin')")
    assert not ok


def test_username_not_translated():
    """USERNAME() is Tableau-only."""
    sql, ok = _translate_formula("USERNAME()")
    assert not ok


def test_isnull():
    """ISNULL(x) -> (x IS NULL)."""
    sql, ok = _translate_formula("ISNULL([has_extract])")
    assert ok
    assert "IS NULL" in sql
    assert "has_extract" in sql


def test_double_quoted_strings():
    """Double-quoted string literals converted to single quotes."""
    sql, ok = _translate_formula('IF [x] THEN "Selected" ELSE "Not Selected" END')
    assert ok
    assert "x" in sql
    assert "'Selected'" in sql
    assert "'Not Selected'" in sql
    assert '"Selected"' not in sql
    assert '"Not Selected"' not in sql


def test_comment_stripped():
    """// comments are stripped before translation."""
    sql, ok = _translate_formula("// Don't notify if alert fails\n[status]")
    assert ok
    assert "status" in sql
    assert "//" not in sql


def test_isnull_in_iif():
    """ISNULL inside IIF."""
    sql, ok = _translate_formula("IIF(ISNULL([x]), 0, [x])")
    assert ok
    assert "IS NULL" in sql
    assert "CASE WHEN" in sql


def test_string_concat_plus_to_pipes():
    """Tableau + string concat becomes SQL ||."""
    sql, ok = _translate_formula("[prefix] + '://' + [suffix]")
    assert ok
    assert "||" in sql
    assert "+" not in sql


def test_arithmetic_plus_preserved():
    """Arithmetic + is NOT converted to ||."""
    sql, ok = _translate_formula("[x] + [y]")
    assert ok
    assert "+" in sql
    assert "||" not in sql


def test_dateadd():
    """DATEADD('unit', n, date) -> date_add(date, INTERVAL (n) unit)."""
    sql, ok = _translate_formula("DATEADD('hour', 3, [created_at])")
    assert ok
    assert "date_add" in sql
    assert "INTERVAL" in sql
    assert "hour" in sql
    assert "created_at" in sql


def test_dateadd_with_field_amount():
    """DATEADD with field reference as amount."""
    sql, ok = _translate_formula("DATEADD('day', [offset], [start_date])")
    assert ok
    assert "date_add" in sql
    assert "offset" in sql
    assert "start_date" in sql


def test_mid():
    """MID() -> SUBSTRING()."""
    sql, ok = _translate_formula("MID([name], 2, 5)")
    assert ok
    assert "SUBSTRING(" in sql


def test_find():
    """FIND() -> STRPOS()."""
    sql, ok = _translate_formula("FIND([name], 'test')")
    assert ok
    assert "STRPOS(" in sql


def test_startswith():
    """STARTSWITH() -> STARTS_WITH()."""
    sql, ok = _translate_formula("STARTSWITH([url], 'https')")
    assert ok
    assert "STARTS_WITH(" in sql


def test_endswith():
    """ENDSWITH() -> ENDS_WITH()."""
    sql, ok = _translate_formula("ENDSWITH([file], '.csv')")
    assert ok
    assert "ENDS_WITH(" in sql


def test_char():
    """CHAR() -> CHR()."""
    sql, ok = _translate_formula("CHAR(65)")
    assert ok
    assert "CHR(" in sql


def test_makedate():
    """MAKEDATE() -> MAKE_DATE()."""
    sql, ok = _translate_formula("MAKEDATE(2024, 1, 15)")
    assert ok
    assert "MAKE_DATE(" in sql


def test_comment_preserves_url_in_string():
    """// inside a string literal is NOT a comment."""
    sql, ok = _translate_formula("[prefix] + '://' + [suffix]")
    assert ok
    assert "://" in sql
    assert "prefix" in sql
    assert "suffix" in sql


def test_iif_with_nested_function():
    """IIF with nested function calls containing commas."""
    sql, ok = _translate_formula("IIF([x] > 0, DATEADD('day', 1, [d]), [d])")
    assert ok
    assert "CASE WHEN" in sql
    assert "date_add" in sql
    assert "ELSE" in sql


def test_iif_simple_still_works():
    """Basic IIF still translates correctly."""
    sql, ok = _translate_formula("IIF([active], 'yes', 'no')")
    assert ok
    assert "CASE WHEN" in sql
    assert "'yes'" in sql
    assert "'no'" in sql


def test_escaped_quote_in_string():
    """Doubled single quotes (escaped apostrophe) handled correctly."""
    sql, ok = _translate_formula("IIF([x]=1, 'O''Reilly', 'no')")
    assert ok
    assert "CASE WHEN" in sql
    assert "O''Reilly" in sql


def test_escaped_quote_before_comment_marker():
    """Escaped quote before // inside a string is preserved."""
    sql, ok = _translate_formula("'O''Reilly // keep' + [x]")
    assert ok
    assert "O''Reilly // keep" in sql


def test_escaped_quote_before_bracket_in_string():
    """Escaped apostrophe before bracket text inside a string literal is preserved."""
    sql, ok = _translate_formula("IIF([a]=1, 'It''s [x]', 'n')")
    assert ok
    assert "It''s [x]" in sql
    # [x] inside the string should NOT be treated as a field reference
    assert "CASE WHEN" in sql


def test_dateadd_nested_args():
    """DATEADD with nested function containing commas."""
    sql, ok = _translate_formula("DATEADD('day', IFNULL([offset], 1), [start_date])")
    assert ok
    assert "date_add" in sql
    assert "start_date" in sql
    assert "COALESCE" in sql
    assert "day" in sql


def test_contains_nested_args():
    """CONTAINS with nested function containing commas."""
    sql, ok = _translate_formula("CONTAINS(IFNULL([name], 'a,b'), 'x')")
    assert ok
    assert "LIKE" in sql
    assert "COALESCE" in sql
    assert "'x'" in sql


def test_nested_if_blocks():
    """Nested IF/THEN/ELSE/END blocks are fully translated."""
    sql, ok = _translate_formula("IF [x]=1 THEN IF [y]=2 THEN 'a' ELSE 'b' END ELSE 'c' END")
    assert ok
    assert "IF" not in sql.upper().replace("CASE WHEN", "")
    assert sql.upper().count("CASE WHEN") == 2
