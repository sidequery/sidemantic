from sidemantic.sql.fragment import replace_outside_sql_protected


def test_standard_sql_trailing_backslash_does_not_swallow_executable_sql():
    sql = r"'C:\' || orders.total"

    assert replace_outside_sql_protected(sql, "orders.total", "{model}.total") == r"'C:\' || {model}.total"
