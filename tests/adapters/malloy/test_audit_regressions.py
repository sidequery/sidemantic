"""Regression tests for correctness bugs found in the Malloy adapter audit.

Each test encodes the corrected behavior for a confirmed bug. Inputs are parsed
through MalloyAdapter end-to-end so the assertions exercise the real visitor.
"""

import tempfile
from pathlib import Path

from sidemantic.adapters.malloy import MalloyAdapter


def _parse(src: str):
    with tempfile.NamedTemporaryFile("w", suffix=".malloy", delete=False) as f:
        f.write(src)
        path = f.name
    try:
        return MalloyAdapter(warn_on_errors=False).parse(Path(path))
    finally:
        Path(path).unlink(missing_ok=True)


# --- Aggregate-arithmetic measures must stay intact as derived (was: mangled) ---


def test_ratio_of_two_aggregates_is_derived():
    g = _parse(
        "source: orders is duckdb.table('orders') extend {\n  measure: ratio1 is sum(amount) / sum(quantity)\n}\n"
    )
    me = g.get_model("orders").get_metric("ratio1")
    assert me.agg is None
    assert me.type == "derived"
    assert me.sql == "sum(amount) / sum(quantity)"


def test_sum_over_count_is_derived():
    g = _parse("source: orders is duckdb.table('orders') extend {\n  measure: avg_ov is sum(amount) / count()\n}\n")
    me = g.get_model("orders").get_metric("avg_ov")
    assert me.agg is None
    assert me.type == "derived"
    assert me.sql == "sum(amount) / count()"


def test_dot_method_aggregate_arithmetic_is_derived():
    g = _parse(
        "source: orders is duckdb.table('orders') extend {\n  measure: dotarith is cost.sum() / quantity.sum()\n}\n"
    )
    me = g.get_model("orders").get_metric("dotarith")
    assert me.agg is None
    assert me.type == "derived"
    assert me.sql == "cost.sum() / quantity.sum()"


def test_single_aggregates_still_parse():
    """Guard: the compound-expression fix must not change single-call measures."""
    g = _parse(
        "source: orders is duckdb.table('orders') extend {\n"
        "  measure: plain is sum(amount)\n"
        "  measure: sum_expr is sum(quantity * price)\n"
        "  measure: cnt is count()\n"
        "  measure: cntd is count(user_id)\n"
        "  measure: dotcnt is event_params.value.double_value.sum()\n"
        "}\n"
    )
    m = g.get_model("orders")
    assert (m.get_metric("plain").agg, m.get_metric("plain").sql) == ("sum", "amount")
    assert (m.get_metric("sum_expr").agg, m.get_metric("sum_expr").sql) == ("sum", "quantity * price")
    assert (m.get_metric("cnt").agg, m.get_metric("cnt").sql) == ("count", None)
    assert (m.get_metric("cntd").agg, m.get_metric("cntd").sql) == ("count_distinct", "user_id")
    assert (m.get_metric("dotcnt").agg, m.get_metric("dotcnt").sql) == ("sum", "event_params.value.double_value")


# --- ?? null-coalesce must not split inside string literals ---


def test_null_coalesce_preserves_string_literal():
    g = _parse("source: o is duckdb.table('o') extend {\n  dimension: co is note ?? 'x ?? y'\n}\n")
    assert g.get_model("o").get_dimension("co").sql == "COALESCE(note, 'x ?? y')"


def test_null_coalesce_chain_still_works():
    g = _parse(
        "source: o is duckdb.table('o') extend {\n  dimension: d is primary_value ?? secondary_value ?? 'default'\n}\n"
    )
    assert g.get_model("o").get_dimension("d").sql == "COALESCE(primary_value, secondary_value, 'default')"


# --- Dimension typing & granularity ---


def test_trailing_timeframe_infers_time_and_granularity():
    g = _parse(
        "source: o is duckdb.table('o') extend {\n"
        "  dimension: order_month is created_at.month\n"
        "  dimension: order_day is shipped.day\n"
        "}\n"
    )
    m = g.get_model("o")
    om = m.get_dimension("order_month")
    assert om.type == "time"
    assert om.granularity == "month"
    assert m.get_dimension("order_day").granularity == "day"


def test_comparison_expression_not_overridden_to_time_by_name():
    g = _parse(
        "source: o is duckdb.table('o') extend {\n  dimension: created_after_cutoff is created_at > @2020-01-01\n}\n"
    )
    assert g.get_model("o").get_dimension("created_after_cutoff").type == "boolean"


# --- Chained filter refinements must AND, not drop ---


def test_chained_where_keeps_all_filters_and_aggregation():
    g = _parse(
        "source: o is duckdb.table('o') extend {\n  measure: f_two is count() { where: a > 1 } { where: b > 2 }\n}\n"
    )
    me = g.get_model("o").get_metric("f_two")
    assert me.agg == "count"
    assert me.filters == ["a > 1", "b > 2"]


def test_single_filtered_measure_still_works():
    g = _parse("source: o is duckdb.table('o') extend {\n  measure: big is sum(amount) { where: amount > 100 }\n}\n")
    me = g.get_model("o").get_metric("big")
    assert me.agg == "sum"
    assert me.sql == "amount"
    assert me.filters == ["amount > 100"]


# --- Join on-condition FK extraction is direction-aware ---


def test_join_one_on_condition_target_qualified_left():
    g = _parse(
        "source: customers is duckdb.table('c') extend { primary_key: id }\n"
        "source: orders is duckdb.table('o') extend {\n"
        "  primary_key: id\n"
        "  join_one: customers is duckdb.table('c') on customers.id = customer_id\n"
        "}\n"
    )
    rel = {r.name: r for r in g.get_model("orders").relationships}["customers"]
    assert rel.type == "many_to_one"
    assert rel.foreign_key == "customer_id"


def test_join_many_on_condition_uses_related_key():
    g = _parse(
        "source: items is duckdb.table('i') extend { measure: ic is count() }\n"
        "source: orders is duckdb.table('o') extend {\n"
        "  primary_key: id\n"
        "  join_many: items is duckdb.table('i') on orders.id = items.order_id\n"
        "}\n"
    )
    rel = {r.name: r for r in g.get_model("orders").relationships}["items"]
    assert rel.type == "one_to_many"
    assert rel.foreign_key == "order_id"


def test_join_composite_keys_reverse_direction():
    g = _parse(
        "source: cohort is duckdb.table('co') extend { primary_key: id }\n"
        "source: orders is duckdb.table('o') extend {\n"
        "  primary_key: id\n"
        "  join_one: cohort is duckdb.table('co') on cohort.gender = gender and cohort.state = state\n"
        "}\n"
    )
    rel = {r.name: r for r in g.get_model("orders").relationships}["cohort"]
    assert rel.foreign_key == "gender"
    assert rel.metadata.get("composite_keys") == ["gender", "state"]


# --- & and-tree must not split inside string literals ---


def _dim_sql(src, name, model="o"):
    return _parse(src).get_model(model).get_dimension(name).sql


def test_and_tree_preserves_string_literal():
    sql = _dim_sql("source: o is duckdb.table('o') extend {\n  dimension: amp is label = 'A & B'\n}\n", "amp")
    assert sql == "label = 'A & B'"


def test_and_tree_still_expands_top_level():
    sql = _dim_sql("source: o is duckdb.table('o') extend {\n  dimension: r is value < 2031 & > -8000\n}\n", "r")
    assert sql == "value < 2031 AND value > -8000"


# --- regex ~ must consume only its own field operand ---


def test_regex_match_does_not_swallow_left_context():
    sql = _dim_sql("source: o is duckdb.table('o') extend {\n  dimension: a is amount = 1 and name ~ r'foo'\n}\n", "a")
    assert sql == "amount = 1 and REGEXP_MATCHES(name, 'foo')"


def test_multiple_regex_matches_stay_separate():
    sql = _dim_sql("source: o is duckdb.table('o') extend {\n  dimension: b is name ~ r'x' and code ~ r'y'\n}\n", "b")
    assert sql == "REGEXP_MATCHES(name, 'x') and REGEXP_MATCHES(code, 'y')"


def test_single_regex_match_still_works():
    sql = _dim_sql(
        "source: o is duckdb.table('o') extend {\n  dimension: c is user_email ~ r'gserviceaccount'\n}\n", "c"
    )
    assert sql == "REGEXP_MATCHES(user_email, 'gserviceaccount')"


# --- pick/when/else on a single line must produce a well-formed CASE ---


def test_single_line_pick_produces_valid_case():
    sql = _dim_sql(
        "source: o is duckdb.table('o') extend {\n  dimension: tier is pick 'lo' when amount < 5 else 'hi'\n}\n",
        "tier",
    )
    assert sql == "CASE WHEN amount < 5 THEN 'lo' ELSE 'hi' END"


def test_single_line_apply_pick_produces_valid_case():
    sql = _dim_sql(
        "source: o is duckdb.table('o') extend {\n"
        "  dimension: bucket is status ? pick 'lo' when < 5 pick 'hi' when 'ASW' else 'x'\n"
        "}\n",
        "bucket",
    )
    assert sql == "CASE WHEN status < 5 THEN 'lo' WHEN status = 'ASW' THEN 'hi' ELSE 'x' END"


def test_apply_pick_with_regex_conditions():
    sql = _dim_sql(
        "source: o is duckdb.table('o') extend {\n"
        "  dimension: faang is title ?\n"
        "      pick 'Facebook' when ~ r'(Facebook|Instagram)'\n"
        "      pick 'Apple' when ~ r'(Apple|iPhone)'\n"
        "      else 'OTHER'\n"
        "}\n",
        "faang",
    )
    assert sql == (
        "CASE WHEN REGEXP_MATCHES(title, '(Facebook|Instagram)') THEN 'Facebook' "
        "WHEN REGEXP_MATCHES(title, '(Apple|iPhone)') THEN 'Apple' ELSE 'OTHER' END"
    )


# --- @date literal carrying a time component ---


def test_datetime_literal_becomes_timestamp():
    sql = _dim_sql(
        "source: o is duckdb.table('o') extend {\n  dimension: f is created_at > @2024-01-01 10:30:00\n}\n", "f"
    )
    assert sql == "created_at > TIMESTAMP '2024-01-01 10:30:00'"
