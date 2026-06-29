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


def test_unspaced_ratio_of_two_aggregates_is_derived():
    g = _parse("source: orders is duckdb.table('orders') extend {\n  measure: ratio1 is sum(amount)/sum(quantity)\n}\n")
    me = g.get_model("orders").get_metric("ratio1")
    assert me.agg is None
    assert me.type == "derived"
    assert me.sql == "sum(amount)/sum(quantity)"


def test_sum_over_count_is_derived():
    g = _parse("source: orders is duckdb.table('orders') extend {\n  measure: avg_ov is sum(amount) / count()\n}\n")
    me = g.get_model("orders").get_metric("avg_ov")
    assert me.agg is None
    assert me.type == "derived"
    # bare count() normalized to count(*) so the derived SQL is executable
    assert me.sql == "sum(amount) / count(*)"


def test_dot_method_aggregate_arithmetic_is_derived():
    g = _parse(
        "source: orders is duckdb.table('orders') extend {\n  measure: dotarith is cost.sum() / quantity.sum()\n}\n"
    )
    me = g.get_model("orders").get_metric("dotarith")
    assert me.agg is None
    assert me.type == "derived"
    # Malloy dot-method aggregates normalized to SQL function calls
    assert me.sql == "SUM(cost) / SUM(quantity)"


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


def test_join_many_unqualified_related_key():
    # Related FK unqualified, source PK qualified by the source name.
    g = _parse(
        "source: items is duckdb.table('i') extend { measure: ic is count() }\n"
        "source: orders is duckdb.table('o') extend {\n"
        "  primary_key: id\n"
        "  join_many: items is duckdb.table('i') on order_id = orders.id\n"
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


def test_join_both_unqualified_keeps_first_identifier():
    g = _parse(
        "source: customers is duckdb.table('c') extend { primary_key: id }\n"
        "source: orders is duckdb.table('o') extend {\n"
        "  primary_key: id\n"
        "  join_one: customers is duckdb.table('c') on customer_id = id\n"
        "}\n"
    )
    rel = {r.name: r for r in g.get_model("orders").relationships}["customers"]
    assert rel.foreign_key == "customer_id"


def test_export_agg_rewrite_ignores_string_literals():
    sql = "SUM(CASE WHEN label = 'COUNT(*)' THEN amount ELSE 0 END) / COUNT(*)"
    assert (
        MalloyAdapter()._sql_aggs_to_malloy(sql) == "sum(CASE WHEN label = 'COUNT(*)' THEN amount ELSE 0 END) / count()"
    )


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


# --- join_cross & export / roundtrip ---


def _export_text(graph):
    import tempfile

    out = tempfile.NamedTemporaryFile("w", suffix=".malloy", delete=False).name
    MalloyAdapter().export(graph, out)
    try:
        return Path(out).read_text()
    finally:
        Path(out).unlink(missing_ok=True)


def test_join_cross_maps_to_cross_type():
    g = _parse(
        "source: regions is duckdb.table('regions') extend { dimension: region_name is name }\n"
        "source: orders is duckdb.table('orders') extend {\n"
        "  primary_key: id\n"
        "  measure: order_count is count()\n"
        "  join_cross: regions\n"
        "}\n"
    )
    rel = {r.name: r for r in g.get_model("orders").relationships}["regions"]
    assert rel.type == "cross"
    assert rel.foreign_key is None


def test_cross_relationship_exports_without_key_clause():
    from sidemantic import Metric, Model
    from sidemantic.core.relationship import Relationship
    from sidemantic.core.semantic_graph import SemanticGraph

    g = SemanticGraph()
    g.add_model(Model(name="regions", table="regions", primary_key="id"))
    g.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[Metric(name="order_count", agg="count")],
            relationships=[Relationship(name="regions", type="cross")],
        )
    )
    text = _export_text(g)
    assert "join_cross: regions" in text
    assert "join_cross: regions with" not in text
    # Round-trips back to a cross relationship.
    import tempfile

    p = tempfile.NamedTemporaryFile("w", suffix=".malloy", delete=False)
    p.write(text)
    p.close()
    g2 = MalloyAdapter(warn_on_errors=False).parse(Path(p.name))
    Path(p.name).unlink(missing_ok=True)
    assert g2.get_model("orders").relationships[0].type == "cross"


def test_time_dimension_granularity_survives_roundtrip():
    from sidemantic import Dimension, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    g = SemanticGraph()
    g.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            dimensions=[Dimension(name="order_month", type="time", granularity="month", sql="created_at")],
        )
    )
    text = _export_text(g)
    assert "order_month is created_at.month" in text
    assert "rename:" not in text


def test_unsupported_metric_not_exported_as_count():
    from sidemantic import Metric, Model
    from sidemantic.core.semantic_graph import SemanticGraph

    g = SemanticGraph()
    g.add_model(
        Model(
            name="o",
            table="o",
            primary_key="id",
            metrics=[
                Metric(name="rt", type="cumulative", window_expression="sum(amount)"),
                Metric(name="c", agg="count"),
            ],
        )
    )
    text = _export_text(g)
    assert "rt is count()" not in text
    assert "c is count()" in text


def test_multiline_description_collapsed_to_one_line():
    from sidemantic import Model
    from sidemantic.core.semantic_graph import SemanticGraph

    g = SemanticGraph()
    g.add_model(Model(name="o", table="o", primary_key="id", description="Line one\nLine two"))
    text = _export_text(g)
    assert "# desc: Line one Line two" in text


def test_one_to_one_exports_as_join_one_not_cross():
    from sidemantic import Model
    from sidemantic.core.relationship import Relationship
    from sidemantic.core.semantic_graph import SemanticGraph

    g = SemanticGraph()
    g.add_model(Model(name="customers", table="c", primary_key="customer_id"))
    g.add_model(
        Model(
            name="orders",
            table="o",
            primary_key="id",
            relationships=[Relationship(name="customers", type="one_to_one", foreign_key="customer_id")],
        )
    )
    text = _export_text(g)
    assert "join_one: customers with customer_id" in text
    assert "join_cross: customers" not in text


# --- Import resolution must not silently drop sources ---


def _src(name):
    return f"source: {name} is duckdb.table('{name}.parquet') extend {{ primary_key: id  dimension: id is id }}\n"


def _models(path):
    return set(MalloyAdapter(warn_on_errors=False).parse(Path(path)).models.keys())


def test_named_import_in_chain_not_dropped(tmp_path):
    (tmp_path / "base.malloy").write_text(_src("alpha") + _src("beta"))
    (tmp_path / "x.malloy").write_text("import { alpha } from 'base.malloy'\n" + _src("x_src"))
    (tmp_path / "y.malloy").write_text("import 'x.malloy'\nimport { beta } from 'base.malloy'\n" + _src("y_src"))
    assert _models(tmp_path / "y.malloy") == {"alpha", "beta", "x_src", "y_src"}


def test_narrow_import_then_import_all(tmp_path):
    (tmp_path / "base.malloy").write_text(_src("alpha") + _src("beta"))
    (tmp_path / "root.malloy").write_text(
        "import { alpha } from 'base.malloy'\nimport 'base.malloy'\n" + _src("root_src")
    )
    assert _models(tmp_path / "root.malloy") == {"alpha", "beta", "root_src"}


def test_source_imported_under_two_aliases(tmp_path):
    (tmp_path / "base.malloy").write_text(
        "source: customers is duckdb.table('c.parquet') extend { primary_key: cid  dimension: cid is cid }\n"
    )
    (tmp_path / "dual.malloy").write_text(
        "import { customers is c1, customers is c2 } from 'base.malloy'\n" + _src("local_src")
    )
    assert _models(tmp_path / "dual.malloy") == {"c1", "c2", "local_src"}


def test_circular_imports_terminate_and_keep_both(tmp_path):
    (tmp_path / "a.malloy").write_text("import 'b.malloy'\n" + _src("a_src"))
    (tmp_path / "b.malloy").write_text("import 'a.malloy'\n" + _src("b_src"))
    assert _models(tmp_path / "a.malloy") == {"a_src", "b_src"}


def test_named_import_still_filters(tmp_path):
    (tmp_path / "base.malloy").write_text(_src("alpha") + _src("beta"))
    (tmp_path / "m.malloy").write_text("import { alpha } from 'base.malloy'\n" + _src("m_src"))
    assert _models(tmp_path / "m.malloy") == {"alpha", "m_src"}


# --- Code-review follow-ups ---


def test_regex_match_preserves_function_lhs():
    # A computed / parenthesised left operand must still become REGEXP_MATCHES.
    assert (
        _dim_sql("source: o is duckdb.table('o') extend {\n  dimension: a is lower(name) ~ r'foo'\n}\n", "a")
        == "REGEXP_MATCHES(lower(name), 'foo')"
    )
    assert (
        _dim_sql("source: o is duckdb.table('o') extend {\n  dimension: b is (name) ~ r'foo'\n}\n", "b")
        == "REGEXP_MATCHES((name), 'foo')"
    )


def test_pick_keyword_inside_string_literal_is_not_a_delimiter():
    sql = _dim_sql(
        "source: o is duckdb.table('o') extend {\n"
        "  dimension: h is pick 'x' when note = 'before else after' else 'y'\n"
        "}\n",
        "h",
    )
    assert sql == "CASE WHEN note = 'before else after' THEN 'x' ELSE 'y' END"


def test_many_to_many_exports_as_join_many():
    from sidemantic import Model
    from sidemantic.core.relationship import Relationship
    from sidemantic.core.semantic_graph import SemanticGraph

    g = SemanticGraph()
    g.add_model(Model(name="tags", table="t", primary_key="id"))
    g.add_model(
        Model(
            name="orders",
            table="o",
            primary_key="id",
            relationships=[Relationship(name="tags", type="many_to_many", foreign_key="tag_id")],
        )
    )
    text = _export_text(g)
    assert "join_many: tags with tag_id" in text
    assert "join_one: tags" not in text


def test_regex_match_handles_spaced_function_call():
    assert (
        _dim_sql("source: o is duckdb.table('o') extend {\n  dimension: a is lower (name) ~ r'foo'\n}\n", "a")
        == "REGEXP_MATCHES(lower (name), 'foo')"
    )


def test_regex_match_handles_quoted_paren_in_operand():
    # A ')' inside a string-literal argument must not break the operand balance scan.
    assert (
        _dim_sql("source: o is duckdb.table('o') extend {\n  dimension: a is replace(name, ')', '') ~ r'x'\n}\n", "a")
        == "REGEXP_MATCHES(replace(name, ')', ''), 'x')"
    )


def test_date_literal_dimension_is_time_not_numeric():
    g = _parse("source: o is duckdb.table('o') extend {\n  dimension: cutoff_date is @2024-01-01\n}\n")
    d = g.get_model("o").get_dimension("cutoff_date")
    assert d.type == "time"
    assert d.sql == "DATE '2024-01-01'"


def test_regex_match_backtick_operand_with_spaces():
    assert (
        _dim_sql("source: o is duckdb.table('o') extend {\n  dimension: a is `user name` ~ r'foo'\n}\n", "a")
        == "REGEXP_MATCHES(`user name`, 'foo')"
    )


def test_pick_escaped_quote_before_keyword_in_condition():
    sql = _dim_sql(
        "source: o is duckdb.table('o') extend {\n  dimension: b is pick 'x' when note = 'it\\'s else ok' else 'y'\n}\n",
        "b",
    )
    assert sql == "CASE WHEN note = 'it\\'s else ok' THEN 'x' ELSE 'y' END"


def test_regex_match_binary_operand():
    # The regex operand is the full arithmetic expression, not just the last token.
    assert (
        _dim_sql("source: o is duckdb.table('o') extend {\n  dimension: a is amount + tax ~ r'5'\n}\n", "a")
        == "REGEXP_MATCHES(amount + tax, '5')"
    )


def test_unspaced_dot_method_ratio_normalized():
    g = _parse("source: o is duckdb.table('o') extend {\n  measure: d is cost.sum()/quantity.sum()\n}\n")
    me = g.get_model("o").get_metric("d")
    assert me.type == "derived"
    assert me.sql == "SUM(cost)/SUM(quantity)"


def test_timestamp_literal_forms():
    def sql(lit):
        return _dim_sql(
            f"source: o is duckdb.table('o') extend {{\n  dimension: a is t > {lit}\n}}\n",
            "a",
        )

    # All forms pad to HH:MM:SS and normalize a comma fraction to a dot.
    assert sql("@2024-01-01 10") == "t > TIMESTAMP '2024-01-01 10:00:00'"
    assert sql("@2024-01-01 10:30") == "t > TIMESTAMP '2024-01-01 10:30:00'"
    assert sql("@2024-01-01 10:30:00.123") == "t > TIMESTAMP '2024-01-01 10:30:00.123'"
    assert sql("@2024-01-01 10:30:00,123") == "t > TIMESTAMP '2024-01-01 10:30:00.123'"
    assert sql("@2024-01-01 10:30:00[UTC]") == "t > TIMESTAMP '2024-01-01 10:30:00'"


def test_normalize_count_function_forms_to_distinct():
    def sql(expr):
        g = _parse(f"source: o is duckdb.table('o') extend {{\n  measure: m is {expr}\n}}\n")
        return g.get_model("o").get_metric("m").sql

    assert sql("count(user_id) / count()") == "COUNT(DISTINCT user_id) / count(*)"
    assert sql("count_distinct(user_id) / count()") == "COUNT(DISTINCT user_id) / count(*)"


def test_derived_count_measure_roundtrips_to_valid_malloy():
    # Stored SQL is query-valid; export converts the SQL aggregate forms back to
    # Malloy so a strict reparse of the exported file succeeds.
    import tempfile

    src = "source: o is duckdb.table('o') extend {\n  primary_key: id\n  measure: m is count(user_id) / count()\n}\n"
    g = _parse(src)
    assert g.get_model("o").get_metric("m").sql == "COUNT(DISTINCT user_id) / count(*)"
    out = tempfile.NamedTemporaryFile("w", suffix=".malloy", delete=False).name
    try:
        MalloyAdapter().export(g, out)
        text = Path(out).read_text()
        assert "m is count(user_id) / count()" in text
        # Re-parses strictly (valid Malloy, not SQL count(*)).
        MalloyAdapter(strict=True).parse(Path(out))
    finally:
        Path(out).unlink(missing_ok=True)


def test_pick_condition_with_nested_case():
    sql = _dim_sql(
        "source: o is duckdb.table('o') extend {\n"
        "  dimension: f is pick 'x' when case when a = 1 then 1 else 0 end = 1 else 'y'\n"
        "}\n",
        "f",
    )
    assert sql == "CASE WHEN case when a = 1 then 1 else 0 end = 1 THEN 'x' ELSE 'y' END"


def test_agg_normalization_ignores_string_literals():
    g = _parse(
        "source: o is duckdb.table('o') extend {\n"
        "  measure: m is sum(case when label = 'count()' then amount else 0 end) / sum(qty)\n"
        "}\n"
    )
    assert g.get_model("o").get_metric("m").sql == "sum(case when label = 'count()' then amount else 0 end) / sum(qty)"


def test_count_with_string_literal_argument_normalized():
    # A string literal inside the count() argument must not break the match.
    g = _parse(
        "source: o is duckdb.table('o') extend {\n"
        "  measure: m is count(case when plan = 'pro' then user_id end) / count()\n"
        "}\n"
    )
    assert g.get_model("o").get_metric("m").sql == "COUNT(DISTINCT case when plan = 'pro' then user_id end) / count(*)"


def test_count_with_backtick_field_containing_paren():
    g = _parse("source: o is duckdb.table('o') extend {\n  measure: m is count(`user(id`) / count()\n}\n")
    assert g.get_model("o").get_metric("m").sql == "COUNT(DISTINCT `user(id`) / count(*)"


def test_regex_match_case_expression_operand():
    assert (
        _dim_sql(
            "source: o is duckdb.table('o') extend {\n"
            "  dimension: a is case when flag then name else alt end ~ r'foo'\n"
            "}\n",
            "a",
        )
        == "REGEXP_MATCHES(case when flag then name else alt end, 'foo')"
    )


def test_export_does_not_distinctify_plain_count():
    # COUNT(field) has no faithful Malloy form (count(field) is distinct), so it
    # is left untouched; only COUNT(DISTINCT ...) / COUNT(*) translate.
    assert MalloyAdapter()._sql_aggs_to_malloy("COUNT(user_id) / COUNT(*)") == "COUNT(user_id) / count()"
    assert MalloyAdapter()._sql_aggs_to_malloy("COUNT(DISTINCT x) / SUM(y)") == "count(x) / sum(y)"


def test_normalize_spaced_backtick_aggregate_field():
    g = _parse("source: o is duckdb.table('o') extend {\n  measure: m is `cost amount`.sum() / count()\n}\n")
    me = g.get_model("o").get_metric("m")
    assert me.type == "derived"
    assert me.sql == "SUM(`cost amount`) / count(*)"


def test_normalize_dot_method_count_distinct():
    g = _parse("source: o is duckdb.table('o') extend {\n  measure: m is user_id.count_distinct() / count()\n}\n")
    me = g.get_model("o").get_metric("m")
    assert me.type == "derived"
    assert me.sql == "COUNT(DISTINCT user_id) / count(*)"


def test_join_on_condition_parenthesized_equality():
    g = _parse(
        "source: customers is duckdb.table('c') extend { primary_key: id }\n"
        "source: orders is duckdb.table('o') extend {\n"
        "  primary_key: id\n"
        "  join_one: customers is duckdb.table('c') on (customer_id = customers.id)\n"
        "}\n"
    )
    rel = {r.name: r for r in g.get_model("orders").relationships}["customers"]
    assert rel.foreign_key == "customer_id"


def test_join_on_condition_skips_literal_predicate():
    g = _parse(
        "source: customers is duckdb.table('c') extend { primary_key: id }\n"
        "source: orders is duckdb.table('o') extend {\n"
        "  primary_key: id\n"
        "  join_one: customers is duckdb.table('c') on customers.active = true and customer_id = customers.id\n"
        "}\n"
    )
    rel = {r.name: r for r in g.get_model("orders").relationships}["customers"]
    # The `customers.active = true` filter must not become the foreign key.
    assert rel.foreign_key == "customer_id"
    assert rel.metadata.get("composite_keys") is None
