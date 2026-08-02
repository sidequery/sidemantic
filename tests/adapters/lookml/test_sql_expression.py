from sidemantic import Dimension, Model
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.sql.lookml_expression import (
    fold_lookml_aggregate_filters,
    generator_column_nulling_suffices,
    lookml_expression_references_column,
    protect_lookml_sql,
    replace_lookml_placeholders,
    restore_outer_aggregate_all,
    rewrite_lookml_columns,
    strip_lookml_model_qualifiers,
    strip_outer_aggregate_all,
)


def _resolver(name: str, qualifiers: tuple[str, ...], quoted: bool) -> str | None:
    if quoted:
        return None
    if not qualifiers or qualifiers in (("{model}",), ("orders",)):
        return f"R({name})"
    return None


def test_protection_is_lexical_and_restores_lookml_fragments():
    sql = "{model}.status = {{ status }} AND '${TABLE}' = '${TABLE}' -- {model}\n"
    protected = protect_lookml_sql(sql)

    assert "{model}.status" not in protected.text
    assert "{{ status }}" not in protected.text
    assert "'${TABLE}' = '${TABLE}'" in protected.text
    assert "-- {model}" in protected.text
    assert protected.restore(protected.text) == sql


def test_placeholder_conversion_and_qualifier_stripping_are_lexical():
    sql = "{model}.status = '${TABLE}.{model}' -- ${TABLE}.{model}\nAND ${TABLE}.id > 0"

    assert replace_lookml_placeholders(sql, {"{model}": "${TABLE}"}) == (
        "${TABLE}.status = '${TABLE}.{model}' -- ${TABLE}.{model}\nAND ${TABLE}.id > 0"
    )
    assert strip_lookml_model_qualifiers(sql) == (
        "status = '${TABLE}.{model}' -- ${TABLE}.{model}\nAND ${TABLE}.id > 0"
    )


def test_column_rewrite_preserves_templates_literals_and_foreign_qualifiers():
    sql = "{model}.status = {{ status }} AND customers.status != 'status'"

    assert rewrite_lookml_columns(sql, _resolver, known_columns={"status"}) == (
        "R(status) = {{ status }} AND customers.status != 'status'"
    )


def test_dialect_candidates_disambiguate_date_trunc_without_reserializing():
    known = {"date", "month"}

    assert (
        rewrite_lookml_columns(
            "DATE_TRUNC(date, month) = DATE '2024-01-01'",
            _resolver,
            known_columns=known,
            time_columns=known,
        )
        == "DATE_TRUNC(R(date), month) = DATE '2024-01-01'"
    )
    assert (
        rewrite_lookml_columns(
            "DATE_TRUNC(month, date) = DATE '2024-01-01'",
            _resolver,
            known_columns=known,
            time_columns=known,
        )
        == "DATE_TRUNC(month, R(date)) = DATE '2024-01-01'"
    )


def test_numeric_trunc_keeps_scale_as_a_column():
    assert (
        rewrite_lookml_columns(
            "TRUNC(amount, month) > 0",
            _resolver,
            known_columns={"amount", "month"},
            time_columns=set(),
        )
        == "TRUNC(R(amount), R(month)) > 0"
    )


def test_datepart_function_is_ast_classified_and_spelling_is_preserved():
    assert (
        rewrite_lookml_columns(
            "DATENAME(day, created_at) = 'Monday'",
            _resolver,
            known_columns={"day", "created_at"},
            time_columns={"day", "created_at"},
        )
        == "DATENAME(day, R(created_at)) = 'Monday'"
    )


def test_fold_preserves_original_predicate_bytes():
    predicate = "`status` IS DISTINCT FROM DATE '2024-01-01' AND {{ user_filter }}"

    assert fold_lookml_aggregate_filters("COUNT(*)", [predicate], force=True) == (
        f"COUNT(CASE WHEN {predicate} THEN 1 END)"
    )


def test_fold_preserves_balanced_block_templated_predicate():
    predicate = "{% if include_pending %}status = 'completed'{% else %}1=1{% endif %}"

    assert fold_lookml_aggregate_filters("COUNT(*)", [predicate], force=True) == (
        f"COUNT(CASE WHEN {predicate} THEN 1 END)"
    )

    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
    )
    model_predicate = predicate.replace("status", "{model}.status", 1)
    assert LookMLAdapter._fold_filters_into_aggregate("SUM({model}.amount)", [model_predicate], model) == (
        "SUM(CASE WHEN ({% if include_pending %}${TABLE}.status = 'completed'"
        "{% else %}1=1{% endif %}) THEN ${TABLE}.amount END)"
    )


def test_fold_rejects_unbalanced_or_statement_breaking_block_templates():
    assert fold_lookml_aggregate_filters("COUNT(*)", ["{% if active %}status = 'active'"], force=True) is None
    assert (
        fold_lookml_aggregate_filters(
            "COUNT(*)",
            ["{% if active %}status = 'active'; DROP TABLE users{% endif %}"],
            force=True,
        )
        is None
    )
    assert (
        fold_lookml_aggregate_filters(
            "COUNT(*)",
            ["{% if active %}status = 'active' # trailing{% else %}1=1{% endif %}"],
            force=True,
        )
        is None
    )
    assert (
        fold_lookml_aggregate_filters(
            "COUNT(*)",
            ["{% if active %}status = 'active' #trailing{% else %}1=1{% endif %}"],
            force=True,
        )
        is None
    )


def test_fold_supports_lookml_condition_and_jinja_control_edge_cases():
    condition = "{% condition orders.status %}${status}{% endcondition %}"
    assert fold_lookml_aggregate_filters("COUNT(*)", [condition], force=True) == (
        f"COUNT(CASE WHEN {condition} THEN 1 END)"
    )

    liquid_comment = "{% comment %}{% if ignored %}not SQL{% endcomment %}x = 1"
    assert fold_lookml_aggregate_filters("COUNT(*)", [liquid_comment], force=True) == (
        f"COUNT(CASE WHEN {liquid_comment} THEN 1 END)"
    )

    inline_set = "{% set threshold = 10 %}x > threshold"
    assert fold_lookml_aggregate_filters("COUNT(*)", [inline_set], force=True) == (
        f"COUNT(CASE WHEN {inline_set} THEN 1 END)"
    )

    capture_set = "{% set value | replace('=', ':') %}x{% endset %}value = 'x'"
    assert fold_lookml_aggregate_filters("COUNT(*)", [capture_set], force=True) == (
        f"COUNT(CASE WHEN {capture_set} THEN 1 END)"
    )


def test_fold_does_not_treat_postgres_json_operators_as_comments():
    for predicate in (
        "payload #>> '{status}' = 'active'",
        "payload #> '{a,b}' IS NOT NULL",
        "(payload #- '{a,b}') IS NOT NULL",
    ):
        assert fold_lookml_aggregate_filters("COUNT(*)", [predicate], force=True) == (
            f"COUNT(CASE WHEN {predicate} THEN 1 END)"
        )

    temp_table_predicate = "EXISTS (SELECT 1 FROM #allowed WHERE #allowed.id = orders.id)"
    assert fold_lookml_aggregate_filters("COUNT(*)", [temp_table_predicate], force=True) == (
        f"COUNT(CASE WHEN {temp_table_predicate} THEN 1 END)"
    )

    comma_join_predicate = "EXISTS (SELECT 1 FROM #a, #b WHERE #a.id = #b.id)"
    assert fold_lookml_aggregate_filters("COUNT(*)", [comma_join_predicate], force=True) == (
        f"COUNT(CASE WHEN {comma_join_predicate} THEN 1 END)"
    )


def test_force_fold_rejects_dialect_aggregate_renames():
    assert fold_lookml_aggregate_filters("APPROX_COUNT_DISTINCT(x)", ["x > 0"], force=True) is None


def test_fold_renders_with_the_detected_bigquery_dialect():
    assert fold_lookml_aggregate_filters("SUM(`project.dataset.table`.x)", ["x > 0"], force=True) == (
        "SUM(CASE WHEN x > 0 THEN `project.dataset.table`.`x` END)"
    )
    assert fold_lookml_aggregate_filters("SUM(SAFE_CAST(x AS INT64))", ["x > 0"], force=True) == (
        "SUM(CASE WHEN x > 0 THEN SAFE_CAST(x AS INT64) END)"
    )


def test_fold_renders_with_the_detected_tsql_dialect():
    assert fold_lookml_aggregate_filters("SUM([x])", ["x > 0"], force=True) == ("SUM(CASE WHEN x > 0 THEN [x] END)")


def test_adapter_placeholder_conversion_does_not_change_literals():
    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
    )

    folded = LookMLAdapter._fold_filters_into_aggregate(
        "COUNT(CASE WHEN '${TABLE}' = '{model}' THEN x END)",
        ["{model}.status = '{model}.status'"],
        model,
    )

    assert folded is not None
    assert "${TABLE}.status" in folded
    assert "'{model}.status'" in folded
    assert "'${TABLE}' = '{model}'" in folded


def test_fold_rejects_predicate_comments_and_statement_terminators():
    assert fold_lookml_aggregate_filters("COUNT(*)", ["x > 0 -- trailing"], force=True) is None
    assert fold_lookml_aggregate_filters("COUNT(*)", ["x > 0 /* trailing */"], force=True) is None
    assert fold_lookml_aggregate_filters("COUNT(*)", ["x > 0;"], force=True) is None
    assert fold_lookml_aggregate_filters("COUNT(*)", ["x = '--' AND y = ';'"], force=True) is not None


def test_protection_and_filter_markers_cannot_collide_with_user_sql():
    sentinel_sql = "__sidemantic_lookml_fragment_model_0__.status = {model}.status"
    assert rewrite_lookml_columns(sentinel_sql, _resolver, known_columns={"status"}) == (
        "__sidemantic_lookml_fragment_model_0__.status = R(status)"
    )
    assert (
        fold_lookml_aggregate_filters(
            "COUNT(__sidemantic_lookml_fragment_filter_0_0__)",
            ["x > 0"],
            force=True,
        )
        == "COUNT(CASE WHEN x > 0 THEN __sidemantic_lookml_fragment_filter_0_0__ END)"
    )
    assert (
        fold_lookml_aggregate_filters(
            "COUNT('__sidemantic_lookml_fragment_filter_0_0__')",
            ["x > 0"],
            force=True,
        )
        == "COUNT(CASE WHEN x > 0 THEN '__sidemantic_lookml_fragment_filter_0_0__' END)"
    )


def test_analysis_ignores_protected_fragments_as_columns():
    template_only = "COUNT(CASE WHEN {{ user_filter }} THEN 1 END)"

    assert not lookml_expression_references_column(template_only)
    assert not generator_column_nulling_suffices(template_only)
    assert generator_column_nulling_suffices("SUM({model}.amount)")


def test_outer_all_modifier_uses_tokens_and_does_not_move_nested_all():
    assert strip_outer_aggregate_all("COUNT(ALL {model}.x)") == ("COUNT({model}.x)", True)
    assert strip_outer_aggregate_all("COUNT(all)") == ("COUNT(all)", False)
    assert strip_outer_aggregate_all("ABS(COUNT(ALL x))") == ("ABS(COUNT(ALL x))", False)
    assert restore_outer_aggregate_all("COUNT(CASE WHEN x > 0 THEN x END)") == ("COUNT(ALL CASE WHEN x > 0 THEN x END)")
