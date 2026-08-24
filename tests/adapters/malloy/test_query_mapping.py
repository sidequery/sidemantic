from __future__ import annotations

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.adapters.malloy_queries import map_malloy_query, map_malloy_view


def _layer() -> SemanticLayer:
    layer = SemanticLayer(auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            dimensions=[
                Dimension(name="state", type="categorical", sql="state"),
            ],
            metrics=[Metric(name="revenue", agg="sum", sql="revenue")],
        )
    )
    return layer


def test_maps_single_stage_query_to_compilable_native_consumption_objects():
    mapping = map_malloy_query(
        "revenue_by_state",
        """revenue_by_state is orders -> {
          group_by: state
          aggregate: revenue
          where: state != 'deleted'
          having: revenue > 10
          order_by: revenue desc
          limit: 5
        }""",
        dimensions={"state"},
        metrics={"revenue"},
        parameters={"currency": "USD"},
    )

    assert mapping.supported
    assert mapping.explore is not None
    assert mapping.saved_query is not None
    assert mapping.explore.model == "orders"
    assert mapping.saved_query.dimensions == ["state"]
    assert mapping.saved_query.metrics == ["revenue"]
    assert mapping.saved_query.filters == ["state != 'deleted'", "revenue > 10"]
    assert mapping.saved_query.order_by == ["revenue desc"]
    assert mapping.saved_query.limit == 5
    assert mapping.saved_query.parameters == {"currency": "USD"}

    layer = _layer()
    layer.add_explore(mapping.explore)
    layer.add_saved_query(mapping.saved_query)
    sql = layer.compile(saved_query="revenue_by_state")

    assert "GROUP BY" in sql
    assert "state <> 'deleted'" in sql
    assert "HAVING" in sql
    assert "LIMIT 5" in sql


def test_maps_source_local_view_and_classifies_select_fields():
    mapping = map_malloy_view(
        "order_projection",
        "orders",
        "{ select: state, revenue order_by: state asc limit: 20 }",
        dimensions={"state"},
        metrics={"revenue"},
    )

    assert mapping.supported
    assert mapping.saved_query is not None
    assert mapping.saved_query.dimensions == ["state"]
    assert mapping.saved_query.metrics == ["revenue"]


def test_rejects_pipeline_refinement_nesting_and_calculation_with_codes():
    cases = {
        "pipeline": (
            "pipeline is orders -> { group_by: state } -> { aggregate: revenue }",
            "malloy_query_pipeline_unsupported",
        ),
        "refinement": (
            "refinement is orders + { group_by: state }",
            "malloy_query_refinement_unsupported",
        ),
        "nesting": (
            "nesting is orders -> { nest: detail is { select: state } }",
            "malloy_query_nest_unsupported",
        ),
        "calculation": (
            "calculation is orders -> { calculate: share is revenue / 100 }",
            "malloy_query_calculate_unsupported",
        ),
    }

    for name, (definition, code) in cases.items():
        mapping = map_malloy_query(name, definition)
        assert not mapping.supported
        assert mapping.diagnostics[0].code == code


def test_rejects_query_semantics_without_native_equivalent():
    cases = {
        "sampled": "sampled is orders -> { sample: 10% aggregate: revenue }",
        "indexed": "indexed is orders -> { index: state }",
        "zoned": "zoned is orders -> { timezone: 'America/Los_Angeles' aggregate: revenue }",
        "joined": "joined is orders -> { join_one: users on user_id = users.id aggregate: revenue }",
    }

    for name, definition in cases.items():
        mapping = map_malloy_query(name, definition)
        assert not mapping.supported
        assert mapping.diagnostics
        assert mapping.diagnostics[0].status == "rejected"


def test_rejects_rename_wildcard_ambiguous_select_and_source_arguments():
    renamed = map_malloy_query("renamed", "renamed is orders -> { group_by: region is state }")
    wildcard = map_malloy_view("wildcard", "orders", "{ select: * }")
    ambiguous = map_malloy_view("ambiguous", "orders", "{ select: mystery }")
    arguments = map_malloy_query("arguments", "arguments is orders(currency is 'USD') -> { aggregate: revenue }")

    assert renamed.diagnostics[0].code == "malloy_query_rename_unsupported"
    assert wildcard.diagnostics[0].code == "malloy_query_select_wildcard_unsupported"
    assert ambiguous.diagnostics[0].code == "malloy_query_select_field_ambiguous"
    assert arguments.diagnostics[0].code == "malloy_query_source_arguments_unsupported"


def test_rejects_malloy_only_filter_expression_and_mismatched_name():
    given_filter = map_malloy_query(
        "given_filter",
        "given_filter is orders -> { group_by: state where: state = $requested_state }",
        dimensions={"state"},
    )
    mismatched = map_malloy_query("expected", "actual is orders -> { group_by: state }")

    assert given_filter.diagnostics[0].code == "malloy_query_filter_expression_unsupported"
    assert mismatched.diagnostics[0].code == "malloy_query_name_mismatch"


def test_rejects_clause_shapes_native_generator_would_reclassify():
    metric_where = map_malloy_query(
        "metric_where",
        "metric_where is orders -> { aggregate: revenue where: revenue > 10 }",
        metrics={"revenue"},
    )
    dimension_having = map_malloy_query(
        "dimension_having",
        "dimension_having is orders -> { group_by: state having: state != 'CA' }",
        dimensions={"state"},
    )
    unselected_order = map_malloy_query(
        "unselected_order",
        "unselected_order is orders -> { aggregate: revenue order_by: state }",
        dimensions={"state"},
        metrics={"revenue"},
    )
    empty = map_malloy_query("empty", "empty is orders -> { }")

    assert metric_where.diagnostics[0].code == "malloy_query_metric_where_unsupported"
    assert dimension_having.diagnostics[0].code == "malloy_query_dimension_having_unsupported"
    assert unselected_order.diagnostics[0].code == "malloy_query_order_field_unselected"
    assert empty.diagnostics[0].code == "malloy_query_selection_required"


def test_group_by_and_aggregate_validate_known_semantic_roles():
    swapped_group = map_malloy_query(
        "swapped_group",
        "swapped_group is orders -> { group_by: revenue }",
        dimensions={"state"},
        metrics={"revenue"},
    )
    swapped_aggregate = map_malloy_query(
        "swapped_aggregate",
        "swapped_aggregate is orders -> { aggregate: state }",
        dimensions={"state"},
        metrics={"revenue"},
    )
    unknown = map_malloy_query(
        "unknown",
        "unknown is orders -> { group_by: mystery aggregate: revenue }",
        dimensions={"state"},
        metrics={"revenue"},
    )

    assert swapped_group.diagnostics[0].code == "malloy_query_group_by_wrong_role"
    assert swapped_aggregate.diagnostics[0].code == "malloy_query_aggregate_wrong_role"
    assert unknown.diagnostics[0].code == "malloy_query_group_by_field_unknown"


def test_filters_classify_all_known_metrics_not_only_selected_metrics():
    metric_where = map_malloy_query(
        "metric_where",
        "metric_where is orders -> { group_by: state where: revenue > 10 }",
        dimensions={"state"},
        metrics={"revenue"},
    )
    unselected_metric_having = map_malloy_query(
        "unselected_metric_having",
        "unselected_metric_having is orders -> { group_by: state having: revenue > 10 }",
        dimensions={"state"},
        metrics={"revenue"},
    )
    unknown_filter = map_malloy_query(
        "unknown_filter",
        "unknown_filter is orders -> { group_by: state where: mystery = 1 }",
        dimensions={"state"},
        metrics={"revenue"},
    )

    assert metric_where.diagnostics[0].code == "malloy_query_metric_where_unsupported"
    assert unselected_metric_having.supported
    assert unknown_filter.diagnostics[0].code == "malloy_query_filter_field_unknown"


def test_duplicate_group_by_and_aggregate_entries_are_rejected():
    duplicate_group = map_malloy_query(
        "duplicate_group",
        "duplicate_group is orders -> { group_by: state, state aggregate: revenue }",
        dimensions={"state"},
        metrics={"revenue"},
    )
    duplicate_aggregate = map_malloy_query(
        "duplicate_aggregate",
        "duplicate_aggregate is orders -> { group_by: state aggregate: revenue, revenue }",
        dimensions={"state"},
        metrics={"revenue"},
    )

    assert any(diagnostic.code == "malloy_query_duplicate_group_by" for diagnostic in duplicate_group.diagnostics)
    assert any(diagnostic.code == "malloy_query_duplicate_aggregate" for diagnostic in duplicate_aggregate.diagnostics)


def test_qualified_paths_preserve_identity_and_reject_wrong_qualifier():
    distinct_paths = map_malloy_query(
        "distinct_paths",
        "distinct_paths is orders -> { group_by: customers.state, suppliers.state }",
        dimensions={"customers.state", "suppliers.state"},
    )
    wrong_qualifier = map_malloy_query(
        "wrong_qualifier",
        "wrong_qualifier is orders -> { group_by: suppliers.state }",
        dimensions={"customers.state"},
    )
    ambiguous_leaf = map_malloy_query(
        "ambiguous_leaf",
        "ambiguous_leaf is orders -> { group_by: state }",
        dimensions={"customers.state", "suppliers.state"},
    )
    wrong_filter_qualifier = map_malloy_query(
        "wrong_filter_qualifier",
        "wrong_filter_qualifier is orders -> { group_by: customers.state where: suppliers.state = 'CA' }",
        dimensions={"customers.state"},
    )

    assert distinct_paths.supported
    assert distinct_paths.saved_query is not None
    assert distinct_paths.saved_query.dimensions == ["customers.state", "suppliers.state"]
    assert wrong_qualifier.diagnostics[0].code == "malloy_query_group_by_field_unknown"
    assert ambiguous_leaf.diagnostics[0].code == "malloy_query_group_by_field_ambiguous"
    assert any(
        diagnostic.code == "malloy_query_filter_field_unknown" for diagnostic in wrong_filter_qualifier.diagnostics
    )


def test_unqualified_filter_requires_one_typed_semantic_identity():
    same_role = map_malloy_query(
        "same_role",
        "same_role is orders -> { group_by: orders.state where: id = 1 }",
        dimensions={"orders.state", "orders.id", "users.id"},
    )
    cross_role = map_malloy_query(
        "cross_role",
        "cross_role is orders -> { group_by: orders.state where: score > 0 }",
        dimensions={"orders.state", "score"},
        metrics={"score"},
    )

    for mapping in (same_role, cross_role):
        assert not mapping.supported
        assert mapping.explore is None
        assert mapping.saved_query is None
        assert any(diagnostic.code == "malloy_query_filter_field_ambiguous" for diagnostic in mapping.diagnostics)


def test_select_rejects_cross_role_collision_and_qualified_refs_require_exact_path():
    select_collision = map_malloy_view(
        "select_collision",
        "orders",
        "{ select: score }",
        dimensions={"score"},
        metrics={"score"},
    )
    arbitrary_qualifier = map_malloy_query(
        "arbitrary_qualifier",
        "arbitrary_qualifier is orders -> { group_by: users.state }",
        dimensions={"state"},
    )

    assert select_collision.diagnostics[0].code == "malloy_query_select_field_ambiguous"
    assert arbitrary_qualifier.diagnostics[0].code == "malloy_query_group_by_field_unknown"


def test_order_by_requires_unique_selected_full_path():
    ambiguous_order = map_malloy_query(
        "ambiguous_order",
        "ambiguous_order is orders -> { group_by: customers.state, suppliers.state order_by: state }",
        dimensions={"customers.state", "suppliers.state"},
    )
    exact_order = map_malloy_query(
        "exact_order",
        "exact_order is orders -> { group_by: customers.state order_by: state }",
        dimensions={"customers.state"},
    )

    assert ambiguous_order.diagnostics[0].code == "malloy_query_order_field_ambiguous"
    assert exact_order.supported
