from scripts.benchmark_semantic_sql_planner import run_benchmarks


def test_semantic_sql_planner_benchmark_proof_fields():
    results = run_benchmarks(row_count=10_000, iterations=1)

    assert results
    assert {result["name"] for result in results} >= {
        "wrapped_preaggregation",
        "aggregate_boundary_sum_rollup_preagg",
        "window_inner_preaggregation",
        "fanout_child_preaggregation",
        "linear_cte_chain_preaggregation",
        "multi_semantic_cte_island_preaggregation",
        "dimension_distinct_wrapper",
        "dimension_slicer_null_search_limit",
        "global_row_number_topn",
        "topn_pagination_preaggregation",
        "additive_total_union_preaggregation",
        "virtual_dataset_time_rls_preaggregation",
        "conditional_pivot_preaggregation",
        "time_expression_grain_rollup_preaggregation",
        "union_branch_semantic_islands_preaggregation",
        "projection_width_reduction_wide_key",
    }

    for result in results:
        assert result["rows_equal"] is True
        assert result["chosen_plan_matches"] is not False
        assert result["expected_rules_present"] is True
        assert result["expected_fragments_present"] is not False
        assert result["forbidden_fragments_absent"] is not False
        assert isinstance(result["sql_changed"], bool)
        assert result["baseline_ms"] >= 0
        assert result["optimized_ms"] >= 0

    performance_cases = [result for result in results if result["case_type"] == "performance"]
    assert performance_cases
    assert all(result["min_speedup"] is not None for result in performance_cases)
    assert all("speedup_floor_met" in result for result in performance_cases)
