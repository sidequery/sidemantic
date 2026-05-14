from __future__ import annotations

import pytest
import sidemantic_dax

from sidemantic.dax import (
    DaxTranslationError,
    RelationshipEdge,
    translate_dax_metric,
    translate_dax_query,
    translate_dax_scalar,
    translate_dax_table,
)


def _parse_expression(expr: str):
    try:
        return sidemantic_dax.parse_expression(expr)
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise


def _parse_query(query: str):
    try:
        return sidemantic_dax.parse_query(query)
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise


def _sales_maps():
    column_sql_by_table = {
        "sales": {
            "amount": "amount",
            "product_key": "product_key",
            "quantity": "quantity",
            "order_date": "order_date",
        }
    }
    measure_names_by_table = {"sales": {"Total Sales"}}
    time_dimensions_by_table = {"sales": {"order_date"}}
    return column_sql_by_table, measure_names_by_table, time_dimensions_by_table


def test_translate_calculate_with_filter():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), 'sales'[product_key] = 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_metric_reference_uses_measure_metadata():
    expr = _parse_expression("[Total Sales]")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={"sales": {"Total Sales": "sum"}},
        measure_sql_by_table={"sales": {"Total Sales": "amount"}},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.source_table == "sales"


def test_translate_scalar_allows_multi_table_when_model_is_none():
    expr = _parse_expression("'Sales'[Amount] + 'Tax'[Rate]")
    sql = translate_dax_scalar(
        expr,
        model_name=None,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Tax": {"Rate": "Rate"},
        },
    )

    assert sql == "(Sales.Amount + Tax.Rate)"


def test_translate_calculate_filter_argument_propagates_table_filters():
    expr = _parse_expression(
        "CALCULATE("
        "SUM('sales'[amount]), "
        "FILTER(CALCULATETABLE('sales', 'sales'[quantity] = 2), 'sales'[product_key] = 1)"
        ")"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(quantity = 2)", "(product_key = 1)"]


def test_translate_calculate_keepfilters_filter_argument_preserves_inherited_filter():
    expr = _parse_expression(
        "CALCULATE("
        "CALCULATE(SUM('sales'[amount]), 'sales'[product_key] = 3), "
        "KEEPFILTERS(FILTER(CALCULATETABLE('sales', 'sales'[quantity] = 2), 'sales'[product_key] = 1))"
        ")"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(product_key = 3)", "(quantity = 2)", "(product_key = 1)"]


def test_translate_derived_countrows_table_expression_keeps_filters():
    expr = _parse_expression("DIVIDE(COUNTROWS(CALCULATETABLE('sales', 'sales'[product_key] = 1)), COUNTROWS('sales'))")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert "COUNT(CASE WHEN (product_key = 1) THEN 1 END)" in translation.sql
    assert "COUNT(*)" in translation.sql


def test_translate_sumx_row_expression():
    expr = _parse_expression("SUMX('sales', 'sales'[amount] * 'sales'[quantity])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "sum"
    assert translation.sql == "(amount * quantity)"


def test_translate_sumx_filter_all_expression():
    expr = _parse_expression("SUMX(FILTER(ALL('sales'), 'sales'[product_key] = 1), 'sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_averagex_cross_table_row_expression():
    expr = _parse_expression("AVERAGEX('sales', 'sales'[amount] * 'products'[weight])")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount", "order_date": "order_date"},
            "products": {"weight": "weight"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "avg"
    assert translation.sql == "(amount * products.weight)"
    assert "products" in translation.required_models


def test_translate_median_aggregate_expression():
    expr = _parse_expression("MEDIAN('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "median"
    assert translation.sql == "amount"


def test_translate_medianx_row_expression():
    expr = _parse_expression("MEDIANX('sales', 'sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "median"
    assert translation.sql == "amount"


def test_translate_sumx_topn_table_expression():
    expr = _parse_expression("SUMX(TOPN(10, 'sales', 'sales'[amount], DESC), 'sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == []


def test_translate_sumx_selectcolumns_filter_expression():
    expr = _parse_expression(
        "SUMX(SELECTCOLUMNS(FILTER('sales', 'sales'[product_key] = 1), \"Amount\", 'sales'[amount]), 'sales'[amount])"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_sumx_addcolumns_filter_expression():
    expr = _parse_expression("SUMX(ADDCOLUMNS(FILTER('sales', 'sales'[product_key] = 1), \"X\", 1), 'sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_sumx_topn_over_filtered_table_expression():
    expr = _parse_expression(
        "SUMX(TOPN(5, FILTER('sales', 'sales'[product_key] = 1), 'sales'[amount], DESC), 'sales'[amount])"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_avgx_topn_over_filtered_table_expression():
    expr = _parse_expression(
        "AVGX(TOPN(5, FILTER('sales', 'sales'[product_key] = 1), 'sales'[amount], DESC), 'sales'[amount])"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "avg"
    assert translation.sql == "amount"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_maxx_row_expression():
    expr = _parse_expression("MAXX('sales', 'sales'[amount] * 'sales'[quantity])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "max"
    assert translation.sql == "(amount * quantity)"


def test_translate_countx_filtered_table_expression():
    expr = _parse_expression("COUNTX(FILTER('sales', 'sales'[product_key] = 1), 'sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "count"
    assert translation.sql == "amount"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_countax_filtered_table_expression():
    expr = _parse_expression("COUNTAX(FILTER('sales', 'sales'[product_key] = 1), 'sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "count"
    assert translation.sql == "amount"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_totalytd_cumulative():
    expr = _parse_expression("TOTALYTD(SUM('sales'[amount]), 'sales'[order_date])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "cumulative"
    assert translation.grain_to_date == "year"
    assert translation.agg == "sum"
    assert translation.sql == "amount"


def test_translate_totalytd_preserves_inherited_filters():
    expr = _parse_expression("TOTALYTD(CALCULATE(SUM('sales'[amount]), 'sales'[product_key] = 1), 'sales'[order_date])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "cumulative"
    assert translation.grain_to_date == "year"
    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_totalytd_filter_arg_replaces_inherited_filter():
    expr = _parse_expression(
        "TOTALYTD("
        "CALCULATE(SUM('sales'[amount]), 'sales'[product_key] = 1), "
        "'sales'[order_date], "
        "'sales'[product_key] = 2"
        ")"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "cumulative"
    assert translation.filters == ["(product_key = 2)"]


def test_translate_totalytd_ignores_year_end_literal_arg():
    expr = _parse_expression("TOTALYTD(SUM('sales'[amount]), 'sales'[order_date], \"6/30\")")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "cumulative"
    assert translation.filters == []


def test_translate_totalmtd_cumulative_with_filter_arg():
    expr = _parse_expression("TOTALMTD(SUM('sales'[amount]), 'sales'[order_date], 'sales'[product_key] = 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "cumulative"
    assert translation.grain_to_date == "month"
    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_totalwtd_cumulative_with_filter_arg():
    expr = _parse_expression("TOTALWTD(SUM('sales'[amount]), 'sales'[order_date], 'sales'[product_key] = 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "cumulative"
    assert translation.grain_to_date == "week"
    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_totalytd_cross_table_time_column_and_table_filter_candidate():
    expr = _parse_expression("TOTALYTD(SUM('sales'[amount]), 'date'[date_key], 'date')")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount", "date_key": "date_key"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"date_key"}, "date": {"date_key"}},
    )

    assert translation.type == "cumulative"
    assert translation.grain_to_date == "year"
    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.window_order == "date.date_key"
    assert translation.filters == []
    assert "date" in translation.required_models


def test_translate_totalqtd_preserves_inherited_filters():
    expr = _parse_expression("TOTALQTD(CALCULATE(SUM('sales'[amount]), 'sales'[quantity] = 2), 'sales'[order_date])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "cumulative"
    assert translation.grain_to_date == "quarter"
    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(quantity = 2)"]


def test_translate_calculate_sameperiodlastyear():
    expr = _parse_expression("CALCULATE([Total Sales], SAMEPERIODLASTYEAR('sales'[order_date]))")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "time_comparison"
    assert translation.base_metric == "sales.Total Sales"
    assert translation.comparison_type == "yoy"
    assert translation.calculation == "previous_value"


def test_translate_calculate_sameperiodlastyear_cross_table_time_column():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), SAMEPERIODLASTYEAR('date'[date_key]))")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount", "date_key": "date_key"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"date_key"}, "date": {"date_key"}},
    )

    assert translation.type == "time_comparison"
    assert translation.inline_base_agg == "sum"
    assert translation.inline_base_sql == "amount"
    assert translation.comparison_type == "yoy"
    assert translation.calculation == "previous_value"
    assert translation.window_order == "date.date_key"
    assert "date" in translation.required_models


def test_translate_calculate_datesytd_as_cumulative():
    expr = _parse_expression("CALCULATE([Total Sales], DATESYTD('sales'[order_date]))")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={"sales": {"Total Sales": "sum"}},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "cumulative"
    assert translation.sql == "sales.Total Sales"
    assert translation.agg == "sum"
    assert translation.grain_to_date == "year"


def test_translate_calculate_datesmtd_inline_agg_as_cumulative():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), DATESMTD('sales'[order_date]))")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "cumulative"
    assert translation.sql == "amount"
    assert translation.agg == "sum"
    assert translation.grain_to_date == "month"


def test_translate_calculate_dateswtd_inline_agg_as_cumulative():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), DATESWTD('sales'[order_date]))")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "cumulative"
    assert translation.sql == "amount"
    assert translation.agg == "sum"
    assert translation.grain_to_date == "week"


def test_translate_calculate_datesinperiod_inline_agg_as_cumulative_window():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), DATESINPERIOD('sales'[order_date], MAX('sales'[order_date]), -3, MONTH))"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "cumulative"
    assert translation.sql == "amount"
    assert translation.agg == "sum"
    assert translation.window == "3 month"


def test_translate_calculate_datesinperiod_measure_ref_as_cumulative_window():
    expr = _parse_expression(
        "CALCULATE([Total Sales], DATESINPERIOD('sales'[order_date], MAX('sales'[order_date]), -1, YEAR))"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={"sales": {"Total Sales": "sum"}},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "cumulative"
    assert translation.sql == "sales.Total Sales"
    assert translation.agg == "sum"
    assert translation.window == "1 year"


def test_translate_calculate_datesinperiod_positive_inline_agg_as_forward_cumulative_window():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), DATESINPERIOD('sales'[order_date], MAX('sales'[order_date]), 3, MONTH))"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "cumulative"
    assert translation.sql == "amount"
    assert translation.agg == "sum"
    assert translation.window == "3 month following"


def test_translate_calculate_keepfilters_datesqtd_with_additional_filter():
    expr = _parse_expression(
        "CALCULATE([Total Sales], KEEPFILTERS(DATESQTD('sales'[order_date])), 'sales'[product_key] = 1)"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={"sales": {"Total Sales": "sum"}},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "cumulative"
    assert translation.sql == "sales.Total Sales"
    assert translation.agg == "sum"
    assert translation.grain_to_date == "quarter"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_calculate_sameperiodlastyear_with_inline_aggregate_base():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), SAMEPERIODLASTYEAR('sales'[order_date]))")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "time_comparison"
    assert translation.base_metric is None
    assert translation.inline_base_agg == "sum"
    assert translation.inline_base_sql == "amount"
    assert translation.comparison_type == "yoy"
    assert translation.calculation == "previous_value"


def test_translate_calculate_sameperiodlastyear_with_additional_filter():
    expr = _parse_expression(
        "CALCULATE([Total Sales], SAMEPERIODLASTYEAR('sales'[order_date]), 'sales'[product_key] = 1)"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "time_comparison"
    assert translation.base_metric == "sales.Total Sales"
    assert translation.comparison_type == "yoy"
    assert translation.calculation == "previous_value"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_calculate_time_intelligence_allows_additional_time_filter_function():
    expr = _parse_expression(
        "CALCULATE([Total Sales], SAMEPERIODLASTYEAR('sales'[order_date]), DATESYTD('sales'[order_date]))"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "time_comparison"
    assert translation.base_metric == "sales.Total Sales"
    assert translation.comparison_type == "yoy"
    assert translation.calculation == "previous_value"
    assert translation.filters
    assert any("order_date" in clause for clause in translation.filters or [])


def test_translate_calculate_dateadd_with_additional_filter():
    expr = _parse_expression(
        "CALCULATE([Total Sales], DATEADD('sales'[order_date], -1, YEAR), 'sales'[product_key] = 1)"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "time_comparison"
    assert translation.base_metric == "sales.Total Sales"
    assert translation.comparison_type == "yoy"
    assert translation.calculation == "previous_value"
    assert translation.time_offset == "1 year"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_calculate_dateadd_forward_offset_sets_negative_time_offset():
    expr = _parse_expression(
        "CALCULATE([Total Sales], DATEADD('sales'[order_date], 1, YEAR), 'sales'[product_key] = 1)"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "time_comparison"
    assert translation.base_metric == "sales.Total Sales"
    assert translation.comparison_type == "yoy"
    assert translation.calculation == "previous_value"
    assert translation.time_offset == "-1 year"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_calculate_dateadd_plural_unit_normalizes_to_yoy():
    expr = _parse_expression(
        "CALCULATE([Total Sales], DATEADD('sales'[order_date], -1, YEARS), 'sales'[product_key] = 1)"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "time_comparison"
    assert translation.comparison_type == "yoy"
    assert translation.calculation == "previous_value"
    assert translation.time_offset == "1 year"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_calculate_parallelperiod_with_additional_filter():
    expr = _parse_expression(
        "CALCULATE([Total Sales], PARALLELPERIOD('sales'[order_date], -1, YEAR), 'sales'[product_key] = 1)"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "time_comparison"
    assert translation.base_metric == "sales.Total Sales"
    assert translation.comparison_type == "yoy"
    assert translation.calculation == "previous_value"
    assert translation.time_offset == "1 year"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_calculate_previousyear_with_additional_filter():
    expr = _parse_expression("CALCULATE([Total Sales], PREVIOUSYEAR('sales'[order_date]), 'sales'[product_key] = 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "time_comparison"
    assert translation.base_metric == "sales.Total Sales"
    assert translation.comparison_type == "yoy"
    assert translation.calculation == "previous_value"
    assert translation.time_offset == "1 year"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_calculate_nextmonth_sets_negative_time_offset():
    expr = _parse_expression("CALCULATE([Total Sales], NEXTMONTH('sales'[order_date]), 'sales'[product_key] = 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "time_comparison"
    assert translation.base_metric == "sales.Total Sales"
    assert translation.comparison_type == "mom"
    assert translation.calculation == "previous_value"
    assert translation.time_offset == "-1 month"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_calculate_keepfilters_sameperiodlastyear_with_additional_filter():
    expr = _parse_expression(
        "CALCULATE([Total Sales], KEEPFILTERS(SAMEPERIODLASTYEAR('sales'[order_date])), 'sales'[product_key] = 1)"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "time_comparison"
    assert translation.base_metric == "sales.Total Sales"
    assert translation.comparison_type == "yoy"
    assert translation.calculation == "previous_value"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_calculate_keepfilters_dateadd_with_additional_filter():
    expr = _parse_expression(
        "CALCULATE([Total Sales], KEEPFILTERS(DATEADD('sales'[order_date], -1, YEAR)), 'sales'[product_key] = 1)"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "time_comparison"
    assert translation.base_metric == "sales.Total Sales"
    assert translation.comparison_type == "yoy"
    assert translation.calculation == "previous_value"
    assert translation.time_offset == "1 year"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_summarizecolumns_table():
    expr = _parse_expression("SUMMARIZECOLUMNS('sales'[product_key], \"Revenue\", SUM('sales'[amount]))")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert (
        translation.sql
        == "SELECT sales.product_key, SUM(sales.amount) AS Revenue FROM sales GROUP BY sales.product_key"
    )


def test_translate_summarize_table():
    expr = _parse_expression("SUMMARIZE('sales', 'sales'[product_key], \"Revenue\", SUM('sales'[amount]))")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert (
        translation.sql
        == "SELECT sales.product_key, SUM(sales.amount) AS Revenue FROM sales GROUP BY sales.product_key"
    )


def test_translate_summarize_wrapped_row_group_by_bracket_alias():
    expr = _parse_expression('SUMMARIZE(ROW("x", 1, "y", 2), [x])')
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={},
        measure_names_by_table={},
    )

    assert translation.sql == "SELECT x FROM (SELECT 1 AS x, 2 AS y) AS t GROUP BY x"


def test_translate_summarize_wrapped_multitable_row_group_by_bracket_alias():
    expr = _parse_expression("SUMMARIZE(ROW(\"x\", 'sales'[amount], \"d\", 'date'[date_key]), [x])")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
    )

    assert (
        "SELECT x FROM (SELECT amount AS x, date.date_key AS d FROM sales CROSS JOIN date) AS t GROUP BY x"
        in translation.sql
    )
    assert "sales" in translation.required_models
    assert "date" in translation.required_models


def test_translate_summarize_wrapped_row_group_by_identifier_alias():
    expr = _parse_expression('SUMMARIZE(ROW("x", 1, "y", 2), x)')
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={},
        measure_names_by_table={},
    )

    assert translation.sql == "SELECT x FROM (SELECT 1 AS x, 2 AS y) AS t GROUP BY x"


def test_translate_summarizecolumns_rollupgroup_table():
    expr = _parse_expression("SUMMARIZECOLUMNS(ROLLUPGROUP('sales'[product_key]), \"Rows\", COUNTROWS('sales'))")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT sales.product_key, COUNT(*) AS Rows FROM sales GROUP BY sales.product_key"


def test_translate_summarizecolumns_rollupaddissubtotal_table():
    expr = _parse_expression(
        "SUMMARIZECOLUMNS(ROLLUPADDISSUBTOTAL('sales'[product_key], \"is_subtotal\"), \"Rows\", COUNTROWS('sales'))"
    )
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT sales.product_key, COUNT(*) AS Rows FROM sales GROUP BY sales.product_key"


def test_translate_summarizecolumns_all_filter_table_arg():
    expr = _parse_expression("SUMMARIZECOLUMNS('sales'[product_key], ALL('sales'), \"Rows\", COUNTROWS('sales'))")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT sales.product_key, COUNT(*) AS Rows FROM sales GROUP BY sales.product_key"


def test_translate_summarizecolumns_values_filter_table_arg():
    expr = _parse_expression("SUMMARIZECOLUMNS('sales'[product_key], VALUES('sales'), \"Rows\", COUNTROWS('sales'))")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT sales.product_key, COUNT(*) AS Rows FROM sales GROUP BY sales.product_key"


def test_translate_summarizecolumns_rollupissubtotal_table():
    expr = _parse_expression("SUMMARIZECOLUMNS(ROLLUPISSUBTOTAL('sales'[product_key]), \"Rows\", COUNTROWS('sales'))")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT sales.product_key, COUNT(*) AS Rows FROM sales GROUP BY sales.product_key"


def test_translate_summarize_rollup_table():
    expr = _parse_expression("SUMMARIZE('sales', ROLLUP('sales'[product_key]), \"Rows\", COUNTROWS('sales'))")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT sales.product_key, COUNT(*) AS Rows FROM sales GROUP BY sales.product_key"


def test_translate_selectcolumns_table_keeps_base_columns_in_scope():
    expr = _parse_expression("SELECTCOLUMNS('sales', \"Amount\", 'sales'[amount])")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT amount AS Amount FROM sales"


def test_translate_selectcolumns_table_with_cross_table_expression_joins_related_table():
    expr = _parse_expression("SELECTCOLUMNS('sales', \"Weight\", 'products'[weight])")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"product_key": "product_key"},
            "products": {"product_key": "product_key", "weight": "weight"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        relationship_edges=[
            RelationshipEdge(
                from_table="sales",
                from_column="product_key",
                to_table="products",
                to_column="product_key",
            )
        ],
    )

    assert (
        translation.sql
        == "SELECT products.weight AS Weight FROM sales LEFT JOIN products ON sales.product_key = products.product_key"
    )
    assert "products" in translation.required_models


def test_translate_addcolumns_table_with_cross_table_expression_joins_related_table():
    expr = _parse_expression("ADDCOLUMNS('sales', \"Weight\", 'products'[weight])")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount", "product_key": "product_key"},
            "products": {"product_key": "product_key", "weight": "weight"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        relationship_edges=[
            RelationshipEdge(
                from_table="sales",
                from_column="product_key",
                to_table="products",
                to_column="product_key",
            )
        ],
    )

    assert (
        translation.sql
        == "SELECT sales.*, products.weight AS Weight FROM sales LEFT JOIN products ON sales.product_key = products.product_key"
    )
    assert "products" in translation.required_models


def test_translate_selectcolumns_wrapped_base_with_cross_table_expression_joins_related_table():
    expr = _parse_expression("SELECTCOLUMNS(FILTER('sales', 'sales'[amount] > 0), \"Category\", 'products'[category])")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount", "product_key": "product_key"},
            "products": {"product_key": "product_key", "category": "category"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        relationship_edges=[
            RelationshipEdge(
                from_table="sales",
                from_column="product_key",
                to_table="products",
                to_column="product_key",
            )
        ],
    )

    assert (
        translation.sql == "SELECT products.category AS Category FROM (SELECT * FROM sales WHERE (amount > 0)) AS t "
        "LEFT JOIN products ON t.product_key = products.product_key"
    )
    assert "products" in translation.required_models


def test_translate_addcolumns_wrapped_base_with_cross_table_expression_cross_joins_when_unrelated():
    expr = _parse_expression("ADDCOLUMNS(FILTER('sales', 'sales'[amount] > 0), \"Rate\", 'tax'[rate])")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "tax": {"rate": "rate"},
        },
        measure_names_by_table={"sales": set(), "tax": set()},
    )

    assert (
        translation.sql
        == "SELECT t.*, tax.rate AS Rate FROM (SELECT * FROM sales WHERE (amount > 0)) AS t CROSS JOIN tax"
    )
    assert "tax" in translation.required_models


def test_translate_topn_base_table_with_cross_table_order_by_joins_related_table():
    expr = _parse_expression("TOPN(2, 'sales', 'products'[weight], DESC)")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"product_key": "product_key"},
            "products": {"product_key": "product_key", "weight": "weight"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        relationship_edges=[
            RelationshipEdge(
                from_table="sales",
                from_column="product_key",
                to_table="products",
                to_column="product_key",
            )
        ],
    )

    assert (
        translation.sql == "SELECT sales.* FROM sales LEFT JOIN products ON sales.product_key = products.product_key "
        "ORDER BY products.weight DESC LIMIT 2"
    )
    assert "products" in translation.required_models


def test_translate_topn_accepts_scalar_count_expression():
    expr = _parse_expression("TOPN(1 + 1, 'sales', 'sales'[amount], DESC)")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={"sales": {"amount": "amount"}},
        measure_names_by_table={"sales": set()},
    )

    assert "ORDER BY amount DESC" in translation.sql
    assert "LIMIT CAST(((1 + 1)) AS BIGINT)" in translation.sql


def test_translate_topnskip_base_table_with_cross_table_order_by_cross_joins_when_unrelated():
    expr = _parse_expression("TOPNSKIP(2, 1, 'sales', 'tax'[rate], ASC)")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "tax": {"rate": "rate"},
        },
        measure_names_by_table={"sales": set(), "tax": set()},
    )

    assert translation.sql == "SELECT sales.* FROM sales CROSS JOIN tax ORDER BY tax.rate ASC LIMIT 2 OFFSET 1"
    assert "tax" in translation.required_models


def test_translate_topnskip_accepts_scalar_count_and_skip_expressions():
    expr = _parse_expression("TOPNSKIP(3 - 1, 5 / 2, 'sales', 'sales'[amount], DESC)")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={"sales": {"amount": "amount"}},
        measure_names_by_table={"sales": set()},
    )

    assert "ORDER BY amount DESC" in translation.sql
    assert "LIMIT CAST(((3 - 1)) AS BIGINT)" in translation.sql
    assert "OFFSET CAST(((5 / 2)) AS BIGINT)" in translation.sql


def test_translate_topn_wrapped_base_with_cross_table_order_by_joins_related_table():
    expr = _parse_expression("TOPN(2, FILTER('sales', 'sales'[amount] > 0), 'products'[weight], DESC)")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount", "product_key": "product_key"},
            "products": {"product_key": "product_key", "weight": "weight"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        relationship_edges=[
            RelationshipEdge(
                from_table="sales",
                from_column="product_key",
                to_table="products",
                to_column="product_key",
            )
        ],
    )

    assert (
        translation.sql
        == "SELECT t.* FROM (SELECT * FROM sales WHERE (amount > 0)) AS t LEFT JOIN products ON t.product_key = products.product_key "
        "ORDER BY products.weight DESC LIMIT 2"
    )
    assert "products" in translation.required_models


def test_translate_topnperlevel_base_table_with_cross_table_group_order_cross_joins_when_unrelated():
    expr = _parse_expression("TOPNPERLEVEL(1, 'tax'[region], 'sales', 'tax'[rate], DESC)")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "tax": {"region": "region", "rate": "rate"},
        },
        measure_names_by_table={"sales": set(), "tax": set()},
    )

    assert (
        translation.sql
        == "SELECT * EXCLUDE (__topnperlevel_rank) FROM (SELECT sales.*, RANK() OVER (PARTITION BY tax.region "
        "ORDER BY tax.rate DESC) AS __topnperlevel_rank FROM sales CROSS JOIN tax) AS q "
        "WHERE __topnperlevel_rank <= 1"
    )
    assert "tax" in translation.required_models


def test_translate_topnperlevel_wrapped_base_with_cross_table_group_order_joins_related_table():
    expr = _parse_expression(
        "TOPNPERLEVEL(1, 'products'[category], FILTER('sales', 'sales'[amount] > 0), 'products'[weight], DESC)"
    )
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount", "product_key": "product_key"},
            "products": {"product_key": "product_key", "category": "category", "weight": "weight"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        relationship_edges=[
            RelationshipEdge(
                from_table="sales",
                from_column="product_key",
                to_table="products",
                to_column="product_key",
            )
        ],
    )

    assert (
        translation.sql
        == "SELECT * EXCLUDE (__topnperlevel_rank) FROM (SELECT t.*, RANK() OVER (PARTITION BY products.category "
        "ORDER BY products.weight DESC) AS __topnperlevel_rank FROM (SELECT * FROM sales WHERE (amount > 0)) AS t "
        "LEFT JOIN products ON t.product_key = products.product_key) AS q WHERE __topnperlevel_rank <= 1"
    )
    assert "products" in translation.required_models


def test_translate_filter_table_keeps_base_columns_in_scope():
    expr = _parse_expression("FILTER('sales', 'sales'[amount] > 100)")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT * FROM sales WHERE (amount > 100)"


def test_translate_filter_table_with_cross_table_predicate_joins_related_table():
    expr = _parse_expression("FILTER('sales', 'products'[weight] > 0)")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount", "product_key": "product_key"},
            "products": {"weight": "weight", "product_key": "product_key"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        relationship_edges=[
            RelationshipEdge(
                from_table="sales",
                from_column="product_key",
                to_table="products",
                to_column="product_key",
            )
        ],
    )

    assert (
        translation.sql == "SELECT sales.* FROM sales LEFT JOIN products ON sales.product_key = products.product_key "
        "WHERE (products.weight > 0)"
    )
    assert "products" in translation.required_models


def test_translate_filter_table_with_cross_table_predicate_cross_joins_when_unrelated():
    expr = _parse_expression("FILTER('sales', 'tax'[rate] > 0)")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount", "product_key": "product_key"},
            "tax": {"rate": "rate"},
        },
        measure_names_by_table={"sales": set(), "tax": set()},
    )

    assert translation.sql == "SELECT sales.* FROM sales CROSS JOIN tax WHERE (tax.rate > 0)"
    assert "tax" in translation.required_models


def test_translate_filter_wrapped_base_with_cross_table_predicate_joins_related_table():
    expr = _parse_expression("FILTER(FILTER('sales', 'sales'[amount] > 0), 'products'[category] = \"Clothing\")")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount", "product_key": "product_key"},
            "products": {"product_key": "product_key", "category": "category"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        relationship_edges=[
            RelationshipEdge(
                from_table="sales",
                from_column="product_key",
                to_table="products",
                to_column="product_key",
            )
        ],
    )

    assert (
        translation.sql
        == "SELECT t.* FROM (SELECT * FROM sales WHERE (amount > 0)) AS t LEFT JOIN products ON t.product_key = products.product_key "
        "WHERE (products.category = 'Clothing')"
    )
    assert "products" in translation.required_models


def test_translate_filter_wrapped_base_with_cross_table_predicate_cross_joins_when_unrelated():
    expr = _parse_expression("FILTER(FILTER('sales', 'sales'[amount] > 0), 'tax'[rate] > 0)")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "tax": {"rate": "rate"},
        },
        measure_names_by_table={"sales": set(), "tax": set()},
    )

    assert (
        translation.sql
        == "SELECT t.* FROM (SELECT * FROM sales WHERE (amount > 0)) AS t CROSS JOIN tax WHERE (tax.rate > 0)"
    )
    assert "tax" in translation.required_models


def test_translate_calculatetable_table():
    expr = _parse_expression("CALCULATETABLE('sales', 'sales'[product_key] = 1)")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT * FROM sales WHERE (product_key = 1)"


def test_translate_calculatetable_base_table_with_cross_table_filter_joins_related_table():
    expr = _parse_expression("CALCULATETABLE('sales', 'products'[category] = \"Clothing\")")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"product_key": "product_key"},
            "products": {"product_key": "product_key", "category": "category"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        relationship_edges=[
            RelationshipEdge(
                from_table="sales",
                from_column="product_key",
                to_table="products",
                to_column="product_key",
            )
        ],
    )

    assert (
        translation.sql == "SELECT sales.* FROM sales LEFT JOIN products ON sales.product_key = products.product_key "
        "WHERE (products.category = 'Clothing')"
    )
    assert "products" in translation.required_models


def test_translate_calculatetable_base_table_with_cross_table_table_ref_filter_candidate():
    expr = _parse_expression("CALCULATETABLE('sales', 'date')")
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount", "date_key": "date_key"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
    )

    assert translation.sql == "SELECT * FROM sales"
    assert "date" in translation.required_models


def test_translate_calculatetable_base_table_with_cross_table_filter_cross_joins_when_unrelated():
    expr = _parse_expression("CALCULATETABLE('sales', 'tax'[rate] > 0)")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "tax": {"rate": "rate"},
        },
        measure_names_by_table={"sales": set(), "tax": set()},
    )

    assert translation.sql == "SELECT sales.* FROM sales CROSS JOIN tax WHERE (tax.rate > 0)"
    assert "tax" in translation.required_models


def test_translate_calculatetable_base_table_with_datesinperiod_filter():
    expr = _parse_expression("CALCULATETABLE('sales', DATESINPERIOD('sales'[order_date], \"2024-12-31\", -3, MONTH))")
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
    )

    assert (
        translation.sql
        == "SELECT * FROM sales WHERE (order_date > ('2024-12-31' + INTERVAL '-3 month') AND order_date <= '2024-12-31')"
    )


def test_translate_calculatetable_base_table_with_datesytd_filter():
    expr = _parse_expression("CALCULATETABLE('sales', DATESYTD('sales'[order_date]))")
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
    )

    assert (
        translation.sql == "SELECT * FROM sales WHERE "
        "(order_date >= DATE_TRUNC('year', (SELECT MAX(order_date) FROM sales)) "
        "AND order_date <= (SELECT MAX(order_date) FROM sales))"
    )


def test_translate_calculatetable_base_table_with_cross_table_datesinperiod_filter_cross_joins_when_unrelated():
    expr = _parse_expression("CALCULATETABLE('sales', DATESINPERIOD('date'[date_key], \"2024-12-31\", -3, MONTH))")
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"date_key": "date_key"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
    )

    assert (
        translation.sql
        == "SELECT sales.* FROM sales CROSS JOIN date WHERE (date.date_key > ('2024-12-31' + INTERVAL '-3 month') AND date.date_key <= '2024-12-31')"
    )
    assert "date" in translation.required_models


def test_translate_calculatetable_base_table_with_cross_table_datesqtd_filter_cross_joins_when_unrelated():
    expr = _parse_expression("CALCULATETABLE('sales', DATESQTD('date'[date_key]))")
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"date_key": "date_key"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
    )

    assert (
        translation.sql == "SELECT sales.* FROM sales CROSS JOIN date WHERE "
        "(date.date_key >= DATE_TRUNC('quarter', (SELECT MAX(date.date_key) FROM date)) "
        "AND date.date_key <= (SELECT MAX(date.date_key) FROM date))"
    )
    assert "date" in translation.required_models


def test_translate_calculatetable_base_table_with_cross_table_dateadd_filter_cross_joins_when_unrelated():
    expr = _parse_expression("CALCULATETABLE('sales', DATEADD('date'[date_key], -1, YEAR))")
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"date_key": "date_key"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
    )

    assert (
        translation.sql == "SELECT sales.* FROM sales CROSS JOIN date WHERE "
        "(date.date_key > ((SELECT MAX(date.date_key) FROM date) + INTERVAL '-1 year') AND date.date_key <= (SELECT MAX(date.date_key) FROM date))"
    )
    assert "date" in translation.required_models


def test_translate_calculatetable_base_table_with_cross_table_sameperiodlastyear_filter_cross_joins_when_unrelated():
    expr = _parse_expression("CALCULATETABLE('sales', SAMEPERIODLASTYEAR('date'[date_key]))")
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"date_key": "date_key"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
    )

    assert (
        translation.sql == "SELECT sales.* FROM sales CROSS JOIN date WHERE "
        "(date.date_key > ((SELECT MAX(date.date_key) FROM date) + INTERVAL '-1 year') AND date.date_key <= (SELECT MAX(date.date_key) FROM date))"
    )
    assert "date" in translation.required_models


def test_translate_calculatetable_base_table_with_cross_table_nextmonth_filter_cross_joins_when_unrelated():
    expr = _parse_expression("CALCULATETABLE('sales', NEXTMONTH('date'[date_key]))")
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"date_key": "date_key"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
    )

    assert (
        translation.sql == "SELECT sales.* FROM sales CROSS JOIN date WHERE "
        "(date.date_key >= (SELECT MAX(date.date_key) FROM date) AND date.date_key < ((SELECT MAX(date.date_key) FROM date) + INTERVAL '1 month'))"
    )
    assert "date" in translation.required_models


def test_translate_calculatetable_wrapped_base_keeps_columns_in_scope():
    expr = _parse_expression("CALCULATETABLE(FILTER('sales', 'sales'[amount] > 100), 'sales'[amount] > 200)")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT * FROM (SELECT * FROM sales WHERE (amount > 100)) AS t WHERE (amount > 200)"


def test_translate_calculatetable_wrapped_base_with_cross_table_filter_joins_related_table():
    expr = _parse_expression(
        "CALCULATETABLE(FILTER('sales', 'sales'[amount] > 100), 'products'[category] = \"Clothing\")"
    )
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount", "product_key": "product_key"},
            "products": {"product_key": "product_key", "category": "category"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        relationship_edges=[
            RelationshipEdge(
                from_table="sales",
                from_column="product_key",
                to_table="products",
                to_column="product_key",
            )
        ],
    )

    assert (
        translation.sql
        == "SELECT t.* FROM (SELECT * FROM sales WHERE (amount > 100)) AS t LEFT JOIN products ON t.product_key = products.product_key "
        "WHERE (products.category = 'Clothing')"
    )
    assert "products" in translation.required_models


def test_translate_calculatetable_nested_replaces_same_column_filter():
    expr = _parse_expression(
        "CALCULATETABLE(CALCULATETABLE('sales', 'sales'[product_key] = 1), 'sales'[product_key] = 2)"
    )
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT * FROM sales WHERE (product_key = 2)"


def test_translate_calculatetable_nested_keepfilters_preserves_inner_filter():
    expr = _parse_expression(
        "CALCULATETABLE(CALCULATETABLE('sales', 'sales'[product_key] = 1), KEEPFILTERS('sales'[product_key] = 2))"
    )
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT * FROM sales WHERE (product_key = 1) AND (product_key = 2)"


def test_translate_calculatetable_nested_removefilters_clears_inner_filter():
    expr = _parse_expression(
        "CALCULATETABLE(CALCULATETABLE('sales', 'sales'[product_key] = 1), REMOVEFILTERS('sales'[product_key]))"
    )
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT * FROM sales"


def test_translate_countrows_calculatetable_filters():
    expr = _parse_expression("COUNTROWS(CALCULATETABLE('sales', 'sales'[product_key] = 1))")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "count"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_countrows_calculatetable_propagates_relationship_overrides():
    expr = _parse_expression(
        "COUNTROWS(CALCULATETABLE('sales', USERELATIONSHIP('sales'[product_key], 'products'[product_key])))"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"product_key": "product_key"},
            "products": {"product_key": "product_key"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "count"
    assert translation.filters == []
    assert len(translation.relationship_overrides) == 1
    assert translation.relationship_overrides[0].join_type is None
    assert translation.relationship_overrides[0].direction is None


def test_translate_countrows_calculatetable_datesinperiod_cross_table_filter():
    expr = _parse_expression(
        "COUNTROWS(CALCULATETABLE('sales', DATESINPERIOD('date'[date_key], \"2024-12-31\", -3, MONTH)))"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"date_key": "date_key"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"date_key"}, "date": {"date_key"}},
    )

    assert translation.agg == "count"
    assert translation.sql is None
    assert translation.filters == [
        "(date.date_key > ('2024-12-31' + INTERVAL '-3 month') AND date.date_key <= '2024-12-31')"
    ]
    assert "date" in translation.required_models


def test_translate_countrows_calculatetable_datesmtd_cross_table_filter():
    expr = _parse_expression("COUNTROWS(CALCULATETABLE('sales', DATESMTD('date'[date_key])))")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"date_key": "date_key"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"date_key"}, "date": {"date_key"}},
    )

    assert translation.agg == "count"
    assert translation.sql is None
    assert translation.filters == [
        "(date.date_key >= DATE_TRUNC('month', (SELECT MAX(date.date_key) FROM date)) "
        "AND date.date_key <= (SELECT MAX(date.date_key) FROM date))"
    ]
    assert "date" in translation.required_models


def test_translate_countrows_calculatetable_dateadd_cross_table_filter():
    expr = _parse_expression("COUNTROWS(CALCULATETABLE('sales', DATEADD('date'[date_key], -1, YEAR)))")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"date_key": "date_key"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"date_key"}, "date": {"date_key"}},
    )

    assert translation.agg == "count"
    assert translation.sql is None
    assert translation.filters == [
        "(date.date_key > ((SELECT MAX(date.date_key) FROM date) + INTERVAL '-1 year') "
        "AND date.date_key <= (SELECT MAX(date.date_key) FROM date))"
    ]
    assert "date" in translation.required_models


def test_translate_countrows_calculatetable_cross_table_table_ref_filter_candidate():
    expr = _parse_expression("COUNTROWS(CALCULATETABLE('sales', 'date'))")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount", "date_key": "date_key"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"date_key"}, "date": {"date_key"}},
    )

    assert translation.agg == "count"
    assert translation.sql is None
    assert translation.filters == []
    assert "date" in translation.required_models


def test_translate_calculate_filter_argument_propagates_relationship_overrides():
    expr = _parse_expression(
        "CALCULATE("
        "SUM('sales'[amount]), "
        "FILTER("
        "CALCULATETABLE('sales', CROSSFILTER('sales'[product_key], 'products'[product_key], BOTH)), "
        "'sales'[product_key] = 1"
        ")"
        ")"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount", "product_key": "product_key"},
            "products": {"product_key": "product_key"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(product_key = 1)"]
    assert len(translation.relationship_overrides) == 1
    assert translation.relationship_overrides[0].join_type == "inner"
    assert translation.relationship_overrides[0].direction == "Both"


def test_translate_sumx_calculatetable_propagates_relationship_overrides():
    expr = _parse_expression(
        "SUMX("
        "CALCULATETABLE('sales', CROSSFILTER('sales'[product_key], 'products'[product_key], NONE)), "
        "'sales'[amount]"
        ")"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount", "product_key": "product_key"},
            "products": {"product_key": "product_key"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert len(translation.relationship_overrides) == 1
    assert translation.relationship_overrides[0].join_type == "left"
    assert translation.relationship_overrides[0].direction == "None"


def test_translate_values_table_column():
    expr = _parse_expression("VALUES('sales'[product_key])")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT DISTINCT product_key FROM sales"


def test_translate_distinct_table_ref():
    expr = _parse_expression("DISTINCT('sales')")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT DISTINCT * FROM (SELECT * FROM sales) AS t"


def test_translate_countrows_values_as_count_distinct():
    expr = _parse_expression("COUNTROWS(VALUES('sales'[product_key]))")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "count_distinct"
    assert translation.sql == "product_key"
    assert translation.filters == []


def test_translate_countrows_distinct_as_count_distinct():
    expr = _parse_expression("COUNTROWS(DISTINCT('sales'[product_key]))")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "count_distinct"
    assert translation.sql == "product_key"
    assert translation.filters == []


def test_translate_countrows_values_cross_table_column_as_count_distinct():
    expr = _parse_expression("COUNTROWS(VALUES('date'[date_key]))")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "date": {"date_key"}},
    )

    assert translation.agg == "count_distinct"
    assert translation.sql == "date.date_key"
    assert translation.filters == []
    assert "date" in translation.required_models


def test_translate_countrows_filters_cross_table_column_as_count_distinct():
    expr = _parse_expression("COUNTROWS(FILTERS('date'[date_key]))")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "date": {"date_key"}},
    )

    assert translation.agg == "count_distinct"
    assert translation.sql == "date.date_key"
    assert translation.filters == []
    assert "date" in translation.required_models


def test_translate_countrows_cross_table_identifier_table():
    expr = _parse_expression("COUNTROWS('date')")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "date": {"date_key"}},
    )

    assert translation.agg == "count"
    assert translation.sql is None
    assert translation.filters == []
    assert "date" in translation.required_models


def test_translate_countrows_values_filtered_cross_table_propagates_filters():
    expr = _parse_expression("COUNTROWS(VALUES(FILTER('date', 'date'[date_key] = 1)))")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "date": {"date_key"}},
    )

    assert translation.agg == "count"
    assert translation.sql is None
    assert translation.filters == ["(date.date_key = 1)"]
    assert "date" in translation.required_models


def test_translate_countrows_identifier_table():
    expr = _parse_expression("COUNTROWS(sales)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "count"
    assert translation.sql is None
    assert translation.filters == []


def test_translate_counta_aggregate():
    expr = _parse_expression("COUNTA('sales'[product_key])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "count"
    assert translation.sql == "product_key"
    assert translation.filters == []


def test_translate_averagea_aggregate():
    expr = _parse_expression("AVERAGEA('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "avg"
    assert translation.sql == "amount"
    assert translation.filters == []


def test_translate_mina_aggregate():
    expr = _parse_expression("MINA('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "min"
    assert translation.sql == "amount"
    assert translation.filters == []


def test_translate_countblank_aggregate():
    expr = _parse_expression("COUNTBLANK('sales'[product_key])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "count"
    assert translation.sql == "CASE WHEN product_key IS NULL THEN 1 END"
    assert translation.filters == []


def test_translate_distinctcountnoblank_aggregate():
    expr = _parse_expression("DISTINCTCOUNTNOBLANK('sales'[product_key])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "count_distinct"
    assert translation.sql == "product_key"
    assert translation.filters == []


def test_translate_approximatedistinctcount_aggregate():
    expr = _parse_expression("APPROXIMATEDISTINCTCOUNT('sales'[product_key])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "count_distinct"
    assert translation.sql == "product_key"
    assert translation.filters == []


def test_translate_selectedvalue_with_alternate():
    expr = _parse_expression("SELECTEDVALUE('sales'[product_key], -1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "CASE WHEN COUNT(DISTINCT product_key) = 1 THEN MIN(product_key) ELSE -1 END"


def test_translate_selectedvalue_rejects_more_than_two_arguments():
    expr = _parse_expression("SELECTEDVALUE('sales'[product_key], -1, 0)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(
        DaxTranslationError, match="SELECTEDVALUE supports at most column and alternate_result arguments"
    ):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_hasonevalue():
    expr = _parse_expression("HASONEVALUE('sales'[product_key])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "(COUNT(DISTINCT product_key) = 1)"


def test_translate_hasonevalue_rejects_more_than_one_argument():
    expr = _parse_expression("HASONEVALUE('sales'[product_key], 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="HASONEVALUE/HASONEFILTER supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_firstnonblank():
    expr = _parse_expression("FIRSTNONBLANK('sales'[product_key], 'sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "MIN(CASE WHEN amount IS NOT NULL THEN product_key ELSE NULL END)"


def test_translate_firstnonblank_rejects_more_than_two_arguments():
    expr = _parse_expression("FIRSTNONBLANK('sales'[product_key], 'sales'[amount], 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(
        DaxTranslationError, match="FIRSTNONBLANK/LASTNONBLANK supports exactly column and expression arguments"
    ):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_lastnonblank():
    expr = _parse_expression("LASTNONBLANK('sales'[product_key], 'sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "MAX(CASE WHEN amount IS NOT NULL THEN product_key ELSE NULL END)"


def test_translate_firstdate():
    expr = _parse_expression("FIRSTDATE('sales'[order_date])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "MIN(order_date)"


def test_translate_endofmonth():
    expr = _parse_expression("ENDOFMONTH('sales'[order_date])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "MIN(DATE_TRUNC('month', order_date) + INTERVAL '1 month' - INTERVAL '1 day')"


def test_translate_containsstring():
    expr = _parse_expression("CONTAINSSTRING('sales'[status], \"open\")")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.type == "derived"
    assert translation.sql == "(POSITION(LOWER('open') IN LOWER(status)) > 0)"


def test_translate_containsstringexact():
    expr = _parse_expression("CONTAINSSTRINGEXACT('sales'[status], \"Open\")")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.type == "derived"
    assert translation.sql == "(POSITION('Open' IN status) > 0)"


def test_translate_containsrow_table_constructor():
    expr = _parse_expression("CONTAINSROW({1, 2, 3}, 'sales'[product_key])")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"product_key": "product_key", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.type == "derived"
    assert (
        translation.sql
        == "EXISTS (SELECT 1 FROM (SELECT 1 AS value1 UNION ALL SELECT 2 AS value1 UNION ALL SELECT 3 AS value1) "
        "AS t(c1) WHERE t.c1 IS NOT DISTINCT FROM product_key)"
    )


def test_translate_containsrow_rejects_non_table_first_argument():
    expr = _parse_expression("CONTAINSROW(1, 1)")
    with pytest.raises(DaxTranslationError, match="CONTAINSROW requires a table expression as first argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"product_key": "product_key", "order_date": "order_date"}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_containsrow_rejects_value_count_mismatch():
    expr = _parse_expression(
        "CONTAINSROW(SELECTCOLUMNS('sales', \"k\", 'sales'[product_key], \"q\", 'sales'[quantity]), 'sales'[product_key])"
    )
    with pytest.raises(DaxTranslationError, match="CONTAINSROW value argument count must match table column count"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={
                "sales": {"product_key": "product_key", "quantity": "quantity", "order_date": "order_date"}
            },
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_containsrow_rejects_non_inferable_table_width():
    expr = _parse_expression("CONTAINSROW('sales', 1)")
    with pytest.raises(DaxTranslationError, match="CONTAINSROW requires an inferable table column count"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": set()},
        )


def test_translate_containsstring_rejects_more_than_two_arguments():
    expr = _parse_expression('CONTAINSSTRING(\'sales\'[status], "open", "extra")')
    with pytest.raises(DaxTranslationError, match="CONTAINSSTRING supports exactly two arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_containsstringexact_rejects_more_than_two_arguments():
    expr = _parse_expression('CONTAINSSTRINGEXACT(\'sales\'[status], "Open", "extra")')
    with pytest.raises(DaxTranslationError, match="CONTAINSSTRINGEXACT supports exactly two arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_len():
    expr = _parse_expression("LEN('sales'[status])")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.type == "derived"
    assert translation.sql == "LENGTH(status)"


def test_translate_len_rejects_more_than_one_argument():
    expr = _parse_expression("LEN('sales'[status], 1)")
    with pytest.raises(DaxTranslationError, match="LEN supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_left():
    expr = _parse_expression("LEFT('sales'[status], 3)")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.type == "derived"
    assert translation.sql == "SUBSTRING(status, 1, GREATEST(3, 0))"


def test_translate_left_default_num_chars():
    expr = _parse_expression("LEFT('sales'[status])")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.type == "derived"
    assert translation.sql == "SUBSTRING(status, 1, GREATEST(1, 0))"


def test_translate_left_rejects_more_than_two_arguments():
    expr = _parse_expression("LEFT('sales'[status], 1, 2)")
    with pytest.raises(DaxTranslationError, match="LEFT supports at most text and num_chars arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_right():
    expr = _parse_expression("RIGHT('sales'[status], 3)")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.type == "derived"
    assert (
        translation.sql == "CASE WHEN 3 <= 0 THEN '' ELSE SUBSTRING(status, GREATEST(LENGTH(status) - 3 + 1, 1), 3) END"
    )


def test_translate_right_rejects_more_than_two_arguments():
    expr = _parse_expression("RIGHT('sales'[status], 1, 2)")
    with pytest.raises(DaxTranslationError, match="RIGHT supports at most text and num_chars arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_replace():
    expr = _parse_expression("REPLACE('sales'[status], 2, 2, \"xx\")")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.type == "derived"
    assert (
        translation.sql
        == "(CASE WHEN GREATEST(2, 1) <= 1 THEN '' ELSE SUBSTRING(status, 1, GREATEST(2, 1) - 1) END || 'xx' || "
        "SUBSTRING(status, GREATEST(2, 1) + GREATEST(2, 0)))"
    )


def test_translate_replace_requires_four_arguments():
    expr = _parse_expression("REPLACE('sales'[status], 2, 2)")
    with pytest.raises(
        DaxTranslationError, match="REPLACE requires old_text, start_num, num_chars, and new_text arguments"
    ):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_substitute():
    expr = _parse_expression('SUBSTITUTE(\'sales\'[status], "ab", "xy")')
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.type == "derived"
    assert translation.sql == "REPLACE(status, 'ab', 'xy')"


def test_translate_substitute_instance_num():
    expr = _parse_expression('SUBSTITUTE(\'sales\'[status], "ab", "xy", 1)')
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.type == "derived"
    assert (
        translation.sql
        == "CASE WHEN 'ab' = '' THEN status WHEN (INSTR(status, 'ab')) = 0 THEN status ELSE SUBSTR(status, 1, "
        "(INSTR(status, 'ab')) - 1) || 'xy' || SUBSTR(status, (INSTR(status, 'ab')) + LENGTH('ab')) END"
    )


def test_translate_substitute_instance_num_accepts_scalar_expression():
    expr = _parse_expression('SUBSTITUTE(\'sales\'[status], "ab", "xy", 1 + 0)')
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.type == "derived"
    assert "STRING_SPLIT(status, 'ab')" in translation.sql
    assert "CAST(((1 + 0)) AS BIGINT)" in translation.sql


def test_translate_substitute_instance_num_requires_positive_integer():
    expr = _parse_expression('SUBSTITUTE(\'sales\'[status], "ab", "xy", 0)')
    with pytest.raises(DaxTranslationError, match="SUBSTITUTE instance_num must be >= 1"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_rept():
    expr = _parse_expression("REPT('sales'[status], 3)")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.type == "derived"
    assert translation.sql == "REPEAT(status, GREATEST(CAST(FLOOR(3) AS BIGINT), 0))"


def test_translate_rept_requires_two_arguments():
    expr = _parse_expression("REPT('sales'[status])")
    with pytest.raises(DaxTranslationError, match="REPT requires text and number_times arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_rept_rejects_more_than_two_arguments():
    expr = _parse_expression("REPT('sales'[status], 2, 3)")
    with pytest.raises(DaxTranslationError, match="REPT supports exactly two arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_trim():
    expr = _parse_expression("TRIM('sales'[status])")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.type == "derived"
    assert translation.sql == "TRIM(REGEXP_REPLACE(CAST(status AS VARCHAR), ' +', ' ', 'g'))"


def test_translate_trim_requires_argument():
    expr = _parse_expression("TRIM()")
    with pytest.raises(DaxTranslationError, match="TRIM requires an argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_trim_rejects_more_than_one_argument():
    expr = _parse_expression("TRIM('sales'[status], 'x')")
    with pytest.raises(DaxTranslationError, match="TRIM supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_mid():
    expr = _parse_expression("MID('sales'[status], 2, 3)")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.type == "derived"
    assert translation.sql == "CASE WHEN 3 <= 0 THEN '' ELSE SUBSTRING(status, GREATEST(2, 1), 3) END"


def test_translate_mid_requires_three_arguments():
    expr = _parse_expression("MID('sales'[status], 2)")
    with pytest.raises(DaxTranslationError, match="MID requires text, start_num, and num_chars arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_mid_rejects_more_than_three_arguments():
    expr = _parse_expression("MID('sales'[status], 2, 3, 4)")
    with pytest.raises(DaxTranslationError, match="MID requires text, start_num, and num_chars arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_exact():
    expr = _parse_expression("EXACT('sales'[status], \"Open\")")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.type == "derived"
    assert translation.sql == "(status = 'Open')"


def test_translate_exact_rejects_more_than_two_arguments():
    expr = _parse_expression('EXACT(\'sales\'[status], "Open", "Closed")')
    with pytest.raises(DaxTranslationError, match="EXACT supports exactly two arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"status": "status", "order_date": "order_date"}},
            measure_names_by_table={"sales": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_date_ctor():
    expr = _parse_expression("DATE(2024, 2, 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "MAKE_DATE(2024, 2, 1)"


def test_translate_date_ctor_rejects_more_than_three_arguments():
    expr = _parse_expression("DATE(2024, 2, 1, 5)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="DATE requires year, month, and day arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_time_ctor():
    expr = _parse_expression("TIME(12, 30, 0)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "MAKE_TIME(12, 30, 0)"


def test_translate_time_ctor_rejects_more_than_three_arguments():
    expr = _parse_expression("TIME(12, 30, 0, 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TIME requires hour, minute, and second arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_datevalue():
    expr = _parse_expression('DATEVALUE("2024-02-01")')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "CAST('2024-02-01' AS DATE)"


def test_translate_datevalue_rejects_more_than_one_argument():
    expr = _parse_expression('DATEVALUE("2024-02-01", "extra")')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="DATEVALUE supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_timevalue():
    expr = _parse_expression('TIMEVALUE("12:34:56")')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "CAST('12:34:56' AS TIME)"


def test_translate_timevalue_rejects_more_than_one_argument():
    expr = _parse_expression('TIMEVALUE("12:34:56", "extra")')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TIMEVALUE supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_edate():
    expr = _parse_expression('EDATE("2024-02-01", 2)')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "(CAST('2024-02-01' AS DATE) + (2) * INTERVAL '1 month')"


def test_translate_edate_rejects_more_than_two_arguments():
    expr = _parse_expression('EDATE("2024-02-01", 2, 1)')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="EDATE requires start date and month offset arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_eomonth():
    expr = _parse_expression('EOMONTH("2024-02-01", 0)')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == (
        "(DATE_TRUNC('month', (CAST('2024-02-01' AS DATE) + (0) * INTERVAL '1 month')) + INTERVAL '1 month' - "
        "INTERVAL '1 day')"
    )


def test_translate_eomonth_rejects_more_than_two_arguments():
    expr = _parse_expression('EOMONTH("2024-02-01", 0, 1)')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="EOMONTH requires start date and month offset arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_datediff():
    expr = _parse_expression('DATEDIFF("2024-01-01", "2024-02-01", MONTH)')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "DATE_DIFF('month', CAST('2024-01-01' AS DATE), CAST('2024-02-01' AS DATE))"


def test_translate_datediff_rejects_more_than_three_arguments():
    expr = _parse_expression('DATEDIFF("2024-01-01", "2024-02-01", MONTH, DAY)')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="DATEDIFF requires start date, end date, and interval arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_weekday():
    expr = _parse_expression('WEEKDAY("2024-02-01", 2)')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "(((EXTRACT(DOW FROM CAST('2024-02-01' AS DATE)) + 6) % 7) + 1)"


def test_translate_weekday_rejects_more_than_two_arguments():
    expr = _parse_expression('WEEKDAY("2024-02-01", 2, 3)')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="WEEKDAY supports at most date and return_type arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_weekday_return_type_11():
    expr = _parse_expression('WEEKDAY("2024-02-01", 11)')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "(((EXTRACT(DOW FROM CAST('2024-02-01' AS DATE)) - 1 + 7) % 7) + 1)"


def test_translate_weeknum():
    expr = _parse_expression('WEEKNUM("2024-02-01", 2)')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "(CAST(STRFTIME(CAST('2024-02-01' AS DATE), '%W') AS INTEGER) + 1)"


def test_translate_weeknum_rejects_more_than_two_arguments():
    expr = _parse_expression('WEEKNUM("2024-02-01", 2, 3)')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="WEEKNUM supports at most date and return_type arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_weeknum_return_type_21():
    expr = _parse_expression('WEEKNUM("2024-02-01", 21)')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "CAST(STRFTIME(CAST('2024-02-01' AS DATE), '%V') AS INTEGER)"


def test_translate_year_rejects_more_than_one_argument():
    expr = _parse_expression('YEAR("2024-02-01", 2)')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="YEAR supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_isinscope_defaults_false_without_group_context():
    expr = _parse_expression("ISINSCOPE('sales'[product_key])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "FALSE"


def test_translate_isinscope_rejects_more_than_one_argument():
    expr = _parse_expression("ISINSCOPE('sales'[product_key], 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="ISINSCOPE supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_isfiltered_defaults_false_without_filter_context():
    expr = _parse_expression("ISFILTERED('sales'[product_key])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "FALSE"


def test_translate_isfiltered_rejects_more_than_one_argument():
    expr = _parse_expression("ISFILTERED('sales'[product_key], 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="ISFILTERED/ISCROSSFILTERED supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_today():
    expr = _parse_expression("TODAY()")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "CURRENT_DATE"


def test_translate_today_rejects_arguments():
    expr = _parse_expression("TODAY(1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TODAY does not take arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_now_rejects_arguments():
    expr = _parse_expression("NOW(1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="NOW does not take arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_utcnow_rejects_arguments():
    expr = _parse_expression("UTCNOW(1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="UTCNOW does not take arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_utctoday_rejects_arguments():
    expr = _parse_expression("UTCTODAY(1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="UTCTODAY does not take arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_rand():
    expr = _parse_expression("RAND()")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "RANDOM()"


def test_translate_rand_rejects_arguments():
    expr = _parse_expression("RAND(1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="RAND does not take arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_randbetween():
    expr = _parse_expression("RANDBETWEEN(1, 10)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "CAST(FLOOR(RANDOM() * ((10) - (1) + 1) + (1)) AS BIGINT)"


def test_translate_randbetween_rejects_more_than_two_arguments():
    expr = _parse_expression("RANDBETWEEN(1, 10, 20)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="RANDBETWEEN supports exactly two arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_format():
    expr = _parse_expression("FORMAT('sales'[amount], \"0.00\")")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "CAST(amount AS VARCHAR)"


def test_translate_format_requires_format_string_argument():
    expr = _parse_expression("FORMAT('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="FORMAT requires value and format_string arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_format_rejects_more_than_three_arguments():
    expr = _parse_expression('FORMAT(\'sales\'[amount], "0.00", "en-US", "extra")')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="FORMAT supports at most value, format_string, and locale arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_iferror():
    expr = _parse_expression("IFERROR('sales'[amount], 0)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "CASE WHEN amount IS NULL THEN 0 ELSE amount END"


def test_translate_iferror_rejects_more_than_two_arguments():
    expr = _parse_expression("IFERROR('sales'[amount], 0, 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="IFERROR supports exactly two arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_coalesce():
    expr = _parse_expression("COALESCE('sales'[amount], 0)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "COALESCE(amount, 0)"


def test_translate_coalesce_requires_at_least_two_arguments():
    expr = _parse_expression("COALESCE('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="COALESCE requires at least two arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_switch_boolean_predicate_form():
    expr = _parse_expression("SWITCH(TRUE(), 'sales'[amount] > 0, 1, 0)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "CASE WHEN (amount > 0) THEN 1 ELSE 0 END"


def test_translate_switch_requires_expression_and_value_result_pair():
    expr = _parse_expression("SWITCH('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="SWITCH requires expression and at least one value/result pair"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )

    expr_missing_result = _parse_expression("SWITCH('sales'[amount], 1)")
    with pytest.raises(DaxTranslationError, match="SWITCH requires expression and at least one value/result pair"):
        translate_dax_metric(
            expr_missing_result,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_divide_rejects_more_than_three_arguments():
    expr = _parse_expression("DIVIDE('sales'[amount], 2, 0, 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(
        DaxTranslationError,
        match="DIVIDE supports at most numerator, denominator, and alternate result arguments",
    ):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_and_or_reject_more_than_two_arguments():
    and_expr = _parse_expression("AND(TRUE(), FALSE(), TRUE())")
    or_expr = _parse_expression("OR(TRUE(), FALSE(), TRUE())")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()

    with pytest.raises(DaxTranslationError, match="AND supports exactly two arguments"):
        translate_dax_metric(
            and_expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )

    with pytest.raises(DaxTranslationError, match="OR supports exactly two arguments"):
        translate_dax_metric(
            or_expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_roundup():
    expr = _parse_expression("ROUNDUP('sales'[amount], 2)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert (
        translation.sql == "CASE WHEN 2 >= 0 THEN SIGN(amount) * CEIL(ABS(amount) * POWER(10, 2)) / POWER(10, 2) ELSE "
        "SIGN(amount) * CEIL(ABS(amount) / POWER(10, -(2))) * POWER(10, -(2)) END"
    )


def test_translate_round():
    expr = _parse_expression("ROUND('sales'[amount], 2)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert (
        translation.sql
        == "CASE WHEN 2 >= 0 THEN SIGN(amount) * FLOOR(ABS(amount) * POWER(10, 2) + 0.5) / POWER(10, 2) ELSE "
        "SIGN(amount) * FLOOR(ABS(amount) / POWER(10, -(2)) + 0.5) * POWER(10, -(2)) END"
    )


def test_translate_round_requires_two_arguments():
    expr = _parse_expression("ROUND('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="ROUND requires number and num_digits arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_round_rejects_more_than_two_arguments():
    expr = _parse_expression("ROUND('sales'[amount], 2, 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="ROUND requires number and num_digits arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_rounddown():
    expr = _parse_expression("ROUNDDOWN('sales'[amount], 2)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert (
        translation.sql == "CASE WHEN 2 >= 0 THEN SIGN(amount) * FLOOR(ABS(amount) * POWER(10, 2)) / POWER(10, 2) ELSE "
        "SIGN(amount) * FLOOR(ABS(amount) / POWER(10, -(2))) * POWER(10, -(2)) END"
    )


def test_translate_int():
    expr = _parse_expression("INT('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "FLOOR(amount)"


def test_translate_int_rejects_more_than_one_argument():
    expr = _parse_expression("INT('sales'[amount], 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="INT supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_trunc():
    expr = _parse_expression("TRUNC('sales'[amount], 2)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert (
        translation.sql == "CASE WHEN 2 >= 0 THEN SIGN(amount) * FLOOR(ABS(amount) * POWER(10, 2)) / POWER(10, 2) ELSE "
        "SIGN(amount) * FLOOR(ABS(amount) / POWER(10, -(2))) * POWER(10, -(2)) END"
    )


def test_translate_trunc_defaults_num_digits_to_zero():
    expr = _parse_expression("TRUNC('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert (
        translation.sql == "CASE WHEN 0 >= 0 THEN SIGN(amount) * FLOOR(ABS(amount) * POWER(10, 0)) / POWER(10, 0) ELSE "
        "SIGN(amount) * FLOOR(ABS(amount) / POWER(10, -(0))) * POWER(10, -(0)) END"
    )


def test_translate_trunc_rejects_more_than_two_arguments():
    expr = _parse_expression("TRUNC('sales'[amount], 2, 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TRUNC supports at most number and num_digits arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_ceiling_with_significance():
    expr = _parse_expression("CEILING('sales'[amount], 2)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "(CEIL(amount / 2) * 2)"


def test_translate_floor_with_significance():
    expr = _parse_expression("FLOOR('sales'[amount], 2)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "(FLOOR(amount / 2) * 2)"


def test_translate_ceiling_floor_reject_more_than_two_arguments():
    ceiling_expr = _parse_expression("CEILING('sales'[amount], 2, 1)")
    floor_expr = _parse_expression("FLOOR('sales'[amount], 2, 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()

    with pytest.raises(DaxTranslationError, match="CEILING supports at most number and significance arguments"):
        translate_dax_metric(
            ceiling_expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )

    with pytest.raises(DaxTranslationError, match="FLOOR supports at most number and significance arguments"):
        translate_dax_metric(
            floor_expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_mround():
    expr = _parse_expression("MROUND('sales'[amount], 2)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert (
        translation.sql == "CASE WHEN 2 = 0 THEN 0 ELSE SIGN(amount) * FLOOR((ABS(amount) / ABS(2)) + 0.5) * ABS(2) END"
    )


def test_translate_mround_requires_two_arguments():
    expr = _parse_expression("MROUND('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="MROUND requires number and multiple arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_mround_rejects_more_than_two_arguments():
    expr = _parse_expression("MROUND('sales'[amount], 2, 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="MROUND requires number and multiple arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_min_aggregate_single_argument():
    expr = _parse_expression("MIN('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type is None
    assert translation.agg == "min"
    assert translation.sql == "amount"


def test_translate_max_aggregate_single_argument():
    expr = _parse_expression("MAX('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type is None
    assert translation.agg == "max"
    assert translation.sql == "amount"


def test_translate_min_scalar_two_arguments():
    expr = _parse_expression("MIN('sales'[amount], 10)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "LEAST(amount, 10)"


def test_translate_max_scalar_two_arguments():
    expr = _parse_expression("MAX('sales'[amount], 10)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "GREATEST(amount, 10)"


def test_translate_min_max_reject_more_than_two_arguments():
    min_expr = _parse_expression("MIN('sales'[amount], 10, 20)")
    max_expr = _parse_expression("MAX('sales'[amount], 10, 20)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()

    with pytest.raises(DaxTranslationError, match="MIN supports one aggregate argument or two scalar arguments"):
        translate_dax_metric(
            min_expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )

    with pytest.raises(DaxTranslationError, match="MAX supports one aggregate argument or two scalar arguments"):
        translate_dax_metric(
            max_expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_sum_rejects_more_than_one_argument():
    expr = _parse_expression("SUM('sales'[amount], 10)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()

    with pytest.raises(DaxTranslationError, match="SUM supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_countrows_rejects_more_than_one_argument():
    expr = _parse_expression("COUNTROWS('sales', 'sales')")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()

    with pytest.raises(DaxTranslationError, match="COUNTROWS supports at most one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_abs():
    expr = _parse_expression("ABS('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "ABS(amount)"


def test_translate_abs_requires_one_argument():
    expr = _parse_expression("ABS('sales'[amount], 2)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="ABS requires exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_mod():
    expr = _parse_expression("MOD('sales'[amount], 3)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "MOD(amount, 3)"


def test_translate_mod_requires_two_arguments():
    expr = _parse_expression("MOD('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="MOD requires number and divisor arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_power():
    expr = _parse_expression("POWER('sales'[amount], 2)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "POWER(amount, 2)"


def test_translate_power_requires_two_arguments():
    expr = _parse_expression("POWER('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="POWER requires number and exponent arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_sqrt():
    expr = _parse_expression("SQRT('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "SQRT(amount)"


def test_translate_exp():
    expr = _parse_expression("EXP('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "EXP(amount)"


def test_translate_ln():
    expr = _parse_expression("LN('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "LN(amount)"


def test_translate_log10():
    expr = _parse_expression("LOG10('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "LOG10(amount)"


def test_translate_log_default_base():
    expr = _parse_expression("LOG('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "LOG10(amount)"


def test_translate_log_with_base():
    expr = _parse_expression("LOG('sales'[amount], 2)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "(LN(amount) / LN(2))"


def test_translate_log_rejects_more_than_two_arguments():
    expr = _parse_expression("LOG('sales'[amount], 2, 3)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="LOG supports at most number and base arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_pi():
    expr = _parse_expression("PI()")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "PI()"


def test_translate_pi_rejects_arguments():
    expr = _parse_expression("PI(1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="PI does not take arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_nameof():
    expr = _parse_expression("NAMEOF('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "'sales[amount]'"


def test_translate_nameof_rejects_more_than_one_argument():
    expr = _parse_expression("NAMEOF('sales'[amount], 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="NAMEOF supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_convert():
    expr = _parse_expression("CONVERT('sales'[amount], STRING)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "CAST(amount AS VARCHAR)"


def test_translate_convert_rejects_more_than_two_arguments():
    expr = _parse_expression("CONVERT('sales'[amount], STRING, 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="CONVERT supports exactly value and datatype arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_related_rejects_more_than_one_argument():
    expr = _parse_expression("RELATED('other'[amount], 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="RELATED supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_value_rejects_more_than_one_argument():
    expr = _parse_expression("VALUE('sales'[amount_text], 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="VALUE supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_concatenate_rejects_more_than_two_arguments():
    expr = _parse_expression('CONCATENATE(\'sales\'[sku], "-", "x")')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="CONCATENATE supports exactly two arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_concatenatex():
    expr = _parse_expression("CONCATENATEX('sales', 'sales'[product_key], \"-\", 'sales'[product_key], DESC)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert "STRING_AGG(CAST(product_key AS VARCHAR), CAST('-' AS VARCHAR) ORDER BY product_key DESC)" in translation.sql
    assert "FROM sales" in translation.sql


def test_translate_concatenatex_wrapped_table_expression():
    expr = _parse_expression("CONCATENATEX(FILTER('sales', 'sales'[amount] > 10), 'sales'[product_key], \",\")")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert "STRING_AGG(CAST(t.product_key AS VARCHAR), CAST(',' AS VARCHAR))" in translation.sql
    assert "FROM (SELECT * FROM sales WHERE (amount > 10)) AS t" in translation.sql


def test_translate_concatenatex_cross_table_expression_tracks_required_model():
    expr = _parse_expression("CONCATENATEX('sales', 'products'[category], \",\")")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"product_key": "product_key"},
            "products": {"product_key": "product_key", "category": "category"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={},
        relationship_edges=[
            RelationshipEdge(
                from_table="sales",
                from_column="product_key",
                to_table="products",
                to_column="product_key",
            )
        ],
    )

    assert "STRING_AGG(CAST(products.category AS VARCHAR), CAST(',' AS VARCHAR))" in translation.sql
    assert "FROM sales LEFT JOIN products ON sales.product_key = products.product_key" in translation.sql
    assert "products" in translation.required_models


def test_translate_concatenatex_requires_table_and_expression_arguments():
    expr = _parse_expression("CONCATENATEX('sales')")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="CONCATENATEX requires table and expression arguments"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_if_rejects_more_than_three_arguments():
    expr = _parse_expression("IF(TRUE(), 1, 2, 3)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(
        DaxTranslationError, match="IF supports at most condition, value_if_true, and value_if_false arguments"
    ):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_evaluateandlog():
    expr = _parse_expression("EVALUATEANDLOG('sales'[amount])")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "amount"


def test_translate_evaluateandlog_rejects_more_than_one_argument():
    expr = _parse_expression("EVALUATEANDLOG('sales'[amount], 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="EVALUATEANDLOG supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_ignore_rejects_more_than_one_argument():
    expr = _parse_expression("IGNORE('sales'[amount], 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="IGNORE supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_nonvisual_rejects_more_than_one_argument():
    expr = _parse_expression("NONVISUAL('sales'[amount], 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="NONVISUAL supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_upper_lower_reject_more_than_one_argument():
    upper_expr = _parse_expression('UPPER("abc", "x")')
    lower_expr = _parse_expression('LOWER("ABC", "x")')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()

    with pytest.raises(DaxTranslationError, match="UPPER supports exactly one argument"):
        translate_dax_metric(
            upper_expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )

    with pytest.raises(DaxTranslationError, match="LOWER supports exactly one argument"):
        translate_dax_metric(
            lower_expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_isblank_rejects_more_than_one_argument():
    isblank_expr = _parse_expression("ISBLANK('sales'[amount], 1)")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()

    with pytest.raises(DaxTranslationError, match="ISBLANK supports exactly one argument"):
        translate_dax_metric(
            isblank_expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_isempty_table_expression():
    expr = _parse_expression("ISEMPTY(FILTER('sales', 'sales'[amount] > 0))")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.type == "derived"
    assert translation.sql == "NOT EXISTS (SELECT 1 FROM (SELECT * FROM sales WHERE (amount > 0)) AS t)"


def test_translate_isempty_rejects_more_than_one_argument():
    expr = _parse_expression("ISEMPTY('sales', 'sales')")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="ISEMPTY supports exactly one argument"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_values_identifier_table_ref():
    expr = _parse_expression("VALUES(sales)")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT DISTINCT * FROM sales"


@pytest.mark.parametrize(
    ("expr_sql", "expected_fragment"),
    [
        ("UNION('sales', 'tax')", "UNION ALL"),
        ("INTERSECT('sales', 'tax')", "INTERSECT ALL"),
        ("EXCEPT('sales', 'tax')", "EXCEPT ALL"),
        ("CROSSJOIN('sales', 'tax')", "CROSS JOIN"),
        ("NATURALINNERJOIN('sales', 'tax')", "NATURAL INNER JOIN"),
        ("NATURALLEFTOUTERJOIN('sales', 'tax')", "NATURAL LEFT JOIN"),
    ],
)
def test_translate_table_set_operations_allow_multi_table_refs_when_model_is_none(
    expr_sql: str, expected_fragment: str
):
    expr = _parse_expression(expr_sql)
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount", "date_key": "date_key"},
            "tax": {"rate": "rate", "date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "tax": set()},
    )

    assert expected_fragment in translation.sql
    assert "SELECT * FROM sales" in translation.sql
    assert "SELECT * FROM tax" in translation.sql


def test_translate_generate_allows_cross_table_right_table_when_model_is_none():
    expr = _parse_expression("GENERATE('sales', FILTER('tax', 'tax'[rate] > 0))")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount", "date_key": "date_key"},
            "tax": {"rate": "rate", "date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "tax": set()},
    )

    assert "CROSS JOIN LATERAL" in translation.sql
    assert "FROM (SELECT * FROM sales) AS l" in translation.sql
    assert "FROM tax" in translation.sql
    assert "rate > 0" in translation.sql


def test_translate_row_table_allows_multi_table_refs_when_model_is_none():
    expr = _parse_expression("ROW(\"sales_amount\", 'sales'[amount], \"tax_rate\", 'tax'[rate])")
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "tax": {"rate": "rate"},
        },
        measure_names_by_table={"sales": set(), "tax": set()},
    )

    assert "SELECT amount AS sales_amount, tax.rate AS tax_rate FROM sales CROSS JOIN tax" in translation.sql
    assert "sales" in translation.required_models
    assert "tax" in translation.required_models


def test_translate_calendar_table():
    expr = _parse_expression('CALENDAR("2024-01-01", "2024-01-03")')
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount"}},
        measure_names_by_table={"sales": set()},
    )

    assert translation.sql == (
        "SELECT date_value AS Date FROM generate_series(CAST('2024-01-01' AS DATE), "
        "CAST('2024-01-03' AS DATE), INTERVAL '1 day') AS gs(date_value)"
    )


def test_translate_calendar_table_cross_table_aggregate_bounds():
    expr = _parse_expression("CALENDAR(MIN('sales'[order_date]), MAX('date'[date_key]))")
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"order_date": "order_date"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
    )

    assert "CAST((SELECT MIN(order_date) FROM sales) AS DATE)" in translation.sql
    assert "CAST((SELECT MAX(date.date_key) FROM date) AS DATE)" in translation.sql
    assert "date" in translation.required_models


def test_translate_generateseries_table():
    expr = _parse_expression("GENERATESERIES(1, 5, 2)")
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount"}},
        measure_names_by_table={"sales": set()},
    )

    assert translation.sql == "SELECT value FROM generate_series(1, 5, 2) AS gs(value)"


def test_translate_generateseries_table_cross_table_aggregate_bounds():
    expr = _parse_expression("GENERATESERIES(MIN('sales'[amount]), MAX('date'[date_key]), 1)")
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
    )

    assert (
        "generate_series((SELECT MIN(amount) FROM sales), (SELECT MAX(date.date_key) FROM date), 1)" in translation.sql
    )
    assert "date" in translation.required_models


def test_translate_firstdate_table():
    expr = _parse_expression("FIRSTDATE('sales'[order_date])")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT MIN(order_date) AS value1 FROM sales"


def test_translate_startofyear_table():
    expr = _parse_expression("STARTOFYEAR('sales'[order_date])")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT MIN(DATE_TRUNC('year', order_date)) AS value1 FROM sales"


def test_translate_filters_table_column():
    expr = _parse_expression("FILTERS('sales'[product_key])")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT DISTINCT product_key FROM sales"


def test_translate_all_table_ref():
    expr = _parse_expression("ALL('sales')")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT * FROM sales"


def test_translate_all_column_ref():
    expr = _parse_expression("ALL('sales'[product_key])")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT DISTINCT product_key FROM sales"


def test_translate_treatas_table():
    expr = _parse_expression("TREATAS({1, 2}, 'sales'[product_key])")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
    )

    assert translation.sql == "SELECT DISTINCT product_key AS product_key FROM sales WHERE (product_key IN (1, 2))"


def test_translate_treatas_table_cross_table_target_joins_related_table():
    expr = _parse_expression("TREATAS({\"A\"}, 'products'[category])")
    translation = translate_dax_table(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"product_key": "product_key"},
            "products": {"product_key": "product_key", "category": "category"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        relationship_edges=[
            RelationshipEdge(
                from_table="sales",
                from_column="product_key",
                to_table="products",
                to_column="product_key",
            )
        ],
    )

    assert (
        translation.sql
        == "SELECT DISTINCT products.category AS category FROM products WHERE (products.category IN ('A'))"
    )
    assert "products" in translation.required_models


def test_translate_treatas_table_rejects_non_column_target_arguments():
    expr = _parse_expression("TREATAS({1, 2}, 1)")
    column_sql_by_table, measure_names_by_table, _time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TREATAS target arguments must reference table columns"):
        translate_dax_table(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
        )


def test_translate_treatas_filter_rejects_table_expression_width_mismatch():
    expr = _parse_expression(
        "CALCULATE("
        "SUM('sales'[amount]), "
        "TREATAS(SELECTCOLUMNS('sales', \"k\", 'sales'[product_key], \"q\", 'sales'[quantity]), 'sales'[product_key])"
        ")"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_treatas_filter_rejects_table_ref_width_mismatch_when_known():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), TREATAS('sales', 'sales'[product_key]))")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_treatas_filter_rejects_filter_wrapper_width_mismatch():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), TREATAS(FILTER('sales', 'sales'[amount] > 10), 'sales'[product_key]))"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_treatas_filter_rejects_calculatetable_wrapper_width_mismatch():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), TREATAS(CALCULATETABLE('sales', 'sales'[amount] > 10), 'sales'[product_key]))"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_treatas_filter_rejects_values_wrapper_width_mismatch():
    expr = _parse_expression(
        "CALCULATE("
        "SUM('sales'[amount]), "
        "TREATAS(FILTER(VALUES('sales'[product_key]), 'sales'[product_key] > 1), 'sales'[product_key], 'sales'[quantity])"
        ")"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_treatas_filter_rejects_union_wrapper_width_mismatch():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), TREATAS(UNION('sales', 'sales'), 'sales'[product_key]))")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_treatas_filter_rejects_crossjoin_wrapper_width_mismatch():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), TREATAS(CROSSJOIN('sales', 'sales'), 'sales'[product_key]))"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_treatas_filter_rejects_generate_wrapper_width_mismatch():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), TREATAS(GENERATE('sales', 'sales'), 'sales'[product_key]))"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_treatas_filter_rejects_naturalinnerjoin_wrapper_width_mismatch():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), TREATAS(NATURALINNERJOIN('sales', 'sales'), 'sales'[product_key]))"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_treatas_filter_rejects_naturalleftouterjoin_wrapper_width_mismatch():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), TREATAS(NATURALLEFTOUTERJOIN('sales', 'sales'), 'sales'[product_key]))"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_treatas_filter_rejects_renamecolumns_wrapper_width_mismatch():
    expr = _parse_expression(
        "CALCULATE("
        "SUM('sales'[amount]), "
        "TREATAS(RENAMECOLUMNS('sales', 'sales'[product_key], \"k\"), 'sales'[product_key], 'sales'[quantity])"
        ")"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_treatas_filter_rejects_removecolumns_wrapper_width_mismatch():
    expr = _parse_expression(
        "CALCULATE("
        "SUM('sales'[amount]), "
        "TREATAS(REMOVECOLUMNS('sales', 'sales'[amount], 'sales'[quantity], 'sales'[order_date]), "
        "'sales'[product_key], 'sales'[quantity])"
        ")"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_treatas_filter_rejects_keepcolumns_wrapper_width_mismatch():
    expr = _parse_expression(
        "CALCULATE("
        "SUM('sales'[amount]), "
        "TREATAS(KEEPCOLUMNS('sales', 'sales'[product_key], 'sales'[quantity]), 'sales'[product_key])"
        ")"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table=column_sql_by_table,
            measure_names_by_table=measure_names_by_table,
            measure_aggs_by_table={},
            time_dimensions_by_table=time_dimensions_by_table,
        )


def test_translate_countrows_filters_as_count_distinct():
    expr = _parse_expression("COUNTROWS(FILTERS('sales'[product_key]))")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "count_distinct"
    assert translation.sql == "product_key"
    assert translation.filters == []


def test_translate_countrows_values_filtered_table_propagates_filters():
    expr = _parse_expression("COUNTROWS(VALUES(FILTER('sales', 'sales'[product_key] = 1)))")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "count"
    assert translation.sql is None
    assert translation.filters == ["(product_key = 1)"]


def test_translate_countrows_distinct_filtered_table_propagates_filters():
    expr = _parse_expression("COUNTROWS(DISTINCT(FILTER('sales', 'sales'[product_key] = 1)))")
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "count"
    assert translation.sql is None
    assert translation.filters == ["(product_key = 1)"]


def test_translate_countrows_datesbetween_table_propagates_filters():
    expr = _parse_expression('COUNTROWS(DATESBETWEEN(\'sales\'[order_date], "2024-01-01", "2024-12-31"))')
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "count"
    assert translation.sql is None
    assert translation.filters == ["(order_date >= '2024-01-01' AND order_date <= '2024-12-31')"]


def test_translate_summarizecolumns_multiple_tables_cross_join():
    expr = _parse_expression("SUMMARIZECOLUMNS('sales'[product_key], 'products'[category])")
    column_sql_by_table = {
        "sales": {"product_key": "product_key"},
        "products": {"category": "category"},
    }
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table={},
    )

    assert translation.sql == (
        "SELECT sales.product_key, products.category FROM sales CROSS JOIN products "
        "GROUP BY sales.product_key, products.category"
    )


def test_translate_dax_query_warns_when_unrelated_tables_are_cross_joined():
    query = _parse_query("EVALUATE SUMMARIZECOLUMNS('sales'[product_key], 'products'[category])")
    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "sales": {"product_key": "product_key"},
            "products": {"category": "category"},
        },
        measure_names_by_table={},
    )

    assert translation.warnings == []
    assert len(translation.evaluates) == 1
    assert translation.evaluates[0].warnings == [
        {
            "code": "dax_unrelated_cross_join",
            "context": "query",
            "base_table": "sales",
            "table": "products",
            "message": (
                "DAX query cross joins unrelated table 'products' with 'sales' because no relationship path is defined"
            ),
        }
    ]


def test_translate_summarizecolumns_multiple_tables_with_relationships():
    expr = _parse_expression("SUMMARIZECOLUMNS('products'[category], \"Revenue\", SUM('sales'[amount]))")
    column_sql_by_table = {
        "sales": {"amount": "amount", "product_key": "product_key"},
        "products": {"product_key": "product_key", "category": "category"},
    }
    edges = [
        RelationshipEdge(
            from_table="sales",
            from_column="product_key",
            to_table="products",
            to_column="product_key",
        )
    ]
    translation = translate_dax_table(
        expr,
        model_name=None,
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table={},
        relationship_edges=edges,
    )

    assert "LEFT JOIN" in translation.sql
    assert "products.product_key" in translation.sql
    assert "sales.product_key" in translation.sql
    assert "GROUP BY products.category" in translation.sql


def test_translate_calculate_cross_table_filter():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), 'products'[category] = \"A\")")
    column_sql_by_table = {
        "sales": {"amount": "amount"},
        "products": {"category": "category"},
    }
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "products": {"category"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert any("products" in clause for clause in translation.filters)
    assert "products" in translation.required_models


def test_translate_calculate_table_ref_filter_candidate_same_table():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), 'sales')")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == []
    assert translation.required_models == set()


def test_translate_calculate_identifier_table_filter_candidate_cross_table():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), date)")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "date": {"date_key"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == []
    assert "date" in translation.required_models


def test_translate_calculate_values_cross_table_filter_candidate():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), VALUES('date'))")
    column_sql_by_table = {
        "sales": {"amount": "amount"},
        "date": {"date_key": "date_key"},
    }
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "date": {"date_key"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == []
    assert "date" in translation.required_models


def test_translate_calculate_filter_cross_table_base_table_candidate():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), FILTER('date', 'date'[date_key] = 20240101))")
    column_sql_by_table = {
        "sales": {"amount": "amount"},
        "date": {"date_key": "date_key"},
    }
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "date": {"date_key"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(date.date_key = 20240101)"]
    assert "date" in translation.required_models


def test_translate_calculate_filter_cross_table_derived_constructor_alias_predicate_uses_exists():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), FILTER({('sales'[amount], 'date'[date_key])}, [value1] > 0))"
    )
    column_sql_by_table = {
        "sales": {"amount": "amount", "date_key": "date_key"},
        "date": {"date_key": "date_key"},
    }
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "date": {"date_key"}},
        relationship_edges=[
            RelationshipEdge(
                from_table="sales",
                from_column="date_key",
                to_table="date",
                to_column="date_key",
            )
        ],
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert len(translation.filters) == 1
    assert translation.filters[0].startswith("EXISTS (SELECT 1 FROM (SELECT * FROM (SELECT")
    assert "AS value2 FROM sales" in translation.filters[0]
    assert "AS t WHERE (value1 > 0)" in translation.filters[0]
    assert "date" in translation.required_models


def test_translate_calculate_filter_cross_table_derived_selectcolumns_alias_predicate_rewrites_directly():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), FILTER(SELECTCOLUMNS('date', \"x\", 'date'[date_key]), [x] > 20240101))"
    )
    column_sql_by_table = {
        "sales": {"amount": "amount"},
        "date": {"date_key": "date_key"},
    }
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "date": {"date_key"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert len(translation.filters) == 1
    assert translation.filters == ["(date.date_key > 20240101)"]
    assert "date" in translation.required_models


def test_translate_calculate_filter_same_table_derived_addcolumns_alias_predicate_rewrites_directly():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), FILTER(ADDCOLUMNS('sales', \"x\", 'sales'[amount]), [x] > 0))"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(amount > 0)"]


def test_translate_calculate_filters_cross_table_filter_candidate():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), FILTERS('date'[date_key]))")
    column_sql_by_table = {
        "sales": {"amount": "amount"},
        "date": {"date_key": "date_key"},
    }
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "date": {"date_key"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == []
    assert "date" in translation.required_models


def test_translate_calculate_distinct_cross_table_filter_candidate():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), DISTINCT('date'[date_key]))")
    column_sql_by_table = {
        "sales": {"amount": "amount"},
        "date": {"date_key": "date_key"},
    }
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "date": {"date_key"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == []
    assert "date" in translation.required_models


def test_translate_calculate_treatas_filter():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), TREATAS({1, 2}, 'sales'[product_key]))")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(product_key IN (1, 2))"]


def test_translate_calculate_values_table_filter_candidate():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), VALUES('sales'))")
    translation = translate_dax_metric(
        expr,
        model_name=None,
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == []
    assert translation.required_models == {"sales"}


def test_translate_calculate_values_column_filter_candidate():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), VALUES('sales'[product_key]))")
    translation = translate_dax_metric(
        expr,
        model_name=None,
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == []
    assert translation.required_models == {"sales"}


def test_translate_calculate_filters_column_filter_candidate():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), FILTERS('sales'[product_key]))")
    translation = translate_dax_metric(
        expr,
        model_name=None,
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == []
    assert translation.required_models == {"sales"}


def test_translate_calculate_distinct_column_filter_candidate():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), DISTINCT('sales'[product_key]))")
    translation = translate_dax_metric(
        expr,
        model_name=None,
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == []
    assert translation.required_models == {"sales"}


def test_translate_calculate_datesbetween_filter():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), DATESBETWEEN('sales'[order_date], \"2024-01-01\", \"2024-12-31\"))"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(order_date >= '2024-01-01' AND order_date <= '2024-12-31')"]


def test_translate_calculate_datesbetween_filter_open_ended_with_blank():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), DATESBETWEEN('sales'[order_date], BLANK, \"2024-12-31\"))"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount", "order_date": "order_date"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(order_date <= '2024-12-31')"]


def test_translate_calculate_datesbetween_filter_cross_table_start_bound():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), DATESBETWEEN('sales'[order_date], 'date'[date_key], BLANK))"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount", "order_date": "order_date"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "date": {"date_key"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(order_date >= date.date_key)"]
    assert "date" in translation.required_models


def test_translate_calculate_treatas_cross_table_filter():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), TREATAS({\"A\"}, 'products'[category]))")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "products": {"category": "category"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "products": {"category"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(products.category IN ('A'))"]
    assert "products" in translation.required_models


def test_translate_calculate_nonvisual_treatas_filter():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), NONVISUAL(TREATAS({1}, 'sales'[product_key])))")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(product_key IN (1))"]


def test_translate_calculate_removefilters_applies_to_inherited_filters():
    expr = _parse_expression(
        "CALCULATE(CALCULATE(SUM('sales'[amount]), 'sales'[product_key] = 1), REMOVEFILTERS('sales'[product_key]))"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.filters == []


def test_translate_calculate_removefilters_cross_table_clears_inherited_cross_table_filter():
    expr = _parse_expression(
        "CALCULATE(CALCULATE(SUM('sales'[amount]), FILTER('date', 'date'[date_key] = 1)), REMOVEFILTERS('date'))"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "date": {"date_key"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == []
    assert "date" in translation.required_models


def test_translate_calculate_all_clears_inherited_filters():
    expr = _parse_expression("CALCULATE(CALCULATE(SUM('sales'[amount]), 'sales'[product_key] = 1), ALL())")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.filters == []


def test_translate_calculate_all_cross_table_table_arg_tracks_required_model():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), ALL('date'))")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "date": {"date_key": "date_key"},
        },
        measure_names_by_table={"sales": set(), "date": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}, "date": {"date_key"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == []
    assert "date" in translation.required_models


def test_translate_calculate_allnoblankrow_clears_inherited_filters():
    expr = _parse_expression("CALCULATE(CALCULATE(SUM('sales'[amount]), 'sales'[product_key] = 1), ALLNOBLANKROW())")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.filters == []


def test_translate_calculate_allexcept_keeps_selected_columns():
    expr = _parse_expression(
        "CALCULATE("
        "CALCULATE(SUM('sales'[amount]), 'sales'[product_key] = 1, 'sales'[quantity] = 2), "
        "ALLEXCEPT('sales', 'sales'[product_key])"
        ")"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key", "quantity": "quantity"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_calculate_allselected_column_removes_targeted_filters():
    expr = _parse_expression(
        "CALCULATE("
        "CALCULATE(SUM('sales'[amount]), 'sales'[product_key] = 1, 'sales'[quantity] = 2), "
        "ALLSELECTED('sales'[quantity])"
        ")"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key", "quantity": "quantity"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_calculate_allcrossfiltered_table_removes_table_filters():
    expr = _parse_expression(
        "CALCULATE("
        "CALCULATE(SUM('sales'[amount]), 'sales'[product_key] = 1, 'sales'[quantity] = 2), "
        "ALLCROSSFILTERED('sales')"
        ")"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key", "quantity": "quantity"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.filters == []


def test_translate_calculate_allselected_no_args_keeps_inherited_filters():
    expr = _parse_expression("CALCULATE(CALCULATE(SUM('sales'[amount]), 'sales'[product_key] = 1), ALLSELECTED())")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.filters == ["(product_key = 1)"]


def test_translate_calculate_allselected_no_args_clears_current_scope_filters():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), 'sales'[product_key] = 1, ALLSELECTED())")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.filters == []


def test_translate_calculate_allexcept_rejects_cross_table_columns():
    expr = _parse_expression("CALCULATE(SUM('sales'[amount]), ALLEXCEPT('sales', 'products'[category]))")
    with pytest.raises(DaxTranslationError):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={
                "sales": {"amount": "amount"},
                "products": {"category": "category"},
            },
            measure_names_by_table={"sales": set(), "products": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_calculate_replaces_inherited_filter_on_same_column():
    expr = _parse_expression(
        "CALCULATE(CALCULATE(SUM('sales'[amount]), 'sales'[product_key] = 1), 'sales'[product_key] = 2)"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.filters == ["(product_key = 2)"]


def test_translate_calculate_keepfilters_preserves_inherited_filter_on_same_column():
    expr = _parse_expression(
        "CALCULATE(CALCULATE(SUM('sales'[amount]), 'sales'[product_key] = 1), KEEPFILTERS('sales'[product_key] = 2))"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={"sales": {"amount": "amount", "product_key": "product_key"}},
        measure_names_by_table={"sales": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.filters == ["(product_key = 1)", "(product_key = 2)"]


def test_translate_calculate_accumulates_relationship_overrides():
    expr = _parse_expression(
        "CALCULATE("
        "CALCULATE(SUM('sales'[amount]), USERELATIONSHIP('sales'[product_key], 'products'[product_key])), "
        "CROSSFILTER('sales'[product_key], 'products'[product_key], BOTH)"
        ")"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount", "product_key": "product_key"},
            "products": {"product_key": "product_key"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert len(translation.relationship_overrides) == 2
    assert translation.relationship_overrides[0].join_type is None
    assert translation.relationship_overrides[1].join_type == "inner"
    assert translation.relationship_overrides[1].direction == "Both"


def test_translate_calculate_crossfilter_none_direction():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), CROSSFILTER('sales'[product_key], 'products'[product_key], NONE))"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount", "product_key": "product_key"},
            "products": {"product_key": "product_key"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert len(translation.relationship_overrides) == 1
    assert translation.relationship_overrides[0].join_type == "left"
    assert translation.relationship_overrides[0].direction == "None"


def test_translate_calculate_crossfilter_oneway_leftfiltersright_direction():
    expr = _parse_expression(
        "CALCULATE("
        "SUM('sales'[amount]), "
        "CROSSFILTER('products'[product_key], 'sales'[product_key], ONEWAY_LEFTFILTERSRIGHT)"
        ")"
    )
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount", "product_key": "product_key"},
            "products": {"product_key": "product_key"},
        },
        measure_names_by_table={"sales": set(), "products": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert len(translation.relationship_overrides) == 1
    assert translation.relationship_overrides[0].join_type is None
    assert translation.relationship_overrides[0].direction == "OneWay_LeftFiltersRight"


def test_translate_calculate_crossfilter_invalid_direction_error():
    expr = _parse_expression(
        "CALCULATE(SUM('sales'[amount]), CROSSFILTER('sales'[product_key], 'products'[product_key], SIDEWAYS))"
    )
    with pytest.raises(DaxTranslationError):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={
                "sales": {"amount": "amount", "product_key": "product_key"},
                "products": {"product_key": "product_key"},
            },
            measure_names_by_table={"sales": set(), "products": set()},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_cross_table_metric_tracks_required_model():
    expr = _parse_expression("SUM('other'[amount])")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount", "order_date": "order_date"},
            "other": {"amount": "amount"},
        },
        measure_names_by_table={"sales": {"Total Sales"}, "other": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={"sales": {"order_date"}},
    )

    assert translation.agg == "sum"
    assert translation.sql == "other.amount"
    assert "other" in translation.required_models


def test_translate_model_scoped_derived_cross_table_metric_tracks_required_model():
    expr = _parse_expression("'sales'[amount] + 'other'[amount]")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "other": {"amount": "amount"},
        },
        measure_names_by_table={"sales": set(), "other": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={},
    )

    assert translation.type == "derived"
    assert translation.sql == "(amount + other.amount)"
    assert "other" in translation.required_models


def test_translate_model_scoped_var_derived_cross_table_metric_tracks_required_model():
    expr = _parse_expression("VAR x = 'other'[amount] RETURN x + 'sales'[amount]")
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "other": {"amount": "amount"},
        },
        measure_names_by_table={"sales": set(), "other": set()},
        measure_aggs_by_table={},
        time_dimensions_by_table={},
    )

    assert translation.type == "derived"
    assert translation.sql == "(other.amount + amount)"
    assert "other" in translation.required_models


def test_time_intelligence_requires_known_time_dimension():
    expr = _parse_expression("CALCULATE([Total Sales], SAMEPERIODLASTYEAR('sales'[event_ts]))")
    with pytest.raises(DaxTranslationError):
        translate_dax_metric(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"amount": "amount", "event_ts": "event_ts"}},
            measure_names_by_table={"sales": {"Total Sales"}},
            measure_aggs_by_table={},
            time_dimensions_by_table={"sales": {"order_date"}},
        )


def test_translate_table_rejects_scalar_expression_type():
    expr = _parse_expression("1")
    with pytest.raises(DaxTranslationError, match="Unsupported table expression type 'Number'"):
        translate_dax_table(
            expr,
            model_name=None,
            column_sql_by_table={"sales": {"amount": "amount"}},
            measure_names_by_table={},
        )


def test_translate_table_rejects_unknown_identifier_as_table():
    expr = _parse_expression("UnknownTable")
    with pytest.raises(DaxTranslationError, match="Unknown table identifier 'UnknownTable'"):
        translate_dax_table(
            expr,
            model_name=None,
            column_sql_by_table={"sales": {"amount": "amount"}},
            measure_names_by_table={},
        )


def test_translate_scalar_rejects_query_expression_node_type():
    expr = _parse_query("EVALUATE 'sales'")
    with pytest.raises(DaxTranslationError, match="Unsupported DAX expression type 'Query'"):
        translate_dax_scalar(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"amount": "amount"}},
        )


def test_translate_scalar_unknown_function_error_is_explicit():
    expr = _parse_expression("UNKNOWNFUNC(1, 2)")
    with pytest.raises(DaxTranslationError, match="Unsupported scalar function 'UNKNOWNFUNC'"):
        translate_dax_scalar(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"amount": "amount"}},
        )


def test_translate_scalar_calc_group_function_error_is_explicit():
    expr = _parse_expression("SELECTEDMEASURE()")
    with pytest.raises(DaxTranslationError, match="SELECTEDMEASURE is only supported in calculation group expressions"):
        translate_dax_scalar(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"amount": "amount"}},
        )


def test_translate_table_detailrows_function_error_is_explicit():
    expr = _parse_expression("DETAILROWS('sales')")
    with pytest.raises(DaxTranslationError, match="DETAILROWS is only supported in model detail rows expressions"):
        translate_dax_table(
            expr,
            model_name=None,
            column_sql_by_table={"sales": {"amount": "amount"}},
            measure_names_by_table={},
        )


def test_translate_scalar_substitutewithindex_table_function_error_is_explicit():
    expr = _parse_expression("SUBSTITUTEWITHINDEX('sales', \"Idx\", 'sales'[amount], 'sales')")
    with pytest.raises(
        DaxTranslationError, match="SUBSTITUTEWITHINDEX returns a table and is not valid in scalar context"
    ):
        translate_dax_scalar(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"amount": "amount"}},
        )


def test_translate_calculate_substitutewithindex_filter_propagates_underlying_filters():
    expr = _parse_expression(
        "CALCULATE("
        "SUM('sales'[amount]), "
        "SUBSTITUTEWITHINDEX("
        "FILTER('sales', 'sales'[amount] > 100), "
        '"Idx", '
        "VALUES('sales'[product_key]), "
        "'sales'[product_key]"
        ")"
        ")"
    )
    column_sql_by_table, measure_names_by_table, time_dimensions_by_table = _sales_maps()
    translation = translate_dax_metric(
        expr,
        model_name="sales",
        column_sql_by_table=column_sql_by_table,
        measure_names_by_table=measure_names_by_table,
        measure_aggs_by_table={},
        time_dimensions_by_table=time_dimensions_by_table,
    )

    assert translation.agg == "sum"
    assert translation.sql == "amount"
    assert translation.filters == ["(amount > 100)"]


def test_translate_table_calc_group_function_error_is_explicit():
    expr = _parse_expression("SELECTEDMEASURE()")
    with pytest.raises(DaxTranslationError, match="SELECTEDMEASURE is only supported in calculation group expressions"):
        translate_dax_table(
            expr,
            model_name=None,
            column_sql_by_table={"sales": {"amount": "amount"}},
            measure_names_by_table={},
        )


def test_translate_scalar_table_function_error_is_explicit():
    expr = _parse_expression("INTERSECT({1, 2}, {2, 3})")
    with pytest.raises(DaxTranslationError, match="INTERSECT returns a table and is not valid in scalar context"):
        translate_dax_scalar(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"amount": "amount"}},
        )


def test_translate_scalar_calculate_filter_only_function_error_is_explicit():
    expr = _parse_expression("USERELATIONSHIP('sales'[product_key], 'products'[product_key])")
    with pytest.raises(DaxTranslationError, match="USERELATIONSHIP is only valid in CALCULATE filter arguments"):
        translate_dax_scalar(
            expr,
            model_name="sales",
            column_sql_by_table={
                "sales": {"product_key": "product_key"},
                "products": {"product_key": "product_key"},
            },
        )


def test_translate_scalar_crossfilter_calculate_filter_only_function_error_is_explicit():
    expr = _parse_expression("CROSSFILTER('sales'[product_key], 'products'[product_key], BOTH)")
    with pytest.raises(DaxTranslationError, match="CROSSFILTER is only valid in CALCULATE filter arguments"):
        translate_dax_scalar(
            expr,
            model_name="sales",
            column_sql_by_table={
                "sales": {"product_key": "product_key"},
                "products": {"product_key": "product_key"},
            },
        )


def test_translate_scalar_previousweek_table_function_error_is_explicit():
    expr = _parse_expression("PREVIOUSWEEK('date'[date_key])")
    with pytest.raises(DaxTranslationError, match="PREVIOUSWEEK returns a table and is not valid in scalar context"):
        translate_dax_scalar(
            expr,
            model_name="sales",
            column_sql_by_table={
                "sales": {"amount": "amount"},
                "date": {"date_key": "date_key"},
            },
            time_dimensions_by_table={"date": {"date_key"}},
        )


def test_translate_scalar_nextweek_table_function_error_is_explicit():
    expr = _parse_expression("NEXTWEEK('date'[date_key])")
    with pytest.raises(DaxTranslationError, match="NEXTWEEK returns a table and is not valid in scalar context"):
        translate_dax_scalar(
            expr,
            model_name="sales",
            column_sql_by_table={
                "sales": {"amount": "amount"},
                "date": {"date_key": "date_key"},
            },
            time_dimensions_by_table={"date": {"date_key"}},
        )


def test_translate_scalar_rollup_wrapper_table_function_error_is_explicit():
    expr = _parse_expression("ROLLUP('sales'[product_key])")
    with pytest.raises(DaxTranslationError, match="ROLLUP returns a table and is not valid in scalar context"):
        translate_dax_scalar(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"product_key": "product_key"}},
        )


def test_translate_scalar_keepcolumns_table_function_error_is_explicit():
    expr = _parse_expression("KEEPCOLUMNS('sales', 'sales'[product_key])")
    with pytest.raises(DaxTranslationError, match="KEEPCOLUMNS returns a table and is not valid in scalar context"):
        translate_dax_scalar(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"product_key": "product_key"}},
        )


@pytest.mark.parametrize(
    ("expr_text", "func_name"),
    [
        ("ROLLUPGROUP('sales'[product_key])", "ROLLUPGROUP"),
        ("ROLLUPADDISSUBTOTAL(\"IsTotal\", 'sales'[product_key])", "ROLLUPADDISSUBTOTAL"),
        ("ROLLUPISSUBTOTAL(\"IsTotal\", 'sales'[product_key])", "ROLLUPISSUBTOTAL"),
    ],
)
def test_translate_scalar_rollup_wrapper_table_functions_error_is_explicit(expr_text: str, func_name: str):
    expr = _parse_expression(expr_text)
    with pytest.raises(DaxTranslationError, match=rf"{func_name} returns a table and is not valid in scalar context"):
        translate_dax_scalar(
            expr,
            model_name="sales",
            column_sql_by_table={"sales": {"product_key": "product_key"}},
        )
