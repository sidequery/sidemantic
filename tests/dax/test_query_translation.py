from __future__ import annotations

from pathlib import Path

import pytest
import sidemantic_dax

from sidemantic.dax import DaxTranslationError, RelationshipEdge, translate_dax_query
from sidemantic.dax.translator import _rewrite_expr_for_alias

ROOT = Path(__file__).resolve().parents[2]


def _parse_query(query: str):
    try:
        return sidemantic_dax.parse_query(query)
    except RuntimeError as exc:
        if "native module is not available" in str(exc):
            pytest.skip("sidemantic_dax native module not available")
        raise


def _query_docs_blocks() -> list[str]:
    fixture = ROOT / "tests" / "dax" / "fixtures" / "query-docs" / "queries.txt"
    text = fixture.read_text()
    blocks = [block.strip() for block in text.split("---") if block.strip()]
    queries: list[str] = []
    for block in blocks:
        lines = [line for line in block.splitlines() if not line.strip().startswith("# source:")]
        query = "\n".join(lines).strip()
        if query:
            queries.append(query)
    return queries


def test_translate_query_order_by_and_start_at():
    query = _parse_query(
        """
        EVALUATE
            'Sales Order'
            ORDER BY 'Sales Order'[Sales Order] ASC
            START AT "SO43661"
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales Order": {
                "Sales Order": '"Sales Order"',
            }
        },
        measure_names_by_table={"Sales Order": set()},
    )

    assert len(translation.evaluates) == 1
    sql = translation.evaluates[0].sql
    assert 'SELECT * FROM (SELECT * FROM "Sales Order") AS q' in sql
    assert "WHERE (q.\"Sales Order\" >= 'SO43661')" in sql
    assert 'ORDER BY q."Sales Order" ASC' in sql


def test_translate_query_metric_reference_preserves_metric_filters():
    query = _parse_query('EVALUATE ROW("West", [West Sales])')

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {
                "Amount": "Amount",
                "Region": "Region",
            }
        },
        measure_names_by_table={"Sales": {"West Sales"}},
        measure_aggs_by_table={"Sales": {"West Sales": "sum"}},
        measure_sql_by_table={"Sales": {"West Sales": "Amount"}},
        measure_filters_by_table={"Sales": {"West Sales": ["Sales.Region = 'West'"]}},
    )

    sql = translation.evaluates[0].sql
    assert "SUM(CASE WHEN Sales.Region = 'West' THEN Sales.Amount ELSE NULL END)" in sql


def test_translate_query_docs_fixture_blocks():
    column_sql_by_table = {
        "Sales Order": {
            "Sales Order": '"Sales Order"',
            "Sales Order Line": '"Sales Order Line"',
            "SalesOrderLineKey": "SalesOrderLineKey",
            "CustomerKey": "CustomerKey",
            "DateKey": "DateKey",
        },
        "Date": {
            "Month Name": '"Month Name"',
            "Month of Year": '"Month of Year"',
            "Fiscal Year": '"Fiscal Year"',
            "DateKey": "DateKey",
        },
        "Product": {
            "Category": "Category",
        },
        "Sales": {
            "Amount": "Amount",
            "ProductKey": "ProductKey",
            "DateKey": "DateKey",
            "CustomerKey": "CustomerKey",
        },
        "Customer": {
            "CustomerKey": "CustomerKey",
        },
        "Unbought products": {
            "Year Range": '"Year Range"',
        },
        "Pick a sales measure": {},
    }

    for idx, query_text in enumerate(_query_docs_blocks(), start=1):
        query = _parse_query(query_text)
        translation = translate_dax_query(query, column_sql_by_table=column_sql_by_table)
        assert translation.evaluates, f"Expected EVALUATE statements for query-doc block {idx}"


def test_translate_query_keepfilters_wrapped_table_expression():
    query = _parse_query(
        """
        EVALUATE
            KEEPFILTERS(
                FILTER('Sales', 'Sales'[Amount] > 0)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT * FROM Sales WHERE (Amount > 0)" in sql


def test_translate_query_nonvisual_wrapped_table_expression():
    query = _parse_query(
        """
        EVALUATE
            NONVISUAL(
                FILTER('Sales', 'Sales'[Amount] > 0)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT * FROM Sales WHERE (Amount > 0)" in sql


def test_translate_query_order_by_multikey_start_at_mixed_direction():
    query = _parse_query(
        """
        EVALUATE
            'Sales'
            ORDER BY 'Sales'[Region] ASC, 'Sales'[Amount] DESC
            START AT "US", 100
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {
                "Region": "Region",
                "Amount": "Amount",
            }
        },
    )

    sql = translation.evaluates[0].sql
    assert "WHERE (q.Region > 'US') OR (q.Region = 'US' AND q.Amount <= 100)" in sql
    assert "ORDER BY q.Region ASC, q.Amount DESC" in sql


def test_translate_query_order_by_multikey_start_at_prefix():
    query = _parse_query(
        """
        EVALUATE
            'Sales'
            ORDER BY 'Sales'[Region] ASC, 'Sales'[Amount] DESC
            START AT "US"
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {
                "Region": "Region",
                "Amount": "Amount",
            }
        },
    )

    sql = translation.evaluates[0].sql
    assert "WHERE (q.Region >= 'US')" in sql
    assert "ORDER BY q.Region ASC, q.Amount DESC" in sql


def test_translate_query_order_by_expression_start_at():
    query = _parse_query(
        """
        EVALUATE
            'Sales'
            ORDER BY UPPER('Sales'[Region]) ASC
            START AT "US"
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {
                "Region": "Region",
            }
        },
    )

    sql = translation.evaluates[0].sql
    assert "WHERE (UPPER(q.Region) >= 'US')" in sql
    assert "ORDER BY UPPER(q.Region) ASC" in sql


def test_translate_query_order_by_expression_start_at_when_sqlglot_rewrite_fails(monkeypatch):
    import sqlglot

    query = _parse_query(
        """
        EVALUATE
            'Sales'
            ORDER BY UPPER('Sales'[Region]) ASC
            START AT "US"
        """
    )

    monkeypatch.setattr(sqlglot, "parse_one", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("boom")))

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {
                "Region": "Region",
            }
        },
    )

    sql = translation.evaluates[0].sql
    assert "WHERE (UPPER(q.Region) >= 'US')" in sql
    assert "ORDER BY UPPER(q.Region) ASC" in sql


def test_translate_query_start_at_accepts_expression_value():
    query = _parse_query(
        """
        EVALUATE
            'Sales'
            ORDER BY 'Sales'[Order Date] ASC
            START AT DATE(2024, 1, 1)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {
                "Order Date": '"Order Date"',
            }
        },
    )

    sql = translation.evaluates[0].sql
    assert 'WHERE (q."Order Date" >= MAKE_DATE(2024, 1, 1))' in sql
    assert 'ORDER BY q."Order Date" ASC' in sql


def test_translate_query_summarizecolumns_order_by():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Month of Year],
                "Revenue", SUM('Sales'[Amount])
            )
            ORDER BY 'Date'[Month of Year] ASC
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"DateKey": "DateKey", "Month of Year": '"Month of Year"'},
            "Sales": {"DateKey": "DateKey", "Amount": "Amount"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="DateKey",
                to_table="Date",
                to_column="DateKey",
            )
        ],
    )

    assert len(translation.evaluates) == 1
    sql = translation.evaluates[0].sql
    assert "SUM(Sales.Amount) AS Revenue" in sql
    assert 'GROUP BY "Month of Year"' in sql
    assert 'ORDER BY q."Month of Year" ASC' in sql


def test_translate_query_summarizecolumns_countrows_cross_table_uses_relationship_group_context():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Month of Year],
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"DateKey": "DateKey", "Month of Year": '"Month of Year"'},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="DateKey",
                to_table="Date",
                to_column="DateKey",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(Sales.DateKey) AS Rows" in sql
    assert "FROM Date LEFT JOIN Sales ON Date.DateKey = Sales.DateKey" in sql
    assert 'GROUP BY "Month of Year"' in sql


def test_translate_query_summarizecolumns_countrows_relatedtable_uses_relationship_group_context():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[FiscalYear],
                "Rows", COUNTROWS(RELATEDTABLE('Sales'))
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"DateKey": "DateKey", "FiscalYear": "FiscalYear"},
            "Sales": {"DateKey": "DateKey", "Amount": "Amount"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="DateKey",
                to_table="Date",
                to_column="DateKey",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(Sales.DateKey) AS Rows" in sql
    assert "FROM Date LEFT JOIN Sales ON Date.DateKey = Sales.DateKey" in sql
    assert "GROUP BY Date.FiscalYear" in sql


def test_translate_query_summarizecolumns_countrows_calculatetable_related_table_uses_relationship_group_context():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[FiscalYear],
                "Customers", COUNTROWS(CALCULATETABLE('Customer'))
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"DateKey": "DateKey", "FiscalYear": "FiscalYear"},
            "Sales": {"DateKey": "DateKey", "CustomerKey": "CustomerKey"},
            "Customer": {"CustomerKey": "CustomerKey"},
        },
        relationship_edges=[
            RelationshipEdge("Sales", "DateKey", "Date", "DateKey"),
            RelationshipEdge("Sales", "CustomerKey", "Customer", "CustomerKey"),
        ],
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(Customer.CustomerKey) AS Customers" in sql
    assert (
        "FROM Date LEFT JOIN Sales ON Date.DateKey = Sales.DateKey LEFT JOIN Customer ON Sales.CustomerKey = Customer.CustomerKey"
        in sql
    )
    assert "GROUP BY Date.FiscalYear" in sql


@pytest.mark.parametrize("wrapper", ["VALUES", "FILTERS", "DISTINCT"])
def test_translate_query_summarizecolumns_countrows_distinct_table_wrappers_use_group_context(wrapper: str):
    query = _parse_query(
        f"""
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[FiscalYear],
                "Customers", COUNTROWS({wrapper}('Customer'))
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"DateKey": "DateKey", "FiscalYear": "FiscalYear"},
            "Sales": {"DateKey": "DateKey", "CustomerKey": "CustomerKey"},
            "Customer": {"Name": "Name", "CustomerKey": "CustomerKey"},
        },
        relationship_edges=[
            RelationshipEdge("Sales", "DateKey", "Date", "DateKey"),
            RelationshipEdge("Sales", "CustomerKey", "Customer", "CustomerKey"),
        ],
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(DISTINCT Customer.CustomerKey) AS Customers" in sql
    assert (
        "FROM Date LEFT JOIN Sales ON Date.DateKey = Sales.DateKey LEFT JOIN Customer ON Sales.CustomerKey = Customer.CustomerKey"
        in sql
    )
    assert "GROUP BY Date.FiscalYear" in sql


def test_translate_query_summarizecolumns_countrows_calculatetable_values_with_filter_uses_grouped_distinct_count():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[FiscalYear],
                "Customers", COUNTROWS(CALCULATETABLE(VALUES('Customer'), 'Customer'[Name] <> ""))
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"DateKey": "DateKey", "FiscalYear": "FiscalYear"},
            "Sales": {"DateKey": "DateKey", "CustomerKey": "CustomerKey"},
            "Customer": {"Name": "Name", "CustomerKey": "CustomerKey"},
        },
        relationship_edges=[
            RelationshipEdge("Sales", "DateKey", "Date", "DateKey"),
            RelationshipEdge("Sales", "CustomerKey", "Customer", "CustomerKey"),
        ],
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(DISTINCT CASE WHEN (Customer.Name <> '') THEN Customer.CustomerKey END) AS Customers" in sql
    assert (
        "FROM Date LEFT JOIN Sales ON Date.DateKey = Sales.DateKey LEFT JOIN Customer ON Sales.CustomerKey = Customer.CustomerKey"
        in sql
    )
    assert "GROUP BY Date.FiscalYear" in sql


def test_translate_query_summarizecolumns_keepfilters_filter():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[ProductKey],
                KEEPFILTERS(FILTER('Sales', 'Sales'[ProductKey] = 1)),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey"}},
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(*) AS Rows" in sql
    assert "WHERE (Sales.ProductKey = 1)" in sql
    assert "GROUP BY Sales.ProductKey" in sql


def test_translate_query_summarizecolumns_filter_with_keepfilters_wrapped_base_table():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[ProductKey],
                FILTER(
                    KEEPFILTERS(FILTER('Sales', 'Sales'[Amount] > 0)),
                    'Sales'[Amount] > 10
                ),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey", "Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "WHERE (Sales.Amount > 0) AND (Sales.Amount > 10)" in sql
    assert "GROUP BY Sales.ProductKey" in sql


def test_translate_query_summarizecolumns_filter_with_nonvisual_wrapped_base_table():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[ProductKey],
                FILTER(
                    NONVISUAL(FILTER('Sales', 'Sales'[Amount] > 0)),
                    'Sales'[Amount] > 10
                ),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey", "Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "WHERE (Sales.Amount > 0) AND (Sales.Amount > 10)" in sql
    assert "GROUP BY Sales.ProductKey" in sql


def test_translate_query_summarizecolumns_all_filter_table_arg():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[ProductKey],
                ALL('Sales'),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey"}},
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(*) AS Rows" in sql
    assert "GROUP BY Sales.ProductKey" in sql


def test_translate_query_summarizecolumns_allnoblankrow_filter_table_arg():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[ProductKey],
                ALLNOBLANKROW('Sales'),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey"}},
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(*) AS Rows" in sql
    assert "GROUP BY Sales.ProductKey" in sql


def test_translate_query_summarizecolumns_groupby_filter_table_arg():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[Category],
                GROUPBY(
                    FILTER('Sales', 'Sales'[Amount] > 0),
                    'Sales'[Category]
                ),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Category": "Category", "Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(*) AS Rows" in sql
    assert "WHERE (Sales.Amount > 0)" in sql
    assert "GROUP BY Sales.Category" in sql


def test_translate_query_summarizecolumns_datatable_filter_table_arg():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[Category],
                DATATABLE(
                    "k", INTEGER,
                    "v", STRING,
                    {{1, "a"}}
                ),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Category": "Category"}},
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(*) AS Rows" in sql
    assert "GROUP BY Sales.Category" in sql


def test_translate_query_summarizecolumns_topnskip_filter_table_arg():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[Category],
                TOPNSKIP(
                    2,
                    0,
                    FILTER('Sales', 'Sales'[Amount] > 0),
                    'Sales'[Amount], DESC
                ),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Category": "Category", "Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(*) AS Rows" in sql
    assert "WHERE (Sales.Amount > 0)" in sql
    assert "GROUP BY Sales.Category" in sql


def test_translate_query_summarizecolumns_rejects_scalar_function_arg():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[Category],
                ABS(1),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    with pytest.raises(DaxTranslationError, match="Unsupported SUMMARIZECOLUMNS argument"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category"}},
        )


def test_translate_query_summarizecolumns_rejects_unknown_identifier_arg():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[Category],
                UnknownIdentifier,
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    with pytest.raises(DaxTranslationError, match="Unsupported SUMMARIZECOLUMNS argument"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category"}},
        )


def test_translate_query_summarizecolumns_rejects_unknown_table_function_arg():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[Category],
                UNKNOWNTABLEFN('Sales'),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    with pytest.raises(DaxTranslationError, match="Unsupported SUMMARIZECOLUMNS argument"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category"}},
        )


def test_translate_query_unknown_table_function_error_is_explicit():
    query = _parse_query(
        """
        EVALUATE
            UNKNOWNTABLEFN('Sales')
        """
    )

    with pytest.raises(DaxTranslationError, match="Unsupported table function 'UNKNOWNTABLEFN'"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category"}},
        )


def test_translate_query_unknown_scalar_function_error_is_explicit():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", UNKNOWNFUNC(1))
        """
    )

    with pytest.raises(DaxTranslationError, match="Unsupported scalar function 'UNKNOWNFUNC'"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category"}},
        )


@pytest.mark.parametrize(
    ("scalar_expr", "func_name"),
    [
        ("SELECTEDMEASURE()", "SELECTEDMEASURE"),
        ("SELECTEDMEASURENAME()", "SELECTEDMEASURENAME"),
        ("SELECTEDMEASUREFORMATSTRING()", "SELECTEDMEASUREFORMATSTRING"),
        ("ISSELECTEDMEASURE(1)", "ISSELECTEDMEASURE"),
    ],
)
def test_translate_query_calc_group_scalar_function_error_is_explicit(scalar_expr: str, func_name: str):
    query = _parse_query(
        f"""
        EVALUATE
            ROW("x", {scalar_expr})
        """
    )

    with pytest.raises(DaxTranslationError, match=f"{func_name} is only supported in calculation group expressions"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category"}},
        )


def test_translate_query_detailrows_table_function_error_is_explicit():
    query = _parse_query(
        """
        EVALUATE
            DETAILROWS('Sales')
        """
    )

    with pytest.raises(DaxTranslationError, match="DETAILROWS is only supported in model detail rows expressions"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category"}},
        )


def test_translate_query_substitutewithindex_table_expression():
    query = _parse_query(
        """
        EVALUATE
            SUBSTITUTEWITHINDEX('Sales', "Idx", VALUES('Sales'[Category]), 'Sales'[Category])
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Category": "Category", "Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT l.Amount, i.__substitutewithindex_rank AS Idx FROM (SELECT * FROM Sales) AS l LEFT JOIN (" in sql
    assert "DENSE_RANK() OVER (ORDER BY i0.Category ASC) AS __substitutewithindex_rank" in sql
    assert "ON l.Category IS NOT DISTINCT FROM i.Category" in sql


def test_translate_query_substitutewithindex_table_expression_desc_order():
    query = _parse_query(
        """
        EVALUATE
            SUBSTITUTEWITHINDEX('Sales', "Idx", VALUES('Sales'[Category]), 'Sales'[Category], DESC)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Category": "Category", "Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "DENSE_RANK() OVER (ORDER BY i0.Category DESC) AS __substitutewithindex_rank" in sql


def test_translate_query_substitutewithindex_table_expression_multi_order_keys():
    query = _parse_query(
        """
        EVALUATE
            SUBSTITUTEWITHINDEX(
                'Sales',
                "Idx",
                SUMMARIZE('Sales', 'Sales'[Category], 'Sales'[Amount]),
                'Sales'[Category], ASC,
                'Sales'[Amount], DESC
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Category": "Category", "Amount": "Amount", "Quantity": "Quantity"}},
    )

    sql = translation.evaluates[0].sql
    assert "DENSE_RANK() OVER (ORDER BY i0.Category ASC, i0.Amount DESC) AS __substitutewithindex_rank" in sql


def test_translate_query_substitutewithindex_requires_exactly_one_index_table_argument():
    query = _parse_query(
        """
        EVALUATE
            SUBSTITUTEWITHINDEX('Sales', "Idx", 'Sales'[Category], 'Sales'[Category], ASC)
        """
    )

    with pytest.raises(DaxTranslationError, match="SUBSTITUTEWITHINDEX requires exactly one index table argument"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category", "Amount": "Amount"}},
        )


def test_translate_query_substitutewithindex_supports_wrapped_index_table_argument():
    query = _parse_query(
        """
        EVALUATE
            SUBSTITUTEWITHINDEX(
                'Sales',
                "Idx",
                CALCULATETABLE(
                    VALUES('Sales'[Category]),
                    'Sales'[Amount] > 100
                ),
                'Sales'[Category]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Category": "Category", "Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Sales.Category FROM Sales" in sql
    assert "WHERE (Amount > 100)" in sql
    assert "DENSE_RANK() OVER (ORDER BY i0.Category ASC) AS __substitutewithindex_rank" in sql


def test_translate_query_substitutewithindex_rejects_order_by_column_not_from_index_table():
    query = _parse_query(
        """
        EVALUATE
            SUBSTITUTEWITHINDEX(
                'Sales',
                "Idx",
                VALUES('Sales'[Category]),
                'Sales'[Amount],
                ASC
            )
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="SUBSTITUTEWITHINDEX ORDER BY expressions must reference columns from the index table argument",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category", "Amount": "Amount"}},
        )


def test_translate_query_substitutewithindex_supports_cross_table_order_by_column_from_index_table():
    query = _parse_query(
        """
        EVALUATE
            SUBSTITUTEWITHINDEX(
                VALUES('Sales'[Amount]),
                "Idx",
                CROSSJOIN('Sales', 'Products'),
                'Products'[Weight],
                DESC
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
            "Products": {"ProductKey": "ProductKey", "Weight": "Weight"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "DENSE_RANK() OVER (ORDER BY i0.Weight DESC) AS __substitutewithindex_rank" in sql


def test_translate_query_substitutewithindex_rejects_ambiguous_common_column_in_source_table():
    query = _parse_query(
        """
        EVALUATE
            SUBSTITUTEWITHINDEX(
                CROSSJOIN('Sales', 'Products'),
                "Idx",
                VALUES('Sales'[ProductKey]),
                'Sales'[ProductKey]
            )
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="SUBSTITUTEWITHINDEX source table has ambiguous common column 'ProductKey'",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Sales": {"ProductKey": "ProductKey", "Amount": "Amount"},
                "Products": {"ProductKey": "ProductKey", "Weight": "Weight"},
            },
        )


def test_translate_query_substitutewithindex_rejects_ambiguous_order_by_column_in_index_table_qualified_ref():
    query = _parse_query(
        """
        EVALUATE
            SUBSTITUTEWITHINDEX(
                VALUES('Sales'[Amount]),
                "Idx",
                CROSSJOIN('Sales', 'Products'),
                'Products'[ProductKey]
            )
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="SUBSTITUTEWITHINDEX ORDER BY column 'ProductKey' is ambiguous in index table expression",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Sales": {"ProductKey": "ProductKey", "Amount": "Amount"},
                "Products": {"ProductKey": "ProductKey", "Weight": "Weight"},
            },
        )


def test_translate_query_substitutewithindex_rejects_ambiguous_common_column_in_index_table():
    query = _parse_query(
        """
        EVALUATE
            SUBSTITUTEWITHINDEX(
                VALUES('Sales'[ProductKey]),
                "Idx",
                CROSSJOIN('Sales', 'Products'),
                'Sales'[ProductKey]
            )
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="SUBSTITUTEWITHINDEX index table has ambiguous common column 'ProductKey'",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Sales": {"ProductKey": "ProductKey", "Amount": "Amount"},
                "Products": {"ProductKey": "ProductKey", "Weight": "Weight"},
            },
        )


def test_translate_query_substitutewithindex_rejects_ambiguous_order_by_column_in_index_table():
    query = _parse_query(
        """
        EVALUATE
            SUBSTITUTEWITHINDEX(
                VALUES('Sales'[Amount]),
                "Idx",
                CROSSJOIN('Sales', 'Products'),
                'Sales'[ProductKey]
            )
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="SUBSTITUTEWITHINDEX ORDER BY column 'ProductKey' is ambiguous in index table expression",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Sales": {"ProductKey": "ProductKey", "Amount": "Amount"},
                "Products": {"ProductKey": "ProductKey", "Weight": "Weight"},
            },
        )


def test_translate_query_substitutewithindex_in_scalar_context_error_is_explicit():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", SUBSTITUTEWITHINDEX('Sales', "Idx", 'Sales'[Category], 'Sales'))
        """
    )

    with pytest.raises(
        DaxTranslationError, match="SUBSTITUTEWITHINDEX returns a table and is not valid in scalar context"
    ):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category"}},
        )


def test_translate_query_evaluate_calculatetable_substitutewithindex_preserves_underlying_filters():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE(
                'Sales',
                SUBSTITUTEWITHINDEX(
                    FILTER('Sales', 'Sales'[Amount] > 100),
                    "Idx",
                    VALUES('Sales'[Category]),
                    'Sales'[Category]
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount", "Category": "Category"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT * FROM Sales WHERE (Sales.Amount > 100)" in sql


@pytest.mark.parametrize(
    ("table_expr", "func_name"),
    [
        ("SELECTEDMEASURE()", "SELECTEDMEASURE"),
        ("SELECTEDMEASURENAME()", "SELECTEDMEASURENAME"),
        ("SELECTEDMEASUREFORMATSTRING()", "SELECTEDMEASUREFORMATSTRING"),
        ("ISSELECTEDMEASURE(1)", "ISSELECTEDMEASURE"),
    ],
)
def test_translate_query_calc_group_scalar_function_in_table_context_error_is_explicit(table_expr: str, func_name: str):
    query = _parse_query(
        f"""
        EVALUATE
            {table_expr}
        """
    )

    with pytest.raises(DaxTranslationError, match=f"{func_name} is only supported in calculation group expressions"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category"}},
        )


def test_translate_query_row_table_function_in_scalar_context_error_is_explicit():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", INTERSECT({1, 2}, {2, 3}))
        """
    )

    with pytest.raises(DaxTranslationError, match="INTERSECT returns a table and is not valid in scalar context"):
        translate_dax_query(query)


def test_translate_query_row_keepcolumns_table_function_in_scalar_context_error_is_explicit():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", KEEPCOLUMNS('Sales', 'Sales'[ProductKey]))
        """
    )

    with pytest.raises(DaxTranslationError, match="KEEPCOLUMNS returns a table and is not valid in scalar context"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"ProductKey": "ProductKey"}},
        )


def test_translate_query_row_calculate_filter_only_function_error_is_explicit():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", USERELATIONSHIP('sales'[product_key], 'products'[product_key]))
        """
    )

    with pytest.raises(DaxTranslationError, match="USERELATIONSHIP is only valid in CALCULATE filter arguments"):
        translate_dax_query(query)


def test_translate_query_row_crossfilter_calculate_filter_only_function_error_is_explicit():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", CROSSFILTER('sales'[product_key], 'products'[product_key], BOTH))
        """
    )

    with pytest.raises(DaxTranslationError, match="CROSSFILTER is only valid in CALCULATE filter arguments"):
        translate_dax_query(query)


def test_translate_query_row_previousweek_table_function_in_scalar_context_error_is_explicit():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", PREVIOUSWEEK('Date'[DateKey]))
        """
    )

    with pytest.raises(DaxTranslationError, match="PREVIOUSWEEK returns a table and is not valid in scalar context"):
        translate_dax_query(
            query,
            column_sql_by_table={"Date": {"DateKey": "DateKey"}},
            time_dimensions_by_table={"Date": {"DateKey"}},
        )


def test_translate_query_row_nextweek_table_function_in_scalar_context_error_is_explicit():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", NEXTWEEK('Date'[DateKey]))
        """
    )

    with pytest.raises(DaxTranslationError, match="NEXTWEEK returns a table and is not valid in scalar context"):
        translate_dax_query(
            query,
            column_sql_by_table={"Date": {"DateKey": "DateKey"}},
            time_dimensions_by_table={"Date": {"DateKey"}},
        )


def test_translate_query_row_rollup_wrapper_in_scalar_context_error_is_explicit():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", ROLLUP('Sales'[ProductKey]))
        """
    )

    with pytest.raises(DaxTranslationError, match="ROLLUP returns a table and is not valid in scalar context"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"ProductKey": "ProductKey"}},
        )


@pytest.mark.parametrize(
    ("func_expr", "func_name"),
    [
        ("ROLLUPGROUP('Sales'[ProductKey])", "ROLLUPGROUP"),
        ("ROLLUPADDISSUBTOTAL(\"IsTotal\", 'Sales'[ProductKey])", "ROLLUPADDISSUBTOTAL"),
        ("ROLLUPISSUBTOTAL(\"IsTotal\", 'Sales'[ProductKey])", "ROLLUPISSUBTOTAL"),
    ],
)
def test_translate_query_row_rollup_wrapper_table_functions_in_scalar_context_error_is_explicit(
    func_expr: str, func_name: str
):
    query = _parse_query(
        f"""
        EVALUATE
            ROW("x", {func_expr})
        """
    )

    with pytest.raises(DaxTranslationError, match=rf"{func_name} returns a table and is not valid in scalar context"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"ProductKey": "ProductKey"}},
        )


def test_translate_query_sum_rejects_more_than_one_argument():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", SUM(1, 2))
        """
    )

    with pytest.raises(DaxTranslationError, match="SUM supports exactly one argument"):
        translate_dax_query(query)


def test_translate_query_countrows_rejects_more_than_one_argument():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", COUNTROWS('Sales', 'Sales'))
        """
    )

    with pytest.raises(DaxTranslationError, match="COUNTROWS supports at most one argument"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category"}},
        )


def test_translate_query_divide_rejects_more_than_three_arguments():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", DIVIDE(10, 2, 0, 1))
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="DIVIDE supports at most numerator, denominator, and alternate result arguments",
    ):
        translate_dax_query(query)


def test_translate_query_and_or_reject_more_than_two_arguments():
    and_query = _parse_query(
        """
        EVALUATE
            ROW("x", AND(TRUE(), FALSE(), TRUE()))
        """
    )
    or_query = _parse_query(
        """
        EVALUATE
            ROW("x", OR(TRUE(), FALSE(), TRUE()))
        """
    )

    with pytest.raises(DaxTranslationError, match="AND supports exactly two arguments"):
        translate_dax_query(and_query)

    with pytest.raises(DaxTranslationError, match="OR supports exactly two arguments"):
        translate_dax_query(or_query)


def test_translate_query_weekday_rejects_more_than_two_arguments():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", WEEKDAY("2024-02-01", 2, 3))
        """
    )

    with pytest.raises(DaxTranslationError, match="WEEKDAY supports at most date and return_type arguments"):
        translate_dax_query(query)


def test_translate_query_year_rejects_more_than_one_argument():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", YEAR("2024-02-01", 2))
        """
    )

    with pytest.raises(DaxTranslationError, match="YEAR supports exactly one argument"):
        translate_dax_query(query)


def test_translate_query_left_rejects_more_than_two_arguments():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", LEFT("abcd", 2, 1))
        """
    )

    with pytest.raises(DaxTranslationError, match="LEFT supports at most text and num_chars arguments"):
        translate_dax_query(query)


def test_translate_query_date_ctor_rejects_more_than_three_arguments():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", DATE(2024, 2, 1, 3))
        """
    )

    with pytest.raises(DaxTranslationError, match="DATE requires year, month, and day arguments"):
        translate_dax_query(query)


def test_translate_query_value_rejects_more_than_one_argument():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", VALUE("10", 1))
        """
    )

    with pytest.raises(DaxTranslationError, match="VALUE supports exactly one argument"):
        translate_dax_query(query)


def test_translate_query_concatenate_rejects_more_than_two_arguments():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", CONCATENATE("a", "b", "c"))
        """
    )

    with pytest.raises(DaxTranslationError, match="CONCATENATE supports exactly two arguments"):
        translate_dax_query(query)


def test_translate_query_concatenatex_requires_table_and_expression_arguments():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", CONCATENATEX('Sales'))
        """
    )

    with pytest.raises(DaxTranslationError, match="CONCATENATEX requires table and expression arguments"):
        translate_dax_query(query)


def test_translate_query_selectedvalue_rejects_more_than_two_arguments():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", SELECTEDVALUE('Sales'[Category], "a", "b"))
        """
    )

    with pytest.raises(
        DaxTranslationError, match="SELECTEDVALUE supports at most column and alternate_result arguments"
    ):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category"}},
        )


def test_translate_query_hasonevalue_rejects_more_than_one_argument():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", HASONEVALUE('Sales'[Category], 1))
        """
    )

    with pytest.raises(DaxTranslationError, match="HASONEVALUE/HASONEFILTER supports exactly one argument"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category"}},
        )


def test_translate_query_firstnonblank_rejects_more_than_two_arguments():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", FIRSTNONBLANK('Sales'[Category], 'Sales'[Category], 1))
        """
    )

    with pytest.raises(
        DaxTranslationError, match="FIRSTNONBLANK/LASTNONBLANK supports exactly column and expression arguments"
    ):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category"}},
        )


def test_translate_query_isfiltered_rejects_more_than_one_argument():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", ISFILTERED('Sales'[Category], 1))
        """
    )

    with pytest.raises(DaxTranslationError, match="ISFILTERED/ISCROSSFILTERED supports exactly one argument"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category"}},
        )


def test_translate_query_nameof_rejects_more_than_one_argument():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", NAMEOF('Sales'[Category], 1))
        """
    )

    with pytest.raises(DaxTranslationError, match="NAMEOF supports exactly one argument"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Category": "Category"}},
        )


def test_translate_query_today_rand_reject_arguments():
    today_query = _parse_query(
        """
        EVALUATE
            ROW("x", TODAY(1))
        """
    )
    rand_query = _parse_query(
        """
        EVALUATE
            ROW("x", RAND(1))
        """
    )

    with pytest.raises(DaxTranslationError, match="TODAY does not take arguments"):
        translate_dax_query(today_query)

    with pytest.raises(DaxTranslationError, match="RAND does not take arguments"):
        translate_dax_query(rand_query)


def test_translate_query_round_rejects_more_than_two_arguments():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", ROUND(1, 2, 3))
        """
    )

    with pytest.raises(DaxTranslationError, match="ROUND requires number and num_digits arguments"):
        translate_dax_query(query)


def test_translate_query_upper_isblank_reject_extra_arguments():
    upper_query = _parse_query(
        """
        EVALUATE
            ROW("x", UPPER("abc", "x"))
        """
    )
    isblank_query = _parse_query(
        """
        EVALUATE
            ROW("x", ISBLANK(1, 2))
        """
    )

    with pytest.raises(DaxTranslationError, match="UPPER supports exactly one argument"):
        translate_dax_query(upper_query)

    with pytest.raises(DaxTranslationError, match="ISBLANK supports exactly one argument"):
        translate_dax_query(isblank_query)


def test_translate_query_coalesce():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", COALESCE('Sales'[Amount], 0))
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "COALESCE(Amount, 0) AS x" in sql


def test_translate_query_coalesce_requires_at_least_two_arguments():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", COALESCE('Sales'[Amount]))
        """
    )

    with pytest.raises(DaxTranslationError, match="COALESCE requires at least two arguments"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Amount": "Amount"}},
        )


def test_translate_query_switch_boolean_predicate_form():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", SWITCH(TRUE(), 'Sales'[Amount] > 0, 1, 0))
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "CASE WHEN (Amount > 0) THEN 1 ELSE 0 END AS x" in sql


def test_translate_query_switch_requires_expression_and_value_result_pair():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", SWITCH('Sales'[Amount]))
        """
    )

    with pytest.raises(DaxTranslationError, match="SWITCH requires expression and at least one value/result pair"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Amount": "Amount"}},
        )

    query_missing_result = _parse_query(
        """
        EVALUATE
            ROW("x", SWITCH('Sales'[Amount], 1))
        """
    )

    with pytest.raises(DaxTranslationError, match="SWITCH requires expression and at least one value/result pair"):
        translate_dax_query(
            query_missing_result,
            column_sql_by_table={"Sales": {"Amount": "Amount"}},
        )


def test_translate_query_isempty_table_expression():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", ISEMPTY(FILTER('Sales', 'Sales'[Amount] > 0)))
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "NOT EXISTS (SELECT 1 FROM (SELECT * FROM Sales WHERE (Amount > 0)) AS t) AS x" in sql


def test_translate_query_isempty_rejects_more_than_one_argument():
    query = _parse_query(
        """
        EVALUATE
            ROW("x", ISEMPTY('Sales', 'Sales'))
        """
    )

    with pytest.raises(DaxTranslationError, match="ISEMPTY supports exactly one argument"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Amount": "Amount"}},
        )


def test_translate_query_summarizecolumns_rollupgroup_group_by():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                ROLLUPGROUP('Sales'[ProductKey]),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey"}},
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(*) AS Rows" in sql
    assert "GROUP BY Sales.ProductKey" in sql


def test_translate_query_summarize_rejects_scalar_group_by_arg():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZE(
                'Sales',
                ABS(1),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    with pytest.raises(DaxTranslationError, match="Unsupported SUMMARIZE group-by argument"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Amount": "Amount"}},
        )


def test_translate_query_summarizecolumns_rollupaddissubtotal_group_by():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                ROLLUPADDISSUBTOTAL('Sales'[ProductKey], "is_subtotal"),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey"}},
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(*) AS Rows" in sql
    assert "GROUP BY Sales.ProductKey" in sql


def test_translate_query_summarizecolumns_unrelated_tables_cross_join():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[ProductKey],
                'Products'[Category]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"ProductKey": "ProductKey"},
            "Products": {"Category": "Category"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "FROM Sales CROSS JOIN Products" in sql
    assert "GROUP BY Sales.ProductKey, Products.Category" in sql


def test_translate_query_summarizecolumns_cross_join_with_disconnected_component_relationship():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'A'[Id],
                'X'[XId],
                'Y'[YLabel]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "A": {"Id": "Id"},
            "X": {"XId": "XId", "YId": "YId"},
            "Y": {"YId": "YId", "YLabel": "YLabel"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="X",
                from_column="YId",
                to_table="Y",
                to_column="YId",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert "FROM A CROSS JOIN X LEFT JOIN Y ON X.YId = Y.YId" in sql
    assert "GROUP BY A.Id, X.XId, Y.YLabel" in sql


def test_translate_query_summarizecolumns_treatas_filter():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[ProductKey],
                TREATAS({1, 2}, 'Sales'[ProductKey]),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey"}},
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(*) AS Rows" in sql
    assert "WHERE (Sales.ProductKey IN (1, 2))" in sql
    assert "GROUP BY Sales.ProductKey" in sql


def test_translate_query_evaluate_treatas_table_expression():
    query = _parse_query(
        """
        EVALUATE
            TREATAS({1, 2}, 'Sales'[ProductKey])
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Sales.ProductKey AS ProductKey FROM Sales WHERE (Sales.ProductKey IN (1, 2))" in sql


def test_translate_query_evaluate_treatas_table_expression_rejects_width_mismatch():
    query = _parse_query(
        """
        EVALUATE
            TREATAS(SELECTCOLUMNS('Sales', "k", 'Sales'[ProductKey], "q", 'Sales'[Quantity]), 'Sales'[ProductKey])
        """
    )

    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"ProductKey": "ProductKey", "Quantity": "Quantity"}},
        )


def test_translate_query_evaluate_treatas_filter_wrapper_rejects_width_mismatch():
    query = _parse_query(
        """
        EVALUATE
            TREATAS(FILTER('Sales', 'Sales'[Amount] > 10), 'Sales'[ProductKey])
        """
    )

    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"ProductKey": "ProductKey", "Amount": "Amount"}},
        )


def test_translate_query_evaluate_treatas_values_wrapper_rejects_width_mismatch():
    query = _parse_query(
        """
        EVALUATE
            TREATAS(
                FILTER(VALUES('Sales'[ProductKey]), 'Sales'[ProductKey] > 1),
                'Sales'[ProductKey],
                'Sales'[Quantity]
            )
        """
    )

    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"ProductKey": "ProductKey", "Quantity": "Quantity"}},
        )


def test_translate_query_evaluate_treatas_union_wrapper_rejects_width_mismatch():
    query = _parse_query(
        """
        EVALUATE
            TREATAS(UNION('Sales', 'Sales'), 'Sales'[ProductKey])
        """
    )

    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"ProductKey": "ProductKey", "Amount": "Amount"}},
        )


def test_translate_query_evaluate_treatas_naturalinnerjoin_wrapper_rejects_width_mismatch():
    query = _parse_query(
        """
        EVALUATE
            TREATAS(NATURALINNERJOIN('Sales', 'Sales'), 'Sales'[ProductKey])
        """
    )

    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"ProductKey": "ProductKey", "Amount": "Amount"}},
        )


def test_translate_query_evaluate_treatas_naturalleftouterjoin_wrapper_rejects_width_mismatch():
    query = _parse_query(
        """
        EVALUATE
            TREATAS(NATURALLEFTOUTERJOIN('Sales', 'Sales'), 'Sales'[ProductKey])
        """
    )

    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"ProductKey": "ProductKey", "Amount": "Amount"}},
        )


def test_translate_query_evaluate_treatas_renamecolumns_wrapper_rejects_width_mismatch():
    query = _parse_query(
        """
        EVALUATE
            TREATAS(
                RENAMECOLUMNS('Sales', 'Sales'[ProductKey], "k"),
                'Sales'[ProductKey],
                'Sales'[Quantity]
            )
        """
    )

    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"ProductKey": "ProductKey", "Quantity": "Quantity", "Amount": "Amount"}},
        )


def test_translate_query_evaluate_treatas_removecolumns_wrapper_rejects_width_mismatch():
    query = _parse_query(
        """
        EVALUATE
            TREATAS(
                REMOVECOLUMNS('Sales', 'Sales'[Amount], 'Sales'[Quantity]),
                'Sales'[ProductKey],
                'Sales'[Quantity]
            )
        """
    )

    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"ProductKey": "ProductKey", "Quantity": "Quantity", "Amount": "Amount"}},
        )


def test_translate_query_evaluate_treatas_keepcolumns_wrapper_rejects_width_mismatch():
    query = _parse_query(
        """
        EVALUATE
            TREATAS(
                KEEPCOLUMNS('Sales', 'Sales'[ProductKey], 'Sales'[Quantity]),
                'Sales'[ProductKey]
            )
        """
    )

    with pytest.raises(DaxTranslationError, match="TREATAS table column count must match target column count"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"ProductKey": "ProductKey", "Quantity": "Quantity", "Amount": "Amount"}},
        )


def test_translate_query_summarizecolumns_datesbetween_filter():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[ProductKey],
                DATESBETWEEN('Sales'[OrderDate], "2024-01-01", "2024-12-31"),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey", "OrderDate": "OrderDate"}},
    )

    sql = translation.evaluates[0].sql
    assert "WHERE (Sales.OrderDate >= '2024-01-01' AND Sales.OrderDate <= '2024-12-31')" in sql
    assert "GROUP BY Sales.ProductKey" in sql


def test_translate_query_evaluate_datesbetween_cross_table_start_bound_joins_referenced_table():
    query = _parse_query(
        """
        EVALUATE
            DATESBETWEEN('Date'[DateKey], 'Sales'[DateKey], "2024-12-31")
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"DateKey": "DateKey"},
            "Sales": {"DateKey": "DateKey"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="DateKey",
                to_table="Date",
                to_column="DateKey",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Date.* FROM Date LEFT JOIN Sales ON Date.DateKey = Sales.DateKey" in sql
    assert "WHERE (Date.DateKey >= Sales.DateKey AND Date.DateKey <= '2024-12-31')" in sql
    assert "Sales" in translation.evaluates[0].required_models


def test_translate_query_summarizecolumns_nonvisual_treatas_filter():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[ProductKey],
                NONVISUAL(TREATAS({1}, 'Sales'[ProductKey])),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey"}},
    )

    sql = translation.evaluates[0].sql
    assert "WHERE (Sales.ProductKey IN (1))" in sql
    assert "GROUP BY Sales.ProductKey" in sql


def test_translate_query_summarizecolumns_ignore_measure_expression():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[ProductKey],
                "Rows", IGNORE(COUNTROWS('Sales'))
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey"}},
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(*) AS Rows" in sql
    assert "IGNORE(" not in sql


def test_translate_query_summarize_order_by():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZE(
                'Sales',
                'Sales'[Amount],
                "Rows", COUNTROWS('Sales')
            )
            ORDER BY 'Sales'[Amount] DESC
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    assert len(translation.evaluates) == 1
    sql = translation.evaluates[0].sql
    assert "SELECT Sales.Amount, COUNT(*) AS Rows FROM Sales GROUP BY Sales.Amount" in sql
    assert "ORDER BY q.Amount DESC" in sql


def test_translate_query_summarize_filter_table_expression_order_by():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZE(
                FILTER('Sales', 'Sales'[Amount] > 10),
                'Sales'[Amount],
                "Rows", COUNTROWS('Sales')
            )
            ORDER BY 'Sales'[Amount] DESC
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    assert len(translation.evaluates) == 1
    sql = translation.evaluates[0].sql
    assert "FROM (SELECT * FROM Sales WHERE (Amount > 10)) AS t" in sql
    assert "SELECT Amount, COUNT(*) AS Rows" in sql
    assert "GROUP BY Amount" in sql
    assert "ORDER BY q.Amount DESC" in sql


def test_translate_query_summarize_wrapped_multitable_row_group_by_bracket_alias():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZE(
                ROW("x", 'Sales'[Amount], "d", 'Date'[DateKey]),
                [x]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Date": {"DateKey": "DateKey"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT x FROM (SELECT Amount AS x, Date.DateKey AS d FROM Sales CROSS JOIN Date) AS t GROUP BY x" in sql
    assert "Sales" in translation.evaluates[0].required_models
    assert "Date" in translation.evaluates[0].required_models


def test_translate_query_summarize_wrapped_multitable_row_group_by_identifier_alias():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZE(
                ROW("x", 'Sales'[Amount], "d", 'Date'[DateKey]),
                x
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Date": {"DateKey": "DateKey"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT x FROM (SELECT Amount AS x, Date.DateKey AS d FROM Sales CROSS JOIN Date) AS t GROUP BY x" in sql
    assert "Sales" in translation.evaluates[0].required_models
    assert "Date" in translation.evaluates[0].required_models


def test_translate_query_summarize_filter_table_expression_cross_table_group_by():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZE(
                FILTER('Sales', 'Sales'[Amount] > 10),
                'Date'[Fiscal Year],
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
        },
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "FROM (SELECT * FROM Sales WHERE (Amount > 10)) AS t LEFT JOIN Date ON t.DateKey = Date.DateKey" in sql
    assert "SELECT Date.FiscalYear, COUNT(*) AS Rows" in sql
    assert "GROUP BY Date.FiscalYear" in sql


def test_translate_query_summarize_filter_table_expression_cross_join_disconnected_group_by():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZE(
                FILTER('Sales', 'Sales'[Amount] > 10),
                'Product'[Category],
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Product": {"Category": "Category"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "FROM (SELECT * FROM Sales WHERE (Amount > 10)) AS t CROSS JOIN Product" in sql
    assert "SELECT Product.Category, COUNT(*) AS Rows" in sql
    assert "GROUP BY Product.Category" in sql


def test_translate_query_define_measure_inlines_bracket_reference():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales Order'[Orders] = DISTINCTCOUNT('Sales Order'[Sales Order])
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales Order'[Sales Order],
                "Orders", [Orders]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales Order": {"Sales Order": '"Sales Order"'}},
        measure_names_by_table={"Sales Order": {"Orders"}},
    )

    assert len(translation.evaluates) == 1
    sql = translation.evaluates[0].sql
    assert 'COUNT(DISTINCT "Sales Order") AS Orders' in sql


def test_translate_query_define_measure_with_identifier_table_ref():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[Customers] = COUNTROWS(Customer)
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[FiscalYear],
                "Customers", [Customers]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"SalesOrderLineKey": "SalesOrderLineKey"},
            "Date": {"FiscalYear": "FiscalYear"},
            "Customer": {"CustomerKey": "CustomerKey"},
        },
        measure_names_by_table={"Sales": {"Customers"}},
    )

    assert len(translation.evaluates) == 1
    sql = translation.evaluates[0].sql
    assert "(SELECT COUNT(*) FROM Customer) AS Customers" in sql
    assert "GROUP BY Date.FiscalYear" in sql


def test_translate_query_define_measure_with_identifier_table_ref_uses_relationship_group_context():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[Customers] = COUNTROWS(Customer)
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[FiscalYear],
                "Customers", [Customers]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"SalesOrderLineKey": "SalesOrderLineKey", "DateKey": "DateKey", "CustomerKey": "CustomerKey"},
            "Date": {"DateKey": "DateKey", "FiscalYear": "FiscalYear"},
            "Customer": {"Name": "Name", "CustomerKey": "CustomerKey"},
        },
        measure_names_by_table={"Sales": {"Customers"}},
        relationship_edges=[
            RelationshipEdge("Sales", "DateKey", "Date", "DateKey"),
            RelationshipEdge("Sales", "CustomerKey", "Customer", "CustomerKey"),
        ],
    )

    assert len(translation.evaluates) == 1
    sql = translation.evaluates[0].sql
    assert "COUNT(Customer.CustomerKey) AS Customers" in sql
    assert (
        "FROM Date LEFT JOIN Sales ON Date.DateKey = Sales.DateKey LEFT JOIN Customer ON Sales.CustomerKey = Customer.CustomerKey"
        in sql
    )
    assert "GROUP BY Date.FiscalYear" in sql


def test_translate_query_define_measure_with_calculate_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Pick a sales measure'[Customers] = CALCULATE(
                    COUNTROWS(Customer),
                    FILTER(
                        'Sales',
                        'Sales'[Amount] > 0
                    )
                )
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "Customers", [Customers]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey", "CustomerKey": "CustomerKey"},
            "Customer": {"CustomerKey": "CustomerKey"},
        },
        measure_names_by_table={"Pick a sales measure": {"Customers"}},
        relationship_edges=[
            RelationshipEdge("Sales", "DateKey", "Date", "DateKey"),
            RelationshipEdge("Sales", "CustomerKey", "Customer", "CustomerKey"),
        ],
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(CASE WHEN" in sql
    assert "Sales.Amount > 0" in sql
    assert "AS Customers" in sql


def test_translate_query_define_measure_calculate_values_cross_table_filter_candidate():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[SalesByDateContext] = CALCULATE(
                SUM('Sales'[Amount]),
                VALUES('Date')
            )
        EVALUATE
            ROW("Value", [SalesByDateContext])
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
            "Date": {"DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"SalesByDateContext"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    assert len(translation.evaluates) == 1
    assert "SUM(Sales.Amount) AS Value" in translation.evaluates[0].sql
    assert "Date" in translation.evaluates[0].required_models


def test_translate_query_define_measure_calculate_table_ref_cross_table_filter_candidate():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[SalesByDateTableContext] = CALCULATE(
                SUM('Sales'[Amount]),
                'Date'
            )
        EVALUATE
            ROW("Value", [SalesByDateTableContext])
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
            "Date": {"DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"SalesByDateTableContext"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    assert len(translation.evaluates) == 1
    assert "SUM(Sales.Amount) AS Value" in translation.evaluates[0].sql
    assert "Date" in translation.evaluates[0].required_models


def test_translate_query_define_measure_calculate_filters_cross_table_filter_candidate():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[SalesByDateFilterContext] = CALCULATE(
                SUM('Sales'[Amount]),
                FILTERS('Date'[DateKey])
            )
        EVALUATE
            ROW("Value", [SalesByDateFilterContext])
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
            "Date": {"DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"SalesByDateFilterContext"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    assert len(translation.evaluates) == 1
    assert "SUM(Sales.Amount) AS Value" in translation.evaluates[0].sql
    assert "Date" in translation.evaluates[0].required_models


def test_translate_query_define_measure_calculate_sameperiodlastyear_cross_table_time_column():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[Prev] = CALCULATE(
                SUM('Sales'[Amount]),
                SAMEPERIODLASTYEAR('Date'[DateKey])
            )
        EVALUATE
            ROW("Value", [Prev])
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
            "Date": {"DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"Prev"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
        time_dimensions_by_table={"Sales": {"DateKey"}, "Date": {"DateKey"}},
    )

    assert len(translation.evaluates) == 1
    assert "SUM(Sales.Amount) AS Value" in translation.evaluates[0].sql
    assert "Date" in translation.evaluates[0].required_models


def test_translate_query_define_measure_totalytd_cross_table_time_column_and_table_filter():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[YTD] = TOTALYTD(
                SUM('Sales'[Amount]),
                'Date'[DateKey],
                'Date'
            )
        EVALUATE
            ROW("Value", [YTD])
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
            "Date": {"DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"YTD"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
        time_dimensions_by_table={"Sales": {"DateKey"}, "Date": {"DateKey"}},
    )

    assert len(translation.evaluates) == 1
    assert "SUM(Sales.Amount) AS Value" in translation.evaluates[0].sql
    assert "Date" in translation.evaluates[0].required_models


def test_translate_query_define_measure_with_countrows_datesbetween_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[RowsInRange] = COUNTROWS(
                DATESBETWEEN('Sales'[OrderDate], "2024-01-01", "2024-12-31")
            )
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "RowsInRange", [RowsInRange]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"OrderDate": "OrderDate", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"RowsInRange"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(CASE WHEN (Sales.OrderDate >= '2024-01-01' AND Sales.OrderDate <= '2024-12-31') THEN 1 END)" in sql
    assert "AS RowsInRange" in sql


def test_translate_query_define_measure_with_countrows_filters_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[SelectedProducts] = COUNTROWS(FILTERS('Sales'[ProductKey]))
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "SelectedProducts", [SelectedProducts]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"ProductKey": "ProductKey", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"SelectedProducts"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(DISTINCT Sales.ProductKey) AS SelectedProducts" in sql


def test_translate_query_define_measure_with_countrows_cross_table_filters_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[SelectedDates] = COUNTROWS(FILTERS('Date'[DateKey]))
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "SelectedDates", [SelectedDates]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"SelectedDates"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(DISTINCT Date.DateKey) AS SelectedDates" in sql


def test_translate_query_define_measure_with_countrows_all_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[AllRows] = COUNTROWS(ALL('Sales'))
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "AllRows", [AllRows]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"ProductKey": "ProductKey", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"AllRows"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "(SELECT COUNT(*) FROM (SELECT * FROM Sales) AS t) AS AllRows" in sql


def test_translate_query_define_measure_with_approximatedistinctcount_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[ApproxProducts] = APPROXIMATEDISTINCTCOUNT('Sales'[ProductKey])
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "ApproxProducts", [ApproxProducts]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"ProductKey": "ProductKey", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"ApproxProducts"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(DISTINCT Sales.ProductKey) AS ApproxProducts" in sql


def test_translate_query_define_measure_with_sumx_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[PositiveAmount] = SUMX(
                FILTER('Sales', 'Sales'[Amount] > 0),
                'Sales'[Amount]
            )
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "PositiveAmount", [PositiveAmount]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"PositiveAmount"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "SUM(CASE WHEN" in sql
    assert "Sales.Amount > 0" in sql
    assert "THEN Sales.Amount ELSE NULL END) AS PositiveAmount" in sql


def test_translate_query_define_measure_with_avgx_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[AvgPositiveAmount] = AVGX(
                FILTER('Sales', 'Sales'[Amount] > 0),
                'Sales'[Amount]
            )
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "AvgPositiveAmount", [AvgPositiveAmount]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"AvgPositiveAmount"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "AVG(CASE WHEN" in sql
    assert "Sales.Amount > 0" in sql
    assert "THEN Sales.Amount ELSE NULL END) AS AvgPositiveAmount" in sql


def test_translate_query_define_measure_with_countx_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[CountPositiveAmount] = COUNTX(
                FILTER('Sales', 'Sales'[Amount] > 0),
                'Sales'[Amount]
            )
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "CountPositiveAmount", [CountPositiveAmount]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"CountPositiveAmount"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(CASE WHEN" in sql
    assert "Sales.Amount > 0" in sql
    assert "THEN Sales.Amount END) AS CountPositiveAmount" in sql


def test_translate_query_define_measure_with_maxx_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[MaxPositiveAmount] = MAXX(
                FILTER('Sales', 'Sales'[Amount] > 0),
                'Sales'[Amount]
            )
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "MaxPositiveAmount", [MaxPositiveAmount]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"MaxPositiveAmount"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "MAX(CASE WHEN" in sql
    assert "Sales.Amount > 0" in sql
    assert "THEN Sales.Amount ELSE NULL END) AS MaxPositiveAmount" in sql


def test_translate_query_define_measure_with_countblank_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[BlankProductKeys] = COUNTBLANK('Sales'[ProductKey])
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "BlankProductKeys", [BlankProductKeys]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"ProductKey": "ProductKey", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"BlankProductKeys"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "COUNT(CASE WHEN Sales.ProductKey IS NULL THEN 1 END) AS BlankProductKeys" in sql


def test_translate_query_define_measure_with_selectedvalue_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[SelectedProduct] = SELECTEDVALUE('Sales'[ProductKey], -1)
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "SelectedProduct", [SelectedProduct]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"ProductKey": "ProductKey", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"SelectedProduct"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert (
        "CASE WHEN COUNT(DISTINCT Sales.ProductKey) = 1 THEN MIN(Sales.ProductKey) ELSE -1 END AS SelectedProduct"
        in sql
    )


def test_translate_query_define_measure_with_firstnonblank_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[FirstProductWithAmount] = FIRSTNONBLANK('Sales'[ProductKey], 'Sales'[Amount])
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "FirstProductWithAmount", [FirstProductWithAmount]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"ProductKey": "ProductKey", "Amount": "Amount", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"FirstProductWithAmount"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert (
        "MIN(CASE WHEN Sales.Amount IS NOT NULL THEN Sales.ProductKey ELSE NULL END) AS FirstProductWithAmount" in sql
    )


def test_translate_query_define_measure_with_firstdate_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[FirstOrderDate] = FIRSTDATE('Sales'[OrderDate])
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "FirstOrderDate", [FirstOrderDate]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"OrderDate": "OrderDate", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"FirstOrderDate"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "MIN(Sales.OrderDate) AS FirstOrderDate" in sql


def test_translate_query_define_measure_with_isinscope_true_when_grouped():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[InScopeProduct] = ISINSCOPE('Sales'[ProductKey])
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[ProductKey],
                "InScopeProduct", [InScopeProduct]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey"}},
        measure_names_by_table={"Sales": {"InScopeProduct"}},
    )

    sql = translation.evaluates[0].sql
    assert "TRUE AS InScopeProduct" in sql


def test_translate_query_define_measure_with_isinscope_false_when_not_grouped():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[InScopeProduct] = ISINSCOPE('Sales'[ProductKey])
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "InScopeProduct", [InScopeProduct]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"ProductKey": "ProductKey", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"InScopeProduct"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "FALSE AS InScopeProduct" in sql


def test_translate_query_define_measure_with_isfiltered_true_when_filtered():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[ProductFiltered] = ISFILTERED('Sales'[ProductKey])
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                TREATAS({1}, 'Sales'[ProductKey]),
                "ProductFiltered", [ProductFiltered]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"ProductKey": "ProductKey", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"ProductFiltered"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "TRUE AS ProductFiltered" in sql


def test_translate_query_selectcolumns_containsstring_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "HasOpen",
                CONTAINSSTRING('Sales'[Status], "open")
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Status": "Status"}},
    )

    sql = translation.evaluates[0].sql
    assert "(POSITION(LOWER('open') IN LOWER(Status)) > 0) AS HasOpen" in sql


def test_translate_query_selectcolumns_containsrow_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "HasKey",
                CONTAINSROW(VALUES('Sales'[ProductKey]), 1)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey"}},
    )

    sql = translation.evaluates[0].sql
    assert (
        "EXISTS (SELECT 1 FROM (SELECT DISTINCT Sales.ProductKey FROM Sales) AS t(c1) "
        "WHERE t.c1 IS NOT DISTINCT FROM 1) AS HasKey" in sql
    )


def test_translate_query_row_containsrow_rejects_value_count_mismatch():
    query = _parse_query(
        """
        EVALUATE
            ROW(
                "HasKey",
                CONTAINSROW(
                    SELECTCOLUMNS('Sales', "k", 'Sales'[ProductKey], "q", 'Sales'[Quantity]),
                    1
                )
            )
        """
    )

    with pytest.raises(DaxTranslationError, match="CONTAINSROW value argument count must match table column count"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"ProductKey": "ProductKey", "Quantity": "Quantity"}},
        )


def test_translate_query_row_containsrow_rejects_non_inferable_table_width():
    query = _parse_query(
        """
        EVALUATE
            ROW(
                "HasKey",
                CONTAINSROW('Sales', 1)
            )
        """
    )

    with pytest.raises(DaxTranslationError, match="CONTAINSROW requires an inferable table column count"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {}},
        )


def test_translate_query_selectcolumns_len_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "StatusLen",
                LEN('Sales'[Status])
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Status": "Status"}},
    )

    sql = translation.evaluates[0].sql
    assert "LENGTH(Status) AS StatusLen" in sql


def test_translate_query_selectcolumns_left_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "StatusLeft",
                LEFT('Sales'[Status], 3)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Status": "Status"}},
    )

    sql = translation.evaluates[0].sql
    assert "SUBSTRING(Status, 1, GREATEST(3, 0)) AS StatusLeft" in sql


def test_translate_query_selectcolumns_right_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "StatusRight",
                RIGHT('Sales'[Status], 3)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Status": "Status"}},
    )

    sql = translation.evaluates[0].sql
    assert "CASE WHEN 3 <= 0 THEN '' ELSE SUBSTRING(Status, GREATEST(LENGTH(Status) - 3 + 1, 1), 3) END" in sql
    assert "AS StatusRight" in sql


def test_translate_query_selectcolumns_mid_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "StatusMid",
                MID('Sales'[Status], 2, 3)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Status": "Status"}},
    )

    sql = translation.evaluates[0].sql
    assert "CASE WHEN 3 <= 0 THEN '' ELSE SUBSTRING(Status, GREATEST(2, 1), 3) END AS StatusMid" in sql


def test_translate_query_selectcolumns_replace_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "StatusReplaced",
                REPLACE('Sales'[Status], 2, 2, "xx")
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Status": "Status"}},
    )

    sql = translation.evaluates[0].sql
    assert "CASE WHEN GREATEST(2, 1) <= 1 THEN '' ELSE SUBSTRING(Status, 1, GREATEST(2, 1) - 1) END" in sql
    assert "|| 'xx' || SUBSTRING(Status, GREATEST(2, 1) + GREATEST(2, 0))" in sql
    assert "AS StatusReplaced" in sql


def test_translate_query_selectcolumns_substitute_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "StatusSubstituted",
                SUBSTITUTE('Sales'[Status], "ab", "xy")
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Status": "Status"}},
    )

    sql = translation.evaluates[0].sql
    assert "REPLACE(Status, 'ab', 'xy') AS StatusSubstituted" in sql


def test_translate_query_selectcolumns_substitute_instance_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "StatusSubstituted",
                SUBSTITUTE('Sales'[Status], "ab", "xy", 2)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Status": "Status"}},
    )

    sql = translation.evaluates[0].sql
    assert "CASE WHEN 'ab' = '' THEN Status" in sql
    assert "INSTR(SUBSTR(Status, (INSTR(Status, 'ab')) + LENGTH('ab')), 'ab')" in sql
    assert "AS StatusSubstituted" in sql


def test_translate_query_selectcolumns_rept_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "StatusRepeated",
                REPT('Sales'[Status], 3)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Status": "Status"}},
    )

    sql = translation.evaluates[0].sql
    assert "REPEAT(Status, GREATEST(CAST(FLOOR(3) AS BIGINT), 0)) AS StatusRepeated" in sql


def test_translate_query_selectcolumns_trim_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "StatusTrimmed",
                TRIM('Sales'[Status])
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Status": "Status"}},
    )

    sql = translation.evaluates[0].sql
    assert "TRIM(REGEXP_REPLACE(CAST(Status AS VARCHAR), ' +', ' ', 'g')) AS StatusTrimmed" in sql


def test_translate_query_selectcolumns_weekday_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "WeekdayNum",
                WEEKDAY('Sales'[OrderDate], 2)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"OrderDate": "OrderDate"}},
    )

    sql = translation.evaluates[0].sql
    assert "(((EXTRACT(DOW FROM CAST(OrderDate AS DATE)) + 6) % 7) + 1) AS WeekdayNum" in sql


def test_translate_query_selectcolumns_format_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "AmountText",
                FORMAT('Sales'[Amount], "0.00")
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "CAST(Amount AS VARCHAR) AS AmountText" in sql


def test_translate_query_selectcolumns_cross_table_expression_joins_related_table():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "Category",
                'Product'[Category]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"ProductKey": "ProductKey"},
            "Product": {"ProductKey": "ProductKey", "Category": "Category"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="ProductKey",
                to_table="Product",
                to_column="ProductKey",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert (
        "SELECT Product.Category AS Category FROM Sales LEFT JOIN Product ON Sales.ProductKey = Product.ProductKey"
        in sql
    )


def test_translate_query_addcolumns_cross_table_expression_joins_related_table():
    query = _parse_query(
        """
        EVALUATE
            ADDCOLUMNS(
                'Sales',
                "Category",
                'Product'[Category]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
            "Product": {"ProductKey": "ProductKey", "Category": "Category"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="ProductKey",
                to_table="Product",
                to_column="ProductKey",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert (
        "SELECT Sales.*, Product.Category AS Category FROM Sales LEFT JOIN Product ON Sales.ProductKey = Product.ProductKey"
        in sql
    )


def test_translate_query_selectcolumns_wrapped_base_cross_table_expression_joins_related_table():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                FILTER('Sales', 'Sales'[Amount] > 0),
                "Category",
                'Product'[Category]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
            "Product": {"ProductKey": "ProductKey", "Category": "Category"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="ProductKey",
                to_table="Product",
                to_column="ProductKey",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert (
        "SELECT Product.Category AS Category FROM (SELECT * FROM Sales WHERE (Amount > 0)) AS t LEFT JOIN Product ON t.ProductKey = Product.ProductKey"
        in sql
    )


def test_translate_query_addcolumns_wrapped_base_cross_table_expression_cross_join_when_unrelated():
    query = _parse_query(
        """
        EVALUATE
            ADDCOLUMNS(
                FILTER('Sales', 'Sales'[Amount] > 0),
                "Rate",
                'Tax'[Rate]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Tax": {"Rate": "Rate"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT t.*, Tax.Rate AS Rate FROM (SELECT * FROM Sales WHERE (Amount > 0)) AS t CROSS JOIN Tax" in sql


def test_translate_query_selectcolumns_rounddown_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "AmountRoundedDown",
                ROUNDDOWN('Sales'[Amount], 2)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "CASE WHEN 2 >= 0 THEN SIGN(Amount) * FLOOR(ABS(Amount) * POWER(10, 2)) / POWER(10, 2) ELSE" in sql
    assert "AS AmountRoundedDown" in sql


def test_translate_query_selectcolumns_round_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "AmountRounded",
                ROUND('Sales'[Amount], 2)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "CASE WHEN 2 >= 0 THEN SIGN(Amount) * FLOOR(ABS(Amount) * POWER(10, 2) + 0.5) / POWER(10, 2) ELSE" in sql
    assert "AS AmountRounded" in sql


def test_translate_query_selectcolumns_int_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "AmountInt",
                INT('Sales'[Amount])
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "FLOOR(Amount) AS AmountInt" in sql


def test_translate_query_selectcolumns_trunc_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "AmountTrunc",
                TRUNC('Sales'[Amount], 2)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "CASE WHEN 2 >= 0 THEN SIGN(Amount) * FLOOR(ABS(Amount) * POWER(10, 2)) / POWER(10, 2) ELSE" in sql
    assert "AS AmountTrunc" in sql


def test_translate_query_selectcolumns_ceiling_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "AmountCeiling",
                CEILING('Sales'[Amount], 2)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "(CEIL(Amount / 2) * 2) AS AmountCeiling" in sql


def test_translate_query_selectcolumns_floor_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "AmountFloor",
                FLOOR('Sales'[Amount], 2)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "(FLOOR(Amount / 2) * 2) AS AmountFloor" in sql


def test_translate_query_selectcolumns_mround_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "AmountMround",
                MROUND('Sales'[Amount], 2)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "CASE WHEN 2 = 0 THEN 0 ELSE SIGN(Amount) * FLOOR((ABS(Amount) / ABS(2)) + 0.5) * ABS(2) END" in sql
    assert "AS AmountMround" in sql


def test_translate_query_evaluate_generateseries():
    query = _parse_query(
        """
        EVALUATE
            GENERATESERIES(1, 5, 2)
        """
    )

    translation = translate_dax_query(query)

    sql = translation.evaluates[0].sql
    assert "SELECT value FROM generate_series(1, 5, 2) AS gs(value)" in sql


def test_translate_query_evaluate_calendar_cross_table_aggregate_bounds():
    query = _parse_query(
        """
        EVALUATE
            CALENDAR(MIN('Sales'[DateKey]), MAX('Date'[DateKey]))
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"DateKey": "DateKey"},
            "Date": {"DateKey": "DateKey"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "CAST((SELECT MIN(Sales.DateKey) FROM Sales) AS DATE)" in sql
    assert "CAST((SELECT MAX(Date.DateKey) FROM Date) AS DATE)" in sql
    assert "Sales" in translation.evaluates[0].required_models
    assert "Date" in translation.evaluates[0].required_models


def test_translate_query_selectcolumns_evaluateandlog_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "Amt",
                EVALUATEANDLOG('Sales'[Amount])
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "Amount AS Amt" in sql


def test_translate_query_selectcolumns_lookupvalue_expression():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "Category",
                LOOKUPVALUE('Product'[Category], 'Product'[ProductKey], 'Sales'[ProductKey], "unknown")
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"ProductKey": "ProductKey"},
            "Product": {"Category": "Category", "ProductKey": "ProductKey"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Product.Category FROM Product WHERE Product.ProductKey = Sales.ProductKey LIMIT 1" in sql
    assert "AS Category" in sql


def test_translate_query_summarizecolumns_related_expression():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[ProductKey],
                "Category", RELATED('Product'[Category])
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"ProductKey": "ProductKey"},
            "Product": {"Category": "Category", "ProductKey": "ProductKey"},
        },
        relationship_edges=[RelationshipEdge("Sales", "ProductKey", "Product", "ProductKey")],
    )

    sql = translation.evaluates[0].sql
    assert "Product.Category AS Category" in sql
    assert "JOIN Product ON Sales.ProductKey = Product.ProductKey" in sql


def test_translate_query_evaluate_relatedtable_expression():
    query = _parse_query(
        """
        EVALUATE
            RELATEDTABLE('Sales')
        """
    )

    translation = translate_dax_query(query, column_sql_by_table={"Sales": {"Amount": "Amount"}})
    sql = translation.evaluates[0].sql
    assert "SELECT * FROM Sales" in sql


def test_translate_query_selectcolumns_find_and_search_expressions():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "SearchPos", SEARCH("ab", 'Sales'[Sku], 1, -1),
                "FindPos", FIND("AB", 'Sales'[Sku], 1, 0)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Sku": "Sku"}},
    )
    sql = translation.evaluates[0].sql
    assert "POSITION(LOWER('ab') IN SUBSTRING(LOWER(Sku), 1))" in sql
    assert "POSITION('AB' IN SUBSTRING(Sku, 1))" in sql
    assert "AS SearchPos" in sql
    assert "AS FindPos" in sql


def test_translate_query_selectcolumns_now_and_datepart_expressions():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "Y", YEAR('Sales'[OrderDate]),
                "Q", QUARTER('Sales'[OrderDate]),
                "NowTs", NOW(),
                "UtcDate", UTCTODAY()
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"OrderDate": "OrderDate"}},
    )
    sql = translation.evaluates[0].sql
    assert "EXTRACT(YEAR FROM CAST(OrderDate AS DATE)) AS Y" in sql
    assert "EXTRACT(QUARTER FROM CAST(OrderDate AS DATE)) AS Q" in sql
    assert "CURRENT_TIMESTAMP AS NowTs" in sql
    assert "CURRENT_DATE AS UtcDate" in sql


def test_translate_query_selectcolumns_value_and_concatenate_expressions():
    query = _parse_query(
        """
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "AmountNum", VALUE('Sales'[AmountText]),
                "SkuTag", CONCATENATE('Sales'[Sku], "-X")
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"AmountText": "AmountText", "Sku": "Sku"}},
    )
    sql = translation.evaluates[0].sql
    assert "CAST(AmountText AS DOUBLE) AS AmountNum" in sql
    assert "(Sku || '-X') AS SkuTag" in sql


def test_translate_query_row_concatenatex_expression():
    query = _parse_query(
        """
        EVALUATE
            ROW(
                "SkuList",
                CONCATENATEX('Sales', 'Sales'[Sku], ",", 'Sales'[Amount], DESC)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Sku": "Sku", "Amount": "Amount"}},
    )
    sql = translation.evaluates[0].sql
    assert "STRING_AGG(CAST(Sku AS VARCHAR), CAST(',' AS VARCHAR) ORDER BY Amount DESC)" in sql
    assert "FROM Sales" in sql
    assert "AS SkuList" in sql


def test_translate_query_summarizecolumns_median_and_medianx_expressions():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[ProductKey],
                "MedianAmt", MEDIAN('Sales'[Amount]),
                "MedianAmtX", MEDIANX('Sales', 'Sales'[Amount])
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey", "Amount": "Amount"}},
    )
    sql = translation.evaluates[0].sql
    assert "MEDIAN(Sales.Amount) AS MedianAmt" in sql
    assert "MEDIAN(Sales.Amount) AS MedianAmtX" in sql


def test_translate_query_define_measure_with_totalytd_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[SalesYTD] = TOTALYTD(SUM('Sales'[Amount]), 'Date'[Date])
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "SalesYTD", [SalesYTD]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "Date": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"SalesYTD"}},
        time_dimensions_by_table={"Date": {"Date"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )
    sql = translation.evaluates[0].sql
    assert "OVER (" in sql
    assert "AS SalesYTD" in sql


def test_translate_query_define_measure_with_sameperiodlastyear_expression():
    query = _parse_query(
        """
        DEFINE
            MEASURE 'Sales'[PrevSales] = CALCULATE(SUM('Sales'[Amount]), SAMEPERIODLASTYEAR('Date'[Date]))
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "PrevSales", [PrevSales]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "Date": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"PrevSales"}},
        time_dimensions_by_table={"Date": {"Date"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )
    sql = translation.evaluates[0].sql
    assert "LAG(" in sql
    assert "AS PrevSales" in sql


def test_translate_query_sameperiodlastyear_model_measure_reference_uses_measure_sql():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "PrevSales",
                CALCULATE([Total Sales], SAMEPERIODLASTYEAR('Date'[Date]))
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "Date": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"Total Sales"}},
        measure_aggs_by_table={"Sales": {"Total Sales": "sum"}},
        measure_sql_by_table={"Sales": {"Total Sales": "Amount"}},
        time_dimensions_by_table={"Date": {"Date"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )
    sql = translation.evaluates[0].sql
    assert "LAG(SUM(Amount), 1) OVER (" in sql
    assert "AS PrevSales" in sql


def test_translate_query_model_measure_reference_uses_measure_metadata():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[Category],
                "Revenue", [Revenue]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Category": "Category"}},
        measure_names_by_table={"Sales": {"Revenue"}},
        measure_aggs_by_table={"Sales": {"Revenue": "sum"}},
        measure_sql_by_table={"Sales": {"Revenue": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "SUM(Sales.Amount) AS Revenue" in sql
    assert "GROUP BY Sales.Category" in sql


def test_translate_query_model_measure_reference_joins_cross_table_grouping():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[Fiscal Year],
                "Revenue", [Revenue]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        measure_names_by_table={"Sales": {"Revenue"}},
        measure_aggs_by_table={"Sales": {"Revenue": "sum"}},
        measure_sql_by_table={"Sales": {"Revenue": "Amount"}},
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "SUM(Sales.Amount) AS Revenue" in sql
    assert "FROM Date LEFT JOIN Sales ON Date.DateKey = Sales.DateKey" in sql
    assert "GROUP BY Date.FiscalYear" in sql


def test_translate_query_calculate_model_measure_reference_applies_filters_inside_aggregate():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[Category],
                "Revenue", CALCULATE([Revenue], 'Sales'[Amount] > 0)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Category": "Category", "Amount": "Amount"}},
        measure_names_by_table={"Sales": {"Revenue"}},
        measure_aggs_by_table={"Sales": {"Revenue": "sum"}},
        measure_sql_by_table={"Sales": {"Revenue": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "SUM(CASE WHEN (Sales.Amount > 0) THEN Sales.Amount ELSE NULL END) AS Revenue" in sql
    assert "CASE WHEN (Sales.Amount > 0) THEN SUM" not in sql


def test_translate_query_row_model_measure_reference_adds_measure_table_source():
    query = _parse_query('EVALUATE ROW("Revenue", [Revenue])')

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Category": "Category"}},
        measure_names_by_table={"Sales": {"Revenue"}},
        measure_aggs_by_table={"Sales": {"Revenue": "sum"}},
        measure_sql_by_table={"Sales": {"Revenue": "Amount"}},
    )

    assert translation.evaluates[0].sql == "SELECT SUM(Sales.Amount) AS Revenue FROM Sales"


def test_translate_query_define_var_is_resolved():
    query = _parse_query(
        """
        DEFINE
            VAR threshold = 10
        EVALUATE
            FILTER('Sales', 'Sales'[Amount] > threshold)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "WHERE (Amount > 10)" in sql


def test_translate_query_define_function_is_inlined():
    query = _parse_query(
        """
        DEFINE
            FUNCTION add_tax = (x : NUMERIC) => x * 1.1
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "Gross",
                add_tax('Sales'[Amount])
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "(Amount * 1.1) AS Gross" in sql


def test_translate_query_define_column_is_resolved():
    query = _parse_query(
        """
        DEFINE
            COLUMN 'Sales'[Net] = 'Sales'[Amount] - 1
        EVALUATE
            SELECTCOLUMNS(
                'Sales',
                "Net",
                'Sales'[Net]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount", "Net": "Net"}},
    )

    sql = translation.evaluates[0].sql
    assert "(Amount - 1) AS Net" in sql


def test_translate_query_table_constructor_evaluate():
    query = _parse_query(
        """
        EVALUATE
            { 1, 2 }
        """
    )

    translation = translate_dax_query(query)

    sql = translation.evaluates[0].sql
    assert "SELECT 1 AS value1" in sql
    assert "SELECT 2 AS value1" in sql


def test_translate_query_table_constructor_evaluate_with_table_column_ref_adds_from_clause():
    query = _parse_query(
        """
        EVALUATE
            { ('Sales'[Amount], 2) }
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Amount AS value1, 2 AS value2 FROM Sales" in sql
    assert "Sales" in translation.evaluates[0].required_models


def test_translate_query_table_constructor_evaluate_allows_multi_table_refs():
    query = _parse_query(
        """
        EVALUATE
            { ('Sales'[Amount], 'Date'[DateKey]) }
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Date": {"DateKey": "DateKey"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Amount AS value1, Date.DateKey AS value2 FROM Sales CROSS JOIN Date" in sql
    assert "Sales" in translation.evaluates[0].required_models
    assert "Date" in translation.evaluates[0].required_models


def test_translate_query_row_evaluate():
    query = _parse_query(
        """
        EVALUATE
            ROW("one", 1, "two", 2)
        """
    )

    translation = translate_dax_query(query)

    sql = translation.evaluates[0].sql
    assert "SELECT 1 AS one, 2 AS two" in sql


def test_translate_query_row_evaluate_allows_multi_table_refs():
    query = _parse_query(
        """
        EVALUATE
            ROW("sales_amount", 'Sales'[Amount], "date_key", 'Date'[DateKey])
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Date": {"DateKey": "DateKey"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Amount AS sales_amount, Date.DateKey AS date_key FROM Sales CROSS JOIN Date" in sql
    assert "Sales" in translation.evaluates[0].required_models
    assert "Date" in translation.evaluates[0].required_models


def test_translate_query_row_evaluate_numeric_scalar_functions():
    query = _parse_query(
        """
        EVALUATE
            ROW(
                "abs_value", ABS(-5),
                "mod_value", MOD(10, 3),
                "pow_value", POWER(2, 3),
                "sqrt_value", SQRT(9),
                "log_value", LOG(100),
                "pi_value", PI(),
                "min_value", MIN(2, 3),
                "max_value", MAX(2, 3)
            )
        """
    )

    translation = translate_dax_query(query)

    sql = translation.evaluates[0].sql
    assert "ABS(-5) AS abs_value" in sql
    assert "MOD(10, 3) AS mod_value" in sql
    assert "POWER(2, 3) AS pow_value" in sql
    assert "SQRT(9) AS sqrt_value" in sql
    assert "LOG10(100) AS log_value" in sql
    assert "PI() AS pi_value" in sql
    assert "LEAST(2, 3) AS min_value" in sql
    assert "GREATEST(2, 3) AS max_value" in sql


def test_translate_query_row_requires_name_expression_pairs():
    query = _parse_query(
        """
        EVALUATE
            ROW("one", 1, "bad")
        """
    )

    with pytest.raises(DaxTranslationError, match="ROW requires name/expression pairs"):
        translate_dax_query(query)


def test_translate_query_union_evaluate():
    query = _parse_query(
        """
        EVALUATE
            UNION({1}, {2})
        """
    )

    translation = translate_dax_query(query)

    sql = translation.evaluates[0].sql
    assert "UNION ALL" in sql
    assert "SELECT * FROM (SELECT 1 AS value1) AS t0" in sql
    assert "SELECT * FROM (SELECT 2 AS value1) AS t1" in sql


def test_translate_query_union_allows_multi_table_refs():
    query = _parse_query(
        """
        EVALUATE
            UNION('sales', 'tax')
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "sales": {"amount": "amount"},
            "tax": {"rate": "rate"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "UNION ALL" in sql
    assert "SELECT * FROM (SELECT * FROM sales) AS t0" in sql
    assert "SELECT * FROM (SELECT * FROM tax) AS t1" in sql


def test_translate_query_union_requires_two_tables():
    query = _parse_query(
        """
        EVALUATE
            UNION({1})
        """
    )

    with pytest.raises(DaxTranslationError, match="UNION requires at least two table arguments"):
        translate_dax_query(query)


def test_translate_query_intersect_evaluate():
    query = _parse_query(
        """
        EVALUATE
            INTERSECT({1, 2}, {2, 3})
        """
    )

    translation = translate_dax_query(query)

    sql = translation.evaluates[0].sql
    assert "INTERSECT ALL" in sql
    assert "SELECT * FROM (SELECT 1 AS value1 UNION ALL SELECT 2 AS value1) AS t0" in sql
    assert "SELECT * FROM (SELECT 2 AS value1 UNION ALL SELECT 3 AS value1) AS t1" in sql


def test_translate_query_except_evaluate():
    query = _parse_query(
        """
        EVALUATE
            EXCEPT({1, 2}, {2, 3})
        """
    )

    translation = translate_dax_query(query)

    sql = translation.evaluates[0].sql
    assert "EXCEPT ALL" in sql
    assert "SELECT * FROM (SELECT 1 AS value1 UNION ALL SELECT 2 AS value1) AS t0" in sql
    assert "SELECT * FROM (SELECT 2 AS value1 UNION ALL SELECT 3 AS value1) AS t1" in sql


def test_translate_query_intersect_requires_two_tables():
    query = _parse_query(
        """
        EVALUATE
            INTERSECT({1})
        """
    )

    with pytest.raises(DaxTranslationError, match="INTERSECT requires exactly two table arguments"):
        translate_dax_query(query)


def test_translate_query_except_requires_two_tables():
    query = _parse_query(
        """
        EVALUATE
            EXCEPT({1})
        """
    )

    with pytest.raises(DaxTranslationError, match="EXCEPT requires exactly two table arguments"):
        translate_dax_query(query)


def test_translate_query_crossjoin_evaluate():
    query = _parse_query(
        """
        EVALUATE
            CROSSJOIN({1}, {2})
        """
    )

    translation = translate_dax_query(query)

    sql = translation.evaluates[0].sql
    assert "SELECT * FROM (SELECT 1 AS value1) AS t0 CROSS JOIN (SELECT 2 AS value1) AS t1" in sql


def test_translate_query_crossjoin_requires_two_tables():
    query = _parse_query(
        """
        EVALUATE
            CROSSJOIN({1})
        """
    )

    with pytest.raises(DaxTranslationError, match="CROSSJOIN requires at least two table arguments"):
        translate_dax_query(query)


def test_translate_query_naturalinnerjoin_evaluate():
    query = _parse_query(
        """
        EVALUATE
            NATURALINNERJOIN(ROW("k", 1), ROW("k", 1))
        """
    )

    translation = translate_dax_query(query)

    sql = translation.evaluates[0].sql
    assert "NATURAL INNER JOIN" in sql
    assert "SELECT * FROM (SELECT 1 AS k) AS t0 NATURAL INNER JOIN (SELECT 1 AS k) AS t1" in sql


def test_translate_query_naturalinnerjoin_requires_two_tables():
    query = _parse_query(
        """
        EVALUATE
            NATURALINNERJOIN(ROW("k", 1))
        """
    )

    with pytest.raises(DaxTranslationError, match="NATURALINNERJOIN requires exactly two table arguments"):
        translate_dax_query(query)


def test_translate_query_naturalleftouterjoin_evaluate():
    query = _parse_query(
        """
        EVALUATE
            NATURALLEFTOUTERJOIN(ROW("k", 1), ROW("k", 2))
        """
    )

    translation = translate_dax_query(query)

    sql = translation.evaluates[0].sql
    assert "NATURAL LEFT JOIN" in sql
    assert "SELECT * FROM (SELECT 1 AS k) AS t0 NATURAL LEFT JOIN (SELECT 2 AS k) AS t1" in sql


def test_translate_query_naturalleftouterjoin_requires_two_tables():
    query = _parse_query(
        """
        EVALUATE
            NATURALLEFTOUTERJOIN(ROW("k", 1))
        """
    )

    with pytest.raises(DaxTranslationError, match="NATURALLEFTOUTERJOIN requires exactly two table arguments"):
        translate_dax_query(query)


def test_translate_query_generate_table():
    query = _parse_query(
        """
        EVALUATE
            GENERATE(
                VALUES('Date'[Fiscal Year]),
                VALUES('Product'[Category])
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear"},
            "Product": {"Category": "Category"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "CROSS JOIN LATERAL" in sql
    assert "SELECT DISTINCT Date.FiscalYear FROM Date" in sql
    assert "SELECT DISTINCT Product.Category FROM Product" in sql


def test_translate_query_generate_allows_cross_table_right_input():
    query = _parse_query(
        """
        EVALUATE
            GENERATE(
                'sales',
                FILTER('tax', 'tax'[rate] > 0)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "sales": {"amount": "amount", "date_key": "date_key"},
            "tax": {"rate": "rate", "date_key": "date_key"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "CROSS JOIN LATERAL" in sql
    assert "FROM (SELECT * FROM sales) AS l" in sql
    assert "FROM tax" in sql
    assert "rate > 0" in sql


def test_translate_query_generateall_table():
    query = _parse_query(
        """
        EVALUATE
            GENERATEALL(
                VALUES('Date'[Fiscal Year]),
                VALUES('Product'[Category])
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear"},
            "Product": {"Category": "Category"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "LEFT JOIN LATERAL" in sql
    assert "ON TRUE" in sql


def test_translate_query_generateall_allows_cross_table_right_input():
    query = _parse_query(
        """
        EVALUATE
            GENERATEALL(
                'sales',
                FILTER('tax', 'tax'[rate] > 0)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "sales": {"amount": "amount", "date_key": "date_key"},
            "tax": {"rate": "rate", "date_key": "date_key"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "LEFT JOIN LATERAL" in sql
    assert "ON TRUE" in sql
    assert "FROM (SELECT * FROM sales) AS l" in sql
    assert "FROM tax" in sql
    assert "rate > 0" in sql


def test_translate_query_generate_uses_lateral_join_shape():
    query = _parse_query(
        """
        EVALUATE
            GENERATE(
                VALUES('Date'[Fiscal Year]),
                FILTER('Date', 'Date'[Fiscal Year] > 2022)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Date": {"Fiscal Year": "FiscalYear"}},
    )

    sql = translation.evaluates[0].sql
    assert "CROSS JOIN LATERAL" in sql
    assert "WHERE (FiscalYear > 2022)" in sql


def test_translate_query_generate_correlates_left_alias_column_in_right_filter():
    query = _parse_query(
        """
        EVALUATE
            GENERATE(
                SELECTCOLUMNS('Date', "FY", 'Date'[Fiscal Year]),
                FILTER('Date', [FY] >= 2024)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Date": {"Fiscal Year": "FiscalYear"}},
    )

    sql = translation.evaluates[0].sql
    assert "CROSS JOIN LATERAL" in sql
    assert "SELECT FiscalYear AS FY FROM Date" in sql
    assert "WHERE (l.FY >= 2024)" in sql


def test_translate_query_generate_correlates_qualified_left_column_lineage_to_alias():
    query = _parse_query(
        """
        EVALUATE
            GENERATE(
                SELECTCOLUMNS('Date', "FY", 'Date'[Fiscal Year]),
                FILTER('Date', 'Date'[Fiscal Year] >= 2024)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Date": {"Fiscal Year": "FiscalYear"}},
    )

    sql = translation.evaluates[0].sql
    assert "CROSS JOIN LATERAL" in sql
    assert "SELECT FiscalYear AS FY FROM Date" in sql
    assert "WHERE (FiscalYear >= 2024)" in sql


def test_translate_query_generate_does_not_correlate_non_projected_left_column():
    query = _parse_query(
        """
        EVALUATE
            GENERATE(
                VALUES('Date'[Fiscal Year]),
                VALUES('Date'[DateKey])
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"}},
    )

    sql = translation.evaluates[0].sql
    assert "CROSS JOIN LATERAL" in sql
    assert "SELECT DISTINCT Date.FiscalYear FROM Date" in sql
    assert "SELECT DISTINCT Date.DateKey FROM Date" in sql
    assert "l.DateKey" not in sql


def test_translate_query_generate_nested_filter_preserves_local_right_row_context():
    query = _parse_query(
        """
        EVALUATE
            GENERATE(
                SELECTCOLUMNS('Date', "FY", 'Date'[Fiscal Year]),
                SUMMARIZE(
                    FILTER('Date', 'Date'[Fiscal Year] > [FY]),
                    'Date'[DateKey]
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"}},
    )

    sql = translation.evaluates[0].sql
    assert "CROSS JOIN LATERAL" in sql
    assert "WHERE (FiscalYear > l.FY)" in sql
    assert "l.FY > l.FY" not in sql


def test_translate_query_generate_nested_filter_with_addcolumns_alias_keeps_right_row_context():
    query = _parse_query(
        """
        EVALUATE
            GENERATE(
                ADDCOLUMNS(VALUES('Date'[Fiscal Year]), "FY2", 'Date'[Fiscal Year]),
                FILTER('Date', 'Date'[Fiscal Year] = [FY2])
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Date": {"Fiscal Year": "FiscalYear"}},
    )

    sql = translation.evaluates[0].sql
    assert "CROSS JOIN LATERAL" in sql
    assert "WHERE (FiscalYear = l.FY2)" in sql
    assert "l.FY2 = l.FY2" not in sql


def test_translate_query_generate_does_not_correlate_local_wrapped_alias_in_right_filter():
    query = _parse_query(
        """
        EVALUATE
            GENERATE(
                SELECTCOLUMNS('Date', "FY", 'Date'[Fiscal Year]),
                FILTER(
                    SELECTCOLUMNS('Date', "FY", 'Date'[Fiscal Year]),
                    [FY] >= 2024
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Date": {"Fiscal Year": "FiscalYear"}},
    )

    sql = translation.evaluates[0].sql
    assert "CROSS JOIN LATERAL" in sql
    assert "WHERE (FY >= 2024)" in sql
    assert "WHERE (l.FY >= 2024)" not in sql


def test_translate_query_generate_correlates_outer_column_when_left_uses_star_projection():
    query = _parse_query(
        """
        EVALUATE
            GENERATE(
                'Date',
                ROW("OuterDateKey", [DateKey])
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Date": {"DateKey": "DateKey", "Fiscal Year": "FiscalYear"}},
    )

    sql = translation.evaluates[0].sql
    assert "CROSS JOIN LATERAL" in sql
    assert "SELECT l.DateKey AS OuterDateKey" in sql


def test_translate_query_generate_left_star_does_not_rewrite_local_wrapped_right_column():
    query = _parse_query(
        """
        EVALUATE
            GENERATE(
                'Date',
                FILTER(
                    SELECTCOLUMNS('Date', "DateKey", 'Date'[DateKey]),
                    [DateKey] >= 2024
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Date": {"DateKey": "DateKey", "Fiscal Year": "FiscalYear"}},
    )

    sql = translation.evaluates[0].sql
    assert "CROSS JOIN LATERAL" in sql
    assert "WHERE (DateKey >= 2024)" in sql
    assert "WHERE (l.DateKey >= 2024)" not in sql


def test_translate_query_generateall_correlates_outer_column_when_left_uses_star_projection():
    query = _parse_query(
        """
        EVALUATE
            GENERATEALL(
                'Date',
                ROW("OuterDateKey", [DateKey])
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Date": {"DateKey": "DateKey", "Fiscal Year": "FiscalYear"}},
    )

    sql = translation.evaluates[0].sql
    assert "LEFT JOIN LATERAL" in sql
    assert "ON TRUE" in sql
    assert "SELECT l.DateKey AS OuterDateKey" in sql


def test_translate_query_generate_raises_for_ambiguous_multitable_star_outer_column_reference():
    query = _parse_query(
        """
        EVALUATE
            GENERATE(
                CROSSJOIN(
                    VALUES('Date'[DateKey]),
                    VALUES('Sales'[DateKey])
                ),
                ROW("OuterDateKey", [DateKey])
            )
        """
    )

    with pytest.raises(DaxTranslationError, match="Ambiguous outer column reference"):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Date": {"DateKey": "DateKey"},
                "Sales": {"DateKey": "DateKey"},
            },
        )


def test_translate_query_generateall_raises_for_ambiguous_multitable_star_outer_column_reference():
    query = _parse_query(
        """
        EVALUATE
            GENERATEALL(
                CROSSJOIN(
                    VALUES('Date'[DateKey]),
                    VALUES('Sales'[DateKey])
                ),
                ROW("OuterDateKey", [DateKey])
            )
        """
    )

    with pytest.raises(DaxTranslationError, match="Ambiguous outer column reference"):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Date": {"DateKey": "DateKey"},
                "Sales": {"DateKey": "DateKey"},
            },
        )


@pytest.mark.parametrize("fn_name", ["GENERATE", "GENERATEALL"])
def test_translate_query_generate_raises_for_ambiguous_duplicate_lineage_outer_column_reference(fn_name: str):
    query = _parse_query(
        f"""
        EVALUATE
            {fn_name}(
                SELECTCOLUMNS(
                    'Date',
                    "DateKey1", 'Date'[DateKey],
                    "DateKey2", 'Date'[DateKey]
                ),
                ROW("OuterDateKey", 'Date'[DateKey])
            )
        """
    )

    with pytest.raises(DaxTranslationError, match="Ambiguous outer column reference"):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Date": {"DateKey": "DateKey"},
            },
        )


def test_rewrite_expr_for_alias_keeps_nested_qualified_reference_local_to_enclosing_scope():
    sql = "SELECT * FROM Date AS Date WHERE EXISTS (SELECT 1 FROM Sales WHERE Sales.DateKey = Date.DateKey)"
    rewritten = _rewrite_expr_for_alias(sql, "l", source_tables={"Date"}, source_columns={"DateKey"})

    assert "Sales.DateKey = Date.DateKey" in rewritten
    assert "Sales.DateKey = l.DateKey" not in rewritten


def test_rewrite_expr_for_alias_rewrites_when_no_enclosing_local_table_scope_exists():
    sql = "SELECT * FROM Sales WHERE Sales.DateKey = Date.DateKey"
    rewritten = _rewrite_expr_for_alias(sql, "l", source_tables={"Date"}, source_columns={"DateKey"})

    assert "Sales.DateKey = l.DateKey" in rewritten


def test_rewrite_expr_for_alias_skips_ambiguous_multitable_wildcard_rewrite():
    sql = "SELECT * FROM Sales WHERE Date.DateKey = Sales.DateKey"
    rewritten = _rewrite_expr_for_alias(
        sql,
        "l",
        source_tables={"Date", "Product"},
        source_columns={"*"},
        ambiguous_source_columns={"DateKey"},
    )

    assert "Date.DateKey = Sales.DateKey" in rewritten
    assert "l.DateKey = Sales.DateKey" not in rewritten


def test_rewrite_expr_for_alias_raises_when_strict_rewrite_cannot_parse(monkeypatch):
    import sqlglot

    def _boom(*_args, **_kwargs):
        raise ValueError("parse failed")

    monkeypatch.setattr(sqlglot, "parse_one", _boom)

    with pytest.raises(DaxTranslationError, match="Unable to safely correlate outer column references"):
        _rewrite_expr_for_alias(
            "SELECT Date.DateKey",
            "l",
            source_tables={"Date"},
            source_columns={"DateKey"},
            allow_fallback=False,
            strict_source_resolution=True,
        )


def test_translate_query_topnskip():
    query = _parse_query(
        """
        EVALUATE
            TOPNSKIP(2, 1, 'Sales', 'Sales'[Amount], DESC)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "ORDER BY" in sql
    assert "DESC" in sql
    assert "LIMIT 2 OFFSET 1" in sql


def test_translate_query_topn_accepts_scalar_count_expression():
    query = _parse_query(
        """
        EVALUATE
            TOPN(1 + 1, 'Sales', 'Sales'[Amount], DESC)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "ORDER BY" in sql
    assert "DESC" in sql
    assert "LIMIT CAST(((1 + 1)) AS BIGINT)" in sql


def test_translate_query_topnskip_accepts_scalar_skip_expression():
    query = _parse_query(
        """
        EVALUATE
            TOPNSKIP(3 - 1, 5 / 2, 'Sales', 'Sales'[Amount], ASC)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "ORDER BY" in sql
    assert "ASC" in sql
    assert "LIMIT CAST(((3 - 1)) AS BIGINT)" in sql
    assert "OFFSET CAST(((5 / 2)) AS BIGINT)" in sql


def test_translate_query_topn_cross_table_order_by_joins_related_table():
    query = _parse_query(
        """
        EVALUATE
            TOPN(2, 'Sales', 'Product'[Category], ASC)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"ProductKey": "ProductKey"},
            "Product": {"ProductKey": "ProductKey", "Category": "Category"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="ProductKey",
                to_table="Product",
                to_column="ProductKey",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Sales.* FROM Sales LEFT JOIN Product ON Sales.ProductKey = Product.ProductKey" in sql
    assert "ORDER BY Product.Category ASC LIMIT 2" in sql


def test_translate_query_topnskip_cross_table_order_by_cross_join_when_unrelated():
    query = _parse_query(
        """
        EVALUATE
            TOPNSKIP(2, 1, 'Sales', 'Tax'[Rate], ASC)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Tax": {"Rate": "Rate"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Sales.* FROM Sales CROSS JOIN Tax ORDER BY Tax.Rate ASC LIMIT 2 OFFSET 1" in sql


def test_translate_query_topn_wrapped_base_cross_table_order_by_joins_related_table():
    query = _parse_query(
        """
        EVALUATE
            TOPN(2, FILTER('Sales', 'Sales'[Amount] > 0), 'Product'[Category], ASC)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
            "Product": {"ProductKey": "ProductKey", "Category": "Category"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="ProductKey",
                to_table="Product",
                to_column="ProductKey",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert (
        "SELECT t.* FROM (SELECT * FROM Sales WHERE (Amount > 0)) AS t LEFT JOIN Product ON t.ProductKey = Product.ProductKey"
        in sql
    )
    assert "ORDER BY Product.Category ASC LIMIT 2" in sql


def test_translate_query_topnskip_wrapped_base_cross_table_order_by_cross_join_when_unrelated():
    query = _parse_query(
        """
        EVALUATE
            TOPNSKIP(2, 1, FILTER('Sales', 'Sales'[Amount] > 0), 'Tax'[Rate], DESC)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Tax": {"Rate": "Rate"},
        },
    )

    sql = translation.evaluates[0].sql
    assert (
        "SELECT t.* FROM (SELECT * FROM Sales WHERE (Amount > 0)) AS t CROSS JOIN Tax ORDER BY Tax.Rate DESC LIMIT 2 OFFSET 1"
        in sql
    )


def test_translate_query_addmissingitems_with_summarizecolumns():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                SUMMARIZECOLUMNS(
                    'Date'[Fiscal Year],
                    "Revenue", SUM('Sales'[Amount])
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "ADDMISSINGITEMS" not in sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date" in sql
    assert "LEFT JOIN (" in sql
    assert "b.FiscalYear IS NOT DISTINCT FROM d.__addmissingitems_k0" in sql
    assert "b.* EXCLUDE (FiscalYear)" in sql
    assert "AS Revenue" in sql


def test_translate_query_addmissingitems_with_multiple_group_columns():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                'Product'[Category],
                SUMMARIZECOLUMNS(
                    'Date'[Fiscal Year],
                    'Product'[Category],
                    "Revenue", SUM('Sales'[Amount])
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear"},
            "Product": {"Category": "Category"},
            "Sales": {"Amount": "Amount"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0, Product.Category AS __addmissingitems_k1" in sql
    assert "LEFT JOIN (" in sql
    assert "b.FiscalYear IS NOT DISTINCT FROM d.__addmissingitems_k0" in sql
    assert "b.Category IS NOT DISTINCT FROM d.__addmissingitems_k1" in sql


def test_translate_query_addmissingitems_showall_column_not_projected_by_table_arg():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                'Product'[Category],
                SUMMARIZECOLUMNS(
                    'Date'[Fiscal Year],
                    "Revenue", SUM('Sales'[Amount])
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Product": {"Category": "Category", "ProductKey": "ProductKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey", "ProductKey": "ProductKey"},
        },
        relationship_edges=[
            RelationshipEdge("Sales", "DateKey", "Date", "DateKey"),
            RelationshipEdge("Sales", "ProductKey", "Product", "ProductKey"),
        ],
    )

    sql = translation.evaluates[0].sql
    assert "b.FiscalYear IS NOT DISTINCT FROM d.__addmissingitems_k0" in sql
    assert "b.Category IS NOT DISTINCT FROM d.__addmissingitems_k1" not in sql
    assert "AS Revenue" in sql


def test_translate_query_addmissingitems_showall_only_with_scalar_table_arg_uses_true_join():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Product'[Category],
                ROW("Revenue", 1)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Product": {"Category": "Category"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "ON TRUE" in sql
    assert "SELECT DISTINCT Product.Category AS __addmissingitems_k0 FROM Product" in sql


def test_translate_query_addmissingitems_applies_trailing_filter_table_to_domain():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                SUMMARIZECOLUMNS(
                    'Date'[Fiscal Year],
                    "Revenue", SUM('Sales'[Amount])
                ),
                FILTER('Date', 'Date'[Fiscal Year] >= 2024)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date WHERE (Date.FiscalYear >= 2024)" in sql
    assert "LEFT JOIN (" in sql


def test_translate_query_addmissingitems_filter_table_adds_domain_join_tables():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                SUMMARIZECOLUMNS(
                    'Date'[Fiscal Year],
                    "Revenue", SUM('Sales'[Amount])
                ),
                FILTER('Sales', 'Sales'[Amount] > 0)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "FROM Date LEFT JOIN Sales ON Date.DateKey = Sales.DateKey WHERE (Sales.Amount > 0)" in sql
    assert "LEFT JOIN (" in sql


def test_translate_query_addmissingitems_filter_before_table_arg():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                FILTER('Date', 'Date'[Fiscal Year] >= 2024),
                SUMMARIZECOLUMNS(
                    'Date'[Fiscal Year],
                    "Revenue", SUM('Sales'[Amount])
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date WHERE (Date.FiscalYear >= 2024)" in sql
    assert "LEFT JOIN (" in sql
    assert "SUM(Sales.Amount) AS Revenue" in sql


def test_translate_query_addmissingitems_direct_group_table_before_main_table():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                'Date',
                'Sales'
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear"},
            "Sales": {"Amount": "Amount"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date" in sql
    assert "LEFT JOIN (SELECT * FROM Sales) AS b ON TRUE" in sql


def test_translate_query_addmissingitems_direct_group_table_before_calculatetable_main():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                'Date',
                CALCULATETABLE('Sales', 'Sales'[Amount] > 0)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear"},
            "Sales": {"Amount": "Amount"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date" in sql
    assert "LEFT JOIN (SELECT * FROM Sales WHERE (Sales.Amount > 0)) AS b ON TRUE" in sql


def test_translate_query_addmissingitems_direct_group_table_before_nonvisual_calculatetable_main():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                'Date',
                NONVISUAL(CALCULATETABLE('Sales', 'Sales'[Amount] > 0))
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear"},
            "Sales": {"Amount": "Amount"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date" in sql
    assert "LEFT JOIN (SELECT * FROM Sales WHERE (Sales.Amount > 0)) AS b ON TRUE" in sql


def test_translate_query_addmissingitems_leading_nonvisual_calculatetable_filter_before_main_table():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                NONVISUAL(CALCULATETABLE('Date', 'Date'[Fiscal Year] >= 2024)),
                SUMMARIZECOLUMNS(
                    'Date'[Fiscal Year],
                    "Revenue", SUM('Sales'[Amount])
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date WHERE (Date.FiscalYear >= 2024)" in sql
    assert "SUM(Sales.Amount) AS Revenue" in sql
    assert "b.FiscalYear IS NOT DISTINCT FROM d.__addmissingitems_k0" in sql
    assert "LEFT JOIN (SELECT * FROM Date WHERE (Date.FiscalYear >= 2024)) AS b ON TRUE" not in sql


def test_translate_query_addmissingitems_trailing_calculatetable_filter():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                SUMMARIZECOLUMNS(
                    'Date'[Fiscal Year],
                    "Revenue", SUM('Sales'[Amount])
                ),
                CALCULATETABLE('Date', 'Date'[Fiscal Year] >= 2024)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date WHERE (Date.FiscalYear >= 2024)" in sql
    assert "SUM(Sales.Amount) AS Revenue" in sql
    assert "LEFT JOIN (" in sql


def test_translate_query_addmissingitems_prefers_wrapped_summarizecolumns_table_arg():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                FILTER('Date', 'Date'[Fiscal Year] >= 2023),
                CALCULATETABLE(
                    SUMMARIZECOLUMNS(
                        'Date'[Fiscal Year],
                        "Revenue", SUM('Sales'[Amount])
                    ),
                    'Date'[Fiscal Year] >= 2024
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date WHERE (Date.FiscalYear >= 2023)" in sql
    assert "SUM(Sales.Amount) AS Revenue" in sql
    assert "WHERE (FiscalYear >= 2024)" in sql
    assert "b.FiscalYear IS NOT DISTINCT FROM d.__addmissingitems_k0" in sql
    assert " ON TRUE" not in sql


def test_translate_query_addmissingitems_prefers_keepfilters_wrapped_summarize_table_arg():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                FILTER('Date', 'Date'[Fiscal Year] >= 2024),
                KEEPFILTERS(
                    SUMMARIZE(
                        'Sales',
                        'Date'[Fiscal Year],
                        "Revenue", SUM('Sales'[Amount])
                    )
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date WHERE (Date.FiscalYear >= 2024)" in sql
    assert "SUM(Sales.Amount) AS Revenue" in sql
    assert "b.FiscalYear IS NOT DISTINCT FROM d.__addmissingitems_k0" in sql
    assert "LEFT JOIN (SELECT * FROM Date WHERE (FiscalYear >= 2024)) AS b ON TRUE" not in sql


def test_translate_query_addmissingitems_prefers_calculatetable_wrapped_summarize_table_arg():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                FILTER('Date', 'Date'[Fiscal Year] >= 2024),
                CALCULATETABLE(
                    SUMMARIZE(
                        'Sales',
                        'Date'[Fiscal Year],
                        "Revenue", SUM('Sales'[Amount])
                    ),
                    'Date'[Fiscal Year] >= 2023
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date WHERE (Date.FiscalYear >= 2024)" in sql
    assert "SUM(Sales.Amount) AS Revenue" in sql
    assert "WHERE (Date.FiscalYear >= 2023)" in sql
    assert "b.FiscalYear IS NOT DISTINCT FROM d.__addmissingitems_k0" in sql
    assert "LEFT JOIN (SELECT * FROM Date WHERE (FiscalYear >= 2024)) AS b ON TRUE" not in sql


def test_translate_query_addmissingitems_prefers_nonvisual_keepfilters_wrapped_summarize_table_arg():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                FILTER('Date', 'Date'[Fiscal Year] >= 2024),
                NONVISUAL(
                    KEEPFILTERS(
                        SUMMARIZE(
                            'Sales',
                            'Date'[Fiscal Year],
                            "Revenue", SUM('Sales'[Amount])
                        )
                    )
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date WHERE (Date.FiscalYear >= 2024)" in sql
    assert "SUM(Sales.Amount) AS Revenue" in sql
    assert "b.FiscalYear IS NOT DISTINCT FROM d.__addmissingitems_k0" in sql
    assert "LEFT JOIN (SELECT * FROM Date WHERE (FiscalYear >= 2024)) AS b ON TRUE" not in sql


def test_translate_query_addmissingitems_keeps_first_non_filter_table_arg():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                SUMMARIZECOLUMNS(
                    'Date'[Fiscal Year],
                    "Revenue", SUM('Sales'[Amount])
                ),
                UNION(VALUES('Date'[Fiscal Year]), VALUES('Date'[Fiscal Year]))
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "SUM(Sales.Amount) AS Revenue" in sql
    assert "LEFT JOIN (" in sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date" in sql


def test_translate_query_addmissingitems_union_before_main_table_arg():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                UNION(VALUES('Date'[Fiscal Year]), VALUES('Date'[Fiscal Year])),
                SUMMARIZECOLUMNS(
                    'Date'[Fiscal Year],
                    "Revenue", SUM('Sales'[Amount])
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "SUM(Sales.Amount) AS Revenue" in sql
    assert "LEFT JOIN (" in sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date" in sql


def test_translate_query_addmissingitems_union_main_table_with_trailing_filter():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                UNION(VALUES('Date'[Fiscal Year]), VALUES('Date'[Fiscal Year])),
                FILTER('Date', 'Date'[Fiscal Year] >= 2024)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Date": {"Fiscal Year": "FiscalYear"}},
    )

    sql = translation.evaluates[0].sql
    assert (
        "LEFT JOIN (SELECT * FROM (SELECT DISTINCT Date.FiscalYear FROM Date) AS t0 UNION ALL SELECT * FROM (SELECT DISTINCT Date.FiscalYear FROM Date) AS t1) AS b"
        in sql
    )
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date WHERE (Date.FiscalYear >= 2024)" in sql


def test_translate_query_addmissingitems_prefers_non_group_calculatetable_over_leading_group_union():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                UNION(VALUES('Date'[Fiscal Year]), VALUES('Date'[Fiscal Year])),
                CALCULATETABLE('Sales', 'Sales'[Amount] > 0)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear"},
            "Sales": {"Amount": "Amount"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date" in sql
    assert "FROM Date CROSS JOIN Sales WHERE (Sales.Amount > 0)" not in sql
    assert "LEFT JOIN (SELECT * FROM Sales WHERE (Sales.Amount > 0)) AS b ON TRUE" in sql


def test_translate_query_addmissingitems_prefers_summarize_over_leading_union_candidate():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                UNION(VALUES('Sales'[DateKey]), VALUES('Sales'[DateKey])),
                SUMMARIZE(
                    'Sales',
                    'Date'[Fiscal Year],
                    "Revenue", SUM('Sales'[Amount])
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"DateKey": "DateKey", "Amount": "Amount"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SUM(Sales.Amount) AS Revenue" in sql
    assert "LEFT JOIN (SELECT * FROM (SELECT DISTINCT Sales.DateKey FROM Sales) AS t0 UNION ALL" not in sql


def test_translate_query_addmissingitems_prefers_wrapped_summarize_over_leading_union_candidate():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                UNION(VALUES('Sales'[DateKey]), VALUES('Sales'[DateKey])),
                CALCULATETABLE(
                    SUMMARIZE(
                        'Sales',
                        'Date'[Fiscal Year],
                        "Revenue", SUM('Sales'[Amount])
                    ),
                    'Date'[Fiscal Year] >= 2024
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"DateKey": "DateKey", "Amount": "Amount"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SUM(Sales.Amount) AS Revenue" in sql
    assert "WHERE (Date.FiscalYear >= 2024)" in sql
    assert "LEFT JOIN (SELECT * FROM (SELECT DISTINCT Sales.DateKey FROM Sales) AS t0 UNION ALL" not in sql


@pytest.mark.parametrize(
    "leading_set_expr",
    [
        "UNION(VALUES('Date'[Fiscal Year]), VALUES('Date'[Fiscal Year]))",
        "INTERSECT(VALUES('Date'[Fiscal Year]), VALUES('Date'[Fiscal Year]))",
        "EXCEPT(VALUES('Date'[Fiscal Year]), VALUES('Date'[Fiscal Year]))",
        "CROSSJOIN(VALUES('Date'[Fiscal Year]), VALUES('Date'[Fiscal Year]))",
        "NATURALINNERJOIN(VALUES('Date'[Fiscal Year]), VALUES('Date'[Fiscal Year]))",
        "NATURALLEFTOUTERJOIN(VALUES('Date'[Fiscal Year]), VALUES('Date'[Fiscal Year]))",
    ],
)
def test_translate_query_addmissingitems_prefers_summarize_over_leading_set_or_join_candidate(leading_set_expr: str):
    query = _parse_query(
        f"""
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                {leading_set_expr},
                SUMMARIZE(
                    'Sales',
                    'Date'[Fiscal Year],
                    "Revenue", SUM('Sales'[Amount])
                )
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear"},
            "Sales": {"Amount": "Amount"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SUM(Sales.Amount) AS Revenue" in sql
    assert "b.FiscalYear IS NOT DISTINCT FROM d.__addmissingitems_k0" in sql


def test_translate_query_addmissingitems_supports_trailing_group_by_column():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                SUMMARIZECOLUMNS(
                    'Date'[Fiscal Year],
                    'Product'[Category],
                    "Revenue", SUM('Sales'[Amount])
                ),
                'Product'[Category]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Product": {"Category": "Category", "ProductKey": "ProductKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey", "ProductKey": "ProductKey"},
        },
        relationship_edges=[
            RelationshipEdge("Sales", "DateKey", "Date", "DateKey"),
            RelationshipEdge("Sales", "ProductKey", "Product", "ProductKey"),
        ],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0, Product.Category AS __addmissingitems_k1" in sql
    assert "b.FiscalYear IS NOT DISTINCT FROM d.__addmissingitems_k0" in sql
    assert "b.Category IS NOT DISTINCT FROM d.__addmissingitems_k1" in sql


def test_translate_query_addmissingitems_deduplicates_repeated_group_column():
    query = _parse_query(
        """
        EVALUATE
            ADDMISSINGITEMS(
                'Date'[Fiscal Year],
                SUMMARIZECOLUMNS(
                    'Date'[Fiscal Year],
                    "Revenue", SUM('Sales'[Amount])
                ),
                FILTER('Date', 'Date'[Fiscal Year] >= 2024),
                'Date'[Fiscal Year]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Date": {"Fiscal Year": "FiscalYear", "DateKey": "DateKey"},
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
        },
        relationship_edges=[RelationshipEdge("Sales", "DateKey", "Date", "DateKey")],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Date.FiscalYear AS __addmissingitems_k0 FROM Date WHERE (Date.FiscalYear >= 2024)" in sql
    assert "__addmissingitems_k1" not in sql
    assert "b.FiscalYear IS NOT DISTINCT FROM d.__addmissingitems_k0" in sql


def test_translate_query_groupby_with_currentgroup_iterators():
    query = _parse_query(
        """
        EVALUATE
            GROUPBY(
                'Sales',
                'Sales'[Category],
                "Revenue", SUMX(CURRENTGROUP(), 'Sales'[Amount]),
                "Rows", COUNTROWS(CURRENTGROUP())
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Category": "Category", "Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "GROUP BY" in sql
    assert "SUM(Sales.Amount) AS Revenue" in sql
    assert "COUNT(*) AS Rows" in sql


def test_translate_query_datatable():
    query = _parse_query(
        """
        EVALUATE
            DATATABLE(
                "k", INTEGER,
                "v", STRING,
                {{1, "a"}, {2, "b"}}
            )
        """
    )

    translation = translate_dax_query(query)

    sql = translation.evaluates[0].sql
    assert "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(k, v)" in sql


def test_translate_query_topnperlevel():
    query = _parse_query(
        """
        EVALUATE
            TOPNPERLEVEL(2, 'Sales'[Category], 'Sales', 'Sales'[Amount], DESC)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Category": "Category", "Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "RANK() OVER (PARTITION BY Sales.Category ORDER BY Amount DESC)" in sql
    assert "__topnperlevel_rank <= 2" in sql


def test_translate_query_topnperlevel_multiple_group_columns():
    query = _parse_query(
        """
        EVALUATE
            TOPNPERLEVEL(1, 'Sales'[Category], 'Sales'[Region], 'Sales', 'Sales'[Amount], DESC)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Category": "Category", "Region": "Region", "Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "PARTITION BY Sales.Category, Sales.Region" in sql
    assert "ORDER BY Amount DESC" in sql


def test_translate_query_topnperlevel_cross_table_group_order_joins_related_table():
    query = _parse_query(
        """
        EVALUATE
            TOPNPERLEVEL(1, 'Product'[Category], 'Sales', 'Product'[Category], DESC)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"ProductKey": "ProductKey", "Amount": "Amount"},
            "Product": {"ProductKey": "ProductKey", "Category": "Category"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="ProductKey",
                to_table="Product",
                to_column="ProductKey",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Sales.*, RANK() OVER (PARTITION BY Product.Category ORDER BY Product.Category DESC)" in sql
    assert "FROM Sales LEFT JOIN Product ON Sales.ProductKey = Product.ProductKey" in sql


def test_translate_query_topnperlevel_wrapped_base_cross_table_group_order_joins_related_table():
    query = _parse_query(
        """
        EVALUATE
            TOPNPERLEVEL(1, 'Product'[Category], FILTER('Sales', 'Sales'[Amount] > 0), 'Product'[Category], DESC)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"ProductKey": "ProductKey", "Amount": "Amount"},
            "Product": {"ProductKey": "ProductKey", "Category": "Category"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="ProductKey",
                to_table="Product",
                to_column="ProductKey",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT t.*, RANK() OVER (PARTITION BY Product.Category ORDER BY Product.Category DESC)" in sql
    assert (
        "FROM (SELECT * FROM Sales WHERE (Amount > 0)) AS t LEFT JOIN Product ON t.ProductKey = Product.ProductKey"
        in sql
    )


def test_translate_query_topnperlevel_wrapped_base_cross_table_group_order_cross_join_when_unrelated():
    query = _parse_query(
        """
        EVALUATE
            TOPNPERLEVEL(1, 'Tax'[Region], FILTER('Sales', 'Sales'[Amount] > 0), 'Tax'[Rate], DESC)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Tax": {"Region": "Region", "Rate": "Rate"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT t.*, RANK() OVER (PARTITION BY Tax.Region ORDER BY Tax.Rate DESC)" in sql
    assert "FROM (SELECT * FROM Sales WHERE (Amount > 0)) AS t CROSS JOIN Tax" in sql


def test_translate_query_define_table_is_resolved():
    query = _parse_query(
        """
        DEFINE
            TABLE MyTable = FILTER('Sales', 'Sales'[Amount] > 100)
        EVALUATE
            MyTable
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT * FROM Sales WHERE (Amount > 100)" in sql


def test_translate_query_evaluate_filter_with_cross_table_predicate_joins_related_table():
    query = _parse_query(
        """
        EVALUATE
            FILTER('Sales', 'Product'[Category] = "Clothing")
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"ProductKey": "ProductKey"},
            "Product": {"ProductKey": "ProductKey", "Category": "Category"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="ProductKey",
                to_table="Product",
                to_column="ProductKey",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Sales.* FROM Sales LEFT JOIN Product ON Sales.ProductKey = Product.ProductKey" in sql
    assert "WHERE (Product.Category = 'Clothing')" in sql


def test_translate_query_evaluate_filter_with_cross_table_predicate_cross_join_when_unrelated():
    query = _parse_query(
        """
        EVALUATE
            FILTER('Sales', 'Tax'[Rate] > 0)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Tax": {"Rate": "Rate"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Sales.* FROM Sales CROSS JOIN Tax WHERE (Tax.Rate > 0)" in sql


def test_translate_query_summarizecolumns_filter_derived_table_alias_predicate_uses_exists_subquery():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[Amount],
                FILTER({ ('Sales'[Amount], 'Date'[DateKey]) }, [value1] > 0)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Date": {"DateKey": "DateKey"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "WHERE EXISTS (SELECT 1 FROM (" in sql
    assert "SELECT * FROM (SELECT" in sql
    assert "AS value1" in sql
    assert "AS value2 FROM Sales CROSS JOIN Date) AS t WHERE (value1 > 0)" in sql
    assert "Sales.value1" not in sql
    assert "FROM Sales CROSS JOIN Date WHERE EXISTS" not in sql
    assert "FROM Sales WHERE EXISTS" in sql


def test_translate_query_summarizecolumns_filter_values_known_column_stays_direct_predicate():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[ProductKey],
                FILTER(VALUES('Sales'[ProductKey]), 'Sales'[ProductKey] > 1)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"ProductKey": "ProductKey"}},
    )

    sql = translation.evaluates[0].sql
    assert "ProductKey > 1" in sql
    assert "EXISTS (SELECT 1 FROM (" not in sql


def test_translate_query_summarizecolumns_filter_selectcolumns_alias_predicate_rewrites_directly():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Sales'[Amount],
                FILTER(SELECTCOLUMNS('Sales', "x", 'Sales'[Amount]), [x] > 0)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "WHERE (Sales.Amount > 0)" in sql
    assert "EXISTS (SELECT 1 FROM (" not in sql


def test_translate_query_evaluate_calculatetable_base_table_selectcolumns_alias_filter_rewrites_directly():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE(
                'Sales',
                FILTER(SELECTCOLUMNS('Sales', "x", 'Sales'[Amount]), [x] > 0)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT * FROM Sales WHERE (Sales.Amount > 0)" in sql
    assert "EXISTS (SELECT 1 FROM (" not in sql


def test_translate_query_evaluate_calculatetable():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE('Sales', 'Sales'[Amount] > 100)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT * FROM Sales WHERE (Sales.Amount > 100)" in sql


def test_translate_query_evaluate_calculatetable_base_table_cross_table_filter_joins_related_table():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE('Sales', 'Product'[Category] = "Clothing")
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"ProductKey": "ProductKey"},
            "Product": {"ProductKey": "ProductKey", "Category": "Category"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="ProductKey",
                to_table="Product",
                to_column="ProductKey",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Sales.* FROM Sales LEFT JOIN Product ON Sales.ProductKey = Product.ProductKey" in sql
    assert "WHERE (Product.Category = 'Clothing')" in sql


def test_translate_query_evaluate_calculatetable_base_table_cross_table_table_ref_filter_candidate():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE('Sales', 'Date')
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount", "DateKey": "DateKey"},
            "Date": {"DateKey": "DateKey"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT * FROM Sales" in sql
    assert "Date" in translation.evaluates[0].required_models


def test_translate_query_evaluate_calculatetable_base_table_cross_table_filter_cross_join_when_unrelated():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE('Sales', 'Tax'[Rate] > 0)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Tax": {"Rate": "Rate"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Sales.* FROM Sales CROSS JOIN Tax WHERE (Tax.Rate > 0)" in sql


def test_translate_query_evaluate_calculatetable_base_table_cross_table_datesinperiod_filter_cross_join_when_unrelated():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE('Sales', DATESINPERIOD('Date'[DateKey], "2024-12-31", -3, MONTH))
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"DateKey": "DateKey"},
            "Date": {"DateKey": "DateKey"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Sales.* FROM Sales CROSS JOIN Date" in sql
    assert "WHERE (Date.DateKey > ('2024-12-31' + INTERVAL '-3 month') AND Date.DateKey <= '2024-12-31')" in sql


def test_translate_query_evaluate_calculatetable_base_table_cross_table_datesytd_filter_cross_join_when_unrelated():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE('Sales', DATESYTD('Date'[DateKey]))
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"DateKey": "DateKey"},
            "Date": {"DateKey": "DateKey"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Sales.* FROM Sales CROSS JOIN Date" in sql
    assert (
        "WHERE (Date.DateKey >= DATE_TRUNC('year', (SELECT MAX(Date.DateKey) FROM Date)) "
        "AND Date.DateKey <= (SELECT MAX(Date.DateKey) FROM Date))"
    ) in sql


def test_translate_query_evaluate_calculatetable_base_table_cross_table_dateadd_filter_cross_join_when_unrelated():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE('Sales', DATEADD('Date'[DateKey], -1, YEAR))
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"DateKey": "DateKey"},
            "Date": {"DateKey": "DateKey"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Sales.* FROM Sales CROSS JOIN Date" in sql
    assert (
        "WHERE (Date.DateKey > ((SELECT MAX(Date.DateKey) FROM Date) + INTERVAL '-1 year') "
        "AND Date.DateKey <= (SELECT MAX(Date.DateKey) FROM Date))"
    ) in sql


def test_translate_query_evaluate_calculatetable_base_table_derived_alias_filter_does_not_expand_outer_from():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE(
                'Sales',
                FILTER({ ('Sales'[Amount], 'Date'[DateKey]) }, [value1] > 0)
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Date": {"DateKey": "DateKey"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT * FROM Sales WHERE EXISTS" in sql
    assert "FROM Sales CROSS JOIN Date WHERE EXISTS" not in sql
    assert "AS value2 FROM Sales CROSS JOIN Date) AS t WHERE (value1 > 0)" in sql


def test_translate_query_evaluate_summarizecolumns_cross_table_dateadd_filter_table_arg():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[FiscalYear],
                DATEADD('Date'[DateKey], -1, YEAR),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"DateKey": "DateKey"},
            "Date": {"DateKey": "DateKey", "FiscalYear": "FiscalYear"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Date.FiscalYear, (SELECT COUNT(*) FROM Sales) AS Rows FROM Date" in sql
    assert (
        "WHERE (Date.DateKey > ((SELECT MAX(Date.DateKey) FROM Date) + INTERVAL '-1 year') "
        "AND Date.DateKey <= (SELECT MAX(Date.DateKey) FROM Date))"
    ) in sql
    assert "GROUP BY Date.FiscalYear" in sql


def test_translate_query_evaluate_summarizecolumns_cross_table_datesytd_filter_table_arg():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[FiscalYear],
                DATESYTD('Date'[DateKey]),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"DateKey": "DateKey"},
            "Date": {"DateKey": "DateKey", "FiscalYear": "FiscalYear"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Date.FiscalYear, (SELECT COUNT(*) FROM Sales) AS Rows FROM Date" in sql
    assert (
        "WHERE (Date.DateKey >= DATE_TRUNC('year', (SELECT MAX(Date.DateKey) FROM Date)) "
        "AND Date.DateKey <= (SELECT MAX(Date.DateKey) FROM Date))"
    ) in sql
    assert "GROUP BY Date.FiscalYear" in sql


def test_translate_query_evaluate_summarizecolumns_cross_table_datesinperiod_filter_table_arg():
    query = _parse_query(
        """
        EVALUATE
            SUMMARIZECOLUMNS(
                'Date'[FiscalYear],
                DATESINPERIOD('Date'[DateKey], "2024-12-31", -3, MONTH),
                "Rows", COUNTROWS('Sales')
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"DateKey": "DateKey"},
            "Date": {"DateKey": "DateKey", "FiscalYear": "FiscalYear"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Date.FiscalYear, (SELECT COUNT(*) FROM Sales) AS Rows FROM Date" in sql
    assert "WHERE (Date.DateKey > ('2024-12-31' + INTERVAL '-3 month') AND Date.DateKey <= '2024-12-31')" in sql
    assert "GROUP BY Date.FiscalYear" in sql


def test_translate_query_evaluate_calculatetable_wrapped_base():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE(FILTER('Sales', 'Sales'[Amount] > 100), 'Sales'[Amount] > 200)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT * FROM (SELECT * FROM Sales WHERE (Amount > 100)) AS t WHERE (Amount > 200)" in sql


def test_translate_query_evaluate_calculatetable_wrapped_base_cross_table_filter_joins_related_table():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE(
                FILTER('Sales', 'Sales'[Amount] > 100),
                'Product'[Category] = "Clothing"
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
            "Product": {"ProductKey": "ProductKey", "Category": "Category"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="ProductKey",
                to_table="Product",
                to_column="ProductKey",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert (
        "SELECT t.* FROM (SELECT * FROM Sales WHERE (Amount > 100)) AS t LEFT JOIN Product ON t.ProductKey = Product.ProductKey"
        in sql
    )
    assert "WHERE (Product.Category = 'Clothing')" in sql


def test_translate_query_evaluate_calculatetable_wrapped_base_cross_table_filter_cross_join_when_unrelated():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE(
                FILTER('Sales', 'Sales'[Amount] > 100),
                'Tax'[Rate] > 0
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Tax": {"Rate": "Rate"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT t.* FROM (SELECT * FROM Sales WHERE (Amount > 100)) AS t CROSS JOIN Tax WHERE (Tax.Rate > 0)" in sql


def test_translate_query_evaluate_filter_wrapped_base_cross_table_filter_joins_related_table():
    query = _parse_query(
        """
        EVALUATE
            FILTER(
                FILTER('Sales', 'Sales'[Amount] > 100),
                'Product'[Category] = "Clothing"
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
            "Product": {"ProductKey": "ProductKey", "Category": "Category"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="ProductKey",
                to_table="Product",
                to_column="ProductKey",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert (
        "SELECT t.* FROM (SELECT * FROM Sales WHERE (Amount > 100)) AS t LEFT JOIN Product ON t.ProductKey = Product.ProductKey"
        in sql
    )
    assert "WHERE (Product.Category = 'Clothing')" in sql


def test_translate_query_evaluate_filter_wrapped_base_cross_table_filter_cross_join_when_unrelated():
    query = _parse_query(
        """
        EVALUATE
            FILTER(
                FILTER('Sales', 'Sales'[Amount] > 100),
                'Tax'[Rate] > 0
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount"},
            "Tax": {"Rate": "Rate"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT t.* FROM (SELECT * FROM Sales WHERE (Amount > 100)) AS t CROSS JOIN Tax WHERE (Tax.Rate > 0)" in sql


def test_translate_query_evaluate_nested_calculatetable_replaces_same_column_filter():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE(CALCULATETABLE('Sales', 'Sales'[Amount] > 100), 'Sales'[Amount] > 200)
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT * FROM Sales WHERE (Sales.Amount > 200)" in sql
    assert "(Sales.Amount > 100)" not in sql


def test_translate_query_evaluate_values_column():
    query = _parse_query(
        """
        EVALUATE
            VALUES('Sales'[Amount])
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT Sales.Amount FROM Sales" in sql


def test_translate_query_evaluate_values_multitable_scalar():
    query = _parse_query(
        """
        EVALUATE
            VALUES('Sales'[Amount] + 'Products'[Weight])
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"ProductKey": "ProductKey", "Amount": "Amount"},
            "Products": {"ProductKey": "ProductKey", "Weight": "Weight"},
        },
        relationship_edges=[
            RelationshipEdge(
                from_table="Sales",
                from_column="ProductKey",
                to_table="Products",
                to_column="ProductKey",
            )
        ],
    )

    sql = translation.evaluates[0].sql
    assert "SELECT DISTINCT (Sales.Amount + Products.Weight)" in sql
    assert "LEFT JOIN Products ON Sales.ProductKey = Products.ProductKey" in sql


def test_translate_query_evaluate_removecolumns_table():
    query = _parse_query(
        """
        EVALUATE
            REMOVECOLUMNS('Sales', 'Sales'[Amount])
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount", "Quantity": "Quantity"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT * EXCLUDE (Amount) FROM (SELECT * FROM Sales) AS t" in sql


def test_translate_query_evaluate_keepcolumns_table():
    query = _parse_query(
        """
        EVALUATE
            KEEPCOLUMNS('Sales', 'Sales'[Amount])
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount", "Quantity": "Quantity"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT t.Amount FROM (SELECT * FROM Sales) AS t" in sql


def test_translate_query_evaluate_keepcolumns_wrapped_table_with_named_column():
    query = _parse_query(
        """
        EVALUATE
            KEEPCOLUMNS(
                SELECTCOLUMNS('Sales', "Amt", 'Sales'[Amount], "Qty", 'Sales'[Quantity]),
                "Qty"
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount", "Quantity": "Quantity"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Amount AS Amt, Quantity AS Qty FROM Sales" in sql
    assert "SELECT t.Qty FROM (" in sql


def test_translate_query_evaluate_keepcolumns_addcolumns_preserves_base_star_columns():
    query = _parse_query(
        """
        EVALUATE
            KEEPCOLUMNS(
                ADDCOLUMNS('Sales', "W", 1),
                'Sales'[Amount]
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount", "Quantity": "Quantity"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT *, 1 AS W FROM Sales" in sql
    assert "SELECT t.Amount FROM (" in sql


def test_translate_query_evaluate_calculatetable_keepcolumns_preserves_underlying_filters():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE(
                'Sales',
                KEEPCOLUMNS(FILTER('Sales', 'Sales'[Amount] > 100), 'Sales'[Amount])
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount", "Quantity": "Quantity"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT * FROM Sales WHERE (Sales.Amount > 100)" in sql


def test_translate_query_evaluate_keepcolumns_requires_column_arg():
    query = _parse_query(
        """
        EVALUATE
            KEEPCOLUMNS('Sales')
        """
    )

    with pytest.raises(
        DaxTranslationError, match="KEEPCOLUMNS requires a table expression and at least one column argument"
    ):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Amount": "Amount"}},
        )


def test_translate_query_evaluate_keepcolumns_rejects_missing_input_column():
    query = _parse_query(
        """
        EVALUATE
            KEEPCOLUMNS('Sales', 'Products'[Weight])
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="KEEPCOLUMNS column 'Weight' references table 'Products' not present in input table expression",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
                "Products": {"Weight": "Weight", "ProductKey": "ProductKey"},
            },
        )


def test_translate_query_evaluate_keepcolumns_rejects_wrong_qualified_table_reference():
    query = _parse_query(
        """
        EVALUATE
            KEEPCOLUMNS('Sales', 'Products'[Amount])
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="KEEPCOLUMNS column 'Amount' references table 'Products' not present in input table expression",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
                "Products": {"Amount": "Amount", "Weight": "Weight"},
            },
        )


def test_translate_query_evaluate_keepcolumns_rejects_unprojected_related_column_in_addcolumns_input():
    query = _parse_query(
        """
        EVALUATE
            KEEPCOLUMNS(
                ADDCOLUMNS('Sales', "W", 'Products'[Weight]),
                'Products'[Weight]
            )
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="KEEPCOLUMNS column 'Weight' is not present in input table expression",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
                "Products": {"Weight": "Weight", "ProductKey": "ProductKey"},
            },
            relationship_edges=[
                RelationshipEdge(
                    from_table="Sales",
                    from_column="ProductKey",
                    to_table="Products",
                    to_column="ProductKey",
                )
            ],
        )


def test_translate_query_evaluate_keepcolumns_rejects_unprojected_related_column_in_wrapped_addcolumns_input():
    query = _parse_query(
        """
        EVALUATE
            KEEPCOLUMNS(
                ADDCOLUMNS(
                    FILTER('Sales', 'Sales'[Amount] > 0),
                    "W", 'Products'[Weight]
                ),
                'Products'[Weight]
            )
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="KEEPCOLUMNS column 'Weight' is not present in input table expression",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
                "Products": {"Weight": "Weight", "ProductKey": "ProductKey"},
            },
            relationship_edges=[
                RelationshipEdge(
                    from_table="Sales",
                    from_column="ProductKey",
                    to_table="Products",
                    to_column="ProductKey",
                )
            ],
        )


def test_translate_query_evaluate_keepcolumns_rejects_ambiguous_duplicate_input_column():
    query = _parse_query(
        """
        EVALUATE
            KEEPCOLUMNS(CROSSJOIN('Sales', 'Products'), "ProductKey")
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="KEEPCOLUMNS column 'ProductKey' is ambiguous in input table expression",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
                "Products": {"Weight": "Weight", "ProductKey": "ProductKey"},
            },
        )


def test_translate_query_evaluate_keepcolumns_accepts_qualified_unique_column_from_multitable_input():
    query = _parse_query(
        """
        EVALUATE
            KEEPCOLUMNS(CROSSJOIN('Sales', 'Products'), 'Products'[Weight])
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={
            "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
            "Products": {"Weight": "Weight", "ProductKey": "ProductKey"},
        },
    )

    sql = translation.evaluates[0].sql
    assert "SELECT t.Weight FROM (" in sql


def test_translate_query_evaluate_removecolumns_wrapped_table_with_named_column():
    query = _parse_query(
        """
        EVALUATE
            REMOVECOLUMNS(
                SELECTCOLUMNS('Sales', "Amt", 'Sales'[Amount], "Qty", 'Sales'[Quantity]),
                "Qty"
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount", "Quantity": "Quantity"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Amount AS Amt, Quantity AS Qty FROM Sales" in sql
    assert "SELECT * EXCLUDE (Qty) FROM (" in sql


def test_translate_query_evaluate_calculatetable_removecolumns_preserves_underlying_filters():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE(
                'Sales',
                REMOVECOLUMNS(FILTER('Sales', 'Sales'[Amount] > 100), 'Sales'[Amount])
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount", "Quantity": "Quantity"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT * FROM Sales WHERE (Sales.Amount > 100)" in sql


def test_translate_query_evaluate_removecolumns_requires_column_arg():
    query = _parse_query(
        """
        EVALUATE
            REMOVECOLUMNS('Sales')
        """
    )

    with pytest.raises(
        DaxTranslationError, match="REMOVECOLUMNS requires a table expression and at least one column argument"
    ):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Amount": "Amount"}},
        )


def test_translate_query_evaluate_removecolumns_rejects_missing_input_column():
    query = _parse_query(
        """
        EVALUATE
            REMOVECOLUMNS('Sales', 'Products'[Weight])
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="REMOVECOLUMNS column 'Weight' references table 'Products' not present in input table expression",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
                "Products": {"Weight": "Weight", "ProductKey": "ProductKey"},
            },
        )


def test_translate_query_evaluate_removecolumns_rejects_wrong_qualified_table_reference():
    query = _parse_query(
        """
        EVALUATE
            REMOVECOLUMNS('Sales', 'Products'[Amount])
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="REMOVECOLUMNS column 'Amount' references table 'Products' not present in input table expression",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
                "Products": {"Amount": "Amount", "Weight": "Weight"},
            },
        )


def test_translate_query_evaluate_removecolumns_rejects_ambiguous_duplicate_input_column():
    query = _parse_query(
        """
        EVALUATE
            REMOVECOLUMNS(CROSSJOIN('Sales', 'Products'), "ProductKey")
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="REMOVECOLUMNS column 'ProductKey' is ambiguous in input table expression",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
                "Products": {"Weight": "Weight", "ProductKey": "ProductKey"},
            },
        )


def test_translate_query_evaluate_renamecolumns_table():
    query = _parse_query(
        """
        EVALUATE
            RENAMECOLUMNS('Sales', 'Sales'[Amount], "Amt")
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount", "Quantity": "Quantity"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT * RENAME (Amount AS Amt) FROM (SELECT * FROM Sales) AS t" in sql


def test_translate_query_evaluate_renamecolumns_wrapped_table():
    query = _parse_query(
        """
        EVALUATE
            RENAMECOLUMNS(
                SELECTCOLUMNS('Sales', "Amt", 'Sales'[Amount], "Qty", 'Sales'[Quantity]),
                "Qty", "QuantityRenamed"
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount", "Quantity": "Quantity"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT Amount AS Amt, Quantity AS Qty FROM Sales" in sql
    assert "SELECT * RENAME (Qty AS QuantityRenamed) FROM (" in sql


def test_translate_query_evaluate_calculatetable_renamecolumns_preserves_underlying_filters():
    query = _parse_query(
        """
        EVALUATE
            CALCULATETABLE(
                'Sales',
                RENAMECOLUMNS(FILTER('Sales', 'Sales'[Amount] > 100), 'Sales'[Amount], "Amt")
            )
        """
    )

    translation = translate_dax_query(
        query,
        column_sql_by_table={"Sales": {"Amount": "Amount", "Quantity": "Quantity"}},
    )

    sql = translation.evaluates[0].sql
    assert "SELECT * FROM Sales WHERE (Sales.Amount > 100)" in sql


def test_translate_query_evaluate_renamecolumns_requires_old_new_pairs():
    query = _parse_query(
        """
        EVALUATE
            RENAMECOLUMNS('Sales', 'Sales'[Amount])
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="RENAMECOLUMNS requires a table expression and at least one old/new column pair",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Amount": "Amount"}},
        )


def test_translate_query_evaluate_renamecolumns_requires_even_old_new_pairs():
    query = _parse_query(
        """
        EVALUATE
            RENAMECOLUMNS('Sales', 'Sales'[Amount], "Amt", 'Sales'[Quantity])
        """
    )

    with pytest.raises(DaxTranslationError, match="RENAMECOLUMNS requires old/new column argument pairs"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Amount": "Amount", "Quantity": "Quantity"}},
        )


def test_translate_query_evaluate_renamecolumns_rejects_missing_input_source_column():
    query = _parse_query(
        """
        EVALUATE
            RENAMECOLUMNS('Sales', 'Products'[Weight], "WeightRenamed")
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="RENAMECOLUMNS column 'Weight' references table 'Products' not present in input table expression",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
                "Products": {"Weight": "Weight", "ProductKey": "ProductKey"},
            },
        )


def test_translate_query_evaluate_renamecolumns_rejects_wrong_qualified_source_table_reference():
    query = _parse_query(
        """
        EVALUATE
            RENAMECOLUMNS('Sales', 'Products'[Amount], "Amt")
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="RENAMECOLUMNS column 'Amount' references table 'Products' not present in input table expression",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
                "Products": {"Amount": "Amount", "Weight": "Weight"},
            },
        )


def test_translate_query_evaluate_renamecolumns_rejects_duplicate_source_columns():
    query = _parse_query(
        """
        EVALUATE
            RENAMECOLUMNS('Sales', 'Sales'[Amount], "Amt", 'Sales'[Amount], "AmountAgain")
        """
    )

    with pytest.raises(DaxTranslationError, match="RENAMECOLUMNS source columns must be unique"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Amount": "Amount", "Quantity": "Quantity"}},
        )


def test_translate_query_evaluate_renamecolumns_rejects_duplicate_target_columns():
    query = _parse_query(
        """
        EVALUATE
            RENAMECOLUMNS('Sales', 'Sales'[Amount], "Value", 'Sales'[Quantity], "Value")
        """
    )

    with pytest.raises(DaxTranslationError, match="RENAMECOLUMNS target column names must be unique"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Amount": "Amount", "Quantity": "Quantity"}},
        )


def test_translate_query_evaluate_renamecolumns_rejects_ambiguous_duplicate_input_source_column():
    query = _parse_query(
        """
        EVALUATE
            RENAMECOLUMNS(CROSSJOIN('Sales', 'Products'), "ProductKey", "KeyRenamed")
        """
    )

    with pytest.raises(
        DaxTranslationError,
        match="RENAMECOLUMNS source column 'ProductKey' is ambiguous in input table expression",
    ):
        translate_dax_query(
            query,
            column_sql_by_table={
                "Sales": {"Amount": "Amount", "ProductKey": "ProductKey"},
                "Products": {"Weight": "Weight", "ProductKey": "ProductKey"},
            },
        )


def test_translate_query_define_function_arity_error():
    query = _parse_query(
        """
        DEFINE
            FUNCTION f = (x : NUMERIC) => x + 1
        EVALUATE
            SELECTCOLUMNS('Sales', "x", f('Sales'[Amount], 2))
        """
    )

    with pytest.raises(DaxTranslationError, match="expects 1 args, got 2"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Amount": "Amount"}},
        )


def test_translate_query_define_function_cycle_error():
    query = _parse_query(
        """
        DEFINE
            FUNCTION loop = (x : NUMERIC) => loop(x)
        EVALUATE
            SELECTCOLUMNS('Sales', "x", loop('Sales'[Amount]))
        """
    )

    with pytest.raises(DaxTranslationError, match="Cyclic DEFINE FUNCTION reference"):
        translate_dax_query(
            query,
            column_sql_by_table={"Sales": {"Amount": "Amount"}},
        )
