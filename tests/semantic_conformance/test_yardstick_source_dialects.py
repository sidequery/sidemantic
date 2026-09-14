"""Yardstick extension syntax is bound before ordinary source SQL translation."""

import pytest
import sqlglot

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.rust_bridge import rewrite_semantic_input
from sidemantic.sql.query_rewriter import QueryRewriter


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Requires matching native extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="sales_v",
            table="dialect_sales",
            primary_key="year",
            dimensions=[Dimension(name="year", type="numeric"), Dimension(name="region", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            metadata={"yardstick": {}},
        )
    )
    layer.adapter.execute("create table dialect_sales(year integer, region varchar, amount integer)")
    layer.adapter.execute(
        "insert into dialect_sales values (2022,'US',100),(2022,'EU',50),(2023,'US',150),(2023,'EU',75)"
    )
    try:
        yield layer
    finally:
        layer.adapter.close()


def result(layer, source, sql):
    if layer.engine == "rust":
        executable = rewrite_semantic_input(layer.graph, sql, sql_dialect=source, output_dialect="duckdb")
    else:
        rewritten = QueryRewriter(layer.graph, dialect=source, use_rust_rewriter=False).rewrite(sql)
        executable = sqlglot.transpile(rewritten, read=source, write="duckdb")[0]
    return layer.adapter.execute(executable).fetchall()


@pytest.mark.parametrize("source,quote,conditional", [("bigquery", "`", "IF"), ("snowflake", '"', "IFF")])
def test_source_functions_and_quoted_all_dimensions(layer, source, quote, conditional):
    q = quote
    assert result(
        layer,
        source,
        f"SEMANTIC SELECT {q}year{q}, {conditional}(1 = 1, AGGREGATE({q}revenue{q}) AT (ALL {q}region{q}), 0) "
        f"AS total FROM {q}sales_v{q} GROUP BY {q}year{q} ORDER BY {q}year{q}",
    ) == [(2022, 150), (2023, 225)]


@pytest.mark.parametrize("source,quote", [("bigquery", "`"), ("snowflake", '"')])
def test_source_current_modifier_uses_canonical_group_context(layer, source, quote):
    q = quote
    assert result(
        layer,
        source,
        f"SELECT {q}year{q}, AGGREGATE({q}revenue{q}) AT (SET {q}year{q} = CURRENT {q}year{q} - 1) AS prior "
        f"FROM {q}sales_v{q} GROUP BY {q}year{q} ORDER BY {q}year{q}",
    ) == [(2022, None), (2023, 150)]


@pytest.mark.parametrize(
    "source,predicate",
    [
        ("bigquery", "DATE_DIFF(DATE '2023-01-02', DATE '2023-01-01', DAY) = 1 AND `year` = 2023"),
        ("snowflake", "DATEDIFF(day, DATE '2023-01-01', DATE '2023-01-02') = 1 AND \"year\" = 2023"),
    ],
)
def test_source_date_functions_inside_where_modifier(layer, source, predicate):
    assert result(layer, source, f"SELECT AGGREGATE(revenue) AT (WHERE {predicate}) FROM sales_v") == [(225,)]


@pytest.mark.parametrize(
    "source,literal,value",
    [
        ("bigquery", r"r'C:\user'", r"C:\user"),
        ("snowflake", r"'C:\\user'", r"C:\user"),
        ("snowflake", r"'can\'t'", "can't"),
    ],
)
def test_source_modifier_literals_and_unicode_offsets(layer, source, literal, value):
    layer.adapter.executemany("insert into dialect_sales values (2024, ?, 17)", [(value,)])
    assert result(
        layer,
        source,
        f"SELECT 'é' AS label, AGGREGATE(revenue) AT (WHERE region = {literal}) AS total FROM sales_v",
    ) == [("é", 17)]


@pytest.mark.parametrize("source,quote", [("bigquery", "`"), ("snowflake", '"')])
def test_table_function_inner_sql_keeps_authored_dialect(layer, source, quote):
    q = quote
    inner = f"SELECT AGGREGATE({q}revenue{q}) AT (ALL) AS total FROM {q}sales_v{q}"
    assert result(layer, source, f"SELECT total FROM yardstick('{inner}')") == [(375,)]


@pytest.mark.parametrize("source", ["bigquery", "snowflake"])
def test_public_rewriter_preserves_multi_statement_strictness(layer, source):
    sql = "SELECT 1; SELECT 2"
    rewriter = QueryRewriter(
        layer.graph,
        dialect=source,
        use_rust_rewriter=layer.engine == "rust",
        rust_no_fallback=True,
    )
    with pytest.raises(ValueError, match="Multiple statements"):
        rewriter.rewrite(sql)
    assert rewriter.rewrite(sql, strict=False) == sql


@pytest.mark.parametrize("source,quote", [("bigquery", "`"), ("snowflake", '"')])
def test_native_ordinary_statement_batches_retain_both_results(layer, source, quote):
    if layer.engine != "rust":
        pytest.skip("Existing native direct-call batch contract; public Python rejects strict batches")
    # Ordinary semantic models, not Yardstick's single-statement extension.
    layer.graph.models["sales_v"].metadata = None
    q = quote
    sql = f"SELECT {q}sales_v{q}.{q}revenue{q} FROM metrics; SELECT 'a;b' AS label"
    rewritten = rewrite_semantic_input(layer.graph, sql, sql_dialect=source, output_dialect="duckdb")
    statements = sqlglot.parse(rewritten, read="duckdb")
    assert len(statements) == 2
    assert layer.adapter.execute(statements[0].sql(dialect="duckdb")).fetchall() == [(375,)]
    assert layer.adapter.execute(statements[1].sql(dialect="duckdb")).fetchall() == [("a;b",)]
