"""Filtered counts remain valid on supported older SQLGlot generators."""

import duckdb
import pytest
import sqlglot
from sqlglot import exp

from sidemantic.interchange.ossie.portable import lower_ossie_sql
from sidemantic.sql.rendering import render_ossie_sql


@pytest.mark.parametrize("target", ["snowflake", "bigquery", "duckdb"])
@pytest.mark.parametrize("population", ["TRUE", "amount < 0", "FALSE"])
@pytest.mark.parametrize("argument,matched", [("*", 2), ("DISTINCT amount", 1), ("DISTINCT NULL", 0)])
@pytest.mark.parametrize("window", [False, True])
def test_filtered_counts_preserve_populations_and_distinct(target, population, argument, matched, window):
    source = f"COUNT({argument}) FILTER (WHERE amount > 5)"
    if window:
        # Ordinary Layer 2 window expressions do not pass through portable
        # declaration lowering, so exercise its final emitter independently.
        tree = sqlglot.parse_one(f"{source} OVER ()", read="duckdb")
        original = tree.dump()
        compiled = render_ossie_sql(tree, dialect=target)
        assert tree.dump() == original
    else:
        compiled = lower_ossie_sql(source, target)
    tree = sqlglot.parse_one(compiled, read=target)
    for conditional in tree.find_all(exp.If, exp.Case):
        assert not list(conditional.find_all(exp.Star, exp.Distinct)), compiled
    if argument.startswith("DISTINCT"):
        assert tree.find(exp.Distinct) is not None
    if target in {"snowflake", "bigquery"}:
        assert tree.find(exp.Filter) is None
    expression = tree.sql(dialect="duckdb")
    expected = matched if population == "TRUE" else 0
    if window:
        rows = 4 if population == "TRUE" else 1 if population == "amount < 0" else 0
    else:
        rows = 1
    with duckdb.connect() as connection:
        assert (
            connection.execute(
                f"SELECT {expression} FROM (VALUES (11),(11),(NULL),(-1)) AS data(amount) WHERE {population}"
            ).fetchall()
            == [(expected,)] * rows
        )


def test_count_normalization_does_not_rewrite_other_aggregates_or_qualified_stars():
    for source in ("ARRAY_AGG(amount) FILTER (WHERE amount > 5)", "COUNT(data.*) FILTER (WHERE amount > 5)"):
        tree = sqlglot.parse_one(source, read="duckdb")
        assert render_ossie_sql(tree, dialect="duckdb") == tree.sql(dialect="duckdb")


@pytest.mark.parametrize("route", ["python", "public_rust", "native_rust"])
@pytest.mark.parametrize("target", ["snowflake", "bigquery"])
@pytest.mark.parametrize("argument,expected", [("*", 2), ("DISTINCT amount", 1), ("DISTINCT NULL", 0)])
def test_layer2_filtered_count_window_reaches_target_emitter(route, target, argument, expected):
    import json

    from sidemantic.interchange.ossie import OssieParseOptions, lower_ossie_document, parse_ossie_document
    from sidemantic.sql.query_rewriter import QueryRewriter
    from tests.interchange.ossie.test_layer2_conformance import LATEST
    from tests.rust_layer_adapter import _rust_request

    document = {
        "version": "0.2.0.dev0",
        "name": "sales",
        "datasets": [
            {
                "name": "orders",
                "source": "SELECT 11 AS amount UNION ALL SELECT 11 UNION ALL SELECT NULL UNION ALL SELECT -1",
                "fields": [{"name": "amount", "expression": {"sql": "amount"}}],
            }
        ],
    }
    query = f"SELECT COUNT({argument}) FILTER (WHERE amount > 5) OVER () AS n FROM sales.orders"
    if route == "native_rust":
        result = _rust_request(
            {
                "action": "ossie_compile",
                "content": json.dumps(document),
                "serialization": "json",
                "target": target.upper(),
                "dialect": target,
                "schema_revision": LATEST,
                "sql": query,
            }
        )
        assert result["status"] == "ok", result
        compiled = result["sql"]
    else:
        parsed = parse_ossie_document(
            json.dumps(document).encode(),
            options=OssieParseOptions(validate_schema=True, schema_revision=LATEST, target_dialect=target),
        )
        lowered = lower_ossie_document(parsed)
        assert lowered.valid and lowered.executable, lowered.diagnostics
        graph = next(iter(lowered.catalog.scopes.values())).graph
        compiled = QueryRewriter(
            graph, dialect=target, use_rust_rewriter=route == "public_rust", rust_no_fallback=True
        ).rewrite(query)
    tree = sqlglot.parse_one(compiled, read=target)
    for conditional in tree.find_all(exp.If, exp.Case):
        assert not list(conditional.find_all(exp.Star, exp.Distinct)), compiled
    # Nonempty window input avoids COUNTIF's dialect-specific empty behavior.
    with duckdb.connect() as connection:
        assert connection.execute(tree.sql(dialect="duckdb")).fetchall() == [(expected,)] * 4
