# /// script
# dependencies = ["pytest"]
# ///
"""Check compiler explanations against real SQL compiled by the extension."""

import json

from test_catalog_host import execute, literal
from test_semantic_views_host import SETUP as VIEW_SETUP


def test_explain_matches_rewrite_and_reports_selected_join_population():
    sql = "select customers.country, orders.revenue from orders where customers.country = 'US'"
    rows = execute(f"select sqlctx_explain({literal(sql)}) as report, sqlctx_rewrite_sql({literal(sql)}) as sql")
    report = json.loads(rows[0][0]["report"])
    assert report["sql"] == rows[0][0]["sql"]
    assert any(event["kind"] == "grain" and event["dimensions"] == ["customers.country"] for event in report["events"])
    joins = [event for event in report["events"] if event["kind"] == "join"]
    assert [(event["from"], event["to"], event["join_type"]) for event in joins] == [
        ("orders", "customers", "INNER JOIN")
    ]
    assert "matching" in joins[0]["population_effect"]
    assert not any(event["kind"] == "fanout_guard" for event in report["events"])


def test_explain_fanout_guard_and_physical_passthrough():
    rows = execute("""
        metric customers.customer_count as count(*);
        select sqlctx_explain('select orders.status, customers.customer_count from customers') as report;
        select sqlctx_explain('select * from main.raw_orders') as report;
    """)
    reports = [json.loads(result[0]["report"]) for result in rows if "report" in result[0]]
    events = reports[0]["events"]
    guards = [event for event in events if event["kind"] == "fanout_guard"]
    assert any(event["model"] == "customers" and event["strategy"] == "count_distinct_entity" for event in guards)
    assert reports[1]["events"] == []


def test_explain_invalid_query_errors_instead_of_guessing():
    execute("select sqlctx_explain('select orders.missing from orders')", error="missing")


def test_explain_view_expands_filtered_ratio_and_aliases_like_rewrite():
    sql = "select country, aov, paid from sales"
    rows = execute(
        f"select sqlctx_explain({literal(sql)}) as report, sqlctx_rewrite_sql({literal(sql)}) as sql",
        setup=VIEW_SETUP.replace("numerator: revenue", "numerator: paid"),
    )
    report = json.loads(rows[0][0]["report"])
    assert report["sql"] == rows[0][0]["sql"]
    assert "NULLIF" in report["sql"] and "CASE WHEN" in report["sql"]
    assert report["events"]
