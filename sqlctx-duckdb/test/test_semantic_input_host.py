# /// script
# dependencies = ["pytest"]
# ///
"""Run SemanticInput through the built DuckDB SQL host, without ABI substitution."""

import copy
import json
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SOURCE = json.loads((ROOT / "sqlctx-rs/tests/fixtures/semantic_host.json").read_text())
CONTEXT = {"user_attributes": {"tenant": "a"}, "enforce_visibility": True}
QUERY = {"metrics": ["orders.revenue"], **CONTEXT}


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def execute(sql, error=False):
    binary = Path(os.environ["SQLCTX_DUCKDB_BINARY"]).resolve()
    extension = Path(os.environ["SQLCTX_DUCKDB_EXTENSION"]).resolve()
    assert binary.is_file(), f"Missing built DuckDB shell: {binary}"
    assert extension.is_file(), f"Missing built extension: {extension}"
    setup = f"""
        load {literal(str(extension))};
        create table orders(id integer, tenant varchar, amount integer, deleted boolean);
        insert into orders values (1, 'a', 10, false), (2, 'a', 50, true), (3, 'b', 100, false);
    """
    result = subprocess.run(
        [str(binary), "-unsigned", "-json", "-bail", ":memory:"],
        input=setup + sql,
        text=True,
        capture_output=True,
    )
    if error:
        assert result.returncode != 0, result.stdout
        assert "INTERNAL Error" not in result.stderr, result.stderr
        assert '"exception_type":"Internal"' not in result.stderr, result.stderr
        assert "SemanticInput" in result.stderr, result.stderr
        return result.stderr
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def call(query=None, source=None, sql=None, context=None, error=False):
    arguments = [literal(json.dumps(SOURCE if source is None else source))]
    if sql is None:
        function = "sqlctx_compile_semantic_input"
        arguments.append(literal(json.dumps(QUERY if query is None else query)))
    else:
        function = "sqlctx_rewrite_semantic_input"
        arguments.extend([literal(sql), literal(json.dumps(CONTEXT if context is None else context))])
    result = execute(f"select {function}({', '.join(arguments)}) as sql;", error=error)
    return result if error else result[0]["sql"]


def relation(query=None, source=None):
    input_json = literal(json.dumps(SOURCE if source is None else source))
    query_json = literal(json.dumps(QUERY if query is None else query))
    return f"sqlctx_query_semantic_input({input_json}, {query_json})"


@pytest.mark.parametrize("expression", [False, True])
def test_graph_window_identity_shadows_same_named_local_aggregate(expression):
    source = {
        "version": 1,
        "input_dialect": "duckdb",
        "models": [
            {
                "name": "events",
                "sql": """
                    select * from (values
                        (1, date '2024-01-01', 10, 900), (2, date '2024-01-02', 20, 800)
                    ) events(id, day, amount, shadow)
                """,
                "primary_key": "id",
                "dimensions": [{"name": "day", "type": "time", "granularity": "day"}],
                "metrics": [
                    {"name": "total", "agg": "sum", "sql": "amount"},
                    {"name": "running", "agg": "sum", "sql": "shadow"},
                ],
            }
        ],
        "metrics": [
            {
                "name": "events.running",
                "type": "cumulative",
                **({"window_expression": "SUM(base.total)"} if expression else {"sql": "total"}),
            }
        ],
        "metric_owners": {"events.running": "events"},
    }
    compiled = call(
        source=source,
        query={"metrics": ["events.running"], "dimensions": ["events.day"], "order_by": ["events.day"]},
    )
    assert execute(compiled) == [
        {"day": "2024-01-01", "total": "10", "events.running": "10"},
        {"day": "2024-01-02", "total": "20", "events.running": "30"},
    ]


def test_computed_time_identity_stays_unbucketed_for_join_and_fanout():
    source = {
        "version": 1,
        "input_dialect": "duckdb",
        "models": [
            {
                "name": "events",
                "sql": """
                    select * from (values
                        ('2026-01-01 01:00:00', 3), ('2026-01-01 02:00:00', 4),
                        ('2026-01-02 01:00:00', 8)
                    ) events(raw_time, amount)
                """,
                "primary_key": "event_time",
                "dimensions": [
                    {
                        "name": "event_time",
                        "type": "time",
                        "granularity": "day",
                        "sql": "cast(raw_time as timestamp)",
                    }
                ],
                "metrics": [
                    {"name": "amount", "agg": "sum", "sql": "amount"},
                    {"name": "people", "agg": "count_distinct"},
                ],
            },
            {
                "name": "logs",
                "sql": """
                    select * from (values
                        (1, '2026-01-01 01:00:00', 'x'), (2, '2026-01-01 01:00:00', 'x'),
                        (3, '2026-01-01 02:00:00', 'x'), (4, '2026-01-02 01:00:00', 'y')
                    ) logs(id, raw_time, category)
                """,
                "primary_key": "id",
                "dimensions": [
                    {
                        "name": "event_ref",
                        "type": "time",
                        "granularity": "day",
                        "sql": "cast(raw_time as timestamp)",
                    },
                    {"name": "category", "type": "categorical"},
                ],
                "relationships": [{"name": "events", "type": "many_to_one", "foreign_key": "event_ref"}],
            },
        ],
    }
    compiled = call(
        source=source,
        query={
            "metrics": ["events.amount", "events.people"],
            "dimensions": ["events.event_time__day", "logs.category"],
            "order_by": ["events.event_time__day", "logs.category"],
        },
    )
    assert execute(compiled) == [
        {"event_time__day": "2026-01-01", "category": "x", "amount": "7", "people": 2},
        {"event_time__day": "2026-01-02", "category": "y", "amount": "8", "people": 1},
    ]


@pytest.mark.parametrize("rewrite", [False, True])
@pytest.mark.parametrize("suffix", ["", " ASC NULLS LAST", " DESC NULLS FIRST"])
def test_fanout_null_ordering_precedes_pagination(rewrite, suffix):
    source = {
        "version": 1,
        "input_dialect": "duckdb",
        "models": [
            {
                "name": "facts",
                "sql": "select * from (values (1, 10), (2, 20), (3, 30)) facts(id, amount)",
                "primary_key": "id",
                "metrics": [{"name": "total", "agg": "sum", "sql": "amount"}],
                "relationships": [{"name": "items", "type": "one_to_many", "foreign_key": "fact_id"}],
            },
            {
                "name": "items",
                "sql": """
                    select * from (values (1, 1, NULL), (2, 1, NULL), (3, 2, 'a'), (4, 3, 'b'))
                    items(id, fact_id, label)
                """,
                "primary_key": "id",
                "dimensions": [{"name": "label", "type": "categorical"}],
            },
        ],
    }
    if rewrite:
        compiled = call(
            source=source,
            sql=f"select items.label, facts.total from metrics order by label{suffix} limit 1",
            context={},
        )
    else:
        compiled = call(
            source=source,
            query={
                "metrics": ["facts.total"],
                "dimensions": ["items.label"],
                "order_by": ["items.label" + suffix],
                "limit": 1,
            },
        )
    # Structured queries default to NULLS FIRST; semantic SQL keeps DuckDB's NULLS LAST.
    nulls_first = suffix.endswith("FIRST") or (not rewrite and not suffix)
    assert execute(compiled) == [{"label": None, "total": "10"} if nulls_first else {"label": "a", "total": "20"}]


@pytest.mark.parametrize("direct_relation", [False, True])
def test_retention_table_keeps_inactive_cohort_members_in_denominator(direct_relation):
    source = {
        "version": 1,
        "input_dialect": "duckdb",
        "models": [
            {
                "name": "events",
                "table": "retention_events",
                "primary_key": "id",
                "dimensions": [{"name": "event_date", "type": "time"}],
                "metrics": [
                    {
                        "name": "retention",
                        "type": "retention",
                        "entity": "user_id",
                        "cohort_event": "event_type = 'signup'",
                        "activity_event": "event_type = 'active'",
                        "periods": 7,
                        "retention_granularity": "day",
                    }
                ],
            }
        ],
    }
    query = {"metrics": ["events.retention"]}
    compiled = (
        f"select * from {relation(source=source, query=query)}" if direct_relation else call(source=source, query=query)
    )
    rows = execute(
        """
        create table retention_events(id integer, user_id varchar, event_type varchar, event_date date);
        insert into retention_events values
            (1, 'u1', 'signup', '2024-01-01'), (2, 'u2', 'signup', '2024-01-01'),
            (3, 'u3', 'signup', '2024-01-01'), (4, 'u1', 'active', '2024-01-02'),
            (5, 'u2', 'active', '2024-01-03');
        """
        + compiled
    )
    assert rows == [
        {
            "cohort_date": "2024-01-01",
            "days_since": day,
            "active_users": 1,
            "cohort_size": 3,
            "retention_pct": 33.3,
        }
        for day in (1, 2)
    ]


@pytest.mark.parametrize("rewrite", [False, True])
def test_population_and_injection_value(rewrite):
    for tenant, expected in [("a", 10), ("b", 100), ("a' OR 1=1 --", None)]:
        context = {"user_attributes": {"tenant": tenant}, "enforce_visibility": True}
        if rewrite:
            sql = call(sql="select orders.revenue as total from metrics", context=context)
        else:
            sql = call(query={"metrics": ["orders.revenue"], **context})
        rows = execute(sql)
        assert len(rows) == 1
        # DuckDB's shell encodes HUGEINT aggregate results as JSON strings.
        assert list(rows[0].values()) == [None if expected is None else str(expected)]


@pytest.mark.parametrize("mutation", ["version", "envelope", "field", "key", "scope", "capability", "policy"])
@pytest.mark.parametrize("rewrite", [False, True])
def test_invalid_contract_is_rejected(mutation, rewrite):
    source = copy.deepcopy(SOURCE)
    if mutation == "version":
        source["version"] = 999
    elif mutation == "envelope":
        source["unknown"] = True
    elif mutation == "field":
        source["models"][0]["metrics"][0]["unknown"] = True
    elif mutation == "key":
        source["models"][0]["primary_key"] = {"sql": "id"}
    elif mutation == "scope":
        source["metric_owners"] = {"ghost": "missing"}
    elif mutation == "capability":
        source["required_capabilities"] = ["future_capability"]
    else:
        source["models"][0]["security"]["unknown"] = True
    call(source=source, sql="select orders.revenue from metrics" if rewrite else None, error=True)


@pytest.mark.parametrize(
    "query",
    [
        {"metrics": ["orders.revenue"]},
        {"metrics": ["orders.secret"], **CONTEXT},
        {"metrics": ["orders.revenue"], "prepared_policies": {}},
    ],
)
def test_compile_denials(query):
    call(query=query, error=True)


def test_rewrite_denials():
    call(sql="select orders.revenue from metrics", context={}, error=True)
    call(sql="select orders.secret from metrics", error=True)
    call(sql="select orders.revenue from metrics", context={"prepared_policies": {}}, error=True)


def test_nulls_and_nul_bytes():
    source = literal(json.dumps(SOURCE))
    assert execute(f"select sqlctx_compile_semantic_input({source}, NULL) as sql;") == [{"sql": None}]
    assert execute(f"select sqlctx_rewrite_semantic_input({source}, NULL, '{{}}') as sql;") == [{"sql": None}]
    execute(f"select sqlctx_compile_semantic_input({source}, '{{}}' || chr(0));", error=True)
    execute(f"select sqlctx_rewrite_semantic_input({source}, 'select 1' || chr(0), '{{}}');", error=True)


def test_vectorized_callers_are_isolated():
    source = literal(json.dumps(SOURCE))
    query_a = literal(json.dumps(QUERY))
    query_b = literal(json.dumps({**QUERY, "user_attributes": {"tenant": "b"}}))
    compiled = execute(
        f"select sqlctx_compile_semantic_input({source}, query) as sql "
        f"from (values (1, {query_a}), (2, {query_b}), (3, {query_a})) requests(id, query) order by id;"
    )
    assert [list(execute(row["sql"])[0].values()) for row in compiled] == [["10"], ["100"], ["10"]]


def test_relation_dynamic_schema_and_composition():
    selected = relation(query={**QUERY, "dimensions": ["orders.tenant"]})
    columns = execute(f"describe select * from {selected};")
    assert [(column["column_name"], column["column_type"]) for column in columns] == [
        ("tenant", "VARCHAR"),
        ("revenue", "HUGEINT"),
    ]
    assert execute(
        f"with totals as (select * from {selected}) "
        "select label, revenue from totals join (values ('a', 'Tenant A')) names(tenant, label) "
        "using (tenant) where revenue > 5;"
    ) == [{"label": "Tenant A", "revenue": "10"}]


@pytest.mark.parametrize("rollback", [False, True])
def test_relation_observes_callers_transaction(rollback):
    transaction = "begin; insert into orders values (4, 'a', 20, false);"
    if rollback:
        transaction += "rollback;"
    assert execute(transaction + f"select * from {relation()};") == [{"revenue": "10" if rollback else "30"}]


def test_relation_observes_temporary_tables():
    source = copy.deepcopy(SOURCE)
    source["models"][0]["table"] = "temporary_orders"
    assert execute(
        "create temp table temporary_orders as select * from orders;"
        "insert into temporary_orders values (4, 'a', 20, false);"
        f"select * from {relation(source=source)};"
    ) == [{"revenue": "30"}]


def test_relation_rebinds_prepared_input_and_caller_parameters():
    source = literal(json.dumps(SOURCE))
    maximum = copy.deepcopy(SOURCE)
    maximum["models"][0]["metrics"][0]["agg"] = "max"
    maximum = literal(json.dumps(maximum))
    query_a = literal(json.dumps(QUERY))
    query_b = literal(json.dumps({**QUERY, "user_attributes": {"tenant": "b"}}))
    assert execute(
        "create temp table results(caller integer, revenue hugeint);"
        "prepare semantic_query as insert into results "
        "select $3, revenue from sqlctx_query_semantic_input($1, $2);"
        f"execute semantic_query({source}, {query_a}, 1);"
        f"execute semantic_query({source}, {query_b}, 2);"
        "insert into orders values (4, 'a', 25, false);"
        f"execute semantic_query({maximum}, {query_a}, 3);"
        "select * from results order by caller;"
    ) == [{"caller": 1, "revenue": "10"}, {"caller": 2, "revenue": "100"}, {"caller": 3, "revenue": "25"}]


@pytest.mark.parametrize(
    "query",
    [{"metrics": ["orders.revenue"]}, {"metrics": ["orders.secret"], **CONTEXT}],
)
def test_relation_enforces_policies(query):
    execute(f"select * from {relation(query=query)};", error=True)


@pytest.mark.parametrize("argument", ["NULL", "'{}'", "'{}' || chr(0)"])
@pytest.mark.parametrize("position", [0, 1])
def test_relation_rejects_invalid_arguments(argument, position):
    arguments = [literal(json.dumps(SOURCE)), literal(json.dumps(QUERY))]
    arguments[position] = argument
    execute(f"select * from sqlctx_query_semantic_input({', '.join(arguments)});", error=True)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q", "-o", "addopts="]))
