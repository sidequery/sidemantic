"""Execute pinned Ossie queries on the selected database, with physical SQL oracles.

OSSIE_INTEGRATION_BACKEND is required to opt in. CI selects each backend explicitly:
DuckDB and PostgreSQL/ClickHouse servers are real databases; BigQuery and Snowflake
jobs exercise emulators, not live cloud services. Missing selected dependencies or
unreachable services are failures, never skips. Generated SQL is executed as emitted.
"""

from __future__ import annotations

import json
import os
import re
from contextlib import ExitStack
from functools import partial

import pytest
import sqlglot

from sidemantic import SemanticLayer
from sidemantic.interchange.ossie import OssieParseOptions, lower_ossie_document, parse_ossie_document
from tests.interchange.ossie.test_layer2_conformance import LATEST, ROOT, ROUTES, assert_rows, model
from tests.rust_layer_adapter import _rust_request

BACKEND = os.getenv("OSSIE_INTEGRATION_BACKEND")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not BACKEND, reason="Set OSSIE_INTEGRATION_BACKEND to opt in"),
]
TABLES = ("raw_customers", "raw_orders", "raw_tickets", "raw_visits", "raw_unrelated")


def physical_sql(sql):
    """Render physical oracles with standard NULL semantics and explicit namespaces."""
    if BACKEND == "clickhouse":
        # SQL-standard SUM over an empty/all-NULL population is NULL, unlike
        # ClickHouse's default sum. The oracle must express the Ossie contract.
        return re.sub(r"\bSUM\(", "sumOrNull(", sql, flags=re.IGNORECASE)
    if BACKEND != "bigquery":
        return sql
    dataset = os.getenv("BIGQUERY_DATASET", "test_dataset")
    return re.sub(r"\b(raw_customers|raw_orders|raw_tickets|raw_visits|raw_unrelated)\b", rf"{dataset}.\1", sql)


def document_for_database():
    document = model()
    for dataset in document["datasets"]:
        dataset["source"] = physical_sql(dataset["source"])
    return document


def reference_rows(adapter, sql):
    sql = physical_sql(sql)
    if BACKEND == "clickhouse":
        # The oracle follows SQL-standard outer-join NULLs too. In particular,
        # COUNT(right.id) must not count ClickHouse's default unmatched zero.
        sql += " SETTINGS join_use_nulls=1"
    return adapter.execute(sql).fetchall()


@pytest.fixture(scope="module")
def database(tmp_path_factory):
    assert BACKEND in {"duckdb", "postgres", "bigquery", "snowflake", "clickhouse"}, BACKEND
    with ExitStack() as stack:
        if BACKEND == "duckdb":
            url = f"duckdb:///{tmp_path_factory.mktemp('ossie') / 'integration.duckdb'}"
        elif BACKEND == "postgres":
            url = os.getenv("POSTGRES_URL", "postgres://test:test@localhost:5433/sidemantic_test")
        elif BACKEND == "bigquery":
            assert os.getenv("BIGQUERY_EMULATOR_HOST"), "Set BIGQUERY_EMULATOR_HOST for the emulator integration suite"
            url = f"bigquery://{os.getenv('BIGQUERY_PROJECT', 'test-project')}/{os.getenv('BIGQUERY_DATASET', 'test_dataset')}"
        elif BACKEND == "snowflake":
            import fakesnow

            stack.enter_context(fakesnow.patch())
            url = "snowflake://test:test@test/testdb/public?warehouse=test_warehouse"
        else:
            host, port = os.getenv("CLICKHOUSE_HOST", "localhost"), os.getenv("CLICKHOUSE_PORT", "8123")
            url = f"clickhouse://default:{os.getenv('CLICKHOUSE_PASSWORD', 'clickhouse')}@{host}:{port}/default"
        owner = SemanticLayer(connection=url, auto_register=False)
        adapter = owner.adapter
        stack.callback(adapter.close)
        if BACKEND == "bigquery":
            # The emulator reports syntax errors as jobInternalError. Disable
            # automatic job resubmission so invalid generated SQL fails promptly.
            adapter.client.query = partial(adapter.client.query, job_retry=None)
        for table in TABLES:
            adapter.execute(physical_sql(f"DROP TABLE IF EXISTS {table}"))
        for statement in sqlglot.parse((ROOT / "setup.sql").read_text(), read="duckdb"):
            # Only fixture DDL is adapted. The compiler's SQL below is never transpiled.
            sql = statement.sql(dialect=BACKEND)
            if BACKEND == "clickhouse" and isinstance(statement, sqlglot.exp.Create):
                sql += " ENGINE = Memory"
            if BACKEND == "clickhouse":
                adapter.client.command(sql)
            else:
                adapter.execute(physical_sql(sql))
        yield adapter, url
        for table in TABLES:
            adapter.execute(physical_sql(f"DROP TABLE IF EXISTS {table}"))


def layer_for(route, adapter, document):
    parsed = parse_ossie_document(
        json.dumps(document).encode(),
        options=OssieParseOptions(validate_schema=True, target_dialect=BACKEND, schema_revision=LATEST),
    )
    lowered = lower_ossie_document(parsed)
    assert lowered.valid, lowered.diagnostics
    return SemanticLayer.from_catalog(
        lowered.catalog,
        connection=adapter,
        engine="rust" if route == "public_rust" else "python",
        fallback=False,
        auto_register=False,
    )


def execute(route, adapter, query, *, wide=False, document=None, metadata=False):
    document = document_for_database() if document is None else document
    if route == "native_rust":
        response = _rust_request(
            {
                "action": "ossie_wide_compile" if wide else "ossie_compile",
                "content": json.dumps(document),
                "serialization": "json",
                "target": BACKEND.upper(),
                "dialect": BACKEND,
                "schema_revision": LATEST,
                "query" if wide else "sql": query,
            }
        )
        assert response["status"] == "ok", response
        result = adapter.execute(response["sql"])
        rows = result.fetchall()
        return (rows, [column[0] for column in result.description]) if metadata else rows
    layer = layer_for(route, adapter, document)
    result = layer.query_ossie_wide(query) if wide else layer.sql(query)
    rows = result.fetchall()
    assert layer.last_engine_selection == {"engine": "rust" if route == "public_rust" else "python", "reason": None}
    return (rows, [column[0] for column in result.description]) if metadata else rows


LAYER2 = [
    (
        "grouping_sets_filter_fanout",
        "SELECT c.region, AGG(o.revenue) FILTER(WHERE o.status='paid'), AGG(o.order_count) "
        "FROM commerce.customers c LEFT JOIN commerce.orders o ON c.id=o.customer_id "
        "LEFT JOIN commerce.tickets t ON c.id=t.customer_id GROUP BY GROUPING SETS((c.region),())",
        "SELECT c.region,SUM(CASE WHEN o.status='paid' THEN o.amount END),COUNT(o.id) "
        "FROM raw_customers c LEFT JOIN raw_orders o ON c.id=o.customer_id GROUP BY GROUPING SETS((c.region),())",
    ),
    (
        "grouped_named_qualify",
        "SELECT status, AGG(revenue) AS total FROM commerce.orders GROUP BY status "
        "WINDOW ranked AS (ORDER BY AGG(revenue) DESC) QUALIFY ROW_NUMBER() OVER ranked=1",
        "SELECT status,total FROM (SELECT status,SUM(amount) AS total,"
        "ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) AS rank FROM raw_orders GROUP BY status) ranked WHERE rank=1",
    ),
    (
        "quoted_wildcard_rename",
        'SELECT AGG("sales.total") FROM (SELECT * RENAME (revenue AS "sales.total") FROM commerce.orders) renamed',
        "SELECT SUM(amount) FROM raw_orders",
    ),
    (
        "grand_total",
        "SELECT AGG(revenue), AGG(order_count) FROM commerce.orders",
        "SELECT SUM(amount), COUNT(*) FROM raw_orders",
    ),
    (
        "join_filter_fanout",
        "SELECT AGG(o.revenue), AGG(o.order_count) FROM commerce.orders o "
        "JOIN commerce.tickets t ON o.customer_id=t.customer_id WHERE t.cost>2 AND o.status='paid'",
        "SELECT SUM(o.amount),COUNT(*) FROM raw_orders o WHERE o.status='paid' "
        "AND EXISTS (SELECT 1 FROM raw_tickets t WHERE t.customer_id=o.customer_id AND t.cost>2)",
    ),
    (
        "grouped_join",
        "SELECT c.region, AGG(o.revenue) FROM commerce.orders o "
        "LEFT JOIN commerce.customers c ON o.customer_id=c.id GROUP BY c.region",
        "SELECT c.region,SUM(o.amount) FROM raw_orders o LEFT JOIN raw_customers c ON o.customer_id=c.id GROUP BY c.region",
    ),
    (
        "derived_field_binding",
        "SELECT AGG(doubled_total) FROM commerce.orders WHERE status='paid'",
        "SELECT SUM(amount*2) FROM raw_orders WHERE status='paid'",
    ),
    (
        "empty_aggregate",
        "SELECT AGG(revenue),AGG(order_count) FROM commerce.orders WHERE 1=0",
        "SELECT SUM(amount),COUNT(*) FROM raw_orders WHERE 1=0",
    ),
]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("name,query,reference", LAYER2, ids=[case[0] for case in LAYER2])
def test_layer2_on_database(database, route, name, query, reference):
    adapter, _ = database
    expected = reference_rows(adapter, reference)
    if BACKEND == "bigquery" and name == "quoted_wildcard_rename":
        # BigQuery uses backticks and treats a dot inside them as a path;
        # use a space to exercise one quoted identifier in that dialect.
        query = query.replace('"sales.total"', "`sales total`")
    assert_rows(execute(route, adapter, query), expected)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("filtered", [False, True], ids=["all_populations", "filter_each_population"])
def test_layer3_independent_populations_on_database(database, route, filtered):
    adapter, _ = database
    predicate = "WHERE c.segment='SMB'" if filtered else ""
    # UNION ALL preserves each fact population. Ordinary SUM over that union is
    # an independent oracle; no semantic compiler or other route supplies results.
    reference = (
        "SELECT region,SUM(revenue),SUM(cost) FROM ("
        f"SELECT c.region,o.amount AS revenue,CAST(NULL AS BIGINT) AS cost FROM raw_orders o "
        f"LEFT JOIN raw_customers c ON o.customer_id=c.id {predicate} UNION ALL "
        f"SELECT c.region,CAST(NULL AS BIGINT) AS revenue,t.cost FROM raw_tickets t "
        f"LEFT JOIN raw_customers c ON t.customer_id=c.id {predicate}) populations GROUP BY region"
    )
    reference = sqlglot.transpile(reference, read="duckdb", write=BACKEND)[0]
    query = {"dimensions": ["customers.region"], "metrics": ["orders.revenue", "tickets.total_cost"]}
    if filtered:
        query["filter"] = "customers.segment='SMB'"
    assert_rows(execute(route, adapter, query, wide=True), reference_rows(adapter, reference))


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("empty", [False, True], ids=["disconnected_populations", "empty_disconnected_populations"])
def test_layer3_disconnected_populations_on_database(database, route, empty):
    adapter, _ = database
    query = {"metrics": ["orders.revenue", "orders.order_count", "unrelated.total"]}
    predicate = " WHERE 1=0" if empty else ""
    if empty:
        query["filter"] = "FALSE"
    reference = (
        f"SELECT a.revenue,a.n,b.total FROM (SELECT SUM(amount) AS revenue,COUNT(*) AS n FROM raw_orders{predicate}) a "
        f"CROSS JOIN (SELECT SUM(value) AS total FROM raw_unrelated{predicate}) b"
    )
    assert_rows(execute(route, adapter, query, wide=True), reference_rows(adapter, reference))


@pytest.mark.parametrize("route", ROUTES)
def test_source_query_binding_on_database(database, route):
    adapter, _ = database
    document = document_for_database()
    orders = next(dataset for dataset in document["datasets"] if dataset["name"] == "orders")
    orders["source"] = physical_sql(
        "SELECT id,customer_id,amount+5 AS amount,status FROM raw_orders WHERE status='paid'"
    )
    query = "SELECT AGG(doubled_total),AGG(order_count) FROM commerce.orders"
    reference = "SELECT SUM((amount+5)*2),COUNT(*) FROM raw_orders WHERE status='paid'"
    assert_rows(execute(route, adapter, query, document=document), reference_rows(adapter, reference))


@pytest.mark.parametrize("route", ROUTES)
def test_zero_row_metadata_on_database(database, route):
    adapter, _ = database
    rows, names = execute(route, adapter, "SELECT * FROM commerce.orders LIMIT 0", metadata=True)
    orders = next(dataset for dataset in model()["datasets"] if dataset["name"] == "orders")
    assert rows == []
    assert [name.lower() for name in names] == [item["name"] for item in [*orders["fields"], *orders["metrics"]]]


@pytest.mark.parametrize("route", ["python", "public_rust"])
@pytest.mark.skipif(
    BACKEND == "bigquery",
    reason="Semantic view lifecycle coverage targets DuckDB, PostgreSQL, Snowflake and ClickHouse",
)
def test_semantic_view_on_database(database, route):
    adapter, _ = database
    layer = layer_for(route, adapter, document_for_database())
    adapter.execute("DROP VIEW IF EXISTS ossie_paid_orders")
    try:
        layer.sql("CREATE VIEW ossie_paid_orders AS SELECT revenue AS paid FROM commerce.orders WHERE status='paid'")
        view = "OSSIE_PAID_ORDERS" if BACKEND == "postgres" else "ossie_paid_orders"
        actual = layer.sql(f"SELECT AGG(paid) FROM {view}").fetchall()
        assert_rows(
            actual, adapter.execute(physical_sql("SELECT SUM(amount) FROM raw_orders WHERE status='paid'")).fetchall()
        )
    finally:
        layer.sql("DROP VIEW IF EXISTS ossie_paid_orders")


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.skipif(BACKEND not in {"duckdb", "postgres"}, reason="CLI smoke targets the two required core databases")
def test_ossie_cli_on_core_database(database, engine, tmp_path):
    from typer.testing import CliRunner

    from sidemantic.cli import app

    adapter, url = database
    document = tmp_path / "commerce.ossie.json"
    document.write_text(json.dumps(document_for_database()))
    output = tmp_path / "result.csv"
    result = CliRunner().invoke(
        app,
        [
            "query",
            "--models",
            str(document),
            "--engine",
            engine,
            "--no-fallback",
            "--connection",
            url,
            "--output",
            str(output),
            "--",
            "SELECT AGG(revenue),AGG(order_count) FROM commerce.orders",
        ],
    )
    assert result.exit_code == 0, result.output
    import csv
    from decimal import Decimal

    rows = list(csv.reader(output.read_text().splitlines()))
    assert_rows(
        [[Decimal(value) for value in rows[1]]],
        adapter.execute("SELECT SUM(amount),COUNT(*) FROM raw_orders").fetchall(),
    )


def _wide_boundary_document(component="COALESCE(SUM(amount),42)"):
    def dataset(name, source, fields, metric=None):
        definition = {
            "name": name,
            "source": source,
            "fields": [{"name": field, "expression": {"sql": field}} for field in fields],
        }
        if metric:
            definition["metrics"] = [{"name": "total", "expression": {"sql": metric}}]
        return definition

    document = {
        "version": "0.2.0.dev0",
        "name": "boundary",
        "datasets": [
            dataset("customers", "SELECT 1 AS id, 'EAST' AS region UNION ALL SELECT 2, 'WEST'", ["id", "region"]),
            dataset(
                "orders",
                "SELECT 1 AS customer_id, 5 AS amount UNION ALL SELECT 99, 11",
                ["customer_id", "amount"],
                component,
            ),
            dataset("returns", "SELECT 2 AS customer_id, 7 AS amount", ["customer_id", "amount"], "SUM(amount)"),
        ],
        "relationships": [
            {
                "name": name + "_customer",
                "from": name,
                "to": "customers",
                "from_columns": ["customer_id"],
                "to_columns": ["id"],
            }
            for name in ("orders", "returns")
        ],
    }
    document["datasets"][0]["primary_key"] = ["id"]
    return document


@pytest.mark.parametrize("route", ROUTES)
def test_wide_empty_aggregates_have_ossie_values_on_database(database, route):
    adapter, _ = database
    document = {
        "version": "0.2.0.dev0",
        "name": "empty_values",
        "datasets": [
            {
                "name": "orders",
                "source": "SELECT amount FROM (SELECT 1 AS amount) seed WHERE FALSE",
                "fields": [{"name": "amount", "expression": {"sql": "amount"}}],
                "metrics": [
                    {"name": "total", "expression": {"sql": "SUM(amount)"}},
                    {"name": "rows", "expression": {"sql": "COUNT(*)"}},
                ],
            }
        ],
    }
    # Native warehouse SUM(empty) can differ from the pinned Ossie contract.
    # Assert explicit normative values instead of using that same aggregate as oracle.
    assert execute(route, adapter, {"metrics": ["orders.total", "orders.rows"]}, wide=True, document=document) == [
        (None, 0)
    ]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "component,expected",
    [
        ("COALESCE(SUM(amount),42)", [(None, 11, None), ("EAST", 5, None), ("WEST", 42, 7)]),
        ("NULLIF(COUNT(*),1)", [(None, None, None), ("EAST", None, None), ("WEST", 0, 7)]),
    ],
    ids=["missing_branch_empty_value", "present_null_is_not_missing"],
)
def test_wide_nonnullable_joins_and_missing_populations_on_database(database, route, component, expected):
    adapter, _ = database
    query = {
        "dimensions": ["customers.region"],
        "metrics": [
            {"metric": "orders.total", "alias": "orders_total"},
            {"metric": "returns.total", "alias": "returns_total"},
        ],
    }
    # Literal source columns are nonnullable; an unmatched customer must still
    # produce a NULL group. A missing component takes its empty-input value,
    # whereas a present component whose expression is NULL must retain NULL.
    assert_rows(execute(route, adapter, query, wide=True, document=_wide_boundary_document(component)), expected)


@pytest.mark.parametrize("route", ["python", "public_rust"])
def test_wide_structured_ordering_keeps_backend_semantics_on_database(database, route):
    adapter, _ = database
    layer = layer_for(route, adapter, _wide_boundary_document())
    sql = layer.compile(
        metrics=["orders.total"],
        dimensions=["customers.region"],
        order_by=["orders.total DESC"],
        limit=1,
    )
    assert_rows(adapter.execute(sql).fetchall(), [(None, 11)])


def _grouped_measure_boundary_document():
    document = model()
    document["datasets"] = [d for d in document["datasets"] if d["name"] in {"customers", "orders", "tickets"}]
    document["relationships"] = []
    for dataset in document["datasets"]:
        if dataset["name"] == "customers":
            dataset["source"] = (
                "SELECT 1 AS id, NULL AS region, 'SMB' AS segment "
                "UNION ALL SELECT 2, 'E', 'SMB' UNION ALL SELECT 3, 'W', 'SMB'"
            )
        elif dataset["name"] == "orders":
            # The identical rows are separate source identities; ticket fanout
            # must preserve both, but must not multiply their contributions.
            dataset["source"] = (
                "SELECT 1 AS id, 1 AS customer_id, 10 AS amount, 'paid' AS status "
                "UNION ALL SELECT 1,1,10,'paid' UNION ALL SELECT 2,1,20,'open' "
                "UNION ALL SELECT 3,2,NULL,'paid'"
            )
            dataset["metrics"].append({"name": "present_null", "expression": {"sql": "NULLIF(COUNT(*),1)"}})
        else:
            dataset["source"] = (
                "SELECT 1 AS id, 1 AS customer_id, 1 AS cost "
                "UNION ALL SELECT 2,1,1 UNION ALL SELECT 3,2,1 UNION ALL SELECT 4,3,1"
            )
    return document


_GROUPED_MEASURE_SOURCE = (
    " FROM commerce.customers c LEFT JOIN commerce.orders o ON c.id=o.customer_id "
    "LEFT JOIN commerce.tickets t ON c.id=t.customer_id"
)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "query,expected",
    [
        (
            "SELECT c.region,GROUPING(c.region),AGG(o.revenue),AGG(o.order_count),"
            "AGG(o.coalesced_total),AGG(o.present_null),AGG(o.revenue) FILTER(WHERE o.status='paid'),COUNT(*)"
            + _GROUPED_MEASURE_SOURCE
            + " GROUP BY GROUPING SETS((c.region),())",
            [
                (None, 0, 40, 3, 40, 3, 20, 6),
                ("E", 0, None, 1, 0, None, None, 1),
                ("W", 0, None, 0, 0, 0, None, 1),
                (None, 1, 40, 4, 40, 4, 20, 8),
            ],
        ),
        (
            "SELECT region,GROUPING(region),AGG(combined),AGG(combined) FILTER(WHERE status='paid') "
            "FROM (SELECT c.region,o.status,TO_MEASURE(AGG(o.coalesced_total)+AGG(o.order_count)) AS combined"
            + _GROUPED_MEASURE_SOURCE
            + ") q GROUP BY GROUPING SETS((region),())",
            [(None, 0, 43, 22), ("E", 0, 1, 1), ("W", 0, 0, 0), (None, 1, 44, 23)],
        ),
    ],
    ids=["independent_measure_and_raw_populations", "filtered_derived_recipe"],
)
def test_grouped_measure_population_boundaries(database, route, query, expected):
    adapter, _ = database
    # Explicit expected values distinguish a true NULL key from the grand total,
    # missing populations from present NULL aggregates, and raw join multiplicity.
    assert_rows(execute(route, adapter, query, document=_grouped_measure_boundary_document()), expected)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("rows", [("1,5", "99,11", "1,5", "99,13"), ("99,13", "1,5", "99,11", "1,5")])
def test_wide_source_identity_stays_with_measure_inputs(database, route, rows):
    adapter, _ = database
    document = _wide_boundary_document()
    orders = next(dataset for dataset in document["datasets"] if dataset["name"] == "orders")
    first_customer, first_amount = rows[0].split(",")
    orders["source"] = f"SELECT {first_customer} AS customer_id, {first_amount} AS amount " + " ".join(
        f"UNION ALL SELECT {row}" for row in rows[1:]
    )
    orders["metrics"].extend(
        {"name": name, "expression": {"sql": sql}}
        for name, sql in [
            ("row_count", "COUNT(*)"),
            ("filtered", "SUM(amount) FILTER(WHERE amount > 5)"),
            ("combined", "SUM(amount) + COUNT(*)"),
        ]
    )
    query = {
        "dimensions": ["customers.region"],
        "metrics": [
            {"metric": "orders.total", "alias": "orders_total"},
            "orders.row_count",
            "orders.filtered",
            "orders.combined",
            {"metric": "returns.total", "alias": "returns_total"},
        ],
    }
    expected = [(None, 24, 2, 24, 26, None), ("EAST", 10, 2, None, 12, None), ("WEST", 42, 0, None, None, 7)]
    # Inlining a UNION source can change scan order between references. Values
    # must stay with their group while both identical physical rows still count.
    for _ in range(3):
        assert_rows(execute(route, adapter, query, wide=True, document=document), expected)
