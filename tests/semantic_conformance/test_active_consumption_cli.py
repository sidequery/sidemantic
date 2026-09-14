"""Execute structured CLI consumption against independent result populations."""

import csv
import io
import json

import duckdb
import pytest
from typer.testing import CliRunner

import sidemantic.cli as cli_module
from sidemantic.cli import app


@pytest.fixture(params=["python", "rust"])
def project(request, tmp_path, monkeypatch):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="CLI acceptance requires the real Rust extension")
    monkeypatch.setattr(cli_module, "_loaded_config", None)
    monkeypatch.setattr(cli_module, "_project_context", None)
    models = tmp_path / "models.yml"
    models.write_text("""
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - {name: region, type: categorical}
      - {name: status, type: categorical}
    metrics:
      - {name: revenue, agg: sum, sql: amount}
    invariant_filters: [not deleted]
    security:
      row_filters: ['tenant = {{ user.tenant }}']
explores:
  - name: paid_regions
    model: orders
    default_metrics: [revenue]
    default_dimensions: [region]
    allowed_metrics: [revenue]
    allowed_dimensions: [region]
    allowed_filter_fields: [status]
    allowed_order_by: [revenue]
    filters: ["status = 'paid'"]
    default_order_by: [revenue DESC]
    default_limit: 2
    max_limit: 2
saved_queries:
  - name: top_region
    explore: paid_regions
    metrics: [revenue]
    dimensions: [region]
    order_by: [revenue DESC]
    limit: 1
table_calculations:
  - {name: doubled, type: formula, expression: '${revenue} * 2'}
  - {name: running, type: running_total, field: revenue}
""")
    database = tmp_path / "data.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute(
            "create table orders(id integer, region varchar, status varchar, amount integer, "
            "tenant varchar, deleted boolean)"
        )
        connection.execute("""
            insert into orders values
                (1,'west','paid',30,'a',false), (2,'east','paid',10,'a',false),
                (3,'north','paid',5,'a',false), (4,'west','paid',100,'a',true),
                (5,'east','paid',1000,'b',false), (6,'west','pending',200,'a',false)
        """)
    attributes = tmp_path / "attributes.json"
    attributes.write_text(json.dumps({"tenant": "a"}))
    options = ["--models", str(models), "--db", str(database), "--engine", request.param]
    if request.param == "rust":
        options.append("--no-fallback")
    return options, attributes, database


def invoke(project, arguments, *, authenticated=True):
    options, attributes, _ = project
    if authenticated:
        options = [*options, "--user-attrs-file", str(attributes)]
    return CliRunner().invoke(app, ["query", *arguments, *options])


def csv_rows(result):
    assert result.exit_code == 0, result.output
    return list(csv.DictReader(io.StringIO(result.stdout)))


def test_explore_defaults_and_saved_query_use_real_populations(project):
    assert csv_rows(invoke(project, ["--explore", "paid_regions"])) == [
        {"region": "west", "revenue": "30"},
        {"region": "east", "revenue": "10"},
    ]
    assert csv_rows(invoke(project, ["--saved-query", "top_region"])) == [{"region": "west", "revenue": "30"}]


def test_structured_selection_filter_order_and_pagination(project):
    assert csv_rows(
        invoke(
            project,
            [
                "--metric",
                "orders.revenue",
                "--dimension",
                "orders.region",
                "--filter",
                "orders.status = 'paid'",
                "--order-by",
                "orders.revenue DESC",
                "--limit",
                "1",
                "--offset",
                "1",
            ],
        )
    ) == [{"region": "east", "revenue": "10"}]


def test_explore_calculations_follow_selected_paginated_rows(project):
    arguments = [
        "--explore",
        "paid_regions",
        "--limit",
        "1",
        "--offset",
        "1",
        "--table-calculation",
        "doubled",
        "--table-calculation",
        "running",
    ]
    assert csv_rows(invoke(project, arguments)) == [
        {"region": "east", "revenue": "10", "doubled": "20", "running": "10"}
    ]
    dry_run = invoke(project, [*arguments, "--dry-run"])
    assert dry_run.exit_code == 0, dry_run.output
    with duckdb.connect(str(project[2])) as connection:
        result = connection.execute(dry_run.stdout)
        assert [column[0] for column in result.description] == ["region", "revenue", "doubled", "running"]
        assert result.fetchall() == [("east", 10, 20, 10)]


def test_explore_rejects_disallowed_selection_and_preserves_security(project):
    for arguments, message in [
        (["--explore", "paid_regions", "--dimension", "orders.status"], "does not allow dimension"),
        (["--explore", "paid_regions", "--limit", "3"], "exceeds max_limit"),
        (["--saved-query", "top_region", "--metric", "orders.revenue"], "immutable"),
    ]:
        result = invoke(project, arguments)
        assert result.exit_code != 0
        assert message in result.output
    result = invoke(project, ["--explore", "paid_regions"], authenticated=False)
    assert result.exit_code != 0
    assert "user_attributes" in result.output or "user attributes" in result.output


@pytest.mark.parametrize(
    "arguments",
    [
        ["SELECT 1", "--explore", "paid_regions"],
        ["SELECT 1", "--metric", "orders.revenue"],
        ["SELECT 1", "--table-calculation", "running"],
        ["SELECT 1", "--limit", "1"],
    ],
)
def test_sql_and_structured_options_cannot_be_mixed(arguments):
    result = CliRunner().invoke(app, ["query", *arguments])
    assert result.exit_code == 2
    assert "SQL cannot be combined with structured query options" in result.output


@pytest.mark.parametrize("arguments", [[], ["--table-calculation", "running"], ["--limit", "1"]])
def test_query_requires_sql_or_a_structured_selection(arguments):
    result = CliRunner().invoke(app, ["query", *arguments])
    assert result.exit_code == 2
    assert "Provide SQL or select" in result.output
