"""End-to-end CLI tests for scaffolding, testing, and live validation.

These exercise ``init``, ``init --from``, ``demo``, ``test``, ``validate --live``,
and the ``serve``/``server api`` help surfaces through the real Typer app, the
way the project-contract suite drives commands.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

import sidemantic.cli as cli_module
from sidemantic.cli import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def _reset_cli_config():
    cli_module._loaded_config = None
    yield
    cli_module._loaded_config = None


# --- init (starter) -----------------------------------------------------------


def test_init_creates_starter_project(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init"])

    assert result.exit_code == 0, result.output
    assert (tmp_path / "sidemantic.yaml").is_file()
    assert (tmp_path / "models" / "orders.yml").is_file()
    assert (tmp_path / "tests" / "orders.yml").is_file()


def test_init_then_test_passes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["init"]).exit_code == 0

    cli_module._loaded_config = None
    result = runner.invoke(app, ["test"])

    assert result.exit_code == 0, result.output
    assert "passed" in result.output


def test_init_again_without_force_fails_with_overwrite_message(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["init"]).exit_code == 0

    cli_module._loaded_config = None
    result = runner.invoke(app, ["init"])

    assert result.exit_code != 0
    assert "overwrite" in result.output.lower()


def test_init_json_emits_parseable_payload(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert "root" in payload
    assert "created" in payload
    assert isinstance(payload["created"], list)


# --- init --from data files ---------------------------------------------------


@pytest.fixture
def csv_sources(tmp_path: Path) -> tuple[Path, Path]:
    data = tmp_path / "data"
    data.mkdir()
    orders = data / "orders.csv"
    orders.write_text("id,customer_id,amount,status\n1,1,10.0,paid\n2,2,20.0,pending\n")
    customers = data / "customers.csv"
    customers.write_text("id,region\n1,North\n2,South\n")
    return orders, customers


def test_init_from_csvs_generates_models_and_files_connection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, csv_sources: tuple[Path, Path]
):
    orders, customers = csv_sources
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "--from", str(orders), "--from", str(customers)])

    assert result.exit_code == 0, result.output
    assert (tmp_path / "models" / "orders.yml").is_file()
    assert (tmp_path / "models" / "customers.yml").is_file()
    assert (tmp_path / "tests" / "smoke.yml").is_file()
    config = yaml.safe_load((tmp_path / "sidemantic.yaml").read_text())
    assert config["connection"]["type"] == "files"


def test_init_from_csvs_project_passes_test_and_live_validate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, csv_sources: tuple[Path, Path]
):
    orders, customers = csv_sources
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["init", "--from", str(orders), "--from", str(customers)]).exit_code == 0

    cli_module._loaded_config = None
    test_result = runner.invoke(app, ["test"])
    assert test_result.exit_code == 0, test_result.output

    cli_module._loaded_config = None
    validate_result = runner.invoke(app, ["validate", "--live"])
    assert validate_result.exit_code == 0, validate_result.output


def _query_values(project: Path, monkeypatch: pytest.MonkeyPatch, sql: str, *extra: str) -> list[dict]:
    monkeypatch.chdir(project)
    cli_module._loaded_config = None
    result = runner.invoke(app, ["query", sql, "--format", "json", *extra])
    assert result.exit_code == 0, result.output
    return json.loads(result.output)


@pytest.mark.parametrize(
    "from_args",
    [
        pytest.param(["--from", "data/*.csv"], id="quoted-glob"),
        # What a shell passes for an unquoted `--from data/*.csv` matching two files.
        pytest.param(["--from", "data/customers.csv", "data/orders.csv"], id="shell-expanded-glob"),
    ],
)
def test_init_from_glob_bootstraps_every_matching_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, csv_sources: tuple[Path, Path], from_args: list[str]
):
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "analytics", *from_args])

    assert result.exit_code == 0, result.output
    project = tmp_path / "analytics"
    assert sorted(path.name for path in (project / "models").iterdir()) == ["customers.yml", "orders.yml"]
    assert _query_values(project, monkeypatch, "SELECT orders.total_amount FROM orders") == [{"total_amount": 30.0}]


def test_init_from_shell_expanded_glob_without_directory_uses_cwd(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, csv_sources: tuple[Path, Path]
):
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "--from", "data/customers.csv", "data/orders.csv"])

    assert result.exit_code == 0, result.output
    assert (tmp_path / "models" / "orders.yml").is_file()
    assert (tmp_path / "models" / "customers.yml").is_file()
    assert not (tmp_path / "data" / "orders.csv" / "models").exists()


def test_init_from_glob_matching_nothing_fails_with_pattern(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "analytics", "--from", "data/*.parquet"])

    assert result.exit_code == 2
    assert "matched no files: data/*.parquet" in result.output


def test_init_extra_positional_argument_explains_how_to_pass_sources(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "analytics", "other"])

    assert result.exit_code == 2
    assert "other" in result.output
    assert "--from 'data/*.csv'" in result.output
    assert not (tmp_path / "analytics").exists()


@pytest.mark.parametrize("source", ["data/orders.csv", "data/warehouse.duckdb"])
def test_init_from_writes_paths_relative_to_project_that_survive_a_move(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, csv_sources: tuple[Path, Path], source: str
):
    import shutil

    import duckdb

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (tmp_path / "data").rename(workspace / "data")
    with duckdb.connect(str(workspace / "data" / "warehouse.duckdb")) as con:
        con.execute("CREATE TABLE orders AS SELECT * FROM read_csv_auto(?)", [str(workspace / "data" / "orders.csv")])
    monkeypatch.chdir(workspace)
    assert runner.invoke(app, ["init", "analytics", "--from", source]).exit_code == 0

    config_text = (workspace / "analytics" / "sidemantic.yaml").read_text()
    assert str(tmp_path) not in config_text
    assert f"../{source}" in config_text

    moved = shutil.copytree(workspace, tmp_path / "moved")
    shutil.rmtree(workspace)
    assert _query_values(moved / "analytics", monkeypatch, "SELECT orders.record_count FROM orders") == [
        {"record_count": 2}
    ]


def test_init_from_connection_explains_how_to_supply_the_connection(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    import duckdb

    database = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(database)) as con:
        con.execute("CREATE TABLE customers AS SELECT * FROM (VALUES (1, 'North'), (2, 'South')) t(id, region)")
    monkeypatch.chdir(tmp_path)
    connection = f"duckdb:///{database}"

    result = runner.invoke(app, ["init", "conn", "--from-connection", connection])

    assert result.exit_code == 0, result.output
    project = tmp_path / "conn"
    # The printed next steps carry the connection, so running them works.
    assert f"sidemantic test --connection {connection}" in result.output
    assert _query_values(
        project, monkeypatch, "SELECT customers.record_count FROM customers", "--connection", connection
    ) == [{"record_count": 2}]
    cli_module._loaded_config = None
    assert runner.invoke(app, ["test", "--connection", connection]).exit_code == 0

    # Uncommenting the block the generated config suggests saves the connection.
    config_path = project / "sidemantic.yaml"
    lines = config_path.read_text().splitlines()
    start = lines.index("# connection:")
    suggested = [line.removeprefix("# ") for line in lines[start:] if line.startswith("# ")]
    config_path.write_text("\n".join([*lines[:start], *suggested, *lines[start + len(suggested) :]]) + "\n")
    assert _query_values(project, monkeypatch, "SELECT customers.record_count FROM customers") == [{"record_count": 2}]


def test_init_from_connection_hides_password_in_next_steps(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from sidemantic.bootstrap import BootstrapResult

    monkeypatch.setattr(
        "sidemantic.bootstrap.bootstrap_models",
        lambda connection, init_sql=None: BootstrapResult(
            tables=[], model_dicts={"orders": {"name": "orders", "table": "orders", "metrics": []}}
        ),
    )
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app, ["init", "pg", "--from-connection", "postgres://analyst:s3cret@db.internal:5432/analytics"]
    )

    assert result.exit_code == 0, result.output
    assert "s3cret" not in result.output
    assert "--connection 'postgres://analyst:****@db.internal:5432/analytics'" in result.output
    config_text = (tmp_path / "pg" / "sidemantic.yaml").read_text()
    assert "s3cret" not in config_text
    assert "#   password_file: .secrets/db-password" in config_text


# --- demo ---------------------------------------------------------------------


def test_demo_creates_project_and_prints_query_results(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["demo", "myproj"])

    assert result.exit_code == 0, result.output
    assert (tmp_path / "myproj" / "data" / "demo.duckdb").is_file()
    # A first query is executed and shown, so revenue appears in the output.
    assert "revenue" in result.output


def test_demo_project_passes_test(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["demo", "myproj"]).exit_code == 0

    cli_module._loaded_config = None
    monkeypatch.chdir(tmp_path / "myproj")
    result = runner.invoke(app, ["test"])

    assert result.exit_code == 0, result.output


# --- test command failures ----------------------------------------------------


@pytest.fixture
def failing_test_project(tmp_path: Path) -> Path:
    (tmp_path / "models").mkdir()
    (tmp_path / "tests").mkdir()
    (tmp_path / "sidemantic.yaml").write_text("models_dir: models\n")
    (tmp_path / "models" / "orders.yml").write_text(
        "models:\n"
        "  - name: orders\n"
        "    sql: select 1 as id, 250.0 as amount\n"
        "    primary_key: id\n"
        "    metrics:\n"
        "      - name: revenue\n"
        "        agg: sum\n"
        "        sql: amount\n"
    )
    (tmp_path / "tests" / "revenue.yml").write_text(
        "tests:\n  - name: wrong revenue\n    sql: SELECT orders.revenue FROM orders\n    expect:\n      value: 999.0\n"
    )
    return tmp_path


def test_test_command_failing_expectation_exits_1_with_fail_line(
    monkeypatch: pytest.MonkeyPatch, failing_test_project: Path
):
    monkeypatch.chdir(failing_test_project)

    result = runner.invoke(app, ["test"])

    assert result.exit_code == 1
    assert any(line.startswith("FAIL") for line in result.output.splitlines())


def test_test_command_json_reports_failure(monkeypatch: pytest.MonkeyPatch, failing_test_project: Path):
    monkeypatch.chdir(failing_test_project)

    result = runner.invoke(app, ["test", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["passed"] is False


# --- validate --live ----------------------------------------------------------


def test_validate_live_db_csv_fails_on_missing_column(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    (tmp_path / "orders.csv").write_text("id,amount\n1,10\n")
    models = tmp_path / "models"
    models.mkdir()
    (models / "orders.yml").write_text(
        "models:\n"
        "  - name: orders\n"
        "    table: orders\n"
        "    primary_key: id\n"
        "    dimensions:\n"
        "      - name: ghost\n"
        "        type: categorical\n"
        "        sql: does_not_exist\n"
        "    metrics:\n"
        "      - name: c\n"
        "        agg: count\n"
    )
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["validate", "models", "--live", "--db", str(tmp_path / "orders.csv")])

    assert result.exit_code == 1
    assert "does_not_exist" in result.output or "missing column" in result.output


def test_validate_live_accepts_metrics_that_reference_other_metrics(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    (tmp_path / "orders.csv").write_text("id,status,amount\n1,completed,10\n2,pending,30\n")
    models = tmp_path / "models"
    models.mkdir()
    (models / "orders.yml").write_text(
        "models:\n"
        "  - name: orders\n"
        "    table: orders\n"
        "    primary_key: id\n"
        "    dimensions:\n"
        "      - {name: status, type: categorical}\n"
        "    metrics:\n"
        "      - {name: revenue, agg: sum, sql: amount}\n"
        "      - {name: completed_revenue, agg: sum, sql: amount, filters: [\"status = 'completed'\"]}\n"
        "      - {name: completion_share, type: derived, sql: completed_revenue / revenue}\n"
        "      - {name: cumulative_revenue, type: cumulative, sql: revenue}\n"
    )
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["validate", "models", "--live", "--db", str(tmp_path / "orders.csv")])

    assert result.exit_code == 0, result.output
    assert "missing column" not in result.output


def test_validate_live_db_csv_passes_on_correct_model(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    (tmp_path / "orders.csv").write_text("id,amount\n1,10\n2,20\n")
    models = tmp_path / "models"
    models.mkdir()
    (models / "orders.yml").write_text(
        "models:\n"
        "  - name: orders\n"
        "    table: orders\n"
        "    primary_key: id\n"
        "    metrics:\n"
        "      - name: revenue\n"
        "        agg: sum\n"
        "        sql: amount\n"
    )
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["validate", "models", "--live", "--db", str(tmp_path / "orders.csv")])

    assert result.exit_code == 0, result.output


# --- serve / server api help surfaces -----------------------------------------


def test_serve_help_mentions_mcp():
    result = runner.invoke(app, ["serve", "--help"])
    assert result.exit_code == 0, result.output
    assert "mcp" in result.output.lower()


def test_server_api_help_shows_mcp_toggle():
    result = runner.invoke(app, ["server", "api", "--help"])
    assert result.exit_code == 0, result.output
    assert "--mcp" in result.output
    assert "--no-mcp" in result.output


def test_init_from_nondefault_schema_produces_runnable_query(tmp_path, monkeypatch):
    import duckdb

    database = tmp_path / "source.duckdb"
    with duckdb.connect(str(database)) as con:
        con.execute("CREATE SCHEMA sales")
        con.execute("CREATE TABLE sales.orders AS SELECT 1 id, 10 amount")
    project = tmp_path / "project"
    result = runner.invoke(app, ["init", str(project), "--from", str(database)])
    assert result.exit_code == 0, result.output
    model = yaml.safe_load((project / "models/orders.yml").read_text())["models"][0]
    assert '"sales"."orders"' in model["table"]
    monkeypatch.chdir(project)
    monkeypatch.setattr(cli_module, "_loaded_config", None)
    result = runner.invoke(app, ["query", "SELECT orders.record_count FROM orders"])
    assert result.exit_code == 0, result.output
    assert "1" in result.output
