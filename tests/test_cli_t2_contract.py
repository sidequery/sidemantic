"""Behavioral contract for T2 command-line presentation and configuration."""

from __future__ import annotations

import csv
import io
import json
import sys
import tomllib
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

import sidemantic.cli as cli_module
from sidemantic.cli import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def _reset_cli_state(monkeypatch: pytest.MonkeyPatch):
    cli_module._loaded_config = None
    cli_module._project_context = None
    for name in (
        "SIDEMANTIC_PROJECT",
        "SIDEMANTIC_CONFIG",
        "SIDEMANTIC_FORMAT",
        "SIDEMANTIC_PLAIN",
        "SIDEMANTIC_QUIET",
        "SIDEMANTIC_VERBOSE",
        "SIDEMANTIC_DEBUG",
        "SIDEMANTIC_NO_COLOR",
        "SIDEMANTIC_ENGINE",
        "SIDEMANTIC_ENGINE_FALLBACK",
        "SIDEMANTIC_RS_SQL_GENERATOR",
        "SIDEMANTIC_RS_QUERY_VALIDATION",
        "SIDEMANTIC_RS_REWRITER",
        "SIDEMANTIC_RS_SQL_GENERATOR_VERIFY",
        "SIDEMANTIC_RS_NO_FALLBACK",
        "SIDEMANTIC_PG_PASSWORD_FILE",
        "SIDEMANTIC_API_AUTH_TOKEN_FILE",
        "NO_COLOR",
        "FORCE_COLOR",
        "TERM",
        "CI",
    ):
        monkeypatch.delenv(name, raising=False)
    yield
    cli_module._loaded_config = None
    cli_module._project_context = None


@pytest.fixture
def project(tmp_path: Path) -> Path:
    models = tmp_path / "models"
    models.mkdir()
    (models / "orders.yml").write_text(
        """models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: status
        sql: status
        type: categorical
    metrics:
      - name: order_count
        agg: count
"""
    )
    data = tmp_path / "data"
    data.mkdir()
    database = data / "warehouse.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute("CREATE TABLE orders (id INTEGER, status VARCHAR)")
    connection.execute("INSERT INTO orders VALUES (1, 'complete'), (2, 'pending')")
    connection.close()
    (tmp_path / "sidemantic.yaml").write_text(
        """models_dir: models
connection:
  type: duckdb
  path: data/warehouse.duckdb
"""
    )
    return tmp_path


@pytest.mark.parametrize("output_format", ["table", "csv", "json", "jsonl"])
def test_query_supports_standard_formats(project: Path, output_format: str):
    result = runner.invoke(
        app,
        [
            "--project",
            str(project),
            "query",
            "SELECT status, order_count FROM orders ORDER BY status",
            "--format",
            output_format,
        ],
    )

    assert result.exit_code == 0, result.output
    if output_format == "json":
        assert json.loads(result.stdout) == [
            {"order_count": 1, "status": "complete"},
            {"order_count": 1, "status": "pending"},
        ]
    elif output_format == "jsonl":
        assert [json.loads(line) for line in result.stdout.splitlines()] == [
            {"order_count": 1, "status": "complete"},
            {"order_count": 1, "status": "pending"},
        ]
    elif output_format == "csv":
        assert list(csv.reader(io.StringIO(result.stdout))) == [
            ["status", "order_count"],
            ["complete", "1"],
            ["pending", "1"],
        ]
    else:
        assert "status" in result.stdout
        assert "complete" in result.stdout
        assert "pending" in result.stdout


def test_plain_query_is_stable_tsv(project: Path):
    result = runner.invoke(
        app,
        ["--project", str(project), "query", "SELECT status FROM orders ORDER BY status", "--plain"],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == ["status", "complete", "pending"]
    assert "\x1b[" not in result.stdout


@pytest.mark.parametrize("output_format", ["table", "csv", "json", "jsonl"])
def test_query_formats_preserve_database_scalar_values(project: Path, output_format: str):
    result = runner.invoke(
        app,
        [
            "query",
            "SELECT DATE '2024-01-02' AS day, "
            "TIMESTAMP '2024-01-02 03:04:05' AS occurred_at, "
            "12.50::DECIMAL(10, 2) AS amount, "
            "9007199254740993::DECIMAL(38, 0) AS exact_count",
            "--project",
            str(project),
            "--format",
            output_format,
        ],
    )

    assert result.exit_code == 0, result.output
    if output_format == "csv":
        assert list(csv.reader(io.StringIO(result.stdout))) == [
            ["day", "occurred_at", "amount", "exact_count"],
            ["2024-01-02", "2024-01-02 03:04:05", "12.50", "9007199254740993"],
        ]
    elif output_format in {"json", "jsonl"}:
        payload = json.loads(result.stdout)
        row = payload[0] if output_format == "json" else payload
        assert row == {
            "amount": "12.50",
            "day": "2024-01-02",
            "exact_count": "9007199254740993",
            "occurred_at": "2024-01-02T03:04:05",
        }
    else:
        assert "2024-01-02" in result.stdout
        assert "2024-01-02 03:04:05" in result.stdout
        assert "12.50" in result.stdout
        assert "9007199254740993" in result.stdout


def test_plain_query_preserves_database_scalar_values(project: Path):
    result = runner.invoke(
        app,
        [
            "query",
            "SELECT DATE '2024-01-02' AS day, "
            "TIMESTAMP '2024-01-02 03:04:05' AS occurred_at, "
            "12.50::DECIMAL(10, 2) AS amount",
            "--project",
            str(project),
            "--plain",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        "day\toccurred_at\tamount",
        "2024-01-02\t2024-01-02 03:04:05\t12.50",
    ]


@pytest.mark.parametrize("output_format", ["table", "csv", "json", "jsonl"])
def test_query_formats_render_binary_values(project: Path, output_format: str):
    result = runner.invoke(
        app,
        [
            "query",
            "SELECT from_hex('00ff') AS payload",
            "--project",
            str(project),
            "--format",
            output_format,
        ],
    )

    assert result.exit_code == 0, result.output
    if output_format == "csv":
        assert list(csv.reader(io.StringIO(result.stdout))) == [["payload"], ["00ff"]]
    elif output_format in {"json", "jsonl"}:
        payload = json.loads(result.stdout)
        row = payload[0] if output_format == "json" else payload
        assert row == {"payload": "00ff"}
    else:
        assert "00ff" in result.stdout


def test_plain_query_renders_binary_values(project: Path):
    result = runner.invoke(
        app,
        ["query", "SELECT from_hex('00ff') AS payload", "--project", str(project), "--plain"],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == ["payload", "00ff"]


@pytest.mark.parametrize("output_format", [None, "table", "csv", "json", "jsonl"])
def test_query_formats_preserve_duplicate_columns_by_position(project: Path, output_format: str | None):
    arguments = ["query", "SELECT 1 AS x, 2 AS x", "--project", str(project)]
    if output_format:
        arguments.extend(["--format", output_format])

    result = runner.invoke(app, arguments)

    assert result.exit_code == 0, result.output
    if output_format in {"json", "jsonl"}:
        payload = json.loads(result.stdout)
        row = payload[0] if output_format == "json" else payload
        assert row == {"x": 1, "x_2": 2}
    elif output_format == "table":
        assert result.stdout.splitlines()[0].split() == ["x", "x"]
        assert result.stdout.splitlines()[2].split() == ["1", "2"]
    else:
        assert list(csv.reader(io.StringIO(result.stdout))) == [["x", "x"], ["1", "2"]]


def test_plain_query_preserves_duplicate_columns_by_position(project: Path):
    result = runner.invoke(
        app,
        ["query", "SELECT 1 AS x, 2 AS x", "--project", str(project), "--plain"],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == ["x\tx", "1\t2"]


def test_query_dry_run_honors_csv_format(project: Path):
    result = runner.invoke(
        app,
        [
            "query",
            "SELECT status, order_count FROM orders",
            "--project",
            str(project),
            "--dry-run",
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.output
    rows = list(csv.reader(io.StringIO(result.stdout)))
    assert rows[0] == ["sql"]
    assert len(rows) == 2
    assert "SELECT" in rows[1][0]
    assert "," in rows[1][0]


def test_query_dry_run_defaults_to_raw_sql(project: Path):
    result = runner.invoke(
        app,
        [
            "query",
            "SELECT status, order_count FROM orders",
            "--project",
            str(project),
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "SELECT" in result.stdout.upper()
    assert not result.stdout.startswith("sql\n")


def test_explain_accepts_json_format(project: Path):
    result = runner.invoke(
        app,
        [
            "explain",
            "SELECT status, order_count FROM orders",
            "--project",
            str(project),
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "rewritten_sql" in json.loads(result.stdout)


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (["--format", "csv"], "explain supports only --format json"),
        (["--format", "jsonl"], "explain supports only --format json"),
        (["--plain"], "explain does not support --plain; use --format json"),
    ],
)
def test_explain_rejects_unsupported_output_modes(
    project: Path,
    arguments: list[str],
    message: str,
):
    result = runner.invoke(
        app,
        [
            "explain",
            "SELECT status, order_count FROM orders",
            "--project",
            str(project),
            *arguments,
        ],
    )

    assert result.exit_code == 2
    assert result.stdout == ""
    assert message in result.stderr


def test_format_default_does_not_suppress_unstructured_server_status(
    project: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    pytest.importorskip("fastapi")
    monkeypatch.setenv("SIDEMANTIC_FORMAT", "json")
    monkeypatch.setattr("sidemantic.api_server.start_api_server", lambda *args, **kwargs: None)

    result = runner.invoke(app, ["server", "api", "--project", str(project), "--no-ui"])

    assert result.exit_code == 0, result.output
    assert result.stdout == ""
    assert "Starting HTTP API server" in result.stderr
    assert "Listening on http://" in result.stderr
    assert "Authentication: disabled" in result.stderr


@pytest.mark.parametrize("command", ["info", "validate"])
@pytest.mark.parametrize("output_format", ["table", "csv", "json", "jsonl"])
def test_structured_inspection_commands_support_formats(
    project: Path,
    command: str,
    output_format: str,
):
    result = runner.invoke(app, [command, "--project", str(project), "--format", output_format])

    assert result.exit_code == 0, result.output
    if output_format == "json":
        assert json.loads(result.stdout)
    elif output_format == "jsonl":
        assert all(isinstance(json.loads(line), dict) for line in result.stdout.splitlines())
    elif output_format == "csv":
        assert len(list(csv.reader(io.StringIO(result.stdout)))) >= 2
    else:
        assert result.stdout.strip()


def test_json_alias_matches_format_json(project: Path):
    legacy = runner.invoke(app, ["--project", str(project), "info", "--json"])
    standard = runner.invoke(app, ["info", "--project", str(project), "--format", "json"])

    assert legacy.exit_code == standard.exit_code == 0
    assert json.loads(legacy.stdout) == json.loads(standard.stdout)


@pytest.mark.parametrize(
    ("environment", "config_default"),
    [({"SIDEMANTIC_FORMAT": "csv"}, None), ({}, "csv")],
)
def test_json_alias_overrides_non_cli_format_defaults(
    project: Path,
    environment: dict[str, str],
    config_default: str | None,
):
    if config_default:
        config = project / "sidemantic.yaml"
        config.write_text(f"{config.read_text()}cli:\n  format: {config_default}\n")

    result = runner.invoke(app, ["--project", str(project), "info", "--json"], env=environment)

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["models"][0]["name"] == "orders"


def test_json_alias_rejects_explicit_non_json_format(project: Path):
    result = runner.invoke(app, ["--project", str(project), "--format", "csv", "info", "--json"])

    assert result.exit_code == 2
    assert "cannot be combined" in result.stderr


def test_plain_rejects_machine_format(project: Path):
    result = runner.invoke(app, ["info", "--project", str(project), "--plain", "--format", "json"])

    assert result.exit_code == 2
    assert "plain" in result.stderr.lower()


@pytest.mark.parametrize("default_source", ["environment", "config"])
def test_explicit_plain_overrides_machine_format_defaults(project: Path, default_source: str):
    environment = {}
    if default_source == "environment":
        environment["SIDEMANTIC_FORMAT"] = "json"
    else:
        config = project / "sidemantic.yaml"
        config.write_text(f"{config.read_text()}cli:\n  format: json\n")

    result = runner.invoke(app, ["info", "--project", str(project), "--plain"], env=environment)

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines()[0].split("\t") == [
        "name",
        "table",
        "dimensions",
        "metrics",
        "relationships",
        "connected_to",
    ]


@pytest.mark.parametrize("default_source", ["environment", "config"])
def test_explicit_json_overrides_plain_defaults(project: Path, default_source: str):
    environment = {}
    if default_source == "environment":
        environment["SIDEMANTIC_PLAIN"] = "1"
    else:
        config = project / "sidemantic.yaml"
        config.write_text(f"{config.read_text()}cli:\n  plain: true\n")

    result = runner.invoke(app, ["info", "--project", str(project), "--json"], env=environment)

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["models"][0]["name"] == "orders"


@pytest.mark.parametrize("output_format", ["table", "json"])
@pytest.mark.parametrize("default_source", ["environment", "config"])
def test_explicit_format_overrides_plain_defaults(project: Path, output_format: str, default_source: str):
    environment = {}
    if default_source == "environment":
        environment["SIDEMANTIC_PLAIN"] = "1"
    else:
        config = project / "sidemantic.yaml"
        config.write_text(f"{config.read_text()}cli:\n  plain: true\n")

    result = runner.invoke(
        app,
        ["info", "--project", str(project), "--format", output_format],
        env=environment,
    )

    assert result.exit_code == 0, result.output
    if output_format == "json":
        assert json.loads(result.stdout)["models"][0]["name"] == "orders"
    else:
        lines = result.stdout.splitlines()
        assert lines[0].split() == ["name", "table", "dimensions", "metrics", "relationships", "connected_to"]
        assert "--" in lines[1]


def test_long_global_options_work_after_subcommand(project: Path):
    before = runner.invoke(app, ["--project", str(project), "--format", "json", "info"])
    after = runner.invoke(app, ["info", "--project", str(project), "--format", "json"])

    assert before.exit_code == after.exit_code == 0
    assert json.loads(before.stdout) == json.loads(after.stdout)


def test_quiet_suppresses_status_but_not_primary_result(project: Path, tmp_path: Path):
    output = tmp_path / "results.csv"
    result = runner.invoke(
        app,
        [
            "query",
            "SELECT order_count FROM orders",
            "--project",
            str(project),
            "--output",
            str(output),
            "--quiet",
        ],
    )

    assert result.exit_code == 0, result.output
    assert output.read_text().startswith("order_count")
    assert result.stderr == ""


def test_debug_implies_verbose_and_quiet_wins_for_status(project: Path):
    result = runner.invoke(app, ["info", "--project", str(project), "--debug", "--quiet", "--format", "json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)
    assert result.stderr == ""


def test_project_and_format_environment_defaults(project: Path):
    result = runner.invoke(
        app,
        ["info"],
        env={"SIDEMANTIC_PROJECT": str(project), "SIDEMANTIC_FORMAT": "json"},
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["models"][0]["name"] == "orders"


def test_flags_override_environment(project: Path):
    result = runner.invoke(
        app,
        ["--project", str(project), "info", "--format", "csv"],
        env={"SIDEMANTIC_FORMAT": "json", "SIDEMANTIC_QUIET": "1"},
    )

    assert result.exit_code == 0, result.output
    assert list(csv.reader(io.StringIO(result.stdout)))[1][0] == "orders"


def test_config_environment_selects_project(project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        app,
        ["query", "SELECT order_count FROM orders", "--format", "json"],
        env={"SIDEMANTIC_CONFIG": str(project / "sidemantic.yaml")},
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == [{"order_count": 2}]


def test_project_config_supplies_presentation_defaults(project: Path):
    config = project / "sidemantic.yaml"
    config.write_text(f"{config.read_text()}cli:\n  format: json\n  quiet: true\n")

    result = runner.invoke(app, ["--project", str(project), "info"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["models"][0]["name"] == "orders"
    assert result.stderr == ""


@pytest.mark.parametrize("output_format", ["table", "csv", "json", "jsonl"])
def test_validate_parse_failures_preserve_requested_format(project: Path, output_format: str):
    (project / "models" / "orders.yml").write_text("models: [not valid")

    result = runner.invoke(
        app,
        ["validate", "--project", str(project), "--format", output_format],
    )

    assert result.exit_code == 1, result.output
    if output_format == "json":
        payload = json.loads(result.stdout)
        assert payload["valid"] is False
        assert payload["errors"]
    elif output_format == "jsonl":
        assert json.loads(result.stdout)["level"] == "error"
    elif output_format == "csv":
        rows = list(csv.DictReader(io.StringIO(result.stdout)))
        assert rows[0]["level"] == "error"
    else:
        assert "error" in result.stdout.lower()


@pytest.mark.parametrize("output_mode", ["table", "csv", "json", "jsonl", "plain"])
def test_validate_rust_failures_preserve_requested_output(
    project: Path,
    output_mode: str,
    monkeypatch: pytest.MonkeyPatch,
):
    def fail_to_load(_directory: Path):
        raise RuntimeError("bridge unavailable")

    monkeypatch.setattr("sidemantic.rust_bridge.load_graph_from_directory_with_rust", fail_to_load)
    arguments = ["validate", "--project", str(project), "--engine", "rust"]
    arguments.extend(["--plain"] if output_mode == "plain" else ["--format", output_mode])

    result = runner.invoke(app, arguments)

    assert result.exit_code == 1, result.output
    if output_mode == "json":
        payload = json.loads(result.stdout)
        assert payload["valid"] is False
        assert payload["errors"] == ["Rust validation failed: bridge unavailable"]
    elif output_mode == "jsonl":
        assert json.loads(result.stdout) == {
            "level": "error",
            "message": "Rust validation failed: bridge unavailable",
        }
    elif output_mode == "csv":
        assert list(csv.reader(io.StringIO(result.stdout))) == [
            ["level", "message"],
            ["error", "Rust validation failed: bridge unavailable"],
        ]
    else:
        assert "Rust validation failed: bridge unavailable" in result.stdout


def test_empty_jsonl_result_emits_no_blank_record(tmp_path: Path):
    queries = tmp_path / "queries.sql"
    queries.write_text(
        "SELECT revenue FROM orders\n-- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status;\n"
    )

    result = runner.invoke(
        app,
        ["preagg", "recommend", "--queries", str(queries), "--format", "jsonl"],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout == ""


@pytest.mark.parametrize("output_mode", ["table", "csv", "jsonl", "plain"])
def test_dashboard_validation_records_include_errors(project: Path, output_mode: str):
    dashboard = project / "broken-dashboard.yaml"
    dashboard.write_text("schema: unsupported\ntabs: []\n")
    arguments = [
        "dashboard",
        "validate",
        str(dashboard),
        "--project",
        str(project),
    ]
    arguments.extend(["--plain"] if output_mode == "plain" else ["--format", output_mode])

    result = runner.invoke(app, arguments)

    assert result.exit_code == 1, result.output
    if output_mode == "jsonl":
        record = json.loads(result.stdout)
        assert any("schema must be" in error for error in record["errors"])
        assert "title is required" in record["errors"]
    elif output_mode == "csv":
        record = next(csv.DictReader(io.StringIO(result.stdout)))
        errors = json.loads(record["errors"])
        assert any("schema must be" in error for error in errors)
        assert "title is required" in errors
    else:
        assert "schema must be" in result.stdout
        assert "title is required" in result.stdout


def test_root_and_complex_help_include_examples_docs_and_support():
    for arguments in (["--help"], ["query", "--help"], ["migrate", "--help"], ["preagg", "--help"]):
        result = runner.invoke(app, arguments)
        assert result.exit_code == 0, result.output
        assert "Example" in result.stdout
        assert "https://sidemantic.com" in result.stdout
        assert "https://github.com/sidequery/sidemantic/issues" in result.stdout


def test_color_policy_precedence(monkeypatch: pytest.MonkeyPatch):
    from sidemantic.cli_contract import color_enabled

    monkeypatch.setenv("FORCE_COLOR", "1")
    assert color_enabled(is_tty=False)
    monkeypatch.setenv("NO_COLOR", "1")
    assert not color_enabled(is_tty=True)
    monkeypatch.delenv("NO_COLOR")
    assert not color_enabled(is_tty=True, no_color=True)
    monkeypatch.delenv("FORCE_COLOR")
    monkeypatch.setenv("TERM", "dumb")
    assert not color_enabled(is_tty=True)


def test_progress_policy_requires_human_tty(monkeypatch: pytest.MonkeyPatch):
    from sidemantic.cli_contract import cli_state, is_terminal, progress_enabled

    state = cli_state()
    assert is_terminal(io.StringIO()) is False
    state.quiet = False
    state.machine_output = False
    assert progress_enabled(is_tty=True)
    assert not progress_enabled(is_tty=False)
    state.quiet = True
    assert not progress_enabled(is_tty=True)
    state.quiet = False
    state.machine_output = True
    assert not progress_enabled(is_tty=True)
    state.machine_output = False
    monkeypatch.setenv("CI", "true")
    assert not progress_enabled(is_tty=True)


def test_click_and_typer_have_compatible_runtime_floors():
    metadata = tomllib.loads(Path("pyproject.toml").read_text())

    assert "click>=8.0.0" in metadata["project"]["dependencies"]
    assert "typer>=0.18.0" in metadata["project"]["dependencies"]


def test_progress_falls_back_when_rich_is_unavailable(monkeypatch: pytest.MonkeyPatch):
    import sidemantic.cli_contract as contract

    monkeypatch.setattr(contract, "progress_enabled", lambda: True)
    monkeypatch.setitem(sys.modules, "rich.console", None)
    entered = False

    with contract.progress("Working"):
        entered = True

    assert entered


def test_convert_uses_shared_progress_seam(project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    messages: list[str] = []

    @contextmanager
    def recording_progress(message: str):
        messages.append(message)
        yield

    monkeypatch.setattr(cli_module, "progress", recording_progress)
    output = tmp_path / "converted.yml"
    result = runner.invoke(
        app,
        ["convert", "--project", str(project), "--output", str(output), "--to", "sidemantic"],
    )

    assert result.exit_code == 0, result.output
    assert output.exists()
    assert messages == ["Converting semantic definitions to sidemantic"]


def test_safe_credential_file_environment_is_not_echoed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from sidemantic.cli_contract import credential_file_from_env

    credential = tmp_path / "pg-password"
    credential.write_text("correct horse battery staple\n")
    monkeypatch.setenv("SIDEMANTIC_PG_PASSWORD_FILE", str(credential))

    assert credential_file_from_env("SIDEMANTIC_PG_PASSWORD_FILE") == credential
    assert "correct horse battery staple" not in repr(cli_module.cli_state())
