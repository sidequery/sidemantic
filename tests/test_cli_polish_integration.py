"""End-to-end behavioral tests for the T3 CLI polish layer."""

import json
from pathlib import Path
from types import SimpleNamespace

from click.testing import CliRunner
from typer.main import get_command

import sidemantic.cli as cli_module
from sidemantic.cli import app


def _cli():
    return get_command(app)


def _write_model(directory: Path) -> Path:
    models = directory / "models"
    models.mkdir()
    (models / "orders.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: id
        type: numeric
        sql: id
"""
    )
    return models


def test_root_and_nested_help_suggest_reviewable_command_corrections() -> None:
    runner = CliRunner()

    root = runner.invoke(_cli(), ["vlaidate"])
    nested = runner.invoke(_cli(), ["help", "migrate", "genrate"])

    assert root.exit_code == 2
    assert "Did you mean 'validate'?" in root.stderr
    assert "validate --help" in root.stderr
    assert nested.exit_code == 2
    assert "Did you mean 'migrate generate'?" in nested.stderr
    assert "sidemantic help migrate generate" in nested.stderr


def test_machine_invocation_suppresses_command_recovery_hint() -> None:
    result = CliRunner().invoke(_cli(), ["vlaidate", "--format", "json"])

    assert result.exit_code == 2
    assert "Did you mean" not in result.stderr


def test_legacy_alias_warns_with_target_release_and_quiet_suppresses_it() -> None:
    runner = CliRunner()

    human = runner.invoke(_cli(), ["tree", "/does-not-exist"])
    quiet = runner.invoke(_cli(), ["tree", "/does-not-exist", "--quiet"])

    assert human.exit_code == 2
    assert "use 'workbench'" in human.stderr
    assert "Sidemantic 0.12.0" in human.stderr
    assert "Deprecated" not in quiet.stderr


def test_common_error_adds_safe_recovery_but_never_mutates(tmp_path: Path) -> None:
    source = _write_model(tmp_path) / "orders.yml"
    output = tmp_path / "converted.yml"
    output.write_text("keep me")

    result = CliRunner().invoke(
        _cli(),
        ["convert", str(source), "--output", str(output), "--to", "sidemantic"],
    )

    assert result.exit_code == 1
    assert "pass --force only if replacing it is intentional" in result.stderr
    assert output.read_text() == "keep me"


def test_bad_parameter_recovery_is_human_only(tmp_path: Path) -> None:
    models = _write_model(tmp_path)
    missing = tmp_path / "missing.sql"
    runner = CliRunner()

    human = runner.invoke(_cli(), ["migrate", "check", str(missing), "--models", str(models)])
    machine = runner.invoke(
        _cli(),
        ["migrate", "check", str(missing), "--models", str(models), "--format", "json"],
    )

    assert human.exit_code == 2
    assert "Hint: Check the query path" in human.stderr
    assert human.stderr.count("Hint: Check the query path") == 1
    assert "Hint:" not in machine.stderr


def test_validate_next_step_is_stderr_only_and_machine_safe(tmp_path: Path) -> None:
    models = _write_model(tmp_path)
    runner = CliRunner()

    human = runner.invoke(_cli(), ["validate", str(models)])
    machine = runner.invoke(_cli(), ["validate", str(models), "--format", "json"])
    quiet = runner.invoke(_cli(), ["validate", str(models), "--quiet"])

    assert human.exit_code == 0
    assert "Next: inspect the model" in human.stderr
    assert "Next:" not in human.stdout
    assert machine.exit_code == 0
    assert json.loads(machine.stdout)["valid"] is True
    assert "Next:" not in machine.stderr
    assert "Next:" not in quiet.stderr


def test_info_and_validation_route_human_reports_through_pager_seam(tmp_path: Path, monkeypatch) -> None:
    models = _write_model(tmp_path)
    reports: list[str] = []
    monkeypatch.setattr(cli_module, "_emit_long_report", reports.append)

    info = CliRunner().invoke(_cli(), ["info", str(models)])
    validate = CliRunner().invoke(_cli(), ["validate", str(models)])

    assert info.exit_code == 0
    assert validate.exit_code == 0
    assert any("Semantic Layer:" in report for report in reports)
    assert any("Validation Results:" in report for report in reports)


def test_explain_routes_human_output_but_not_json_through_pager_seam(tmp_path: Path, monkeypatch) -> None:
    reports: list[str] = []
    explanation = SimpleNamespace(to_dict=lambda: {"rewritten_sql": "SELECT 1"})
    layer = SimpleNamespace(explain_sql=lambda *_args, **_kwargs: explanation)
    monkeypatch.setattr(cli_module, "_load_query_layer", lambda *_args, **_kwargs: layer)
    monkeypatch.setattr(
        "sidemantic.cli_contract.emit_long_output",
        lambda text, **_kwargs: reports.append(text),
    )

    human = CliRunner().invoke(_cli(), ["explain", "SELECT 1"])
    machine = CliRunner().invoke(_cli(), ["explain", "SELECT 1", "--format", "json"])

    assert human.exit_code == 0
    assert json.loads(reports[0])["rewritten_sql"] == "SELECT 1"
    assert machine.exit_code == 0
    assert json.loads(machine.stdout)["rewritten_sql"] == "SELECT 1"
    assert len(reports) == 1


def test_preagg_recommend_routes_long_human_report_through_pager_seam(tmp_path: Path, monkeypatch) -> None:
    query_log = tmp_path / "queries.sql"
    query_log.write_text("SELECT orders.id FROM orders")
    reports: list[str] = []
    pattern = SimpleNamespace(model="orders", metrics={"count"}, dimensions={"id"}, granularities=set())
    recommendation = SimpleNamespace(
        suggested_name="orders_rollup",
        pattern=pattern,
        query_count=12,
        estimated_benefit_score=0.8,
    )

    class FakeRecommender:
        def __init__(self, **_kwargs) -> None:
            pass

        def parse_query_log_file(self, _path: str) -> None:
            pass

        def get_summary(self) -> dict[str, object]:
            return {
                "total_queries": 12,
                "unique_patterns": 1,
                "patterns_above_threshold": 1,
                "models": {"orders": 12},
            }

        def get_recommendations(self, *, top_n=None):
            del top_n
            return [recommendation]

    monkeypatch.setattr("sidemantic.core.preagg_recommender.PreAggregationRecommender", FakeRecommender)
    monkeypatch.setattr(cli_module, "_emit_long_report", reports.append)

    result = CliRunner().invoke(_cli(), ["preagg", "recommend", "--queries", str(query_log)])

    assert result.exit_code == 0
    assert "Pre-Aggregation Recommendations" in reports[0]
    assert "orders_rollup" in reports[0]


def _complete(words: str, cword: int) -> list[str]:
    result = CliRunner().invoke(
        _cli(),
        [],
        env={"_ROOT_COMPLETE": "complete_bash", "COMP_WORDS": words, "COMP_CWORD": str(cword)},
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    return result.stdout.splitlines()


def test_shell_completion_covers_commands_and_formats() -> None:
    assert "convert" in _complete("root con", 1)
    assert "sidemantic" in _complete("root convert --to s", 3)
    assert "auto" in _complete("root convert --from a", 3)


def test_shell_completion_covers_models_dashboard_specs_and_paths(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_model(tmp_path)
    (tmp_path / "dashboard.yml").touch()
    (tmp_path / "source.yml").touch()
    monkeypatch.chdir(tmp_path)

    assert any("dashboard.yml" in value for value in _complete("root dashboard validate da", 3))
    assert "orders" in _complete(f"root --project {tmp_path} preagg refresh --model ord", 6)
    assert any("source.yml" in value for value in _complete("root convert so", 2))
