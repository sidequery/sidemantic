"""Behavioral contract tests for the public Sidemantic CLI surface."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

import sidemantic.cli as cli_module
from sidemantic.cli import app
from tests.optional_dep_stubs import ensure_fake_riffq

runner = CliRunner()


@pytest.fixture(autouse=True)
def _reset_cli_globals():
    cli_module._loaded_config = None
    cli_module._project_context = None
    yield
    cli_module._loaded_config = None
    cli_module._project_context = None


def _write_model(directory: Path, *, invalid: bool = False) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    model = directory / "orders.yml"
    model.write_text(
        "models:\n  - name: broken\n    table: [\n"
        if invalid
        else """models:
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
    return model


def _write_db(path: Path) -> None:
    connection = duckdb.connect(str(path))
    connection.execute("CREATE TABLE orders (id INTEGER, status VARCHAR)")
    connection.execute("INSERT INTO orders VALUES (1, 'complete'), (2, 'pending')")
    connection.close()


@pytest.mark.parametrize("option", ["-h", "--help"])
def test_root_and_subcommand_help_aliases(option: str):
    root = runner.invoke(app, [option], prog_name="sidemantic")
    nested = runner.invoke(app, ["migrate", "generate", option], prog_name="sidemantic")

    assert root.exit_code == 0
    assert "Usage: sidemantic" in root.stdout
    assert nested.exit_code == 0
    assert "sidemantic migrate generate" in nested.stdout
    assert not root.stderr
    assert not nested.stderr


@pytest.mark.parametrize(
    ("arguments", "usage"),
    [
        (["help"], "Usage: sidemantic"),
        (["help", "info"], "Usage: sidemantic info"),
        (["help", "migrate", "generate"], "Usage: sidemantic migrate generate"),
    ],
)
def test_help_command_resolves_nested_paths(arguments: list[str], usage: str):
    result = runner.invoke(app, arguments)

    assert result.exit_code == 0
    assert usage in result.stdout
    assert not result.stderr


@pytest.mark.parametrize(
    ("arguments", "usage", "exit_code"),
    [
        (["help"], "Usage: sidemantic", 0),
        (["help", "migrate", "generate"], "Usage: sidemantic migrate generate", 0),
        (["info", "--help"], "Usage: sidemantic info", 0),
        (["migrate", "generate", "-h"], "Usage: sidemantic migrate generate", 0),
        (["migrate"], "Usage: sidemantic migrate", 2),
    ],
)
def test_all_help_paths_bypass_malformed_project_config(
    arguments: list[str],
    usage: str,
    exit_code: int,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    (tmp_path / "sidemantic.yaml").write_text("models_dir: [\n")
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, arguments, prog_name="sidemantic")

    assert result.exit_code == exit_code, result.output
    assert usage in result.stdout
    assert not result.stderr


def test_exit_codes_and_stream_separation(tmp_path: Path):
    models = tmp_path / "models"
    _write_model(models)

    success = runner.invoke(app, ["info", str(models)])
    invalid = runner.invoke(app, ["does-not-exist"])
    bad_config = runner.invoke(app, ["--config", str(tmp_path / "missing.yml"), "info"])
    broken_models = tmp_path / "broken"
    _write_model(broken_models, invalid=True)
    operational = runner.invoke(app, ["info", str(broken_models)])

    assert success.exit_code == 0
    assert success.stdout
    assert not success.stderr
    assert invalid.exit_code == 2
    assert bad_config.exit_code == 2
    assert operational.exit_code == 1
    assert not operational.stdout
    assert "Error:" in operational.stderr
    assert "Traceback" not in operational.stderr


def test_info_and_validate_json_are_valid_and_stdout_only(tmp_path: Path):
    models = tmp_path / "models"
    _write_model(models)

    info = runner.invoke(app, ["info", str(models), "--json"])
    validation = runner.invoke(app, ["validate", str(models), "--json"])

    assert info.exit_code == 0
    assert json.loads(info.stdout)["models"][0]["name"] == "orders"
    assert not info.stderr
    assert validation.exit_code == 0
    assert json.loads(validation.stdout)["valid"] is True
    assert not validation.stderr


def test_validation_failure_keeps_valid_json_and_exit_one(tmp_path: Path):
    models = tmp_path / "models"
    _write_model(models, invalid=True)

    result = runner.invoke(app, ["validate", str(models), "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["valid"] is False
    assert payload["errors"]


def test_sql_stdin_and_query_stdout_dash(tmp_path: Path):
    models = tmp_path / "models"
    _write_model(models)
    database = tmp_path / "warehouse.duckdb"
    _write_db(database)

    rewritten = runner.invoke(
        app,
        ["rewrite", "-", "--models", str(models)],
        input="SELECT order_count, status FROM orders\n",
    )
    explained = runner.invoke(
        app,
        ["explain", "-", "--models", str(models)],
        input="SELECT order_count FROM orders\n",
    )
    queried = runner.invoke(
        app,
        ["query", "-", "--models", str(models), "--db", str(database), "--output", "-"],
        input="SELECT order_count FROM orders\n",
    )

    assert rewritten.exit_code == 0, rewritten.output
    assert "count" in rewritten.stdout.lower()
    assert explained.exit_code == 0, explained.output
    assert "rewritten_sql" in json.loads(explained.stdout)
    assert queried.exit_code == 0, queried.output
    assert "order_count" in queried.stdout
    assert "2" in queried.stdout
    assert not queried.stderr


def test_migration_stdin_json_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    models = tmp_path / "models"
    _write_model(models)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app,
        ["migrate", "check", "-", "--models", str(models), "--json"],
        input="SELECT COUNT(*) FROM orders;\n",
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["mode"] == "check"
    assert payload["report"]["total_queries"] == 1


def test_migration_generate_and_preagg_reports_are_json(tmp_path: Path):
    generated = tmp_path / "generated"
    migration = runner.invoke(
        app,
        ["migrate", "generate", "-", "--output", str(generated), "--json"],
        input="SELECT status, COUNT(*) AS order_count FROM orders GROUP BY status;\n",
    )
    query_log = tmp_path / "queries.sql"
    query_log.write_text(
        ("SELECT revenue FROM orders\n-- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status;\n")
        * 15
    )
    preagg = runner.invoke(app, ["preagg", "recommend", "--queries", str(query_log), "--json"])

    assert migration.exit_code == 0, migration.output
    migration_payload = json.loads(migration.stdout)
    assert migration_payload["mode"] == "generate"
    assert migration_payload["generated"]["models"] == 1
    assert preagg.exit_code == 0, preagg.output
    preagg_payload = json.loads(preagg.stdout)
    assert preagg_payload["summary"]["total_queries"] == 15
    assert len(preagg_payload["recommendations"]) == 1


def test_convert_can_stream_single_file_input_and_output():
    source = """models:
  - name: orders
    table: orders
    dimensions: []
    metrics:
      - name: order_count
        agg: count
"""

    result = runner.invoke(
        app,
        ["convert", "-", "--from", "sidemantic", "--to", "sidemantic", "--output", "-"],
        input=source,
    )

    assert result.exit_code == 0, result.output
    assert "name: orders" in result.stdout
    assert not result.stderr


@pytest.mark.parametrize("extension_arguments", [[], ["--source-extension", ".sql"]])
def test_convert_preserves_or_infers_sql_extension_for_stdin(extension_arguments: list[str]):
    source = "MODEL (name orders, table orders, primary_key id);\n"

    result = runner.invoke(
        app,
        [
            "convert",
            "-",
            "--from",
            "sidemantic",
            "--to",
            "sidemantic",
            "--output",
            "-",
            *extension_arguments,
        ],
        input=source,
    )

    assert result.exit_code == 0, result.output
    assert "name: orders" in result.stdout
    assert not result.stderr


def test_convert_renders_feature_only_fidelity_counts_details_and_readiness(tmp_path, monkeypatch):
    from sidemantic.fidelity import record_import_feature

    source = _write_model(tmp_path / "source")
    output = tmp_path / "converted.yml"

    class FakeGraph:
        models = {"orders": object()}

    def fake_convert(source_path, output_path, **kwargs):
        del source_path, kwargs
        Path(output_path).write_text("models: []\n")
        record_import_feature(
            "measure.percentile",
            "partial",
            detail="Imported as an approximate aggregate",
            source="orders.malloy",
            location="12",
        )
        return FakeGraph()

    monkeypatch.setattr("sidemantic.formats.convert_semantic_source", fake_convert)

    result = runner.invoke(app, ["convert", str(source), "--output", str(output), "--to", "sidemantic"])

    assert result.exit_code == 0, result.output
    assert "Import fidelity: 1 construct(s)" in result.stderr
    assert "1 partial" in result.stderr
    assert "measure.percentile: Imported as an approximate aggregate (orders.malloy:12)" in result.stderr
    assert "Import readiness: review_required" in result.stderr
    assert "0 construct" not in result.stderr


def test_convert_blocked_fidelity_preserves_forced_destination_and_exits_nonzero(tmp_path, monkeypatch):
    from sidemantic.fidelity import record_import_feature, record_import_note

    source = _write_model(tmp_path / "source")
    output = tmp_path / "converted.yml"
    output.write_text("known-good\n")

    class FakeGraph:
        models = {"orders": object()}

    def fake_convert(source_path, output_path, **kwargs):
        del source_path, kwargs
        Path(output_path).write_text("blocked-output\n")
        record_import_feature("query.nesting", "unsupported", detail="Cannot execute nested query")
        record_import_feature("source.filter", "rejected", detail="Unsafe to omit")
        record_import_note("future_construct", "Unknown compatibility state", severity="future_status")
        return FakeGraph()

    monkeypatch.setattr("sidemantic.formats.convert_semantic_source", fake_convert)

    result = runner.invoke(
        app,
        ["convert", str(source), "--output", str(output), "--to", "sidemantic", "--force"],
    )

    assert result.exit_code == 1, result.output
    assert output.read_text() == "known-good\n"
    assert "query.nesting: Cannot execute nested query" in result.stderr
    assert "source.filter: Unsafe to omit" in result.stderr
    assert "future_construct: Unknown compatibility state" in result.stderr
    assert "Import readiness: blocked" in result.stderr
    assert "Converted 1 model" not in result.stderr


def test_convert_legacy_review_only_note_still_writes_output(tmp_path, monkeypatch):
    from sidemantic.fidelity import record_import_note

    source = _write_model(tmp_path / "source")
    output = tmp_path / "converted.yml"

    class FakeGraph:
        models = {"orders": object()}

    def fake_convert(source_path, output_path, **kwargs):
        del source_path, kwargs
        Path(output_path).write_text("reviewed-output\n")
        record_import_note("legacy_case", "Dropped optional case", severity="dropped")
        return FakeGraph()

    monkeypatch.setattr("sidemantic.formats.convert_semantic_source", fake_convert)

    result = runner.invoke(app, ["convert", str(source), "--output", str(output), "--to", "sidemantic"])

    assert result.exit_code == 0, result.output
    assert output.read_text() == "reviewed-output\n"
    assert "legacy_case: Dropped optional case" in result.stderr
    assert "Import readiness: review_required" in result.stderr


def test_convert_force_directory_replaces_tree_without_stale_files(tmp_path, monkeypatch):
    source = _write_model(tmp_path / "source")
    output = tmp_path / "converted"
    output.mkdir()
    (output / "stale.yml").write_text("stale\n")
    (output / "replaced.yml").write_text("old\n")

    class FakeGraph:
        models = {"orders": object()}

    def fake_convert(source_path, output_path, **kwargs):
        del source_path, kwargs
        staged = Path(output_path)
        staged.mkdir()
        (staged / "replaced.yml").write_text("new\n")
        (staged / "fresh.yml").write_text("fresh\n")
        return FakeGraph()

    monkeypatch.setattr("sidemantic.formats.convert_semantic_source", fake_convert)

    result = runner.invoke(
        app,
        ["convert", str(source), "--output", str(output), "--to", "rill", "--force"],
    )

    assert result.exit_code == 0, result.output
    assert sorted(path.name for path in output.iterdir()) == ["fresh.yml", "replaced.yml"]
    assert (output / "replaced.yml").read_text() == "new\n"
    assert not list(tmp_path.glob(".sidemantic-promote-*"))


def test_convert_force_directory_promotion_failure_restores_exact_prior_tree(tmp_path, monkeypatch):
    source = _write_model(tmp_path / "source")
    output = tmp_path / "converted"
    (output / "nested").mkdir(parents=True)
    (output / "old.yml").write_text("old\n")
    (output / "nested" / "kept.yml").write_text("kept\n")
    before = {path.relative_to(output): path.read_text() for path in output.rglob("*") if path.is_file()}

    class FakeGraph:
        models = {"orders": object()}

    def fake_convert(source_path, output_path, **kwargs):
        del source_path, kwargs
        staged = Path(output_path)
        staged.mkdir()
        (staged / "new.yml").write_text("new\n")
        return FakeGraph()

    original_replace = Path.replace

    def fail_new_tree_promotion(path, target):
        if path.name == "next" and Path(target) == output:
            raise OSError("injected directory promotion failure")
        return original_replace(path, target)

    monkeypatch.setattr("sidemantic.formats.convert_semantic_source", fake_convert)
    monkeypatch.setattr(Path, "replace", fail_new_tree_promotion)

    result = runner.invoke(
        app,
        ["convert", str(source), "--output", str(output), "--to", "rill", "--force"],
    )

    after = {path.relative_to(output): path.read_text() for path in output.rglob("*") if path.is_file()}
    assert result.exit_code == 1, result.output
    assert "injected directory promotion failure" in result.stderr
    assert after == before
    assert not (output / "new.yml").exists()
    assert not list(tmp_path.glob(".sidemantic-promote-*"))


def test_file_promotion_prepares_on_destination_filesystem_before_atomic_replace(tmp_path, monkeypatch):
    staged_root = tmp_path / "system-temp"
    staged_root.mkdir()
    staged = staged_root / "converted.yml"
    staged.write_text("new\n")
    destination_root = tmp_path / "destination"
    destination_root.mkdir()
    output = destination_root / "converted.yml"
    output.write_text("old\n")
    original_replace = Path.replace

    def reject_direct_cross_filesystem_replace(path, target):
        if path == staged:
            raise OSError("simulated EXDEV")
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", reject_direct_cross_filesystem_replace)

    cli_module._promote_converted_output(staged, output)

    assert output.read_text() == "new\n"
    assert staged.read_text() == "new\n"
    assert not list(destination_root.glob(".sidemantic-promote-*"))


def test_file_promotion_failure_preserves_existing_destination(tmp_path, monkeypatch):
    staged = tmp_path / "staged.yml"
    staged.write_text("new\n")
    destination_root = tmp_path / "destination"
    destination_root.mkdir()
    output = destination_root / "converted.yml"
    output.write_text("old\n")
    original_replace = Path.replace

    def fail_prepared_file_replace(path, target):
        if path.name == "next" and Path(target) == output:
            raise OSError("injected file promotion failure")
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", fail_prepared_file_replace)

    with pytest.raises(OSError, match="injected file promotion failure"):
        cli_module._promote_converted_output(staged, output)

    assert output.read_text() == "old\n"
    assert staged.read_text() == "new\n"
    assert not list(destination_root.glob(".sidemantic-promote-*"))


def test_generated_output_dash_writes_stdout(tmp_path: Path):
    models = tmp_path / "models"
    _write_model(models)

    result = runner.invoke(app, ["generate", "client", "--models", str(models), "--output", "-"])

    assert result.exit_code == 0, result.output
    assert "orders" in result.stdout
    assert not (tmp_path / "-").exists()
    assert not result.stderr


def test_debug_propagates_unexpected_exception(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    models = tmp_path / "models"
    _write_model(models)

    def explode(*_args, **_kwargs):
        raise RuntimeError("unexpected loader failure")

    monkeypatch.setattr(cli_module, "load_from_directory", explode)

    concise = runner.invoke(app, ["info", str(models)])
    debug = runner.invoke(app, ["--debug", "info", str(models)])

    assert concise.exit_code == 1
    assert "unexpected loader failure" in concise.stderr
    assert "Traceback" not in concise.output
    assert debug.exit_code == 1
    assert isinstance(debug.exception, RuntimeError)


def test_validate_debug_propagates_unexpected_exception(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    models = tmp_path / "models"
    _write_model(models)

    def explode(_directory):
        raise RuntimeError("unexpected validation failure")

    monkeypatch.setattr("sidemantic.validation_runner.validate_directory", explode)

    concise = runner.invoke(app, ["validate", str(models)])
    debug = runner.invoke(app, ["--debug", "validate", str(models)])

    assert concise.exit_code == 1
    assert "unexpected validation failure" in concise.stderr
    assert "Traceback" not in concise.output
    assert debug.exit_code == 1
    assert isinstance(debug.exception, RuntimeError)


def test_postgres_password_file_is_safe_and_deprecated_flag_is_hidden(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    ensure_fake_riffq()
    models = tmp_path / "models"
    _write_model(models)
    password_file = tmp_path / "pg-password"
    password_file.write_text("correct horse battery staple\n")
    captured: dict[str, object] = {}

    def fake_start_server(_layer, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("sidemantic.server.server.start_server", fake_start_server)

    result = runner.invoke(
        app,
        [
            "server",
            "postgres",
            str(models),
            "--username",
            "user",
            "--password-file",
            str(password_file),
        ],
    )
    help_result = runner.invoke(app, ["server", "postgres", "--help"])

    assert result.exit_code == 0, result.output
    assert captured["password"] == "correct horse battery staple"
    assert "correct horse battery staple" not in result.output
    assert "--password-file" in help_result.stdout
    assert "--password " not in help_result.stdout


def test_deprecated_secret_flag_warns_without_printing_or_leaking_value(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    ensure_fake_riffq()
    models = tmp_path / "models"
    _write_model(models)
    secret = "deprecated-secret-value"

    def explode(_layer, **_kwargs):
        raise RuntimeError(f"backend rejected {secret}")

    monkeypatch.setattr("sidemantic.server.server.start_server", explode)

    result = runner.invoke(
        app,
        ["server", "postgres", str(models), "--username", "user", "--password", secret],
    )

    assert result.exit_code == 1
    assert "--password is deprecated" in result.stderr
    assert "[REDACTED]" in result.stderr
    assert secret not in result.output


def test_api_auth_token_can_be_read_from_stdin(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    pytest.importorskip("fastapi")
    models = tmp_path / "models"
    _write_model(models)
    captured: dict[str, object] = {}

    def fake_start_api_server(_layer, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("sidemantic.api_server.start_api_server", fake_start_api_server)

    result = runner.invoke(
        app,
        ["server", "api", str(models), "--no-ui", "--auth-token-file", "-"],
        input="stdin-api-token\n",
    )

    assert result.exit_code == 0, result.output
    assert captured["auth_token"] == "stdin-api-token"
    assert "stdin-api-token" not in result.output
