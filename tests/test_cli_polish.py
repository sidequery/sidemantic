"""Behavioral contract tests for human-facing CLI polish."""

import inspect
import os
from io import StringIO
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from sidemantic.cli_polish import (
    DEPRECATIONS,
    SuggestionGroup,
    _model_names,
    complete_dashboard_spec,
    complete_model,
    complete_path,
    complete_semantic_format,
    complete_source_format,
    complete_target_format,
    emit_deprecation,
    emit_long_output,
    emit_next_step,
    invocation_requests_machine_output,
    recovery_hint,
    should_page,
)


def test_typer_completion_callbacks_use_supported_parameter_names() -> None:
    callbacks = (
        complete_semantic_format,
        complete_source_format,
        complete_target_format,
        complete_dashboard_spec,
        complete_path,
        complete_model,
    )

    for callback in callbacks:
        names = set(inspect.signature(callback).parameters)
        assert names <= {"ctx", "param", "args", "incomplete"}


class TTYBuffer(StringIO):
    def isatty(self) -> bool:
        return True


class PipeBuffer(StringIO):
    def isatty(self) -> bool:
        return False


def _suggestion_cli() -> click.Group:
    @click.group(cls=SuggestionGroup)
    def cli() -> None:
        pass

    @cli.command()
    def validate() -> None:
        click.echo("valid")

    return cli


def test_close_command_name_suggests_reviewable_correction() -> None:
    result = CliRunner().invoke(_suggestion_cli(), ["vlaidate"])

    assert result.exit_code == 2
    assert "Did you mean 'validate'?" in result.stderr
    assert "validate --help" in result.stderr


@pytest.mark.parametrize(
    "args",
    [
        ["--quiet", "vlaidate"],
        ["--format", "json", "vlaidate"],
        ["--format=jsonl", "vlaidate"],
        ["--plain", "vlaidate"],
    ],
)
def test_machine_invocations_do_not_add_command_hints(args: list[str]) -> None:
    result = CliRunner().invoke(_suggestion_cli(), args)

    assert result.exit_code == 2
    assert "Did you mean" not in result.stderr


@pytest.mark.parametrize(
    ("args", "expected"),
    [(["--json"], True), (["--format=csv"], True), (["--format", "table"], False), ([], False)],
)
def test_machine_output_detection(args: list[str], expected: bool) -> None:
    assert invocation_requests_machine_output(args) is expected


def test_deprecation_registry_has_versions_and_compatibility_targets() -> None:
    assert {item.legacy for item in DEPRECATIONS} >= {"migrator", "export-native", "tree", "gen"}
    assert all(item.deprecated_since and item.remove_in and item.replacement for item in DEPRECATIONS)
    assert {item.legacy for item in DEPRECATIONS if item.kind == "flag"} == {
        "dashboard serve --output-dir",
        "dashboard serve --warm-interaction-preaggregations",
        "server api --auth-token",
        "server postgres --password",
    }


def test_deprecation_registry_is_fully_documented() -> None:
    policy = (Path(__file__).parents[1] / "docs" / "cli-deprecations.md").read_text()

    for item in DEPRECATIONS:
        assert item.legacy in policy
        assert item.deprecated_since in policy
        assert item.remove_in in policy


def test_deprecation_warning_uses_diagnostic_seam_and_is_suppressible() -> None:
    messages: list[str] = []

    emit_deprecation("tree", human_output=True, emit_diagnostic=messages.append)
    emit_deprecation("migrator", human_output=False, emit_diagnostic=messages.append)

    assert len(messages) == 1
    assert "workbench" in messages[0]
    assert "0.12.0" in messages[0]


def test_next_steps_are_concise_and_suppressible() -> None:
    messages: list[str] = []

    emit_next_step("validate", human_output=True, emit_diagnostic=messages.append)
    emit_next_step("convert", human_output=False, emit_diagnostic=messages.append)

    assert messages == ["Next: inspect the model with 'sidemantic info'."]


@pytest.mark.parametrize(
    ("message", "fragment"),
    [
        ("Destination already exists: output.yml", "--force"),
        ("No models found", "sidemantic validate"),
        ("Unknown semantic format 'foo'", "help convert"),
    ],
)
def test_common_errors_offer_safe_corrective_actions(message: str, fragment: str) -> None:
    assert fragment in (recovery_hint(message) or "")


def test_unknown_errors_do_not_get_speculative_recovery() -> None:
    assert recovery_hint("database returned an unfamiliar protocol error") is None


def test_pager_requires_tty_human_output_and_long_content(monkeypatch: pytest.MonkeyPatch) -> None:
    text = "\n".join(str(index) for index in range(30))
    monkeypatch.setenv("TERM", "xterm-256color")

    assert should_page(text, stream=TTYBuffer(), human_output=True, terminal_height=10)
    assert not should_page(text, stream=PipeBuffer(), human_output=True, terminal_height=10)
    assert not should_page(text, stream=TTYBuffer(), human_output=False, terminal_height=10)
    assert not should_page("short", stream=TTYBuffer(), human_output=True, terminal_height=10)


def test_term_dumb_disables_pager(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TERM", "dumb")
    assert not should_page("one\ntwo\nthree", stream=TTYBuffer(), human_output=True, pager_enabled=True)


def test_redirected_long_output_is_written_directly(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = PipeBuffer()

    def fail_pager(*_args, **_kwargs) -> None:
        raise AssertionError("pager must not run for redirected output")

    monkeypatch.setattr(click, "echo_via_pager", fail_pager)
    emit_long_output("one\ntwo", stream=stream, human_output=True, pager_enabled=True)

    assert stream.getvalue() == "one\ntwo\n"


def test_sidemantic_pager_overrides_pager_only_during_page(monkeypatch: pytest.MonkeyPatch) -> None:
    selected: list[str | None] = []
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setenv("PAGER", "less")
    monkeypatch.setenv("SIDEMANTIC_PAGER", "most")
    monkeypatch.setattr(click, "echo_via_pager", lambda *_args, **_kwargs: selected.append(os.environ.get("PAGER")))

    emit_long_output("long", stream=TTYBuffer(), human_output=True, pager_enabled=True)

    assert selected == ["most"]
    assert os.environ["PAGER"] == "less"


def test_format_completion_filters_import_and_export_capabilities() -> None:
    source = click.Option(["--from", "source_format"])
    target = click.Option(["--to", "target_format"])
    ctx = click.Context(click.Command("convert"))

    source_values = [item.value for item in complete_semantic_format(ctx, source, "")]
    target_values = [item.value for item in complete_semantic_format(ctx, target, "")]

    assert source_values[0] == "auto"
    assert "graphene" in source_values
    assert "graphene" not in target_values
    assert "sidemantic" in target_values


def test_model_completion_reads_names_from_selected_project(tmp_path: Path) -> None:
    models = tmp_path / "models"
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
    command = click.Command("refresh")
    ctx = click.Context(command, info_name="refresh")
    ctx.params["project"] = tmp_path

    assert _model_names(ctx) == ["orders"]
    values = [item[0] for item in complete_model(ctx, "ord")]

    assert values == ["orders"]


def test_dashboard_completion_filters_unrelated_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "dashboard.yml").touch()
    (tmp_path / "dashboard.json").touch()
    (tmp_path / "notes.txt").touch()
    (tmp_path / "nested").mkdir()
    monkeypatch.chdir(tmp_path)
    ctx = click.Context(click.Command("validate"))

    values = complete_dashboard_spec(ctx, "d")
    directory_values = complete_dashboard_spec(ctx, "n")

    assert "dashboard.yml" in values
    assert "dashboard.json" in values
    assert "notes.txt" not in directory_values
    assert "nested/" in directory_values


def test_path_completion_includes_files_and_directories(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "source.yml").touch()
    (tmp_path / "src").mkdir()
    monkeypatch.chdir(tmp_path)
    ctx = click.Context(click.Command("convert"))

    values = complete_path(ctx, "s")

    assert values == ["source.yml", "src/"]
