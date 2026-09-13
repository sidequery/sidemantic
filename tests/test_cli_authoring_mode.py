"""CLI authoring contracts reuse portable export representability."""

import json

import pytest
from typer.testing import CliRunner

import sidemantic.cli as cli_module
from sidemantic.cli import app

runner = CliRunner()
PORTABLE = ["--authoring-mode", "ossie-portable", "--ossie-expression-dialect", "ANSI_SQL"]


@pytest.fixture(autouse=True)
def reset_cli():
    cli_module._loaded_config = None
    cli_module._project_context = None
    yield
    cli_module._loaded_config = None
    cli_module._project_context = None


def write_source(tmp_path, *, extended=False):
    source = tmp_path / "orders.yml"
    source.write_text(
        """# Authoritative source stays byte-for-byte intact.
models:
  - name: orders
    table: orders
    dimensions:
      - {name: day, type: categorical}
    metrics:
      - name: balance
        agg: sum
        sql: amount
"""
        + ("        non_additive_dimension: day\n" if extended else "")
    )
    if extended:
        source.write_text(source.read_text().replace("type: categorical", "type: time, granularity: day"))
    return source


def test_portable_core_and_full_mode_preserve_source(tmp_path):
    source = write_source(tmp_path)
    original = source.read_bytes()
    for options in ([], ["--authoring-mode", "sidemantic"], PORTABLE):
        result = runner.invoke(app, ["validate", str(tmp_path), "--json", *options])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["valid"]
        if options == PORTABLE:
            assert any("Portable Ossie core representation validated" in item for item in payload["info"])
    assert source.read_bytes() == original
    assert list(tmp_path.iterdir()) == [source]


def test_native_extension_semantics_fail_portable_only(tmp_path):
    source = write_source(tmp_path, extended=True)
    original = source.read_bytes()
    full = runner.invoke(app, ["validate", str(tmp_path), "--json"])
    portable = runner.invoke(app, ["validate", str(tmp_path), "--json", *PORTABLE])
    assert full.exit_code == 0, full.output
    assert portable.exit_code == 1, portable.output
    errors = json.loads(portable.stdout)["errors"]
    assert any("ossie.synthesis." in error and "non_additive_dimension" in error for error in errors)
    assert source.read_bytes() == original


@pytest.mark.parametrize("options", [[], PORTABLE])
def test_invalid_source_stays_invalid(tmp_path, options):
    source = tmp_path / "orders.yml"
    source.write_text("models: [\n")
    result = runner.invoke(app, ["validate", str(tmp_path), "--json", *options])
    assert result.exit_code == 1
    assert not json.loads(result.stdout)["valid"]


@pytest.mark.parametrize(
    "options",
    [
        ["--authoring-mode", "unknown"],
        ["--authoring-mode", "ossie-portable"],
        ["--ossie-expression-dialect", "ANSI_SQL"],
    ],
)
def test_invalid_mode_options_are_usage_errors(tmp_path, options):
    write_source(tmp_path)
    result = runner.invoke(app, ["validate", str(tmp_path), *options])
    assert result.exit_code == 2, result.output


def test_unknown_profile_fails_with_canonical_diagnostic(tmp_path):
    write_source(tmp_path)
    result = runner.invoke(
        app, ["validate", str(tmp_path), "--json", *PORTABLE, "--ossie-consumer-profile", "future-profile"]
    )
    assert result.exit_code == 1, result.output
    assert any("ossie.synthesis.profile_unsupported" in error for error in json.loads(result.stdout)["errors"])


def test_preserved_external_expression_is_not_portable_execution(tmp_path):
    source = tmp_path / "model.ossie.yaml"
    source.write_text("""version: 0.2.0.dev0
semantic_model:
  - name: commerce
    datasets:
      - name: orders
        source: orders
        fields:
          - name: amount
            expression:
              dialects:
                - {dialect: SIGMA, expression: '[Amount]'}
""")
    from sidemantic.interchange.ossie import OssieParseOptions, parse_ossie_document

    original = source.read_bytes()
    parsed = parse_ossie_document(original, options=OssieParseOptions(validate_schema=True))
    assert parsed.valid, parsed.diagnostics
    result = runner.invoke(app, ["validate", str(tmp_path), "--json", *PORTABLE])
    assert result.exit_code == 1, result.output
    assert not json.loads(result.stdout)["valid"]
    assert source.read_bytes() == original


@pytest.mark.parametrize("extended", [False, True])
def test_ossie_source_uses_same_authoring_contract(tmp_path, extended):
    from sidemantic.adapters.ossie import OssieAdapter
    from sidemantic.adapters.sidemantic import SidemanticAdapter

    native_dir = tmp_path / "native"
    native_dir.mkdir()
    native = write_source(native_dir, extended=extended)
    ossie_dir = tmp_path / "ossie"
    ossie_dir.mkdir()
    source = ossie_dir / "model.ossie.json"
    graph = SidemanticAdapter().parse(native)
    OssieAdapter(export_scope_name="commerce", expression_dialect="ANSI_SQL").export(graph, source)
    original = source.read_bytes()
    result = runner.invoke(app, ["validate", str(ossie_dir), "--json", *PORTABLE])
    assert result.exit_code == (1 if extended else 0), result.output
    assert source.read_bytes() == original
