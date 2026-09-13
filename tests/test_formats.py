from pathlib import Path

import pytest

from sidemantic.formats import (
    OutputKind,
    UnknownFormatError,
    UnsupportedFormatOperationError,
    convert_semantic_source,
    export_semantic_graph,
    get_semantic_format,
    load_semantic_source,
    semantic_formats,
)


def _native_model(name: str) -> str:
    return f"""version: 1
models:
  - name: {name}
    table: {name}
    primary_key: id
    dimensions:
      - name: id
        type: numeric
        sql: id
"""


def test_registry_has_stable_names_aliases_and_capabilities():
    names = [spec.name for spec in semantic_formats()]

    assert names == sorted(names)
    assert get_semantic_format("native").name == "sidemantic"
    assert get_semantic_format("cube_js").name == "cube"
    assert get_semantic_format("powerbi").name == "tmdl"
    assert get_semantic_format("ossie").name == "ossie"
    assert get_semantic_format("apache_ossie").name == "ossie"
    assert get_semantic_format("osi").name == "ossie"
    assert get_semantic_format("open-semantic-interchange").name == "ossie"
    assert get_semantic_format("rill").output_kind == OutputKind.DIRECTORY
    assert get_semantic_format("tableau").supports_export is False


def test_registry_reports_unknown_and_unsupported_formats():
    with pytest.raises(UnknownFormatError, match="Unknown semantic format 'wat'"):
        get_semantic_format("wat")

    with pytest.raises(UnsupportedFormatOperationError, match="supports import but not export"):
        get_semantic_format("tableau", operation="export")


def test_auto_file_load_is_exact_and_does_not_scan_siblings(tmp_path: Path):
    selected = tmp_path / "selected.yml"
    selected.write_text(_native_model("selected"))
    (tmp_path / "sibling.yml").write_text(_native_model("sibling"))

    graph = load_semantic_source(selected)

    assert set(graph.models) == {"selected"}


def test_explicit_native_format_rejects_directory(tmp_path: Path):
    with pytest.raises(ValueError, match="requires a file source"):
        load_semantic_source(tmp_path, source_format="native")


def test_explicit_native_alias_loads_exact_file(tmp_path: Path):
    source = tmp_path / "orders.yml"
    source.write_text(_native_model("orders"))

    graph = load_semantic_source(source, source_format="native")

    assert set(graph.models) == {"orders"}


def test_convert_auto_file_to_native_yaml(tmp_path: Path):
    source = tmp_path / "source.yml"
    output = tmp_path / "converted.yml"
    source.write_text(_native_model("orders"))

    graph = convert_semantic_source(source, output, target_format="native")

    assert set(graph.models) == {"orders"}
    assert "version: 1" in output.read_text()
    assert "name: orders" in output.read_text()


def test_explicit_ossie_format_uses_scoped_validated_importer(tmp_path: Path):
    source = tmp_path / "orders.ossie.yaml"
    source.write_text(
        """version: 0.2.0.dev0
semantic_model:
  - name: commerce
    datasets:
      - name: orders
        source: analytics.orders
"""
    )

    graph = load_semantic_source(
        source,
        source_format="ossie",
        adapter_options={"scope_id": "commerce", "target_dialect": "duckdb"},
    )

    assert graph.get_model("orders").table == "analytics.orders"


def test_ossie_graph_export_requires_and_accepts_explicit_synthesis_options(tmp_path: Path):
    source = tmp_path / "source.yml"
    output = tmp_path / "output.json"
    source.write_text(_native_model("orders"))
    graph = load_semantic_source(source, source_format="native")

    with pytest.raises(ValueError, match="scope_name"):
        export_semantic_graph(graph, output, target_format="ossie")

    export_semantic_graph(
        graph,
        output,
        target_format="ossie",
        export_options={"scope_name": "commerce", "expression_dialect": "ANSI_SQL"},
    )

    assert '"version": "0.2.0.dev0"' in output.read_text()
    assert '"name": "commerce"' in output.read_text()


def test_convert_plumbs_explicit_ossie_export_options(tmp_path: Path):
    source = tmp_path / "source.yml"
    output = tmp_path / "output.yaml"
    source.write_text(_native_model("orders"))

    convert_semantic_source(
        source,
        output,
        source_format="native",
        target_format="ossie",
        target_export_options={"scope_name": "commerce", "expression_dialect": "SNOWFLAKE"},
    )

    text = output.read_text()
    assert "version: 0.2.0.dev0" in text
    assert "dialect: SNOWFLAKE" in text
