from pathlib import Path

import pytest

from sidemantic.formats import (
    OutputKind,
    UnknownFormatError,
    UnsupportedFormatOperationError,
    convert_semantic_source,
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
