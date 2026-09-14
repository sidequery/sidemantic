"""Native consumption catalogs survive authoring, discovery and export."""

import json

import pytest
import yaml

from sidemantic import SemanticLayer
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.loaders import load_from_directory

CATALOGS = {
    "explores": [{"name": "sales", "model": "orders", "default_metrics": ["revenue"], "metadata": {"source": "view"}}],
    "saved_queries": [{"name": "top", "explore": "sales", "limit": 2, "metadata": {"source": "query"}}],
    "table_calculations": [{"name": "doubled", "type": "formula", "expression": "${revenue} * 2"}],
}
MODELS = {
    "models": [{"name": "orders", "table": "orders", "metrics": [{"name": "revenue", "agg": "sum", "sql": "amount"}]}]
}


def test_roundtrip_all_consumption_catalogs(tmp_path):
    source = tmp_path / "catalog.yml"
    source.write_text(yaml.safe_dump({**MODELS, **CATALOGS}))
    original = source.read_bytes()
    adapter = SidemanticAdapter()
    graph = adapter.parse(source)
    output = tmp_path / "export.yml"
    adapter.export(graph, output)
    restored = adapter.parse(output)
    for catalog in CATALOGS:
        assert getattr(restored, catalog) == getattr(graph, catalog)
    assert source.read_bytes() == original


@pytest.mark.parametrize("suffix", [".yml", ".json"])
def test_catalog_only_sidecar_loads_before_models(tmp_path, suffix):
    source = tmp_path / f"a_catalog{suffix}"
    source.write_text(json.dumps({"version": 1, **CATALOGS}))
    (tmp_path / "z_models.yml").write_text(yaml.safe_dump(MODELS))
    original = source.read_bytes()
    layer = SemanticLayer()
    load_from_directory(layer, tmp_path, strict=True)
    assert layer.graph.get_explore("sales").model == "orders"
    assert layer.graph.get_saved_query("top").explore == "sales"
    assert layer.graph.get_table_calculation("doubled").expression == "${revenue} * 2"
    assert source.read_bytes() == original


@pytest.mark.parametrize("catalog", list(CATALOGS))
def test_duplicate_catalog_is_rejected_within_file(tmp_path, catalog):
    source = tmp_path / "duplicate.yml"
    source.write_text(yaml.safe_dump({catalog: CATALOGS[catalog] * 2}))
    with pytest.raises(ValueError, match="already exists"):
        SidemanticAdapter().parse(source)


@pytest.mark.parametrize("catalog", list(CATALOGS))
def test_duplicate_catalog_is_rejected_across_files(tmp_path, catalog):
    for name in ("a.yml", "b.yml"):
        (tmp_path / name).write_text(yaml.safe_dump({catalog: CATALOGS[catalog]}))
    layer = SemanticLayer()
    with pytest.raises(ValueError, match=f"Duplicate {catalog}"):
        load_from_directory(layer, tmp_path, strict=True)
    assert not getattr(layer.graph, catalog)


@pytest.mark.parametrize("catalog", list(CATALOGS))
def test_unknown_catalog_fields_are_rejected(tmp_path, catalog):
    source = tmp_path / "invalid.yml"
    source.write_text(yaml.safe_dump({catalog: [{**CATALOGS[catalog][0], "typo": True}]}))
    with pytest.raises(ValueError, match="typo"):
        SidemanticAdapter().parse(source)


@pytest.mark.parametrize("definition", [{}, ["invalid"], [{"name": "missing_type"}]])
def test_malformed_catalog_is_rejected(tmp_path, definition):
    source = tmp_path / "invalid.yml"
    source.write_text(yaml.safe_dump({"table_calculations": definition}))
    with pytest.raises(ValueError):
        SidemanticAdapter().parse(source)


def test_sql_frontmatter_catalogs(tmp_path):
    source = tmp_path / "catalog.sql"
    source.write_text("---\n" + yaml.safe_dump({"version": 1, **CATALOGS}) + "---\n")
    graph = SidemanticAdapter().parse(source)
    assert graph.get_saved_query("top").explore == "sales"
    assert graph.get_table_calculation("doubled").type == "formula"


@pytest.mark.parametrize("content", ['{"unrelated": ', '{"hello": "world"}', "[1, 2]"])
def test_unrelated_json_is_ignored(tmp_path, content):
    (tmp_path / "unrelated.json").write_text(content)
    layer = SemanticLayer()
    load_from_directory(layer, tmp_path, strict=True)
    assert not layer.graph.models


@pytest.mark.parametrize("catalog", list(CATALOGS))
@pytest.mark.parametrize("suffix", [".yml", ".json"])
def test_malformed_catalog_file_is_reported(tmp_path, catalog, suffix):
    source = tmp_path / f"invalid{suffix}"
    source.write_text(f"{catalog}: [" if suffix == ".yml" else f'{{"{catalog}": [')
    with pytest.raises(ValueError):
        load_from_directory(SemanticLayer(), tmp_path, strict=True)
