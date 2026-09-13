"""Generated filenames must stay in the user-selected export directory."""

import pytest

from sidemantic import Model, SemanticLayer
from sidemantic.adapters.atscale_sml import AtScaleSMLAdapter
from sidemantic.adapters.bsl import BSLAdapter
from sidemantic.adapters.gooddata import GoodDataAdapter
from sidemantic.adapters.hex import HexAdapter
from sidemantic.adapters.holistics import HolisticsAdapter
from sidemantic.adapters.omni import OmniAdapter
from sidemantic.adapters.rill import RillAdapter
from sidemantic.adapters.superset import SupersetAdapter
from sidemantic.adapters.thoughtspot import ThoughtSpotAdapter
from sidemantic.adapters.tmdl import TMDLAdapter
from sidemantic.core.metric import Metric
from sidemantic.core.migrator import Migrator
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.paths import output_child

ADAPTERS = [
    AtScaleSMLAdapter,
    BSLAdapter,
    HexAdapter,
    HolisticsAdapter,
    OmniAdapter,
    RillAdapter,
    SupersetAdapter,
    ThoughtSpotAdapter,
]


@pytest.mark.parametrize("adapter_class", ADAPTERS)
@pytest.mark.parametrize("name", ["../escape", "/absolute", "nested/name", "..\\escape", "C:escape"])
def test_export_rejects_semantic_path_names(tmp_path, adapter_class, name):
    graph = SemanticGraph()
    graph.add_model(Model(name=name, table="orders"))
    with pytest.raises(ValueError, match="Unsafe output filename"):
        adapter_class().export(graph, tmp_path / "output")
    assert not (tmp_path / "escape.yml").exists()


@pytest.mark.parametrize("adapter_class", ADAPTERS)
def test_export_preserves_ordinary_names(tmp_path, adapter_class):
    graph = SemanticGraph()
    graph.add_model(Model(name="sales", table="orders"))
    adapter_class().export(graph, tmp_path / "output")
    assert any((tmp_path / "output").rglob("sales.*"))


def test_metric_filename_cannot_escape(tmp_path):
    graph = SemanticGraph()
    graph.add_model(Model(name="sales", table="orders", metrics=[Metric(name="../../escape", agg="count")]))
    with pytest.raises(ValueError, match="Unsafe output filename"):
        AtScaleSMLAdapter().export(graph, tmp_path / "output")
    assert not (tmp_path / "escape.yml").exists()


@pytest.mark.parametrize(
    "method,values",
    [("write_model_files", {"../escape": {"table": "orders"}}), ("write_rewritten_queries", {"../escape": "select 1"})],
)
def test_migrator_rejects_paths(tmp_path, method, values):
    with pytest.raises(ValueError, match="Unsafe output filename"):
        getattr(Migrator(SemanticLayer()), method)(values, str(tmp_path / "output"))


def test_output_rejects_symlink_file_and_directory(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sales.yml"
    sentinel.write_text("untouched")
    output = tmp_path / "output"
    output.mkdir()
    (output / "sales.yml").symlink_to(sentinel)
    (output / "datasets").symlink_to(outside, target_is_directory=True)
    graph = SemanticGraph()
    graph.add_model(Model(name="sales", table="orders"))
    for adapter in [BSLAdapter(), AtScaleSMLAdapter()]:
        with pytest.raises(ValueError, match="escapes destination"):
            adapter.export(graph, output)
    assert sentinel.read_text() == "untouched"


def test_output_preserves_spaces_dots_and_explicit_destination(tmp_path):
    outside = tmp_path / "chosen"
    outside.mkdir()
    alias = tmp_path / "output"
    alias.symlink_to(outside, target_is_directory=True)
    path = output_child(alias, "Sales Revenue.v1.yml")
    path.write_text("ok")
    assert (outside / path.name).read_text() == "ok"


def test_rill_full_project_rejects_source_traversal(tmp_path):
    graph = SemanticGraph()
    graph.add_model(Model(name="../../escape", table="orders", source_uri="orders.csv"))
    with pytest.raises(ValueError, match="Unsafe output filename"):
        RillAdapter().export(graph, tmp_path / "output", full_project=True)


def test_tmdl_rejects_existing_output_symlink(tmp_path):
    output = tmp_path / "output"
    tables = output / "definition" / "tables"
    tables.mkdir(parents=True)
    sentinel = tmp_path / "sentinel.tmdl"
    sentinel.write_text("untouched")
    (tables / "Sales Revenue.tmdl").symlink_to(sentinel)
    graph = SemanticGraph()
    graph.add_model(Model(name="Sales Revenue", table="orders"))
    with pytest.raises(ValueError, match="escapes destination"):
        TMDLAdapter().export(graph, output)
    assert sentinel.read_text() == "untouched"


def test_graph_metrics_filename_stays_in_destination(tmp_path):
    migrator = Migrator(SemanticLayer())
    with pytest.raises(ValueError, match="Unsafe output filename"):
        migrator.write_graph_metrics_file([{"name": "revenue", "sql": "1"}], str(tmp_path / "output"), "../escape.yml")


def test_gooddata_generated_filename_rejects_symlink(tmp_path):
    output = tmp_path / "output"
    output.mkdir()
    sentinel = tmp_path / "external.json"
    sentinel.write_text("untouched")
    (output / "ldm.json").symlink_to(sentinel)
    graph = SemanticGraph()
    graph.add_model(Model(name="sales", table="orders"))
    with pytest.raises(ValueError, match="escapes destination"):
        GoodDataAdapter().export(graph, output)
    assert sentinel.read_text() == "untouched"

    # A user-selected file remains an explicit output destination.
    GoodDataAdapter().export(graph, sentinel)
    assert '"ldm"' in sentinel.read_text()
