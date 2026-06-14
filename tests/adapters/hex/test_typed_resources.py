"""Tests for current Hex Semantic Authoring YAML.

Covers the post-Aug-2025 schema: the top-level `type:` discriminator,
multi-document files separated by `---`, the `view` resource type, display
`name` labels, `visibility`, and object-form `semi_additive` measures.
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.hex import HexAdapter

FIXTURE = "tests/fixtures/hex/subscriptions_project.yml"


# =============================================================================
# MULTI-DOCUMENT + TYPED RESOURCE PARSING
# =============================================================================


class TestTypedMultiDocParsing:
    @pytest.fixture
    def graph(self):
        return HexAdapter().parse(FIXTURE)

    def test_multi_doc_yields_both_resources(self, graph):
        """A `---`-separated file yields every resource, not just the first."""
        assert "subscriptions" in graph.models
        assert "revenue_overview" in graph.models

    def test_typed_model_parsed(self, graph):
        """`type: model` is parsed as a model with its table."""
        model = graph.models["subscriptions"]
        assert model.table == "analytics.subscriptions"

    def test_model_visibility_in_meta(self, graph):
        """Model-level visibility is preserved in meta."""
        model = graph.models["subscriptions"]
        assert model.meta is not None
        assert model.meta.get("visibility") == "public"

    def test_model_display_name_label(self, graph):
        """Model `name` maps to the display label on metadata."""
        model = graph.models["subscriptions"]
        assert (model.metadata or {}).get("label") == "Subscriptions"

    def test_dimension_display_name_label(self, graph):
        """Dimension `name` maps to the Sidemantic display label."""
        model = graph.models["subscriptions"]
        assert model.get_dimension("plan").label == "Plan Tier"
        assert model.get_dimension("customer_id").label == "Customer"

    def test_measure_display_name_label(self, graph):
        """Measure `name` maps to the Sidemantic display label."""
        model = graph.models["subscriptions"]
        assert model.get_metric("total_mrr").label == "Total MRR"
        assert model.get_metric("current_mrr").label == "Current MRR"

    def test_dimension_visibility_internal(self, graph):
        """`visibility: internal` hides the dimension and is recorded in meta."""
        model = graph.models["subscriptions"]
        dim = model.get_dimension("internal_notes")
        assert dim.public is False
        assert dim.meta.get("visibility") == "internal"

    def test_dimension_visibility_private(self, graph):
        """`visibility: private` hides the dimension."""
        model = graph.models["subscriptions"]
        dim = model.get_dimension("secret_token")
        assert dim.public is False
        assert dim.meta.get("visibility") == "private"

    def test_dimension_visibility_public_default(self, graph):
        """Dimensions without a visibility stay public."""
        model = graph.models["subscriptions"]
        assert model.get_dimension("plan").public is True

    def test_measure_visibility_internal(self, graph):
        """`visibility: internal` hides the measure."""
        model = graph.models["subscriptions"]
        m = model.get_metric("internal_mrr")
        assert m.public is False
        assert m.meta.get("visibility") == "internal"

    def test_semi_additive_object_form(self, graph):
        """Object-form `semi_additive.over[].dimension` maps to non_additive_dimension."""
        model = graph.models["subscriptions"]
        assert model.get_metric("current_mrr").non_additive_dimension == "snapshot_date"


# =============================================================================
# VIEW RESOURCE PARSING
# =============================================================================


class TestViewResource:
    @pytest.fixture
    def view(self):
        graph = HexAdapter().parse(FIXTURE)
        return graph.models["revenue_overview"]

    def test_view_recorded_as_view(self, view):
        """View resources are tagged so they round-trip back to `type: view`."""
        assert view.meta.get("hex_resource_type") == "view"

    def test_view_base_reference(self, view):
        """View `base` model reference is preserved."""
        assert view.meta.get("base") == "subscriptions"

    def test_view_contents_preserved(self, view):
        """View `contents` groups are preserved verbatim."""
        contents = view.meta.get("contents")
        assert contents is not None
        assert contents[0]["name"] == "Revenue"
        assert "total_mrr" in contents[0]["measures"]

    def test_view_label_and_description(self, view):
        """View display name and description are preserved."""
        assert view.description == "Curated revenue entrypoint"
        assert (view.metadata or {}).get("label") == "Revenue Overview"

    def test_view_has_no_table(self, view):
        """Views are not backed by a table or SQL of their own."""
        assert view.table is None
        assert view.sql is None


# =============================================================================
# TYPE DISCRIMINATOR / BACKWARD COMPATIBILITY
# =============================================================================


def test_explicit_type_model():
    """`type: model` is accepted explicitly."""
    hex_def = {
        "id": "m",
        "type": "model",
        "base_sql_table": "t",
        "dimensions": [{"id": "id", "type": "number", "unique": True}],
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)
    try:
        graph = HexAdapter().parse(temp_path)
        assert "m" in graph.models
        assert graph.models["m"].table == "t"
    finally:
        temp_path.unlink()


def test_legacy_untyped_model_still_parses():
    """Legacy single-doc files without a `type` are still treated as models."""
    hex_def = {
        "id": "legacy",
        "base_sql_table": "t",
        "dimensions": [{"id": "id", "type": "number", "unique": True}],
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(hex_def, f)
        temp_path = Path(f.name)
    try:
        graph = HexAdapter().parse(temp_path)
        assert "legacy" in graph.models
    finally:
        temp_path.unlink()


# =============================================================================
# EXPORT / ROUNDTRIP OF NEW FEATURES
# =============================================================================


def test_export_emits_type_discriminator():
    """Exported models carry the required `type: model` discriminator."""
    adapter = HexAdapter()
    graph = adapter.parse(FIXTURE)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)
    try:
        # Export just the model (single-file export writes the first model).
        adapter.export(graph, temp_path)
        with open(temp_path) as fh:
            data = yaml.safe_load(fh)
        assert data["type"] == "model"
    finally:
        temp_path.unlink(missing_ok=True)


def test_roundtrip_view_and_typed_features(tmp_path):
    """Typed model + view survive an import/export/import roundtrip to a directory."""
    adapter = HexAdapter()
    graph1 = adapter.parse(FIXTURE)

    out_dir = tmp_path / "hex_out"
    adapter.export(graph1, out_dir)

    graph2 = adapter.parse(out_dir)

    # Model survives with label, visibility, and semi-additive.
    model = graph2.models["subscriptions"]
    assert model.meta.get("visibility") == "public"
    assert model.get_dimension("plan").label == "Plan Tier"
    assert model.get_metric("current_mrr").non_additive_dimension == "snapshot_date"
    assert model.get_dimension("internal_notes").public is False
    assert model.get_metric("internal_mrr").public is False

    # View survives as a view with its base + contents.
    view = graph2.models["revenue_overview"]
    assert view.meta.get("hex_resource_type") == "view"
    assert view.meta.get("base") == "subscriptions"
    assert view.meta.get("contents")[0]["name"] == "Revenue"


def test_export_view_emits_type_view(tmp_path):
    """A view model exports back to a `type: view` resource file."""
    adapter = HexAdapter()
    graph = adapter.parse(FIXTURE)

    out_dir = tmp_path / "hex_out"
    adapter.export(graph, out_dir)

    with open(out_dir / "revenue_overview.yml") as fh:
        data = yaml.safe_load(fh)

    assert data["type"] == "view"
    assert data["base"] == "subscriptions"
    assert data["name"] == "Revenue Overview"
    assert data["contents"][0]["measures"] == ["total_mrr", "current_mrr"]


def test_semi_additive_pick_and_groupings_preserved_in_meta():
    """The full object-form `semi_additive` config is stashed in measure meta."""
    doc = {
        "type": "model",
        "id": "balances",
        "sql_table": "analytics.balances",
        "measures": [
            {
                "id": "opening_balance",
                "func": "sum",
                "of": "amount",
                "semi_additive": {
                    "over": [{"dimension": "snapshot_date", "pick": "min"}],
                    "groupings": ["account_id"],
                },
            }
        ],
    }
    adapter = HexAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        Path(f.name).write_text(yaml.safe_dump(doc))
        in_path = Path(f.name)
    try:
        graph = adapter.parse(in_path)
        metric = graph.models["balances"].get_metric("opening_balance")
        # The single non-additive dimension still maps through.
        assert metric.non_additive_dimension == "snapshot_date"
        # The full config (including the non-default `pick: min` and `groupings`)
        # is retained so it can be re-emitted on export.
        preserved = metric.meta["hex_semi_additive"]
        assert preserved["over"][0]["pick"] == "min"
        assert preserved["groupings"] == ["account_id"]
    finally:
        in_path.unlink(missing_ok=True)


def test_semi_additive_pick_min_survives_roundtrip(tmp_path):
    """`pick: min`/`groupings` survive parse -> export -> parse without corruption.

    Without preservation the export would default `pick` to `max` per the Hex
    spec, silently changing an opening-balance snapshot's semantics.
    """
    doc = {
        "type": "model",
        "id": "balances",
        "sql_table": "analytics.balances",
        "measures": [
            {
                "id": "opening_balance",
                "func": "sum",
                "of": "amount",
                "semi_additive": {
                    "over": [{"dimension": "snapshot_date", "pick": "min"}],
                    "groupings": ["account_id"],
                },
            }
        ],
    }
    adapter = HexAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        Path(f.name).write_text(yaml.safe_dump(doc))
        in_path = Path(f.name)
    out_dir = tmp_path / "hex_out"
    try:
        graph1 = adapter.parse(in_path)
        adapter.export(graph1, out_dir)

        with open(out_dir / "balances.yml") as fh:
            exported = yaml.safe_load(fh)
        measure = exported["measures"][0]
        assert measure["semi_additive"]["over"][0]["pick"] == "min"
        assert measure["semi_additive"]["groupings"] == ["account_id"]

        # And it re-imports identically.
        graph2 = adapter.parse(out_dir)
        metric = graph2.models["balances"].get_metric("opening_balance")
        assert metric.meta["hex_semi_additive"]["over"][0]["pick"] == "min"
    finally:
        in_path.unlink(missing_ok=True)


def test_semi_additive_inline_dimension_object(tmp_path):
    """`semi_additive.over[].dimension` as an inline Dimension object parses.

    The Hex spec allows the `over` dimension to be either a bare dimension id or
    an inline Dimension object (`{id: ..., type: ...}`). Sidemantic's
    `non_additive_dimension` is a plain string, so the inline object form must be
    reduced to its id; passing the dict through fails Pydantic validation and
    breaks the CLI load path for otherwise-valid snapshot measures.
    """
    doc = {
        "type": "model",
        "id": "balances",
        "sql_table": "analytics.balances",
        "measures": [
            {
                "id": "ending_balance",
                "func": "sum",
                "of": "amount",
                "semi_additive": {
                    "over": [
                        {
                            "dimension": {"id": "snapshot_date", "type": "date"},
                            "pick": "last",
                        }
                    ],
                    "groupings": ["account_id"],
                },
            }
        ],
    }
    adapter = HexAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        Path(f.name).write_text(yaml.safe_dump(doc))
        in_path = Path(f.name)
    out_dir = tmp_path / "hex_out"
    try:
        graph = adapter.parse(in_path)
        metric = graph.models["balances"].get_metric("ending_balance")
        # The inline dimension object is reduced to its id.
        assert metric.non_additive_dimension == "snapshot_date"
        # The full inline config still round-trips through preserved meta.
        adapter.export(graph, out_dir)
        with open(out_dir / "balances.yml") as fh:
            exported = yaml.safe_load(fh)
        measure = exported["measures"][0]
        assert measure["semi_additive"]["over"][0]["dimension"] == {"id": "snapshot_date", "type": "date"}
        assert measure["semi_additive"]["over"][0]["pick"] == "last"
        assert measure["semi_additive"]["groupings"] == ["account_id"]
    finally:
        in_path.unlink(missing_ok=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
