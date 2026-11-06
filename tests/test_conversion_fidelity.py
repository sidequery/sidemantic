"""Cross-converter publication, preservation, and pinned core-subset regressions."""

import json
from pathlib import Path

import duckdb
import jsonschema
import pytest
import yaml
from typer.testing import CliRunner

from sidemantic import Dimension, Model
from sidemantic.adapters.ossie import OssieAdapter
from sidemantic.cli import app
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.fidelity import (
    ConversionFidelityError,
    ExportReport,
    capture_export_report,
    capture_import_report,
    record_export_feature,
    record_export_note,
    record_import_feature,
)
from sidemantic.formats import convert_semantic_source, export_semantic_graph, get_semantic_format, load_semantic_source
from sidemantic.interchange.ossie.compatibility import for_official_converters
from sidemantic.interchange.ossie.documents import OssieLogicalDocument
from sidemantic.interchange.ossie.profiles import LATEST_OSSIE_SCHEMA_COMMIT, OssieSerialization

RUNNER = CliRunner()
FIXTURES = Path(__file__).parent / "fixtures" / "converter_interop"


def expression(sql):
    return {"dialects": [{"dialect": "ANSI_SQL", "expression": sql}]}


def document():
    return {
        "version": "0.2.0.dev0",
        "name": "sales",
        "datasets": [
            {
                "name": "orders",
                "source": "orders",
                "fields": [{"name": "amount", "datatype": "Decimal", "expression": expression("amount")}],
                "metrics": [{"name": "revenue", "expression": expression("SUM(amount)")}],
            }
        ],
    }


def native_graph():
    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="orders", dimensions=[Dimension(name="amount", type="numeric")]))
    return graph


def write_document(tmp_path, data=None):
    path = tmp_path / "input.ossie.yaml"
    path.write_text(yaml.safe_dump(data or document(), sort_keys=False))
    return path


def test_export_captures_are_nested_and_independent_of_import():
    with capture_import_report() as imports, capture_export_report() as outer:
        with capture_export_report() as inner:
            record_export_feature("metric.sum", "exact")
            record_export_note("format.label", "Label omitted")
        record_export_feature("metric.window", "unsupported", detail="Not supported")
    assert imports.to_dict()["notes"] == []
    assert imports.features == []
    assert inner.readiness == "review_required"
    assert outer.is_blocked
    assert outer.feature_counts() == {"exact": 1, "unsupported": 1}
    assert ExportReport.from_dict(outer.to_dict()).to_dict() == outer.to_dict()


@pytest.mark.parametrize("blocked", ["unsupported", "rejected"])
def test_shared_export_does_not_publish_blocked_adapter_output(tmp_path, monkeypatch, blocked):
    destination = tmp_path / "result.yaml"
    destination.write_text("prior valid output")

    class Adapter:
        def export(self, graph, output):
            output.write_text("partial result")
            record_export_feature("security.filter", blocked, detail="Cannot preserve scope")

    monkeypatch.setattr("sidemantic.formats.SemanticFormat.create_adapter", lambda *a, **kw: Adapter())
    with capture_export_report() as report, pytest.raises(ConversionFidelityError, match="Cannot preserve scope"):
        export_semantic_graph(native_graph(), destination)
    assert report.is_blocked
    assert destination.read_text() == "prior valid output"


@pytest.mark.parametrize("target,suffix", [("bsl", ".yml"), ("superset", ".yaml"), ("thoughtspot", ".table.tml")])
@pytest.mark.parametrize("entrypoint", ["api", "cli"])
@pytest.mark.parametrize("blocked", [False, True])
def test_existing_suffixed_directory_keeps_shape_and_publication_gate(
    tmp_path, monkeypatch, target, suffix, entrypoint, blocked
):
    output = tmp_path / "datasets.v1"
    (output / "nested").mkdir(parents=True)
    (output / "stale.yml").write_bytes(b"prior output\n")
    (output / "nested" / "keep.yml").write_bytes(b"preserve on failure\n")
    before = {path.relative_to(output): path.read_bytes() for path in output.rglob("*") if path.is_file()}
    graph = native_graph()
    graph.add_model(Model(name="customers", table="customers"))

    if blocked:
        adapter_type = type(get_semantic_format(target).create_adapter())
        original_export = adapter_type.export

        def blocked_export(self, graph, staged):
            original_export(self, graph, staged)
            record_export_feature("test.blocked", "rejected", detail="Injected export loss")

        monkeypatch.setattr(adapter_type, "export", blocked_export)

    if entrypoint == "cli":
        source = tmp_path / "source.yml"
        get_semantic_format("sidemantic").create_adapter().export(graph, source)
        result = RUNNER.invoke(app, ["convert", str(source), "--to", target, "--output", str(output), "--force"])
        assert result.exit_code == (1 if blocked else 0), result.output
        if blocked:
            assert "Injected export loss" in result.stderr
    elif blocked:
        with pytest.raises(ConversionFidelityError, match="Injected export loss"):
            export_semantic_graph(graph, output, target_format=target)
    else:
        export_semantic_graph(graph, output, target_format=target)

    after = {path.relative_to(output): path.read_bytes() for path in output.rglob("*") if path.is_file()}
    assert output.is_dir()
    if blocked:
        assert after == before
    else:
        assert set(after) == {Path(f"{name}{suffix}") for name in graph.models}
        assert all(after.values())
    assert not list(tmp_path.glob(".sidemantic-promote-*"))


@pytest.mark.parametrize("contents", ["", " \n", "{}", "[]", "models: []", "datasets: []"])
def test_shared_export_rejects_empty_output(tmp_path, monkeypatch, contents):
    class Adapter:
        def export(self, graph, output):
            output.write_text(contents)

    monkeypatch.setattr("sidemantic.formats.SemanticFormat.create_adapter", lambda *a, **kw: Adapter())
    output = tmp_path / "output.yaml"
    with pytest.raises(ValueError, match="no non-empty model output"):
        export_semantic_graph(native_graph(), output)
    assert not output.exists()


@pytest.mark.parametrize("target", ["lookml", "looker", "malloy"])
def test_native_language_cli_default_output_is_not_parsed_as_yaml(tmp_path, monkeypatch, target):
    monkeypatch.chdir(tmp_path)
    source = tmp_path / "input.yml"
    source.write_text("models: [{name: orders, table: orders}]")
    result = RUNNER.invoke(app, ["convert", str(source), "--to", target])
    assert result.exit_code == 0, result.output
    output = tmp_path / f"converted.{target}.yml"
    assert output.is_file()
    assert "orders" in output.read_text()


@pytest.mark.parametrize("target", ["lookml", "malloy"])
def test_native_language_shared_export_ignores_yaml_filename(tmp_path, target):
    output = tmp_path / "output.yaml"
    export_semantic_graph(native_graph(), output, target_format=target)
    assert "orders" in output.read_text()


def test_native_language_empty_output_still_rejected(tmp_path, monkeypatch):
    class Adapter:
        def export(self, graph, output):
            output.write_text(" \n")

    monkeypatch.setattr("sidemantic.formats.SemanticFormat.create_adapter", lambda *a, **kw: Adapter())
    with pytest.raises(ValueError, match="no non-empty model output"):
        export_semantic_graph(native_graph(), tmp_path / "output.yml", target_format="lookml")


def test_shared_convert_blocks_import_before_export(tmp_path, monkeypatch):
    def load(*args, **kwargs):
        record_import_feature("join", "rejected", detail="Unsafe join")
        return native_graph()

    monkeypatch.setattr("sidemantic.formats.load_semantic_source", load)
    monkeypatch.setattr("sidemantic.formats.export_semantic_graph", lambda *a, **kw: pytest.fail("export must not run"))
    with pytest.raises(ConversionFidelityError, match="Unsafe join"):
        convert_semantic_source(tmp_path / "input", tmp_path / "output")


def test_cli_reports_export_partial_and_blocks_unsupported(tmp_path, monkeypatch):
    source = tmp_path / "input.yaml"
    source.write_text("models: [{name: orders, table: orders}]")
    output = tmp_path / "output.yaml"
    output.write_text("prior output")
    status = "partial"

    def convert(source, staged, **kwargs):
        staged.write_text("models: [{name: orders, table: orders}]")
        record_export_feature("metric", status, detail="Not fully representable")
        return native_graph()

    monkeypatch.setattr("sidemantic.formats.convert_semantic_source", convert)
    args = ["convert", str(source), "--output", str(output), "--force"]
    result = RUNNER.invoke(app, args)
    assert result.exit_code == 0, result.output
    assert "Export readiness: review_required" in result.stderr
    before = output.read_bytes()
    status = "unsupported"
    result = RUNNER.invoke(app, args)
    assert result.exit_code == 1, result.output
    assert "Export readiness: blocked" in result.stderr
    assert output.read_bytes() == before


def test_official_subset_hoists_dataset_metrics_and_preserves_aggregate_binding(tmp_path):
    source = write_document(tmp_path)
    output = tmp_path / "portable.yaml"
    with capture_export_report() as report:
        convert_semantic_source(
            source,
            output,
            source_format="ossie",
            target_format="ossie",
            target_export_options={
                "scope_name": "sales",
                "expression_dialect": "ANSI_SQL",
                "official_compatible": True,
            },
        )
    data = yaml.safe_load(output.read_text())
    jsonschema.validate(data, json.loads((FIXTURES / "official-core-01a5488.schema.json").read_text()))
    assert "metrics" not in data["datasets"][0]
    assert data["metrics"][0]["name"] == "revenue"
    sql = data["metrics"][0]["expression"]["dialects"][0]["expression"]
    assert sql == "SUM(orders.amount)"
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders(amount DECIMAL(12, 2))")
        connection.execute("INSERT INTO orders VALUES (10), (20), (NULL)")
        assert connection.execute(f"SELECT {sql} FROM orders").fetchone()[0] == 30
    assert report.feature_counts() == {"exact": 1}


@pytest.mark.parametrize("mode", ["collision", "unbound_count", "mixed_count", "mixed_constant"])
def test_official_subset_rejects_ambiguous_hoisting(mode):
    data = document()
    if mode == "collision":
        data["metrics"] = [{"name": "revenue", "expression": expression("SUM(orders.amount)")}]
    else:
        sql = {
            "unbound_count": "COUNT(*)",
            "mixed_count": "SUM(amount) / COUNT(*)",
            "mixed_constant": "SUM(amount) + SUM(1)",
        }[mode]
        data["datasets"][0]["metrics"][0]["expression"] = expression(sql)
    doc = OssieLogicalDocument(
        canonical_data=data, serialization=OssieSerialization.YAML, schema_revision=LATEST_OSSIE_SCHEMA_COMMIT
    )
    with pytest.raises(ValueError, match="unique model metric names|without a field binding"):
        for_official_converters(doc)


def test_official_subset_refuses_runtime_extension_without_replacing_destination(tmp_path):
    graph = native_graph()
    graph.models["orders"].invariant_filters = ["amount > 0"]
    output = tmp_path / "output.yaml"
    output.write_text("prior output")
    with pytest.raises(ValueError, match="invariant_filters|row scope"):
        export_semantic_graph(
            graph,
            output,
            target_format="ossie",
            export_options={
                "scope_name": "sales",
                "expression_dialect": "ANSI_SQL",
                "official_compatible": True,
            },
        )
    assert output.read_text() == "prior output"


def test_preservation_keeps_foreign_metadata_while_graph_projection_reports_loss(tmp_path):
    data = document()
    data["ai_context"] = {"synonyms": ["sales facts"]}
    data["custom_extensions"] = [{"vendor_name": "OTHER", "data": '{"keep":true}'}]
    source = write_document(tmp_path, data)
    with capture_import_report() as report:
        load_semantic_source(source, source_format="ossie")
    assert {note.construct for note in report.notes} == {"ossie.ai_context", "ossie.custom_extensions"}
    output = tmp_path / "preserved.yaml"
    result = RUNNER.invoke(
        app,
        [
            "convert",
            str(source),
            "--from",
            "ossie",
            "--to",
            "ossie",
            "--output",
            str(output),
            "--ossie-preserve-document",
        ],
    )
    assert result.exit_code == 0, result.output
    assert output.read_bytes() == source.read_bytes()
    assert "Document fidelity: exact" in result.stderr
    converted = tmp_path / "preserved.json"
    OssieAdapter().preserve_document(source, converted)
    assert json.loads(converted.read_text()) == data


def test_cli_source_dialect_accepts_quoted_physical_source(tmp_path):
    data = document()
    data["datasets"][0]["source"] = "`analytics.prod`.`sales.area`.`Orders`"
    source = write_document(tmp_path, data)
    output = tmp_path / "native.yaml"
    args = ["convert", str(source), "--from", "ossie", "--output", str(output), "--ossie-target-dialect", "bigquery"]
    failed = RUNNER.invoke(app, args)
    assert failed.exit_code == 1
    result = RUNNER.invoke(app, [*args, "--ossie-source-dialect", "bigquery"])
    assert result.exit_code == 0, result.output
    assert "analytics.prod" in output.read_text()


def test_cli_official_compatible(tmp_path):
    source = write_document(tmp_path)
    output = tmp_path / "portable.yaml"
    result = RUNNER.invoke(
        app,
        [
            "convert",
            str(source),
            "--from",
            "ossie",
            "--to",
            "ossie",
            "--output",
            str(output),
            "--ossie-scope",
            "sales",
            "--ossie-expression-dialect",
            "ANSI_SQL",
            "--ossie-official-compatible",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "metrics" not in yaml.safe_load(output.read_text())["datasets"][0]


def test_auto_projection_also_reports_foreign_metadata(tmp_path):
    data = document()
    data["custom_extensions"] = [{"vendor_name": "OTHER", "data": "opaque"}]
    source = write_document(tmp_path, data)
    with capture_import_report() as report:
        load_semantic_source(source)
    assert any(note.construct == "ossie.custom_extensions" for note in report.notes)


def test_latest_sql_expression_exports_to_pinned_official_dialect_form(tmp_path):
    data = document()
    data["datasets"][0]["fields"][0]["expression"] = {"sql": "amount"}
    data["datasets"][0]["metrics"][0]["expression"] = {"sql": "SUM(amount)"}
    source = write_document(tmp_path, data)
    output = tmp_path / "compat.yaml"
    convert_semantic_source(
        source,
        output,
        source_format="ossie",
        target_format="ossie",
        target_export_options={
            "scope_name": "sales",
            "expression_dialect": "ANSI_SQL",
            "official_compatible": True,
        },
    )
    result = yaml.safe_load(output.read_text())
    jsonschema.validate(result, json.loads((FIXTURES / "official-core-01a5488.schema.json").read_text()))
    assert result["metrics"][0]["expression"]["dialects"][0]["expression"] == "SUM(orders.amount)"


def test_official_subset_rejects_forward_only_relationship_metadata(tmp_path):
    from sidemantic import Relationship

    graph = native_graph()
    graph.add_model(
        Model(name="customers", table="customers", primary_key="id", dimensions=[Dimension(name="id", type="numeric")])
    )
    graph.models["orders"].relationships.append(
        Relationship(
            name="customers",
            edge_id="orders_customer",
            type="many_to_one",
            foreign_key="amount",
            primary_key="id",
            metadata={"reversible": False},
        )
    )
    output = tmp_path / "out.yaml"
    with pytest.raises(ValueError, match="metadata"):
        export_semantic_graph(
            graph,
            output,
            target_format="ossie",
            export_options={
                "scope_name": "sales",
                "expression_dialect": "ANSI_SQL",
                "official_compatible": True,
            },
        )
    assert not output.exists()


def test_cli_preservation_rejects_invalid_input_without_replacing_destination(tmp_path):
    source = write_document(tmp_path, {"version": "0.2.0.dev0", "name": "sales", "datasets": []})
    output = tmp_path / "preserved.yaml"
    output.write_text("prior output")
    result = RUNNER.invoke(
        app,
        [
            "convert",
            str(source),
            "--from",
            "ossie",
            "--to",
            "ossie",
            "--output",
            str(output),
            "--force",
            "--ossie-preserve-document",
        ],
    )
    assert result.exit_code == 1
    assert output.read_text() == "prior output"


def test_preservation_rejects_duplicate_fields_without_runtime_lowering(tmp_path):
    data = document()
    data["datasets"][0]["fields"].append(dict(data["datasets"][0]["fields"][0]))
    source = write_document(tmp_path, data)
    output = tmp_path / "preserved.yaml"
    output.write_text("prior output")
    result = RUNNER.invoke(
        app,
        [
            "convert",
            str(source),
            "--from",
            "ossie",
            "--to",
            "ossie",
            "--output",
            str(output),
            "--force",
            "--ossie-preserve-document",
        ],
    )
    assert result.exit_code == 1
    assert "field.name_duplicate" in result.stderr
    assert output.read_text() == "prior output"


@pytest.mark.parametrize("official_compatible", [False, True])
def test_official_subset_reports_unspecified_datatype_without_guessing(tmp_path, official_compatible):
    graph = native_graph()
    output = tmp_path / "output.yaml"
    with capture_export_report() as report:
        export_semantic_graph(
            graph,
            output,
            target_format="ossie",
            export_options={
                "scope_name": "sales",
                "expression_dialect": "ANSI_SQL",
                "official_compatible": official_compatible,
            },
        )
    field = yaml.safe_load(output.read_text())["datasets"][0]["fields"][0]
    assert "datatype" not in field
    missing = [feature for feature in report.features if feature.feature == "ossie.datatype_unspecified"]
    if official_compatible:
        assert report.readiness == "review_required"
        assert len(missing) == 1
        assert missing[0].status == "partial"
        assert "orders.amount" in missing[0].detail
        assert "default it to string" in missing[0].detail
        assert missing[0].location == "/datasets/0/fields/0/datatype"
    else:
        assert report.is_ready
        assert missing == []


def test_official_subset_preserves_explicit_numeric_datatype_without_warning(tmp_path):
    graph = native_graph()
    graph.models["orders"].dimensions[0].logical_data_type = "Decimal"
    output = tmp_path / "typed.yaml"
    with capture_export_report() as report:
        export_semantic_graph(
            graph,
            output,
            target_format="ossie",
            export_options={
                "scope_name": "sales",
                "expression_dialect": "ANSI_SQL",
                "official_compatible": True,
            },
        )
    assert yaml.safe_load(output.read_text())["datasets"][0]["fields"][0]["datatype"] == "Decimal"
    assert report.is_ready
    assert not any(feature.feature == "ossie.datatype_unspecified" for feature in report.features)


def test_cli_official_compatible_surfaces_numeric_datatype_risk(tmp_path):
    source = tmp_path / "native.yaml"
    source.write_text("models: [{name: orders, table: orders, dimensions: [{name: amount, type: numeric}]}]")
    output = tmp_path / "portable.yaml"
    result = RUNNER.invoke(
        app,
        [
            "convert",
            str(source),
            "--from",
            "sidemantic",
            "--to",
            "ossie",
            "--output",
            str(output),
            "--ossie-scope",
            "sales",
            "--ossie-expression-dialect",
            "ANSI_SQL",
            "--ossie-official-compatible",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "orders.amount" in result.stderr
    assert "logical_data_type" in result.stderr
    assert "Export readiness: review_required" in result.stderr
