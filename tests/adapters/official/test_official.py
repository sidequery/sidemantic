"""External CLI contracts, semantic refusal, and optional real offline bridges."""

import copy
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.adapters.official import LightdashAdapter, OfficialConverterError, SigmaAdapter
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.fidelity import capture_export_report
from sidemantic.interchange.ossie import OssieSynthesisError

LIGHTDASH = {
    "type": "model",
    "name": "orders",
    "sql_from": "orders",
    "primary_key": "id",
    "dimensions": [
        {"name": "id", "type": "number", "sql": "${TABLE}.id"},
        {"name": "region", "type": "string", "sql": "${TABLE}.region"},
        {"name": "amount", "type": "number", "sql": "${TABLE}.amount", "metrics": {"revenue": {"type": "sum"}}},
    ],
}
SIGMA = {
    "kind": "data-model",
    "name": "sales",
    "schemaVersion": 1,
    "pages": [
        {
            "id": "p",
            "name": "Sales",
            "elements": [
                {
                    "id": "orders",
                    "kind": "table",
                    "name": "orders",
                    "source": {"kind": "warehouse-table", "connectionId": "local", "path": ["orders"]},
                    "columns": [
                        {"id": "id", "name": "id", "formula": "[id]"},
                        {"id": "region", "name": "region", "formula": "[region]"},
                        {"id": "amount", "name": "amount", "formula": "[amount]", "format": {"kind": "number"}},
                    ],
                    "uniqueKeys": ["id"],
                    "metrics": [{"id": "revenue", "name": "revenue", "formula": "Sum([amount])"}],
                }
            ],
        }
    ],
}
OSSIE = {
    "version": "0.2.0.dev0",
    "name": "sales",
    "datasets": [
        {
            "name": "orders",
            "source": "orders",
            "primary_key": ["id"],
            "fields": [
                {
                    "name": name,
                    "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": name}]},
                    "dimension": {"is_time": False},
                }
                for name in ("id", "region", "amount")
            ],
        }
    ],
    "metrics": [
        {"name": "revenue", "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"}]}}
    ],
}


def write_native(tmp_path, vendor, document=None):
    path = tmp_path / ("model.json" if vendor == "sigma" else "model.yml")
    document = document if document is not None else (SIGMA if vendor == "sigma" else LIGHTDASH)
    path.write_text(json.dumps(document) if vendor == "sigma" else yaml.safe_dump(document))
    return path


def graph():
    result = SemanticGraph()
    result.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric"),
                Dimension(name="region", type="categorical"),
                Dimension(name="amount", type="numeric"),
            ],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    return result


def mock_cli(monkeypatch, *, document=OSSIE, stderr="", returncode=0):
    calls = []
    monkeypatch.setattr("sidemantic.adapters.official.shutil.which", lambda _: "/mock/official-cli")

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        output = Path(argv[argv.index("-o") + 1])
        output.write_text(yaml.safe_dump(document))
        return subprocess.CompletedProcess(argv, returncode, "", stderr)

    monkeypatch.setattr("sidemantic.adapters.official.subprocess.run", run)
    return calls


def test_external_prerequisite_is_explicit_and_never_installed(tmp_path, monkeypatch):
    path = write_native(tmp_path, "sigma")
    monkeypatch.setattr("sidemantic.adapters.official.shutil.which", lambda _: None)
    with pytest.raises(OfficialConverterError, match="does not install it automatically"):
        SigmaAdapter().parse(path)


def test_subprocess_is_argv_staged_and_bounded(tmp_path, monkeypatch):
    path = write_native(tmp_path, "sigma")
    calls = mock_cli(monkeypatch)
    result = SigmaAdapter(timeout=7).parse(path)
    assert len(result.models) == 1
    assert len(result.models["orders"].dimensions) == 3
    argv, kwargs = calls[0]
    assert argv[0] == "/mock/official-cli"
    assert argv[1] == "sigma-to-ossie"
    assert str(path) not in argv
    assert not kwargs.get("shell")
    assert kwargs["timeout"] == 7
    assert not Path(argv[argv.index("-i") + 1]).exists()


@pytest.mark.parametrize(
    "vendor,key",
    [
        ("sigma", "columnSecurities"),
        ("sigma", "filters"),
        ("lightdash", "sql_filter"),
        ("lightdash", "required_filters"),
        ("lightdash", "access"),
    ],
)
def test_native_filters_security_and_unknown_state_rejected_before_cli(tmp_path, monkeypatch, vendor, key):
    document = copy.deepcopy(SIGMA if vendor == "sigma" else LIGHTDASH)
    host = document["pages"][0]["elements"][0] if vendor == "sigma" else document
    host[key] = ["restricted"]
    path = write_native(tmp_path, vendor, document)
    calls = mock_cli(monkeypatch)
    adapter = SigmaAdapter() if vendor == "sigma" else LightdashAdapter(expression_dialect="ANSI_SQL")
    with pytest.raises(OfficialConverterError, match=key):
        adapter.parse(path)
    assert not calls


def test_lightdash_metric_filters_and_runtime_parameters_rejected(tmp_path, monkeypatch):
    document = copy.deepcopy(LIGHTDASH)
    document["dimensions"][2]["metrics"]["revenue"]["filters"] = [{"status": "active"}]
    calls = mock_cli(monkeypatch)
    with pytest.raises(OfficialConverterError, match="filters"):
        LightdashAdapter(expression_dialect="ANSI_SQL").parse(write_native(tmp_path, "lightdash", document))
    document = copy.deepcopy(LIGHTDASH)
    document["dimensions"][0]["sql"] = "${ld.user.id}"
    with pytest.raises(OfficialConverterError, match="user attributes"):
        LightdashAdapter(expression_dialect="ANSI_SQL").parse(write_native(tmp_path, "lightdash", document))
    assert not calls


def test_derived_sigma_source_is_rejected(tmp_path, monkeypatch):
    document = copy.deepcopy(SIGMA)
    document["pages"][0]["elements"][0]["source"]["kind"] = "sql"
    calls = mock_cli(monkeypatch)
    with pytest.raises(OfficialConverterError, match="warehouse-table"):
        SigmaAdapter().parse(write_native(tmp_path, "sigma", document))
    assert not calls


@pytest.mark.parametrize(
    "stderr", ["[WARNING] EXPRESSION_NOT_TRANSLATABLE: revenue — omitted", "[WARNING] NEW_LOSS: orders — missing"]
)
def test_upstream_semantic_warnings_fail_even_with_success_exit(tmp_path, monkeypatch, stderr):
    calls = mock_cli(monkeypatch, stderr=stderr)
    with pytest.warns(UserWarning, match="WARNING"), pytest.raises(OfficialConverterError, match="conversion issues"):
        SigmaAdapter().parse(write_native(tmp_path, "sigma"))
    assert len(calls) == 1


def test_lightdash_issue_contract_is_checked(tmp_path, monkeypatch):
    mock_cli(monkeypatch, stderr="ROW_FILTER_NOT_PORTABLE  (1 element)\n  Unsafe filter\n  orders\n1 issue(s)")
    with pytest.warns(UserWarning), pytest.raises(OfficialConverterError, match="ROW_FILTER_NOT_PORTABLE"):
        LightdashAdapter(expression_dialect="ANSI_SQL").parse(write_native(tmp_path, "lightdash"))


@pytest.mark.parametrize("document", [{}, {"version": "0.2.0.dev0", "datasets": []}])
def test_empty_upstream_success_is_rejected(tmp_path, monkeypatch, document):
    mock_cli(monkeypatch, document=document)
    with pytest.raises(OfficialConverterError, match="no executable datasets"):
        SigmaAdapter().parse(write_native(tmp_path, "sigma"))


def test_silent_member_loss_is_rejected(tmp_path, monkeypatch):
    document = copy.deepcopy(OSSIE)
    document["metrics"] = []
    mock_cli(monkeypatch, document=document)
    with pytest.raises(OfficialConverterError, match="member counts"):
        SigmaAdapter().parse(write_native(tmp_path, "sigma"))


def test_runtime_extension_output_is_rejected(tmp_path, monkeypatch):
    document = copy.deepcopy(OSSIE)
    document["custom_extensions"] = [{"vendor_name": "SIDEMANTIC", "data": "{}"}]
    mock_cli(monkeypatch, document=document)
    with pytest.raises(OfficialConverterError, match="runtime extensions"):
        SigmaAdapter().parse(write_native(tmp_path, "sigma"))


def test_timeout_and_process_failure_are_actionable(tmp_path, monkeypatch):
    source = write_native(tmp_path, "sigma")
    mock_cli(monkeypatch, returncode=2, stderr="bad syntax")
    with pytest.warns(UserWarning), pytest.raises(OfficialConverterError, match="exit 2.*bad syntax"):
        SigmaAdapter().parse(source)

    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])

    monkeypatch.setattr("sidemantic.adapters.official.subprocess.run", timeout)
    with pytest.raises(OfficialConverterError, match="60s timeout"):
        SigmaAdapter().parse(source)


def test_export_requires_explicit_actual_dialect(tmp_path):
    with pytest.raises(OfficialConverterError, match="expression-dialect"):
        SigmaAdapter().export(graph(), tmp_path / "result.json")
    with pytest.raises(OfficialConverterError, match="does not relabel"):
        SigmaAdapter(expression_dialect="SNOWFLAKE").export(graph(), tmp_path / "result.json")


def test_runtime_only_graph_refused_without_touching_destination(tmp_path, monkeypatch):
    source = graph()
    source.models["orders"].invariant_filters = ["region = 'private'"]
    calls = mock_cli(monkeypatch)
    destination = tmp_path / "result.json"
    destination.write_text("keep me")
    with capture_export_report() as report, pytest.raises(OssieSynthesisError):
        SigmaAdapter(expression_dialect="ANSI_SQL").export(source, destination)
    assert report.readiness == "blocked"
    assert destination.read_text() == "keep me"
    assert not calls


@pytest.mark.parametrize("vendor,adapter_type", [("lightdash", LightdashAdapter), ("sigma", SigmaAdapter)])
def test_real_offline_cli_import_export_and_query(tmp_path, vendor, adapter_type):
    executable = os.environ.get(f"SIDEMANTIC_TEST_{vendor.upper()}_CLI") or shutil.which(f"ossie-{vendor}")
    if not executable:
        pytest.skip(f"Install official ossie-{vendor} or set SIDEMANTIC_TEST_{vendor.upper()}_CLI")
    adapter = adapter_type(executable=executable, expression_dialect="ANSI_SQL")
    imported = adapter.parse(write_native(tmp_path, vendor))
    assert len(imported.models) == 1
    assert len(imported.models["orders"].dimensions) == 3
    assert len(imported.models["orders"].metrics) + len(imported.metrics) == 1
    layer = SemanticLayer()
    for model in imported.models.values():
        layer.add_model(model)
    for metric in imported.metrics.values():
        layer.add_metric(metric)
    layer.adapter.execute("CREATE TABLE orders (id INTEGER, region VARCHAR, amount DECIMAL(12,2))")
    layer.adapter.execute("INSERT INTO orders VALUES (1, 'west', 10), (2, 'west', 25), (3, 'east', 7)")
    metric_name = next(iter(imported.metrics), None)
    if metric_name is None:
        metric_name = "orders." + imported.models["orders"].metrics[0].name
    rows = layer.query(metrics=[metric_name], dimensions=["orders.region"]).fetchall()
    assert sorted((row[0], float(row[1])) for row in rows) == [("east", 7), ("west", 35)]
    for label, source_graph in (("native", imported), ("sidemantic", graph())):
        destination = tmp_path / (f"{label}.json" if vendor == "sigma" else label)
        with capture_export_report() as report:
            adapter.export(source_graph, destination)
        assert report.readiness != "blocked"
        exported_graph = adapter.parse(destination)
        assert len(exported_graph.models) == 1
        assert len(exported_graph.models["orders"].dimensions) == 3
        assert len(exported_graph.models["orders"].metrics) + len(exported_graph.metrics) == 1
        exported_layer = SemanticLayer()
        for model in exported_graph.models.values():
            exported_layer.add_model(model)
        for metric in exported_graph.metrics.values():
            exported_layer.add_metric(metric)
        exported_layer.adapter.execute("CREATE TABLE orders (id INTEGER, region VARCHAR, amount DECIMAL(12,2))")
        exported_layer.adapter.execute("INSERT INTO orders VALUES (1, 'west', 10), (2, 'west', 25), (3, 'east', 7)")
        exported_metric = next(iter(exported_graph.metrics), None)
        if exported_metric is None:
            exported_metric = "orders." + exported_graph.models["orders"].metrics[0].name
        exported_rows = exported_layer.query(metrics=[exported_metric], dimensions=["orders.region"]).fetchall()
        assert sorted((row[0], float(row[1])) for row in exported_rows) == [("east", 7), ("west", 35)]


def test_missing_output_file_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr("sidemantic.adapters.official.shutil.which", lambda _: "/mock/official-cli")
    monkeypatch.setattr(
        "sidemantic.adapters.official.subprocess.run",
        lambda argv, **kwargs: subprocess.CompletedProcess(argv, 0, "", ""),
    )
    with pytest.raises(OfficialConverterError, match="no nonempty output"):
        SigmaAdapter().parse(write_native(tmp_path, "sigma"))


def test_export_warning_does_not_replace_existing_output(tmp_path, monkeypatch):
    mock_cli(monkeypatch, stderr="[WARNING] CROSS_DATASET_METRIC_DROPPED: revenue — dropped")
    destination = tmp_path / "existing.json"
    destination.write_text("original")
    with (
        capture_export_report() as report,
        pytest.warns(UserWarning),
        pytest.raises(OfficialConverterError, match="CROSS_DATASET"),
    ):
        SigmaAdapter(expression_dialect="ANSI_SQL").export(graph(), destination)
    assert report.readiness == "blocked"
    assert destination.read_text() == "original"


def test_sigma_logical_binding_preserves_physical_sql_and_string_literals():
    document = copy.deepcopy(OSSIE)
    document["datasets"][0]["fields"][2]["expression"]["dialects"][0]["expression"] = '"physicalAmount"'
    document["metrics"][0]["expression"]["dialects"][0]["expression"] = (
        """SUM(CASE WHEN "orders"."region" = 'orders.amount' THEN "orders"."amount" ELSE 0 END)"""
    )
    SigmaAdapter._bind_sigma_metric_references(document)
    metric = document["metrics"][0]["expression"]["dialects"][0]["expression"]
    assert '"ORDERS"."AMOUNT"' in metric
    assert "'orders.amount'" in metric
    assert document["datasets"][0]["fields"][2]["expression"]["dialects"][0]["expression"] == '"physicalAmount"'


def sigma_with_join():
    document = copy.deepcopy(SIGMA)
    source = document["pages"][0]["elements"][0]
    target = copy.deepcopy(source)
    target.update(id="customers", name="customers", metrics=[], uniqueKeys=["id", "amount"])
    target["source"]["path"] = ["customers"]
    source["relationships"] = [
        {
            "id": "join",
            "name": "orders_customers",
            "targetElementId": "customers",
            "relationshipType": "N:1",
            "keys": [
                {"sourceColumnId": "id", "targetColumnId": "id"},
                {"sourceColumnId": "amount", "targetColumnId": "amount"},
            ],
        }
    ]
    document["pages"][0]["elements"].append(target)
    return document


@pytest.mark.parametrize(
    "key,value", [("targetColumnId", None), ("sourceColumnId", ""), ("sourceColumnId", "inode-x/raw")]
)
def test_sigma_composite_join_requires_every_modeled_key_before_cli(tmp_path, monkeypatch, key, value):
    document = sigma_with_join()
    pair = document["pages"][0]["elements"][0]["relationships"][0]["keys"][1]
    if value is None:
        pair.pop(key)
    else:
        pair[key] = value
    calls = mock_cli(monkeypatch)
    with pytest.raises(OfficialConverterError, match=key):
        SigmaAdapter().parse(write_native(tmp_path, "sigma", document))
    assert not calls


@pytest.mark.parametrize("changed_targets", [["id"], ["amount", "id"]])
def test_sigma_silent_composite_join_change_is_rejected(tmp_path, monkeypatch, changed_targets):
    document = copy.deepcopy(OSSIE)
    target = copy.deepcopy(document["datasets"][0])
    target.update(name="customers", source="customers", primary_key=["id", "amount"])
    document["datasets"].append(target)
    document["relationships"] = [
        {
            "name": "orders_customers",
            "from": "orders",
            "to": "customers",
            "from_columns": ["id", "amount"][: len(changed_targets)],
            "to_columns": changed_targets,
        }
    ]
    mock_cli(monkeypatch, document=document)
    with pytest.raises(OfficialConverterError, match="ordered key pairs"):
        SigmaAdapter().parse(write_native(tmp_path, "sigma", sigma_with_join()))


def test_sigma_separate_connections_are_not_flattened(tmp_path, monkeypatch):
    document = sigma_with_join()
    document["pages"][0]["elements"][1]["source"]["connectionId"] = "other-warehouse"
    calls = mock_cli(monkeypatch)
    with pytest.raises(OfficialConverterError, match="connectionId"):
        SigmaAdapter().parse(write_native(tmp_path, "sigma", document))
    assert not calls


def test_malformed_warning_is_not_hidden_by_safe_warning(tmp_path, monkeypatch):
    mock_cli(monkeypatch, stderr="[WARNING] OPAQUE_DATATYPE: id — type unknown\n[WARNING] NEW-CODE: semantic loss")
    with pytest.warns(UserWarning), pytest.raises(OfficialConverterError, match="UNRECOGNIZED_WARNING"):
        SigmaAdapter().parse(write_native(tmp_path, "sigma"))


def test_lightdash_extra_join_predicate_is_rejected_before_cli(tmp_path, monkeypatch):
    document = copy.deepcopy(LIGHTDASH)
    document["joins"] = [{"join": "customers", "sql_on": "${orders.id} = ${customers.id} AND ${orders.amount} > 0"}]
    calls = mock_cli(monkeypatch)
    with pytest.raises(OfficialConverterError, match="direct equality join pairs"):
        LightdashAdapter(expression_dialect="ANSI_SQL").parse(write_native(tmp_path, "lightdash", document))
    assert not calls


def test_export_invalid_target_key_ids_never_publish(tmp_path, monkeypatch):
    from sidemantic.core.relationship import Relationship

    source_graph = graph()
    target = source_graph.models["orders"].model_copy(deep=True)
    target.name = "customers"
    target.table = "customers"
    target.metrics = []
    target.primary_key = ["id", "amount"]
    source_graph.add_model(target)
    source_graph.models["orders"].relationships = [
        Relationship(
            name="customers",
            edge_id="orders_customers",
            type="many_to_one",
            foreign_key=["id", "amount"],
            primary_key=["id", "amount"],
        )
    ]
    native = sigma_with_join()
    for column in native["pages"][0]["elements"][1]["columns"]:
        column["id"] = "target_" + column["id"]
    # Model the pinned upstream bug: target keys reuse the source field IDs.
    monkeypatch.setattr("sidemantic.adapters.official.shutil.which", lambda _: "/mock/official-cli")

    def run(argv, **kwargs):
        Path(argv[argv.index("-o") + 1]).write_text(json.dumps(native))
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr("sidemantic.adapters.official.subprocess.run", run)
    destination = tmp_path / "native.json"
    destination.write_text("original")
    with pytest.raises(OfficialConverterError, match="targetColumnId"):
        SigmaAdapter(expression_dialect="ANSI_SQL").export(source_graph, destination)
    assert destination.read_text() == "original"


def real_adapter(vendor):
    executable = os.environ.get(f"SIDEMANTIC_TEST_{vendor.upper()}_CLI") or shutil.which(f"ossie-{vendor}")
    if not executable:
        pytest.skip(f"Install official ossie-{vendor} or set SIDEMANTIC_TEST_{vendor.upper()}_CLI")
    adapter_type = SigmaAdapter if vendor == "sigma" else LightdashAdapter
    return adapter_type(executable=executable, expression_dialect="ANSI_SQL")


@pytest.mark.parametrize("vendor", ["lightdash", "sigma"])
def test_real_calculated_field_reference_cannot_bind_to_physical_collision(tmp_path, vendor):
    # In native semantics revenue is 20 for raw_amount=10. The pinned converters
    # lower the sibling reference to physical amount, giving 200 when amount=100.
    # Refuse this shape before an executable graph with the wrong binding exists.
    if vendor == "lightdash":
        native = {
            "type": "model",
            "name": "orders",
            "sql_from": "orders",
            "dimensions": [
                {"name": "amount", "type": "number", "sql": "${TABLE}.raw_amount"},
                {
                    "name": "doubled",
                    "type": "number",
                    "sql": "${amount} * 2",
                    "metrics": {"revenue": {"type": "sum"}},
                },
            ],
        }
    else:
        native = copy.deepcopy(SIGMA)
        element = native["pages"][0]["elements"][0]
        element["columns"] = [
            {"id": "amount", "name": "amount", "formula": "[raw_amount]"},
            {"id": "doubled", "name": "doubled", "formula": "[amount] * 2"},
        ]
        element.pop("uniqueKeys")
        element["metrics"][0]["formula"] = "Sum([doubled])"
    with pytest.raises(OfficialConverterError, match="calculated-field references"):
        real_adapter(vendor).parse(write_native(tmp_path, vendor, native))


@pytest.mark.parametrize("vendor", ["lightdash", "sigma"])
def test_real_explicit_physical_reference_preserves_renamed_column(tmp_path, vendor):
    native = copy.deepcopy(SIGMA if vendor == "sigma" else LIGHTDASH)
    if vendor == "sigma":
        native["pages"][0]["elements"][0]["columns"][2]["formula"] = "[raw_amount]"
    else:
        native["dimensions"][2]["sql"] = "${TABLE}.raw_amount * 2"
    imported = real_adapter(vendor).parse(write_native(tmp_path, vendor, native))
    layer = SemanticLayer()
    for model in imported.models.values():
        layer.add_model(model)
    for metric in imported.metrics.values():
        layer.add_metric(metric)
    layer.adapter.execute("CREATE TABLE orders(id INT, region VARCHAR, raw_amount INT, amount INT)")
    layer.adapter.execute("INSERT INTO orders VALUES(1, 'west', 10, 100)")
    assert layer.query(metrics=list(imported.metrics)).fetchall() == [(10 if vendor == "sigma" else 20,)]


def test_sigma_plain_sibling_reference_is_also_rejected(tmp_path, monkeypatch):
    native = copy.deepcopy(SIGMA)
    element = native["pages"][0]["elements"][0]
    element["columns"][2]["formula"] = "[raw_amount]"
    element["columns"].append({"id": "alias", "name": "alias", "formula": "[amount]"})
    calls = mock_cli(monkeypatch)
    with pytest.raises(OfficialConverterError, match="sibling-column references"):
        SigmaAdapter().parse(write_native(tmp_path, "sigma", native))
    assert not calls


@pytest.mark.parametrize("path", [["a.b"], ["schema", "a.b"], ["My Orders"], ['"orders"'], [1], [""]])
def test_real_sigma_import_refuses_ambiguous_source_components(tmp_path, path):
    native = copy.deepcopy(SIGMA)
    native["pages"][0]["elements"][0]["source"]["path"] = path
    # ["a.b"] previously executed schema a/table b, not the table named "a.b".
    with pytest.raises(OfficialConverterError, match="source path components"):
        real_adapter("sigma").parse(write_native(tmp_path, "sigma", native))


@pytest.mark.parametrize("table", ['"a.b"', 'schema."a.b"', '"My Orders"', '"orders"'])
def test_real_sigma_export_refuses_corrupted_source_paths_without_publication(tmp_path, table):
    source = graph()
    source.models["orders"].table = table
    destination = tmp_path / "native.json"
    destination.write_text("original")
    with pytest.raises(OfficialConverterError, match="source path components"):
        real_adapter("sigma").export(source, destination)
    assert destination.read_text() == "original"


def test_real_sigma_simple_source_path_roundtrip(tmp_path):
    native = copy.deepcopy(SIGMA)
    native["pages"][0]["elements"][0]["source"]["path"] = ["warehouse", "sales", "orders"]
    adapter = real_adapter("sigma")
    imported = adapter.parse(write_native(tmp_path, "sigma", native))
    assert imported.models["orders"].table == "warehouse.sales.orders"
    destination = tmp_path / "roundtrip.json"
    adapter.export(imported, destination)
    exported = json.loads(destination.read_text())
    assert exported["pages"][0]["elements"][0]["source"]["path"] == ["warehouse", "sales", "orders"]
