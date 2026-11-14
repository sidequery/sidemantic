"""Source-only Cube semantics survive export without claiming runtime support."""

import warnings

import duckdb
import pytest
import yaml

from sidemantic.adapters.cube import CubeAdapter, CubeImportWarning
from sidemantic.fidelity import capture_import_report


def _roundtrip(tmp_path, source):
    original = tmp_path / "source.yml"
    original.write_text(yaml.safe_dump(source))
    adapter = CubeAdapter()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", CubeImportWarning)
        graph = adapter.parse(original)
    output = tmp_path / "output.yml"
    adapter.export(graph, output)
    return graph, yaml.safe_load(output.read_text())


def test_dynamic_case_labels_execute_and_roundtrip(tmp_path):
    case = {
        "when": [{"sql": "kind = 'large'", "label": {"sql": "UPPER(label)"}}],
        "else": {"label": "it's small"},
    }
    source = {
        "cubes": [
            {
                "name": "products",
                "sql_table": "products",
                "dimensions": [
                    {"name": "size", "type": "string", "case": case},
                ],
            }
        ]
    }
    graph, output = _roundtrip(tmp_path, source)
    dimension = graph.models["products"].get_dimension("size")
    with duckdb.connect() as db:
        rows = db.execute(
            f"SELECT {dimension.sql} FROM (VALUES ('large', 'huge'), ('tiny', 'mini')) t(kind, label)"
        ).fetchall()
    assert rows == [("HUGE",), ("it's small",)]
    exported = output["cubes"][0]["dimensions"][0]
    assert exported["case"] == case
    assert "sql" not in exported
    # Editing the executable expression must not resurrect the old source CASE.
    dimension.sql = "'edited'"
    path = tmp_path / "edited.yml"
    CubeAdapter().export(graph, path)
    edited = yaml.safe_load(path.read_text())["cubes"][0]["dimensions"][0]
    assert edited["sql"] == "'edited'"
    assert "case" not in edited


def test_malformed_case_label_has_actionable_error(tmp_path):
    path = tmp_path / "bad.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "cubes": [
                    {
                        "name": "p",
                        "sql_table": "p",
                        "dimensions": [
                            {
                                "name": "size",
                                "type": "string",
                                "case": {"when": [{"sql": "x > 0", "label": {"text": "large"}}]},
                            },
                        ],
                    }
                ]
            }
        )
    )
    with pytest.raises(ValueError, match="CASE label mapping requires non-empty sql"):
        CubeAdapter().parse(path)


def test_views_hierarchies_and_groups_survive_and_edited_views_are_refused(tmp_path):
    view = {
        "name": "public_orders",
        "cubes": [{"join_path": "orders", "includes": [{"name": "region", "alias": "area"}]}],
        "folders": [{"name": "Details", "includes": ["area"]}],
    }
    hierarchies = [{"name": "geography", "levels": ["region", "city"]}]
    source = {
        "cubes": [
            {
                "name": "orders",
                "sql_table": "orders",
                "hierarchies": hierarchies,
                "dimensions": [
                    {"name": "region", "type": "string", "sql": "region"},
                    {"name": "city", "type": "string", "sql": "city"},
                ],
            }
        ],
        "views": [view],
        "view_groups": [{"name": "Sales", "views": ["public_orders"]}],
    }
    graph, output = _roundtrip(tmp_path, source)
    assert output["views"] == [view]
    assert output["view_groups"] == source["view_groups"]
    assert output["cubes"][0]["hierarchies"] == hierarchies
    graph.models["public_orders"].dimensions[0].name = "changed"
    with pytest.raises(ValueError, match="unchanged source projection"):
        CubeAdapter().export(graph, tmp_path / "edited.yml")


def test_native_dimension_properties_and_user_meta_do_not_collide(tmp_path):
    granularities = [{"name": "fiscal_year", "interval": "1 year", "offset": "3 months"}]
    source = {
        "cubes": [
            {
                "name": "events",
                "sql_table": "events",
                "dimensions": [
                    {
                        "name": "created",
                        "sql": "created",
                        "type": "time",
                        "granularities": granularities,
                        "meta": {"custom_granularities": "user value"},
                    },
                    {
                        "name": "currency",
                        "type": "switch",
                        "values": ["USD", "EUR"],
                        "meta": {"switch_values": "user value"},
                    },
                ],
            }
        ]
    }
    _, output = _roundtrip(tmp_path, source)
    created, currency = output["cubes"][0]["dimensions"][:2]
    assert created["granularities"] == granularities
    assert created["meta"] == {"custom_granularities": "user value"}
    assert currency["type"] == "switch"
    assert currency["values"] == ["USD", "EUR"]
    assert currency["meta"] == {"switch_values": "user value"}


def test_multistage_and_time_shift_restore_native_keys_with_diagnostics(tmp_path):
    shift = [{"time_dimension": "created", "interval": "1 year", "type": "prior"}]
    source = {
        "cubes": [
            {
                "name": "orders",
                "sql_table": "orders",
                "measures": [
                    {
                        "name": "revenue",
                        "type": "sum",
                        "sql": "amount",
                        "currency": "USD",
                        "meta": {"currency": "user value"},
                    },
                    {
                        "name": "by_region",
                        "type": "number",
                        "sql": "{revenue}",
                        "multi_stage": True,
                        "group_by": ["region"],
                    },
                    {"name": "prior", "type": "sum", "sql": "amount", "multi_stage": True, "time_shift": shift},
                ],
            }
        ]
    }
    with capture_import_report() as report:
        _, output = _roundtrip(tmp_path, source)
    revenue, by_region, prior = output["cubes"][0]["measures"]
    assert revenue["currency"] == "USD"
    assert revenue["meta"] == {"currency": "user value"}
    assert by_region["multi_stage"] is True
    assert by_region["group_by"] == ["region"]
    assert "meta" not in by_region
    assert prior["time_shift"] == shift
    assert prior["multi_stage"] is True
    assert report.is_blocked
    assert any(note.construct == "inert_cube_feature" and "time_shift" in note.detail for note in report.notes)


def test_simple_cube_does_not_acquire_source_only_state_that_blocks_portable_export(tmp_path):
    from sidemantic.adapters.ossie import OssieAdapter

    source = {
        "cubes": [
            {
                "name": "orders",
                "sql_table": "orders",
                "dimensions": [{"name": "amount", "type": "number", "sql": "amount"}],
                "measures": [{"name": "revenue", "type": "sum", "sql": "amount"}],
            }
        ]
    }
    graph, _ = _roundtrip(tmp_path, source)
    output = tmp_path / "portable.yml"
    OssieAdapter().export(graph, output, scope_name="sales", expression_dialect="ANSI_SQL", official_compatible=True)
    assert yaml.safe_load(output.read_text())["metrics"][0]["name"] == "revenue"


def test_switch_type_edit_does_not_restore_switch_properties(tmp_path):
    graph, _ = _roundtrip(
        tmp_path,
        {
            "cubes": [
                {
                    "name": "events",
                    "sql_table": "events",
                    "dimensions": [{"name": "currency", "type": "switch", "values": ["USD", "EUR"]}],
                }
            ]
        },
    )
    dimension = graph.models["events"].dimensions[0]
    dimension.type = "numeric"
    dimension.sql = "amount"
    output = tmp_path / "edited.yml"
    CubeAdapter().export(graph, output)
    exported = yaml.safe_load(output.read_text())["cubes"][0]["dimensions"][0]
    assert exported["type"] == "number"
    assert "values" not in exported
    restored = CubeAdapter().parse(output).models["events"].dimensions[0]
    assert (restored.type, restored.sql) == ("numeric", "amount")


def _time_comparison_source():
    return {
        "cubes": [
            {
                "name": "orders",
                "sql_table": "orders",
                "measures": [
                    {"name": "revenue", "type": "sum", "sql": "amount"},
                    {
                        "name": "prior",
                        "type": "number",
                        "sql": "{revenue}",
                        "time_shift": [{"time_dimension": "created", "interval": "1 year", "type": "prior"}],
                    },
                ],
            }
        ]
    }


def test_time_comparison_offset_edit_preserves_source_dimension(tmp_path):
    graph, original_export = _roundtrip(tmp_path, _time_comparison_source())
    adapter = CubeAdapter()
    output = tmp_path / "edited.yml"
    metric = graph.models["orders"].metrics[1]
    metric.time_offset = "1 month"
    adapter.export(graph, output)
    shift = yaml.safe_load(output.read_text())["cubes"][0]["measures"][1]["time_shift"]
    assert shift == [{"time_dimension": "created", "interval": "1 month", "type": "prior"}]
    restored = adapter.parse(output).models["orders"].metrics[1]
    assert (restored.type, restored.time_offset, restored.base_metric) == (
        "time_comparison",
        "1 month",
        "orders.revenue",
    )
    assert (
        original_export["cubes"][0]["measures"][1]["time_shift"]
        == _time_comparison_source()["cubes"][0]["measures"][1]["time_shift"]
    )


def test_time_comparison_type_edit_removes_old_shift(tmp_path):
    graph, _ = _roundtrip(tmp_path, _time_comparison_source())
    metric = graph.models["orders"].metrics[1]
    metric.type = "derived"
    metric.sql = "orders.revenue * 2"
    output = tmp_path / "edited.yml"
    CubeAdapter().export(graph, output)
    exported = yaml.safe_load(output.read_text())["cubes"][0]["measures"][1]
    assert "time_shift" not in exported
    assert CubeAdapter().parse(output).models["orders"].metrics[1].sql == metric.sql


def test_time_comparison_conflicting_edits_are_rejected(tmp_path):
    graph, _ = _roundtrip(tmp_path, _time_comparison_source())
    metric = graph.models["orders"].metrics[1]
    metric.comparison_type = "mom"
    with pytest.raises(ValueError, match="reconcile time_offset and comparison_type"):
        CubeAdapter().export(graph, tmp_path / "edited.yml")
    metric.time_offset = None
    CubeAdapter().export(graph, tmp_path / "edited.yml")
    restored = CubeAdapter().parse(tmp_path / "edited.yml").models["orders"].metrics[1]
    assert (restored.comparison_type, restored.time_offset) == ("mom", "1 month")


def test_multiple_time_shifts_survive_but_ambiguous_offset_edits_are_rejected(tmp_path):
    source = _time_comparison_source()
    shifts = source["cubes"][0]["measures"][1]["time_shift"]
    shifts.append({"time_dimension": "shipped", "interval": "1 week", "type": "prior"})
    graph, exported = _roundtrip(tmp_path, source)
    assert exported["cubes"][0]["measures"][1]["time_shift"] == shifts
    graph.models["orders"].metrics[1].time_offset = "1 month"
    with pytest.raises(ValueError, match="reconcile multiple source time shifts"):
        CubeAdapter().export(graph, tmp_path / "edited.yml")
