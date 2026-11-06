"""Native project restoration and executable relationship semantics."""

import duckdb
import pytest
import yaml

from sidemantic import SemanticLayer
from sidemantic.adapters.omni import OmniAdapter


def test_native_layout_topics_and_non_equi_join_roundtrip(tmp_path):
    source = tmp_path / "source"
    files = {
        "warehouse/orders.view": "# Reference this view as orders\nschema: main\ntable_name: orders\ndimensions:\n  id: {primary_key: true}\nmeasures:\n  revenue: {aggregate_type: sum, sql: amount}\n",
        "warehouse/tiers.view": "# Reference this view as tiers\nschema: main\ntable_name: tiers\ndimensions:\n  id: {primary_key: true}\n",
        "topics/sales.topic": "base_view: orders\nlabel: Sales\nfields: [orders.revenue]\n",
        "model.yaml": "label: Business model\nweek_start_day: Monday\n",
        "relationships.yaml": "- join_from_view: orders\n  join_to_view: tiers\n  on_sql: ${orders.amount} > ${tiers.threshold} AND ${orders.region} = ${tiers.region}\n  join_type: inner\n  relationship_type: many_to_one\n  reversible: false\n",
    }
    for name, content in files.items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    adapter = OmniAdapter()
    graph = adapter.parse(source)
    assert set(graph.models) == {"orders", "tiers"}
    destination = tmp_path / "export"
    adapter.export(graph, destination)
    assert yaml.safe_load((destination / "model.yaml").read_text()) == yaml.safe_load(files["model.yaml"])
    assert yaml.safe_load((destination / "topics/sales.topic").read_text()) == yaml.safe_load(
        files["topics/sales.topic"]
    )
    relation = yaml.safe_load((destination / "relationships.yaml").read_text())[0]
    assert relation["join_type"] == "inner"
    assert relation["reversible"] is False
    assert relation["on_sql"] == "${orders.amount} > ${tiers.threshold} AND ${orders.region} = ${tiers.region}"
    for candidate in (graph, adapter.parse(destination)):
        layer = SemanticLayer(auto_register=False)
        layer.graph = candidate
        with duckdb.connect() as connection:
            connection.execute(
                "CREATE TABLE orders AS SELECT * FROM (VALUES (1,5,'e'),(2,15,'e'),(3,30,'w'),(4,25,'e')) t(id,amount,region)"
            )
            connection.execute(
                "CREATE TABLE tiers AS SELECT * FROM (VALUES (1,10,'e'),(2,20,'w')) t(id,threshold,region)"
            )
            rows = connection.execute(
                layer.compile(metrics=["orders.revenue"], dimensions=["orders.id", "tiers.id"])
            ).fetchall()
            assert set(rows) == {(2, 1, 15), (3, 2, 30), (4, 1, 25)}


def test_restored_paths_cannot_escape_output(tmp_path):
    source = tmp_path / "orders.view"
    source.write_text("name: orders\ntable_name: orders\ndimensions: {id: {}}\n")
    adapter = OmniAdapter()
    graph = adapter.parse(source)
    graph.metadata["omni"]["view_paths"]["orders"] = "../escape.view"
    with pytest.raises(ValueError, match="Unsafe output filename"):
        adapter.export(graph, tmp_path / "out")
    assert not (tmp_path / "escape.view").exists()


def test_join_literals_survive_roundtrip(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "orders.view").write_text("name: orders\ntable_name: orders\n")
    (source / "tiers.view").write_text("name: tiers\ntable_name: tiers\n")
    predicate = "${orders.amount} > ${tiers.threshold} AND ${orders.label} = '${tiers.label} {to}.text'"
    (source / "relationships.yaml").write_text(
        yaml.safe_dump(
            [
                {
                    "join_from_view": "orders",
                    "join_to_view": "tiers",
                    "on_sql": predicate,
                }
            ]
        )
    )
    adapter = OmniAdapter()
    adapter.export(adapter.parse(source), tmp_path / "out")
    restored = yaml.safe_load((tmp_path / "out/relationships.yaml").read_text())
    assert restored[0]["on_sql"] == predicate


def test_reversible_requires_boolean():
    from sidemantic.core.model import Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="orders"))
    with pytest.raises(ValueError, match="must be a boolean"):
        OmniAdapter()._parse_relationships_list(
            [
                {
                    "join_from_view": "orders",
                    "join_to_view": "tiers",
                    "reversible": "false",
                }
            ],
            graph,
        )


def test_edited_native_fields_override_preserved_metadata(tmp_path):
    source = tmp_path / "orders.view"
    source.write_text(
        yaml.safe_dump(
            {
                "name": "orders",
                "table_name": "old_orders",
                "schema": "old_schema",
                "dimensions": {"id": {"sql": "id + 1", "primary_key": True}},
                "measures": {
                    "revenue": {"aggregate_type": "sum", "sql": "amount", "filters": {"status": {"is": "paid"}}}
                },
            }
        )
    )
    adapter = OmniAdapter()
    graph = adapter.parse(source)
    model = graph.models["orders"]
    model.table = "new_orders"
    model.primary_key = None
    model.dimensions[0].sql = "id"
    model.metrics[0].filters = None
    model.metrics[0].sql = None
    adapter.export(graph, tmp_path / "out")
    restored = yaml.safe_load((tmp_path / "out/orders.view").read_text())
    assert restored["table_name"] == "new_orders"
    assert "schema" not in restored
    assert "primary_key" not in restored["dimensions"]["id"]
    assert "sql" not in restored["dimensions"]["id"]
    assert "sql" not in restored["measures"]["revenue"]
    assert "filters" not in restored["measures"]["revenue"]
    model.metrics[0].filters = ["amount >= 10"]
    adapter.export(graph, tmp_path / "edited")
    edited = yaml.safe_load((tmp_path / "edited/orders.view").read_text())
    assert edited["measures"]["revenue"]["filters"] == {"amount": {"greater_than_or_equal_to": 10}}


@pytest.mark.parametrize(
    "definition", [{"query": {"base_view": "orders", "filters": {"status": "paid"}}}, {"extends": ["orders"]}]
)
def test_query_views_are_rejected_before_fabricating_a_table(tmp_path, definition):
    source = tmp_path / "summary.view"
    source.write_text(yaml.safe_dump(definition))
    with pytest.raises(ValueError, match="require vendor query semantics"):
        OmniAdapter().parse(source)


def test_foreign_dimension_metadata_is_not_promoted_to_omni():
    from sidemantic.core.dimension import Dimension
    from sidemantic.core.model import Model

    model = Model(
        name="orders",
        table="orders",
        dimensions=[
            Dimension(name="id", type="numeric", metadata={"cube_source": {"case": {}}}),
        ],
    )
    assert "cube_source" not in OmniAdapter()._export_view(model)["dimensions"]["id"]


@pytest.mark.parametrize(
    "metadata,expected",
    [
        ({"bsl_how": "inner"}, "inner"),
        ({"join_type": "full_outer"}, "full_outer"),
        ({"omni_join_type": "always_left", "bsl_how": "inner"}, "inner"),
    ],
)
def test_cross_format_and_edited_join_types_are_authoritative(tmp_path, metadata, expected):
    from sidemantic.core.model import Model
    from sidemantic.core.relationship import Relationship
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            relationships=[
                Relationship(name="customers", type="many_to_one", foreign_key="customer_id", metadata=metadata),
            ],
        )
    )
    graph.add_model(Model(name="customers", table="customers"))
    OmniAdapter().export(graph, tmp_path)
    assert yaml.safe_load((tmp_path / "relationships.yaml").read_text())[0]["join_type"] == expected


@pytest.mark.parametrize("edit", ["remove_relationship", "deactivate_relationship", "remove_view", "remove_base"])
def test_retained_topics_reject_conflicting_graph_edits_before_writing(tmp_path, edit):
    from sidemantic.fidelity import capture_export_report

    source = tmp_path / "source"
    source.mkdir()
    for name in ("orders", "customers", "regions"):
        (source / f"{name}.view").write_text(f"name: {name}\ntable_name: {name}\n")
    topic = {"base_view": "orders", "joins": {"customers": {"regions": {}}}}
    (source / "sales.topic").write_text(yaml.safe_dump(topic))
    (source / "relationships.yaml").write_text(
        yaml.safe_dump(
            [
                {"join_from_view": left, "join_to_view": right, "on_sql": f"${{{left}.{key}}} = ${{{right}.id}}"}
                for left, right, key in [("orders", "customers", "buyer_id"), ("customers", "regions", "region_id")]
            ]
        )
    )
    adapter = OmniAdapter()
    graph = adapter.parse(source)
    adapter.export(graph, tmp_path / "unchanged")
    assert yaml.safe_load((tmp_path / "unchanged/sales.topic").read_text()) == topic
    if edit == "remove_relationship":
        graph.models["customers"].relationships = []
    elif edit == "deactivate_relationship":
        graph.models["customers"].relationships[0].active = False
    elif edit == "remove_view":
        del graph.models["regions"]
    else:
        del graph.models["orders"]
    output = tmp_path / "edited"
    with capture_export_report() as report, pytest.raises(ValueError, match="Omni topic 'sales.topic'"):
        adapter.export(graph, output)
    assert report.is_blocked
    assert not output.exists()


@pytest.mark.parametrize(
    "granularity,supported,expected",
    [("year", ["year"], ["year"]), ("month", ["day", "month"], ["month", "date"]), (None, None, None)],
)
def test_edited_time_properties_override_preserved_timeframes(tmp_path, granularity, supported, expected):
    source = tmp_path / "events.view"
    source.write_text(
        "name: events\ntable_name: events\ndimensions:\n  created: {type: timestamp, timeframes: [date, month]}\n"
    )
    adapter = OmniAdapter()
    graph = adapter.parse(source)
    adapter.export(graph, tmp_path / "untouched")
    original = yaml.safe_load((tmp_path / "untouched/events.view").read_text())
    assert original["dimensions"]["created"]["timeframes"] == ["date", "month"]
    dimension = graph.models["events"].dimensions[0]
    dimension.granularity = granularity
    dimension.supported_granularities = supported
    adapter.export(graph, tmp_path / "edited")
    restored = yaml.safe_load((tmp_path / "edited/events.view").read_text())
    assert restored["dimensions"]["created"].get("timeframes") == expected
    reparsed = adapter.parse(tmp_path / "edited").models["events"].dimensions[0]
    assert reparsed.granularity == granularity
    assert set(reparsed.supported_granularities or []) == set(supported or [])


def test_conflicting_native_time_properties_are_rejected():
    from sidemantic import Dimension, Model

    model = Model(
        name="events",
        table="events",
        dimensions=[Dimension(name="created", type="time", granularity="year", supported_granularities=["day"])],
    )
    with pytest.raises(ValueError, match="must be included in supported_granularities"):
        OmniAdapter()._export_view(model)


@pytest.mark.parametrize("config", [{}, {"label": "Business model", "week_start_day": "Monday"}])
@pytest.mark.parametrize("edit", ["change", "delete"])
@pytest.mark.parametrize("suffix", ["yaml", "yml"])
def test_in_place_export_removes_legacy_relationships(tmp_path, config, edit, suffix):
    for name in ("orders", "customers"):
        (tmp_path / f"{name}.view").write_text(f"name: {name}\ntable_name: {name}\n")
    (tmp_path / f"model.{suffix}").write_text(
        yaml.safe_dump(
            {
                **config,
                "relationships": [
                    {
                        "join_from_view": "orders",
                        "join_to_view": "customers",
                        "on_sql": "${orders.old_customer_id} = ${customers.id}",
                    }
                ],
            }
        )
    )
    adapter = OmniAdapter()
    graph = adapter.parse(tmp_path)
    if edit == "change":
        graph.models["orders"].relationships[0].foreign_key = "customer_id"
    else:
        graph.models["orders"].relationships.clear()

    adapter.export(graph, tmp_path)

    assert yaml.safe_load((tmp_path / "model.yaml").read_text()) == config
    assert yaml.safe_load((tmp_path / f"model.{suffix}").read_text()) == config
    restored = adapter.parse(tmp_path)
    relationships = restored.models["orders"].relationships
    assert [relationship.foreign_key for relationship in relationships] == (["customer_id"] if edit == "change" else [])

    # A subsequent deletion also clears the current relationships.yaml output.
    if edit == "change":
        restored.models["orders"].relationships.clear()
        adapter.export(restored, tmp_path)
        assert yaml.safe_load((tmp_path / "relationships.yaml").read_text()) == []
        assert not adapter.parse(tmp_path).models["orders"].relationships


@pytest.mark.parametrize("edit", ["change", "delete"])
def test_in_place_export_clears_alternate_relationship_file(tmp_path, edit):
    for name in ("orders", "customers"):
        (tmp_path / f"{name}.view").write_text(f"name: {name}\ntable_name: {name}\n")
    (tmp_path / "relationships.yml").write_text(
        yaml.safe_dump(
            [
                {
                    "join_from_view": "orders",
                    "join_to_view": "customers",
                    "on_sql": "${orders.old_customer_id} = ${customers.id}",
                }
            ]
        )
    )
    adapter = OmniAdapter()
    graph = adapter.parse(tmp_path)
    if edit == "change":
        graph.models["orders"].relationships[0].foreign_key = "customer_id"
    else:
        graph.models["orders"].relationships.clear()
    adapter.export(graph, tmp_path)
    assert yaml.safe_load((tmp_path / "relationships.yml").read_text()) == []
    restored = adapter.parse(tmp_path).models["orders"].relationships
    assert [relationship.foreign_key for relationship in restored] == (["customer_id"] if edit == "change" else [])
