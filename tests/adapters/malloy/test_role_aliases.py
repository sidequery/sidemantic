from pathlib import Path

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.adapters.malloy import MalloyAdapter
from sidemantic.core.consumption import Explore
from sidemantic.core.semantic_graph import SemanticGraph


def _parse(tmp_path: Path, text: str):
    path = tmp_path / "roles.malloy"
    path.write_text(text)
    adapter = MalloyAdapter(warn_on_errors=False)
    return adapter, adapter.parse(path)


def test_role_aliases_import_compile_execute_and_roundtrip(tmp_path):
    adapter, graph = _parse(
        tmp_path,
        """source: airports is duckdb.table('airports') extend {
  primary_key: id
  dimension: city is city
}
source: flights is duckdb.table('flights') extend {
  primary_key: id
  measure: flight_count is count()
  join_one:
    origin is airports with origin_id
    destination is airports with destination_id
}
""",
    )
    relationships = {relationship.name: relationship for relationship in graph.get_model("flights").relationships}
    assert relationships["origin"].target_model == "airports"
    assert relationships["destination"].target_model == "airports"

    layer = SemanticLayer(auto_register=False, engine="python")
    layer.adapter.execute("create table airports(id int, city text)")
    layer.adapter.execute("insert into airports values (1, 'SFO'), (2, 'LAX'), (3, 'JFK')")
    layer.adapter.execute("create table flights(id int, origin_id int, destination_id int)")
    layer.adapter.execute("insert into flights values (10, 1, 2), (11, 1, 3)")
    for model in graph.models.values():
        layer.add_model(model)
    layer.graph.add_explore(Explore(name="flight_roles", model="flights"))
    rows = layer.query(
        metrics=["flights.flight_count"],
        dimensions=["origin.city", "destination.city"],
        order_by=["destination.city"],
        explore="flight_roles",
    ).fetchall()
    assert rows == [("SFO", "JFK", 1), ("SFO", "LAX", 1)]

    exported = tmp_path / "roundtrip.malloy"
    adapter.export(graph, exported)
    text = exported.read_text()
    assert "origin is airports with origin_id" in text
    assert "destination is airports with destination_id" in text
    reparsed = MalloyAdapter(warn_on_errors=False).parse(exported)
    reparsed_relationships = {
        relationship.name: relationship for relationship in reparsed.get_model("flights").relationships
    }
    assert reparsed_relationships["origin"].target_model == "airports"
    assert reparsed_relationships["destination"].target_model == "airports"


def test_cross_role_alias_roundtrips_target_identity(tmp_path):
    adapter, graph = _parse(
        tmp_path,
        """source: calendar is duckdb.table('calendar')
source: facts is duckdb.table('facts') extend {
  join_cross: reporting_calendar is calendar
}
""",
    )
    relationship = graph.get_model("facts").relationships[0]
    assert relationship.name == "reporting_calendar"
    assert relationship.target_model == "calendar"

    exported = tmp_path / "cross.malloy"
    adapter.export(graph, exported)
    assert "join_cross: reporting_calendar is calendar" in exported.read_text()


def test_native_custom_join_sql_exports_exactly_and_executes_after_reparse(tmp_path):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            relationships=[
                Relationship(
                    name="customers",
                    type="many_to_one",
                    foreign_key="customer_id",
                    primary_key="id",
                    sql="{from}.customer_id = {to}.id and {to}.active = true",
                )
            ],
        )
    )
    graph.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            dimensions=[Dimension(name="name", type="categorical", sql="name")],
        )
    )
    output = tmp_path / "custom.malloy"
    MalloyAdapter(warn_on_errors=False).export(graph, output)
    text = output.read_text()
    assert "on orders.customer_id = customers.id and customers.active = true" in text

    reparsed = MalloyAdapter(warn_on_errors=False).parse(output)
    relationship = reparsed.get_model("orders").relationships[0]
    assert relationship.sql == "{from}.customer_id = {to}.id and {to}.active = true"

    layer = SemanticLayer(auto_register=False, engine="python")
    layer.adapter.execute("create table customers(id int, name text, active boolean)")
    layer.adapter.execute("insert into customers values (1, 'Ada', true), (2, 'Grace', false)")
    layer.adapter.execute("create table orders(id int, customer_id int, amount int)")
    layer.adapter.execute("insert into orders values (10, 1, 20), (11, 2, 30)")
    for model in reparsed.models.values():
        layer.add_model(model)
    assert sorted(
        layer.query(metrics=["orders.revenue"], dimensions=["customers.name"]).fetchall(),
        key=lambda row: row[0],
    ) == [("Ada", 20), ("Grace", None)]


@pytest.mark.parametrize(
    "protected_sql",
    [
        "{from}.customer_id = {to}.id and '{to}' = '{to}'",
        "{from}.customer_id = {to}.id /* {from}.must_not_change */",
    ],
)
def test_native_custom_join_export_rejects_placeholders_in_literals_or_comments(tmp_path, protected_sql):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            relationships=[
                Relationship(
                    name="customers",
                    type="many_to_one",
                    foreign_key="customer_id",
                    primary_key="id",
                    sql=protected_sql,
                )
            ],
        )
    )
    graph.add_model(Model(name="customers", table="customers", primary_key="id"))

    with pytest.raises(ValueError, match="protected placeholders"):
        MalloyAdapter(warn_on_errors=False).export(graph, tmp_path / "unsafe.malloy")
