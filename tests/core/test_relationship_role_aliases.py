import duckdb
import pytest
import yaml

from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.sql.generator import SQLGenerator
from sidemantic.validation import QueryValidationError


def _flight_graph() -> SemanticGraph:
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="flights",
            table="flights",
            primary_key="id",
            dimensions=[Dimension(name="id", type="categorical", sql="id")],
            metrics=[Metric(name="flight_count", agg="count", sql="id")],
            relationships=[
                Relationship(
                    name="origin",
                    target_model="airports",
                    type="many_to_one",
                    foreign_key="origin_id",
                ),
                Relationship(
                    name="destination",
                    target_model="airports",
                    type="many_to_one",
                    foreign_key="destination_id",
                ),
            ],
        )
    )
    graph.add_model(
        Model(
            name="airports",
            table="airports",
            primary_key="id",
            dimensions=[Dimension(name="city", type="categorical", sql="city")],
        )
    )
    return graph


def test_two_roles_to_same_target_use_distinct_join_instances():
    graph = _flight_graph()
    sql = SQLGenerator(graph, base_model="flights").generate(
        metrics=["flights.flight_count"],
        dimensions=["origin.city", "destination.city"],
        use_preaggregations=False,
    )

    assert "origin_cte" in sql
    assert "destination_cte" in sql
    assert "origin_cte.id = flights_cte.origin_id" in sql or "flights_cte.origin_id = origin_cte.id" in sql
    assert "destination_cte.id = flights_cte.destination_id" in sql or (
        "flights_cte.destination_id = destination_cte.id" in sql
    )

    con = duckdb.connect()
    con.execute("create table airports(id integer, city varchar)")
    con.execute("insert into airports values (1, 'SFO'), (2, 'LAX'), (3, 'JFK')")
    con.execute("create table flights(id integer, origin_id integer, destination_id integer)")
    con.execute("insert into flights values (10, 1, 2), (11, 1, 3)")
    assert sorted(con.execute(sql).fetchall()) == [("SFO", "JFK", 1), ("SFO", "LAX", 1)]


def test_role_alias_native_yaml_roundtrip(tmp_path):
    graph = _flight_graph()
    output = tmp_path / "semantic.yml"
    adapter = SidemanticAdapter()
    adapter.export(graph, output)

    payload = yaml.safe_load(output.read_text())
    relationships = payload["models"][0]["relationships"]
    assert relationships[0]["target_model"] == "airports"

    reparsed = adapter.parse(output)
    origin = reparsed.models["flights"].relationships[0]
    assert origin.name == "origin"
    assert origin.target_model == "airports"
    assert origin.related_model == "airports"


def test_legacy_relationship_name_remains_target_model():
    relationship = Relationship(name="customers", type="many_to_one", foreign_key="customer_id")
    assert relationship.target_model is None
    assert relationship.related_model == "customers"


def test_nested_role_alias_is_scoped_to_parent_instance():
    graph = _flight_graph()
    graph.models["airports"].relationships = [
        Relationship(
            name="countries",
            type="many_to_one",
            foreign_key="country_id",
        )
    ]
    graph.add_model(
        Model(
            name="countries",
            table="countries",
            primary_key="id",
            dimensions=[Dimension(name="name", type="categorical", sql="name")],
        )
    )
    graph.build_adjacency()

    path = graph.find_relationship_path("flights", "origin$countries")
    assert [hop.to_instance for hop in path] == ["origin", "origin$countries"]
    assert path[-1].to_target_model == "countries"

    sql = SQLGenerator(graph, base_model="flights").generate(
        metrics=["flights.flight_count"],
        dimensions=["origin$countries.name"],
        use_preaggregations=False,
    )
    assert '"origin$countries_cte"' in sql
    assert 'origin_cte.country_id = "origin$countries_cte".id' in sql or (
        '"origin$countries_cte".id = origin_cte.country_id' in sql
    )
    con = duckdb.connect()
    con.execute("create table countries(id integer, name varchar)")
    con.execute("insert into countries values (1, 'US')")
    con.execute("create table airports(id integer, city varchar, country_id integer)")
    con.execute("insert into airports values (1, 'SFO', 1), (2, 'LAX', 1)")
    con.execute("create table flights(id integer, origin_id integer, destination_id integer)")
    con.execute("insert into flights values (10, 1, 2)")
    assert con.execute(sql).fetchall() == [("US", 1)]


def test_duplicate_role_names_under_different_parents_are_scoped():
    graph = SemanticGraph()
    for parent in ("orders", "customers"):
        graph.add_model(
            Model(
                name=parent,
                table=parent,
                primary_key="id",
                relationships=[
                    Relationship(
                        name="address",
                        target_model="addresses",
                        type="many_to_one",
                        foreign_key="address_id",
                    )
                ],
            )
        )
    graph.add_model(Model(name="addresses", table="addresses", primary_key="id"))

    assert graph.find_relationship_path("orders", "orders$address")[0].to_target_model == "addresses"
    assert graph.find_relationship_path("customers", "customers$address")[0].to_target_model == "addresses"
    assert graph.get_model("orders$address") is graph.models["addresses"]
    assert graph.get_model("customers$address") is graph.models["addresses"]


def test_role_alias_one_to_many_keeps_base_measure_fanout_safe():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            relationships=[
                Relationship(
                    name="line_items",
                    target_model="items",
                    type="one_to_many",
                    foreign_key="order_id",
                )
            ],
        )
    )
    graph.add_model(
        Model(
            name="items",
            table="items",
            primary_key="id",
            dimensions=[Dimension(name="sku", type="categorical", sql="sku")],
        )
    )
    sql = SQLGenerator(graph, base_model="orders").generate(
        metrics=["orders.revenue"],
        dimensions=["line_items.sku"],
        use_preaggregations=False,
    )
    con = duckdb.connect()
    con.execute("create table orders(id integer, amount integer)")
    con.execute("insert into orders values (1, 100), (2, 50)")
    con.execute("create table items(id integer, order_id integer, sku varchar)")
    con.execute("insert into items values (10, 1, 'a'), (11, 1, 'b'), (12, 2, 'a')")
    assert sorted(con.execute(sql).fetchall()) == [("a", 150), ("b", 100)]


def test_role_alias_cannot_hijack_canonical_model_name():
    graph = _flight_graph()
    graph.add_model(Model(name="origin", table="actual_origin"))

    with pytest.raises(ValueError, match="collides with canonical model"):
        graph.build_adjacency()


def test_many_to_many_through_column_check_uses_scoped_target_instance(monkeypatch):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            relationships=[
                Relationship(
                    name="buyer",
                    target_model="customers",
                    type="many_to_many",
                    through="order_customers",
                    through_foreign_key="order_id",
                    related_foreign_key="customer_id",
                )
            ],
        )
    )
    graph.add_model(
        Model(
            name="returns",
            table="returns",
            primary_key="id",
            relationships=[
                Relationship(
                    name="buyer",
                    target_model="customers",
                    type="many_to_one",
                    foreign_key="customer_id",
                )
            ],
        )
    )
    graph.add_model(Model(name="customers", table="customers", primary_key="id"))
    graph.add_model(Model(name="order_customers", table="order_customers", primary_key="id"))
    graph.build_adjacency()
    relationship = graph.models["orders"].relationships[0]
    assert graph.relationship_target_instance("orders", relationship) == "orders$buyer"
    monkeypatch.setattr(graph, "instance_has_keyed_relationship", lambda *_args: False)

    generator = SQLGenerator(graph)

    assert generator._model_needs_keyed_join_columns("order_customers", {"orders", "order_customers", "orders$buyer"})


def test_reserved_scoped_role_namespace_is_rejected():
    with pytest.raises(ValueError, match="reserved for scoped role paths"):
        Relationship(
            name="origin$country",
            target_model="countries",
            type="many_to_one",
            foreign_key="country_id",
        )

    graph = _flight_graph()
    graph.add_model(Model(name="literal$path", table="literal_path"))
    with pytest.raises(ValueError, match="Model names cannot contain '\\$'"):
        graph.build_adjacency()


def test_duplicate_roles_under_one_parent_are_rejected():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="flights",
            table="flights",
            relationships=[
                Relationship(name="origin", target_model="airports", type="cross"),
                Relationship(name="origin", target_model="airports", type="cross"),
            ],
        )
    )
    graph.add_model(Model(name="airports", table="airports"))

    with pytest.raises(ValueError, match="declares relationship role 'origin' more than once"):
        graph.build_adjacency()


def test_inactive_and_unresolved_roles_do_not_scope_the_only_active_role():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="flights",
            table="flights",
            relationships=[
                Relationship(name="airport", target_model="airports", type="cross"),
                Relationship(name="airport", target_model="airports", type="cross", active=False),
            ],
        )
    )
    graph.add_model(
        Model(
            name="archived_flights",
            table="archived_flights",
            relationships=[
                Relationship(name="airport", target_model="missing_airports", type="cross"),
            ],
        )
    )
    graph.add_model(Model(name="airports", table="airports"))

    path = graph.find_relationship_path("flights", "airport")
    assert path[0].to_instance == "airport"
    assert "flights$airport" not in graph._role_models


def test_auto_engine_bypasses_rust_validation_and_generation_for_roles(monkeypatch):
    layer = SemanticLayer(engine="python")
    layer.graph = _flight_graph()
    layer._use_rust_query_validation = True
    layer._use_rust_sql_generator = True

    def unexpected(*_args, **_kwargs):
        raise AssertionError("role aliases must stay on the Python path")

    monkeypatch.setattr("sidemantic.rust_bridge.validate_query_with_rust", unexpected)
    monkeypatch.setattr(layer, "_compile_with_rust", unexpected)

    sql = layer.compile(
        metrics=["flights.flight_count"],
        dimensions=["origin.city"],
        use_preaggregations=False,
    )
    assert "origin_cte" in sql


def test_strict_rust_modes_fail_closed_for_role_aliases():
    validation_layer = SemanticLayer(engine="python")
    validation_layer.graph = _flight_graph()
    validation_layer._use_rust_query_validation = True
    validation_layer._strict_rust_query_validation = True
    with pytest.raises(QueryValidationError, match="does not support relationship role aliases"):
        validation_layer.compile(metrics=["flights.flight_count"], dimensions=["origin.city"])

    generation_layer = SemanticLayer(engine="python")
    generation_layer.graph = _flight_graph()
    generation_layer._use_rust_query_validation = False
    generation_layer._use_rust_sql_generator = True
    generation_layer._strict_rust_sql_generator_entrypoint = True
    with pytest.raises(ValueError, match="does not support relationship role aliases"):
        generation_layer.compile(metrics=["flights.flight_count"], dimensions=["origin.city"])
