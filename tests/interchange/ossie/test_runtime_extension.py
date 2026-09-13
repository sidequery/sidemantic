from __future__ import annotations

import json

import pytest

from sidemantic import Dimension, Metric, Model, SecurityPolicy, SemanticLayer
from sidemantic.core.registry import reset_current_layer, set_current_layer
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.interchange.ossie import (
    OssieImportPolicy,
    OssieParseOptions,
    lower_ossie_document,
    parse_ossie_document,
)
from sidemantic.interchange.ossie.runtime_extension import decode_runtime_extension, encode_runtime_extension


def _document(graph, *, dialect="ANSI_SQL"):
    scope = {
        "name": "native",
        "datasets": [{"name": "runtime_required", "source": "SELECT NULL AS required WHERE 1 = 0"}],
    }
    scope["custom_extensions"] = [encode_runtime_extension(graph, scope, expression_dialect=dialect)]
    return {"version": "0.2.0.dev0", "vendors": ["SIDEMANTIC"], "semantic_model": [scope]}


def _lower(document, *, policy=OssieImportPolicy.STRICT, target="duckdb"):
    return lower_ossie_document(
        parse_ossie_document(
            json.dumps(document).encode(),
            options=OssieParseOptions(import_policy=policy, validate_schema=True),
        ),
        target_dialect=target,
    )


def test_runtime_extension_restores_per_request_security_and_private_fields():
    original = SemanticLayer()
    original.add_model(
        Model(
            name="orders",
            table="orders",
            dimensions=[
                Dimension(name="tenant", type="numeric"),
                Dimension(name="secret", type="categorical", public=False),
            ],
            metrics=[Metric(name="total", agg="sum", sql="amount")],
            security=SecurityPolicy(access="user.enabled", row_filters=["tenant = {{ user.tenant }}"]),
        )
    )
    document = _document(original.graph)
    assert document["semantic_model"][0]["datasets"][0]["source"] == "SELECT NULL AS required WHERE 1 = 0"
    lowered = _lower(document)
    assert lowered.valid
    graph = lowered.catalog["native"].graph
    assert graph.models["orders"].model_dump() == original.graph.models["orders"].model_dump()
    layer = SemanticLayer(enforce_visibility=True)
    layer.graph = graph
    layer.adapter.conn.execute("create table orders(tenant integer, amount integer, secret varchar)")
    layer.adapter.conn.execute("insert into orders values (1,10,'one'), (2,50,'two')")
    for tenant, expected in [(1, 10), (2, 50)]:
        assert layer.query(
            metrics=["orders.total"], user_attributes={"enabled": True, "tenant": tenant}
        ).fetchall() == [(expected,)]
    with pytest.raises(SecurityError):
        layer.compile(metrics=["orders.total"], user_attributes={"enabled": False, "tenant": 1})
    with pytest.raises(SecurityError):
        layer.compile(dimensions=["orders.secret"], user_attributes={"enabled": True, "tenant": 1})
    with pytest.raises(SecurityError):
        layer.compile(metrics=["orders.total"])


def test_preserves_runtime_metrics_roles_and_inheritance():
    graph = SemanticGraph()
    graph.add_model(Model(name="base_orders", table="orders"))
    graph.add_model(Model(name="customers", table="customers", primary_key="id"))
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            extends="base_orders",
            default_time_dimension="day",
            dimensions=[Dimension(name="day", type="time", granularity="day")],
            relationships=[
                Relationship(name="buyer", target_model="customers", type="many_to_one", foreign_key="buyer_id")
            ],
            metrics=[
                Metric(name="balance", agg="sum", sql="amount", non_additive_dimension="day"),
                Metric(name="running", type="cumulative", sql="amount", agg="sum", window="7 days"),
                Metric(name="previous", type="time_comparison", base_metric="balance", comparison_type="mom"),
            ],
        )
    )
    graph.add_metric(Metric(name="owned", agg="sum", sql="amount"), model_name="orders")
    restored = _lower(_document(graph)).catalog["native"].graph
    from sidemantic.core.inheritance import resolve_model_inheritance

    assert restored.models["orders"].model_dump(mode="json") == resolve_model_inheritance(graph.models)[
        "orders"
    ].model_dump(mode="json")
    assert restored.metric_owners == {"owned": "orders"}
    assert restored.metrics.keys() == graph.metrics.keys()
    assert restored.get_model("buyer").name == "customers"


def test_nonadditive_metric_roundtrip_executes_last_snapshot():
    layer = SemanticLayer()
    layer.add_model(
        Model(
            name="balance",
            table="balance",
            dimensions=[Dimension(name="day", type="time", granularity="day")],
            metrics=[Metric(name="total", agg="sum", sql="amount", non_additive_dimension="day")],
        )
    )
    layer.graph = _lower(_document(layer.graph)).catalog["native"].graph
    layer.adapter.conn.execute("create table balance(day date, amount integer)")
    layer.adapter.conn.execute("insert into balance values ('2026-01-01',100), ('2026-01-31',120)")
    assert layer.query(metrics=["balance.total"], dimensions=["balance.day__month"]).fetchall()[0][1] == 120


@pytest.mark.parametrize("policy", [OssieImportPolicy.STRICT, OssieImportPolicy.PERMISSIVE])
@pytest.mark.parametrize("corruption", ["json", "version", "core", "field", "owner", "duplicate", "unknown_state"])
def test_invalid_runtime_extension_never_falls_back_to_core(policy, corruption):
    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="orders", security=SecurityPolicy(access=False)))
    document = _document(graph)
    scope = document["semantic_model"][0]
    extension = scope["custom_extensions"][0]
    payload = json.loads(extension["data"])
    if corruption == "json":
        extension["data"] = "{"
    elif corruption == "core":
        scope["datasets"][0]["source"] = "orders"
    elif corruption == "duplicate":
        scope["custom_extensions"].append(dict(extension))
    else:
        if corruption == "version":
            payload["version"] = 2
        elif corruption == "field":
            payload["graph"]["models"]["orders"]["security"]["future_policy"] = "deny"
        elif corruption == "owner":
            payload["graph"]["metric_owners"] = {"missing": "orders"}
        elif corruption == "unknown_state":
            payload["graph"]["future_policy"] = "deny"
        extension["data"] = json.dumps(payload)
    lowered = _lower(document, policy=policy)
    assert not lowered.executable
    assert any(item.code == "ossie.lowering.runtime_extension_invalid" for item in lowered.diagnostics)


def test_runtime_extension_enforces_source_dialect():
    document = _document(SemanticGraph(), dialect="BIGQUERY")
    assert _lower(document, target="bigquery").valid
    assert not _lower(document, target="duckdb").executable


def test_foreign_extensions_are_preserved_but_not_executed():
    assert (
        decode_runtime_extension(
            {"custom_extensions": [{"vendor_name": "OTHER", "data": "arbitrary"}]}, target_dialect="duckdb"
        )
        is None
    )


def test_runtime_model_construction_does_not_register_in_ambient_layer():
    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="orders", metrics=[Metric(name="total", agg="count")]))
    document = _document(graph)
    ambient = SemanticLayer()
    token = set_current_layer(ambient)
    try:
        assert _lower(document).valid
    finally:
        reset_current_layer(token)
    assert ambient.graph.models == {}
    assert ambient.graph.metrics == {}


def test_inherited_source_and_metric_defaults_execute_after_roundtrip():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="base",
            table="orders",
            metrics=[Metric(name="total", agg="sum", sql="amount", filters=["amount > 0"])],
        )
    )
    child = Model(name="child", extends="base")
    child.metrics.append(Metric(name="large", extends="total", filters=["amount > 20"]))
    graph.add_model(child)
    document = _document(graph)
    decoded = decode_runtime_extension(document["semantic_model"][0], target_dialect="duckdb")
    assert "table" not in decoded.models["child"].model_fields_set
    assert decoded.models["child"].metrics[0].extends == "total"
    layer = SemanticLayer()
    layer.graph = _lower(document).catalog["native"].graph
    layer.adapter.conn.execute("create table orders(amount integer)")
    layer.adapter.conn.execute("insert into orders values (-5), (10), (50)")
    assert layer.query(metrics=["child.total", "child.large"]).fetchall() == [(60, 50)]


@pytest.mark.parametrize("parent", ["missing", "child"])
def test_invalid_inheritance_is_refused_before_export(parent):
    graph = SemanticGraph()
    graph.add_model(Model(name="child", extends=parent))
    with pytest.raises(ValueError, match="not found|Circular inheritance"):
        _document(graph)


def test_inherited_runtime_fields_survive_native_flattening():
    graph = SemanticGraph()
    graph.add_model(Model(name="customers", table="customers", primary_key="id"))
    graph.add_model(
        Model(
            name="base",
            table="orders",
            dimensions=[
                Dimension(name="day", type="time", granularity="day", logical_data_type="Date", declared_is_time=True)
            ],
            relationships=[
                Relationship(name="customers", type="many_to_one", foreign_key="customer_id", edge_id="customer_edge")
            ],
            metrics=[Metric(name="total", agg="sum", sql="amount", logical_data_type="Decimal")],
        )
    )
    graph.add_model(Model(name="child", extends="base"))
    restored = _lower(_document(graph)).catalog["native"].graph.models["child"]
    assert restored.relationships[0].edge_id == "customer_edge"
    assert restored.dimensions[0].logical_data_type == "Date"
    assert restored.dimensions[0].declared_is_time is True
    assert restored.metrics[0].logical_data_type == "Decimal"


def test_relationship_role_instances_execute_distinct_joins_after_roundtrip():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            dimensions=[Dimension(name="name", type="categorical")],
        )
    )
    orders = Model(
        name="orders",
        table="orders",
        dimensions=[Dimension(name="id", type="numeric")],
        metrics=[Metric(name="total", agg="sum", sql="amount")],
    )
    for role in ["buyer", "recipient"]:
        orders.relationships.append(
            Relationship(
                name=role,
                target_model="customers",
                edge_id=f"order_{role}",
                type="many_to_one",
                foreign_key=f"{role}_id",
            )
        )
    graph.add_model(orders)
    layer = SemanticLayer()
    layer.graph = _lower(_document(graph)).catalog["native"].graph
    assert [edge.edge_id for edge in layer.graph.models["orders"].relationships] == ["order_buyer", "order_recipient"]
    layer.adapter.conn.execute("create table customers(id integer, name varchar)")
    layer.adapter.conn.execute("insert into customers values (1,'Buyer'), (2,'Recipient')")
    layer.adapter.conn.execute(
        "create table orders(id integer, buyer_id integer, recipient_id integer, amount integer)"
    )
    layer.adapter.conn.execute("insert into orders values (9,1,2,100)")
    assert layer.query(
        metrics=["orders.total"], dimensions=["orders.id", "buyer.name", "recipient.name"]
    ).fetchall() == [(9, "Buyer", "Recipient", 100)]


def test_cumulative_metric_executes_after_roundtrip():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="daily",
            table="daily",
            dimensions=[Dimension(name="day", type="time", granularity="day")],
            metrics=[
                Metric(name="total", agg="sum", sql="amount"),
                Metric(name="running", type="cumulative", sql="daily.total"),
            ],
        )
    )
    layer = SemanticLayer()
    layer.graph = _lower(_document(graph)).catalog["native"].graph
    layer.adapter.conn.execute("create table daily(day date, amount integer)")
    layer.adapter.conn.execute("insert into daily values ('2026-01-01',10), ('2026-01-02',20)")
    rows = layer.query(metrics=["daily.running"], dimensions=["daily.day"], order_by=["daily.day"]).fetchall()
    assert [row[-1] for row in rows] == [10, 30]
