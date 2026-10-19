"""Required filters are caller obligations, checked before compiler dispatch."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.consumption import Explore
from sidemantic.core.inheritance import merge_model
from sidemantic.core.segment import Segment
from sidemantic.sql.query_rewriter import QueryRewriter


@pytest.fixture
def required_layer():
    layer = SemanticLayer(auto_register=False, engine="python")
    layer.add_model(
        Model(
            name="events",
            table="event_rows",
            primary_key="id",
            required_filters=["events.event_date"],
            dimensions=[
                Dimension(name="event_date", type="time", granularity="day"),
                Dimension(name="status", type="categorical"),
                Dimension(name="test_name", type="categorical"),
            ],
            metrics=[Metric(name="count", agg="count")],
            segments=[Segment(name="recent", sql="{model}.event_date >= '2024-01-01'")],
        )
    )
    layer.adapter.execute("create table event_rows (id int, event_date date, status text, test_name text)")
    layer.adapter.execute("insert into event_rows values (1, '2024-01-01', 'ok', 'a'), (2, '2023-01-01', 'ok', 'b')")
    yield layer
    layer.adapter.close()


@pytest.fixture
def native_dispatch(required_layer, monkeypatch):
    """Exercise engine selection and handoff with only native execution replaced."""
    expected = required_layer.compile(metrics=["events.count"], filters=["events.event_date >= '2024-01-01'"])
    module = SimpleNamespace(
        validate_with_semantic_input=Mock(return_value=[]),
        compile_with_semantic_input=Mock(return_value=expected),
        rewrite_with_semantic_input=Mock(return_value=expected),
    )
    monkeypatch.setattr("sidemantic.core.semantic_layer.get_rust_module", lambda: module)
    monkeypatch.setattr("sidemantic.rust_bridge.get_rust_module", lambda: module)
    return module


@pytest.mark.parametrize("engine", ["python", "rust", "auto"])
def test_engine_dispatch_cannot_bypass_requirements(required_layer, native_dispatch, engine):
    layer = SemanticLayer(auto_register=False, engine=engine)
    layer.add_model(required_layer.graph.models["events"])
    try:
        with pytest.raises(ValueError, match="a query on events must filter events.event_date"):
            layer.compile(metrics=["events.count"])
        native_dispatch.compile_with_semantic_input.assert_not_called()
        native_dispatch.validate_with_semantic_input.assert_not_called()
        sql = layer.compile(metrics=["events.count"], filters=["events.event_date >= '2024-01-01'"])
        assert required_layer.adapter.execute(sql).fetchall() == [(1,)]
        if engine != "python":
            native_dispatch.compile_with_semantic_input.assert_called_once()
            snapshot = json.loads(native_dispatch.compile_with_semantic_input.call_args.args[0])
            assert "required_filters" not in snapshot["models"][0]
            assert layer.last_engine_selection["engine"] == "rust"
    finally:
        layer.adapter.close()


@pytest.mark.parametrize(
    "filters",
    [[], ["events.status = 'event_date'"], ["events.status = 'events.event_date'"], ["'events.event_date' = 'x'"]],
)
def test_unrelated_filters_and_literals_do_not_satisfy(required_layer, filters):
    with pytest.raises(ValueError, match="must filter events.event_date"):
        required_layer.compile(metrics=["events.count"], filters=filters)


@pytest.mark.parametrize("field", ["events.event_date", '"events"."event_date"', "events.event_date__day"])
def test_date_picker_filter(required_layer, field):
    assert required_layer.query(metrics=["events.count"], filters=[f"{field} >= '2024-01-01'"]).fetchall() == [(1,)]


def test_grouping_is_per_field_and_only_for_or_groups(required_layer):
    model = required_layer.graph.models["events"]
    with pytest.raises(ValueError, match="must filter events.event_date"):
        required_layer.compile(metrics=["events.count"], dimensions=["events.event_date__month"])
    model.required_filters = []
    model.required_filters_or_groups = ["event_date", "test_name"]
    with pytest.raises(ValueError, match="must filter or group by events.test_name"):
        required_layer.compile(metrics=["events.count"], dimensions=["events.event_date__month"])
    assert (
        required_layer.query(
            metrics=["events.count"], dimensions=["events.event_date__month"], filters=["events.test_name = 'a'"]
        ).fetchall()[0][-1]
        == 1
    )
    with pytest.raises(ValueError, match="must filter or group by events.event_date"):
        required_layer.compile(dimensions=["events.event_date", "events.test_name"], ungrouped=True)


def test_segments_explore_defaults_and_mandatory_filters(required_layer):
    assert required_layer.query(metrics=["events.count"], segments=["events.recent"]).fetchall() == [(1,)]
    required_layer.add_explore(
        Explore(
            name="recent", model="events", default_metrics=["count"], default_filters=["event_date >= '2024-01-01'"]
        )
    )
    assert required_layer.query(explore="recent").fetchall() == [(1,)]
    with pytest.raises(ValueError, match="must filter events.event_date"):
        required_layer.compile(explore="recent", filters=["events.status = 'ok'"])
    required_layer.add_explore(
        Explore(name="always_recent", model="events", default_metrics=["count"], filters=["event_date >= '2024-01-01'"])
    )
    assert required_layer.query(explore="always_recent", filters=["events.status = 'ok'"]).fetchall() == [(1,)]


def test_invariants_do_not_satisfy_caller_obligations(required_layer):
    required_layer.graph.models["events"].invariant_filters = ["event_date >= '2024-01-01'"]
    with pytest.raises(ValueError, match="must filter events.event_date"):
        required_layer.compile(metrics=["events.count"])


def test_graph_metric_and_intermediate_join_model(required_layer):
    layer = required_layer
    layer.add_metric(Metric(name="total", type="derived", sql="{events.count} * 2"))
    with pytest.raises(ValueError, match="must filter events.event_date"):
        layer.compile(metrics=["total"])
    layer.add_metric(Metric(name="stats.total", type="derived", sql="{events.count} * 2"))
    with pytest.raises(ValueError, match="must filter events.event_date"):
        QueryRewriter(layer.graph).rewrite("select stats.total from metrics")
    layer.graph.models["events"].relationships = [Relationship(name="mapping", type="many_to_one", foreign_key="id")]
    layer.add_model(
        Model(
            name="mapping",
            table="mapping",
            primary_key="id",
            required_filters_or_groups=["test_name"],
            dimensions=[Dimension(name="test_name", type="categorical")],
            relationships=[Relationship(name="users", type="many_to_one", foreign_key="id")],
        )
    )
    layer.add_model(
        Model(name="users", table="users", primary_key="id", dimensions=[Dimension(name="name", type="categorical")])
    )
    with pytest.raises(ValueError, match="must filter or group by mapping.test_name"):
        layer.compile(metrics=["events.count"], dimensions=["users.name"], filters=["events.event_date > '2024-01-01'"])
    sql = layer.compile(
        metrics=["events.count"],
        dimensions=["users.name", "mapping.test_name"],
        filters=["events.event_date > '2024-01-01'"],
    )
    assert "mapping" in sql


@pytest.mark.parametrize("rust", [False, True])
def test_sql_scopes_aliases_and_engine_dispatch(required_layer, native_dispatch, rust):
    rewriter = QueryRewriter(required_layer.graph, use_rust_rewriter=rust, rust_no_fallback=True)
    for sql in (
        "select e.count from events e",
        "select * from (select count from events) x where event_date >= '2024-01-01'",
        "select e.count from events e where exists (select event_date from events where event_date > '2024-01-01')",
    ):
        with pytest.raises(ValueError, match="must filter events.event_date"):
            rewriter.rewrite(sql, strict=False)
    native_dispatch.rewrite_with_semantic_input.assert_not_called()
    sql = rewriter.rewrite("select e.count from events e where e.event_date >= '2024-01-01'")
    assert required_layer.adapter.execute(sql).fetchall() == [(1,)]
    if rust:
        native_dispatch.rewrite_with_semantic_input.assert_called_once()


@pytest.mark.parametrize("group", ["test_name", "test"])
def test_sql_group_fields_and_aliases(required_layer, group):
    model = required_layer.graph.models["events"]
    model.required_filters = []
    model.required_filters_or_groups = ["test_name"]
    sql = QueryRewriter(required_layer.graph).rewrite(f"select test_name as test, count from events group by {group}")
    assert len(required_layer.adapter.execute(sql).fetchall()) == 2


def test_yaml_roundtrip_and_inheritance(required_layer, tmp_path):
    from sidemantic.semantic_handoff import graph_to_semantic_input

    adapter = SidemanticAdapter()
    model = required_layer.graph.models["events"]
    model.required_filters_or_groups = ["test_name"]
    child = merge_model(Model(name="child", extends="events", required_filters=["status"]), model)
    assert child.required_filters == ["event_date", "status"]
    path = tmp_path / "models.yml"
    adapter.export(required_layer.graph, path)
    assert adapter.parse(path).models["events"].required_filters == ["event_date"]
    assert adapter.parse(path).models["events"].required_filters_or_groups == ["test_name"]
    assert graph_to_semantic_input(required_layer.graph)["models"][0]["required_filters"] == ["event_date"]
    assert child.required_filters_or_groups == ["test_name"]
    assert merge_model(Model(name="child", extends="events", required_filters=[]), model).required_filters == [
        "event_date"
    ]


def test_default_off(required_layer):
    required_layer.graph.models["events"].required_filters = []
    assert required_layer.query(metrics=["events.count"]).fetchall() == [(2,)]


def test_cli_filter_error_and_success(required_layer, tmp_path):
    from typer.testing import CliRunner

    from sidemantic.cli import app

    SidemanticAdapter().export(required_layer.graph, tmp_path / "models.yml")
    args = ["query", "--models", str(tmp_path), "--engine", "python", "--metric", "events.count", "--dry-run"]
    runner = CliRunner()
    missing = runner.invoke(app, args)
    assert missing.exit_code != 0
    assert "must filter events.event_date" in missing.output
    present = runner.invoke(app, [*args, "--filter", "events.event_date >= '2024-01-01'"])
    assert present.exit_code == 0, present.output


def test_api_returns_400_and_accepts_date_picker(required_layer):
    pytest.importorskip("fastapi")
    pytest.importorskip("pyarrow")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from sidemantic.api_server import create_app

    with TestClient(create_app(required_layer)) as client:
        missing = client.post("/query", json={"metrics": ["events.count"]})
        assert missing.status_code == 400
        assert "must filter events.event_date" in missing.json()["error"]
        present = client.post(
            "/query", json={"metrics": ["events.count"], "filters": ["events.event_date >= '2024-01-01'"]}
        )
        assert present.status_code == 200
        invalid = client.post("/query", json={"metrics": ["events.count"], "filters": ["events.event_date > ("]})
        assert invalid.status_code == 400
        raw = client.post("/raw", json={"query": "select * from event_rows"})
        assert raw.status_code == 403


def test_pgwire_transport_and_raw_bypass(required_layer):
    from sidemantic.core.semantic_layer import SecurityError
    from sidemantic.core.transport_security import deny_raw_sql, rewrite_transport_sql

    with pytest.raises(ValueError, match="must filter events.event_date"):
        rewrite_transport_sql(
            required_layer, "select count from events", transport="PostgreSQL", user_attributes=None, strict=False
        )
    sql = rewrite_transport_sql(
        required_layer,
        "select e.count from events e where e.event_date >= '2024-01-01'",
        transport="PostgreSQL",
        user_attributes=None,
        strict=False,
    )
    assert required_layer.adapter.execute(sql).fetchall() == [(1,)]
    with pytest.raises(SecurityError, match="model required filters"):
        deny_raw_sql(required_layer, transport="MCP raw query")


def test_filter_only_model_and_multiple_requirements(required_layer):
    required_layer.graph.models["events"].required_filters.append("status")
    with pytest.raises(ValueError, match="must filter events.status"):
        required_layer.compile(metrics=["events.count"], filters=["events.event_date > '2024-01-01'"])
    required_layer.add_model(
        Model(name="users", table="users", primary_key="id", metrics=[Metric(name="count", agg="count")])
    )
    with pytest.raises(ValueError, match="must filter events.event_date"):
        required_layer.compile(metrics=["users.count"], filters=["events.status = 'ok'"])


def test_predicate_subquery_does_not_satisfy_outer_read(required_layer):
    from sidemantic.core.required_filters import validate_required_filters

    with pytest.raises(ValueError, match="must filter events.event_date"):
        validate_required_filters(
            required_layer.graph, metrics=["events.count"], filters=["exists (select events.event_date from events)"]
        )


@pytest.mark.parametrize("rust", [False, True])
def test_yardstick_wrapper_cannot_bypass_requirements(required_layer, native_dispatch, rust):
    rewriter = QueryRewriter(required_layer.graph, use_rust_rewriter=rust)
    with pytest.raises(ValueError, match=r"yardstick\(\) is unsupported"):
        rewriter.rewrite("select * from yardstick('select count from events')")
    native_dispatch.rewrite_with_semantic_input.assert_not_called()


def test_output_alias_cannot_satisfy_required_filter(required_layer):
    with pytest.raises(ValueError, match="must filter events.event_date"):
        required_layer.compile(metrics=["events.count"], aliases={"events.count": "event_date"})


@pytest.mark.parametrize("policy", ["required_filters", "required_filters_or_groups"])
@pytest.mark.parametrize("mutation", ["assign", "append", "replace"])
def test_cached_sql_observes_policy_mutations(required_layer, policy, mutation):
    model = required_layer.graph.models["events"]
    model.required_filters = []
    if mutation == "replace":
        setattr(model, policy, ["status"])
    query = "select count from events where status = 'ok'"
    assert required_layer.sql(query).fetchall() == [(2,)]

    if mutation == "assign":
        setattr(model, policy, ["event_date"])
    elif mutation == "append":
        getattr(model, policy).append("event_date")
    else:
        getattr(model, policy)[0] = "event_date"

    with pytest.raises(ValueError, match="events.event_date"):
        required_layer.sql(query)

    getattr(model, policy).clear()
    assert required_layer.sql(query).fetchall() == [(2,)]
