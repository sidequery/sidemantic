"""Tests for fail-closed physical schema exposure."""

import duckdb
import pytest
import yaml
from pydantic import ValidationError

from sidemantic import Model, SchemaExposure, SemanticLayer
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.inheritance import merge_model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.semantic_layer import SchemaIntrospectionError
from sidemantic.db.duckdb import DuckDBAdapter


@pytest.fixture
def layer():
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        create table accounts (
            account_id integer primary key,
            region varchar,
            balance decimal(10, 2),
            secret_token varchar
        )
        """
    )
    adapter = DuckDBAdapter()
    adapter.conn = conn
    return SemanticLayer(connection=adapter, auto_register=False)


def test_schema_exposure_can_include_primary_key(layer):
    model = Model(
        name="accounts",
        table="accounts",
        primary_key="account_id",
        auto_dimensions=True,
        schema_exposure=SchemaExposure(include_primary_key=True, strict=True),
    )

    layer.add_model(model)

    assert model.get_dimension("account_id") is not None
    assert model.get_dimension("region") is not None
    assert "accounts.account_id" in layer.compile(dimensions=["accounts.account_id"])


def test_schema_exposure_accept_filters_before_dimension_creation(layer):
    model = Model(
        name="accounts",
        table="accounts",
        primary_key="account_id",
        auto_dimensions=True,
        schema_exposure=SchemaExposure(accept=["account_id", "region"], include_primary_key=True),
    )

    layer.add_model(model)

    assert {dimension.name for dimension in model.dimensions} == {"account_id", "region"}


def test_schema_exposure_except_and_private_are_not_queryable(layer):
    model = Model(
        name="accounts",
        table="accounts",
        primary_key="account_id",
        auto_dimensions=True,
        schema_exposure=SchemaExposure(
            **{
                "except": ["balance"],
                "private": ["secret_token"],
                "include_primary_key": True,
            }
        ),
    )

    layer.add_model(model)

    assert {dimension.name for dimension in model.dimensions} == {"account_id", "region"}
    with pytest.raises(ValueError, match="secret_token"):
        layer.compile(dimensions=["accounts.secret_token"])


@pytest.mark.parametrize(
    "config",
    [
        {"accept": ["region"], "except": ["balance"]},
        {"accept": ["region"], "private": ["region"]},
        {"except": ["secret_token"], "private": ["secret_token"]},
    ],
)
def test_schema_exposure_rejects_contradictory_visibility(config):
    with pytest.raises(ValidationError):
        SchemaExposure(**config)


def test_schema_exposure_strict_introspection_failure_raises(layer, monkeypatch):
    def fail_get_columns(table_name, schema=None):
        raise RuntimeError("catalog unavailable")

    monkeypatch.setattr(layer.adapter, "get_columns", fail_get_columns)
    model = Model(
        name="accounts",
        table="accounts",
        auto_dimensions=True,
        schema_exposure=SchemaExposure(strict=True),
    )

    with pytest.raises(SchemaIntrospectionError, match="accounts") as exc_info:
        layer.add_model(model)

    assert isinstance(exc_info.value.__cause__, RuntimeError)


def test_default_auto_dimensions_remains_lenient_on_introspection_failure(layer, monkeypatch):
    def fail_get_columns(table_name, schema=None):
        raise RuntimeError("catalog unavailable")

    monkeypatch.setattr(layer.adapter, "get_columns", fail_get_columns)
    model = Model(name="accounts", table="accounts", auto_dimensions=True)

    layer.add_model(model)

    assert model.dimensions == []


def test_schema_exposure_is_inherited_unless_child_explicitly_overrides():
    parent = Model(
        name="base",
        table="accounts",
        auto_dimensions=True,
        schema_exposure=SchemaExposure(accept=["region"], strict=True),
    )
    child = Model(name="child", extends="base")
    inherited = merge_model(child, parent)

    assert inherited.schema_exposure == parent.schema_exposure

    override = Model(
        name="override",
        extends="base",
        schema_exposure=SchemaExposure(**{"except": ["secret_token"]}),
    )
    overridden = merge_model(override, parent)
    assert overridden.schema_exposure == override.schema_exposure


def test_native_schema_exposure_round_trip(tmp_path):
    source = tmp_path / "source.yml"
    source.write_text(
        """
version: 1
models:
  - name: accounts
    table: accounts
    auto_dimensions: true
    schema_exposure:
      strict: true
      include_primary_key: true
      except: [balance]
      private: [secret_token]
"""
    )
    adapter = SidemanticAdapter()
    graph = adapter.parse(source)
    exposure = graph.models["accounts"].schema_exposure
    assert exposure is not None
    assert exposure.except_fields == ["balance"]

    exported = tmp_path / "exported.yml"
    adapter.export(graph, exported)
    exported_data = yaml.safe_load(exported.read_text())
    assert exported_data["models"][0]["auto_dimensions"] is True
    assert exported_data["models"][0]["schema_exposure"] == {
        "strict": True,
        "include_primary_key": True,
        "except": ["balance"],
        "private": ["secret_token"],
    }

    reparsed = adapter.parse(exported)
    assert reparsed.models["accounts"].auto_dimensions is True
    assert reparsed.models["accounts"].schema_exposure == exposure


def test_rust_bridge_omits_python_only_schema_exposure():
    from sidemantic.rust_bridge import graph_to_rust_yaml

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="accounts",
            table="accounts",
            auto_dimensions=True,
            schema_exposure=SchemaExposure(strict=True),
        )
    )

    assert "schema_exposure" not in graph_to_rust_yaml(graph)
