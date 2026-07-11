"""Tests for the SecurityPolicy data model, serialization, and row-filter rendering.

These cover ONLY the data model + serialization + the pure rendering helper.
Enforcement (query-path integration) is a separate work item and is not tested here.
"""

import pytest

from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.model import Model
from sidemantic.core.security import SecurityPolicy, render_row_filter
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.semantic_layer import SecurityError


def test_security_policy_defaults():
    """A SecurityPolicy constructed with no args allows access with no row filters."""
    policy = SecurityPolicy()
    assert policy.access is True
    assert policy.row_filters == []


def test_security_policy_construction():
    """Access expression and row filters are stored as given."""
    policy = SecurityPolicy(
        access="user.role in ['analyst', 'admin']",
        row_filters=["region = '{{ user.region }}'", "team_id = {{ user.team_id }}"],
    )
    assert policy.access == "user.role in ['analyst', 'admin']"
    assert policy.row_filters == ["region = '{{ user.region }}'", "team_id = {{ user.team_id }}"]


def test_security_policy_access_bool():
    """access accepts a plain bool constant."""
    assert SecurityPolicy(access=False).access is False


def test_model_security_defaults_none():
    """Model.security defaults to None (no policy = unchanged behavior)."""
    assert Model(name="orders", table="public.orders").security is None


def test_model_with_security_policy():
    """A SecurityPolicy attaches to a Model via the security field."""
    model = Model(
        name="orders",
        table="public.orders",
        security=SecurityPolicy(access="user.role == 'admin'", row_filters=["region = '{{ user.region }}'"]),
    )
    assert model.security is not None
    assert model.security.access == "user.role == 'admin'"
    assert model.security.row_filters == ["region = '{{ user.region }}'"]


def test_security_policy_yaml_round_trip(tmp_path):
    """A model with an access expr + 2 row filters survives native YAML dump/load."""
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="public.orders",
            primary_key="order_id",
            security=SecurityPolicy(
                access="user.role in ['analyst', 'admin']",
                row_filters=[
                    "region = '{{ user.region }}'",
                    "team_id = {{ user.team_id }}",
                ],
            ),
        )
    )

    adapter = SidemanticAdapter()
    export_path = tmp_path / "with_security.yml"
    adapter.export(graph, export_path)

    reloaded = adapter.parse(export_path)
    security = reloaded.models["orders"].security
    assert security is not None
    assert security.access == "user.role in ['analyst', 'admin']"
    assert security.row_filters == [
        "region = '{{ user.region }}'",
        "team_id = {{ user.team_id }}",
    ]


def test_security_policy_yaml_round_trip_default_access(tmp_path):
    """Row filters survive even when access stays at its default True."""
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="public.orders",
            primary_key="order_id",
            security=SecurityPolicy(row_filters=["region = '{{ user.region }}'"]),
        )
    )

    adapter = SidemanticAdapter()
    export_path = tmp_path / "default_access.yml"
    adapter.export(graph, export_path)

    reloaded = adapter.parse(export_path)
    security = reloaded.models["orders"].security
    assert security is not None
    assert security.access is True
    assert security.row_filters == ["region = '{{ user.region }}'"]


def test_render_row_filter_happy_path():
    """A row filter renders user attributes under the `user` namespace."""
    rendered = render_row_filter("region = '{{ user.region }}'", {"region": "us-east"})
    assert rendered == "region = 'us-east'"


def test_render_row_filter_undefined_attr_raises():
    """Referencing a user attribute not supplied raises SecurityError (StrictUndefined)."""
    with pytest.raises(SecurityError):
        render_row_filter("region = '{{ user.region }}'", {})


def test_render_row_filter_renders_quote_containing_value():
    """A value containing a quote is SQL-escaped so it cannot break out of the literal.

    render_row_filter doubles embedded single quotes (the standard SQL escape) before
    interpolation, so a value like ``O'Brien`` renders as a single quoted literal
    ``'O''Brien'`` rather than terminating the string early (which would enable
    injection). Non-string values pass through untouched.
    """
    rendered = render_row_filter("name = '{{ user.name }}'", {"name": "O'Brien"})
    assert rendered == "name = 'O''Brien'"
