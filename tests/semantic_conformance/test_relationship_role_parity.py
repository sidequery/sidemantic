"""Scoped role paths must bind separate physical join instances."""

from pathlib import Path

import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.validation import QueryValidationError

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(params=["python", "rust"])
def layer_for(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Scoped role acceptance requires the real Rust extension")
    layers = []

    def create(source):
        layer = SemanticLayer(engine=request.param, auto_register=False)
        layer.graph = SidemanticAdapter().parse(FIXTURES / f"{source}.yml")
        layer.adapter.execute((FIXTURES / "relationship_role_seed.sql").read_text())
        layers.append(layer)
        return layer

    yield create
    for layer in layers:
        layer.adapter.close()


def test_nested_country_paths_keep_parent_role_identity(layer_for):
    layer = layer_for("nested_roles")
    sql = layer.compile(
        metrics=["flights.flight_count"],
        dimensions=["flights.id", "origin$countries.name", "destination$countries.name"],
        order_by=["flights.id"],
    )
    result = layer.adapter.execute(sql)
    assert [column[0] for column in result.description] == [
        "id",
        "origin$countries_name",
        "destination$countries_name",
        "flight_count",
    ]
    assert result.fetchall() == [(10, "US", "Canada", 1), (11, "Canada", "France", 1), (12, "US", None, 1)]


def test_repeated_aliases_are_scoped_by_owning_model(layer_for):
    layer = layer_for("repeated_roles")
    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.id", "orders$address.city", "customers$address.city"],
        order_by=["orders.id"],
    )
    result = layer.adapter.execute(sql)
    assert [column[0] for column in result.description] == [
        "id",
        "orders$address_city",
        "customers$address_city",
        "revenue",
    ]
    assert result.fetchall() == [
        (10, "shipping", "billing", 100),
        (11, "alternate", "billing", 50),
        (12, None, "alternate", 20),
    ]


def test_equally_short_undeclared_join_choice_is_rejected(layer_for):
    layer = layer_for("ambiguous_paths")
    # validate_query translates the graph's AmbiguousJoinPathError into the
    # public compile-time QueryValidationError (tests/test_validation.py).
    with pytest.raises(QueryValidationError, match="Ambiguous join path"):
        layer.compile(metrics=["a.amount"], dimensions=["d.label"])
