"""Tests for BaseAdapter validation behavior."""

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph


class DummyAdapter(BaseAdapter):
    def parse(self, source):
        raise NotImplementedError


def test_validate_reports_missing_pk_and_table():
    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table=None, sql=None, primary_key=""))

    adapter = DummyAdapter()
    errors = adapter.validate(graph)

    assert "Model orders has no primary key" in errors
    assert "Model orders has neither table nor sql definition" in errors


def test_export_not_supported():
    adapter = DummyAdapter()
    try:
        adapter.export(SemanticGraph(), "out")
    except NotImplementedError as exc:
        assert "does not support export" in str(exc)
    else:
        raise AssertionError("Expected NotImplementedError")
