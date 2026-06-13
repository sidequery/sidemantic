"""Tests for Malloy adapter error listener and newer upstream statements.

Covers features added to the Malloy language after the originally vendored ANTLR
grammar snapshot:
- ANTLR error listener (errors surfaced via warning / strict raise, not swallowed)
- ``export { ... }`` top-level re-export
- user-defined ``type:`` statement
- ``given:`` model-level parameters
- ``virtual()`` sources
- source type constraints (``source: a is b::T``)
"""

import warnings
from pathlib import Path

import pytest

from sidemantic.adapters.malloy import MalloyAdapter, MalloySyntaxError

FIXTURES = Path("tests/fixtures/malloy")


class TestErrorListener:
    """The adapter must surface syntax errors instead of silently degrading."""

    def test_strict_mode_raises_on_syntax_error(self):
        adapter = MalloyAdapter(strict=True)
        with pytest.raises(MalloySyntaxError) as exc_info:
            adapter.parse(FIXTURES / "syntax_error.malloy")
        # The raised error carries the collected (line, col, msg) tuples.
        assert exc_info.value.errors
        assert all(len(e) == 3 for e in exc_info.value.errors)

    def test_lenient_mode_collects_errors_and_warns(self):
        adapter = MalloyAdapter()  # default: lenient
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            graph = adapter.parse(FIXTURES / "syntax_error.malloy")

        # Errors are collected, not swallowed.
        assert adapter.errors, "syntax errors should be collected on adapter.errors"
        assert any("syntax_error.malloy" in entry[0] for entry in adapter.errors)
        # A warning is emitted by default.
        assert any(issubclass(w.category, UserWarning) for w in caught)
        # ANTLR error recovery still yields the valid source.
        assert "ok_source" in graph.models

    def test_lenient_mode_can_suppress_warnings(self):
        adapter = MalloyAdapter(warn_on_errors=False)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            adapter.parse(FIXTURES / "syntax_error.malloy")
        assert not caught, "warn_on_errors=False should emit no warnings"
        # Errors are still collected for inspection.
        assert adapter.errors

    def test_valid_fixture_has_no_errors(self):
        adapter = MalloyAdapter(strict=True)
        # A known-good fixture must parse cleanly under strict mode.
        graph = adapter.parse(FIXTURES / "flights.malloy")
        assert not adapter.errors
        assert "flights" in graph.models


class TestNewStatements:
    """New upstream statements parse without error and expose useful metadata."""

    def setup_method(self):
        self.adapter = MalloyAdapter(strict=True)
        self.graph = self.adapter.parse(FIXTURES / "new_statements.malloy")

    def test_fixture_parses_cleanly(self):
        # All new statements must parse with zero syntax errors.
        assert not self.adapter.errors

    def test_virtual_source(self):
        events = self.graph.get_model("events")
        assert events is not None
        assert events.primary_key == "event_id"
        # virtual('event_stream') is recorded in metadata and used as the source.
        assert events.metadata is not None
        assert events.metadata.get("virtual") == "event_stream"
        assert events.table == "event_stream"
        assert events.metadata.get("connection") == "duckdb"

    def test_virtual_source_fields(self):
        events = self.graph.get_model("events")
        dim_names = {d.name for d in events.dimensions}
        assert {"event_name", "region"}.issubset(dim_names)
        metric_names = {m.name for m in events.metrics}
        assert {"event_count", "unique_users"}.issubset(metric_names)
        assert events.get_metric("unique_users").agg == "count_distinct"

    def test_source_type_constraint(self):
        typed = self.graph.get_model("typed_events")
        assert typed is not None
        assert typed.metadata is not None
        assert typed.metadata.get("source_type_constraints") == ["event_shape"]
        # The underlying source is preserved via extends.
        assert typed.extends == "events"

    def test_user_type_collected(self):
        # type: statements are collected as adapter metadata, not as models.
        assert "event_shape" in self.adapter.user_types

    def test_given_parameter_collected(self):
        assert "target_region" in self.adapter.given
        assert "string" in self.adapter.given["target_region"]

    def test_export_collected(self):
        assert "events" in self.adapter.exports
        assert "typed_events" in self.adapter.exports
