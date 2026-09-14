"""Run the complete existing semantic SQL contract with Rust fallback disabled."""

import inspect

import pytest

from sidemantic import SemanticLayer
from sidemantic.sql.query_rewriter import QueryRewriter
from tests.queries import test_sql_rewriter as contracts
from tests.queries.test_sql_rewriter import semantic_layer  # noqa: F401

CASES = [
    function for name, function in vars(contracts).items() if name.startswith("test_") and inspect.isfunction(function)
]


@pytest.fixture(autouse=True)
def strict_rust(monkeypatch):
    runtime = pytest.importorskip("sidemantic_rs", reason="Requires the built Rust extension")
    assert callable(runtime.rewrite_with_semantic_input_context)
    original_layer_init = SemanticLayer.__init__
    original_rewriter_init = QueryRewriter.__init__

    def initialize_layer(self, *args, **kwargs):
        kwargs.update(engine="rust", fallback=False)
        original_layer_init(self, *args, **kwargs)

    def initialize_rewriter(self, *args, **kwargs):
        kwargs.update(use_rust_rewriter=True, rust_no_fallback=True)
        original_rewriter_init(self, *args, **kwargs)

    def reject_python(*args, **kwargs):
        raise AssertionError("Semantic SQL parity must not enter the Python compiler")

    monkeypatch.setattr(SemanticLayer, "__init__", initialize_layer)
    monkeypatch.setattr(QueryRewriter, "__init__", initialize_rewriter)
    monkeypatch.setattr(QueryRewriter, "_rewrite_python", reject_python)


@pytest.mark.parametrize("contract", CASES, ids=lambda function: function.__name__)
def test_semantic_sql_contract(contract, request):
    arguments = {name: request.getfixturevalue(name) for name in inspect.signature(contract).parameters}
    contract(**arguments)
