"""Execute the existing Yardstick result contract through the strict Rust route."""

import inspect

import pytest

from sidemantic.sql.query_rewriter import QueryRewriter
from tests.queries import test_yardstick_query_rewriter as contracts
from tests.queries.test_yardstick_query_rewriter import yardstick_layer, yardstick_paper_layer  # noqa: F401

CASES = [
    function for name, function in vars(contracts).items() if name.startswith("test_") and inspect.isfunction(function)
]


@pytest.fixture(autouse=True)
def strict_rust(monkeypatch):
    runtime = pytest.importorskip("sidemantic_rs", reason="Requires the built Rust extension")
    assert callable(runtime.rewrite_with_semantic_input_context)
    original_init = QueryRewriter.__init__

    def initialize(self, *args, **kwargs):
        kwargs.update(use_rust_rewriter=True, rust_no_fallback=True)
        original_init(self, *args, **kwargs)

    def reject_python(*args, **kwargs):
        raise AssertionError("Yardstick result parity must not enter the Python rewriter")

    monkeypatch.setattr(QueryRewriter, "__init__", initialize)
    monkeypatch.setattr(QueryRewriter, "_rewrite_python", reject_python)
    monkeypatch.setattr(QueryRewriter, "_rewrite_yardstick_query", reject_python)


@pytest.mark.parametrize("contract", CASES, ids=lambda function: function.__name__)
def test_yardstick_result_contract(contract, request):
    """Keep original result/error assertions, including nested and AT contexts."""
    arguments = {name: request.getfixturevalue(name) for name in inspect.signature(contract).parameters}
    contract(**arguments)
