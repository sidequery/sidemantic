"""Execute the existing Yardstick result contract through the strict Rust route."""

import inspect
from itertools import product

import pytest

from sidemantic.sql.query_rewriter import QueryRewriter
from tests.queries import test_yardstick_query_rewriter as contracts
from tests.queries import test_yardstick_warnings as warning_contracts
from tests.queries.test_yardstick_query_rewriter import yardstick_layer, yardstick_paper_layer  # noqa: F401
from tests.queries.test_yardstick_warnings import warning_layer  # noqa: F401


def contract_cases():
    for module in (contracts, warning_contracts):
        for name, function in vars(module).items():
            if not name.startswith("test_") or not inspect.isfunction(function):
                continue
            parameter_groups = []
            for mark in getattr(function, "pytestmark", []):
                if mark.name != "parametrize":
                    continue
                names, values = mark.args[:2]
                names = [name.strip() for name in names.split(",")] if isinstance(names, str) else names
                parameter_groups.append(
                    [dict(zip(names, (value,) if len(names) == 1 else value, strict=True)) for value in values]
                )
            for index, combination in enumerate(product(*parameter_groups)):
                parameters = {key: value for group in combination for key, value in group.items()}
                yield pytest.param(function, parameters, id=f"{name}-{index}" if parameters else name)


@pytest.fixture(autouse=True)
def strict_rust(monkeypatch):
    runtime = pytest.importorskip("sidemantic_rs", reason="Requires the built Rust extension")
    assert callable(runtime.rewrite_with_semantic_input_context)
    assert callable(runtime.rewrite_with_semantic_input_context_diagnostics)
    original_init = QueryRewriter.__init__

    def initialize(self, *args, **kwargs):
        kwargs.update(use_rust_rewriter=True, rust_no_fallback=True)
        original_init(self, *args, **kwargs)

    def reject_python(*args, **kwargs):
        raise AssertionError("Yardstick result parity must not enter the Python rewriter")

    monkeypatch.setattr(QueryRewriter, "__init__", initialize)
    monkeypatch.setattr(QueryRewriter, "_rewrite_python", reject_python)
    monkeypatch.setattr(QueryRewriter, "_rewrite_yardstick_query", reject_python)


@pytest.mark.parametrize("contract,parameters", list(contract_cases()))
def test_yardstick_result_contract(contract, parameters, request):
    """Keep original result/error assertions, including nested and AT contexts."""
    arguments = {
        name: parameters[name] if name in parameters else request.getfixturevalue(name)
        for name in inspect.signature(contract).parameters
    }
    contract(**arguments)
