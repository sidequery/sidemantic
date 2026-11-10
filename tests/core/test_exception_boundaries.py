"""Guard the reviewed broad-exception policy in high-risk runtime paths."""

import ast
from pathlib import Path

ROOT = Path(__file__).parents[2]

NO_BROAD_CATCH_PATHS = (
    "sidemantic/sql/generator.py",
    "sidemantic/sql/query_rewriter.py",
    "sidemantic/sql/parsing.py",
    "sidemantic/sql/aggregation_detection.py",
    "sidemantic/api_server.py",
    "sidemantic/server/common.py",
    "sidemantic/adapters/graphene.py",
    "sidemantic/adapters/metricflow.py",
    "sidemantic/adapters/osi.py",
    "sidemantic/adapters/rill.py",
)


def _is_broad_exception(handler: ast.ExceptHandler) -> bool:
    if handler.type is None:
        return True
    if isinstance(handler.type, ast.Name):
        return handler.type.id == "Exception"
    if isinstance(handler.type, ast.Tuple):
        return any(isinstance(item, ast.Name) and item.id == "Exception" for item in handler.type.elts)
    return False


def _broad_handlers(relative_path: str) -> list[ast.ExceptHandler]:
    tree = ast.parse((ROOT / relative_path).read_text(), filename=relative_path)
    return [node for node in ast.walk(tree) if isinstance(node, ast.ExceptHandler) and _is_broad_exception(node)]


def test_high_risk_parse_and_runtime_paths_have_no_broad_catches():
    violations = {
        path: [handler.lineno for handler in _broad_handlers(path)]
        for path in NO_BROAD_CATCH_PATHS
        if _broad_handlers(path)
    }

    assert violations == {}


def _broad_handler_tries_by_function(relative_path: str) -> dict[str, list[ast.Try]]:
    """Map each function to the try statements in it that have a broad handler."""
    tree = ast.parse((ROOT / relative_path).read_text(), filename=relative_path)
    found: dict[str, list[ast.Try]] = {}
    for function in ast.walk(tree):
        if not isinstance(function, ast.FunctionDef):
            continue
        tries = [
            node
            for node in ast.walk(function)
            if isinstance(node, ast.Try) and any(_is_broad_exception(handler) for handler in node.handlers)
        ]
        if tries:
            found[function.name] = tries
    return found


def test_postgres_protocol_boundary_logs_and_reraises_unchanged():
    tries = _broad_handler_tries_by_function("sidemantic/server/connection.py")

    # Exactly two broad handlers: query handling, and the executor-thread boundary
    # checked by the next test. Anything else must catch specific exceptions.
    assert sorted(tries) == ["_handle_query", "_run_query"]
    assert len(_broad_handlers("sidemantic/server/connection.py")) == 2

    (query_try,) = tries["_handle_query"]
    (handler,) = query_try.handlers
    assert any(
        isinstance(node, ast.Raise) and node.exc is None for statement in handler.body for node in ast.walk(statement)
    )


def test_postgres_protocol_boundary_reports_the_exception_to_the_client():
    """Queries run on an executor thread, where a raised exception never reaches riffq.

    The boundary around that thread's work therefore catches what query handling
    logged and re-raised, and its only action is to hand that same exception to
    the client as a PostgreSQL error. It must not swallow, retry, or substitute.
    """
    tries = _broad_handler_tries_by_function("sidemantic/server/connection.py")

    (boundary,) = tries["_run_query"]
    assert [ast.unparse(statement) for statement in boundary.body] == ["self._handle_query(sql, callback, **kwargs)"]
    assert not boundary.orelse and not boundary.finalbody
    (handler,) = boundary.handlers
    assert handler.name == "exc"
    assert [ast.unparse(statement) for statement in handler.body] == ["callback(error_response(exc), is_error=True)"]
