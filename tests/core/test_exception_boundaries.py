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
    "sidemantic/core/registry.py",
    "sidemantic/rust_parity.py",
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


def test_postgres_protocol_boundary_logs_and_reraises_unchanged():
    handlers = _broad_handlers("sidemantic/server/connection.py")

    assert len(handlers) == 1
    assert any(
        isinstance(node, ast.Raise) and node.exc is None
        for statement in handlers[0].body
        for node in ast.walk(statement)
    )
