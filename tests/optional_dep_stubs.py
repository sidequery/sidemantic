"""Helpers to stub optional dependencies in unit tests."""

from __future__ import annotations

import sys
import types


def ensure_fake_mcp() -> None:
    """Install a tiny MCP stub when the optional dependency is unavailable."""
    try:
        import mcp.server.fastmcp  # noqa: F401

        return
    except Exception:
        pass

    mcp_module = types.ModuleType("mcp")
    server_module = types.ModuleType("mcp.server")
    fastmcp_module = types.ModuleType("mcp.server.fastmcp")

    class FastMCP:
        def __init__(self, *_args, **_kwargs):
            self.settings = types.SimpleNamespace(port=None)

        def tool(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

        def resource(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

        def run(self, *args, **kwargs):
            return None

    fastmcp_module.FastMCP = FastMCP
    server_module.fastmcp = fastmcp_module
    mcp_module.server = server_module

    sys.modules.setdefault("mcp", mcp_module)
    sys.modules.setdefault("mcp.server", server_module)
    sys.modules.setdefault("mcp.server.fastmcp", fastmcp_module)


def ensure_fake_riffq() -> None:
    """Install a tiny riffq stub when the optional dependency is unavailable."""
    try:
        import riffq  # noqa: F401

        return
    except Exception:
        pass

    riffq_module = types.ModuleType("riffq")

    class BaseConnection:
        def __init__(self, connection_id, executor):
            self.connection_id = connection_id
            self.executor = executor

        def send_reader(self, reader, callback):
            callback(True)

    class _InnerServer:
        def register_database(self, *args, **kwargs):
            return None

        def register_schema(self, *args, **kwargs):
            return None

        def register_table(self, *args, **kwargs):
            return None

    class RiffqServer:
        def __init__(self, *args, **kwargs):
            self._server = _InnerServer()

        def start(self, **kwargs):
            return None

    riffq_module.BaseConnection = BaseConnection
    riffq_module.RiffqServer = RiffqServer

    sys.modules.setdefault("riffq", riffq_module)
