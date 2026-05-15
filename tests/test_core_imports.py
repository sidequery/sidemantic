from __future__ import annotations

import json
import subprocess
import sys


def test_core_imports_do_not_load_optional_dax_runtime():
    code = """
import json
import sys
from sidemantic import Dimension, Metric, Model

print(json.dumps({
    "classes": [Model.__name__, Dimension.__name__, Metric.__name__],
    "sidemantic_dax_loaded": "sidemantic_dax" in sys.modules,
}))
"""

    result = subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)

    assert json.loads(result.stdout) == {
        "classes": ["Model", "Dimension", "Metric"],
        "sidemantic_dax_loaded": False,
    }


def test_non_dax_yaml_load_does_not_load_optional_dax_runtime(tmp_path):
    model_path = tmp_path / "models.yml"
    model_path.write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: order_count
        agg: count
"""
    )
    code = f"""
import json
import sys
from sidemantic import SemanticLayer

layer = SemanticLayer.from_yaml({str(model_path)!r})
print(json.dumps({{
    "models": list(layer.graph.models),
    "sidemantic_dax_loaded": "sidemantic_dax" in sys.modules,
}}))
"""

    result = subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)

    assert json.loads(result.stdout) == {
        "models": ["orders"],
        "sidemantic_dax_loaded": False,
    }


def test_semantic_layer_can_construct_without_duckdb_runtime():
    code = """
import builtins
import json

real_import = builtins.__import__

def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
    if name == "duckdb":
        raise ModuleNotFoundError("No module named 'duckdb'", name="duckdb")
    return real_import(name, globals, locals, fromlist, level)

builtins.__import__ = blocked_import

from sidemantic import SemanticLayer

layer = SemanticLayer()
try:
    layer.adapter.execute("select 1")
except ModuleNotFoundError as exc:
    error_name = exc.name
else:
    error_name = None

print(json.dumps({
    "dialect": layer.dialect,
    "adapter": type(layer.adapter).__name__,
    "error_name": error_name,
}))
"""

    result = subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)

    assert json.loads(result.stdout) == {
        "dialect": "duckdb",
        "adapter": "UnavailableDatabaseAdapter",
        "error_name": "duckdb",
    }
