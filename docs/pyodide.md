# Pyodide Runtime

Sidemantic's browser/WASM path is a no-dependency wheel install. Install the Pyodide-compatible runtime packages first, then install the Sidemantic wheel with dependency resolution disabled:

```python
import micropip

await pyodide.loadPackage(["micropip", "pydantic", "pyyaml", "jinja2"])
await micropip.install(["sqlglot", "lkml", "inflect"], deps=False)
await micropip.install("emfs:/tmp/sidemantic-<version>-py3-none-any.whl", deps=False)
```

This is intentional. The published Python package includes CLI/database dependencies that are not part of the Pyodide runtime contract. The supported Pyodide import surface is the core semantic model API, for example:

```python
from sidemantic import Model, Dimension, Metric, Relationship
from sidemantic.core.semantic_graph import SemanticGraph
```

Optional server, workbench, chart, and database execution paths are not Pyodide targets.
