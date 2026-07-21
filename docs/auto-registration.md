# Model Registration

Explicit registration is the stable definition style for applications, loaders,
and reusable libraries:

```python
from sidemantic import Metric, Model, SemanticLayer

layer = SemanticLayer(auto_register=False)
layer.add_model(Model(name="orders", table="analytics.orders"))
layer.add_metric(Metric(name="revenue", type="derived", sql="orders.revenue"))
```

Sidemantic historically made the most recently constructed `SemanticLayer` an
ambient target for every subsequently constructed `Model` and standalone
`Metric`. Omitting `auto_register` still enables that behavior for compatibility,
but now emits a `DeprecationWarning`. Pass `auto_register=True` to opt in
explicitly. The omitted default will become `False` in a future release.

For concise, scoped definitions, a context manager remains supported and restores
the previously active layer when nested:

```python
layer = SemanticLayer(auto_register=False)
with layer:
    Model(name="orders", table="analytics.orders")
```

Import adapters, migration tooling, and application startup code should use
`auto_register=False` and add parsed definitions explicitly. This avoids model
leakage between unrelated projects, tasks, or concurrent contexts.
