# Headless Dashboard Example

This example shows the declarative dashboard authoring path.

The dashboard is defined in committed files:

- `models.yml` defines the semantic model.
- `dashboard.yml` defines the dashboard served by the sqlctx CLI.
- `dashboard.ts` shows the same dashboard shape with generated TypeScript field types.
- `sqlctx.generated.ts` is generated from the semantic model and committed here so the typing contract is visible.

`setup_data.py` only creates demo data. It does not define the dashboard.

## Run It

```bash
uv run examples/headless_dashboard/setup_data.py
uv run sqlctx dashboard validate examples/headless_dashboard/dashboard.yml \
  --models examples/headless_dashboard \
  --db examples/headless_dashboard/data/orders.db
uv run sqlctx dashboard serve examples/headless_dashboard/dashboard.yml \
  --models examples/headless_dashboard \
  --db examples/headless_dashboard/data/orders.db \
  --port 8877
```

Then open:

```text
http://127.0.0.1:8877/crossfilter.html
```

## Regenerate TypeScript Types

```bash
uv run sqlctx dashboard types \
  --models examples/headless_dashboard \
  --out examples/headless_dashboard/sqlctx.generated.ts
```

The CLI serves YAML or JSON dashboard specs. The TypeScript file is for app authors who want editor-checked metric and dimension names while producing the same dashboard configuration shape.

