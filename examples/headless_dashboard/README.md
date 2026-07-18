# Declarative Dashboard Example

This example shows the declarative dashboard authoring path in the canonical React application.

The dashboard is defined in committed files:

- `models.yml` defines the semantic model.
- `dashboard.yml` configures the official Sidemantic web UI.
- `dashboard.ts` shows the same dashboard shape with generated TypeScript field types.
- `sidemantic.generated.ts` is generated from the semantic model and committed here so the typing contract is visible.

`setup_data.py` only creates demo data. It does not define the dashboard.

## Run It

```bash
cd examples/headless_dashboard
uv run setup_data.py
uv run sidemantic dashboard validate
uv run sidemantic dashboard serve
```

Then open:

```text
http://127.0.0.1:4400/
```

## Regenerate TypeScript Types

```bash
uv run sidemantic dashboard types \
  --out sidemantic.generated.ts
```

`dashboard serve` discovers the project model, database, and YAML or JSON dashboard spec, validates
them, and serves the canonical React UI. The TypeScript file is for app authors who want
editor-checked metric and dimension names while producing the same dashboard configuration shape.

The cross-library renderers remain available as Python library examples under
`examples/integrations/`; they are not a second dashboard product or CLI frontend.
