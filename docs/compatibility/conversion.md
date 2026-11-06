# Conversion fidelity and Ossie interoperability

`sidemantic convert` reports import and export fidelity separately. Exact features
are listed when adapters report them; partial mappings require review; unsupported
or rejected mappings block publication. A report with no recorded losses is not
proof of vendor compilation or SQL equivalence. Shared conversion APIs stage exports,
reject empty results, and replace the destination only after checks pass. The CLI
also protects existing destinations when `--force` is used.

## Exporting for official Ossie converters

```sh
sidemantic convert models.yml --from sidemantic --to ossie \
  --ossie-scope commerce --ossie-expression-dialect ANSI_SQL \
  --ossie-official-compatible --output commerce.ossie.yaml
```

The explicit compatibility option requires portable core output, moves dataset
metrics into model scope with qualified field references, and validates the
conservative flat `0.2.0.dev0` shape. It rejects duplicate metric names and
aggregates without a field binding, including `COUNT(*)` inside larger expressions.
It also rejects runtime-only semantics such as invariant filters and directed joins
that require Sidemantic's extension. This addresses the observed dataset-metric
loss in the official converter snapshot `011ea9f9a1e2c941f351e18e071622339ccae881`;
individual vendors still have narrower capabilities.

Fields without `logical_data_type` are marked for review: official exporters can
default an untyped numeric field to a string. Set the actual logical datatype
before relying on downstream type preservation; compatibility mode does not
guess numeric precision.

Ordinary Ossie exports can preserve unsupported native behavior in a Sidemantic
runtime extension. The CLI marks this as partial export fidelity. Other consumers
see only an empty sentinel dataset. Use `--ossie-portable-only` to require core
output without changing metric placement, or `--ossie-official-compatible` for
the shared reference-converter subset.

## Preserving a document

Graph conversion can lose vendor extensions and AI annotations. It now reports
those losses. To retain the complete valid document, use:

```sh
sidemantic convert source.ossie.yaml --from ossie --to ossie \
  --ossie-preserve-document --output preserved.ossie.yaml
```

This route validates without lowering into a runtime graph. Matching serialization
reuses source bytes; changing YAML to JSON preserves canonical data. It cannot be
combined with graph synthesis, scope selection, compatibility transformation, or
permissive parsing.

For physical source names with warehouse-specific quoting, supply both source and
execution dialects:

```sh
sidemantic convert source.ossie.yaml --from ossie --to sidemantic \
  --ossie-source-dialect bigquery --ossie-target-dialect bigquery \
  --output models.yml
```
