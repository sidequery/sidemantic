# Conversion fidelity and Ossie interoperability

`sidemantic convert` reports import and export fidelity separately. Exact features
are listed when adapters report them; partial mappings require review; unsupported
or rejected mappings block publication. A report with no recorded losses is not
proof of vendor compilation or SQL equivalence. Shared conversion APIs stage exports,
reject empty results, and replace the destination only after checks pass. The CLI
also protects existing destinations when `--force` is used.

## What each export target carries

An export either returns the same numbers after re-import or names what it did
not carry. Two outcomes are possible when a target cannot represent a construct:

- **Blocked.** Dropping the construct would change a number or remove a metric,
  so nothing is written and `convert` exits 1 with one line per construct
  (`metric_filter: Filter on 'orders.completed_revenue' ...`).
- **Review required.** The written files are correct on their own but narrower
  than the source, for example a single-table format that has no joins. The
  files are written and `convert` lists each loss as a warning.

`Export readiness: ready (no reported losses)` is printed only when neither applies.

| Target | Filtered measure | Row count / distinct count | Primary key | Joins | Graph-level metrics |
|---|---|---|---|---|---|
| `sidemantic`, `metricflow`, `snowflake` | Carried | Carried | Carried | Carried | Carried |
| `ossie` | Carried | Carried | Carried | Carried | Carried (review: needs the Sidemantic runtime extension) |
| `bsl` | Carried (`where=`) | Carried | Carried | Carried | Blocked |
| `cube`, `lookml`, `malloy` | Carried | Carried | Carried | Carried | Blocked |
| `hex`, `omni`, `holistics` | Carried | Carried | Carried (hidden key dimension) | Carried | Blocked |
| `rill`, `superset` | Carried (folded into the expression) | Carried | Review: not exported | Review: not exported | Blocked |
| `thoughtspot` | Blocked | Carried (counts the key column) | Review: not exported | Carried | Blocked |
| `atscale-sml` | Blocked | Carried (counts the key column) | Carried (unique-key level) | Carried | Blocked |
| `gooddata` | Blocked | Blocked | Carried | Carried | Blocked |
| `tmdl` | Blocked | Row count carried; other aggregates need the column as a dimension | Carried when the key is a dimension; otherwise review: not exported | Carried | Blocked |

`lightdash` and `sigma` accept only portable Ossie core. Time granularity, keys
that are not declared fields, and joins without an edge identity are refused
before the vendor converter runs.

Formats without a measure filter (`snowflake`, `holistics`, `rill`, `superset`)
receive a filtered aggregate as `AGG(CASE WHEN <filter> THEN <value> END)`, which
their importers read back as the same aggregate. A filter that reads another
model's columns cannot be folded and blocks the export. A GoodData LDM fact is a
physical column that imports as `SUM`; every other aggregation is a MAQL metric,
which this adapter does not write.

`tests/adapters/test_export_roundtrip.py` enforces this table: it exports one
model to every registered target, re-imports it, and compares metric values
grouped by categorical dimensions on DuckDB against the native model. It checks
import through Sidemantic's own adapters, not compilation by the vendor, and it
does not compare time-dimension names or grains.

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

## Optional reference-converter bridges

Lightdash and Sigma are explicitly selected formats backed by separately installed
`ossie-lightdash` and `ossie-sigma` executables. Sidemantic does not download or
install them automatically. Lightdash accepts native model YAML, not arbitrary dbt
schema metadata. Sigma accepts native JSON. The bridge validates its supported
subset and rejects unsupported semantics before publishing output.

```sh
sidemantic convert models.yml --from sidemantic --to lightdash \
  --ossie-expression-dialect ANSI_SQL --output lightdash-project
sidemantic convert models.yml --from sidemantic --to sigma \
  --ossie-expression-dialect ANSI_SQL --output sigma.json
```

The expression dialect must be explicit for exports and Lightdash imports; Sigma
export currently accepts ANSI_SQL. These are file conversions, not live vendor
connections. Export diagnostics can also be captured from Python with
`capture_export_report`, alongside the existing `capture_import_report` API.

Detailed supported subsets and known limits: [Lightdash](lightdash.md) and [Sigma](sigma.md).
