# Lightdash compatibility

`--from lightdash` and `--to lightdash` use the separately installed official
`ossie-lightdash` executable. Sidemantic never installs it, downloads dependencies,
or calls Lightdash automatically. Install it in a separate environment and expose
its executable on `PATH`; the converter requires Python 3.11+. See the
[upstream installation instructions](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/lightdash/README.md#installation).

The bridge was tested against `apache/ossie-converters` commit
`011ea9f9a1e2c941f351e18e071622339ccae881`, with its `apache/ossie` submodule at
`01a5488d840deda309aac68cc0b312c195369b8d`. Installing another revision does not
extend Sidemantic's supported subset automatically.

## CLI

```sh
sidemantic convert lightdash/models --from lightdash --to sidemantic \
  --ossie-expression-dialect ANSI_SQL --output models.yml

sidemantic convert models.yml --from sidemantic --to lightdash \
  --ossie-expression-dialect ANSI_SQL --ossie-scope commerce \
  --output lightdash-project
```

Select the format explicitly. Both directions require
`--ossie-expression-dialect` to describe the actual input SQL; this flag does not
relabel arbitrary warehouse SQL safely. Import lowers through Ossie to DuckDB by
default; `--ossie-target-dialect` selects another supported execution dialect.
`--ossie-scope` optionally names the export's intermediate semantic model
(default `sidemantic`). The bridge applies official-compatible, portable Ossie
export internally; do not pass `--ossie-official-compatible`, which is reserved
for `--to ossie`.

Export creates a directory containing `lightdash/models/*.yml` and
`lightdash.config.yml`. With `ANSI_SQL`, the upstream CLI writes
`warehouse.type: CHANGE_ME`; configure it before using Lightdash. Known warehouse
dialects such as `SNOWFLAKE`, `BIGQUERY`, and `DATABRICKS` let the upstream CLI
populate that type. No credentials, warehouse connection, compilation, or
deployment are verified. Use `--force` only when replacing an existing output.

## Supported native subset

Import accepts one native model YAML file or a directory of native model files.
It does **not** accept dbt schema metadata or arbitrary dbt projects. The supported
native model types are `model`, `model/v1`, and `model/v1beta`.

| Construct | Bridge contract |
|---|---|
| Model | Identifier-like `name`, nonempty `sql_from`, optional `description`, `primary_key`, `label`, and `ai_hint`; nonempty `dimensions`; optional `metrics` and `joins`. |
| Dimension | `name`, `type`, `sql`, `description`, `label`, `ai_hint`, `metrics`, and `hidden`. SQL may use `${TABLE}` for physical columns, but references to other dimensions or metrics are rejected. `time_intervals` may be absent or `OFF`. Hidden is a presentation hint, not an enforced access restriction. |
| Metric | Named metric mappings on models or dimensions; only `type`, `sql`, `description`, `label`, `ai_hint`, and `percentile`. Expressions must survive official conversion and strict Ossie lowering. |
| Join | `join`, `sql_on`, and optional `relationship: many-to-one`. Only direct `${source.field} = ${target.field}` pairs joined by `AND`; simple identifier names, no aliases, chained joins, self-joins, or extra predicates. |
| Project config | An optional `lightdash.config.yml` containing only `name`, `version`, and `warehouse.type`. It is not used to infer the SQL dialect or establish a connection. |

Unknown native properties are rejected. This includes model/metric filters,
required filters, access controls, additional dimensions, and unsupported project
configuration. Jinja templates, `${ld.*}` user attributes, and
`${lightdash.*}` parameters are rejected. This subset is deliberately narrower
than the official converter's preservation capabilities.

Dimension references such as `${amount} * 2` are outside the bridge subset:
the pinned converter can turn them into physical column references without
expanding the referenced dimension's SQL. This could return a different value
when a logical dimension has been renamed or calculated. Use explicit physical
SQL such as `${TABLE}.raw_amount * 2` instead.

## Fidelity and checks

The bridge stages conversion through validated Ossie, rejects empty outputs,
and checks model, field, metric, and relationship counts. It also compares join
endpoints and ordered field pairs. A model rename that changes those identities
can therefore be refused. Export rejects semantics requiring Sidemantic's runtime
extension, including invariant filters, instead of sending an empty sentinel to
Lightdash.

Upstream stderr is surfaced. Conversion issues fail the operation except the
explicitly reported `SOURCE_UNQUALIFIED`, `METRIC_REFERENCE_INLINED`, and
`DIMENSION_TYPE_DEFAULTED` cases. Defaulted types require review. Native
presentation metadata is not retained by the executable graph; this is not a
lossless project archive.

Offline tests exercised native and Sidemantic-origin exports, reimports,
DuckDB aggregation results, and a two-column join. Those checks do not guarantee
all Lightdash features or live deployment compatibility. See
[conversion fidelity and Ossie interoperability](conversion.md) for separate
import/export reports and output publication rules.
