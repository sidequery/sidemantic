# Sigma compatibility

`--from sigma` and `--to sigma` use the separately installed official
`ossie-sigma` executable. Sidemantic never installs it, downloads dependencies,
or calls Sigma automatically. Install it in a separate environment and expose
its executable on `PATH`; the converter requires Python 3.11+. See the
[upstream converter documentation](https://github.com/apache/ossie-converters/blob/011ea9f9a1e2c941f351e18e071622339ccae881/sigma/README.md).

The bridge was tested against `apache/ossie-converters` commit
`011ea9f9a1e2c941f351e18e071622339ccae881`, with its `apache/ossie` submodule at
`01a5488d840deda309aac68cc0b312c195369b8d`. Its checks remain conservative when a
different installed converter version is used.

## CLI

```sh
sidemantic convert data-model.json --from sigma --to sidemantic \
  --output models.yml

sidemantic convert models.yml --from sidemantic --to sigma \
  --ossie-expression-dialect ANSI_SQL --ossie-scope commerce \
  --output data-model.json
```

Select the format explicitly. Input is one Sigma Data Model code-representation
JSON file, the format used by `/v2/dataModels/{id}/spec`; output is one such JSON
file. Obtaining or applying that file through Sigma is a separate operation.

Import uses the official converter's translated ANSI SQL and lowers to DuckDB by
default. `--ossie-target-dialect` selects another supported execution dialect.
Export requires `--ossie-expression-dialect ANSI_SQL`: other dialect labels are
rejected, and the bridge does not relabel their SQL. `--ossie-scope` optionally
names the intermediate semantic model (default `sidemantic`). Official-compatible,
portable Ossie export is applied internally; `--ossie-official-compatible` itself
is reserved for `--to ossie`. Use `--force` only to replace an existing output.

## Supported native subset

| Construct | Bridge contract |
|---|---|
| Root | `kind: data-model`, `name`, optional `description`, `pages`, and ordinary model identity/version/audit fields (`schemaVersion`, `dataModelId`, `folderId`, `documentVersion`, `latestDocumentVersion`, `createdAt`, `createdBy`, `updatedAt`, `updatedBy`, `ownerId`, `url`). |
| Page | Only `id`, `name`, and `elements`; the document must contain table elements. |
| Element | `kind: table`, unique nonempty `id`, optional `name`/`description`, `source`, nonempty `columns`, optional `metrics`, `relationships`, and `uniqueKeys`. |
| Source | Only `kind: warehouse-table`, `path`, and optional `connectionId`; `path` must be nonempty and contain only simple unquoted identifiers (`[A-Za-z_][A-Za-z0-9_]*`). SQL, join, union, and other derived source kinds are rejected. |
| Column/metric | Only `id`, `name`, `formula`, `description`, and `format`. Columns require direct unqualified physical references such as `[raw_amount]`; calculated formulas and sibling-column references are rejected. Metrics may use supported formulas with a faithful official SQL translation. |
| Relationship import | Only `id`, `name`, `description`, `targetElementId`, `keys`, and optional `relationshipType: N:1`. Every key needs both column IDs, each resolving to exactly one modeled column on its respective element. Raw `inode-...` physical references are outside this subset. |

All tables must share the same `connectionId`, or all must leave it unspecified.
Mixed connections are rejected because the executable graph cannot preserve
separate connection identities. Export may omit connection IDs; configure them
before applying the file to Sigma. No connection mapping, credentials, or live
API validation are provided.

The pinned converter joins source path components with dots on import and splits
SQL source names at dots on export. Names containing dots, spaces, or SQL quotes
are therefore rejected in both directions, before output is published. For
example, `path: ["a.b"]` cannot be treated as `path: ["a", "b"]`.

Calculated columns are outside this bridge subset because the pinned converter
can translate references to sibling columns as physical SQL identifiers without
expanding the sibling's formula. A renamed column `amount = [raw_amount]` is
supported, but a second column referring to `[amount]` is rejected; otherwise it
could read a different physical column named `amount`.

Unknown native properties are rejected, including filters, column security,
hidden/presentation layout configuration, grouping, sorting, and metric timeline
settings. Unsupported user-dependent or parameterized formulas fail translation
or strict Ossie lowering. The native IDs and presentation metadata accepted by
the file reader are not retained by the executable graph; generated IDs must not
be treated as a faithful update to existing Sigma objects.

## Relationship export limitation

The pinned official exporter can synthesize a relationship's `targetColumnId`
from the **source** element's column-ID map. The result can reference a column
that does not exist on the target element. Sidemantic validates native IDs and
ordered key pairs before publishing and **rejects those exports**, including the
composite-key case exercised during validation. Prefer models without
relationships for this pinned Sigma export path; do not assume relationship
export works merely because import succeeded. No repair or key omission is
performed silently.

## Fidelity and checks

The bridge stages conversion through validated Ossie, rejects empty output,
checks model/field/metric/relationship counts, and compares relationship
endpoints and ordered key pairs. It binds the official converter's quoted
metric references to their exact logical declarations without changing physical
field SQL. Export rejects features requiring Sidemantic runtime extensions.

Upstream stderr is surfaced. Every upstream warning is checked; only the
reported `OPAQUE_DATATYPE` approximation is allowed. Unknown warnings, dropped
metrics, untranslatable expressions, and unresolved relationships fail the
conversion. Existing destinations remain unchanged when conversion is rejected.

Offline tests exercised native and Sidemantic-origin exports, reimports,
DuckDB aggregation results, composite-key import, and rejection of incorrect
exported keys. They provide no blanket Sigma feature or deployment guarantee.
See [conversion fidelity and Ossie interoperability](conversion.md) for the
separate import/export reports and publication rules.
