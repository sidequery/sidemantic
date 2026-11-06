# Snowflake Cortex YAML compatibility

The `snowflake` format adapter imports and exports Cortex semantic-model YAML. It is separate from the Snowflake SQL database connector.

The adapter preserves complete primary keys, unique keys, compound relationship column pairs, explicitly declared join types, model names/descriptions, table synonyms, and query-backed `base_table.definition` sources. Missing primary keys remain unknown. Declared numeric precision and temporal types survive roundtrips, including `NUMBER(38,0)`, `DECIMAL(12,2)`, and `TIMESTAMP_TZ(9)`. Numeric precision no longer causes a dimension to become text. Native dimension type edits take precedence over incompatible retained type metadata.

Supported Cortex enrichment includes synonyms, sample values, search-service definitions, private access, labels/tags, non-additive dimensions, relationship names used by metrics, verified queries, and custom instructions. Preserved metadata is distinct from executable behavior: unsupported metric types and platform-only features still require review before using the output.

Relationships export all key pairs, resolving omitted target keys from the referenced model where possible. Missing or mismatched keys, custom SQL predicates, cross joins, and junction-model relationships cannot be represented as Cortex column-pair relationships and raise an error. Only `many_to_one` and `one_to_one` relationships and `left_outer`/`inner` join types are emitted. Reverse, many-to-many, full-outer, and right-outer relationships raise an error rather than producing unsupported vendor YAML. This follows the [Snowflake semantic-view YAML relationship specification](https://docs.snowflake.com/en/user-guide/views-semantic/semantic-view-yaml-spec). Explicit supported join types populate the runtime join hint; an omitted join type keeps native query filtering behavior.

An unchanged implicit join type remains omitted on export. Native edits to either `join_type` or the runtime `bsl_how` hint override the imported value and are emitted explicitly; conflicting edits to both are rejected.
