# Offline Apache Ossie validator gate

This directory is test-only and deliberately contains no network-dependent
test step.

`validation/validate.py` is an unmodified vendored copy of the Apache Ossie
validator at commit
`831f48e582731cf1ee2e65380ca5abf8157869c7`:

https://github.com/apache/ossie/blob/831f48e582731cf1ee2e65380ca5abf8157869c7/validation/validate.py

Its Apache License 2.0 header is preserved in the vendored file. The
`0.2.0.dev0` schema is the matching Apache Ossie `core-spec/ossie-schema.json`
from that same commit. The validator gate always passes that schema through
the CLI's explicit `--schema` option.

The Apache snapshot contains the draft `0.2.0.dev0` core schema, but not the
released `0.1.1` schema. The `0.1.1` schema here is the released upstream OSI
schema from legacy repository commit
`2af09b20b8ff5641c3780a9940dc0c94249ae1b3` (blob
`30210d18eb47a1ccb67a036557bf9730842d8c93`), retained so the exact pinned
validator can also validate the supported released JSON profile. It is not
presented as a schema from the `831f48e5` Apache commit. The dbt `0.1.0`
compatibility alias is intentionally not covered by this gate.

The gate invokes the vendored script as a subprocess with only these local
validator, schema, and generated-output paths. It checks both successful
canonical Sidemantic exports and a deliberately invalid input.
