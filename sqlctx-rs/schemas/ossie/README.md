# Pinned Ossie logical schemas

The 0.1 and 0.2 Apache-2.0 assets are exact copies of the shared runtime schemas in
`sqlctx/interchange/ossie/schemas/logical/<profile>/schema.json`. They are
bundled inside the Rust crate so standalone Cargo and Python source distributions
can validate offline without files outside the package.

| File/profile | Immutable upstream revision |
| --- | --- |
| `0.1.1.json` | OSI `faf581054dcf7964d5fe0ceae7d6f415c8ce32a5` |
| `0.2.0.dev0.json` | Apache Ossie `831f48e582731cf1ee2e65380ca5abf8157869c7` |
| `0.2.0.dev0-b6c702e.json` | Apache Ossie `b6c702ed1c07e91382a69e870c875cbd19570828` |
| `0.2.0.dev0-172a3e5.json` | Apache Ossie `172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1` |
| `0.3.0.dev0-d27a6be.json` | Apache Ossie `d27a6beab519c8d3bf08ad5fc901c293205134eb` |

The native 0.3 draft schema is an unmodified copy of
[`core-spec/ossie-schema.json`](https://github.com/apache/ossie/blob/d27a6beab519c8d3bf08ad5fc901c293205134eb/core-spec/ossie-schema.json),
verified against upstream HEAD on 2026-11-24. Its SHA-256 is
`b833534b1da935e373cb75517f8b983dff9d57b399283bac5f78870c928ca1bb`.
It adds object-valued `nonportable_extensions` and unique key items. Native
lowering also supports model `AGG(dataset.metric)` composition; source measure
relations requiring `AGG(source.measure)` are explicitly unsupported.

The shared `schemas/manifest.json` records original URLs, hashes, and any runtime
identifier transformations. OSI's original path is `core-spec/osi-schema.json`;
Apache Ossie's is `core-spec/ossie-schema.json`. `LICENSE` and `NOTICE` accompany
the redistributed assets. `test_native_import_parity.py` verifies byte-for-byte
identity with the canonical shared files.

The native evaluator supports every assertion used by these five immutable
schemas, checks the complete schema vocabulary at initialization, and rejects
unknown assertions or references outside the bundled schema. It is not a
user-supplied or general-purpose JSON Schema service. Schema changes require
updating both copies, provenance, and conformance tests explicitly.
