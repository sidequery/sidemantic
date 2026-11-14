# Pinned Ossie logical schemas

These Apache-2.0 assets are exact copies of the shared runtime schemas in
`sidemantic/interchange/ossie/schemas/logical/<profile>/schema.json`. They are
bundled inside the Rust crate so standalone Cargo and Python source distributions
can validate offline without files outside the package.

| File/profile | Immutable upstream revision |
| --- | --- |
| `0.1.1.json` | OSI `faf581054dcf7964d5fe0ceae7d6f415c8ce32a5` |
| `0.2.0.dev0.json` | Apache Ossie `831f48e582731cf1ee2e65380ca5abf8157869c7` |
| `0.2.0.dev0-b6c702e.json` | Apache Ossie `b6c702ed1c07e91382a69e870c875cbd19570828` |
| `0.2.0.dev0-172a3e5.json` | Apache Ossie `172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1` |

The shared `schemas/manifest.json` records original URLs, hashes, and any runtime
identifier transformations. OSI's original path is `core-spec/osi-schema.json`;
Apache Ossie's is `core-spec/ossie-schema.json`. `LICENSE` and `NOTICE` accompany
the redistributed assets. `test_native_import_parity.py` verifies byte-for-byte
identity with the canonical shared files.

The native evaluator supports every assertion used by these four immutable
schemas, checks the complete schema vocabulary at initialization, and rejects
unknown assertions or references outside the bundled schema. It is not a
user-supplied or general-purpose JSON Schema service. Schema changes require
updating both copies, provenance, and conformance tests explicitly.
