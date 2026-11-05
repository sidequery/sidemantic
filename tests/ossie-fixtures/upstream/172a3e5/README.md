# Apache Ossie latest-draft test snapshot

Files in this directory are unmodified Apache Ossie assets from commit
[`172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1`](https://github.com/apache/ossie/tree/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1),
observed November 5, 2026. Relative paths match the upstream checkout. Existing
Apache License 2.0 headers are retained.

The validator requires its shared duplicate-key YAML loader at
`python/src/ossie/_loading.py`. The local core and ontology schemas are used for
reference resolution; no network lookup is needed. Explicit schema arguments
select the logical, ontology, or standalone mapping family.

`tests/interchange/ossie/test_latest_upstream_gate.py` checks native graph exports,
current model constructs, invalid input, and the unmodified flights mapping and
ontology examples with this pinned official validator. These are draft tests,
not certification of future upstream commits or the entire query specification.

Runtime schemas live separately in `sidemantic/interchange/ossie/schemas/` with
checksum-verified manifest records and documented deterministic local-URI
rewrites. Historical snapshots in sibling directories remain unchanged.
