# Vendored Apache Ossie schemas

`manifest.json` is the source of truth for schema identity, origin, commit, and
SHA-256 integrity. Logical schema files are byte-for-byte copies of their pinned
upstream assets.

The ontology directory contains both the untouched upstream schema and the
runtime schema. The runtime copy has two deterministic rewrites recorded in the
manifest: a unique ontology `$id`, and local URN references to the pinned logical
schema. Runtime validation registers only vendored resources and has no remote
resource retriever.

Do not update an asset without updating its source commit, checksums, fixture
expectations, and validation tests together.
