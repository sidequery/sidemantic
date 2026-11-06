# Pinned converter interoperability regressions

`official-core-01a5488.schema.json` is an unmodified Apache Ossie core schema
from https://github.com/apache/ossie/blob/01a5488d840deda309aac68cc0b312c195369b8d/core-spec/ossie-schema.json,
used by Apache Ossie Converters at `011ea9f9a1e2c941f351e18e071622339ccae881`.
Its Apache 2.0 license is included. Tests require no external executable/network.

The independently authored orders fixture in `test_conversion_fidelity.py`
reproduces two observed interoperability boundaries: upstream consumers that
read model metrics only, and consumers requiring `expression.dialects` rather
than `expression.sql`. Expected field bindings are evaluated against DuckDB;
schema validation alone does not establish SQL or vendor equivalence.
