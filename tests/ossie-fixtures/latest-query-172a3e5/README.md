# Apache Ossie query fixture

The SQL and YAML files in this directory are unmodified copies of
`compliance/query/scott/` and `compliance/query/cases/a01_grand_total/` at
[Apache Ossie commit 172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1](https://github.com/apache/ossie/tree/172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1/compliance/query).
Their original Apache 2.0 license headers are preserved.

The tests submit `query.ossie.sql` to each implementation after importing the
unchanged model and loading `scott.sql`. They compare against `expected.sql`.
The physical `reference.duckdb.sql` is retained for provenance and is not used
as the implementation under test.
