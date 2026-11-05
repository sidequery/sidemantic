# Independent Ossie Layer 2 query corpus

Pinned to Apache Ossie commit `172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1`.
Cases are authored against the normative relational semantics, not an official
Apache certification suite. `model.json` and `setup.sql` define independent
commerce populations with duplicates, NULLs, unmatched keys and multiple facts.

The 85 success/error cases cover measure declarations and typing, relational
operators, filters, joins and fan-out, CTEs/subqueries, grouping, and independent
measure populations. Expected rows come from those populations or ordinary
physical SQL, never another backend. Tests run Python, public Rust without
fallback, and native Rust import/compile with DuckDB execution. Separate tests
cover views, discovery, dialect conversion, QUALIFY, and catalog identities.

Layer 3 cases and the complete requirement matrix arrive in the next stack PR.
