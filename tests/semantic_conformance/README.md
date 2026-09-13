# Semantic compiler conformance

`fixtures/native/*.yml`, `fixtures/cube.yml`, and `fixtures/ossie.yml` are independently authored source
inputs. `fixtures/seed.sql` supplies deliberately small synthetic data; these are
not production datasets. `fixtures/cases.yml` specifies expected columns and
rows independently of either compiler. Its error cases require compile-time
rejection with a specific exception and diagnostic.

`test_reference.py` loads the source adapter into the semantic layer,
compiles the structured query, and executes generated SQL in DuckDB. It checks
the database's actual output column names as well as row values. Each case gets
a fresh layer and connection. `test_cli.py` checks the supported single-query
join rewrite through the CLI for both source formats and executes the output.
The Ossie case uses schema-validated source parsing and lowering, verifies the
complete aggregate expression survives the handoff, and checks its independent
result using the same compiler parameters.

Run with `uv run pytest --no-cov -q tests/semantic_conformance`.

Both tests parameterize explicit `python` and `rust` engine selection. Rust cases
require the real extension; absence skips only the Rust parameters and is not
acceptance evidence. The final acceptance run must execute those parameters
with the versioned handoff implementation installed. `rust_unsupported` entries
require typed capability rejection instead of Python fallback. Independent
result assertions must never be replaced with one compiler's observed output.
