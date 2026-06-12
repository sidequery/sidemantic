# Native Fixture Suite

These fixtures define the shared native compatibility corpus for Python and Rust.

Each fixture is listed in `manifest.yml` and should include:

- `models/`: native YAML or SQL model files.
- `queries/`: structured semantic query YAML files.
- `seed/duckdb.sql`: optional DuckDB seed data for executable fixtures.
- `expected/validation.json`: expected validation status.
- `expected/result.json`: optional expected result rows.
- `README.md`: fixture intent and covered behavior.

Manifest entries use `valid: true` by default. Invalid fixtures set `valid: false` plus `error_contains` tokens. They are expected to fail validation or load without producing a usable graph.

Python tests load every manifest entry, compile every query for valid fixtures, and compare DuckDB result rows when `expected_result` is present.

Rust tests load every manifest entry and compile every query. Rust execution checks will be added as the ADBC fixture harness matures.

New fixtures should prefer small, focused semantic behavior over broad kitchen-sink coverage.
