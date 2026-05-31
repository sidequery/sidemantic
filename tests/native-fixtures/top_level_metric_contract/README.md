# Top-Level Metric Contract

Validates the portable native contract for top-level metrics and parameters.

Top-level metrics may have graph-style names, including dotted names, but the Rust
runtime must be able to infer exactly one owning model before query compilation.
Top-level parameters remain graph-scoped and must round-trip through Python and
interpolate in Python and Rust query paths.
