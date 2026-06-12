# Table Calculations

Valid native fixture that proves query-local table calculations produce the same
result in Python and Rust for the shared subset.

Python applies table calculations after query execution with
`TableCalculationProcessor`. Rust compiles the same calculations into SQL window
expressions, and the Rust runner also asserts the generated SQL shape.
