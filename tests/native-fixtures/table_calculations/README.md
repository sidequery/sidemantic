# Table Calculations

Valid native fixture that proves Rust can compile query-local table calculations into SQL window expressions.

Python currently applies table calculations after query execution, so the Python fixture runner compiles the base query while the Rust runner asserts the table-calculation SQL shape.
