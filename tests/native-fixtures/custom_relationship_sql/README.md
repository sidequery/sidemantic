# Custom Relationship SQL

Validates that native relationship `sql` is honored by Python and Rust query
generation. The fixture uses `IS NOT DISTINCT FROM` in the custom join so the
result set differs from the default composite equality join when tenant IDs are
NULL.
