# Relationship Key Resolution

Verifies explicit relationship-key compatibility between Python and Rust:

- `one_to_many` and `one_to_one` relationships declare their foreign-key columns
- `many_to_one` relationships declare their foreign-key columns instead of relying on naming conventions
- an omitted relationship `primary_key` safely resolves to the related model's declared primary key
