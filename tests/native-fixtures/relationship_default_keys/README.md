# Relationship Default Keys

Verifies relationship default-key compatibility between Python and Rust:

- omitted `one_to_many` and `one_to_one` keys use `id`
- omitted `many_to_one` foreign keys use `{name}_id`
- omitted relationship `primary_key` resolves to the target model's declared primary key
