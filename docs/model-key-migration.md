# Explicit model keys and warehouse validation

Sidemantic no longer assumes that an omitted model or relationship key is named
`id`. The old fallback could make a structurally plausible model compile a join
against a nonexistent or non-unique column. Omitted keys now remain unknown.

## Compatibility and rollout

This change preserves models that declare keys and preserves explicit key metadata
imported from source formats. Keyless models also continue to work for single-model
queries. The intentional compatibility break is limited to operations that relied
on an invented key; those relationships now fail structural validation before SQL
compilation.

Use this staged rollout for an existing project:

1. Run offline validation and inventory keyless models:

   ```bash
   sidemantic validate ./models --verbose
   ```

2. Add each model's real `primary_key`. For composite keys, use a list. If a model
   has another model-wide unique key, declare it under `unique_keys`:

   ```yaml
   primary_key: [tenant_id, order_id]
   unique_keys:
     - [tenant_id, external_order_id]
   ```

3. Make relationship join columns explicit. `many_to_one` relationships need the
   source `foreign_key`; they may use the target model's primary key or an explicit
   relationship `primary_key`. `one_to_many` and `one_to_one` relationships need the
   foreign key on the related model and a declared key on the local model.

4. Validate metadata against the warehouse:

   ```bash
   sidemantic validate ./models --warehouse
   ```

5. In a controlled environment, verify key and cardinality data contracts:

   ```bash
   sidemantic validate ./models --warehouse --check-keys
   ```

`--check-keys` is opt-in because null and duplicate checks may scan warehouse
tables. It verifies model primary/unique keys, relationship-scoped alternate keys,
and one-to-one uniqueness assumptions.

## Relationship examples

Use the target model's declared primary key:

```yaml
models:
  - name: orders
    table: analytics.orders
    primary_key: order_id
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id

  - name: customers
    table: analytics.customers
    primary_key: customer_id
```

Use a scoped alternate unique key:

```yaml
relationships:
  - name: customers
    type: many_to_one
    foreign_key: customer_email
    primary_key: email
```

The relationship-level `primary_key` is an explicit uniqueness assertion for that
join. Prefer model `unique_keys` when multiple relationships or queries share the
same alternate identity.

There is no flag that restores inferred `id` joins. This is deliberate: retaining
that fallback would continue to compile SQL whose correctness cannot be established.

`sidemantic migrate generate` uses warehouse foreign-key constraints when available
to emit `many_to_one` cardinality. When query SQL alone only proves an equality
between two columns, migration emits an explicit direct `many_to_many` relationship;
it does not infer uniqueness from `id` or `_id` naming. After generation, replace
that conservative cardinality only when the warehouse contract establishes a
unique one side.
