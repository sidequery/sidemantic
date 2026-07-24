# Consumption contracts

This example keeps the physical `orders` model separate from a curated Explore and
an immutable SavedQuery.

```bash
sidemantic validate examples/consumption_contracts --verbose
sidemantic info examples/consumption_contracts
sidemantic query --models examples/consumption_contracts \
  --saved-query top_revenue_statuses --dry-run
```

Point the query command at a connection containing the `orders` table to execute it.
