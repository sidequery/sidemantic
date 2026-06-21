---
version: 1
name: orders
table: orders
primary_key: order_id
dimensions:
  - name: status
    type: categorical
---

METRIC (
  name total_revenue,
  agg sum,
  sql amount
);

SEGMENT (
  name paid,
  sql {model}.status = 'paid'
);
