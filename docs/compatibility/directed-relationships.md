# Directed relationships

Relationships with `metadata.reversible: false` retain their direction in the
Python graph and SQL planner. Relationships remain reversible by default.

```yaml
relationships:
  - name: customers
    type: many_to_one
    foreign_key: customer_id
    metadata:
      reversible: false
```

This relationship permits traversal from its declaring model to `customers`.
An automatic query anchor must reach the selected models through permitted
paths; an explicitly chosen invalid anchor is rejected. The same direction
restriction applies to junction paths and nested relationship roles.

The automatic anchor is the model that owns the selected metrics. When that model
cannot reach every selected model through permitted paths, the first selected
model that can do so anchors the query instead. A metric on `customers` grouped
by an `orders` dimension therefore anchors on `orders` here, because nothing may
traverse from `customers` to `orders`. If another permitted route exists, such as
`customers -> accounts -> orders`, the metric owner anchors and that route is the
one joined; name the anchor with an Explore to choose the population yourself.

Aggregate deduplication still accounts for repeated target rows. Queries with
metrics at different grains use preaggregation when either a many-to-one or
one-to-many path can repeat a metric's rows, including forward-only paths.

The experimental Rust engine does not implement this direction restriction and
rejects graphs requiring `relationship.forward_only` instead of ignoring it.
