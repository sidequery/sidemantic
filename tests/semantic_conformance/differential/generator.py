"""Generate bounded, valid semantic graphs, queries, and physical populations.

The index determines the family; a private RNG determines everything else. A
case can therefore be regenerated without generating any preceding cases.
"""

import json
import random
from copy import deepcopy
from datetime import date, timedelta

from .harness import Case

FAMILIES = (
    "base",
    "derived",
    "ratio",
    "cumulative",
    "filtered",
    "dimension_join",
    "fanout",
    "multi_source",
    "double_fanout",
    "nested_derived",
    "graph_metrics",
    "composite_join",
    "default_time",
)


def generate_case(seed: int, index: int) -> Case:
    rng = random.Random(f"sidemantic-differential-v1:{seed}:{index}")
    family = FAMILIES[index % len(FAMILIES)]
    numeric_type = rng.choice(["integer", "double", "decimal(12,2)"])
    amount = rng.choice(["amount", "amount * quantity", "coalesce(amount, 0)", "amount - quantity"])
    aggregation = rng.choice(["sum", "avg", "min", "max", "count", "count_distinct"])
    metric_filter = rng.choice(["status = 'paid'", "amount > 0", "status IS NULL", "quantity >= 2"])
    models = [
        {
            "name": "facts",
            "table": "generated_facts",
            "primary_key": "id",
            "dimensions": [
                {"name": "id", "type": "numeric"},
                {"name": "category", "type": "categorical", "sql": rng.choice(["category", "upper(category)"])},
                {"name": "status", "type": "categorical"},
                {"name": "amount", "type": "numeric"},
                {"name": "quantity", "type": "numeric"},
                {"name": "account_id", "type": "numeric"},
                {"name": "day", "type": "time", "granularity": "day"},
            ],
            "metrics": [
                {"name": "total", "agg": "sum", "sql": amount},
                {"name": "observed", "agg": aggregation, "sql": rng.choice(["amount", "quantity"])},
                {"name": "rows", "agg": "count"},
                {
                    "name": "filtered",
                    "agg": rng.choice(["sum", "count", "avg"]),
                    "sql": amount,
                    "filters": [metric_filter],
                },
                {"name": "derived", "type": "derived", "sql": f"total + rows * {rng.randint(1, 7)}"},
                {
                    "name": "ratio",
                    "type": "ratio",
                    "numerator": "total",
                    "denominator": rng.choice(["rows", "observed", "filtered"]),
                },
                {"name": "running", "type": "cumulative", "sql": "total"},
                {"name": "nested", "type": "derived", "sql": "derived + coalesce(ratio, 0)"},
            ],
            "segments": [
                {
                    "name": "selected",
                    "sql": rng.choice(
                        ["{model}.quantity >= 1", "{model}.status = 'paid'", "{model}.category IS NOT NULL"]
                    ),
                }
            ],
            "relationships": [],
        }
    ]
    facts = models[0]
    if rng.randrange(4) == 0:
        facts["metrics"][0]["fill_nulls_with"] = rng.choice([0, -1])
    if rng.randrange(5) == 0:
        facts["invariant_filters"] = ["quantity >= 0"]
    if family == "cumulative":
        window = rng.choice([None, "7 days", "1 month"])
        if window:
            facts["metrics"][6]["window"] = window
    row_count = rng.choice([0, 1, 2, 5, 9, 17])
    rows = []
    for row_id in range(1, row_count + 1):
        rows.append(
            [
                row_id,
                rng.choice([None, "a", "b", "a", "O'Brien"]),
                rng.choice([None, "paid", "open"]),
                rng.choice([None, 0, -4, 1, 1, 8, 12.5])
                if numeric_type != "integer"
                else rng.choice([None, 0, -4, 1, 1, 8]),
                rng.choice([0, 1, 2, 3]),
                rng.choice([None, 1, 2, 3, 99]),
                None
                if rng.randrange(8) == 0
                else (date(2024, 1, 1) + timedelta(days=rng.choice([0, 0, 1, 6, 31, 65]))).isoformat(),
            ]
        )
    tables = [
        {
            "name": "generated_facts",
            "columns": [
                ["id", "integer"],
                ["category", "varchar"],
                ["status", "varchar"],
                ["amount", numeric_type],
                ["quantity", "integer"],
                ["account_id", "integer"],
                ["day", "date"],
            ],
            "rows": rows,
        }
    ]
    dimensions = rng.sample(["facts.category", "facts.status"], rng.randrange(3))
    selected = {
        "base": ["total", "observed", "rows"],
        "derived": ["derived"],
        "ratio": ["ratio", "rows"],
        "cumulative": ["running"],
        "filtered": ["filtered", "rows"],
        "nested_derived": ["nested", "derived"],
    }.get(family, ["total", "rows", "observed"])
    relational_families = ["dimension_join", "fanout", "multi_source", "double_fanout", "composite_join"]
    if family in relational_families:
        # Cross metric semantics with relational topology, rather than keeping
        # filtering/windows/ratios isolated in their simplest single-table form.
        metric_shapes = ["total", "filtered", "derived", "ratio", "running", "nested"]
        shape_index = (index // len(FAMILIES) + relational_families.index(family)) % len(metric_shapes)
        selected = [metric_shapes[shape_index], "rows"]
    metrics = [f"facts.{name}" for name in selected]
    global_metrics = []
    features = [family, f"numeric:{numeric_type}", f"aggregation:{aggregation}"]
    if not rows:
        features.append("empty_source")
    if any(value is None for row in rows for value in row):
        features.append("null_values")
    if family == "default_time":
        facts["default_time_dimension"] = "day"
    elif "running" in selected or rng.randrange(4) == 0:
        grain = rng.choice(["day", "week", "month", "quarter", "year"])
        dimensions.insert(0, f"facts.day__{grain}")
        features.append(f"grain:{grain}")

    if family in {"dimension_join", "multi_source", "composite_join", "default_time"}:
        accounts = {
            "name": "accounts",
            "table": "generated_accounts",
            "primary_key": "id",
            "dimensions": [{"name": "region", "type": "categorical"}],
            "metrics": [{"name": "quota", "agg": "sum", "sql": "quota"}],
        }
        relationship = {"name": "accounts", "type": "many_to_one", "foreign_key": "account_id"}
        account_columns = [["id", "integer"], ["region", "varchar"], ["quota", numeric_type]]
        account_rows = [[i, rng.choice([None, "east", "west"]), rng.choice([None, 0, 10, 25])] for i in range(1, 5)]
        if family == "composite_join":
            accounts["primary_key"] = ["id", "tenant"]
            account_columns.append(["tenant", "integer"])
            for row in account_rows:
                row.append(1)
            account_rows += [[i, "other", 999, 2] for i in range(1, 3)]
            tables[0]["columns"].append(["tenant", "integer"])
            for row in rows:
                row.append(rng.choice([1, 2]))
            relationship.update(foreign_key=["account_id", "tenant"], primary_key=["id", "tenant"])
        facts["relationships"].append(relationship)
        models.append(accounts)
        tables.append({"name": "generated_accounts", "columns": account_columns, "rows": account_rows})
        if family != "default_time":
            dimensions.append("accounts.region")
        features.append("many_to_one")
        if family in {"multi_source", "default_time"}:
            global_metrics.append({"name": "combined", "type": "derived", "sql": "facts.total + accounts.quota"})
            metrics = ["combined", "accounts.quota", *metrics]
            if family == "default_time":
                metrics = ["combined"]
                dimensions = []

    if family in {"fanout", "double_fanout"}:
        for child in ["items", "refunds"] if family == "double_fanout" else ["items"]:
            facts["relationships"].append({"name": child, "type": "one_to_many", "foreign_key": "fact_id"})
            models.append(
                {
                    "name": child,
                    "table": f"generated_{child}",
                    "primary_key": "id",
                    "dimensions": [{"name": "label", "type": "categorical"}],
                    "metrics": [{"name": f"{child}_value", "agg": "sum", "sql": "value"}],
                }
            )
            child_rows = [
                [
                    i,
                    rng.choice(list(range(1, row_count + 1)) + [None, 99]),
                    rng.choice([None, "x", "y"]),
                    rng.choice([None, 0, 3, 5]),
                ]
                for i in range(1, rng.randint(3, 20))
            ]
            tables.append(
                {
                    "name": f"generated_{child}",
                    "columns": [["id", "integer"], ["fact_id", "integer"], ["label", "varchar"], ["value", "integer"]],
                    "rows": child_rows,
                }
            )
            dimensions.append(f"{child}.label")
            if rng.choice([False, True]):
                metrics.append(f"{child}.{child}_value")
        features.append("one_to_many")
        # Explicit duplication means fanout coverage does not depend on chance.
        if rows:
            tables[1]["rows"].extend([[100, 1, "duplicate", 2], [101, 1, "duplicate", 3]])

    if family == "graph_metrics":
        global_metrics = [
            {"name": "global_derived", "type": "derived", "sql": "facts.total - facts.rows"},
            {"name": "global_ratio", "type": "ratio", "numerator": "facts.filtered", "denominator": "facts.rows"},
        ]
        metrics = [metric["name"] for metric in global_metrics]

    features.extend(f"selected:{metric}" for metric in metrics)
    if any(row[6] is None for row in rows):
        features.append("null_time")
    query = {"metrics": metrics, "dimensions": dimensions}
    if rng.randrange(3) == 0:
        query["segments"] = ["facts.selected"]
        features.append("segment")
    filters = []
    if rng.randrange(3) == 0:
        filters.append(
            rng.choice(
                ["facts.amount IS NULL", "facts.quantity >= 2", "facts.status != 'open'", "facts.category = 'absent'"]
            )
        )
    if "facts.running" not in metrics and rng.randrange(4) == 0:
        filters.append(f"{metrics[0]} > {rng.choice([-1, 0, 5])}")
        features.append("metric_filter")
    if filters:
        query["filters"] = filters
    if family != "default_time" and rng.randrange(3) != 0:
        # Every group key is a final tie breaker, including nullable keys.
        query["order_by"] = ([f"{metrics[0]} DESC"] if rng.choice([False, True]) else []) + dimensions
        if not query["order_by"]:
            query.pop("order_by")
        if dimensions and rng.choice([False, True]):
            query["limit"] = rng.randint(1, 5)
            features.append("limit")
            if rng.randrange(3) == 0:
                query["offset"] = rng.randint(1, 3)
                features.append("offset")
    return Case(
        seed=seed,
        index=index,
        family=family,
        models=models,
        metrics=global_metrics,
        query=query,
        tables=tables,
        features=features,
    )


def data_variant(case: Case, variant: int) -> Case:
    """Independently seed physical rows without changing any compiler input."""
    if variant == 0:
        return deepcopy(case)
    result = deepcopy(case)
    result.data_variant = variant
    rng = random.Random(f"sidemantic-population-v1:{case.seed}:{case.index}:{variant}")
    models_by_table = {model["table"]: model for model in result.models}
    models_by_name = {model["name"]: model for model in result.models}
    table_by_name = {table["name"]: table for table in result.tables}
    for table in result.tables:
        model = models_by_table[table["name"]]
        primary = model.get("primary_key") or []
        primary = [primary] if isinstance(primary, str) else primary
        rows = []
        for row_index in range(rng.choice([0, 1, 2, 5, 9, 17])):
            row = []
            for column, dtype in table["columns"]:
                if column in primary:
                    # Exercise composite targets with repeated first-key values.
                    value = row_index // 2 + 1 if len(primary) > 1 and column == primary[0] else row_index + 1
                    if len(primary) > 1 and column == primary[1]:
                        value = row_index % 2 + 1
                elif dtype == "date":
                    value = (
                        None
                        if rng.randrange(5) == 0
                        else (date(2024, 1, 1) + timedelta(days=rng.choice([0, 1, 6, 31, 65]))).isoformat()
                    )
                elif dtype == "varchar":
                    choices = {
                        "category": [None, "a", "b", "O'Brien"],
                        "status": [None, "paid", "open"],
                        "region": [None, "east", "west"],
                    }.get(column, [None, "x", "y", "duplicate"])
                    value = rng.choice(choices)
                elif column == "quantity":
                    value = rng.choice([0, 1, 2, 3])
                else:
                    value = rng.choice([None, 0, 0, -4, 1, 3, 8, 25])
                    if value is not None and dtype != "integer" and rng.choice([False, True]):
                        value += rng.choice([0.25, 0.5, 0.75])
                row.append(value)
            rows.append(row)
        table["rows"] = rows

    # Assign foreign-key tuples together after all parent populations exist.
    # Repeated references, absent parents and null keys remain valid data.
    for model in result.models:
        for relationship in model.get("relationships", []):
            target = models_by_name[relationship.get("target_model", relationship["name"])]
            parent, child = (target, model) if relationship["type"] == "many_to_one" else (model, target)
            foreign = relationship["foreign_key"]
            foreign = [foreign] if isinstance(foreign, str) else foreign
            primary = relationship.get("primary_key") or parent["primary_key"]
            primary = [primary] if isinstance(primary, str) else primary
            parent_table = table_by_name[parent["table"]]
            child_table = table_by_name[child["table"]]
            parent_positions = [
                next(i for i, c in enumerate(parent_table["columns"]) if c[0] == key) for key in primary
            ]
            child_positions = [next(i for i, c in enumerate(child_table["columns"]) if c[0] == key) for key in foreign]
            references = [tuple(row[i] for i in parent_positions) for row in parent_table["rows"]]
            choices = references + [tuple(None for _ in primary), tuple(999 for _ in primary)]
            for row in child_table["rows"]:
                values = rng.choice(choices)
                for position, value in zip(child_positions, values):
                    row[position] = value
    result.features = [
        feature for feature in result.features if feature not in {"empty_source", "null_values", "null_time"}
    ]
    if not result.tables[0]["rows"]:
        result.features.append("empty_source")
    if any(value is None for table in result.tables for row in table["rows"] for value in row):
        result.features.append("null_values")
    if any(
        row[position] is None
        for table in result.tables
        for position, (_, dtype) in enumerate(table["columns"])
        if dtype == "date"
        for row in table["rows"]
    ):
        result.features.append("null_time")
    return result


def population_cases(case: Case, count: int):
    """Yield distinct populations per immutable compiler input, reproducibly.

    Empty-source draws may repeat. Skip exact duplicate populations so requested
    volume never includes the same data twice for a model/query/schema input.
    The stored variant index identifies the actual RNG draw, including retries.
    """
    seen = set()
    variant = 0
    while len(seen) < count:
        candidate = data_variant(case, variant)
        variant += 1
        signature = json.dumps(candidate.tables, sort_keys=True, separators=(",", ":"))
        if signature not in seen:
            seen.add(signature)
            yield candidate
