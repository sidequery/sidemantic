import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.adapters.malloy import MalloyAdapter, MalloySchemaExposureError
from sidemantic.adapters.malloy_queries import map_malloy_query
from sidemantic.fidelity import capture_import_report


def _parse(tmp_path, source: str, *, strict: bool = True):
    path = tmp_path / "model.malloy"
    path.write_text(source)
    return MalloyAdapter(strict=strict, warn_on_errors=False).parse(path)


def _layer(graph, *setup_sql):
    layer = SemanticLayer(auto_register=False, engine="python")
    for statement in setup_sql:
        layer.adapter.execute(statement)
    for model in graph.models.values():
        layer.add_model(model)
    for explore in graph.explores.values():
        layer.add_explore(explore)
    for saved_query in graph.saved_queries.values():
        layer.add_saved_query(saved_query)
    return layer


def test_typed_scalar_and_derived_metric_execute(tmp_path):
    graph = _parse(
        tmp_path,
        """source: orders is duckdb.table('orders') extend {
  dimension: state is state
  dimension: extended is price * quantity
  measure: revenue is sum(price)
  measure: doubled_revenue is revenue * 2
}
""",
    )
    model = graph.get_model("orders")
    assert model.get_dimension("extended").sql == "price * quantity"
    assert model.get_metric("doubled_revenue").sql == "revenue * 2"

    layer = _layer(
        graph,
        "create table orders (state text, price int, quantity int)",
        "insert into orders values ('CA', 10, 2), ('CA', 5, 3)",
    )
    assert layer.query(metrics=["orders.doubled_revenue"]).fetchone() == (30,)


def test_one_stage_query_maps_compiles_and_executes(tmp_path):
    graph = _parse(
        tmp_path,
        """source: orders is duckdb.table('orders') extend {
  dimension: state is state
  measure: revenue is sum(amount)
}
query: revenue_by_state is orders -> {
  group_by: state
  aggregate: revenue
  where: state != 'deleted'
  having: revenue > 10
  order_by: revenue desc
  limit: 5
}
""",
    )
    assert set(graph.saved_queries) == {"revenue_by_state"}
    layer = _layer(
        graph,
        "create table orders (state text, amount int)",
        "insert into orders values ('CA', 20), ('NY', 5), ('deleted', 100)",
    )
    assert layer.query(saved_query="revenue_by_state").fetchall() == [("CA", 20)]


def test_unsupported_expression_and_query_fail_closed(tmp_path):
    source = """source: orders is duckdb.table('orders') extend {
  dimension: unsafe is mystery_function(amount)
}
query: unsafe_query is orders -> { group_by: unsafe } -> { limit: 1 }
"""
    with capture_import_report() as report:
        graph = _parse(tmp_path, source, strict=False)
    assert graph.get_model("orders").get_dimension("unsafe") is None
    assert not graph.explores
    assert not graph.saved_queries
    assert any(warning["code"] == "malloy_query_pipeline_unsupported" for warning in graph.import_warnings)
    expression = next(feature for feature in report.features if feature.feature == "malloy_expression_function")
    assert expression.status == "rejected"
    assert expression.source.endswith("model.malloy")
    assert expression.location is not None

    with pytest.raises(MalloySchemaExposureError):
        _parse(tmp_path, source, strict=True)


def test_rejected_descendants_block_legacy_and_aggregate_roots_atomically(tmp_path):
    graph = _parse(
        tmp_path,
        """source: orders is duckdb.table('orders') extend {
  dimension: unsafe_pick is pick mystery_function(amount) when true else amount
  measure: unsafe_aggregate is sum(mystery_function(amount))
  measure: unsafe_filter is count() { where: mystery_function(amount) > 0 }
}
""",
        strict=False,
    )
    model = graph.get_model("orders")
    assert model.get_dimension("unsafe_pick") is None
    assert model.get_metric("unsafe_aggregate") is None
    assert model.get_metric("unsafe_filter") is None


def test_custom_connection_requires_explicit_dialect_for_sensitive_expression(tmp_path):
    source = """source: events is warehouse_prod.table('events') extend {
  dimension: event_label is amount::string
}
"""
    unresolved = _parse(tmp_path, source, strict=False)
    assert unresolved.get_model("events").get_dimension("event_label") is None

    path = tmp_path / "model.malloy"
    path.write_text(source)
    resolved = MalloyAdapter(
        strict=True,
        warn_on_errors=False,
        connection_dialects={"warehouse_prod": "bigquery"},
    ).parse(path)
    assert "STRING" in resolved.get_model("events").get_dimension("event_label").sql


@pytest.mark.parametrize(
    ("dialect", "cast_type"),
    [("duckdb", "TEXT"), ("postgres", "TEXT"), ("bigquery", "STRING"), ("snowflake", "VARCHAR")],
)
def test_connection_dialect_mapping_renders_supported_target_sql(tmp_path, dialect, cast_type):
    path = tmp_path / f"{dialect}.malloy"
    path.write_text("source: events is warehouse.table('events') extend { dimension: label is amount::string }\n")
    graph = MalloyAdapter(
        strict=True,
        warn_on_errors=False,
        connection_dialects={"warehouse": dialect},
    ).parse(path)
    assert cast_type in graph.get_model("events").get_dimension("label").sql


def test_query_catalog_does_not_admit_fields_from_unrelated_models(tmp_path):
    graph = _parse(
        tmp_path,
        """source: orders is duckdb.table('orders') extend {
  dimension: state is state
  measure: revenue is sum(amount)
}
source: inventory is duckdb.table('inventory') extend {
  dimension: sku is sku
}
query: unsafe is orders -> { group_by: sku aggregate: revenue }
""",
        strict=False,
    )
    assert not graph.explores
    assert not graph.saved_queries
    assert any(warning["code"] == "malloy_query_group_by_field_unknown" for warning in graph.import_warnings)


def test_rejected_source_filter_omits_entire_source(tmp_path):
    source = """source: orders is duckdb.table('orders') extend {
  where: mystery_function(amount) > 0
  dimension: amount is amount
}
"""
    graph = _parse(tmp_path, source, strict=False)
    assert "orders" not in graph.models
    with pytest.raises(MalloySchemaExposureError):
        _parse(tmp_path, source, strict=True)


def test_query_catalog_terminates_cycles_and_preserves_sibling_roles():
    people = Model(
        name="people",
        table="people",
        dimensions=[Dimension(name="name", type="categorical", sql="name")],
        metrics=[Metric(name="person_count", agg="count", sql="*")],
        relationships=[
            Relationship(name="manager", target_model="people", type="many_to_one", foreign_key="manager_id"),
            Relationship(name="home", target_model="places", type="many_to_one", foreign_key="home_id"),
            Relationship(name="work", target_model="places", type="many_to_one", foreign_key="work_id"),
        ],
    )
    places = Model(
        name="places",
        table="places",
        dimensions=[Dimension(name="city", type="categorical", sql="city")],
        relationships=[Relationship(name="resident", target_model="people", type="many_to_one")],
    )

    dimensions, metrics = MalloyAdapter._query_field_catalog({"people": people, "places": places}, "people")

    assert {"name", "manager.name", "home.city", "work.city"} <= dimensions
    assert {"person_count", "manager.person_count"} <= metrics
    assert "manager.manager.name" not in dimensions
    assert "home.resident.home.city" not in dimensions
    assert max(path.count(".") for path in dimensions | metrics) <= 5

    supported = map_malloy_query(
        "role_cities",
        "role_cities is people -> { group_by: home.city, work.city aggregate: person_count }",
        dimensions=dimensions,
        metrics=metrics,
    )
    assert supported.supported

    rejected = map_malloy_query(
        "cycle_escape",
        "cycle_escape is people -> { group_by: manager.manager.name aggregate: person_count }",
        dimensions=dimensions,
        metrics=metrics,
    )
    assert not rejected.supported
    assert rejected.diagnostics[0].code == "malloy_query_group_by_field_unknown"


def test_cyclic_relationship_query_import_is_bounded(tmp_path):
    source = """source: places is duckdb.table('places') extend {
  primary_key: id
  dimension: city is city
  join_one: resident is people with resident_id
}
source: people is duckdb.table('people') extend {
  primary_key: id
  dimension: name is name
  measure: person_count is count()
  join_one:
    manager is people with manager_id
    home is places with home_id
    work is places with work_id
}
query: reachable is people -> {
  group_by: manager.name, home.city, work.city
  aggregate: person_count
}
query: cycle_escape is people -> {
  group_by: manager.manager.name
  aggregate: person_count
}
"""
    graph = _parse(tmp_path, source, strict=False)
    assert "reachable" in graph.saved_queries
    assert "cycle_escape" not in graph.saved_queries
    assert any(warning["code"] == "malloy_query_group_by_field_unknown" for warning in graph.import_warnings)

    with pytest.raises(MalloySchemaExposureError, match="manager.manager.name"):
        _parse(tmp_path, source, strict=True)


def test_invalid_inline_source_is_isolated_from_outer_source_lenient(tmp_path):
    source = """source: orders is duckdb.table('orders') extend {
  dimension: amount is amount
  join_one: unsafe is duckdb.table('unsafe') extend {
    where: mystery_function(flag) > 0
    dimension: label is label
  } with unsafe_id
  dimension: after_inline is amount + 1
}
"""
    with capture_import_report() as report:
        graph = _parse(tmp_path, source, strict=False)

    assert "unsafe" not in graph.models
    orders = graph.get_model("orders")
    assert orders is not None
    assert orders.get_dimension("after_inline") is not None
    assert not orders.relationships
    assert any(feature.feature == "malloy_expression_function" for feature in report.features)


def test_invalid_inline_source_fails_strict_without_partial_models(tmp_path):
    source = """source: orders is duckdb.table('orders') extend {
  dimension: amount is amount
  join_one: unsafe is duckdb.table('unsafe') extend {
    where: mystery_function(flag) > 0
  } with unsafe_id
  dimension: after_inline is amount + 1
}
"""
    with pytest.raises(MalloySchemaExposureError):
        _parse(tmp_path, source, strict=True)


@pytest.mark.parametrize(
    ("dialect", "cast_token", "date_token", "regex_token"),
    [
        ("duckdb", "DECIMAL", "DATE_TRUNC", "REGEXP_MATCHES"),
        ("postgres", "DECIMAL", "DATE_TRUNC", "~"),
        ("bigquery", "NUMERIC", "TIMESTAMP_TRUNC", "REGEXP_CONTAINS"),
        ("snowflake", "DECIMAL", "DATE_TRUNC", "REGEXP_LIKE"),
    ],
)
def test_aggregate_arguments_and_filters_use_connection_dialect(tmp_path, dialect, cast_token, date_token, regex_token):
    source = """source: events is warehouse.table('events') extend {
  dimension: event_at is event_at
  dimension: label is label
  measure: cast_total is sum(amount::number)
  measure: first_month is min(event_at.month)
  measure: matching is count() { where: label ~ r'^x' }
}
"""
    path = tmp_path / f"{dialect}.malloy"
    path.write_text(source)
    graph = MalloyAdapter(
        strict=True,
        warn_on_errors=False,
        connection_dialects={"warehouse": dialect},
    ).parse(path)
    model = graph.get_model("events")

    cast_total = model.get_metric("cast_total")
    assert cast_total.agg == "sum"
    assert cast_token in cast_total.sql.upper()
    first_month = model.get_metric("first_month")
    assert first_month.agg == "min"
    assert date_token in first_month.sql.upper()
    matching = model.get_metric("matching")
    assert matching.agg == "count"
    assert regex_token in matching.filters[0].upper()


def test_aggregate_dialect_sensitive_descendants_fail_closed_when_unresolved(tmp_path):
    source = """source: events is warehouse.table('events') extend {
  dimension: event_at is event_at
  dimension: label is label
  measure: cast_total is sum(amount::number)
  measure: first_month is min(event_at.month)
  measure: matching is count() { where: label ~ r'^x' }
  measure: safe_count is count()
}
"""
    graph = _parse(tmp_path, source, strict=False)
    model = graph.get_model("events")
    assert model.get_metric("safe_count") is not None
    assert model.get_metric("cast_total") is None
    assert model.get_metric("first_month") is None
    assert model.get_metric("matching") is None

    with pytest.raises(MalloySchemaExposureError):
        _parse(tmp_path, source, strict=True)


def test_dialect_sensitive_aggregate_arguments_and_filter_execute_on_duckdb(tmp_path):
    graph = _parse(
        tmp_path,
        """source: events is duckdb.table('events') extend {
  dimension: label is label
  measure: cast_total is sum(amount::number)
  measure: first_month is min(event_at.month)
  measure: matching is count() { where: label ~ r'^x' }
}
""",
    )
    layer = _layer(
        graph,
        "create table events (amount varchar, event_at timestamp, label varchar)",
        "insert into events values ('2', '2024-02-03', 'x-one'), ('3', '2024-01-04', 'other')",
    )

    row = layer.query(metrics=["events.cast_total", "events.first_month", "events.matching"]).fetchone()
    assert float(row[0]) == 5
    assert str(row[1]).startswith("2024-01-01")
    assert row[2] == 1
