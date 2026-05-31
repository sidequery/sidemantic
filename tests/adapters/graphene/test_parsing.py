from sidemantic import SemanticLayer, load_from_directory
from sidemantic.adapters.graphene import GrapheneAdapter


def test_graphene_table_imports_columns_joins_and_measures(tmp_path):
    model_file = tmp_path / "orders.gsql"
    model_file.write_text(
        """
-- Customer orders.
table orders (
  order_id INT64
  user_id INT64
  created_at TIMESTAMP #timeGrain=day
  status STRING -- One of 'Processing', 'Complete'
  amount FLOAT64 #currency=USD
  cost FLOAT64 #currency=USD

  join one users on user_id = users.id
  join many order_items on order_id = order_items.order_id

  is_complete: status = 'Complete'
  revenue: sum(case when is_complete then amount else 0 end) #currency=USD
  sum(amount) as gross_revenue #currency=USD
  cogs: sum(case when is_complete then cost else 0 end) #currency=USD
  profit: revenue - cogs #currency=USD
  profit_margin: profit / revenue #ratio
)
"""
    )

    graph = GrapheneAdapter().parse(tmp_path)

    assert "orders" in graph.models
    orders = graph.models["orders"]
    assert orders.table == "orders"
    assert orders.primary_key == "order_id"

    created_at = orders.get_dimension("created_at")
    assert created_at is not None
    assert created_at.type == "time"
    assert created_at.granularity == "day"

    is_complete = orders.get_dimension("is_complete")
    assert is_complete is not None
    assert is_complete.type == "boolean"
    assert is_complete.sql == "status = 'Complete'"

    revenue = orders.get_metric("revenue")
    assert revenue is not None
    assert revenue.agg == "sum"
    assert "amount" in revenue.sql
    assert revenue.value_format_name == "usd"

    gross_revenue = orders.get_metric("gross_revenue")
    assert gross_revenue is not None
    assert gross_revenue.agg == "sum"
    assert gross_revenue.sql == "amount"

    profit_margin = orders.get_metric("profit_margin")
    assert profit_margin is not None
    assert profit_margin.type == "derived"
    assert profit_margin.sql == "profit / revenue"
    assert profit_margin.value_format_name == "percent"

    users = next(rel for rel in orders.relationships if rel.name == "users")
    assert users.type == "many_to_one"
    assert users.foreign_key == "user_id"
    assert users.primary_key == "id"

    order_items = next(rel for rel in orders.relationships if rel.name == "order_items")
    assert order_items.type == "one_to_many"
    assert order_items.foreign_key == "order_id"


def test_graphene_alias_join_creates_role_model(tmp_path):
    (tmp_path / "flights.gsql").write_text(
        """
table flights (
  id BIGINT
  origin VARCHAR
  destination VARCHAR

  join one airports as origin_airport on origin = origin_airport.code
  join one airports as destination_airport on destination = destination_airport.code
)
"""
    )
    (tmp_path / "airports.gsql").write_text(
        """
table airports (
  code VARCHAR
  name VARCHAR
)
"""
    )

    graph = GrapheneAdapter().parse(tmp_path)

    assert "airports" in graph.models
    assert "origin_airport" in graph.models
    assert graph.models["origin_airport"].table == "airports"
    assert graph.models["origin_airport"].primary_key == "code"

    flights = graph.models["flights"]
    origin = next(rel for rel in flights.relationships if rel.name == "origin_airport")
    assert origin.type == "many_to_one"
    assert origin.foreign_key == "origin"
    assert origin.primary_key == "code"


def test_graphene_extend_updates_existing_model(tmp_path):
    (tmp_path / "models.gsql").write_text(
        """
table regional_orders as (
  select region, count(*) as num_orders, sum(amount) as total_revenue
  from orders
  group by 1
)

extend regional_orders (
  avg_order_value: total_revenue / num_orders #currency=USD
)
"""
    )

    graph = GrapheneAdapter().parse(tmp_path)
    model = graph.models["regional_orders"]

    assert model.sql.startswith("select region")
    assert model.get_dimension("region") is not None
    avg_order_value = model.get_dimension("avg_order_value")
    assert avg_order_value is not None
    assert avg_order_value.type == "numeric"
    assert avg_order_value.value_format_name == "usd"


def test_graphene_measure_composition_is_not_order_dependent(tmp_path):
    (tmp_path / "orders.gsql").write_text(
        """
table orders (
  id BIGINT
  amount FLOAT
  cost FLOAT

  profit: revenue - cogs #currency=USD
  revenue: sum(amount) #currency=USD
  cogs: sum(cost) #currency=USD
)
"""
    )

    graph = GrapheneAdapter().parse(tmp_path)
    orders = graph.models["orders"]

    profit = orders.get_metric("profit")
    assert profit is not None
    assert profit.type == "derived"
    assert orders.get_dimension("profit") is None


def test_graphene_metadata_annotations_are_preserved(tmp_path):
    (tmp_path / "events.gsql").write_text(
        """
table events (
  id BIGINT
  duration_minutes FLOAT #unit=minutes
  created_month: extract(month from created_at) #timeOrdinal=month_of_year
)
"""
    )

    graph = GrapheneAdapter().parse(tmp_path)
    events = graph.models["events"]

    duration = events.get_dimension("duration_minutes")
    assert duration is not None
    assert duration.metadata == {"graphene": {"annotations": {"unit": "minutes"}, "data_type": "FLOAT"}}

    created_month = events.get_dimension("created_month")
    assert created_month is not None
    assert created_month.metadata == {"graphene": {"annotations": {"timeOrdinal": "month_of_year"}}}


def test_graphene_clause_order_view_select_fields(tmp_path):
    (tmp_path / "regional_orders.gsql").write_text(
        """
table regional_orders as (
  from orders
  select region, count() as num_orders
)
"""
    )

    graph = GrapheneAdapter().parse(tmp_path)
    model = graph.models["regional_orders"]

    assert model.sql.startswith("from orders")
    assert model.get_dimension("region") is not None
    assert model.get_dimension("num_orders") is not None


def test_graphene_view_preserves_gsql_clause_order_query(tmp_path):
    (tmp_path / "carrier_performance.gsql").write_text(
        """
table carrier_performance as (
  from flights
  where cancelled = 'N'
  group by carrier
  select carrier, count() as flights, avg(arr_delay) as avg_arrival_delay
  order by flights desc
  limit 10
)
"""
    )

    graph = GrapheneAdapter().parse(tmp_path)
    model = graph.models["carrier_performance"]

    assert model.sql.startswith("from flights")
    assert "group by carrier" in model.sql
    assert model.get_dimension("carrier") is not None
    assert model.get_dimension("flights") is not None
    assert model.get_dimension("avg_arrival_delay") is not None


def test_graphene_view_ignores_cte_selects_when_inferring_projection(tmp_path):
    (tmp_path / "weekly_orders.gsql").write_text(
        """
table weekly_orders as (
  with filtered_orders as (
    select id, created_at, amount
    from orders
    where status = 'Complete'
  )
  from filtered_orders
  select date_trunc('week', created_at) as order_week, sum(amount) as revenue
)
"""
    )

    graph = GrapheneAdapter().parse(tmp_path)
    model = graph.models["weekly_orders"]

    assert "with filtered_orders as" in model.sql
    assert model.get_dimension("id") is None
    assert model.get_dimension("order_week") is not None
    assert model.get_dimension("revenue") is not None


def test_graphene_view_preserves_page_input_placeholders(tmp_path):
    (tmp_path / "filtered_orders.gsql").write_text(
        """
table filtered_orders as (
  from orders
  where region = $selected_region and created_at >= $start_date
  select region, count() as orders, sum(amount) as revenue
)
"""
    )

    graph = GrapheneAdapter().parse(tmp_path)
    model = graph.models["filtered_orders"]

    assert "$selected_region" in model.sql
    assert "$start_date" in model.sql
    assert model.get_dimension("region") is not None
    assert model.get_dimension("orders") is not None
    assert model.get_dimension("revenue") is not None


def test_graphene_file_can_include_example_query_after_models(tmp_path):
    (tmp_path / "orders.gsql").write_text(
        """
table orders (
  id BIGINT
  status STRING
  amount FLOAT

  revenue: sum(amount)
)

-- Example usage query, not a semantic model declaration.
from orders
select status, revenue
group by status
;
"""
    )

    graph = GrapheneAdapter().parse(tmp_path)

    assert sorted(graph.models) == ["orders"]
    assert graph.models["orders"].get_metric("revenue") is not None


def test_graphene_multiline_case_expression_and_metadata(tmp_path):
    (tmp_path / "flights.gsql").write_text(
        """
table flights (
  id BIGINT primary_key
  dep_delay INTEGER
  cancelled VARCHAR

  status: case
    when cancelled = 'Y' then 'Cancelled'
    when dep_delay > 15 then 'Delayed'
    else 'On Time'
  end

  on_time_departure_rate: avg(case when dep_delay <= 0 then 1 else 0 end) #ratio
)
"""
    )

    graph = GrapheneAdapter().parse(tmp_path)
    flights = graph.models["flights"]

    status = flights.get_dimension("status")
    assert status is not None
    assert (
        status.sql == "case when cancelled = 'Y' then 'Cancelled' when dep_delay > 15 then 'Delayed' else 'On Time' end"
    )

    rate = flights.get_metric("on_time_departure_rate")
    assert rate is not None
    assert rate.value_format_name == "percent"


def test_graphene_array_column_type_and_count_empty_call(tmp_path):
    (tmp_path / "events.gsql").write_text(
        """
table events (
  id BIGINT
  tags array<string>
  scores array<int64>

  event_count: count()
)
"""
    )

    graph = GrapheneAdapter().parse(tmp_path)
    events = graph.models["events"]

    tags = events.get_dimension("tags")
    assert tags is not None
    assert tags.type == "categorical"
    assert tags.metadata == {"graphene": {"data_type": "array<string>"}}

    scores = events.get_dimension("scores")
    assert scores is not None
    assert scores.metadata == {"graphene": {"data_type": "array<int64>"}}

    event_count = events.get_metric("event_count")
    assert event_count is not None
    assert event_count.agg == "count"


def test_graphene_composite_join_keys(tmp_path):
    (tmp_path / "events.gsql").write_text(
        """
table events (
  account_id INT64
  tenant_id INT64

  join one accounts as account on account_id = account.account_id and tenant_id = account.tenant_id
)
"""
    )
    (tmp_path / "accounts.gsql").write_text(
        """
table accounts (
  account_id INT64
  tenant_id INT64
  name STRING
)
"""
    )

    graph = GrapheneAdapter().parse(tmp_path)
    events = graph.models["events"]
    account = next(rel for rel in events.relationships if rel.name == "account")

    assert account.foreign_key == ["account_id", "tenant_id"]
    assert account.primary_key == ["account_id", "tenant_id"]


def test_graphene_explicit_id_primary_key_survives_join_many_candidate(tmp_path):
    (tmp_path / "accounts.gsql").write_text(
        """
table accounts (
  id BIGINT primary_key
  account_id BIGINT
  name STRING

  join many invoices on account_id = invoices.account_id
)
"""
    )
    (tmp_path / "invoices.gsql").write_text(
        """
table invoices (
  id BIGINT primary_key
  account_id BIGINT
  amount FLOAT
)
"""
    )

    graph = GrapheneAdapter().parse(tmp_path)
    relationship = next(rel for rel in graph.models["accounts"].relationships if rel.name == "invoices")
    path = graph.find_relationship_path("accounts", "invoices")

    assert graph.models["accounts"].primary_key == "id"
    assert relationship.primary_key == "account_id"
    assert relationship.foreign_key == "account_id"
    assert path[0].from_columns == ["account_id"]
    assert path[0].to_columns == ["account_id"]


def test_graphene_comment_markers_inside_strings_are_preserved(tmp_path):
    (tmp_path / "orders.gsql").write_text(
        """
table orders (
  id INT64
  status STRING

  status_label: case when status = '#paid' then 'paid--order' else 'other' end #description="Display label"
)
"""
    )

    graph = GrapheneAdapter().parse(tmp_path)
    status_label = graph.models["orders"].get_dimension("status_label")

    assert status_label is not None
    assert "'#paid'" in status_label.sql
    assert "'paid--order'" in status_label.sql
    assert status_label.description == "Display label"


def test_load_from_directory_detects_graphene_gsql(tmp_path):
    (tmp_path / "orders.gsql").write_text(
        """
table orders (
  id INT64
  amount FLOAT64

  revenue: sum(amount)
)
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert "orders" in layer.graph.models
    assert layer.graph.models["orders"].get_metric("revenue") is not None


def test_load_from_directory_accepts_graphene_percentile_aggregate(tmp_path):
    (tmp_path / "events.gsql").write_text(
        """
table events (
  id BIGINT
  latency FLOAT

  p90_latency: p90(latency)
)
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    metric = layer.graph.models["events"].get_metric("p90_latency")
    assert metric is not None
    assert metric.type == "derived"
    assert metric.sql == "PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY latency)"


def test_load_from_directory_parses_graphene_files_as_one_project(tmp_path):
    (tmp_path / "orders.gsql").write_text(
        """
table orders (
  id BIGINT
  user_id BIGINT
  amount FLOAT

  join one users as buyer on user_id = buyer.id
)
"""
    )
    (tmp_path / "users.gsql").write_text(
        """
table users (
  id BIGINT
  email STRING
)
"""
    )
    (tmp_path / "orders_metrics.gsql").write_text(
        """
extend orders (
  revenue: sum(amount)
)
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert "orders" in layer.graph.models
    assert "users" in layer.graph.models
    assert "buyer" in layer.graph.models
    assert layer.graph.models["orders"].get_metric("revenue") is not None
    assert layer.graph.models["buyer"].table == "users"
