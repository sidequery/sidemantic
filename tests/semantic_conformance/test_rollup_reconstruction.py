"""Rollup routing against independently specified aggregate populations."""

from copy import deepcopy

import pytest

from sidemantic import Dimension, Metric, Model, PreAggregation, Relationship, SecurityPolicy, SemanticLayer
from sidemantic.semantic_handoff import graph_to_semantic_input


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Rollup execution requires the matching Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.graph.add_model(
        Model(
            name="orders",
            table="rollup_orders",
            primary_key="id",
            dimensions=[
                Dimension(name="status", type="categorical"),
                Dimension(name="customer_id", type="numeric"),
                Dimension(name="created", type="time"),
                Dimension(name="tenant", type="numeric"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="discount", agg="sum", sql="discount"),
                Metric(name="rows", agg="count"),
                Metric(name="count", agg="count", sql="amount"),
                Metric(name="avg_amount", agg="avg", sql="amount"),
                Metric(name="paid_average", agg="avg", sql="amount", filters=["status = 'paid'"]),
                Metric(name="paid_count", agg="count", sql="amount", filters=["status = 'paid'"]),
                Metric(name="net", type="derived", sql="revenue - discount"),
                Metric(name="per_order", type="ratio", numerator="revenue", denominator="rows"),
            ],
            pre_aggregations=[
                PreAggregation(
                    name="daily",
                    dimensions=["status", "customer_id"],
                    measures=["revenue", "discount", "rows", "count", "avg_amount", "paid_average", "paid_count"],
                    time_dimension="created",
                    granularity="day",
                )
            ],
        )
    )
    layer.adapter.execute("""
        create table rollup_orders(id integer,customer_id integer,status varchar,amount integer,discount integer,created timestamp,tenant integer);
        insert into rollup_orders values
          (1,1,'paid',10,1,'2026-01-01 01:00:00',1),(2,1,'paid',20,2,'2026-01-01 02:00:00',1),
          (3,2,'paid',null,5,'2026-01-01 03:00:00',1),(4,2,'open',40,4,'2026-01-02 01:00:00',1),
          (5,2,'paid',90,9,'2026-01-02 15:00:00',2),(6,3,'paid',50,5,'2026-02-01 01:00:00',1);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def build(layer, *, rust=False):
    model = layer.graph.models["orders"]
    rollup = model.pre_aggregations[0]
    if rust:
        pytest.importorskip("sidemantic_rs")
        from sidemantic.rust_bridge import generate_preaggregation_materialization_sql_with_rust

        sql = generate_preaggregation_materialization_sql_with_rust(model, rollup)
    else:
        sql = rollup.generate_materialization_sql(model)
    layer.adapter.execute(f"create table orders_preagg_daily as {sql}")


def execute(layer, query, *, routed=True):
    source = deepcopy(graph_to_semantic_input(layer.graph))
    sql = layer.compile(**query, use_preaggregations=True)
    assert graph_to_semantic_input(layer.graph) == source
    if layer.engine == "rust":
        assert layer.last_engine_selection["engine"] == "rust"
    assert ("orders_preagg_daily" in sql) is routed, sql
    cursor = layer.adapter.execute(sql)
    return [column[0] for column in cursor.description], cursor.fetchall()


@pytest.mark.parametrize("rust_build", [False, True], ids=["python-build", "rust-build"])
def test_derived_ratio_and_nullable_average_reconstruct_from_states(layer, rust_build):
    build(layer, rust=rust_build)
    assert execute(
        layer,
        {"metrics": ["orders.revenue", "orders.avg_amount", "orders.paid_average", "orders.net", "orders.per_order"]},
    ) == (["revenue", "avg_amount", "paid_average", "net", "per_order"], [(210, 42.0, 42.5, 184, 35.0)])


def test_average_and_derived_having_reconstruct_unselected_dependencies(layer):
    build(layer)
    query = {
        "metrics": ["orders.avg_amount"],
        "dimensions": ["orders.status"],
        "filters": ["orders.net > 100", "orders.status != 'missing'"],
        "order_by": ["orders.status"],
    }
    assert execute(layer, query) == (["status", "avg_amount"], [("paid", 42.5)])


@pytest.mark.parametrize("threshold,expected", [(100, [(210,)]), (250, [])])
def test_sum_having_filters_reaggregated_rollup_total(layer, threshold, expected):
    build(layer)
    query = {"metrics": ["orders.revenue"], "filters": [f"orders.revenue > {threshold}"]}
    assert execute(layer, query) == (["revenue"], expected)
    assert layer.adapter.execute(layer.compile(**query, use_preaggregations=False)).fetchall() == expected


def test_empty_rollup_count_is_zero_and_average_is_null(layer):
    layer.adapter.execute("delete from rollup_orders")
    build(layer)
    assert execute(layer, {"metrics": ["orders.rows", "orders.avg_amount", "orders.net", "orders.per_order"]}) == (
        ["rows", "avg_amount", "net", "per_order"],
        [(0, None, None, None)],
    )


@pytest.mark.parametrize("count_sql,count_filters", [(None, []), ("discount", []), ("amount", ["status = 'paid'"])])
def test_incompatible_average_denominator_falls_back_to_raw(layer, count_sql, count_filters):
    model = layer.graph.models["orders"]
    model.get_metric("count").sql = count_sql
    model.get_metric("count").filters = count_filters
    model.pre_aggregations[0].measures = ["avg_amount", "count"]
    build(layer)
    assert execute(layer, {"metrics": ["orders.avg_amount"]}, routed=False) == (["avg_amount"], [(42.0,)])


def test_nonnull_average_can_use_row_count(layer):
    model = layer.graph.models["orders"]
    model.get_metric("avg_amount").sql = "COALESCE(amount, 0)"
    model.get_metric("count").sql = None
    build(layer)
    assert execute(layer, {"metrics": ["orders.avg_amount"]}) == (["avg_amount"], [(35.0,)])


def add_customers(layer):
    layer.graph.models["orders"].relationships.append(
        Relationship(name="customers", type="many_to_one", foreign_key="customer_id")
    )
    layer.add_model(
        Model(
            name="customers",
            table="rollup_customers",
            primary_key="id",
            dimensions=[Dimension(name="name", type="categorical")],
        )
    )
    layer.adapter.execute(
        "create table rollup_customers(id integer,name varchar); insert into rollup_customers values (1,'a'),(2,'b'),(3,'c'),(4,'empty')"
    )


def test_join_key_rollup_preserves_remote_domain_and_metric_states(layer):
    add_customers(layer)
    build(layer)
    assert execute(
        layer,
        {
            "metrics": ["orders.revenue", "orders.avg_amount", "orders.net", "orders.rows", "orders.per_order"],
            "dimensions": ["customers.name"],
            "order_by": ["customers.name"],
        },
    ) == (
        ["name", "revenue", "avg_amount", "net", "rows", "per_order"],
        [
            ("a", 30, 15.0, 27, 2, 15.0),
            ("b", 130, 65.0, 112, 3, pytest.approx(130 / 3)),
            ("c", 50, 50.0, 45, 1, 50.0),
            ("empty", None, None, None, 0, None),
        ],
    )


def test_join_key_rollup_with_empty_source_retains_remote_members(layer):
    add_customers(layer)
    layer.adapter.execute("delete from rollup_orders")
    build(layer)
    assert execute(
        layer,
        {
            "metrics": ["orders.rows", "orders.avg_amount"],
            "dimensions": ["customers.name"],
            "order_by": ["customers.name"],
        },
    ) == (["name", "rows", "avg_amount"], [("a", 0, None), ("b", 0, None), ("c", 0, None), ("empty", 0, None)])


def test_join_key_rollup_alias_order_limit_and_local_dimension(layer):
    add_customers(layer)
    build(layer)
    query = {
        "metrics": ["orders.revenue"],
        "dimensions": ["customers.name", "orders.status"],
        "order_by": ["orders.revenue DESC"],
        "limit": 1,
        "offset": 1,
    }
    assert execute(layer, query) == (["name", "status", "revenue"], [("c", "paid", 50)])


@pytest.mark.parametrize("restriction", ["policy", "invariant"])
def test_mandatory_policy_bypasses_join_rollup(layer, restriction):
    add_customers(layer)
    build(layer)
    query = {"metrics": ["orders.revenue"], "dimensions": ["customers.name"], "order_by": ["customers.name"]}
    if restriction == "policy":
        layer.graph.models["orders"].security = SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"])
        query["user_attributes"] = {"tenant": 1}
    else:
        layer.graph.models["orders"].invariant_filters = ["tenant = 1"]
    assert execute(layer, query, routed=False) == (["name", "revenue"], [("a", 30), ("b", 40), ("c", 50)])


@pytest.mark.parametrize("rust_build", [False, True], ids=["python-build", "rust-build"])
def test_lambda_rebuilds_entire_boundary_bucket_from_fresh_source(layer, rust_build):
    rollup = layer.graph.models["orders"].pre_aggregations[0]
    rollup.type = "lambda"
    rollup.rollups = ["historical_metadata_only"]
    rollup.union_with_source_data = True
    rollup.build_range_end = "'2026-01-02 12:00:00'"
    build(layer, rust=rust_build)
    layer.adapter.execute("""
        update rollup_orders set amount = 45 where id = 4;
        insert into rollup_orders values (7,3,'paid',7,0,'2026-01-02 02:00:00',1);
        delete from rollup_orders where created < '2026-01-02';
    """)
    assert execute(
        layer, {"metrics": ["orders.revenue", "orders.avg_amount", "orders.rows", "orders.net", "orders.per_order"]}
    ) == (["revenue", "avg_amount", "rows", "net", "per_order"], [(222, 37.0, 7, 196, pytest.approx(222 / 7))])


def test_lambda_without_boundary_reads_only_materialized_state(layer):
    rollup = layer.graph.models["orders"].pre_aggregations[0]
    rollup.type = "lambda"
    rollup.union_with_source_data = True
    build(layer)
    layer.adapter.execute("delete from rollup_orders")
    assert execute(layer, {"metrics": ["orders.revenue", "orders.avg_amount"]}) == (
        ["revenue", "avg_amount"],
        [(210, 42.0)],
    )


def test_missing_rollup_table_query_retries_raw_source(layer):
    result = layer.query(metrics=["orders.avg_amount", "orders.net"], use_preaggregations=True)
    assert result.fetchall() == [(42.0, 184)]


def test_composite_join_rollup_uses_every_key_and_preserves_literals(layer):
    model = layer.graph.models["orders"]
    model.pre_aggregations[0].dimensions.append("tenant")
    model.relationships.append(
        Relationship(
            name="customers", type="many_to_one", foreign_key=["tenant", "customer_id"], primary_key=["tenant", "id"]
        )
    )
    layer.add_model(
        Model(
            name="customers",
            table="composite_rollup_customers",
            primary_key=["tenant", "id"],
            dimensions=[Dimension(name="name", type="categorical", sql="COALESCE(name, 'customers.name')")],
        )
    )
    layer.adapter.execute("""
        create table composite_rollup_customers(tenant integer,id integer,name varchar);
        insert into composite_rollup_customers values (1,1,'a'),(2,1,'unused'),(1,2,'b'),(2,2,'c'),(1,3,'d'),(1,4,null);
    """)
    build(layer)
    assert execute(
        layer,
        {
            "metrics": ["orders.rows", "orders.avg_amount"],
            "dimensions": ["customers.name"],
            "order_by": ["customers.name"],
        },
    ) == (
        ["name", "rows", "avg_amount"],
        [
            ("a", 2, 15.0),
            ("b", 2, 40.0),
            ("c", 1, 90.0),
            ("customers.name", 0, None),
            ("d", 1, 50.0),
            ("unused", 0, None),
        ],
    )


def test_missing_stored_join_key_uses_raw_source(layer):
    add_customers(layer)
    layer.graph.models["orders"].pre_aggregations[0].dimensions.remove("customer_id")
    build(layer)
    assert execute(
        layer,
        {"metrics": ["orders.rows"], "dimensions": ["customers.name"], "order_by": ["customers.name"]},
        routed=False,
    ) == (["name", "rows"], [("a", 2), ("b", 3), ("c", 1), ("empty", 0)])


def test_empty_remote_dimension_domain_stays_empty(layer):
    add_customers(layer)
    layer.adapter.execute("delete from rollup_customers")
    build(layer)
    assert execute(layer, {"metrics": ["orders.rows"], "dimensions": ["customers.name"]}) == (["name", "rows"], [])


def test_lambda_daily_states_roll_up_to_months(layer):
    rollup = layer.graph.models["orders"].pre_aggregations[0]
    rollup.type = "lambda"
    rollup.union_with_source_data = True
    rollup.build_range_end = "'2026-01-02 12:00:00'"
    build(layer)
    layer.adapter.execute("update rollup_orders set amount = 45 where id = 4")
    columns, rows = execute(
        layer,
        {
            "metrics": ["orders.revenue", "orders.avg_amount"],
            "dimensions": ["orders.created__month"],
            "order_by": ["orders.created__month"],
        },
    )
    assert columns == ["created__month", "revenue", "avg_amount"]
    assert [(str(month)[:10], total, average) for month, total, average in rows] == [
        ("2026-01-01", 165, 41.25),
        ("2026-02-01", 50, 50.0),
    ]


@pytest.mark.parametrize("mode", ["direct", "join", "lambda"])
def test_postgres_rollup_reconstruction_executes(mode):
    import os
    from decimal import Decimal

    dsn = os.environ.get("SIDEMANTIC_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("PostgreSQL execution requires SIDEMANTIC_TEST_POSTGRES_DSN")
    import psycopg
    import sidemantic_rs

    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.rust_bridge import compile_semantic_input

    assert callable(sidemantic_rs.compile_with_semantic_input)
    graph = SemanticGraph()
    model = Model(
        name="orders",
        table="pg_rollup_orders",
        primary_key="id",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="customer_id", type="numeric"),
            Dimension(name="created", type="time"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="count", agg="count", sql="amount"),
            Metric(name="avg_amount", agg="avg", sql="amount"),
            Metric(name="discount", agg="sum", sql="discount"),
            Metric(name="net", type="derived", sql="revenue - discount"),
            Metric(name="per_value", type="ratio", numerator="revenue", denominator="count"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="daily",
                dimensions=["status", "customer_id"],
                time_dimension="created",
                granularity="day",
                measures=["revenue", "count", "avg_amount", "discount"],
            )
        ],
    )
    if mode == "lambda":
        model.pre_aggregations[0].type = "lambda"
        model.pre_aggregations[0].union_with_source_data = True
        model.pre_aggregations[0].build_range_end = "'2026-01-02 12:00:00'"
    if mode == "join":
        model.relationships.append(Relationship(name="customers", type="many_to_one", foreign_key="customer_id"))
        graph.add_model(
            Model(
                name="customers",
                table="pg_rollup_customers",
                primary_key="id",
                dimensions=[Dimension(name="name", type="categorical")],
            )
        )
    graph.add_model(model)
    dimension = "customers.name" if mode == "join" else "orders.status"
    query = {
        "metrics": ["orders.avg_amount", "orders.net", "orders.per_value", "orders.count"],
        "dimensions": [dimension],
        "order_by": [dimension],
        "dialect": "postgres",
        "use_preaggregations": True,
    }
    sql = compile_semantic_input(graph, query)
    assert "orders_preagg_daily" in sql
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(
            "create temporary table pg_rollup_orders(id integer,customer_id integer,status text,amount integer,discount integer,created timestamp)"
        )
        connection.execute(
            "insert into pg_rollup_orders values (1,1,'paid',2,0,'2026-01-01 01:00:00'),(2,1,'paid',3,1,'2026-01-02 01:00:00'),(3,2,'paid',null,0,'2026-01-02 02:00:00'),(4,2,'open',100,10,'2026-01-02 15:00:00')"
        )
        connection.execute(
            f"create temporary table orders_preagg_daily as {model.pre_aggregations[0].generate_materialization_sql(model)}"
        )
        expected = [("open", 100.0, 90, 100.0, 1), ("paid", 2.5, 4, 2.5, 2)]
        if mode == "lambda":
            connection.execute("update pg_rollup_orders set amount = 8 where id = 2")
            expected = [("open", 100.0, 90, 100.0, 1), ("paid", 5.0, 9, 5.0, 2)]
        elif mode == "join":
            connection.execute("create temporary table pg_rollup_customers(id integer,name text)")
            connection.execute("insert into pg_rollup_customers values (1,'a'),(2,'b'),(3,'empty')")
            expected = [("a", 2.5, 4, 2.5, 2), ("b", 100.0, 90, 100.0, 1), ("empty", None, None, None, 0)]
        rows = connection.execute(sql).fetchall()
        assert [
            tuple(float(value) if isinstance(value, Decimal) else value for value in row) for row in rows
        ] == expected


def test_average_chooses_compatible_count_by_definition_not_name(layer):
    model = layer.graph.models["orders"]
    model.get_metric("count").agg = "sum"  # Its name does not make it count state.
    model.metrics.append(Metric(name="non_null_values", agg="count", sql="amount"))
    model.pre_aggregations[0].measures.append("non_null_values")
    build(layer)
    assert execute(layer, {"metrics": ["orders.avg_amount"]}) == (["avg_amount"], [(42.0,)])


def test_explicit_inner_join_declines_remote_domain_rollup(layer):
    add_customers(layer)
    layer.graph.models["orders"].relationships[0].metadata = {"bsl_how": "inner"}
    build(layer)
    query = {"metrics": ["orders.revenue"], "dimensions": ["customers.name"], "order_by": ["customers.name"]}
    expected = (["name", "revenue"], [("a", 30), ("b", 130), ("c", 50)])
    assert execute(layer, query, routed=False) == expected
    cursor = layer.adapter.execute(layer.compile(**query, use_preaggregations=False))
    assert ([column[0] for column in cursor.description], cursor.fetchall()) == expected


def test_colliding_join_dimension_outputs_keep_raw_contract(layer):
    if layer.engine != "rust":
        pytest.skip("Native output-name collision contract")
    add_customers(layer)
    model = layer.graph.models["orders"]
    model.dimensions.append(Dimension(name="name", type="categorical", sql="status"))
    model.pre_aggregations[0].dimensions.append("name")
    build(layer)
    query = {"metrics": ["orders.revenue"], "dimensions": ["orders.name", "customers.name"]}
    columns, rows = execute(layer, query, routed=False)
    cursor = layer.adapter.execute(layer.compile(**query, use_preaggregations=False))
    assert columns == [column[0] for column in cursor.description]
    assert columns == ["orders_name", "customers_name", "revenue"]
    assert sorted(rows, key=str) == sorted(cursor.fetchall(), key=str)


@pytest.mark.parametrize("anchor", ["local-first", "role-owner"])
def test_join_rollup_preserves_local_and_role_source_population(layer, anchor):
    if layer.engine != "rust":
        pytest.skip("Native effective-anchor routing regression")
    add_customers(layer)
    if anchor == "role-owner":
        relationship = layer.graph.models["orders"].relationships[0]
        relationship.name = "buyer"
        relationship.target_model = "customers"
        dimensions = ["buyer.name"]
    else:
        dimensions = ["orders.status", "customers.name"]
    build(layer)
    query = {"metrics": ["orders.revenue"], "dimensions": dimensions}
    columns, rows = execute(layer, query, routed=False)
    cursor = layer.adapter.execute(layer.compile(**query, use_preaggregations=False))
    assert columns == [column[0] for column in cursor.description]
    assert sorted(rows, key=str) == sorted(cursor.fetchall(), key=str)
    assert all("empty" not in row for row in rows)


@pytest.mark.parametrize("mode", ["exact", "coarser-time", "omitted-dimension", "lambda"])
@pytest.mark.parametrize("rust_build", [False, True], ids=["python-build", "rust-build"])
def test_distinct_rollup_preserves_exact_grain_only(layer, mode, rust_build):
    model = layer.graph.models["orders"]
    model.metrics.append(Metric(name="people", agg="count_distinct", sql="customer_id"))
    rollup = model.pre_aggregations[0]
    rollup.dimensions = ["status"]
    rollup.measures = ["people"]
    layer.adapter.execute("update rollup_orders set customer_id = null where id = 6")
    if mode == "lambda":
        # Build ordinary batch state first; routing must still decline the lambda declaration.
        build(layer, rust=rust_build)
        rollup.type = "lambda"
        rollup.build_range_end = "'2026-01-02 12:00:00'"
        rollup.union_with_source_data = True
    else:
        build(layer, rust=rust_build)
    dimensions = ["orders.status", "orders.created__day"]
    if mode == "coarser-time":
        dimensions = ["orders.status", "orders.created__month"]
    elif mode == "omitted-dimension":
        dimensions = ["orders.created__day"]
    query = {"metrics": ["orders.people"], "dimensions": dimensions, "order_by": dimensions}
    raw = layer.adapter.execute(layer.compile(**query, use_preaggregations=False))
    expected = ([column[0] for column in raw.description], raw.fetchall())
    routed = mode == "exact" or (mode == "lambda" and layer.engine == "python")
    assert execute(layer, query, routed=routed) == expected
    if mode == "exact":
        assert [row[-1] for row in expected[1]] == [1, 2, 1, 0]
    elif mode == "coarser-time":
        assert [row[-1] for row in expected[1]] == [1, 2, 0]


@pytest.mark.parametrize("empty", [False, True])
def test_global_filtered_distinct_rollup_count_population(layer, empty):
    model = layer.graph.models["orders"]
    model.metrics.append(Metric(name="people", agg="count_distinct", sql="customer_id", filters=["status = 'paid'"]))
    rollup = model.pre_aggregations[0]
    rollup.dimensions = []
    rollup.time_dimension = None
    rollup.granularity = None
    rollup.measures = ["people"]
    if empty:
        layer.adapter.execute("delete from rollup_orders")
    build(layer)
    assert execute(layer, {"metrics": ["orders.people"]}) == (["people"], [(0 if empty else 3,)])
