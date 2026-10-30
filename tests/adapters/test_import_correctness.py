"""Execution regressions for imported keys and metric serialization."""

import textwrap

import pytest
import yaml

from sidemantic import Dimension, Metric, Model
from sidemantic.adapters.atscale_sml import AtScaleSMLAdapter
from sidemantic.adapters.bsl import BSLAdapter
from sidemantic.adapters.hex import HexAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.malloy import MalloyAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.adapters.superset import SupersetAdapter
from sidemantic.core.relationship import Relationship
from sidemantic.loaders import load_from_directory


def write_yaml(path, data):
    path.write_text(yaml.safe_dump(data))
    return path


@pytest.mark.parametrize("custom_window", [False, True])
def test_native_graph_window_roundtrip(tmp_path, layer, custom_window):
    layer.conn.execute("CREATE TABLE sales(id INT, day DATE, amount INT)")
    layer.conn.execute("INSERT INTO sales VALUES (1, '2024-01-31', 10), (2, '2024-02-01', 20)")
    layer.add_model(
        Model(
            name="sales",
            table="sales",
            dimensions=[Dimension(name="day", type="time", granularity="day")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    if custom_window:
        metric = Metric(
            name="windowed",
            type="cumulative",
            window_expression="AVG(base.revenue)",
            window_frame="ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
            window_order="day",
        )
        expected = [10, 15]
    else:
        metric = Metric(name="windowed", type="cumulative", sql="sales.revenue", grain_to_date="month")
        expected = [10, 20]
    layer.add_metric(metric)
    query = dict(metrics=["sales.revenue", "windowed"], dimensions=["sales.day"], order_by=["sales.day"])
    before = layer.query(**query).fetchall()
    assert [row[-1] for row in before] == expected
    path = tmp_path / "native.yml"
    adapter = SidemanticAdapter()
    adapter.export(layer.graph, path)
    layer.graph = adapter.parse(path)
    for option in ("grain_to_date", "window_expression", "window_frame", "window_order"):
        assert getattr(layer.graph.metrics["windowed"], option) == getattr(metric, option)
    assert layer.query(**query).fetchall() == before


@pytest.mark.parametrize("direction", ["one_to_many", "many_to_one"])
@pytest.mark.parametrize("reverse_operands", [False, True])
@pytest.mark.parametrize("quoted_keys", [False, True])
def test_hex_join_keys_execute_and_roundtrip(tmp_path, layer, direction, reverse_operands, quoted_keys):
    join = "code = ${children}.parent_code" if direction == "one_to_many" else "parent_code = ${parents}.code"
    if quoted_keys:
        join = join.replace("parent_code", '"parent_code"').replace(".code", '."code"')
        if join.startswith("code "):
            join = '"code"' + join[4:]
    if reverse_operands:
        left, right = join.split(" = ")
        join = f"{right} = {left}"
    parents = {
        "id": "parents",
        "base_sql_table": "parents",
        "dimensions": [{"id": "id", "type": "number", "unique": True}, {"id": "code", "type": "number"}],
    }
    children = {
        "id": "children",
        "base_sql_table": "children",
        "dimensions": [{"id": "id", "type": "number", "unique": True}, {"id": "parent_code", "type": "number"}],
        "measures": [{"id": "total", "func": "sum", "of": "amount"}],
    }
    owner, target = (parents, "children") if direction == "one_to_many" else (children, "parents")
    owner["relations"] = [{"id": target, "type": direction, "join_sql": join}]
    path = tmp_path / "hex.yml"
    path.write_text(yaml.safe_dump_all([parents, children]))
    adapter = HexAdapter()
    layer.graph = adapter.parse(path)
    layer.conn.execute("CREATE TABLE parents(id INT, code INT); INSERT INTO parents VALUES (1, 20), (2, 10)")
    layer.conn.execute("CREATE TABLE children(id INT, parent_code INT, amount INT)")
    layer.conn.execute("INSERT INTO children VALUES (1, 10, 10), (2, 10, 20), (3, 20, 40)")
    query = dict(metrics=["children.total"], dimensions=["parents.id"], order_by=["parents.id"])
    assert layer.query(**query).fetchall() == [(1, 40), (2, 30)]
    output = tmp_path / "export"
    adapter.export(layer.graph, output)
    layer.graph = adapter.parse(output)
    assert layer.query(**query).fetchall() == [(1, 40), (2, 30)]


@pytest.mark.parametrize(
    "expression", ["COUNT(DISTINCT id)", 'count ( distinct "id" )', "COUNT(DISTINCT COALESCE(id, 0))"]
)
def test_superset_count_distinct_operand(tmp_path, layer, expression):
    source = write_yaml(
        tmp_path / "superset.yml",
        {
            "table_name": "sales",
            "columns": [{"column_name": "id", "type": "INT"}],
            "metrics": [{"metric_name": "users", "metric_type": "count_distinct", "expression": expression}],
        },
    )
    layer.conn.execute("CREATE TABLE sales(id INT); INSERT INTO sales VALUES (1), (1), (2)")
    adapter = SupersetAdapter()
    layer.graph = adapter.parse(source)
    assert layer.query(metrics=["sales.users"]).fetchall() == [(2,)]
    output = tmp_path / "out.yml"
    adapter.export(layer.graph, output)
    layer.graph = adapter.parse(output)
    assert layer.query(metrics=["sales.users"]).fetchall() == [(2,)]


@pytest.mark.parametrize("container", ["captured", "layer", "graph", "metric_only"])
def test_python_graph_metrics_loaded_with_provenance(tmp_path, layer, container):
    model_code = "Model(name='sales', table='sales', metrics=[Metric(name='revenue', agg='sum', sql='amount')])"
    metric_code = "Metric(name='double_revenue', type='derived', sql='sales.revenue * 2')"
    imports = "from sidemantic import Model, Metric, SemanticLayer\nfrom sidemantic.core.semantic_graph import SemanticGraph\n"
    if container == "captured":
        code = f"{model_code}\n{metric_code}\n"
    elif container == "metric_only":
        (tmp_path / "model.py").write_text(imports + model_code + "\n")
        code = metric_code + "\n"
    else:
        initializer = "SemanticLayer(auto_register=False)" if container == "layer" else "SemanticGraph()"
        code = (
            f"definitions = {initializer}\ndefinitions.add_model({model_code})\ndefinitions.add_metric({metric_code})\n"
        )
        code += "nested = {'definitions': [definitions]}\ndel definitions\n"
    (tmp_path / "metrics.py").write_text(imports + code)
    load_from_directory(layer, tmp_path, strict=True)
    assert set(layer.graph.metrics) == {"double_revenue"}
    metric = layer.graph.metrics["double_revenue"]
    assert metric._source_format == "Python"
    assert metric._source_file == "metrics.py"
    layer.conn.execute("CREATE TABLE sales(id INT, amount INT); INSERT INTO sales VALUES (1, 10), (2, 20)")
    assert layer.query(metrics=["double_revenue"]).fetchall() == [(60,)]


@pytest.mark.parametrize(
    "agg, expr, expected", [("sum", "amount", 30), ("count", None, 2), ("count_distinct", "amount", 2)]
)
def test_metricflow_folded_simple_roundtrip(tmp_path, layer, agg, expr, expected):
    metric = {"name": "revenue", "type": "simple", "agg": agg}
    if expr is not None:
        metric["expr"] = expr
    source = write_yaml(
        tmp_path / "source.yml",
        {
            "models": [
                {
                    "name": "orders",
                    "semantic_model": {"enabled": True},
                    "columns": [{"name": "id", "entity": "primary"}],
                    "metrics": [metric],
                }
            ]
        },
    )
    adapter = MetricFlowAdapter()
    layer.graph = adapter.parse(source)
    layer.conn.execute("CREATE TABLE orders(id INT, amount INT); INSERT INTO orders VALUES (1, 10), (2, 20)")
    assert layer.query(metrics=["revenue"]).fetchall() == [(expected,)]
    path = tmp_path / "out.yml"
    adapter.export(layer.graph, path)
    exported = yaml.safe_load(path.read_text())["metrics"][0]
    assert exported["type"] == "simple"
    measure_name = exported["type_params"]["measure"]["name"]
    exported_model = yaml.safe_load(path.read_text())["semantic_models"][0]
    assert next(m for m in exported_model["measures"] if m["name"] == measure_name)["agg"] == agg
    layer.graph = adapter.parse(path)
    assert layer.query(metrics=["revenue"]).fetchall() == [(expected,)]


@pytest.mark.parametrize("direction", ["many_to_one", "one_to_many", "one_to_one"])
@pytest.mark.parametrize("reversed_operands", [False, True])
def test_lookml_target_join_keys(tmp_path, layer, direction, reversed_operands):
    predicate = "${sales.customer_code} = ${customers.code}"
    if reversed_operands:
        predicate = "${customers.code} = ${sales.customer_code}"
    explore = "sales" if direction == "many_to_one" else "customers"
    target = "customers" if direction == "many_to_one" else "sales"
    source = tmp_path / "join.lkml"
    source.write_text(
        """
view: sales { sql_table_name: sales ;;
 dimension: id { primary_key: yes type: number sql: ${TABLE}.id ;; }
 dimension: customer_code { type: number sql: ${TABLE}.customer_code ;; }
 measure: total { type: sum sql: ${TABLE}.amount ;; }
}
view: customers { sql_table_name: customers ;;
 dimension: id { primary_key: yes type: number sql: ${TABLE}.id ;; }
 dimension: code { type: number sql: ${TABLE}.code ;; }
 dimension: name { sql: ${TABLE}.name ;; }
}
"""
        + f"explore: {explore} {{ join: {target} {{ relationship: {direction} sql_on: {predicate} ;; }} }}"
    )
    layer.graph = LookMLAdapter().parse(source)
    layer.conn.execute(
        "CREATE TABLE sales(id INT, customer_code INT, amount INT); INSERT INTO sales VALUES (1,2,10),(2,1,20)"
    )
    layer.conn.execute(
        "CREATE TABLE customers(id INT, code INT, name VARCHAR); INSERT INTO customers VALUES (1,2,'Alice'),(2,1,'Bob')"
    )
    assert layer.query(
        metrics=["sales.total"], dimensions=["customers.name"], order_by=["customers.name"]
    ).fetchall() == [("Alice", 10), ("Bob", 20)]


@pytest.mark.parametrize(
    "filter_value,expected", [(">15", 20), ("<=15", 10), ("!=10", 20), ("5 to 15", 10), ("-NULL", 30)]
)
def test_lookml_measure_filter_roundtrip(tmp_path, layer, filter_value, expected):
    source = tmp_path / "filter.lkml"
    source.write_text(
        '''view: sales { sql_table_name: sales ;;
 dimension: id { primary_key: yes type: number sql: ${TABLE}.id ;; }
 dimension: amount { type: number sql: ${TABLE}.amount ;; }
 measure: total { type: sum sql: ${TABLE}.amount ;; filters: [amount: "'''
        + filter_value
        + """"] }
}"""
    )
    layer.conn.execute("CREATE TABLE sales(id INT, amount INT); INSERT INTO sales VALUES (1,10),(2,20),(3,NULL)")
    adapter = LookMLAdapter()
    layer.graph = adapter.parse(source)
    assert layer.query(metrics=["sales.total"]).fetchall() == [(expected,)]
    path = tmp_path / "out.lkml"
    adapter.export(layer.graph, path)
    layer.graph = adapter.parse(path)
    assert layer.query(metrics=["sales.total"]).fetchall() == [(expected,)]


@pytest.mark.parametrize("shared_keys", [False, True])
def test_atscale_composite_relationship_keys(tmp_path, layer, shared_keys):
    write_yaml(
        tmp_path / "fact.yml",
        dict(
            unique_name="fact",
            object_type="dataset",
            table="sales",
            columns=[{"name": n, "data_type": "int"} for n in ["id", "customer_id", "tenant", "amount"]],
        ),
    )
    write_yaml(
        tmp_path / "customers.yml",
        dict(
            unique_name="customers",
            object_type="dataset",
            table="customers",
            columns=[{"name": n, "data_type": t} for n, t in [("id", "int"), ("tenant", "int"), ("name", "string")]],
        ),
    )
    level = dict(unique_name="customer_key", dataset="customers", name_column="name", key_columns=["id", "tenant"])
    if shared_keys:
        level.pop("dataset")
        level.pop("key_columns")
        level["shared_degenerate_columns"] = [
            {"dataset": "customers", "name_column": "name", "key_columns": ["id", "tenant"]}
        ]
    write_yaml(
        tmp_path / "dim.yml",
        dict(unique_name="customer_dim", object_type="dimension", type="standard", level_attributes=[level]),
    )
    write_yaml(
        tmp_path / "total.yml",
        dict(unique_name="total", object_type="metric", dataset="fact", column="amount", calculation_method="sum"),
    )
    write_yaml(
        tmp_path / "model.yml",
        dict(
            unique_name="sales_model",
            object_type="model",
            dimensions=["customer_dim"],
            metrics=[{"unique_name": "total"}],
            relationships=[
                {
                    "unique_name": "sales_customer",
                    "from": {"dataset": "fact", "join_columns": ["customer_id", "tenant"]},
                    "to": {"dimension": "customer_dim", "level": "customer_key"},
                }
            ],
        ),
    )
    layer.graph = AtScaleSMLAdapter().parse(tmp_path)
    rel = layer.graph.models["sales_model"].relationships[0]
    assert rel.foreign_key == ["customer_id", "tenant"]
    assert rel.primary_key == ["id", "tenant"]
    layer.conn.execute(
        "CREATE TABLE sales(id INT, customer_id INT, tenant INT, amount INT); INSERT INTO sales VALUES (1,7,1,10),(2,7,2,20)"
    )
    layer.conn.execute(
        "CREATE TABLE customers(id INT, tenant INT, name VARCHAR); INSERT INTO customers VALUES (7,1,'Alice'),(7,2,'Bob')"
    )
    assert layer.query(
        metrics=["sales_model.total"], dimensions=["customers.customer_key"], order_by=["customers.customer_key"]
    ).fetchall() == [("Alice", 10), ("Bob", 20)]


@pytest.mark.parametrize("filtered", [False, True])
def test_malloy_count_column_roundtrip(tmp_path, layer, filtered):
    layer.conn.execute("CREATE TABLE events(id INT, value INT); INSERT INTO events VALUES (1,7),(2,7),(3,NULL)")
    layer.add_model(
        Model(
            name="events",
            table="events",
            dimensions=[Dimension(name="id", type="numeric"), Dimension(name="value", type="numeric")],
            metrics=[Metric(name="n", agg="count", sql="value", filters=["id > 1"] if filtered else None)],
        )
    )
    expected = 1 if filtered else 2
    assert layer.query(metrics=["events.n"]).fetchall() == [(expected,)]
    path = tmp_path / "out.malloy"
    adapter = MalloyAdapter()
    adapter.export(layer.graph, path)
    layer.graph = adapter.parse(path)
    assert layer.graph.models["events"].get_metric("n").agg == "count"
    assert layer.query(metrics=["events.n"]).fetchall() == [(expected,)]


@pytest.mark.parametrize("composite", [False, True])
def test_bsl_one_to_many_keys_roundtrip(tmp_path, layer, composite):
    source = tmp_path / "many.yml"
    source.write_text(
        textwrap.dedent("""\
        parents:
          table: parents
          dimensions:
            id: _.id
            tenant: _.tenant
          measures:
            n: _.count()
          joins:
            children:
              type: many
              left_on: id
              right_on: parent_id
        children:
          table: children
          dimensions:
            id: _.id
            parent_id: _.parent_id
            tenant: _.tenant
          measures:
            total: _.amount.sum()
    """)
    )
    if composite:
        source.write_text(
            source.read_text()
            .replace("left_on: id", "left_on: [id, tenant]")
            .replace("right_on: parent_id", "right_on: [parent_id, tenant]")
        )
    layer.conn.execute("CREATE TABLE parents(id INT, tenant INT); INSERT INTO parents VALUES (1,1),(2,2)")
    layer.conn.execute(
        "CREATE TABLE children(id INT, parent_id INT, tenant INT, amount INT); INSERT INTO children VALUES (10,1,1,10),(11,1,1,20),(12,2,2,30)"
    )
    adapter = BSLAdapter()
    layer.graph = adapter.parse(source)
    query = dict(metrics=["children.total"], dimensions=["parents.id"], order_by=["parents.id"])
    assert layer.query(**query).fetchall() == [(1, 30), (2, 30)]
    path = tmp_path / "out.yml"
    adapter.export(layer.graph, path)
    layer.graph = adapter.parse(path)
    assert layer.query(**query).fetchall() == [(1, 30), (2, 30)]


@pytest.mark.parametrize("kind", ["ratio", "cumulative", "reference"])
def test_python_metric_only_file_resolves_models_from_other_files(tmp_path, layer, kind):
    write_yaml(
        tmp_path / "z_models.yml",
        {
            "models": [
                {
                    "name": "sales",
                    "table": "sales",
                    "metrics": [{"name": "revenue", "agg": "sum", "sql": "amount"}, {"name": "n", "agg": "count"}],
                }
            ]
        },
    )
    definitions = {
        "ratio": "Metric(name='result', type='ratio', numerator='sales.revenue', denominator='sales.n')",
        "cumulative": "Metric(name='result', type='cumulative', sql='sales.revenue', grain_to_date='month')",
        "reference": "Metric(name='result', sql='sales.revenue')",
    }
    (tmp_path / "a_metrics.py").write_text("from sidemantic import Metric\n" + definitions[kind] + "\n")
    load_from_directory(layer, tmp_path, strict=True)
    assert layer.graph.metrics["result"]._source_file == "a_metrics.py"
    if kind != "cumulative":
        layer.conn.execute("CREATE TABLE sales(id INT, amount INT); INSERT INTO sales VALUES (1, 10), (2, 20)")
        assert layer.query(metrics=["result"]).fetchall() == [(15 if kind == "ratio" else 30,)]


def test_metricflow_folded_filters_and_measure_name_collisions(tmp_path, layer):
    layer.conn.execute("CREATE TABLE orders(id INT, amount INT); INSERT INTO orders VALUES (1,10),(2,20),(3,30)")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[
                Metric(name="revenue_measure", agg="sum", sql="amount"),
            ],
        )
    )
    layer.add_metric(
        Metric(name="revenue", agg="sum", sql="orders.amount", filters=["orders.amount > 10", "orders.amount < 30"])
    )
    assert layer.query(metrics=["revenue"]).fetchall() == [(20,)]
    adapter = MetricFlowAdapter()
    path = tmp_path / "out.yml"
    adapter.export(layer.graph, path)
    layer.graph = adapter.parse(path)
    assert layer.graph.models["orders"].get_metric("revenue_measure") is not None
    assert layer.query(metrics=["revenue"]).fetchall() == [(20,)]
    assert layer.query(metrics=["orders.revenue_measure"]).fetchall() == [(60,)]


def test_hex_one_to_one_nondefault_keys_roundtrip(tmp_path, layer):
    layer.conn.execute("CREATE TABLE users(id INT, account_code INT); INSERT INTO users VALUES (1,20),(2,10)")
    layer.conn.execute(
        "CREATE TABLE accounts(id INT, code INT, amount INT); INSERT INTO accounts VALUES (1,10,30),(2,20,40)"
    )
    layer.add_model(
        Model(
            name="accounts",
            table="accounts",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric"),
                Dimension(name="code", type="numeric"),
            ],
            metrics=[Metric(name="total", agg="sum", sql="amount")],
        )
    )
    layer.add_model(
        Model(
            name="users",
            table="users",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric"),
                Dimension(name="account_code", type="numeric"),
            ],
            relationships=[
                Relationship(name="accounts", type="one_to_one", foreign_key="code", primary_key="account_code")
            ],
        )
    )
    query = dict(metrics=["accounts.total"], dimensions=["users.id"], order_by=["users.id"])
    assert layer.query(**query).fetchall() == [(1, 40), (2, 30)]
    adapter = HexAdapter()
    for index in range(2):
        destination = tmp_path / f"roundtrip_{index}"
        adapter.export(layer.graph, destination)
        layer.graph = adapter.parse(destination)
        relationship = layer.graph.models["users"].relationships[0]
        assert relationship.type == "one_to_one"
        assert relationship.foreign_key == "code"
        assert relationship.primary_key == "account_code"
        assert layer.query(**query).fetchall() == [(1, 40), (2, 30)]
