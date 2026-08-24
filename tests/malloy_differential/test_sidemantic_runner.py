"""Python-side regression tests for the Malloy differential runner."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


def _runner_module():
    runner_path = Path(__file__).with_name("sidemantic_runner.py")
    spec = spec_from_file_location("malloy_differential_sidemantic_runner", runner_path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_manifest_model_roots_execution_through_explore(tmp_path):
    runner = _runner_module()
    source_file = tmp_path / "rooted.malloy"
    source_file.write_text(
        """source: customers is duckdb.table('customers') extend {
  primary_key: id
  dimension: name is name
}
source: orders is duckdb.table('orders') extend {
  primary_key: id
  join_one: customers on customer_id = customers.id
}
"""
    )
    seed_file = tmp_path / "seed.sql"
    seed_file.write_text(
        """create table customers (id integer, name varchar);
insert into customers values (1, 'kept'), (2, 'customer_without_order');
create table orders (id integer, customer_id integer);
insert into orders values (10, 1), (11, 3);
"""
    )
    runner._seed(tmp_path, seed_file)

    result = runner._run_query(
        tmp_path,
        source_file,
        {
            "model": "orders",
            "metrics": [],
            "dimensions": ["customers.name"],
            "order_by": ["customers.name"],
        },
    )

    assert result == {
        "schema": ["name"],
        "rows": [{"name": None}, {"name": "kept"}],
    }


def test_runner_introspects_physical_fields_without_inventing_primary_key(tmp_path):
    from sidemantic.adapters.malloy import MalloyAdapter

    runner = _runner_module()
    source_file = tmp_path / "intrinsic.malloy"
    source_file.write_text(
        """source: orders is duckdb.table('orders') extend {
  measure: revenue is sum(amount)
}
"""
    )
    seed_file = tmp_path / "seed.sql"
    seed_file.write_text(
        """create table orders (order_id integer, region varchar, amount integer);
insert into orders values (1, 'west', 10), (2, 'east', 20);
"""
    )
    runner._seed(tmp_path, seed_file)

    imported = MalloyAdapter(strict=True).parse(source_file).get_model("orders")
    assert imported.primary_key is None

    result = runner._run_query(
        tmp_path,
        source_file,
        {
            "model": "orders",
            "metrics": [],
            "dimensions": ["orders.order_id", "orders.region"],
            "order_by": ["orders.order_id"],
        },
    )

    assert result == {
        "schema": ["order_id", "region"],
        "rows": [
            {"order_id": 1, "region": "west"},
            {"order_id": 2, "region": "east"},
        ],
    }
