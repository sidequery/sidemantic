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
