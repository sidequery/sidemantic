"""Tests for bundled workbench demo behavior."""

import importlib.util
import sys
from pathlib import Path

from sidemantic import SemanticLayer, load_from_directory
from sidemantic.sql.query_rewriter import QueryRewriter
from sidemantic.workbench.examples import EXAMPLE_QUERIES


def _load_demo_data_module():
    demo_data_path = Path(__file__).resolve().parent.parent / "examples" / "multi_format_demo" / "demo_data.py"
    spec = importlib.util.spec_from_file_location("demo_data", demo_data_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["demo_data"] = module
    spec.loader.exec_module(module)
    return module


def _load_demo_layer():
    demo_dir = Path(__file__).resolve().parent.parent / "examples" / "multi_format_demo"
    demo_data = _load_demo_data_module()

    layer = SemanticLayer(connection="duckdb:///:memory:")
    demo_conn = demo_data.create_demo_database()

    for table in ["customers", "products", "orders"]:
        rows = demo_conn.execute(f"SELECT * FROM {table}").fetchall()
        columns = [desc[0] for desc in demo_conn.execute(f"SELECT * FROM {table} LIMIT 0").description]
        create_sql = demo_conn.execute(f"SELECT sql FROM duckdb_tables() WHERE table_name = '{table}'").fetchone()[0]
        layer.adapter.execute(create_sql)

        if rows:
            placeholders = ", ".join(["?" for _ in columns])
            layer.adapter.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)

    load_from_directory(layer, str(demo_dir))
    return layer


def test_workbench_demo_starter_queries_execute():
    layer = _load_demo_layer()
    rewriter = QueryRewriter(layer.graph, dialect=layer.dialect)

    for name, sql in EXAMPLE_QUERIES.items():
        if name == "Custom":
            continue

        rendered_sql = rewriter.rewrite(sql)
        result = layer.adapter.execute(rendered_sql)

        assert result.description, name
        assert result.fetchall(), name
