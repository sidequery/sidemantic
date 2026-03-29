import importlib.util
import sys
from pathlib import Path


def load_create_demo_database():
    demo_data_path = Path("/app/examples/multi_format_demo/demo_data.py")
    spec = importlib.util.spec_from_file_location("demo_data", demo_data_path)
    demo_data_module = importlib.util.module_from_spec(spec)
    sys.modules["demo_data"] = demo_data_module
    spec.loader.exec_module(demo_data_module)
    return demo_data_module.create_demo_database


def build_demo_duckdb(output_path: str) -> None:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        output.unlink()

    create_demo_database = load_create_demo_database()
    demo_conn = create_demo_database()
    try:
        demo_conn.execute(f"attach '{output}' as seeded")
        tables = [row[0] for row in demo_conn.execute("show tables").fetchall()]
        for table in tables:
            demo_conn.execute(f"create table seeded.{table} as select * from main.{table}")
    finally:
        demo_conn.close()


if __name__ == "__main__":
    build_demo_duckdb("/app/models/demo.duckdb")
