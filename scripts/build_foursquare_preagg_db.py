from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from sidemantic import Dimension, Metric, Model, PreAggregation

PLACES_URL = "https://sampledata.sidequery.dev/sidemantic-demo/places.parquet"
CATEGORIES_URL = "https://sampledata.sidequery.dev/sidemantic-demo/categories.parquet"


def _build_places_view(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("install httpfs")
    conn.execute("load httpfs")
    conn.execute(
        f"""
        create or replace view places as
        select
            p.country,
            p.region,
            p.locality,
            p.admin_region,
            c.level1_category_name as category_l1,
            c.level2_category_name as category_l2,
            c.level3_category_name as category_l3,
            cast(p.date_created as date) as date_created,
            p.latitude,
            p.longitude
        from read_parquet('{PLACES_URL}') p
        left join read_parquet('{CATEGORIES_URL}') c
            on c.category_id = p.fsq_category_ids[1]
        """
    )


def _preagg_model() -> Model:
    return Model(
        name="places",
        table="places",
        primary_key="rowid",
        default_time_dimension="date_created",
        dimensions=[
            Dimension(name="date_created", type="time", granularity="day"),
            Dimension(name="country", type="categorical"),
            Dimension(name="region", type="categorical"),
            Dimension(name="admin_region", type="categorical"),
            Dimension(name="category_l1", type="categorical"),
            Dimension(name="category_l2", type="categorical"),
            Dimension(name="category_l3", type="categorical"),
        ],
        metrics=[
            Metric(name="row_count", agg="count"),
            Metric(name="sum_latitude", sql="sum(latitude)"),
            Metric(name="sum_longitude", sql="sum(longitude)"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="daily_metrics",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
            ),
            PreAggregation(
                name="daily_all_dims",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=[
                    "country",
                    "region",
                    "admin_region",
                    "category_l1",
                    "category_l2",
                    "category_l3",
                ],
            ),
            PreAggregation(
                name="by_country",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["country"],
            ),
            PreAggregation(
                name="by_region",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["region"],
            ),
            PreAggregation(
                name="by_admin_region",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["admin_region"],
            ),
            PreAggregation(
                name="by_category_l1",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["category_l1"],
            ),
            PreAggregation(
                name="by_category_l2",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["category_l2"],
            ),
            PreAggregation(
                name="by_category_l3",
                measures=["row_count", "sum_latitude", "sum_longitude"],
                time_dimension="date_created",
                granularity="day",
                dimensions=["category_l3"],
            ),
        ],
    )


def _existing_tables(conn: duckdb.DuckDBPyConnection) -> set[str]:
    return {
        row[0] for row in conn.execute("select table_name from duckdb_tables() where schema_name = 'main'").fetchall()
    }


def build_preaggregations(db_path: Path, overwrite: bool) -> None:
    if overwrite and db_path.exists():
        db_path.unlink()

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))

    try:
        _build_places_view(conn)
        model = _preagg_model()
        tables = _existing_tables(conn)

        for preagg in model.pre_aggregations:
            table_name = preagg.get_table_name(model.name)
            if table_name in tables:
                continue
            source_sql = preagg.generate_materialization_sql(model)
            conn.execute(f"create table {table_name} as {source_sql}")
            tables.add(table_name)
            print(f"created {table_name}")

        conn.execute("drop view if exists places")
    finally:
        conn.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("examples/foursquare_preagg.db"),
        help="Output DuckDB file for pre-aggregations",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete existing DB file before building",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    build_preaggregations(args.db, args.overwrite)


if __name__ == "__main__":
    main()
