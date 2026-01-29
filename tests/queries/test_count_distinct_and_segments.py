"""Regression tests for GitHub issue #22.

count_distinct metrics without explicit sql should use the primary key,
and segment filters should properly include referenced columns in CTEs.

See: https://github.com/sidequery/sidemantic/issues/22
"""

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model, Segment, SemanticLayer
from tests.utils import fetch_dicts


@pytest.fixture
def con():
    """DuckDB connection with a dim_location table."""
    c = duckdb.connect()
    c.execute("""
        CREATE TABLE dim_location AS
        SELECT * FROM (VALUES
            (1, '3000', 'Leuven', 'BE'),
            (2, '3000', 'Leuven', 'BE'),
            (3, '7090', 'Braine', 'BE'),
            (4, '7090', 'Braine', 'FR'),
            (5, '7090', 'Braine', 'FR')
        ) AS t(sk_location_id, zipcode, city, country)
    """)
    return c


def _make_layer(model):
    layer = SemanticLayer(auto_register=False)
    layer.add_model(model)
    return layer


class TestCountDistinctWithoutSql:
    """count_distinct with no sql should use the primary key, not the metric name."""

    def test_single_pk(self, con):
        layer = _make_layer(
            Model(
                name="location",
                table="dim_location",
                primary_key="sk_location_id",
                dimensions=[Dimension(name="sk_location_id", type="categorical")],
                metrics=[Metric(name="count", agg="count_distinct")],
            )
        )
        sql = layer.compile(metrics=["location.count"])
        assert "sk_location_id AS count_raw" in sql
        result = fetch_dicts(con.execute(sql))
        assert result == [{"count": 5}]

    def test_composite_pk(self, con):
        # Add a row where sk_location_id=1 appears with a second zipcode.
        # Without both keys in the CONCAT, COUNT(DISTINCT sk_location_id)
        # would return 5 instead of the correct 6.
        con.execute("""
            INSERT INTO dim_location VALUES (1, '7090', 'Braine', 'BE')
        """)
        layer = _make_layer(
            Model(
                name="location",
                table="dim_location",
                primary_key=["sk_location_id", "zipcode"],
                dimensions=[
                    Dimension(name="sk_location_id", type="categorical"),
                    Dimension(name="zipcode", type="categorical"),
                ],
                metrics=[Metric(name="count", agg="count_distinct")],
            )
        )
        sql = layer.compile(metrics=["location.count"])
        assert "CONCAT(" in sql
        result = fetch_dicts(con.execute(sql))
        assert result == [{"count": 6}]

    def test_explicit_sql_overrides_pk(self, con):
        layer = _make_layer(
            Model(
                name="location",
                table="dim_location",
                primary_key="sk_location_id",
                dimensions=[Dimension(name="city", type="categorical")],
                metrics=[Metric(name="unique_cities", agg="count_distinct", sql="city")],
            )
        )
        sql = layer.compile(metrics=["location.unique_cities"])
        assert "city AS unique_cities_raw" in sql
        result = fetch_dicts(con.execute(sql))
        assert result == [{"unique_cities": 2}]


class TestSegmentsWithCountDistinct:
    """Segments should work correctly with count_distinct metrics (issue #22 part 2)."""

    def _location_model(self):
        return Model(
            name="location",
            table="dim_location",
            primary_key="sk_location_id",
            dimensions=[
                Dimension(name="sk_location_id", type="categorical"),
                Dimension(name="zipcode", type="categorical"),
                Dimension(name="country", type="categorical"),
            ],
            metrics=[Metric(name="count", agg="count_distinct")],
            segments=[
                Segment(name="zip_3000", sql="zipcode = '3000'"),
                Segment(name="zip_7090", sql="zipcode = '7090'"),
                Segment(name="country_fr", sql="country = 'FR'"),
            ],
        )

    def test_segment_filters_cte(self, con):
        layer = _make_layer(self._location_model())
        sql = layer.compile(
            metrics=["location.count"],
            segments=["location.zip_3000"],
        )
        assert "WHERE" in sql
        assert "zipcode = '3000'" in sql
        result = fetch_dicts(con.execute(sql))
        assert result == [{"count": 2}]

    def test_segment_with_dimension(self, con):
        layer = _make_layer(self._location_model())
        sql = layer.compile(
            metrics=["location.count"],
            dimensions=["location.zipcode"],
            segments=["location.zip_3000"],
        )
        result = fetch_dicts(con.execute(sql))
        assert result == [{"zipcode": "3000", "count": 2}]

    def test_multiple_segments(self, con):
        layer = _make_layer(self._location_model())
        sql = layer.compile(
            metrics=["location.count"],
            segments=["location.zip_7090", "location.country_fr"],
        )
        assert "zipcode = '7090'" in sql
        assert "country = 'FR'" in sql
        result = fetch_dicts(con.execute(sql))
        assert result == [{"count": 2}]

    def test_segment_plus_filter(self, con):
        layer = _make_layer(self._location_model())
        sql = layer.compile(
            metrics=["location.count"],
            segments=["location.zip_7090"],
            filters=["location.country = 'FR'"],
        )
        result = fetch_dicts(con.execute(sql))
        assert result == [{"count": 2}]
