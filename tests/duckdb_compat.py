"""DuckDB-version-aware expectations for day-or-coarser date_trunc buckets."""

from datetime import date, datetime, time
from functools import cache

import duckdb


@cache
def _bucket_type():
    # DuckDB 1.4 returns DATE for these overloads; 1.5 returns TIMESTAMP.
    # Probe only the type: the expected bucket values remain independent.
    with duckdb.connect() as connection:
        row = connection.execute(
            "select date_trunc('month', date '2000-01-02'), date_trunc('day', timestamp '2000-01-02 12:34:56')"
        ).fetchone()
    assert type(row[0]) is type(row[1])
    assert type(row[0]) in (date, datetime)
    return type(row[0])


def date_bucket(year: int, month: int, day: int) -> date | datetime:
    """Expected coarse bucket, without coercing any actual query result."""
    value = date(year, month, day)
    return datetime.combine(value, time()) if _bucket_type() is datetime else value
