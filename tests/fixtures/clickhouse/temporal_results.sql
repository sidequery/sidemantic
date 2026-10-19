-- Generated with ClickHouse local 26.8.2.7:
-- clickhouse local --queries-file tests/fixtures/clickhouse/temporal_results.sql > tests/fixtures/clickhouse/temporal_results.parquet
SELECT
    dateTrunc('DAY', toDateTime('2026-10-12 13:45:56', 'UTC')) AS day,
    dateTrunc('HOUR', toDateTime('2026-10-12 13:45:56', 'America/New_York')) AS hour,
    toDateTime64('2026-10-12 13:45:56.123456', 6, 'UTC') AS precise,
    dateTrunc('WEEK', toDate('2026-10-12')) AS week,
    toUInt32(1791763200) AS integer32,
    toUInt64(18446744073709551615) AS integer64,
    toDecimal64('1234.56', 2) AS amount,
    CAST(NULL AS Nullable(DateTime('UTC'))) AS nullable_time,
    CAST(NULL AS Nullable(UInt32)) AS nullable_integer
FORMAT Parquet
