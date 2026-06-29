"""Integration tests for pre-aggregation CLI commands."""

import pytest
from typer.testing import CliRunner

from sidemantic.cli import app

runner = CliRunner()


def test_preagg_recommend_with_queries_file(tmp_path):
    """Test preagg recommend command with queries file."""
    queries_file = tmp_path / "queries.sql"
    queries_file.write_text(
        """
        SELECT revenue FROM orders
        -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status granularities=day;
        """
        * 15  # 15 queries to exceed default min_count of 10
    )

    result = runner.invoke(app, ["preagg", "recommend", "--queries", str(queries_file)])

    assert result.exit_code == 0
    # Check summary output
    assert "✓ Analyzed 15 queries" in result.stderr
    assert "Found 1 unique patterns" in result.stderr or "Found 1 unique pattern" in result.stderr
    # Check detailed recommendations
    assert "Pre-Aggregation Recommendations" in result.stdout
    assert "Query Count: 15" in result.stdout
    assert "Model: orders" in result.stdout


def test_preagg_recommend_no_recommendations(tmp_path):
    """Test preagg recommend when no patterns meet threshold."""
    queries_file = tmp_path / "queries.sql"
    queries_file.write_text(
        """
        SELECT revenue FROM orders
        -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status;
        """  # Only 1 query, below min_count of 10
    )

    result = runner.invoke(app, ["preagg", "recommend", "--queries", str(queries_file)])

    # Exit code 0 because we handled it gracefully (not an error)
    # The typer.Exit(0) in the CLI code should result in exit code 0
    assert result.exit_code == 0 or "No recommendations found above thresholds" in result.stderr


def test_preagg_apply_dry_run(tmp_path):
    """Test preagg apply with --dry-run."""
    # Create model YAML file
    models_dir = tmp_path / "models"
    models_dir.mkdir()

    model_file = models_dir / "orders.yml"
    model_file.write_text(
        """
models:
  - name: orders
    table: orders
    dimensions:
      - name: status
        type: categorical
      - name: created_at
        type: time
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: count
        agg: count
"""
    )

    # Create queries file
    queries_file = tmp_path / "queries.sql"
    queries_file.write_text(
        """
        SELECT revenue FROM orders
        -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status granularities=day;
        """
        * 15
    )

    result = runner.invoke(
        app,
        ["preagg", "apply", str(models_dir), "--queries", str(queries_file), "--dry-run"],
    )

    assert result.exit_code == 0
    assert "Dry run: Would add" in result.stderr
    assert "pre-aggregations" in result.stderr

    # File should NOT be modified
    original_content = model_file.read_text()
    assert "pre_aggregations:" not in original_content


def test_preagg_apply_writes_to_yaml(tmp_path):
    """Test preagg apply actually writes to YAML file."""
    # Create model YAML file
    models_dir = tmp_path / "models"
    models_dir.mkdir()

    model_file = models_dir / "orders.yml"
    model_file.write_text(
        """
models:
  - name: orders
    table: orders
    dimensions:
      - name: status
        type: categorical
      - name: created_at
        type: time
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: count
        agg: count
"""
    )

    # Create queries file
    queries_file = tmp_path / "queries.sql"
    queries_file.write_text(
        """
        SELECT revenue FROM orders
        -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status granularities=day;
        """
        * 15
    )

    result = runner.invoke(
        app,
        ["preagg", "apply", str(models_dir), "--queries", str(queries_file)],
    )

    assert result.exit_code == 0
    assert "✓ Added" in result.stderr
    assert "pre-aggregations to model files" in result.stderr

    # File should be modified
    updated_content = model_file.read_text()
    assert "pre_aggregations:" in updated_content or "pre_aggregations" in updated_content


def test_preagg_recommend_with_thresholds(tmp_path):
    """Test preagg recommend with custom thresholds."""
    queries_file = tmp_path / "queries.sql"
    queries_file.write_text(
        """
        SELECT revenue FROM orders
        -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status;
        """
        * 5  # 5 queries
    )

    # Should find recommendation with --min-count 5
    result = runner.invoke(
        app,
        ["preagg", "recommend", "--queries", str(queries_file), "--min-count", "5"],
    )

    assert result.exit_code == 0
    assert "Pre-Aggregation Recommendations" in result.stdout


def test_preagg_apply_top_n(tmp_path):
    """Test preagg apply with --top N to limit recommendations."""
    models_dir = tmp_path / "models"
    models_dir.mkdir()

    model_file = models_dir / "orders.yml"
    model_file.write_text(
        """
models:
  - name: orders
    table: orders
    dimensions:
      - name: status
        type: categorical
      - name: region
        type: categorical
    metrics:
      - name: revenue
        agg: sum
      - name: count
        agg: count
"""
    )

    # Create queries with 2 different patterns
    queries_file = tmp_path / "queries.sql"
    queries_file.write_text(
        """
        SELECT revenue FROM orders
        -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status;
        """
        * 20
        + """
        SELECT count FROM orders
        -- sidemantic: models=orders metrics=orders.count dimensions=orders.region;
        """
        * 15
    )

    result = runner.invoke(
        app,
        ["preagg", "apply", str(models_dir), "--queries", str(queries_file), "--top", "1", "--dry-run"],
    )

    assert result.exit_code == 0
    # Should only add 1 pre-aggregation (the top one by benefit score)
    assert "Would add 1 pre-aggregations" in result.stderr


def test_preagg_refresh_creates_indexes_on_duckdb(tmp_path):
    """`preagg refresh --db` infers the duckdb dialect so declared indexes are materialized."""
    import duckdb

    db = tmp_path / "data.db"
    conn = duckdb.connect(str(db))
    conn.execute("CREATE TABLE orders AS SELECT i id, i % 2 status, i * 10 amount FROM generate_series(1, 6) t(i)")
    conn.close()

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "orders.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
    pre_aggregations:
      - name: by_status
        type: rollup
        measures: [revenue]
        dimensions: [status]
        indexes:
          - name: status_idx
            columns: [status]
"""
    )

    result = runner.invoke(app, ["preagg", "refresh", str(models_dir), "--db", str(db), "--mode", "full"])
    assert result.exit_code == 0, result.stderr + result.stdout

    conn = duckdb.connect(str(db))
    names = {
        row[0]
        for row in conn.execute(
            "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'orders_preagg_by_status'"
        ).fetchall()
    }
    conn.close()
    assert "orders_preagg_by_status_status_idx" in names


def test_preagg_refresh_partitioned_builds_via_cli(tmp_path):
    """`preagg refresh` passes the model through, so partitioned rollups build instead of erroring."""
    import duckdb

    db = tmp_path / "data.db"
    conn = duckdb.connect(str(db))
    conn.execute(
        "CREATE TABLE orders AS "
        "SELECT (DATE '2024-01-01' + INTERVAL (i) MONTH) created_at, (i + 1) * 100 amount FROM generate_series(0, 2) t(i)"
    )
    conn.close()

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "orders.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: created_at
        type: time
        granularity: day
    metrics:
      - name: revenue
        agg: sum
        sql: amount
    pre_aggregations:
      - name: monthly
        type: rollup
        measures: [revenue]
        time_dimension: created_at
        granularity: month
        partition_granularity: month
"""
    )

    result = runner.invoke(app, ["preagg", "refresh", str(models_dir), "--db", str(db), "--mode", "full"])
    assert result.exit_code == 0, result.stderr + result.stdout

    conn = duckdb.connect(str(db))
    total = conn.execute("SELECT SUM(revenue_raw) FROM orders_preagg_monthly").fetchone()[0]
    kind = conn.execute(
        "SELECT table_type FROM information_schema.tables WHERE table_name = 'orders_preagg_monthly'"
    ).fetchone()[0]
    conn.close()
    assert total == 600  # 100 + 200 + 300 across the three monthly partitions
    assert kind == "VIEW"  # covering view over the partition tables


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
