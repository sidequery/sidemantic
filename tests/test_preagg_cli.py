"""Integration tests for pre-aggregation CLI commands."""

import tempfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from sidemantic.cli import app

runner = CliRunner()


def test_preagg_analyze_with_queries_file(tmp_path):
    """Test preagg analyze command with queries file."""
    # Create a queries file with instrumented queries
    queries_file = tmp_path / "queries.sql"
    queries_file.write_text(
        """
        SELECT revenue, status FROM orders
        -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status granularities=day;

        SELECT revenue, status FROM orders
        -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status granularities=day;

        SELECT revenue, status FROM orders
        -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status granularities=day;

        SELECT count, region FROM orders
        -- sidemantic: models=orders metrics=orders.count dimensions=orders.region;

        SELECT count, region FROM orders
        -- sidemantic: models=orders metrics=orders.count dimensions=orders.region;
        """
    )

    result = runner.invoke(app, ["preagg", "analyze", "--queries", str(queries_file), "--min-count", "2"])

    assert result.exit_code == 0
    assert "✓ Analyzed 5 queries" in result.stderr
    assert "Found 2 unique patterns" in result.stderr
    assert "2 patterns above threshold" in result.stderr


def test_preagg_recommend_with_queries_file(tmp_path):
    """Test preagg recommend command with queries file."""
    queries_file = tmp_path / "queries.sql"
    queries_file.write_text(
        """
        SELECT revenue FROM orders
        -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status granularities=day;
        """ * 15  # 15 queries to exceed default min_count of 10
    )

    result = runner.invoke(app, ["preagg", "recommend", "--queries", str(queries_file)])

    assert result.exit_code == 0
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
        """ * 15
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
        """ * 15
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


def test_preagg_analyze_requires_source():
    """Test that preagg analyze requires --queries, --connection, or --db."""
    result = runner.invoke(app, ["preagg", "analyze"])

    assert result.exit_code == 1
    assert "Must specify --queries, --connection, or --db" in result.stderr


def test_preagg_recommend_with_thresholds(tmp_path):
    """Test preagg recommend with custom thresholds."""
    queries_file = tmp_path / "queries.sql"
    queries_file.write_text(
        """
        SELECT revenue FROM orders
        -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status;
        """ * 5  # 5 queries
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
        """ * 20 +
        """
        SELECT count FROM orders
        -- sidemantic: models=orders metrics=orders.count dimensions=orders.region;
        """ * 15
    )

    result = runner.invoke(
        app,
        ["preagg", "apply", str(models_dir), "--queries", str(queries_file), "--top", "1", "--dry-run"],
    )

    assert result.exit_code == 0
    # Should only add 1 pre-aggregation (the top one by benefit score)
    assert "Would add 1 pre-aggregations" in result.stderr


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
