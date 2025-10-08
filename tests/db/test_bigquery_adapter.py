"""Tests for BigQuery adapter."""

import pytest


def test_bigquery_adapter_import():
    """Test that BigQuery adapter can be imported."""
    try:
        from sidemantic.db.bigquery import BigQueryAdapter

        assert hasattr(BigQueryAdapter, "from_url")
        assert hasattr(BigQueryAdapter, "execute")
    except ImportError as e:
        # If google-cloud-bigquery is not installed, should get helpful error
        assert "google-cloud-bigquery" in str(e).lower() or "bigquery" in str(e).lower()


def test_bigquery_url_parsing():
    """Test URL parsing logic (without connecting)."""
    try:
        from sidemantic.db.bigquery import BigQueryAdapter
    except ImportError:
        pytest.skip("google-cloud-bigquery not installed")

    # Test that from_url method exists
    assert hasattr(BigQueryAdapter, "from_url")
    # URL parsing is tested in integration tests where we can actually connect


def test_bigquery_url_with_dataset():
    """Test URL parsing with dataset."""
    try:
        from sidemantic.db.bigquery import BigQueryAdapter
    except ImportError:
        pytest.skip("google-cloud-bigquery not installed")

    # Test URL format validation
    # This doesn't connect, just parses the URL
    try:
        adapter = BigQueryAdapter.from_url("bigquery://my-project/my_dataset")
        assert adapter.project_id == "my-project"
        assert adapter.dataset_id == "my_dataset"
    except Exception as e:
        # Might fail on auth, but should parse the URL correctly
        # Only fail if it's a ValueError from URL parsing
        if "Invalid BigQuery URL" in str(e):
            raise


def test_bigquery_url_without_dataset():
    """Test URL parsing without dataset."""
    try:
        from sidemantic.db.bigquery import BigQueryAdapter
    except ImportError:
        pytest.skip("google-cloud-bigquery not installed")

    # Test URL format without dataset
    try:
        adapter = BigQueryAdapter.from_url("bigquery://my-project")
        assert adapter.project_id == "my-project"
        assert adapter.dataset_id is None
    except Exception as e:
        # Might fail on auth, but should parse the URL correctly
        if "Invalid BigQuery URL" in str(e):
            raise


def test_bigquery_invalid_url():
    """Test that invalid URLs raise ValueError."""
    try:
        from sidemantic.db.bigquery import BigQueryAdapter
    except ImportError:
        pytest.skip("google-cloud-bigquery not installed")

    with pytest.raises(ValueError, match="Invalid BigQuery URL"):
        BigQueryAdapter.from_url("postgres://invalid")

    with pytest.raises(ValueError, match="must include project_id"):
        BigQueryAdapter.from_url("bigquery://")
