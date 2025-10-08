"""Simple BigQuery integration test."""
import os
import pytest

os.environ['BIGQUERY_TEST'] = '1'
os.environ['BIGQUERY_EMULATOR_HOST'] = 'bigquery:9050'
os.environ['BIGQUERY_PROJECT'] = 'test-project'
os.environ['BIGQUERY_DATASET'] = 'test_dataset'

def test_simple():
    """Test basic query execution."""
    from sidemantic.db.bigquery import BigQueryAdapter

    adapter = BigQueryAdapter(project_id='test-project', dataset_id='test_dataset')
    result = adapter.execute("SELECT 1 as x, 2 as y")
    row = result.fetchone()
    assert row == (1, 2)
    print("TEST PASSED!")
