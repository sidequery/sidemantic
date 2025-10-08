"""Tests for environment variable substitution in YAML files."""

import os

from sidemantic import SemanticLayer
from sidemantic.adapters.sidemantic import substitute_env_vars


def test_substitute_env_vars_basic():
    """Test basic environment variable substitution."""
    os.environ["TEST_VAR"] = "test_value"
    result = substitute_env_vars("connection: ${TEST_VAR}")
    assert result == "connection: test_value"
    del os.environ["TEST_VAR"]


def test_substitute_env_vars_with_default():
    """Test environment variable substitution with default value."""
    # Variable not set, should use default
    result = substitute_env_vars("connection: ${MISSING_VAR:-default_value}")
    assert result == "connection: default_value"

    # Variable set, should use value
    os.environ["PRESENT_VAR"] = "actual_value"
    result = substitute_env_vars("connection: ${PRESENT_VAR:-default_value}")
    assert result == "connection: actual_value"
    del os.environ["PRESENT_VAR"]


def test_substitute_env_vars_simple_form():
    """Test simple $VAR form (no braces)."""
    os.environ["DATABASE_URL"] = "postgres://localhost:5432/db"
    result = substitute_env_vars("connection: $DATABASE_URL")
    assert result == "connection: postgres://localhost:5432/db"
    del os.environ["DATABASE_URL"]


def test_substitute_env_vars_missing_keeps_original():
    """Test that missing variables without defaults are kept as-is."""
    result = substitute_env_vars("connection: ${MISSING_VAR}")
    assert result == "connection: ${MISSING_VAR}"


def test_substitute_env_vars_multiple():
    """Test multiple environment variables in one string."""
    os.environ["DB_HOST"] = "localhost"
    os.environ["DB_PORT"] = "5432"
    os.environ["DB_NAME"] = "analytics"

    result = substitute_env_vars("connection: postgres://${DB_HOST}:${DB_PORT}/${DB_NAME}")
    assert result == "connection: postgres://localhost:5432/analytics"

    del os.environ["DB_HOST"]
    del os.environ["DB_PORT"]
    del os.environ["DB_NAME"]


def test_env_vars_in_yaml_file(tmp_path):
    """Test environment variable substitution in a real YAML file."""
    test_db = tmp_path / "test.duckdb"
    os.environ["TEST_CONNECTION"] = f"duckdb:///{test_db}"

    yaml_content = """
connection: ${TEST_CONNECTION}

models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"""

    yaml_file = tmp_path / "test.yml"
    yaml_file.write_text(yaml_content)

    # Load via SemanticLayer.from_yaml
    layer = SemanticLayer.from_yaml(yaml_file)

    # Verify connection was substituted
    assert str(test_db) in layer.connection_string
    assert layer.dialect == "duckdb"

    del os.environ["TEST_CONNECTION"]


def test_env_vars_with_defaults_in_yaml(tmp_path):
    """Test environment variable with default values in YAML."""
    # Don't set DB_HOST, should use default
    yaml_content = """
connection: duckdb:///${DB_FILE:-/tmp/default.duckdb}

models:
  - name: test
    table: test
    primary_key: id
"""

    yaml_file = tmp_path / "test.yml"
    yaml_file.write_text(yaml_content)

    layer = SemanticLayer.from_yaml(yaml_file)
    assert "/tmp/default.duckdb" in layer.connection_string


def test_env_vars_in_model_table():
    """Test environment variables in model table names."""
    os.environ["SCHEMA_NAME"] = "analytics"

    yaml_content = """
models:
  - name: orders
    table: ${SCHEMA_NAME}.orders
    primary_key: order_id
"""

    import yaml

    from sidemantic.adapters.sidemantic import substitute_env_vars

    # Test substitution
    substituted = substitute_env_vars(yaml_content)
    assert "analytics.orders" in substituted

    data = yaml.safe_load(substituted)
    assert data["models"][0]["table"] == "analytics.orders"

    del os.environ["SCHEMA_NAME"]


def test_env_vars_complex_connection_string():
    """Test complex connection string with multiple env vars."""
    os.environ["SNOWFLAKE_USER"] = "analyst"
    os.environ["SNOWFLAKE_PASSWORD"] = "secret"
    os.environ["SNOWFLAKE_ACCOUNT"] = "xy12345.us-east-1"
    os.environ["SNOWFLAKE_DB"] = "ANALYTICS"
    os.environ["SNOWFLAKE_SCHEMA"] = "PUBLIC"
    os.environ["SNOWFLAKE_WAREHOUSE"] = "COMPUTE_WH"

    connection = (
        "snowflake://${SNOWFLAKE_USER}:${SNOWFLAKE_PASSWORD}@${SNOWFLAKE_ACCOUNT}/"
        "${SNOWFLAKE_DB}/${SNOWFLAKE_SCHEMA}?warehouse=${SNOWFLAKE_WAREHOUSE}"
    )

    result = substitute_env_vars(f"connection: {connection}")

    expected = "connection: snowflake://analyst:secret@xy12345.us-east-1/ANALYTICS/PUBLIC?warehouse=COMPUTE_WH"
    assert result == expected

    # Cleanup
    for var in [
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_DB",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_WAREHOUSE",
    ]:
        del os.environ[var]


def test_env_vars_case_sensitive():
    """Test that environment variables are case-sensitive."""
    os.environ["DB_HOST"] = "localhost"

    # Should substitute DB_HOST
    result = substitute_env_vars("host: ${DB_HOST}")
    assert result == "host: localhost"

    # Should not substitute db_host (lowercase)
    result = substitute_env_vars("host: ${db_host}")
    assert result == "host: ${db_host}"

    del os.environ["DB_HOST"]


def test_simple_var_only_uppercase():
    """Test that simple $VAR form only matches uppercase vars."""
    os.environ["DATABASE_URL"] = "postgres://localhost/db"

    # Should substitute
    result = substitute_env_vars("conn: $DATABASE_URL")
    assert result == "conn: postgres://localhost/db"

    # Should not substitute lowercase
    result = substitute_env_vars("conn: $database_url")
    assert result == "conn: $database_url"

    del os.environ["DATABASE_URL"]
