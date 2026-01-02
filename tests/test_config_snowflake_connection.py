from sidemantic.config import build_connection_string, load_config


def test_load_config_builds_snowflake_connection(tmp_path):
    config_path = tmp_path / "sidemantic.yaml"
    config_path.write_text(
        """
models_dir: ./models
connection:
  type: snowflake
  account: xy12345.us-east-1
  username: analyst
  password: secret
  database: ANALYTICS
  schema: PUBLIC
  warehouse: COMPUTE_WH
  role: ANALYST
"""
    )

    config = load_config(config_path)
    connection = build_connection_string(config)

    assert (
        connection == "snowflake://analyst:secret@xy12345.us-east-1/ANALYTICS/PUBLIC?warehouse=COMPUTE_WH&role=ANALYST"
    )
