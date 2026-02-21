# ADBC + Snowflake Example

Uses ADBC with key-pair auth to connect to Snowflake. ADBC provides Arrow-native data transfer for better performance with large datasets.

## Prerequisites

```bash
pip install adbc-driver-manager
dbc install snowflake
```

Or install the Python package directly:
```bash
pip install adbc-driver-snowflake
```

## Config (CLI)

Copy `sidemantic.yaml.example` to `sidemantic.yaml` and fill in your Snowflake details.

```bash
sidemantic query "SELECT orders.total_revenue, orders.region FROM orders"
```

## Python API

When your semantic model YAML is generated from another tool (Cube, dbt, etc.) and doesn't contain connection parameters, use the Python API to supply the connection separately.

### Option 1: Pass ADBCAdapter directly

```python
from sidemantic import SemanticLayer, load_from_directory
from sidemantic.db.adbc import ADBCAdapter

adapter = ADBCAdapter(
    driver="snowflake",
    db_kwargs={
        "adbc.snowflake.sql.account": "ORG-ACCOUNT",
        "adbc.snowflake.sql.db": "MY_DATABASE",
        "adbc.snowflake.sql.schema": "MY_SCHEMA",
        "adbc.snowflake.sql.warehouse": "COMPUTE_WH",
        "username": "myuser",
        "password": "mypassword",
    },
)
layer = SemanticLayer(connection=adapter)
load_from_directory(layer, "path/to/models/")

result = layer.query(
    metrics=["orders.total_revenue"],
    dimensions=["orders.region"],
)
rows = result.fetchall()
```

### Option 2: Key-pair authentication

```python
adapter = ADBCAdapter(
    driver="snowflake",
    db_kwargs={
        "adbc.snowflake.sql.account": "ORG-ACCOUNT",
        "adbc.snowflake.sql.db": "MY_DATABASE",
        "adbc.snowflake.sql.warehouse": "COMPUTE_WH",
        "adbc.snowflake.sql.auth_type": "auth_jwt",
        "adbc.snowflake.sql.client_option.jwt_private_key": "path/to/key.p8",
        "username": "service_user",
    },
)
layer = SemanticLayer(connection=adapter)
```

### Option 3: Connection URL

```python
layer = SemanticLayer(
    connection="adbc://snowflake?adbc.snowflake.sql.account=ORG-ACCOUNT&adbc.snowflake.sql.db=MY_DATABASE&adbc.snowflake.sql.warehouse=COMPUTE_WH&username=myuser&password=mypassword"
)
```

### Option 4: from_yaml with connection override

```python
# Load models from YAML but supply your own connection
layer = SemanticLayer.from_yaml(
    "models/orders.yaml",
    connection=adapter,  # ADBCAdapter instance
)
```

## Cross-Database References

If your models reference tables in a different database than the connection default, use fully-qualified table names (database.schema.table) in the model definition:

```yaml
models:
  - name: orders
    table: OTHER_DB.PUBLIC.ORDERS
    primary_key: order_id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
```

## ADBC Snowflake Connection Parameters

Common `db_kwargs` parameters for the ADBC Snowflake driver:

| Parameter | Description |
|-----------|-------------|
| `adbc.snowflake.sql.account` | Snowflake account identifier (ORG-ACCOUNT format) |
| `adbc.snowflake.sql.db` | Default database |
| `adbc.snowflake.sql.schema` | Default schema |
| `adbc.snowflake.sql.warehouse` | Warehouse name |
| `adbc.snowflake.sql.role` | Role name |
| `adbc.snowflake.sql.auth_type` | Auth type: `auth_snowflake` (default), `auth_jwt`, `auth_ext_browser` |
| `adbc.snowflake.sql.client_option.jwt_private_key` | Path to JWT private key file |
| `username` | Username |
| `password` | Password |

See [ADBC Snowflake driver docs](https://arrow.apache.org/adbc/current/driver/snowflake.html) for the full list.
