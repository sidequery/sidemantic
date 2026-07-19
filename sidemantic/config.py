"""Configuration file format for Sidemantic."""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class DuckDBConnection(BaseModel):
    """DuckDB connection configuration."""

    type: Literal["duckdb"] = "duckdb"
    path: str = Field(..., description="Path to DuckDB database file or :memory:")
    read_only: bool | None = Field(
        default=None,
        description="Open DuckDB read-only; serving/query commands default file databases to true when omitted",
    )
    init_sql: list[str] | None = Field(
        default=None,
        description="SQL statements to run after connecting (e.g., loading extensions, attaching catalogs)",
    )


class PostgreSQLConnection(BaseModel):
    """PostgreSQL connection configuration."""

    type: Literal["postgres"] = "postgres"
    host: str = Field(..., description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Username")
    password: str = Field(..., description="Password")


class BigQueryConnection(BaseModel):
    """BigQuery connection configuration."""

    type: Literal["bigquery"] = "bigquery"
    project_id: str = Field(..., description="GCP project ID")
    dataset_id: str | None = Field(default=None, description="Default dataset ID (optional)")
    location: str = Field(default="US", description="BigQuery location")


class ClickHouseConnection(BaseModel):
    """ClickHouse connection configuration."""

    type: Literal["clickhouse"] = "clickhouse"
    host: str = Field(..., description="ClickHouse host")
    port: int = Field(default=8123, description="ClickHouse port")
    database: str = Field(default="default", description="Database name")
    username: str = Field(default="default", description="Username")
    password: str | None = Field(default=None, description="Password (optional)")


class SnowflakeConnection(BaseModel):
    """Snowflake connection configuration."""

    model_config = ConfigDict(populate_by_name=True)

    type: Literal["snowflake"] = "snowflake"
    account: str = Field(..., description="Snowflake account identifier")
    username: str = Field(..., description="Username")
    password: str = Field(..., description="Password")
    database: str | None = Field(default=None, description="Database name (optional)")
    schema_name: str | None = Field(default=None, alias="schema", description="Schema name (optional)")
    warehouse: str | None = Field(default=None, description="Warehouse name (optional)")
    role: str | None = Field(default=None, description="Role name (optional)")


class SparkConnection(BaseModel):
    """Spark SQL connection configuration."""

    type: Literal["spark"] = "spark"
    host: str = Field(..., description="Spark host")
    port: int = Field(default=10000, description="Spark port")
    database: str = Field(default="default", description="Database name")
    username: str | None = Field(default=None, description="Username (optional)")
    password: str | None = Field(default=None, description="Password (optional)")


class ADBCConnection(BaseModel):
    """ADBC (Arrow Database Connectivity) connection configuration.

    Uses dbc-installed drivers for efficient Arrow-native database access.
    Pass driver-specific parameters directly - they're passed through as-is.

    Prerequisites:
        pip install adbc-driver-manager
        dbc install <driver>  # e.g., dbc install snowflake

    Example (Snowflake with key-pair auth):
        connection:
          type: adbc
          driver: snowflake
          adbc.snowflake.sql.account: ORG-ACCOUNT
          adbc.snowflake.sql.db: MY_DATABASE
          adbc.snowflake.sql.warehouse: COMPUTE_WH
          adbc.snowflake.sql.auth_type: auth_jwt
          adbc.snowflake.sql.client_option.jwt_private_key: key.p8
          username: service_user
    """

    model_config = ConfigDict(extra="allow")

    type: Literal["adbc"] = "adbc"
    driver: str = Field(..., description="ADBC driver name (e.g., snowflake, postgresql, bigquery)")


class PostgresServerConfig(BaseModel):
    """PostgreSQL wire protocol server configuration (ALPHA).

    This feature is experimental and may change.
    """

    host: str = Field(default="127.0.0.1", description="Host/IP to bind to (use 0.0.0.0 for Docker)")
    port: int = Field(default=5433, description="Port to listen on")
    username: str | None = Field(default=None, description="Username for authentication (optional)")
    password_file: str | None = Field(default=None, description="Credential file containing the password")
    password: str | None = Field(default=None, description="Inline password (prefer password_file in shared config)")
    max_rows: int = Field(default=10_000, gt=0, description="Maximum rows returned by one query")
    max_response_bytes: int = Field(default=16 * 1024 * 1024, gt=0, description="Maximum buffered result bytes")
    execution_timeout_seconds: float = Field(default=30.0, gt=0, description="Query execution deadline")
    max_concurrent_queries: int = Field(default=4, gt=0, description="Maximum simultaneously executing queries")
    max_queued_queries: int = Field(default=16, ge=0, description="Maximum queries waiting for execution")
    queue_timeout_seconds: float = Field(default=5.0, gt=0, description="Maximum query queue wait")


class APIServerConfig(BaseModel):
    """HTTP API server configuration."""

    host: str = Field(default="127.0.0.1", description="Host/IP to bind to")
    port: int = Field(default=4400, description="Port to listen on")
    auth_token_file: str | None = Field(default=None, description="Credential file containing the bearer token")
    auth_token: str | None = Field(
        default=None, description="Inline bearer token (prefer auth_token_file in shared config)"
    )
    cors_origins: list[str] = Field(default_factory=list, description="Allowed CORS origins")
    max_request_body_bytes: int = Field(default=1024 * 1024, description="Maximum request body size in bytes")
    max_rows: int = Field(default=10_000, gt=0, description="Maximum rows returned by one query")
    max_response_bytes: int = Field(
        default=16 * 1024 * 1024, gt=0, description="Maximum serialized JSON or Arrow response size"
    )
    execution_timeout_seconds: float = Field(
        default=30.0, gt=0, description="Query execution deadline and requested warehouse statement timeout"
    )
    max_concurrent_queries: int = Field(default=4, gt=0, description="Maximum simultaneously executing queries")
    max_queued_queries: int = Field(default=16, ge=0, description="Maximum queries waiting for an execution slot")
    queue_timeout_seconds: float = Field(default=5.0, gt=0, description="Maximum time a query may wait in the queue")
    query_history_size: int = Field(default=1000, ge=0, description="Process-local sanitized query events retained")
    result_cache_mb: int = Field(default=0, description="Result cache size in megabytes (0 disables the result cache)")
    result_cache_ttl: float = Field(default=60.0, description="Result cache entry TTL in seconds")


class RuntimeConfig(BaseModel):
    """Runtime engine selection."""

    engine: Literal["python", "rust", "auto"] = Field(default="python", description="Runtime engine")
    fallback: bool = Field(default=False, description="Allow Rust runtime fallback to Python")


class CLIConfig(BaseModel):
    """Project-level defaults for command-line presentation."""

    format: Literal["table", "csv", "json", "jsonl"] | None = Field(
        default=None, description="Default structured output format"
    )
    plain: bool = Field(default=False, description="Use stable undecorated tabular output")
    quiet: bool = Field(default=False, description="Suppress non-essential command status")
    verbose: bool = Field(default=False, description="Enable detailed command diagnostics")
    no_color: bool = Field(default=False, description="Disable terminal color")


Connection = (
    DuckDBConnection
    | PostgreSQLConnection
    | BigQueryConnection
    | ClickHouseConnection
    | SnowflakeConnection
    | SparkConnection
    | ADBCConnection
)


class SidemanticConfig(BaseModel):
    """Sidemantic configuration file format.

    Can be saved as sidemantic.yaml or sidemantic.json.

    Example YAML:
        models_dir: ./models
        connection:
          type: duckdb
          path: data/warehouse.db
        preagg_database: analytics
        preagg_schema: preagg
        pg_server:
          port: 5433
          username: admin
          password_file: .secrets/pg-password
        api_server:
          port: 4400
          auth_token_file: .secrets/api-token

    Example JSON:
        {
          "models_dir": "./models",
          "connection": {
            "type": "duckdb",
            "path": "data/warehouse.db"
          },
          "preagg_database": "analytics",
          "preagg_schema": "preagg",
          "pg_server": {
            "port": 5433,
            "username": "admin",
            "password_file": ".secrets/pg-password"
          },
          "api_server": {
            "port": 4400,
            "auth_token_file": ".secrets/api-token"
          }
        }
    """

    models_dir: str = Field(
        default=".", description="Directory containing semantic layer files (defaults to current dir)"
    )
    connection: Connection | None = Field(default=None, description="Database connection configuration")
    preagg_database: str | None = Field(default=None, description="Database for pre-aggregation tables (optional)")
    preagg_schema: str | None = Field(default=None, description="Schema for pre-aggregation tables (optional)")
    runtime: RuntimeConfig | None = Field(default=None, description="Runtime engine settings")
    cli: CLIConfig = Field(default_factory=CLIConfig, description="Command-line presentation defaults")
    pg_server: PostgresServerConfig = Field(
        default_factory=PostgresServerConfig, description="PostgreSQL server settings (ALPHA)"
    )
    api_server: APIServerConfig = Field(default_factory=APIServerConfig, description="HTTP API server settings")

    def resolve_paths(self, base_dir: Path | None = None) -> "SidemanticConfig":
        """Resolve relative paths to absolute paths.

        Args:
            base_dir: Base directory for resolving relative paths (defaults to cwd)

        Returns:
            New config with resolved paths
        """
        base = base_dir or Path.cwd()

        models_path = Path(self.models_dir)
        if not models_path.is_absolute():
            models_path = (base / models_path).resolve()

        # Resolve connection paths
        connection = self.connection
        if connection and isinstance(connection, DuckDBConnection) and connection.path != ":memory:":
            db_p = Path(connection.path)
            if not db_p.is_absolute():
                db_p = (base / db_p).resolve()
            connection = DuckDBConnection(
                type="duckdb",
                path=str(db_p),
                read_only=connection.read_only,
                init_sql=connection.init_sql,
            )

        pg_server = self.pg_server.model_copy()
        if pg_server.password_file:
            password_file = Path(pg_server.password_file)
            if not password_file.is_absolute():
                password_file = (base / password_file).resolve()
            pg_server.password_file = str(password_file)

        api_server = self.api_server.model_copy()
        if api_server.auth_token_file:
            auth_token_file = Path(api_server.auth_token_file)
            if not auth_token_file.is_absolute():
                auth_token_file = (base / auth_token_file).resolve()
            api_server.auth_token_file = str(auth_token_file)

        return SidemanticConfig(
            models_dir=str(models_path),
            connection=connection,
            preagg_database=self.preagg_database,
            preagg_schema=self.preagg_schema,
            runtime=self.runtime,
            cli=self.cli,
            pg_server=pg_server,
            api_server=api_server,
        )


def load_config(config_path: Path) -> SidemanticConfig:
    """Load configuration from YAML or JSON file.

    Args:
        config_path: Path to config file (sidemantic.yaml or sidemantic.json)

    Returns:
        Loaded and validated configuration

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config file format is invalid
    """
    import json

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    suffix = config_path.suffix.lower()

    if suffix in {".yaml", ".yml"}:
        import yaml

        with open(config_path) as f:
            data = yaml.safe_load(f)
    elif suffix == ".json":
        with open(config_path) as f:
            data = json.load(f)
    else:
        raise ValueError(f"Unsupported config format: {suffix}. Use .yaml, .yml, or .json")

    config = SidemanticConfig(**data)

    # Resolve relative paths relative to config file directory
    return config.resolve_paths(config_path.parent)


def find_config(start_dir: Path | None = None) -> Path | None:
    """Find config file by searching up the directory tree.

    Searches for sidemantic.yaml, sidemantic.yml, or sidemantic.json.

    Args:
        start_dir: Directory to start searching from (defaults to cwd)

    Returns:
        Path to config file if found, None otherwise
    """
    current = (start_dir or Path.cwd()).resolve()

    # Search up to root
    while True:
        for name in ["sidemantic.yaml", "sidemantic.yml", "sidemantic.json"]:
            config_path = current / name
            if config_path.exists():
                return config_path

        parent = current.parent
        if parent == current:
            # Reached root
            break
        current = parent

    return None


def get_init_sql(config: SidemanticConfig) -> list[str] | None:
    """Get init_sql statements from config, if any.

    Args:
        config: Sidemantic configuration

    Returns:
        List of SQL statements to run after connecting, or None
    """
    if config.connection and isinstance(config.connection, DuckDBConnection):
        return config.connection.init_sql
    return None


def build_connection_string(config: SidemanticConfig) -> str:
    """Build database connection string from config.

    Args:
        config: Sidemantic configuration

    Returns:
        Connection string for SemanticLayer
    """
    if not config.connection:
        return "duckdb:///:memory:"

    if isinstance(config.connection, DuckDBConnection):
        connection = f"duckdb:///{config.connection.path}"
        if config.connection.read_only is not None:
            connection += f"?read_only={'true' if config.connection.read_only else 'false'}"
        return connection
    elif isinstance(config.connection, PostgreSQLConnection):
        password_part = f":{config.connection.password}" if config.connection.password else ""
        return (
            f"postgres://{config.connection.username}{password_part}@"
            f"{config.connection.host}:{config.connection.port}/{config.connection.database}"
        )
    elif isinstance(config.connection, BigQueryConnection):
        dataset_part = f"/{config.connection.dataset_id}" if config.connection.dataset_id else ""
        return f"bigquery://{config.connection.project_id}{dataset_part}"
    elif isinstance(config.connection, ClickHouseConnection):
        password_part = f":{config.connection.password}" if config.connection.password else ""
        return (
            f"clickhouse://{config.connection.username}{password_part}@"
            f"{config.connection.host}:{config.connection.port}/{config.connection.database}"
        )
    elif isinstance(config.connection, SnowflakeConnection):
        path = ""
        if config.connection.database:
            path = f"/{config.connection.database}"
            if config.connection.schema_name:
                path = f"{path}/{config.connection.schema_name}"

        params = []
        if config.connection.warehouse:
            params.append(f"warehouse={config.connection.warehouse}")
        if config.connection.role:
            params.append(f"role={config.connection.role}")
        query = f"?{'&'.join(params)}" if params else ""

        return (
            f"snowflake://{config.connection.username}:{config.connection.password}"
            f"@{config.connection.account}{path}{query}"
        )
    elif isinstance(config.connection, SparkConnection):
        if config.connection.username:
            password_part = f":{config.connection.password}" if config.connection.password else ""
            return (
                f"spark://{config.connection.username}{password_part}@"
                f"{config.connection.host}:{config.connection.port}/{config.connection.database}"
            )
        return f"spark://{config.connection.host}:{config.connection.port}/{config.connection.database}"
    elif isinstance(config.connection, ADBCConnection):
        from urllib.parse import quote

        conn = config.connection
        # Pass through all fields except type and driver
        params = []
        for key, value in conn.model_dump(exclude={"type", "driver"}, exclude_none=True).items():
            params.append(f"{quote(key, safe='')}={quote(str(value), safe='')}")

        query = "&".join(params)
        return f"adbc://{conn.driver}?{query}" if query else f"adbc://{conn.driver}"
    else:
        raise ValueError(f"Unknown connection type: {type(config.connection)}")
