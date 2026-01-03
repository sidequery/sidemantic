"""BigQuery database adapter."""

from typing import Any

from sidemantic.db.base import BaseDatabaseAdapter, validate_identifier


class BigQueryResult:
    """Wrapper for BigQuery query result to match DuckDB result API."""

    def __init__(self, query_job):
        """Initialize BigQuery result wrapper.

        Args:
            query_job: BigQuery query job result
        """
        self.query_job = query_job
        self._result = query_job.result()
        self._rows_iter = iter(self._result)

    def fetchone(self) -> tuple | None:
        """Fetch one row from the result."""
        try:
            row = next(self._rows_iter)
            return tuple(row.values())
        except StopIteration:
            return None

    def fetchall(self) -> list[tuple]:
        """Fetch all remaining rows."""
        return [tuple(row.values()) for row in self._rows_iter]

    def fetch_record_batch(self) -> Any:
        """Convert result to PyArrow RecordBatchReader."""
        import pyarrow as pa

        # BigQuery can return Arrow tables directly
        arrow_table = self._result.to_arrow()
        return pa.RecordBatchReader.from_batches(arrow_table.schema, arrow_table.to_batches())

    @property
    def description(self):
        """Get column descriptions."""
        return [(field.name, field.field_type) for field in self._result.schema]


class BigQueryAdapter(BaseDatabaseAdapter):
    """BigQuery database adapter.

    Example:
        >>> adapter = BigQueryAdapter(project_id="my-project", dataset_id="my_dataset")
        >>> result = adapter.execute("SELECT * FROM table")
    """

    def __init__(
        self,
        project_id: str | None = None,
        dataset_id: str | None = None,
        credentials: Any | None = None,
        location: str = "US",
        **kwargs,
    ):
        """Initialize BigQuery adapter.

        Args:
            project_id: GCP project ID (if None, uses default credentials project)
            dataset_id: Default dataset ID (optional)
            credentials: Google Cloud credentials (if None, uses default credentials)
            location: BigQuery location (default: US)
            **kwargs: Additional arguments passed to bigquery.Client
        """
        try:
            from google.cloud import bigquery
        except ImportError as e:
            raise ImportError(
                "BigQuery support requires google-cloud-bigquery. "
                "Install with: pip install sidemantic[bigquery] or pip install google-cloud-bigquery"
            ) from e

        # Check if using emulator
        import os

        emulator_host = os.getenv("BIGQUERY_EMULATOR_HOST")
        if emulator_host:
            # Use anonymous credentials for emulator
            from google.api_core.client_options import ClientOptions
            from google.auth.credentials import AnonymousCredentials

            # Set API endpoint to emulator
            client_options = ClientOptions(api_endpoint=f"http://{emulator_host}")
            credentials = AnonymousCredentials()
            self.client = bigquery.Client(
                project=project_id, credentials=credentials, location=location, client_options=client_options, **kwargs
            )
        else:
            self.client = bigquery.Client(project=project_id, credentials=credentials, location=location, **kwargs)
        self.project_id = project_id or self.client.project
        self.dataset_id = dataset_id

    def execute(self, sql: str) -> BigQueryResult:
        """Execute SQL query."""
        query_job = self.client.query(sql)
        return BigQueryResult(query_job)

    def executemany(self, sql: str, params: list) -> Any:
        """Execute SQL with multiple parameter sets.

        Note: BigQuery doesn't have native executemany, so we run queries sequentially.
        """
        results = []
        for param_set in params:
            # BigQuery uses @param syntax for parameters
            query_job = self.client.query(sql, job_config={"query_parameters": param_set})
            results.append(BigQueryResult(query_job))
        return results

    def fetchone(self, result: BigQueryResult) -> tuple | None:
        """Fetch one row from result."""
        return result.fetchone()

    def fetch_record_batch(self, result: BigQueryResult) -> Any:
        """Fetch result as PyArrow RecordBatchReader."""
        return result.fetch_record_batch()

    def get_tables(self) -> list[dict]:
        """List all tables in the dataset."""
        if not self.dataset_id:
            # If no dataset specified, list tables from all datasets
            tables = []
            for dataset in self.client.list_datasets():
                dataset_ref = self.client.dataset(dataset.dataset_id)
                for table in self.client.list_tables(dataset_ref):
                    tables.append({"table_name": table.table_id, "schema": dataset.dataset_id})
            return tables

        # List tables in specific dataset
        dataset_ref = self.client.dataset(self.dataset_id)
        tables = []
        for table in self.client.list_tables(dataset_ref):
            tables.append({"table_name": table.table_id, "schema": self.dataset_id})
        return tables

    def get_columns(self, table_name: str, schema: str | None = None) -> list[dict]:
        """Get column information for a table."""
        # Validate identifiers for consistency and defense in depth
        # (BigQuery API handles these, but validation catches bad input early)
        validate_identifier(table_name, "table name")
        schema = schema or self.dataset_id
        if not schema:
            raise ValueError("schema (dataset_id) required for get_columns")
        validate_identifier(schema, "schema")

        table_ref = self.client.dataset(schema).table(table_name)
        table = self.client.get_table(table_ref)

        columns = []
        for field in table.schema:
            columns.append(
                {
                    "column_name": field.name,
                    "data_type": field.field_type,
                    "is_nullable": field.mode != "REQUIRED",
                }
            )
        return columns

    def get_query_history(self, days_back: int = 7, limit: int = 1000) -> list[str]:
        """Fetch query history from BigQuery.

        Queries INFORMATION_SCHEMA.JOBS_BY_PROJECT to find queries with sidemantic instrumentation.

        Args:
            days_back: Number of days of history to fetch (default: 7)
            limit: Maximum number of queries to return (default: 1000)

        Returns:
            List of SQL query strings containing '-- sidemantic:' comments
        """
        sql = f"""
        SELECT query
        FROM `{self.project_id}.region-{self.client.location}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
          AND job_type = 'QUERY'
          AND state = 'DONE'
          AND query LIKE '%-- sidemantic:%'
        ORDER BY creation_time DESC
        LIMIT {limit}
        """

        result = self.execute(sql)
        rows = result.fetchall()
        return [row[0] for row in rows if row[0]]

    def close(self) -> None:
        """Close the BigQuery client."""
        self.client.close()

    @property
    def dialect(self) -> str:
        """Return SQL dialect."""
        return "bigquery"

    @property
    def raw_connection(self) -> Any:
        """Return raw BigQuery client."""
        return self.client

    @classmethod
    def from_url(cls, url: str) -> "BigQueryAdapter":
        """Create adapter from connection URL.

        URL format: bigquery://project_id/dataset_id
        or: bigquery://project_id  (no default dataset)

        Args:
            url: Connection URL

        Returns:
            BigQueryAdapter instance
        """
        if not url.startswith("bigquery://"):
            raise ValueError(f"Invalid BigQuery URL: {url}")

        # Parse URL: bigquery://project_id/dataset_id
        path = url[len("bigquery://") :]
        if not path:
            raise ValueError("BigQuery URL must include project_id: bigquery://project_id/dataset_id")

        parts = path.split("/")
        project_id = parts[0]
        dataset_id = parts[1] if len(parts) > 1 else None

        return cls(project_id=project_id, dataset_id=dataset_id)
