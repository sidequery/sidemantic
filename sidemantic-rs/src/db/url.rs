//! Connection URL parsing for ADBC/dbc-backed execution.

use std::collections::BTreeMap;
use std::path::PathBuf;

use adbc_core::options::AdbcVersion;
use adbc_core::{LoadFlags, LOAD_FLAG_DEFAULT};
use url::form_urlencoded;
use url::Url;

use crate::core::SqlDialect;
use crate::error::{Result, SidemanticError};

/// Drivers exposed by the Columnar dbc registry that Sidemantic knows how to map.
pub const DBC_DRIVER_HINTS: &[&str] = &[
    "bigquery",
    "clickhouse",
    "databricks",
    "duckdb",
    "exasol",
    "flightsql",
    "mssql",
    "mysql",
    "postgresql",
    "redshift",
    "snowflake",
    "sqlite",
    "trino",
    "oracle",
    "teradata",
];

/// Parsed database connection configuration for Rust ADBC execution.
#[derive(Debug, Clone)]
pub struct ConnectionSpec {
    pub driver: String,
    pub uri: Option<String>,
    pub database_options: BTreeMap<String, String>,
    pub entrypoint: Option<String>,
    pub adbc_version: AdbcVersion,
    pub load_flags: LoadFlags,
    pub additional_search_paths: Option<Vec<PathBuf>>,
}

impl ConnectionSpec {
    pub fn with_driver(driver: impl Into<String>) -> Self {
        Self {
            driver: driver.into(),
            uri: None,
            database_options: BTreeMap::new(),
            entrypoint: None,
            adbc_version: AdbcVersion::V110,
            load_flags: LOAD_FLAG_DEFAULT,
            additional_search_paths: None,
        }
    }

    pub fn from_url(raw: &str) -> Result<Self> {
        if raw.trim().is_empty() {
            return Err(SidemanticError::ConnectionUrl(
                "connection URL cannot be empty".into(),
            ));
        }

        if let Some(spec) = parse_direct_file_uri(raw, "sqlite") {
            return Ok(spec);
        }
        if let Some(spec) = parse_direct_file_uri(raw, "duckdb") {
            return Ok(spec);
        }
        if let Some(spec) = parse_duckdb_motherduck_uri(raw) {
            return Ok(spec);
        }

        let parsed = Url::parse(raw)?;
        let scheme = parsed.scheme().to_ascii_lowercase();

        if scheme == "adbc" {
            return parse_adbc_url(&parsed);
        }
        if let Some(driver) = scheme.strip_prefix("adbc+") {
            return parse_adbc_plus_url(raw, &scheme, driver);
        }

        parse_standard_url(raw, &parsed)
    }

    pub fn with_uri(mut self, uri: impl Into<String>) -> Self {
        self.uri = Some(uri.into());
        self
    }

    pub fn with_database_option(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.database_options.insert(key.into(), value.into());
        self
    }

    pub fn with_entrypoint(mut self, entrypoint: impl Into<String>) -> Self {
        self.entrypoint = Some(entrypoint.into());
        self
    }

    pub fn with_load_flags(mut self, load_flags: LoadFlags) -> Self {
        self.load_flags = load_flags;
        self
    }

    pub fn with_additional_search_paths(mut self, paths: Vec<PathBuf>) -> Self {
        self.additional_search_paths = Some(paths);
        self
    }

    pub fn driver_basename(&self) -> String {
        driver_basename(&self.driver)
    }

    pub fn sql_dialect(&self) -> Option<SqlDialect> {
        match self.driver_basename().as_str() {
            "bigquery" => Some(SqlDialect::BigQuery),
            "clickhouse" => Some(SqlDialect::ClickHouse),
            "databricks" => Some(SqlDialect::Databricks),
            "duckdb" => Some(SqlDialect::DuckDB),
            "postgres" | "postgresql" => Some(SqlDialect::Postgres),
            "snowflake" => Some(SqlDialect::Snowflake),
            "spark" => Some(SqlDialect::Spark),
            _ => None,
        }
    }
}

fn parse_direct_file_uri(raw: &str, driver: &str) -> Option<ConnectionSpec> {
    let prefix = format!("{driver}:");
    let slashed_prefix = format!("{driver}://");
    if !raw.starts_with(&prefix) || raw.starts_with(&slashed_prefix) {
        return None;
    }

    let value = raw[prefix.len()..].trim();
    let uri = if value.is_empty() || value == "memory:" || value == ":memory:" {
        ":memory:".to_string()
    } else {
        value.to_string()
    };

    Some(ConnectionSpec::with_driver(driver).with_uri(uri))
}

fn parse_duckdb_motherduck_uri(raw: &str) -> Option<ConnectionSpec> {
    let prefix = "duckdb://";
    let rest = raw.strip_prefix(prefix)?;
    if !rest.starts_with("md:") {
        return None;
    }
    Some(ConnectionSpec::with_driver("duckdb").with_uri(rest.to_string()))
}

fn parse_adbc_url(parsed: &Url) -> Result<ConnectionSpec> {
    let driver = parsed
        .host_str()
        .or_else(|| parsed.path_segments().and_then(|mut parts| parts.next()))
        .filter(|driver| !driver.is_empty())
        .ok_or_else(|| {
            SidemanticError::ConnectionUrl(
                "adbc:// URL must specify a driver, e.g. adbc://postgresql?uri=postgresql://host/db"
                    .into(),
            )
        })?;

    let mut params = query_options(parsed);
    let entrypoint = params.remove("entrypoint");
    let mut spec = ConnectionSpec::with_driver(driver);
    spec.entrypoint = entrypoint;

    if let Some(uri) = params.remove("uri") {
        spec.uri = Some(uri);
    } else {
        let path_uri = parsed.path().trim_start_matches('/');
        if !path_uri.is_empty() && path_uri != driver {
            spec.uri = Some(path_uri.to_string());
        }
    }

    if spec.uri.is_none() && matches!(spec.driver.as_str(), "sqlite" | "duckdb") {
        spec.uri = Some(":memory:".to_string());
    }

    spec.database_options = params;
    Ok(spec)
}

fn parse_adbc_plus_url(raw: &str, scheme: &str, driver: &str) -> Result<ConnectionSpec> {
    if driver.is_empty() {
        return Err(SidemanticError::ConnectionUrl(
            "adbc+ URLs must include a driver name, e.g. adbc+postgresql://host/db".into(),
        ));
    }

    let uri = replace_scheme(raw, scheme, driver);
    let parsed = Url::parse(&uri)?;
    let mut spec = ConnectionSpec::with_driver(driver);
    spec.database_options = query_options(&parsed);
    spec.uri = if matches!(driver, "sqlite" | "duckdb") {
        Some(file_uri_from_url(driver, &parsed))
    } else {
        Some(uri)
    };
    Ok(spec)
}

fn parse_standard_url(raw: &str, parsed: &Url) -> Result<ConnectionSpec> {
    let scheme = parsed.scheme().to_ascii_lowercase();
    let (driver, uri) = match scheme.as_str() {
        "bigquery" => ("bigquery", bigquery_uri_from_url(parsed)?),
        "clickhouse" => ("clickhouse", raw.to_string()),
        "databricks" => ("databricks", databricks_uri_from_url(parsed)?),
        "duckdb" => ("duckdb", file_uri_from_url("duckdb", parsed)),
        "exasol" => ("exasol", raw.to_string()),
        "flightsql" => ("flightsql", raw.to_string()),
        "mssql" => ("mssql", raw.to_string()),
        "mysql" => ("mysql", raw.to_string()),
        "oracle" => ("oracle", raw.to_string()),
        "postgres" => ("postgresql", replace_scheme(raw, "postgres", "postgresql")),
        "postgresql" => ("postgresql", raw.to_string()),
        "redshift" => ("redshift", raw.to_string()),
        "snowflake" => ("snowflake", raw.to_string()),
        "sqlserver" => ("mssql", replace_scheme(raw, "sqlserver", "mssql")),
        "sqlite" => ("sqlite", file_uri_from_url("sqlite", parsed)),
        "teradata" => ("teradata", raw.to_string()),
        "trino" => ("trino", raw.to_string()),
        other => {
            return Err(SidemanticError::ConnectionUrl(format!(
                "unsupported database URL scheme '{other}'. Supported dbc drivers: {}",
                DBC_DRIVER_HINTS.join(", ")
            )));
        }
    };

    Ok(ConnectionSpec::with_driver(driver).with_uri(uri))
}

fn query_options(parsed: &Url) -> BTreeMap<String, String> {
    parsed
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect()
}

fn file_uri_from_url(driver: &str, parsed: &Url) -> String {
    if driver == "duckdb" {
        if let Some(host) = parsed.host_str() {
            if let Some(motherduck) = host.strip_prefix("md:") {
                return format!("md:{motherduck}");
            }
        }
    }

    let path = parsed.path();
    if path.is_empty() || path == "/" || path == "/:memory:" {
        return ":memory:".to_string();
    }
    if path.starts_with("//") {
        return path[1..].to_string();
    }
    path.to_string()
}

fn bigquery_project_dataset(parsed: &Url) -> (Option<String>, Option<String>) {
    let path_parts: Vec<_> = parsed
        .path_segments()
        .map(|parts| parts.filter(|part| !part.is_empty()).collect())
        .unwrap_or_default();
    let query = query_options(parsed);
    let query_dataset = query
        .get("DatasetId")
        .or_else(|| query.get("datasetId"))
        .or_else(|| query.get("dataset_id"))
        .cloned();

    if let Some(host) = parsed.host_str() {
        if !host.contains('.') {
            return (
                Some(host.to_string()),
                path_parts
                    .first()
                    .map(|value| (*value).to_string())
                    .or(query_dataset),
            );
        }
    }

    (
        path_parts.first().map(|value| (*value).to_string()),
        query_dataset.or_else(|| path_parts.get(1).map(|value| (*value).to_string())),
    )
}

fn bigquery_uri_from_url(parsed: &Url) -> Result<String> {
    let (project, dataset) = bigquery_project_dataset(parsed);
    let project = project.ok_or_else(|| {
        SidemanticError::ConnectionUrl("BigQuery URL must include a project id".into())
    })?;

    let mut query_items = Vec::new();
    for (key, value) in parsed.query_pairs() {
        let lowered = key.to_ascii_lowercase();
        if matches!(
            lowered.as_str(),
            "location" | "region" | "datasetid" | "dataset_id"
        ) {
            continue;
        }
        query_items.push((key.into_owned(), value.into_owned()));
    }

    if let Some(dataset) = dataset {
        query_items.push(("DatasetId".into(), dataset));
    }

    let query = query_options(parsed);
    if let Some(location) = query
        .get("Location")
        .or_else(|| query.get("location"))
        .or_else(|| query.get("region"))
    {
        query_items.push(("Location".into(), location.clone()));
    }

    let endpoint = parsed
        .host_str()
        .filter(|host| host.contains('.'))
        .unwrap_or("");
    let encoded_query = form_urlencoded::Serializer::new(String::new())
        .extend_pairs(query_items)
        .finish();
    let uri = format!("bigquery://{endpoint}/{project}");
    Ok(if encoded_query.is_empty() {
        uri
    } else {
        format!("{uri}?{encoded_query}")
    })
}

fn databricks_uri_from_url(parsed: &Url) -> Result<String> {
    let host = parsed.host_str().ok_or_else(|| {
        SidemanticError::ConnectionUrl("Databricks URL must include a server hostname".into())
    })?;
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };

    let username = parsed.username();
    let password = parsed.password();
    let userinfo = if username == "token" {
        password.map(|password| format!("token:{password}"))
    } else if !username.is_empty() && password.is_none() {
        Some(format!("token:{username}"))
    } else if !username.is_empty() {
        Some(format!("{}:{}", username, password.unwrap_or_default()))
    } else {
        None
    };

    let mut uri = String::from("databricks://");
    if let Some(userinfo) = userinfo {
        uri.push_str(&userinfo);
        uri.push('@');
    }
    uri.push_str(&host);
    uri.push(':');
    uri.push_str(&parsed.port().unwrap_or(443).to_string());
    uri.push_str(parsed.path());
    if let Some(query) = parsed.query() {
        uri.push('?');
        uri.push_str(query);
    }
    Ok(uri)
}

fn replace_scheme(raw: &str, old_scheme: &str, new_scheme: &str) -> String {
    format!("{new_scheme}{}", &raw[old_scheme.len()..])
}

fn driver_basename(driver: &str) -> String {
    let driver = driver.to_ascii_lowercase();
    driver
        .strip_prefix("adbc_driver_")
        .unwrap_or(&driver)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(url: &str) -> ConnectionSpec {
        ConnectionSpec::from_url(url).unwrap()
    }

    #[test]
    fn maps_network_schemes_to_dbc_driver_names() {
        let cases = [
            ("postgres://u:p@host:5432/db", "postgresql"),
            ("postgresql://host/db", "postgresql"),
            ("mysql://host/db", "mysql"),
            ("mssql://host/db", "mssql"),
            ("sqlserver://host/db", "mssql"),
            ("redshift://host/db", "redshift"),
            ("snowflake://acct/db/schema", "snowflake"),
            ("bigquery://project/dataset", "bigquery"),
            ("clickhouse://host/default", "clickhouse"),
            ("trino://host/catalog/schema", "trino"),
            ("flightsql://host:31337", "flightsql"),
            (
                "databricks://token@host/sql/1.0/warehouses/abc",
                "databricks",
            ),
            ("exasol://host/db", "exasol"),
            ("oracle://host/service", "oracle"),
            ("teradata://host/db", "teradata"),
        ];

        for (url, expected_driver) in cases {
            assert_eq!(spec(url).driver, expected_driver);
        }
    }

    #[test]
    fn normalizes_postgres_and_sqlserver_aliases() {
        assert_eq!(
            spec("postgres://u:p@host:5432/db").uri.as_deref(),
            Some("postgresql://u:p@host:5432/db")
        );
        assert_eq!(
            spec("sqlserver://host:1433/db").uri.as_deref(),
            Some("mssql://host:1433/db")
        );
    }

    #[test]
    fn normalizes_file_database_urls() {
        assert_eq!(spec("sqlite:///:memory:").uri.as_deref(), Some(":memory:"));
        assert_eq!(spec("sqlite::memory:").uri.as_deref(), Some(":memory:"));
        assert_eq!(
            spec("sqlite:///tmp/test.db").uri.as_deref(),
            Some("/tmp/test.db")
        );
        assert_eq!(spec("duckdb:///:memory:").uri.as_deref(), Some(":memory:"));
        assert_eq!(
            spec("duckdb:///tmp/test.duckdb").uri.as_deref(),
            Some("/tmp/test.duckdb")
        );
        assert_eq!(
            spec("duckdb://md:warehouse").uri.as_deref(),
            Some("md:warehouse")
        );
    }

    #[test]
    fn translates_bigquery_user_url_to_dbc_uri_shape() {
        let parsed = spec("bigquery://proj/dataset?location=EU");
        assert_eq!(parsed.driver, "bigquery");
        assert_eq!(
            parsed.uri.as_deref(),
            Some("bigquery:///proj?DatasetId=dataset&Location=EU")
        );
    }

    #[test]
    fn translates_databricks_token_url_to_dbc_uri_shape() {
        let parsed = spec("databricks://dapi123@workspace.cloud.databricks.com/sql/1.0/warehouses/abc?catalog=main");
        assert_eq!(parsed.driver, "databricks");
        assert_eq!(
            parsed.uri.as_deref(),
            Some("databricks://token:dapi123@workspace.cloud.databricks.com:443/sql/1.0/warehouses/abc?catalog=main")
        );
    }

    #[test]
    fn standard_urls_keep_query_parameters_in_uri_only() {
        let snowflake = spec("snowflake://acct/db/schema?warehouse=compute_wh");
        assert_eq!(
            snowflake.uri.as_deref(),
            Some("snowflake://acct/db/schema?warehouse=compute_wh")
        );
        assert!(snowflake.database_options.is_empty());

        let bigquery = spec("bigquery://proj/dataset?location=EU");
        assert_eq!(
            bigquery.uri.as_deref(),
            Some("bigquery:///proj?DatasetId=dataset&Location=EU")
        );
        assert!(bigquery.database_options.is_empty());
    }

    #[test]
    fn parses_explicit_adbc_url() {
        let parsed = spec("adbc://postgresql?uri=postgresql://host/db&sslmode=require");
        assert_eq!(parsed.driver, "postgresql");
        assert_eq!(parsed.uri.as_deref(), Some("postgresql://host/db"));
        assert_eq!(
            parsed.database_options.get("sslmode").map(String::as_str),
            Some("require")
        );

        let sqlite = spec("adbc://sqlite");
        assert_eq!(sqlite.driver, "sqlite");
        assert_eq!(sqlite.uri.as_deref(), Some(":memory:"));
    }

    #[test]
    fn parses_adbc_plus_escape_hatch() {
        let parsed = spec("adbc+postgresql://host/db");
        assert_eq!(parsed.driver, "postgresql");
        assert_eq!(parsed.uri.as_deref(), Some("postgresql://host/db"));
    }

    #[test]
    fn reports_unknown_scheme_with_dbc_hint() {
        let err = ConnectionSpec::from_url("unknown://host/db").unwrap_err();
        let message = err.to_string();
        assert!(message.contains("unsupported database URL scheme"));
        assert!(message.contains("postgresql"));
    }

    #[test]
    fn maps_driver_to_sql_dialect() {
        assert_eq!(
            spec("postgres://host/db").sql_dialect(),
            Some(SqlDialect::Postgres)
        );
        assert_eq!(
            spec("snowflake://acct/db").sql_dialect(),
            Some(SqlDialect::Snowflake)
        );
        assert_eq!(spec("trino://host/catalog/schema").sql_dialect(), None);
    }
}
