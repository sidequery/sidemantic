#![cfg(feature = "adbc-exec")]

use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use sidemantic::{execute_with_adbc, AdbcExecutionRequest};

struct DriverProbe {
    name: &'static str,
    env_prefix: &'static str,
    default_entrypoint: Option<&'static str>,
    default_dbopts: &'static [(&'static str, &'static str)],
}

const DRIVER_PROBES: &[DriverProbe] = &[
    DriverProbe {
        name: "duckdb",
        env_prefix: "SIDEMANTIC_TEST_ADBC_DUCKDB",
        default_entrypoint: Some("duckdb_adbc_init"),
        default_dbopts: &[("path", ":memory:")],
    },
    DriverProbe {
        name: "sqlite",
        env_prefix: "SIDEMANTIC_TEST_ADBC_SQLITE",
        default_entrypoint: None,
        default_dbopts: &[],
    },
    DriverProbe {
        name: "postgres",
        env_prefix: "SIDEMANTIC_TEST_ADBC_POSTGRES",
        default_entrypoint: None,
        default_dbopts: &[],
    },
    DriverProbe {
        name: "bigquery",
        env_prefix: "SIDEMANTIC_TEST_ADBC_BIGQUERY",
        default_entrypoint: None,
        default_dbopts: &[],
    },
    DriverProbe {
        name: "snowflake",
        env_prefix: "SIDEMANTIC_TEST_ADBC_SNOWFLAKE",
        default_entrypoint: None,
        default_dbopts: &[],
    },
    DriverProbe {
        name: "clickhouse",
        env_prefix: "SIDEMANTIC_TEST_ADBC_CLICKHOUSE",
        default_entrypoint: None,
        default_dbopts: &[],
    },
];

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_option_value(value: &str) -> OptionValue {
    if let Some(rest) = value.strip_prefix("int:") {
        if let Ok(parsed) = rest.parse::<i64>() {
            return OptionValue::Int(parsed);
        }
    }
    if let Some(rest) = value.strip_prefix("float:") {
        if let Ok(parsed) = rest.parse::<f64>() {
            return OptionValue::Double(parsed);
        }
    }
    if let Some(rest) = value.strip_prefix("str:") {
        return OptionValue::String(rest.to_string());
    }
    if let Ok(parsed) = value.parse::<i64>() {
        return OptionValue::Int(parsed);
    }
    if let Ok(parsed) = value.parse::<f64>() {
        return OptionValue::Double(parsed);
    }
    OptionValue::String(value.to_string())
}

fn parse_database_options(prefix: &str) -> Vec<(OptionDatabase, OptionValue)> {
    let mut options = Vec::new();
    if let Some(raw) = env_value(&format!("{prefix}_DBOPTS")) {
        for pair in raw
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
        {
            let Some((key, value)) = pair.split_once('=') else {
                panic!("{prefix}_DBOPTS expects comma-separated key=value pairs, got {pair}");
            };
            options.push((OptionDatabase::from(key.trim()), parse_option_value(value)));
        }
    }
    options
}

fn parse_connection_options(prefix: &str) -> Vec<(OptionConnection, OptionValue)> {
    let mut options = Vec::new();
    if let Some(raw) = env_value(&format!("{prefix}_CONNOPTS")) {
        for pair in raw
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
        {
            let Some((key, value)) = pair.split_once('=') else {
                panic!("{prefix}_CONNOPTS expects comma-separated key=value pairs, got {pair}");
            };
            options.push((
                OptionConnection::from(key.trim()),
                parse_option_value(value),
            ));
        }
    }
    options
}

fn default_database_options(probe: &DriverProbe) -> Vec<(OptionDatabase, OptionValue)> {
    probe
        .default_dbopts
        .iter()
        .map(|(key, value)| (OptionDatabase::from(*key), parse_option_value(value)))
        .collect()
}

#[test]
fn rust_adbc_driver_matrix_executes_configured_drivers() {
    for probe in DRIVER_PROBES {
        let driver_var = format!("{}_DRIVER", probe.env_prefix);
        let Some(driver) = env_value(&driver_var) else {
            eprintln!(
                "skipping {} ADBC probe; {} is not set",
                probe.name, driver_var
            );
            continue;
        };

        let query = env_value(&format!("{}_QUERY", probe.env_prefix))
            .unwrap_or_else(|| "select 1 as sidemantic_adbc_probe".to_string());
        let uri = env_value(&format!("{}_URI", probe.env_prefix));
        let entrypoint = env_value(&format!("{}_ENTRYPOINT", probe.env_prefix))
            .or_else(|| probe.default_entrypoint.map(ToString::to_string));
        let mut database_options = default_database_options(probe);
        database_options.extend(parse_database_options(probe.env_prefix));
        let connection_options = parse_connection_options(probe.env_prefix);

        let result = execute_with_adbc(AdbcExecutionRequest {
            driver,
            sql: query,
            uri,
            entrypoint,
            database_options,
            connection_options,
        })
        .unwrap_or_else(|err| panic!("{} ADBC probe failed: {err}", probe.name));

        assert!(
            !result.columns.is_empty(),
            "{} ADBC probe should return at least one column",
            probe.name
        );
        assert!(
            !result.rows.is_empty(),
            "{} ADBC probe should return at least one row",
            probe.name
        );
    }
}
