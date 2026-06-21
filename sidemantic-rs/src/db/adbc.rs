use adbc_core::{
    options::{AdbcVersion, OptionConnection, OptionDatabase, OptionValue},
    Connection, Database, Driver, Statement, LOAD_FLAG_DEFAULT,
};
use adbc_driver_manager::{ManagedConnection, ManagedDatabase, ManagedDriver};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float16Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, RecordBatchReader, StringArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Schema, TimeUnit};
use std::io::Write;
use std::path::PathBuf;

use crate::core::SemanticGraph;
use crate::error::{Result, SidemanticError};
use crate::sql::{QueryRewriter, SemanticQuery, SqlGenerator};

use super::result::ExecutionResult;
use super::url::ConnectionSpec;

#[derive(Debug, Clone, PartialEq)]
pub enum AdbcValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdbcExecutionResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<AdbcValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdbcArrowIpcResult {
    pub bytes: Vec<u8>,
    pub row_count: usize,
}

#[derive(Debug, Clone)]
pub struct AdbcExecutionRequest {
    pub driver: String,
    pub sql: String,
    pub uri: Option<String>,
    pub entrypoint: Option<String>,
    pub database_options: Vec<(OptionDatabase, OptionValue)>,
    pub connection_options: Vec<(OptionConnection, OptionValue)>,
}

/// Pure Rust ADBC executor for semantic queries.
///
/// Drivers are loaded by the ADBC driver manager. Drivers installed with
/// `dbc install <driver>` are found through the manager's normal manifest
/// search paths.
pub struct AdbcExecutor {
    pub spec: ConnectionSpec,
    database: ManagedDatabase,
    connection: ManagedConnection,
}

impl AdbcExecutor {
    pub fn connect(spec: ConnectionSpec) -> Result<Self> {
        let entrypoint = spec.entrypoint.clone();
        let mut driver = load_managed_driver(
            &spec.driver,
            entrypoint.as_deref(),
            spec.adbc_version,
            spec.load_flags,
            spec.additional_search_paths.clone(),
        )
        .map_err(|err| {
            SidemanticError::Database(format!(
                "failed to load ADBC driver '{}' through the dbc/ADBC registry. \
                 Install the driver with `dbc install {}` and make sure the ADBC driver \
                 manager search path can see it. Underlying error: {err}",
                spec.driver, spec.driver
            ))
        })?;

        let options = connection_spec_database_options(&spec);
        let database = if options.is_empty() {
            driver.new_database()
        } else {
            driver.new_database_with_opts(options)
        }?;
        let connection = database.new_connection()?;

        Ok(Self {
            spec,
            database,
            connection,
        })
    }

    pub fn connect_url(url: &str) -> Result<Self> {
        Self::connect(ConnectionSpec::from_url(url)?)
    }

    /// Execute SQL and return an Arrow record batch reader.
    pub fn execute_sql(&mut self, sql: &str) -> Result<ExecutionResult> {
        let mut statement = self.connection.new_statement()?;
        statement.set_sql_query(sql)?;
        let mut reader = statement.execute()?;
        let schema = reader.schema();
        let mut batches = Vec::new();
        for batch in &mut reader {
            batches.push(batch?);
        }
        Ok(ExecutionResult::new(sql.to_string(), schema, batches))
    }

    /// Execute a SQL statement that does not return a result set.
    pub fn execute_update(&mut self, sql: &str) -> Result<Option<i64>> {
        let mut statement = self.connection.new_statement()?;
        statement.set_sql_query(sql)?;
        Ok(statement.execute_update()?)
    }

    /// Generate SQL from a semantic query and execute it through ADBC.
    pub fn execute_semantic_query(
        &mut self,
        graph: &SemanticGraph,
        query: &SemanticQuery,
    ) -> Result<ExecutionResult> {
        let sql = SqlGenerator::new(graph).generate(query)?;
        self.execute_sql(&sql)
    }

    /// Rewrite SQL through the semantic graph, then execute the rewritten SQL.
    pub fn rewrite_and_execute(
        &mut self,
        graph: &SemanticGraph,
        sql: &str,
    ) -> Result<ExecutionResult> {
        let rewritten = QueryRewriter::new(graph).rewrite(sql)?;
        self.execute_sql(&rewritten)
    }

    /// Return an ADBC metadata stream for catalogs, schemas, tables, and columns.
    pub fn get_objects(
        &self,
        depth: adbc_core::options::ObjectDepth,
    ) -> Result<Box<dyn RecordBatchReader + Send + '_>> {
        Ok(Box::new(
            self.connection
                .get_objects(depth, None, None, None, None, None)?,
        ))
    }

    /// Get the Arrow schema for a table.
    pub fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        Ok(self
            .connection
            .get_table_schema(catalog, db_schema, table_name)?)
    }

    /// Keep a reference to the ADBC database handle for advanced callers.
    pub fn database(&self) -> &ManagedDatabase {
        &self.database
    }
}

/// Candidate driver names to try, in priority order.
///
/// `dbc`/ADBC driver manifests are commonly registered under a short name
/// (e.g. `duckdb`), while the conventional shared library follows the
/// `adbc_driver_<name>` naming convention. Try both so a driver resolves
/// regardless of which scheme the local registry uses. Explicit library paths
/// are passed through untouched.
fn driver_name_candidates(driver: &str) -> Vec<String> {
    let mut names = vec![driver.to_string()];
    if let Some(short) = driver.strip_prefix("adbc_driver_") {
        if !short.is_empty() {
            names.push(short.to_string());
        }
    } else if !driver.contains('/')
        && !driver.contains('\\')
        && !driver.ends_with(".so")
        && !driver.ends_with(".dylib")
        && !driver.ends_with(".dll")
    {
        names.push(format!("adbc_driver_{driver}"));
    }
    names
}

fn load_managed_driver(
    driver: &str,
    entrypoint: Option<&str>,
    adbc_version: AdbcVersion,
    load_flags: adbc_core::LoadFlags,
    additional_search_paths: Option<Vec<PathBuf>>,
) -> std::result::Result<ManagedDriver, adbc_core::error::Error> {
    let entrypoint_bytes = entrypoint.map(str::as_bytes);
    let mut versions = vec![adbc_version];
    if !matches!(adbc_version, AdbcVersion::V100) {
        versions.push(AdbcVersion::V100);
    }

    let mut last_err: Option<adbc_core::error::Error> = None;
    for name in driver_name_candidates(driver) {
        for &version in &versions {
            match ManagedDriver::load_from_name(
                &name,
                entrypoint_bytes,
                version,
                load_flags,
                additional_search_paths.clone(),
            ) {
                Ok(driver) => return Ok(driver),
                Err(err) => last_err = Some(err),
            }
        }
    }
    Err(last_err.expect("at least one driver candidate is always attempted"))
}

fn adbc_error(context: &str, err: impl std::fmt::Display) -> SidemanticError {
    SidemanticError::InvalidConfig(format!("{context}: {err}"))
}

fn is_duckdb_driver(driver: &str, entrypoint: Option<&str>) -> bool {
    driver.to_ascii_lowercase().contains("duckdb")
        || entrypoint
            .map(|entrypoint| entrypoint.to_ascii_lowercase().contains("duckdb"))
            .unwrap_or(false)
}

fn has_database_option(options: &[(OptionDatabase, OptionValue)], key: &str) -> bool {
    options.iter().any(|(option, _)| option.as_ref() == key)
}

fn database_options_with_uri(
    driver: &str,
    entrypoint: Option<&str>,
    uri: Option<String>,
    mut database_options: Vec<(OptionDatabase, OptionValue)>,
) -> Vec<(OptionDatabase, OptionValue)> {
    let Some(uri) = uri else {
        return database_options;
    };

    if is_duckdb_driver(driver, entrypoint) {
        if !has_database_option(&database_options, "path") {
            database_options.push((
                OptionDatabase::Other("path".to_string()),
                OptionValue::String(uri),
            ));
        }
    } else if !has_database_option(&database_options, OptionDatabase::Uri.as_ref()) {
        database_options.push((OptionDatabase::Uri, OptionValue::String(uri)));
    }

    database_options
}

fn connection_spec_database_options(spec: &ConnectionSpec) -> Vec<(OptionDatabase, OptionValue)> {
    let options = spec
        .database_options
        .iter()
        .map(|(key, value)| (database_option_key(key), OptionValue::String(value.clone())))
        .collect();
    database_options_with_uri(
        &spec.driver,
        spec.entrypoint.as_deref(),
        spec.uri.clone(),
        options,
    )
}

fn database_option_key(key: &str) -> OptionDatabase {
    match key {
        "uri" | "adbc.uri" => OptionDatabase::Uri,
        "username" | "user" | "adbc.username" => OptionDatabase::Username,
        "password" | "adbc.password" => OptionDatabase::Password,
        other => OptionDatabase::Other(other.to_string()),
    }
}

pub fn execute_with_adbc(request: AdbcExecutionRequest) -> Result<AdbcExecutionResult> {
    let AdbcExecutionRequest {
        driver,
        sql,
        uri,
        entrypoint,
        database_options,
        connection_options,
    } = request;

    let database_options =
        database_options_with_uri(&driver, entrypoint.as_deref(), uri, database_options);
    let mut managed_driver = load_managed_driver(
        &driver,
        entrypoint.as_deref(),
        AdbcVersion::V110,
        LOAD_FLAG_DEFAULT,
        None,
    )
    .map_err(|e| adbc_error("failed to load ADBC driver", e))?;

    let database = if database_options.is_empty() {
        managed_driver.new_database()
    } else {
        managed_driver.new_database_with_opts(database_options)
    }
    .map_err(|e| adbc_error("failed to create ADBC database", e))?;

    let mut connection = if connection_options.is_empty() {
        database.new_connection()
    } else {
        database.new_connection_with_opts(connection_options)
    }
    .map_err(|e| adbc_error("failed to create ADBC connection", e))?;

    let mut statement = connection
        .new_statement()
        .map_err(|e| adbc_error("failed to create ADBC statement", e))?;
    statement
        .set_sql_query(&sql)
        .map_err(|e| adbc_error("failed to set SQL query", e))?;
    let mut reader = statement
        .execute()
        .map_err(|e| adbc_error("failed to execute SQL query", e))?;

    let fields = reader.schema().fields().clone();
    let columns = fields
        .iter()
        .map(|field| field.name().to_string())
        .collect();

    let mut rows: Vec<Vec<AdbcValue>> = Vec::new();
    for batch in &mut reader {
        let batch = batch.map_err(|e| adbc_error("failed reading Arrow batch", e))?;
        for row_index in 0..batch.num_rows() {
            let mut values: Vec<AdbcValue> = Vec::with_capacity(batch.num_columns());
            for col_index in 0..batch.num_columns() {
                let field = &fields[col_index];
                let array = batch.column(col_index);
                values.push(array_cell_to_value(
                    array.as_ref(),
                    field.data_type(),
                    row_index,
                )?);
            }
            rows.push(values);
        }
    }

    Ok(AdbcExecutionResult { columns, rows })
}

pub fn execute_with_adbc_arrow_ipc(request: AdbcExecutionRequest) -> Result<AdbcArrowIpcResult> {
    let mut bytes = Vec::new();
    let row_count = write_adbc_arrow_ipc(request, &mut bytes)?;
    Ok(AdbcArrowIpcResult { bytes, row_count })
}

pub fn write_adbc_arrow_ipc<W: Write>(request: AdbcExecutionRequest, writer: W) -> Result<usize> {
    let AdbcExecutionRequest {
        driver,
        sql,
        uri,
        entrypoint,
        database_options,
        connection_options,
    } = request;

    let database_options =
        database_options_with_uri(&driver, entrypoint.as_deref(), uri, database_options);
    let mut managed_driver = load_managed_driver(
        &driver,
        entrypoint.as_deref(),
        AdbcVersion::V110,
        LOAD_FLAG_DEFAULT,
        None,
    )
    .map_err(|e| adbc_error("failed to load ADBC driver", e))?;

    let database = if database_options.is_empty() {
        managed_driver.new_database()
    } else {
        managed_driver.new_database_with_opts(database_options)
    }
    .map_err(|e| adbc_error("failed to create ADBC database", e))?;

    let mut connection = if connection_options.is_empty() {
        database.new_connection()
    } else {
        database.new_connection_with_opts(connection_options)
    }
    .map_err(|e| adbc_error("failed to create ADBC connection", e))?;

    let mut statement = connection
        .new_statement()
        .map_err(|e| adbc_error("failed to create ADBC statement", e))?;
    statement
        .set_sql_query(&sql)
        .map_err(|e| adbc_error("failed to set SQL query", e))?;
    let mut reader = statement
        .execute()
        .map_err(|e| adbc_error("failed to execute SQL query", e))?;

    let schema = reader.schema();
    let mut writer = StreamWriter::try_new(writer, &schema)
        .map_err(|e| adbc_error("failed to create Arrow IPC writer", e))?;
    let mut row_count = 0;

    for batch in &mut reader {
        let batch = batch.map_err(|e| adbc_error("failed reading Arrow batch", e))?;
        row_count += batch.num_rows();
        writer
            .write(&batch)
            .map_err(|e| adbc_error("failed writing Arrow IPC batch", e))?;
    }
    writer
        .finish()
        .map_err(|e| adbc_error("failed finishing Arrow IPC stream", e))?;
    let _ = writer
        .into_inner()
        .map_err(|e| adbc_error("failed finishing Arrow IPC stream", e))?;

    Ok(row_count)
}

fn decimal128_to_string(value: i128, scale: i8) -> String {
    if scale <= 0 {
        let multiplier = 10_i128.pow((-scale) as u32);
        return (value * multiplier).to_string();
    }

    let negative = value < 0;
    let digits = value.abs().to_string();
    let scale_usize = scale as usize;

    let rendered = if digits.len() <= scale_usize {
        format!("0.{}{}", "0".repeat(scale_usize - digits.len()), digits)
    } else {
        let split = digits.len() - scale_usize;
        format!("{}.{}", &digits[..split], &digits[split..])
    };

    if negative {
        format!("-{rendered}")
    } else {
        rendered
    }
}

fn downcast_error(ty: &str) -> SidemanticError {
    SidemanticError::InvalidConfig(format!("failed to read {ty} column"))
}

fn array_cell_to_value(
    array: &dyn Array,
    data_type: &DataType,
    row_index: usize,
) -> Result<AdbcValue> {
    if array.is_null(row_index) {
        return Ok(AdbcValue::Null);
    }

    match data_type {
        DataType::Null => Ok(AdbcValue::Null),
        DataType::Boolean => Ok(AdbcValue::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| downcast_error("Boolean"))?
                .value(row_index),
        )),
        DataType::Int8 => Ok(AdbcValue::I64(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| downcast_error("Int8"))?
                .value(row_index) as i64,
        )),
        DataType::Int16 => Ok(AdbcValue::I64(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| downcast_error("Int16"))?
                .value(row_index) as i64,
        )),
        DataType::Int32 => Ok(AdbcValue::I64(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| downcast_error("Int32"))?
                .value(row_index) as i64,
        )),
        DataType::Int64 => Ok(AdbcValue::I64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| downcast_error("Int64"))?
                .value(row_index),
        )),
        DataType::UInt8 => Ok(AdbcValue::U64(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| downcast_error("UInt8"))?
                .value(row_index) as u64,
        )),
        DataType::UInt16 => Ok(AdbcValue::U64(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| downcast_error("UInt16"))?
                .value(row_index) as u64,
        )),
        DataType::UInt32 => Ok(AdbcValue::U64(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| downcast_error("UInt32"))?
                .value(row_index) as u64,
        )),
        DataType::UInt64 => Ok(AdbcValue::U64(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| downcast_error("UInt64"))?
                .value(row_index),
        )),
        DataType::Float16 => Ok(AdbcValue::F64(
            array
                .as_any()
                .downcast_ref::<Float16Array>()
                .ok_or_else(|| downcast_error("Float16"))?
                .value(row_index)
                .to_f32() as f64,
        )),
        DataType::Float32 => Ok(AdbcValue::F64(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| downcast_error("Float32"))?
                .value(row_index) as f64,
        )),
        DataType::Float64 => Ok(AdbcValue::F64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| downcast_error("Float64"))?
                .value(row_index),
        )),
        DataType::Utf8 => Ok(AdbcValue::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| downcast_error("Utf8"))?
                .value(row_index)
                .to_string(),
        )),
        DataType::LargeUtf8 => Ok(AdbcValue::String(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| downcast_error("LargeUtf8"))?
                .value(row_index)
                .to_string(),
        )),
        DataType::Binary => Ok(AdbcValue::Bytes(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| downcast_error("Binary"))?
                .value(row_index)
                .to_vec(),
        )),
        DataType::LargeBinary => Ok(AdbcValue::Bytes(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| downcast_error("LargeBinary"))?
                .value(row_index)
                .to_vec(),
        )),
        DataType::Decimal128(_, scale) => {
            let value = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| downcast_error("Decimal128"))?
                .value(row_index);
            Ok(AdbcValue::String(decimal128_to_string(value, *scale)))
        }
        DataType::Date32 => Ok(AdbcValue::I64(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| downcast_error("Date32"))?
                .value(row_index) as i64,
        )),
        DataType::Date64 => Ok(AdbcValue::I64(
            array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| downcast_error("Date64"))?
                .value(row_index),
        )),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => Ok(AdbcValue::I64(
                array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| downcast_error("Timestamp(second)"))?
                    .value(row_index),
            )),
            TimeUnit::Millisecond => Ok(AdbcValue::I64(
                array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| downcast_error("Timestamp(millisecond)"))?
                    .value(row_index),
            )),
            TimeUnit::Microsecond => Ok(AdbcValue::I64(
                array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| downcast_error("Timestamp(microsecond)"))?
                    .value(row_index),
            )),
            TimeUnit::Nanosecond => Ok(AdbcValue::I64(
                array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| downcast_error("Timestamp(nanosecond)"))?
                    .value(row_index),
            )),
        },
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => Ok(AdbcValue::I64(
                array
                    .as_any()
                    .downcast_ref::<Time32SecondArray>()
                    .ok_or_else(|| downcast_error("Time32(second)"))?
                    .value(row_index) as i64,
            )),
            TimeUnit::Millisecond => Ok(AdbcValue::I64(
                array
                    .as_any()
                    .downcast_ref::<Time32MillisecondArray>()
                    .ok_or_else(|| downcast_error("Time32(millisecond)"))?
                    .value(row_index) as i64,
            )),
            _ => Err(SidemanticError::InvalidConfig(
                "unsupported Time32 unit in Rust ADBC executor".to_string(),
            )),
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => Ok(AdbcValue::I64(
                array
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .ok_or_else(|| downcast_error("Time64(microsecond)"))?
                    .value(row_index),
            )),
            TimeUnit::Nanosecond => Ok(AdbcValue::I64(
                array
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .ok_or_else(|| downcast_error("Time64(nanosecond)"))?
                    .value(row_index),
            )),
            _ => Err(SidemanticError::InvalidConfig(
                "unsupported Time64 unit in Rust ADBC executor".to_string(),
            )),
        },
        _ => Err(SidemanticError::InvalidConfig(format!(
            "unsupported Arrow datatype in Rust ADBC executor: {data_type:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    fn assert_single_string_database_option(
        options: &[(OptionDatabase, OptionValue)],
        key: &str,
        expected_value: &str,
    ) {
        assert_eq!(options.len(), 1);
        assert_eq!(options[0].0.as_ref(), key);
        let OptionValue::String(actual_value) = &options[0].1 else {
            panic!("expected string option value, got {:?}", options[0].1);
        };
        assert_eq!(actual_value, expected_value);
    }

    #[test]
    fn test_duckdb_uri_maps_to_path_database_option() {
        let options = database_options_with_uri(
            "/tmp/libduckdb.so",
            Some("duckdb_adbc_init"),
            Some("/tmp/warehouse.duckdb".to_string()),
            Vec::new(),
        );

        assert_single_string_database_option(&options, "path", "/tmp/warehouse.duckdb");
    }

    #[test]
    fn test_duckdb_uri_preserves_explicit_path_option() {
        let options = database_options_with_uri(
            "adbc_driver_duckdb",
            None,
            Some("/tmp/ignored.duckdb".to_string()),
            vec![(
                OptionDatabase::Other("path".to_string()),
                OptionValue::String("/tmp/explicit.duckdb".to_string()),
            )],
        );

        assert_single_string_database_option(&options, "path", "/tmp/explicit.duckdb");
    }

    #[test]
    fn test_driver_name_candidates_tries_both_naming_schemes() {
        // Short name (dbc manifest convention) also tries the canonical prefix.
        assert_eq!(
            driver_name_candidates("duckdb"),
            vec!["duckdb".to_string(), "adbc_driver_duckdb".to_string()]
        );
        // Canonical prefix also tries the short manifest name.
        assert_eq!(
            driver_name_candidates("adbc_driver_duckdb"),
            vec!["adbc_driver_duckdb".to_string(), "duckdb".to_string()]
        );
        // Explicit library paths are passed through untouched.
        assert_eq!(
            driver_name_candidates("/tmp/libduckdb.dylib"),
            vec!["/tmp/libduckdb.dylib".to_string()]
        );
        assert_eq!(
            driver_name_candidates(".\\drivers\\duckdb.dll"),
            vec![".\\drivers\\duckdb.dll".to_string()]
        );
    }

    #[test]
    fn test_non_duckdb_uri_uses_canonical_uri_option() {
        let options = database_options_with_uri(
            "adbc_driver_sqlite",
            None,
            Some(":memory:".to_string()),
            Vec::new(),
        );

        assert_single_string_database_option(&options, OptionDatabase::Uri.as_ref(), ":memory:");
    }

    #[test]
    fn test_decimal128_to_string() {
        assert_eq!(decimal128_to_string(12345, 2), "123.45");
        assert_eq!(decimal128_to_string(-12345, 2), "-123.45");
        assert_eq!(decimal128_to_string(15, 4), "0.0015");
    }

    #[test]
    fn test_array_cell_to_value_int32_and_null() {
        let array = Int32Array::from(vec![Some(7), None]);
        assert_eq!(
            array_cell_to_value(&array, &DataType::Int32, 0).unwrap(),
            AdbcValue::I64(7)
        );
        assert_eq!(
            array_cell_to_value(&array, &DataType::Int32, 1).unwrap(),
            AdbcValue::Null
        );
    }

    #[test]
    fn test_array_cell_to_value_binary() {
        let array = BinaryArray::from(vec![Some(b"abc".as_slice())]);
        assert_eq!(
            array_cell_to_value(&array, &DataType::Binary, 0).unwrap(),
            AdbcValue::Bytes(b"abc".to_vec())
        );
    }
}
