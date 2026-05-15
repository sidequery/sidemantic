use adbc_core::{
    options::{AdbcVersion, OptionConnection, OptionDatabase, OptionValue},
    Connection, Database, Driver, Statement, LOAD_FLAG_DEFAULT,
};
use adbc_driver_manager::ManagedDriver;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float16Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, RecordBatchReader, StringArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, TimeUnit};

use crate::error::{Result, SidemanticError};

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

#[derive(Debug, Clone)]
pub struct AdbcExecutionRequest {
    pub driver: String,
    pub sql: String,
    pub uri: Option<String>,
    pub entrypoint: Option<String>,
    pub database_options: Vec<(OptionDatabase, OptionValue)>,
    pub connection_options: Vec<(OptionConnection, OptionValue)>,
}

fn adbc_error(context: &str, err: impl std::fmt::Display) -> SidemanticError {
    SidemanticError::InvalidConfig(format!("{context}: {err}"))
}

pub fn execute_with_adbc(request: AdbcExecutionRequest) -> Result<AdbcExecutionResult> {
    let AdbcExecutionRequest {
        driver,
        sql,
        uri,
        entrypoint,
        mut database_options,
        connection_options,
    } = request;

    if let Some(uri) = uri {
        database_options.push((OptionDatabase::Uri, OptionValue::String(uri)));
    }

    let entrypoint_bytes = entrypoint.as_deref().map(str::as_bytes);
    let mut managed_driver = ManagedDriver::load_from_name(
        &driver,
        entrypoint_bytes,
        AdbcVersion::V110,
        LOAD_FLAG_DEFAULT,
        None,
    )
    .or_else(|_| {
        ManagedDriver::load_from_name(
            &driver,
            entrypoint_bytes,
            AdbcVersion::V100,
            LOAD_FLAG_DEFAULT,
            None,
        )
    })
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
