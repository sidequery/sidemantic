//! Result wrappers for ADBC execution.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::error::Result;

/// Arrow-backed result of a database query.
pub struct ExecutionResult {
    pub sql: String,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl ExecutionResult {
    pub(crate) fn new(sql: String, schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            sql,
            schema,
            batches,
        }
    }

    /// Return the Arrow schema for this result.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Collect all record batches from the result stream.
    pub fn collect(self) -> Result<Vec<RecordBatch>> {
        Ok(self.batches)
    }
}
