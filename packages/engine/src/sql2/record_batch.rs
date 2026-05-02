use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result};

pub(crate) fn record_batch_with_row_count(
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
    row_count: usize,
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        return RecordBatch::try_new_with_options(schema, columns, &options)
            .map_err(DataFusionError::from);
    }
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}
