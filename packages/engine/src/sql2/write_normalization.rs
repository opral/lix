use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::physical_expr::PhysicalExpr;

pub(crate) struct UpdateAssignmentValues {
    values: BTreeMap<String, ArrayRef>,
}

impl UpdateAssignmentValues {
    pub(crate) fn evaluate(
        batch: &RecordBatch,
        assignments: &[(String, Arc<dyn PhysicalExpr>)],
    ) -> Result<Self> {
        let mut values = BTreeMap::new();
        for (column_name, assignment) in assignments {
            values.insert(
                column_name.clone(),
                assignment.evaluate(batch)?.into_array(batch.num_rows())?,
            );
        }
        Ok(Self { values })
    }

    pub(crate) fn scalar_value(
        &self,
        batch: &RecordBatch,
        row_index: usize,
        column_name: &str,
    ) -> Result<Option<ScalarValue>> {
        if let Some(array) = self.values.get(column_name) {
            return ScalarValue::try_from_array(array.as_ref(), row_index)
                .map(Some)
                .map_err(|error| {
                    DataFusionError::Execution(format!(
                        "failed to decode SQL UPDATE assignment for column '{column_name}' at row {row_index}: {error}"
                    ))
                });
        }
        optional_scalar_value(batch, row_index, column_name)
    }
}

pub(crate) fn optional_scalar_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<ScalarValue>> {
    let schema = batch.schema();
    let column_index = match schema.index_of(column_name) {
        Ok(column_index) => column_index,
        Err(_) => return Ok(None),
    };
    if row_index >= batch.num_rows() {
        return Err(DataFusionError::Execution(format!(
            "row index {row_index} out of bounds for SQL write batch with {} rows",
            batch.num_rows()
        )));
    }
    ScalarValue::try_from_array(batch.column(column_index).as_ref(), row_index)
        .map(Some)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "failed to decode SQL write column '{column_name}' at row {row_index}: {error}"
            ))
        })
}
