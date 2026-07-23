use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Literal};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;

use crate::LixError;
use crate::sql2::exec::datafusion::LIX_INSERT_COLUMN_OMITTED_METADATA_KEY;

#[derive(Debug, Clone)]
pub(crate) enum SqlCell {
    Null,
    Value(ScalarValue),
}

impl SqlCell {
    pub(crate) fn from_scalar(value: ScalarValue) -> Self {
        if value.is_null() {
            Self::Null
        } else {
            Self::Value(value)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum InsertCell {
    Omitted,
    Provided(SqlCell),
}

#[derive(Debug, Clone)]
pub(crate) enum UpdateCell {
    Unassigned,
    Assigned(SqlCell),
}

#[derive(Debug, Clone)]
pub(crate) struct InsertColumnIntents {
    explicit_columns: Option<BTreeSet<String>>,
}

impl InsertColumnIntents {
    pub(crate) fn from_input(input: &Arc<dyn ExecutionPlan>) -> Self {
        if let Some(explicit_columns) = Self::explicit_columns_from_schema(input) {
            return Self {
                explicit_columns: Some(explicit_columns),
            };
        }

        let Some(projection) = input.as_any().downcast_ref::<ProjectionExec>() else {
            return Self {
                explicit_columns: None,
            };
        };

        let child_schema = projection.children().first().map(|child| child.schema());
        let explicit_columns = projection
            .expr()
            .iter()
            .enumerate()
            .filter(|(index, expr)| {
                !is_generated_null_default(expr.expr.as_ref())
                    && !child_schema
                        .as_ref()
                        .and_then(|schema| schema.fields().get(*index))
                        .is_some_and(|field| field_is_omitted_insert_default(field.as_ref()))
            })
            .map(|(_, expr)| expr.alias.clone())
            .collect();

        Self {
            explicit_columns: Some(explicit_columns),
        }
    }

    fn explicit_columns_from_schema(input: &Arc<dyn ExecutionPlan>) -> Option<BTreeSet<String>> {
        let omitted_columns = input
            .schema()
            .fields()
            .iter()
            .filter(|field| field_is_omitted_insert_default(field.as_ref()))
            .map(|field| field.name().clone())
            .collect::<BTreeSet<_>>();
        if omitted_columns.is_empty() {
            return None;
        }

        Some(
            input
                .schema()
                .fields()
                .iter()
                .filter(|field| !omitted_columns.contains(field.name().as_str()))
                .map(|field| field.name().clone())
                .collect(),
        )
    }

    pub(crate) fn includes_column(&self, column_name: &str) -> bool {
        self.explicit_columns
            .as_ref()
            .is_none_or(|columns| columns.contains(column_name))
    }

    pub(crate) fn omitted_columns(&self, schema: &Schema) -> BTreeSet<String> {
        let Some(explicit_columns) = self.explicit_columns.as_ref() else {
            return BTreeSet::new();
        };
        schema
            .fields()
            .iter()
            .filter(|field| !explicit_columns.contains(field.name().as_str()))
            .map(|field| field.name().clone())
            .collect()
    }
}

fn field_is_omitted_insert_default(field: &datafusion::arrow::datatypes::Field) -> bool {
    field
        .metadata()
        .get(LIX_INSERT_COLUMN_OMITTED_METADATA_KEY)
        .is_some_and(|value| value == "true")
}

pub(crate) fn insert_column_is_omitted(batch: &RecordBatch, column_name: &str) -> bool {
    batch
        .schema()
        .field_with_name(column_name)
        .is_ok_and(field_is_omitted_insert_default)
}

/// Reads a defaultable text input without collapsing an omitted column into an
/// explicitly provided SQL `NULL`.
pub(crate) fn defaultable_text_insert_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
    context: &str,
) -> Result<Option<String>> {
    let schema = batch.schema();
    let Ok(column_index) = schema.index_of(column_name) else {
        return Ok(None);
    };
    let field = schema.field(column_index);
    if field_is_omitted_insert_default(field) {
        return Ok(None);
    }
    match ScalarValue::try_from_array(batch.column(column_index).as_ref(), row_index)? {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Ok(Some(value)),
        value if value.is_null() => {
            Err(super::error::lix_error_to_datafusion_error(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!(
                    "{context} column '{column_name}' may be omitted to use its default, but explicit NULL is not allowed"
                ),
            )))
        }
        other => Err(super::error::lix_error_to_datafusion_error(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("{context} expected text-compatible column '{column_name}', got {other:?}"),
        ))),
    }
}

/// Reads a defaultable boolean input without collapsing an omitted column into
/// an explicitly provided SQL `NULL`.
pub(crate) fn defaultable_bool_insert_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
    context: &str,
) -> Result<Option<bool>> {
    let schema = batch.schema();
    let Ok(column_index) = schema.index_of(column_name) else {
        return Ok(None);
    };
    let field = schema.field(column_index);
    if field_is_omitted_insert_default(field) {
        return Ok(None);
    }
    match ScalarValue::try_from_array(batch.column(column_index).as_ref(), row_index)? {
        ScalarValue::Boolean(Some(value)) => Ok(Some(value)),
        value if value.is_null() => {
            Err(super::error::lix_error_to_datafusion_error(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!(
                    "{context} column '{column_name}' may be omitted to use its default, but explicit NULL is not allowed"
                ),
            )))
        }
        other => Err(super::error::lix_error_to_datafusion_error(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("{context} expected boolean column '{column_name}', got {other:?}"),
        ))),
    }
}

/// Restores insert-column intent metadata at the provider boundary.
///
/// DataFusion can discard alias metadata while executing a physical
/// projection, so detecting omission only from the resulting batch would
/// collapse omitted defaults into explicit `NULL`. The provider computes the
/// intent from the physical input plan and reapplies it here.
pub(crate) fn mark_omitted_insert_columns(
    batch: RecordBatch,
    omitted_columns: &BTreeSet<String>,
) -> Result<RecordBatch> {
    if omitted_columns.is_empty() {
        return Ok(batch);
    }
    let batch_schema = batch.schema();
    let fields = batch_schema
        .fields()
        .iter()
        .map(|field| {
            if !omitted_columns.contains(field.name().as_str()) {
                return field.as_ref().clone();
            }
            let mut metadata = field.metadata().clone();
            metadata.insert(
                LIX_INSERT_COLUMN_OMITTED_METADATA_KEY.to_string(),
                "true".to_string(),
            );
            field.as_ref().clone().with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch_schema.metadata().clone(),
    ));
    Ok(RecordBatch::try_new(schema, batch.columns().to_vec())?)
}

pub(crate) fn scalar_is_binary_or_null(value: &ScalarValue) -> bool {
    value.is_null()
        || matches!(
            value,
            ScalarValue::Binary(_)
                | ScalarValue::LargeBinary(_)
                | ScalarValue::FixedSizeBinary(_, _)
        )
}

pub(crate) fn lix_file_data_type_lix_error() -> LixError {
    LixError::new(
        LixError::CODE_TYPE_MISMATCH,
        "lix_file.data expects binary data",
    )
    .with_hint("Use X'...' or a binary parameter for file contents.")
}

pub(crate) fn lix_file_data_type_error(
    context: &str,
    column_name: &str,
    instruction: &str,
) -> DataFusionError {
    super::error::lix_error_to_datafusion_error(
        LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("{context} expected binary column '{column_name}'"),
        )
        .with_hint(instruction),
    )
}

pub(crate) fn lix_file_data_type_error_with_value(
    context: &str,
    column_name: &str,
    value: &ScalarValue,
    instruction: &str,
) -> DataFusionError {
    super::error::lix_error_to_datafusion_error(
        LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("{context} expected binary column '{column_name}', got {value:?}"),
        )
        .with_hint(instruction),
    )
}

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

    #[cfg(test)]
    pub(crate) fn from_batch_columns(batch: &RecordBatch, columns: &[&str]) -> Self {
        let values = columns
            .iter()
            .filter_map(|column_name| {
                let column_index = batch.schema().index_of(column_name).ok()?;
                Some((
                    (*column_name).to_string(),
                    Arc::clone(batch.column(column_index)),
                ))
            })
            .collect();
        Self { values }
    }

    /// Returns only the value explicitly assigned by SQL UPDATE.
    ///
    /// Use this for document-patch semantics where `Unassigned` must remain
    /// distinct from `Assigned(NULL)`.
    pub(crate) fn assigned_cell(&self, row_index: usize, column_name: &str) -> Result<UpdateCell> {
        let Some(array) = self.values.get(column_name) else {
            return Ok(UpdateCell::Unassigned);
        };

        ScalarValue::try_from_array(array.as_ref(), row_index)
            .map(SqlCell::from_scalar)
            .map(UpdateCell::Assigned)
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "failed to decode SQL UPDATE assignment for column '{column_name}' at row {row_index}: {error}"
                ))
            })
    }

    /// Returns the assigned SQL UPDATE value, or falls back to the existing row
    /// column value when the column was not assigned.
    ///
    /// Use this for scalar row-column semantics. Do not use it to reconstruct
    /// JSON documents from projected property columns, because projection can
    /// erase the difference between an absent property and an explicit null.
    pub(crate) fn assigned_or_existing_cell(
        &self,
        batch: &RecordBatch,
        row_index: usize,
        column_name: &str,
    ) -> Result<InsertCell> {
        match self.assigned_cell(row_index, column_name)? {
            UpdateCell::Assigned(value) => Ok(InsertCell::Provided(value)),
            UpdateCell::Unassigned => {
                optional_scalar_value(batch, row_index, column_name).map(|value| {
                    value.map_or(InsertCell::Omitted, |value| {
                        InsertCell::Provided(SqlCell::from_scalar(value))
                    })
                })
            }
        }
    }
}

pub(crate) fn optional_scalar_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<ScalarValue>> {
    let schema = batch.schema();
    let Ok(column_index) = schema.index_of(column_name) else {
        return Ok(None);
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

fn is_generated_null_default(expr: &dyn PhysicalExpr) -> bool {
    if let Some(literal) = expr.as_any().downcast_ref::<Literal>() {
        return literal.value().is_null();
    }

    if let Some(cast) = expr.as_any().downcast_ref::<CastExpr>() {
        return is_generated_null_default(cast.expr().as_ref());
    }

    false
}
