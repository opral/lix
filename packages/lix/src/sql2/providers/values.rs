//! Typed cell extraction from DataFusion record batches, shared by all table
//! specs. `ctx` is the error-message prefix (e.g. "INSERT lix_branch") so each
//! surface keeps its exact wording; messages
//! are only formatted on the error path.

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, ScalarValue};

pub(super) fn required_string_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
    ctx: &str,
) -> Result<String> {
    optional_string_value(batch, row_index, column_name, ctx)?.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "{ctx} requires non-null text column '{column_name}'"
        ))
    })
}

pub(super) fn optional_string_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
    ctx: &str,
) -> Result<Option<String>> {
    match optional_scalar_value(batch, row_index, column_name)? {
        None
        | Some(
            ScalarValue::Null
            | ScalarValue::Utf8(None)
            | ScalarValue::Utf8View(None)
            | ScalarValue::LargeUtf8(None),
        ) => Ok(None),
        Some(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)),
        ) => Ok(Some(value)),
        Some(other) => Err(DataFusionError::Execution(format!(
            "{ctx} expected text-compatible column '{column_name}', got {other:?}"
        ))),
    }
}

pub(super) fn required_bool_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
    ctx: &str,
) -> Result<bool> {
    optional_bool_value(batch, row_index, column_name, ctx)?.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "{ctx} requires non-null boolean column '{column_name}'"
        ))
    })
}

pub(super) fn optional_bool_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
    ctx: &str,
) -> Result<Option<bool>> {
    match optional_scalar_value(batch, row_index, column_name)? {
        None | Some(ScalarValue::Null | ScalarValue::Boolean(None)) => Ok(None),
        Some(ScalarValue::Boolean(Some(value))) => Ok(Some(value)),
        Some(other) => Err(DataFusionError::Execution(format!(
            "{ctx} expected boolean column '{column_name}', got {other:?}"
        ))),
    }
}

pub(super) fn optional_scalar_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<ScalarValue>> {
    let Ok(column_index) = batch.schema().index_of(column_name) else {
        return Ok(None);
    };
    Ok(Some(ScalarValue::try_from_array(
        batch.column(column_index).as_ref(),
        row_index,
    )?))
}

/// Extract a string literal from a logical expression, shared by the specs
/// that parse pushed-down filters.
pub(super) fn string_expr_literal(expr: &datafusion::logical_expr::Expr) -> Option<String> {
    let datafusion::logical_expr::Expr::Literal(literal, _) = expr else {
        return None;
    };
    match literal {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(value.clone()),
        _ => None,
    }
}

/// Parse row metadata consistently across descriptor-backed SQL surfaces.
pub(super) fn optional_metadata_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
    context: &str,
) -> Result<Option<crate::transaction_types::TransactionJson>> {
    parse_metadata(
        optional_string_value(batch, row_index, column_name, context)?,
        context,
    )
}

pub(super) fn update_optional_metadata_value(
    batch: &RecordBatch,
    assignments: &crate::sql2::write_normalization::UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
    context: &str,
) -> Result<Option<crate::transaction_types::TransactionJson>> {
    use crate::sql2::write_normalization::{InsertCell, SqlCell};
    let value = match assignments.assigned_or_existing_cell(batch, row_index, column_name)? {
        InsertCell::Omitted | InsertCell::Provided(SqlCell::Null) => None,
        InsertCell::Provided(SqlCell::Value(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)),
        )) => Some(value),
        InsertCell::Provided(SqlCell::Value(other)) => {
            return Err(DataFusionError::Execution(format!(
                "UPDATE {context} expected text-compatible column '{column_name}', got {other:?}"
            )));
        }
    };
    parse_metadata(value, context)
}

fn parse_metadata(
    value: Option<String>,
    context: &str,
) -> Result<Option<crate::transaction_types::TransactionJson>> {
    value
        .map(|value| {
            let metadata = crate::parse_row_metadata_value(&value, context)
                .map_err(crate::sql2::error::lix_error_to_datafusion_error)?;
            crate::transaction_types::TransactionJson::from_value(
                metadata,
                &format!("{context} metadata"),
            )
            .map_err(crate::sql2::error::lix_error_to_datafusion_error)
        })
        .transpose()
}
