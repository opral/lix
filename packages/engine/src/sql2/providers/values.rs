//! Typed cell extraction from DataFusion record batches, shared by all table
//! specs. `ctx` is the error-message prefix (e.g. "INSERT lix_branch" or
//! "INSERT into lix_state") so each surface keeps its exact wording; messages
//! are only formatted on the error path.

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Column, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::InList;

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
pub(super) fn string_expr_literal(expr: &Expr) -> Option<String> {
    let Expr::Literal(literal, _) = expr else {
        return None;
    };
    match literal {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(value.clone()),
        _ => None,
    }
}

/// Build the exact string `IN` predicate understood by the file and entity
/// provider filter analyzers.
pub(super) fn string_in_filter(column_name: &str, values: &[String]) -> Expr {
    Expr::InList(InList::new(
        Box::new(Expr::Column(Column::from_name(column_name))),
        values
            .iter()
            .cloned()
            .map(|value| Expr::Literal(ScalarValue::Utf8(Some(value)), None))
            .collect(),
        false,
    ))
}
