use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, ScalarValue};

use crate::LixError;
use crate::filesystem::{
    FilesystemWriteOutcome, MountedEntryKind, MountedWorkspaceTarget, mounted_id_matches,
};
use crate::sql2::SqlWriteContext;
use crate::transaction::types::{TransactionWrite, TransactionWriteMode};

pub(crate) fn mounted_workspace_target_from_batch(
    batch: &RecordBatch,
    row_index: usize,
    active_branch_id: &str,
    kind: MountedEntryKind,
) -> Result<Option<MountedWorkspaceTarget>> {
    let id = required_string_value(batch, row_index, "id")?;
    if !mounted_id_matches(&id, kind) {
        return Ok(None);
    }
    let branch_id = optional_string_value(batch, row_index, "lixcol_branch_id")?
        .unwrap_or_else(|| active_branch_id.to_string());
    Ok(Some(MountedWorkspaceTarget {
        id,
        path: optional_string_value(batch, row_index, "path")?,
        branch_id,
        kind,
    }))
}

pub(crate) async fn stage_filesystem_write_outcome(
    write_ctx: &SqlWriteContext,
    mode: TransactionWriteMode,
    outcome: FilesystemWriteOutcome,
) -> std::result::Result<u64, LixError> {
    let count = outcome.count;
    if !outcome.state_rows.is_empty() || !outcome.file_data.is_empty() {
        let intent = if outcome.file_data.is_empty() {
            TransactionWrite::Rows {
                mode,
                rows: outcome.state_rows,
            }
        } else {
            TransactionWrite::RowsWithFileData {
                mode,
                rows: outcome.state_rows,
                file_data: outcome.file_data,
                count,
            }
        };
        write_ctx.stage_write(intent).await?;
    }
    for op in outcome.mounted_ops {
        write_ctx.stage_mounted_filesystem_op(op).await?;
    }
    Ok(count)
}

fn required_string_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<String> {
    optional_string_value(batch, row_index, column_name)?.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "filesystem write row requires materialized column '{column_name}'"
        ))
    })
}

fn optional_string_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<String>> {
    let column_index = match batch.schema().index_of(column_name) {
        Ok(index) => index,
        Err(_) => return Ok(None),
    };
    match ScalarValue::try_from_array(batch.column(column_index), row_index)? {
        ScalarValue::Null
        | ScalarValue::Utf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::LargeUtf8(None) => Ok(None),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Ok(Some(value)),
        other => Err(DataFusionError::Execution(format!(
            "filesystem write row expected text-compatible column '{column_name}', got {other:?}"
        ))),
    }
}
