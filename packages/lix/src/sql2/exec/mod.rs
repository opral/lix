pub(crate) mod bound_public_write;
pub(crate) mod datafusion;
pub(crate) mod write;

use crate::SqlQueryResult;

/// Internal write outcome.  DML normally only reports its affected-row count,
/// while `DELETE … RETURNING` additionally carries the pre-delete result set.
/// Keeping both values explicit matters for deletes that stage extra cascade
/// rows: the number of returned direct targets can differ from rows affected.
pub(crate) struct SqlWriteResult {
    pub(crate) rows_affected: u64,
    pub(crate) returning: Option<SqlQueryResult>,
}

impl SqlWriteResult {
    pub(crate) fn affected(rows_affected: u64) -> Self {
        Self {
            rows_affected,
            returning: None,
        }
    }

    pub(crate) fn returning(rows_affected: u64, returning: SqlQueryResult) -> Self {
        Self {
            rows_affected,
            returning: Some(returning),
        }
    }

    pub(crate) fn diff_command(
        outcome: crate::sql2::DiffCommandOutcome,
        returning: Option<&crate::sql2::bind::write::BoundReturning>,
    ) -> Result<Self, crate::LixError> {
        let Some(returning) = returning else {
            return Ok(Self::affected(outcome.rows_affected));
        };
        let rows = match outcome.commit_id {
            Some(commit_id) => vec![
                returning
                    .items
                    .iter()
                    .map(|_| crate::Value::Text(commit_id.clone()))
                    .collect(),
            ],
            None if outcome.rows_affected == 0 => Vec::new(),
            None => {
                return Err(crate::LixError::new(
                    crate::LixError::CODE_INTERNAL_ERROR,
                    "diff command staged rows without a commit ID",
                ));
            }
        };
        Ok(Self::returning(
            outcome.rows_affected,
            SqlQueryResult {
                columns: returning
                    .items
                    .iter()
                    .map(|item| item.output_name.clone())
                    .collect(),
                column_types: vec![crate::ResultColumnType::Text; returning.items.len()],
                rows,
                notices: Vec::new(),
            },
        ))
    }
}

#[cfg(feature = "storage-benches")]
pub(crate) use datafusion::{
    BatchRowCursor, execute_read_statement_in_session_with_batch_stream,
    execute_read_statement_in_session_with_collected_batches,
};
pub(crate) use datafusion::{
    DataFusionLogicalPlan as SqlDataFusionLogicalPlan, SessionReadResult, SessionReadSqlResult,
    execute_read_statement_in_session_from_parsed, execute_read_statement_in_session_with_result,
    execute_transaction_read_statement_from_parsed, prepare_read_session,
    prepare_read_session_at_head, query_result_from_batches, query_values_from_batches,
};
#[cfg(test)]
pub(crate) use write::{
    WriteExecutorMode, WriteExecutorPath, create_write_logical_plan, execute_write_logical_plan,
    execute_write_logical_plan_with_mode, execute_write_logical_plan_with_mode_and_trace,
    execute_write_logical_plan_with_mode_and_trace_result,
    execute_write_logical_plan_with_mode_result,
};
pub(crate) use write::{
    WriteLogicalPlan as SqlWriteLogicalPlan, create_write_logical_plan_from_template,
    create_write_plan_template_from_parsed, diff_command_query, full_checkpoint_command,
    execute_write_logical_plan_parameter_batch, execute_write_logical_plan_prepared_dml_batch,
    execute_write_logical_plan_result_with_metadata, execute_write_logical_plan_value_batch,
    parameter_record_batch, parameter_row, write_plan_requires_post_stage_returning_checkpoint,
};

pub(crate) enum SqlLogicalPlan {
    DataFusion(SqlDataFusionLogicalPlan),
    Write(SqlWriteLogicalPlan),
}

pub(crate) fn prepare_path_value_replacement_program(
    ctx: &dyn crate::sql2::SqlWriteExecutionContext,
    plan: &SqlLogicalPlan,
) -> Option<bound_public_write::PreparedPathValueReplacementProgram> {
    let SqlLogicalPlan::Write(write) = plan else {
        return None;
    };
    bound_public_write::prepare_path_value_replacement_program_from_logical(ctx, &write.plan)
}

pub(crate) async fn prepare_path_value_replacement_row(
    ctx: &mut dyn crate::sql2::SqlWriteExecutionContext,
    program: &bound_public_write::PreparedPathValueReplacementProgram,
    params: &[crate::Value],
) -> Result<Option<bound_public_write::PreparedPathValueReplacementRow>, crate::LixError> {
    bound_public_write::prepare_path_value_replacement_row(ctx, program, params).await
}

pub(crate) fn append_path_value_replacement_snapshot(
    program: &bound_public_write::PreparedPathValueReplacementProgram,
    primary_key: &str,
    params: &[crate::Value],
    normalized: &mut Vec<u8>,
) -> Result<(usize, usize), crate::LixError> {
    bound_public_write::append_path_value_replacement_snapshot(
        program,
        primary_key,
        params,
        normalized,
    )
}

pub(crate) fn append_path_value_replacement_snapshot_text(
    primary_key: &str,
    replacement_value: Option<&str>,
    normalized: &mut Vec<u8>,
) -> Result<(usize, usize), crate::LixError> {
    bound_public_write::append_path_value_replacement_snapshot_text(
        primary_key,
        replacement_value,
        normalized,
    )
}

#[cfg(test)]
pub(crate) use bound_public_write::{
    take_certified_row_insert_batch_executions,
    take_certified_row_insert_parameter_batch_executions,
    take_certified_generation_identity_replacements,
    take_certified_replacement_parameter_batch_executions,
    take_certified_single_path_value_replacements, take_row_update_parameter_batch_executions,
};
