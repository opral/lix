pub(crate) mod bound_public_read;
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
            Some(commit_id) => (0..outcome.rows_affected)
                .map(|_| {
                    returning
                        .items
                        .iter()
                        .map(|_| crate::Value::Text(commit_id.clone()))
                        .collect()
                })
                .collect(),
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
                rows,
                notices: Vec::new(),
            },
        ))
    }
}

pub(crate) use datafusion::{
    DataFusionLogicalPlan as SqlDataFusionLogicalPlan, SessionReadSqlResult,
    execute_read_statement_from_parsed, execute_read_statement_in_session_from_parsed,
    execute_transaction_read_statement_from_parsed, prepare_read_session,
    prepare_read_session_at_head,
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
    create_write_plan_template_from_parsed, diff_command_query,
    execute_write_logical_plan_parameter_batch, execute_write_logical_plan_result_with_metadata,
    parameter_record_batch,
};

pub(crate) enum SqlLogicalPlan {
    DataFusion(SqlDataFusionLogicalPlan),
    Write(SqlWriteLogicalPlan),
}

#[cfg(test)]
pub(crate) use bound_public_write::{
    take_certified_entity_insert_batch_executions,
    take_certified_entity_insert_parameter_batch_executions,
    take_certified_replacement_parameter_batch_executions,
    take_entity_update_parameter_batch_executions,
};
