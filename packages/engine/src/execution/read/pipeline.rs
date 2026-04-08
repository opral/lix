use std::time::Instant;

use crate::common::errors::classification::normalize_sql_error_with_read_diagnostic_context;
use crate::contracts::artifacts::{
    PreparedInternalReadArtifact, PreparedReadArtifact, PreparedReadProgram, PreparedReadStep,
    ResultContract,
};
use crate::contracts::explain_output::{
    render_analyzed_explain_result, render_plain_explain_result,
};
use crate::{ExecuteResult, LixBackendTransaction, LixError, QueryResult};

use super::{public::execute_prepared_public_read_artifact_in_transaction, ReadExecutionBindings};

pub(crate) async fn execute_prepared_read_program_in_committed_read_transaction(
    transaction: &mut dyn LixBackendTransaction,
    bindings: &dyn ReadExecutionBindings,
    prepared: &PreparedReadProgram,
) -> Result<ExecuteResult, LixError> {
    let mut results = Vec::new();

    for step in &prepared.steps {
        let result = execute_prepared_read_step_in_transaction(transaction, bindings, step).await?;
        results.push(result);
    }

    Ok(ExecuteResult { statements: results })
}

async fn execute_prepared_read_step_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    bindings: &dyn ReadExecutionBindings,
    prepared: &PreparedReadStep,
) -> Result<QueryResult, LixError> {
    if let Some(template) = &prepared.diagnostic_context.plain_explain_template {
        return render_plain_explain_result(template);
    }

    let execution_started = Instant::now();

    let result = match &prepared.artifact {
        PreparedReadArtifact::Public(public) => {
            execute_prepared_public_read_artifact_in_transaction(transaction, bindings, public)
                .await
        }
        PreparedReadArtifact::Internal(internal) => {
            execute_prepared_internal_read_artifact_in_transaction(transaction, internal).await
        }
    };

    let result = match result {
        Ok(result) => result,
        Err(error) => {
            return Err(normalize_sql_error_with_read_diagnostic_context(
                error,
                &prepared.diagnostic_context,
            ));
        }
    };

    if let Some(template) = &prepared.diagnostic_context.analyzed_explain_template {
        return render_analyzed_explain_result(template, &result, execution_started.elapsed());
    }

    Ok(result)
}

async fn execute_prepared_internal_read_artifact_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    internal: &PreparedInternalReadArtifact,
) -> Result<QueryResult, LixError> {
    let mut internal_result = QueryResult {
        rows: Vec::new(),
        columns: Vec::new(),
    };
    for statement in &internal.prepared_batch.steps {
        internal_result = transaction.execute(&statement.sql, &statement.params).await?;
    }
    Ok(public_result_from_contract(
        internal.result_contract,
        &internal_result,
    ))
}

fn public_result_from_contract(
    contract: ResultContract,
    internal_result: &QueryResult,
) -> QueryResult {
    match contract {
        ResultContract::DmlNoReturning => QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        ResultContract::Select | ResultContract::DmlReturning | ResultContract::Other => {
            internal_result.clone()
        }
    }
}
