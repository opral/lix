use std::time::Instant;

use crate::execution::{render_analyzed_explain_result, render_plain_explain_result};
use crate::sql::{
    normalize_sql_error_with_read_diagnostic_context, PreparedReadArtifact, PreparedReadBatch,
    PreparedReadStatement,
};
use crate::{ExecuteResult, LixBackendTransaction, LixError, QueryResult};

use super::{public::execute_prepared_public_read_artifact_in_transaction, ReadExecutionHost};

pub(crate) async fn execute_prepared_read_batch_in_committed_read_transaction(
    transaction: &mut dyn LixBackendTransaction,
    host: &dyn ReadExecutionHost,
    prepared: &PreparedReadBatch,
) -> Result<ExecuteResult, LixError> {
    let mut results = Vec::new();

    for statement in &prepared.statements {
        let result =
            execute_prepared_read_statement_in_transaction(transaction, host, statement).await?;
        results.push(result);
    }

    Ok(ExecuteResult {
        statements: results,
        write_receipt: None,
    })
}

async fn execute_prepared_read_statement_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    host: &dyn ReadExecutionHost,
    prepared: &PreparedReadStatement,
) -> Result<QueryResult, LixError> {
    if let Some(template) = &prepared.diagnostic_context.plain_explain_template {
        return render_plain_explain_result(template);
    }

    let execution_started = Instant::now();

    let result = match &prepared.artifact {
        PreparedReadArtifact::Public(public) => {
            execute_prepared_public_read_artifact_in_transaction(transaction, host, public).await
        }
        PreparedReadArtifact::Scalar(artifact) => {
            execute_prepared_scalar_read_artifact_in_transaction(
                transaction,
                &artifact.prepared_batch,
            )
            .await
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

async fn execute_prepared_scalar_read_artifact_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    batch: &crate::backend::PreparedBatch,
) -> Result<QueryResult, LixError> {
    let mut result = QueryResult {
        rows: Vec::new(),
        columns: Vec::new(),
    };
    for statement in &batch.steps {
        result = transaction
            .execute(&statement.sql, &statement.params)
            .await?;
    }
    Ok(result)
}
