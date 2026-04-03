mod direct;
mod prepare;
mod public;
mod rowset;

use crate::contracts::artifacts::{
    PreparedReadArtifact, PreparedReadProgram, PreparedReadStep, ResultContract,
};
use crate::explain_output::{render_analyzed_explain_result, render_plain_explain_result};
use crate::runtime::{normalize_sql_execution_error_with_backend, TransactionBackendAdapter};
use crate::{ExecuteResult, LixBackendTransaction, LixError, QueryResult};
use sqlparser::ast::Statement;
use std::time::Instant;

pub(crate) use prepare::{
    compile_committed_read_program_with_context, prepare_public_read_artifact,
};
pub(crate) use public::{
    execute_prepared_public_read_artifact_in_transaction,
    execute_prepared_public_read_artifact_with_backend,
};
pub(crate) use rowset::execute_read_time_projection_read_with_backend;

pub(crate) async fn execute_prepared_read_program_in_committed_read_transaction(
    transaction: &mut dyn LixBackendTransaction,
    prepared: &PreparedReadProgram,
) -> Result<ExecuteResult, LixError> {
    let mut results = Vec::new();

    for step in &prepared.steps {
        let result = execute_prepared_read_step_in_transaction(transaction, step).await?;
        results.push(result);
    }

    Ok(ExecuteResult {
        statements: results,
    })
}

async fn execute_prepared_read_step_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    prepared: &PreparedReadStep,
) -> Result<QueryResult, LixError> {
    if let Some(template) = &prepared.diagnostic_context.plain_explain_template {
        return render_plain_explain_result(template);
    }

    let execution_started = Instant::now();

    let result = match &prepared.artifact {
        PreparedReadArtifact::Public(public) => {
            execute_prepared_public_read_artifact_in_transaction(transaction, public).await
        }
        PreparedReadArtifact::Internal(internal) => {
            execute_prepared_internal_read_artifact_in_transaction(transaction, internal).await
        }
    };

    let result = match result {
        Ok(result) => result,
        Err(error) => {
            let backend = TransactionBackendAdapter::new(transaction);
            return Err(normalize_sql_execution_error_from_source_sql_with_backend(
                &backend,
                error,
                &prepared.diagnostic_context.source_sql,
            )
            .await);
        }
    };

    if let Some(template) = &prepared.diagnostic_context.analyzed_explain_template {
        return render_analyzed_explain_result(template, &result, execution_started.elapsed());
    }

    Ok(result)
}

async fn execute_prepared_internal_read_artifact_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    internal: &crate::contracts::artifacts::PreparedInternalReadArtifact,
) -> Result<QueryResult, LixError> {
    let mut internal_result = QueryResult {
        rows: Vec::new(),
        columns: Vec::new(),
    };
    for statement in &internal.prepared_batch.steps {
        internal_result = transaction
            .execute(&statement.sql, &statement.params)
            .await?;
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

async fn normalize_sql_execution_error_from_source_sql_with_backend(
    backend: &dyn crate::LixBackend,
    error: LixError,
    source_sql: &[String],
) -> LixError {
    let mut statements = Vec::<Statement>::new();
    for sql in source_sql {
        match crate::sql::parser::parse_sql_statements(sql) {
            Ok(mut parsed) => statements.append(&mut parsed),
            Err(_) => {
                return error;
            }
        }
    }
    normalize_sql_execution_error_with_backend(backend, error, &statements).await
}
