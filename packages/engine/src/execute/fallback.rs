use super::super::*;

impl Engine {
    pub(crate) async fn execute_multi_statement_sequential_with_options(
        &self,
        sql: &str,
        params: &[Value],
        options: &ExecuteOptions,
    ) -> Result<QueryResult, LixError> {
        let statements = parse_sql_statements(sql)?;
        self.execute_statement_script_with_options(statements, params, options)
            .await
    }

    pub(crate) async fn execute_multi_statement_sequential_with_options_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        sql: &str,
        params: &[Value],
        options: &ExecuteOptions,
        active_version_id: &mut String,
        pending_state_commit_stream_changes: &mut Vec<StateCommitStreamChange>,
    ) -> Result<QueryResult, LixError> {
        let statements = parse_sql_statements(sql)?;
        self.execute_statement_script_with_options_in_transaction(
            transaction,
            statements,
            params,
            options,
            active_version_id,
            pending_state_commit_stream_changes,
        )
        .await
    }
}

#[cfg(test)]
pub(crate) fn should_sequentialize_postprocess_multi_statement(
    sql: &str,
    params: &[Value],
    error: &LixError,
) -> bool {
    let Ok(statements) = parse_sql_statements(sql) else {
        return false;
    };
    should_sequentialize_postprocess_multi_statement_with_statements(&statements, params, error)
}

pub(crate) fn should_sequentialize_postprocess_multi_statement_with_statements(
    statements: &[Statement],
    params: &[Value],
    error: &LixError,
) -> bool {
    if !params.is_empty() || !is_postprocess_multi_statement_error(&error.message) {
        return false;
    }
    if statements.len() <= 1 {
        return false;
    }

    !statements.iter().any(|statement| {
        matches!(
            statement,
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
        )
    })
}

pub(crate) fn is_postprocess_multi_statement_error(message: &str) -> bool {
    matches!(
        message,
        "postprocess rewrites require a single statement"
            | "only one postprocess rewrite is supported per query"
    )
}
