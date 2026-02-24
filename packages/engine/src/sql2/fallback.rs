use super::super::*;
use super::ast::utils::parse_sql_statements;

#[cfg(test)]
use super::ast::nodes::Statement;
#[cfg(test)]
use super::ast::walk::contains_transaction_control_statement;

impl Engine {
    #[cfg(test)]
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
}

#[cfg(test)]
pub(crate) fn should_sequentialize_postprocess_multi_statement(
    sql: &str,
    params: &[Value],
) -> bool {
    let Ok(statements) = parse_sql_statements(sql) else {
        return false;
    };
    should_sequentialize_postprocess_multi_statement_with_statements(&statements, params)
}

#[cfg(test)]
pub(crate) fn should_sequentialize_postprocess_multi_statement_with_statements(
    statements: &[Statement],
    params: &[Value],
) -> bool {
    if !params.is_empty() {
        return false;
    }
    if statements.len() <= 1 {
        return false;
    }

    !contains_transaction_control_statement(statements)
}
