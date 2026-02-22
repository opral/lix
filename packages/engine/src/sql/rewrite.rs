use crate::LixError;
#[cfg(test)]
use crate::Value;
use sqlparser::ast::Statement;

#[cfg(test)]
pub(crate) fn extract_explicit_transaction_script(
    sql: &str,
    params: &[Value],
) -> Result<Option<Vec<Statement>>, LixError> {
    let _ = params;
    let statements = crate::sql::parse_sql_statements(sql)?;
    extract_explicit_transaction_script_from_statements(&statements)
}

pub(crate) fn extract_explicit_transaction_script_from_statements(
    statements: &[Statement],
) -> Result<Option<Vec<Statement>>, LixError> {
    if statements.len() < 2 {
        return Ok(None);
    }

    let first_is_begin = matches!(statements.first(), Some(Statement::StartTransaction { .. }));
    let last_is_commit = matches!(statements.last(), Some(Statement::Commit { .. }));
    if !first_is_begin || !last_is_commit {
        return Ok(None);
    }

    let middle = &statements[1..statements.len() - 1];
    if middle.iter().any(|statement| {
        matches!(
            statement,
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
        )
    }) {
        return Err(LixError {
            message:
                "nested transaction statements are not supported inside BEGIN ... COMMIT scripts"
                    .to_string(),
        });
    }

    Ok(Some(middle.to_vec()))
}
