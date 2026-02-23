use crate::LixError;

use super::nodes::Statement;

pub(crate) fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, LixError> {
    crate::sql::parse_sql_statements(sql)
}

#[cfg(test)]
pub(crate) fn is_transaction_control_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. }
    )
}
