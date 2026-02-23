use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::{LixError, SqlDialect, Value};

use super::nodes::Statement;

pub(crate) type PlaceholderState = crate::sql::PlaceholderState;

pub(crate) struct BoundSql {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
    pub(crate) state: PlaceholderState,
}

pub(crate) fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, LixError> {
    Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        message: error.to_string(),
    })
}

pub(crate) fn bind_sql_with_state(
    sql: &str,
    params: &[Value],
    dialect: SqlDialect,
    state: PlaceholderState,
) -> Result<BoundSql, LixError> {
    let bound = crate::sql::bind_sql_with_state(sql, params, dialect, state)?;
    Ok(BoundSql {
        sql: bound.sql,
        params: bound.params,
        state: bound.state,
    })
}

#[cfg(test)]
pub(crate) fn is_transaction_control_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. }
    )
}
