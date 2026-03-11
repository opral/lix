use sqlparser::ast::Statement;

use crate::sql::ast::{lowering, utils};
use crate::{LixError, SqlDialect};

pub(crate) fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, LixError> {
    utils::parse_sql_statements(sql)
}

pub(crate) fn lower_statement(
    statement: Statement,
    dialect: SqlDialect,
) -> Result<Statement, LixError> {
    lowering::lower_statement(statement, dialect)
}
