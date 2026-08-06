use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::Query;

use crate::LixError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundRead {
    pub(crate) query: Box<Query>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoundStatementRoute {
    Read,
    Write,
}

pub(crate) fn bind_statement_route(
    statement: &DataFusionStatement,
) -> Result<BoundStatementRoute, LixError> {
    match super::classify::classify_datafusion_statement(statement) {
        super::classify::SqlStatementKind::Read => Ok(BoundStatementRoute::Read),
        super::classify::SqlStatementKind::Write => Ok(BoundStatementRoute::Write),
        super::classify::SqlStatementKind::Other => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "SQL statement is not supported by Lix SQL",
        )),
    }
}

pub(crate) fn bind_read_statement(
    sql: &str,
    statement: &DataFusionStatement,
) -> Result<(), LixError> {
    validate_public_read_sql_surface(sql)?;
    if super::classify::classify_datafusion_statement(statement)
        == super::classify::SqlStatementKind::Write
    {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "SQL writes must use the bound write planning path",
        ));
    }
    super::classify::validate_supported_datafusion_statement_ast(statement)?;
    super::public_udf::validate_public_udf_calls_in_datafusion_statement(statement)?;
    Ok(())
}

fn validate_public_read_sql_surface(sql: &str) -> Result<(), LixError> {
    let normalized = sql.to_ascii_lowercase();
    if normalized.contains("lixcol_branch_id")
        && (normalized.contains("= lower(") || normalized.contains(" in (lower("))
    {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "public column 'lixcol_branch_id' must be compared directly to a literal or parameter",
        ));
    }
    Ok(())
}
