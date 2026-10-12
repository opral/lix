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
    // EXPLAIN reads only when what it explains reads: EXPLAIN ANALYZE runs
    // its statement, and a checkpoint function hides a write in a SELECT.
    if let Some(explained) = explained_statement(statement) {
        if bind_statement_route(&explained)? == BoundStatementRoute::Write {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "EXPLAIN of write statements is not supported by Lix SQL",
            ));
        }
        return Ok(BoundStatementRoute::Read);
    }
    if super::super::checkpoint_function_plan(statement)?.is_some() {
        return Ok(BoundStatementRoute::Write);
    }
    match super::classify::classify_datafusion_statement(statement) {
        super::classify::SqlStatementKind::Read => Ok(BoundStatementRoute::Read),
        super::classify::SqlStatementKind::Write => Ok(BoundStatementRoute::Write),
        super::classify::SqlStatementKind::Other => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "SQL statement is not supported by Lix SQL",
        )
        .with_hint(
            "Lix runs SELECT, INSERT, UPDATE and DELETE. Register schemas instead of CREATE \
             TABLE, and use transactions instead of BEGIN and SET.",
        )),
    }
}

/// The statement an `EXPLAIN` wraps, in either parser form.
fn explained_statement(statement: &DataFusionStatement) -> Option<DataFusionStatement> {
    use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
    match statement {
        DataFusionStatement::Explain(explain) => Some(explain.statement.as_ref().clone()),
        DataFusionStatement::Statement(statement) => match statement.as_ref() {
            SqlStatement::Explain { statement, .. } => Some(DataFusionStatement::Statement(
                Box::new(statement.as_ref().clone()),
            )),
            _ => None,
        },
        _ => None,
    }
}

pub(crate) fn bind_read_statement(
    _sql: &str,
    statement: &DataFusionStatement,
) -> Result<(), LixError> {
    if bind_statement_route(statement)? == BoundStatementRoute::Write {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "SQL writes must use the bound write planning path",
        ));
    }
    super::classify::validate_supported_datafusion_statement_ast(statement)?;
    super::public_udf::validate_public_udf_calls_in_datafusion_statement(statement)?;
    Ok(())
}
