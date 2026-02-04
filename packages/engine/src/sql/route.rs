use sqlparser::ast::Statement;

use crate::sql::steps::untracked;
use crate::LixError;

pub fn rewrite_statement(statement: Statement) -> Result<Statement, LixError> {
    match statement {
        Statement::Insert(insert) => Ok(
            untracked::rewrite_insert(insert.clone())?.unwrap_or(Statement::Insert(insert)),
        ),
        Statement::Update(update) => Ok(
            untracked::rewrite_update(update.clone())?.unwrap_or(Statement::Update(update)),
        ),
        Statement::Delete(delete) => Ok(
            untracked::rewrite_delete(delete.clone())?.unwrap_or(Statement::Delete(delete)),
        ),
        Statement::Query(query) => Ok(untracked::rewrite_query(*query.clone())?
            .map(|rewritten| Statement::Query(Box::new(rewritten)))
            .unwrap_or_else(|| Statement::Query(query))),
        other => Ok(other),
    }
}
