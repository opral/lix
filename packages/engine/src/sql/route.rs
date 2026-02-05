use sqlparser::ast::Statement;

use crate::sql::steps::{stored_schema, vtable_read, vtable_write};
use crate::sql::types::{RewriteOutput, SchemaRegistration};
use crate::LixError;

pub fn rewrite_statement(statement: Statement) -> Result<RewriteOutput, LixError> {
    match statement {
        Statement::Insert(insert) => {
            let mut current = Statement::Insert(insert);
            let mut registrations: Vec<SchemaRegistration> = Vec::new();
            let mut statements: Vec<Statement> = Vec::new();

            if let Statement::Insert(inner) = &current {
                if let Some(rewritten) = stored_schema::rewrite_insert(inner.clone())? {
                    registrations.push(rewritten.registration);
                    current = rewritten.statement;
                }
            }
            if let Statement::Insert(inner) = &current {
                if let Some(rewritten) = vtable_write::rewrite_insert(inner.clone())? {
                    registrations.extend(rewritten.registrations);
                    statements = rewritten.statements;
                }
            }

            if statements.is_empty() {
                statements.push(current);
            }

            Ok(RewriteOutput {
                statements,
                registrations,
            })
        }
        Statement::Update(update) => Ok(RewriteOutput {
            statements: vec![
                vtable_write::rewrite_update(update.clone())?.unwrap_or(Statement::Update(update))
            ],
            registrations: Vec::new(),
        }),
        Statement::Delete(delete) => Ok(RewriteOutput {
            statements: vec![
                vtable_write::rewrite_delete(delete.clone())?.unwrap_or(Statement::Delete(delete))
            ],
            registrations: Vec::new(),
        }),
        Statement::Query(query) => Ok(RewriteOutput {
            statements: vec![vtable_read::rewrite_query(*query.clone())?
                .map(|rewritten| Statement::Query(Box::new(rewritten)))
                .unwrap_or_else(|| Statement::Query(query))],
            registrations: Vec::new(),
        }),
        other => Ok(RewriteOutput {
            statements: vec![other],
            registrations: Vec::new(),
        }),
    }
}
