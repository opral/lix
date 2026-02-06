use sqlparser::ast::Statement;

use crate::functions::LixFunctionProvider;
use crate::sql::steps::{stored_schema, vtable_read, vtable_write};
use crate::sql::types::{
    MutationRow, PostprocessPlan, RewriteOutput, SchemaRegistration, UpdateValidationPlan,
};
use crate::{LixError, Value};

pub fn rewrite_statement<P: LixFunctionProvider>(
    statement: Statement,
    params: &[Value],
    functions: &mut P,
) -> Result<RewriteOutput, LixError> {
    match statement {
        Statement::Insert(insert) => {
            let mut current = Statement::Insert(insert);
            let mut registrations: Vec<SchemaRegistration> = Vec::new();
            let mut statements: Vec<Statement> = Vec::new();
            let mut mutations: Vec<MutationRow> = Vec::new();
            let update_validations: Vec<UpdateValidationPlan> = Vec::new();

            if let Statement::Insert(inner) = &current {
                if let Some(rewritten) = stored_schema::rewrite_insert(inner.clone(), params)? {
                    registrations.push(rewritten.registration);
                    mutations.push(rewritten.mutation);
                    current = rewritten.statement;
                }
            }
            if let Statement::Insert(inner) = &current {
                if let Some(rewritten) =
                    vtable_write::rewrite_insert(inner.clone(), params, functions)?
                {
                    registrations.extend(rewritten.registrations);
                    statements = rewritten.statements;
                    mutations = rewritten.mutations;
                }
            }

            if statements.is_empty() {
                statements.push(current);
            }

            Ok(RewriteOutput {
                statements,
                registrations,
                postprocess: None,
                mutations,
                update_validations,
            })
        }
        Statement::Update(update) => {
            let rewritten = vtable_write::rewrite_update(update.clone(), params)?;
            match rewritten {
                Some(vtable_write::UpdateRewrite::Statement(rewrite)) => Ok(RewriteOutput {
                    statements: vec![rewrite.statement],
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: rewrite.validation.into_iter().collect(),
                }),
                Some(vtable_write::UpdateRewrite::Planned(rewrite)) => Ok(RewriteOutput {
                    statements: vec![rewrite.statement],
                    registrations: Vec::new(),
                    postprocess: Some(PostprocessPlan::VtableUpdate(rewrite.plan)),
                    mutations: Vec::new(),
                    update_validations: rewrite.validation.into_iter().collect(),
                }),
                None => Ok(RewriteOutput {
                    statements: vec![Statement::Update(update)],
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                }),
            }
        }
        Statement::Delete(delete) => {
            let rewritten = vtable_write::rewrite_delete(delete.clone())?;
            match rewritten {
                Some(vtable_write::DeleteRewrite::Statement(statement)) => Ok(RewriteOutput {
                    statements: vec![statement],
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                }),
                Some(vtable_write::DeleteRewrite::Planned(rewrite)) => Ok(RewriteOutput {
                    statements: vec![rewrite.statement],
                    registrations: Vec::new(),
                    postprocess: Some(PostprocessPlan::VtableDelete(rewrite.plan)),
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                }),
                None => Ok(RewriteOutput {
                    statements: vec![Statement::Delete(delete)],
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                }),
            }
        }
        Statement::Query(query) => Ok(RewriteOutput {
            statements: vec![vtable_read::rewrite_query(*query.clone())?
                .map(|rewritten| Statement::Query(Box::new(rewritten)))
                .unwrap_or_else(|| Statement::Query(query))],
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: Vec::new(),
        }),
        other => Ok(RewriteOutput {
            statements: vec![other],
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: Vec::new(),
        }),
    }
}
