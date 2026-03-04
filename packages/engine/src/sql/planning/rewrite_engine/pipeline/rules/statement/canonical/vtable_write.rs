use sqlparser::ast::{Delete, Insert, Statement, Update};

use crate::engine::sql::planning::rewrite_engine::steps::vtable_write;
use crate::engine::sql::planning::rewrite_engine::types::{PostprocessPlan, RewriteOutput};
use crate::engine::sql::planning::rewrite_engine::DetectedFileDomainChange;
use crate::functions::LixFunctionProvider;
use crate::{LixBackend, LixError, Value};

pub(crate) fn rewrite_insert_with_writer_key<P: LixFunctionProvider>(
    insert: Insert,
    params: &[Value],
    writer_key: Option<&str>,
    functions: &mut P,
) -> Result<Option<vtable_write::VtableWriteRewrite>, LixError> {
    vtable_write::rewrite_insert_with_writer_key(insert, params, writer_key, functions)
}

pub(crate) async fn rewrite_insert_with_backend<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    insert: Insert,
    params: &[Value],
    generated_param_offset: usize,
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut P,
) -> Result<Option<vtable_write::VtableWriteRewrite>, LixError> {
    vtable_write::rewrite_insert_with_backend(
        backend,
        insert,
        params,
        generated_param_offset,
        detected_file_domain_changes,
        writer_key,
        functions,
    )
    .await
}

pub(crate) fn rewrite_update(update: Update, params: &[Value]) -> Result<RewriteOutput, LixError> {
    let rewritten = vtable_write::rewrite_update(update.clone(), params)?;
    match rewritten {
        Some(vtable_write::UpdateRewrite::Statement(rewrite)) => Ok(RewriteOutput {
            statements: vec![rewrite.statement],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: rewrite.validation.into_iter().collect(),
        }),
        Some(vtable_write::UpdateRewrite::Planned(rewrite)) => {
            let mut statements = rewrite.pre_statements;
            statements.push(rewrite.statement);
            Ok(RewriteOutput {
                statements,
                params: Vec::new(),
                registrations: Vec::new(),
                postprocess: Some(PostprocessPlan::VtableUpdate(rewrite.plan)),
                mutations: Vec::new(),
                update_validations: rewrite.validations,
            })
        }
        None => {
            let target = update_target_name(&update);
            if is_allowed_internal_write_target(&target) {
                Ok(RewriteOutput {
                    statements: vec![Statement::Update(update)],
                    params: Vec::new(),
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                })
            } else {
                Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                        "strict rewrite violation: statement routing: unsupported UPDATE target '{}'",
                        target
                    ),
                })
            }
        }
    }
}

pub(crate) fn rewrite_delete(
    delete: Delete,
    effective_scope_fallback: bool,
    params: &[Value],
) -> Result<RewriteOutput, LixError> {
    let rewritten = if effective_scope_fallback {
        vtable_write::rewrite_delete_with_options(delete.clone(), true, params)?
    } else {
        vtable_write::rewrite_delete(delete.clone(), params)?
    };

    match rewritten {
        Some(vtable_write::DeleteRewrite::Statement(statement)) => Ok(RewriteOutput {
            statements: vec![statement],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: Vec::new(),
        }),
        Some(vtable_write::DeleteRewrite::Planned(rewrite)) => Ok(RewriteOutput {
            statements: vec![rewrite.statement],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableDelete(rewrite.plan)),
            mutations: Vec::new(),
            update_validations: Vec::new(),
        }),
        None => {
            let target = delete_target_name(&delete);
            if is_allowed_internal_write_target(&target) {
                Ok(RewriteOutput {
                    statements: vec![Statement::Delete(delete)],
                    params: Vec::new(),
                    registrations: Vec::new(),
                    postprocess: None,
                    mutations: Vec::new(),
                    update_validations: Vec::new(),
                })
            } else {
                Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                        "strict rewrite violation: statement routing: unsupported DELETE target '{}'",
                        target
                    ),
                })
            }
        }
    }
}

fn update_target_name(update: &Update) -> String {
    match &update.table.relation {
        sqlparser::ast::TableFactor::Table { name, .. } => name.to_string(),
        _ => "<non-table-target>".to_string(),
    }
}

fn delete_target_name(delete: &Delete) -> String {
    let tables = match &delete.from {
        sqlparser::ast::FromTable::WithFromKeyword(tables)
        | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
    };
    tables
        .first()
        .map(|table| match &table.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => name.to_string(),
            _ => "<non-table-target>".to_string(),
        })
        .unwrap_or_else(|| "<missing-target>".to_string())
}

fn is_allowed_internal_write_target(target: &str) -> bool {
    let normalized = target.trim_matches('"').to_ascii_lowercase();
    normalized.starts_with("lix_internal_")
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use crate::engine::sql::planning::rewrite_engine::types::PostprocessPlan;

    use super::rewrite_update;

    #[test]
    fn rewrite_update_effective_scope_keeps_vtable_update_postprocess_with_two_statements() {
        let sql = r#"UPDATE lix_internal_state_vtable
            SET snapshot_content = '{"key":"value"}'
            WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        let update = match statements.remove(0) {
            Statement::Update(update) => update,
            other => panic!("expected update, got {other:?}"),
        };

        let output = rewrite_update(update, &[]).expect("rewrite should succeed");
        assert_eq!(output.statements.len(), 2);
        assert!(matches!(
            output.postprocess,
            Some(PostprocessPlan::VtableUpdate(_))
        ));
    }
}
