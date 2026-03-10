use crate::engine::sql::planning::rewrite_engine::{PostprocessPlan, RewriteOutput};
use crate::LixError;

pub(crate) fn validate_statement_output(output: &RewriteOutput) -> Result<(), LixError> {
    if output.statements.is_empty()
        && !(output.effect_only
            && output.postprocess.is_none()
            && output.mutations.is_empty()
            && output.update_validations.is_empty())
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "statement rewrite produced no statements".to_string(),
        });
    }
    if requires_single_statement_postprocess(output.postprocess.as_ref())
        && output.statements.len() != 1
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "postprocess rewrites require a single statement".to_string(),
        });
    }
    if output.postprocess.is_some() && !output.mutations.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "postprocess rewrites cannot emit mutation rows".to_string(),
        });
    }
    if !output.mutations.is_empty() && !output.update_validations.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "mutation rewrites cannot emit update validations".to_string(),
        });
    }
    if !output.update_validations.is_empty()
        && !output
            .statements
            .iter()
            .all(|statement| matches!(statement, sqlparser::ast::Statement::Update(_)))
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "update validations require an UPDATE statement output".to_string(),
        });
    }
    if let Some(postprocess) = &output.postprocess {
        match postprocess {
            PostprocessPlan::VtableUpdate(_) => {
                if !matches!(
                    output.statements.last(),
                    Some(sqlparser::ast::Statement::Update(_))
                ) {
                    return Err(LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: "vtable update postprocess requires an UPDATE statement"
                            .to_string(),
                    });
                }
            }
            PostprocessPlan::VtableDelete(_) => {
                if !matches!(
                    output.statements[0],
                    sqlparser::ast::Statement::Update(_) | sqlparser::ast::Statement::Delete(_)
                ) {
                    return Err(LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description:
                            "vtable delete postprocess requires an UPDATE or DELETE statement"
                                .to_string(),
                    });
                }
            }
        }
    }
    Ok(())
}

fn requires_single_statement_postprocess(plan: Option<&PostprocessPlan>) -> bool {
    matches!(plan, Some(PostprocessPlan::VtableDelete(_)))
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use crate::engine::sql::planning::rewrite_engine::{
        PostprocessPlan, RewriteOutput, VtableDeletePlan, VtableUpdatePlan,
    };

    use super::validate_statement_output;

    fn empty_statement() -> Statement {
        let mut statements = Parser::parse_sql(&GenericDialect {}, "SELECT 1").expect("parse SQL");
        statements.remove(0)
    }

    fn empty_update_statement() -> Statement {
        let mut statements = Parser::parse_sql(
            &GenericDialect {},
            "UPDATE lix_internal_state_vtable SET updated_at = updated_at WHERE schema_key = 'schema'",
        )
        .expect("parse SQL");
        statements.remove(0)
    }

    #[test]
    fn statement_validator_rejects_postprocess_with_mutations() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableDelete(VtableDeletePlan {
                schema_key: "schema".to_string(),
                effective_scope_fallback: false,
                effective_scope_selection_sql: None,
                effective_scope_untracked_selection_sql: None,
            })),
            mutations: vec![crate::engine::sql::planning::rewrite_engine::MutationRow {
                operation: crate::engine::sql::planning::rewrite_engine::MutationOperation::Insert,
                entity_id: "e".to_string(),
                schema_key: "s".to_string(),
                schema_version: "1".to_string(),
                file_id: "f".to_string(),
                version_id: "v".to_string(),
                plugin_key: "p".to_string(),
                snapshot_content: None,
                untracked: false,
            }],
            update_validations: Vec::new(),
        };

        let err = validate_statement_output(&output)
            .expect_err("postprocess output with mutations should be rejected");
        assert!(err.description.contains("cannot emit mutation rows"));
    }

    #[test]
    fn statement_validator_rejects_update_validation_with_non_update_statement() {
        let output = RewriteOutput {
            statements: vec![empty_statement(), empty_update_statement()],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: vec![
                crate::engine::sql::planning::rewrite_engine::UpdateValidationPlan {
                    table: "t".to_string(),
                    where_clause: None,
                    snapshot_content: None,
                    snapshot_patch: None,
                },
            ],
        };

        let err = validate_statement_output(&output)
            .expect_err("update validations with non-update statement should be rejected");
        assert!(err
            .description
            .contains("update validations require an UPDATE statement output"));
    }

    #[test]
    fn statement_validator_rejects_update_validation_on_non_update_statement() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: vec![
                crate::engine::sql::planning::rewrite_engine::UpdateValidationPlan {
                    table: "t".to_string(),
                    where_clause: None,
                    snapshot_content: None,
                    snapshot_patch: None,
                },
            ],
        };

        let err = validate_statement_output(&output)
            .expect_err("update validation on query statement should be rejected");
        assert!(err
            .description
            .contains("update validations require an UPDATE statement output"));
    }

    #[test]
    fn statement_validator_rejects_vtable_update_postprocess_on_non_update_statement() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableUpdate(VtableUpdatePlan {
                schema_key: "schema".to_string(),
                explicit_writer_key: None,
                writer_key_assignment_present: false,
            })),
            mutations: Vec::new(),
            update_validations: Vec::new(),
        };

        let err = validate_statement_output(&output)
            .expect_err("vtable update postprocess on query statement should be rejected");
        assert!(err
            .description
            .contains("vtable update postprocess requires an UPDATE statement"));
    }

    #[test]
    fn statement_validator_allows_multi_statement_vtable_update_postprocess() {
        let output = RewriteOutput {
            statements: vec![empty_update_statement(), empty_update_statement()],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableUpdate(VtableUpdatePlan {
                schema_key: "schema".to_string(),
                explicit_writer_key: None,
                writer_key_assignment_present: false,
            })),
            mutations: Vec::new(),
            update_validations: Vec::new(),
        };

        validate_statement_output(&output)
            .expect("multi-statement vtable update postprocess should be valid");
    }

    #[test]
    fn statement_validator_rejects_vtable_delete_postprocess_on_non_delete_or_update_statement() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableDelete(VtableDeletePlan {
                schema_key: "schema".to_string(),
                effective_scope_fallback: false,
                effective_scope_selection_sql: None,
                effective_scope_untracked_selection_sql: None,
            })),
            mutations: Vec::new(),
            update_validations: Vec::new(),
        };

        let err = validate_statement_output(&output)
            .expect_err("vtable delete postprocess on query statement should be rejected");
        assert!(err
            .description
            .contains("vtable delete postprocess requires an UPDATE or DELETE statement"));
    }

    #[test]
    fn statement_validator_allows_effect_only_output_without_sql_statements() {
        let output = RewriteOutput {
            statements: Vec::new(),
            effect_only: true,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: Vec::new(),
        };

        validate_statement_output(&output)
            .expect("effect-only output without SQL statements should be valid");
    }
}
