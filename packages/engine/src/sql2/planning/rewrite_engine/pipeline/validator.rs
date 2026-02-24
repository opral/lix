use std::collections::BTreeSet;

use sqlparser::ast::Query;

use crate::engine::sql2::planning::rewrite_engine::types::RewriteOutput;
use crate::engine::sql2::planning::rewrite_engine::PostprocessPlan;
use crate::engine::sql2::planning::rewrite_engine::{object_name_matches, visit_query_selects, visit_table_factors_in_select};
use crate::LixError;

use super::context::AnalysisContext;
use super::registry::RewritePhase;

const LOGICAL_READ_VIEW_NAMES: &[&str] = &[
    "lix_active_account",
    "lix_active_version",
    "lix_state",
    "lix_state_by_version",
    "lix_state_history",
    "lix_version",
    "lix_file",
    "lix_file_by_version",
    "lix_file_history",
    "lix_directory",
    "lix_directory_by_version",
    "lix_directory_history",
];

pub(crate) fn validate_final_read_query(query: &Query) -> Result<(), LixError> {
    validate_no_unresolved_logical_read_views(query)
}

pub(crate) fn validate_phase_invariants(
    phase: RewritePhase,
    query: &Query,
    _context: &AnalysisContext,
) -> Result<(), LixError> {
    match phase {
        RewritePhase::Analyze => Ok(()),
        RewritePhase::Canonicalize => validate_no_unresolved_logical_read_views(query),
        RewritePhase::Optimize => validate_no_unresolved_logical_read_views(query),
        // Lower can expand SQL substantially; final invariant check covers output.
        RewritePhase::Lower => validate_no_unresolved_logical_read_views(query),
    }
}

pub(crate) fn validate_statement_output(output: &RewriteOutput) -> Result<(), LixError> {
    if output.statements.is_empty() {
        return Err(LixError {
            message: "statement rewrite produced no statements".to_string(),
        });
    }
    if output.postprocess.is_some() && output.statements.len() != 1 {
        return Err(LixError {
            message: "postprocess rewrites require a single statement".to_string(),
        });
    }
    if output.postprocess.is_some() && !output.mutations.is_empty() {
        return Err(LixError {
            message: "postprocess rewrites cannot emit mutation rows".to_string(),
        });
    }
    if !output.update_validations.is_empty() && output.statements.len() != 1 {
        return Err(LixError {
            message: "update validation rewrites require a single statement".to_string(),
        });
    }
    if !output.mutations.is_empty() && !output.update_validations.is_empty() {
        return Err(LixError {
            message: "mutation rewrites cannot emit update validations".to_string(),
        });
    }
    if !output.update_validations.is_empty()
        && !matches!(output.statements[0], sqlparser::ast::Statement::Update(_))
    {
        return Err(LixError {
            message: "update validations require an UPDATE statement output".to_string(),
        });
    }
    if let Some(postprocess) = &output.postprocess {
        match postprocess {
            PostprocessPlan::VtableUpdate(_) => {
                if !matches!(output.statements[0], sqlparser::ast::Statement::Update(_)) {
                    return Err(LixError {
                        message: "vtable update postprocess requires an UPDATE statement"
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
                        message: "vtable delete postprocess requires an UPDATE or DELETE statement"
                            .to_string(),
                    });
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_no_unresolved_logical_read_views(query: &Query) -> Result<(), LixError> {
    validate_no_unresolved_logical_read_views_except(query, &[])
}

pub(crate) fn validate_no_unresolved_logical_read_views_except(
    query: &Query,
    allowed: &[&str],
) -> Result<(), LixError> {
    let allowed: BTreeSet<&str> = allowed.iter().copied().collect();
    let mut unresolved = BTreeSet::new();
    visit_query_selects(query, &mut |select| {
        visit_table_factors_in_select(select, &mut |relation| {
            let sqlparser::ast::TableFactor::Table { name, .. } = relation else {
                return Ok(());
            };
            for candidate in LOGICAL_READ_VIEW_NAMES {
                if object_name_matches(name, candidate) {
                    if allowed.contains(candidate) {
                        continue;
                    }
                    unresolved.insert((*candidate).to_string());
                }
            }
            Ok(())
        })
    })?;

    if unresolved.is_empty() {
        return Ok(());
    }

    Err(LixError {
        message: format!(
            "read rewrite left unresolved logical views: {}",
            unresolved.into_iter().collect::<Vec<_>>().join(", ")
        ),
    })
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use crate::engine::sql2::planning::rewrite_engine::pipeline::context::AnalysisContext;
    use crate::engine::sql2::planning::rewrite_engine::pipeline::registry::RewritePhase;
    use crate::engine::sql2::planning::rewrite_engine::types::{PostprocessPlan, RewriteOutput, VtableDeletePlan, VtableUpdatePlan};

    use super::{validate_phase_invariants, validate_statement_output};

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            other => panic!("expected query, got {other:?}"),
        }
    }

    fn empty_statement() -> Statement {
        Statement::Query(Box::new(parse_query("SELECT 1")))
    }

    #[test]
    fn canonical_phase_rejects_unresolved_state_views() {
        let query = parse_query("SELECT * FROM lix_state_by_version");
        let context = AnalysisContext::from_query(&query);
        let err = validate_phase_invariants(RewritePhase::Canonicalize, &query, &context)
            .expect_err("canonical phase should reject unresolved state views");
        assert!(err.message.contains("lix_state_by_version"));
    }

    #[test]
    fn optimize_phase_rejects_unresolved_state_views() {
        let query = parse_query("SELECT * FROM lix_state_by_version");
        let context = AnalysisContext::from_query(&query);
        let err = validate_phase_invariants(RewritePhase::Optimize, &query, &context)
            .expect_err("optimize phase should reject unresolved state views");
        assert!(err.message.contains("lix_state_by_version"));
    }

    #[test]
    fn statement_validator_rejects_postprocess_with_mutations() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableDelete(VtableDeletePlan {
                schema_key: "schema".to_string(),
                effective_scope_fallback: false,
                effective_scope_selection_sql: None,
            })),
            mutations: vec![crate::engine::sql2::planning::rewrite_engine::types::MutationRow {
                operation: crate::engine::sql2::planning::rewrite_engine::types::MutationOperation::Insert,
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
        assert!(err.message.contains("cannot emit mutation rows"));
    }

    #[test]
    fn statement_validator_rejects_multi_statement_update_validation() {
        let output = RewriteOutput {
            statements: vec![empty_statement(), empty_statement()],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: vec![crate::engine::sql2::planning::rewrite_engine::types::UpdateValidationPlan {
                table: "t".to_string(),
                where_clause: None,
                snapshot_content: None,
                snapshot_patch: None,
            }],
        };

        let err = validate_statement_output(&output)
            .expect_err("update validations with multiple statements should be rejected");
        assert!(err
            .message
            .contains("update validation rewrites require a single statement"));
    }

    #[test]
    fn statement_validator_rejects_update_validation_on_non_update_statement() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: vec![crate::engine::sql2::planning::rewrite_engine::types::UpdateValidationPlan {
                table: "t".to_string(),
                where_clause: None,
                snapshot_content: None,
                snapshot_patch: None,
            }],
        };

        let err = validate_statement_output(&output)
            .expect_err("update validation on query statement should be rejected");
        assert!(err
            .message
            .contains("update validations require an UPDATE statement output"));
    }

    #[test]
    fn statement_validator_rejects_vtable_update_postprocess_on_non_update_statement() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
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
            .message
            .contains("vtable update postprocess requires an UPDATE statement"));
    }

    #[test]
    fn statement_validator_rejects_vtable_delete_postprocess_on_non_delete_or_update_statement() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableDelete(VtableDeletePlan {
                schema_key: "schema".to_string(),
                effective_scope_fallback: false,
                effective_scope_selection_sql: None,
            })),
            mutations: Vec::new(),
            update_validations: Vec::new(),
        };

        let err = validate_statement_output(&output)
            .expect_err("vtable delete postprocess on query statement should be rejected");
        assert!(err
            .message
            .contains("vtable delete postprocess requires an UPDATE or DELETE statement"));
    }
}
