use std::collections::BTreeSet;

use sqlparser::ast::Statement;

use crate::sql::types::{MutationRow, SchemaRegistration, UpdateValidationPlan};
use crate::sql::PostprocessPlan;
use crate::LixError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PostprocessSingleStatementContext {
    RewriteOutput,
    CompilePlan,
    PreprocessOutput,
}

impl PostprocessSingleStatementContext {
    fn label(self) -> &'static str {
        match self {
            Self::RewriteOutput => "rewrite output",
            Self::CompilePlan => "compile plan",
            Self::PreprocessOutput => "preprocess output",
        }
    }
}

pub(crate) fn ensure_single_statement_plan(statement_count: usize) -> Result<(), LixError> {
    if statement_count == 0 {
        return Err(LixError {
            message: "planner received empty statement block".to_string(),
        });
    }
    if statement_count > 1 {
        return Err(LixError {
            message: format!(
                "planner expected a single statement after canonicalization, got {statement_count}"
            ),
        });
    }
    Ok(())
}

pub(crate) fn ensure_postprocess_single_statement(
    postprocess_present: bool,
    statement_count: usize,
    context: PostprocessSingleStatementContext,
) -> Result<(), LixError> {
    if !postprocess_present || statement_count == 1 {
        return Ok(());
    }
    Err(LixError {
        message: format!(
            "postprocess single-statement invariant violated in {}: expected 1 statement, got {statement_count}",
            context.label()
        ),
    })
}

fn ensure_unique_mutation_row_identities(mutations: &[MutationRow]) -> Result<(), LixError> {
    let mut seen = BTreeSet::new();
    for mutation in mutations {
        let identity = format!(
            "{}|{}|{}|{}|{}|{}|{}",
            mutation.entity_id,
            mutation.schema_key,
            mutation.schema_version,
            mutation.file_id,
            mutation.version_id,
            mutation.plugin_key,
            mutation.untracked
        );
        if !seen.insert(identity) {
            return Err(LixError {
                message: "mutation rewrite emitted duplicate row identity".to_string(),
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_statement_output_parts(
    statements: &[Statement],
    registrations: &[SchemaRegistration],
    postprocess: Option<&PostprocessPlan>,
    mutations: &[MutationRow],
    update_validations: &[UpdateValidationPlan],
) -> Result<(), LixError> {
    if statements.is_empty() {
        return Err(LixError {
            message: "statement rewrite produced no statements".to_string(),
        });
    }
    ensure_postprocess_single_statement(
        postprocess.is_some(),
        statements.len(),
        PostprocessSingleStatementContext::RewriteOutput,
    )?;
    if postprocess.is_some() && !mutations.is_empty() {
        return Err(LixError {
            message: "postprocess rewrites cannot emit mutation rows".to_string(),
        });
    }
    if !update_validations.is_empty() && statements.len() != 1 {
        return Err(LixError {
            message: "update validation rewrites require a single statement".to_string(),
        });
    }
    if !mutations.is_empty() && !update_validations.is_empty() {
        return Err(LixError {
            message: "mutation rewrites cannot emit update validations".to_string(),
        });
    }
    if !update_validations.is_empty() && !matches!(statements[0], Statement::Update(_)) {
        return Err(LixError {
            message: "update validations require an UPDATE statement output".to_string(),
        });
    }
    for registration in registrations {
        if registration.schema_key.trim().is_empty() {
            return Err(LixError {
                message: "schema registration cannot have an empty schema_key".to_string(),
            });
        }
    }
    for mutation in mutations {
        validate_non_empty_field("mutation entity_id", &mutation.entity_id)?;
        validate_non_empty_field("mutation schema_key", &mutation.schema_key)?;
        validate_non_empty_field("mutation schema_version", &mutation.schema_version)?;
        validate_non_empty_field("mutation file_id", &mutation.file_id)?;
        validate_non_empty_field("mutation version_id", &mutation.version_id)?;
        validate_non_empty_field("mutation plugin_key", &mutation.plugin_key)?;
    }
    ensure_unique_mutation_row_identities(mutations)?;
    for validation in update_validations {
        validate_non_empty_field("update validation table", &validation.table)?;
        if validation.snapshot_content.is_some() && validation.snapshot_patch.is_some() {
            return Err(LixError {
                message:
                    "update validations cannot define both snapshot_content and snapshot_patch"
                        .to_string(),
            });
        }
    }
    if let Some(postprocess) = postprocess {
        match postprocess {
            PostprocessPlan::VtableUpdate(plan) => {
                validate_non_empty_field("vtable update schema_key", &plan.schema_key)?;
                if !matches!(statements[0], Statement::Update(_)) {
                    return Err(LixError {
                        message: "vtable update postprocess requires an UPDATE statement"
                            .to_string(),
                    });
                }
            }
            PostprocessPlan::VtableDelete(plan) => {
                validate_non_empty_field("vtable delete schema_key", &plan.schema_key)?;
                if !plan.effective_scope_fallback && plan.effective_scope_selection_sql.is_some() {
                    return Err(LixError {
                        message: "vtable delete postprocess cannot emit effective scope selection SQL without fallback"
                            .to_string(),
                    });
                }
                if !matches!(statements[0], Statement::Update(_) | Statement::Delete(_)) {
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

fn validate_non_empty_field(field: &str, value: &str) -> Result<(), LixError> {
    if value.trim().is_empty() {
        return Err(LixError {
            message: format!("{field} cannot be empty"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ensure_postprocess_single_statement, validate_statement_output_parts,
        PostprocessSingleStatementContext,
    };
    use crate::sql::parse_sql_statements_with_dialect;
    use crate::sql::types::{MutationRow, VtableDeletePlan, VtableUpdatePlan};
    use crate::sql::PostprocessPlan;
    use crate::{LixError, SqlDialect};
    use sqlparser::ast::Statement;

    fn statement_from_sql(sql: &str) -> Statement {
        let mut statements =
            parse_sql_statements_with_dialect(sql, SqlDialect::Sqlite).expect("parse statement");
        assert_eq!(statements.len(), 1);
        statements.remove(0)
    }

    fn mutation_row() -> MutationRow {
        MutationRow {
            entity_id: "entity".to_string(),
            schema_key: "schema".to_string(),
            schema_version: "1".to_string(),
            file_id: "file".to_string(),
            version_id: "global".to_string(),
            plugin_key: "plugin".to_string(),
            snapshot_content: None,
            untracked: false,
        }
    }

    #[test]
    fn postprocess_single_statement_invariant_rejects_multi_statement_plan() {
        let result = ensure_postprocess_single_statement(
            true,
            2,
            PostprocessSingleStatementContext::CompilePlan,
        );
        assert!(matches!(result, Err(LixError { message }) if message.contains("single-statement invariant")));
    }

    #[test]
    fn postprocess_invariant_rejects_mutations_when_postprocess_present() {
        let statements = vec![statement_from_sql("UPDATE t SET x = 1")];
        let postprocess = PostprocessPlan::VtableUpdate(VtableUpdatePlan {
            schema_key: "schema".to_string(),
            effective_scope_fallback: false,
            effective_scope_selection_sql: None,
            explicit_writer_key: None,
            writer_key_assignment_present: false,
            file_data_assignment: None,
        });
        let mutations = vec![mutation_row()];

        let result =
            validate_statement_output_parts(&statements, &[], Some(&postprocess), &mutations, &[]);
        assert!(matches!(result, Err(LixError { message }) if message.contains("cannot emit mutation rows")));
    }

    #[test]
    fn postprocess_invariant_rejects_delete_scope_selection_without_fallback() {
        let statements = vec![statement_from_sql("DELETE FROM t")];
        let postprocess = PostprocessPlan::VtableDelete(VtableDeletePlan {
            schema_key: "schema".to_string(),
            effective_scope_fallback: false,
            effective_scope_selection_sql: Some("SELECT 1".to_string()),
        });

        let result = validate_statement_output_parts(&statements, &[], Some(&postprocess), &[], &[]);
        assert!(matches!(result, Err(LixError { message }) if message.contains("effective scope selection SQL without fallback")));
    }
}
