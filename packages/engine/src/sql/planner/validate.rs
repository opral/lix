use sqlparser::ast::Statement;

use crate::sql::types::{MutationRow, SchemaRegistration, UpdateValidationPlan};
use crate::sql::PostprocessPlan;
use crate::LixError;

pub(crate) fn ensure_single_statement_plan(statement_count: usize) -> Result<(), LixError> {
    if statement_count == 0 {
        return Err(LixError {
            message: "planner received empty statement block".to_string(),
        });
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
    if postprocess.is_some() && statements.len() != 1 {
        return Err(LixError {
            message: "postprocess rewrites require a single statement".to_string(),
        });
    }
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
