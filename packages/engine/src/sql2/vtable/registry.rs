use crate::LixError;

use super::internal_state_vtable::capabilities::{
    VtableCapabilities, internal_state_vtable_capabilities,
};
use super::internal_state_vtable::predicates::{
    schema_key_is_valid, statement_targets_internal_state_vtable,
};
use super::super::ast::nodes::Statement;
use super::super::contracts::postprocess_actions::PostprocessPlan;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegisteredVtable {
    InternalStateVtable,
}

pub(crate) fn detect_registered_vtable(statement: &Statement) -> Option<RegisteredVtable> {
    if statement_targets_internal_state_vtable(statement) {
        return Some(RegisteredVtable::InternalStateVtable);
    }
    None
}

pub(crate) fn capabilities_for_statement(statement: &Statement) -> Option<VtableCapabilities> {
    detect_registered_vtable(statement).map(|registered| match registered {
        RegisteredVtable::InternalStateVtable => internal_state_vtable_capabilities(),
    })
}

pub(crate) fn validate_postprocess_plan(plan: &PostprocessPlan) -> Result<(), LixError> {
    let capabilities = internal_state_vtable_capabilities();
    if !capabilities.requires_single_statement_postprocess {
        return Ok(());
    }
    let schema_key = match plan {
        PostprocessPlan::VtableUpdate(update) => &update.schema_key,
        PostprocessPlan::VtableDelete(delete) => &delete.schema_key,
    };
    if !schema_key_is_valid(schema_key) {
        return Err(LixError {
            message: "vtable postprocess plan requires a valid schema_key".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::super::super::contracts::postprocess_actions::{PostprocessPlan, VtableUpdatePlan};

    use super::{detect_registered_vtable, validate_postprocess_plan};

    #[test]
    fn detects_internal_state_vtable_statement() {
        let statements = Parser::parse_sql(
            &GenericDialect {},
            "SELECT * FROM lix_internal_state_vtable WHERE schema_key = 'x'",
        )
        .expect("parse SQL");
        assert!(detect_registered_vtable(&statements[0]).is_some());
    }

    #[test]
    fn rejects_invalid_postprocess_schema_key() {
        let plan = PostprocessPlan::VtableUpdate(VtableUpdatePlan {
            schema_key: "bad key".to_string(),
            explicit_writer_key: None,
            writer_key_assignment_present: false,
        });
        let err = validate_postprocess_plan(&plan).expect_err("invalid plan");
        assert!(err.message.contains("schema_key"));
    }
}
