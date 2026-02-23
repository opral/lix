use super::super::super::ast::nodes::Statement;

use super::capabilities::{detect_operation, internal_state_vtable_capabilities, VtableOperation};
use super::predicates::{
    statement_has_schema_key_predicate, statement_targets_internal_state_vtable,
};

pub(crate) fn supports_internal_state_vtable_write(statement: &Statement) -> bool {
    if !statement_targets_internal_state_vtable(statement) {
        return false;
    }
    let capabilities = internal_state_vtable_capabilities();
    if !capabilities.supports_write || detect_operation(statement) != VtableOperation::Write {
        return false;
    }
    if capabilities.requires_schema_key_predicate_for_mutations
        && !matches!(statement, Statement::Insert(_))
    {
        return statement_has_schema_key_predicate(statement);
    }
    true
}
