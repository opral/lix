use sqlparser::ast::Statement;

use super::capabilities::{detect_operation, internal_state_vtable_capabilities, VtableOperation};
use super::predicates::statement_targets_internal_state_vtable;

pub(crate) fn supports_internal_state_vtable_read(statement: &Statement) -> bool {
    if !statement_targets_internal_state_vtable(statement) {
        return false;
    }
    let capabilities = internal_state_vtable_capabilities();
    capabilities.supports_read && detect_operation(statement) == VtableOperation::Read
}
