use super::super::super::ast::nodes::Statement;
use super::predicates::statement_targets_internal_state_vtable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VtableOperation {
    Read,
    Write,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VtableCapabilities {
    pub(crate) supports_read: bool,
    pub(crate) supports_write: bool,
    pub(crate) requires_schema_key_predicate_for_mutations: bool,
    pub(crate) requires_single_statement_postprocess: bool,
}

pub(crate) fn internal_state_vtable_capabilities() -> VtableCapabilities {
    VtableCapabilities {
        supports_read: true,
        supports_write: true,
        requires_schema_key_predicate_for_mutations: true,
        requires_single_statement_postprocess: true,
    }
}

pub(crate) fn detect_operation(statement: &Statement) -> VtableOperation {
    if !statement_targets_internal_state_vtable(statement) {
        return VtableOperation::Unknown;
    }
    match statement {
        Statement::Query(_) => VtableOperation::Read,
        Statement::Explain {
            statement: inner, ..
        } => detect_operation(inner),
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
            VtableOperation::Write
        }
        _ => VtableOperation::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::{detect_operation, VtableOperation};

    #[test]
    fn detects_read_operation_from_ast() {
        let statements = Parser::parse_sql(
            &GenericDialect {},
            "SELECT * FROM lix_internal_state_vtable WHERE schema_key = 'x'",
        )
        .expect("parse SQL");
        assert_eq!(detect_operation(&statements[0]), VtableOperation::Read);
    }

    #[test]
    fn detects_write_operation_from_ast() {
        let statements = Parser::parse_sql(
            &GenericDialect {},
            "UPDATE lix_internal_state_vtable SET snapshot_content = '{}' WHERE schema_key = 'x'",
        )
        .expect("parse SQL");
        assert_eq!(detect_operation(&statements[0]), VtableOperation::Write);
    }
}
