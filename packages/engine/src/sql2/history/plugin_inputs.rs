use super::super::ast::nodes::Statement;
use crate::sql::FileReadMaterializationScope;

pub(crate) fn file_read_materialization_scope_for_statements(
    statements: &[Statement],
) -> Option<FileReadMaterializationScope> {
    crate::sql::file_read_materialization_scope_for_statements(statements)
}

pub(crate) fn file_history_read_materialization_required_for_statements(
    statements: &[Statement],
) -> bool {
    crate::sql::file_history_read_materialization_required_for_statements(statements)
}
