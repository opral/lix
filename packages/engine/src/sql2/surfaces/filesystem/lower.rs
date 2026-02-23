use super::super::super::ast::nodes::Statement;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoweringKind {
    Read,
    Write,
    Unknown,
}

pub(crate) fn lowering_kind(statement: &Statement) -> LoweringKind {
    let sql = statement.to_string().trim_start().to_ascii_lowercase();
    if sql.starts_with("select") || sql.starts_with("with") || sql.starts_with("explain") {
        LoweringKind::Read
    } else if sql.starts_with("insert") || sql.starts_with("update") || sql.starts_with("delete") {
        LoweringKind::Write
    } else {
        LoweringKind::Unknown
    }
}
