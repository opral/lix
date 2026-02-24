use sqlparser::ast::Statement;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoweringKind {
    Read,
    Write,
    Unknown,
}

pub(crate) fn lowering_kind(statement: &Statement) -> LoweringKind {
    match statement {
        Statement::Query(_) | Statement::Explain { .. } | Statement::ExplainTable { .. } => {
            LoweringKind::Read
        }
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => LoweringKind::Write,
        _ => LoweringKind::Unknown,
    }
}
