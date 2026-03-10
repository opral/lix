#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatementRule {
    QueryRead,
    ExplainRead,
    VtableWriteCanonical,
    Passthrough,
}

const STATEMENT_RULES: &[StatementRule] = &[
    StatementRule::QueryRead,
    StatementRule::ExplainRead,
    StatementRule::VtableWriteCanonical,
    StatementRule::Passthrough,
];

pub(crate) fn statement_rules() -> &'static [StatementRule] {
    STATEMENT_RULES
}
