#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatementRule {
    VtableWriteCanonical,
    Passthrough,
}

const STATEMENT_RULES: &[StatementRule] = &[
    StatementRule::VtableWriteCanonical,
    StatementRule::Passthrough,
];

pub(crate) fn statement_rules() -> &'static [StatementRule] {
    STATEMENT_RULES
}
