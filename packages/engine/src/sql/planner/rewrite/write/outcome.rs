use sqlparser::ast::Statement;

use super::types::WriteRewriteOutput;

pub(crate) enum StatementRuleOutcome {
    Continue(Statement),
    Emit(WriteRewriteOutput),
    NoMatch,
}
