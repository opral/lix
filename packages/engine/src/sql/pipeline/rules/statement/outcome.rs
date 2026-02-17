use sqlparser::ast::Statement;

use crate::sql::types::RewriteOutput;

pub(crate) enum StatementRuleOutcome {
    Continue(Statement),
    Emit(RewriteOutput),
    NoMatch,
}
