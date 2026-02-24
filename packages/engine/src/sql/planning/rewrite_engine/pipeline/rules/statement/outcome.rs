use sqlparser::ast::Statement;

use crate::engine::sql::planning::rewrite_engine::types::RewriteOutput;

pub(crate) enum StatementRuleOutcome {
    Continue(Statement),
    Emit(RewriteOutput),
    NoMatch,
}
