use super::super::matcher::statement_matches_any_table;
use sqlparser::ast::Statement;

const TABLE_PATTERNS: &[&str] = &["lix_entity", "lix_entity_by_version", "lix_entity_history"];

pub(crate) fn matches(statement: &Statement) -> bool {
    statement_matches_any_table(statement, TABLE_PATTERNS)
}
