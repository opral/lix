use super::super::super::ast::nodes::Statement;

const TABLE_PATTERNS: &[&str] = &["lix_entity", "lix_entity_by_version", "lix_entity_history"];

pub(crate) fn matches(statement: &Statement) -> bool {
    statement_matches_any_table(statement, TABLE_PATTERNS)
}

fn statement_matches_any_table(statement: &Statement, patterns: &[&str]) -> bool {
    let sql = statement.to_string().to_ascii_lowercase();
    patterns.iter().any(|pattern| sql.contains(pattern))
}
