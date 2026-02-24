use super::super::ast::nodes::Statement;

pub(crate) fn statement_matches_any_table(statement: &Statement, patterns: &[&str]) -> bool {
    let sql = statement.to_string().to_ascii_lowercase();
    patterns.iter().any(|pattern| sql.contains(pattern))
}
