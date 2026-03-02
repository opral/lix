use crate::lix_table_registry::{columns_for_public_lix_table, public_lix_table_names};
use crate::{errors, LixError};
use sqlparser::ast::{visit_relations, ObjectNamePart, Statement};
use std::ops::ControlFlow;

pub(crate) fn normalize_sql_error(error: LixError, statements: &[Statement]) -> LixError {
    if let Some(missing_column) = parse_unknown_column_name(&error.description) {
        let relation_names = relation_names_from_statements(statements);
        let table_name = choose_table_for_unknown_column(&missing_column, &relation_names);
        let available_columns = table_name
            .as_deref()
            .and_then(columns_for_public_lix_table)
            .unwrap_or(&[]);
        return errors::sql_unknown_column_error(
            &missing_column,
            table_name.as_deref(),
            available_columns,
            parse_sql_offset(&error.description),
        );
    }

    if is_missing_relation_error(&error) {
        if let Some(table_name) = parse_unknown_table_name(&error.description).or_else(|| {
            relation_names_from_statements(statements)
                .into_iter()
                .next()
        }) {
            let available_tables = public_lix_table_names();
            return errors::sql_unknown_table_error(
                &table_name,
                available_tables.as_slice(),
                parse_sql_offset(&error.description),
            );
        }
        return errors::table_not_found_read_error();
    }

    error
}

pub(crate) fn is_missing_relation_error(err: &LixError) -> bool {
    if err.code == "LIX_ERROR_SQL_UNKNOWN_TABLE" || err.code == "LIX_ERROR_TABLE_NOT_FOUND" {
        return true;
    }
    let lower = err.description.to_lowercase();
    lower.contains("no such table")
        || lower.contains("relation")
            && (lower.contains("does not exist")
                || lower.contains("undefined table")
                || lower.contains("unknown"))
}

fn relation_names_from_statements(statements: &[Statement]) -> Vec<String> {
    let mut result = Vec::<String>::new();
    for statement in statements {
        let _ = visit_relations(statement, |relation| {
            if let Some(name) = relation
                .0
                .last()
                .and_then(ObjectNamePart::as_ident)
                .map(|ident| ident.value.clone())
            {
                let exists = result
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(&name));
                if !exists {
                    result.push(name);
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }
    result
}

fn choose_table_for_unknown_column(
    missing_column: &str,
    relation_names: &[String],
) -> Option<String> {
    if let Some((qualifier, _)) = missing_column.split_once('.') {
        for relation in relation_names {
            if relation.eq_ignore_ascii_case(qualifier) {
                return Some(relation.clone());
            }
        }
    }

    if relation_names.len() == 1 {
        return Some(relation_names[0].clone());
    }

    for relation in relation_names {
        if columns_for_public_lix_table(relation).is_some() {
            return Some(relation.clone());
        }
    }

    None
}

fn parse_unknown_column_name(description: &str) -> Option<String> {
    extract_name_after_prefix(description, "no such column:")
        .or_else(|| extract_name_between(description, "column \"", "\" does not exist"))
        .or_else(|| extract_name_between(description, "column '", "' does not exist"))
}

fn parse_unknown_table_name(description: &str) -> Option<String> {
    extract_name_after_prefix(description, "no such table:")
        .or_else(|| extract_name_between(description, "relation \"", "\" does not exist"))
        .or_else(|| extract_name_between(description, "relation '", "' does not exist"))
}

fn parse_sql_offset(description: &str) -> Option<usize> {
    let lower = description.to_ascii_lowercase();
    let marker = "at offset ";
    let index = lower.rfind(marker)?;
    let start = index + marker.len();
    let digits: String = lower[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        return None;
    }
    digits.parse::<usize>().ok()
}

fn extract_name_between(description: &str, start_marker: &str, end_marker: &str) -> Option<String> {
    let lower = description.to_ascii_lowercase();
    let start_marker_lower = start_marker.to_ascii_lowercase();
    let end_marker_lower = end_marker.to_ascii_lowercase();
    let start = lower.find(&start_marker_lower)? + start_marker_lower.len();
    let end = lower[start..].find(&end_marker_lower)? + start;
    sanitize_name(&description[start..end])
}

fn extract_name_after_prefix(description: &str, prefix: &str) -> Option<String> {
    let lower = description.to_ascii_lowercase();
    let marker = prefix.to_ascii_lowercase();
    let mut start = lower.find(&marker)? + marker.len();
    while description[start..]
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_whitespace())
    {
        start += 1;
    }
    let mut end = description.len();
    for stop in [' ', '\n', '\r', '\t', ',', ')', ';'] {
        if let Some(index) = description[start..].find(stop) {
            end = end.min(start + index);
        }
    }
    sanitize_name(&description[start..end])
}

fn sanitize_name(raw: &str) -> Option<String> {
    let trimmed = raw
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('\'');
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::{is_missing_relation_error, normalize_sql_error};
    use crate::LixError;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn classifies_missing_relation_messages() {
        assert!(is_missing_relation_error(&LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "no such table: foo".to_string(),
        }));
        assert!(is_missing_relation_error(&LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "ERROR: relation \"foo\" does not exist".to_string(),
        }));
        assert!(is_missing_relation_error(&LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "undefined table: relation foo".to_string(),
        }));
        assert!(!is_missing_relation_error(&LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "CHECK constraint failed".to_string(),
        }));
    }

    #[test]
    fn normalizes_unknown_column_with_table_hints() {
        let statements = Parser::parse_sql(
            &GenericDialect {},
            "SELECT file_id, schema_key, entity_id, status, plugin_key FROM lix_working_changes",
        )
        .expect("parse SQL");
        let error = normalize_sql_error(
            LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "no such column: plugin_key in SELECT ... at offset 47".to_string(),
            },
            &statements,
        );
        assert_eq!(error.code, "LIX_ERROR_SQL_UNKNOWN_COLUMN");
        assert!(error
            .description
            .contains("Column `plugin_key` does not exist on `lix_working_changes`."));
        assert!(error.description.contains("Available columns:"));
        assert!(error.description.contains("schema_key"));
    }

    #[test]
    fn normalizes_unknown_table_with_table_hints() {
        let statements =
            Parser::parse_sql(&GenericDialect {}, "SELECT * FROM lix_sate").expect("parse SQL");
        let error = normalize_sql_error(
            LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "no such table: lix_sate".to_string(),
            },
            &statements,
        );
        assert_eq!(error.code, "LIX_ERROR_SQL_UNKNOWN_TABLE");
        assert!(error
            .description
            .contains("Table `lix_sate` does not exist."));
        assert!(error.description.contains("lix_state"));
    }

    #[test]
    fn normalizes_unknown_column_for_lix_change_with_columns() {
        let statements = Parser::parse_sql(
            &GenericDialect {},
            "SELECT snapshot_json FROM lix_change WHERE id = 'c1'",
        )
        .expect("parse SQL");
        let error = normalize_sql_error(
            LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "no such column: snapshot_json".to_string(),
            },
            &statements,
        );
        assert_eq!(error.code, "LIX_ERROR_SQL_UNKNOWN_COLUMN");
        assert!(error
            .description
            .contains("Column `snapshot_json` does not exist on `lix_change`."));
        assert!(error
            .description
            .contains("Available columns: id, entity_id"));
    }
}
