use std::collections::HashMap;
use std::ops::ControlFlow;

use sqlparser::ast::{Statement, Value as SqlValue};
use sqlparser::ast::{VisitMut, VisitorMut};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::backend::SqlDialect;
use crate::{LixError, Value};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PlaceholderState {
    next_ordinal: usize,
}

impl PlaceholderState {
    pub(crate) fn new() -> Self {
        Self { next_ordinal: 0 }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BoundSql {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
    pub(crate) state: PlaceholderState,
}

pub(crate) fn bind_sql(
    sql: &str,
    params: &[Value],
    dialect: SqlDialect,
) -> Result<BoundSql, LixError> {
    bind_sql_with_state(sql, params, dialect, PlaceholderState::new())
}

pub(crate) fn bind_sql_with_state(
    sql: &str,
    params: &[Value],
    dialect: SqlDialect,
    mut state: PlaceholderState,
) -> Result<BoundSql, LixError> {
    let mut statements = parse_sql_statements(sql)?;
    let mut used_source_indices = Vec::new();
    let mut source_to_dense: HashMap<usize, usize> = HashMap::new();

    for statement in &mut statements {
        let mut visitor = PlaceholderBinder {
            params_len: params.len(),
            dialect,
            state: &mut state,
            source_to_dense: &mut source_to_dense,
            used_source_indices: &mut used_source_indices,
        };
        if let ControlFlow::Break(error) = statement.visit(&mut visitor) {
            return Err(error);
        }
    }

    let bound_params = used_source_indices
        .into_iter()
        .map(|source_index| params[source_index].clone())
        .collect();
    let sql = statements_to_sql(&statements);

    Ok(BoundSql {
        sql,
        params: bound_params,
        state,
    })
}

struct PlaceholderBinder<'a> {
    params_len: usize,
    dialect: SqlDialect,
    state: &'a mut PlaceholderState,
    source_to_dense: &'a mut HashMap<usize, usize>,
    used_source_indices: &'a mut Vec<usize>,
}

impl VisitorMut for PlaceholderBinder<'_> {
    type Break = LixError;

    fn pre_visit_value(&mut self, value: &mut SqlValue) -> ControlFlow<Self::Break> {
        let SqlValue::Placeholder(token) = value else {
            return ControlFlow::Continue(());
        };
        let source_index = match resolve_placeholder_index(token, self.params_len, self.state) {
            Ok(index) => index,
            Err(error) => return ControlFlow::Break(error),
        };
        let dense_index =
            dense_index_for_source(source_index, self.source_to_dense, self.used_source_indices);
        *value = SqlValue::Placeholder(placeholder_for_dialect(self.dialect, dense_index + 1));
        ControlFlow::Continue(())
    }
}

pub(crate) fn resolve_placeholder_index(
    token: &str,
    params_len: usize,
    state: &mut PlaceholderState,
) -> Result<usize, LixError> {
    let trimmed = token.trim();

    let source_index = if trimmed.is_empty() || trimmed == "?" {
        let source_index = state.next_ordinal;
        state.next_ordinal += 1;
        source_index
    } else if let Some(numeric) = trimmed.strip_prefix('?') {
        let parsed = parse_1_based_index(trimmed, numeric)?;
        state.next_ordinal = state.next_ordinal.max(parsed);
        parsed - 1
    } else if let Some(numeric) = trimmed.strip_prefix('$') {
        let parsed = parse_1_based_index(trimmed, numeric)?;
        state.next_ordinal = state.next_ordinal.max(parsed);
        parsed - 1
    } else {
        return Err(LixError {
            message: format!("unsupported SQL placeholder format '{trimmed}'"),
        });
    };

    if source_index >= params_len {
        return Err(LixError {
            message: format!(
                "placeholder '{trimmed}' references parameter {} but only {} parameters were provided",
                source_index + 1,
                params_len
            ),
        });
    }

    Ok(source_index)
}

fn dense_index_for_source(
    source_index: usize,
    source_to_dense: &mut HashMap<usize, usize>,
    used_source_indices: &mut Vec<usize>,
) -> usize {
    if let Some(existing) = source_to_dense.get(&source_index) {
        return *existing;
    }
    let dense_index = used_source_indices.len();
    used_source_indices.push(source_index);
    source_to_dense.insert(source_index, dense_index);
    dense_index
}

fn placeholder_for_dialect(dialect: SqlDialect, dense_index_1_based: usize) -> String {
    match dialect {
        SqlDialect::Sqlite => format!("?{dense_index_1_based}"),
        SqlDialect::Postgres => format!("${dense_index_1_based}"),
    }
}

fn parse_1_based_index(token: &str, numeric: &str) -> Result<usize, LixError> {
    let parsed = numeric.parse::<usize>().map_err(|_| LixError {
        message: format!("invalid SQL placeholder '{token}'"),
    })?;
    if parsed == 0 {
        return Err(LixError {
            message: format!("invalid SQL placeholder '{token}'"),
        });
    }
    Ok(parsed)
}

fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, LixError> {
    Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        message: error.to_string(),
    })
}

fn statements_to_sql(statements: &[Statement]) -> String {
    statements
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
mod tests {
    use crate::backend::SqlDialect;
    use crate::sql::params::{bind_sql, bind_sql_with_state};
    use crate::Value;

    #[test]
    fn binds_sqlite_placeholders_with_dense_numbering() {
        let bound = bind_sql(
            "SELECT * FROM t WHERE a = ? OR b = ?3 OR c = ?",
            &[
                Value::Text("a".to_string()),
                Value::Text("b".to_string()),
                Value::Text("c".to_string()),
                Value::Text("d".to_string()),
            ],
            SqlDialect::Sqlite,
        )
        .expect("bind should succeed");

        assert_eq!(
            bound.sql,
            "SELECT * FROM t WHERE a = ?1 OR b = ?2 OR c = ?3"
        );
        assert_eq!(
            bound.params,
            vec![
                Value::Text("a".to_string()),
                Value::Text("c".to_string()),
                Value::Text("d".to_string())
            ]
        );
    }

    #[test]
    fn binds_postgres_placeholders_and_reuses_explicit_indices() {
        let bound = bind_sql(
            "SELECT * FROM t WHERE a = $2 OR b = $2 OR c = $1",
            &[Value::Integer(10), Value::Integer(20)],
            SqlDialect::Postgres,
        )
        .expect("bind should succeed");

        assert_eq!(
            bound.sql,
            "SELECT * FROM t WHERE a = $1 OR b = $1 OR c = $2"
        );
        assert_eq!(bound.params, vec![Value::Integer(20), Value::Integer(10)]);
    }

    #[test]
    fn bind_with_state_respects_ordinal_progression() {
        let first = bind_sql(
            "SELECT ?",
            &[Value::Integer(1), Value::Integer(2)],
            SqlDialect::Sqlite,
        )
        .expect("bind first");
        let second = bind_sql_with_state(
            "SELECT ?",
            &[Value::Integer(1), Value::Integer(2)],
            SqlDialect::Sqlite,
            first.state,
        )
        .expect("bind second");
        assert_eq!(first.sql, "SELECT ?1");
        assert_eq!(first.params, vec![Value::Integer(1)]);
        assert_eq!(second.sql, "SELECT ?1");
        assert_eq!(second.params, vec![Value::Integer(2)]);
    }

    #[test]
    fn ignores_placeholders_inside_string_literals() {
        let bound = bind_sql(
            "SELECT '$1', \"?\", ? FROM t WHERE x = '$2'",
            &[Value::Integer(5)],
            SqlDialect::Postgres,
        )
        .expect("bind should succeed");

        assert_eq!(bound.sql, "SELECT '$1', \"?\", $1 FROM t WHERE x = '$2'");
        assert_eq!(bound.params, vec![Value::Integer(5)]);
    }

    #[test]
    fn ignores_question_mark_inside_markdown_json_literal() {
        let sql = "INSERT INTO lix_internal_snapshot (id, content) VALUES ('s1', '{\"wordPattern\":\"[^\\\\\\\\/\\\\\\\\?\\\\\\\\s]+\",\"quote\":\"''\"}')";
        let bound = bind_sql(sql, &[], SqlDialect::Sqlite).expect("bind should succeed");
        assert!(bound.sql.contains("wordPattern"));
        assert!(bound.params.is_empty());
    }

    #[test]
    fn preserves_utf8_characters_while_binding() {
        let bound = bind_sql(
            "SELECT 'é🙂', \"名字\", ? FROM t WHERE note = 'München'",
            &[Value::Text("ok".to_string())],
            SqlDialect::Postgres,
        )
        .expect("bind should succeed");

        assert_eq!(
            bound.sql,
            "SELECT 'é🙂', \"名字\", $1 FROM t WHERE note = 'München'"
        );
        assert_eq!(bound.params, vec![Value::Text("ok".to_string())]);
    }

    #[test]
    fn preserves_utf8_characters_inside_escaped_quotes() {
        let bound = bind_sql(
            "SELECT 'L''été', \"Schrödinger\"\"猫\", ?",
            &[Value::Integer(1)],
            SqlDialect::Sqlite,
        )
        .expect("bind should succeed");

        assert_eq!(bound.sql, "SELECT 'L''été', \"Schrödinger\"\"猫\", ?1");
        assert_eq!(bound.params, vec![Value::Integer(1)]);
    }
}
