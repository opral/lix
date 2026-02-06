use std::collections::HashMap;

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
    let mut output = String::with_capacity(sql.len());
    let mut used_source_indices = Vec::new();
    let mut source_to_dense: HashMap<usize, usize> = HashMap::new();

    let bytes = sql.as_bytes();
    let mut index = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while index < bytes.len() {
        let byte = bytes[index];

        if in_single_quote {
            output.push(byte as char);
            if byte == b'\'' {
                if index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                    output.push('\'');
                    index += 2;
                    continue;
                }
                in_single_quote = false;
            }
            index += 1;
            continue;
        }

        if in_double_quote {
            output.push(byte as char);
            if byte == b'"' {
                if index + 1 < bytes.len() && bytes[index + 1] == b'"' {
                    output.push('"');
                    index += 2;
                    continue;
                }
                in_double_quote = false;
            }
            index += 1;
            continue;
        }

        if byte == b'\'' {
            in_single_quote = true;
            output.push('\'');
            index += 1;
            continue;
        }
        if byte == b'"' {
            in_double_quote = true;
            output.push('"');
            index += 1;
            continue;
        }

        if byte == b'?' {
            let start = index;
            index += 1;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
            let token = &sql[start..index];
            let source_index = resolve_placeholder_index(token, params.len(), &mut state)?;
            let dense_index = dense_index_for_source(
                source_index,
                &mut source_to_dense,
                &mut used_source_indices,
            );
            output.push_str(&placeholder_for_dialect(dialect, dense_index + 1));
            continue;
        }

        if byte == b'$' && index + 1 < bytes.len() && bytes[index + 1].is_ascii_digit() {
            let start = index;
            index += 1;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
            let token = &sql[start..index];
            let source_index = resolve_placeholder_index(token, params.len(), &mut state)?;
            let dense_index = dense_index_for_source(
                source_index,
                &mut source_to_dense,
                &mut used_source_indices,
            );
            output.push_str(&placeholder_for_dialect(dialect, dense_index + 1));
            continue;
        }

        output.push(byte as char);
        index += 1;
    }

    let bound_params = used_source_indices
        .into_iter()
        .map(|source_index| params[source_index].clone())
        .collect();

    Ok(BoundSql {
        sql: output,
        params: bound_params,
        state,
    })
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
}
