use crate::sql::parser::placeholders::{resolve_placeholder_index, PlaceholderState};
use crate::{LixError, SqlDialect, Value};
use sqlparser::ast::{Query, Statement, Value as SqlValue, VisitMut, VisitorMut};
use std::collections::BTreeMap;
use std::ops::ControlFlow;

pub(crate) struct BoundPublicQuery {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
}

pub(crate) struct BoundPublicStatement {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
}

pub(crate) fn bind_public_query(
    query: Query,
    params: &[Value],
    dialect: SqlDialect,
) -> Result<BoundPublicQuery, LixError> {
    let mut statement = Statement::Query(Box::new(query));
    let params = bind_public_statement(&mut statement, params, dialect)?;
    Ok(BoundPublicQuery {
        sql: statement.to_string(),
        params,
    })
}

pub(crate) fn bind_public_statement_sql(
    mut statement: Statement,
    params: &[Value],
    dialect: SqlDialect,
) -> Result<BoundPublicStatement, LixError> {
    let params = bind_public_statement(&mut statement, params, dialect)?;
    Ok(BoundPublicStatement {
        sql: statement.to_string(),
        params,
    })
}

fn bind_public_statement(
    statement: &mut Statement,
    params: &[Value],
    dialect: SqlDialect,
) -> Result<Vec<Value>, LixError> {
    let mut used_source_indices = Vec::new();
    let mut source_to_dense = BTreeMap::new();
    let mut state = PlaceholderState::new();
    let mut visitor = PublicPlaceholderBinder {
        params_len: params.len(),
        dialect,
        state: &mut state,
        source_to_dense: &mut source_to_dense,
        used_source_indices: &mut used_source_indices,
    };
    if let ControlFlow::Break(error) = statement.visit(&mut visitor) {
        return Err(error);
    }
    Ok(used_source_indices
        .into_iter()
        .map(|index| params[index].clone())
        .collect())
}

struct PublicPlaceholderBinder<'a> {
    params_len: usize,
    dialect: SqlDialect,
    state: &'a mut PlaceholderState,
    source_to_dense: &'a mut BTreeMap<usize, usize>,
    used_source_indices: &'a mut Vec<usize>,
}

impl VisitorMut for PublicPlaceholderBinder<'_> {
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

fn dense_index_for_source(
    source_index: usize,
    source_to_dense: &mut BTreeMap<usize, usize>,
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

#[cfg(test)]
mod tests {
    use super::bind_public_query;
    use crate::{SqlDialect, Value};
    use sqlparser::ast::Statement;

    #[test]
    fn binds_sparse_placeholders_without_legacy_ast_utils() {
        let statement = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::GenericDialect {},
            "SELECT $3, $1, $3",
        )
        .expect("query should parse")
        .into_iter()
        .next()
        .expect("query should exist");
        let Statement::Query(query) = statement else {
            panic!("expected query statement");
        };
        let bound = bind_public_query(
            *query,
            &[
                Value::Text("first".to_string()),
                Value::Text("second".to_string()),
                Value::Text("third".to_string()),
            ],
            SqlDialect::Sqlite,
        )
        .expect("query should bind");

        assert_eq!(bound.sql, "SELECT ?1, ?2, ?1");
        assert_eq!(
            bound.params,
            vec![
                Value::Text("third".to_string()),
                Value::Text("first".to_string())
            ]
        );
    }
}
