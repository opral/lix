use std::collections::HashMap;
use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Insert, SetExpr, Statement, Value as SqlValue, VisitMut, VisitorMut};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::sql_support::placeholders::{parse_placeholder_ref, resolve_placeholder_ref};
pub(crate) use crate::sql_support::placeholders::{resolve_placeholder_index, PlaceholderState};
use crate::{LixError, SqlDialect, Value};

#[derive(Debug, Clone)]
pub(crate) struct BoundSql {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
    pub(crate) state: PlaceholderState,
}

#[derive(Debug, Clone)]
pub(crate) struct BoundStatementAst {
    pub(crate) statement: Statement,
    pub(crate) params: Vec<Value>,
}

#[derive(Debug, Clone)]
pub(crate) struct StatementBindingTemplate {
    pub(crate) statement: Statement,
    pub(crate) used_source_indices: Vec<usize>,
    pub(crate) minimum_param_count: usize,
    pub(crate) state: PlaceholderState,
}

pub(crate) fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, LixError> {
    Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.to_string(),
    })
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
    state: PlaceholderState,
) -> Result<BoundSql, LixError> {
    bind_sql_with_state_and_appended_params(sql, params, &[], dialect, state)
}

pub(crate) fn bind_sql_with_state_and_appended_params(
    sql: &str,
    base_params: &[Value],
    appended_params: &[Value],
    dialect: SqlDialect,
    mut state: PlaceholderState,
) -> Result<BoundSql, LixError> {
    let mut statements = parse_sql_statements(sql)?;
    let mut used_source_indices = Vec::new();
    let mut source_to_dense: HashMap<usize, usize> = HashMap::new();
    let total_params_len = base_params.len() + appended_params.len();

    for statement in &mut statements {
        let mut visitor = PlaceholderBinder {
            params_len: total_params_len,
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
        .map(|source_index| clone_param_from_sources(source_index, base_params, appended_params))
        .collect();

    Ok(BoundSql {
        sql: statements_to_sql(&statements),
        params: bound_params,
        state,
    })
}

pub(crate) fn compile_statement_binding_template_with_state(
    statement: &Statement,
    params_len: usize,
    dialect: SqlDialect,
    mut state: PlaceholderState,
) -> Result<StatementBindingTemplate, LixError> {
    let mut statement = statement.clone();
    let mut used_source_indices = Vec::new();
    let mut source_to_dense: HashMap<usize, usize> = HashMap::new();

    let mut visitor = PlaceholderBinder {
        params_len,
        dialect,
        state: &mut state,
        source_to_dense: &mut source_to_dense,
        used_source_indices: &mut used_source_indices,
    };
    if let ControlFlow::Break(error) = statement.visit(&mut visitor) {
        return Err(error);
    }

    let minimum_param_count = used_source_indices
        .iter()
        .max()
        .map(|index| index + 1)
        .unwrap_or(0);

    Ok(StatementBindingTemplate {
        statement,
        used_source_indices,
        minimum_param_count,
        state,
    })
}

pub(crate) fn bind_statement_binding_template(
    template: &StatementBindingTemplate,
    params: &[Value],
) -> Result<BoundStatementAst, LixError> {
    if params.len() < template.minimum_param_count {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "statement binding expected at least {} params, got {}",
                template.minimum_param_count,
                params.len()
            ),
        ));
    }

    Ok(BoundStatementAst {
        statement: template.statement.clone(),
        params: template
            .used_source_indices
            .iter()
            .map(|source_index| params[*source_index].clone())
            .collect(),
    })
}

#[cfg(test)]
pub(crate) fn advance_placeholder_state_for_statement_ast(
    statement: &mut Statement,
    params_len: usize,
    state: &mut PlaceholderState,
) -> Result<(), LixError> {
    let mut visitor = PlaceholderStateAdvancer { params_len, state };
    if let ControlFlow::Break(error) = statement.visit(&mut visitor) {
        return Err(error);
    }
    Ok(())
}

pub(crate) fn insert_values_rows_mut(insert: &mut Insert) -> Option<&mut [Vec<Expr>]> {
    let source = insert.source.as_mut()?;
    let SetExpr::Values(values) = source.body.as_mut() else {
        return None;
    };
    Some(values.rows.as_mut_slice())
}

fn clone_param_from_sources(
    source_index: usize,
    base_params: &[Value],
    appended_params: &[Value],
) -> Value {
    if source_index < base_params.len() {
        return base_params[source_index].clone();
    }

    appended_params[source_index - base_params.len()].clone()
}

struct PlaceholderBinder<'a> {
    params_len: usize,
    dialect: SqlDialect,
    state: &'a mut PlaceholderState,
    source_to_dense: &'a mut HashMap<usize, usize>,
    used_source_indices: &'a mut Vec<usize>,
}

#[cfg(test)]
struct PlaceholderStateAdvancer<'a> {
    params_len: usize,
    state: &'a mut PlaceholderState,
}

#[cfg(test)]
impl VisitorMut for PlaceholderStateAdvancer<'_> {
    type Break = LixError;

    fn pre_visit_value(&mut self, value: &mut SqlValue) -> ControlFlow<Self::Break> {
        let SqlValue::Placeholder(token) = value else {
            return ControlFlow::Continue(());
        };
        let placeholder = match parse_placeholder_ref(token) {
            Ok(placeholder) => placeholder,
            Err(error) => return ControlFlow::Break(error),
        };
        match resolve_placeholder_ref(placeholder, self.params_len, self.state) {
            Ok(_) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }
    }
}

impl VisitorMut for PlaceholderBinder<'_> {
    type Break = LixError;

    fn pre_visit_value(&mut self, value: &mut SqlValue) -> ControlFlow<Self::Break> {
        let SqlValue::Placeholder(token) = value else {
            return ControlFlow::Continue(());
        };
        let placeholder = match parse_placeholder_ref(token) {
            Ok(placeholder) => placeholder,
            Err(error) => return ControlFlow::Break(error),
        };
        let source_index = match resolve_placeholder_ref(placeholder, self.params_len, self.state) {
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

fn statements_to_sql(statements: &[Statement]) -> String {
    statements
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
pub(crate) fn is_transaction_control_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. }
    )
}
