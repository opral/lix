use crate::{LixError, SqlDialect, Value};
use sqlparser::ast::{
    Statement, TableFactor, TableObject, Value as SqlValue, VisitMut, VisitorMut,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub(crate) struct ParsedSql {
    pub(crate) statements: Vec<Statement>,
    pub(crate) parse_duration: Duration,
}

pub(crate) fn parse_sql_script_with_timing(sql: &str) -> Result<ParsedSql, ParserError> {
    let started = Instant::now();
    let statements = Parser::parse_sql(&GenericDialect {}, sql)?;
    Ok(ParsedSql {
        statements,
        parse_duration: started.elapsed(),
    })
}

#[cfg(test)]
pub(crate) fn parse_sql_script(sql: &str) -> Result<Vec<Statement>, ParserError> {
    parse_sql_script_with_timing(sql).map(|parsed| parsed.statements)
}

pub(crate) fn parse_sql_statements_with_timing(sql: &str) -> Result<ParsedSql, LixError> {
    parse_sql_script_with_timing(sql).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.to_string(),
    })
}

pub(crate) fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, LixError> {
    parse_sql_statements_with_timing(sql).map(|parsed| parsed.statements)
}

/// Early safety/UX gate that stops obviously forbidden internal-storage writes
/// before the session opens a transaction, compiles SQL, or touches backend
/// execution owners.
pub(crate) fn reject_internal_table_writes(statements: &[Statement]) -> Result<(), LixError> {
    for statement in statements {
        if statement_mutates_protected_lix_relation(statement) {
            return Err(crate::diagnostics::internal_table_access_denied_error());
        }
    }
    Ok(())
}

/// Early UX gate for unsupported public DDL. This stays at parser stage so a
/// user gets a stable product-level error before the request reaches lower SQL
/// preparation or backend execution layers.
pub(crate) fn reject_public_create_table(statements: &[Statement]) -> Result<(), LixError> {
    if statements
        .iter()
        .any(|statement| matches!(statement, Statement::CreateTable(_)))
    {
        return Err(crate::diagnostics::public_create_table_denied_error());
    }
    Ok(())
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
    let total_params_len = base_params.len() + appended_params.len();
    let mut source_to_dense = HashMap::<usize, usize>::new();
    let mut dense_sources = Vec::new();

    for statement in &mut statements {
        let mut visitor = StatementPlaceholderBinder {
            params_len: total_params_len,
            dialect,
            state: &mut state,
            source_to_dense: &mut source_to_dense,
            dense_sources: &mut dense_sources,
        };
        if let ControlFlow::Break(error) = statement.visit(&mut visitor) {
            return Err(error);
        }
    }

    Ok(BoundSql {
        sql: statements
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; "),
        params: dense_sources
            .into_iter()
            .map(|source_index| {
                clone_param_from_sources(source_index, base_params, appended_params)
            })
            .collect(),
        state,
    })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PlaceholderState {
    next_ordinal: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlaceholderRef {
    Next,
    Explicit(usize),
}

impl PlaceholderState {
    pub(crate) fn new() -> Self {
        Self { next_ordinal: 0 }
    }
}

pub(crate) fn parse_placeholder_ref(token: &str) -> Result<PlaceholderRef, LixError> {
    let trimmed = token.trim();

    if trimmed.is_empty() || trimmed == "?" {
        return Ok(PlaceholderRef::Next);
    }

    if let Some(numeric) = trimmed.strip_prefix('?') {
        return Ok(PlaceholderRef::Explicit(parse_1_based_index(
            trimmed, numeric,
        )?));
    }

    if let Some(numeric) = trimmed.strip_prefix('$') {
        return Ok(PlaceholderRef::Explicit(parse_1_based_index(
            trimmed, numeric,
        )?));
    }

    Err(LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("unsupported SQL placeholder format '{trimmed}'"),
    })
}

pub(crate) fn resolve_placeholder_ref(
    placeholder: PlaceholderRef,
    params_len: usize,
    state: &mut PlaceholderState,
) -> Result<usize, LixError> {
    let source_index = match placeholder {
        PlaceholderRef::Next => {
            let source_index = state.next_ordinal;
            state.next_ordinal += 1;
            source_index
        }
        PlaceholderRef::Explicit(index_1_based) => {
            state.next_ordinal = state.next_ordinal.max(index_1_based);
            index_1_based - 1
        }
    };

    if source_index >= params_len {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "placeholder {:?} references parameter {} but only {} parameters were provided",
                placeholder,
                source_index + 1,
                params_len
            ),
        });
    }

    Ok(source_index)
}

pub(crate) fn resolve_placeholder_index(
    token: &str,
    params_len: usize,
    state: &mut PlaceholderState,
) -> Result<usize, LixError> {
    let placeholder = parse_placeholder_ref(token)?;
    resolve_placeholder_ref(placeholder, params_len, state)
}

fn parse_1_based_index(token: &str, numeric: &str) -> Result<usize, LixError> {
    let parsed = numeric.parse::<usize>().map_err(|_| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("invalid SQL placeholder '{token}'"),
    })?;
    if parsed == 0 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("invalid SQL placeholder '{token}'"),
        });
    }
    Ok(parsed)
}

struct StatementPlaceholderBinder<'a> {
    params_len: usize,
    dialect: SqlDialect,
    state: &'a mut PlaceholderState,
    source_to_dense: &'a mut HashMap<usize, usize>,
    dense_sources: &'a mut Vec<usize>,
}

impl VisitorMut for StatementPlaceholderBinder<'_> {
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
            dense_index_for_source(source_index, self.source_to_dense, self.dense_sources);
        *value = SqlValue::Placeholder(placeholder_for_dialect(self.dialect, dense_index + 1));
        ControlFlow::Continue(())
    }
}

fn dense_index_for_source(
    source_index: usize,
    source_to_dense: &mut HashMap<usize, usize>,
    dense_sources: &mut Vec<usize>,
) -> usize {
    if let Some(existing) = source_to_dense.get(&source_index) {
        return *existing;
    }
    let dense_index = dense_sources.len();
    dense_sources.push(source_index);
    source_to_dense.insert(source_index, dense_index);
    dense_index
}

fn statement_mutates_protected_lix_relation(statement: &Statement) -> bool {
    match statement {
        Statement::Insert(insert) => match &insert.table {
            TableObject::TableName(name) => {
                crate::sql::object_name_is_internal_storage_relation(name)
            }
            _ => false,
        },
        Statement::Update(update) => match &update.table.relation {
            TableFactor::Table { name, .. } => {
                crate::sql::object_name_is_internal_storage_relation(name)
            }
            _ => false,
        },
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables)
                | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
            };
            tables.iter().any(|table| match &table.relation {
                TableFactor::Table { name, .. } => {
                    crate::sql::object_name_is_internal_storage_relation(name)
                }
                _ => false,
            })
        }
        Statement::AlterTable(alter) => {
            crate::sql::object_name_is_protected_builtin_ddl_target(&alter.name)
        }
        Statement::CreateIndex(create_index) => {
            crate::sql::object_name_is_protected_builtin_ddl_target(&create_index.table_name)
        }
        Statement::CreateTrigger(create_trigger) => {
            crate::sql::object_name_is_protected_builtin_ddl_target(&create_trigger.table_name)
                || create_trigger
                    .referenced_table_name
                    .as_ref()
                    .map(crate::sql::object_name_is_protected_builtin_ddl_target)
                    .unwrap_or(false)
        }
        Statement::DropTrigger(drop_trigger) => drop_trigger
            .table_name
            .as_ref()
            .map(crate::sql::object_name_is_protected_builtin_ddl_target)
            .unwrap_or(false),
        Statement::Drop { names, table, .. } => {
            names
                .iter()
                .any(crate::sql::object_name_is_protected_builtin_ddl_target)
                || table
                    .as_ref()
                    .map(crate::sql::object_name_is_protected_builtin_ddl_target)
                    .unwrap_or(false)
        }
        Statement::Truncate(truncate) => truncate
            .table_names
            .iter()
            .any(|target| crate::sql::object_name_is_protected_builtin_ddl_target(&target.name)),
        _ => false,
    }
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

fn placeholder_for_dialect(dialect: SqlDialect, dense_index_1_based: usize) -> String {
    match dialect {
        SqlDialect::Sqlite => format!("?{dense_index_1_based}"),
        SqlDialect::Postgres => format!("${dense_index_1_based}"),
    }
}
