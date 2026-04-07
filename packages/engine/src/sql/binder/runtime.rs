use std::collections::HashMap;
use std::hash::Hash;
use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, FunctionArguments, Insert, SetExpr, Statement, Value as SqlValue, VisitMut, VisitorMut,
};

pub(crate) use crate::sql::parser::placeholders::PlaceholderState;
use crate::sql::parser::placeholders::{parse_placeholder_ref, resolve_placeholder_ref};
pub(crate) use crate::statement_support::{
    bind_sql, bind_sql_with_state, bind_sql_with_state_and_appended_params,
};
use crate::{LixError, SqlDialect, Value};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundStatementAst {
    pub(crate) statement: Statement,
    pub(crate) params: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatementBindingTemplate {
    pub(crate) statement: Statement,
    pub(crate) used_bindings: Vec<StatementBindingSource>,
    pub(crate) minimum_param_count: usize,
    pub(crate) state: PlaceholderState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum RuntimeBindingKind {
    ActiveVersionId,
    ActiveAccountIdsJson,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeBindingValues {
    pub(crate) active_version_id: String,
    pub(crate) active_account_ids_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum StatementBindingSource {
    UserParam(usize),
    Runtime(RuntimeBindingKind),
}

const ACTIVE_VERSION_FUNCTION_NAME: &str = "lix_active_version_id";
const ACTIVE_ACCOUNT_IDS_FUNCTION_NAME: &str = "lix_active_account_ids";
const RUNTIME_PLACEHOLDER_PREFIX: &str = "__lix_runtime_binding_";

pub(crate) fn compile_statement_binding_template_with_state(
    statement: &Statement,
    params_len: usize,
    dialect: SqlDialect,
    mut state: PlaceholderState,
) -> Result<StatementBindingTemplate, LixError> {
    let mut statement = statement.clone();
    rewrite_runtime_binding_functions(&mut statement);
    let mut used_bindings = Vec::new();
    let mut source_to_dense: HashMap<StatementBindingSource, usize> = HashMap::new();

    let mut visitor = PlaceholderBinder {
        params_len,
        dialect,
        state: &mut state,
        source_to_dense: &mut source_to_dense,
        used_bindings: &mut used_bindings,
    };
    if let ControlFlow::Break(error) = statement.visit(&mut visitor) {
        return Err(error);
    }

    let minimum_param_count = used_bindings
        .iter()
        .filter_map(|binding| match binding {
            StatementBindingSource::UserParam(index) => Some(index + 1),
            StatementBindingSource::Runtime(_) => None,
        })
        .max()
        .unwrap_or(0);

    Ok(StatementBindingTemplate {
        statement,
        used_bindings,
        minimum_param_count,
        state,
    })
}

pub(crate) fn bind_statement_binding_template(
    template: &StatementBindingTemplate,
    params: &[Value],
    runtime_bindings: &RuntimeBindingValues,
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
            .used_bindings
            .iter()
            .map(|binding| match binding {
                StatementBindingSource::UserParam(source_index) => params[*source_index].clone(),
                StatementBindingSource::Runtime(RuntimeBindingKind::ActiveVersionId) => {
                    Value::Text(runtime_bindings.active_version_id.clone())
                }
                StatementBindingSource::Runtime(RuntimeBindingKind::ActiveAccountIdsJson) => {
                    Value::Text(runtime_bindings.active_account_ids_json.clone())
                }
            })
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

struct PlaceholderBinder<'a> {
    params_len: usize,
    dialect: SqlDialect,
    state: &'a mut PlaceholderState,
    source_to_dense: &'a mut HashMap<StatementBindingSource, usize>,
    used_bindings: &'a mut Vec<StatementBindingSource>,
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
        if let Some(runtime_binding) = runtime_binding_kind_from_placeholder(token) {
            let dense_index = dense_index_for_binding(
                StatementBindingSource::Runtime(runtime_binding),
                self.source_to_dense,
                self.used_bindings,
            );
            *value = SqlValue::Placeholder(placeholder_for_dialect(self.dialect, dense_index + 1));
            return ControlFlow::Continue(());
        }
        let placeholder = match parse_placeholder_ref(token) {
            Ok(placeholder) => placeholder,
            Err(error) => return ControlFlow::Break(error),
        };
        let source_index = match resolve_placeholder_ref(placeholder, self.params_len, self.state) {
            Ok(index) => index,
            Err(error) => return ControlFlow::Break(error),
        };
        let dense_index = dense_index_for_binding(
            StatementBindingSource::UserParam(source_index),
            self.source_to_dense,
            self.used_bindings,
        );
        *value = SqlValue::Placeholder(placeholder_for_dialect(self.dialect, dense_index + 1));
        ControlFlow::Continue(())
    }
}

fn dense_index_for_binding<K>(
    binding: K,
    source_to_dense: &mut HashMap<K, usize>,
    used_bindings: &mut Vec<K>,
) -> usize
where
    K: Eq + Hash + Clone,
{
    if let Some(existing) = source_to_dense.get(&binding) {
        return *existing;
    }
    let dense_index = used_bindings.len();
    used_bindings.push(binding.clone());
    source_to_dense.insert(binding, dense_index);
    dense_index
}

fn rewrite_runtime_binding_functions(statement: &mut Statement) {
    let mut rewriter = RuntimeBindingFunctionRewriter;
    let _ = statement.visit(&mut rewriter);
}

struct RuntimeBindingFunctionRewriter;

impl VisitorMut for RuntimeBindingFunctionRewriter {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };
        let binding = if object_name_matches(&function.name, ACTIVE_VERSION_FUNCTION_NAME)
            && function_args_empty(&function.args)
        {
            Some(RuntimeBindingKind::ActiveVersionId)
        } else if object_name_matches(&function.name, ACTIVE_ACCOUNT_IDS_FUNCTION_NAME)
            && function_args_empty(&function.args)
        {
            Some(RuntimeBindingKind::ActiveAccountIdsJson)
        } else {
            None
        };
        if let Some(binding) = binding {
            *expr = Expr::Value(SqlValue::Placeholder(runtime_placeholder_token(binding)).into());
        }
        ControlFlow::Continue(())
    }
}

fn runtime_placeholder_token(binding: RuntimeBindingKind) -> String {
    let index = match binding {
        RuntimeBindingKind::ActiveVersionId => 0,
        RuntimeBindingKind::ActiveAccountIdsJson => 1,
    };
    format!("{RUNTIME_PLACEHOLDER_PREFIX}{index}")
}

fn runtime_binding_kind_from_placeholder(token: &str) -> Option<RuntimeBindingKind> {
    let index = token
        .strip_prefix(RUNTIME_PLACEHOLDER_PREFIX)?
        .parse::<u8>()
        .ok()?;
    match index {
        0 => Some(RuntimeBindingKind::ActiveVersionId),
        1 => Some(RuntimeBindingKind::ActiveAccountIdsJson),
        _ => None,
    }
}

fn function_args_empty(args: &FunctionArguments) -> bool {
    match args {
        FunctionArguments::None => true,
        FunctionArguments::List(list) => list.args.is_empty() && list.clauses.is_empty(),
        FunctionArguments::Subquery(_) => false,
    }
}

fn object_name_matches(name: &sqlparser::ast::ObjectName, expected: &str) -> bool {
    name.0
        .iter()
        .last()
        .and_then(sqlparser::ast::ObjectNamePart::as_ident)
        .is_some_and(|ident| ident.value.eq_ignore_ascii_case(expected))
}

fn placeholder_for_dialect(dialect: SqlDialect, dense_index_1_based: usize) -> String {
    match dialect {
        SqlDialect::Sqlite => format!("?{dense_index_1_based}"),
        SqlDialect::Postgres => format!("${dense_index_1_based}"),
    }
}

#[cfg(test)]
pub(crate) fn is_transaction_control_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. }
    )
}
