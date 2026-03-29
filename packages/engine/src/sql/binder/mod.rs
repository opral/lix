//! SQL binding ownership.
//!
//! This stage owns placeholder binding, runtime binding templates, and other
//! parameter/caller-specific AST binding concerns.

pub(crate) mod classifier;
pub(crate) mod public_bind;
pub(crate) mod runtime;

use crate::sql::semantic_ir::{BoundStatement, ExecutionContext};
use crate::Value;
use sqlparser::ast::Statement;

pub(crate) use public_bind::{bind_public_query, bind_public_statement_sql};
pub(crate) use runtime::{
    bind_sql, bind_sql_with_state, bind_sql_with_state_and_appended_params,
    bind_statement_binding_template, compile_statement_binding_template_with_state,
    insert_values_rows_mut, RuntimeBindingValues, StatementBindingTemplate,
};
#[cfg(test)]
pub(crate) use runtime::{
    advance_placeholder_state_for_statement_ast, is_transaction_control_statement,
};

pub(crate) fn bind_statement(
    statement: Statement,
    bound_parameters: Vec<Value>,
    execution_context: ExecutionContext,
) -> BoundStatement {
    let metadata = classifier::bind_statement_metadata(&statement, execution_context);
    BoundStatement {
        statement,
        statement_kind: metadata.statement_kind,
        bound_parameters,
        normalized_scalar_literals: Vec::new(),
        execution_context: metadata.execution_context,
    }
}
