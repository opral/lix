//! SQL binding ownership.
//!
//! This stage owns placeholder binding, runtime binding templates, and other
//! parameter/caller-specific AST binding concerns.

pub(crate) mod classifier;
pub(crate) mod public_reads;
pub(crate) mod runtime;

use crate::catalog::SurfaceRegistry;
use crate::sql::logical_plan::public_ir::BroadPublicReadStatement;
use crate::sql::semantic_ir::{BoundStatement, StatementContext};
use crate::{LixError, Value};
use sqlparser::ast::Statement;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BoundPublicReadArtifacts {
    pub(crate) bound_statement: BoundStatement,
    pub(crate) broad_statement: Option<BroadPublicReadStatement>,
}

pub(crate) use public_reads::bind_broad_public_read_statement_with_registry;
#[cfg(test)]
pub(crate) use public_reads::forbid_broad_binding_for_test;
pub use public_reads::{delay_broad_binding_for_test, BroadBindingDelayForTestGuard};
#[cfg(test)]
pub(crate) use runtime::{
    advance_placeholder_state_for_statement_ast, is_transaction_control_statement,
};
pub(crate) use runtime::{
    bind_sql_with_state, bind_sql_with_state_and_appended_params, bind_statement_binding_template,
    compile_statement_binding_template_with_state, RuntimeBindingValues, StatementBindingTemplate,
};

pub(crate) fn bind_statement(
    statement: Statement,
    bound_parameters: Vec<Value>,
    statement_context: StatementContext,
) -> BoundStatement {
    let metadata = classifier::bind_statement_metadata(&statement, statement_context);
    BoundStatement {
        statement,
        statement_kind: metadata.statement_kind,
        bound_parameters,
        normalized_scalar_literals: Vec::new(),
        statement_context: metadata.statement_context,
    }
}

pub(crate) fn bind_public_read_statement(
    statement: Statement,
    bound_parameters: Vec<Value>,
    statement_context: StatementContext,
    registry: &SurfaceRegistry,
) -> Result<BoundPublicReadArtifacts, LixError> {
    let broad_statement = bind_broad_public_read_statement_with_registry(&statement, registry)?;
    let bound_statement = bind_statement(statement, bound_parameters, statement_context);
    Ok(BoundPublicReadArtifacts {
        bound_statement,
        broad_statement,
    })
}
