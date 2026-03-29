use std::ops::ControlFlow;

use crate::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::engine::Engine;
use crate::functions::SharedFunctionProvider;
use crate::sql::ast::walk::object_name_matches;
use crate::{LixBackend, LixBackendTransaction, LixError};
use sqlparser::ast::{Expr, Function, FunctionArguments, Statement, Visit, Visitor};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ExecutionRuntimeEffects {
    pub(crate) requires_deterministic_sequence_persistence: bool,
}

impl ExecutionRuntimeEffects {
    pub(crate) fn merge(self, other: Self) -> Self {
        Self {
            requires_deterministic_sequence_persistence: self
                .requires_deterministic_sequence_persistence
                || other.requires_deterministic_sequence_persistence,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ExecutionRuntimeState {
    settings: DeterministicSettings,
    functions: SharedFunctionProvider<RuntimeFunctionProvider>,
}

impl ExecutionRuntimeState {
    pub(crate) async fn prepare(
        engine: &Engine,
        backend: &dyn LixBackend,
    ) -> Result<Self, LixError> {
        let (settings, functions) = engine
            .prepare_runtime_functions_with_backend(backend)
            .await?;
        Ok(Self {
            settings,
            functions,
        })
    }

    pub(crate) fn settings(&self) -> DeterministicSettings {
        self.settings
    }

    pub(crate) fn provider(&self) -> &SharedFunctionProvider<RuntimeFunctionProvider> {
        &self.functions
    }

    pub(crate) async fn ensure_sequence_initialized_in_transaction(
        &self,
        engine: &Engine,
        transaction: &mut dyn LixBackendTransaction,
    ) -> Result<(), LixError> {
        engine
            .ensure_runtime_sequence_initialized_in_transaction(transaction, &self.functions)
            .await
    }

    pub(crate) async fn flush_in_transaction(
        &self,
        engine: &Engine,
        transaction: &mut dyn LixBackendTransaction,
    ) -> Result<(), LixError> {
        engine
            .persist_runtime_sequence_in_transaction(transaction, self.settings, &self.functions)
            .await
    }
}

pub(crate) fn derive_execution_runtime_effects(
    statements: &[Statement],
) -> ExecutionRuntimeEffects {
    statements
        .iter()
        .fold(ExecutionRuntimeEffects::default(), |effects, statement| {
            effects.merge(ExecutionRuntimeEffects {
                requires_deterministic_sequence_persistence:
                    statement_requires_deterministic_sequence_persistence(statement),
            })
        })
}

fn statement_requires_deterministic_sequence_persistence(statement: &Statement) -> bool {
    struct Collector {
        found: bool,
    }

    impl Visitor for Collector {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            let Expr::Function(function) = expr else {
                return ControlFlow::Continue(());
            };
            if deterministic_sequence_function(function) {
                self.found = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector { found: false };
    let _ = statement.visit(&mut collector);
    collector.found
}

fn deterministic_sequence_function(function: &Function) -> bool {
    function_args_empty(function)
        && (object_name_matches(&function.name, "lix_uuid_v7")
            || object_name_matches(&function.name, "lix_timestamp"))
}

fn function_args_empty(function: &Function) -> bool {
    match &function.args {
        FunctionArguments::None => true,
        FunctionArguments::List(list) => list.args.is_empty() && list.clauses.is_empty(),
        FunctionArguments::Subquery(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{derive_execution_runtime_effects, ExecutionRuntimeEffects};
    use crate::sql::parser::parse_sql;

    #[test]
    fn deterministic_runtime_effects_detect_uuid_and_timestamp_usage() {
        let statements = parse_sql("SELECT lix_uuid_v7(), lix_timestamp(), 1")
            .expect("parse SQL should succeed");
        assert_eq!(
            derive_execution_runtime_effects(&statements),
            ExecutionRuntimeEffects {
                requires_deterministic_sequence_persistence: true,
            }
        );
    }

    #[test]
    fn deterministic_runtime_effects_ignore_plain_reads() {
        let statements = parse_sql("SELECT 1, 2, 3").expect("parse SQL should succeed");
        assert_eq!(
            derive_execution_runtime_effects(&statements),
            ExecutionRuntimeEffects::default()
        );
    }
}
