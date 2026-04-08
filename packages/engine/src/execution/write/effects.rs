use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, Function, FunctionArguments, ObjectName, ObjectNamePart, Statement, Visit, Visitor,
};

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

fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
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
