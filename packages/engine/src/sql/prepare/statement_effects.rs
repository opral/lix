use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, Function, FunctionArguments, ObjectName, ObjectNamePart, Statement, Visit, Visitor,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct StatementEffects {
    pub(crate) requires_deterministic_sequence_persistence: bool,
}

impl StatementEffects {
    pub(crate) fn merge(self, other: Self) -> Self {
        Self {
            requires_deterministic_sequence_persistence: self
                .requires_deterministic_sequence_persistence
                || other.requires_deterministic_sequence_persistence,
        }
    }
}

pub(crate) fn derive_statement_effects(statements: &[Statement]) -> StatementEffects {
    statements
        .iter()
        .fold(StatementEffects::default(), |effects, statement| {
            effects.merge(StatementEffects {
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
    use super::{derive_statement_effects, StatementEffects};
    use crate::sql::parse_sql_statements;

    #[test]
    fn statement_effects_detect_uuid_and_timestamp_usage() {
        let statements = parse_sql_statements("SELECT lix_uuid_v7(), lix_timestamp(), 1")
            .expect("parse SQL should succeed");
        assert_eq!(
            derive_statement_effects(&statements),
            StatementEffects {
                requires_deterministic_sequence_persistence: true,
            }
        );
    }

    #[test]
    fn statement_effects_ignore_plain_reads() {
        let statements = parse_sql_statements("SELECT 1, 2, 3").expect("parse SQL should succeed");
        assert_eq!(
            derive_statement_effects(&statements),
            StatementEffects::default()
        );
    }
}
