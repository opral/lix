use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Function, FunctionArguments, Statement, Value as SqlValue};
use sqlparser::ast::{VisitMut, VisitorMut};

use crate::functions::LixFunctionProvider;

use super::super::ast::walk::object_name_matches;

pub(crate) fn inline_lix_functions_with_provider<P: LixFunctionProvider>(
    mut statement: Statement,
    provider: &mut P,
) -> Statement {
    let mut inliner = LixFunctionInliner { provider };
    let _ = statement.visit(&mut inliner);
    statement
}

struct LixFunctionInliner<'a, P: LixFunctionProvider> {
    provider: &'a mut P,
}

impl<P: LixFunctionProvider> VisitorMut for LixFunctionInliner<'_, P> {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(function) = expr {
            if function_is(function, "lix_uuid_v7") && function_args_empty(function) {
                *expr = Expr::Value(SqlValue::SingleQuotedString(self.provider.uuid_v7()).into());
            } else if function_is(function, "lix_timestamp") && function_args_empty(function) {
                *expr = Expr::Value(SqlValue::SingleQuotedString(self.provider.timestamp()).into());
            }
        }
        ControlFlow::Continue(())
    }
}

fn function_is(function: &Function, target: &str) -> bool {
    object_name_matches(&function.name, target)
}

fn function_args_empty(function: &Function) -> bool {
    match &function.args {
        FunctionArguments::None => true,
        FunctionArguments::List(list) => list.args.is_empty() && list.clauses.is_empty(),
        FunctionArguments::Subquery(_) => false,
    }
}
