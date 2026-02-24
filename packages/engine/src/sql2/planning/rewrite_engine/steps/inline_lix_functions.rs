use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Function, FunctionArguments, Statement, Value};
use sqlparser::ast::{VisitMut, VisitorMut};

use crate::functions::LixFunctionProvider;
#[cfg(test)]
use crate::functions::SystemFunctionProvider;
use crate::engine::sql2::planning::rewrite_engine::object_name_matches;

#[cfg(test)]
pub fn inline_lix_functions(statement: Statement) -> Statement {
    let mut provider = SystemFunctionProvider;
    inline_lix_functions_with_provider(statement, &mut provider)
}

pub fn inline_lix_functions_with_provider<P: LixFunctionProvider>(
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
                *expr = Expr::Value(Value::SingleQuotedString(self.provider.uuid_v7()).into());
            } else if function_is(function, "lix_timestamp") && function_args_empty(function) {
                *expr = Expr::Value(Value::SingleQuotedString(self.provider.timestamp()).into());
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

#[cfg(test)]
mod tests {
    use super::inline_lix_functions;
    use crate::functions::LixFunctionProvider;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[derive(Default)]
    struct TestProvider {
        uuid_calls: usize,
        timestamp_calls: usize,
    }

    impl LixFunctionProvider for TestProvider {
        fn uuid_v7(&mut self) -> String {
            let value = format!("uuid-{}", self.uuid_calls);
            self.uuid_calls += 1;
            value
        }

        fn timestamp(&mut self) -> String {
            let value = format!("ts-{}", self.timestamp_calls);
            self.timestamp_calls += 1;
            value
        }
    }

    #[test]
    fn inlines_uuid_and_timestamp_calls() {
        let sql = "INSERT INTO foo (id, created_at) VALUES (lix_uuid_v7(), lix_timestamp())";
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).expect("parse sql");
        let statement = statements.remove(0);

        let rewritten = inline_lix_functions(statement).to_string();

        assert!(!rewritten.to_lowercase().contains("lix_uuid_v7"));
        assert!(!rewritten.to_lowercase().contains("lix_timestamp"));
        assert!(rewritten.contains("'"));
        assert!(rewritten.contains("T"));
        assert!(rewritten.contains("Z"));
    }

    #[test]
    fn inlines_with_custom_provider() {
        let sql = "SELECT lix_uuid_v7(), lix_timestamp(), lix_uuid_v7()";
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).expect("parse sql");
        let statement = statements.remove(0);
        let mut provider = TestProvider::default();

        let rewritten = super::inline_lix_functions_with_provider(statement, &mut provider);
        let rewritten_sql = rewritten.to_string();

        assert!(rewritten_sql.contains("'uuid-0'"));
        assert!(rewritten_sql.contains("'ts-0'"));
        assert!(rewritten_sql.contains("'uuid-1'"));
    }
}
