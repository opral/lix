use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, Function, FunctionArguments, ObjectName, ObjectNamePart, Statement, Value,
};
use sqlparser::ast::{VisitMut, VisitorMut};

use crate::functions::timestamp::timestamp;
use crate::functions::uuid_v7::uuid_v7;

pub fn inline_lix_functions(mut statement: Statement) -> Statement {
    let mut inliner = LixFunctionInliner;
    let _ = statement.visit(&mut inliner);
    statement
}

struct LixFunctionInliner;

impl VisitorMut for LixFunctionInliner {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(function) = expr {
            if function_is(function, "lix_uuid_v7") && function_args_empty(function) {
                *expr = Expr::Value(Value::SingleQuotedString(uuid_v7()).into());
            } else if function_is(function, "lix_timestamp") && function_args_empty(function) {
                *expr = Expr::Value(Value::SingleQuotedString(timestamp()).into());
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

fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::inline_lix_functions;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

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
}
