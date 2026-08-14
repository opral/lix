use std::ops::ControlFlow;

use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    Expr, Function, FunctionArguments, ObjectNamePart, Statement, Visit, Visitor,
};
#[cfg(test)]
use datafusion::sql::sqlparser::parser::Parser;

use crate::LixError;

#[cfg(test)]
pub(crate) fn validate_public_udf_calls(sql: &str) -> Result<(), LixError> {
    let statements =
        Parser::parse_sql(&super::super::dialect::lix_sql_dialect(), sql).map_err(|error| {
            LixError::new(
                LixError::CODE_PARSE_ERROR,
                format!("sql2 SQL parse error: {error}"),
            )
        })?;

    let mut visitor = PublicUdfCallVisitor;
    match statements.visit(&mut visitor) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(*error),
    }
}

struct PublicUdfCallVisitor;

impl Visitor for PublicUdfCallVisitor {
    type Break = Box<LixError>;

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };

        match validate_public_function_call(function) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(Box::new(error)),
        }
    }

    fn pre_visit_statement(&mut self, _statement: &Statement) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
}

fn validate_public_function_call(function: &Function) -> Result<(), LixError> {
    let Some(name) = public_lix_function_name(function) else {
        return Ok(());
    };
    let arity = function_arity(&function.args);

    match name {
        "current_timestamp"
        | "uuidv7"
        | "lix_active_branch_id"
        | "lix_active_branch_commit_id" => expect_exact_arity(name, arity, 0),
        _ => Ok(()),
    }
}

pub(crate) fn validate_public_udf_calls_in_datafusion_statement(
    statement: &DataFusionStatement,
) -> Result<(), LixError> {
    let mut visitor = PublicUdfCallVisitor;
    visit_datafusion_statement(statement, &mut visitor)
}

/// Conservatively reports whether a read statement can invoke an engine UDF
/// whose state must be loaded before execution and persisted afterward.
///
/// This classifier runs before provider/session construction, so every
/// inspectable nested expression must be visited and unknown statement shapes
/// must fail toward doing the durable setup.
pub(crate) fn statement_has_durable_runtime_function(statement: &DataFusionStatement) -> bool {
    let mut visitor = DurableRuntimeFunctionVisitor { found: false };
    visit_datafusion_statement_for_durable_runtime_function(statement, &mut visitor);
    visitor.found
}

struct DurableRuntimeFunctionVisitor {
    found: bool,
}

impl Visitor for DurableRuntimeFunctionVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };

        if matches!(
            public_lix_function_name(function),
            Some("current_timestamp" | "uuidv7")
        ) {
            self.found = true;
            return ControlFlow::Break(());
        }

        ControlFlow::Continue(())
    }
}

fn visit_datafusion_statement(
    statement: &DataFusionStatement,
    visitor: &mut PublicUdfCallVisitor,
) -> Result<(), LixError> {
    match statement {
        DataFusionStatement::Statement(statement) => match statement.visit(visitor) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(error) => Err(*error),
        },
        DataFusionStatement::Explain(explain) => {
            visit_datafusion_statement(explain.statement.as_ref(), visitor)
        }
        _ => Ok(()),
    }
}

fn visit_datafusion_statement_for_durable_runtime_function(
    statement: &DataFusionStatement,
    visitor: &mut DurableRuntimeFunctionVisitor,
) {
    match statement {
        DataFusionStatement::Statement(statement) => {
            let _ = statement.visit(visitor);
        }
        DataFusionStatement::Explain(explain) => {
            visit_datafusion_statement_for_durable_runtime_function(
                explain.statement.as_ref(),
                visitor,
            );
        }
        // Extension statements are currently rejected by statement routing,
        // but keep this detector conservative if one becomes readable later.
        // Skipping durable setup on an AST shape we did not inspect would be
        // a correctness bug; doing the setup unnecessarily is only slower.
        DataFusionStatement::CreateExternalTable(_)
        | DataFusionStatement::CopyTo(_)
        | DataFusionStatement::Reset(_) => visitor.found = true,
    }
}

fn public_lix_function_name(function: &Function) -> Option<&'static str> {
    let part = function.name.0.last()?;
    let ident = match part {
        ObjectNamePart::Identifier(ident) => ident.value.as_str(),
        ObjectNamePart::Function(_) => return None,
    };
    match ident.to_ascii_lowercase().as_str() {
        "current_timestamp" | "__lix_current_timestamp" => Some("current_timestamp"),
        "uuidv7" => Some("uuidv7"),
        "lix_active_branch_id" => Some("lix_active_branch_id"),
        "lix_active_branch_commit_id" => Some("lix_active_branch_commit_id"),
        _ => None,
    }
}

fn function_arity(args: &FunctionArguments) -> usize {
    match args {
        FunctionArguments::None => 0,
        FunctionArguments::Subquery(_) => 1,
        FunctionArguments::List(list) => list.args.len(),
    }
}

fn expect_exact_arity(name: &str, actual: usize, expected: usize) -> Result<(), LixError> {
    if actual == expected {
        return Ok(());
    }

    let expectation = if expected == 0 {
        "no arguments".to_string()
    } else if expected == 1 {
        "exactly 1 argument".to_string()
    } else {
        format!("exactly {expected} arguments")
    };
    Err(invalid_param(format!("{name} requires {expectation}")))
}

fn invalid_param(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}

#[cfg(test)]
mod tests {
    use datafusion::sql::parser::Statement as DataFusionStatement;

    use super::{statement_has_durable_runtime_function, validate_public_udf_calls};

    fn parse_statement(sql: &str) -> DataFusionStatement {
        crate::sql2::parse_statement(sql)
            .unwrap_or_else(|error| panic!("failed to parse '{sql}': {error}"))
    }

    #[test]
    fn rejects_lix_udf_wrong_arity_as_public_invalid_param() {
        let error = validate_public_udf_calls("SELECT uuidv7('extra')")
            .expect_err("wrong arity should be rejected");
        assert_eq!(error.code, "LIX_INVALID_PARAM");
        assert!(error.message.contains("uuidv7 requires no arguments"));
    }

    #[test]
    fn accepts_valid_public_lix_udf_calls() {
        validate_public_udf_calls("SELECT '{\"x\":1}'::jsonb, CURRENT_TIMESTAMP")
            .expect("valid calls should pass public validation");
    }

    #[test]
    fn marks_direct_durable_runtime_functions() {
        assert!(statement_has_durable_runtime_function(&parse_statement(
            "SELECT uuidv7()"
        )));
        assert!(statement_has_durable_runtime_function(&parse_statement(
            "SELECT CURRENT_TIMESTAMP"
        )));
        assert!(!statement_has_durable_runtime_function(&parse_statement(
            "SELECT '{\"x\":1}'::jsonb"
        )));
        assert!(!statement_has_durable_runtime_function(&parse_statement(
            "SELECT 'uuidv7()' AS literal"
        )));
        assert!(!statement_has_durable_runtime_function(&parse_statement(
            "SELECT 1 /* CURRENT_TIMESTAMP */"
        )));
    }

    #[test]
    fn marks_nested_aliased_and_explained_durable_runtime_functions() {
        for sql in [
            "WITH generated AS (SELECT uuidv7() AS value) SELECT value FROM generated",
            "SELECT value FROM (SELECT CURRENT_TIMESTAMP AS value) AS generated",
            "SELECT uuidv7() AS generated_id",
            "SELECT CASE WHEN true THEN CURRENT_TIMESTAMP ELSE TIMESTAMPTZ '1970-01-01T00:00:00Z' END AS value",
            "EXPLAIN SELECT uuidv7()",
        ] {
            assert!(
                statement_has_durable_runtime_function(&parse_statement(sql)),
                "nested or aliased durable function should be detected in: {sql}"
            );
        }
    }
}
