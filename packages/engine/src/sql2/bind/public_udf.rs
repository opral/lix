use std::ops::ControlFlow;

use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectNamePart, Statement,
    Value, Visit, Visitor,
};
#[cfg(test)]
use datafusion::sql::sqlparser::dialect::GenericDialect;
#[cfg(test)]
use datafusion::sql::sqlparser::parser::Parser;

use crate::LixError;

#[cfg(test)]
pub(crate) fn validate_public_udf_calls(sql: &str) -> Result<(), LixError> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| {
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
        "lix_json" => expect_exact_arity(name, arity, 1),
        "lix_empty_blob" | "lix_timestamp" | "lix_uuid_v7" | "lix_active_branch_commit_id" => {
            expect_exact_arity(name, arity, 0)
        }
        "lix_text_encode" | "lix_text_decode" => {
            expect_arity_range(name, arity, 1, 2)?;
            validate_literal_utf8_encoding(name, &function.args)
        }
        _ => Ok(()),
    }
}

pub(crate) fn validate_public_udf_calls_in_datafusion_statement(
    statement: &DataFusionStatement,
) -> Result<(), LixError> {
    let mut visitor = PublicUdfCallVisitor;
    visit_datafusion_statement(statement, &mut visitor)
}

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
            Some("lix_timestamp" | "lix_uuid_v7")
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
        _ => {}
    }
}

fn public_lix_function_name(function: &Function) -> Option<&'static str> {
    let part = function.name.0.last()?;
    let ident = match part {
        ObjectNamePart::Identifier(ident) => ident.value.as_str(),
        ObjectNamePart::Function(_) => return None,
    };
    match ident.to_ascii_lowercase().as_str() {
        "lix_json" => Some("lix_json"),
        "lix_empty_blob" => Some("lix_empty_blob"),
        "lix_timestamp" => Some("lix_timestamp"),
        "lix_uuid_v7" => Some("lix_uuid_v7"),
        "lix_active_branch_commit_id" => Some("lix_active_branch_commit_id"),
        "lix_text_encode" => Some("lix_text_encode"),
        "lix_text_decode" => Some("lix_text_decode"),
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

fn expect_arity_range(name: &str, actual: usize, min: usize, max: usize) -> Result<(), LixError> {
    if (min..=max).contains(&actual) {
        return Ok(());
    }
    Err(invalid_param(format!(
        "{name} requires {min} or {max} arguments"
    )))
}

fn validate_literal_utf8_encoding(name: &str, args: &FunctionArguments) -> Result<(), LixError> {
    let Some(encoding) = function_arg(args, 1) else {
        return Ok(());
    };
    let Some(value) = string_literal_arg(encoding) else {
        return Ok(());
    };
    let normalized = value.trim().to_ascii_uppercase().replace('-', "");
    if normalized == "UTF8" {
        Ok(())
    } else {
        Err(invalid_param(format!(
            "{name}() only supports UTF8 encoding, got '{value}'"
        )))
    }
}

fn function_arg(args: &FunctionArguments, index: usize) -> Option<&FunctionArg> {
    match args {
        FunctionArguments::List(list) => list.args.get(index),
        _ => None,
    }
}

fn string_literal_arg(arg: &FunctionArg) -> Option<&str> {
    let (FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
    | FunctionArg::Named {
        arg: FunctionArgExpr::Expr(expr),
        ..
    }
    | FunctionArg::ExprNamed {
        arg: FunctionArgExpr::Expr(expr),
        ..
    }) = arg
    else {
        return None;
    };
    let Expr::Value(value) = expr else {
        return None;
    };
    match &value.value {
        Value::SingleQuotedString(value)
        | Value::DoubleQuotedString(value)
        | Value::TripleSingleQuotedString(value)
        | Value::TripleDoubleQuotedString(value)
        | Value::EscapedStringLiteral(value)
        | Value::UnicodeStringLiteral(value)
        | Value::NationalStringLiteral(value)
        | Value::SingleQuotedRawStringLiteral(value)
        | Value::DoubleQuotedRawStringLiteral(value)
        | Value::TripleSingleQuotedRawStringLiteral(value)
        | Value::TripleDoubleQuotedRawStringLiteral(value) => Some(value.as_str()),
        Value::DollarQuotedString(value) => Some(value.value.as_str()),
        _ => None,
    }
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
        let error = validate_public_udf_calls("SELECT lix_uuid_v7('extra')")
            .expect_err("wrong arity should be rejected");
        assert_eq!(error.code, "LIX_INVALID_PARAM");
        assert!(error.message.contains("lix_uuid_v7 requires no arguments"));
    }

    #[test]
    fn rejects_unsupported_literal_encoding_as_public_invalid_param() {
        let error = validate_public_udf_calls("SELECT lix_text_encode('Ada', 'base64')")
            .expect_err("unsupported encoding should be rejected");
        assert_eq!(error.code, "LIX_INVALID_PARAM");
        assert!(
            error
                .message
                .contains("lix_text_encode() only supports UTF8 encoding")
        );
    }

    #[test]
    fn accepts_valid_public_lix_udf_calls() {
        validate_public_udf_calls(
            "SELECT lix_json('{\"x\":1}'), lix_text_decode(X'416461', 'utf-8')",
        )
        .expect("valid calls should pass public validation");
    }

    #[test]
    fn marks_durable_runtime_functions() {
        assert!(statement_has_durable_runtime_function(&parse_statement(
            "SELECT lix_uuid_v7()"
        )));
        assert!(statement_has_durable_runtime_function(&parse_statement(
            "SELECT lix_timestamp()"
        )));
        assert!(!statement_has_durable_runtime_function(&parse_statement(
            "SELECT lix_json('{\"x\":1}')"
        )));
    }
}
