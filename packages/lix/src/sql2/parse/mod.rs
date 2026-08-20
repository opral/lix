use datafusion::sql::parser::{DFParserBuilder, Statement as DataFusionStatement};
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, DataType as SqlDataType, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart, Value, VisitMut,
    VisitorMut,
};
use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};
use serde_json::json;
use std::ops::ControlFlow;

use crate::LixError;

fn identifier_matches(identifier: &Ident, expected: &str) -> bool {
    if identifier.quote_style.is_some() {
        identifier.value == expected
    } else {
        identifier.value.eq_ignore_ascii_case(expected)
    }
}

/// Matches a public table function using PostgreSQL identifier rules.
/// Unquoted identifiers are case-insensitive; quoted identifiers are exact.
/// DataFusion registers table functions globally, so only the public/default
/// qualification is normalized away rather than accepting arbitrary schemas.
pub(crate) fn object_name_is_public_function(name: &ObjectName, expected: &str) -> bool {
    let identifiers = name
        .0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(identifier) => Some(identifier),
            ObjectNamePart::Function(_) => None,
        })
        .collect::<Option<Vec<_>>>();
    let Some(identifiers) = identifiers else {
        return false;
    };
    match identifiers.as_slice() {
        [function] => identifier_matches(function, expected),
        [schema, function] => {
            identifier_matches(schema, "public") && identifier_matches(function, expected)
        }
        [catalog, schema, function] => {
            identifier_matches(catalog, "datafusion")
                && identifier_matches(schema, "public")
                && identifier_matches(function, expected)
        }
        _ => false,
    }
}

pub(crate) fn parse_statement(sql: &str) -> Result<DataFusionStatement, LixError> {
    let dialect = super::dialect::lix_sql_dialect();
    let mut has_anonymous = false;
    let mut explicit_placeholders = Vec::new();

    let mut tokens = Vec::new();
    Tokenizer::new(&dialect, sql)
        .tokenize_with_location_into_buf_with_mapper(&mut tokens, |token_span| {
            if let Token::Placeholder(placeholder) = &token_span.token {
                if placeholder == "?" {
                    // Disambiguated below after the neighboring tokens are known:
                    // GenericDialect tokenizes both anonymous parameters and
                    // PostgreSQL's JSONB existence operator as `?` placeholders.
                } else {
                    explicit_placeholders.push(placeholder.clone());
                }
            }
            token_span
        })
        .map_err(|error| {
            LixError::new(
                LixError::CODE_PARSE_ERROR,
                format!("sql2 SQL tokenize error: {error}"),
            )
        })?;

    let mut next_index = 1usize;
    for index in 0..tokens.len() {
        if !matches!(&tokens[index].token, Token::Placeholder(value) if value == "?") {
            continue;
        }
        let next_is_literal = tokens[index + 1..]
            .iter()
            .find(|next| !matches!(next.token, Token::Whitespace(_)))
            .is_some_and(|next| matches!(next.token, Token::SingleQuotedString(_)));
        let previous_is_operand = tokens[..index]
            .iter()
            .rev()
            .find(|previous| !matches!(previous.token, Token::Whitespace(_)))
            .is_some_and(|previous| {
                matches!(
                    previous.token,
                    Token::Word(ref word)
                        if word.keyword
                            == datafusion::sql::sqlparser::keywords::Keyword::NoKeyword
                ) || matches!(
                    previous.token,
                    Token::RParen
                        | Token::RBracket
                        | Token::SingleQuotedString(_)
                        | Token::Number(_, _)
                        | Token::Placeholder(_)
                )
            });
        let jsonb_existence_operator = next_is_literal || previous_is_operand;
        if jsonb_existence_operator {
            tokens[index].token = Token::Question;
        } else {
            has_anonymous = true;
            tokens[index].token = Token::Placeholder(format!("${next_index}"));
            next_index += 1;
        }
    }

    reject_sql_hex_literals(&tokens)?;

    if has_anonymous && !explicit_placeholders.is_empty() {
        return Err(LixError::new(
            LixError::CODE_PARSE_ERROR,
            "SQL mixes anonymous and explicit parameter placeholders",
        )
        .with_hint("Use either anonymous placeholders like ?, ? or numbered placeholders like $1, $2, but not both.")
        .with_details(json!({
            "operation": "execute",
            "explicit_placeholders": explicit_placeholders,
        })));
    }

    let mut statements = DFParserBuilder::new(tokens)
        .with_dialect(&dialect)
        .build()
        .map_err(crate::sql2::error::datafusion_error_to_lix_error)?
        .parse_statements()
        .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;

    if statements.len() > 1 {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "Lix SQL only supports one statement per execute() call",
        ));
    }

    let mut statement = statements.pop_front().ok_or_else(|| {
        LixError::new(
            LixError::CODE_PARSE_ERROR,
            "sql2 DataFusion error: No SQL statements were provided in the query string",
        )
    })?;
    rewrite_postgresql_expressions(&mut statement);
    Ok(statement)
}

/// DataFusion 53 parses PostgreSQL JSON operators but does not plan them yet.
/// Lower the public PostgreSQL syntax to private execution functions before
/// either the read planner or bound-write planner sees the statement.
fn rewrite_postgresql_expressions(statement: &mut DataFusionStatement) {
    struct Rewriter;
    impl VisitorMut for Rewriter {
        type Break = ();

        fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
            if let Expr::Function(function) = expr {
                let is_current_timestamp = function
                    .name
                    .0
                    .last()
                    .and_then(|part| match part {
                        ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
                        ObjectNamePart::Function(_) => None,
                    })
                    .is_some_and(|name| name.eq_ignore_ascii_case("current_timestamp"));
                let no_args = matches!(function.args, FunctionArguments::None)
                    || matches!(&function.args, FunctionArguments::List(list) if list.args.is_empty());
                if is_current_timestamp && no_args {
                    *expr = private_function("__lix_current_timestamp", Vec::new());
                    return ControlFlow::Continue(());
                }
            }
            if let Expr::Cast {
                expr: inner,
                data_type: SqlDataType::JSONB,
                array: false,
                format: None,
                ..
            } = expr
            {
                let placeholder = Box::new(Expr::Value(Value::Boolean(false).into()));
                let inner = std::mem::replace(inner, placeholder);
                *expr = private_function("__lix_jsonb", vec![*inner]);
                return ControlFlow::Continue(());
            }
            let Expr::BinaryOp { left, op, right } = expr else {
                return ControlFlow::Continue(());
            };
            let name = match op {
                BinaryOperator::Arrow => "__lix_json_get",
                BinaryOperator::LongArrow => "__lix_json_get_text",
                BinaryOperator::HashArrow => "__lix_json_path_get",
                BinaryOperator::HashLongArrow => "__lix_json_path_get_text",
                BinaryOperator::AtArrow => "__lix_json_contains",
                BinaryOperator::Question => "__lix_json_exists",
                _ => return ControlFlow::Continue(()),
            };
            let placeholder = || Box::new(Expr::Value(Value::Boolean(false).into()));
            let left = std::mem::replace(left, placeholder());
            let right = std::mem::replace(right, placeholder());
            *expr = private_function(name, vec![*left, *right]);
            ControlFlow::Continue(())
        }
    }

    fn private_function(name: &str, args: Vec<Expr>) -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: args
                    .into_iter()
                    .map(|arg| FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)))
                    .collect(),
                clauses: Vec::new(),
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: Vec::new(),
        })
    }

    fn visit(statement: &mut DataFusionStatement, visitor: &mut Rewriter) {
        match statement {
            DataFusionStatement::Statement(statement) => {
                let _ = statement.visit(visitor);
            }
            DataFusionStatement::Explain(explain) => visit(explain.statement.as_mut(), visitor),
            _ => {}
        }
    }
    visit(statement, &mut Rewriter);
}

pub(super) fn reject_sql_hex_literals(tokens: &[TokenWithSpan]) -> Result<(), LixError> {
    if tokens
        .iter()
        .any(|token| matches!(token.token, Token::HexStringLiteral(_)))
    {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "SQL hex literals are not supported",
        )
        .with_hint(
            "Bind binary data directly, or use CAST($1 AS BYTEA) with a text parameter for UTF-8 content.",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_statement;
    use crate::LixError;

    #[test]
    fn parses_postgresql_numbered_parameters() {
        parse_statement("SELECT $1::TEXT, $2")
            .expect("PostgreSQL parameters and casts should parse");
    }

    #[test]
    fn parses_postgresql_values_table_expression() {
        parse_statement("SELECT value FROM (VALUES ($1)) AS selected(value)")
            .expect("PostgreSQL VALUES table expression should parse");
    }

    #[test]
    fn rejects_anonymous_parameters() {
        let error = parse_statement("SELECT ?").expect_err("anonymous parameters are unsupported");
        assert_eq!(error.code, LixError::CODE_PARSE_ERROR);
    }

    #[test]
    fn rejects_multi_statement_scripts() {
        let error = parse_statement("SELECT 1; SELECT 2")
            .expect_err("execute parses one statement, not a script");
        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert_eq!(
            error.message,
            "Lix SQL only supports one statement per execute() call"
        );
    }

    #[test]
    fn rejects_hex_literals_before_read_or_write_planning() {
        for sql in [
            "SELECT X'4142'",
            "EXPLAIN SELECT X'4142'",
            "INSERT INTO lix_file (path, content) VALUES ('/a.bin', X'4142')",
        ] {
            let error = parse_statement(sql).expect_err("hex literal should be rejected");
            assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL, "{sql}");
            assert_eq!(error.message, "SQL hex literals are not supported", "{sql}");
        }
    }
}
