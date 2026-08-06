use datafusion::sql::parser::{DFParserBuilder, Statement as DataFusionStatement};
use datafusion::sql::sqlparser::ast::{BeginTransactionKind, Statement as SqlStatement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};
use serde_json::json;
use std::ops::Range;

use crate::LixError;

/// One executable statement in a parsed SQL transaction script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlScriptStatement {
    /// Canonical SQL generated from the parsed statement.
    pub sql: String,
    /// The request-wide parameter range to bind to this statement.
    pub params: Range<usize>,
}

/// Atomic execution plan produced by [`parse_sql_script`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlScriptPlan {
    /// One or more statements that must execute atomically.
    pub statements: Vec<SqlScriptStatement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionControl {
    None,
    Begin,
    Commit,
    Unsupported,
}

#[derive(Debug, Clone, Copy, Default)]
struct PlaceholderUsage {
    anonymous: usize,
    explicit_max: usize,
    has_explicit: bool,
}

#[derive(Debug, Clone)]
struct ParsedStatement {
    statement: DataFusionStatement,
    tokens: Vec<Token>,
}

/// Parses one or more SQL statements into an atomic execution plan.
///
/// Multi-statement scripts may be unwrapped, or wrapped by `BEGIN` (optionally
/// followed by `TRANSACTION`) and a final `COMMIT`. Transaction aliases,
/// modes, savepoints, rollback, nested controls, and empty transactions are
/// rejected. Anonymous placeholders consume request parameters sequentially;
/// numbered placeholders retain their request-wide positions.
pub fn parse_sql_script(sql: &str, provided_param_count: usize) -> Result<SqlScriptPlan, LixError> {
    let statements = parse_statements(sql)?;
    if statements.is_empty() {
        return Err(LixError::new(
            LixError::CODE_PARSE_ERROR,
            "No SQL statements were provided in the query string",
        ));
    }

    let controls = statements
        .iter()
        .map(transaction_control)
        .collect::<Vec<_>>();
    let executable = if controls.first() == Some(&TransactionControl::Begin) {
        if controls.last() != Some(&TransactionControl::Commit)
            || controls[1..controls.len().saturating_sub(1)]
                .iter()
                .any(|control| *control != TransactionControl::None)
        {
            return Err(unsupported_transaction_control());
        }
        let inner = &statements[1..statements.len().saturating_sub(1)];
        if inner.is_empty() {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "Lix SQL transactions must contain at least one executable statement",
            ));
        }
        inner
    } else {
        if controls
            .iter()
            .any(|control| *control != TransactionControl::None)
        {
            return Err(unsupported_transaction_control());
        }
        statements.as_slice()
    };

    build_atomic_plan(executable, provided_param_count)
}

fn parse_statements(sql: &str) -> Result<Vec<ParsedStatement>, LixError> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .map_err(tokenizer_error)?;
    let statement_tokens = split_statement_tokens(&tokens);
    let statements = DFParserBuilder::new(tokens)
        .with_dialect(&dialect)
        .build()
        .map_err(super::error::datafusion_error_to_lix_error)?
        .parse_statements()
        .map_err(super::error::datafusion_error_to_lix_error)?;
    if statements.len() != statement_tokens.len() {
        if statement_tokens
            .iter()
            .any(|tokens| starts_with_transaction_control(tokens))
        {
            return Err(unsupported_transaction_control());
        }
        return Err(LixError::new(
            LixError::CODE_PARSE_ERROR,
            "Lix could not align parsed SQL statements with their source tokens",
        ));
    }
    Ok(statements
        .into_iter()
        .zip(statement_tokens)
        .map(|(statement, tokens)| ParsedStatement { statement, tokens })
        .collect())
}

fn split_statement_tokens(tokens: &[TokenWithSpan]) -> Vec<Vec<Token>> {
    let mut statements = Vec::new();
    let mut current = Vec::new();
    for token in tokens {
        match &token.token {
            Token::SemiColon => {
                if !current.is_empty() {
                    statements.push(std::mem::take(&mut current));
                }
            }
            Token::Whitespace(_) | Token::EOF => {}
            token => current.push(token.clone()),
        }
    }
    if !current.is_empty() {
        statements.push(current);
    }
    statements
}

fn build_atomic_plan(
    statements: &[ParsedStatement],
    provided_param_count: usize,
) -> Result<SqlScriptPlan, LixError> {
    let statements = statements
        .iter()
        .map(|statement| {
            let sql = statement.statement.to_string();
            let placeholders = placeholder_usage(&statement.tokens);
            (sql, placeholders)
        })
        .collect::<Vec<_>>();
    let anonymous_count = statements
        .iter()
        .map(|(_, placeholders)| placeholders.anonymous)
        .sum::<usize>();
    let has_explicit = statements
        .iter()
        .any(|(_, placeholders)| placeholders.has_explicit);
    if anonymous_count > 0 && has_explicit {
        return Err(LixError::new(
            LixError::CODE_PARSE_ERROR,
            "SQL mixes anonymous and explicit parameter placeholders",
        )
        .with_hint(
            "Use either anonymous placeholders like ?, ? or numbered placeholders like $1, $2, but not both.",
        )
        .with_details(json!({ "operation": "execute" })));
    }

    let expected_param_count = if anonymous_count > 0 {
        anonymous_count
    } else {
        statements
            .iter()
            .map(|(_, placeholders)| placeholders.explicit_max)
            .max()
            .unwrap_or(0)
    };
    if provided_param_count != expected_param_count {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!(
                "SQL expected {expected_param_count} parameter(s), but {provided_param_count} parameter(s) were provided"
            ),
        )
        .with_details(json!({
            "operation": "execute",
            "expected_param_count": expected_param_count,
            "provided_param_count": provided_param_count,
        })));
    }

    let mut anonymous_offset = 0usize;
    let statements = statements
        .into_iter()
        .map(|(sql, placeholders)| {
            let params = if anonymous_count > 0 {
                let start = anonymous_offset;
                anonymous_offset += placeholders.anonymous;
                start..anonymous_offset
            } else {
                0..placeholders.explicit_max
            };
            SqlScriptStatement { sql, params }
        })
        .collect();
    Ok(SqlScriptPlan { statements })
}

fn placeholder_usage(tokens: &[Token]) -> PlaceholderUsage {
    let mut usage = PlaceholderUsage::default();
    for token in tokens {
        let Token::Placeholder(placeholder) = token else {
            continue;
        };
        if placeholder == "?" {
            usage.anonymous += 1;
            continue;
        }
        let Some(index) = placeholder
            .strip_prefix('$')
            .and_then(|value| value.parse::<usize>().ok())
        else {
            continue;
        };
        usage.has_explicit = true;
        usage.explicit_max = usage.explicit_max.max(index);
    }
    usage
}

fn transaction_control(statement: &ParsedStatement) -> TransactionControl {
    let DataFusionStatement::Statement(parsed) = &statement.statement else {
        return TransactionControl::None;
    };
    match parsed.as_ref() {
        SqlStatement::StartTransaction {
            modes,
            begin: true,
            transaction,
            modifier: None,
            statements,
            exception: None,
            has_end_keyword: false,
        } if is_supported_begin(&statement.tokens)
            && modes.is_empty()
            && statements.is_empty()
            && matches!(transaction, None | Some(BeginTransactionKind::Transaction)) =>
        {
            TransactionControl::Begin
        }
        SqlStatement::Commit {
            chain: false,
            end: false,
            modifier: None,
        } if is_exact_words(&statement.tokens, &["COMMIT"]) => TransactionControl::Commit,
        SqlStatement::StartTransaction { .. }
        | SqlStatement::Commit { .. }
        | SqlStatement::Rollback { .. }
        | SqlStatement::Savepoint { .. }
        | SqlStatement::ReleaseSavepoint { .. } => TransactionControl::Unsupported,
        _ if starts_with_transaction_control(&statement.tokens) => TransactionControl::Unsupported,
        _ => TransactionControl::None,
    }
}

fn is_supported_begin(tokens: &[Token]) -> bool {
    is_exact_words(tokens, &["BEGIN"]) || is_exact_words(tokens, &["BEGIN", "TRANSACTION"])
}

fn is_exact_words(tokens: &[Token], expected: &[&str]) -> bool {
    tokens.len() == expected.len()
        && tokens.iter().zip(expected).all(|(token, expected)| {
            matches!(token, Token::Word(word) if word.value.eq_ignore_ascii_case(expected))
        })
}

fn starts_with_transaction_control(tokens: &[Token]) -> bool {
    matches!(
        tokens.first(),
        Some(Token::Word(word))
            if matches!(
                word.value.to_ascii_uppercase().as_str(),
                "BEGIN" | "START" | "COMMIT" | "END" | "ROLLBACK" | "SAVEPOINT" | "RELEASE"
            )
    )
}

fn unsupported_transaction_control() -> LixError {
    LixError::new(
        LixError::CODE_UNSUPPORTED_SQL,
        "Transaction control must either be omitted or wrap the complete script as BEGIN ... COMMIT",
    )
    .with_hint(
        "Use plain BEGIN or BEGIN TRANSACTION followed by executable statements and a final COMMIT.",
    )
}

fn tokenizer_error(error: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_PARSE_ERROR,
        format!("sql2 SQL tokenize error: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn atomic(sql: &str, params: usize) -> Vec<SqlScriptStatement> {
        parse_sql_script(sql, params)
            .expect("script parses")
            .statements
    }

    #[test]
    fn plans_single_statement_atomically() {
        let statements = atomic(" SELECT $1; ", 1);
        assert_eq!(
            statements,
            vec![SqlScriptStatement {
                sql: "SELECT $1".to_string(),
                params: 0..1,
            }]
        );
    }

    #[test]
    fn parses_explicit_and_implicit_atomic_scripts() {
        let implicit = atomic("SELECT 1; SELECT 2", 0);
        let explicit = atomic("bEgIn TrAnSaCtIoN; SELECT 1; SELECT 2; cOmMiT", 0);
        assert_eq!(implicit, explicit);
        assert_eq!(implicit.len(), 2);
    }

    #[test]
    fn plans_request_wide_numbered_parameters() {
        let statements = atomic("SELECT $1; SELECT $2, $1", 2);
        assert_eq!(statements[0].params, 0..1);
        assert_eq!(statements[1].params, 0..2);
    }

    #[test]
    fn plans_sequential_anonymous_parameters() {
        let statements = atomic("SELECT ?, ?; SELECT ?", 3);
        assert_eq!(statements[0].params, 0..2);
        assert_eq!(statements[1].params, 2..3);
    }

    #[test]
    fn parser_ignores_delimiters_and_placeholders_in_sql_literals_and_comments() {
        let statements = atomic(
            r#"
                SELECT '; ? $9' AS value, 1 AS "semi;?;$8", $tag$?; $7$tag$ AS tagged;
                -- ; ? $6
                SELECT ? /* ; ? $5 */ AS bound
            "#,
            1,
        );
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0].params, 0..0);
        assert_eq!(statements[1].params, 0..1);
    }

    #[test]
    fn rejects_unsupported_transaction_boundaries() {
        for sql in [
            "BEGIN; COMMIT",
            "BEGIN IMMEDIATE; SELECT 1; COMMIT",
            "BEGIN WORK; SELECT 1; COMMIT",
            "START TRANSACTION; SELECT 1; COMMIT",
            "BEGIN; SELECT 1; END",
            "BEGIN; SELECT 1; COMMIT TRANSACTION",
            "BEGIN; SELECT 1; COMMIT WORK",
            "SELECT 1; COMMIT",
            "BEGIN; SELECT 1",
            "BEGIN; BEGIN; SELECT 1; COMMIT; COMMIT",
            "BEGIN; SAVEPOINT s; SELECT 1; COMMIT",
            "BEGIN; ROLLBACK; COMMIT",
        ] {
            assert_eq!(
                parse_sql_script(sql, 0)
                    .expect_err("control is unsupported")
                    .code,
                LixError::CODE_UNSUPPORTED_SQL,
                "{sql}"
            );
        }
    }

    #[test]
    fn rejects_mixed_or_mismatched_parameters() {
        assert_eq!(
            parse_sql_script("SELECT ?; SELECT $2", 2)
                .expect_err("mixed placeholders are invalid")
                .code,
            LixError::CODE_PARSE_ERROR
        );
        assert_eq!(
            parse_sql_script("SELECT $1; SELECT $2", 1)
                .expect_err("parameter count is invalid")
                .code,
            LixError::CODE_INVALID_PARAM
        );
        assert_eq!(
            parse_sql_script("SELECT ?; SELECT ?", 1)
                .expect_err("parameter count is invalid")
                .code,
            LixError::CODE_INVALID_PARAM
        );
    }
}
