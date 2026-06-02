use datafusion::sql::parser::{DFParserBuilder, Statement as DataFusionStatement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use serde_json::json;

use crate::LixError;

pub(crate) fn parse_statement(sql: &str) -> Result<DataFusionStatement, LixError> {
    let dialect = GenericDialect {};
    let mut next_index = 1usize;
    let mut has_anonymous = false;
    let mut explicit_placeholders = Vec::new();

    let mut tokens = Vec::new();
    Tokenizer::new(&dialect, sql)
        .tokenize_with_location_into_buf_with_mapper(&mut tokens, |mut token_span| {
            if let Token::Placeholder(placeholder) = &token_span.token {
                if placeholder == "?" {
                    has_anonymous = true;
                    token_span.token = Token::Placeholder(format!("${next_index}"));
                    next_index += 1;
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

    statements.pop_front().ok_or_else(|| {
        LixError::new(
            LixError::CODE_PARSE_ERROR,
            "sql2 DataFusion error: No SQL statements were provided in the query string",
        )
    })
}
