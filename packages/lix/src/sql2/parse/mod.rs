use datafusion::sql::parser::{DFParserBuilder, Statement as DataFusionStatement};
use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};

use crate::LixError;

pub(crate) fn parse_statement(sql: &str) -> Result<DataFusionStatement, LixError> {
    let dialect = super::dialect::lix_sql_dialect();
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .map_err(|error| {
            LixError::new(
                LixError::CODE_PARSE_ERROR,
                format!("sql2 SQL tokenize error: {error}"),
            )
        })?;

    reject_sql_hex_literals(&tokens)?;

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
