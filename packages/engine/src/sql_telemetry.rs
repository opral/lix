use std::future::Future;
use std::sync::Arc;

use crate::telemetry::{
    ActiveTelemetrySpan, TelemetryAttribute, TelemetrySink, TelemetrySpanKind, TelemetrySpanStart,
    TelemetrySpanStatus, unix_time_ms,
};
use crate::{ExecuteResult, LixError};

const MAX_QUERY_TEXT_CHARS: usize = 4_096;

pub(crate) struct SqlStatementTelemetry {
    span: ActiveTelemetrySpan,
}

impl SqlStatementTelemetry {
    pub(crate) fn start(
        sink: Option<&Arc<dyn TelemetrySink>>,
        sql: &str,
        execution_kind: &'static str,
        batch_index: Option<usize>,
    ) -> Option<Self> {
        let sink = sink?;
        if !sink.enabled(TelemetrySpanKind::SqlQuery) {
            return None;
        }
        Some(Self {
            span: ActiveTelemetrySpan::start(
                sink,
                statement_start(sql, execution_kind, batch_index),
            ),
        })
    }

    pub(crate) async fn instrument<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.span.instrument(future).await
    }

    pub(crate) fn finish(self, result: &Result<ExecuteResult, LixError>) {
        let (status, attributes) = statement_end(result);
        self.span.finish(status, attributes);
    }
}

fn statement_start(
    sql: &str,
    execution_kind: &'static str,
    batch_index: Option<usize>,
) -> TelemetrySpanStart {
    let query_text = sanitize_query_text(sql);
    let operation = query_operation(&query_text);
    let fingerprint = blake3::hash(query_text.as_bytes()).to_hex().to_string();
    let mut attributes = vec![
        TelemetryAttribute::string("otel.name", operation.clone()),
        TelemetryAttribute::string("otel.kind", "internal"),
        TelemetryAttribute::string("db.system.name", "lix"),
        TelemetryAttribute::string("db.operation.name", operation.clone()),
        TelemetryAttribute::string("db.query.summary", operation),
        TelemetryAttribute::string("db.query.text", query_text),
        TelemetryAttribute::string("lix.sql.fingerprint", fingerprint),
        TelemetryAttribute::string("lix.execution.kind", execution_kind),
    ];
    if let Some(batch_index) = batch_index {
        attributes.push(TelemetryAttribute::u64(
            "lix.batch.index",
            u64::try_from(batch_index).unwrap_or(u64::MAX),
        ));
    }
    TelemetrySpanStart {
        kind: TelemetrySpanKind::SqlQuery,
        name: "lix.sql.query",
        started_at_unix_ms: unix_time_ms(),
        attributes,
    }
}

fn statement_end(
    result: &Result<ExecuteResult, LixError>,
) -> (TelemetrySpanStatus, Vec<TelemetryAttribute>) {
    match result {
        Ok(result) => (
            TelemetrySpanStatus::Ok,
            vec![
                TelemetryAttribute::u64(
                    "db.response.returned_rows",
                    u64::try_from(result.len()).unwrap_or(u64::MAX),
                ),
                TelemetryAttribute::u64("lix.rows_affected", result.rows_affected()),
                TelemetryAttribute::string("otel.status_code", "OK"),
            ],
        ),
        Err(error) => (
            TelemetrySpanStatus::Error,
            vec![
                TelemetryAttribute::string("error.type", error.code.clone()),
                TelemetryAttribute::string("otel.status_code", "ERROR"),
            ],
        ),
    }
}

pub(crate) fn start_batch(
    sink: Option<&Arc<dyn TelemetrySink>>,
    kind: TelemetrySpanKind,
    size: usize,
) -> Option<ActiveTelemetrySpan> {
    let sink = sink?;
    if !sink.enabled(kind) {
        return None;
    }
    let (name, display_name, execution_kind) = match kind {
        TelemetrySpanKind::SqlBatch => ("lix.sql.batch", "SQL batch", "batch"),
        TelemetrySpanKind::SqlCoherentReadBatch => (
            "lix.sql.coherent_read_batch",
            "SQL coherent read batch",
            "coherent_read_batch",
        ),
        TelemetrySpanKind::SqlQuery => return None,
    };
    Some(ActiveTelemetrySpan::start(
        sink,
        TelemetrySpanStart {
            kind,
            name,
            started_at_unix_ms: unix_time_ms(),
            attributes: vec![
                TelemetryAttribute::string("otel.name", display_name),
                TelemetryAttribute::string("otel.kind", "internal"),
                TelemetryAttribute::string("db.system.name", "lix"),
                TelemetryAttribute::u64(
                    "db.operation.batch.size",
                    u64::try_from(size).unwrap_or(u64::MAX),
                ),
                TelemetryAttribute::string("lix.execution.kind", execution_kind),
            ],
        },
    ))
}

pub(crate) fn finish_operation<T>(span: ActiveTelemetrySpan, result: &Result<T, LixError>) {
    match result {
        Ok(_) => span.finish(
            TelemetrySpanStatus::Ok,
            vec![TelemetryAttribute::string("otel.status_code", "OK")],
        ),
        Err(error) => span.finish(
            TelemetrySpanStatus::Error,
            vec![
                TelemetryAttribute::string("error.type", error.code.clone()),
                TelemetryAttribute::string("otel.status_code", "ERROR"),
            ],
        ),
    }
}

#[cfg(test)]
fn statement_fingerprint(sql: &str) -> String {
    statement_start(sql, "execute", None)
        .attributes
        .into_iter()
        .find_map(|attribute| {
            if attribute.key == "lix.sql.fingerprint"
                && let crate::telemetry::TelemetryValue::String(value) = attribute.value
            {
                Some(value)
            } else {
                None
            }
        })
        .expect("statement telemetry includes a fingerprint")
}
fn query_operation(query_text: &str) -> String {
    const OPERATIONS: &[&str] = &[
        "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE",
        "EXPLAIN", "SHOW", "DESCRIBE", "SET",
    ];
    query_text
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .map(str::to_ascii_uppercase)
        .find(|token| OPERATIONS.contains(&token.as_str()))
        .unwrap_or_else(|| "SQL".to_string())
}

/// Removes SQL comments and literal values while preserving statement shape and
/// placeholders. If a construct is ambiguous, it is redacted rather than
/// copied so telemetry cannot become a side channel for query parameters.
fn sanitize_query_text(sql: &str) -> String {
    let characters = sql.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(sql.len().min(MAX_QUERY_TEXT_CHARS));
    let mut output_chars = 0;
    let mut index = 0;
    let mut pending_space = false;

    'query: while index < characters.len() {
        let character = characters[index];
        if character.is_whitespace() {
            pending_space = true;
            index += 1;
            continue;
        }
        if character == '-' && characters.get(index + 1) == Some(&'-') {
            index += 2;
            while index < characters.len() && characters[index] != '\n' {
                index += 1;
            }
            pending_space = true;
            continue;
        }
        if character == '/' && characters.get(index + 1) == Some(&'*') {
            index += 2;
            let mut depth = 1_u32;
            while index < characters.len() && depth > 0 {
                if characters.get(index) == Some(&'/') && characters.get(index + 1) == Some(&'*') {
                    depth = depth.saturating_add(1);
                    index += 2;
                } else if characters.get(index) == Some(&'*')
                    && characters.get(index + 1) == Some(&'/')
                {
                    depth -= 1;
                    index += 2;
                } else {
                    index += 1;
                }
            }
            pending_space = true;
            continue;
        }

        if !push_pending_space(&mut output, &mut output_chars, &mut pending_space) {
            break;
        }

        if character == '\'' {
            if !push_query_text_char(&mut output, &mut output_chars, '?') {
                break;
            }
            index = skip_single_quoted_literal(&characters, index + 1);
            continue;
        }
        if character == '$' {
            if characters.get(index + 1).is_some_and(char::is_ascii_digit) {
                if !push_query_text_char(&mut output, &mut output_chars, '$') {
                    break;
                }
                index += 1;
                while index < characters.len() && characters[index].is_ascii_digit() {
                    if !push_query_text_char(&mut output, &mut output_chars, characters[index]) {
                        break 'query;
                    }
                    index += 1;
                }
                continue;
            }
            if let Some((delimiter, body_start)) = dollar_quote_delimiter(&characters, index) {
                if !push_query_text_char(&mut output, &mut output_chars, '?') {
                    break;
                }
                index = skip_dollar_quoted_literal(&characters, body_start, &delimiter);
                continue;
            }
        }
        if character.is_ascii_digit()
            && !characters
                .get(index.wrapping_sub(1))
                .is_some_and(|previous| previous.is_ascii_alphanumeric() || *previous == '_')
        {
            if !push_query_text_char(&mut output, &mut output_chars, '?') {
                break;
            }
            index = skip_numeric_literal(&characters, index + 1);
            continue;
        }
        if matches!(character, '"' | '`' | '[') {
            let closing = if character == '[' { ']' } else { character };
            if !push_query_text_char(&mut output, &mut output_chars, character) {
                break;
            }
            index += 1;
            while index < characters.len() {
                let current = characters[index];
                if !push_query_text_char(&mut output, &mut output_chars, current) {
                    break 'query;
                }
                index += 1;
                if current == closing {
                    if characters.get(index) == Some(&closing) {
                        if !push_query_text_char(&mut output, &mut output_chars, closing) {
                            break 'query;
                        }
                        index += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }

        if !push_query_text_char(&mut output, &mut output_chars, character) {
            break;
        }
        index += 1;
    }

    output.trim().to_string()
}

fn push_query_text_char(output: &mut String, output_chars: &mut usize, character: char) -> bool {
    if *output_chars >= MAX_QUERY_TEXT_CHARS {
        return false;
    }
    output.push(character);
    *output_chars += 1;
    true
}

fn push_pending_space(
    output: &mut String,
    output_chars: &mut usize,
    pending_space: &mut bool,
) -> bool {
    if *pending_space && !output.is_empty() && !output.ends_with(' ') {
        if !push_query_text_char(output, output_chars, ' ') {
            return false;
        }
    }
    *pending_space = false;
    true
}

fn skip_single_quoted_literal(characters: &[char], mut index: usize) -> usize {
    while index < characters.len() {
        if characters[index] == '\'' {
            if characters.get(index + 1) == Some(&'\'') {
                index += 2;
                continue;
            }
            return index + 1;
        }
        if characters[index] == '\\' && index + 1 < characters.len() {
            index += 2;
        } else {
            index += 1;
        }
    }
    index
}

fn dollar_quote_delimiter(characters: &[char], start: usize) -> Option<(Vec<char>, usize)> {
    let mut index = start + 1;
    while index < characters.len()
        && (characters[index].is_ascii_alphanumeric() || characters[index] == '_')
    {
        index += 1;
    }
    if characters.get(index) != Some(&'$') {
        return None;
    }
    if index > start + 1 && characters[start + 1].is_ascii_digit() {
        return None;
    }
    Some((characters[start..=index].to_vec(), index + 1))
}

fn skip_dollar_quoted_literal(characters: &[char], mut index: usize, delimiter: &[char]) -> usize {
    while index + delimiter.len() <= characters.len() {
        if &characters[index..index + delimiter.len()] == delimiter {
            return index + delimiter.len();
        }
        index += 1;
    }
    characters.len()
}

fn skip_numeric_literal(characters: &[char], mut index: usize) -> usize {
    while index < characters.len()
        && matches!(characters[index], '0'..='9' | '.' | 'e' | 'E' | '+' | '-' | 'x' | 'X' | 'a'..='f' | 'A'..='F' | '_')
    {
        index += 1;
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_sink_disables_statement_telemetry() {
        assert!(SqlStatementTelemetry::start(None, "SELECT 'private'", "execute", None).is_none());
    }

    #[test]
    fn sanitizes_literals_comments_and_preserves_placeholders() {
        let sql = "SELECT value, 'private'' value', 42, $tag$secret$tag$ FROM lix_key_value -- hidden\n WHERE key = $1 AND other = ?";
        assert_eq!(
            sanitize_query_text(sql),
            "SELECT value, ?, ?, ? FROM lix_key_value WHERE key = $1 AND other = ?"
        );
    }

    #[test]
    fn preserves_quoted_identifiers() {
        assert_eq!(
            sanitize_query_text("SELECT \"odd table\".value FROM \"odd table\" WHERE id = 9"),
            "SELECT \"odd table\".value FROM \"odd table\" WHERE id = ?"
        );
    }

    #[test]
    fn caps_query_text_inside_quoted_identifiers() {
        let oversized_identifier = format!("SELECT \"{}", "a".repeat(MAX_QUERY_TEXT_CHARS * 2));
        let sanitized = sanitize_query_text(&oversized_identifier);

        assert_eq!(sanitized.chars().count(), MAX_QUERY_TEXT_CHARS);
        assert!(sanitized.starts_with("SELECT \""));
    }

    #[test]
    fn nested_comments_cannot_leak_text() {
        assert_eq!(
            sanitize_query_text("SELECT 1 /* outer /* private */ still-private */ FROM t"),
            "SELECT ? FROM t"
        );
    }

    #[test]
    fn fingerprint_depends_on_shape_not_literal_values() {
        let first = statement_fingerprint("SELECT * FROM t WHERE id = 1");
        let second = statement_fingerprint("SELECT * FROM t WHERE id = 999");
        assert_eq!(first, second);
    }
}
