use std::future::Future;
use std::sync::Arc;

use crate::telemetry::{
    ActiveTelemetrySpan, SQL_BATCH, SQL_COHERENT_READ_BATCH, SQL_QUERY, Status,
    TelemetryAttribute, TelemetrySink, TelemetrySpanStart,
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
        if !sink.enabled(&SQL_QUERY) {
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
    let (query_text, truncated, fingerprint) = sanitize_query_text_with_truncation(sql);
    let operation = query_operation(&query_text);
    let mut attributes = vec![
        TelemetryAttribute::string("db.system.name", "lix"),
        TelemetryAttribute::string("db.operation.name", operation.clone()),
        TelemetryAttribute::string("db.query.summary", operation.clone()),
        TelemetryAttribute::string("otel.name", operation),
        TelemetryAttribute::string("db.query.text", query_text),
        TelemetryAttribute::string("lix.sql.fingerprint", fingerprint),
        TelemetryAttribute::string("lix.execution.kind", execution_kind),
    ];
    if truncated {
        attributes.push(TelemetryAttribute::boolean(
            "lix.sql.query_text_truncated",
            true,
        ));
    }
    if let Some(batch_index) = batch_index {
        attributes.push(TelemetryAttribute::i64(
            "lix.batch.index",
            i64::try_from(batch_index).unwrap_or(i64::MAX),
        ));
    }
    TelemetrySpanStart::new(&SQL_QUERY, attributes)
}

fn statement_end(
    result: &Result<ExecuteResult, LixError>,
) -> (Status, Vec<TelemetryAttribute>) {
    match result {
        Ok(result) => (
            Status::Unset,
            vec![
                TelemetryAttribute::i64(
                    "db.response.returned_rows",
                    i64::try_from(result.len()).unwrap_or(i64::MAX),
                ),
                TelemetryAttribute::i64(
                    "lix.rows_affected",
                    i64::try_from(result.rows_affected()).unwrap_or(i64::MAX),
                ),
            ],
        ),
        Err(error) => (
            Status::error(error.code.clone()),
            vec![
                TelemetryAttribute::string("error.type", error.code.clone()),
            ],
        ),
    }
}

pub(crate) fn start_batch<'a>(
    sink: Option<&Arc<dyn TelemetrySink>>,
    descriptor: &'static crate::telemetry::TelemetrySpanDescriptor,
    statement_count: usize,
    statements: impl Iterator<Item = &'a str>,
) -> Option<ActiveTelemetrySpan> {
    let sink = sink?;
    let execution_kind = if descriptor == &SQL_BATCH {
        "batch"
    } else if descriptor == &SQL_COHERENT_READ_BATCH {
        "coherent_read_batch"
    } else {
        return None;
    };
    let enabled_descriptor = if statement_count == 1 {
        &SQL_QUERY
    } else {
        descriptor
    };
    if !sink.enabled(enabled_descriptor) {
        return None;
    }
    let mut statements = statements;
    if statement_count == 1 {
        let Some(sql) = statements.next() else {
            return None;
        };
        return Some(ActiveTelemetrySpan::start(
            sink,
            statement_start(sql, execution_kind, None),
        ));
    }

    let mut attributes = vec![
        TelemetryAttribute::string("db.system.name", "lix"),
        TelemetryAttribute::i64(
            "db.operation.batch.size",
            i64::try_from(statement_count).unwrap_or(i64::MAX),
        ),
        TelemetryAttribute::string("lix.execution.kind", execution_kind),
    ];
    let mut query_text = String::with_capacity(MAX_QUERY_TEXT_CHARS.min(512));
    let mut query_text_chars = 0;
    let mut query_text_truncated = false;
    let mut first_shape: Option<String> = None;
    let mut first_operation: Option<String> = None;
    let mut first_shape_truncated = false;
    let mut homogeneous_shape = true;
    let mut homogeneous_operation = true;
    let mut fingerprint = blake3::Hasher::new();

    for (index, sql) in statements.take(statement_count).enumerate() {
        let (shape, statement_truncated, shape_fingerprint) =
            sanitize_query_text_with_truncation(sql);
        let operation = query_operation(&shape);
        if index > 0 {
            fingerprint.update(b"; ");
            if homogeneous_shape && let Some(first) = &first_shape {
                if first != &shape {
                    homogeneous_shape = false;
                }
            }
            if let Some(first) = &first_operation {
                if first != &operation {
                    homogeneous_operation = false;
                }
            }
        }
        fingerprint.update(shape_fingerprint.as_bytes());
        if first_shape.is_none() {
            first_shape = Some(shape.clone());
            first_operation = Some(operation.clone());
            first_shape_truncated = statement_truncated;
        }
        query_text_truncated |= statement_truncated;
        if index > 0 {
            append_bounded(
                &mut query_text,
                &mut query_text_chars,
                "; ",
                &mut query_text_truncated,
            );
        }
        append_bounded(
            &mut query_text,
            &mut query_text_chars,
            &shape,
            &mut query_text_truncated,
        );
    }

    let operation = if statement_count == 0 {
        "BATCH".to_owned()
    } else if homogeneous_operation {
        first_operation.unwrap_or_else(|| "SQL".to_owned())
    } else {
        "BATCH".to_owned()
    };
    let batch_summary = if statement_count > 0 && homogeneous_operation {
        format!("BATCH {operation}")
    } else {
        "BATCH".to_owned()
    };
    attributes.extend([
        TelemetryAttribute::string("db.operation.name", batch_summary.clone()),
        TelemetryAttribute::string("db.query.summary", batch_summary.clone()),
        TelemetryAttribute::string("otel.name", batch_summary),
    ]);
    if statement_count > 0 {
        attributes.push(TelemetryAttribute::string(
            "lix.sql.fingerprint",
            fingerprint.finalize().to_hex().to_string(),
        ));
        let shape = if homogeneous_shape {
            first_shape.unwrap_or_default()
        } else {
            query_text
        };
        if !shape.is_empty() {
            attributes.push(TelemetryAttribute::string("db.query.text", shape));
        }
        let truncated = if homogeneous_shape {
            first_shape_truncated
        } else {
            query_text_truncated
        };
        if truncated {
            attributes.push(TelemetryAttribute::boolean(
                "lix.sql.query_text_truncated",
                true,
            ));
        }
    }
    Some(ActiveTelemetrySpan::start(
        sink,
        TelemetrySpanStart::new(descriptor, attributes),
    ))
}

pub(crate) fn finish_operation<T>(span: ActiveTelemetrySpan, result: &Result<T, LixError>) {
    match result {
        Ok(_) => span.finish(Status::Unset, Vec::new()),
        Err(error) => span.finish(
            Status::error(error.code.clone()),
            vec![
                TelemetryAttribute::string("error.type", error.code.clone()),
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
    sanitize_query_text_with_truncation(sql).0
}

fn sanitize_query_text_with_truncation(sql: &str) -> (String, bool, String) {
    let characters = sql.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(sql.len().min(MAX_QUERY_TEXT_CHARS));
    let mut output_chars = 0;
    let mut truncated = false;
    let mut fingerprint = blake3::Hasher::new();
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

        if !push_pending_space(
            &mut output,
            &mut output_chars,
            &mut pending_space,
            &mut fingerprint,
            &mut truncated,
        ) {
            break;
        }

        if character == '\'' {
            if !push_query_text_char(
                &mut output,
                &mut output_chars,
                '?',
                &mut fingerprint,
                &mut truncated,
            ) {
                break;
            }
            index = skip_single_quoted_literal(&characters, index + 1);
            continue;
        }
        if character == '$' {
            if characters.get(index + 1).is_some_and(char::is_ascii_digit) {
                if !push_query_text_char(
                    &mut output,
                    &mut output_chars,
                    '$',
                    &mut fingerprint,
                    &mut truncated,
                ) {
                    break;
                }
                index += 1;
                while index < characters.len() && characters[index].is_ascii_digit() {
                    if !push_query_text_char(
                        &mut output,
                        &mut output_chars,
                        characters[index],
                        &mut fingerprint,
                        &mut truncated,
                    ) {
                        break 'query;
                    }
                    index += 1;
                }
                continue;
            }
            if let Some((delimiter, body_start)) = dollar_quote_delimiter(&characters, index) {
                if !push_query_text_char(
                    &mut output,
                    &mut output_chars,
                    '?',
                    &mut fingerprint,
                    &mut truncated,
                ) {
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
            if !push_query_text_char(
                &mut output,
                &mut output_chars,
                '?',
                &mut fingerprint,
                &mut truncated,
            ) {
                break;
            }
            index = skip_numeric_literal(&characters, index + 1);
            continue;
        }
        if matches!(character, '"' | '`' | '[') {
            let closing = if character == '[' { ']' } else { character };
            if !push_query_text_char(
                &mut output,
                &mut output_chars,
                character,
                &mut fingerprint,
                &mut truncated,
            ) {
                break;
            }
            index += 1;
            while index < characters.len() {
                let current = characters[index];
                if !push_query_text_char(
                    &mut output,
                    &mut output_chars,
                    current,
                    &mut fingerprint,
                    &mut truncated,
                ) {
                    break 'query;
                }
                index += 1;
                if current == closing {
                    if characters.get(index) == Some(&closing) {
                        if !push_query_text_char(
                            &mut output,
                            &mut output_chars,
                            closing,
                            &mut fingerprint,
                            &mut truncated,
                        ) {
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

        if !push_query_text_char(
            &mut output,
            &mut output_chars,
            character,
            &mut fingerprint,
            &mut truncated,
        ) {
            break;
        }
        index += 1;
    }

    (
        output.trim().to_string(),
        truncated,
        fingerprint.finalize().to_hex().to_string(),
    )
}

fn append_bounded(
    output: &mut String,
    output_chars: &mut usize,
    value: &str,
    truncated: &mut bool,
) {
    let remaining = MAX_QUERY_TEXT_CHARS.saturating_sub(*output_chars);
    if remaining == 0 {
        *truncated |= !value.is_empty();
        return;
    }
    let mut chars = value.chars();
    for character in chars.by_ref().take(remaining) {
        output.push(character);
        *output_chars += 1;
    }
    *truncated |= chars.next().is_some();
}

fn push_query_text_char(
    output: &mut String,
    output_chars: &mut usize,
    character: char,
    fingerprint: &mut blake3::Hasher,
    truncated: &mut bool,
) -> bool {
    fingerprint.update(character.encode_utf8(&mut [0; 4]).as_bytes());
    if *output_chars >= MAX_QUERY_TEXT_CHARS {
        *truncated = true;
        return true;
    }
    output.push(character);
    *output_chars += 1;
    true
}

fn push_pending_space(
    output: &mut String,
    output_chars: &mut usize,
    pending_space: &mut bool,
    fingerprint: &mut blake3::Hasher,
    truncated: &mut bool,
) -> bool {
    if *pending_space && !output.is_empty() && !output.ends_with(' ') {
        if !push_query_text_char(output, output_chars, ' ', fingerprint, truncated) {
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
    use std::sync::Mutex;

    fn batch_start(
        descriptor: &'static crate::telemetry::TelemetrySpanDescriptor,
        statement_count: usize,
        statements: &[&str],
    ) -> TelemetrySpanStart {
        let captured = Arc::new(Mutex::new(None));
        let capture = Arc::clone(&captured);
        let sink: Arc<dyn TelemetrySink> = Arc::new(crate::telemetry::CallbackTelemetrySink::new(
            move |span| *capture.lock().expect("captured span lock") = Some(span.start),
        ));
        start_batch(
            Some(&sink),
            descriptor,
            statement_count,
            statements.iter().copied(),
        )
        .expect("batch span")
        .finish(Status::Unset, Vec::new());
        let span = captured
            .lock()
            .expect("captured span lock")
            .take()
            .expect("callback span");
        span
    }

    fn string_attribute<'a>(span: &'a TelemetrySpanStart, key: &str) -> Option<&'a str> {
        span.attributes.iter().find_map(|attribute| {
            (attribute.key == key).then_some(&attribute.value).and_then(|value| match value {
                crate::telemetry::TelemetryValue::String(value) => Some(value.as_str()),
                _ => None,
            })
        })
    }

    #[test]
    fn absent_sink_disables_statement_telemetry() {
        assert!(SqlStatementTelemetry::start(None, "SELECT 'private'", "execute", None).is_none());
    }

    #[test]
    fn absent_sink_does_not_inspect_batch_statements() {
        let statements = std::iter::once_with(|| panic!("disabled telemetry inspected SQL"));
        assert!(start_batch(None, &SQL_BATCH, 1, statements).is_none());
    }

    #[test]
    fn coherent_read_batches_accept_query_shape_attributes() {
        let sink: Arc<dyn TelemetrySink> = Arc::new(
            crate::telemetry::CallbackTelemetrySink::new(|_| {}),
        );
        start_batch(
            Some(&sink),
            &SQL_COHERENT_READ_BATCH,
            1,
            std::iter::once("SELECT 'private' AS value"),
        )
        .expect("coherent read telemetry span")
        .finish(Status::Unset, Vec::new());
    }

    #[test]
    fn one_statement_batch_is_a_query_span_without_batch_size() {
        let span = batch_start(&SQL_BATCH, 1, &["SELECT 'private' AS value"]);
        assert_eq!(span.name, "lix.sql.query");
        assert_eq!(string_attribute(&span, "otel.name"), Some("SELECT"));
        assert_eq!(string_attribute(&span, "db.query.summary"), Some("SELECT"));
        assert_eq!(string_attribute(&span, "db.query.text"), Some("SELECT ? AS value"));
        assert!(!span
            .attributes
            .iter()
            .any(|attribute| attribute.key == "db.operation.batch.size"));
    }

    #[test]
    fn homogeneous_batches_use_batch_summary_and_single_query_shape() {
        let span = batch_start(
            &SQL_BATCH,
            2,
            &[
                "UPDATE item SET value = 'first' WHERE id = $1",
                "UPDATE item SET value = 'second' WHERE id = $1",
            ],
        );
        assert_eq!(span.name, "lix.sql.batch");
        assert_eq!(string_attribute(&span, "db.system.name"), Some("lix"));
        assert_eq!(string_attribute(&span, "otel.name"), Some("BATCH UPDATE"));
        assert_eq!(
            string_attribute(&span, "db.operation.name"),
            Some("BATCH UPDATE")
        );
        assert_eq!(
            string_attribute(&span, "db.query.text"),
            Some("UPDATE item SET value = ? WHERE id = $1")
        );
        assert!(span.attributes.iter().any(|attribute| {
            attribute.key == "db.operation.batch.size"
                && attribute.value == crate::telemetry::TelemetryValue::I64(2)
        }));
    }

    #[test]
    fn heterogeneous_batch_includes_joined_sanitized_query_text() {
        let span = batch_start(
            &SQL_BATCH,
            2,
            &["SELECT 'private'", "UPDATE item SET value = 42"],
        );
        assert_eq!(string_attribute(&span, "otel.name"), Some("BATCH"));
        assert_eq!(string_attribute(&span, "db.query.summary"), Some("BATCH"));
        assert_eq!(
            string_attribute(&span, "db.query.text"),
            Some("SELECT ?; UPDATE item SET value = ?")
        );
    }

    #[test]
    fn empty_batch_is_reported_without_fake_query_details() {
        let span = batch_start(&SQL_BATCH, 0, &[]);
        assert_eq!(string_attribute(&span, "otel.name"), Some("BATCH"));
        assert!(span.attributes.iter().any(|attribute| {
            attribute.key == "db.operation.batch.size"
                && attribute.value == crate::telemetry::TelemetryValue::I64(0)
        }));
        assert!(!span.attributes.iter().any(|attribute| {
            matches!(attribute.key, "db.query.text" | "lix.sql.fingerprint")
        }));
    }

    #[test]
    fn batch_query_text_is_bounded_and_marks_truncation() {
        let first = format!("SELECT '{}'", "a".repeat(MAX_QUERY_TEXT_CHARS));
        let second = "UPDATE item SET value = 'private'";
        let span = batch_start(&SQL_BATCH, 2, &[&first, second]);
        let text = string_attribute(&span, "db.query.text").expect("query text");
        assert_eq!(text.chars().count(), MAX_QUERY_TEXT_CHARS);
        assert!(span.attributes.iter().any(|attribute| {
            attribute.key == "lix.sql.query_text_truncated"
                && attribute.value == crate::telemetry::TelemetryValue::Boolean(true)
        }));
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

    #[test]
    fn fingerprint_covers_query_shape_past_exported_text_limit() {
        let prefix = format!("SELECT {}", "a".repeat(MAX_QUERY_TEXT_CHARS));
        let first = format!("{prefix} tail_a");
        let second = format!("{prefix} tail_b");
        assert_eq!(sanitize_query_text(&first), sanitize_query_text(&second));
        assert_ne!(statement_fingerprint(&first), statement_fingerprint(&second));
    }
}
