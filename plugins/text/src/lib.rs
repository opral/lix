// TODO: workaround wit_bindgen
#![expect(clippy::same_length_and_capacity)]

wit_bindgen::generate!({
    path: "../../packages/engine/wit",
    world: "plugin",
});

pub use crate::exports::lix::plugin::api::{DetectedChange, File, PluginError, Scalar};
use crate::exports::lix::plugin::api::{EntityState, Guest as Plugin};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use imara_diff::{Algorithm, Diff, InternedInput};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::OnceLock;

pub const LINE_SCHEMA_KEY: &str = "text_line";
pub const DOCUMENT_SCHEMA_KEY: &str = "text_document";
pub const DOCUMENT_ENTITY_PK: &str = "__document__";
const MANIFEST_JSON: &str = include_str!("../manifest.json");
const LINE_SCHEMA_JSON: &str = include_str!("../schema/text_line.json");
const DOCUMENT_SCHEMA_JSON: &str = include_str!("../schema/text_document.json");

static LINE_SCHEMA: OnceLock<Value> = OnceLock::new();
static DOCUMENT_SCHEMA: OnceLock<Value> = OnceLock::new();

#[derive(Clone, Copy, Debug)]
struct TextLinesPlugin;
#[cfg(target_family = "wasm")]
export!(TextLinesPlugin);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum LineEnding {
    None,
    Lf,
    Crlf,
}

impl LineEnding {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "",
            Self::Lf => "\n",
            Self::Crlf => "\r\n",
        }
    }

    fn marker_byte(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Lf => 1,
            Self::Crlf => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedLine {
    id: String,
    content: Vec<u8>,
    ending: LineEnding,
}

#[derive(Debug, Serialize)]
struct DocumentSnapshot<'a> {
    line_ids: &'a [String],
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DocumentSnapshotOwned {
    line_ids: Vec<String>,
}

#[derive(Debug)]
struct RenderRow {
    entity_pk: Vec<String>,
    schema_key: String,
    snapshot_content: Option<SnapshotContent>,
}

type SnapshotContent = BTreeMap<String, Scalar>;

impl Plugin for TextLinesPlugin {
    fn detect_changes(
        state: Vec<EntityState>,
        file: File,
    ) -> Result<Vec<DetectedChange>, PluginError> {
        let before = file_from_entity_state(state)?;
        detect_changes_from_files(before, file)
    }

    fn render(state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
        render_entity_state(empty_file(), state)
    }
}

fn detect_changes_from_files(
    before: Option<File>,
    after: File,
) -> Result<Vec<DetectedChange>, PluginError> {
    if let Some(previous) = before.as_ref() {
        if previous.data == after.data {
            return Ok(Vec::new());
        }
    }

    let before_lines = before
        .as_ref()
        .map(|file| parse_lines_with_ids(&file.data))
        .unwrap_or_default();
    let after_lines = if let Some(before_file) = before.as_ref() {
        parse_after_lines_with_histogram_matching(&before_lines, &before_file.data, &after.data)
    } else {
        parse_lines_with_ids(&after.data)
    };

    let before_ids = before_lines
        .iter()
        .map(|line| line.id.clone())
        .collect::<Vec<_>>();
    let after_ids = after_lines
        .iter()
        .map(|line| line.id.clone())
        .collect::<Vec<_>>();

    let before_id_set = before_ids.iter().cloned().collect::<HashSet<_>>();
    let after_id_set = after_ids.iter().cloned().collect::<HashSet<_>>();
    let mut changes = Vec::new();

    if before.is_some() {
        let mut removed_ids = HashSet::<String>::with_capacity(before_lines.len());
        for line in &before_lines {
            if after_id_set.contains(&line.id) {
                continue;
            }
            if removed_ids.insert(line.id.clone()) {
                changes.push(DetectedChange {
                    entity_pk: vec![line.id.clone()],
                    schema_key: LINE_SCHEMA_KEY.to_string(),
                    snapshot_content: None,
                    metadata: None,
                });
            }
        }
    }

    for line in &after_lines {
        if before_id_set.contains(&line.id) {
            continue;
        }
        changes.push(DetectedChange {
            entity_pk: vec![line.id.clone()],
            schema_key: LINE_SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content_from_json(
                &serialize_line_snapshot(line)?,
                "text line",
            )?),
            metadata: None,
        });
    }

    if before.is_none() || before_ids != after_ids {
        let snapshot = serde_json::to_string(&DocumentSnapshot {
            line_ids: &after_ids,
        })
        .map_err(|error| {
            PluginError::Internal(format!("failed to encode document snapshot: {error}"))
        })?;
        changes.push(DetectedChange {
            entity_pk: vec![DOCUMENT_ENTITY_PK.to_string()],
            schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content_from_json(&snapshot, "text document")?),
            metadata: None,
        });
    }

    Ok(changes)
}

fn render_entity_changes(file: File, changes: Vec<DetectedChange>) -> Result<Vec<u8>, PluginError> {
    render_rows(
        file,
        changes.into_iter().map(|change| RenderRow {
            entity_pk: change.entity_pk,
            schema_key: change.schema_key,
            snapshot_content: change.snapshot_content,
        }),
    )
}

fn render_entity_state(file: File, state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
    render_rows(
        file,
        state.into_iter().map(|row| RenderRow {
            entity_pk: row.entity_pk,
            schema_key: row.schema_key,
            snapshot_content: Some(row.snapshot_content),
        }),
    )
}

fn render_rows(
    file: File,
    rows: impl IntoIterator<Item = RenderRow>,
) -> Result<Vec<u8>, PluginError> {
    let rows = rows.into_iter().collect::<Vec<_>>();
    let expected_line_changes = rows
        .iter()
        .filter(|row| row.schema_key == LINE_SCHEMA_KEY)
        .count();
    let mut document_snapshot: Option<DocumentSnapshotOwned> = None;
    let mut document_tombstoned = false;
    let mut line_by_id = parse_lines_with_ids(&file.data)
        .into_iter()
        .map(|line| (line.id.clone(), line))
        .collect::<HashMap<_, _>>();
    line_by_id.reserve(expected_line_changes);
    let mut seen_line_change_ids = HashSet::<String>::with_capacity(expected_line_changes);

    for row in rows {
        if row.schema_key == LINE_SCHEMA_KEY {
            let entity_pk = single_entity_pk(row.entity_pk)?;
            if !seen_line_change_ids.insert(entity_pk.clone()) {
                return Err(PluginError::InvalidInput(
                    "duplicate text_line snapshot in render_changes input".to_string(),
                ));
            }

            match row.snapshot_content {
                Some(snapshot_content) => {
                    let snapshot_raw = snapshot_content_to_json(&snapshot_content, "text line")?;
                    let snapshot = parse_line_snapshot(&snapshot_raw, &entity_pk)?;
                    line_by_id.insert(
                        entity_pk.clone(),
                        ParsedLine {
                            id: entity_pk,
                            content: snapshot.content,
                            ending: snapshot.ending,
                        },
                    );
                }
                None => {
                    line_by_id.remove(&entity_pk);
                }
            }
            continue;
        }

        if row.schema_key == DOCUMENT_SCHEMA_KEY {
            let entity_pk = single_entity_pk(row.entity_pk)?;
            if entity_pk != DOCUMENT_ENTITY_PK {
                return Err(PluginError::InvalidInput(format!(
                    "document snapshot entity_pk must be '{DOCUMENT_ENTITY_PK}', got '{entity_pk}'"
                )));
            }

            match row.snapshot_content {
                Some(snapshot_content) => {
                    if document_snapshot.is_some() || document_tombstoned {
                        return Err(PluginError::InvalidInput(
                            "duplicate text_document snapshot in render_changes input".to_string(),
                        ));
                    }
                    let snapshot_raw =
                        snapshot_content_to_json(&snapshot_content, "text document")?;
                    let parsed = parse_document_snapshot(&snapshot_raw)?;
                    document_snapshot = Some(parsed);
                }
                None => {
                    if document_snapshot.is_some() || document_tombstoned {
                        return Err(PluginError::InvalidInput(
                            "duplicate text_document snapshot in render_changes input".to_string(),
                        ));
                    }
                    document_tombstoned = true;
                }
            }
        }
    }

    if document_tombstoned {
        return Ok(Vec::new());
    }

    let document_snapshot = document_snapshot.ok_or_else(|| {
        PluginError::InvalidInput(
            "missing text_document snapshot; render_changes requires full latest projection"
                .to_string(),
        )
    })?;

    let mut output = Vec::new();
    for line_id in document_snapshot.line_ids {
        let Some(line) = line_by_id.get(&line_id) else {
            return Err(PluginError::InvalidInput(format!(
                "document references missing text_line entity_pk '{line_id}'"
            )));
        };
        output.extend_from_slice(&line.content);
        output.extend_from_slice(line.ending.as_str().as_bytes());
    }

    Ok(output)
}

fn parse_document_snapshot(raw: &str) -> Result<DocumentSnapshotOwned, PluginError> {
    let parsed: DocumentSnapshotOwned = serde_json::from_str(raw).map_err(|error| {
        PluginError::InvalidInput(format!("invalid text_document snapshot_content: {error}"))
    })?;

    let mut seen = HashSet::new();
    for line_id in &parsed.line_ids {
        if line_id.is_empty() {
            return Err(PluginError::InvalidInput(
                "text_document.line_ids must not contain empty ids".to_string(),
            ));
        }
        if !seen.insert(line_id.clone()) {
            return Err(PluginError::InvalidInput(format!(
                "text_document.line_ids contains duplicate id '{line_id}'"
            )));
        }
    }

    Ok(parsed)
}

fn parse_line_snapshot(raw: &str, entity_pk: &str) -> Result<ParsedLine, PluginError> {
    let (content_base64, ending) = parse_line_snapshot_fields(raw).map_err(|error| {
        PluginError::InvalidInput(format!(
            "invalid text_line snapshot_content for entity_pk '{entity_pk}': {error}"
        ))
    })?;

    let content = base64_to_bytes(content_base64).map_err(|error| {
        PluginError::InvalidInput(format!(
            "invalid text_line.content_base64 for entity_pk '{entity_pk}': {error}"
        ))
    })?;
    let ending = parse_line_ending_literal(ending).map_err(|error| {
        PluginError::InvalidInput(format!(
            "invalid text_line.ending for entity_pk '{entity_pk}': {error}"
        ))
    })?;

    Ok(ParsedLine {
        id: entity_pk.to_string(),
        content,
        ending,
    })
}

#[expect(clippy::unnecessary_wraps)]
fn serialize_line_snapshot(line: &ParsedLine) -> Result<String, PluginError> {
    let content_base64 = bytes_to_base64(&line.content);
    let ending = line_ending_json_literal(line.ending);
    let mut encoded = String::with_capacity(
        LINE_SNAPSHOT_PREFIX.len()
            + content_base64.len()
            + LINE_SNAPSHOT_SEPARATOR.len()
            + ending.len()
            + LINE_SNAPSHOT_SUFFIX.len(),
    );
    encoded.push_str(LINE_SNAPSHOT_PREFIX);
    encoded.push_str(&content_base64);
    encoded.push_str(LINE_SNAPSHOT_SEPARATOR);
    encoded.push_str(ending);
    encoded.push_str(LINE_SNAPSHOT_SUFFIX);
    Ok(encoded)
}

fn parse_lines_with_ids(data: &[u8]) -> Vec<ParsedLine> {
    parse_lines_with_ids_from_split(split_lines(data))
}

fn parse_lines_with_ids_from_split(split: Vec<(Vec<u8>, LineEnding)>) -> Vec<ParsedLine> {
    let mut occurrence_by_key = HashMap::<[u8; 20], u32>::new();
    let mut lines = Vec::with_capacity(split.len());

    for (content, ending) in split {
        let fingerprint = line_fingerprint(&content, ending);
        let occurrence = occurrence_by_key.entry(fingerprint).or_insert(0);
        let id = format!("line:{}:{}", bytes_to_hex(&fingerprint), occurrence);
        *occurrence += 1;

        lines.push(ParsedLine {
            id,
            content,
            ending,
        });
    }

    lines
}

fn parse_after_lines_with_histogram_matching(
    before_lines: &[ParsedLine],
    before_data: &[u8],
    after_data: &[u8],
) -> Vec<ParsedLine> {
    let after_split = split_lines(after_data);

    let matching_pairs = compute_histogram_line_matching_pairs(before_data, after_data);

    let mut matched_after_to_before = HashMap::<usize, usize>::new();
    for (before_index, after_index) in matching_pairs {
        matched_after_to_before.insert(after_index, before_index);
    }

    let mut used_ids = before_lines
        .iter()
        .map(|line| line.id.clone())
        .collect::<HashSet<_>>();
    let mut occurrence_by_key = HashMap::<[u8; 20], u32>::new();
    let mut after_lines = Vec::with_capacity(after_split.len());

    for (after_index, (content, ending)) in after_split.into_iter().enumerate() {
        let fingerprint = line_fingerprint(&content, ending);
        let occurrence = occurrence_by_key.entry(fingerprint).or_insert(0);
        let canonical_occurrence = *occurrence;
        *occurrence += 1;

        let entity_pk = matched_after_to_before.get(&after_index).map_or_else(
            || {
                let canonical_entity_pk = format!(
                    "line:{}:{}",
                    bytes_to_hex(&fingerprint),
                    canonical_occurrence
                );
                allocate_inserted_line_id(&canonical_entity_pk, &used_ids)
            },
            |before_index| before_lines[*before_index].id.clone(),
        );
        used_ids.insert(entity_pk.clone());

        after_lines.push(ParsedLine {
            id: entity_pk,
            content,
            ending,
        });
    }

    after_lines
}

fn compute_histogram_line_matching_pairs(
    before_data: &[u8],
    after_data: &[u8],
) -> Vec<(usize, usize)> {
    let input = InternedInput::new(before_data, after_data);
    let mut diff = Diff::compute(Algorithm::Histogram, &input);
    diff.postprocess_lines(&input);

    let mut pairs = Vec::new();
    let mut before_pos = 0usize;
    let mut after_pos = 0usize;

    for hunk in diff.hunks() {
        let hunk_before_start = hunk.before.start as usize;
        let hunk_after_start = hunk.after.start as usize;
        let unchanged_before_len = hunk_before_start.saturating_sub(before_pos);
        let unchanged_after_len = hunk_after_start.saturating_sub(after_pos);
        let unchanged_len = unchanged_before_len.min(unchanged_after_len);

        for offset in 0..unchanged_len {
            pairs.push((before_pos + offset, after_pos + offset));
        }

        before_pos = hunk.before.end as usize;
        after_pos = hunk.after.end as usize;
    }

    let before_tail = input.before.len().saturating_sub(before_pos);
    let after_tail = input.after.len().saturating_sub(after_pos);
    let tail_len = before_tail.min(after_tail);
    for offset in 0..tail_len {
        pairs.push((before_pos + offset, after_pos + offset));
    }

    pairs
}

fn allocate_inserted_line_id(base: &str, used_ids: &HashSet<String>) -> String {
    if !used_ids.contains(base) {
        return base.to_string();
    }

    let mut suffix = 0u32;
    loop {
        let candidate = format!("{base}:ins:{suffix}");
        if !used_ids.contains(&candidate) {
            return candidate;
        }
        suffix += 1;
    }
}

fn split_lines(data: &[u8]) -> Vec<(Vec<u8>, LineEnding)> {
    if data.is_empty() {
        return Vec::new();
    }

    let mut lines = Vec::new();
    let mut start = 0usize;

    for index in 0..data.len() {
        if data[index] != b'\n' {
            continue;
        }

        if index > start && data[index - 1] == b'\r' {
            lines.push((data[start..index - 1].to_vec(), LineEnding::Crlf));
        } else {
            lines.push((data[start..index].to_vec(), LineEnding::Lf));
        }
        start = index + 1;
    }

    if start < data.len() {
        lines.push((data[start..].to_vec(), LineEnding::None));
    }

    lines
}

fn line_fingerprint(content: &[u8], ending: LineEnding) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(content);
    hasher.update([0xff, ending.marker_byte()]);
    let digest = hasher.finalize();
    let mut fingerprint = [0u8; 20];
    fingerprint.copy_from_slice(&digest);
    fingerprint
}

const LINE_SNAPSHOT_PREFIX: &str = "{\"content_base64\":\"";
const LINE_SNAPSHOT_SEPARATOR: &str = "\",\"ending\":\"";
const LINE_SNAPSHOT_SUFFIX: &str = "\"}";

fn parse_line_snapshot_fields(raw: &str) -> Result<(&str, &str), String> {
    let inner = raw
        .strip_prefix(LINE_SNAPSHOT_PREFIX)
        .and_then(|value| value.strip_suffix(LINE_SNAPSHOT_SUFFIX))
        .ok_or_else(|| "expected {\"content_base64\":\"...\",\"ending\":\"...\"}".to_string())?;
    inner
        .split_once(LINE_SNAPSHOT_SEPARATOR)
        .ok_or_else(|| "missing content_base64 or ending field".to_string())
}

fn line_ending_json_literal(ending: LineEnding) -> &'static str {
    match ending {
        LineEnding::None => "",
        LineEnding::Lf => "\\n",
        LineEnding::Crlf => "\\r\\n",
    }
}

fn parse_line_ending_literal(value: &str) -> Result<LineEnding, String> {
    match value {
        "" => Ok(LineEnding::None),
        "\\n" => Ok(LineEnding::Lf),
        "\\r\\n" => Ok(LineEnding::Crlf),
        _ => Err(
            "unsupported ending literal; expected \"\", \"\\\\n\", or \"\\\\r\\\\n\"".to_string(),
        ),
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(hex_char(byte >> 4));
        output.push(hex_char(byte & 0x0f));
    }
    output
}

fn hex_char(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => unreachable!(),
    }
}

fn bytes_to_base64(bytes: &[u8]) -> String {
    BASE64_STANDARD.encode(bytes)
}

fn base64_to_bytes(raw: &str) -> Result<Vec<u8>, String> {
    BASE64_STANDARD
        .decode(raw)
        .map_err(|error| format!("invalid base64: {error}"))
}

fn snapshot_content_from_json(raw: &str, label: &str) -> Result<SnapshotContent, PluginError> {
    let value: Value = serde_json::from_str(raw).map_err(|error| {
        PluginError::Internal(format!("failed to parse {label} snapshot JSON: {error}"))
    })?;
    snapshot_content_from_value(value, label)
}

fn snapshot_content_from_value(value: Value, label: &str) -> Result<SnapshotContent, PluginError> {
    let Value::Object(object) = value else {
        return Err(PluginError::Internal(format!(
            "{label} snapshot must serialize to a JSON object"
        )));
    };

    object
        .into_iter()
        .map(|(key, value)| Ok((key, scalar_from_json_value(value)?)))
        .collect()
}

fn snapshot_content_to_json(
    snapshot_content: &SnapshotContent,
    label: &str,
) -> Result<String, PluginError> {
    let object = snapshot_content
        .iter()
        .map(|(key, value)| Ok((key.clone(), json_value_from_scalar(value, label)?)))
        .collect::<Result<Map<_, _>, _>>()?;
    serde_json::to_string(&Value::Object(object)).map_err(|error| {
        PluginError::Internal(format!("failed to encode {label} snapshot JSON: {error}"))
    })
}

fn scalar_from_json_value(value: Value) -> Result<Scalar, PluginError> {
    match value {
        Value::Null => Ok(Scalar::Nil),
        Value::Bool(value) => Ok(Scalar::Boolean(value)),
        Value::String(value) => Ok(Scalar::Text(value)),
        Value::Number(_) | Value::Array(_) | Value::Object(_) => serde_json::to_string(&value)
            .map(Scalar::Json)
            .map_err(|error| {
                PluginError::Internal(format!("failed to encode snapshot scalar JSON: {error}"))
            }),
    }
}

fn json_value_from_scalar(value: &Scalar, label: &str) -> Result<Value, PluginError> {
    match value {
        Scalar::Nil => Ok(Value::Null),
        Scalar::Boolean(value) => Ok(Value::Bool(*value)),
        Scalar::Number(value) => serde_json::Number::from_f64(*value)
            .map(Value::Number)
            .ok_or_else(|| {
                PluginError::InvalidInput(format!(
                    "{label} snapshot contains NaN or infinite number"
                ))
            }),
        Scalar::Text(value) => Ok(Value::String(value.clone())),
        Scalar::Json(value) => serde_json::from_str(value).map_err(|error| {
            PluginError::InvalidInput(format!(
                "{label} snapshot contains invalid JSON scalar: {error}"
            ))
        }),
    }
}

fn file_from_state_context(state: Vec<DetectedChange>) -> Result<Option<File>, PluginError> {
    if state.is_empty() {
        return Ok(None);
    }

    Ok(Some(File {
        data: render_entity_changes(empty_file(), state)?,
    }))
}

fn file_from_entity_state(state: Vec<EntityState>) -> Result<Option<File>, PluginError> {
    if state.is_empty() {
        return Ok(None);
    }

    Ok(Some(File {
        data: render_entity_state(empty_file(), state)?,
    }))
}

fn single_entity_pk(mut entity_pk: Vec<String>) -> Result<String, PluginError> {
    validate_single_entity_pk(&entity_pk)?;
    Ok(entity_pk.remove(0))
}

fn validate_single_entity_pk(entity_pk: &[String]) -> Result<(), PluginError> {
    if entity_pk.len() != 1 {
        return Err(PluginError::InvalidInput(format!(
            "expected single-component entity_pk, got {} components",
            entity_pk.len()
        )));
    }
    Ok(())
}

fn empty_file() -> File {
    File { data: Vec::new() }
}

pub fn detect_changes(
    before: Option<File>,
    after: File,
) -> Result<Vec<DetectedChange>, PluginError> {
    detect_changes_from_files(before, after)
}

pub fn detect_changes_with_state_context(
    before: Option<File>,
    after: File,
    state_context: Option<Vec<DetectedChange>>,
) -> Result<Vec<DetectedChange>, PluginError> {
    match state_context {
        Some(state) => {
            let before = file_from_state_context(state)?;
            detect_changes_from_files(before, after)
        }
        None => detect_changes_from_files(before, after),
    }
}

pub fn render(state_context: Vec<DetectedChange>) -> Result<Vec<u8>, PluginError> {
    render_entity_changes(empty_file(), state_context)
}

pub fn render_changes(file: File, changes: Vec<DetectedChange>) -> Result<Vec<u8>, PluginError> {
    render_entity_changes(file, changes)
}

pub fn manifest_json() -> &'static str {
    MANIFEST_JSON
}

pub fn line_schema_json() -> &'static str {
    LINE_SCHEMA_JSON
}

pub fn line_schema_definition() -> &'static Value {
    LINE_SCHEMA.get_or_init(|| {
        serde_json::from_str(LINE_SCHEMA_JSON).expect("text line schema must parse")
    })
}

pub fn document_schema_json() -> &'static str {
    DOCUMENT_SCHEMA_JSON
}

pub fn document_schema_definition() -> &'static Value {
    DOCUMENT_SCHEMA.get_or_init(|| {
        serde_json::from_str(DOCUMENT_SCHEMA_JSON).expect("text document schema must parse")
    })
}
