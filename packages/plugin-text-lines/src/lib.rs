use crate::exports::lix::plugin::api::{EntityChange, File, Guest, PluginError};
use imara_diff::{Algorithm, Diff, InternedInput};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet};

wit_bindgen::generate!({
    path: "../engine/wit",
    world: "plugin",
});

pub const LINE_SCHEMA_KEY: &str = "text_line";
pub const DOCUMENT_SCHEMA_KEY: &str = "text_document";
pub const SCHEMA_VERSION: &str = "1";
pub const DOCUMENT_ENTITY_ID: &str = "__document__";

pub use crate::exports::lix::plugin::api::{
    EntityChange as PluginEntityChange, File as PluginFile, PluginError as PluginApiError,
};

struct TextLinesPlugin;

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

    fn from_str(value: &str) -> Result<Self, PluginError> {
        match value {
            "" => Ok(Self::None),
            "\n" => Ok(Self::Lf),
            "\r\n" => Ok(Self::Crlf),
            _ => Err(PluginError::InvalidInput(format!(
                "unsupported line ending '{value}', expected one of '', '\\n', '\\r\\n'"
            ))),
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
    entity_id: String,
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

#[derive(Debug, Serialize)]
struct LineSnapshot<'a> {
    content_hex: &'a str,
    ending: &'a str,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LineSnapshotOwned {
    content_hex: String,
    ending: String,
}

impl Guest for TextLinesPlugin {
    fn detect_changes(before: Option<File>, after: File) -> Result<Vec<EntityChange>, PluginError> {
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
            .map(|line| line.entity_id.clone())
            .collect::<Vec<_>>();
        let after_ids = after_lines
            .iter()
            .map(|line| line.entity_id.clone())
            .collect::<Vec<_>>();

        let before_id_set = before_ids.iter().cloned().collect::<HashSet<_>>();
        let after_id_set = after_ids.iter().cloned().collect::<HashSet<_>>();
        let mut changes = Vec::new();

        if before.is_some() {
            let mut removed_ids = HashSet::<String>::with_capacity(before_lines.len());
            for line in &before_lines {
                if after_id_set.contains(&line.entity_id) {
                    continue;
                }
                if removed_ids.insert(line.entity_id.clone()) {
                    changes.push(EntityChange {
                        entity_id: line.entity_id.clone(),
                        schema_key: LINE_SCHEMA_KEY.to_string(),
                        schema_version: SCHEMA_VERSION.to_string(),
                        snapshot_content: None,
                    });
                }
            }
        }

        for line in &after_lines {
            if before_id_set.contains(&line.entity_id) {
                continue;
            }
            changes.push(EntityChange {
                entity_id: line.entity_id.clone(),
                schema_key: LINE_SCHEMA_KEY.to_string(),
                schema_version: SCHEMA_VERSION.to_string(),
                snapshot_content: Some(serialize_line_snapshot(line)?),
            });
        }

        if before.is_none() || before_ids != after_ids {
            let snapshot = serde_json::to_string(&DocumentSnapshot {
                line_ids: &after_ids,
            })
            .map_err(|error| {
                PluginError::Internal(format!("failed to encode document snapshot: {error}"))
            })?;
            changes.push(EntityChange {
                entity_id: DOCUMENT_ENTITY_ID.to_string(),
                schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
                schema_version: SCHEMA_VERSION.to_string(),
                snapshot_content: Some(snapshot),
            });
        }

        Ok(changes)
    }

    fn apply_changes(file: File, changes: Vec<EntityChange>) -> Result<Vec<u8>, PluginError> {
        let mut document_snapshot: Option<DocumentSnapshotOwned> = None;
        let mut document_tombstoned = false;
        let mut line_by_id = parse_lines_with_ids(&file.data)
            .into_iter()
            .map(|line| (line.entity_id.clone(), line))
            .collect::<HashMap<_, _>>();
        let mut seen_line_change_ids = HashSet::<String>::new();

        for change in changes {
            if change.schema_key == LINE_SCHEMA_KEY {
                validate_schema_version(&change.schema_version, LINE_SCHEMA_KEY)?;
                if !seen_line_change_ids.insert(change.entity_id.clone()) {
                    return Err(PluginError::InvalidInput(
                        "duplicate text_line snapshot in apply_changes input".to_string(),
                    ));
                }

                match change.snapshot_content {
                    Some(snapshot_raw) => {
                        let snapshot = parse_line_snapshot(&snapshot_raw, &change.entity_id)?;
                        line_by_id.insert(
                            change.entity_id.clone(),
                            ParsedLine {
                                entity_id: change.entity_id,
                                content: snapshot.content,
                                ending: snapshot.ending,
                            },
                        );
                    }
                    None => {
                        line_by_id.remove(&change.entity_id);
                    }
                }
                continue;
            }

            if change.schema_key == DOCUMENT_SCHEMA_KEY {
                validate_schema_version(&change.schema_version, DOCUMENT_SCHEMA_KEY)?;
                if change.entity_id != DOCUMENT_ENTITY_ID {
                    return Err(PluginError::InvalidInput(format!(
                        "document snapshot entity_id must be '{DOCUMENT_ENTITY_ID}', got '{}'",
                        change.entity_id
                    )));
                }

                match change.snapshot_content {
                    Some(snapshot_raw) => {
                        if document_snapshot.is_some() || document_tombstoned {
                            return Err(PluginError::InvalidInput(
                                "duplicate text_document snapshot in apply_changes input"
                                    .to_string(),
                            ));
                        }
                        let parsed = parse_document_snapshot(&snapshot_raw)?;
                        document_snapshot = Some(parsed);
                    }
                    None => {
                        if document_snapshot.is_some() || document_tombstoned {
                            return Err(PluginError::InvalidInput(
                                "duplicate text_document snapshot in apply_changes input"
                                    .to_string(),
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
                "missing text_document snapshot; apply_changes requires full latest projection"
                    .to_string(),
            )
        })?;

        let mut output = Vec::new();
        for line_id in document_snapshot.line_ids {
            let Some(line) = line_by_id.get(&line_id) else {
                return Err(PluginError::InvalidInput(format!(
                    "document references missing text_line entity_id '{line_id}'"
                )));
            };
            output.extend_from_slice(&line.content);
            output.extend_from_slice(line.ending.as_str().as_bytes());
        }

        Ok(output)
    }
}

fn validate_schema_version(value: &str, schema_key: &str) -> Result<(), PluginError> {
    if value == SCHEMA_VERSION {
        return Ok(());
    }
    Err(PluginError::InvalidInput(format!(
        "unsupported schema_version '{value}' for schema_key '{schema_key}', expected '{SCHEMA_VERSION}'"
    )))
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

fn parse_line_snapshot(raw: &str, entity_id: &str) -> Result<ParsedLine, PluginError> {
    let parsed: LineSnapshotOwned = serde_json::from_str(raw).map_err(|error| {
        PluginError::InvalidInput(format!(
            "invalid text_line snapshot_content for entity_id '{entity_id}': {error}"
        ))
    })?;

    let content = hex_to_bytes(&parsed.content_hex).map_err(|error| {
        PluginError::InvalidInput(format!(
            "invalid text_line.content_hex for entity_id '{entity_id}': {error}"
        ))
    })?;
    let ending = LineEnding::from_str(&parsed.ending)?;

    Ok(ParsedLine {
        entity_id: entity_id.to_string(),
        content,
        ending,
    })
}

fn serialize_line_snapshot(line: &ParsedLine) -> Result<String, PluginError> {
    let content_hex = bytes_to_hex(&line.content);
    serde_json::to_string(&LineSnapshot {
        content_hex: &content_hex,
        ending: line.ending.as_str(),
    })
    .map_err(|error| PluginError::Internal(format!("failed to encode text_line snapshot: {error}")))
}

fn parse_lines_with_ids(data: &[u8]) -> Vec<ParsedLine> {
    parse_lines_with_ids_from_split(split_lines(data))
}

fn parse_lines_with_ids_from_split(split: Vec<(Vec<u8>, LineEnding)>) -> Vec<ParsedLine> {
    let mut occurrence_by_key = HashMap::<Vec<u8>, u32>::new();
    let mut lines = Vec::with_capacity(split.len());

    for (content, ending) in split {
        let key = line_key_bytes(&content, ending);
        let occurrence = occurrence_by_key.entry(key.clone()).or_insert(0);
        let entity_id = format!("line:{}:{}", sha1_hex(&key), occurrence);
        *occurrence += 1;

        lines.push(ParsedLine {
            entity_id,
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
        .map(|line| line.entity_id.clone())
        .collect::<HashSet<_>>();
    let mut occurrence_by_key = HashMap::<Vec<u8>, u32>::new();
    let mut after_lines = Vec::with_capacity(after_split.len());

    for (after_index, (content, ending)) in after_split.into_iter().enumerate() {
        let key = line_key_bytes(&content, ending);
        let occurrence = occurrence_by_key.entry(key.clone()).or_insert(0);
        let canonical_occurrence = *occurrence;
        *occurrence += 1;

        let entity_id = if let Some(before_index) = matched_after_to_before.get(&after_index) {
            before_lines[*before_index].entity_id.clone()
        } else {
            let canonical_entity_id = format!("line:{}:{}", sha1_hex(&key), canonical_occurrence);
            allocate_inserted_line_id(&canonical_entity_id, &used_ids)
        };
        used_ids.insert(entity_id.clone());

        after_lines.push(ParsedLine {
            entity_id,
            content,
            ending,
        });
    }

    after_lines
}

fn compute_histogram_line_matching_pairs(before_data: &[u8], after_data: &[u8]) -> Vec<(usize, usize)> {
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

fn line_key_bytes(content: &[u8], ending: LineEnding) -> Vec<u8> {
    let mut key = Vec::with_capacity(content.len() + 2);
    key.extend_from_slice(content);
    key.push(0xff);
    key.push(ending.marker_byte());
    key
}

fn sha1_hex(bytes: &[u8]) -> String {
    let digest = Sha1::digest(bytes);
    bytes_to_hex(&digest)
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
        _ => '?',
    }
}

fn hex_to_bytes(raw: &str) -> Result<Vec<u8>, String> {
    if raw.len() % 2 != 0 {
        return Err("expected even-length hex string".to_string());
    }

    let mut out = Vec::with_capacity(raw.len() / 2);
    let bytes = raw.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        let high = from_hex_nibble(bytes[index]).ok_or_else(|| {
            format!(
                "invalid hex character '{}' at index {}",
                bytes[index] as char, index
            )
        })?;
        let low = from_hex_nibble(bytes[index + 1]).ok_or_else(|| {
            format!(
                "invalid hex character '{}' at index {}",
                bytes[index + 1] as char,
                index + 1
            )
        })?;
        out.push((high << 4) | low);
        index += 2;
    }

    Ok(out)
}

fn from_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

pub fn detect_changes(before: Option<File>, after: File) -> Result<Vec<EntityChange>, PluginError> {
    <TextLinesPlugin as Guest>::detect_changes(before, after)
}

pub fn apply_changes(file: File, changes: Vec<EntityChange>) -> Result<Vec<u8>, PluginError> {
    <TextLinesPlugin as Guest>::apply_changes(file, changes)
}

#[cfg(target_arch = "wasm32")]
export!(TextLinesPlugin);
