// TODO: workaround wit_bindgen
#![expect(clippy::same_length_and_capacity)]

wit_bindgen::generate!({
    path: "../../packages/engine/wit",
    world: "plugin",
});

mod diff;
mod order_key;
pub mod schemas;
mod text;

use crate::diff::{Op, imara_diff_runs};
pub use crate::exports::lix::plugin::api::{DetectedChange, File, PluginError};
use crate::exports::lix::plugin::api::{EntityState, Guest as Plugin};
use crate::order_key::OrderKey;
use crate::text::{
    ParsedText, TextDocumentSnapshot, document_upsert_change, parse_document_snapshot, parse_file,
    render_projection,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::str;
use uuid::Uuid;

pub const ROOT_ENTITY_PK: &str = "root";
pub const DOCUMENT_SCHEMA_KEY: &str = schemas::DOCUMENT_SCHEMA_KEY;
pub const LINE_SCHEMA_KEY: &str = schemas::LINE_SCHEMA_KEY;

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");

#[derive(Clone, Copy, Debug)]
pub struct TextPlugin;
#[cfg(target_family = "wasm")]
export!(TextPlugin);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Projection {
    lines_by_id: BTreeMap<String, LineSnapshot>,
    document: TextDocumentSnapshot,
    document_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Line {
    id: String,
    order_key: OrderKey,
    line: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LineSnapshot {
    order_key: OrderKey,
    line: String,
}

impl Plugin for TextPlugin {
    fn detect_changes(
        state: Vec<EntityState>,
        file: File,
    ) -> Result<Vec<DetectedChange>, PluginError> {
        let before = Projection::from_entity_state(state.into_iter())?;
        let after = parse_file(&file);
        detect_changes_for_text(&before, &after)
    }

    fn render(state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
        let projection = Projection::from_entity_state(state.into_iter())?;
        Ok(render_projection(&projection))
    }
}

fn detect_changes_for_text(
    before: &Projection,
    after: &ParsedText,
) -> Result<Vec<DetectedChange>, PluginError> {
    let base = before.to_lines();
    let op_runs = imara_diff_runs(base.iter().map(|line| &line.line), after.lines.iter());
    let mut changes = Vec::new();
    let mut base_index = 0;
    let mut file_index = 0;
    let mut previous_order_key = None;

    for run in op_runs {
        match run.op {
            Op::Equal => {
                for _ in 0..run.len {
                    previous_order_key = Some(base[base_index].order_key);
                    base_index += 1;
                    file_index += 1;
                }
            }
            Op::Replace => {
                for _ in 0..run.len {
                    let line = &base[base_index];
                    changes.push(line_upsert_change(
                        &line.id,
                        line.order_key,
                        &after.lines[file_index],
                    )?);
                    previous_order_key = Some(line.order_key);
                    base_index += 1;
                    file_index += 1;
                }
            }
            Op::Delete => {
                for _ in 0..run.len {
                    changes.push(DetectedChange {
                        entity_pk: vec![base[base_index].id.clone()],
                        schema_key: LINE_SCHEMA_KEY.to_string(),
                        snapshot_content: None,
                        metadata: None,
                    });
                    base_index += 1;
                }
            }
            Op::Insert => {
                let next_order_key = base.get(base_index).map(|line| line.order_key);
                let order_keys =
                    OrderKey::evenly_between(previous_order_key, next_order_key, run.len);
                for order_key in order_keys {
                    let id = Uuid::now_v7().to_string();
                    changes.push(line_upsert_change(
                        &id,
                        order_key,
                        &after.lines[file_index],
                    )?);
                    previous_order_key = Some(order_key);
                    file_index += 1;
                }
            }
        }
    }

    if !before.document_present || before.document != after.document {
        changes.push(document_upsert_change(after.document)?);
    }

    Ok(changes)
}

fn single_entity_pk(mut entity_pk: Vec<String>) -> Result<String, PluginError> {
    if entity_pk.len() != 1 {
        return Err(PluginError::InvalidInput(format!(
            "expected single-component entity_pk, got {} components",
            entity_pk.len()
        )));
    }
    Ok(entity_pk.remove(0))
}

impl Projection {
    fn from_entity_state(changes: impl Iterator<Item = EntityState>) -> Result<Self, PluginError> {
        let mut lines_by_id = BTreeMap::new();
        let mut document = None;

        for change in changes {
            match change.schema_key.as_str() {
                DOCUMENT_SCHEMA_KEY => {
                    let entity_pk = single_entity_pk(change.entity_pk)?;
                    if entity_pk != ROOT_ENTITY_PK {
                        return Err(PluginError::InvalidInput(format!(
                            "unsupported entity_pk '{entity_pk}' for schema_key '{DOCUMENT_SCHEMA_KEY}', expected '{ROOT_ENTITY_PK}'"
                        )));
                    }
                    if document.is_some() {
                        return Err(PluginError::InvalidInput(format!(
                            "duplicate entity_pk '{ROOT_ENTITY_PK}' for schema_key '{DOCUMENT_SCHEMA_KEY}'"
                        )));
                    }
                    document = Some(parse_document_snapshot(&change.snapshot_content)?);
                }
                LINE_SCHEMA_KEY => {
                    let entity_pk = single_entity_pk(change.entity_pk)?;
                    match lines_by_id.entry(entity_pk) {
                        Entry::Occupied(entry) => {
                            return Err(PluginError::InvalidInput(format!(
                                "duplicate entity_pk '{}' for schema_key '{LINE_SCHEMA_KEY}'",
                                entry.key()
                            )));
                        }
                        Entry::Vacant(entry) => {
                            let line = parse_line_snapshot(&change.snapshot_content, entry.key())?;
                            entry.insert(line);
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(Self {
            lines_by_id,
            document: document.unwrap_or_default(),
            document_present: document.is_some(),
        })
    }

    fn to_lines(&self) -> Vec<Line> {
        let mut lines = self
            .lines_by_id
            .iter()
            .map(|(id, line)| Line {
                id: id.clone(),
                order_key: line.order_key,
                line: line.line.clone(),
            })
            .collect::<Vec<_>>();
        lines.sort_by(|a, b| a.order_key.cmp(&b.order_key).then_with(|| a.id.cmp(&b.id)));
        lines
    }
}

fn line_upsert_change(
    id: &str,
    order_key: OrderKey,
    line: &str,
) -> Result<DetectedChange, PluginError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "id": id,
        "order_key": order_key.to_snapshot_string(),
        "line": line,
    }))
    .map_err(|error| PluginError::Internal(format!("failed to serialize text line: {error}")))?;

    Ok(DetectedChange {
        entity_pk: vec![id.to_string()],
        schema_key: LINE_SCHEMA_KEY.to_string(),
        snapshot_content: Some(snapshot_content),
        metadata: None,
    })
}

fn parse_line_snapshot(raw: &str, entity_pk: &str) -> Result<LineSnapshot, PluginError> {
    let value: Value = serde_json::from_str(raw).map_err(|error| {
        PluginError::InvalidInput(format!(
            "invalid text line snapshot_content for entity_pk '{entity_pk}': {error}"
        ))
    })?;
    let object = value.as_object().ok_or_else(|| {
        PluginError::InvalidInput(format!(
            "text line snapshot_content for entity_pk '{entity_pk}' must be an object"
        ))
    })?;
    reject_unknown_fields(object.keys(), &["id", "order_key", "line"], "text line")?;

    let id = object.get("id").and_then(Value::as_str).ok_or_else(|| {
        PluginError::InvalidInput(format!(
            "text line snapshot for entity_pk '{entity_pk}' must contain string 'id'"
        ))
    })?;
    if id != entity_pk {
        return Err(PluginError::InvalidInput(format!(
            "text line snapshot id '{id}' does not match entity_pk '{entity_pk}'"
        )));
    }
    if id.is_empty() {
        return Err(PluginError::InvalidInput(format!(
            "text line snapshot id for entity_pk '{entity_pk}' must not be empty"
        )));
    }

    let order_key = parse_order_key_snapshot(object.get("order_key"), entity_pk)?;
    let line = object
        .get("line")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            PluginError::InvalidInput(format!(
                "text line snapshot for entity_pk '{entity_pk}' must contain string 'line'"
            ))
        })?;

    Ok(LineSnapshot { order_key, line })
}

fn reject_unknown_fields<'a>(
    keys: impl Iterator<Item = &'a String>,
    allowed: &[&str],
    label: &str,
) -> Result<(), PluginError> {
    for key in keys {
        if !allowed.contains(&key.as_str()) {
            return Err(PluginError::InvalidInput(format!(
                "{label} snapshot contains unsupported field '{key}'"
            )));
        }
    }
    Ok(())
}

fn parse_order_key_snapshot(
    value: Option<&Value>,
    entity_pk: &str,
) -> Result<OrderKey, PluginError> {
    let raw = value.and_then(Value::as_str).ok_or_else(|| {
        PluginError::InvalidInput(format!(
            "text line snapshot for entity_pk '{entity_pk}' must contain string 'order_key'"
        ))
    })?;

    OrderKey::from_snapshot_string(raw).map_err(|message| {
        PluginError::InvalidInput(format!(
            "invalid text line order_key for entity_pk '{entity_pk}': {message}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::text::TextLineEndings;
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};
    use std::collections::BTreeMap;

    #[test]
    fn fuzz_detect_changes_round_trips_lines() {
        let mut rng = SmallRng::seed_from_u64(0);

        for _ in 0..100_000 {
            let before_text = random_text(&mut rng);
            let after_text = random_text(&mut rng);
            let before = projection_from_text(before_text.clone());

            let changes = detect_changes_for_text(&before, &after_text).unwrap();

            let should_have_changes = before_text != after_text || !before.document_present;
            assert!(
                changes.is_empty() != should_have_changes,
                "changes emptiness did not match expected repair/diff behavior: changes={}, should_have_changes={should_have_changes}",
                changes.len()
            );

            let mut applied = before;
            for change in changes {
                apply_entity_change(&mut applied, change).unwrap();
            }

            let applied_lines = applied
                .to_lines()
                .into_iter()
                .map(|line| line.line)
                .collect::<Vec<_>>();
            assert_eq!(applied_lines, after_text.lines);
            assert_eq!(applied.document, after_text.document);
        }
    }

    fn random_text(rng: &mut (impl Rng + ?Sized)) -> ParsedText {
        let random_line_alphabet_len: u8 = rng.random_range(1..=6);
        let height = rng.random_range(0..=10);
        let lines = (0..height)
            .map(|_| {
                let width = rng.random_range(0..=8);
                (0..width)
                    .map(|_| {
                        let offset = rng.random_range(0..random_line_alphabet_len);
                        char::from(b'a' + offset)
                    })
                    .collect()
            })
            .collect::<Vec<_>>();
        let line_endings = match rng.random_range(0..3) {
            0 => TextLineEndings::Lf,
            1 => TextLineEndings::CrLf,
            _ => TextLineEndings::Cr,
        };

        ParsedText {
            document: TextDocumentSnapshot { line_endings },
            lines,
        }
    }

    fn projection_from_text(text: ParsedText) -> Projection {
        let document_present =
            text.document != TextDocumentSnapshot::default() || !text.lines.is_empty();
        let order_keys = OrderKey::evenly_between(None, None, text.lines.len());
        let lines_by_id = text
            .lines
            .into_iter()
            .zip(order_keys)
            .enumerate()
            .map(|(offset, (line, order_key))| {
                (format!("line:{offset}"), LineSnapshot { order_key, line })
            })
            .collect::<BTreeMap<_, _>>();

        Projection {
            lines_by_id,
            document: text.document,
            document_present,
        }
    }

    fn apply_entity_change(
        projection: &mut Projection,
        change: DetectedChange,
    ) -> Result<(), PluginError> {
        match change.schema_key.as_str() {
            DOCUMENT_SCHEMA_KEY => {
                if let Some(raw) = change.snapshot_content {
                    projection.document = parse_document_snapshot(&raw)?;
                    projection.document_present = true;
                } else {
                    projection.document = TextDocumentSnapshot::default();
                    projection.document_present = false;
                }
            }
            LINE_SCHEMA_KEY => {
                let entity_pk = single_entity_pk(change.entity_pk)?;
                if let Some(raw) = change.snapshot_content {
                    let line = parse_line_snapshot(&raw, &entity_pk)?;
                    projection.lines_by_id.insert(entity_pk, line);
                } else {
                    projection.lines_by_id.remove(&entity_pk);
                }
            }
            _ => {}
        }

        Ok(())
    }
}
