// TODO: workaround wit_bindgen
#![expect(clippy::same_length_and_capacity)]

wit_bindgen::generate!({
    path: "../../packages/engine/wit",
    world: "plugin",
});

mod diff;
pub mod schemas;
mod text;

use crate::diff::{DiffRun, imara_diff_runs};
pub use crate::exports::lix::plugin::api::{DetectedChange, File, PluginError};
use crate::exports::lix::plugin::api::{EntityState, Guest as Plugin};
use crate::text::{
    ParsedText, TextDocumentSnapshot, document_upsert_change, parse_document_snapshot, parse_file,
    render_projection,
};
use lix_order_key::OrderKey;
use serde_json::Value;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReplaceRun {
    old: Range<usize>,
    new: Range<usize>,
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
    if has_duplicate_order_keys(&base) {
        return detect_changes_for_text_with_reindexed_order(before, after, &base);
    }

    let (old_for_new, new_for_old) = match_diff_lines(
        &base,
        &after.lines,
        imara_diff_runs(base.iter().map(|line| &line.line), after.lines.iter()),
    );
    let inserted_ids = inserted_line_ids(&old_for_new);
    let mut changes = Vec::new();

    for base_index in (0..base.len()).filter(|index| new_for_old[*index].is_none()) {
        changes.push(DetectedChange {
            entity_pk: vec![base[base_index].id.clone()],
            schema_key: LINE_SCHEMA_KEY.to_string(),
            snapshot_content: None,
            metadata: None,
        });
    }
    detect_line_upsert_changes(
        &base,
        &after.lines,
        &old_for_new,
        &inserted_ids,
        &mut changes,
    )?;

    if !before.document_present || before.document != after.document {
        changes.push(document_upsert_change(after.document)?);
    }

    Ok(changes)
}

fn detect_changes_for_text_with_reindexed_order(
    before: &Projection,
    after: &ParsedText,
    base: &[Line],
) -> Result<Vec<DetectedChange>, PluginError> {
    let (old_for_new, new_for_old) = match_diff_lines(
        base,
        &after.lines,
        imara_diff_runs(base.iter().map(|line| &line.line), after.lines.iter()),
    );
    let inserted_ids = inserted_line_ids(&old_for_new);
    let planned_ids = (0..after.lines.len())
        .map(|new_index| line_id_for_new(base, &old_for_new, &inserted_ids, new_index).to_string())
        .collect::<Vec<_>>();
    let order_keys =
        OrderKey::evenly_between(None, None, planned_ids.len()).map_err(PluginError::Internal)?;
    let planned_id_set = planned_ids
        .iter()
        .collect::<std::collections::BTreeSet<_>>();
    let mut changes = Vec::new();

    for base_index in (0..base.len()).filter(|index| new_for_old[*index].is_none()) {
        let id = &base[base_index].id;
        if !planned_id_set.contains(id) {
            changes.push(DetectedChange {
                entity_pk: vec![id.clone()],
                schema_key: LINE_SCHEMA_KEY.to_string(),
                snapshot_content: None,
                metadata: None,
            });
        }
    }

    for ((id, line), order_key) in planned_ids
        .iter()
        .zip(after.lines.iter())
        .zip(order_keys.iter())
    {
        changes.push(line_upsert_change(id, order_key, line)?);
    }

    if !before.document_present || before.document != after.document {
        changes.push(document_upsert_change(after.document)?);
    }

    Ok(changes)
}

fn match_diff_lines(
    base: &[Line],
    file_lines: &[String],
    diff_runs: impl IntoIterator<Item = DiffRun>,
) -> (Vec<Option<usize>>, Vec<Option<usize>>) {
    let mut old_for_new = vec![None; file_lines.len()];
    let mut new_for_old = vec![None; base.len()];
    let mut replace_runs = Vec::new();
    let mut base_index = 0usize;
    let mut file_index = 0usize;

    for run in diff_runs {
        match run {
            DiffRun::Equal { len } => {
                for (old_index, new_index) in
                    (base_index..base_index + len).zip(file_index..file_index + len)
                {
                    old_for_new[new_index] = Some(old_index);
                    new_for_old[old_index] = Some(new_index);
                }
                base_index += len;
                file_index += len;
            }
            DiffRun::Replace { old, new } => {
                replace_runs.push(ReplaceRun {
                    old: base_index..base_index + old,
                    new: file_index..file_index + new,
                });
                base_index += old;
                file_index += new;
            }
        }
    }

    let old_replace_len = replace_runs.iter().map(|run| run.old.len()).sum();
    let mut old_lines_by_content = HashMap::<&str, Vec<usize>>::with_capacity(old_replace_len);
    for run in replace_runs.iter().rev() {
        for old_index in run.old.clone().rev() {
            old_lines_by_content
                .entry(base[old_index].line.as_str())
                .or_default()
                .push(old_index);
        }
    }

    for run in &replace_runs {
        for new_index in run.new.clone() {
            let Some(old_indices) = old_lines_by_content.get_mut(file_lines[new_index].as_str())
            else {
                continue;
            };
            let Some(old_index) = old_indices.pop() else {
                continue;
            };
            old_for_new[new_index] = Some(old_index);
            new_for_old[old_index] = Some(new_index);
        }
    }

    for run in &replace_runs {
        let mut old_index = run.old.start;
        let mut new_index = run.new.start;
        loop {
            while old_index < run.old.end && new_for_old[old_index].is_some() {
                old_index += 1;
            }
            while new_index < run.new.end && old_for_new[new_index].is_some() {
                new_index += 1;
            }
            if old_index == run.old.end || new_index == run.new.end {
                break;
            }
            old_for_new[new_index] = Some(old_index);
            new_for_old[old_index] = Some(new_index);
            old_index += 1;
            new_index += 1;
        }
    }

    (old_for_new, new_for_old)
}

fn inserted_line_ids(old_for_new: &[Option<usize>]) -> Vec<Option<String>> {
    old_for_new
        .iter()
        .map(|old_index| match old_index {
            Some(_) => None,
            None => Some(Uuid::now_v7().to_string()),
        })
        .collect()
}

fn line_id_for_new<'a>(
    base: &'a [Line],
    old_for_new: &[Option<usize>],
    inserted_ids: &'a [Option<String>],
    new_index: usize,
) -> &'a str {
    match old_for_new[new_index] {
        Some(old_index) => &base[old_index].id,
        None => inserted_ids[new_index]
            .as_deref()
            .expect("inserted line id should exist"),
    }
}

fn detect_line_upsert_changes(
    base: &[Line],
    file_lines: &[String],
    old_for_new: &[Option<usize>],
    inserted_ids: &[Option<String>],
    changes: &mut Vec<DetectedChange>,
) -> Result<(), PluginError> {
    let keep_order_key = kept_order_key_indices(base, old_for_new);
    let mut previous_order_key = None::<OrderKey>;
    let mut pending = Vec::new();

    for new_index in 0..file_lines.len() {
        if keep_order_key[new_index] {
            let old_index =
                old_for_new[new_index].expect("kept order key should belong to an existing line");
            let order_key = &base[old_index].order_key;
            flush_generated_line_upserts(
                &mut pending,
                &mut previous_order_key,
                Some(order_key),
                base,
                file_lines,
                old_for_new,
                inserted_ids,
                changes,
            )?;
            if base[old_index].line != file_lines[new_index] {
                changes.push(line_upsert_change(
                    line_id_for_new(base, old_for_new, inserted_ids, new_index),
                    order_key,
                    &file_lines[new_index],
                )?);
            }
            previous_order_key = Some(order_key.clone());
        } else {
            pending.push(new_index);
        }
    }

    flush_generated_line_upserts(
        &mut pending,
        &mut previous_order_key,
        None,
        base,
        file_lines,
        old_for_new,
        inserted_ids,
        changes,
    )
}

fn kept_order_key_indices(base: &[Line], old_for_new: &[Option<usize>]) -> Vec<bool> {
    let mut keep = vec![false; old_for_new.len()];
    if old_for_new
        .iter()
        .copied()
        .flatten()
        .map(|old_index| &base[old_index].order_key)
        .is_sorted_by(|previous, current| previous < current)
    {
        for (new_index, old_index) in old_for_new.iter().enumerate() {
            keep[new_index] = old_index.is_some();
        }
        return keep;
    }

    let mut pile_tops = Vec::<usize>::new();
    let mut predecessors = vec![None; old_for_new.len()];

    for (new_index, old_index) in old_for_new.iter().copied().enumerate() {
        let Some(old_index) = old_index else {
            continue;
        };
        let order_key = &base[old_index].order_key;
        let pile = pile_tops
            .partition_point(|top_index| old_order_key(base, old_for_new, *top_index) < order_key);
        if pile != 0 {
            predecessors[new_index] = Some(pile_tops[pile - 1]);
        }
        if pile == pile_tops.len() {
            pile_tops.push(new_index);
        } else if old_order_key(base, old_for_new, pile_tops[pile]) > order_key {
            pile_tops[pile] = new_index;
        }
    }

    let Some(mut current) = pile_tops.last().copied() else {
        return keep;
    };
    loop {
        keep[current] = true;
        let Some(previous) = predecessors[current] else {
            break;
        };
        current = previous;
    }
    keep
}

fn old_order_key<'a>(
    base: &'a [Line],
    old_for_new: &[Option<usize>],
    new_index: usize,
) -> &'a OrderKey {
    let old_index = old_for_new[new_index].expect("old order key should belong to existing line");
    &base[old_index].order_key
}

fn flush_generated_line_upserts(
    pending: &mut Vec<usize>,
    previous_order_key: &mut Option<OrderKey>,
    next_order_key: Option<&OrderKey>,
    base: &[Line],
    file_lines: &[String],
    old_for_new: &[Option<usize>],
    inserted_ids: &[Option<String>],
    changes: &mut Vec<DetectedChange>,
) -> Result<(), PluginError> {
    if pending.is_empty() {
        return Ok(());
    }

    let order_keys =
        OrderKey::evenly_between(previous_order_key.as_ref(), next_order_key, pending.len())
            .map_err(PluginError::Internal)?;

    for (new_index, order_key) in pending.drain(..).zip(order_keys) {
        changes.push(line_upsert_change(
            line_id_for_new(base, old_for_new, inserted_ids, new_index),
            &order_key,
            &file_lines[new_index],
        )?);
        *previous_order_key = Some(order_key);
    }

    Ok(())
}

fn has_duplicate_order_keys(lines: &[Line]) -> bool {
    lines
        .windows(2)
        .any(|pair| pair[0].order_key == pair[1].order_key)
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
                order_key: line.order_key.clone(),
                line: line.line.clone(),
            })
            .collect::<Vec<_>>();
        lines.sort_by(|a, b| a.order_key.cmp(&b.order_key).then_with(|| a.id.cmp(&b.id)));
        lines
    }
}

fn line_upsert_change(
    id: &str,
    order_key: &OrderKey,
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

    #[test]
    fn fuzz_detect_changes_reorders_lines_without_changing_ids() {
        let mut rng = SmallRng::seed_from_u64(1);

        for _ in 0..100_000 {
            let before_text = random_text(&mut rng);
            let mut before = projection_from_text(before_text.clone());
            before.document_present = true;
            let mut reordered_lines = before_text.lines.clone();
            shuffle(&mut reordered_lines, &mut rng);
            let after_text = ParsedText {
                document: before_text.document,
                lines: reordered_lines.clone(),
            };

            let changes = detect_changes_for_text(&before, &after_text).unwrap();

            for change in &changes {
                assert_eq!(
                    change.schema_key, LINE_SCHEMA_KEY,
                    "reordering lines should not change the document snapshot"
                );

                let entity_pk = single_entity_pk(change.entity_pk.clone()).unwrap();
                let before_line = before
                    .lines_by_id
                    .get(&entity_pk)
                    .expect("reordering lines should only update existing line ids");
                let snapshot_content = change
                    .snapshot_content
                    .as_deref()
                    .expect("reordering lines should not delete existing line entities");
                let after_line = parse_line_snapshot(snapshot_content, &entity_pk).unwrap();

                assert_eq!(
                    after_line.line, before_line.line,
                    "reordering lines should only update order keys"
                );
                assert_ne!(
                    after_line.order_key, before_line.order_key,
                    "reordering lines should not emit unchanged line snapshots"
                );
            }

            let mut applied = before;
            for change in changes {
                apply_entity_change(&mut applied, change).unwrap();
            }

            let applied_lines = applied
                .to_lines()
                .into_iter()
                .map(|line| line.line)
                .collect::<Vec<_>>();
            assert_eq!(applied_lines, reordered_lines);
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

    fn shuffle<T>(items: &mut [T], rng: &mut (impl Rng + ?Sized)) {
        for index in (1..items.len()).rev() {
            items.swap(index, rng.random_range(0..=index));
        }
    }

    fn projection_from_text(text: ParsedText) -> Projection {
        let document_present =
            text.document != TextDocumentSnapshot::default() || !text.lines.is_empty();
        let ids = (0..text.lines.len())
            .map(|offset| format!("line:{offset}"))
            .collect::<Vec<_>>();
        let order_keys = OrderKey::evenly_between(None, None, ids.len()).unwrap();
        let lines_by_id = text
            .lines
            .into_iter()
            .zip(ids.into_iter().zip(order_keys))
            .map(|(line, (id, order_key))| (id, LineSnapshot { order_key, line }))
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
