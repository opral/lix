// TODO: workaround wit_bindgen
#![expect(clippy::same_length_and_capacity)]

wit_bindgen::generate!({
    path: "../../packages/engine/wit",
    world: "plugin",
});

mod diff;
mod markdown_file;
pub mod schemas;

use crate::diff::{DiffRun, imara_diff_runs};
pub use crate::exports::lix::plugin::api::{DetectedChange, File, PluginError};
use crate::exports::lix::plugin::api::{EntityState, Guest as Plugin};
use crate::markdown_file::{
    MarkdownDocumentSnapshot, ParsedMarkdown, document_upsert_change, parse_document_snapshot,
    parse_file, render_projection,
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
pub const BLOCK_SCHEMA_KEY: &str = schemas::BLOCK_SCHEMA_KEY;

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");

#[derive(Clone, Copy, Debug)]
pub struct MarkdownPlugin;
#[cfg(target_family = "wasm")]
export!(MarkdownPlugin);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Projection {
    blocks_by_id: BTreeMap<String, BlockSnapshot>,
    document: MarkdownDocumentSnapshot,
    document_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Block {
    id: String,
    order_key: OrderKey,
    block: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockSnapshot {
    order_key: OrderKey,
    block: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReplaceRun {
    old: Range<usize>,
    new: Range<usize>,
}

impl Plugin for MarkdownPlugin {
    fn detect_changes(
        state: Vec<EntityState>,
        file: File,
    ) -> Result<Vec<DetectedChange>, PluginError> {
        let before = Projection::from_entity_state(state.into_iter())?;
        let after = parse_file(&file)?;
        detect_changes_for_markdown(&before, &after)
    }

    fn render(state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
        let projection = Projection::from_entity_state(state.into_iter())?;
        Ok(render_projection(&projection))
    }
}

fn detect_changes_for_markdown(
    before: &Projection,
    after: &ParsedMarkdown,
) -> Result<Vec<DetectedChange>, PluginError> {
    let base = before.to_blocks();
    if has_duplicate_order_keys(&base) {
        return detect_changes_for_markdown_with_reindexed_order(before, after, &base);
    }

    let (old_for_new, new_for_old) = match_diff_blocks(
        &base,
        &after.blocks,
        imara_diff_runs(base.iter().map(|block| &block.block), after.blocks.iter()),
    );
    let inserted_ids = inserted_block_ids(&old_for_new);
    let mut changes = Vec::new();

    for base_index in (0..base.len()).filter(|index| new_for_old[*index].is_none()) {
        changes.push(DetectedChange {
            entity_pk: vec![base[base_index].id.clone()],
            schema_key: BLOCK_SCHEMA_KEY.to_string(),
            snapshot_content: None,
            metadata: None,
        });
    }
    detect_block_upsert_changes(
        &base,
        &after.blocks,
        &old_for_new,
        &inserted_ids,
        &mut changes,
    )?;

    if !before.document_present || before.document != after.document {
        changes.push(document_upsert_change(after.document)?);
    }

    Ok(changes)
}

fn detect_changes_for_markdown_with_reindexed_order(
    before: &Projection,
    after: &ParsedMarkdown,
    base: &[Block],
) -> Result<Vec<DetectedChange>, PluginError> {
    let (old_for_new, new_for_old) = match_diff_blocks(
        base,
        &after.blocks,
        imara_diff_runs(base.iter().map(|block| &block.block), after.blocks.iter()),
    );
    let inserted_ids = inserted_block_ids(&old_for_new);
    let planned_ids = (0..after.blocks.len())
        .map(|new_index| block_id_for_new(base, &old_for_new, &inserted_ids, new_index).to_string())
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
                schema_key: BLOCK_SCHEMA_KEY.to_string(),
                snapshot_content: None,
                metadata: None,
            });
        }
    }

    for ((id, block), order_key) in planned_ids
        .iter()
        .zip(after.blocks.iter())
        .zip(order_keys.iter())
    {
        changes.push(block_upsert_change(id, order_key, block)?);
    }

    if !before.document_present || before.document != after.document {
        changes.push(document_upsert_change(after.document)?);
    }

    Ok(changes)
}

fn match_diff_blocks(
    base: &[Block],
    file_blocks: &[String],
    diff_runs: impl IntoIterator<Item = DiffRun>,
) -> (Vec<Option<usize>>, Vec<Option<usize>>) {
    let mut old_for_new = vec![None; file_blocks.len()];
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
    let mut old_blocks_by_content = HashMap::<&str, Vec<usize>>::with_capacity(old_replace_len);
    for run in replace_runs.iter().rev() {
        for old_index in run.old.clone().rev() {
            old_blocks_by_content
                .entry(base[old_index].block.as_str())
                .or_default()
                .push(old_index);
        }
    }

    for run in &replace_runs {
        for new_index in run.new.clone() {
            let Some(old_indices) = old_blocks_by_content.get_mut(file_blocks[new_index].as_str())
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

fn inserted_block_ids(old_for_new: &[Option<usize>]) -> Vec<Option<String>> {
    old_for_new
        .iter()
        .map(|old_index| match old_index {
            Some(_) => None,
            None => Some(Uuid::now_v7().to_string()),
        })
        .collect()
}

fn block_id_for_new<'a>(
    base: &'a [Block],
    old_for_new: &[Option<usize>],
    inserted_ids: &'a [Option<String>],
    new_index: usize,
) -> &'a str {
    match old_for_new[new_index] {
        Some(old_index) => &base[old_index].id,
        None => inserted_ids[new_index]
            .as_deref()
            .expect("inserted block id should exist"),
    }
}

fn detect_block_upsert_changes(
    base: &[Block],
    file_blocks: &[String],
    old_for_new: &[Option<usize>],
    inserted_ids: &[Option<String>],
    changes: &mut Vec<DetectedChange>,
) -> Result<(), PluginError> {
    let keep_order_key = kept_order_key_indices(base, old_for_new);
    let mut previous_order_key = None::<OrderKey>;
    let mut pending = Vec::new();

    for new_index in 0..file_blocks.len() {
        if keep_order_key[new_index] {
            let old_index =
                old_for_new[new_index].expect("kept order key should belong to an existing block");
            let order_key = &base[old_index].order_key;
            flush_generated_block_upserts(
                &mut pending,
                &mut previous_order_key,
                Some(order_key),
                base,
                file_blocks,
                old_for_new,
                inserted_ids,
                changes,
            )?;
            if base[old_index].block != file_blocks[new_index] {
                changes.push(block_upsert_change(
                    block_id_for_new(base, old_for_new, inserted_ids, new_index),
                    order_key,
                    &file_blocks[new_index],
                )?);
            }
            previous_order_key = Some(order_key.clone());
        } else {
            pending.push(new_index);
        }
    }

    flush_generated_block_upserts(
        &mut pending,
        &mut previous_order_key,
        None,
        base,
        file_blocks,
        old_for_new,
        inserted_ids,
        changes,
    )
}

fn kept_order_key_indices(base: &[Block], old_for_new: &[Option<usize>]) -> Vec<bool> {
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
    base: &'a [Block],
    old_for_new: &[Option<usize>],
    new_index: usize,
) -> &'a OrderKey {
    let old_index = old_for_new[new_index].expect("old order key should belong to existing block");
    &base[old_index].order_key
}

fn flush_generated_block_upserts(
    pending: &mut Vec<usize>,
    previous_order_key: &mut Option<OrderKey>,
    next_order_key: Option<&OrderKey>,
    base: &[Block],
    file_blocks: &[String],
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
        changes.push(block_upsert_change(
            block_id_for_new(base, old_for_new, inserted_ids, new_index),
            &order_key,
            &file_blocks[new_index],
        )?);
        *previous_order_key = Some(order_key);
    }

    Ok(())
}

fn has_duplicate_order_keys(blocks: &[Block]) -> bool {
    blocks
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
        let mut blocks_by_id = BTreeMap::new();
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
                BLOCK_SCHEMA_KEY => {
                    let entity_pk = single_entity_pk(change.entity_pk)?;
                    match blocks_by_id.entry(entity_pk) {
                        Entry::Occupied(entry) => {
                            return Err(PluginError::InvalidInput(format!(
                                "duplicate entity_pk '{}' for schema_key '{BLOCK_SCHEMA_KEY}'",
                                entry.key()
                            )));
                        }
                        Entry::Vacant(entry) => {
                            let block =
                                parse_block_snapshot(&change.snapshot_content, entry.key())?;
                            entry.insert(block);
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(Self {
            blocks_by_id,
            document: document.unwrap_or_default(),
            document_present: document.is_some(),
        })
    }

    fn to_blocks(&self) -> Vec<Block> {
        let mut blocks = self
            .blocks_by_id
            .iter()
            .map(|(id, block)| Block {
                id: id.clone(),
                order_key: block.order_key.clone(),
                block: block.block.clone(),
            })
            .collect::<Vec<_>>();
        blocks.sort_by(|a, b| a.order_key.cmp(&b.order_key).then_with(|| a.id.cmp(&b.id)));
        blocks
    }
}

fn block_upsert_change(
    id: &str,
    order_key: &OrderKey,
    block: &str,
) -> Result<DetectedChange, PluginError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "id": id,
        "order_key": order_key.to_snapshot_string(),
        "block": block,
    }))
    .map_err(|error| {
        PluginError::Internal(format!("failed to serialize markdown block: {error}"))
    })?;

    Ok(DetectedChange {
        entity_pk: vec![id.to_string()],
        schema_key: BLOCK_SCHEMA_KEY.to_string(),
        snapshot_content: Some(snapshot_content),
        metadata: None,
    })
}

fn parse_block_snapshot(raw: &str, entity_pk: &str) -> Result<BlockSnapshot, PluginError> {
    let value: Value = serde_json::from_str(raw).map_err(|error| {
        PluginError::InvalidInput(format!(
            "invalid markdown block snapshot_content for entity_pk '{entity_pk}': {error}"
        ))
    })?;
    let object = value.as_object().ok_or_else(|| {
        PluginError::InvalidInput(format!(
            "markdown block snapshot_content for entity_pk '{entity_pk}' must be an object"
        ))
    })?;
    reject_unknown_fields(
        object.keys(),
        &["id", "order_key", "block"],
        "markdown block",
    )?;

    let id = object.get("id").and_then(Value::as_str).ok_or_else(|| {
        PluginError::InvalidInput(format!(
            "markdown block snapshot for entity_pk '{entity_pk}' must contain string 'id'"
        ))
    })?;
    if id != entity_pk {
        return Err(PluginError::InvalidInput(format!(
            "markdown block snapshot id '{id}' does not match entity_pk '{entity_pk}'"
        )));
    }
    if id.is_empty() {
        return Err(PluginError::InvalidInput(format!(
            "markdown block snapshot id for entity_pk '{entity_pk}' must not be empty"
        )));
    }

    let order_key = parse_order_key_snapshot(object.get("order_key"), entity_pk)?;
    let block = object
        .get("block")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            PluginError::InvalidInput(format!(
                "markdown block snapshot for entity_pk '{entity_pk}' must contain string 'block'"
            ))
        })?;

    Ok(BlockSnapshot { order_key, block })
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
            "markdown block snapshot for entity_pk '{entity_pk}' must contain string 'order_key'"
        ))
    })?;

    OrderKey::from_snapshot_string(raw).map_err(|message| {
        PluginError::InvalidInput(format!(
            "invalid markdown block order_key for entity_pk '{entity_pk}': {message}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::markdown_file::parse_markdown_source;
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};
    use std::collections::BTreeMap;

    #[test]
    fn fuzz_detect_changes_round_trips_blocks() {
        let mut rng = SmallRng::seed_from_u64(0);

        for _ in 0..10_000 {
            let before_markdown = parse_markdown_source(&random_markdown_source(&mut rng))
                .expect("random before markdown should parse");
            let after_markdown = parse_markdown_source(&random_markdown_source(&mut rng))
                .expect("random after markdown should parse");
            let before = projection_from_markdown(before_markdown);

            let changes = detect_changes_for_markdown(&before, &after_markdown).unwrap();

            let mut applied = before;
            for change in changes {
                apply_entity_change(&mut applied, change).unwrap();
            }

            let applied_blocks = applied
                .to_blocks()
                .into_iter()
                .map(|block| block.block)
                .collect::<Vec<_>>();
            assert_eq!(applied_blocks, after_markdown.blocks);
            assert_eq!(applied.document, after_markdown.document);
            assert_eq!(
                render_projection(&applied),
                normalized_bytes(&after_markdown)
            );
        }
    }

    #[test]
    fn fuzz_detect_changes_reorders_blocks_without_changing_ids() {
        let mut rng = SmallRng::seed_from_u64(1);

        for _ in 0..10_000 {
            let before_markdown = parse_markdown_source(&random_markdown_source(&mut rng))
                .expect("random before markdown should parse");
            let before = projection_from_markdown(before_markdown.clone());
            let mut reordered_blocks = before_markdown.blocks.clone();
            shuffle(&mut reordered_blocks, &mut rng);
            let after_markdown = ParsedMarkdown {
                document: before_markdown.document,
                blocks: reordered_blocks.clone(),
            };

            let changes = detect_changes_for_markdown(&before, &after_markdown).unwrap();

            for change in &changes {
                assert_eq!(
                    change.schema_key, BLOCK_SCHEMA_KEY,
                    "reordering blocks should not change the document snapshot"
                );

                let entity_pk = single_entity_pk(change.entity_pk.clone()).unwrap();
                let before_block = before
                    .blocks_by_id
                    .get(&entity_pk)
                    .expect("reordering blocks should only update existing block ids");
                let snapshot_content = change
                    .snapshot_content
                    .as_deref()
                    .expect("reordering blocks should not delete existing block entities");
                let after_block = parse_block_snapshot(snapshot_content, &entity_pk).unwrap();

                assert_eq!(
                    after_block.block, before_block.block,
                    "reordering blocks should only update order keys"
                );
                assert_ne!(
                    after_block.order_key, before_block.order_key,
                    "reordering blocks should not emit unchanged block snapshots"
                );
            }

            let mut applied = before;
            for change in changes {
                apply_entity_change(&mut applied, change).unwrap();
            }

            let applied_blocks = applied
                .to_blocks()
                .into_iter()
                .map(|block| block.block)
                .collect::<Vec<_>>();
            assert_eq!(applied_blocks, reordered_blocks);
            assert_eq!(applied.document, after_markdown.document);
        }
    }

    fn random_markdown_source(rng: &mut (impl Rng + ?Sized)) -> String {
        let block_count = rng.random_range(0..=8);
        let mut output = String::new();

        if rng.random_range(0..4) == 0 {
            output.push('\n');
        }

        for offset in 0..block_count {
            if offset != 0 {
                output.push_str(random_separator(rng));
            }
            output.push_str(&random_markdown_block(rng));
        }

        if rng.random_range(0..4) == 0 {
            output.push('\n');
        }

        output
    }

    fn random_separator(rng: &mut (impl Rng + ?Sized)) -> &'static str {
        match rng.random_range(0..5) {
            0 => "\n\n",
            1 => "\r\n\r\n",
            2 => "\n\n\n",
            3 => "\r\r",
            _ => "\n \n",
        }
    }

    fn random_markdown_block(rng: &mut (impl Rng + ?Sized)) -> String {
        match rng.random_range(0..8) {
            0 => format!(
                "{} {}",
                "#".repeat(rng.random_range(1..=3)),
                random_words(rng)
            ),
            1 => format!("{}\n{}", random_words(rng), random_words(rng)),
            2 => {
                let mut list = String::new();
                for item in 0..rng.random_range(1..=4) {
                    if item != 0 {
                        list.push('\n');
                    }
                    list.push_str("- ");
                    list.push_str(&random_words(rng));
                }
                list
            }
            3 => format!("> {}\n> {}", random_words(rng), random_words(rng)),
            4 => format!("```\n{}\n```", random_words(rng)),
            5 => "---".to_string(),
            6 => format!(
                "[{}]: https://example.com/{}",
                random_word(rng),
                random_word(rng)
            ),
            _ => format!(
                "| {} | {} |\n| --- | --- |\n| {} | {} |",
                random_word(rng),
                random_word(rng),
                random_word(rng),
                random_word(rng)
            ),
        }
    }

    fn random_words(rng: &mut (impl Rng + ?Sized)) -> String {
        let count = rng.random_range(1..=5);
        (0..count)
            .map(|_| random_word(rng))
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn random_word(rng: &mut (impl Rng + ?Sized)) -> String {
        let alphabet = ["alpha", "beta", "gamma", "delta", "one", "two", "x"];
        alphabet[rng.random_range(0..alphabet.len())].to_string()
    }

    fn shuffle<T>(items: &mut [T], rng: &mut (impl Rng + ?Sized)) {
        for index in (1..items.len()).rev() {
            items.swap(index, rng.random_range(0..=index));
        }
    }

    fn projection_from_markdown(markdown: ParsedMarkdown) -> Projection {
        let ids = (0..markdown.blocks.len())
            .map(|offset| format!("block:{offset}"))
            .collect::<Vec<_>>();
        let order_keys = OrderKey::evenly_between(None, None, ids.len()).unwrap();
        let blocks_by_id = markdown
            .blocks
            .into_iter()
            .zip(ids.into_iter().zip(order_keys))
            .map(|(block, (id, order_key))| (id, BlockSnapshot { order_key, block }))
            .collect::<BTreeMap<_, _>>();

        Projection {
            blocks_by_id,
            document: markdown.document,
            document_present: true,
        }
    }

    fn normalized_bytes(markdown: &ParsedMarkdown) -> Vec<u8> {
        let mut rendered = markdown.blocks.join("\n\n");
        rendered.push('\n');
        rendered.into_bytes()
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
                    projection.document = MarkdownDocumentSnapshot;
                    projection.document_present = false;
                }
            }
            BLOCK_SCHEMA_KEY => {
                let entity_pk = single_entity_pk(change.entity_pk)?;
                if let Some(raw) = change.snapshot_content {
                    let block = parse_block_snapshot(&raw, &entity_pk)?;
                    projection.blocks_by_id.insert(entity_pk, block);
                } else {
                    projection.blocks_by_id.remove(&entity_pk);
                }
            }
            _ => {}
        }

        Ok(())
    }
}
