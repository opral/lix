// TODO: workaround wit_bindgen
#![expect(clippy::same_length_and_capacity)]

wit_bindgen::generate!({
    path: "../../packages/engine/wit",
    world: "plugin",
});

mod diff;
mod markdown_file;
pub mod schemas;

use crate::diff::{Op, imara_diff_runs};
pub use crate::exports::lix::plugin::api::{DetectedChange, File, PluginError};
use crate::exports::lix::plugin::api::{EntityState, Guest as Plugin};
use crate::markdown_file::{
    MarkdownDocumentSnapshot, ParsedMarkdown, document_upsert_change, parse_document_snapshot,
    parse_file, render_projection,
};
use lix_order_key::OrderKey;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
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

    let op_runs = imara_diff_runs(base.iter().map(|block| &block.block), after.blocks.iter());
    let mut changes = Vec::new();
    let mut base_index = 0;
    let mut file_index = 0;
    let mut previous_order_key = None::<OrderKey>;

    for run in op_runs {
        match run.op {
            Op::Equal => {
                for _ in 0..run.len {
                    previous_order_key = Some(base[base_index].order_key.clone());
                    base_index += 1;
                    file_index += 1;
                }
            }
            Op::Replace => {
                for _ in 0..run.len {
                    let block = &base[base_index];
                    changes.push(block_upsert_change(
                        &block.id,
                        &block.order_key,
                        &after.blocks[file_index],
                    )?);
                    previous_order_key = Some(block.order_key.clone());
                    base_index += 1;
                    file_index += 1;
                }
            }
            Op::Delete => {
                for _ in 0..run.len {
                    changes.push(DetectedChange {
                        entity_pk: vec![base[base_index].id.clone()],
                        schema_key: BLOCK_SCHEMA_KEY.to_string(),
                        snapshot_content: None,
                        metadata: None,
                    });
                    base_index += 1;
                }
            }
            Op::Insert => {
                let next_order_key = base.get(base_index).map(|block| &block.order_key);
                let ids = new_ids(run.len);
                let order_keys =
                    OrderKey::evenly_between(previous_order_key.as_ref(), next_order_key, run.len)
                        .map_err(PluginError::Internal)?;
                for (id, order_key) in ids.into_iter().zip(order_keys) {
                    changes.push(block_upsert_change(
                        &id,
                        &order_key,
                        &after.blocks[file_index],
                    )?);
                    previous_order_key = Some(order_key.clone());
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

#[derive(Debug)]
struct PlannedBlock {
    id: String,
    block: String,
}

fn detect_changes_for_markdown_with_reindexed_order(
    before: &Projection,
    after: &ParsedMarkdown,
    base: &[Block],
) -> Result<Vec<DetectedChange>, PluginError> {
    let planned_blocks = plan_markdown_blocks(base, after);
    let planned_ids = planned_blocks
        .iter()
        .map(|block| block.id.clone())
        .collect::<Vec<_>>();
    let order_keys =
        OrderKey::evenly_between(None, None, planned_ids.len()).map_err(PluginError::Internal)?;
    let planned_id_set = planned_ids
        .iter()
        .collect::<std::collections::BTreeSet<_>>();
    let mut changes = Vec::new();

    for id in before.blocks_by_id.keys() {
        if !planned_id_set.contains(id) {
            changes.push(DetectedChange {
                entity_pk: vec![id.clone()],
                schema_key: BLOCK_SCHEMA_KEY.to_string(),
                snapshot_content: None,
                metadata: None,
            });
        }
    }

    for (block, order_key) in planned_blocks.iter().zip(order_keys.iter()) {
        changes.push(block_upsert_change(&block.id, order_key, &block.block)?);
    }

    if !before.document_present || before.document != after.document {
        changes.push(document_upsert_change(after.document)?);
    }

    Ok(changes)
}

fn plan_markdown_blocks(base: &[Block], after: &ParsedMarkdown) -> Vec<PlannedBlock> {
    let op_runs = imara_diff_runs(base.iter().map(|block| &block.block), after.blocks.iter());
    let mut planned_blocks = Vec::with_capacity(after.blocks.len());
    let mut base_index = 0;
    let mut file_index = 0;

    for run in op_runs {
        match run.op {
            Op::Equal | Op::Replace => {
                for _ in 0..run.len {
                    planned_blocks.push(PlannedBlock {
                        id: base[base_index].id.clone(),
                        block: after.blocks[file_index].clone(),
                    });
                    base_index += 1;
                    file_index += 1;
                }
            }
            Op::Delete => {
                base_index += run.len;
            }
            Op::Insert => {
                for id in new_ids(run.len) {
                    planned_blocks.push(PlannedBlock {
                        id,
                        block: after.blocks[file_index].clone(),
                    });
                    file_index += 1;
                }
            }
        }
    }

    planned_blocks
}

fn has_duplicate_order_keys(blocks: &[Block]) -> bool {
    blocks
        .windows(2)
        .any(|pair| pair[0].order_key == pair[1].order_key)
}

fn new_ids(count: usize) -> Vec<String> {
    (0..count).map(|_| Uuid::now_v7().to_string()).collect()
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
