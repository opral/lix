use crate::common::{BlockSnapshotContent, DocumentSnapshotContent};
use crate::exports::lix::plugin::api::{DetectStateContext, EntityChange, File, PluginError};
use crate::schemas::{BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY, ENTITY_SCHEMA_VERSION};
use crate::ROOT_ENTITY_ID;
use markdown::mdast::{Node, Root};
use markdown::{to_mdast, Constructs, ParseOptions};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use strsim::normalized_levenshtein;
use unicode_normalization::UnicodeNormalization;

#[derive(Debug, Clone)]
struct ParsedBlock {
    id: String,
    schema_key: String,
    node_type: String,
    node_json: Value,
    markdown: String,
    fingerprint: String,
}

#[derive(Debug, Clone)]
struct ParsedBlockCandidate {
    node_type: String,
    node_json: Value,
    markdown: String,
    fingerprint: String,
}

#[derive(Debug, Clone)]
struct BeforeProjection {
    order: Vec<String>,
    blocks_by_id: BTreeMap<String, ParsedBlock>,
}

pub(crate) fn detect_changes(
    before: Option<File>,
    after: File,
    state_context: Option<DetectStateContext>,
) -> Result<Vec<EntityChange>, PluginError> {
    let (before_projection, use_existing_id_matching) =
        match parse_state_context_projection(state_context.as_ref())? {
            Some(context_projection) => (context_projection, true),
            None => (before_projection_from_file(before.as_ref())?, false),
        };

    let before_order = before_projection.order.clone();
    let before_by_id = before_projection.blocks_by_id;

    let after_markdown = decode_markdown_bytes(&after.data)?;
    let after_candidates = parse_top_level_block_candidates(&after_markdown)?;
    let after_blocks = if use_existing_id_matching {
        assign_ids_with_existing_state(after_candidates, &before_order, &before_by_id)
    } else {
        assign_content_based_ids(after_candidates)
    };
    let after_order = after_blocks
        .iter()
        .map(|block| block.id.clone())
        .collect::<Vec<_>>();
    let after_by_id = to_block_map(after_blocks)?;

    let mut changes = Vec::new();

    for id in before_by_id.keys() {
        if !after_by_id.contains_key(id) {
            let before_block = before_by_id
                .get(id)
                .expect("key came from before_by_id.keys() iterator");
            changes.push(EntityChange {
                entity_id: id.clone(),
                schema_key: before_block.schema_key.clone(),
                schema_version: ENTITY_SCHEMA_VERSION.to_string(),
                snapshot_content: None,
            });
        }
    }

    for (id, after_block) in &after_by_id {
        match before_by_id.get(id) {
            Some(before_block)
                if before_block.fingerprint == after_block.fingerprint
                    && before_block.schema_key == after_block.schema_key => {}
            _ => changes.push(block_upsert_change(after_block)?),
        }
    }

    if before_order != after_order {
        let snapshot_content = serde_json::to_string(&DocumentSnapshotContent {
            id: ROOT_ENTITY_ID.to_string(),
            order: after_order,
        })
        .map_err(|error| {
            PluginError::Internal(format!(
                "failed to serialize markdown document snapshot: {error}"
            ))
        })?;

        changes.push(EntityChange {
            entity_id: ROOT_ENTITY_ID.to_string(),
            schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
            schema_version: ENTITY_SCHEMA_VERSION.to_string(),
            snapshot_content: Some(snapshot_content),
        });
    }

    Ok(changes)
}

fn parse_state_context_projection(
    state_context: Option<&DetectStateContext>,
) -> Result<Option<BeforeProjection>, PluginError> {
    let Some(state_context) = state_context else {
        return Ok(None);
    };
    let Some(rows) = state_context.active_state.as_ref() else {
        return Ok(None);
    };
    if rows.is_empty() {
        return Ok(None);
    }

    let mut document_order = None::<Vec<String>>;
    let mut blocks_by_id = BTreeMap::<String, ParsedBlock>::new();

    for row in rows {
        let Some(schema_key) = row.schema_key.as_deref() else {
            continue;
        };
        let Some(snapshot_content) = row.snapshot_content.as_deref() else {
            continue;
        };

        if schema_key == DOCUMENT_SCHEMA_KEY {
            let snapshot: DocumentSnapshotContent = serde_json::from_str(snapshot_content)
                .map_err(|error| {
                    PluginError::Internal(format!(
                        "invalid markdown document row in detect state context: {error}"
                    ))
                })?;
            document_order = Some(snapshot.order);
            continue;
        }

        if schema_key != BLOCK_SCHEMA_KEY {
            continue;
        }

        let snapshot: BlockSnapshotContent =
            serde_json::from_str(snapshot_content).map_err(|error| {
                PluginError::Internal(format!(
                    "invalid markdown block row in detect state context: {error}"
                ))
            })?;
        let fingerprint = stable_json_string(&snapshot.node)?;
        let block = ParsedBlock {
            id: row.entity_id.clone(),
            schema_key: BLOCK_SCHEMA_KEY.to_string(),
            node_type: snapshot.node_type,
            node_json: snapshot.node,
            markdown: snapshot.markdown,
            fingerprint,
        };
        blocks_by_id.insert(block.id.clone(), block);
    }

    if blocks_by_id.is_empty() {
        return Ok(None);
    }

    let mut order = document_order.unwrap_or_default();
    order.retain(|id| blocks_by_id.contains_key(id));

    if order.len() != blocks_by_id.len() {
        let remaining = blocks_by_id
            .keys()
            .filter(|id| !order.contains(id))
            .cloned()
            .collect::<Vec<_>>();
        order.extend(remaining);
    }

    Ok(Some(BeforeProjection {
        order,
        blocks_by_id,
    }))
}

fn before_projection_from_file(before: Option<&File>) -> Result<BeforeProjection, PluginError> {
    let before_blocks = before
        .map(|file| {
            let markdown = decode_markdown_bytes(&file.data)?;
            let candidates = parse_top_level_block_candidates(&markdown)?;
            Ok::<_, PluginError>(assign_content_based_ids(candidates))
        })
        .transpose()?
        .unwrap_or_default();

    let order = before_blocks
        .iter()
        .map(|block| block.id.clone())
        .collect::<Vec<_>>();
    let blocks_by_id = to_block_map(before_blocks)?;
    Ok(BeforeProjection {
        order,
        blocks_by_id,
    })
}

fn assign_ids_with_existing_state(
    candidates: Vec<ParsedBlockCandidate>,
    before_order: &[String],
    before_by_id: &BTreeMap<String, ParsedBlock>,
) -> Vec<ParsedBlock> {
    if candidates.is_empty() {
        return Vec::new();
    }

    let mut ordered_before_ids = before_order
        .iter()
        .filter(|id| before_by_id.contains_key(*id))
        .cloned()
        .collect::<Vec<_>>();
    for id in before_by_id.keys() {
        if !ordered_before_ids.contains(id) {
            ordered_before_ids.push(id.clone());
        }
    }

    let mut assigned_ids = vec![None::<String>; candidates.len()];
    let mut matched_before_ids = BTreeSet::<String>::new();

    let mut before_exact = BTreeMap::<(String, String), Vec<String>>::new();
    for id in &ordered_before_ids {
        let before = before_by_id
            .get(id)
            .expect("ordered_before_ids are sourced from before_by_id");
        before_exact
            .entry((before.node_type.clone(), before.fingerprint.clone()))
            .or_default()
            .push(id.clone());
    }

    let mut after_exact = BTreeMap::<(String, String), Vec<usize>>::new();
    for (idx, after) in candidates.iter().enumerate() {
        after_exact
            .entry((after.node_type.clone(), after.fingerprint.clone()))
            .or_default()
            .push(idx);
    }

    for (key, after_indexes) in after_exact {
        let Some(before_ids) = before_exact.get(&key) else {
            continue;
        };
        let limit = before_ids.len().min(after_indexes.len());
        for offset in 0..limit {
            let before_id = before_ids[offset].clone();
            let after_idx = after_indexes[offset];
            if assigned_ids[after_idx].is_none() {
                assigned_ids[after_idx] = Some(before_id.clone());
                matched_before_ids.insert(before_id);
            }
        }
    }

    let before_positions = ordered_before_ids
        .iter()
        .enumerate()
        .map(|(idx, id)| (id.clone(), idx))
        .collect::<BTreeMap<_, _>>();

    for (after_idx, after) in candidates.iter().enumerate() {
        if assigned_ids[after_idx].is_some() {
            continue;
        }

        let mut pool = ordered_before_ids
            .iter()
            .filter(|id| !matched_before_ids.contains(*id))
            .filter_map(|id| {
                let before = before_by_id.get(id)?;
                if before.node_type != after.node_type {
                    return None;
                }
                let before_idx = *before_positions.get(id).unwrap_or(&0);
                Some((id.clone(), before, before_idx))
            })
            .collect::<Vec<_>>();

        if pool.is_empty() {
            continue;
        }

        let chosen = if pool.len() == 1 {
            Some(pool.swap_remove(0).0)
        } else {
            let after_text = normalize_text_for_fingerprint(&after.markdown);
            let total = candidates.len().max(ordered_before_ids.len()).max(1) as f64;
            let mut scored = pool
                .iter()
                .map(|(id, before, before_idx)| {
                    let before_text = normalize_text_for_fingerprint(&before.markdown);
                    let similarity = normalized_levenshtein(&before_text, &after_text);
                    let position = 1.0 - ((after_idx as f64 - *before_idx as f64).abs() / total);
                    let score = similarity * 0.75 + position * 0.25;
                    (id.clone(), similarity, score)
                })
                .collect::<Vec<_>>();

            scored.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| b.1.total_cmp(&a.1)));

            let top = scored[0].clone();
            let second = scored.get(1).cloned();
            let accept = match second {
                None => true,
                Some((_, second_similarity, second_score)) => {
                    top.1 >= 0.55
                        || top.2 >= 0.65
                        || (top.1 >= 0.35
                            && (top.1 - second_similarity) >= 0.15
                            && (top.2 - second_score) >= 0.08)
                }
            };

            if accept {
                Some(top.0)
            } else {
                None
            }
        };

        if let Some(id) = chosen {
            matched_before_ids.insert(id.clone());
            assigned_ids[after_idx] = Some(id);
        }
    }

    assign_missing_ids(candidates, assigned_ids)
}

fn assign_content_based_ids(candidates: Vec<ParsedBlockCandidate>) -> Vec<ParsedBlock> {
    let assigned_ids = vec![None; candidates.len()];
    assign_missing_ids(candidates, assigned_ids)
}

fn assign_missing_ids(
    candidates: Vec<ParsedBlockCandidate>,
    assigned_ids: Vec<Option<String>>,
) -> Vec<ParsedBlock> {
    let mut occurrence_counter: HashMap<(String, String), u32> = HashMap::new();
    let mut used_ids = assigned_ids
        .iter()
        .filter_map(|id| id.clone())
        .collect::<BTreeSet<_>>();

    candidates
        .into_iter()
        .enumerate()
        .map(|(idx, candidate)| {
            let occurrence_key = (candidate.node_type.clone(), candidate.fingerprint.clone());
            let occurrence = occurrence_counter
                .entry(occurrence_key)
                .and_modify(|count| *count += 1)
                .or_insert(1);

            let id = if let Some(existing) = assigned_ids[idx].clone() {
                existing
            } else {
                let base = block_id(&candidate.node_type, &candidate.fingerprint, *occurrence);
                if !used_ids.contains(&base) {
                    base
                } else {
                    let mut suffix = 2u32;
                    let mut candidate_id = format!("{base}_{suffix}");
                    while used_ids.contains(&candidate_id) {
                        suffix += 1;
                        candidate_id = format!("{base}_{suffix}");
                    }
                    candidate_id
                }
            };

            used_ids.insert(id.clone());

            ParsedBlock {
                id,
                schema_key: BLOCK_SCHEMA_KEY.to_string(),
                node_type: candidate.node_type,
                node_json: candidate.node_json,
                markdown: candidate.markdown,
                fingerprint: candidate.fingerprint,
            }
        })
        .collect()
}

fn block_upsert_change(block: &ParsedBlock) -> Result<EntityChange, PluginError> {
    let snapshot_content = serde_json::to_string(&BlockSnapshotContent {
        id: block.id.clone(),
        node_type: block.node_type.clone(),
        node: block.node_json.clone(),
        markdown: block.markdown.clone(),
    })
    .map_err(|error| {
        PluginError::Internal(format!(
            "failed to serialize markdown block snapshot: {error}"
        ))
    })?;

    Ok(EntityChange {
        entity_id: block.id.clone(),
        schema_key: block.schema_key.clone(),
        schema_version: ENTITY_SCHEMA_VERSION.to_string(),
        snapshot_content: Some(snapshot_content),
    })
}

fn to_block_map(blocks: Vec<ParsedBlock>) -> Result<BTreeMap<String, ParsedBlock>, PluginError> {
    let mut map = BTreeMap::new();
    for block in blocks {
        if map.insert(block.id.clone(), block).is_some() {
            return Err(PluginError::Internal(
                "generated duplicate markdown block id".to_string(),
            ));
        }
    }
    Ok(map)
}

fn parse_top_level_block_candidates(
    markdown: &str,
) -> Result<Vec<ParsedBlockCandidate>, PluginError> {
    let root = parse_markdown_to_root(markdown)?;
    let mut blocks = Vec::new();

    for node in root.children {
        let node_type = node_type_name(&node).to_string();
        let node_json = node_json_without_position(&node)?;
        let fingerprint = stable_json_string(&node_json)?;
        let markdown_fragment = extract_block_markdown(markdown, &node)?;
        blocks.push(ParsedBlockCandidate {
            node_type,
            node_json,
            markdown: markdown_fragment,
            fingerprint,
        });
    }

    Ok(blocks)
}

fn parse_markdown_to_root(markdown: &str) -> Result<Root, PluginError> {
    let tree = to_mdast(markdown, &parse_options_all_extensions()).map_err(|error| {
        PluginError::InvalidInput(format!(
            "markdown parse failed with configured extensions: {}",
            error
        ))
    })?;

    match tree {
        Node::Root(root) => Ok(root),
        _ => Err(PluginError::Internal(
            "markdown parser returned non-root AST node".to_string(),
        )),
    }
}

fn node_json_without_position(node: &Node) -> Result<Value, PluginError> {
    let mut value = serde_json::to_value(node).map_err(|error| {
        PluginError::Internal(format!("failed to serialize mdast node: {error}"))
    })?;
    strip_position_recursively(&mut value);
    Ok(value)
}

fn strip_position_recursively(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.remove("position");
            for child in object.values_mut() {
                strip_position_recursively(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                strip_position_recursively(item);
            }
        }
        _ => {}
    }
}

fn stable_json_string(value: &Value) -> Result<String, PluginError> {
    let mut normalized = value.clone();
    normalize_json_for_fingerprint(&mut normalized);
    serde_json::to_string(&normalized).map_err(|error| {
        PluginError::Internal(format!("failed to serialize node fingerprint: {error}"))
    })
}

fn normalize_json_for_fingerprint(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for child in object.values_mut() {
                normalize_json_for_fingerprint(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_json_for_fingerprint(item);
            }
        }
        Value::String(text) => {
            *text = normalize_text_for_fingerprint(text);
        }
        _ => {}
    }
}

fn normalize_text_for_fingerprint(input: &str) -> String {
    let normalized_newlines = input.replace("\r\n", "\n").replace('\r', "\n");
    normalized_newlines.nfc().collect()
}

fn extract_block_markdown(markdown: &str, node: &Node) -> Result<String, PluginError> {
    let Some(position) = node.position() else {
        return Err(PluginError::Internal(
            "top-level markdown node is missing position metadata".to_string(),
        ));
    };

    let start = position.start.offset;
    let end = position.end.offset;
    if start > end || end > markdown.len() {
        return Err(PluginError::Internal(
            "markdown node position offsets are out of bounds".to_string(),
        ));
    }
    if !markdown.is_char_boundary(start) || !markdown.is_char_boundary(end) {
        return Err(PluginError::Internal(
            "markdown node position offsets are not valid UTF-8 boundaries".to_string(),
        ));
    }

    Ok(markdown[start..end].to_string())
}

fn block_id(node_type: &str, fingerprint: &str, occurrence: u32) -> String {
    let node_type_sanitized = node_type
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>()
        .to_ascii_lowercase();
    let hash = fnv1a64(fingerprint.as_bytes());
    format!("b_{node_type_sanitized}_{hash:016x}_{occurrence}")
}

fn fnv1a64(input: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in input {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn decode_markdown_bytes(bytes: &[u8]) -> Result<String, PluginError> {
    String::from_utf8(bytes.to_vec()).map_err(|error| {
        PluginError::InvalidInput(format!(
            "file.data must be valid UTF-8 markdown bytes: {error}"
        ))
    })
}

fn parse_options_all_extensions() -> ParseOptions {
    let mut options = ParseOptions::mdx();
    let constructs = &mut options.constructs;

    constructs.frontmatter = true;
    constructs.gfm_autolink_literal = true;
    constructs.gfm_footnote_definition = true;
    constructs.gfm_label_start_footnote = true;
    constructs.gfm_strikethrough = true;
    constructs.gfm_table = true;
    constructs.gfm_task_list_item = true;
    constructs.math_flow = true;
    constructs.math_text = true;

    ensure_mdx_constructs(constructs);
    options
}

fn ensure_mdx_constructs(constructs: &mut Constructs) {
    constructs.mdx_esm = true;
    constructs.mdx_expression_flow = true;
    constructs.mdx_expression_text = true;
    constructs.mdx_jsx_flow = true;
    constructs.mdx_jsx_text = true;
}

fn node_type_name(node: &Node) -> &'static str {
    match node {
        Node::Root(_) => "root",
        Node::Blockquote(_) => "blockquote",
        Node::FootnoteDefinition(_) => "footnoteDefinition",
        Node::MdxJsxFlowElement(_) => "mdxJsxFlowElement",
        Node::List(_) => "list",
        Node::MdxjsEsm(_) => "mdxjsEsm",
        Node::Toml(_) => "toml",
        Node::Yaml(_) => "yaml",
        Node::Break(_) => "break",
        Node::InlineCode(_) => "inlineCode",
        Node::InlineMath(_) => "inlineMath",
        Node::Delete(_) => "delete",
        Node::Emphasis(_) => "emphasis",
        Node::MdxTextExpression(_) => "mdxTextExpression",
        Node::FootnoteReference(_) => "footnoteReference",
        Node::Html(_) => "html",
        Node::Image(_) => "image",
        Node::ImageReference(_) => "imageReference",
        Node::MdxJsxTextElement(_) => "mdxJsxTextElement",
        Node::Link(_) => "link",
        Node::LinkReference(_) => "linkReference",
        Node::Strong(_) => "strong",
        Node::Text(_) => "text",
        Node::Code(_) => "code",
        Node::Math(_) => "math",
        Node::MdxFlowExpression(_) => "mdxFlowExpression",
        Node::Heading(_) => "heading",
        Node::Table(_) => "table",
        Node::ThematicBreak(_) => "thematicBreak",
        Node::TableRow(_) => "tableRow",
        Node::TableCell(_) => "tableCell",
        Node::ListItem(_) => "listItem",
        Node::Definition(_) => "definition",
        Node::Paragraph(_) => "paragraph",
    }
}
