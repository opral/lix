use crate::common::{BlockSnapshotContent, DocumentSnapshotContent};
use crate::exports::lix::plugin::api::{EntityChange, File, PluginError};
use crate::schemas::{BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY, ENTITY_SCHEMA_VERSION};
use crate::ROOT_ENTITY_ID;
use markdown::mdast::{Node, Root};
use markdown::{to_mdast, Constructs, ParseOptions};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone)]
struct ParsedBlock {
    id: String,
    schema_key: String,
    node_type: String,
    node_json: Value,
    markdown: String,
    fingerprint: String,
}

pub(crate) fn detect_changes(
    before: Option<File>,
    after: File,
) -> Result<Vec<EntityChange>, PluginError> {
    let before_blocks = before
        .as_ref()
        .map(|file| {
            let markdown = decode_markdown_bytes(&file.data)?;
            parse_top_level_blocks(&markdown)
        })
        .transpose()?
        .unwrap_or_default();

    let after_markdown = decode_markdown_bytes(&after.data)?;
    let after_blocks = parse_top_level_blocks(&after_markdown)?;

    let before_order = before_blocks
        .iter()
        .map(|block| block.id.clone())
        .collect::<Vec<_>>();
    let after_order = after_blocks
        .iter()
        .map(|block| block.id.clone())
        .collect::<Vec<_>>();

    let before_by_id = to_block_map(before_blocks)?;
    let after_by_id = to_block_map(after_blocks)?;

    let mut changes = Vec::new();

    // Tombstones for deleted blocks.
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

    // Upserts for inserted/changed blocks.
    for (id, after_block) in &after_by_id {
        match before_by_id.get(id) {
            Some(before_block)
                if before_block.fingerprint == after_block.fingerprint
                    && before_block.markdown == after_block.markdown
                    && before_block.schema_key == after_block.schema_key => {}
            _ => changes.push(block_upsert_change(after_block)?),
        }
    }

    // Root order row for structural changes.
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

fn parse_top_level_blocks(markdown: &str) -> Result<Vec<ParsedBlock>, PluginError> {
    let root = parse_markdown_to_root(markdown)?;
    let mut occurrence_counter: HashMap<(String, String), u32> = HashMap::new();
    let mut blocks = Vec::new();

    for node in root.children {
        let node_type = node_type_name(&node).to_string();
        let node_json = node_json_without_position(&node)?;
        let fingerprint = stable_json_string(&node_json)?;
        let markdown_fragment = extract_block_markdown(markdown, &node)?;
        let occurrence_key = (node_type.clone(), fingerprint.clone());
        let occurrence = occurrence_counter
            .entry(occurrence_key)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        let id = block_id(&node_type, &fingerprint, *occurrence);

        blocks.push(ParsedBlock {
            id,
            schema_key: BLOCK_SCHEMA_KEY.to_string(),
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
    serde_json::to_string(value).map_err(|error| {
        PluginError::Internal(format!("failed to serialize node fingerprint: {error}"))
    })
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

    // Keep MDX mode and turn on extra extensions we need for parity.
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
