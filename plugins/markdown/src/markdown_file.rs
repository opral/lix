use crate::exports::lix::plugin::api::PluginError;
use crate::{DOCUMENT_SCHEMA_KEY, DetectedChange, File, Projection, ROOT_ENTITY_PK};
use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use encoding_rs::Encoding;
use markdown::mdast::Node;
use serde_json::Value;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(crate) struct MarkdownDocumentSnapshot;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParsedMarkdown {
    pub(crate) document: MarkdownDocumentSnapshot,
    pub(crate) blocks: Vec<String>,
}

pub(crate) fn parse_file(file: &File) -> Result<ParsedMarkdown, PluginError> {
    let (buf, encoding) = buffer_with_encoding(&file.data);
    let (decoded, _had_errors) = encoding.decode_without_bom_handling(buf);
    parse_markdown_source(&decoded)
}

fn buffer_with_encoding(buf: &[u8]) -> (&[u8], &'static Encoding) {
    if let Some((encoding, skip)) = Encoding::for_bom(buf) {
        (&buf[skip..], encoding)
    } else {
        let mut detector = EncodingDetector::new(Iso2022JpDetection::Allow);
        detector.feed(buf, true);
        (buf, detector.guess(None, Utf8Detection::Allow))
    }
}

pub(crate) fn parse_markdown_source(source: &str) -> Result<ParsedMarkdown, PluginError> {
    let normalized = normalize_line_endings(source);
    let root =
        markdown::to_mdast(&normalized, &markdown::ParseOptions::gfm()).map_err(|error| {
            PluginError::InvalidInput(format!("file.data must be valid Markdown: {error}"))
        })?;

    let Node::Root(root) = root else {
        return Err(PluginError::Internal(
            "markdown parser did not return a root node".to_string(),
        ));
    };

    let mut blocks = Vec::with_capacity(root.children.len());
    for child in &root.children {
        let position = child.position().ok_or_else(|| {
            PluginError::Internal("markdown parser returned a block without a position".to_string())
        })?;
        let raw_block = normalized
            .get(position.start.offset..position.end.offset)
            .ok_or_else(|| {
                PluginError::Internal(format!(
                    "markdown parser returned invalid block offsets {}..{}",
                    position.start.offset, position.end.offset
                ))
            })?;
        let block = normalize_block(raw_block);
        if !block.is_empty() {
            blocks.push(block);
        }
    }

    Ok(ParsedMarkdown {
        document: MarkdownDocumentSnapshot,
        blocks,
    })
}

fn normalize_line_endings(source: &str) -> String {
    let mut normalized = String::with_capacity(source.len());
    let bytes = source.as_bytes();
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'\r' => {
                normalized.push('\n');
                if index + 1 < bytes.len() && bytes[index + 1] == b'\n' {
                    index += 2;
                } else {
                    index += 1;
                }
            }
            _ => {
                let ch = source[index..]
                    .chars()
                    .next()
                    .expect("index should be within source");
                normalized.push(ch);
                index += ch.len_utf8();
            }
        }
    }

    normalized
}

fn normalize_block(raw: &str) -> String {
    raw.trim_matches('\n').to_string()
}

pub(crate) fn document_upsert_change(
    _document: MarkdownDocumentSnapshot,
) -> Result<DetectedChange, PluginError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "id": ROOT_ENTITY_PK,
    }))
    .map_err(|error| {
        PluginError::Internal(format!("failed to serialize markdown document: {error}"))
    })?;

    Ok(DetectedChange {
        entity_pk: vec![ROOT_ENTITY_PK.to_string()],
        schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
        snapshot_content: Some(snapshot_content),
        metadata: None,
    })
}

pub(crate) fn parse_document_snapshot(raw: &str) -> Result<MarkdownDocumentSnapshot, PluginError> {
    let value: Value = serde_json::from_str(raw).map_err(|error| {
        PluginError::InvalidInput(format!(
            "invalid markdown document snapshot_content: {error}"
        ))
    })?;
    let object = value.as_object().ok_or_else(|| {
        PluginError::InvalidInput(
            "markdown document snapshot_content must be an object".to_string(),
        )
    })?;
    crate::reject_unknown_fields(object.keys(), &["id"], "markdown document")?;

    let id = object.get("id").and_then(Value::as_str).ok_or_else(|| {
        PluginError::InvalidInput("markdown document snapshot must contain string 'id'".to_string())
    })?;
    if id != ROOT_ENTITY_PK {
        return Err(PluginError::InvalidInput(format!(
            "markdown document snapshot id '{id}' does not match expected '{ROOT_ENTITY_PK}'"
        )));
    }

    Ok(MarkdownDocumentSnapshot)
}

pub(crate) fn render_projection(projection: &Projection) -> Vec<u8> {
    let mut rendered = projection
        .to_blocks()
        .into_iter()
        .map(|block| block.block)
        .collect::<Vec<_>>()
        .join("\n\n");
    rendered.push('\n');
    rendered.into_bytes()
}
