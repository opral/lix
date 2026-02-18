use crate::common::SnapshotContent;
use crate::exports::lix::plugin::api::{EntityChange, File, PluginError};
use crate::{ROOT_ENTITY_ID, SCHEMA_KEY, SCHEMA_VERSION};
use markdown::{to_mdast, Constructs, ParseOptions};

pub(crate) fn detect_changes(
    before: Option<File>,
    after: File,
) -> Result<Vec<EntityChange>, PluginError> {
    let after_markdown = decode_markdown_bytes(&after.data)?;
    let after_ast = parse_markdown_to_mdast(&after_markdown)?;

    let before_ast = before
        .as_ref()
        .map(|file| {
            let markdown = decode_markdown_bytes(&file.data)?;
            parse_markdown_to_mdast(&markdown)
        })
        .transpose()?;

    if before_ast.as_ref() == Some(&after_ast) {
        return Ok(Vec::new());
    }

    let snapshot_content = serde_json::to_string(&SnapshotContent {
        markdown: after_markdown,
    })
    .map_err(|error| {
        PluginError::Internal(format!("failed to serialize markdown snapshot: {error}"))
    })?;

    Ok(vec![EntityChange {
        entity_id: ROOT_ENTITY_ID.to_string(),
        schema_key: SCHEMA_KEY.to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        snapshot_content: Some(snapshot_content),
    }])
}

fn decode_markdown_bytes(bytes: &[u8]) -> Result<String, PluginError> {
    String::from_utf8(bytes.to_vec()).map_err(|error| {
        PluginError::InvalidInput(format!(
            "file.data must be valid UTF-8 markdown bytes: {error}"
        ))
    })
}

fn parse_markdown_to_mdast(markdown: &str) -> Result<markdown::mdast::Node, PluginError> {
    to_mdast(markdown, &parse_options_all_extensions()).map_err(|error| {
        PluginError::InvalidInput(format!(
            "markdown parse failed with configured extensions: {}",
            error
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
