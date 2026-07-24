use crate::core::{File, PARSED_ROOT_ID, PluginError};
use crate::model::{
    AutolinkFormat, CharacterReferenceFormat, DeleteFormat, DelimiterFormat,
    FootnoteReferenceFormat, InlineCodeFormat, InlineContent, InlineNode, LineBreakFormat,
    NodeKind, NodeSnapshot, NodeTree, ReferenceFormat, ResourceFormat, inline_payload,
    parse_inline_payload,
};
use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use encoding_rs::Encoding;
use markdown_syntax::ast as md;
use markdown_syntax::{LineEnding, SerializeOptions, Span, SyntaxOptions};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParsedMarkdown {
    pub(crate) root: NodeTree,
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
    let mut canonical = source.to_string();
    let mut parsed = parse_markdown_source_once(&canonical)?;
    for _ in 0..8 {
        let rendered = render_tree(&parsed.root)?;
        if rendered == canonical.as_bytes() {
            return Ok(parsed);
        }
        canonical = String::from_utf8(rendered).map_err(|error| {
            PluginError::Internal(format!(
                "Markdown serializer emitted invalid UTF-8: {error}"
            ))
        })?;
        parsed = parse_markdown_source_once(&canonical)?;
    }
    Err(PluginError::Internal(
        "Markdown parser/serializer did not reach a stable representation after 8 passes"
            .to_string(),
    ))
}

fn parse_markdown_source_once(source: &str) -> Result<ParsedMarkdown, PluginError> {
    let mut options = SyntaxOptions::gfm();
    options.constructs.frontmatter = true;
    options.parse.preserve_character_escapes = true;
    options.parse.preserve_character_references = true;
    let mut output = options.parse(source);
    if let Some(diagnostic) = output
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.severity == markdown_syntax::DiagnosticSeverity::Error)
    {
        return Err(PluginError::InvalidInput(format!(
            "file.data must be valid GitHub Flavored Markdown: {}",
            diagnostic.message
        )));
    }

    repair_definition_adjacency(&mut output.document, source);
    let children = output
        .document
        .children
        .iter()
        .map(|block| tree_from_block(block, source))
        .collect::<Result<Vec<_>, _>>()?;
    let root = NodeTree {
        node: NodeSnapshot {
            id: PARSED_ROOT_ID.to_string(),
            kind: NodeKind::Document,
            parent_id: None,
            order_key: None,
            payload: json!({ "dialect": "gfm" }),
            format: json!({
                "line_ending": detected_line_ending(source),
                "final_newline": source.ends_with(['\n', '\r']),
            }),
        },
        children,
    };

    Ok(ParsedMarkdown { root })
}

fn repair_definition_adjacency(document: &mut md::Document, source: &str) {
    for index in 1..document.children.len() {
        let md::Block::Definition(definition) = &document.children[index - 1] else {
            continue;
        };
        let Some(definition_span) = definition.meta.span else {
            continue;
        };
        let Some(next_span) = block_span(&document.children[index]) else {
            continue;
        };
        let between = source
            .get(definition_span.end..next_span.start)
            .unwrap_or_default();
        if line_break_count(between) > 1 {
            continue;
        }
        let strip_indent = matches!(
            document.children[index],
            md::Block::CodeBlock(md::CodeBlock {
                kind: md::CodeBlockKind::Indented,
                ..
            })
        );
        if !strip_indent
            && !matches!(
                document.children[index],
                md::Block::ThematicBreak(_) | md::Block::HtmlBlock(_)
            )
        {
            continue;
        }
        let Some(raw) = source.get(next_span.start..next_span.end) else {
            continue;
        };
        document.children[index] = literal_paragraph(raw, next_span, strip_indent);
    }
}

fn block_span(block: &md::Block) -> Option<Span> {
    match block {
        md::Block::Paragraph(node) => node.meta.span,
        md::Block::Heading(node) => node.meta.span,
        md::Block::ThematicBreak(node) => node.meta.span,
        md::Block::BlockQuote(node) => node.meta.span,
        md::Block::Alert(node) => node.meta.span,
        md::Block::List(node) => node.meta.span,
        md::Block::DescriptionList(node) => node.meta.span,
        md::Block::CodeBlock(node) => node.meta.span,
        md::Block::HtmlBlock(node) => node.meta.span,
        md::Block::HtmlContainer(node) => node.meta.span,
        md::Block::Definition(node) => node.meta.span,
        md::Block::FootnoteDefinition(node) => node.meta.span,
        md::Block::Table(node) => node.meta.span,
        md::Block::MathBlock(node) => node.meta.span,
        md::Block::Frontmatter(node) => node.meta.span,
        md::Block::MdxEsm(node) => node.meta.span,
        md::Block::MdxExpression(node) => node.meta.span,
        md::Block::MdxJsx(node) => node.meta.span,
        md::Block::LeafDirective(node) => node.meta.span,
        md::Block::ContainerDirective(node) => node.meta.span,
    }
}

fn line_break_count(source: &str) -> usize {
    source
        .as_bytes()
        .iter()
        .enumerate()
        .filter(|(index, byte)| {
            **byte == b'\n' || (**byte == b'\r' && source.as_bytes().get(index + 1) != Some(&b'\n'))
        })
        .count()
}

fn literal_paragraph(raw: &str, span: Span, strip_indent: bool) -> md::Block {
    let raw = raw.trim_matches(['\r', '\n']);
    let lines = raw.lines().collect::<Vec<_>>();
    let mut children = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if index > 0 {
            children.push(md::Inline::SoftBreak(md::SoftBreak {
                meta: md::NodeMeta::default(),
            }));
        }
        let value = if strip_indent {
            line.strip_prefix("    ").unwrap_or(line)
        } else {
            line
        };
        children.push(md::Inline::Text(md::Text {
            meta: md::NodeMeta::default(),
            value: value.to_string(),
        }));
    }
    md::Block::Paragraph(md::Paragraph {
        meta: md::NodeMeta::new(Some(span)),
        children,
    })
}

fn detected_line_ending(source: &str) -> &'static str {
    let bytes = source.as_bytes();
    let mut index = 0;
    let mut saw_crlf = false;
    let mut saw_other = false;
    while index < bytes.len() {
        match bytes[index] {
            b'\r' if index + 1 < bytes.len() && bytes[index + 1] == b'\n' => {
                saw_crlf = true;
                index += 2;
            }
            b'\r' | b'\n' => {
                saw_other = true;
                index += 1;
            }
            _ => index += 1,
        }
    }
    if saw_crlf && !saw_other { "crlf" } else { "lf" }
}

fn new_tree(kind: NodeKind, payload: Value, format: Value, children: Vec<NodeTree>) -> NodeTree {
    NodeTree {
        node: NodeSnapshot {
            id: Uuid::now_v7().to_string(),
            kind,
            parent_id: None,
            order_key: None,
            payload,
            format,
        },
        children,
    }
}

fn tree_from_block(block: &md::Block, source: &str) -> Result<NodeTree, PluginError> {
    let empty = || json!({});
    match block {
        md::Block::Paragraph(node) => Ok(new_tree(
            NodeKind::Paragraph,
            inline_payload(leaf_inlines_from_ast(&node.children, source)?),
            empty(),
            Vec::new(),
        )),
        md::Block::Heading(node) => Ok(new_tree(
            NodeKind::Heading,
            json!({
                "depth": node.depth,
                "inline": leaf_inlines_from_ast(&node.children, source)?,
            }),
            json!({
                "style": match node.kind {
                    md::HeadingKind::Atx => "atx",
                    md::HeadingKind::Setext => "setext",
                },
            }),
            Vec::new(),
        )),
        md::Block::ThematicBreak(node) => Ok(new_tree(
            NodeKind::ThematicBreak,
            empty(),
            json!({
                "marker": match node.marker {
                    md::ThematicBreakMarker::Dash => "dash",
                    md::ThematicBreakMarker::Asterisk => "asterisk",
                    md::ThematicBreakMarker::Underscore => "underscore",
                },
            }),
            Vec::new(),
        )),
        md::Block::Frontmatter(node) => Ok(new_tree(
            NodeKind::Frontmatter,
            json!({
                "kind": match node.kind {
                    md::FrontmatterKind::Yaml => "yaml",
                    md::FrontmatterKind::Toml => "toml",
                },
                "value": node.value,
            }),
            empty(),
            Vec::new(),
        )),
        md::Block::BlockQuote(node) => Ok(new_tree(
            NodeKind::BlockQuote,
            empty(),
            empty(),
            node.children
                .iter()
                .map(|child| tree_from_block(child, source))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        md::Block::List(node) => Ok(new_tree(
            NodeKind::List,
            json!({
                "ordered": node.ordered,
                "start": node.start,
                "tight": node.tight,
            }),
            json!({
                "delimiter": list_delimiter_name(node.delimiter),
            }),
            node.children
                .iter()
                .map(|item| tree_from_list_item(item, source))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        md::Block::CodeBlock(node) => {
            let format = match node.kind {
                md::CodeBlockKind::Indented => json!({ "style": "indented" }),
                md::CodeBlockKind::Fenced { marker, length } => json!({
                    "style": "fenced",
                    "marker": match marker {
                        md::FenceMarker::Backtick => "backtick",
                        md::FenceMarker::Tilde => "tilde",
                    },
                    "fence_length": length,
                }),
            };
            Ok(new_tree(
                NodeKind::CodeBlock,
                json!({
                    "value": normalized_code_block_value(node, source),
                    "info": node.info,
                }),
                format,
                Vec::new(),
            ))
        }
        md::Block::HtmlBlock(node) => Ok(new_tree(
            NodeKind::HtmlBlock,
            json!({ "value": node.value }),
            empty(),
            Vec::new(),
        )),
        md::Block::Definition(node) => Ok(new_tree(
            NodeKind::Definition,
            json!({
                "identifier": node.identifier,
                "destination": node.destination,
                "title": node.title,
            }),
            json!({
                "label": node.label,
                "destination": link_destination_name(node.destination_kind),
                "title": node.title_kind.map(link_title_name),
            }),
            Vec::new(),
        )),
        md::Block::FootnoteDefinition(node) => Ok(new_tree(
            NodeKind::FootnoteDefinition,
            json!({ "identifier": node.identifier }),
            json!({ "label": node.label }),
            node.children
                .iter()
                .map(|child| tree_from_block(child, source))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        md::Block::Table(node) => tree_from_table(node, source),
        unsupported => Err(PluginError::InvalidInput(format!(
            "GFM parser produced unsupported block node: {unsupported:?}"
        ))),
    }
}

fn normalized_code_block_value(node: &md::CodeBlock, _source: &str) -> String {
    normalize_embedded_line_endings(&node.value)
}

fn normalize_embedded_line_endings(value: &str) -> String {
    value.replace("\r\n", "\n").replace('\r', "\n")
}

fn tree_from_list_item(node: &md::ListItem, source: &str) -> Result<NodeTree, PluginError> {
    Ok(new_tree(
        NodeKind::ListItem,
        json!({ "checked": node.checked }),
        json!({}),
        node.children
            .iter()
            .map(|child| tree_from_block(child, source))
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

fn tree_from_table(node: &md::Table, source: &str) -> Result<NodeTree, PluginError> {
    let mut children = Vec::new();
    let mut column_ids = Vec::new();
    for alignment in &node.alignments {
        let column = new_tree(
            NodeKind::TableColumn,
            json!({
                "alignment": match alignment {
                    md::TableAlignment::None => "none",
                    md::TableAlignment::Left => "left",
                    md::TableAlignment::Center => "center",
                    md::TableAlignment::Right => "right",
                },
            }),
            json!({}),
            Vec::new(),
        );
        column_ids.push(column.node.id.clone());
        children.push(column);
    }
    for (row_index, row) in node.rows.iter().enumerate() {
        let mut cells = Vec::new();
        for (column_index, cell) in row.cells.iter().enumerate() {
            let column_id = column_ids.get(column_index).ok_or_else(|| {
                PluginError::InvalidInput(
                    "GFM table row contains more cells than declared columns".to_string(),
                )
            })?;
            cells.push(new_tree(
                NodeKind::TableCell,
                json!({
                    "column_id": column_id,
                    "inline": leaf_inlines_from_ast(&cell.children, source)?,
                }),
                json!({}),
                Vec::new(),
            ));
        }
        children.push(new_tree(
            NodeKind::TableRow,
            json!({ "role": if row_index == 0 { "header" } else { "body" } }),
            json!({}),
            cells,
        ));
    }
    Ok(new_tree(NodeKind::Table, json!({}), json!({}), children))
}

fn inlines_from_ast(nodes: &[md::Inline], source: &str) -> Result<Vec<InlineNode>, PluginError> {
    nodes
        .iter()
        .map(|node| inline_from_ast(node, source))
        .collect()
}

fn leaf_inlines_from_ast(
    nodes: &[md::Inline],
    source: &str,
) -> Result<Vec<InlineNode>, PluginError> {
    let mut inlines = inlines_from_ast(nodes, source)?;
    if let Some(InlineNode {
        content: InlineContent::Text { value },
        ..
    }) = inlines.last_mut()
    {
        let trimmed = value.trim_end_matches([' ', '\t']).len();
        value.truncate(trimmed);
        if value.is_empty() {
            inlines.pop();
        }
    }
    Ok(inlines)
}

fn inline_from_ast(node: &md::Inline, source: &str) -> Result<InlineNode, PluginError> {
    let (id, content) = match node {
        md::Inline::Text(node) => (
            None,
            InlineContent::Text {
                value: node.value.clone(),
            },
        ),
        md::Inline::Escape(node) => (
            Some(new_inline_id()),
            InlineContent::Escape { value: node.value },
        ),
        md::Inline::CharacterReference(node) => (
            Some(new_inline_id()),
            InlineContent::CharacterReference {
                value: node.value.clone(),
                format: CharacterReferenceFormat {
                    reference: node.reference.clone(),
                },
            },
        ),
        md::Inline::Emphasis(node) => (
            Some(new_inline_id()),
            InlineContent::Emphasis {
                children: inlines_from_ast(&node.children, source)?,
                format: DelimiterFormat {
                    marker: delimiter_from_span(node.meta.span, source, "*"),
                },
            },
        ),
        md::Inline::Strong(node) => (
            Some(new_inline_id()),
            InlineContent::Strong {
                children: inlines_from_ast(&node.children, source)?,
                format: DelimiterFormat {
                    marker: delimiter_from_span(node.meta.span, source, "**"),
                },
            },
        ),
        md::Inline::Delete(node) => (
            Some(new_inline_id()),
            InlineContent::Delete {
                children: inlines_from_ast(&node.children, source)?,
                format: DeleteFormat {
                    marker: match node.marker {
                        md::DeleteMarker::SingleTilde => "~",
                        md::DeleteMarker::DoubleTilde => "~~",
                    }
                    .to_string(),
                },
            },
        ),
        md::Inline::Code(node) => (
            Some(new_inline_id()),
            InlineContent::Code {
                value: node.value.clone(),
                format: InlineCodeFormat {
                    raw: node.raw.clone(),
                    fence_length: node.fence_length,
                },
            },
        ),
        md::Inline::Link(node) => (
            Some(new_inline_id()),
            InlineContent::Link {
                destination: node.destination.clone(),
                title: node.title.clone(),
                children: inlines_from_ast(&node.children, source)?,
                format: ResourceFormat {
                    destination: link_destination_name(node.destination_kind).to_string(),
                    title: node.title_kind.map(link_title_name).map(str::to_string),
                },
            },
        ),
        md::Inline::Image(node) => (
            Some(new_inline_id()),
            InlineContent::Image {
                destination: node.destination.clone(),
                title: node.title.clone(),
                alt: inlines_from_ast(&node.alt, source)?,
                format: ResourceFormat {
                    destination: link_destination_name(node.destination_kind).to_string(),
                    title: node.title_kind.map(link_title_name).map(str::to_string),
                },
            },
        ),
        md::Inline::LinkReference(node) => (
            Some(new_inline_id()),
            InlineContent::LinkReference {
                identifier: node.identifier.clone(),
                children: inlines_from_ast(&node.children, source)?,
                format: ReferenceFormat {
                    label: node.label.clone(),
                    kind: reference_kind_name(node.kind).to_string(),
                },
            },
        ),
        md::Inline::ImageReference(node) => (
            Some(new_inline_id()),
            InlineContent::ImageReference {
                identifier: node.identifier.clone(),
                alt: inlines_from_ast(&node.alt, source)?,
                format: ReferenceFormat {
                    label: node.label.clone(),
                    kind: reference_kind_name(node.kind).to_string(),
                },
            },
        ),
        md::Inline::Autolink(node) => (
            Some(new_inline_id()),
            InlineContent::Autolink {
                destination: node.destination.clone(),
                format: match &node.kind {
                    md::AutolinkKind::Angle => AutolinkFormat {
                        kind: "angle".to_string(),
                        original: None,
                    },
                    md::AutolinkKind::GfmLiteral { original } => AutolinkFormat {
                        kind: "literal".to_string(),
                        original: Some(original.clone()),
                    },
                },
            },
        ),
        md::Inline::Html(node) => (
            Some(new_inline_id()),
            InlineContent::Html {
                value: node.value.clone(),
            },
        ),
        md::Inline::SoftBreak(_) => (None, InlineContent::SoftBreak),
        md::Inline::LineBreak(node) => (
            Some(new_inline_id()),
            InlineContent::LineBreak {
                format: LineBreakFormat {
                    kind: match node.kind {
                        md::LineBreakKind::Backslash => "backslash",
                        md::LineBreakKind::Spaces => "spaces",
                    }
                    .to_string(),
                },
            },
        ),
        md::Inline::FootnoteReference(node) => (
            Some(new_inline_id()),
            InlineContent::FootnoteReference {
                identifier: node.identifier.clone(),
                format: FootnoteReferenceFormat {
                    label: node.label.clone(),
                },
            },
        ),
        unsupported => {
            return Err(PluginError::InvalidInput(format!(
                "GFM parser produced unsupported inline node: {unsupported:?}"
            )));
        }
    };
    Ok(InlineNode { id, content })
}

fn new_inline_id() -> String {
    Uuid::now_v7().to_string()
}

fn delimiter_from_span(span: Option<Span>, source: &str, fallback: &str) -> String {
    span.and_then(|span| source.get(span.start..span.end))
        .and_then(|raw| raw.chars().next())
        .filter(|marker| matches!(marker, '*' | '_'))
        .map(|marker| marker.to_string().repeat(fallback.len()))
        .unwrap_or_else(|| fallback.to_string())
}

pub(crate) fn render_tree(root: &NodeTree) -> Result<Vec<u8>, PluginError> {
    if root.node.kind != NodeKind::Document {
        return Err(PluginError::InvalidInput(
            "Markdown state must contain a document node".to_string(),
        ));
    }
    let mut document = md::Document {
        meta: md::NodeMeta::default(),
        children: root
            .children
            .iter()
            .map(block_from_tree)
            .collect::<Result<Vec<_>, _>>()?,
    };
    let line_ending = match string_field(&root.node.format, "line_ending")? {
        "lf" => LineEnding::Lf,
        "crlf" => LineEnding::CrLf,
        value => return Err(invalid_field(&root.node, "line_ending", value)),
    };
    let final_newline = bool_field(&root.node.format, "final_newline")?;
    let mut options = SerializeOptions::default();
    options.line_ending = line_ending;
    options.final_newline = final_newline;
    let has_empty_fenced_code = contains_empty_fenced_code(&document.children);
    let mut rendered = document
        .to_markdown_with(&options)
        .map_err(|error| PluginError::InvalidInput(format!("invalid Markdown state: {error:?}")))?;
    if has_empty_fenced_code {
        let sentinel = absent_empty_code_sentinel(&rendered);
        replace_empty_fenced_code_values(&mut document.children, &sentinel);
        rendered = document.to_markdown_with(&options).map_err(|error| {
            PluginError::InvalidInput(format!("invalid Markdown state: {error:?}"))
        })?;
        remove_line_containing(&mut rendered, &sentinel);
    }
    Ok(rendered.into_bytes())
}

fn remove_line_containing(source: &mut String, needle: &str) {
    while let Some(position) = source.find(needle) {
        let line_start = source[..position]
            .rfind('\n')
            .map_or(0, |newline| newline + 1);
        let after_needle = position + needle.len();
        let line_end = source[after_needle..]
            .find('\n')
            .map_or(source.len(), |newline| after_needle + newline + 1);
        source.replace_range(line_start..line_end, "");
    }
}

fn contains_empty_fenced_code(blocks: &[md::Block]) -> bool {
    blocks.iter().any(|block| match block {
        md::Block::CodeBlock(node) => {
            matches!(node.kind, md::CodeBlockKind::Fenced { .. }) && node.value.is_empty()
        }
        md::Block::BlockQuote(node) => contains_empty_fenced_code(&node.children),
        md::Block::List(node) => node
            .children
            .iter()
            .any(|item| contains_empty_fenced_code(&item.children)),
        md::Block::FootnoteDefinition(node) => contains_empty_fenced_code(&node.children),
        _ => false,
    })
}

fn replace_empty_fenced_code_values(blocks: &mut [md::Block], sentinel: &str) {
    for block in blocks {
        match block {
            md::Block::CodeBlock(node)
                if matches!(node.kind, md::CodeBlockKind::Fenced { .. })
                    && node.value.is_empty() =>
            {
                node.value = sentinel.to_string();
            }
            md::Block::BlockQuote(node) => {
                replace_empty_fenced_code_values(&mut node.children, sentinel);
            }
            md::Block::List(node) => {
                for item in &mut node.children {
                    replace_empty_fenced_code_values(&mut item.children, sentinel);
                }
            }
            md::Block::FootnoteDefinition(node) => {
                replace_empty_fenced_code_values(&mut node.children, sentinel);
            }
            _ => {}
        }
    }
}

fn absent_empty_code_sentinel(rendered_without_sentinel: &str) -> String {
    for suffix in 0usize.. {
        let sentinel = format!("\u{e000}lix-empty-code:{suffix}\u{e001}");
        if !rendered_without_sentinel.contains(&sentinel) {
            return sentinel;
        }
    }
    unreachable!("usize iteration is unbounded")
}

fn block_from_tree(tree: &NodeTree) -> Result<md::Block, PluginError> {
    let meta = md::NodeMeta::default();
    match tree.node.kind {
        NodeKind::Paragraph => Ok(md::Block::Paragraph(md::Paragraph {
            meta,
            children: inlines_to_ast(&parse_inline_payload_plugin(&tree.node)?)?,
        })),
        NodeKind::Heading => Ok(md::Block::Heading(md::Heading {
            meta,
            depth: u8_field(&tree.node.payload, "depth")?,
            kind: match string_field(&tree.node.format, "style")? {
                "atx" => md::HeadingKind::Atx,
                "setext" => md::HeadingKind::Setext,
                value => return Err(invalid_field(&tree.node, "style", value)),
            },
            children: inlines_to_ast(&parse_inline_payload_plugin(&tree.node)?)?,
        })),
        NodeKind::ThematicBreak => Ok(md::Block::ThematicBreak(md::ThematicBreak {
            meta,
            marker: match string_field(&tree.node.format, "marker")? {
                "dash" => md::ThematicBreakMarker::Dash,
                "asterisk" => md::ThematicBreakMarker::Asterisk,
                "underscore" => md::ThematicBreakMarker::Underscore,
                value => return Err(invalid_field(&tree.node, "marker", value)),
            },
        })),
        NodeKind::Frontmatter => Ok(md::Block::Frontmatter(md::Frontmatter {
            meta,
            kind: match string_field(&tree.node.payload, "kind")? {
                "yaml" => md::FrontmatterKind::Yaml,
                "toml" => md::FrontmatterKind::Toml,
                value => return Err(invalid_field(&tree.node, "kind", value)),
            },
            value: owned_string_field(&tree.node.payload, "value")?,
        })),
        NodeKind::BlockQuote => Ok(md::Block::BlockQuote(md::BlockQuote {
            meta,
            children: child_blocks(tree)?,
        })),
        NodeKind::List => Ok(md::Block::List(md::List {
            meta,
            ordered: bool_field(&tree.node.payload, "ordered")?,
            start: optional_u64_field(&tree.node.payload, "start")?,
            delimiter: parse_list_delimiter(
                string_field(&tree.node.format, "delimiter")?,
                &tree.node,
            )?,
            tight: bool_field(&tree.node.payload, "tight")?,
            children: tree
                .children
                .iter()
                .map(list_item_from_tree)
                .collect::<Result<Vec<_>, _>>()?,
        })),
        NodeKind::CodeBlock => {
            let style = string_field(&tree.node.format, "style")?;
            let kind = match style {
                "indented" => md::CodeBlockKind::Indented,
                "fenced" => md::CodeBlockKind::Fenced {
                    marker: match string_field(&tree.node.format, "marker")? {
                        "backtick" => md::FenceMarker::Backtick,
                        "tilde" => md::FenceMarker::Tilde,
                        value => return Err(invalid_field(&tree.node, "marker", value)),
                    },
                    length: usize_field(&tree.node.format, "fence_length")?,
                },
                value => return Err(invalid_field(&tree.node, "style", value)),
            };
            Ok(md::Block::CodeBlock(md::CodeBlock {
                meta,
                kind,
                info: optional_string_field(&tree.node.payload, "info")?,
                value: owned_string_field(&tree.node.payload, "value")?,
            }))
        }
        NodeKind::HtmlBlock => Ok(md::Block::HtmlBlock(md::HtmlBlock {
            meta,
            value: owned_string_field(&tree.node.payload, "value")?,
        })),
        NodeKind::Definition => Ok(md::Block::Definition(md::Definition {
            meta: authored_meta(),
            label: owned_string_field(&tree.node.format, "label")?,
            identifier: owned_string_field(&tree.node.payload, "identifier")?,
            destination: owned_string_field(&tree.node.payload, "destination")?,
            destination_kind: parse_link_destination(
                string_field(&tree.node.format, "destination")?,
                &tree.node,
            )?,
            title: optional_string_field(&tree.node.payload, "title")?,
            title_kind: optional_string_field(&tree.node.format, "title")?
                .as_deref()
                .map(|value| parse_link_title(value, &tree.node))
                .transpose()?,
        })),
        NodeKind::FootnoteDefinition => Ok(md::Block::FootnoteDefinition(md::FootnoteDefinition {
            meta: authored_meta(),
            label: owned_string_field(&tree.node.format, "label")?,
            identifier: owned_string_field(&tree.node.payload, "identifier")?,
            children: child_blocks(tree)?,
        })),
        NodeKind::Table => table_from_tree(tree),
        unexpected => Err(PluginError::InvalidInput(format!(
            "node '{}' of kind {unexpected:?} cannot appear in a block position",
            tree.node.id
        ))),
    }
}

fn child_blocks(tree: &NodeTree) -> Result<Vec<md::Block>, PluginError> {
    tree.children.iter().map(block_from_tree).collect()
}

fn list_item_from_tree(tree: &NodeTree) -> Result<md::ListItem, PluginError> {
    if tree.node.kind != NodeKind::ListItem {
        return Err(PluginError::InvalidInput(format!(
            "list node contains non-list-item child '{}'",
            tree.node.id
        )));
    }
    Ok(md::ListItem {
        meta: md::NodeMeta::default(),
        checked: optional_bool_field(&tree.node.payload, "checked")?,
        children: child_blocks(tree)?,
    })
}

fn table_from_tree(tree: &NodeTree) -> Result<md::Block, PluginError> {
    let columns = tree
        .children
        .iter()
        .filter(|child| child.node.kind == NodeKind::TableColumn)
        .collect::<Vec<_>>();
    let rows = tree
        .children
        .iter()
        .filter(|child| child.node.kind == NodeKind::TableRow)
        .collect::<Vec<_>>();
    let mut column_indices = BTreeMap::new();
    let alignments = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            column_indices.insert(column.node.id.as_str(), index);
            match string_field(&column.node.payload, "alignment")? {
                "none" => Ok(md::TableAlignment::None),
                "left" => Ok(md::TableAlignment::Left),
                "center" => Ok(md::TableAlignment::Center),
                "right" => Ok(md::TableAlignment::Right),
                value => Err(invalid_field(&column.node, "alignment", value)),
            }
        })
        .collect::<Result<Vec<_>, PluginError>>()?;
    let rows = rows
        .iter()
        .map(|row| {
            let mut cells = vec![None; columns.len()];
            for cell in &row.children {
                if cell.node.kind != NodeKind::TableCell {
                    return Err(PluginError::InvalidInput(format!(
                        "table row '{}' contains non-cell child '{}'",
                        row.node.id, cell.node.id
                    )));
                }
                let column_id = string_field(&cell.node.payload, "column_id")?;
                let index = *column_indices.get(column_id).ok_or_else(|| {
                    PluginError::InvalidInput(format!(
                        "table cell '{}' references unknown column '{column_id}'",
                        cell.node.id
                    ))
                })?;
                if cells[index].is_some() {
                    return Err(PluginError::InvalidInput(format!(
                        "table row '{}' contains multiple cells for column '{column_id}'",
                        row.node.id
                    )));
                }
                cells[index] = Some(md::TableCell {
                    meta: md::NodeMeta::default(),
                    children: inlines_to_ast(&parse_inline_payload_plugin(&cell.node)?)?,
                });
            }
            let cells = cells
                .into_iter()
                .enumerate()
                .map(|(index, cell)| {
                    cell.ok_or_else(|| {
                        PluginError::InvalidInput(format!(
                            "table row '{}' is missing a cell for column '{}'",
                            row.node.id, columns[index].node.id
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(md::TableRow {
                meta: md::NodeMeta::default(),
                cells,
            })
        })
        .collect::<Result<Vec<_>, PluginError>>()?;
    Ok(md::Block::Table(md::Table {
        meta: md::NodeMeta::default(),
        alignments,
        rows,
    }))
}

fn inlines_to_ast(nodes: &[InlineNode]) -> Result<Vec<md::Inline>, PluginError> {
    let mut output = Vec::new();
    let mut index = 0;
    while let Some(node) = nodes.get(index) {
        if let InlineContent::Text { value } = &node.content {
            if let Some((source, delimiter)) =
                nodes.get(index + 1).and_then(ambiguous_delimited_source)
                && value.ends_with(delimiter)
            {
                output.push(raw_inline(&format!("{value}{source}")));
                index += 2;
                continue;
            }
            let follows_autolink = index
                .checked_sub(1)
                .and_then(|previous| nodes.get(previous))
                .is_some_and(|previous| matches!(previous.content, InlineContent::Autolink { .. }));
            let precedes_autolink = nodes
                .get(index + 1)
                .is_some_and(|next| matches!(next.content, InlineContent::Autolink { .. }));
            let mut body = value.as_str();
            if follows_autolink && body.starts_with(']') {
                output.push(raw_inline("]"));
                body = &body[1..];
            }
            let trailing_bracket = precedes_autolink && body.ends_with('[');
            if trailing_bracket {
                body = &body[..body.len() - 1];
            }
            if !body.is_empty() {
                if follows_autolink {
                    output.push(raw_inline(body));
                } else {
                    output.push(md::Inline::Text(md::Text {
                        meta: md::NodeMeta::default(),
                        value: body.to_string(),
                    }));
                }
            }
            if trailing_bracket {
                output.push(raw_inline("["));
            }
            index += 1;
            continue;
        }
        if let Some((mut source, delimiter)) = ambiguous_delimited_source(node) {
            if let Some(InlineNode {
                content: InlineContent::Text { value },
                ..
            }) = nodes.get(index + 1)
                && value.starts_with(delimiter)
            {
                source.push_str(value);
                index += 1;
            }
            output.push(raw_inline(&source));
            index += 1;
            continue;
        }
        inline_to_ast(node, &mut output)?;
        index += 1;
    }
    Ok(output)
}

fn raw_inline(value: &str) -> md::Inline {
    md::Inline::Html(md::HtmlInline {
        meta: md::NodeMeta::default(),
        value: value.to_string(),
    })
}

fn inline_to_ast(node: &InlineNode, output: &mut Vec<md::Inline>) -> Result<(), PluginError> {
    let meta = md::NodeMeta::default();
    match &node.content {
        InlineContent::Text { value } => output.push(md::Inline::Text(md::Text {
            meta,
            value: value.clone(),
        })),
        InlineContent::Escape { value } => output.push(md::Inline::Escape(md::Escape {
            meta,
            value: *value,
        })),
        InlineContent::CharacterReference { value, format } => {
            output.push(md::Inline::CharacterReference(md::CharacterReference {
                meta,
                reference: format.reference.clone(),
                value: value.clone(),
            }));
        }
        InlineContent::Emphasis { children, format } => {
            validate_delimiter(&format.marker, false)?;
            if delimiter_content_is_ambiguous(children, &format.marker) {
                output.push(md::Inline::Emphasis(md::Emphasis {
                    meta,
                    children: inlines_to_ast(children)?,
                }));
            } else {
                push_delimited_inlines(output, &format.marker, children)?;
            }
        }
        InlineContent::Strong { children, format } => {
            validate_delimiter(&format.marker, true)?;
            if delimiter_content_is_ambiguous(children, &format.marker) {
                output.push(md::Inline::Strong(md::Strong {
                    meta,
                    children: inlines_to_ast(children)?,
                }));
            } else {
                push_delimited_inlines(output, &format.marker, children)?;
            }
        }
        InlineContent::Delete { children, format } => {
            let marker = match format.marker.as_str() {
                "~" => md::DeleteMarker::SingleTilde,
                "~~" => md::DeleteMarker::DoubleTilde,
                value => return Err(invalid_inline_field(node, "marker", value)),
            };
            output.push(md::Inline::Delete(md::Delete {
                meta,
                marker,
                children: inlines_to_ast(children)?,
            }));
        }
        InlineContent::Code { value, format } => output.push(md::Inline::Code(md::CodeInline {
            meta,
            value: value.clone(),
            raw: format.raw.clone(),
            fence_length: format.fence_length,
        })),
        InlineContent::Link {
            destination,
            title,
            children,
            format,
        } => output.push(md::Inline::Link(md::Link {
            meta,
            destination: destination.clone(),
            destination_kind: parse_link_destination_inline(&format.destination, node)?,
            title: title.clone(),
            title_kind: format
                .title
                .as_deref()
                .map(|value| parse_link_title_inline(value, node))
                .transpose()?,
            children: inlines_to_ast(children)?,
        })),
        InlineContent::Image {
            destination,
            title,
            alt,
            format,
        } => output.push(md::Inline::Image(md::Image {
            meta,
            destination: destination.clone(),
            destination_kind: parse_link_destination_inline(&format.destination, node)?,
            title: title.clone(),
            title_kind: format
                .title
                .as_deref()
                .map(|value| parse_link_title_inline(value, node))
                .transpose()?,
            alt: inlines_to_ast(alt)?,
        })),
        InlineContent::LinkReference {
            identifier,
            children,
            format,
        } => output.push(md::Inline::LinkReference(md::LinkReference {
            meta: authored_meta(),
            identifier: identifier.clone(),
            label: format.label.clone(),
            kind: parse_reference_kind(&format.kind, node)?,
            children: inlines_to_ast(children)?,
        })),
        InlineContent::ImageReference {
            identifier,
            alt,
            format,
        } => output.push(md::Inline::ImageReference(md::ImageReference {
            meta: authored_meta(),
            identifier: identifier.clone(),
            label: format.label.clone(),
            kind: parse_reference_kind(&format.kind, node)?,
            alt: inlines_to_ast(alt)?,
        })),
        InlineContent::Autolink {
            destination,
            format,
        } => output.push(md::Inline::Autolink(md::Autolink {
            meta,
            destination: destination.clone(),
            kind: match format.kind.as_str() {
                "angle" => md::AutolinkKind::Angle,
                "literal" => md::AutolinkKind::GfmLiteral {
                    original: format.original.clone().ok_or_else(|| {
                        PluginError::InvalidInput(
                            "literal autolink format must contain original spelling".to_string(),
                        )
                    })?,
                },
                value => return Err(invalid_inline_field(node, "kind", value)),
            },
        })),
        InlineContent::Html { value } => output.push(md::Inline::Html(md::HtmlInline {
            meta,
            value: value.clone(),
        })),
        InlineContent::SoftBreak => output.push(md::Inline::SoftBreak(md::SoftBreak { meta })),
        InlineContent::LineBreak { format } => {
            output.push(md::Inline::LineBreak(md::LineBreak {
                meta,
                kind: match format.kind.as_str() {
                    "backslash" => md::LineBreakKind::Backslash,
                    "spaces" => md::LineBreakKind::Spaces,
                    value => return Err(invalid_inline_field(node, "kind", value)),
                },
            }));
        }
        InlineContent::FootnoteReference { identifier, format } => {
            output.push(md::Inline::FootnoteReference(md::FootnoteReference {
                meta: authored_meta(),
                label: format.label.clone(),
                identifier: identifier.clone(),
            }));
        }
    }
    Ok(())
}

fn push_delimited_inlines(
    output: &mut Vec<md::Inline>,
    marker: &str,
    children: &[InlineNode],
) -> Result<(), PluginError> {
    if !matches!(marker, "*" | "_" | "**" | "__") {
        return Err(PluginError::InvalidInput(format!(
            "unsupported emphasis delimiter '{marker}'"
        )));
    }
    output.push(md::Inline::Html(md::HtmlInline {
        meta: md::NodeMeta::default(),
        value: marker.to_string(),
    }));
    output.extend(inlines_to_ast(children)?);
    output.push(md::Inline::Html(md::HtmlInline {
        meta: md::NodeMeta::default(),
        value: marker.to_string(),
    }));
    Ok(())
}

fn validate_delimiter(marker: &str, strong: bool) -> Result<(), PluginError> {
    let valid = if strong {
        matches!(marker, "**" | "__")
    } else {
        matches!(marker, "*" | "_")
    };
    if valid {
        Ok(())
    } else {
        Err(PluginError::InvalidInput(format!(
            "unsupported emphasis delimiter '{marker}'"
        )))
    }
}

fn ambiguous_delimited_source(node: &InlineNode) -> Option<(String, char)> {
    let (children, marker, strong) = match &node.content {
        InlineContent::Emphasis { children, format } => {
            (children.as_slice(), format.marker.as_str(), false)
        }
        InlineContent::Strong { children, format } => {
            (children.as_slice(), format.marker.as_str(), true)
        }
        _ => return None,
    };
    validate_delimiter(marker, strong).ok()?;
    if !delimiter_content_is_ambiguous(children, marker) {
        return None;
    }
    let mut source = marker.to_string();
    for child in children {
        source.push_str(&simple_inline_source(child)?);
    }
    source.push_str(marker);
    Some((source, marker.chars().next()?))
}

fn delimiter_content_is_ambiguous(children: &[InlineNode], marker: &str) -> bool {
    let delimiter = marker.as_bytes()[0] as char;
    children
        .iter()
        .any(|child| inline_source_contains_delimiter(child, delimiter))
}

fn inline_source_contains_delimiter(node: &InlineNode, delimiter: char) -> bool {
    match &node.content {
        InlineContent::Text { value } | InlineContent::Html { value } => value.contains(delimiter),
        InlineContent::Escape { value } => *value == delimiter,
        InlineContent::CharacterReference { format, .. } => format.reference.contains(delimiter),
        InlineContent::Emphasis { children, format }
        | InlineContent::Strong { children, format } => {
            format.marker.contains(delimiter)
                || children
                    .iter()
                    .any(|child| inline_source_contains_delimiter(child, delimiter))
        }
        InlineContent::Delete { children, format } => {
            format.marker.contains(delimiter)
                || children
                    .iter()
                    .any(|child| inline_source_contains_delimiter(child, delimiter))
        }
        InlineContent::Code { format, .. } => format.raw.contains(delimiter) || delimiter == '`',
        InlineContent::Link { children, .. } | InlineContent::LinkReference { children, .. } => {
            children
                .iter()
                .any(|child| inline_source_contains_delimiter(child, delimiter))
        }
        InlineContent::Image { alt, .. } | InlineContent::ImageReference { alt, .. } => alt
            .iter()
            .any(|child| inline_source_contains_delimiter(child, delimiter)),
        InlineContent::Autolink {
            destination,
            format,
        } => {
            destination.contains(delimiter)
                || format
                    .original
                    .as_deref()
                    .is_some_and(|original| original.contains(delimiter))
        }
        InlineContent::SoftBreak
        | InlineContent::LineBreak { .. }
        | InlineContent::FootnoteReference { .. } => false,
    }
}

fn simple_inline_source(node: &InlineNode) -> Option<String> {
    match &node.content {
        InlineContent::Text { value } | InlineContent::Html { value } => Some(value.clone()),
        InlineContent::Escape { value } => Some(format!("\\{value}")),
        InlineContent::CharacterReference { format, .. } => Some(format.reference.clone()),
        InlineContent::Emphasis { children, format } => {
            simple_delimited_source(children, &format.marker, false)
        }
        InlineContent::Strong { children, format } => {
            simple_delimited_source(children, &format.marker, true)
        }
        InlineContent::Delete { children, format } => {
            if !matches!(format.marker.as_str(), "~" | "~~") {
                return None;
            }
            simple_delimited_source(children, &format.marker, format.marker.len() == 2)
        }
        InlineContent::SoftBreak => Some("\n".to_string()),
        InlineContent::LineBreak { format } => match format.kind.as_str() {
            "backslash" => Some("\\\n".to_string()),
            "spaces" => Some("  \n".to_string()),
            _ => None,
        },
        _ => None,
    }
}

fn simple_delimited_source(children: &[InlineNode], marker: &str, strong: bool) -> Option<String> {
    if marker.starts_with(['*', '_']) {
        validate_delimiter(marker, strong).ok()?;
    }
    let mut source = marker.to_string();
    for child in children {
        source.push_str(&simple_inline_source(child)?);
    }
    source.push_str(marker);
    Some(source)
}

fn authored_meta() -> md::NodeMeta {
    md::NodeMeta::new(Some(Span::new(0, 0)))
}

fn parse_inline_payload_plugin(node: &NodeSnapshot) -> Result<Vec<InlineNode>, PluginError> {
    parse_inline_payload(&node.payload).map_err(|message| {
        PluginError::InvalidInput(format!(
            "invalid {} node '{}': {message}",
            kind_name(node.kind),
            node.id
        ))
    })
}

fn list_delimiter_name(delimiter: md::ListDelimiter) -> &'static str {
    match delimiter {
        md::ListDelimiter::Dash => "dash",
        md::ListDelimiter::Asterisk => "asterisk",
        md::ListDelimiter::Plus => "plus",
        md::ListDelimiter::Period => "period",
        md::ListDelimiter::Paren => "paren",
    }
}

fn parse_list_delimiter(
    value: &str,
    node: &NodeSnapshot,
) -> Result<md::ListDelimiter, PluginError> {
    match value {
        "dash" => Ok(md::ListDelimiter::Dash),
        "asterisk" => Ok(md::ListDelimiter::Asterisk),
        "plus" => Ok(md::ListDelimiter::Plus),
        "period" => Ok(md::ListDelimiter::Period),
        "paren" => Ok(md::ListDelimiter::Paren),
        value => Err(invalid_field(node, "delimiter", value)),
    }
}

fn link_destination_name(kind: md::LinkDestinationKind) -> &'static str {
    match kind {
        md::LinkDestinationKind::Bare => "bare",
        md::LinkDestinationKind::Angle => "angle",
        md::LinkDestinationKind::Omitted => "omitted",
    }
}

fn parse_link_destination(
    value: &str,
    node: &NodeSnapshot,
) -> Result<md::LinkDestinationKind, PluginError> {
    match value {
        "bare" => Ok(md::LinkDestinationKind::Bare),
        "angle" => Ok(md::LinkDestinationKind::Angle),
        "omitted" => Ok(md::LinkDestinationKind::Omitted),
        value => Err(invalid_field(node, "destination", value)),
    }
}

fn parse_link_destination_inline(
    value: &str,
    node: &InlineNode,
) -> Result<md::LinkDestinationKind, PluginError> {
    match value {
        "bare" => Ok(md::LinkDestinationKind::Bare),
        "angle" => Ok(md::LinkDestinationKind::Angle),
        "omitted" => Ok(md::LinkDestinationKind::Omitted),
        value => Err(invalid_inline_field(node, "destination", value)),
    }
}

fn link_title_name(kind: md::LinkTitleKind) -> &'static str {
    match kind {
        md::LinkTitleKind::DoubleQuote => "double_quote",
        md::LinkTitleKind::SingleQuote => "single_quote",
        md::LinkTitleKind::Paren => "paren",
    }
}

fn parse_link_title(value: &str, node: &NodeSnapshot) -> Result<md::LinkTitleKind, PluginError> {
    match value {
        "double_quote" => Ok(md::LinkTitleKind::DoubleQuote),
        "single_quote" => Ok(md::LinkTitleKind::SingleQuote),
        "paren" => Ok(md::LinkTitleKind::Paren),
        value => Err(invalid_field(node, "title", value)),
    }
}

fn parse_link_title_inline(
    value: &str,
    node: &InlineNode,
) -> Result<md::LinkTitleKind, PluginError> {
    match value {
        "double_quote" => Ok(md::LinkTitleKind::DoubleQuote),
        "single_quote" => Ok(md::LinkTitleKind::SingleQuote),
        "paren" => Ok(md::LinkTitleKind::Paren),
        value => Err(invalid_inline_field(node, "title", value)),
    }
}

fn reference_kind_name(kind: md::ReferenceKind) -> &'static str {
    match kind {
        md::ReferenceKind::Full => "full",
        md::ReferenceKind::Collapsed => "collapsed",
        md::ReferenceKind::Shortcut => "shortcut",
    }
}

fn parse_reference_kind(value: &str, node: &InlineNode) -> Result<md::ReferenceKind, PluginError> {
    match value {
        "full" => Ok(md::ReferenceKind::Full),
        "collapsed" => Ok(md::ReferenceKind::Collapsed),
        "shortcut" => Ok(md::ReferenceKind::Shortcut),
        value => Err(invalid_inline_field(node, "kind", value)),
    }
}

fn string_field<'a>(value: &'a Value, field: &str) -> Result<&'a str, PluginError> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        PluginError::InvalidInput(format!("Markdown state field '{field}' must be a string"))
    })
}

fn owned_string_field(value: &Value, field: &str) -> Result<String, PluginError> {
    string_field(value, field).map(str::to_string)
}

fn optional_string_field(value: &Value, field: &str) -> Result<Option<String>, PluginError> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => Err(PluginError::InvalidInput(format!(
            "Markdown state field '{field}' must be a string or null"
        ))),
    }
}

fn bool_field(value: &Value, field: &str) -> Result<bool, PluginError> {
    value.get(field).and_then(Value::as_bool).ok_or_else(|| {
        PluginError::InvalidInput(format!("Markdown state field '{field}' must be a boolean"))
    })
}

fn optional_bool_field(value: &Value, field: &str) -> Result<Option<bool>, PluginError> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        _ => Err(PluginError::InvalidInput(format!(
            "Markdown state field '{field}' must be a boolean or null"
        ))),
    }
}

fn optional_u64_field(value: &Value, field: &str) -> Result<Option<u64>, PluginError> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value.as_u64().map(Some).ok_or_else(|| {
            PluginError::InvalidInput(format!(
                "Markdown state field '{field}' must be an unsigned integer or null"
            ))
        }),
        _ => Err(PluginError::InvalidInput(format!(
            "Markdown state field '{field}' must be an unsigned integer or null"
        ))),
    }
}

fn usize_field(value: &Value, field: &str) -> Result<usize, PluginError> {
    let value = value.get(field).and_then(Value::as_u64).ok_or_else(|| {
        PluginError::InvalidInput(format!(
            "Markdown state field '{field}' must be an unsigned integer"
        ))
    })?;
    usize::try_from(value).map_err(|_| {
        PluginError::InvalidInput(format!("Markdown state field '{field}' is too large"))
    })
}

fn u8_field(value: &Value, field: &str) -> Result<u8, PluginError> {
    let value = value.get(field).and_then(Value::as_u64).ok_or_else(|| {
        PluginError::InvalidInput(format!(
            "Markdown state field '{field}' must be an unsigned integer"
        ))
    })?;
    u8::try_from(value).map_err(|_| {
        PluginError::InvalidInput(format!("Markdown state field '{field}' is too large"))
    })
}

fn invalid_field(node: &NodeSnapshot, field: &str, value: &str) -> PluginError {
    PluginError::InvalidInput(format!(
        "node '{}' has unsupported {field} value '{value}'",
        node.id
    ))
}

fn invalid_inline_field(node: &InlineNode, field: &str, value: &str) -> PluginError {
    PluginError::InvalidInput(format!(
        "inline {} node has unsupported {field} value '{value}'",
        node.kind_tag()
    ))
}

fn kind_name(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::Document => "document",
        NodeKind::Frontmatter => "frontmatter",
        NodeKind::Paragraph => "paragraph",
        NodeKind::Heading => "heading",
        NodeKind::ThematicBreak => "thematic_break",
        NodeKind::BlockQuote => "block_quote",
        NodeKind::List => "list",
        NodeKind::ListItem => "list_item",
        NodeKind::CodeBlock => "code_block",
        NodeKind::HtmlBlock => "html_block",
        NodeKind::Definition => "definition",
        NodeKind::FootnoteDefinition => "footnote_definition",
        NodeKind::Table => "table",
        NodeKind::TableColumn => "table_column",
        NodeKind::TableRow => "table_row",
        NodeKind::TableCell => "table_cell",
    }
}
