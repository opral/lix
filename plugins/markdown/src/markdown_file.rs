use crate::core::{File, PARSED_ROOT_ID, PluginError};
use crate::markdown_syntax::ast as md;
use crate::markdown_syntax::{LineEnding, SerializeOptions, Span, SyntaxOptions};
use crate::model::{
    AutolinkFormat, CharacterReferenceFormat, DeleteFormat, DelimiterFormat,
    FootnoteReferenceFormat, InlineCodeFormat, InlineContent, InlineNode, LineBreakFormat,
    NodeKind, NodeSnapshot, NodeTree, ReferenceFormat, ResourceFormat, inline_payload,
    parse_inline_payload,
};
use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use encoding_rs::Encoding;
use serde_json::{Value, json};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::ops::Range;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParsedMarkdown {
    pub(crate) root: NodeTree,
    pub(crate) top_level_ranges: Vec<Range<usize>>,
    /// The source was proven to be canonical literal prose directly from the
    /// parser AST and source spans. `parse_markdown_source` can then retain
    /// the original bytes without building and serializing a second AST.
    pub(crate) canonical_literal_paragraph_layout: bool,
    /// The stable canonical bytes already rendered while parsing. Callers that
    /// need to compare source layout can consume this instead of rendering the
    /// reconstructed tree a second time.
    pub(crate) canonical_render: Option<Vec<u8>>,
}

pub(crate) fn parse_file(file: &File) -> Result<ParsedMarkdown, PluginError> {
    parse_file_with_literal_fast_path(file, true)
}

pub(crate) fn parse_file_with_literal_fast_path(
    file: &File,
    allow_literal_fast_path: bool,
) -> Result<ParsedMarkdown, PluginError> {
    let (buf, encoding) = buffer_with_encoding(&file.content);
    let (decoded, _had_errors) = encoding.decode_without_bom_handling(buf);
    parse_markdown_source_with_literal_fast_path(&decoded, allow_literal_fast_path)
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
    parse_markdown_source_with_literal_fast_path(source, true)
}

fn parse_markdown_source_with_literal_fast_path(
    source: &str,
    allow_literal_fast_path: bool,
) -> Result<ParsedMarkdown, PluginError> {
    let mut parsed = parse_markdown_source_once(source)?;
    let source_is_canonical_literal_paragraph_layout = parsed.canonical_literal_paragraph_layout;
    if allow_literal_fast_path && source_is_canonical_literal_paragraph_layout {
        parsed.canonical_render = Some(source.as_bytes().to_vec());
        return Ok(parsed);
    }
    let mut canonical = source.to_string();
    for _ in 0..8 {
        let rendered = fully_escape_orphan_table_delimiters(render_tree(&parsed.root)?);
        if rendered == canonical.as_bytes() {
            parsed.canonical_literal_paragraph_layout =
                source_is_canonical_literal_paragraph_layout;
            parsed.canonical_render = Some(rendered);
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

fn fully_escape_orphan_table_delimiters(rendered: Vec<u8>) -> Vec<u8> {
    let mut output = Vec::with_capacity(rendered.len());
    for line in rendered.split_inclusive(|byte| *byte == b'\n') {
        let content = line
            .strip_suffix(b"\n")
            .unwrap_or(line)
            .strip_suffix(b"\r")
            .unwrap_or_else(|| line.strip_suffix(b"\n").unwrap_or(line));
        let leading_whitespace = content
            .iter()
            .position(|byte| !byte.is_ascii_whitespace())
            .unwrap_or(content.len());
        let trimmed = &content[leading_whitespace..];
        let candidate = trimmed.starts_with(b"|")
            && trimmed.ends_with(b"|")
            && trimmed.contains(&b'\\')
            && trimmed
                .iter()
                .all(|byte| matches!(byte, b'|' | b'-' | b':' | b'\\' | b' ' | b'\t'));
        if !candidate {
            output.extend_from_slice(line);
            continue;
        }
        let mut previous = None;
        for byte in line {
            if *byte == b'-' && previous != Some(b'\\') {
                output.push(b'\\');
            }
            output.push(*byte);
            previous = Some(*byte);
        }
    }
    output
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
        .find(|diagnostic| diagnostic.severity == crate::markdown_syntax::DiagnosticSeverity::Error)
    {
        return Err(PluginError::InvalidInput(format!(
            "file.content must be valid GitHub Flavored Markdown: {}",
            diagnostic.message
        )));
    }

    repair_definition_adjacency(&mut output.document, source);
    let canonical_literal_paragraph_layout =
        canonical_literal_paragraph_layout(&output.document, source);
    let top_level_ranges = output
        .document
        .children
        .iter()
        .map(block_span)
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            PluginError::Internal("Markdown parser omitted a top-level block span".to_owned())
        })?
        .into_iter()
        .map(|span| span.start..span.end)
        .collect();
    let parse_ids = ParseIds::default();
    let children = output
        .document
        .children
        .iter()
        .map(|block| tree_from_block(block, source, &parse_ids))
        .collect::<Result<Vec<_>, _>>()?;
    let root = NodeTree {
        node: NodeSnapshot {
            id: PARSED_ROOT_ID,
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

    Ok(ParsedMarkdown {
        root,
        top_level_ranges,
        canonical_literal_paragraph_layout,
        canonical_render: None,
    })
}

/// Proves that the parser produced the exact canonical form for a deliberately
/// narrow, high-frequency Markdown shape: one literal text paragraph per
/// line, blank-line separated, ending in LF. The predicate runs on parser AST
/// spans before NodeTree payload JSON is built, so its linear scan is much
/// cheaper than the canonical serializer/fixpoint work it avoids.
///
/// The character filter excludes every ASCII construct whose Markdown
/// serializer may add an escape. It intentionally accepts ordinary prose,
/// Unicode, and punctuation that is never escaped in a literal paragraph.
fn canonical_literal_paragraph_layout(document: &md::Document, source: &str) -> bool {
    if source.is_empty() || !source.ends_with('\n') || source.as_bytes().contains(&b'\r') {
        return false;
    }
    let Some(body) = source.strip_suffix('\n') else {
        return false;
    };
    if body.is_empty() || document.children.is_empty() {
        return false;
    }

    let mut start = 0;
    for (index, block) in document.children.iter().enumerate() {
        let last = index + 1 == document.children.len();
        let end = if last {
            body.len()
        } else {
            let Some(relative) = body[start..].find("\n\n") else {
                return false;
            };
            start + relative
        };
        let Some(raw) = source.get(start..end) else {
            return false;
        };
        if !literal_paragraph_source_is_safe(raw) {
            return false;
        }

        let md::Block::Paragraph(paragraph) = block else {
            return false;
        };
        let Some(paragraph_span) = paragraph.meta.span else {
            return false;
        };
        if paragraph_span.start != start || paragraph_span.end != end {
            return false;
        }
        let [md::Inline::Text(text)] = paragraph.children.as_slice() else {
            return false;
        };
        let Some(text_span) = text.meta.span else {
            return false;
        };
        if text_span.start != start || text_span.end != end || text.value != raw {
            return false;
        }

        if last {
            return end + 1 == source.len();
        }
        start = end + 2;
    }
    false
}

fn literal_paragraph_source_is_safe(raw: &str) -> bool {
    let Some(first) = raw.chars().next() else {
        return false;
    };
    if matches!(first, ' ' | '\t' | '-' | '+' | '=' | '>')
        || matches!(raw.chars().last(), Some(' ' | '\t'))
        || raw.contains('\n')
        || starts_with_ambiguous_ordered_list_marker(raw)
    {
        return false;
    }
    let bytes = raw.as_bytes();
    for (index, &byte) in bytes.iter().enumerate() {
        if matches!(
            byte,
            b'\\'
                | b'&'
                | b'`'
                | b'|'
                | b'*'
                | b'_'
                | b'+'
                | b'='
                | b'~'
                | b'^'
                | b'['
                | b']'
                | b'$'
                | b'<'
                | b'>'
                | b'{'
                | b'}'
                | b':'
                | b'@'
                | b'#'
                | b'\0'
        ) {
            return false;
        }
        if byte == b'.' && index >= 3 && bytes[index - 3..index].eq_ignore_ascii_case(b"www") {
            return false;
        }
    }
    raw.chars().all(|character| {
        character == ' ' || (!character.is_control() && !character.is_whitespace())
    })
}

fn starts_with_ambiguous_ordered_list_marker(raw: &str) -> bool {
    let digits = raw.bytes().take_while(u8::is_ascii_digit).count();
    let Some(marker) = raw.as_bytes().get(digits) else {
        return false;
    };
    if !matches!(*marker, b'.' | b')') {
        return false;
    }
    raw.get(digits + 1..)
        .and_then(|tail| tail.chars().next())
        .is_none_or(char::is_whitespace)
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

#[derive(Default)]
struct ParseIds(Cell<u32>);

impl ParseIds {
    fn next(&self) -> uuid::Uuid {
        let ordinal = self.0.get();
        self.0.set(
            ordinal
                .checked_add(1)
                .expect("one Markdown parse cannot contain more than u32::MAX identified nodes"),
        );
        uuid::Uuid::from_u128(u128::from(ordinal) + 1)
    }
}

fn new_tree(
    ids: &ParseIds,
    kind: NodeKind,
    payload: Value,
    format: Value,
    children: Vec<NodeTree>,
) -> NodeTree {
    NodeTree {
        node: NodeSnapshot {
            id: ids.next(),
            kind,
            parent_id: None,
            order_key: None,
            payload,
            format,
        },
        children,
    }
}

fn tree_from_block(
    block: &md::Block,
    source: &str,
    ids: &ParseIds,
) -> Result<NodeTree, PluginError> {
    let empty = || json!({});
    match block {
        md::Block::Paragraph(node) => Ok(new_tree(
            ids,
            NodeKind::Paragraph,
            inline_payload(leaf_inlines_from_ast(&node.children, source, ids)?),
            empty(),
            Vec::new(),
        )),
        md::Block::Heading(node) => Ok(new_tree(
            ids,
            NodeKind::Heading,
            json!({
                "depth": node.depth,
                "inline": leaf_inlines_from_ast(&node.children, source, ids)?,
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
            ids,
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
            ids,
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
            ids,
            NodeKind::BlockQuote,
            empty(),
            empty(),
            node.children
                .iter()
                .map(|child| tree_from_block(child, source, ids))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        md::Block::List(node) => Ok(new_tree(
            ids,
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
                .map(|item| tree_from_list_item(item, source, ids))
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
                ids,
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
            ids,
            NodeKind::HtmlBlock,
            json!({ "value": node.value }),
            empty(),
            Vec::new(),
        )),
        md::Block::Definition(node) => Ok(new_tree(
            ids,
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
            ids,
            NodeKind::FootnoteDefinition,
            json!({ "identifier": node.identifier }),
            json!({ "label": node.label }),
            node.children
                .iter()
                .map(|child| tree_from_block(child, source, ids))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        md::Block::Table(node) => tree_from_table(node, source, ids),
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

fn tree_from_list_item(
    node: &md::ListItem,
    source: &str,
    ids: &ParseIds,
) -> Result<NodeTree, PluginError> {
    Ok(new_tree(
        ids,
        NodeKind::ListItem,
        json!({ "checked": node.checked }),
        json!({}),
        node.children
            .iter()
            .map(|child| tree_from_block(child, source, ids))
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

fn tree_from_table(
    node: &md::Table,
    source: &str,
    ids: &ParseIds,
) -> Result<NodeTree, PluginError> {
    let mut children = Vec::new();
    let mut column_ids = Vec::new();
    for alignment in &node.alignments {
        let column = new_tree(
            ids,
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
        column_ids.push(column.node.id);
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
                ids,
                NodeKind::TableCell,
                json!({
                    "column_id": column_id,
                    "inline": leaf_inlines_from_ast(&cell.children, source, ids)?,
                }),
                json!({}),
                Vec::new(),
            ));
        }
        children.push(new_tree(
            ids,
            NodeKind::TableRow,
            json!({ "role": if row_index == 0 { "header" } else { "body" } }),
            json!({}),
            cells,
        ));
    }
    Ok(new_tree(
        ids,
        NodeKind::Table,
        json!({}),
        json!({}),
        children,
    ))
}

fn inlines_from_ast(
    nodes: &[md::Inline],
    source: &str,
    ids: &ParseIds,
) -> Result<Vec<InlineNode>, PluginError> {
    nodes
        .iter()
        .map(|node| inline_from_ast(node, source, ids))
        .collect()
}

fn leaf_inlines_from_ast(
    nodes: &[md::Inline],
    source: &str,
    ids: &ParseIds,
) -> Result<Vec<InlineNode>, PluginError> {
    let mut inlines = inlines_from_ast(nodes, source, ids)?;
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

fn inline_from_ast(
    node: &md::Inline,
    source: &str,
    ids: &ParseIds,
) -> Result<InlineNode, PluginError> {
    let (id, content) = match node {
        md::Inline::Text(node) => (
            None,
            InlineContent::Text {
                value: node.value.clone(),
            },
        ),
        md::Inline::Escape(node) => (
            Some(ids.next()),
            InlineContent::Escape { value: node.value },
        ),
        md::Inline::CharacterReference(node) => (
            Some(ids.next()),
            InlineContent::CharacterReference {
                value: node.value.clone(),
                format: CharacterReferenceFormat {
                    reference: node.reference.clone(),
                },
            },
        ),
        md::Inline::Emphasis(node) => (
            Some(ids.next()),
            InlineContent::Emphasis {
                children: inlines_from_ast(&node.children, source, ids)?,
                format: DelimiterFormat {
                    marker: delimiter_from_span(node.meta.span, source, "*"),
                },
            },
        ),
        md::Inline::Strong(node) => (
            Some(ids.next()),
            InlineContent::Strong {
                children: inlines_from_ast(&node.children, source, ids)?,
                format: DelimiterFormat {
                    marker: delimiter_from_span(node.meta.span, source, "**"),
                },
            },
        ),
        md::Inline::Delete(node) => (
            Some(ids.next()),
            InlineContent::Delete {
                children: inlines_from_ast(&node.children, source, ids)?,
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
            Some(ids.next()),
            InlineContent::Code {
                value: node.value.clone(),
                format: InlineCodeFormat {
                    raw: node.raw.clone(),
                    fence_length: node.fence_length,
                },
            },
        ),
        md::Inline::Link(node) => (
            Some(ids.next()),
            InlineContent::Link {
                destination: node.destination.clone(),
                title: node.title.clone(),
                children: inlines_from_ast(&node.children, source, ids)?,
                format: ResourceFormat {
                    destination: link_destination_name(node.destination_kind).to_string(),
                    title: node.title_kind.map(link_title_name).map(str::to_string),
                },
            },
        ),
        md::Inline::Image(node) => (
            Some(ids.next()),
            InlineContent::Image {
                destination: node.destination.clone(),
                title: node.title.clone(),
                alt: inlines_from_ast(&node.alt, source, ids)?,
                format: ResourceFormat {
                    destination: link_destination_name(node.destination_kind).to_string(),
                    title: node.title_kind.map(link_title_name).map(str::to_string),
                },
            },
        ),
        md::Inline::LinkReference(node) => (
            Some(ids.next()),
            InlineContent::LinkReference {
                identifier: node.identifier.clone(),
                children: inlines_from_ast(&node.children, source, ids)?,
                format: ReferenceFormat {
                    label: node.label.clone(),
                    kind: reference_kind_name(node.kind).to_string(),
                },
            },
        ),
        md::Inline::ImageReference(node) => (
            Some(ids.next()),
            InlineContent::ImageReference {
                identifier: node.identifier.clone(),
                alt: inlines_from_ast(&node.alt, source, ids)?,
                format: ReferenceFormat {
                    label: node.label.clone(),
                    kind: reference_kind_name(node.kind).to_string(),
                },
            },
        ),
        md::Inline::Autolink(node) => (
            Some(ids.next()),
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
            Some(ids.next()),
            InlineContent::Html {
                value: node.value.clone(),
            },
        ),
        md::Inline::SoftBreak(_) => (None, InlineContent::SoftBreak),
        md::Inline::LineBreak(node) => (
            Some(ids.next()),
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
            Some(ids.next()),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_parse_serialize_fixpoint(label: &str, source: &str) {
        let first = parse_markdown_source(source)
            .unwrap_or_else(|error| panic!("{label} should parse: {error:?}"));
        let canonical = first
            .canonical_render
            .clone()
            .expect("parser returns canonical bytes");
        let canonical_source =
            std::str::from_utf8(&canonical).expect("canonical Markdown is UTF-8");
        let second = parse_markdown_source(canonical_source)
            .unwrap_or_else(|error| panic!("{label} canonical form should parse: {error:?}"));
        assert_eq!(
            second.canonical_render.as_deref(),
            Some(canonical.as_slice()),
            "{label} canonical serialization must be a fixpoint"
        );
        let third = parse_markdown_source(canonical_source)
            .unwrap_or_else(|error| panic!("{label} repeated parse should parse: {error:?}"));
        assert_eq!(
            serde_json::to_vec(&second.root).expect("serialize second AST"),
            serde_json::to_vec(&third.root).expect("serialize third AST"),
            "{label} AST serialization must be deterministic"
        );
    }

    #[test]
    fn markdown_corpus_parse_serialize_is_deterministic_and_stable() {
        let cases = [
            (
                "reported punctuation and wrapped strong",
                "*Counter:\n\n(~26 users)\n\nA paragraph directly followed by\n- list item\n\n**knowledge base / shared workspace agents read and\nwrite to.**\n",
            ),
            (
                "emphasis and strike",
                "*single-asterisk* _underscore emphasis_ and ~~strike~~\n\n***nested***\n",
            ),
            (
                "unicode and crlf",
                "# 日本語 😀\r\n\r\nRésumé — naïve café\r\n",
            ),
            (
                "frontmatter",
                "---\nDateApproved: 6/10/2020\nOwner: team\n---\n\n# Title\n\nA suffix.\n",
            ),
            (
                "fenced and inline code",
                "```rust\nlet value = *Counter;\n```\n\nUse `a\\~b` literally.\n",
            ),
            (
                "malformed but storable markdown",
                "An unmatched *marker and [unfinished link\n\n> quote without closure\n",
            ),
        ];
        for (label, source) in cases {
            assert_parse_serialize_fixpoint(label, source);
        }

        for index in 0..128 {
            let source = format!(
                "# Case {index}\n\n*Counter:\n\n(~{} users)\n\nparagraph {index}\n- list item\n\n**wrapped strong {index}\nand unicode λ 😀.**\n\n`code {index}` and *emphasis*.\n",
                index + 1
            );
            assert_parse_serialize_fixpoint("generated corpus case", &source);
        }
    }

    /// Native parser profile target. This deliberately excludes Wasm, Lix,
    /// row emission, and storage so `perf` can resolve the parser call tree.
    #[test]
    #[ignore = "manual Markdown parser profile target"]
    fn markdown_parser_profile_target() {
        let target_bytes = std::env::var("LIX_MARKDOWN_PARSER_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(2 * 1024 * 1024);
        let repeats = std::env::var("LIX_MARKDOWN_PARSER_REPEATS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(3);
        let mode =
            std::env::var("LIX_MARKDOWN_PARSER_MODE").unwrap_or_else(|_| "plugin_full".to_owned());
        let rich = std::env::var_os("LIX_MARKDOWN_PARSER_PLAIN").is_none();
        let source = parser_profile_source(target_bytes, rich);

        let started = std::time::Instant::now();
        for _ in 0..repeats {
            match mode.as_str() {
                "syntax" => {
                    let output = SyntaxOptions::gfm().parse(&source);
                    assert!(output.diagnostics.is_empty());
                    std::hint::black_box(output);
                }
                "plugin_once" => {
                    let output =
                        parse_markdown_source_once(&source).expect("profile corpus should parse");
                    std::hint::black_box(output);
                }
                "plugin_full" => {
                    let output = parse_markdown_source(&source)
                        .expect("profile corpus should parse and serialize");
                    std::hint::black_box(output);
                }
                other => panic!("unknown LIX_MARKDOWN_PARSER_MODE: {other}"),
            }
        }
        let elapsed = started.elapsed();
        eprintln!(
            "markdown_parser_profile mode={} rich={} bytes={} repeats={} total_ms={:.3} mean_ms={:.3}",
            mode,
            rich,
            source.len(),
            repeats,
            elapsed.as_secs_f64() * 1_000.0,
            elapsed.as_secs_f64() * 1_000.0 / repeats as f64,
        );
    }

    fn parser_profile_source(target_bytes: usize, rich: bool) -> String {
        const BODY_BYTES: usize = 496;
        const RICH_BLOCK_INTERVAL: usize = 64;
        const WORDS: &[&str] = &[
            "amber", "branch", "canvas", "delta", "ember", "forest", "gentle", "harbor", "island",
            "jungle", "kernel", "lantern", "meadow", "native", "orbit", "paper", "quiet", "river",
            "silver", "timber", "update", "violet", "window", "yellow",
        ];

        let mut source = String::with_capacity(target_bytes + 512);
        for index in 0usize.. {
            let mut body = String::with_capacity(BODY_BYTES);
            let mut cursor = index.wrapping_mul(17);
            while body.len() < BODY_BYTES {
                if !body.is_empty() {
                    body.push(' ');
                }
                body.push_str(WORDS[cursor % WORDS.len()]);
                cursor = cursor.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            }
            body.truncate(BODY_BYTES);
            let prose = format!("P{index:06} {body}\n\n");
            if source.len() + prose.len() > target_bytes {
                break;
            }
            source.push_str(&prose);

            if rich && index % RICH_BLOCK_INTERVAL == RICH_BLOCK_INTERVAL - 1 {
                let block = format!(
                    "## Section {index}\n\nParagraph {index} has *emphasis*, **strong**, ~~delete~~, [a link](https://example.com/{index}), and `code`.\n\n- alpha {index}\n- beta {index}\n\n| key | value |\n| --- | :--- |\n| left {index} | right {index} |\n\n```rust\nlet value_{index} = {index};\n```\n\n> quoted paragraph {index}\n\n"
                );
                if source.len() + block.len() <= target_bytes {
                    source.push_str(&block);
                }
            }
        }
        source.push('\n');
        source
    }
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
            column_indices.insert(column.node.id, index);
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
                let parsed_column_id = uuid::Uuid::parse_str(column_id).map_err(|_| {
                    PluginError::InvalidInput(format!(
                        "table cell '{}' has non-UUID column identity '{column_id}'",
                        cell.node.id
                    ))
                })?;
                let index = *column_indices.get(&parsed_column_id).ok_or_else(|| {
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
