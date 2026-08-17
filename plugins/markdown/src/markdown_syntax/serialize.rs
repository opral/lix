// Originally based on markdown-syntax 0.2.0 by Plimeor and modified by Lix.
// Upstream: https://github.com/plimeor/markdown-syntax
// Original license: MIT; see LICENSE-MIT beside this module.
//
//! AST to canonical Markdown. The verbs live on [`Document`]
//! ([`to_markdown`](Document::to_markdown) /
//! [`to_markdown_with`](Document::to_markdown_with)); [`SerializeOptions`] tunes
//! the output style. The document is validated first, so serialization can fail
//! with a [`SerializeError`].

use alloc::{
    format,
    string::{String, ToString},
    vec::Vec,
};

use crate::{
    ast::*,
    diagnostic::Diagnostic,
    parse::{gfm_table_can_start_source, line_starts_html_block},
    validate::validate_document,
};

/// The newline style emitted by the serializer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LineEnding {
    /// Unix `\n`.
    Lf,
    /// Windows `\r\n`.
    CrLf,
}

impl LineEnding {
    fn as_str(self) -> &'static str {
        match self {
            Self::Lf => "\n",
            Self::CrLf => "\r\n",
        }
    }
}

/// Output-style options for serialization. Defaults: LF, trailing newline, `-`
/// bullets, `.` ordered markers, and backtick code fences.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SerializeOptions {
    /// Newline style to emit.
    pub line_ending: LineEnding,
    /// Whether to end the output with a trailing newline.
    pub final_newline: bool,
    /// The bullet marker for unordered lists.
    pub bullet: ListDelimiter,
    /// The delimiter for ordered-list markers (e.g. `.` → `1.`).
    pub ordered_delimiter: ListDelimiter,
    /// The fence character for fenced code blocks.
    pub fence_marker: FenceMarker,
}

impl Default for SerializeOptions {
    fn default() -> Self {
        Self {
            line_ending: LineEnding::Lf,
            final_newline: true,
            bullet: ListDelimiter::Dash,
            ordered_delimiter: ListDelimiter::Period,
            fence_marker: FenceMarker::Backtick,
        }
    }
}

/// Why serialization failed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SerializeError {
    /// The AST failed validation; carries the validation diagnostics.
    InvalidDocument(Vec<Diagnostic>),
    /// A node kind that the serializer does not support was encountered.
    UnsupportedNode(&'static str),
}

impl Document {
    /// Serialize this document to canonical Markdown with default options.
    pub fn to_markdown(&self) -> Result<String, SerializeError> {
        self.to_markdown_with(&SerializeOptions::default())
    }

    /// Serialize this document to canonical Markdown with explicit options.
    pub fn to_markdown_with(&self, options: &SerializeOptions) -> Result<String, SerializeError> {
        let diagnostics = validate_document(self);
        if !diagnostics.is_empty() {
            return Err(SerializeError::InvalidDocument(diagnostics));
        }

        serialize_document_body(self, options)
    }
}

fn serialize_document_body(
    document: &Document,
    options: &SerializeOptions,
) -> Result<String, SerializeError> {
    let mut output = serialize_blocks_at_start(&document.children, options, true)?;
    if options.line_ending == LineEnding::CrLf {
        output = output.replace('\n', "\r\n");
    }
    if options.final_newline
        && !output.is_empty()
        && !output.ends_with(options.line_ending.as_str())
    {
        output.push_str(options.line_ending.as_str());
    }
    Ok(output)
}

/// Serialize a block sequence. `document_start` is true only for the top-level
/// document body, where the first block sits at byte 0 and a contiguous `---`
/// would open frontmatter; that one position emits a spaced dash thematic break
/// instead. Nested sequences (blockquotes, list items, ...) are never at byte 0.
fn serialize_blocks_at_start(
    blocks: &[Block],
    options: &SerializeOptions,
    document_start: bool,
) -> Result<String, SerializeError> {
    let mut output = String::new();
    for (index, block) in blocks.iter().enumerate() {
        if index > 0 {
            output.push_str("\n\n");
        }
        let at_document_start = document_start && index == 0;
        if let (
            Block::List(list),
            Some(Block::CodeBlock(CodeBlock {
                kind: CodeBlockKind::Indented,
                ..
            })),
        ) = (block, blocks.get(index + 1))
        {
            output.push_str(&serialize_list_with_marker_spacing(
                list, options, " ", "    ",
            )?);
        } else {
            output.push_str(&serialize_block(block, options, at_document_start)?);
        }
    }
    Ok(output)
}

fn serialize_block(
    block: &Block,
    options: &SerializeOptions,
    at_document_start: bool,
) -> Result<String, SerializeError> {
    match block {
        Block::Paragraph(node) => serialize_paragraph(node, options),
        Block::Heading(node) => {
            let content = serialize_inlines(&node.children, options)?;
            // A setext underline can only express depth 1 (`=`) or 2 (`-`); any
            // other depth must fall back to ATX, otherwise the depth is lost.
            // Multi-line content stays setext because ATX is single-line and
            // would split a heading the parser legitimately produces.
            let setext_representable = matches!(node.depth, 1 | 2);
            Ok(match node.kind {
                HeadingKind::Setext if setext_representable => {
                    let marker = if node.depth == 1 { '=' } else { '-' };
                    format!(
                        "{}\n{}",
                        content,
                        marker.to_string().repeat(content.len().max(3))
                    )
                }
                _ if content.is_empty() => "#".repeat(node.depth as usize),
                _ => format!(
                    "{} {}",
                    "#".repeat(node.depth as usize),
                    escape_atx_heading_content(&content)
                ),
            })
        }
        Block::ThematicBreak(node) => Ok(match node.marker {
            // A Dash break is normally written contiguous (`---`) — the form
            // that survives after a `-` bullet list, where the spaced `- - -`
            // would be re-read as nested list items. The one exception is the
            // document start, where a contiguous `---` opens frontmatter, so the
            // spaced form (which is not a frontmatter fence) is used there.
            ThematicBreakMarker::Dash if at_document_start => "- - -".into(),
            ThematicBreakMarker::Dash => "---".into(),
            ThematicBreakMarker::Asterisk => "***".into(),
            ThematicBreakMarker::Underscore => "___".into(),
        }),
        Block::BlockQuote(node) => {
            let inner = serialize_blocks_at_start(&node.children, options, false)?;
            if inner.is_empty() {
                Ok(">".into())
            } else {
                Ok(prefix_lines(&inner, "> "))
            }
        }
        Block::Alert(node) => serialize_alert(node, options),
        Block::List(node) => serialize_list(node, options),
        Block::DescriptionList(node) => serialize_description_list(node, options),
        Block::CodeBlock(node) => serialize_code_block(node, options),
        Block::HtmlBlock(node) => Ok(trim_trailing_newline(&node.value).into()),
        Block::HtmlContainer(node) => serialize_html_container(node, options),
        Block::Definition(node) => {
            let destination = serialize_destination_kind(
                &node.destination,
                node.destination_kind,
                InlineSerializeContext::default(),
            );
            let mut label = if node.meta.span.is_some() {
                escape_definition_label_source(&node.label)
            } else {
                escape_reference_label_with_pipe(&node.label, false)
            };
            if node.meta.span.is_none() && label.starts_with('^') {
                label.insert(0, '\\');
            }
            let mut output = format!("[{}]: {}", label, destination);
            if let (Some(title), Some(title_kind)) = (&node.title, node.title_kind) {
                output.push(' ');
                output.push_str(&serialize_title_kind(
                    title,
                    title_kind,
                    InlineSerializeContext::default(),
                ));
            }
            Ok(output)
        }
        Block::FootnoteDefinition(node) => {
            let inner = serialize_blocks_at_start(&node.children, options, false)?;
            let label = if node.meta.span.is_some() {
                escape_footnote_label_source(&node.label)
            } else {
                escape_footnote_label_semantic(&node.label)
            };
            Ok(format!("[^{}]: {}", label, indent_continuation(&inner)))
        }
        Block::Table(node) => serialize_table(node, options),
        Block::MathBlock(node) => {
            let fence = block_math_fence(&node.value);
            Ok(format!(
                "{fence}\n{}\n{fence}",
                trim_trailing_newline(&node.value)
            ))
        }
        Block::Frontmatter(node) => {
            let fence = match node.kind {
                FrontmatterKind::Yaml => "---",
                FrontmatterKind::Toml => "+++",
            };
            Ok(format!(
                "{fence}\n{}\n{fence}",
                trim_trailing_newline(&node.value)
            ))
        }
        Block::MdxEsm(node) => Ok(node.value.clone()),
        Block::MdxExpression(node) => Ok(format!("{{{}}}", node.value)),
        Block::MdxJsx(node) => Ok(node.value.clone()),
        Block::LeafDirective(node) => Ok(format!(
            "::{}{}{}",
            node.name,
            serialize_directive_label(&node.label, options)?,
            serialize_attributes(&node.attributes)
        )),
        Block::ContainerDirective(node) => {
            let inner = serialize_blocks_at_start(&node.children, options, false)?;
            let fence = directive_fence(&inner);
            Ok(format!(
                "{fence}{}{}{}\n{}\n{fence}",
                node.name,
                serialize_directive_label(&node.label, options)?,
                serialize_attributes(&node.attributes),
                inner
            ))
        }
    }
}

fn serialize_html_container(
    node: &HtmlContainer,
    options: &SerializeOptions,
) -> Result<String, SerializeError> {
    match &node.content {
        HtmlContainerContent::Blocks(children) => {
            let inner = serialize_blocks_at_start(children, options, false)?;
            if inner.is_empty() {
                Ok(format!("{}\n{}", node.opening.raw, node.closing.raw))
            } else {
                Ok(format!(
                    "{}\n{}\n\n{}",
                    node.opening.raw, inner, node.closing.raw
                ))
            }
        }
        HtmlContainerContent::Inlines(children) => Ok(format!(
            "{}{}{}",
            node.opening.raw,
            serialize_inlines(children, options)?,
            node.closing.raw
        )),
    }
}

/// Escape a trailing `#`-run in ATX heading content so it is not consumed as a
/// closing hash sequence. CommonMark treats a final run of `#` preceded by
/// whitespace (after trailing whitespace is trimmed) as the optional closing
/// sequence; escaping the first `#` of that run keeps it as literal text.
fn escape_atx_heading_content(content: &str) -> String {
    let trimmed_len = content.trim_end_matches([' ', '\t']).len();
    let trimmed = &content[..trimmed_len];
    let hash_start = trimmed.trim_end_matches('#').len();
    let preceded_by_whitespace = trimmed[..hash_start]
        .chars()
        .next_back()
        .is_some_and(|char| char == ' ' || char == '\t');
    if hash_start == trimmed_len || !preceded_by_whitespace {
        return content.into();
    }
    let mut output = String::with_capacity(content.len() + 1);
    output.push_str(&content[..hash_start]);
    output.push('\\');
    output.push_str(&content[hash_start..]);
    output
}

fn serialize_paragraph(
    node: &Paragraph,
    options: &SerializeOptions,
) -> Result<String, SerializeError> {
    let mut output = serialize_inlines(&node.children, options)?;
    if let Some(offset) = paragraph_html_block_escape_offset(&output) {
        output.insert(offset, '\\');
    }
    if let Some(offset) = paragraph_table_escape_offset(&output) {
        output.insert(offset, '\\');
    }
    Ok(output)
}

fn paragraph_html_block_escape_offset(input: &str) -> Option<usize> {
    let first_line = input.split('\n').next().unwrap_or(input);
    if !line_starts_html_block(first_line) {
        return None;
    }

    Some(
        first_line
            .as_bytes()
            .iter()
            .take_while(|byte| **byte == b' ')
            .count(),
    )
}

fn paragraph_table_escape_offset(input: &str) -> Option<usize> {
    let first_line_end = input.find('\n')?;
    let first_line = &input[..first_line_end];
    let second_line_start = first_line_end + 1;
    let second_line_end = input[second_line_start..]
        .find('\n')
        .map(|offset| second_line_start + offset)
        .unwrap_or(input.len());
    let second_line = &input[second_line_start..second_line_end];

    if !gfm_table_can_start_source(first_line, second_line) {
        return None;
    }

    second_line
        .find('-')
        .map(|offset| second_line_start + offset)
}

fn serialize_alert(node: &Alert, options: &SerializeOptions) -> Result<String, SerializeError> {
    let mut output = String::from("> [!");
    output.push_str(alert_kind_name(node.kind));
    output.push(']');
    if let Some(title) = &node.title {
        if !title.is_empty() {
            output.push(' ');
            output.push_str(&escape_alert_title(title));
        }
    }
    let inner = serialize_blocks_at_start(&node.children, options, false)?;
    if !inner.is_empty() {
        output.push('\n');
        output.push_str(&prefix_lines(&inner, "> "));
    }
    Ok(output)
}

fn alert_kind_name(kind: AlertKind) -> &'static str {
    match kind {
        AlertKind::Note => "NOTE",
        AlertKind::Tip => "TIP",
        AlertKind::Important => "IMPORTANT",
        AlertKind::Warning => "WARNING",
        AlertKind::Caution => "CAUTION",
    }
}

fn escape_alert_title(input: &str) -> String {
    let mut output = String::new();
    for char in input.chars() {
        match char {
            '\n' | '\r' => output.push(' '),
            char if char.is_control() => output.push_str(&format!("&#x{:X};", char as u32)),
            _ => output.push(char),
        }
    }
    output
}

fn serialize_list(node: &List, options: &SerializeOptions) -> Result<String, SerializeError> {
    serialize_list_with_marker_spacing(node, options, "", " ")
}

fn serialize_list_with_marker_spacing(
    node: &List,
    options: &SerializeOptions,
    marker_prefix: &str,
    marker_padding: &str,
) -> Result<String, SerializeError> {
    let mut output = String::new();
    for (index, item) in node.children.iter().enumerate() {
        if index > 0 {
            if node.tight {
                output.push('\n');
            } else {
                output.push_str("\n\n");
            }
        }
        let list_delimiter = if node.ordered {
            if options.ordered_delimiter == SerializeOptions::default().ordered_delimiter {
                node.delimiter
            } else {
                options.ordered_delimiter
            }
        } else if options.bullet == SerializeOptions::default().bullet {
            node.delimiter
        } else {
            options.bullet
        };
        let marker = if node.ordered {
            let start = node.start.unwrap_or(1).saturating_add(index as u64);
            let delimiter = ordered_list_marker(list_delimiter);
            format!("{marker_prefix}{start}{delimiter}{marker_padding}")
        } else {
            format!(
                "{marker_prefix}{}{marker_padding}",
                unordered_list_marker(list_delimiter)
            )
        };
        let mut inner = serialize_item_blocks(&item.children, options, node.tight)?;
        if !node.ordered && unordered_list_marker(list_delimiter) == '*' {
            inner = disambiguate_asterisk_list_item(inner);
        }
        if let Some(checked) = item.checked {
            if let Some(rest) = inner.strip_prefix("- ") {
                inner = rest.into();
            }
            let checkbox = if checked { "[x] " } else { "[ ] " };
            inner = format!("{checkbox}{inner}");
        }
        if !node.tight
            && node.children.len() == 1
            && matches!(item.children.as_slice(), [Block::Paragraph(_)])
            && !inner.is_empty()
        {
            output.push_str(marker.trim_end());
            output.push_str("\n\n");
            output.push_str(&prefix_lines(&inner, &" ".repeat(marker.len())));
            continue;
        }
        output.push_str(&marker);
        output.push_str(&indent_after_first_line(&inner, marker.len()));
    }
    Ok(output)
}

fn disambiguate_asterisk_list_item(inner: String) -> String {
    let first_line_end = inner.find('\n').unwrap_or(inner.len());
    let first_line = &inner[..first_line_end];
    if !asterisk_bullet_first_line_is_thematic_break(first_line) {
        return inner;
    }
    let mut output = String::from("---");
    output.push_str(&inner[first_line_end..]);
    output
}

/// Whether a `*`-bullet item's first content line, once prefixed by the `* `
/// marker, would escape the list as an asterisk thematic break. This is the
/// rendering of a `ThematicBreak` child: a contiguous run of asterisks (`***`,
/// rendered with no internal whitespace). A line with interior spaces such as
/// `* *` is a genuine nested bullet and must be left alone, since `* * *`
/// re-parses back into the nested list it came from.
fn asterisk_bullet_first_line_is_thematic_break(first_line: &str) -> bool {
    first_line.len() >= 2 && first_line.bytes().all(|byte| byte == b'*')
}

fn serialize_item_blocks(
    blocks: &[Block],
    options: &SerializeOptions,
    tight: bool,
) -> Result<String, SerializeError> {
    let mut output = String::new();
    for (index, block) in blocks.iter().enumerate() {
        if index > 0 {
            if tight {
                output.push('\n');
            } else {
                output.push_str("\n\n");
            }
        }
        output.push_str(&serialize_block(block, options, false)?);
    }
    Ok(output)
}

fn serialize_description_list(
    node: &DescriptionList,
    options: &SerializeOptions,
) -> Result<String, SerializeError> {
    let mut output = String::new();
    for (item_index, item) in node.children.iter().enumerate() {
        if item_index > 0 {
            output.push_str(if node.tight { "\n" } else { "\n\n" });
        }
        output.push_str(&serialize_inlines(&item.term, options)?);
        for (detail_index, detail) in item.details.iter().enumerate() {
            if node.tight && detail.children.len() == 1 {
                if let Block::Paragraph(paragraph) = &detail.children[0] {
                    output.push('\n');
                    output.push_str(": ");
                    output.push_str(&serialize_inlines(&paragraph.children, options)?);
                    continue;
                }
            }
            // A loose list is re-parsed as loose only through an intra-item blank;
            // the parser treats blanks BETWEEN items as tight-preserving group
            // separators. Encode the looseness with a blank line before the term's
            // first definition marker (a `blank_after_term`), so the round trip
            // keeps `tight=false`.
            if !node.tight && detail_index == 0 {
                output.push('\n');
            }
            output.push_str("\n:");
            let inner = serialize_blocks_at_start(&detail.children, options, false)?;
            if !inner.is_empty() {
                output.push('\n');
                output.push_str(&indent_lines(&inner, 4));
            }
        }
    }
    Ok(output)
}

fn serialize_code_block(
    node: &CodeBlock,
    options: &SerializeOptions,
) -> Result<String, SerializeError> {
    match node.kind {
        CodeBlockKind::Indented => Ok(prefix_lines(trim_trailing_newline(&node.value), "    ")),
        CodeBlockKind::Fenced { marker, length } => {
            let marker = code_block_fence_marker(node, marker, options);
            let fence = fence_for(&node.value, marker, length.max(3));
            let mut opener = fence.clone();
            if let Some(info) = &node.info {
                opener.push(' ');
                opener.push_str(&escape_code_info(info));
            }
            let mut output = opener;
            output.push('\n');
            output.push_str(&node.value);
            if !ends_with_line_ending(&node.value) {
                output.push('\n');
            }
            output.push_str(&fence);
            Ok(output)
        }
    }
}

fn code_block_fence_marker(
    node: &CodeBlock,
    marker: FenceMarker,
    options: &SerializeOptions,
) -> FenceMarker {
    if node.info.as_deref().is_some_and(|info| info.contains('`')) {
        return FenceMarker::Tilde;
    }
    if options.fence_marker == SerializeOptions::default().fence_marker {
        marker
    } else {
        options.fence_marker
    }
}

fn escape_code_info(input: &str) -> String {
    let mut output = String::new();
    for char in input.chars() {
        match char {
            '\n' => output.push_str("&#xA;"),
            '\r' => output.push_str("&#xD;"),
            '\t' => output.push(char),
            char if char.is_control() => output.push_str(&format!("&#x{:X};", char as u32)),
            '\\' | '&' => {
                output.push('\\');
                output.push(char);
            }
            _ => output.push(char),
        }
    }
    output
}

fn serialize_table(node: &Table, options: &SerializeOptions) -> Result<String, SerializeError> {
    let header = &node.rows[0];
    let mut output = serialize_table_row(header, options)?;
    output.push('\n');
    output.push('|');
    output.push(' ');
    output.push_str(
        &node
            .alignments
            .iter()
            .map(|alignment| match alignment {
                TableAlignment::None => "---",
                TableAlignment::Left => ":---",
                TableAlignment::Center => ":---:",
                TableAlignment::Right => "---:",
            })
            .collect::<Vec<_>>()
            .join(" | "),
    );
    output.push(' ');
    output.push('|');
    for row in node.rows.iter().skip(1) {
        output.push('\n');
        output.push_str(&serialize_table_row(row, options)?);
    }
    Ok(output)
}

fn serialize_table_row(
    row: &TableRow,
    options: &SerializeOptions,
) -> Result<String, SerializeError> {
    let mut cells = Vec::new();
    for cell in &row.cells {
        let cell = serialize_inlines_with_context(
            &cell.children,
            options,
            InlineSerializeContext::table_cell(),
        )?;
        if table_cell_has_unescaped_pipe(&cell) {
            return Err(SerializeError::UnsupportedNode(
                "table cell inline contains a pipe that cannot be escaped without changing source",
            ));
        }
        cells.push(cell);
    }
    Ok(format!("| {} |", cells.join(" | ")))
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct InlineSerializeContext {
    table_cell: bool,
    avoid_star_edges: bool,
}

impl InlineSerializeContext {
    const fn table_cell() -> Self {
        Self {
            table_cell: true,
            avoid_star_edges: false,
        }
    }

    const fn avoiding_star_edges(self) -> Self {
        Self {
            table_cell: self.table_cell,
            avoid_star_edges: true,
        }
    }
}

fn serialize_inlines(
    inlines: &[Inline],
    options: &SerializeOptions,
) -> Result<String, SerializeError> {
    serialize_inlines_with_context(inlines, options, InlineSerializeContext::default())
}

/// Escape a trailing unescaped `!` already in `output` before emitting a
/// following `[`-starting node (link / reference / footnote), so the pair does
/// not reparse as an image (`![…]`). The within-text `!`-before-`[` escaper
/// only sees a single text node, so this handles the cross-node boundary.
fn escape_trailing_bang(output: &mut String) {
    if output.ends_with('!') && !output.ends_with("\\!") {
        output.pop();
        output.push_str("\\!");
    }
}

// A GFM literal autolink is serialized as its raw URL text. If the preceding
// output ends with `<`, that `<` plus the URL plus a following `>` could be
// reparsed as an angle autolink (`<http://x>`) instead of the literal. Escaping
// the trailing `<` keeps it literal text, so the URL stays a GFM literal on the
// round trip (`\<` before `http://…` is just text + literal).
fn escape_trailing_less_than(output: &mut String) {
    if output.ends_with('<') && !output.ends_with("\\<") {
        output.pop();
        output.push_str("\\<");
    }
}

// A GFM bare-email literal anchors at its leftmost run of email-local chars
// (`[A-Za-z0-9.+_-]`). If the preceding output ends with such a char, the
// reparse would extend the email's local part leftward into that text (e.g.
// `A` + `i@i.a` → `Ai@i.a`). Re-emit the trailing email-local char as a numeric
// character reference (which decodes back to the same text but is not an
// email-local char), preserving the boundary on the round trip.
fn escape_trailing_email_local(output: &mut String) {
    let Some(last) = output.chars().next_back() else {
        return;
    };
    // Only an ASCII alphanumeric immediately before the email forces a leftward
    // re-anchor on reparse (the local part starts at the leftmost local-char
    // run, and the dispatch reaches that alnum first). The punctuation
    // local-chars (`.+-_`) are handled by the text serializer's own escaping.
    if !last.is_ascii_alphanumeric() {
        return;
    }
    output.pop();
    output.push_str(&alloc::format!("&#{};", last as u32));
}

// True when `inline` is a GFM literal autolink (its raw URL serialization can
// re-absorb a following text char on reparse).
fn is_gfm_literal_autolink(inline: &Inline) -> bool {
    matches!(
        inline,
        Inline::Autolink(node) if matches!(node.kind, AutolinkKind::GfmLiteral { .. })
    )
}

fn is_gfm_literal_email(inline: &Inline) -> bool {
    matches!(
        inline,
        Inline::Autolink(node)
            if matches!(&node.kind, AutolinkKind::GfmLiteral { original }
                if node.destination.strip_prefix("mailto:") == Some(original.as_str()))
    )
}

// A GFM literal autolink's URL scan stops at whitespace, `<`, `]`, and a
// backslash-escaped punctuation char, and trims trailing punctuation/entities.
// A following text node whose first char is none of those — in particular a
// non-ASCII char such as `©` decoded from `&copy;` — would otherwise be pulled
// into the URL on reparse. Re-emit that leading char as a hex numeric character
// reference (`&#xNN;`), which `autolink_delim` trims back off the URL and which
// decodes to the same text, keeping the boundary stable.
fn encode_leading_char_after_autolink(value: &str) -> Option<(String, &str)> {
    let first = value.chars().next()?;
    if first.is_ascii() {
        // ASCII merge chars are handled by the text serializer's own backslash
        // escaping (`\[`, `\&`, …) and the parser's matching `\<punct>` stop.
        return None;
    }
    let encoded = alloc::format!("&#x{:X};", first as u32);
    Some((encoded, &value[first.len_utf8()..]))
}

fn serialize_inlines_with_context(
    inlines: &[Inline],
    options: &SerializeOptions,
    context: InlineSerializeContext,
) -> Result<String, SerializeError> {
    let mut output = String::new();
    for (index, inline) in inlines.iter().enumerate() {
        match inline {
            Inline::Text(node) => {
                let after_literal_autolink = index
                    .checked_sub(1)
                    .is_some_and(|prev| is_gfm_literal_autolink(&inlines[prev]));
                let before_literal_autolink =
                    inlines.get(index + 1).is_some_and(is_gfm_literal_autolink);

                // Leading guard: a non-ASCII char abutting the END of a literal
                // autolink would merge into its URL on reparse — encode it.
                let (lead, body) = match after_literal_autolink
                    .then(|| encode_leading_char_after_autolink(&node.value))
                    .flatten()
                {
                    Some((encoded, rest)) => (encoded, rest),
                    None => (String::new(), node.value.as_str()),
                };

                // Trailing guard: when this text is immediately followed by a
                // www/http/email literal, its trailing whitespace must survive
                // as a real whitespace preceder. A trailing space/tab is
                // otherwise re-encoded (`&#x20;`/`&#x9;`) at an edge or as a
                // control char, which would break the literal's left boundary on
                // reparse — emit the trailing space/tab run literally instead.
                let (escape_body, trailing_ws) = if before_literal_autolink {
                    let head = body.trim_end_matches([' ', '\t']);
                    (head, &body[head.len()..])
                } else {
                    (body, "")
                };

                output.push_str(&lead);
                output.push_str(&escape_text_with_context(
                    escape_body,
                    lead.is_empty()
                        && trailing_ws.len() != body.len()
                        && output_line_len(&output) == 0,
                    trailing_ws.is_empty() && text_is_at_line_end(inlines, index),
                    context,
                ));
                output.push_str(trailing_ws);
            }
            Inline::Escape(node) => {
                output.push('\\');
                output.push(node.value);
            }
            Inline::CharacterReference(node) => output.push_str(&node.reference),
            Inline::Emphasis(node) => {
                let children = serialize_inlines_with_context(&node.children, options, context)?;
                let touches_underscore = children.starts_with('_')
                    || children.ends_with('_')
                    || children.starts_with("\\_")
                    || children.ends_with("\\_");
                // An emphasis abutting a `*` already in the output (e.g. a
                // preceding `*`-emphasis) would otherwise merge into one run, so
                // switch this run to `_` when that does not introduce a new
                // `_`-collision with the children.
                let abuts_star = output.ends_with('*') && !touches_underscore;
                let prefer_underscore = (context.avoid_star_edges && !touches_underscore)
                    || abuts_star
                    || children.starts_with('*')
                    || children.ends_with('*');
                let delimiter = if prefer_underscore { '_' } else { '*' };
                let children = if delimiter == '*' {
                    serialize_inlines_with_context(
                        &node.children,
                        options,
                        context.avoiding_star_edges(),
                    )?
                } else {
                    children
                };
                output.push(delimiter);
                output.push_str(&children);
                output.push(delimiter);
            }
            Inline::Strong(node) => {
                let children = serialize_inlines_with_context(
                    &node.children,
                    options,
                    context.avoiding_star_edges(),
                )?;
                // NOTE: two abutting `Strong` nodes (`**a****b**`) reparse as a
                // single run. The only zero-insertion separator is flipping one
                // run to `__`, but `__` reparses as `Underline` when that
                // construct is enabled and the serializer has no signal for it,
                // so this hand-built-AST sub-case is left as a known limitation.
                output.push_str("**");
                output.push_str(&children);
                output.push_str("**");
            }
            Inline::Underline(node) => {
                output.push_str("__");
                output.push_str(&serialize_inlines_with_context(
                    &node.children,
                    options,
                    context,
                )?);
                output.push_str("__");
            }
            Inline::Delete(node) => {
                let children = serialize_inlines_with_context(&node.children, options, context)?;
                let marker = match node.marker {
                    DeleteMarker::SingleTilde => "~",
                    DeleteMarker::DoubleTilde => "~~",
                };
                output.push_str(marker);
                output.push_str(&children);
                output.push_str(marker);
            }
            Inline::Insert(node) => {
                output.push_str("++");
                output.push_str(&serialize_inlines_with_context(
                    &node.children,
                    options,
                    context,
                )?);
                output.push_str("++");
            }
            Inline::Mark(node) => {
                output.push_str("==");
                output.push_str(&serialize_inlines_with_context(
                    &node.children,
                    options,
                    context,
                )?);
                output.push_str("==");
            }
            Inline::Subscript(node) => {
                output.push('~');
                output.push_str(&serialize_inlines_with_context(
                    &node.children,
                    options,
                    context,
                )?);
                output.push('~');
            }
            Inline::Superscript(node) => {
                output.push('^');
                output.push_str(&serialize_inlines_with_context(
                    &node.children,
                    options,
                    context,
                )?);
                output.push('^');
            }
            Inline::Spoiler(node) => {
                output.push_str("||");
                output.push_str(&serialize_inlines_with_context(
                    &node.children,
                    options,
                    context,
                )?);
                output.push_str("||");
            }
            Inline::Shortcode(node) => {
                output.push(':');
                output.push_str(&node.name);
                output.push(':');
            }
            Inline::Code(node) => {
                if node.fence_length > 0 && !node.raw.is_empty() {
                    let fence = "`".repeat(node.fence_length);
                    let raw = if context.table_cell {
                        table_cell_escape_code_pipes(&node.raw)
                    } else {
                        node.raw.clone()
                    };
                    output.push_str(&fence);
                    output.push_str(&raw);
                    output.push_str(&fence);
                    continue;
                }
                if node.value.is_empty() {
                    output.push_str("`` ``");
                    continue;
                }
                let value = if context.table_cell {
                    table_cell_escape_code_pipes(&node.value)
                } else {
                    node.value.clone()
                };
                let fence = inline_code_fence(&value);
                output.push_str(&fence);
                if code_span_needs_padding(&value) {
                    output.push(' ');
                    output.push_str(&value);
                    output.push(' ');
                } else {
                    output.push_str(&value);
                }
                output.push_str(&fence);
            }
            Inline::Link(node) => {
                escape_trailing_bang(&mut output);
                output.push('[');
                output.push_str(&serialize_inlines_with_context(
                    &node.children,
                    options,
                    context,
                )?);
                output.push_str("](");
                output.push_str(&serialize_destination_kind(
                    &node.destination,
                    node.destination_kind,
                    context,
                ));
                if let (Some(title), Some(title_kind)) = (&node.title, node.title_kind) {
                    output.push(' ');
                    output.push_str(&serialize_title_kind(title, title_kind, context));
                }
                output.push(')');
            }
            Inline::Image(node) => {
                output.push_str("![");
                output.push_str(&serialize_inlines_with_context(
                    &node.alt, options, context,
                )?);
                output.push_str("](");
                output.push_str(&serialize_destination_kind(
                    &node.destination,
                    node.destination_kind,
                    context,
                ));
                if let (Some(title), Some(title_kind)) = (&node.title, node.title_kind) {
                    output.push(' ');
                    output.push_str(&serialize_title_kind(title, title_kind, context));
                }
                output.push(')');
            }
            Inline::LinkReference(node) => {
                let children = serialize_inlines_with_context(&node.children, options, context)?;
                let children_identifier = normalize_reference_label(&children);
                escape_trailing_bang(&mut output);
                push_reference_body(
                    &mut output,
                    node.kind,
                    &children,
                    children_identifier == node.identifier,
                    &reference_explicit_label(node.meta.span.is_some(), &node.label, context),
                );
            }
            Inline::ImageReference(node) => {
                let alt = serialize_inlines_with_context(&node.alt, options, context)?;
                let alt_identifier = normalize_reference_label(&alt);
                output.push('!');
                push_reference_body(
                    &mut output,
                    node.kind,
                    &alt,
                    alt_identifier == node.identifier,
                    &reference_explicit_label(node.meta.span.is_some(), &node.label, context),
                );
            }
            Inline::Autolink(node) => match &node.kind {
                AutolinkKind::Angle => {
                    output.push('<');
                    output.push_str(&node.destination);
                    output.push('>');
                }
                // A GFM literal autolink re-emits its original source text,
                // which re-parses to the same literal (the synthesized
                // `http://`/`mailto:` destination is reconstructed on parse).
                AutolinkKind::GfmLiteral { original } => {
                    // Bare-email literals (`destination` is the original with a
                    // synthesized `mailto:` prefix) re-anchor leftward over
                    // email-local chars on reparse; guard the preceding char.
                    let is_bare_email = node.destination == alloc::format!("mailto:{original}");
                    let follows_literal_email_plus = original.starts_with('+')
                        && index
                            .checked_sub(1)
                            .is_some_and(|prev| is_gfm_literal_email(&inlines[prev]));
                    if is_bare_email && !follows_literal_email_plus {
                        escape_trailing_email_local(&mut output);
                    } else {
                        escape_trailing_less_than(&mut output);
                    }
                    output.push_str(original);
                }
            },
            Inline::Html(node) => output.push_str(&node.value),
            Inline::SoftBreak(_) => output.push('\n'),
            Inline::LineBreak(node) => match node.kind {
                LineBreakKind::Backslash => output.push_str("\\\n"),
                LineBreakKind::Spaces => output.push_str("  \n"),
            },
            Inline::Math(node) => {
                output.push_str(&serialize_inline_math_with_context(node, context)?);
            }
            Inline::FootnoteReference(node) => {
                escape_trailing_bang(&mut output);
                output.push_str("[^");
                if node.meta.span.is_some() {
                    output.push_str(&escape_footnote_label_source(&node.label));
                } else {
                    output.push_str(&escape_footnote_label_semantic(&node.label));
                }
                output.push(']');
            }
            Inline::InlineFootnote(node) => {
                output.push_str("^[");
                output.push_str(&serialize_inlines_with_context(
                    &node.children,
                    options,
                    context,
                )?);
                output.push(']');
            }
            Inline::WikiLink(node) => {
                output.push_str("[[");
                let target = escape_wikilink_part(&node.target);
                let label = escape_wikilink_part(&node.label);
                if node.target == node.label {
                    output.push_str(&target);
                } else {
                    match node.label_order {
                        WikiLinkLabelOrder::AfterPipe => {
                            output.push_str(&target);
                            output.push('|');
                            output.push_str(&label);
                        }
                        WikiLinkLabelOrder::BeforePipe => {
                            output.push_str(&label);
                            output.push('|');
                            output.push_str(&target);
                        }
                    }
                }
                output.push_str("]]");
            }
            Inline::MdxExpression(node) => {
                output.push('{');
                output.push_str(&node.value);
                output.push('}');
            }
            Inline::MdxJsx(node) => output.push_str(&node.value),
            Inline::TextDirective(node) => {
                output.push(':');
                output.push_str(&node.name);
                output.push_str(&serialize_directive_label_with_context(
                    &node.label,
                    options,
                    context,
                )?);
                output.push_str(&serialize_attributes_with_context(
                    &node.attributes,
                    context,
                ));
            }
        }
    }
    Ok(output)
}

fn serialize_directive_label(
    label: &[Inline],
    options: &SerializeOptions,
) -> Result<String, SerializeError> {
    serialize_directive_label_with_context(label, options, InlineSerializeContext::default())
}

fn serialize_directive_label_with_context(
    label: &[Inline],
    options: &SerializeOptions,
    context: InlineSerializeContext,
) -> Result<String, SerializeError> {
    if label.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!(
            "[{}]",
            serialize_inlines_with_context(label, options, context)?
        ))
    }
}

fn serialize_attributes(attributes: &[DirectiveAttribute]) -> String {
    serialize_attributes_with_context(attributes, InlineSerializeContext::default())
}

fn serialize_attributes_with_context(
    attributes: &[DirectiveAttribute],
    context: InlineSerializeContext,
) -> String {
    if attributes.is_empty() {
        return String::new();
    }
    let mut output = String::from("{");
    for (index, attribute) in attributes.iter().enumerate() {
        if index > 0 {
            output.push(' ');
        }
        match (&*attribute.name, &attribute.value) {
            ("id", Some(value)) if is_directive_shorthand_value(value) => {
                output.push('#');
                output.push_str(value);
            }
            ("class", Some(value)) if is_directive_shorthand_value(value) => {
                output.push('.');
                output.push_str(value);
            }
            (_, Some(value)) => {
                output.push_str(&attribute.name);
                output.push('=');
                output.push('"');
                output.push_str(&escape_title_with_context(
                    value,
                    LinkTitleKind::DoubleQuote,
                    context,
                ));
                output.push('"');
            }
            (_, None) => output.push_str(&attribute.name),
        }
    }
    output.push('}');
    output
}

fn is_directive_shorthand_value(input: &str) -> bool {
    !input.is_empty()
        && input
            .chars()
            .all(|char| char.is_ascii_alphanumeric() || matches!(char, '_' | '-'))
}

fn text_is_at_line_end(inlines: &[Inline], index: usize) -> bool {
    matches!(
        inlines.get(index + 1),
        None | Some(Inline::SoftBreak(_)) | Some(Inline::LineBreak(_))
    )
}

fn escape_text_with_context(
    input: &str,
    preserve_leading: bool,
    preserve_trailing: bool,
    context: InlineSerializeContext,
) -> String {
    // This function encodes source line endings as entities below, so its
    // output never contains a literal newline. `output.len()` is therefore
    // the current line length and avoids rescanning the growing string with
    // `rsplit_once` for every input character.
    let avoid_star_edges = context.avoid_star_edges;
    let mut output = String::new();
    let mut line_digit_prefix = 0usize;
    let trailing_start = if preserve_trailing {
        input
            .trim_end_matches(|char| matches!(char, ' ' | '\t'))
            .len()
    } else {
        input.len()
    };
    let mut chars = input.char_indices().peekable();
    let mut at_leading_edge = preserve_leading;
    while let Some((offset, char)) = chars.next() {
        if char == '\n' {
            output.push_str("&#xA;");
            at_leading_edge = false;
            continue;
        }
        if char == '\r' {
            output.push_str("&#xD;");
            at_leading_edge = false;
            continue;
        }
        if (at_leading_edge || offset >= trailing_start) && char == ' ' {
            output.push_str("&#x20;");
            continue;
        }
        if (at_leading_edge || offset >= trailing_start) && char == '\t' {
            output.push_str("&#x9;");
            continue;
        }
        if char.is_control() {
            output.push_str(&format!("&#x{:X};", char as u32));
            at_leading_edge = false;
            continue;
        }
        at_leading_edge = false;
        if line_digit_prefix == output.len() && char.is_ascii_digit() {
            output.push(char);
            line_digit_prefix += 1;
            continue;
        }
        if char == ':'
            && (input[..offset].ends_with("http") || input[..offset].ends_with("https"))
            && input[offset + char.len_utf8()..].starts_with("//")
        {
            output.push('\\');
            output.push(char);
            line_digit_prefix = usize::MAX;
            continue;
        }
        if char == '.' && input[..offset].ends_with("www") {
            output.push('\\');
            output.push(char);
            line_digit_prefix = usize::MAX;
            continue;
        }
        if char == '@' {
            if at_sign_can_start_email_autolink(input, offset) {
                output.push_str("&#x40;");
            } else {
                output.push(char);
            }
            line_digit_prefix = usize::MAX;
            continue;
        }
        if line_digit_prefix != usize::MAX
            && line_digit_prefix > 0
            && matches!(char, '.' | ')')
            && chars
                .peek()
                .map(|(_, next)| next.is_whitespace())
                .unwrap_or(true)
        {
            output.push('\\');
            output.push(char);
            line_digit_prefix = usize::MAX;
            continue;
        }
        if output.is_empty()
            && matches!(char, '-' | '+')
            && chars
                .peek()
                .map(|(_, next)| next.is_whitespace())
                .unwrap_or(true)
        {
            output.push('\\');
            output.push(char);
            line_digit_prefix = usize::MAX;
            continue;
        }
        if output.is_empty()
            && ((char == '-' && chars.peek().is_some_and(|(_, next)| *next == '-')) || char == '=')
        {
            output.push('\\');
            output.push(char);
            line_digit_prefix = usize::MAX;
            continue;
        }
        line_digit_prefix = usize::MAX;
        match char {
            '*' if avoid_star_edges => output.push_str("&#x2A;"),
            '|' if context.table_cell => output.push_str("&#x7C;"),
            '|' if output.is_empty() => {
                output.push('\\');
                output.push(char);
            }
            '`' if text_code_span_can_start(input, offset) => {
                output.push('\\');
                output.push(char);
            }
            '*' if text_attention_delimiter_can_start(input, offset, "*", false) => {
                output.push('\\');
                output.push(char);
            }
            '_' if text_attention_delimiter_can_start(input, offset, "_", true) => {
                output.push('\\');
                output.push(char);
            }
            '<' if text_less_than_can_start_inline(input, offset) => {
                output.push('\\');
                output.push(char);
            }
            '>' if output.is_empty() => {
                output.push('\\');
                output.push(char);
            }
            '{' if input[offset + char.len_utf8()..].contains('}') => {
                output.push('\\');
                output.push(char);
            }
            '#' if text_atx_heading_can_start(input, offset, &output) => {
                output.push('\\');
                output.push(char);
            }
            '|' if text_spoiler_can_start(input, offset) => output.push_str("&#x7C;"),
            '$' if text_math_can_start(input, offset) => {
                output.push('\\');
                output.push(char);
            }
            '!' if input[offset + char.len_utf8()..].starts_with('[') => {
                output.push('\\');
                output.push(char);
            }
            '~' if text_tilde_can_start(input, offset) => {
                output.push('\\');
                output.push(char);
            }
            '^' if text_caret_can_start(input, offset) => {
                output.push('\\');
                output.push(char);
            }
            '+' if text_attention_delimiter_can_start(input, offset, "++", false) => {
                output.push('\\');
                output.push(char);
            }
            '=' if text_attention_delimiter_can_start(input, offset, "==", false) => {
                output.push('\\');
                output.push(char);
            }
            '&' if text_character_reference_can_start(input, offset) => {
                output.push('\\');
                output.push(char);
            }
            '\\' | '[' | ']' => {
                output.push('\\');
                output.push(char);
            }
            _ => output.push(char),
        }
    }
    output
}

fn text_code_span_can_start(input: &str, offset: usize) -> bool {
    let marker_len = same_char_run_len(input, offset, '`');
    if marker_len == 0 || text_char_at_edge(input, offset, marker_len) {
        return true;
    }
    find_same_char_run(input, offset + marker_len, '`', marker_len).is_some()
}

fn text_attention_delimiter_can_start(
    input: &str,
    offset: usize,
    marker: &str,
    underscore: bool,
) -> bool {
    if !input[offset..].starts_with(marker) {
        return false;
    }
    if input[offset + marker.len()..].starts_with(marker)
        || text_char_at_edge(input, offset, marker.len())
    {
        return true;
    }
    if !text_delimiter_can_open(input, offset, marker.len(), underscore) {
        return false;
    }

    let mut cursor = offset + marker.len();
    while let Some(candidate) = input[cursor..].find(marker).map(|index| cursor + index) {
        if !input[candidate + marker.len()..].starts_with(marker)
            && text_delimiter_can_close(input, candidate, marker.len(), underscore)
        {
            return true;
        }
        cursor = candidate + marker.len();
    }
    false
}

fn text_delimiter_can_open(
    input: &str,
    offset: usize,
    marker_len: usize,
    underscore: bool,
) -> bool {
    let flanking = text_delimiter_flanking(input, offset, marker_len);
    if underscore {
        flanking.left
            && (!flanking.right
                || flanking
                    .previous
                    .is_some_and(|char| char.is_ascii_punctuation()))
    } else {
        flanking.left
    }
}

fn text_delimiter_can_close(
    input: &str,
    offset: usize,
    marker_len: usize,
    underscore: bool,
) -> bool {
    let flanking = text_delimiter_flanking(input, offset, marker_len);
    if underscore {
        flanking.right
            && (!flanking.left
                || flanking
                    .next
                    .is_some_and(|char| char.is_ascii_punctuation()))
    } else {
        flanking.right
    }
}

#[derive(Clone, Copy)]
struct TextDelimiterFlanking {
    left: bool,
    right: bool,
    previous: Option<char>,
    next: Option<char>,
}

fn text_delimiter_flanking(input: &str, offset: usize, marker_len: usize) -> TextDelimiterFlanking {
    let previous = input[..offset].chars().next_back();
    let next = input[offset + marker_len..].chars().next();

    let previous_whitespace = previous.is_none_or(char::is_whitespace);
    let next_whitespace = next.is_none_or(char::is_whitespace);
    let previous_punctuation = previous.is_some_and(|char| char.is_ascii_punctuation());
    let next_punctuation = next.is_some_and(|char| char.is_ascii_punctuation());

    let left = next.is_some()
        && !next_whitespace
        && !(next_punctuation && !previous_whitespace && !previous_punctuation);
    let right = previous.is_some()
        && !previous_whitespace
        && !(previous_punctuation && !next_whitespace && !next_punctuation);

    TextDelimiterFlanking {
        left,
        right,
        previous,
        next,
    }
}

fn text_less_than_can_start_inline(input: &str, offset: usize) -> bool {
    let after = &input[offset + '<'.len_utf8()..];
    if after.contains('>') {
        let next = after.chars().next();
        return next.is_some_and(|char| {
            char.is_ascii_alphabetic() || matches!(char, '/' | '!' | '?' | '_')
        }) || after.starts_with("http://")
            || after.starts_with("https://")
            || after.contains('@');
    }
    false
}

fn text_atx_heading_can_start(input: &str, offset: usize, output: &str) -> bool {
    if output_line_len(output) != 0 {
        return false;
    }
    let hashes = same_char_run_len(input, offset, '#');
    (1..=6).contains(&hashes)
        && input[offset + hashes..]
            .chars()
            .next()
            .is_none_or(char::is_whitespace)
}

fn text_spoiler_can_start(input: &str, offset: usize) -> bool {
    input[offset..].starts_with("||")
        && !input[offset + "||".len()..].starts_with('|')
        && input[offset + "||".len()..].contains("||")
}

fn text_math_can_start(input: &str, offset: usize) -> bool {
    // Mirror the parser's dollar-math start (code-span analogue): an opening run
    // of N dollars starts math when an exact-length-N closing run exists ahead.
    // Edge whitespace no longer blocks it, so a literal `$` adjacent to such a
    // run must be escaped to avoid forming math on the round trip.
    let marker_len = same_char_run_len(input, offset, '$');
    if marker_len == 0 || text_char_at_edge(input, offset, marker_len) {
        return true;
    }
    let after_open = offset + marker_len;
    find_same_char_run(input, after_open, '$', marker_len).is_some()
}

fn text_tilde_can_start(input: &str, offset: usize) -> bool {
    if input[offset..].starts_with("~~") {
        return text_attention_delimiter_can_start(input, offset, "~~", false)
            || text_simple_delimiter_can_start(input, offset, '~');
    }
    text_simple_delimiter_can_start(input, offset, '~')
}

fn text_caret_can_start(input: &str, offset: usize) -> bool {
    input[offset + '^'.len_utf8()..].starts_with('[')
        || text_simple_delimiter_can_start(input, offset, '^')
}

fn text_simple_delimiter_can_start(input: &str, offset: usize, marker: char) -> bool {
    let marker_len = marker.len_utf8();
    if text_char_at_edge(input, offset, marker_len)
        || input[offset + marker_len..].starts_with(marker)
        || input[..offset].ends_with(marker)
    {
        return true;
    }
    input[offset + marker_len..].contains(marker)
}

fn text_character_reference_can_start(input: &str, offset: usize) -> bool {
    let after = &input[offset + '&'.len_utf8()..];
    if let Some(rest) = after.strip_prefix('#') {
        let (digits, rest) = if let Some(hex) = rest.strip_prefix(['x', 'X']) {
            (
                hex.chars()
                    .take_while(|char| char.is_ascii_hexdigit())
                    .count(),
                hex,
            )
        } else {
            (
                rest.chars()
                    .take_while(|char| char.is_ascii_digit())
                    .count(),
                rest,
            )
        };
        return digits > 0 && rest[digits..].starts_with(';');
    }

    let name_len = after
        .chars()
        .take_while(|char| char.is_ascii_alphanumeric())
        .count();
    name_len > 0 && after[name_len..].starts_with(';')
}

fn text_char_at_edge(input: &str, offset: usize, len: usize) -> bool {
    offset == 0 || offset + len >= input.len()
}

fn same_char_run_len(input: &str, offset: usize, needle: char) -> usize {
    input[offset..]
        .chars()
        .take_while(|char| *char == needle)
        .map(char::len_utf8)
        .sum()
}

fn find_same_char_run(
    input: &str,
    mut offset: usize,
    needle: char,
    run_len: usize,
) -> Option<usize> {
    while offset < input.len() {
        let candidate = input[offset..].find(needle).map(|index| offset + index)?;
        if same_char_run_len(input, candidate, needle) == run_len {
            return Some(candidate);
        }
        offset = candidate + needle.len_utf8();
    }
    None
}

fn at_sign_can_start_email_autolink(input: &str, offset: usize) -> bool {
    let before = input[..offset]
        .chars()
        .next_back()
        .is_some_and(|char| char.is_ascii_alphanumeric());
    if !before {
        return false;
    }

    let mut saw_domain_char = false;
    let mut saw_dot = false;
    let mut saw_domain_char_after_dot = false;
    for char in input[offset + 1..].chars() {
        if char.is_ascii_alphanumeric() {
            saw_domain_char = true;
            if saw_dot {
                saw_domain_char_after_dot = true;
            }
            continue;
        }
        if char == '.' && saw_domain_char {
            saw_dot = true;
            continue;
        }
        if matches!(char, '-' | '_') && saw_domain_char {
            continue;
        }
        break;
    }
    saw_domain_char_after_dot
}

fn output_line_len(output: &str) -> usize {
    output
        .rsplit_once('\n')
        .map(|(_, line)| line.len())
        .unwrap_or_else(|| output.len())
}

fn escape_destination_with_pipe(input: &str, escape_pipe: bool) -> String {
    let mut output = String::new();
    for char in input.chars() {
        match char {
            char if char.is_control() => output.push_str(&format!("&#x{:X};", char as u32)),
            '|' if escape_pipe => {
                output.push('\\');
                output.push(char);
            }
            // NB: a literal space is NOT escaped here — `\ ` is not a valid
            // escape, so a space-containing destination is routed to the
            // angle-bracket form by `serialize_destination_kind`.
            '(' | ')' | '\\' | '<' | '>' | '&' => {
                output.push('\\');
                output.push(char);
            }
            _ => output.push(char),
        }
    }
    output
}

/// Normalize a serialized reference label exactly the way the parser matches
/// reference labels: collapse internal whitespace and Unicode case-fold the RAW
/// text (no backslash/entity unescape). Delegating to the parser's
/// `normalize_label` keeps this in lockstep so the Shortcut/Collapsed arms
/// decide correctly whether the rendered children already reproduce the
/// definition identifier.
fn normalize_reference_label(input: &str) -> String {
    crate::parse::normalize_label(input)
}

/// Emit the bracketed body of a link/image reference (`[text]`, `[text][]`, or
/// `[text][label]`) given the already-serialized `rendered` children and the
/// escaped raw `label`.
///
/// A Shortcut/Collapsed reference normally re-uses the rendered children as the
/// matching label, so it is only whole if those children fold back to the
/// definition identifier. Under RAW label matching the children can re-escape
/// in a fold-breaking way (e.g. a leading `^` becomes `\^`), so when the
/// children no longer reproduce the identifier we substitute the escaped raw
/// label as the bracket body — keeping the Shortcut/Collapsed kind (and its
/// re-parse) intact instead of degrading it into a Full reference.
fn push_reference_body(
    output: &mut String,
    kind: ReferenceKind,
    rendered: &str,
    children_match_identifier: bool,
    escaped_label: &str,
) {
    // For a Shortcut/Collapsed reference the bracket body must fold back to the
    // identifier on its own. Substitute the escaped raw label when the rendered
    // children would not (keeping the reference kind), but a Full reference
    // always keeps its rendered text since its explicit label does the matching.
    let use_label_body = !children_match_identifier && !matches!(kind, ReferenceKind::Full);
    let body = if use_label_body {
        escaped_label
    } else {
        rendered
    };

    output.push('[');
    output.push_str(body);
    output.push(']');

    match kind {
        ReferenceKind::Shortcut => {}
        ReferenceKind::Collapsed => output.push_str("[]"),
        ReferenceKind::Full => {
            output.push('[');
            output.push_str(escaped_label);
            output.push(']');
        }
    }
}

/// Escape the explicit label of a link/image reference. The original `label`
/// (not the normalized identifier) is used so case and entity spelling survive
/// the round-trip. A parsed label (`span.is_some()`) is already source text, so
/// only control characters are escaped; a hand-built label is semantic text and
/// is escaped like any reference label.
fn reference_explicit_label(
    from_source: bool,
    label: &str,
    context: InlineSerializeContext,
) -> String {
    if from_source {
        escape_reference_label_source(label, context.table_cell)
    } else {
        escape_reference_label_with_pipe(label, context.table_cell)
    }
}

/// Escapes a parsed definition label for re-emission. A definition label may
/// span several physical lines (CommonMark §4.7), and the parser stores those
/// interior newlines verbatim in `label`. Emitting them as literal line breaks
/// (rather than `&#xA;`) lets the multi-line label re-parse to the same raw
/// label, keeping the round trip stable; other control characters are still
/// numeric-escaped, and tabs pass through as in `escape_reference_label_source`.
fn escape_definition_label_source(input: &str) -> String {
    escape_reference_label_source(input, false)
}

fn escape_reference_label_source(input: &str, escape_pipe: bool) -> String {
    let mut output = String::new();
    for char in input.chars() {
        match char {
            // A reference label may span several physical lines, and the parser
            // matches the RAW label (whitespace collapsed, no entity decode).
            // Emitting interior newlines/tabs literally (rather than `&#xA;`)
            // keeps a whitespace-bearing label re-parsing as the same reference
            // — crucially, a `^`-prefixed label with literal whitespace stays a
            // link reference instead of becoming a footnote (which requires `^`
            // followed by non-whitespace).
            '\t' | '\n' | '\r' => output.push(char),
            char if char.is_control() => output.push_str(&format!("&#x{:X};", char as u32)),
            '|' if escape_pipe => {
                output.push('\\');
                output.push(char);
            }
            _ => output.push(char),
        }
    }
    output
}

fn escape_reference_label_with_pipe(input: &str, escape_pipe: bool) -> String {
    escape_label_syntax(input, escape_pipe, false)
}

fn escape_footnote_label_source(input: &str) -> String {
    let mut output = String::new();
    for char in input.chars() {
        match char {
            char if char.is_control() => output.push_str(&format!("&#x{:X};", char as u32)),
            _ => output.push(char),
        }
    }
    output
}

fn escape_footnote_label_semantic(input: &str) -> String {
    escape_label_syntax(input, false, true)
}

fn escape_label_syntax(input: &str, escape_pipe: bool, escape_whitespace: bool) -> String {
    let mut output = String::new();
    for char in input.chars() {
        match char {
            char if char.is_whitespace() && escape_whitespace => {
                output.push_str(&format!("&#x{:X};", char as u32));
            }
            char if char.is_control() => output.push_str(&format!("&#x{:X};", char as u32)),
            '|' if escape_pipe => {
                output.push('\\');
                output.push(char);
            }
            '\\' | '[' | ']' => {
                output.push('\\');
                output.push(char);
            }
            _ => output.push(char),
        }
    }
    output
}

fn escape_wikilink_part(input: &str) -> String {
    let mut output = String::new();
    for char in input.chars() {
        match char {
            char if char.is_control() => output.push_str(&format!("&#x{:X};", char as u32)),
            '\\' | '[' | ']' | '|' => {
                output.push('\\');
                output.push(char);
            }
            _ => output.push(char),
        }
    }
    output
}

fn serialize_destination_kind(
    input: &str,
    kind: LinkDestinationKind,
    context: InlineSerializeContext,
) -> String {
    match kind {
        LinkDestinationKind::Omitted if input.is_empty() => String::new(),
        LinkDestinationKind::Angle => {
            let mut output = String::from("<");
            output.push_str(&escape_angle_destination_with_context(input, context));
            output.push('>');
            output
        }
        LinkDestinationKind::Bare | LinkDestinationKind::Omitted => {
            if input.is_empty() {
                "<>".into()
            } else if input.contains(' ') {
                // A bare destination cannot contain a space (it would terminate
                // the destination, and `\ ` is not an escape), so emit the
                // angle-bracket form instead.
                let mut output = String::from("<");
                output.push_str(&escape_angle_destination_with_context(input, context));
                output.push('>');
                output
            } else {
                escape_destination_with_pipe(input, context.table_cell)
            }
        }
    }
}

fn escape_angle_destination_with_context(input: &str, context: InlineSerializeContext) -> String {
    let mut output = String::new();
    for char in input.chars() {
        match char {
            char if char.is_control() => output.push_str(&format!("&#x{:X};", char as u32)),
            '|' if context.table_cell => {
                output.push('\\');
                output.push(char);
            }
            '\\' | '<' | '>' => {
                output.push('\\');
                output.push(char);
            }
            _ => output.push(char),
        }
    }
    output
}

fn serialize_title_kind(
    input: &str,
    kind: LinkTitleKind,
    context: InlineSerializeContext,
) -> String {
    let (open, close) = match kind {
        LinkTitleKind::DoubleQuote => ('"', '"'),
        LinkTitleKind::SingleQuote => ('\'', '\''),
        LinkTitleKind::Paren => ('(', ')'),
    };
    let mut output = String::new();
    output.push(open);
    output.push_str(&escape_title_with_context(input, kind, context));
    output.push(close);
    output
}

fn escape_title_with_context(
    input: &str,
    kind: LinkTitleKind,
    context: InlineSerializeContext,
) -> String {
    let mut output = String::new();
    for char in input.chars() {
        match char {
            char if char.is_control() => output.push_str(&format!("&#x{:X};", char as u32)),
            '|' if context.table_cell => {
                output.push('\\');
                output.push(char);
            }
            '\\' | '&' => {
                output.push('\\');
                output.push(char);
            }
            '"' if kind == LinkTitleKind::DoubleQuote => {
                output.push('\\');
                output.push(char);
            }
            '\'' if kind == LinkTitleKind::SingleQuote => {
                output.push('\\');
                output.push(char);
            }
            '(' | ')' if kind == LinkTitleKind::Paren => {
                output.push('\\');
                output.push(char);
            }
            _ => output.push(char),
        }
    }
    output
}

fn unordered_list_marker(delimiter: ListDelimiter) -> char {
    match delimiter {
        ListDelimiter::Dash => '-',
        ListDelimiter::Asterisk => '*',
        ListDelimiter::Plus => '+',
        ListDelimiter::Period | ListDelimiter::Paren => '-',
    }
}

fn ordered_list_marker(delimiter: ListDelimiter) -> char {
    match delimiter {
        ListDelimiter::Paren => ')',
        ListDelimiter::Dash
        | ListDelimiter::Asterisk
        | ListDelimiter::Plus
        | ListDelimiter::Period => '.',
    }
}

fn prefix_lines(input: &str, prefix: &str) -> String {
    if input.is_empty() {
        return String::new();
    }
    let bytes = input.as_bytes();
    let mut output = String::new();
    let mut line_start = 0;
    let mut cursor = 0;
    while cursor < input.len() {
        let eol_end = match bytes[cursor] {
            b'\n' => Some(cursor + 1),
            b'\r' if bytes.get(cursor + 1) == Some(&b'\n') => Some(cursor + 2),
            b'\r' => Some(cursor + 1),
            _ => None,
        };
        if let Some(end) = eol_end {
            output.push_str(prefix);
            output.push_str(&input[line_start..end]);
            cursor = end;
            line_start = cursor;
        } else {
            cursor += 1;
        }
    }
    if line_start < input.len() {
        output.push_str(prefix);
        output.push_str(&input[line_start..]);
    }
    output
}

fn indent_after_first_line(input: &str, width: usize) -> String {
    let indent = " ".repeat(width);
    input
        .lines()
        .enumerate()
        .map(|(index, line)| {
            if index == 0 {
                line.into()
            } else {
                format!("{indent}{line}")
            }
        })
        .collect::<Vec<String>>()
        .join("\n")
}

fn indent_lines(input: &str, width: usize) -> String {
    let indent = " ".repeat(width);
    input
        .lines()
        .map(|line| {
            if line.is_empty() {
                String::new()
            } else {
                format!("{indent}{line}")
            }
        })
        .collect::<Vec<String>>()
        .join("\n")
}

fn indent_continuation(input: &str) -> String {
    input
        .lines()
        .enumerate()
        .map(|(index, line)| {
            if index == 0 {
                line.into()
            } else {
                format!("    {line}")
            }
        })
        .collect::<Vec<String>>()
        .join("\n")
}

fn trim_trailing_newline(input: &str) -> &str {
    input.trim_end_matches('\n').trim_end_matches('\r')
}

fn ends_with_line_ending(input: &str) -> bool {
    input.ends_with('\n') || input.ends_with('\r')
}

fn fence_for(input: &str, marker: FenceMarker, min_len: usize) -> String {
    let char = match marker {
        FenceMarker::Backtick => '`',
        FenceMarker::Tilde => '~',
    };
    let longest = longest_char_streak(input, char);
    char.to_string().repeat(min_len.max(longest + 1))
}

fn inline_code_fence(input: &str) -> String {
    fence_for(input, FenceMarker::Backtick, 1)
}

fn code_span_needs_padding(input: &str) -> bool {
    input.starts_with('`')
        || input.ends_with('`')
        || (input.starts_with(' ') && input.ends_with(' ') && input.chars().any(|char| char != ' '))
}

fn table_cell_escape_code_pipes(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    for char in input.chars() {
        if char == '|' {
            output.push('\\');
        }
        output.push(char);
    }
    output
}

fn block_math_fence(input: &str) -> String {
    let mut length = 2;
    for line in trim_trailing_newline(input).lines() {
        let trimmed = line.trim();
        if trimmed.len() >= 2 && trimmed.chars().all(|char| char == '$') {
            length = length.max(trimmed.len() + 1);
        }
    }
    "$".repeat(length)
}

fn serialize_inline_math_with_context(
    node: &MathInline,
    context: InlineSerializeContext,
) -> Result<String, SerializeError> {
    let input = node.value.as_str();

    // A table-cell pipe cannot live inside a dollar fence (it would split the
    // cell), so it is forced into the `$`…`$` code-math form regardless of the
    // node's recorded kind. That form cannot represent a value that itself
    // contains a `` `$ `` close.
    if context.table_cell && input.contains('|') {
        if input.contains("`$") {
            return Err(SerializeError::UnsupportedNode(
                "inline math containing a table pipe and a code-math close",
            ));
        }
        let input = table_cell_escape_code_pipes(input);
        return Ok(format!("$`{input}`$"));
    }

    match node.kind {
        MathInlineKind::Code => {
            if input.contains("`$") {
                return Err(SerializeError::UnsupportedNode(
                    "inline math (code-math form) containing a `$` close",
                ));
            }
            Ok(format!("$`{input}`$"))
        }
        // Dollar math is emitted verbatim behind an exact-length fence: no
        // padding strip and no fence widening. A single-`$` value can only
        // contain a `$` that is backslash-escaped (`\$`), which the flanking
        // parser skips, so an exact `$`…`$` fence round-trips; a `$$` display
        // value is verbatim including any edge spaces or newlines.
        MathInlineKind::Dollar { dollars } => {
            let fence = "$".repeat(usize::from(dollars));
            Ok(format!("{fence}{input}{fence}"))
        }
    }
}

fn table_cell_has_unescaped_pipe(input: &str) -> bool {
    let mut cursor = 0;
    let mut code_fence = None;
    let mut spoiler_open = false;
    while cursor < input.len() {
        let Some((next, char)) = input[cursor..]
            .chars()
            .next()
            .map(|char| (cursor + char.len_utf8(), char))
        else {
            break;
        };
        // Backticks are never escapable: a preceding backslash is code-span
        // content, so it must not suppress the code-span boundary here. Track
        // code spans only for extension syntax such as spoilers; a single
        // unescaped pipe still splits a table row, even inside code.
        if char == '`' {
            let length = input[cursor..]
                .as_bytes()
                .iter()
                .take_while(|byte| **byte == b'`')
                .count();
            if code_fence == Some(length) {
                code_fence = None;
            } else if code_fence.is_none() {
                code_fence = Some(length);
            }
            cursor += length;
            continue;
        }
        if char == '|' && input.as_bytes().get(cursor + 1) == Some(&b'|') && code_fence.is_some() {
            cursor += 2;
            continue;
        }
        if char == '|'
            && input.as_bytes().get(cursor + 1) == Some(&b'|')
            && code_fence.is_none()
            && !crate::parse::is_escaped_at(input, cursor)
        {
            let closes_spoiler =
                spoiler_open && input.as_bytes().get(cursor.wrapping_sub(1)) != Some(&b'|');
            let opens_spoiler = !spoiler_open
                && input.as_bytes().get(cursor + 2) != Some(&b'|')
                && find_table_cell_spoiler_close(input, cursor + 2).is_some();
            if closes_spoiler || opens_spoiler {
                spoiler_open = opens_spoiler;
                cursor += 2;
                continue;
            }
        }
        if char == '|' && !spoiler_open && !crate::parse::is_escaped_at(input, cursor) {
            return true;
        }
        cursor = next;
    }
    false
}

fn find_table_cell_spoiler_close(input: &str, mut offset: usize) -> Option<usize> {
    while offset < input.len() {
        let candidate = input[offset..].find("||").map(|index| offset + index)?;
        if !crate::parse::is_escaped_at(input, candidate)
            && input.as_bytes().get(candidate + 2) != Some(&b'|')
        {
            return Some(candidate);
        }
        offset = candidate + 2;
    }
    None
}

fn longest_char_streak(input: &str, needle: char) -> usize {
    let mut longest = 0;
    let mut current = 0;
    for char in input.chars() {
        if char == needle {
            current += 1;
            longest = longest.max(current);
        } else {
            current = 0;
        }
    }
    longest
}

fn directive_fence(inner: &str) -> String {
    ":".repeat(directive_fence_len(inner))
}

fn directive_fence_len(inner: &str) -> usize {
    let mut max = 3;
    for line in inner.lines() {
        if let Some(length) = directive_closing_fence_len(line) {
            max = max.max(length + 1);
        }
    }
    max
}

fn directive_closing_fence_len(line: &str) -> Option<usize> {
    let trimmed = trim_up_to_three_indent_columns(line)?;
    let length = trimmed
        .as_bytes()
        .iter()
        .take_while(|byte| **byte == b':')
        .count();
    if length >= 3 && trimmed[length..].trim().is_empty() {
        Some(length)
    } else {
        None
    }
}

fn trim_up_to_three_indent_columns(input: &str) -> Option<&str> {
    let mut columns = 0usize;
    let mut bytes = 0usize;
    for byte in input.as_bytes() {
        match *byte {
            b' ' => columns += 1,
            b'\t' => columns += 4 - (columns % 4),
            _ => break,
        }
        bytes += 1;
    }
    (columns <= 3).then_some(&input[bytes..])
}
