// Originally based on markdown-syntax 0.2.0 by Plimeor and modified by Lix.
// Upstream: https://github.com/plimeor/markdown-syntax
// Original license: MIT; see LICENSE-MIT beside this module.
//
//! Markdown source to AST. The entry points are the free [`parse`] function
//! (maximal default dialect) and the [`SyntaxOptions::parse`] /
//! [`SyntaxOptions::parse_strict`] methods. Parsing is tolerant: problems are
//! collected as [`Diagnostic`]s rather than aborting.

use alloc::{borrow::Cow, string::String, vec, vec::Vec};

use crate::{
    ast::*,
    diagnostic::{Diagnostic, DiagnosticCode, DiagnosticSeverity},
    entities::named_character_reference,
    options::{SyntaxConfigError, SyntaxOptions},
    span::Span,
    validate::is_directive_name,
};

/// The result of a tolerant parse: the document plus any diagnostics gathered
/// along the way (empty on a clean parse).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseOutput {
    /// The parsed document tree.
    pub document: Document,
    /// Diagnostics collected during parsing.
    pub diagnostics: Vec<Diagnostic>,
}

/// The error returned by [`SyntaxOptions::parse_strict`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParseStrictError {
    /// The options themselves were contradictory.
    Config(SyntaxConfigError),
    /// An error-severity diagnostic was promoted to a hard failure.
    Diagnostic(Diagnostic),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParsedLinkResource {
    destination: String,
    destination_kind: LinkDestinationKind,
    title: Option<String>,
    title_kind: Option<LinkTitleKind>,
}

const REFERENCE_LABEL_MAX_CHARS: usize = 999;
const WIKILINK_MAX_BYTES: usize = 999;

#[derive(Clone, Copy, Debug)]
struct Line<'a> {
    text: &'a str,
    eol: &'a str,
    start: usize,
    end: usize,
    end_with_eol: usize,
    /// True when this line reached the current container as a *lazy continuation*
    /// — a line with no container marker that nonetheless continues an open
    /// paragraph (CommonMark §5.2 laziness). Block constructs that must not be
    /// started by a lazy line (e.g. a setext underline) consult this flag.
    lazy: bool,
}

#[derive(Clone, Copy, Debug)]
struct ListMarkerInfo<'a> {
    ordered: bool,
    start: Option<u64>,
    delimiter: ListDelimiter,
    indent: usize,
    marker_len: usize,
    content_indent: usize,
    content: &'a str,
}

#[derive(Clone, Copy, Debug)]
struct DescriptionMarker<'a> {
    content_offset: usize,
    content: &'a str,
}

#[derive(Clone, Debug)]
struct DescriptionTerm {
    marker_index: usize,
    term_end: usize,
    blank_after_term: bool,
    source: String,
    source_offset: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HtmlBlockKind {
    RawTag,
    BlockTag,
    Until(&'static str),
    UntilBlank,
}

/// Parse `input` under the maximal default dialect ([`SyntaxOptions::default`]).
/// Infallible and tolerant; sugar for `SyntaxOptions::default().parse(input)`.
pub fn parse(input: &str) -> ParseOutput {
    SyntaxOptions::default().parse(input)
}

impl SyntaxOptions {
    /// Parse `input` under these options. Infallible and tolerant: a config
    /// conflict (reachable only by hand-building contradictory `Constructs`) is
    /// surfaced as an error diagnostic rather than a hard error. Call
    /// [`SyntaxOptions::validate`] first for fail-fast config checking.
    pub fn parse(&self, input: &str) -> ParseOutput {
        match parse_checked(input, self) {
            Ok(output) => output,
            Err(error) => ParseOutput {
                document: Document::default(),
                diagnostics: vec![Diagnostic::new(
                    DiagnosticSeverity::Error,
                    DiagnosticCode::StrictParse,
                    Span::new(0, input.len()),
                    error.message(),
                )],
            },
        }
    }

    /// Parse `input`, promoting a config conflict or any error-severity
    /// diagnostic to a hard [`ParseStrictError`].
    pub fn parse_strict(&self, input: &str) -> Result<ParseOutput, ParseStrictError> {
        let output = parse_checked(input, self).map_err(ParseStrictError::Config)?;
        if let Some(diagnostic) = output
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
        {
            return Err(ParseStrictError::Diagnostic(diagnostic.clone()));
        }
        Ok(output)
    }
}

fn parse_checked(input: &str, options: &SyntaxOptions) -> Result<ParseOutput, SyntaxConfigError> {
    options.validate()?;
    Ok(parse_with_definitions(
        input,
        options,
        definition_identifiers(input, options),
    ))
}

fn parse_with_definitions(
    input: &str,
    options: &SyntaxOptions,
    definitions: Vec<String>,
) -> ParseOutput {
    let mut diagnostics = Vec::new();
    let children = parse_blocks(input, 0, true, options, &definitions, &mut diagnostics);

    ParseOutput {
        document: Document {
            meta: NodeMeta::new(Some(Span::new(0, input.len()))),
            children,
        },
        diagnostics,
    }
}

/// A definition can only close with an adjacent `]:`. If that byte sequence is
/// absent, `collect_definitions` can only return an empty list, so avoid its
/// otherwise full discarded block/inline parse. Inputs that merely contain the
/// marker still take the established collector unchanged; false positives are
/// intentionally conservative.
fn definition_identifiers(input: &str, options: &SyntaxOptions) -> Vec<String> {
    if input.contains("]:") {
        collect_definitions(input, options)
    } else {
        Vec::new()
    }
}

fn parse_blocks(
    input: &str,
    base_offset: usize,
    allow_frontmatter: bool,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Vec<Block> {
    let lines = collect_lines(input, base_offset);
    parse_blocks_from_lines(&lines, allow_frontmatter, options, definitions, diagnostics)
}

fn parse_blocks_from_lines(
    lines: &[Line<'_>],
    allow_frontmatter: bool,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Vec<Block> {
    let mut blocks = Vec::new();
    let mut index = 0;

    while index < lines.len() {
        let line = lines[index];
        if line.text.trim().is_empty() {
            index += 1;
            continue;
        }
        let after_definition_unbroken = index > 0
            && !lines[index - 1].text.trim().is_empty()
            && matches!(blocks.last(), Some(Block::Definition(_)));

        if allow_frontmatter && index == 0 {
            if let Some((block, next)) = parse_frontmatter(lines, index, options) {
                blocks.push(block);
                index = next;
                continue;
            }
        }

        if let Some((block, next)) =
            parse_container_directive(lines, index, options, definitions, diagnostics)
        {
            blocks.push(block);
            index = next;
            continue;
        }

        if let Some((block, next)) = parse_math_block(lines, index, options) {
            blocks.push(block);
            index = next;
            continue;
        }

        if let Some((block, next)) = parse_fenced_code(lines, index, options) {
            blocks.push(block);
            index = next;
            continue;
        }

        if let Some((block, next)) =
            parse_block_quote(lines, index, options, definitions, diagnostics)
        {
            blocks.push(block);
            index = next;
            continue;
        }

        if let Some(block) = parse_atx_heading(line, options, definitions) {
            blocks.push(block);
            index += 1;
            continue;
        }

        if let Some(block) = parse_thematic_break(line) {
            blocks.push(block);
            index += 1;
            continue;
        }

        if let Some((block, next)) = parse_list(lines, index, options, definitions, diagnostics) {
            blocks.push(block);
            index = next;
            continue;
        }

        if let Some((block, next)) =
            parse_footnote_definition(lines, index, options, definitions, diagnostics)
        {
            blocks.push(block);
            index = next;
            continue;
        }

        if let Some((block, next)) =
            parse_definition(lines, index, options, after_definition_unbroken)
        {
            blocks.push(block);
            index = next;
            continue;
        }

        if let Some(block) = parse_leaf_directive(line, options, definitions, diagnostics) {
            blocks.push(block);
            index += 1;
            continue;
        }

        if let Some((block, next)) =
            parse_html_container(lines, index, options, definitions, diagnostics)
        {
            blocks.push(block);
            index = next;
            continue;
        }

        if let Some((block, next)) = parse_html_block(lines, index, options) {
            blocks.push(block);
            index = next;
            continue;
        }

        if let Some((block, next)) = parse_mdx_flow(lines, index, options, diagnostics) {
            blocks.push(block);
            index = next;
            continue;
        }

        if !after_definition_unbroken {
            if let Some((block, next)) = parse_indented_code(lines, index, options) {
                blocks.push(block);
                index = next;
                continue;
            }
        }

        if let Some((block, next)) = parse_table(lines, index, options, definitions, diagnostics) {
            blocks.push(block);
            index = next;
            continue;
        }

        if let Some((block, next)) = parse_setext_heading(lines, index, options, definitions) {
            blocks.push(block);
            index = next;
            continue;
        }

        if let Some((block, next)) =
            parse_description_list(lines, index, options, definitions, diagnostics)
        {
            blocks.push(block);
            index = next;
            continue;
        }

        let (block, next) = parse_paragraph(lines, index, options, definitions, diagnostics);
        blocks.push(block);
        index = next;
    }

    blocks
}

fn collect_lines(input: &str, base_offset: usize) -> Vec<Line<'_>> {
    let bytes = input.as_bytes();
    let mut lines = Vec::new();
    let mut start = 0;
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'\n' => {
                let end = index;
                lines.push(Line {
                    text: &input[start..end],
                    eol: &input[index..index + 1],
                    start: base_offset + start,
                    end: base_offset + end,
                    end_with_eol: base_offset + index + 1,
                    lazy: false,
                });
                index += 1;
                start = index;
            }
            b'\r' => {
                let end = index;
                let eol_end = if index + 1 < bytes.len() && bytes[index + 1] == b'\n' {
                    index + 2
                } else {
                    index + 1
                };
                lines.push(Line {
                    text: &input[start..end],
                    eol: &input[index..eol_end],
                    start: base_offset + start,
                    end: base_offset + end,
                    end_with_eol: base_offset + eol_end,
                    lazy: false,
                });
                index = eol_end;
                start = index;
            }
            _ => index += 1,
        }
    }

    if start < bytes.len() || input.is_empty() {
        lines.push(Line {
            text: &input[start..],
            eol: "",
            start: base_offset + start,
            end: base_offset + bytes.len(),
            end_with_eol: base_offset + bytes.len(),
            lazy: false,
        });
    }

    lines
}

fn collect_definitions(input: &str, options: &SyntaxOptions) -> Vec<String> {
    let mut diagnostics = Vec::new();
    let blocks = parse_blocks(input, 0, true, options, &[], &mut diagnostics);
    let mut definitions = Vec::new();
    collect_definition_refs_from_blocks(&blocks, &mut definitions);
    definitions
}

fn collect_definition_refs_from_blocks(blocks: &[Block], definitions: &mut Vec<String>) {
    for block in blocks {
        match block {
            Block::Definition(definition) => {
                if definitions
                    .iter()
                    .all(|identifier| identifier != &definition.identifier)
                {
                    definitions.push(definition.identifier.clone());
                }
            }
            Block::BlockQuote(node) => {
                collect_definition_refs_from_blocks(&node.children, definitions);
            }
            Block::Alert(node) => {
                collect_definition_refs_from_blocks(&node.children, definitions);
            }
            Block::List(node) => {
                for item in &node.children {
                    collect_definition_refs_from_blocks(&item.children, definitions);
                }
            }
            Block::DescriptionList(node) => {
                for item in &node.children {
                    for details in &item.details {
                        collect_definition_refs_from_blocks(&details.children, definitions);
                    }
                }
            }
            Block::FootnoteDefinition(node) => {
                collect_definition_refs_from_blocks(&node.children, definitions);
            }
            Block::HtmlContainer(node) => {
                if let HtmlContainerContent::Blocks(children) = &node.content {
                    collect_definition_refs_from_blocks(children, definitions);
                }
            }
            Block::ContainerDirective(node) => {
                collect_definition_refs_from_blocks(&node.children, definitions);
            }
            _ => {}
        }
    }
}

fn parse_frontmatter(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
) -> Option<(Block, usize)> {
    if !options.constructs.frontmatter {
        return None;
    }
    let kind = frontmatter_fence_kind(lines[index].text)?;

    let mut value = String::new();
    let mut cursor = index + 1;
    while cursor < lines.len() {
        if frontmatter_fence_kind(lines[cursor].text) == Some(kind) {
            let span = Span::new(lines[index].start, lines[cursor].end_with_eol);
            return Some((
                Block::Frontmatter(Frontmatter {
                    meta: NodeMeta::new(Some(span)),
                    kind,
                    value,
                }),
                cursor + 1,
            ));
        }
        push_line(&mut value, lines[cursor].text);
        cursor += 1;
    }

    None
}

fn frontmatter_fence_kind(line: &str) -> Option<FrontmatterKind> {
    match line.trim_end_matches([' ', '\t']) {
        "---" => Some(FrontmatterKind::Yaml),
        "+++" => Some(FrontmatterKind::Toml),
        _ => None,
    }
}

fn parse_container_directive(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<(Block, usize)> {
    if !options.constructs.directive_container {
        return None;
    }
    let trimmed = trim_up_to_three_spaces(lines[index].text)?;
    let Some((fence_len, opener_rest)) = directive_container_opener_prefix(trimmed) else {
        return None;
    };
    let opener_base = lines[index].start + (lines[index].text.len() - trimmed.len()) + fence_len;

    let Some((name, label_source, attributes, _consumed)) = parse_directive_opener(opener_rest)
    else {
        diagnostics.push(Diagnostic::new(
            DiagnosticSeverity::Error,
            DiagnosticCode::InvalidDirectiveName,
            Span::new(lines[index].start, lines[index].end),
            "container directive must have a valid name",
        ));
        return None;
    };
    let label_base = opener_base + name.len() + 1;

    let mut content = String::new();
    let mut cursor = index + 1;
    let mut nested_fences = Vec::new();
    while cursor < lines.len() {
        let line = lines[cursor].text;
        let trimmed = trim_up_to_three_spaces(line);
        if let Some(trimmed) = trimmed {
            if let Some(nested_len) = nested_fences.last().copied() {
                if directive_container_closing_fence(trimmed, nested_len).is_some() {
                    nested_fences.pop();
                    push_line(&mut content, line);
                    cursor += 1;
                    continue;
                }
            } else if directive_container_closing_fence(trimmed, fence_len).is_some() {
                let label = label_source
                    .map(|source| {
                        parse_inlines(source, label_base, options, definitions, diagnostics)
                    })
                    .unwrap_or_default();
                let children = parse_blocks(
                    &content,
                    lines[index + 1].start,
                    false,
                    options,
                    definitions,
                    diagnostics,
                );
                return Some((
                    Block::ContainerDirective(ContainerDirective {
                        meta: NodeMeta::new(Some(Span::new(
                            lines[index].start,
                            lines[cursor].end_with_eol,
                        ))),
                        name,
                        label,
                        attributes,
                        children,
                    }),
                    cursor + 1,
                ));
            }

            if let Some((nested_len, nested_rest)) = directive_container_opener_prefix(trimmed) {
                if parse_directive_opener(nested_rest).is_some() {
                    nested_fences.push(nested_len);
                }
            }
        }

        push_line(&mut content, line);
        cursor += 1;
    }

    diagnostics.push(Diagnostic::new(
        DiagnosticSeverity::Error,
        DiagnosticCode::UnclosedDirectiveContainer,
        Span::new(lines[index].start, lines[index].end),
        "container directive is missing a closing fence",
    ));
    Some((
        Block::ContainerDirective(ContainerDirective {
            meta: NodeMeta::new(Some(Span::new(
                lines[index].start,
                lines.last()?.end_with_eol,
            ))),
            name,
            label: label_source
                .map(|source| parse_inlines(source, label_base, options, definitions, diagnostics))
                .unwrap_or_default(),
            attributes,
            children: parse_blocks(
                &content,
                lines
                    .get(index + 1)
                    .map(|line| line.start)
                    .unwrap_or(lines[index].end),
                false,
                options,
                definitions,
                diagnostics,
            ),
        }),
        lines.len(),
    ))
}

fn directive_container_opener_prefix(input: &str) -> Option<(usize, &str)> {
    let fence_len = input
        .as_bytes()
        .iter()
        .take_while(|byte| **byte == b':')
        .count();
    if fence_len >= 3 {
        Some((fence_len, &input[fence_len..]))
    } else {
        None
    }
}

fn directive_container_closing_fence(input: &str, min_len: usize) -> Option<usize> {
    let fence_len = input
        .as_bytes()
        .iter()
        .take_while(|byte| **byte == b':')
        .count();
    if fence_len >= min_len && input[fence_len..].trim().is_empty() {
        Some(fence_len)
    } else {
        None
    }
}

fn parse_math_block(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
) -> Option<(Block, usize)> {
    if !options.constructs.math_block {
        return None;
    }
    // A math-flow opener is the fenced-code analogue: a `>=2` dollar run after
    // 0–3 columns of indent, optionally followed by an "info"/meta string that
    // must NOT contain another `$` (`$$ $$` is inline math, not a flow open).
    // The opening indent is stripped (up to its own width) from each content
    // line, exactly like a fenced code block.
    let opener = trim_up_to_three_spaces(lines[index].text)?;
    let fence_length = math_block_fence_length(opener)?;
    let opening_indent = leading_indent_columns(lines[index].text);

    let mut value = String::new();
    let mut content_lines = 0usize;
    let mut cursor = index + 1;
    while cursor < lines.len() {
        if let Some(close_line) = trim_up_to_three_spaces(lines[cursor].text) {
            if math_block_fence_closes(close_line, fence_length) {
                return Some((
                    Block::MathBlock(MathBlock {
                        meta: NodeMeta::new(Some(Span::new(
                            lines[index].start,
                            lines[cursor].end_with_eol,
                        ))),
                        value,
                    }),
                    cursor + 1,
                ));
            }
        }
        if content_lines > 0 {
            // The previous content line's `eol` usually separates lines. This
            // fallback only covers synthetic child input that lacks an EOL despite
            // yielding another line.
            ensure_line_separator(&mut value);
        }
        let stripped = strip_leading_indent_columns(lines[cursor].text, opening_indent);
        value.push_str(&stripped);
        value.push_str(lines[cursor].eol);
        content_lines += 1;
        cursor += 1;
    }

    // EOF closes the block (an unclosed opener runs to end of document); an
    // immediate EOF after the opener yields an empty math block.
    Some((
        Block::MathBlock(MathBlock {
            meta: NodeMeta::new(Some(Span::new(
                lines[index].start,
                lines.last()?.end_with_eol,
            ))),
            value,
        }),
        lines.len(),
    ))
}

/// Length of the leading `$` run if `input` (already indent-stripped) is a valid
/// math-flow opener: `>=2` dollars, then an info string with no further `$`.
fn math_block_fence_length(input: &str) -> Option<usize> {
    let length = input
        .as_bytes()
        .iter()
        .take_while(|byte| **byte == b'$')
        .count();
    if length < 2 || input[length..].contains('$') {
        return None;
    }
    Some(length)
}

/// A math-flow closing line (already indent-stripped) is a run of `>=length`
/// dollars and nothing else (trailing whitespace aside).
fn math_block_fence_closes(input: &str, length: usize) -> bool {
    let count = input
        .as_bytes()
        .iter()
        .take_while(|byte| **byte == b'$')
        .count();
    count >= length && input[count..].trim().is_empty()
}

fn parse_fenced_code(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
) -> Option<(Block, usize)> {
    let line = fence_line(lines[index].text, options)?;
    let (marker, length) = fence_start(line)?;
    // CommonMark: up to N columns of indentation (N = the opening fence's
    // indent, 0–3) are removed from each content line.
    let opening_indent = leading_indent_columns(lines[index].text);
    let info = line[length..].trim();
    if marker == FenceMarker::Backtick && info.contains('`') {
        return None;
    }
    let info = if info.is_empty() {
        None
    } else {
        Some(unescape_string(info))
    };

    let mut value = String::new();
    // Join content lines with `\n` while preserving a leading blank line: a
    // fenced block can open with a blank content line, and `push_line`'s
    // empty-output proxy cannot tell zero lines from one empty line, so it would
    // drop that leading blank. Track the count explicitly (as parse_math_block).
    let mut content_lines = 0usize;
    let mut cursor = index + 1;
    while cursor < lines.len() {
        if let Some(close_line) = fence_line(lines[cursor].text, options) {
            if fence_close(close_line, marker, length) {
                return Some((
                    Block::CodeBlock(CodeBlock {
                        meta: NodeMeta::new(Some(Span::new(
                            lines[index].start,
                            lines[cursor].end_with_eol,
                        ))),
                        kind: CodeBlockKind::Fenced { marker, length },
                        info,
                        value,
                    }),
                    cursor + 1,
                ));
            }
        }
        if content_lines > 0 {
            // The previous content line's `eol` usually separates lines. This
            // fallback only covers synthetic child input that lacks an EOL despite
            // yielding another line.
            ensure_line_separator(&mut value);
        }
        let stripped = strip_leading_indent_columns(lines[cursor].text, opening_indent);
        value.push_str(&stripped);
        value.push_str(lines[cursor].eol);
        content_lines += 1;
        cursor += 1;
    }
    Some((
        Block::CodeBlock(CodeBlock {
            meta: NodeMeta::new(Some(Span::new(
                lines[index].start,
                lines.last()?.end_with_eol,
            ))),
            kind: CodeBlockKind::Fenced { marker, length },
            info,
            value,
        }),
        lines.len(),
    ))
}

fn fence_line<'a>(line: &'a str, options: &SyntaxOptions) -> Option<&'a str> {
    if options.constructs.indented_code {
        trim_up_to_three_spaces(line)
    } else {
        Some(trim_ascii_start(line))
    }
}

fn container_closed_after_unclosed_fence(
    lines: &[Line<'_>],
    cursor: usize,
    last_content_index: usize,
    content: &str,
    options: &SyntaxOptions,
) -> bool {
    !lines[last_content_index].eol.is_empty()
        && (cursor >= lines.len() || lines[cursor].text.trim().is_empty())
        && content_has_unclosed_fenced_code(content, options)
}

fn content_has_unclosed_fenced_code(content: &str, options: &SyntaxOptions) -> bool {
    let lines = collect_lines(content, 0);
    let mut open_fence = None;
    for line in lines {
        let Some(trimmed) = fence_line(line.text, options) else {
            continue;
        };
        if let Some((marker, length, has_nonblank_content)) = open_fence {
            if fence_close(trimmed, marker, length) {
                open_fence = None;
            } else {
                open_fence = Some((
                    marker,
                    length,
                    has_nonblank_content || !trimmed.trim().is_empty(),
                ));
            }
            continue;
        }
        let Some((marker, length)) = fence_start(trimmed) else {
            continue;
        };
        let info = trimmed[length..].trim();
        if marker != FenceMarker::Backtick || !info.contains('`') {
            open_fence = Some((marker, length, false));
        }
    }
    open_fence.is_some_and(|(_, _, has_nonblank_content)| !has_nonblank_content)
}

/// Recursively determines whether the innermost block reachable through this
/// (already marker-stripped) block-quote content line is an OPEN paragraph —
/// the only block kind that a following lazy continuation line may extend.
///
/// Nested quote markers are stripped one level at a time so that, e.g.,
/// `> > a` reports that the deepest content `a` is an open paragraph (this is
/// what lets a lazy line continue a paragraph buried inside several quotes).
/// Indented code, blank lines, HTML blocks, and every other block start are
/// reported as NOT-an-open-paragraph.
fn block_quote_content_paragraph_open(content: &str, options: &SyntaxOptions) -> bool {
    let Some(trimmed) = trim_up_to_three_spaces(content) else {
        // >= 4 columns of indentation: indented code, never a paragraph.
        return false;
    };
    if trimmed.is_empty() {
        return false;
    }
    if let Some(rest) = trimmed.strip_prefix('>') {
        let rest = rest.strip_prefix(' ').unwrap_or(rest);
        return block_quote_content_paragraph_open(rest, options);
    }
    if let Some(marker) = list_marker_info(trimmed) {
        let first_content = list_marker_first_content(trimmed, marker);
        return block_quote_content_paragraph_open(&first_content, options);
    }
    !lazy_line_starts_block(trimmed, options)
}

/// Whether a line starts a block for the purpose of LAZY-continuation
/// suppression. Identical to [`likely_block_start`] except that *every* HTML
/// block start — including the type-7 "complete tag" form that cannot interrupt
/// a paragraph with a marker present — blocks lazy continuation. A bare `<a>`
/// after `> a` must close the quote, not be absorbed as paragraph text.
fn lazy_line_starts_block(input: &str, options: &SyntaxOptions) -> bool {
    likely_block_start(input, options)
        || (options.constructs.html_block && line_starts_html_block(input))
        // A lazy line that almost opens a fenced code block — any fence-char
        // run after up to three spaces of indent — ends the paragraph instead
        // of continuing it (GH-19): `> x\n``\n` closes the quote rather than
        // joining `` ` `` onto the paragraph.
        || trim_up_to_three_spaces(input).is_some_and(|t| t.starts_with('`') || t.starts_with('~'))
}

fn parse_block_quote(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<(Block, usize)> {
    if !trim_up_to_three_spaces(lines[index].text)?.starts_with('>') {
        return None;
    }

    let mut content = String::new();
    // Lazy provenance per collected content line, parallel to the `\n`-joined
    // `content`. Re-split (`collect_lines`) lines map 1:1 to these flags, so the
    // child parser can suppress lazy-only constructs (e.g. setext underlines).
    let mut lazy_flags: Vec<bool> = Vec::new();
    let mut cursor = index;
    let mut paragraph_open = false;
    let mut in_table = false;
    let mut last_content_line: Option<String> = None;
    let mut content_base_offset = None;
    while cursor < lines.len() {
        let raw = lines[cursor].text;
        let trimmed_opt = trim_up_to_three_spaces(raw);
        let marked = trimmed_opt.is_some_and(|trimmed| trimmed.starts_with('>'));
        let quote_rest_owned: String;
        if let Some(trimmed) = trimmed_opt {
            if trimmed.is_empty() {
                break;
            }
        }
        let (line, line_start) = if marked {
            let trimmed = trimmed_opt.expect("marked implies a trimmed line");
            let trimmed_start = lines[cursor].start + (raw.len() - trimmed.len());
            let mut rest_start = 1;
            let mut rest = &trimmed[rest_start..];
            if rest.starts_with(' ') {
                rest_start += 1;
                rest = &rest[1..];
            } else if rest.starts_with('\t') {
                let marker_end_column = leading_indent_columns(raw) + 1;
                match strip_leading_indent_columns_from(rest, 1, marker_end_column) {
                    Cow::Borrowed(stripped) => rest = stripped,
                    Cow::Owned(stripped) => {
                        quote_rest_owned = stripped;
                        rest = &quote_rest_owned;
                    }
                }
            }
            (rest, trimmed_start + rest_start)
        } else if in_table {
            // An open GFM table absorbs unmarked rows (lazy table body); a
            // non-row unmarked line ends the quote.
            break;
        } else if paragraph_open && !lazy_line_starts_block(raw, options) {
            // Lazy paragraph continuation: a marker-less line that continues an
            // open paragraph (possibly nested). The RAW line is used verbatim —
            // its indentation (even >= 4 columns) is paragraph text, not code.
            (raw, lines[cursor].start)
        } else {
            break;
        };

        let mut escaped_lazy = String::new();
        let line = if !marked
            && last_content_line.as_deref().is_some_and(|previous| {
                table_can_start_source(
                    previous,
                    line,
                    options.constructs.indented_code,
                    options.constructs.spoiler,
                )
            }) {
            escaped_lazy.push_str(line);
            if let Some(offset) = escaped_lazy.find('-') {
                escaped_lazy.insert(offset, '\\');
            }
            &escaped_lazy
        } else {
            line
        };

        let starts_table = last_content_line.as_deref().is_some_and(|previous| {
            table_can_start_source(
                previous,
                line,
                options.constructs.indented_code,
                options.constructs.spoiler,
            )
        });
        if marked && starts_table {
            paragraph_open = false;
            in_table = true;
        } else if marked && in_table && block_quote_table_body_row(line, options) {
            paragraph_open = false;
        } else {
            in_table = false;
            // Track the innermost open paragraph across nested quote markers so a
            // following lazy line can reach a paragraph buried in nested quotes.
            paragraph_open = block_quote_content_paragraph_open(line, options);
        }
        last_content_line = Some(line.into());
        if content_base_offset.is_none() {
            content_base_offset = Some(line_start);
        }
        push_line(&mut content, line);
        lazy_flags.push(!marked);
        cursor += 1;
    }

    let span = Span::new(lines[index].start, lines[cursor - 1].end_with_eol);
    let child_base_offset = content_base_offset.unwrap_or(lines[index].start);
    if !lines[cursor - 1].eol.is_empty() && !ends_with_line_ending(&content) {
        content.push_str(lines[cursor - 1].eol);
    }
    if container_closed_after_unclosed_fence(lines, cursor, cursor - 1, &content, options) {
        content.push('\n');
    }
    if let Some(alert) = parse_alert_from_block_quote(
        &content,
        child_base_offset,
        span,
        options,
        definitions,
        diagnostics,
    ) {
        return Some((alert, cursor));
    }

    let mut child_lines = collect_lines(&content, child_base_offset);
    for (child, &lazy) in child_lines.iter_mut().zip(lazy_flags.iter()) {
        child.lazy = lazy;
    }
    let children = parse_blocks_from_lines(&child_lines, false, options, definitions, diagnostics);
    Some((
        Block::BlockQuote(BlockQuote {
            meta: NodeMeta::new(Some(span)),
            children,
        }),
        cursor,
    ))
}

fn parse_alert_from_block_quote(
    content: &str,
    base_offset: usize,
    span: Span,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<Block> {
    if !options.constructs.gfm_alert {
        return None;
    }
    let (first_line, rest) = content.split_once('\n').unwrap_or((content, ""));
    let (kind, title) = parse_alert_marker(first_line)?;
    let rest_base_offset = base_offset + first_line.len() + usize::from(!rest.is_empty());
    let children = if rest.is_empty() {
        Vec::new()
    } else {
        parse_blocks(
            rest,
            rest_base_offset,
            false,
            options,
            definitions,
            diagnostics,
        )
    };
    Some(Block::Alert(Alert {
        meta: NodeMeta::new(Some(span)),
        kind,
        title,
        children,
    }))
}

fn parse_alert_marker(line: &str) -> Option<(AlertKind, Option<String>)> {
    let close = line.find(']')?;
    let marker = line.get(0..close + 1)?;
    if !marker.starts_with("[!") {
        return None;
    }
    let kind = match &marker[2..close].to_ascii_lowercase()[..] {
        "note" => AlertKind::Note,
        "tip" => AlertKind::Tip,
        "important" => AlertKind::Important,
        "warning" => AlertKind::Warning,
        "caution" => AlertKind::Caution,
        _ => return None,
    };
    let title = line[close + 1..].trim();
    Some((
        kind,
        if title.is_empty() {
            None
        } else {
            Some(title.into())
        },
    ))
}

fn block_quote_table_body_row(line: &str, options: &SyntaxOptions) -> bool {
    table_indent_line(line, options.constructs.indented_code).is_some_and(|row| {
        !row.trim().is_empty() && contains_unescaped_pipe(row, options.constructs.spoiler)
    })
}

fn parse_list(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<(Block, usize)> {
    let first_marker = list_marker_info(lines[index].text)?;
    let mut items = Vec::new();
    let mut cursor = index;
    let mut tight = true;

    while cursor < lines.len() {
        // A thematic break (`* * *`, `---`, …) outranks a list marker at the same
        // position: it ends the list rather than opening a nested item. Test it
        // before accepting the line as a marker (precedence belongs at the call
        // site, not inside `list_marker_info`).
        if parse_thematic_break(lines[cursor]).is_some() {
            break;
        }
        let Some(marker) = list_marker_info(lines[cursor].text) else {
            break;
        };
        if !same_list_marker(first_marker, marker) {
            break;
        }

        let item_start = cursor;
        let mut item_end = cursor;
        let mut item_tight = true;
        // Byte offsets within `content` at which an item-internal blank line
        // sits. After the item's children are parsed, a blank loosens the item
        // only when it falls in the GAP between two consecutive top-level
        // children (a direct separator); a blank absorbed inside a nested
        // container's span does not (per-list tightness).
        let mut item_blank_offsets: Vec<usize> = Vec::new();
        let mut content = String::new();
        // Lazy provenance per collected content line (parallel to the `\n`-joined
        // `content`, mapped 1:1 by the re-split `collect_lines`). A line is lazy
        // when it reached the item only as a paragraph continuation while
        // dedented below the item's content start: it is paragraph text and must
        // not begin a new block (e.g. `- d\n    - e` keeps `- e` as the lazy tail
        // of `d`'s paragraph, not a sublist — CommonMark "too few spaces").
        let mut lazy_flags: Vec<bool> = Vec::new();
        let mut open_fence = None;
        let first_content = list_marker_first_content(lines[cursor].text, marker);
        let mut last_content_line: Option<String> = Some(first_content.as_ref().into());
        let mut paragraph_open = list_item_paragraph_stays_open(None, &first_content, options);
        // CommonMark §5.2: a list item can begin with at most one blank line.
        // When the marker has no content the item starts blank, and the first
        // following blank line ends it — later indented content cannot join
        // (`-\n\n  foo` → empty item + separate paragraph).
        let mut item_started_blank = first_content.trim().is_empty();
        push_line(&mut content, &first_content);
        lazy_flags.push(false);
        update_list_item_fence(&first_content, &mut open_fence);
        cursor += 1;

        while cursor < lines.len() {
            if lines[cursor].text.trim().is_empty() {
                // Blank/whitespace lines inside an open fenced code block are
                // verbatim code content, not item-ending blanks: keep them.
                if open_fence.is_some() {
                    let stripped = strip_list_continuation(
                        lines[cursor].text,
                        marker.content_indent,
                        first_marker.indent,
                    );
                    push_line(&mut content, &stripped);
                    lazy_flags.push(false);
                    update_list_item_fence(&stripped, &mut open_fence);
                    item_end = cursor;
                    cursor += 1;
                    continue;
                }
                let next = next_nonblank_line(lines, cursor + 1);
                if item_started_blank
                    || next >= lines.len()
                    || sibling_list_marker_at_line(
                        lines[next].text,
                        first_marker,
                        marker.content_indent,
                    )
                    || leading_indent_columns(lines[next].text) < marker.content_indent
                {
                    if next < lines.len()
                        && sibling_list_marker_at_line(
                            lines[next].text,
                            first_marker,
                            marker.content_indent,
                        )
                    {
                        item_tight = false;
                    }
                    cursor = next;
                    break;
                }
                // A blank between item content is recorded; whether it actually
                // loosens THIS list is decided structurally after the item's
                // children are parsed (a blank buried in a nested sublist must
                // not loosen the outer list — CommonMark requires the item to
                // *directly* contain the blank-separated blocks). Track the blank
                // line's offset within the collected content so the structural
                // check can tell a direct-child separator from a nested one.
                item_blank_offsets.push(content.len() + usize::from(!content.is_empty()));
                paragraph_open = false;
                push_line(&mut content, "");
                lazy_flags.push(false);
                item_end = cursor;
                cursor += 1;
                continue;
            }

            item_started_blank = false;

            if sibling_list_marker_at_line(lines[cursor].text, first_marker, marker.content_indent)
            {
                break;
            }

            // A list marker of a different type/delimiter is a block boundary
            // (CommonMark §5.3: changing the marker starts a new list). It is not
            // a same-list sibling, so it would otherwise be absorbed as lazy
            // paragraph text — break the item instead so a new list can start.
            if leading_indent_columns(lines[cursor].text) < marker.content_indent
                && !same_list_marker_line(lines[cursor].text, first_marker)
                && list_marker_info(lines[cursor].text).is_some()
            {
                break;
            }

            if leading_indent_columns(lines[cursor].text) < marker.content_indent {
                if likely_block_start(lines[cursor].text, options) || !paragraph_open {
                    break;
                }
            }

            // A line dedented below the item's content start only stays in the
            // item as a lazy paragraph continuation (it reached here because a
            // paragraph was open). Mark it lazy so the re-parse keeps it as
            // paragraph text rather than letting a stripped `- e`/`> q`/`# h`
            // begin a fresh block inside the item.
            let lazy = paragraph_open
                && leading_indent_columns(lines[cursor].text) < marker.content_indent;
            let stripped = strip_list_continuation(
                lines[cursor].text,
                marker.content_indent,
                first_marker.indent,
            );
            let starts_table = last_content_line.as_deref().is_some_and(|previous| {
                table_can_start_source(
                    previous,
                    &stripped,
                    options.constructs.indented_code,
                    options.constructs.spoiler,
                )
            });
            paragraph_open = if starts_table {
                false
            } else {
                list_item_paragraph_stays_open(Some(paragraph_open), &stripped, options)
            };
            push_line(&mut content, &stripped);
            lazy_flags.push(lazy);
            update_list_item_fence(&stripped, &mut open_fence);
            last_content_line = Some(stripped.into_owned());
            item_end = cursor;
            cursor += 1;
        }

        let child_base = lines[item_start].start + marker.content_indent;
        if !lines[item_end].eol.is_empty() && !ends_with_line_ending(&content) {
            content.push_str(lines[item_end].eol);
        }
        if container_closed_after_unclosed_fence(lines, cursor, item_end, &content, options) {
            content.push('\n');
        }
        let mut child_lines = collect_lines(&content, child_base);
        for (child, &lazy) in child_lines.iter_mut().zip(lazy_flags.iter()) {
            child.lazy = lazy;
        }
        let mut children =
            parse_blocks_from_lines(&child_lines, false, options, definitions, diagnostics);
        let checked = if options.constructs.gfm_task_list_item {
            take_task_marker_from_children(&mut children)
        } else {
            None
        };

        if item_tight
            && blank_separates_top_level_blocks(&item_blank_offsets, &children, child_base)
        {
            item_tight = false;
        }
        tight = tight && item_tight;
        items.push(ListItem {
            meta: NodeMeta::new(Some(Span::new(
                lines[item_start].start,
                lines[item_end].end_with_eol,
            ))),
            checked,
            children,
        });
    }

    Some((
        Block::List(List {
            meta: NodeMeta::new(Some(Span::new(
                lines[index].start,
                lines[cursor - 1].end_with_eol,
            ))),
            ordered: first_marker.ordered,
            start: first_marker.start,
            delimiter: first_marker.delimiter,
            tight,
            children: items,
        }),
        cursor,
    ))
}

/// Whether an item-internal blank line directly separates two of the item's own
/// top-level block children — which loosens the list. A blank loosens the item
/// when some top-level child STARTS after the blank: that child was split off
/// from the preceding content by the blank. A blank with no top-level child
/// starting after it was either trailing or absorbed into a nested container
/// (e.g. a sublist), so it does not loosen the outer list — CommonMark only
/// counts blank lines between blocks the item *directly* contains, and per-list
/// tightness keeps a sublist's internal blank from propagating outward.
///
/// Blank offsets and child spans share the `child_base` content origin (both
/// were produced from the same stripped item content), so the comparison is in
/// one coordinate space.
fn blank_separates_top_level_blocks(
    blank_offsets: &[usize],
    children: &[Block],
    child_base: usize,
) -> bool {
    if blank_offsets.is_empty() || children.len() < 2 {
        return false;
    }
    let Some(&first_blank) = blank_offsets.iter().min() else {
        return false;
    };
    children.iter().any(|child| {
        block_span(child).is_some_and(|span| span.start.saturating_sub(child_base) > first_blank)
    })
}

fn block_span(block: &Block) -> Option<Span> {
    let meta = match block {
        Block::Paragraph(node) => &node.meta,
        Block::Heading(node) => &node.meta,
        Block::ThematicBreak(node) => &node.meta,
        Block::BlockQuote(node) => &node.meta,
        Block::Alert(node) => &node.meta,
        Block::List(node) => &node.meta,
        Block::DescriptionList(node) => &node.meta,
        Block::CodeBlock(node) => &node.meta,
        Block::HtmlBlock(node) => &node.meta,
        Block::HtmlContainer(node) => &node.meta,
        Block::Definition(node) => &node.meta,
        Block::FootnoteDefinition(node) => &node.meta,
        Block::Table(node) => &node.meta,
        Block::MathBlock(node) => &node.meta,
        Block::Frontmatter(node) => &node.meta,
        Block::MdxEsm(node) => &node.meta,
        Block::MdxExpression(node) => &node.meta,
        Block::MdxJsx(node) => &node.meta,
        Block::LeafDirective(node) => &node.meta,
        Block::ContainerDirective(node) => &node.meta,
    };
    meta.span
}

fn list_item_paragraph_stays_open(
    previous_open: Option<bool>,
    line: &str,
    options: &SyntaxOptions,
) -> bool {
    if line.trim().is_empty() {
        return false;
    }
    if previous_open == Some(false) {
        return false;
    }
    block_quote_content_paragraph_open(line, options)
}

fn parse_description_list(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<(Block, usize)> {
    if !options.constructs.description_list || !is_description_term_line(lines[index].text, options)
    {
        return None;
    }

    let mut cursor = index;
    let mut items = Vec::new();
    let mut tight = true;
    let mut list_end = lines[index].end_with_eol;

    while cursor < lines.len() {
        if !is_description_term_line(lines[cursor].text, options) {
            break;
        }
        let Some(term) = description_term(lines, cursor, options) else {
            break;
        };
        let term_line = lines[cursor];
        let mut details = Vec::new();
        let item_start = term_line.start;
        let mut item_end = lines[term.term_end].end_with_eol;
        tight = tight && !term.blank_after_term;
        cursor = term.marker_index;

        loop {
            let Some(marker) = description_marker(lines[cursor].text) else {
                break;
            };
            let (detail, next, detail_tight) = parse_description_details(
                lines,
                cursor,
                marker,
                options,
                definitions,
                diagnostics,
            )?;
            tight = tight && detail_tight;
            item_end = detail
                .meta
                .span
                .map(|span| span.end)
                .unwrap_or(lines[cursor].end_with_eol);
            details.push(detail);
            cursor = next;

            let next_nonblank = next_nonblank_line(lines, cursor);
            if next_nonblank < lines.len()
                && description_marker(lines[next_nonblank].text).is_some()
            {
                if next_nonblank != cursor {
                    tight = false;
                }
                cursor = next_nonblank;
                continue;
            }
            break;
        }

        if details.is_empty() {
            return None;
        }
        list_end = item_end;
        items.push(DescriptionItem {
            meta: NodeMeta::new(Some(Span::new(item_start, item_end))),
            term: parse_inlines(
                &term.source,
                term.source_offset,
                options,
                definitions,
                diagnostics,
            ),
            details,
        });

        let next_item = next_nonblank_line(lines, cursor);
        if next_item >= lines.len() {
            cursor = next_item;
            break;
        }
        if description_term(lines, next_item, options).is_some() {
            if next_item != cursor {
                tight = false;
            }
            cursor = next_item;
            continue;
        }
        cursor = next_item;
        break;
    }

    (!items.is_empty()).then_some((
        Block::DescriptionList(DescriptionList {
            meta: NodeMeta::new(Some(Span::new(lines[index].start, list_end))),
            tight,
            children: items,
        }),
        cursor,
    ))
}

fn parse_description_details(
    lines: &[Line<'_>],
    index: usize,
    marker: DescriptionMarker<'_>,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<(DescriptionDetails, usize, bool)> {
    let mut content = String::new();
    push_line(&mut content, marker.content);
    let mut cursor = index + 1;
    let mut end = lines[index].end_with_eol;
    let mut tight = true;
    let mut paragraph_open = paragraph_stays_open(marker.content, options);

    while cursor < lines.len() {
        if lines[cursor].text.trim().is_empty() {
            let next = next_nonblank_line(lines, cursor + 1);
            // A blank that merely separates this definition from a following
            // `:`/`~` marker (another definition of the SAME term) is
            // content-separating, so it loosens the list. A blank that ends the
            // item — because the next non-blank line begins a new TERM, or the
            // document ends — is just an item boundary and must NOT loosen the
            // list (such blank-separated term groups stay tight).
            if next >= lines.len() || description_term(lines, next, options).is_some() {
                cursor = next;
                break;
            }
            if description_marker(lines[next].text).is_some() {
                tight = false;
                cursor = next;
                break;
            }
            if strip_indent_continuation(lines[next].text).is_none() {
                break;
            }
            push_line(&mut content, "");
            paragraph_open = false;
            tight = false;
            end = lines[cursor].end_with_eol;
            cursor += 1;
            continue;
        }

        if description_marker(lines[cursor].text).is_some()
            || description_term(lines, cursor, options).is_some()
        {
            break;
        }

        let continuation = if let Some(continuation) = strip_indent_continuation(lines[cursor].text)
        {
            continuation
        } else if paragraph_open && !likely_block_start(lines[cursor].text, options) {
            trim_ascii_start(lines[cursor].text)
        } else {
            break;
        };
        paragraph_open = paragraph_stays_open(continuation, options);
        push_line(&mut content, continuation);
        end = lines[cursor].end_with_eol;
        cursor += 1;
    }

    if content.trim().is_empty() {
        return None;
    }

    Some((
        DescriptionDetails {
            meta: NodeMeta::new(Some(Span::new(lines[index].start, end))),
            children: parse_blocks(
                &content,
                lines[index].start + marker.content_offset,
                false,
                options,
                definitions,
                diagnostics,
            ),
        },
        cursor,
        tight,
    ))
}

fn description_term(
    lines: &[Line<'_>],
    term_index: usize,
    options: &SyntaxOptions,
) -> Option<DescriptionTerm> {
    if term_index >= lines.len() || !is_description_term_line(lines[term_index].text, options) {
        return None;
    }
    let mut source = String::new();
    let mut term_end = term_index;
    let mut cursor = term_index;
    while cursor < lines.len() && is_description_term_line(lines[cursor].text, options) {
        if !source.is_empty() {
            source.push('\n');
        }
        source.push_str(trim_ascii_start(lines[cursor].text).trim_end());
        term_end = cursor;
        cursor += 1;
    }

    let mut marker_index = cursor;
    let mut blank_after_term = false;
    while marker_index < lines.len() && lines[marker_index].text.trim().is_empty() {
        blank_after_term = true;
        marker_index += 1;
    }
    (marker_index < lines.len() && description_marker(lines[marker_index].text).is_some()).then(
        || DescriptionTerm {
            marker_index,
            term_end,
            blank_after_term,
            source,
            source_offset: lines[term_index].start + leading_trim_bytes(lines[term_index].text),
        },
    )
}

fn is_description_term_line(line: &str, options: &SyntaxOptions) -> bool {
    leading_indent_columns(line) <= 3
        && !line.trim().is_empty()
        && description_marker(line).is_none()
        && !likely_block_start(line, options)
}

fn description_marker(line: &str) -> Option<DescriptionMarker<'_>> {
    let (columns, bytes) = leading_indent(line);
    if columns > 2 || !matches!(line.as_bytes().get(bytes), Some(b':' | b'~')) {
        return None;
    }
    if line
        .as_bytes()
        .get(bytes + 1)
        .is_some_and(|byte| !matches!(*byte, b' ' | b'\t'))
    {
        return None;
    }
    let mut content_offset = bytes + 1;
    while line
        .as_bytes()
        .get(content_offset)
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
    {
        content_offset += 1;
    }
    Some(DescriptionMarker {
        content_offset,
        content: &line[content_offset..],
    })
}

/// A paragraph inside an indent-continuation container (footnote/description
/// detail) keeps absorbing the next line as long as it is non-blank and does
/// not itself begin a new block.
fn paragraph_stays_open(line: &str, options: &SyntaxOptions) -> bool {
    !line.trim().is_empty() && !likely_block_start(line, options)
}

/// Strips one level of indent-continuation (four spaces or a tab) from a line.
fn strip_indent_continuation(input: &str) -> Option<&str> {
    input
        .strip_prefix("    ")
        .or_else(|| input.strip_prefix('\t'))
}

fn parse_atx_heading(
    line: Line<'_>,
    options: &SyntaxOptions,
    definitions: &[String],
) -> Option<Block> {
    let text = trim_up_to_three_spaces(line.text)?;
    let depth = text
        .as_bytes()
        .iter()
        .take_while(|byte| **byte == b'#')
        .count();
    if depth == 0 || depth > 6 {
        return None;
    }
    if text
        .as_bytes()
        .get(depth)
        .is_some_and(|byte| !matches!(*byte, b' ' | b'\t'))
        && text.len() != depth
    {
        return None;
    }
    let after_opening = &text[depth..];
    let content_start_in_text = depth + leading_trim_bytes(after_opening);
    let content = trim_closing_hashes(after_opening.trim_start());
    let content_start = line.start + (line.text.len() - text.len()) + content_start_in_text;
    Some(Block::Heading(Heading {
        meta: NodeMeta::new(Some(Span::new(line.start, line.end))),
        depth: depth as u8,
        kind: HeadingKind::Atx,
        children: parse_inlines(
            content,
            content_start,
            options,
            definitions,
            &mut Vec::new(),
        ),
    }))
}

fn parse_thematic_break(line: Line<'_>) -> Option<Block> {
    let text = trim_up_to_three_spaces(line.text)?.trim();
    let mut marker = None;
    let mut count = 0;
    for char in text.chars() {
        if char == ' ' || char == '\t' {
            continue;
        }
        let current = match char {
            '-' => ThematicBreakMarker::Dash,
            '*' => ThematicBreakMarker::Asterisk,
            '_' => ThematicBreakMarker::Underscore,
            _ => return None,
        };
        if marker.is_some_and(|marker| marker != current) {
            return None;
        }
        marker = Some(current);
        count += 1;
    }
    if count >= 3 {
        Some(Block::ThematicBreak(ThematicBreak {
            meta: NodeMeta::new(Some(Span::new(line.start, line.end))),
            marker: marker?,
        }))
    } else {
        None
    }
}

fn parse_definition(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
    allow_subsequent_indent: bool,
) -> Option<(Block, usize)> {
    let line = lines[index];
    let text = trim_definition_start(line.text, allow_subsequent_indent)?;
    if !text.starts_with('[') {
        return None;
    }

    // A reference-definition label may span several lines (CommonMark §4.7): the
    // `]:` closing the label can appear on a later line. Accumulate continuation
    // lines until the label closes, stopping at a blank line or end of input (a
    // blank line cannot occur inside a label). The first line's <=3-space indent
    // is already stripped by `trim_up_to_three_spaces`; continuation lines are
    // appended verbatim, and `normalize_label` collapses the interior newlines and
    // surrounding whitespace when the label is matched.
    let mut accumulated = String::from(text);
    let mut label_end_line = index;
    let close = loop {
        if let Some(close) = find_reference_label_end(&accumulated, 0) {
            if accumulated.as_bytes().get(close + 1) == Some(&b':') {
                break close;
            }
            // A closed label not followed by `:` is not a definition.
            return None;
        }
        let next = label_end_line + 1;
        if next >= lines.len() || lines[next].text.trim().is_empty() {
            return None;
        }
        // The unclosed label behaves like an open paragraph: a continuation line
        // that itself begins a block construct (a setext underline, or a GFM table
        // header/delimiter pair) interrupts it, so the definition fails and the
        // lines are re-parsed as blocks (CommonMark/GFM prefer setext headings,
        // thematic breaks, fenced code, and tables over a label that has not yet
        // closed — e.g. `[\na\n=\n]: b` or `[\na\n:-\n]: b`).
        if likely_block_start(lines[next].text, options)
            || setext_underline_depth(lines[next].text).is_some()
            || table_can_start(lines, next, options)
        {
            return None;
        }
        accumulated.push('\n');
        accumulated.push_str(lines[next].text);
        label_end_line = next;
    };
    let label = String::from(&accumulated[1..close]);
    if normalize_label(&label).is_empty() {
        return None;
    }
    let label = label.as_str();
    let mut source = String::from(&accumulated[close + 2..]);
    let mut cursor = label_end_line;
    let mut best_without_title = None;

    loop {
        if let Some(resource) = parse_definition_destination_title(&source) {
            if resource.title.is_some() {
                return Some((
                    Block::Definition(Definition {
                        meta: NodeMeta::new(Some(Span::new(
                            line.start,
                            lines[cursor].end_with_eol,
                        ))),
                        label: label.into(),
                        identifier: normalize_label(label),
                        destination: resource.destination,
                        destination_kind: resource.destination_kind,
                        title: resource.title,
                        title_kind: resource.title_kind,
                    }),
                    cursor + 1,
                ));
            }

            best_without_title = Some((resource, cursor + 1));
            let next = cursor + 1;
            if next >= lines.len()
                || lines[next].text.trim().is_empty()
                || !line_can_start_definition_title(lines[next].text)
            {
                break;
            }
        }

        let next = cursor + 1;
        if next >= lines.len() || lines[next].text.trim().is_empty() {
            break;
        }
        // A continuation line that itself begins a block-level construct (or a
        // setext underline) cannot be swallowed into the definition's pending,
        // not-yet-closed title: such a line interrupts the would-be paragraph, so
        // the definition fails and the lines are re-parsed as blocks (e.g.
        // `[a]: b '` then `***` is a paragraph + thematic break, not a title).
        if likely_block_start(lines[next].text, options)
            || setext_underline_depth(lines[next].text).is_some()
        {
            break;
        }
        source.push('\n');
        source.push_str(lines[next].text);
        cursor = next;
    }

    let (resource, next) = best_without_title?;
    let end = lines[next - 1].end_with_eol;
    Some((
        Block::Definition(Definition {
            meta: NodeMeta::new(Some(Span::new(line.start, end))),
            label: label.into(),
            identifier: normalize_label(label),
            destination: resource.destination,
            destination_kind: resource.destination_kind,
            title: resource.title,
            title_kind: resource.title_kind,
        }),
        next,
    ))
}

fn trim_definition_start(input: &str, allow_subsequent_indent: bool) -> Option<&str> {
    if let Some(trimmed) = trim_up_to_three_spaces(input) {
        return Some(trimmed);
    }
    if allow_subsequent_indent {
        let (columns, bytes) = leading_indent(input);
        if columns == 4 {
            return Some(&input[bytes..]);
        }
    }
    None
}

fn parse_footnote_definition(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<(Block, usize)> {
    if !options.constructs.footnote_definition {
        return None;
    }
    let line = lines[index];
    let text = line.text.trim();
    if !text.starts_with("[^") {
        return None;
    }
    let close = find_footnote_definition_label_end(text)?;
    let label = &text[2..close];
    if !is_footnote_label(label) {
        return None;
    }
    let rest = text[close + 2..].trim();
    let mut content = String::new();
    push_line(&mut content, rest);
    let mut cursor = index + 1;
    let mut end = line.end_with_eol;
    let mut paragraph_open = paragraph_stays_open(rest, options);

    while cursor < lines.len() {
        if lines[cursor].text.trim().is_empty() {
            let next = next_nonblank_line(lines, cursor + 1);
            if next >= lines.len() || !is_footnote_continuation(lines[next].text) {
                break;
            }
            push_line(&mut content, "");
            paragraph_open = false;
            end = lines[cursor].end_with_eol;
            cursor += 1;
            continue;
        }

        let continuation = if let Some(continuation) = strip_indent_continuation(lines[cursor].text)
        {
            continuation
        } else if paragraph_open && !likely_block_start(lines[cursor].text, options) {
            trim_ascii_start(lines[cursor].text)
        } else {
            break;
        };
        paragraph_open = paragraph_stays_open(continuation, options);
        push_line(&mut content, continuation);
        end = lines[cursor].end_with_eol;
        cursor += 1;
    }

    Some((
        Block::FootnoteDefinition(FootnoteDefinition {
            meta: NodeMeta::new(Some(Span::new(line.start, end))),
            label: label.into(),
            identifier: normalize_label(label),
            children: parse_blocks(
                &content,
                line.end.saturating_sub(rest.len()),
                false,
                options,
                definitions,
                diagnostics,
            ),
        }),
        cursor,
    ))
}

fn is_footnote_continuation(input: &str) -> bool {
    strip_indent_continuation(input).is_some()
}

fn parse_leaf_directive(
    line: Line<'_>,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<Block> {
    if !options.constructs.directive_leaf {
        return None;
    }
    let trimmed = line.text.trim_start();
    if trimmed.starts_with(":::") || !trimmed.starts_with("::") {
        return None;
    }
    let opener_base = line.start + (line.text.len() - trimmed.len()) + 2;
    let Some((name, label_source, attributes, _)) = parse_directive_opener(&trimmed[2..]) else {
        diagnostics.push(Diagnostic::new(
            DiagnosticSeverity::Error,
            DiagnosticCode::InvalidDirectiveName,
            Span::new(line.start, line.end),
            "leaf directive must have a valid name",
        ));
        return None;
    };
    let label = label_source
        .map(|source| {
            parse_inlines(
                source,
                opener_base + name.len() + 1,
                options,
                definitions,
                diagnostics,
            )
        })
        .unwrap_or_default();
    Some(Block::LeafDirective(LeafDirective {
        meta: NodeMeta::new(Some(Span::new(line.start, line.end))),
        name,
        label,
        attributes,
    }))
}

fn parse_html_container(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<(Block, usize)> {
    if !options.constructs.html_container {
        return None;
    }

    let (opening, leading_summary) =
        parse_details_container_opening_line(lines[index], options, definitions, diagnostics)?;
    let close_index = find_html_container_close(lines, index + 1, "details")?;
    let closing =
        parse_html_container_tag_line(lines[close_index], "details", HtmlContainerTag::Closing)?;
    let children = parse_details_container_children(
        &lines[index + 1..close_index],
        leading_summary,
        options,
        definitions,
        diagnostics,
    );

    Some((
        Block::HtmlContainer(HtmlContainer {
            meta: NodeMeta::new(Some(Span::new(
                opening
                    .meta
                    .span
                    .map(|span| span.start)
                    .unwrap_or(lines[index].start),
                lines[close_index].end_with_eol,
            ))),
            opening,
            content: HtmlContainerContent::Blocks(children),
            closing,
        }),
        close_index + 1,
    ))
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum HtmlContainerTag {
    Opening,
    Closing,
}

#[derive(Clone, Copy)]
struct HtmlContainerFence {
    marker: FenceMarker,
    length: usize,
}

fn parse_details_container_children(
    lines: &[Line<'_>],
    leading_summary: Option<Block>,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Vec<Block> {
    let mut children = Vec::new();
    if let Some(summary) = leading_summary {
        children.push(summary);
        children.extend(parse_blocks_from_lines(
            lines,
            false,
            options,
            definitions,
            diagnostics,
        ));
        return children;
    }

    let Some(summary_index) = lines.iter().position(|line| !line.text.trim().is_empty()) else {
        return children;
    };

    if let Some((summary, next)) =
        parse_summary_container(lines, summary_index, options, definitions, diagnostics)
    {
        children.push(summary);
        children.extend(parse_blocks_from_lines(
            &lines[next..],
            false,
            options,
            definitions,
            diagnostics,
        ));
        return children;
    }

    parse_blocks_from_lines(lines, false, options, definitions, diagnostics)
}

fn parse_summary_container(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<(Block, usize)> {
    let line = lines[index];
    let (trimmed, indent_bytes) = trim_html_container_line(line.text)?;
    parse_summary_container_source(
        trimmed,
        line.start + indent_bytes,
        options,
        definitions,
        diagnostics,
    )
    .map(|block| (block, index + 1))
}

fn parse_details_container_opening_line(
    line: Line<'_>,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<(HtmlTag, Option<Block>)> {
    let (trimmed, indent_bytes) = trim_html_container_line(line.text)?;
    let (open_end, name) = parse_html_tag(trimmed, 0)?;
    if !name.eq_ignore_ascii_case("details")
        || html_tag_is_closing(trimmed, 0)
        || html_tag_is_self_closing(&trimmed[..open_end])
    {
        return None;
    }

    let tag_start = line.start + indent_bytes;
    let rest_start = open_end + leading_ascii_whitespace_len(&trimmed[open_end..]);
    let summary = if trimmed[rest_start..].is_empty() {
        None
    } else {
        Some(parse_summary_container_source(
            &trimmed[rest_start..],
            tag_start + rest_start,
            options,
            definitions,
            diagnostics,
        )?)
    };

    Some((
        HtmlTag {
            meta: NodeMeta::new(Some(Span::new(tag_start, tag_start + open_end))),
            name: "details".into(),
            raw: trimmed[..open_end].into(),
        },
        summary,
    ))
}

fn parse_summary_container_source(
    source: &str,
    base_offset: usize,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<Block> {
    let (open_end, open_name) = parse_html_tag(source, 0)?;
    if !open_name.eq_ignore_ascii_case("summary")
        || html_tag_is_closing(source, 0)
        || html_tag_is_self_closing(&source[..open_end])
    {
        return None;
    }

    let close_start = source[open_end..]
        .find("</")
        .map(|offset| open_end + offset)?;
    let (close_end, close_name) = parse_html_tag(source, close_start)?;
    if !close_name.eq_ignore_ascii_case("summary")
        || !html_tag_is_closing(source, close_start)
        || !source[close_end..].trim().is_empty()
    {
        return None;
    }

    let content = &source[open_end..close_start];
    let content_base = base_offset + open_end;
    let opening = HtmlTag {
        meta: NodeMeta::new(Some(Span::new(base_offset, base_offset + open_end))),
        name: "summary".into(),
        raw: source[..open_end].into(),
    };
    let closing = HtmlTag {
        meta: NodeMeta::new(Some(Span::new(
            base_offset + close_start,
            base_offset + close_end,
        ))),
        name: "summary".into(),
        raw: source[close_start..close_end].into(),
    };

    Some(Block::HtmlContainer(HtmlContainer {
        meta: NodeMeta::new(Some(Span::new(base_offset, base_offset + source.len()))),
        opening,
        content: HtmlContainerContent::Inlines(parse_inlines(
            content,
            content_base,
            options,
            definitions,
            diagnostics,
        )),
        closing,
    }))
}

fn find_html_container_close(lines: &[Line<'_>], mut cursor: usize, tag: &str) -> Option<usize> {
    let mut depth = 1usize;
    let mut fence = None;

    while cursor < lines.len() {
        let line = lines[cursor].text;
        if let Some(open_fence) = fence {
            if html_container_fence_closes(line, open_fence) {
                fence = None;
            }
            cursor += 1;
            continue;
        }

        if let Some(open_fence) = html_container_fence_opens(line) {
            fence = Some(open_fence);
            cursor += 1;
            continue;
        }

        if parse_html_container_tag_line(lines[cursor], tag, HtmlContainerTag::Closing).is_some() {
            depth -= 1;
            if depth == 0 {
                return Some(cursor);
            }
        } else if parse_html_container_opening_line(lines[cursor], tag).is_some() {
            depth += 1;
        }

        cursor += 1;
    }

    None
}

fn parse_html_container_tag_line(
    line: Line<'_>,
    tag: &str,
    kind: HtmlContainerTag,
) -> Option<HtmlTag> {
    let (trimmed, indent_bytes) = trim_html_container_line(line.text)?;
    let (end, name) = parse_html_tag(trimmed, 0)?;
    if !name.eq_ignore_ascii_case(tag) || !trimmed[end..].trim().is_empty() {
        return None;
    }

    let closing = html_tag_is_closing(trimmed, 0);
    if (kind == HtmlContainerTag::Opening && closing)
        || (kind == HtmlContainerTag::Closing && !closing)
        || html_tag_is_self_closing(&trimmed[..end])
    {
        return None;
    }

    let start = line.start + indent_bytes;
    Some(HtmlTag {
        meta: NodeMeta::new(Some(Span::new(start, start + end))),
        name: tag.into(),
        raw: trimmed[..end].into(),
    })
}

fn parse_html_container_opening_line(line: Line<'_>, tag: &str) -> Option<HtmlTag> {
    let (trimmed, indent_bytes) = trim_html_container_line(line.text)?;
    let (end, name) = parse_html_tag(trimmed, 0)?;
    if !name.eq_ignore_ascii_case(tag)
        || html_tag_is_closing(trimmed, 0)
        || html_tag_is_self_closing(&trimmed[..end])
    {
        return None;
    }

    let rest_start = end + leading_ascii_whitespace_len(&trimmed[end..]);
    if !trimmed[rest_start..].is_empty() && !html_container_line_has_summary(&trimmed[rest_start..])
    {
        return None;
    }

    let start = line.start + indent_bytes;
    Some(HtmlTag {
        meta: NodeMeta::new(Some(Span::new(start, start + end))),
        name: tag.into(),
        raw: trimmed[..end].into(),
    })
}

fn html_container_line_has_summary(input: &str) -> bool {
    let Some((open_end, name)) = parse_html_tag(input, 0) else {
        return false;
    };
    if !name.eq_ignore_ascii_case("summary")
        || html_tag_is_closing(input, 0)
        || html_tag_is_self_closing(&input[..open_end])
    {
        return false;
    }
    let Some(close_start) = input[open_end..].find("</").map(|offset| open_end + offset) else {
        return false;
    };
    let Some((close_end, close_name)) = parse_html_tag(input, close_start) else {
        return false;
    };
    close_name.eq_ignore_ascii_case("summary")
        && html_tag_is_closing(input, close_start)
        && input[close_end..].trim().is_empty()
}

fn trim_html_container_line(input: &str) -> Option<(&str, usize)> {
    let trimmed = trim_up_to_three_spaces(input)?;
    Some((trimmed, input.len() - trimmed.len()))
}

fn leading_ascii_whitespace_len(input: &str) -> usize {
    input
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_whitespace())
        .count()
}

fn html_tag_is_closing(input: &str, index: usize) -> bool {
    input.as_bytes().get(index + 1) == Some(&b'/')
}

fn html_tag_is_self_closing(input: &str) -> bool {
    input.trim_end().ends_with("/>")
}

fn html_container_fence_opens(line: &str) -> Option<HtmlContainerFence> {
    let trimmed = trim_up_to_three_spaces(line)?;
    let (marker, length) = fence_start(trimmed)?;
    Some(HtmlContainerFence { marker, length })
}

fn html_container_fence_closes(line: &str, fence: HtmlContainerFence) -> bool {
    trim_up_to_three_spaces(line)
        .is_some_and(|trimmed| fence_close(trimmed, fence.marker, fence.length))
}

fn parse_html_block(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
) -> Option<(Block, usize)> {
    if !options.constructs.html_block {
        return None;
    }

    let trimmed = trim_up_to_three_spaces(lines[index].text)?;
    let kind = html_block_start(trimmed)?;
    let mut value = String::new();
    let mut cursor = index;
    match kind {
        HtmlBlockKind::RawTag => {
            // CommonMark §4.6 type-1: the block ends on a line containing ANY of
            // `</script>`, `</pre>`, `</style>`, `</textarea>` (case-insensitive),
            // regardless of which opened it.
            while cursor < lines.len() {
                push_line(&mut value, lines[cursor].text);
                if ["script", "pre", "style", "textarea"]
                    .iter()
                    .any(|tag| line_contains_raw_closing_tag(lines[cursor].text, tag))
                {
                    cursor += 1;
                    break;
                }
                cursor += 1;
            }
        }
        HtmlBlockKind::BlockTag => {
            while cursor < lines.len() && !lines[cursor].text.trim().is_empty() {
                push_line(&mut value, lines[cursor].text);
                cursor += 1;
            }
        }
        HtmlBlockKind::Until(end) => {
            while cursor < lines.len() {
                push_line(&mut value, lines[cursor].text);
                if lines[cursor].text.contains(end) {
                    cursor += 1;
                    break;
                }
                cursor += 1;
            }
        }
        HtmlBlockKind::UntilBlank => {
            while cursor < lines.len() && !lines[cursor].text.trim().is_empty() {
                push_line(&mut value, lines[cursor].text);
                cursor += 1;
            }
        }
    }
    Some((
        Block::HtmlBlock(HtmlBlock {
            meta: NodeMeta::new(Some(Span::new(
                lines[index].start,
                lines[cursor - 1].end_with_eol,
            ))),
            value,
        }),
        cursor,
    ))
}

fn html_block_start(input: &str) -> Option<HtmlBlockKind> {
    let trimmed = input.trim_end();
    if !trimmed.starts_with('<') {
        return None;
    }

    if raw_html_tag_start(trimmed) {
        return Some(HtmlBlockKind::RawTag);
    }
    if trimmed.starts_with("<!--") {
        return Some(HtmlBlockKind::Until("-->"));
    }
    if trimmed.starts_with("<?") {
        return Some(HtmlBlockKind::Until("?>"));
    }
    if is_declaration_start(trimmed) {
        return Some(HtmlBlockKind::Until(">"));
    }
    if trimmed.starts_with("<![CDATA[") {
        return Some(HtmlBlockKind::Until("]]>"));
    }

    if html_block_tag_start(trimmed) {
        return Some(HtmlBlockKind::BlockTag);
    }

    let Some((end, _tag_name)) = parse_html_tag(trimmed, 0) else {
        return None;
    };
    let rest = trimmed[end..].trim();
    if rest.is_empty() {
        Some(HtmlBlockKind::UntilBlank)
    } else {
        None
    }
}

pub(crate) fn line_starts_html_block(input: &str) -> bool {
    trim_up_to_three_spaces(input)
        .and_then(html_block_start)
        .is_some()
}

fn line_starts_html_container(input: &str) -> bool {
    let line = Line {
        text: input,
        eol: "",
        start: 0,
        end: input.len(),
        end_with_eol: input.len(),
        lazy: false,
    };
    parse_html_container_opening_line(line, "details").is_some()
}

fn raw_html_tag_start(input: &str) -> bool {
    for tag in ["script", "pre", "style", "textarea"] {
        if html_raw_open_tag_prefix(input, tag) {
            return true;
        }
    }
    false
}

fn html_raw_open_tag_prefix(input: &str, tag: &str) -> bool {
    let Some(rest) = input.strip_prefix('<') else {
        return false;
    };
    if rest.starts_with('/') || rest.len() < tag.len() {
        return false;
    }
    let rest_bytes = rest.as_bytes();
    let tag_bytes = tag.as_bytes();
    if !rest_bytes
        .get(..tag_bytes.len())
        .is_some_and(|name| name.eq_ignore_ascii_case(tag_bytes))
    {
        return false;
    }
    match rest_bytes.get(tag.len()) {
        None => true,
        Some(b' ' | b'\t' | b'\n' | b'\r' | b'>') => true,
        Some(b'/') => {
            rest_bytes.get(tag.len() + 1) == Some(&b'>') && rest_bytes.get(tag.len() + 2).is_none()
        }
        _ => false,
    }
}

fn line_contains_raw_closing_tag(input: &str, tag: &str) -> bool {
    let bytes = input.as_bytes();
    let tag_bytes = tag.as_bytes();
    let mut cursor = 0;

    while cursor + 2 + tag_bytes.len() <= bytes.len() {
        let tag_start = cursor + 2;
        let tag_end = tag_start + tag_bytes.len();
        if bytes.get(cursor) == Some(&b'<')
            && bytes.get(cursor + 1) == Some(&b'/')
            && bytes
                .get(tag_start..tag_end)
                .is_some_and(|name| name.eq_ignore_ascii_case(tag_bytes))
        {
            match bytes.get(tag_end) {
                Some(b'>') => return true,
                Some(byte) if byte.is_ascii_whitespace() => {
                    let mut after_space = tag_end;
                    while bytes
                        .get(after_space)
                        .is_some_and(|byte| byte.is_ascii_whitespace())
                    {
                        after_space += 1;
                    }
                    if bytes.get(after_space) == Some(&b'>') {
                        return true;
                    }
                }
                _ => {}
            }
        }
        cursor += 1;
    }

    false
}

fn html_block_tag_start(input: &str) -> bool {
    let bytes = input.as_bytes();
    if bytes.first() != Some(&b'<') {
        return false;
    }

    let mut cursor = 1;
    if bytes.get(cursor) == Some(&b'/') {
        cursor += 1;
    }

    let name_start = cursor;
    if !bytes
        .get(cursor)
        .is_some_and(|byte| byte.is_ascii_alphabetic())
    {
        return false;
    }
    cursor += 1;
    while bytes.get(cursor).is_some_and(|byte| html_name_byte(*byte)) {
        cursor += 1;
    }

    let name = &input[name_start..cursor];
    if !html_block_tag(name) {
        return false;
    }

    match bytes.get(cursor) {
        None | Some(b' ' | b'\t' | b'\n' | b'\r' | b'>') => true,
        Some(b'/') if bytes.get(cursor + 1) == Some(&b'>') => true,
        _ => false,
    }
}

fn html_block_tag(tag: &str) -> bool {
    matches!(
        tag.to_ascii_lowercase().as_str(),
        "address"
            | "article"
            | "aside"
            | "base"
            | "basefont"
            | "blockquote"
            | "body"
            | "caption"
            | "center"
            | "col"
            | "colgroup"
            | "dd"
            | "details"
            | "dialog"
            | "dir"
            | "div"
            | "dl"
            | "dt"
            | "fieldset"
            | "figcaption"
            | "figure"
            | "footer"
            | "form"
            | "frame"
            | "frameset"
            | "h1"
            | "h2"
            | "h3"
            | "h4"
            | "h5"
            | "h6"
            | "head"
            | "header"
            | "hr"
            | "html"
            | "iframe"
            | "legend"
            | "li"
            | "link"
            | "main"
            | "menu"
            | "menuitem"
            | "nav"
            | "noframes"
            | "ol"
            | "optgroup"
            | "option"
            | "p"
            | "param"
            | "search"
            | "section"
            | "summary"
            | "table"
            | "tbody"
            | "td"
            | "tfoot"
            | "th"
            | "thead"
            | "title"
            | "tr"
            | "track"
            | "ul"
    )
}

fn is_declaration_start(input: &str) -> bool {
    input
        .as_bytes()
        .get(2)
        .is_some_and(|byte| input.starts_with("<!") && byte.is_ascii_alphabetic())
}

fn parse_mdx_flow(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<(Block, usize)> {
    if options.constructs.mdx_esm {
        if let Some((block, next)) = parse_mdx_esm_flow(lines, index, diagnostics) {
            return Some((block, next));
        }
    }

    let line = lines[index];
    let trimmed = line.text.trim_start();
    if options.constructs.mdx_expression_block && trimmed.starts_with('{') {
        let open_byte = line.text.len() - trimmed.len();
        if let Some((close_line, close_byte)) = find_mdx_expression_close(lines, index, open_byte) {
            return Some((
                Block::MdxExpression(MdxExpression {
                    meta: NodeMeta::new(Some(Span::new(line.start, lines[close_line].end))),
                    value: collect_mdx_expression_value(
                        lines, index, open_byte, close_line, close_byte,
                    ),
                }),
                close_line + 1,
            ));
        }
        diagnostics.push(Diagnostic::new(
            DiagnosticSeverity::Error,
            DiagnosticCode::InvalidMdx,
            Span::new(line.start + open_byte, lines.last()?.end_with_eol),
            "MDX expression block is missing a closing brace",
        ));
    }
    if options.constructs.mdx_jsx_block && trimmed.starts_with('<') {
        if let Some(close_line) = find_mdx_jsx_close(lines, index) {
            return Some((
                Block::MdxJsx(MdxJsx {
                    meta: NodeMeta::new(Some(Span::new(line.start, lines[close_line].end))),
                    value: collect_line_range(lines, index, close_line),
                }),
                close_line + 1,
            ));
        }
        let start_byte = line.text.len() - trimmed.len();
        if let Some(root) = mdx_jsx_tag_start(line.text, start_byte) {
            if !root.closing {
                if let Some((_tag_end_line, _tag_end_byte, self_closing)) =
                    find_mdx_jsx_tag_end(lines, index, start_byte)
                {
                    if !self_closing {
                        diagnostics.push(Diagnostic::new(
                            DiagnosticSeverity::Error,
                            DiagnosticCode::InvalidMdx,
                            Span::new(line.start + start_byte, lines.last()?.end_with_eol),
                            "MDX JSX block is missing a closing tag",
                        ));
                    }
                }
            }
        }
    }
    None
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct MdxEsmState {
    brace_depth: usize,
    bracket_depth: usize,
    paren_depth: usize,
    block_comment: bool,
    quote: Option<u8>,
    escaped: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MdxBraceState {
    Normal,
    SingleQuoted,
    DoubleQuoted,
    Template,
    LineComment,
    BlockComment,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MdxJsxTag<'a> {
    Fragment,
    Named(&'a str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MdxJsxTagStart<'a> {
    tag: MdxJsxTag<'a>,
    closing: bool,
}

fn parse_mdx_esm_flow(
    lines: &[Line<'_>],
    index: usize,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<(Block, usize)> {
    if !is_mdx_esm_start(lines[index].text) {
        return None;
    }

    let mut value = String::new();
    let mut state = MdxEsmState::default();
    let mut cursor = index;
    while cursor < lines.len() {
        let line = lines[cursor].text;
        if cursor > index && !is_mdx_esm_continuation(line, &state) {
            break;
        }
        if cursor > index {
            value.push('\n');
        }
        value.push_str(line);
        update_mdx_esm_state(line, &mut state);
        cursor += 1;
    }
    if cursor >= lines.len() && state_has_open_mdx_esm_construct(&state) {
        diagnostics.push(Diagnostic::new(
            DiagnosticSeverity::Error,
            DiagnosticCode::InvalidMdx,
            Span::new(lines[index].start, lines[cursor - 1].end_with_eol),
            "MDX ESM block is missing a closing delimiter",
        ));
    }

    Some((
        Block::MdxEsm(MdxEsm {
            meta: NodeMeta::new(Some(Span::new(lines[index].start, lines[cursor - 1].end))),
            value,
        }),
        cursor,
    ))
}

fn is_mdx_esm_start(line: &str) -> bool {
    line.starts_with("import ") || line.starts_with("export ")
}

fn is_mdx_esm_continuation(line: &str, state: &MdxEsmState) -> bool {
    if state_has_open_mdx_esm_construct(state) {
        return true;
    }
    let trimmed = line.trim_start();
    if trimmed.is_empty() {
        return false;
    }
    is_mdx_esm_start(line) || trimmed.starts_with("//") || trimmed.starts_with("/*")
}

fn state_has_open_mdx_esm_construct(state: &MdxEsmState) -> bool {
    state.brace_depth > 0
        || state.bracket_depth > 0
        || state.paren_depth > 0
        || state.block_comment
        || state.quote == Some(b'`')
}

fn update_mdx_esm_state(line: &str, state: &mut MdxEsmState) {
    let bytes = line.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if state.block_comment {
            if byte == b'*' && bytes.get(index + 1) == Some(&b'/') {
                state.block_comment = false;
                index += 1;
            }
            index += 1;
            continue;
        }

        if let Some(delimiter) = state.quote {
            if state.escaped {
                state.escaped = false;
            } else if byte == b'\\' {
                state.escaped = true;
            } else if byte == delimiter {
                state.quote = None;
            }
            index += 1;
            continue;
        }

        match byte {
            b'\'' | b'"' | b'`' => state.quote = Some(byte),
            b'/' if bytes.get(index + 1) == Some(&b'/') => break,
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                state.block_comment = true;
                index += 1;
            }
            b'{' => state.brace_depth += 1,
            b'}' => state.brace_depth = state.brace_depth.saturating_sub(1),
            b'[' => state.bracket_depth += 1,
            b']' => state.bracket_depth = state.bracket_depth.saturating_sub(1),
            b'(' => state.paren_depth += 1,
            b')' => state.paren_depth = state.paren_depth.saturating_sub(1),
            _ => {}
        }
        index += 1;
    }
}

fn find_mdx_expression_close(
    lines: &[Line<'_>],
    index: usize,
    open_byte: usize,
) -> Option<(usize, usize)> {
    let mut depth = 0usize;
    let mut state = MdxBraceState::Normal;
    let mut escaped = false;
    let mut cursor = index;

    while cursor < lines.len() {
        let bytes = lines[cursor].text.as_bytes();
        let mut byte_index = if cursor == index { open_byte } else { 0 };
        while byte_index < bytes.len() {
            let byte = bytes[byte_index];
            match state {
                MdxBraceState::Normal => match byte {
                    b'\'' => state = MdxBraceState::SingleQuoted,
                    b'"' => state = MdxBraceState::DoubleQuoted,
                    b'`' => state = MdxBraceState::Template,
                    b'/' if bytes.get(byte_index + 1) == Some(&b'/') => {
                        state = MdxBraceState::LineComment;
                        break;
                    }
                    b'/' if bytes.get(byte_index + 1) == Some(&b'*') => {
                        state = MdxBraceState::BlockComment;
                        byte_index += 1;
                    }
                    b'{' => depth += 1,
                    b'}' => {
                        depth = depth.checked_sub(1)?;
                        if depth == 0 {
                            return lines[cursor].text[byte_index + 1..]
                                .trim()
                                .is_empty()
                                .then_some((cursor, byte_index));
                        }
                    }
                    _ => {}
                },
                MdxBraceState::SingleQuoted => {
                    update_mdx_quote_state(byte, b'\'', &mut state, &mut escaped);
                }
                MdxBraceState::DoubleQuoted => {
                    update_mdx_quote_state(byte, b'"', &mut state, &mut escaped);
                }
                MdxBraceState::Template => {
                    update_mdx_quote_state(byte, b'`', &mut state, &mut escaped);
                }
                MdxBraceState::LineComment => break,
                MdxBraceState::BlockComment => {
                    if byte == b'*' && bytes.get(byte_index + 1) == Some(&b'/') {
                        state = MdxBraceState::Normal;
                        byte_index += 1;
                    }
                }
            }
            byte_index += 1;
        }
        if state == MdxBraceState::LineComment {
            state = MdxBraceState::Normal;
        }
        cursor += 1;
    }

    None
}

fn update_mdx_quote_state(byte: u8, delimiter: u8, state: &mut MdxBraceState, escaped: &mut bool) {
    if *escaped {
        *escaped = false;
        return;
    }
    if byte == b'\\' {
        *escaped = true;
        return;
    }
    if byte == delimiter {
        *state = MdxBraceState::Normal;
    }
}

fn find_mdx_expression_inline_close(input: &str, open_byte: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    if bytes.get(open_byte) != Some(&b'{') {
        return None;
    }

    let mut depth = 0usize;
    let mut state = MdxBraceState::Normal;
    let mut escaped = false;
    let mut cursor = open_byte;
    while cursor < bytes.len() {
        let byte = bytes[cursor];
        match state {
            MdxBraceState::Normal => match byte {
                b'\'' => state = MdxBraceState::SingleQuoted,
                b'"' => state = MdxBraceState::DoubleQuoted,
                b'`' => state = MdxBraceState::Template,
                b'/' if bytes.get(cursor + 1) == Some(&b'/') => {
                    state = MdxBraceState::LineComment;
                    cursor += 1;
                }
                b'/' if bytes.get(cursor + 1) == Some(&b'*') => {
                    state = MdxBraceState::BlockComment;
                    cursor += 1;
                }
                b'{' => depth += 1,
                b'}' => {
                    depth = depth.checked_sub(1)?;
                    if depth == 0 {
                        return Some(cursor);
                    }
                }
                _ => {}
            },
            MdxBraceState::SingleQuoted => {
                update_mdx_quote_state(byte, b'\'', &mut state, &mut escaped);
            }
            MdxBraceState::DoubleQuoted => {
                update_mdx_quote_state(byte, b'"', &mut state, &mut escaped);
            }
            MdxBraceState::Template => {
                update_mdx_quote_state(byte, b'`', &mut state, &mut escaped);
            }
            MdxBraceState::LineComment => {
                if byte == b'\n' {
                    state = MdxBraceState::Normal;
                }
            }
            MdxBraceState::BlockComment => {
                if byte == b'*' && bytes.get(cursor + 1) == Some(&b'/') {
                    state = MdxBraceState::Normal;
                    cursor += 1;
                }
            }
        }
        cursor += 1;
    }
    None
}

fn collect_mdx_expression_value(
    lines: &[Line<'_>],
    start_line: usize,
    open_byte: usize,
    close_line: usize,
    close_byte: usize,
) -> String {
    let mut value = String::new();
    let mut cursor = start_line;
    while cursor <= close_line {
        if cursor > start_line {
            value.push('\n');
        }
        let line = lines[cursor].text;
        let segment = if cursor == start_line && cursor == close_line {
            &line[open_byte + 1..close_byte]
        } else if cursor == start_line {
            &line[open_byte + 1..]
        } else if cursor == close_line {
            &line[..close_byte]
        } else {
            line
        };
        value.push_str(segment);
        cursor += 1;
    }
    value
}

fn find_mdx_jsx_close<'a>(lines: &'a [Line<'a>], index: usize) -> Option<usize> {
    let line = lines[index];
    let trimmed = line.text.trim_start();
    let start_byte = line.text.len() - trimmed.len();
    let root = mdx_jsx_tag_start(line.text, start_byte)?;
    if root.closing {
        return None;
    }

    let (mut cursor_line, mut cursor_byte, self_closing) =
        find_mdx_jsx_tag_end(lines, index, start_byte)?;
    if self_closing {
        return Some(cursor_line);
    }

    let mut depth = 1usize;
    cursor_byte += 1;
    'scan: while cursor_line < lines.len() {
        let line = lines[cursor_line].text;
        while cursor_byte < line.len() {
            let Some(relative_start) = line[cursor_byte..].find('<') else {
                break;
            };
            let tag_start_byte = cursor_byte + relative_start;
            let Some(candidate) = mdx_jsx_tag_start(line, tag_start_byte) else {
                cursor_byte = tag_start_byte + 1;
                continue;
            };
            let Some((tag_end_line, tag_end_byte, candidate_self_closing)) =
                find_mdx_jsx_tag_end(lines, cursor_line, tag_start_byte)
            else {
                return None;
            };

            if mdx_jsx_tag_matches(root.tag, candidate.tag) {
                if candidate.closing {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return Some(tag_end_line);
                    }
                } else if !candidate_self_closing {
                    depth += 1;
                }
            }

            cursor_byte = tag_end_byte + 1;
            if tag_end_line != cursor_line {
                cursor_line = tag_end_line;
                continue 'scan;
            }
        }
        cursor_line += 1;
        cursor_byte = 0;
    }
    None
}

fn parse_mdx_jsx_inline(input: &str, index: usize) -> Option<(usize, String)> {
    let root = mdx_jsx_tag_start(input, index)?;
    if root.closing {
        return None;
    }

    let (mut cursor, self_closing) = find_mdx_jsx_tag_end_in_text(input, index)?;
    if self_closing {
        let end = cursor + 1;
        return Some((end, input[index..end].into()));
    }

    let mut depth = 1usize;
    cursor += 1;
    while cursor < input.len() {
        let Some(relative_start) = input[cursor..].find('<') else {
            return None;
        };
        let tag_start_byte = cursor + relative_start;
        let Some(candidate) = mdx_jsx_tag_start(input, tag_start_byte) else {
            cursor = tag_start_byte + 1;
            continue;
        };
        let Some((tag_end, candidate_self_closing)) =
            find_mdx_jsx_tag_end_in_text(input, tag_start_byte)
        else {
            return None;
        };

        if mdx_jsx_tag_matches(root.tag, candidate.tag) {
            if candidate.closing {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    let end = tag_end + 1;
                    return Some((end, input[index..end].into()));
                }
            } else if !candidate_self_closing {
                depth += 1;
            }
        }
        cursor = tag_end + 1;
    }
    None
}

fn mdx_jsx_tag_start(input: &str, start: usize) -> Option<MdxJsxTagStart<'_>> {
    let bytes = input.as_bytes();
    if bytes.get(start) != Some(&b'<') {
        return None;
    }

    match bytes.get(start + 1) {
        Some(b'>') => {
            return Some(MdxJsxTagStart {
                tag: MdxJsxTag::Fragment,
                closing: false,
            });
        }
        Some(b'/') if bytes.get(start + 2) == Some(&b'>') => {
            return Some(MdxJsxTagStart {
                tag: MdxJsxTag::Fragment,
                closing: true,
            });
        }
        Some(b'!' | b'?') | None => return None,
        _ => {}
    }

    let closing = bytes.get(start + 1) == Some(&b'/');
    let name_start = start + if closing { 2 } else { 1 };
    if !bytes
        .get(name_start)
        .is_some_and(|byte| is_mdx_jsx_name_start_byte(*byte))
    {
        return None;
    }

    let mut name_end = name_start + 1;
    while bytes
        .get(name_end)
        .is_some_and(|byte| is_mdx_jsx_name_byte(*byte))
    {
        name_end += 1;
    }
    if name_end == name_start {
        return None;
    }
    if bytes
        .get(name_end)
        .is_some_and(|byte| !is_mdx_jsx_name_delimiter(*byte))
    {
        return None;
    }
    Some(MdxJsxTagStart {
        tag: MdxJsxTag::Named(&input[name_start..name_end]),
        closing,
    })
}

fn mdx_jsx_tag_matches(left: MdxJsxTag<'_>, right: MdxJsxTag<'_>) -> bool {
    match (left, right) {
        (MdxJsxTag::Fragment, MdxJsxTag::Fragment) => true,
        (MdxJsxTag::Named(left), MdxJsxTag::Named(right)) => left == right,
        _ => false,
    }
}

fn find_mdx_jsx_tag_end(
    lines: &[Line<'_>],
    start_line: usize,
    start_byte: usize,
) -> Option<(usize, usize, bool)> {
    let mut line_index = start_line;
    let mut byte_index = start_byte + 1;
    let mut quote = None;
    let mut escaped = false;
    let mut expression_depth = 0usize;
    let mut expression_state = MdxBraceState::Normal;
    let mut expression_escaped = false;

    while line_index < lines.len() {
        let bytes = lines[line_index].text.as_bytes();
        while byte_index < bytes.len() {
            let byte = bytes[byte_index];
            if expression_depth > 0 {
                if update_mdx_jsx_expression_state(
                    byte,
                    bytes.get(byte_index + 1).copied(),
                    &mut expression_depth,
                    &mut expression_state,
                    &mut expression_escaped,
                ) {
                    byte_index += 1;
                }
                byte_index += 1;
                continue;
            }

            if let Some(delimiter) = quote {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == delimiter {
                    quote = None;
                }
                byte_index += 1;
                continue;
            }

            match byte {
                b'\'' | b'"' => quote = Some(byte),
                b'{' => {
                    expression_depth = 1;
                    expression_state = MdxBraceState::Normal;
                    expression_escaped = false;
                }
                b'>' if expression_depth == 0 => {
                    let self_closing =
                        previous_nonspace_before(lines, line_index, byte_index) == Some(b'/');
                    return Some((line_index, byte_index, self_closing));
                }
                _ => {}
            }
            byte_index += 1;
        }
        if expression_state == MdxBraceState::LineComment {
            expression_state = MdxBraceState::Normal;
        }
        line_index += 1;
        byte_index = 0;
    }
    None
}

fn previous_nonspace_before(
    lines: &[Line<'_>],
    line_index: usize,
    byte_index: usize,
) -> Option<u8> {
    let mut cursor_line = line_index;
    let mut cursor_byte = byte_index;

    loop {
        if let Some(byte) = lines[cursor_line].text.as_bytes()[..cursor_byte]
            .iter()
            .rev()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
        {
            return Some(byte);
        }
        if cursor_line == 0 {
            return None;
        }
        cursor_line -= 1;
        cursor_byte = lines[cursor_line].text.len();
    }
}

fn find_mdx_jsx_tag_end_in_text(input: &str, start_byte: usize) -> Option<(usize, bool)> {
    let bytes = input.as_bytes();
    let mut byte_index = start_byte + 1;
    let mut quote = None;
    let mut escaped = false;
    let mut expression_depth = 0usize;
    let mut expression_state = MdxBraceState::Normal;
    let mut expression_escaped = false;

    while byte_index < bytes.len() {
        let byte = bytes[byte_index];
        if expression_depth > 0 {
            if update_mdx_jsx_expression_state(
                byte,
                bytes.get(byte_index + 1).copied(),
                &mut expression_depth,
                &mut expression_state,
                &mut expression_escaped,
            ) {
                byte_index += 1;
            }
            byte_index += 1;
            continue;
        }

        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == delimiter {
                quote = None;
            }
            byte_index += 1;
            continue;
        }

        match byte {
            b'\'' | b'"' => quote = Some(byte),
            b'{' => {
                expression_depth = 1;
                expression_state = MdxBraceState::Normal;
                expression_escaped = false;
            }
            b'>' if expression_depth == 0 => {
                let self_closing = previous_nonspace_before_text(input, byte_index) == Some(b'/');
                return Some((byte_index, self_closing));
            }
            _ => {}
        }
        byte_index += 1;
    }
    None
}

fn previous_nonspace_before_text(input: &str, byte_index: usize) -> Option<u8> {
    input.as_bytes()[..byte_index]
        .iter()
        .rev()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
}

fn update_mdx_jsx_expression_state(
    byte: u8,
    next: Option<u8>,
    depth: &mut usize,
    state: &mut MdxBraceState,
    escaped: &mut bool,
) -> bool {
    match *state {
        MdxBraceState::Normal => match byte {
            b'\'' => *state = MdxBraceState::SingleQuoted,
            b'"' => *state = MdxBraceState::DoubleQuoted,
            b'`' => *state = MdxBraceState::Template,
            b'/' if next == Some(b'/') => {
                *state = MdxBraceState::LineComment;
                return true;
            }
            b'/' if next == Some(b'*') => {
                *state = MdxBraceState::BlockComment;
                return true;
            }
            b'{' => *depth += 1,
            b'}' => {
                *depth = (*depth).saturating_sub(1);
                if *depth == 0 {
                    *state = MdxBraceState::Normal;
                    *escaped = false;
                }
            }
            _ => {}
        },
        MdxBraceState::SingleQuoted => {
            update_mdx_quote_state(byte, b'\'', state, escaped);
        }
        MdxBraceState::DoubleQuoted => {
            update_mdx_quote_state(byte, b'"', state, escaped);
        }
        MdxBraceState::Template => {
            update_mdx_quote_state(byte, b'`', state, escaped);
        }
        MdxBraceState::LineComment => {
            if byte == b'\n' {
                *state = MdxBraceState::Normal;
            }
        }
        MdxBraceState::BlockComment => {
            if byte == b'*' && next == Some(b'/') {
                *state = MdxBraceState::Normal;
                return true;
            }
        }
    }
    false
}

fn is_mdx_jsx_name_start_byte(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || matches!(byte, b'_' | b'$')
}

fn is_mdx_jsx_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b':' | b'_' | b'-' | b'$')
}

fn is_mdx_jsx_name_delimiter(byte: u8) -> bool {
    byte.is_ascii_whitespace() || matches!(byte, b'/' | b'>' | b'{' | b'}')
}

fn collect_line_range(lines: &[Line<'_>], start: usize, end: usize) -> String {
    let mut value = String::new();
    let mut cursor = start;
    while cursor <= end {
        if cursor > start {
            value.push('\n');
        }
        value.push_str(lines[cursor].text);
        cursor += 1;
    }
    value
}

fn parse_indented_code(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
) -> Option<(Block, usize)> {
    if !options.constructs.indented_code || strip_indented_code_prefix(lines[index].text).is_none()
    {
        return None;
    }
    let mut value = String::new();
    let mut cursor = index;
    // Track the last line that carried real content: leading and trailing blank
    // lines are not part of an indented code block, only interior ones are.
    let mut content_end = index;
    let mut content_end_len = 0usize;
    while cursor < lines.len() {
        if let Some(text) = strip_indented_code_prefix(lines[cursor].text) {
            ensure_line_separator(&mut value);
            value.push_str(text);
            value.push_str(lines[cursor].eol);
            if !text.trim().is_empty() {
                content_end = cursor;
                content_end_len = value.len();
            }
            cursor += 1;
            continue;
        }

        if !lines[cursor].text.trim().is_empty() {
            break;
        }
        ensure_line_separator(&mut value);
        value.push_str(lines[cursor].eol);
        cursor += 1;
    }
    // Drop trailing blank lines accumulated past the last real content line.
    value.truncate(content_end_len);
    Some((
        Block::CodeBlock(CodeBlock {
            meta: NodeMeta::new(Some(Span::new(
                lines[index].start,
                lines[content_end].end_with_eol,
            ))),
            kind: CodeBlockKind::Indented,
            info: None,
            value,
        }),
        cursor,
    ))
}

fn strip_indented_code_prefix(input: &str) -> Option<&str> {
    let mut column = 0usize;
    for (index, byte) in input.as_bytes().iter().enumerate() {
        match *byte {
            b' ' => {
                column += 1;
                if column == 4 {
                    return Some(&input[index + 1..]);
                }
            }
            b'\t' => {
                column += 4 - (column % 4);
                if column >= 4 {
                    return Some(&input[index + 1..]);
                }
            }
            _ => return None,
        }
    }
    None
}

fn parse_table(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<(Block, usize)> {
    if !options.constructs.gfm_table || index + 1 >= lines.len() {
        return None;
    }
    let delimiter = table_indent_line(lines[index + 1].text, options.constructs.indented_code)?;
    if list_marker_info(delimiter).is_some() {
        return None;
    }
    if !table_has_separator(lines[index].text, delimiter, options.constructs.spoiler) {
        return None;
    }
    let alignments = parse_table_delimiter(delimiter, options.constructs.spoiler)?;
    let headers = split_table_row(lines[index].text, options.constructs.spoiler);
    if headers.len() != alignments.len() {
        return None;
    }

    let mut rows = Vec::new();
    rows.push(TableRow {
        meta: NodeMeta::new(Some(Span::new(lines[index].start, lines[index].end))),
        cells: headers
            .iter()
            .map(|cell| TableCell {
                meta: NodeMeta::default(),
                children: parse_inlines(
                    cell.trim(),
                    lines[index].start,
                    options,
                    definitions,
                    diagnostics,
                ),
            })
            .collect(),
    });

    let mut cursor = index + 2;
    while cursor < lines.len() {
        let Some(row) = table_indent_line(lines[cursor].text, options.constructs.indented_code)
        else {
            break;
        };
        // Once a table is open, every non-blank line that isn't a real block
        // start is a body row (GFM); pipeless lines (incl. setext underlines)
        // become a single padded cell.
        if row.trim().is_empty() || table_body_line_ends_table(lines[cursor].text, options) {
            break;
        }
        let cells = split_table_row(row, options.constructs.spoiler);
        rows.push(TableRow {
            meta: NodeMeta::new(Some(Span::new(lines[cursor].start, lines[cursor].end))),
            cells: alignments
                .iter()
                .enumerate()
                .map(|(cell_index, _)| {
                    let value = cells.get(cell_index).map(String::as_str).unwrap_or("");
                    TableCell {
                        meta: NodeMeta::default(),
                        children: parse_inlines(
                            value.trim(),
                            lines[cursor].start,
                            options,
                            definitions,
                            diagnostics,
                        ),
                    }
                })
                .collect(),
        });
        cursor += 1;
    }

    Some((
        Block::Table(Table {
            meta: NodeMeta::new(Some(Span::new(
                lines[index].start,
                lines[cursor - 1].end_with_eol,
            ))),
            alignments,
            rows,
        }),
        cursor,
    ))
}

fn parse_setext_heading(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
    definitions: &[String],
) -> Option<(Block, usize)> {
    if index + 1 >= lines.len() || lines[index].text.trim().is_empty() {
        return None;
    }

    // A setext heading is a (possibly multi-line) paragraph followed by an
    // underline. Scan over paragraph-continuation lines to find the underline,
    // stopping if a continuation line is itself a block start (which would
    // interrupt the paragraph before any underline could apply).
    let mut underline_index = index + 1;
    loop {
        // A setext underline that arrived as a LAZY block-quote continuation is
        // paragraph text, not an underline: `> a\n===` is `<p>a\n===</p>`, while
        // a MARKED `> a\n> ---` stays an H2 (its `---` is not lazy). The lazy
        // flag distinguishes the two; a lazy underline keeps scanning as
        // ordinary paragraph-continuation text.
        let underline_depth = if lines[underline_index].lazy {
            None
        } else {
            setext_underline_depth(lines[underline_index].text)
        };
        if let Some(depth) = underline_depth {
            let mut value = String::new();
            for line in &lines[index..underline_index] {
                // Trim leading indentation only: a fully `.trim()`ed content line
                // would discard the trailing spaces that form a hard line break.
                push_line(&mut value, trim_ascii_start(line.text));
            }
            return Some((
                Block::Heading(Heading {
                    meta: NodeMeta::new(Some(Span::new(
                        lines[index].start,
                        lines[underline_index].end,
                    ))),
                    depth,
                    kind: HeadingKind::Setext,
                    children: parse_inlines(
                        &value,
                        lines[index].start,
                        options,
                        definitions,
                        &mut Vec::new(),
                    ),
                }),
                underline_index + 1,
            ));
        }

        // Not an underline: it must be a valid paragraph-continuation line for
        // the run to remain a setext heading.
        let line = lines[underline_index].text;
        if line.trim().is_empty()
            || table_can_start(lines, underline_index, options)
            || likely_block_start(line, options)
        {
            return None;
        }
        underline_index += 1;
        if underline_index >= lines.len() {
            return None;
        }
    }
}

fn setext_underline_depth(input: &str) -> Option<u8> {
    let underline = trim_up_to_three_spaces(input)?.trim();
    match underline {
        text if !text.is_empty() && text.chars().all(|char| char == '=') => Some(1),
        text if !text.is_empty() && text.chars().all(|char| char == '-') => Some(2),
        _ => None,
    }
}

fn parse_paragraph(
    lines: &[Line<'_>],
    index: usize,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> (Block, usize) {
    let mut value = String::new();
    let start = lines[index].start;
    let mut cursor = index;
    while cursor < lines.len() {
        if lines[cursor].text.trim().is_empty() {
            break;
        }
        // A lazy continuation line is paragraph text by construction (it reached
        // this paragraph as the dedented tail of an enclosing container), so it
        // cannot itself start a new block — skip the block-boundary checks.
        if cursor > index && !lines[cursor].lazy {
            if table_can_start(lines, cursor, options) {
                break;
            }
            if likely_block_start(lines[cursor].text, options) {
                break;
            }
        }
        if !value.is_empty() {
            value.push('\n');
        }
        value.push_str(trim_ascii_start(lines[cursor].text));
        cursor += 1;
    }

    let end = lines[cursor - 1].end;
    (
        Block::Paragraph(Paragraph {
            meta: NodeMeta::new(Some(Span::new(start, end))),
            children: parse_inlines(&value, start, options, definitions, diagnostics),
        }),
        cursor,
    )
}

/// A `*` or `_` delimiter run recorded during the inline scan for later
/// resolution by the CommonMark delimiter-stack algorithm (`process_emphasis`).
#[derive(Clone, Copy)]
struct DelimMarker {
    /// Index of the placeholder text node in the flat node list. The text node
    /// holds the as-yet-unmatched delimiter characters; matching trims it from
    /// the appropriate side and matched characters are removed entirely.
    node_index: usize,
    marker: u8,
    /// Remaining unmatched delimiter characters in this run.
    length: usize,
    can_open: bool,
    can_close: bool,
    /// Absolute byte offset of the run's first remaining delimiter character.
    span_start: usize,
    /// `true` once this run is consumed (fully matched) or demoted to plain text.
    inactive: bool,
}

/// Records a `*`/`_`/`~` delimiter run as a literal text node plus a stack
/// entry.
///
/// Flanking is computed on the whole run (CommonMark treats left/right-flanking
/// as a property of the run, not of an individual delimiter), so the same
/// `can_open`/`can_close` helpers that the older ad-hoc scanner used are reused
/// here unchanged — including the `_` intraword punctuation rules.
///
/// `strikethrough` enables the GFM cross-marker bonus: when strikethrough is an
/// active construct, a `*`/`_` run immediately adjacent to a `~` counts as
/// openable/closeable even though `~` is a punctuation character (this is what
/// makes `a*~b~*c` emphasize). The bonus is never granted to a `~` run itself —
/// tilde gets plain CommonMark flanking.
fn record_emphasis_delimiter(
    nodes: &mut Vec<Inline>,
    delimiters: &mut Vec<DelimMarker>,
    input: &str,
    index: usize,
    base_offset: usize,
    marker: u8,
    strikethrough: bool,
) {
    let length = delimiter_byte_run_len(input, index, marker);
    let (mut can_open, mut can_close) = if marker == b'_' {
        (
            can_open_underscore(input, index, length),
            can_close_underscore(input, index, length),
        )
    } else {
        (
            can_open_delimited(input, index, length),
            can_close_delimited(input, index, length),
        )
    };

    // GFM: a `*`/`_` run touching a `~` strikethrough marker may open/close even
    // when ordinary flanking refuses it (the `~` would otherwise be a blocking
    // punctuation neighbour). Tilde itself never receives this bonus.
    if strikethrough && marker != b'~' {
        let before = input[..index].chars().next_back();
        let after = input[index + length..].chars().next();
        if after == Some('~') {
            can_open = true;
        }
        if before == Some('~') {
            can_close = true;
        }
    }

    let value = String::from(marker as char).repeat(length);

    let node_index = nodes.len();
    nodes.push(Inline::Text(Text {
        meta: NodeMeta::new(Some(Span::new(
            base_offset + index,
            base_offset + index + length,
        ))),
        value,
    }));

    delimiters.push(DelimMarker {
        node_index,
        marker,
        length,
        can_open,
        can_close,
        span_start: base_offset + index,
        inactive: false,
    });
}

/// Resolves recorded `*`/`_` delimiter runs into `Emphasis`/`Strong` nodes using
/// the CommonMark delimiter-stack algorithm, leaving unmatched runs as text.
fn process_emphasis(mut nodes: Vec<Inline>, mut delimiters: Vec<DelimMarker>) -> Vec<Inline> {
    if delimiters.is_empty() {
        return nodes;
    }

    // `openers_bottom` records, per (marker, opener-can-also-close, length % 3),
    // the lowest opener index a closer is allowed to reach. Closers below this
    // bound for their key have already been proven to have no compatible opener.
    // Three markers (`*`, `_`, `~`) × both-flag × length%3.
    let mut openers_bottom: [Option<usize>; 18] = [None; 18];
    let mut closer_idx = 0;

    while closer_idx < delimiters.len() {
        let closer = delimiters[closer_idx];
        if closer.inactive || !closer.can_close {
            closer_idx += 1;
            continue;
        }

        let key = openers_bottom_key(&closer);
        let bottom = openers_bottom[key];

        // Walk back to the nearest compatible opener above the recorded bound.
        let mut opener_idx = None;
        let mut search = closer_idx;
        while search > 0 {
            search -= 1;
            if let Some(bottom) = bottom {
                if search < bottom {
                    break;
                }
            }
            let candidate = delimiters[search];
            if candidate.inactive || candidate.marker != closer.marker || !candidate.can_open {
                continue;
            }
            if emphasis_delimiters_match(&candidate, &closer) {
                opener_idx = Some(search);
                break;
            }
        }

        let Some(opener_idx) = opener_idx else {
            // No opener found: remember how far we searched so future closers of
            // the same key skip the same dead range. A closer that cannot also
            // open is removed so it is never revisited.
            openers_bottom[key] = Some(closer_idx);
            if !closer.can_open {
                delimiters[closer_idx].inactive = true;
            }
            closer_idx += 1;
            continue;
        };

        let (used, wrap) = if closer.marker == b'~' {
            // Strikethrough consumes the whole (equal-length) run on each side at
            // once; the marker width selects the `Delete` flavour.
            let length = delimiters[closer_idx].length;
            let marker = if length >= 2 {
                DeleteMarker::DoubleTilde
            } else {
                DeleteMarker::SingleTilde
            };
            (length, EmphasisWrap::Delete(marker))
        } else {
            let strong = delimiters[opener_idx].length >= 2 && delimiters[closer_idx].length >= 2;
            let used = if strong { 2 } else { 1 };
            let wrap = if strong {
                EmphasisWrap::Strong
            } else {
                EmphasisWrap::Emphasis
            };
            (used, wrap)
        };

        apply_emphasis(
            &mut nodes,
            &mut delimiters,
            opener_idx,
            closer_idx,
            used,
            wrap,
        );

        // Drop delimiters strictly between the opener and closer: they could not
        // match outward across this newly closed span.
        let mut inner = opener_idx + 1;
        while inner < closer_idx {
            delimiters[inner].inactive = true;
            inner += 1;
        }

        if delimiters[opener_idx].length == 0 {
            delimiters[opener_idx].inactive = true;
        }
        if delimiters[closer_idx].length == 0 {
            delimiters[closer_idx].inactive = true;
            closer_idx += 1;
        }
        // When the closer still has delimiters left it stays the active closer so
        // the leftover can match an earlier opener (e.g. `***foo*` keeps `**`).
    }

    // Adjacent text nodes can appear where unmatched delimiter runs ended up
    // beside literal text (`**foo*bar*` -> `**foo` + emphasis). CommonMark
    // coalesces them as the final step; do the same for the spans we created.
    merge_adjacent_text(&mut nodes);
    nodes
}

/// Merges consecutive `Text` nodes in a list, recursing into the `Emphasis`/
/// `Strong` nodes produced at this level. Other containers were already
/// finalized by their own `parse_inlines` pass and are left untouched.
fn merge_adjacent_text(nodes: &mut Vec<Inline>) {
    let mut write = 0;
    for read in 0..nodes.len() {
        if read != write {
            nodes.swap(read, write);
        }
        if write > 0 {
            let (head, tail) = nodes.split_at_mut(write);
            if let (Inline::Text(previous), Inline::Text(current)) =
                (&mut head[write - 1], &tail[0])
            {
                previous.value.push_str(&current.value);
                if let (Some(previous_span), Some(current_span)) =
                    (previous.meta.span.as_mut(), current.meta.span)
                {
                    previous_span.end = current_span.end;
                }
                continue;
            }
        }
        write += 1;
    }
    nodes.truncate(write);

    for node in nodes.iter_mut() {
        match node {
            Inline::Emphasis(emphasis) => merge_adjacent_text(&mut emphasis.children),
            Inline::Strong(strong) => merge_adjacent_text(&mut strong.children),
            Inline::Delete(delete) => merge_adjacent_text(&mut delete.children),
            _ => {}
        }
    }
}

/// Index into `openers_bottom` for a closer's (marker, both-flags, length%3) key.
fn openers_bottom_key(closer: &DelimMarker) -> usize {
    let marker = match closer.marker {
        b'_' => 1,
        b'~' => 2,
        _ => 0,
    };
    let both = usize::from(closer.can_open && closer.can_close);
    let modulo = closer.length % 3;
    ((marker * 2) + both) * 3 + modulo
}

/// CommonMark opener/closer compatibility, including the rule of three.
fn emphasis_delimiters_match(opener: &DelimMarker, closer: &DelimMarker) -> bool {
    // GFM strikethrough: opener and closer runs must be the same length (a `~`
    // never pairs with `~~`). The rule of three does not apply to `~`.
    if opener.marker == b'~' {
        return opener.length == closer.length;
    }

    // Rule of three: if either delimiter can both open and close, the sum of the
    // two run lengths must not be a multiple of three, unless both lengths are
    // themselves multiples of three.
    let opener_both = opener.can_open && opener.can_close;
    let closer_both = closer.can_open && closer.can_close;
    if opener_both || closer_both {
        let sum = opener.length + closer.length;
        if sum % 3 == 0 && !(opener.length % 3 == 0 && closer.length % 3 == 0) {
            return false;
        }
    }
    true
}

/// The node a matched delimiter pair collapses into.
#[derive(Clone, Copy)]
enum EmphasisWrap {
    Emphasis,
    Strong,
    Delete(DeleteMarker),
}

/// Wraps the nodes between two delimiter runs into an `Emphasis`/`Strong`/
/// `Delete` node, consuming `used` characters from each side and keeping every
/// other delimiter's `node_index` consistent with the rewritten node list.
fn apply_emphasis(
    nodes: &mut Vec<Inline>,
    delimiters: &mut [DelimMarker],
    opener_idx: usize,
    closer_idx: usize,
    used: usize,
    wrap: EmphasisWrap,
) {
    let opener_node = delimiters[opener_idx].node_index;
    let closer_node = delimiters[closer_idx].node_index;

    // Trim the consumed characters from the opener's text node (right side) and
    // the closer's text node (left side), updating their recorded lengths/spans.
    trim_delimiter_text_tail(&mut nodes[opener_node], used);
    delimiters[opener_idx].length -= used;
    delimiters[opener_idx].span_start += used;

    trim_delimiter_text_head(&mut nodes[closer_node], used);
    delimiters[closer_idx].length -= used;

    // Span covers the consumed opener delimiters through the consumed closer
    // delimiters. The exact value is informational; structure is what matters.
    let span_start = delimiters[opener_idx].span_start - used;
    let span_end = delimiters[closer_idx].span_start + delimiters[closer_idx].length + used;

    // The wrapped children are the nodes strictly between the opener and closer
    // text nodes.
    let children_start = opener_node + 1;
    let children_end = closer_node; // exclusive
    let children: Vec<Inline> = nodes.drain(children_start..children_end).collect();
    let removed = children.len();

    let meta = NodeMeta::new(Some(Span::new(span_start, span_end)));
    let wrapped = match wrap {
        EmphasisWrap::Strong => Inline::Strong(Strong { meta, children }),
        EmphasisWrap::Emphasis => Inline::Emphasis(Emphasis { meta, children }),
        EmphasisWrap::Delete(marker) => Inline::Delete(Delete {
            meta,
            marker,
            children,
        }),
    };
    nodes.insert(children_start, wrapped);

    // Indices at or past the (old) closer node shift by `1 - removed`: the drain
    // removed `removed` nodes then the insert added one. Apply this using the
    // original `children_end` threshold before any further mutation.
    reindex_delimiters(delimiters, children_end, 1 - removed as isize);

    // Drop any placeholder text node that has been fully consumed so leftover
    // delimiters never survive as literal text. Remove the closer first because
    // it sits at the higher index and removal shifts everything after it.
    if delimiters[closer_idx].length == 0 {
        let pos = delimiters[closer_idx].node_index;
        nodes.remove(pos);
        reindex_delimiters(delimiters, pos, -1);
    }
    if delimiters[opener_idx].length == 0 {
        let pos = delimiters[opener_idx].node_index;
        nodes.remove(pos);
        reindex_delimiters(delimiters, pos, -1);
    }
}

/// Adjusts `node_index` for every delimiter at or after `from` by `delta`.
fn reindex_delimiters(delimiters: &mut [DelimMarker], from: usize, delta: isize) {
    if delta == 0 {
        return;
    }
    for delimiter in delimiters.iter_mut() {
        if delimiter.node_index >= from {
            delimiter.node_index = (delimiter.node_index as isize + delta) as usize;
        }
    }
}

/// Removes `count` trailing delimiter characters from a placeholder text node.
fn trim_delimiter_text_tail(node: &mut Inline, count: usize) {
    if let Inline::Text(text) = node {
        let new_len = text.value.len().saturating_sub(count);
        text.value.truncate(new_len);
        if let Some(span) = text.meta.span.as_mut() {
            span.end = span.end.saturating_sub(count);
        }
    }
}

/// Removes `count` leading delimiter characters from a placeholder text node.
fn trim_delimiter_text_head(node: &mut Inline, count: usize) {
    if let Inline::Text(text) = node {
        let count = count.min(text.value.len());
        text.value.drain(..count);
        if let Some(span) = text.meta.span.as_mut() {
            span.start += count;
        }
    }
}

fn parse_inlines(
    input: &str,
    base_offset: usize,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Vec<Inline> {
    if is_single_text_inline(input) {
        return vec![Inline::Text(Text {
            meta: NodeMeta::new(Some(Span::new(base_offset, base_offset + input.len()))),
            value: input.into(),
        })];
    }

    parse_inlines_with_context(
        input,
        base_offset,
        options,
        definitions,
        diagnostics,
        InlineContext::default(),
    )
}

/// Whether the inline scanner can only produce one literal [`Inline::Text`].
///
/// This is deliberately a *superset* of the scanner's possible syntax
/// starters: a false positive merely keeps the established parser path, while
/// a false negative would change the AST. It is consequently independent of
/// `SyntaxOptions`; disabled syntax still takes the fallback. `www.` is the
/// only literal-autolink prefix without one of the punctuation starters, so it
/// is checked separately (case-insensitively, as the GFM parser does).
///
/// NUL is included because the generic scanner replaces it with U+FFFD. Empty
/// input keeps the generic empty-vector result rather than manufacturing an
/// empty text node.
fn is_single_text_inline(input: &str) -> bool {
    let bytes = input.as_bytes();
    if bytes.is_empty() {
        return false;
    }

    for (index, &byte) in bytes.iter().enumerate() {
        if matches!(
            byte,
            b'\\'
                | b'&'
                | b'\n'
                | b'\r'
                | b'`'
                | b'|'
                | b'*'
                | b'_'
                | b'+'
                | b'='
                | b'~'
                | b'^'
                | b'!'
                | b'['
                | b'$'
                | b'<'
                | b'{'
                | b':'
                | b'@'
                | b'\0'
        ) {
            return false;
        }
        if byte == b'.' && index >= 3 && bytes[index - 3..index].eq_ignore_ascii_case(b"www") {
            return false;
        }
    }

    true
}

#[derive(Clone, Copy)]
struct InlineContext {
    allow_links: bool,
}

impl Default for InlineContext {
    fn default() -> Self {
        Self { allow_links: true }
    }
}

fn parse_inlines_with_context(
    input: &str,
    base_offset: usize,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
    context: InlineContext,
) -> Vec<Inline> {
    let bytes = input.as_bytes();
    let mut nodes = Vec::new();
    let mut text_start = 0;
    let mut text = String::new();
    let mut index = 0;
    // Core `*`/`_` emphasis is resolved with a CommonMark delimiter stack after
    // the scan completes. During the scan we emit each candidate delimiter run as
    // a literal text node and record its position here so `process_emphasis` can
    // rewrite the flat node list into Emphasis/Strong (or leave it as text).
    let mut delimiters: Vec<DelimMarker> = Vec::new();

    while index < bytes.len() {
        if bytes[index] == b'\\' {
            if let Some((next_index, char)) = next_char(input, index + 1) {
                if char.is_ascii_punctuation() {
                    if options.parse.preserve_character_escapes {
                        flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                        nodes.push(Inline::Escape(Escape {
                            meta: NodeMeta::new(Some(Span::new(
                                base_offset + index,
                                base_offset + next_index,
                            ))),
                            value: char,
                        }));
                        index = next_index;
                        text_start = index;
                        continue;
                    }
                    if text.is_empty() {
                        text_start = base_offset + index;
                    }
                    if gfm_link_label_preserves_url_dot_escape(&text, char, options, context) {
                        text.push('\\');
                    }
                    text.push(char);
                    index = next_index;
                    continue;
                }
            }
        }

        if bytes[index] == b'&' {
            if let Some((end, value)) = parse_character_reference(input, index) {
                if options.parse.preserve_character_references {
                    flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                    nodes.push(Inline::CharacterReference(CharacterReference {
                        meta: NodeMeta::new(Some(Span::new(
                            base_offset + index,
                            base_offset + end,
                        ))),
                        reference: input[index..end].into(),
                        value,
                    }));
                    index = end;
                    text_start = index;
                    continue;
                }
                if text.is_empty() {
                    text_start = base_offset + index;
                }
                text.push_str(&value);
                index = end;
                continue;
            }
        }

        if bytes[index] == b'\n' {
            if text.ends_with('\\') {
                text.pop();
                flush_text(
                    &mut nodes,
                    &mut text,
                    text_start,
                    base_offset + index.saturating_sub(1),
                );
                nodes.push(Inline::LineBreak(LineBreak {
                    meta: NodeMeta::new(Some(Span::new(
                        base_offset + index.saturating_sub(1),
                        base_offset + index + 1,
                    ))),
                    kind: LineBreakKind::Backslash,
                }));
                index += 1;
                text_start = index;
                continue;
            }
            let trailing_spaces = trailing_space_count(&text);
            if is_hard_break_suffix(&text, trailing_spaces) {
                text.truncate(text.len() - trailing_spaces);
                flush_text(
                    &mut nodes,
                    &mut text,
                    text_start,
                    base_offset + index.saturating_sub(trailing_spaces),
                );
                nodes.push(Inline::LineBreak(LineBreak {
                    meta: NodeMeta::new(Some(Span::new(
                        base_offset + index.saturating_sub(trailing_spaces),
                        base_offset + index + 1,
                    ))),
                    kind: LineBreakKind::Spaces,
                }));
                index += 1;
                text_start = index;
                continue;
            }
            if trailing_spaces > 0 {
                text.truncate(text.len() - trailing_spaces);
            }
            flush_text(&mut nodes, &mut text, text_start, base_offset + index);
            nodes.push(Inline::SoftBreak(SoftBreak {
                meta: NodeMeta::new(Some(Span::new(
                    base_offset + index,
                    base_offset + index + 1,
                ))),
            }));
            index += 1;
            text_start = index;
            continue;
        }

        if bytes[index] == b'`' {
            if let Some((end, code_span)) = parse_code_span(input, index) {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                nodes.push(Inline::Code(CodeInline {
                    meta: NodeMeta::new(Some(Span::new(base_offset + index, base_offset + end))),
                    value: code_span.value,
                    raw: code_span.raw,
                    fence_length: code_span.fence_length,
                }));
                index = end;
                text_start = index;
                continue;
            } else {
                // No matching-length close for this opening backtick run:
                // CommonMark renders the whole run as literal text. Consume the
                // entire run here so the loop does not advance one byte and retry
                // a shorter sub-run that could spuriously match a shorter close
                // (```foo`` stayed a phantom 2-backtick code span).
                let run = bytes[index..]
                    .iter()
                    .take_while(|byte| **byte == b'`')
                    .count();
                if text.is_empty() {
                    text_start = base_offset + index;
                }
                for _ in 0..run {
                    text.push('`');
                }
                index += run;
                continue;
            }
        }

        if options.constructs.spoiler
            && bytes.get(index) == Some(&b'|')
            && bytes.get(index + 1) == Some(&b'|')
            && bytes.get(index + 2) != Some(&b'|')
        {
            if let Some(end) = find_spoiler_close(input, index + 2) {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                let inner = &input[index + 2..end];
                nodes.push(Inline::Spoiler(Spoiler {
                    meta: NodeMeta::new(Some(Span::new(
                        base_offset + index,
                        base_offset + end + 2,
                    ))),
                    children: parse_inlines_with_context(
                        inner,
                        base_offset + index + 2,
                        options,
                        definitions,
                        diagnostics,
                        context,
                    ),
                }));
                index = end + 2;
                text_start = index;
                continue;
            }
        }

        if bytes[index] == b'*' && delimiter_byte_run_start(input, index, b'*') == index {
            let run_len = delimiter_byte_run_len(input, index, b'*');
            flush_text(&mut nodes, &mut text, text_start, base_offset + index);
            record_emphasis_delimiter(
                &mut nodes,
                &mut delimiters,
                input,
                index,
                base_offset,
                b'*',
                options.constructs.gfm_strikethrough,
            );
            index += run_len;
            text_start = index;
            continue;
        }

        if options.constructs.underline
            && bytes.get(index) == Some(&b'_')
            && bytes.get(index + 1) == Some(&b'_')
            && bytes.get(index + 2) == Some(&b'_')
            && can_open_underscore(input, index, 1)
        {
            if let Some(end) = find_closing_delimiter(input, index + 3, "___", true) {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                let inner = &input[index + 3..end];
                let underline = Inline::Underline(Underline {
                    meta: NodeMeta::new(Some(Span::new(
                        base_offset + index + 1,
                        base_offset + end + 2,
                    ))),
                    children: parse_inlines_with_context(
                        inner,
                        base_offset + index + 3,
                        options,
                        definitions,
                        diagnostics,
                        context,
                    ),
                });
                nodes.push(Inline::Emphasis(Emphasis {
                    meta: NodeMeta::new(Some(Span::new(
                        base_offset + index,
                        base_offset + end + 3,
                    ))),
                    children: vec![underline],
                }));
                index = end + 3;
                text_start = index;
                continue;
            }
        }

        if options.constructs.underline
            && bytes.get(index) == Some(&b'_')
            && bytes.get(index + 1) == Some(&b'_')
            && can_open_underscore(input, index, 2)
        {
            if let Some(end) = find_closing_delimiter(input, index + 2, "__", true) {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                let inner = &input[index + 2..end];
                nodes.push(Inline::Underline(Underline {
                    meta: NodeMeta::new(Some(Span::new(
                        base_offset + index,
                        base_offset + end + 2,
                    ))),
                    children: parse_inlines_with_context(
                        inner,
                        base_offset + index + 2,
                        options,
                        definitions,
                        diagnostics,
                        context,
                    ),
                }));
                index = end + 2;
                text_start = index;
                continue;
            }
        }

        // Core `_` emphasis/strong is resolved by the delimiter stack, just like
        // `*`. The `___`/`__` underline-extension branches above run first and
        // `continue` when they consume the run, so reaching this point means the
        // run is plain emphasis material (underline disabled, or no underline
        // close was found).
        if bytes[index] == b'_' && delimiter_byte_run_start(input, index, b'_') == index {
            // A leading `_` can begin a GFM email local part (`_a@b.c`); try the
            // literal autolink before recording the `_` as an emphasis
            // delimiter, otherwise the `_` would be consumed and the email would
            // wrongly start one char later (where its left boundary fails).
            if (options.constructs.gfm_autolink_literal || options.constructs.relaxed_autolinks)
                && context.allow_links
            {
                if let Some((end, destination)) = parse_literal_autolink(
                    input,
                    index,
                    options.constructs.gfm_autolink_literal,
                    options.constructs.relaxed_autolinks,
                ) {
                    flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                    nodes.push(Inline::Autolink(Autolink {
                        meta: NodeMeta::new(Some(Span::new(
                            base_offset + index,
                            base_offset + end,
                        ))),
                        destination,
                        kind: AutolinkKind::GfmLiteral {
                            original: input[index..end].into(),
                        },
                    }));
                    index = end;
                    text_start = index;
                    continue;
                }
            }
            let run_len = delimiter_byte_run_len(input, index, b'_');
            flush_text(&mut nodes, &mut text, text_start, base_offset + index);
            record_emphasis_delimiter(
                &mut nodes,
                &mut delimiters,
                input,
                index,
                base_offset,
                b'_',
                options.constructs.gfm_strikethrough,
            );
            index += run_len;
            text_start = index;
            continue;
        }

        if options.constructs.insert
            && bytes.get(index) == Some(&b'+')
            && bytes.get(index + 1) == Some(&b'+')
            && bytes.get(index + 2) != Some(&b'+')
            && can_open_delimited(input, index, 2)
        {
            if let Some(end) = find_closing_delimiter(input, index + 2, "++", false) {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                let inner = &input[index + 2..end];
                nodes.push(Inline::Insert(Insert {
                    meta: NodeMeta::new(Some(Span::new(
                        base_offset + index,
                        base_offset + end + 2,
                    ))),
                    children: parse_inlines_with_context(
                        inner,
                        base_offset + index + 2,
                        options,
                        definitions,
                        diagnostics,
                        context,
                    ),
                }));
                index = end + 2;
                text_start = index;
                continue;
            }
        }

        if options.constructs.highlight
            && bytes.get(index) == Some(&b'=')
            && bytes.get(index + 1) == Some(&b'=')
            && bytes.get(index + 2) != Some(&b'=')
            && can_open_delimited(input, index, 2)
        {
            if let Some(end) = find_closing_delimiter(input, index + 2, "==", false) {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                let inner = &input[index + 2..end];
                nodes.push(Inline::Mark(Mark {
                    meta: NodeMeta::new(Some(Span::new(
                        base_offset + index,
                        base_offset + end + 2,
                    ))),
                    children: parse_inlines_with_context(
                        inner,
                        base_offset + index + 2,
                        options,
                        definitions,
                        diagnostics,
                        context,
                    ),
                }));
                index = end + 2;
                text_start = index;
                continue;
            }
        }

        if options.constructs.subscript
            && starts_exact_byte_run(input, index, b'~', 1)
            && !single_tilde_delete_takes_precedence(options, input, index)
        {
            if let Some(end) = find_simple_inline_close(input, index + 1, b'~') {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                let inner = &input[index + 1..end];
                nodes.push(Inline::Subscript(Subscript {
                    meta: NodeMeta::new(Some(Span::new(
                        base_offset + index,
                        base_offset + end + 1,
                    ))),
                    children: parse_inlines_with_context(
                        inner,
                        base_offset + index + 1,
                        options,
                        definitions,
                        diagnostics,
                        context,
                    ),
                }));
                index = end + 1;
                text_start = index;
                continue;
            }
        }

        if options.constructs.inline_footnote
            && options.constructs.footnote_reference
            && bytes.get(index) == Some(&b'^')
            && bytes.get(index + 1) == Some(&b'[')
        {
            if let Some(close) = find_inline_footnote_end(input, index + 2) {
                let inner = &input[index + 2..close];
                if !inner.trim().is_empty() {
                    flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                    nodes.push(Inline::InlineFootnote(InlineFootnote {
                        meta: NodeMeta::new(Some(Span::new(
                            base_offset + index,
                            base_offset + close + 1,
                        ))),
                        children: parse_inlines_with_context(
                            inner,
                            base_offset + index + 2,
                            options,
                            definitions,
                            diagnostics,
                            context,
                        ),
                    }));
                    index = close + 1;
                    text_start = index;
                    continue;
                }
            }
        }

        if options.constructs.superscript
            && bytes.get(index) == Some(&b'^')
            && !(options.constructs.inline_footnote && bytes.get(index + 1) == Some(&b'['))
        {
            if let Some(end) = find_simple_inline_close(input, index + 1, b'^') {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                let inner = &input[index + 1..end];
                nodes.push(Inline::Superscript(Superscript {
                    meta: NodeMeta::new(Some(Span::new(
                        base_offset + index,
                        base_offset + end + 1,
                    ))),
                    children: parse_inlines_with_context(
                        inner,
                        base_offset + index + 1,
                        options,
                        definitions,
                        diagnostics,
                        context,
                    ),
                }));
                index = end + 1;
                text_start = index;
                continue;
            }
        }

        // GFM strikethrough joins the shared CommonMark delimiter stack: a `~`
        // run is recorded as a candidate run (just like `*`/`_`) and paired into
        // `Delete` by `process_emphasis`, rather than scanned greedily here. Only
        // runs of length 1 (single-tilde mode) or 2 can ever form strikethrough;
        // runs of 3+ never do, so they fall through to literal text. The
        // subscript branch above already claimed single `~` runs it owns.
        if options.constructs.gfm_strikethrough
            && bytes[index] == b'~'
            && delimiter_byte_run_start(input, index, b'~') == index
        {
            let run_len = delimiter_byte_run_len(input, index, b'~');
            let recordable =
                run_len == 2 || (run_len == 1 && options.parse.single_tilde_strikethrough);
            if recordable {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                record_emphasis_delimiter(
                    &mut nodes,
                    &mut delimiters,
                    input,
                    index,
                    base_offset,
                    b'~',
                    true,
                );
                index += run_len;
                text_start = index;
                continue;
            }
        }

        if bytes[index] == b'!' && index + 1 < bytes.len() && bytes[index + 1] == b'[' {
            if let Some((end, image)) =
                parse_image(input, index, base_offset, options, definitions, diagnostics)
            {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                nodes.push(image);
                index = end;
                text_start = index;
                continue;
            }
        }

        if bytes[index] == b'[' {
            if let Some((end, wikilink)) = parse_wikilink(input, index, base_offset, options) {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                nodes.push(wikilink);
                index = end;
                text_start = index;
                continue;
            }
            if let Some((end, link)) = parse_link(
                input,
                index,
                base_offset,
                options,
                definitions,
                diagnostics,
                context,
            ) {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                nodes.push(link);
                index = end;
                text_start = index;
                continue;
            }
            if options.constructs.footnote_reference
                && bytes.get(index) == Some(&b'[')
                && bytes.get(index + 1) == Some(&b'^')
            {
                if let Some(close) = find_footnote_reference_label_end(input, index + 2) {
                    let label = &input[index + 2..close];
                    if is_footnote_label(label) {
                        flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                        nodes.push(Inline::FootnoteReference(FootnoteReference {
                            meta: NodeMeta::new(Some(Span::new(
                                base_offset + index,
                                base_offset + close + 1,
                            ))),
                            label: label.into(),
                            identifier: normalize_label(label),
                        }));
                        index = close + 1;
                        text_start = index;
                        continue;
                    }
                }
            }
        }

        if bytes[index] == b'$' && options.constructs.math_inline {
            if let Some((end, value, kind)) = parse_math_inline(input, index) {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                nodes.push(Inline::Math(MathInline {
                    meta: NodeMeta::new(Some(Span::new(base_offset + index, base_offset + end))),
                    value,
                    kind,
                }));
                index = end;
                text_start = index;
                continue;
            }
            // A dollar run that opens but finds no exact-length close is emitted
            // as literal text in one piece (like a code-span). Skipping the
            // whole run prevents re-opening with a shorter marker inside it, so
            // `$$$foo$$` stays literal rather than matching `$$foo$$`. A lone
            // `$` before a backtick (the code-math form) is a run of 1, so this
            // still advances correctly when that form fails.
            let run = bytes[index..]
                .iter()
                .take_while(|byte| **byte == b'$')
                .count();
            if run > 1 {
                if text.is_empty() {
                    text_start = base_offset + index;
                }
                text.push_str(&input[index..index + run]);
                index += run;
                continue;
            }
        }

        // GFM bare autolinks must not fire inside an existing link's text
        // (no links in links) — `context.allow_links` is false in label scans.
        if (options.constructs.gfm_autolink_literal || options.constructs.relaxed_autolinks)
            && context.allow_links
        {
            if let Some((end, destination)) = parse_literal_autolink(
                input,
                index,
                options.constructs.gfm_autolink_literal,
                options.constructs.relaxed_autolinks,
            ) {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                nodes.push(Inline::Autolink(Autolink {
                    meta: NodeMeta::new(Some(Span::new(base_offset + index, base_offset + end))),
                    destination,
                    kind: AutolinkKind::GfmLiteral {
                        original: input[index..end].into(),
                    },
                }));
                index = end;
                text_start = index;
                continue;
            }
        }

        if bytes[index] == b'<' {
            if let Some(end) = parse_autolink_end(input, index) {
                let raw = &input[index..end];
                if is_autolink(raw) {
                    flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                    if context.allow_links {
                        nodes.push(Inline::Autolink(Autolink {
                            meta: NodeMeta::new(Some(Span::new(
                                base_offset + index,
                                base_offset + end,
                            ))),
                            destination: raw[1..raw.len() - 1].into(),
                            kind: AutolinkKind::Angle,
                        }));
                    } else {
                        nodes.push(Inline::Text(Text {
                            meta: NodeMeta::new(Some(Span::new(
                                base_offset + index,
                                base_offset + end,
                            ))),
                            value: raw[1..raw.len() - 1].into(),
                        }));
                    }
                    index = end;
                    text_start = index;
                    continue;
                }
            }
            if options.constructs.mdx_jsx_inline {
                if let Some((end, raw)) = parse_mdx_jsx_inline(input, index) {
                    flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                    nodes.push(Inline::MdxJsx(MdxJsxInline {
                        meta: NodeMeta::new(Some(Span::new(
                            base_offset + index,
                            base_offset + end,
                        ))),
                        value: raw,
                    }));
                    index = end;
                    text_start = index;
                    continue;
                }
            }
            if let Some((end, raw)) = parse_html_inline(input, index) {
                if options.constructs.html_inline {
                    flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                    nodes.push(Inline::Html(HtmlInline {
                        meta: NodeMeta::new(Some(Span::new(
                            base_offset + index,
                            base_offset + end,
                        ))),
                        value: raw,
                    }));
                    index = end;
                    text_start = index;
                    continue;
                }
            }
        }

        if bytes[index] == b'{' && options.constructs.mdx_expression_inline {
            if let Some(end) = find_mdx_expression_inline_close(input, index) {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                nodes.push(Inline::MdxExpression(MdxExpressionInline {
                    meta: NodeMeta::new(Some(Span::new(
                        base_offset + index,
                        base_offset + end + 1,
                    ))),
                    value: input[index + 1..end].into(),
                }));
                index = end + 1;
                text_start = index;
                continue;
            } else {
                diagnostics.push(Diagnostic::new(
                    DiagnosticSeverity::Error,
                    DiagnosticCode::InvalidMdx,
                    Span::new(base_offset + index, base_offset + input.len()),
                    "MDX expression is missing a closing brace",
                ));
            }
        }

        if bytes[index] == b':' && options.constructs.shortcode {
            if let Some((end, name)) = parse_shortcode(input, index) {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                nodes.push(Inline::Shortcode(Shortcode {
                    meta: NodeMeta::new(Some(Span::new(base_offset + index, base_offset + end))),
                    name,
                }));
                index = end;
                text_start = index;
                continue;
            }
        }

        if bytes[index] == b':' && options.constructs.directive_text {
            if let Some((end, directive)) =
                parse_text_directive(input, index, base_offset, options, definitions, diagnostics)
            {
                flush_text(&mut nodes, &mut text, text_start, base_offset + index);
                nodes.push(directive);
                index = end;
                text_start = index;
                continue;
            }
        }

        let (next_index, char) = next_char(input, index).expect("valid UTF-8 byte index");
        if text.is_empty() {
            text_start = base_offset + index;
        }
        text.push(if char == '\0' { '\u{FFFD}' } else { char });
        index = next_index;
    }

    flush_text(&mut nodes, &mut text, text_start, base_offset + input.len());
    process_emphasis(nodes, delimiters)
}

fn parse_shortcode(input: &str, index: usize) -> Option<(usize, String)> {
    if input[index..].starts_with("::") {
        return None;
    }

    let mut cursor = index + 1;
    while let Some((next, char)) = next_char(input, cursor) {
        if char == ':' {
            if cursor == index + 1 {
                return None;
            }
            return Some((next, input[index + 1..cursor].into()));
        }
        if !(char.is_ascii_alphanumeric() || matches!(char, '_' | '-' | '+')) {
            return None;
        }
        cursor = next;
    }
    None
}

fn parse_wikilink(
    input: &str,
    index: usize,
    base_offset: usize,
    options: &SyntaxOptions,
) -> Option<(usize, Inline)> {
    let configured_order = if options.constructs.wikilink_title_after_pipe {
        WikiLinkLabelOrder::AfterPipe
    } else if options.constructs.wikilink_title_before_pipe {
        WikiLinkLabelOrder::BeforePipe
    } else {
        return None;
    };
    if input.as_bytes().get(index) != Some(&b'[') || input.as_bytes().get(index + 1) != Some(&b'[')
    {
        return None;
    }

    let close = find_wikilink_close(input, index + 2)?;
    let source = &input[index + 2..close];
    if source.is_empty() || source.len() > WIKILINK_MAX_BYTES {
        return None;
    }

    let (target_source, label_source, label_order) =
        if let Some(separator) = find_wikilink_separator(source) {
            match configured_order {
                WikiLinkLabelOrder::AfterPipe => (
                    &source[..separator],
                    &source[separator + 1..],
                    WikiLinkLabelOrder::AfterPipe,
                ),
                WikiLinkLabelOrder::BeforePipe => (
                    &source[separator + 1..],
                    &source[..separator],
                    WikiLinkLabelOrder::BeforePipe,
                ),
            }
        } else {
            (source, source, configured_order)
        };

    let target = unescape_string(target_source);
    if target.is_empty() {
        return None;
    }
    let label = unescape_string(label_source);
    let end = close + 2;
    Some((
        end,
        Inline::WikiLink(WikiLink {
            meta: NodeMeta::new(Some(Span::new(base_offset + index, base_offset + end))),
            target,
            label,
            label_order,
        }),
    ))
}

fn find_wikilink_close(input: &str, start: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut cursor = start;
    while cursor < input.len() {
        match bytes[cursor] {
            b'\\' => {
                cursor += 1;
                if cursor < input.len() {
                    cursor = next_char(input, cursor)?.0;
                }
            }
            b'\n' | b'\r' => return None,
            b']' if bytes.get(cursor + 1) == Some(&b']') => return Some(cursor),
            _ => cursor = next_char(input, cursor)?.0,
        }
    }
    None
}

fn find_wikilink_separator(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut cursor = 0;
    while cursor < input.len() {
        match bytes[cursor] {
            b'\\' => {
                cursor += 1;
                if cursor < input.len() {
                    cursor = next_char(input, cursor)?.0;
                }
            }
            b'|' => return Some(cursor),
            _ => cursor = next_char(input, cursor)?.0,
        }
    }
    None
}

fn trailing_space_count(input: &str) -> usize {
    input
        .as_bytes()
        .iter()
        .rev()
        .take_while(|byte| matches!(**byte, b' ' | b'\t'))
        .count()
}

fn is_hard_break_suffix(input: &str, trailing: usize) -> bool {
    // A hard line break is two or more spaces immediately before the newline
    // with no intervening tab; a tab anywhere in the trailing whitespace run
    // demotes it to a soft break.
    let bytes = input.as_bytes();
    trailing >= 2
        && bytes[bytes.len() - trailing..]
            .iter()
            .all(|byte| *byte == b' ')
}

fn parse_image(
    input: &str,
    index: usize,
    base_offset: usize,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<(usize, Inline)> {
    let label_start = index + 2;
    let label_end = find_link_label_end(input, index + 1)?;
    let alt_source = &input[label_start..label_end];
    let after_label = label_end + 1;
    if input.as_bytes().get(after_label) == Some(&b'(') {
        let (close, resource) = parse_link_resource(input, after_label)?;
        return Some((
            close,
            Inline::Image(Image {
                meta: NodeMeta::new(Some(Span::new(base_offset + index, base_offset + close))),
                destination: resource.destination,
                destination_kind: resource.destination_kind,
                title: resource.title,
                title_kind: resource.title_kind,
                alt: parse_inlines(
                    alt_source,
                    base_offset + label_start,
                    options,
                    definitions,
                    diagnostics,
                ),
            }),
        ));
    }
    if input.as_bytes().get(after_label) == Some(&b'[') {
        let close = find_reference_label_end(input, after_label)?;
        let label = &input[after_label + 1..close];
        let identifier = if label.is_empty() { alt_source } else { label };
        if definition_exists(definitions, identifier) {
            return Some((
                close + 1,
                Inline::ImageReference(ImageReference {
                    meta: NodeMeta::new(Some(Span::new(
                        base_offset + index,
                        base_offset + close + 1,
                    ))),
                    identifier: normalize_label(identifier),
                    label: identifier.into(),
                    kind: if label.is_empty() {
                        ReferenceKind::Collapsed
                    } else {
                        ReferenceKind::Full
                    },
                    alt: parse_inlines(
                        alt_source,
                        base_offset + label_start,
                        options,
                        definitions,
                        diagnostics,
                    ),
                }),
            ));
        }
        // A present `[...]` second label that resolves to no definition is not a
        // reference and does not fall back to a shortcut (mirrors parse_link).
        return None;
    }
    // Shortcut image reference `![foo]` (no following `(`/`[`) where `foo` is a
    // defined label — mirrors parse_link's shortcut branch.
    if definition_exists(definitions, alt_source) {
        return Some((
            after_label,
            Inline::ImageReference(ImageReference {
                meta: NodeMeta::new(Some(Span::new(
                    base_offset + index,
                    base_offset + after_label,
                ))),
                identifier: normalize_label(alt_source),
                label: alt_source.into(),
                kind: ReferenceKind::Shortcut,
                alt: parse_inlines(
                    alt_source,
                    base_offset + label_start,
                    options,
                    definitions,
                    diagnostics,
                ),
            }),
        ));
    }
    None
}

fn parse_link(
    input: &str,
    index: usize,
    base_offset: usize,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
    context: InlineContext,
) -> Option<(usize, Inline)> {
    if !context.allow_links {
        return None;
    }
    let label_end = find_link_label_end(input, index)?;
    let label_source = &input[index + 1..label_end];
    if label_contains_link(label_source, base_offset + index + 1, options, definitions) {
        return None;
    }
    let after_label = label_end + 1;
    if input.as_bytes().get(after_label) == Some(&b'(') {
        // A present-but-invalid `(...)` resource is not an inline link, but
        // CommonMark still resolves `[label]` as a shortcut reference and leaves
        // the invalid `(...)` as literal text (links 568) — so fall through to
        // the reference branches below instead of bailing out of parse_link.
        if let Some((close, resource)) = parse_link_resource(input, after_label) {
            return Some((
                close,
                Inline::Link(Link {
                    meta: NodeMeta::new(Some(Span::new(base_offset + index, base_offset + close))),
                    destination: resource.destination,
                    destination_kind: resource.destination_kind,
                    title: resource.title,
                    title_kind: resource.title_kind,
                    children: parse_inlines_with_context(
                        label_source,
                        base_offset + index + 1,
                        options,
                        definitions,
                        diagnostics,
                        InlineContext { allow_links: false },
                    ),
                }),
            ));
        }
    }
    if input.as_bytes().get(after_label) == Some(&b'[') {
        let close = find_reference_label_end(input, after_label)?;
        let label = &input[after_label + 1..close];
        let identifier = if label.is_empty() {
            label_source
        } else {
            label
        };
        if definition_exists(definitions, identifier) {
            return Some((
                close + 1,
                Inline::LinkReference(LinkReference {
                    meta: NodeMeta::new(Some(Span::new(
                        base_offset + index,
                        base_offset + close + 1,
                    ))),
                    identifier: normalize_label(identifier),
                    label: identifier.into(),
                    kind: if label.is_empty() {
                        ReferenceKind::Collapsed
                    } else {
                        ReferenceKind::Full
                    },
                    children: parse_inlines_with_context(
                        label_source,
                        base_offset + index + 1,
                        options,
                        definitions,
                        diagnostics,
                        InlineContext { allow_links: false },
                    ),
                }),
            ));
        }
        // A present `[...]` second label that resolves to no definition is NOT a
        // link, and CommonMark does not fall back to treating the first label as
        // a shortcut (`[x][ ]`, `[x][undef]` stay literal). Only a truly absent
        // `[...]` reaches the shortcut path below.
        return None;
    }
    if definition_exists(definitions, label_source) {
        return Some((
            after_label,
            Inline::LinkReference(LinkReference {
                meta: NodeMeta::new(Some(Span::new(
                    base_offset + index,
                    base_offset + after_label,
                ))),
                identifier: normalize_label(label_source),
                label: label_source.into(),
                kind: ReferenceKind::Shortcut,
                children: parse_inlines_with_context(
                    label_source,
                    base_offset + index + 1,
                    options,
                    definitions,
                    diagnostics,
                    InlineContext { allow_links: false },
                ),
            }),
        ));
    }
    None
}

fn find_reference_label_end(input: &str, open: usize) -> Option<usize> {
    // A reference/definition link label does not nest: it ends at the first
    // unescaped `]`, and an unescaped interior `[` disqualifies it.
    if input.as_bytes().get(open) != Some(&b'[') {
        return None;
    }

    let mut cursor = open + 1;
    while cursor < input.len() {
        let (next, char) = next_char(input, cursor)?;
        match char {
            '\\' => {
                cursor = next_char(input, next)
                    .map(|(after_escape, _)| after_escape)
                    .unwrap_or(next);
                continue;
            }
            '[' => return None,
            ']' => {
                return reference_label_is_within_limit(&input[open + 1..cursor]).then_some(cursor);
            }
            _ => {}
        }
        cursor = next;
    }
    None
}

fn label_contains_link(
    label_source: &str,
    base_offset: usize,
    options: &SyntaxOptions,
    definitions: &[String],
) -> bool {
    let mut diagnostics = Vec::new();
    let inlines = parse_inlines_with_context(
        label_source,
        base_offset,
        options,
        definitions,
        &mut diagnostics,
        InlineContext::default(),
    );
    contains_link_inline(&inlines)
}

fn contains_link_inline(inlines: &[Inline]) -> bool {
    inlines.iter().any(|inline| match inline {
        Inline::Link(_) | Inline::LinkReference(_) => true,
        Inline::Emphasis(node) => contains_link_inline(&node.children),
        Inline::Strong(node) => contains_link_inline(&node.children),
        Inline::Delete(node) => contains_link_inline(&node.children),
        Inline::TextDirective(node) => contains_link_inline(&node.label),
        _ => false,
    })
}

fn find_link_label_end(input: &str, open: usize) -> Option<usize> {
    if input.as_bytes().get(open) != Some(&b'[') {
        return None;
    }

    let mut depth = 1usize;
    let mut cursor = open + 1;
    while cursor < input.len() {
        let (next, char) = next_char(input, cursor)?;
        match char {
            '\\' => {
                cursor = next_char(input, next)
                    .map(|(after_escape, _)| after_escape)
                    .unwrap_or(next);
                continue;
            }
            '`' => {
                if let Some((end, _)) = parse_code_span(input, cursor) {
                    cursor = end;
                    continue;
                }
            }
            '<' => {
                if let Some(end) = parse_autolink_end(input, cursor) {
                    let raw = &input[cursor..end];
                    if is_autolink(raw) {
                        cursor = end;
                        continue;
                    }
                }
                if let Some((end, _)) = parse_html_inline(input, cursor) {
                    cursor = end;
                    continue;
                }
            }
            '[' => depth += 1,
            ']' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(cursor);
                }
            }
            _ => {}
        }
        cursor = next;
    }
    None
}

fn parse_text_directive(
    input: &str,
    index: usize,
    base_offset: usize,
    options: &SyntaxOptions,
    definitions: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<(usize, Inline)> {
    if input[index..].starts_with("::") {
        return None;
    }
    if index > 0 {
        let previous = input[..index].chars().next_back()?;
        if !previous.is_whitespace() && !matches!(previous, '(' | '[' | '{') {
            return None;
        }
    }
    let opener_source = &input[index + 1..];
    let (name, label_source, attributes, consumed) = match parse_directive_opener(opener_source) {
        Some(opener) => opener,
        None => {
            if directive_opener_looks_malformed(opener_source) {
                diagnostics.push(Diagnostic::new(
                    DiagnosticSeverity::Error,
                    DiagnosticCode::InvalidDirectiveName,
                    Span::new(base_offset + index, base_offset + input.len()),
                    "text directive opener is malformed",
                ));
            }
            return None;
        }
    };
    let label = label_source
        .map(|source| {
            parse_inlines(
                source,
                base_offset + index + 1 + name.len() + 1,
                options,
                definitions,
                diagnostics,
            )
        })
        .unwrap_or_default();
    Some((
        index + 1 + consumed,
        Inline::TextDirective(TextDirective {
            meta: NodeMeta::new(Some(Span::new(
                base_offset + index,
                base_offset + index + 1 + consumed,
            ))),
            name,
            label,
            attributes,
        }),
    ))
}

fn parse_directive_opener(
    input: &str,
) -> Option<(String, Option<&str>, Vec<DirectiveAttribute>, usize)> {
    let mut index = 0;
    while let Some((next, char)) = next_char(input, index) {
        if char.is_ascii_alphanumeric() || char == '_' || char == '-' {
            index = next;
        } else {
            break;
        }
    }
    let name = &input[..index];
    if !is_directive_name(name) {
        return None;
    }

    let mut label = None;
    let mut attributes = Vec::new();
    let mut consumed = index;
    if input.as_bytes().get(consumed) == Some(&b'[') {
        let close = find_link_label_end(input, consumed)?;
        label = Some(&input[consumed + 1..close]);
        consumed = close + 1;
    }
    if input.as_bytes().get(consumed) == Some(&b'{') {
        let close = find_directive_attributes_close(input, consumed)?;
        attributes = parse_attributes(&input[consumed + 1..close]);
        consumed = close + 1;
    }

    Some((name.into(), label, attributes, consumed))
}

fn directive_opener_looks_malformed(input: &str) -> bool {
    let mut index = 0;
    while let Some((next, char)) = next_char(input, index) {
        if char.is_ascii_alphanumeric() || char == '_' || char == '-' {
            index = next;
        } else {
            break;
        }
    }
    index > 0
        && is_directive_name(&input[..index])
        && matches!(input.as_bytes().get(index), Some(b'[' | b'{'))
}

fn find_directive_attributes_close(input: &str, open: usize) -> Option<usize> {
    if input.as_bytes().get(open) != Some(&b'{') {
        return None;
    }

    let bytes = input.as_bytes();
    let mut cursor = open + 1;
    let mut quote = None;
    let mut escaped = false;
    while cursor < input.len() {
        let byte = bytes[cursor];
        if escaped {
            escaped = false;
            cursor += 1;
            continue;
        }
        if byte == b'\\' {
            escaped = true;
            cursor += 1;
            continue;
        }
        if let Some(delimiter) = quote {
            if byte == delimiter {
                quote = None;
            }
            cursor += 1;
            continue;
        }
        match byte {
            b'"' | b'\'' => quote = Some(byte),
            b'}' => return Some(cursor),
            _ => {}
        }
        cursor += 1;
    }
    None
}

fn parse_attributes(input: &str) -> Vec<DirectiveAttribute> {
    let mut attributes = Vec::new();
    let mut cursor = 0;
    while cursor < input.len() {
        cursor = skip_spaces(input, cursor);
        if cursor >= input.len() {
            break;
        }

        if input.as_bytes().get(cursor) == Some(&b'#') {
            let (id, next) = parse_attribute_token(input, cursor + 1);
            if !id.is_empty() {
                attributes.push(DirectiveAttribute {
                    name: "id".into(),
                    value: Some(id.into()),
                });
            }
            cursor = next;
            continue;
        }

        if input.as_bytes().get(cursor) == Some(&b'.') {
            let (class, next) = parse_attribute_token(input, cursor + 1);
            if !class.is_empty() {
                attributes.push(DirectiveAttribute {
                    name: "class".into(),
                    value: Some(class.into()),
                });
            }
            cursor = next;
            continue;
        }

        let (name, next) = parse_attribute_name(input, cursor);
        if name.is_empty() {
            break;
        }
        cursor = skip_spaces(input, next);
        if input.as_bytes().get(cursor) == Some(&b'=') {
            cursor = skip_spaces(input, cursor + 1);
            if let Some((value, next)) = parse_attribute_value(input, cursor) {
                attributes.push(DirectiveAttribute {
                    name: name.into(),
                    value: Some(value),
                });
                cursor = next;
            } else {
                attributes.push(DirectiveAttribute {
                    name: name.into(),
                    value: Some(String::new()),
                });
            }
        } else {
            attributes.push(DirectiveAttribute {
                name: name.into(),
                value: None,
            });
        }
    }
    attributes
}

fn parse_attribute_token(input: &str, index: usize) -> (&str, usize) {
    let mut cursor = index;
    while let Some((next, char)) = next_char(input, cursor) {
        if char.is_whitespace() {
            break;
        }
        cursor = next;
    }
    (&input[index..cursor], cursor)
}

fn parse_attribute_name(input: &str, index: usize) -> (&str, usize) {
    let mut cursor = index;
    while let Some((next, char)) = next_char(input, cursor) {
        if char.is_whitespace() || char == '=' {
            break;
        }
        cursor = next;
    }
    (&input[index..cursor], cursor)
}

fn parse_attribute_value(input: &str, index: usize) -> Option<(String, usize)> {
    let quote = input.as_bytes().get(index).copied();
    if matches!(quote, Some(b'"' | b'\'')) {
        let quote = quote?;
        let mut cursor = index + 1;
        while cursor < input.len() {
            let (next, char) = next_char(input, cursor)?;
            if char as u8 == quote && !is_escaped_at(input, cursor) {
                return Some((unescape_ascii_punctuation(&input[index + 1..cursor]), next));
            }
            cursor = next;
        }
        return None;
    }

    let (value, next) = parse_attribute_token(input, index);
    Some((
        unescape_selected(value, |char| matches!(char, '\\' | '&')),
        next,
    ))
}

struct CodeSpanSource {
    value: String,
    raw: String,
    fence_length: usize,
}

fn parse_code_span(input: &str, index: usize) -> Option<(usize, CodeSpanSource)> {
    let len = input[index..]
        .as_bytes()
        .iter()
        .take_while(|byte| **byte == b'`')
        .count();
    let search_start = index + len;
    let close = find_code_span_close(input, search_start, len)?;
    let raw = &input[search_start..close];
    Some((
        close + len,
        CodeSpanSource {
            value: normalize_code_span(raw),
            raw: raw.into(),
            fence_length: len,
        },
    ))
}

fn find_code_span_close(input: &str, start: usize, marker_len: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut cursor = start;
    while cursor < bytes.len() {
        if bytes[cursor] != b'`' {
            cursor = next_char(input, cursor)
                .map(|(next, _)| next)
                .unwrap_or(bytes.len());
            continue;
        }
        let run_len = bytes[cursor..]
            .iter()
            .take_while(|byte| **byte == b'`')
            .count();
        if run_len == marker_len {
            return Some(cursor);
        }
        cursor += run_len;
    }
    None
}

fn normalize_code_span(input: &str) -> String {
    let mut normalized = String::new();
    let mut cursor = 0;
    while cursor < input.len() {
        let (next, char) = next_char(input, cursor).expect("valid UTF-8 byte index");
        if char == '\r' {
            if input.as_bytes().get(next) == Some(&b'\n') {
                cursor = next + 1;
            } else {
                cursor = next;
            }
            normalized.push(' ');
            continue;
        }
        if char == '\n' {
            normalized.push(' ');
            cursor = next;
            continue;
        }
        normalized.push(char);
        cursor = next;
    }

    if normalized.starts_with(' ')
        && normalized.ends_with(' ')
        && normalized.chars().any(|char| char != ' ')
    {
        normalized[1..normalized.len() - 1].into()
    } else {
        normalized
    }
}

fn can_open_delimited(input: &str, index: usize, marker_len: usize) -> bool {
    delimiter_flanking(input, index, marker_len).left
}

fn can_close_delimited(input: &str, index: usize, marker_len: usize) -> bool {
    delimiter_flanking(input, index, marker_len).right
}

fn find_closing_delimiter(
    input: &str,
    start: usize,
    marker: &str,
    underscore: bool,
) -> Option<usize> {
    let marker_len = marker.len();
    let mut cursor = start;
    let mut nested = 0usize;
    while cursor <= input.len() {
        let candidate = input[cursor..].find(marker).map(|offset| cursor + offset)?;
        if is_escaped_at(input, candidate) {
            cursor = candidate + marker_len;
            continue;
        }
        if delimiter_candidate_precedes_link_close(input, start, candidate, marker_len) {
            cursor = candidate + marker_len;
            continue;
        }
        if marker_len == 1 && nested == 0 && starts_longer_delimiter_run(input, candidate, marker) {
            cursor = candidate + delimiter_run_len(input, candidate, marker);
            continue;
        }

        let can_open = if underscore {
            can_open_underscore(input, candidate, marker_len)
        } else {
            can_open_delimited(input, candidate, marker_len)
        };
        let can_close = if underscore {
            can_close_underscore(input, candidate, marker_len)
        } else {
            can_close_delimited(input, candidate, marker_len)
        };

        if can_close {
            if nested == 0 {
                return Some(candidate);
            }
            nested -= 1;
            cursor = candidate + marker_len;
            continue;
        }
        if can_open {
            nested += 1;
        }
        cursor = candidate + marker_len;
    }
    None
}

fn find_single_tilde_delete_close(input: &str, start: usize) -> Option<usize> {
    let mut cursor = start;
    while cursor < input.len() {
        let Some(candidate) = input[cursor..].find('~').map(|index| cursor + index) else {
            break;
        };
        if !is_escaped_at(input, candidate) && single_tilde_can_close_delete(input, candidate) {
            return Some(candidate);
        }
        cursor = candidate + 1;
    }
    None
}

fn single_tilde_can_open_delete(input: &str, index: usize) -> bool {
    starts_exact_byte_run(input, index, b'~', 1)
        && can_open_delimited(input, index, 1)
        && !tilde_is_alphanumeric_interior(input, index)
}

fn single_tilde_can_close_delete(input: &str, index: usize) -> bool {
    starts_exact_byte_run(input, index, b'~', 1)
        && can_close_delimited(input, index, 1)
        && !tilde_is_alphanumeric_interior(input, index)
}

fn single_tilde_delete_takes_precedence(
    options: &SyntaxOptions,
    input: &str,
    index: usize,
) -> bool {
    options.constructs.gfm_strikethrough
        && options.parse.single_tilde_strikethrough
        && single_tilde_can_open_delete(input, index)
        && find_single_tilde_delete_close(input, index + 1).is_some()
}

fn tilde_is_alphanumeric_interior(input: &str, index: usize) -> bool {
    let previous = input[..index].chars().next_back();
    let next = input[index + 1..].chars().next();
    previous.is_some_and(|char| char.is_alphanumeric())
        && next.is_some_and(|char| char.is_alphanumeric())
}

fn starts_exact_byte_run(input: &str, index: usize, marker: u8, len: usize) -> bool {
    input.as_bytes().get(index) == Some(&marker)
        && delimiter_byte_run_start(input, index, marker) == index
        && delimiter_byte_run_len(input, index, marker) == len
}

fn delimiter_byte_run_start(input: &str, index: usize, marker: u8) -> usize {
    let bytes = input.as_bytes();
    let mut start = index;
    while start > 0 && bytes[start - 1] == marker && !is_escaped_at(input, start - 1) {
        start -= 1;
    }
    start
}

fn delimiter_byte_run_len(input: &str, index: usize, marker: u8) -> usize {
    let bytes = input.as_bytes();
    let mut cursor = index;
    while bytes.get(cursor) == Some(&marker) {
        cursor += 1;
    }
    cursor - index
}

fn find_simple_inline_close(input: &str, start: usize, marker: u8) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut cursor = start;
    while cursor < input.len() {
        match bytes[cursor] {
            b'\\' => {
                cursor += 1;
                if cursor < input.len() {
                    cursor = next_char(input, cursor)?.0;
                }
            }
            b'\n' | b'\r' => return None,
            byte if byte == marker => return (cursor > start).then_some(cursor),
            _ => cursor = next_char(input, cursor)?.0,
        }
    }
    None
}

fn find_spoiler_close(input: &str, start: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut cursor = start;
    while cursor + 1 < input.len() {
        match bytes[cursor] {
            b'\\' => {
                cursor += 1;
                if cursor < input.len() {
                    cursor = next_char(input, cursor)?.0;
                }
            }
            b'\n' | b'\r' => return None,
            b'|' if bytes.get(cursor + 1) == Some(&b'|')
                && cursor > start
                && bytes.get(cursor.wrapping_sub(1)) != Some(&b'|') =>
            {
                return Some(cursor);
            }
            _ => cursor = next_char(input, cursor)?.0,
        }
    }
    None
}

fn starts_longer_delimiter_run(input: &str, index: usize, marker: &str) -> bool {
    input[index..].starts_with(marker)
        && !input[..index].ends_with(marker)
        && input[index + marker.len()..].starts_with(marker)
}

fn delimiter_run_len(input: &str, index: usize, marker: &str) -> usize {
    let mut cursor = index;
    while input[cursor..].starts_with(marker) {
        cursor += marker.len();
    }
    cursor - index
}

fn delimiter_candidate_precedes_link_close(
    input: &str,
    start: usize,
    candidate: usize,
    marker_len: usize,
) -> bool {
    let bytes = input.as_bytes();
    if bytes.get(candidate + marker_len) != Some(&b']') {
        return false;
    }
    if !matches!(bytes.get(candidate + marker_len + 1), Some(b'(' | b'[')) {
        return false;
    }

    let mut depth = 0usize;
    let mut cursor = start;
    while cursor < candidate {
        let Some((next, char)) = next_char(input, cursor) else {
            break;
        };
        match char {
            '\\' => {
                cursor = next_char(input, next)
                    .map(|(after_escape, _)| after_escape)
                    .unwrap_or(next);
                continue;
            }
            '`' => {
                if let Some((end, _)) = parse_code_span(input, cursor) {
                    cursor = end;
                    continue;
                }
            }
            '[' => depth += 1,
            ']' => depth = depth.saturating_sub(1),
            _ => {}
        }
        cursor = next;
    }
    depth > 0
}

fn can_open_underscore(input: &str, index: usize, marker_len: usize) -> bool {
    let flanking = delimiter_flanking(input, index, marker_len);
    flanking.left
        && (!flanking.right || flanking.previous.is_some_and(|c| c.is_ascii_punctuation()))
}

fn can_close_underscore(input: &str, index: usize, marker_len: usize) -> bool {
    let flanking = delimiter_flanking(input, index, marker_len);
    flanking.right && (!flanking.left || flanking.next.is_some_and(|c| c.is_ascii_punctuation()))
}

#[derive(Clone, Copy)]
struct DelimiterFlanking {
    left: bool,
    right: bool,
    previous: Option<char>,
    next: Option<char>,
}

fn delimiter_flanking(input: &str, index: usize, marker_len: usize) -> DelimiterFlanking {
    let previous = input[..index].chars().next_back();
    let next = input[index + marker_len..].chars().next();

    let previous_whitespace = previous.is_none_or(char::is_whitespace);
    let next_whitespace = next.is_none_or(char::is_whitespace);
    let previous_punctuation = previous.is_some_and(is_flanking_punctuation);
    let next_punctuation = next.is_some_and(is_flanking_punctuation);

    let left = next.is_some()
        && !next_whitespace
        && !(next_punctuation && !previous_whitespace && !previous_punctuation);
    let right = previous.is_some()
        && !previous_whitespace
        && !(previous_punctuation && !next_whitespace && !next_punctuation);

    DelimiterFlanking {
        left,
        right,
        previous,
        next,
    }
}

/// Dollar-fenced inline math, GitHub Flavored Markdown dialect.
///
/// A `$` is a flanking delimiter resolved at scan time (math is not pushed onto
/// the emphasis delimiter stack). An opening run of one or two `$` (runs of
/// three or more never form math) scans forward for a matching closing run:
///
/// * single `$`: cannot open if the next char is ASCII whitespace; the closing
///   `$` cannot be preceded by ASCII whitespace nor followed by an ASCII digit;
///   a `\$` inside is skipped (the backslash is kept verbatim, never a
///   delimiter); the close must be a run of exactly one `$`.
/// * double `$$`: no flanking and no digit guard; closes on the next run of two
///   `$`; content is kept verbatim and may span newlines (this is still an
///   inline display span — `$$` flow blocks are handled by `parse_math_block`).
///
/// The closing run is matched greedily (the nearest valid close wins), which is
/// equivalent to emphasis-style "nearest preceding open" because a failed open
/// emits a literal `$`/`$$` and the scan resumes after it. Content for the
/// single-`$` form is normalized like a code span (line endings → spaces, one
/// edge-space strip); the `$$` display form is verbatim. The `` $`…`$ `` code
/// form takes precedence.
fn parse_math_inline(input: &str, index: usize) -> Option<(usize, String, MathInlineKind)> {
    if let Some((end, value)) = parse_math_code_inline(input, index) {
        return Some((end, value, MathInlineKind::Code));
    }

    let bytes = input.as_bytes();
    let open_dollars = bytes[index..]
        .iter()
        .take_while(|byte| **byte == b'$')
        .count();
    // The maximum math fence length is 2 dollars: a run of three or more never
    // opens math.
    if open_dollars == 0 || open_dollars > 2 {
        return None;
    }

    let content_start = index + open_dollars;
    let close = scan_to_closing_dollar(input, content_start, open_dollars)?;
    let content_end = close - open_dollars;
    // The span requires `endpos - startpos >= fence_length * 2 + 1`, i.e. at
    // least one content byte between the open and close fences.
    if content_end <= content_start {
        return None;
    }

    let raw = &input[content_start..content_end];
    let value = if open_dollars == 1 {
        normalize_math_text(raw)
    } else {
        raw.into()
    };
    let dollars = u8::try_from(open_dollars).unwrap_or(u8::MAX);
    Some((close, value, MathInlineKind::Dollar { dollars }))
}

/// Scans for the closing dollar run. `start` is the first content byte
/// (just past the opening run); returns the byte offset just past a matching
/// closing run of exactly `open_dollars` `$`.
fn scan_to_closing_dollar(input: &str, start: usize, open_dollars: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    // A space immediately after a single opening `$` forbids the open.
    if open_dollars == 1 && bytes.get(start).is_some_and(|byte| is_math_space(*byte)) {
        return None;
    }

    let mut cursor = start;
    loop {
        while cursor < bytes.len() && bytes[cursor] != b'$' {
            cursor += 1;
        }
        if cursor >= bytes.len() {
            return None;
        }
        // `cursor` now points at the first `$` of a potential closing run; the
        // char just before it gates the single-`$` flanking and escape rules.
        let prev = bytes[cursor - 1];
        if open_dollars == 1 && is_math_space(prev) {
            return None;
        }
        if open_dollars == 1 && prev == b'\\' {
            // An escaped `\$` is content, not a delimiter: skip this one `$` and
            // keep scanning (the backslash stays in the content verbatim).
            cursor += 1;
            continue;
        }
        let run = bytes[cursor..]
            .iter()
            .take(open_dollars)
            .take_while(|byte| **byte == b'$')
            .count();
        // The single-`$` close cannot be followed by an ASCII digit.
        if open_dollars == 1 && bytes.get(cursor + run).is_some_and(u8::is_ascii_digit) {
            return None;
        }
        if run == open_dollars {
            return Some(cursor + run);
        }
        cursor += run;
    }
}

/// Math whitespace: ASCII tab, line feed, carriage return, and space.
fn is_math_space(byte: u8) -> bool {
    matches!(byte, b'\t' | b'\n' | b'\r' | b' ')
}

/// Applies the code-span content rules to dollar-fenced math: line endings
/// become single spaces, then if the content begins AND ends with U+0020 and is
/// not entirely spaces, one space is stripped from each edge.
fn normalize_math_text(input: &str) -> String {
    let mut normalized = String::new();
    let mut cursor = 0;
    while cursor < input.len() {
        let (next, char) = next_char(input, cursor).expect("valid UTF-8 byte index");
        if char == '\r' {
            if input.as_bytes().get(next) == Some(&b'\n') {
                cursor = next + 1;
            } else {
                cursor = next;
            }
            normalized.push(' ');
            continue;
        }
        if char == '\n' {
            normalized.push(' ');
            cursor = next;
            continue;
        }
        normalized.push(char);
        cursor = next;
    }

    if normalized.starts_with(' ')
        && normalized.ends_with(' ')
        && normalized.chars().any(|char| char != ' ')
    {
        normalized[1..normalized.len() - 1].into()
    } else {
        normalized
    }
}

fn parse_math_code_inline(input: &str, index: usize) -> Option<(usize, String)> {
    if !input[index..].starts_with("$`") {
        return None;
    }

    let search_start = index + 2;
    let close = input[search_start..]
        .find("`$")
        .map(|offset| search_start + offset)?;
    if close == search_start {
        return None;
    }

    Some((close + 2, input[search_start..close].into()))
}

fn parse_link_resource(input: &str, open: usize) -> Option<(usize, ParsedLinkResource)> {
    let bytes = input.as_bytes();
    if bytes.get(open) != Some(&b'(') {
        return None;
    }
    let (mut cursor, initial_space) = skip_link_resource_space_with_info(input, open + 1)?;
    if bytes.get(cursor) == Some(&b')') {
        return Some((
            cursor + 1,
            ParsedLinkResource {
                destination: String::new(),
                destination_kind: LinkDestinationKind::Omitted,
                title: None,
                title_kind: None,
            },
        ));
    }
    if initial_space && matches!(bytes.get(cursor), Some(b'"' | b'\'' | b'(')) {
        let (title, title_kind, next) = parse_link_title(input, cursor)?;
        cursor = skip_link_resource_space(input, next)?;
        if bytes.get(cursor) == Some(&b')') {
            return Some((
                cursor + 1,
                ParsedLinkResource {
                    destination: String::new(),
                    destination_kind: LinkDestinationKind::Omitted,
                    title: Some(title),
                    title_kind: Some(title_kind),
                },
            ));
        }
        return None;
    }
    let (destination, destination_kind, next) = parse_link_destination(input, cursor)?;
    let (after_destination, had_space) = skip_link_resource_space_with_info(input, next)?;
    cursor = after_destination;
    if bytes.get(cursor) == Some(&b')') {
        return Some((
            cursor + 1,
            ParsedLinkResource {
                destination,
                destination_kind,
                title: None,
                title_kind: None,
            },
        ));
    }
    if !had_space {
        return None;
    }

    let (title, title_kind, next) = parse_link_title(input, cursor)?;
    cursor = skip_link_resource_space(input, next)?;
    if bytes.get(cursor) == Some(&b')') {
        Some((
            cursor + 1,
            ParsedLinkResource {
                destination,
                destination_kind,
                title: Some(title),
                title_kind: Some(title_kind),
            },
        ))
    } else {
        None
    }
}

fn parse_link_destination(
    input: &str,
    index: usize,
) -> Option<(String, LinkDestinationKind, usize)> {
    if input.as_bytes().get(index) == Some(&b'<') {
        let mut cursor = index + 1;
        while cursor < input.len() {
            let (next, char) = next_char(input, cursor)?;
            if char == '>' && !is_escaped_at(input, cursor) {
                return Some((
                    unescape_ascii_punctuation(&input[index + 1..cursor]),
                    LinkDestinationKind::Angle,
                    next,
                ));
            }
            if (char == '<' && !is_escaped_at(input, cursor)) || char == '\n' || char == '\r' {
                return None;
            }
            cursor = next;
        }
        return None;
    }

    let mut cursor = index;
    let mut depth = 0usize;
    while cursor < input.len() {
        let (next, char) = next_char(input, cursor)?;
        // A bare destination terminates on ASCII space or an ASCII control
        // character; Unicode whitespace (e.g. U+00A0) is ordinary. A backslash
        // before a space is NOT an escape (only ASCII punctuation is escapable),
        // so `\ ` still terminates the destination → `[a](\ b)` is not a link.
        if (char == ' ' || char.is_ascii_control()) && depth == 0 {
            break;
        }
        if char == '(' && !is_escaped_at(input, cursor) {
            depth += 1;
            // CommonMark caps balanced parens in a bare destination at depth 32.
            if depth > 32 {
                return None;
            }
        } else if char == ')' && !is_escaped_at(input, cursor) {
            if depth == 0 {
                break;
            }
            depth -= 1;
        }
        cursor = next;
    }

    if cursor == index || depth > 0 {
        None
    } else {
        Some((
            unescape_ascii_punctuation(&input[index..cursor]),
            LinkDestinationKind::Bare,
            cursor,
        ))
    }
}

fn parse_link_title(input: &str, index: usize) -> Option<(String, LinkTitleKind, usize)> {
    let opener = input.as_bytes().get(index).copied()?;
    let (closer, title_kind) = match opener {
        b'"' => ('"', LinkTitleKind::DoubleQuote),
        b'\'' => ('\'', LinkTitleKind::SingleQuote),
        b'(' => (')', LinkTitleKind::Paren),
        _ => return None,
    };
    let mut cursor = index + 1;
    while cursor < input.len() {
        let (next, char) = next_char(input, cursor)?;
        if char == closer && !is_escaped_at(input, cursor) {
            if contains_blank_line(&input[index + 1..cursor]) {
                return None;
            }
            return Some((
                unescape_ascii_punctuation(&input[index + 1..cursor]),
                title_kind,
                next,
            ));
        }
        if opener == b'(' && char == '(' && !is_escaped_at(input, cursor) {
            return None;
        }
        cursor = next;
    }
    None
}

fn contains_blank_line(input: &str) -> bool {
    if !input.bytes().any(|byte| matches!(byte, b'\n' | b'\r')) {
        return false;
    }
    // A title that merely begins or ends with an EOL is allowed; only an INTERIOR
    // blank line (a blank line bounded by content on both sides) is rejected. The
    // empty first/last line entries that a leading/trailing newline produces are
    // boundary artifacts, not blank lines in the title content.
    let lines = collect_lines(input, 0);
    let interior = lines.len().saturating_sub(1);
    lines
        .iter()
        .take(interior)
        .skip(1)
        .any(|line| line.text.trim().is_empty())
}

fn skip_link_resource_space(input: &str, index: usize) -> Option<usize> {
    skip_link_resource_space_with_info(input, index).map(|(index, _)| index)
}

fn skip_link_resource_space_with_info(input: &str, mut index: usize) -> Option<(usize, bool)> {
    let mut line_breaks = 0usize;
    let mut had_space = false;
    while input
        .as_bytes()
        .get(index)
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t' | b'\n' | b'\r'))
    {
        had_space = true;
        match input.as_bytes()[index] {
            b'\n' => {
                line_breaks += 1;
                if line_breaks > 1 {
                    return None;
                }
                index += 1;
            }
            b'\r' => {
                line_breaks += 1;
                if line_breaks > 1 {
                    return None;
                }
                if input.as_bytes().get(index + 1) == Some(&b'\n') {
                    index += 2;
                } else {
                    index += 1;
                }
            }
            _ => index += 1,
        }
    }
    Some((index, had_space))
}

pub(crate) fn parse_character_reference(input: &str, index: usize) -> Option<(usize, String)> {
    let rest = input.get(index..)?;
    if let Some(rest) = rest
        .strip_prefix("&#x")
        .or_else(|| rest.strip_prefix("&#X"))
    {
        let digits = rest.find(';')?;
        if digits == 0 || digits > 6 || !rest[..digits].bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return None;
        }
        let value = u32::from_str_radix(&rest[..digits], 16).ok()?;
        return Some((
            index + 3 + digits + 1,
            character_reference_value(value).into(),
        ));
    }
    if let Some(rest) = rest.strip_prefix("&#") {
        let digits = rest.find(';')?;
        if digits == 0 || digits > 7 || !rest[..digits].bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        let value = rest[..digits].parse::<u32>().ok()?;
        return Some((
            index + 2 + digits + 1,
            character_reference_value(value).into(),
        ));
    }

    let name_end = rest.find(';')?;
    if name_end == 0 || name_end > 32 {
        return None;
    }
    let name = &rest[1..name_end];
    named_character_reference(name).map(|value| (index + name_end + 1, value.into()))
}

/// Decode a numeric character reference codepoint to its scalar value.
///
/// This follows the CommonMark reference behavior: `U+0000`, the UTF-16
/// surrogate range, and codepoints beyond the Unicode scalar range decode to
/// `U+FFFD`; every other codepoint decodes to itself.
///
/// Two deliberate non-behaviors:
/// - We do NOT apply the HTML5 Windows-1252 remapping of C1 bytes; `&#128;`
///   decodes to `U+0080`, not the Euro sign. The CommonMark reference does not
///   perform that remapping.
/// - We do NOT extend replacement to the C0/C1 controls, DEL, or the Unicode
///   noncharacters the way some HTML-oriented decoders do. Keeping those as
///   their literal scalar is what makes the serializer's `&#xNN;` escaping of
///   control characters round-trip through a re-parse. The roundtrip corpus
///   only pins `{0 -> FFFD, 9 -> tab, 10 -> line feed, surrogate -> FFFD,
///   out-of-range -> FFFD}`, all of which this matches.
pub(crate) fn character_reference_value(value: u32) -> char {
    if value == 0 {
        '\u{FFFD}'
    } else {
        char::from_u32(value).unwrap_or('\u{FFFD}')
    }
}

pub(crate) fn is_escaped_at(input: &str, index: usize) -> bool {
    let bytes = input.as_bytes();
    let mut cursor = index;
    let mut count = 0;
    while cursor > 0 && bytes[cursor - 1] == b'\\' {
        count += 1;
        cursor -= 1;
    }
    count % 2 == 1
}

fn parse_definition_destination_title(input: &str) -> Option<ParsedLinkResource> {
    let (mut cursor, _) = skip_link_resource_space_with_info(input, 0)?;
    let (destination, destination_kind, next) = parse_link_destination(input, cursor)?;
    cursor = next;

    let (next, had_space) = skip_link_resource_space_with_info(input, cursor)?;
    cursor = next;
    if cursor >= input.len() {
        return Some(ParsedLinkResource {
            destination,
            destination_kind,
            title: None,
            title_kind: None,
        });
    }
    if !had_space {
        return None;
    }

    let (title, title_kind, next) = parse_link_title(input, cursor)?;
    let after_title = skip_link_resource_space(input, next)?;
    (after_title == input.len()).then_some(ParsedLinkResource {
        destination,
        destination_kind,
        title: Some(title),
        title_kind: Some(title_kind),
    })
}

fn line_can_start_definition_title(input: &str) -> bool {
    let trimmed = input.trim_start();
    matches!(trimmed.as_bytes().first(), Some(b'"' | b'\'' | b'('))
}

fn unescape_ascii_punctuation(input: &str) -> String {
    // Only ASCII punctuation is escapable (`\ ` keeps its backslash).
    unescape_selected(input, |char| char.is_ascii_punctuation())
}

fn unescape_string(input: &str) -> String {
    unescape_selected(input, |char| char.is_ascii_punctuation() || char == '&')
}

fn unescape_selected(input: &str, should_unescape: impl Fn(char) -> bool) -> String {
    let mut output = String::new();
    let mut cursor = 0;
    while cursor < input.len() {
        if input.as_bytes().get(cursor) == Some(&b'&') {
            if let Some((end, value)) = parse_character_reference(input, cursor) {
                output.push_str(&value);
                cursor = end;
                continue;
            }
        }
        let (next, char) = next_char(input, cursor).expect("valid UTF-8 byte index");
        if char == '\\' {
            if let Some((after_escape, escaped)) = next_char(input, next) {
                if should_unescape(escaped) {
                    output.push(escaped);
                } else {
                    output.push(char);
                    output.push(escaped);
                }
                cursor = after_escape;
            } else {
                output.push(char);
                cursor = next;
            }
        } else {
            output.push(if char == '\0' { '\u{FFFD}' } else { char });
            cursor = next;
        }
    }
    output
}

fn push_line(output: &mut String, line: &str) {
    if !output.is_empty() {
        output.push('\n');
    }
    output.push_str(line);
}

fn ensure_line_separator(output: &mut String) {
    if !output.is_empty() && !ends_with_line_ending(output) {
        output.push('\n');
    }
}

fn ends_with_line_ending(input: &str) -> bool {
    input.ends_with('\n') || input.ends_with('\r')
}

fn flush_text(nodes: &mut Vec<Inline>, text: &mut String, text_start: usize, end: usize) {
    if !text.is_empty() {
        nodes.push(Inline::Text(Text {
            meta: NodeMeta::new(Some(Span::new(text_start, end))),
            value: core::mem::take(text),
        }));
    }
}

fn gfm_link_label_preserves_url_dot_escape(
    text: &str,
    escaped: char,
    options: &SyntaxOptions,
    context: InlineContext,
) -> bool {
    escaped == '.'
        && !context.allow_links
        && options.constructs.gfm_autolink_literal
        && (text.starts_with("www.") || text.starts_with("http://") || text.starts_with("https://"))
}

fn next_char(input: &str, index: usize) -> Option<(usize, char)> {
    let char = input[index..].chars().next()?;
    Some((index + char.len_utf8(), char))
}

/// A CommonMark "Unicode punctuation character" for emphasis/strong flanking:
/// ASCII punctuation plus the non-ASCII Unicode `P*`/`S*` categories. Only the
/// flanking classification needs the Unicode set; escape/label logic stays
/// ASCII-only via `char::is_ascii_punctuation`.
fn is_flanking_punctuation(value: char) -> bool {
    value.is_ascii_punctuation() || crate::unicode_punctuation::is_unicode_punctuation(value)
}

/// Fold a reference label to its matching identifier. Per CommonMark, two
/// labels match when their RAW source (no backslash unescape, no entity decode)
/// agrees after collapsing internal whitespace to a single space, trimming, and
/// Unicode case-folding (`to_uppercase()` then `to_lowercase()`). So `[foo\!]`
/// does NOT match `[foo!]`, and `[&copy;]` does NOT match `[©]`.
///
/// The serializer's `normalize_reference_label` delegates here so the
/// Shortcut/Collapsed omission oracle stays in lockstep with this matcher.
pub(crate) fn normalize_label(label: &str) -> String {
    label
        // Unicode full casefold maps capital sharp S (ẞ, U+1E9E) to "ss"; Rust's
        // `to_uppercase` leaves it unchanged (it is already uppercase), so without
        // this `[ẞ]` would not match a `[SS]: …` definition (links 540). This is
        // the only char where `to_uppercase().to_lowercase()` diverges from the
        // full casefold that matters for label matching.
        .replace('ẞ', "ss")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_uppercase()
        .to_lowercase()
}

fn definition_exists(definitions: &[String], label: &str) -> bool {
    if label.is_empty() || !reference_label_is_within_limit(label) {
        return false;
    }

    let identifier = normalize_label(label);
    definitions
        .iter()
        .any(|definition| definition == &identifier)
}

fn reference_label_is_within_limit(label: &str) -> bool {
    label.chars().take(REFERENCE_LABEL_MAX_CHARS + 1).count() <= REFERENCE_LABEL_MAX_CHARS
}

fn trim_up_to_three_spaces(input: &str) -> Option<&str> {
    let (columns, bytes) = leading_indent(input);
    if columns <= 3 {
        Some(&input[bytes..])
    } else {
        None
    }
}

fn fence_start(input: &str) -> Option<(FenceMarker, usize)> {
    let marker = match input.as_bytes().first()? {
        b'`' => FenceMarker::Backtick,
        b'~' => FenceMarker::Tilde,
        _ => return None,
    };
    let byte = match marker {
        FenceMarker::Backtick => b'`',
        FenceMarker::Tilde => b'~',
    };
    let length = input
        .as_bytes()
        .iter()
        .take_while(|item| **item == byte)
        .count();
    if length >= 3 {
        Some((marker, length))
    } else {
        None
    }
}

fn fence_close(input: &str, marker: FenceMarker, length: usize) -> bool {
    let byte = match marker {
        FenceMarker::Backtick => b'`',
        FenceMarker::Tilde => b'~',
    };
    let count = input
        .as_bytes()
        .iter()
        .take_while(|item| **item == byte)
        .count();
    count >= length && input[count..].trim().is_empty()
}

fn trim_closing_hashes(input: &str) -> &str {
    let input = input.trim_end();
    let hash_start = input.trim_end_matches('#').len();
    if hash_start == input.len() {
        return input;
    }
    if hash_start == 0 {
        return "";
    }

    let before = &input[..hash_start];
    if before.ends_with(' ') || before.ends_with('\t') {
        before.trim_end()
    } else {
        input
    }
}

fn list_marker_info(input: &str) -> Option<ListMarkerInfo<'_>> {
    let trimmed = trim_up_to_three_spaces(input)?;
    let indent = input.len() - trimmed.len();
    let bytes = trimmed.as_bytes();
    match bytes.first()? {
        b'-' | b'*' | b'+' if is_list_padding_byte(bytes.get(1).copied()) => {
            let delimiter = match bytes[0] {
                b'-' => ListDelimiter::Dash,
                b'*' => ListDelimiter::Asterisk,
                _ => ListDelimiter::Plus,
            };
            let (content_offset, content_indent) = list_content_offset(trimmed, 1, indent);
            Some(ListMarkerInfo {
                ordered: false,
                start: None,
                delimiter,
                indent,
                marker_len: 1,
                content_indent,
                content: &trimmed[content_offset..],
            })
        }
        byte if byte.is_ascii_digit() => {
            let mut end = 0;
            while bytes.get(end).is_some_and(|byte| byte.is_ascii_digit()) {
                end += 1;
            }
            if end > 9 {
                return None;
            }
            let delimiter = match bytes.get(end)? {
                b'.' => ListDelimiter::Period,
                b')' => ListDelimiter::Paren,
                _ => return None,
            };
            if !is_list_padding_byte(bytes.get(end + 1).copied()) {
                return None;
            }
            let start = trimmed[..end].parse().ok()?;
            let marker_len = end + 1;
            let (content_offset, content_indent) = list_content_offset(trimmed, marker_len, indent);
            Some(ListMarkerInfo {
                ordered: true,
                start: Some(start),
                delimiter,
                indent,
                marker_len,
                content_indent,
                content: &trimmed[content_offset..],
            })
        }
        _ => None,
    }
}

fn list_content_offset(input: &str, marker_len: usize, indent: usize) -> (usize, usize) {
    let bytes = input.as_bytes();
    if bytes.get(marker_len).is_none() {
        return (marker_len, indent + marker_len + 1);
    }
    let mut cursor = marker_len;
    let mut column = indent + marker_len;
    let marker_end_column = column;
    while let Some(byte) = bytes.get(cursor) {
        match *byte {
            b' ' => column += 1,
            b'\t' => column += 4 - (column % 4),
            _ => break,
        }
        cursor += 1;
    }
    // The line is the marker followed only by whitespace: an empty item whose
    // first line is blank. CommonMark §5.2 fixes its content indent at marker
    // width + 1 regardless of how many trailing spaces follow, so content on the
    // next line indented one column past the marker joins the item.
    if cursor >= bytes.len() {
        return (cursor, marker_end_column + 1);
    }
    let padding_columns = column.saturating_sub(marker_end_column);
    if padding_columns > 0 && padding_columns <= 4 {
        (cursor, column)
    } else {
        (marker_len + 1, marker_end_column + 1)
    }
}

fn list_marker_first_content<'a>(input: &'a str, marker: ListMarkerInfo<'a>) -> Cow<'a, str> {
    let Some(trimmed) = trim_up_to_three_spaces(input) else {
        return Cow::Borrowed(marker.content);
    };
    let after_marker = &trimmed[marker.marker_len..];
    if after_marker.starts_with('\t') {
        strip_leading_indent_columns_from(after_marker, 1, marker.indent + marker.marker_len)
    } else {
        Cow::Borrowed(marker.content)
    }
}

fn is_list_padding_byte(byte: Option<u8>) -> bool {
    matches!(byte, None | Some(b' ' | b'\t'))
}

fn same_list_marker(left: ListMarkerInfo<'_>, right: ListMarkerInfo<'_>) -> bool {
    // CommonMark §5.3: list items belong to the same list when they share a
    // bullet character or ordered delimiter. Indentation does not enter into
    // it — `- foo\n - bar\n  - baz` is one four-item bullet list, not three.
    left.ordered == right.ordered && left.delimiter == right.delimiter
}

/// Whether `input` begins a *sibling* item of the current list item.
///
/// A same-delimiter marker is a sibling only when it is not indented far enough
/// to nest inside the current item — i.e. its indent is less than the item's
/// `content_indent`. A marker indented at or beyond the content start belongs to
/// a sublist within the item and is consumed as item content instead.
fn sibling_list_marker_at_line(
    input: &str,
    first_marker: ListMarkerInfo<'_>,
    content_indent: usize,
) -> bool {
    list_marker_info(input).is_some_and(|candidate| {
        same_list_marker(first_marker, candidate) && candidate.indent < content_indent
    })
}

/// Whether `input` begins a list marker belonging to the same list as
/// `first_marker` (same ordered/unordered kind and delimiter). Used to tell a
/// marker that merely continues the current list apart from one that, by
/// changing the marker type, starts a new list (CommonMark §5.3).
fn same_list_marker_line(input: &str, first_marker: ListMarkerInfo<'_>) -> bool {
    list_marker_info(input).is_some_and(|candidate| same_list_marker(first_marker, candidate))
}

fn next_nonblank_line(lines: &[Line<'_>], mut index: usize) -> usize {
    while index < lines.len() && lines[index].text.trim().is_empty() {
        index += 1;
    }
    index
}

fn leading_indent(input: &str) -> (usize, usize) {
    let mut column = 0usize;
    let mut bytes = 0usize;
    for byte in input.as_bytes() {
        match *byte {
            b' ' => column += 1,
            b'\t' => column += 4 - (column % 4),
            _ => break,
        }
        bytes += 1;
    }
    (column, bytes)
}

fn leading_indent_columns(input: &str) -> usize {
    leading_indent(input).0
}

/// Removes up to `max_columns` columns of leading whitespace, stopping at the
/// first non-space/tab byte (tabs advance to the next 4-column tab stop). A tab
/// that straddles the column budget is PARTIALLY consumed: the columns beyond the
/// budget are re-emitted as spaces (CommonMark tab-expansion of indentation), so
/// the result may be an owned `String`. Whitespace already at/over the budget
/// (and any literal tab whose start sits at the budget) is returned verbatim.
fn strip_leading_indent_columns(input: &str, max_columns: usize) -> Cow<'_, str> {
    strip_leading_indent_columns_from(input, max_columns, 0)
}

fn strip_leading_indent_columns_from(
    input: &str,
    max_columns: usize,
    start_column: usize,
) -> Cow<'_, str> {
    let mut column = start_column;
    let target_column = start_column + max_columns;
    for (index, byte) in input.as_bytes().iter().enumerate() {
        let next = match *byte {
            b' ' => column + 1,
            b'\t' => column + (4 - (column % 4)),
            _ => return Cow::Borrowed(&input[index..]),
        };
        if next > target_column {
            // A tab whose expansion crosses the budget (its start still inside the
            // budget) is split: the over-budget columns survive as spaces.
            if *byte == b'\t' && column < target_column {
                let residual = next - target_column;
                let mut owned = String::with_capacity(residual + input.len() - (index + 1));
                for _ in 0..residual {
                    owned.push(' ');
                }
                let mut rest_column = next;
                let mut rest_index = index + 1;
                while let Some(rest_byte) = input.as_bytes().get(rest_index) {
                    match *rest_byte {
                        b' ' => {
                            owned.push(' ');
                            rest_column += 1;
                            rest_index += 1;
                        }
                        b'\t' => {
                            let width = 4 - (rest_column % 4);
                            for _ in 0..width {
                                owned.push(' ');
                            }
                            rest_column += width;
                            rest_index += 1;
                        }
                        _ => break,
                    }
                }
                owned.push_str(&input[rest_index..]);
                return Cow::Owned(owned);
            }
            return Cow::Borrowed(&input[index..]);
        }
        column = next;
    }
    Cow::Borrowed("")
}

fn strip_list_continuation(input: &str, content_indent: usize, list_indent: usize) -> Cow<'_, str> {
    let (indent_columns, indent_bytes) = leading_indent(input);
    if indent_columns >= content_indent {
        // Remove exactly `content_indent` columns. A tab straddling that budget
        // is split: the columns past the budget survive as spaces (CommonMark
        // tab expansion of list-item indentation), so a `\t`-only line inside a
        // 2-column item keeps the residual two spaces instead of vanishing.
        strip_leading_indent_columns(input, content_indent)
    } else if indent_columns > list_indent {
        Cow::Borrowed(&input[indent_bytes..])
    } else {
        Cow::Borrowed(trim_ascii_start(input))
    }
}

fn take_task_marker_from_children(children: &mut [Block]) -> Option<bool> {
    let Some(Block::Paragraph(paragraph)) = children.first_mut() else {
        return None;
    };
    take_task_marker_from_inlines(&mut paragraph.children)
}

fn take_task_marker_from_inlines(inlines: &mut Vec<Inline>) -> Option<bool> {
    let Some(Inline::Text(text)) = inlines.first() else {
        return None;
    };
    let first = text.value.clone();

    if let Some((checked, consumed)) = task_marker_inline_prefix(&first) {
        if !first[consumed..].is_empty() || inlines_have_content_after(inlines, 1) {
            remove_text_prefix(inlines, consumed);
            return Some(checked);
        }
    }

    if let Some(checked) = task_marker_at_text_end(&first) {
        if inlines
            .get(1)
            .is_some_and(|inline| matches!(inline, Inline::SoftBreak(_)))
            && inlines_have_content_after(inlines, 2)
        {
            inlines.remove(1);
            inlines.remove(0);
            return Some(checked);
        }
    }

    if task_marker_split_open(&first)
        && inlines
            .get(1)
            .is_some_and(|inline| matches!(inline, Inline::SoftBreak(_)))
    {
        let Some(Inline::Text(next)) = inlines.get(2) else {
            return None;
        };
        if let Some((checked, consumed)) = task_marker_split_close_prefix(&next.value) {
            if !next.value[consumed..].is_empty() || inlines_have_content_after(inlines, 3) {
                inlines.remove(1);
                inlines.remove(0);
                remove_text_prefix(inlines, consumed);
                return Some(checked);
            }
        }
    }

    None
}

fn task_marker_inline_prefix(input: &str) -> Option<(bool, usize)> {
    let start = leading_trim_bytes(input);
    let rest = &input[start..];
    let checked = task_marker_checked(rest)?;
    let after_marker = start + 3;
    match input.as_bytes().get(after_marker) {
        Some(b' ' | b'\t') => Some((checked, after_marker + 1)),
        _ => None,
    }
}

fn task_marker_at_text_end(input: &str) -> Option<bool> {
    let start = leading_trim_bytes(input);
    let rest = &input[start..];
    let checked = task_marker_checked(rest)?;
    if rest.len() == 3 { Some(checked) } else { None }
}

fn task_marker_split_open(input: &str) -> bool {
    let start = leading_trim_bytes(input);
    input[start..] == *"["
}

fn task_marker_split_close_prefix(input: &str) -> Option<(bool, usize)> {
    match input.as_bytes().get(..2)? {
        b"] " => Some((false, 2)),
        b"]\t" => Some((false, 2)),
        b"x]" | b"X]" if matches!(input.as_bytes().get(2), Some(b' ' | b'\t')) => Some((true, 3)),
        _ => None,
    }
}

fn task_marker_checked(input: &str) -> Option<bool> {
    if input.starts_with("[ ]") {
        Some(false)
    } else if input.starts_with("[x]") || input.starts_with("[X]") {
        Some(true)
    } else {
        None
    }
}

fn remove_text_prefix(inlines: &mut Vec<Inline>, consumed: usize) {
    if let Some(Inline::Text(text)) = inlines.first_mut() {
        text.value = text.value[consumed..].into();
        if text.value.is_empty() {
            inlines.remove(0);
        }
    }
}

fn inlines_have_content_after(inlines: &[Inline], start: usize) -> bool {
    inlines.iter().skip(start).any(|inline| match inline {
        Inline::Text(text) => !text.value.is_empty(),
        Inline::SoftBreak(_) | Inline::LineBreak(_) => false,
        _ => true,
    })
}

fn update_list_item_fence(line: &str, open_fence: &mut Option<(FenceMarker, usize)>) {
    let Some(trimmed) = trim_up_to_three_spaces(line) else {
        return;
    };
    if let Some((marker, length)) = *open_fence {
        if fence_close(trimmed, marker, length) {
            *open_fence = None;
        }
        return;
    }
    if let Some((marker, length)) = fence_start(trimmed) {
        *open_fence = Some((marker, length));
    }
}

fn trim_ascii_start(input: &str) -> &str {
    input.trim_start_matches(|char| matches!(char, ' ' | '\t'))
}

fn leading_trim_bytes(input: &str) -> usize {
    input.len() - trim_ascii_start(input).len()
}

fn parse_table_delimiter(input: &str, spoiler: bool) -> Option<Vec<TableAlignment>> {
    let cells = split_table_row(input, spoiler);
    if cells.is_empty() {
        return None;
    }
    let mut alignments = Vec::new();
    for cell in cells {
        alignments.push(table_delimiter_alignment(cell.trim())?);
    }
    Some(alignments)
}

// A delimiter cell is `:?` `-`+ `:?` once trimmed: colons only at the
// boundaries, the dashes contiguous, no interior space or colon.
fn table_delimiter_alignment(cell: &str) -> Option<TableAlignment> {
    let bytes = cell.as_bytes();
    let mut cursor = 0;
    let left = bytes.first() == Some(&b':');
    if left {
        cursor += 1;
    }
    let dash_start = cursor;
    while bytes.get(cursor) == Some(&b'-') {
        cursor += 1;
    }
    if cursor == dash_start {
        return None;
    }
    let right = bytes.get(cursor) == Some(&b':');
    if right {
        cursor += 1;
    }
    if cursor != bytes.len() {
        return None;
    }
    Some(match (left, right) {
        (true, true) => TableAlignment::Center,
        (true, false) => TableAlignment::Left,
        (false, true) => TableAlignment::Right,
        (false, false) => TableAlignment::None,
    })
}

/// Normalizes a table line's leading indentation: when indented code is enabled
/// a four-space indent would start a code block, so up to three leading spaces
/// are trimmed and four or more disqualifies the line.
fn table_indent_line(input: &str, indented_code: bool) -> Option<&str> {
    if indented_code {
        trim_up_to_three_spaces(input)
    } else {
        Some(input)
    }
}

// True if a backtick run of `length` at `start` has a matching-length closing
// run later in `input`. The table row scanner still treats unescaped pipes as
// cell boundaries; this state only prevents extension syntax such as spoilers
// from being recognized inside a code span.
fn backtick_run_has_close(input: &str, start: usize, length: usize) -> bool {
    let bytes = input.as_bytes();
    let mut i = start + length;
    while i < input.len() {
        if bytes[i] == b'`' {
            let run = input[i..]
                .as_bytes()
                .iter()
                .take_while(|byte| **byte == b'`')
                .count();
            if run == length {
                return true;
            }
            i += run;
        } else {
            i += 1;
        }
    }
    false
}

fn table_backslash_pipe_run(input: &str, cursor: usize) -> Option<(usize, bool)> {
    let bytes = input.as_bytes();
    if bytes.get(cursor) != Some(&b'\\') {
        return None;
    }
    let mut pipe = cursor;
    while bytes.get(pipe) == Some(&b'\\') {
        pipe += 1;
    }
    (bytes.get(pipe) == Some(&b'|')).then_some((pipe, (pipe - cursor) % 2 == 1))
}

fn split_table_row(input: &str, spoiler: bool) -> Vec<String> {
    let trimmed = input.trim();
    let mut cells = Vec::new();
    let mut cell = String::new();
    let mut cursor = 0;
    let mut code_fence = None;
    let mut spoiler_open = false;
    // Byte offset just past the most recent genuine cell-delimiter pipe. When the
    // scan ends with only whitespace after it, that pipe was a trailing border and
    // the empty leftover cell is dropped (rather than blindly trusting that the
    // line ends with `|`, which mis-fires on a spoiler-close `||` or a code-span
    // pipe — see tbl-4).
    let mut trailing_delimiter_end = None;

    while cursor < trimmed.len() {
        let (next, char) = next_char(trimmed, cursor).expect("valid UTF-8 byte index");
        // GitHub/cmark-gfm treats an odd backslash run before `|` as a literal
        // cell-content pipe, but an even run leaves the pipe as a delimiter. Keep
        // the original run before an even delimiter so the inline parser resolves
        // the visible backslashes correctly.
        if char == '\\' {
            if let Some((pipe, escaped)) = table_backslash_pipe_run(trimmed, cursor) {
                if escaped {
                    for _ in 0..pipe - cursor - 1 {
                        cell.push('\\');
                    }
                    cell.push('|');
                    cursor = pipe + 1;
                } else {
                    for _ in 0..pipe - cursor {
                        cell.push('\\');
                    }
                    cursor = pipe;
                }
                continue;
            }
        }
        // Backticks are never escapable, so a preceding backslash does not block a
        // code-span boundary (a `\` directly before a closing backtick is content,
        // not an escape — see tbl-3).
        if char == '`' {
            let length = trimmed[cursor..]
                .as_bytes()
                .iter()
                .take_while(|byte| **byte == b'`')
                .count();
            if code_fence == Some(length) {
                code_fence = None;
            } else if code_fence.is_none() && backtick_run_has_close(trimmed, cursor, length) {
                code_fence = Some(length);
            }
            cell.push_str(&trimmed[cursor..cursor + length]);
            cursor += length;
            continue;
        }

        if spoiler
            && char == '|'
            && trimmed.as_bytes().get(cursor + 1) == Some(&b'|')
            && code_fence.is_some()
        {
            cell.push_str("||");
            cursor += 2;
            continue;
        }

        if spoiler
            && char == '|'
            && trimmed.as_bytes().get(cursor + 1) == Some(&b'|')
            && code_fence.is_none()
            && !is_escaped_at(trimmed, cursor)
        {
            let closes_spoiler =
                spoiler_open && trimmed.as_bytes().get(cursor.wrapping_sub(1)) != Some(&b'|');
            let opens_spoiler = !spoiler_open
                && trimmed.as_bytes().get(cursor + 2) != Some(&b'|')
                && find_spoiler_close(trimmed, cursor + 2).is_some();
            if closes_spoiler || opens_spoiler {
                spoiler_open = opens_spoiler;
                cell.push_str("||");
                cursor += 2;
                continue;
            }
        }

        if char == '|' && !spoiler_open && !is_escaped_at(trimmed, cursor) {
            cells.push(core::mem::take(&mut cell));
            // A delimiter ends the cell; spoiler state never spans a cell boundary.
            spoiler_open = false;
            trailing_delimiter_end = Some(next);
        } else {
            cell.push(char);
        }
        cursor = next;
    }
    cells.push(cell);

    if trimmed.starts_with('|') {
        cells.remove(0);
    }
    // Drop the empty cell created by a trailing border pipe: the last genuine
    // delimiter must sit at the very end (only whitespace after it).
    if let Some(end) = trailing_delimiter_end {
        if trimmed[end..].trim().is_empty() {
            cells.pop();
        }
    }
    cells
}

fn table_can_start(lines: &[Line<'_>], index: usize, options: &SyntaxOptions) -> bool {
    if !options.constructs.gfm_table || index + 1 >= lines.len() {
        return false;
    }
    table_can_start_source(
        lines[index].text,
        lines[index + 1].text,
        options.constructs.indented_code,
        options.constructs.spoiler,
    )
}

pub(crate) fn gfm_table_can_start_source(header: &str, delimiter: &str) -> bool {
    table_can_start_source(header, delimiter, true, false)
}

fn table_can_start_source(
    header: &str,
    delimiter: &str,
    indented_code: bool,
    spoiler: bool,
) -> bool {
    let Some(delimiter) = table_indent_line(delimiter, indented_code) else {
        return false;
    };
    if list_marker_info(delimiter).is_some() {
        return false;
    }
    if !table_has_separator(header, delimiter, spoiler) {
        return false;
    }
    let Some(alignments) = parse_table_delimiter(delimiter, spoiler) else {
        return false;
    };
    split_table_row(header, spoiler).len() == alignments.len()
}

fn table_has_separator(header: &str, delimiter: &str, spoiler: bool) -> bool {
    // GFM makes leading/trailing pipes optional, so `parse_table_delimiter` plus
    // the header/alignment column-count check usually suffice. The one exception
    // is a single resolved column with no disambiguating syntax: `a\n-\nb` has
    // matching one-column shapes yet no pipe and no alignment colon, so it is a
    // loose paragraph/setext, not a table. A single column still forms a table
    // when a pipe appears in the header/delimiter or the delimiter carries an
    // explicit alignment colon (`a\n-:`, `a\n:-:`, …).
    let Some(alignments) = parse_table_delimiter(delimiter, spoiler) else {
        return true;
    };
    if alignments.len() == 1 {
        return contains_unescaped_pipe(header, spoiler)
            || contains_unescaped_pipe(delimiter, spoiler)
            || delimiter.contains(':');
    }
    true
}

// Still used by `block_quote_table_body_row` to detect a table row appearing as
// a block-quote continuation line (which DOES require a pipe).
fn contains_unescaped_pipe(input: &str, spoiler: bool) -> bool {
    let mut cursor = 0;
    let mut code_fence = None;
    let mut spoiler_open = false;
    while cursor < input.len() {
        let (next, char) = next_char(input, cursor).expect("valid UTF-8 byte index");
        if char == '\\' {
            if let Some((pipe, escaped)) = table_backslash_pipe_run(input, cursor) {
                cursor = if escaped { pipe + 1 } else { pipe };
                continue;
            }
        }
        // Backticks are never escapable; a preceding backslash is code-span content.
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
        if spoiler
            && char == '|'
            && input.as_bytes().get(cursor + 1) == Some(&b'|')
            && code_fence.is_some()
        {
            cursor += 2;
            continue;
        }
        if spoiler
            && char == '|'
            && input.as_bytes().get(cursor + 1) == Some(&b'|')
            && code_fence.is_none()
            && !is_escaped_at(input, cursor)
        {
            let closes_spoiler =
                spoiler_open && input.as_bytes().get(cursor.wrapping_sub(1)) != Some(&b'|');
            let opens_spoiler = !spoiler_open
                && input.as_bytes().get(cursor + 2) != Some(&b'|')
                && find_spoiler_close(input, cursor + 2).is_some();
            if closes_spoiler || opens_spoiler {
                spoiler_open = opens_spoiler;
                cursor += 2;
                continue;
            }
        }
        if char == '|' && !spoiler_open && !is_escaped_at(input, cursor) {
            return true;
        }
        cursor = next;
    }
    false
}

fn likely_block_start(input: &str, options: &SyntaxOptions) -> bool {
    // Block-structure markers (ATX, fences, thematic breaks, list markers, math
    // fences, directives, …) only begin a block when indented at most 3 columns.
    // At >=4 columns the line is indented code, which never interrupts a
    // paragraph, so no marker test should fire.
    let Some(trimmed) = trim_up_to_three_spaces(input) else {
        return false;
    };
    trimmed.starts_with('#')
        || trimmed.starts_with('>')
        || trimmed.starts_with("```")
        || trimmed.starts_with("~~~")
        || list_marker_can_interrupt_paragraph(input)
        || parse_thematic_break(Line {
            text: input,
            eol: "",
            start: 0,
            end: input.len(),
            end_with_eol: input.len(),
            lazy: false,
        })
        .is_some()
        || (options.constructs.html_container && line_starts_html_container(input))
        || (options.constructs.html_block && line_starts_interrupting_html_block(input))
        || (options.constructs.math_block && math_block_fence_length(trimmed).is_some())
        || (options.constructs.directive_container && trimmed.starts_with(":::"))
        || (options.constructs.directive_leaf && trimmed.starts_with("::"))
        || (options.constructs.footnote_definition && line_starts_footnote_definition(trimmed))
}

// A GFM footnote definition `[^label]:` is a block boundary: it interrupts a
// paragraph and ends a prior footnote's lazy continuation.
fn line_starts_footnote_definition(trimmed: &str) -> bool {
    trimmed.starts_with("[^")
        && find_footnote_definition_label_end(trimmed)
            .is_some_and(|close| is_footnote_label(&trimmed[2..close]))
}

fn list_marker_can_interrupt_paragraph(input: &str) -> bool {
    list_marker_info(input).is_some_and(|marker| {
        // An empty list item never interrupts a paragraph (CommonMark §5.3):
        // `foo\n*` is a single paragraph, not a paragraph plus an empty list.
        !marker.content.trim().is_empty() && (!marker.ordered || marker.start == Some(1))
    })
}

// GFM table-body termination is stricter than paragraph interruption: an open
// table also ends on a list marker with EMPTY content (`-`, `*`, `1.`), which
// `likely_block_start` deliberately ignores for paragraphs. Used only by the
// table body loop; `likely_block_start` itself is left untouched.
fn table_body_line_ends_table(line: &str, options: &SyntaxOptions) -> bool {
    likely_block_start(line, options)
        || list_marker_info(line).is_some()
        || (options.constructs.html_container && line_starts_html_container(line))
        || (options.constructs.html_block && line_starts_html_block(line))
}

fn line_starts_interrupting_html_block(input: &str) -> bool {
    match trim_up_to_three_spaces(input).and_then(html_block_start) {
        Some(HtmlBlockKind::UntilBlank) | None => false,
        Some(_) => true,
    }
}

fn parse_autolink_end(input: &str, index: usize) -> Option<usize> {
    input[index..].find('>').map(|end| index + end + 1)
}

fn parse_html_inline(input: &str, index: usize) -> Option<(usize, String)> {
    let rest = &input[index..];
    if rest.starts_with("<!--") {
        let end = rest.find("-->")? + 3;
        return Some((index + end, rest[..end].into()));
    }
    if rest.starts_with("<?") {
        let end = rest.find("?>")? + 2;
        return Some((index + end, rest[..end].into()));
    }
    if rest.starts_with("<![CDATA[") {
        let end = rest.find("]]>")? + 3;
        return Some((index + end, rest[..end].into()));
    }
    if is_declaration_start(rest) {
        let end = rest.find('>')? + 1;
        return Some((index + end, rest[..end].into()));
    }

    let (end, _) = parse_html_tag(input, index)?;
    Some((end, input[index..end].into()))
}

fn parse_html_tag(input: &str, index: usize) -> Option<(usize, &str)> {
    let bytes = input.as_bytes();
    if bytes.get(index) != Some(&b'<') {
        return None;
    }

    let closing = bytes.get(index + 1) == Some(&b'/');
    let name_start = index + if closing { 2 } else { 1 };
    let first = *bytes.get(name_start)?;
    if !first.is_ascii_alphabetic() {
        return None;
    }

    let mut cursor = name_start + 1;
    while bytes.get(cursor).is_some_and(|byte| html_name_byte(*byte)) {
        cursor += 1;
    }
    let name = &input[name_start..cursor];

    if closing {
        cursor = skip_spaces(input, cursor);
        if bytes.get(cursor) == Some(&b'>') {
            return Some((cursor + 1, name));
        }
        return None;
    }

    let mut needs_space = false;
    loop {
        let before_spaces = cursor;
        cursor = skip_spaces(input, cursor);
        let had_space = cursor > before_spaces;
        match bytes.get(cursor) {
            Some(b'>') => return Some((cursor + 1, name)),
            Some(b'/') if bytes.get(cursor + 1) == Some(&b'>') => return Some((cursor + 2, name)),
            Some(byte) if had_space && html_attribute_name_start(*byte) => {
                cursor += 1;
                while bytes
                    .get(cursor)
                    .is_some_and(|byte| html_attribute_name_byte(*byte))
                {
                    cursor += 1;
                }
                let after_name = cursor;
                let after_spaces = skip_spaces(input, cursor);
                if bytes.get(after_spaces) == Some(&b'=') {
                    cursor = skip_spaces(input, after_spaces + 1);
                    cursor = parse_html_attribute_value(input, cursor)?;
                } else {
                    cursor = after_name;
                }
                needs_space = true;
            }
            Some(_) if needs_space => return None,
            _ => return None,
        }
    }
}

fn parse_html_attribute_value(input: &str, index: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    match bytes.get(index)? {
        b'"' | b'\'' => {
            let quote = bytes[index];
            let mut cursor = index + 1;
            while cursor < bytes.len() {
                if bytes[cursor] == quote {
                    return Some(cursor + 1);
                }
                cursor += 1;
            }
            None
        }
        b'=' | b'<' | b'>' | b'`' => None,
        _ => {
            let mut cursor = index;
            while bytes.get(cursor).is_some_and(|byte| {
                !byte.is_ascii_whitespace()
                    && !matches!(*byte, b'"' | b'\'' | b'=' | b'<' | b'>' | b'`')
            }) {
                cursor += 1;
            }
            if cursor == index { None } else { Some(cursor) }
        }
    }
}

fn html_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'-'
}

fn html_attribute_name_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte == b':'
}

fn html_attribute_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':' | b'.' | b'-')
}

fn skip_spaces(input: &str, mut index: usize) -> usize {
    while input
        .as_bytes()
        .get(index)
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t' | b'\n' | b'\r'))
    {
        index += 1;
    }
    index
}

fn is_autolink(input: &str) -> bool {
    let inner = &input[1..input.len() - 1];
    is_uri_autolink(inner) || is_email_autolink(inner)
}

fn is_uri_autolink(input: &str) -> bool {
    let Some(colon) = input.find(':') else {
        return false;
    };
    let scheme = &input[..colon];
    if scheme.len() < 2 || scheme.len() > 32 {
        return false;
    }
    let mut bytes = scheme.bytes();
    if !bytes.next().is_some_and(|byte| byte.is_ascii_alphabetic()) {
        return false;
    }
    if !bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'.' | b'-')) {
        return false;
    }
    input[colon + 1..]
        .chars()
        .all(|char| !matches!(char, '<' | '>') && !char.is_control() && !char.is_whitespace())
}

fn is_email_autolink(input: &str) -> bool {
    if input.chars().any(char::is_whitespace) {
        return false;
    }
    let Some(at) = input.find('@') else {
        return false;
    };
    if at == 0 || at + 1 >= input.len() {
        return false;
    }
    // Angle-bracket `<email>` autolinks use the strict CommonMark domain
    // grammar but, unlike the GFM bare form, allow a single (dotless) label.
    is_email_local_part(&input[..at]) && is_email_domain(&input[at + 1..], 1)
}

// GFM literal-autolink dispatch. Tries, in order: `http(s)://` URLs, `www.`
// URLs, extended-protocol (`mailto:`/`xmpp:`) emails, and bare emails. Each
// branch enforces cmark-gfm's per-scheme preceding-character guard and its
// domain/host rules; the trailing trim is shared (`autolink_delim`). The
// returned destination is the synthesized href (a `http://`/`mailto:` prefix
// may be prepended); the caller keeps `input[index..end]` as the visible
// original.
fn parse_literal_autolink(
    input: &str,
    index: usize,
    gfm: bool,
    relaxed: bool,
) -> Option<(usize, String)> {
    let rest = &input[index..];

    if gfm {
        // `http://` / `https://` URLs. cmark requires the char before the scheme
        // to be non-alphanumeric (so `mmmhttp://…` does not link from `mmmh`).
        if let Some(scheme_len) = rest
            .starts_with("http://")
            .then_some(7)
            .or_else(|| rest.starts_with("https://").then_some(8))
        {
            if !literal_scheme_prefix_ok(input, index) {
                return None;
            }
            let host = &input[index + scheme_len..];
            // A non-empty domain or bracketed IPv6 host is additionally
            // required, so `http://`, `http://#`, `http://$` are not links.
            if !http_literal_host_ok(host) {
                if relaxed {
                    // Let cmark-gfm's relaxed `scheme://` pass decide cases
                    // such as a bare `http://` followed by whitespace.
                } else {
                    return None;
                }
            } else {
                // The URL extent is scanned from the very start (after `://`) and the
                // trailing trim runs over the whole URL. Relaxed mode balances
                // brackets/braces so `[abc]`/`{abc}`/IPv6 hosts stay in the URL.
                let end = autolink_url_end(input, index + scheme_len, index + scheme_len, relaxed);
                if end <= index + scheme_len {
                    return None;
                }
                if literal_autolink_suppressed_by_link_label(input, index, end, relaxed, gfm) {
                    return None;
                }
                return Some((end, input[index..end].into()));
            }
        }

        // `www.` URLs (synthesize a `http://` href). cmark allows the preceding
        // char to be one of `*_~(` or whitespace (or start of input).
        if rest
            .as_bytes()
            .get(..4)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"www."))
        {
            if !literal_www_prefix_ok(input, index) {
                return None;
            }
            check_domain(rest, false)?;
            let end = autolink_url_end(input, index, index, relaxed);
            if end <= index || (!relaxed && end <= index + 3 && !literal_starts_line(input, index))
            {
                return None;
            }
            if literal_autolink_suppressed_by_link_label(input, index, end, relaxed, gfm) {
                return None;
            }
            let mut destination = String::from("http://");
            destination.push_str(&input[index..end]);
            return Some((end, destination));
        }

        if let Some(email) = parse_literal_email(input, index) {
            return Some(email);
        }
    }

    if relaxed {
        // cmark-gfm "relaxed" URL autolinks: a bare `scheme://…` for any scheme
        // (`smb://`, `irc://`, `rdar://`, `we://`, `nex://[…]`, …) or a
        // scheme-less leading `://…` (`://-`). Requires the same non-alphanumeric
        // preceding char as the http literal and at least one non-whitespace
        // char after `://`; no host/domain validation (cmark-gfm is permissive
        // here — `smb:///path` and `://-` both linkify). The extent is balanced.
        if literal_scheme_prefix_ok(input, index) {
            if let Some(after_slashes) = relaxed_scheme_after_slashes(rest) {
                let body_start = index + after_slashes;
                let next = input[body_start..].chars().next();
                if next.is_none_or(|char| char.is_whitespace()) && after_slashes == 3 {
                    return None;
                }
                let end = autolink_url_end(input, body_start, body_start, true);
                if end > index {
                    if literal_autolink_suppressed_by_link_label(input, index, end, relaxed, gfm) {
                        return None;
                    }
                    return Some((end, input[index..end].into()));
                }
            }
        }
    }

    None
}

// Returns the byte offset (within `rest`) just past a relaxed `scheme://` (any
// ASCII-alpha-then-`[alnum+. -]` scheme) or scheme-less `://` prefix, if `rest`
// starts with one. No scheme length cap — cmark-gfm's relaxed autolink is
// permissive. Returns `None` for a bare `scheme:` without `//` (that is the
// email/angle-autolink path's job).
fn relaxed_scheme_after_slashes(rest: &str) -> Option<usize> {
    let bytes = rest.as_bytes();
    if bytes.starts_with(b"://") {
        return Some(3);
    }
    let first = bytes.first()?;
    if !first.is_ascii_alphabetic() {
        return None;
    }
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b':' => break,
            byte if byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'.' | b'-') => i += 1,
            _ => return None,
        }
    }
    if bytes.get(i..i + 3) == Some(b"://") {
        Some(i + 3)
    } else {
        None
    }
}

// The char immediately before a `http(s)://` literal must be non-alphabetic.
// An escaped `<` (`\<http://…`) is just literal text before the URL, so the
// literal still forms (the `<` is not treated as an angle-autolink opener).
fn literal_scheme_prefix_ok(input: &str, index: usize) -> bool {
    if index == 0 {
        return true;
    }
    let Some(previous) = input[..index].chars().next_back() else {
        return true;
    };
    !previous.is_ascii_alphabetic()
}

// The char before a `www.` literal must be one of cmark-gfm's accepted ASCII
// delimiters or ordinary Markdown layout whitespace. Unicode whitespace is not
// a start delimiter for this branch.
fn literal_www_prefix_ok(input: &str, index: usize) -> bool {
    if index == 0 {
        return true;
    }
    let Some(previous) = input[..index].chars().next_back() else {
        return true;
    };
    if matches!(previous, '*' | '_' | '~' | '(' | '[' | ']') {
        return true;
    }
    matches!(previous, ' ' | '\t' | '\n' | '\r')
}

fn literal_starts_line(input: &str, index: usize) -> bool {
    index == 0
        || input
            .as_bytes()
            .get(index - 1)
            .is_some_and(|byte| matches!(byte, b'\n' | b'\r'))
}

fn literal_autolink_suppressed_by_link_label(
    input: &str,
    index: usize,
    end: usize,
    relaxed: bool,
    gfm_autolink_literal: bool,
) -> bool {
    if !has_unclosed_link_label_opener(input, index) {
        return false;
    }
    if input[end..].starts_with("](") && !link_resource_tail_has_close(input, end + 2) {
        return true;
    }
    !relaxed && !gfm_autolink_literal && input.as_bytes().get(end).is_some_and(|byte| *byte == b']')
}

fn has_unclosed_link_label_opener(input: &str, index: usize) -> bool {
    let line_start = input[..index]
        .rfind(['\n', '\r'])
        .map_or(0, |offset| offset + 1);
    let mut depth = 0usize;
    let mut cursor = line_start;
    while cursor < index {
        let Some((next, char)) = next_char(input, cursor) else {
            break;
        };
        match char {
            '\\' => {
                cursor = next_char(input, next)
                    .map(|(after_escape, _)| after_escape)
                    .unwrap_or(next);
                continue;
            }
            '[' => depth += 1,
            ']' => {
                depth = depth.saturating_sub(1);
            }
            _ => {}
        }
        cursor = next;
    }
    depth > 0
}

fn link_resource_tail_has_close(input: &str, start: usize) -> bool {
    let mut cursor = start;
    while cursor < input.len() {
        let Some((next, char)) = next_char(input, cursor) else {
            break;
        };
        match char {
            '\\' => {
                cursor = next_char(input, next)
                    .map(|(after_escape, _)| after_escape)
                    .unwrap_or(next);
                continue;
            }
            '\n' | '\r' => return false,
            ')' => return true,
            _ => {}
        }
        cursor = next;
    }
    false
}

fn http_literal_host_ok(host: &str) -> bool {
    if host.starts_with('[') {
        return bracketed_ipv6_host_end(host).is_some();
    }
    match host.chars().next() {
        Some(char) if char.is_ascii() && char.is_ascii_alphanumeric() => {
            check_domain(host, true).is_some()
        }
        Some(char) if !char.is_ascii() && is_valid_hostchar(char) => {
            check_domain(host, true).is_some()
        }
        _ => false,
    }
}

fn bracketed_ipv6_host_end(host: &str) -> Option<usize> {
    let close = host.find(']')?;
    (close > 1).then_some(close + 1)
}

// Port of cmark-gfm `is_valid_hostchar`: a host char is valid when it is not a
// Unicode space and not a Unicode punctuation character.
fn is_valid_hostchar(char: char) -> bool {
    !char.is_whitespace() && !crate::unicode_punctuation::is_unicode_punctuation(char)
}

// Port of cmark-gfm `check_domain`. Scans the leading host of `data` (up to the
// first non-host char) and returns its byte length, or `None` when invalid.
// Rejects a `_` in either of the last two `.`-separated host segments (unless
// the host has >10 segments — a DoS guard). When `allow_short` is false a dot
// is required (the `www.` rule). The URL extent past the host is determined by
// `autolink_url_end`, so the precise length here only gates validity.
//
// cmark walks bytes with `is_valid_hostchar` decoding each char; this walks
// chars directly (UTF-8 safe) over the host prefix, which yields the same
// dot/underscore-segment verdict. A `\` escapes the following char.
fn check_domain(data: &str, allow_short: bool) -> Option<usize> {
    let mut np = 0usize;
    let mut uscore1 = 0usize;
    let mut uscore2 = 0usize;
    let mut host_len = 0usize;

    let mut chars = data.char_indices().peekable();
    while let Some((offset, char)) = chars.next() {
        // cmark's accounting loop runs `for (i = 1; i < size - 1; i++)`: it
        // never inspects the first char (offset 0) nor the final char of the
        // chunk. We replicate that — a trailing `_` (e.g. `http://a_`) is not
        // counted, so the link still forms.
        let account = offset != 0 && chars.peek().is_some();
        match char {
            '\\' => {
                // Escape: consume the next char as a literal host char.
                host_len = offset + char.len_utf8();
                if let Some((next_off, next)) = chars.next() {
                    host_len = next_off + next.len_utf8();
                }
            }
            '_' if account => {
                uscore2 += 1;
                host_len = offset + char.len_utf8();
            }
            '.' if account => {
                uscore1 = uscore2;
                uscore2 = 0;
                np += 1;
                host_len = offset + char.len_utf8();
            }
            '_' | '.' | '-' => {
                host_len = offset + char.len_utf8();
            }
            _ => {
                if !is_valid_hostchar(char) {
                    break;
                }
                host_len = offset + char.len_utf8();
            }
        }
    }

    if (uscore1 > 0 || uscore2 > 0) && np <= 10 {
        return None;
    }

    if allow_short || np > 0 {
        Some(host_len)
    } else {
        None
    }
}

// Forward scan from `start` for the URL extent: every char up to whitespace,
// `<`, or `]` ends the URL. CommonMark allows `>` and `[` inside (the renderer
// percent-encodes them); a `]` is additionally treated as a hard URL boundary
// (autolink-3), so a `]` ends the scan and is never part of the link.
// `trim_from` is where the trailing trim may reach (the URL start).
fn autolink_url_end(input: &str, start: usize, trim_from: usize, balanced: bool) -> usize {
    let bytes = input.as_bytes();
    let mut end = start;
    // Relaxed (cmark-gfm) URL extents balance `[`/`]` and `{`/`}` so an IPv6
    // host `nex://[fe80…]/z` and a balanced `[abc]`/`{abc}` run stay inside the
    // URL while an unbalanced trailing `]`/`}` ends it. Strict (GFM literal)
    // extents stop at the first `]` (no balancing) — the two oracle shapes
    // differ on purpose (`autolink_brackets_unbalanced` keeps both `]`;
    // `autolink_relaxed_links_brackets_balanced` keeps one).
    let mut bracket_depth = 0i32;
    let mut curly_depth = 0i32;
    let mut strict_has_open_bracket = false;
    let mut strict_inside_backticks = false;
    for (offset, char) in input[start..].char_indices() {
        if char.is_whitespace() || char == '<' || is_autolink_terminating_control(char) {
            break;
        }
        if balanced {
            match char {
                '[' => bracket_depth += 1,
                ']' => {
                    if bracket_depth > 0 {
                        bracket_depth -= 1;
                    } else {
                        break;
                    }
                }
                '{' => curly_depth += 1,
                '}' => {
                    if curly_depth > 0 {
                        curly_depth -= 1;
                    } else {
                        break;
                    }
                }
                _ => {}
            }
        } else {
            match char {
                '[' => strict_has_open_bracket = true,
                '`' => strict_inside_backticks = !strict_inside_backticks,
                ']' if !strict_has_open_bracket && !strict_inside_backticks => break,
                _ => {}
            }
        }
        // Round-trip guard: when a literal autolink ends (a trailing entity
        // run, punctuation trim, unbalanced `)`, or the `]`/`<` hard boundary),
        // the text that follows often begins with a char the serializer escapes
        // with a backslash (`\&`, `\[`, `\]`, `\<`, `\>`, `\*`, `\_`, …). The
        // URL scan must stop at such a `\<punct>` so the escape is not re-merged
        // into the destination. A `\` before `.` (or any non-punctuation) is a
        // genuine literal backslash inside the URL (e.g. `www.x.com/a\.`), which
        // the serializer never produces, so it stays part of the URL.
        if char == '\\' {
            if let Some(&next) = bytes.get(start + offset + 1) {
                let next_is_escapable_punct = next.is_ascii_punctuation() && next != b'.';
                if next_is_escapable_punct {
                    break;
                }
            }
        }
        end = start + offset + char.len_utf8();
    }
    autolink_delim(input, trim_from, end)
}

fn is_autolink_terminating_control(char: char) -> bool {
    matches!(char, '\u{2066}'..='\u{2069}')
}

// Port of cmark-gfm `autolink_delim`: trim trailing delimiters from the end of
// the URL. A trailing `) ? ! . , : * _ ~ ' "` is trimmed; `)` only when there
// are more `)` than `(` in the link; a trailing `&…;` entity run is excluded
// whole; a lone trailing `;` is trimmed.
fn autolink_delim(input: &str, start: usize, mut end: usize) -> usize {
    let bytes = input.as_bytes();
    let mut opening = 0usize;
    let mut closing = 0usize;
    for &byte in &bytes[start..end] {
        match byte {
            b'(' => opening += 1,
            b')' => closing += 1,
            _ => {}
        }
    }

    while end > start {
        match bytes[end - 1] {
            b')' => {
                if closing <= opening {
                    break;
                }
                closing -= 1;
                end -= 1;
            }
            b'?' | b'!' | b'.' | b',' | b':' | b'*' | b'_' | b'~' | b'\'' | b'"' => {
                end -= 1;
            }
            b';' => {
                // A trailing hex numeric character reference `&#x…;` is excluded
                // whole. This is the round-trip dual of the serializer, which
                // encodes a text char that would otherwise merge into the URL as
                // a hex entity; no autolink-oracle URL ends in `&#x…;`, so this
                // is conformance-safe (decimal `&#…;` is left intact to match
                // the oracle, which keeps `www.a&#35` in the URL).
                if let Some(amp) = trailing_hex_entity_run_start(bytes, start, end) {
                    end = amp;
                } else {
                    // Walk back over alphanumerics; if they reach a `&`, exclude
                    // the whole `&…;` entity run, otherwise trim just the `;`.
                    let mut new_end = end - 1;
                    while new_end > start && bytes[new_end - 1].is_ascii_alphanumeric() {
                        new_end -= 1;
                    }
                    if new_end > start && new_end < end - 1 && bytes[new_end - 1] == b'&' {
                        end = new_end - 1;
                    } else {
                        end -= 1;
                    }
                }
            }
            _ => break,
        }
    }
    end
}

// When the URL ends with a hex numeric character reference `&#x[hex]+;`, returns
// the offset of its leading `&`; otherwise `None`. Used only by `autolink_delim`
// to trim the serializer's round-trip boundary marker (the serializer encodes a
// would-merge text char as `&#xNN;`). Decimal `&#…;` is intentionally NOT
// matched so the oracle's `www.a&#35` URLs stay intact.
fn trailing_hex_entity_run_start(bytes: &[u8], start: usize, end: usize) -> Option<usize> {
    if end <= start || bytes[end - 1] != b';' {
        return None;
    }
    let mut cursor = end - 1;
    while cursor > start && bytes[cursor - 1].is_ascii_hexdigit() {
        cursor -= 1;
    }
    // Require at least one hex digit, then `&#x` (case-insensitive `x`).
    if cursor == end - 1 || cursor < start + 3 {
        return None;
    }
    let x = bytes[cursor - 1];
    if (x == b'x' || x == b'X') && bytes[cursor - 2] == b'#' && bytes[cursor - 3] == b'&' {
        Some(cursor - 3)
    } else {
        None
    }
}

// GFM bare-email literal (and the extended `mailto:`/`xmpp:` protocol forms).
// `index` must be the link start: cmark anchors the email at the left edge
// found by rewinding from `@` over `[A-Za-z0-9._+-]` (or a `mailto:`/`xmpp:`
// scheme), so this only succeeds when the char before `index` is not part of
// that left extent.
fn parse_literal_email(input: &str, index: usize) -> Option<(usize, String)> {
    let rest = &input[index..];
    let at = rest.find('@')?;
    if at == 0 {
        return None;
    }
    let local = &rest[..at];

    // Determine whether this `@` is preceded by an extended protocol scheme
    // (`mailto:` / `xmpp:`), which both relaxes the href synthesis and (xmpp)
    // allows `/` in the domain.
    let (auto_mailto, is_xmpp) = classify_email_local(local);

    // Left-boundary guard (autolink-1): the char before `index` must not be a
    // local-part continuation char, otherwise the true link starts earlier and
    // this position is interior. After a recognized scheme, the scheme's own
    // preceding-char rule is what matters.
    if !email_left_boundary_ok(input, index, auto_mailto) {
        return None;
    }

    if !email_local_is_valid(local, auto_mailto) {
        return None;
    }

    let domain_start = index + at + 1;
    let domain_end = literal_email_domain_end(input, domain_start, is_xmpp)?;
    let trimmed = autolink_delim(input, domain_start, domain_end);
    if trimmed <= domain_start {
        return None;
    }

    let domain = &input[domain_start..trimmed];
    if !is_gfm_email_domain(domain, is_xmpp) {
        return None;
    }

    let mut destination = String::new();
    if auto_mailto {
        destination.push_str("mailto:");
    }
    destination.push_str(&input[index..trimmed]);
    Some((trimmed, destination))
}

// Classify the local part for the extended-protocol forms. Returns
// `(auto_mailto, is_xmpp)`: `mailto:user` → (false, false); `xmpp:user` →
// (false, true); a bare local part → (true, false). The scheme match is
// case-insensitive.
fn classify_email_local(local: &str) -> (bool, bool) {
    if let Some(rest) = strip_ci_prefix(local, "mailto:") {
        if !rest.is_empty() {
            return (false, false);
        }
    }
    if let Some(rest) = strip_ci_prefix(local, "xmpp:") {
        if !rest.is_empty() {
            return (false, true);
        }
    }
    (true, false)
}

fn strip_ci_prefix<'a>(input: &'a str, prefix: &str) -> Option<&'a str> {
    let bytes = input.as_bytes();
    let plen = prefix.len();
    if bytes.len() >= plen && bytes[..plen].eq_ignore_ascii_case(prefix.as_bytes()) {
        Some(&input[plen..])
    } else {
        None
    }
}

// The left-boundary check for an email literal. The link is anchored at its
// true left edge: the preceding char must not be an ASCII alphanumeric (which
// would extend the local part leftward). For the bare form, a preceding `/` is
// also rejected (`/a@b.c` is not linked), while the extended
// `mailto:`/`xmpp:` form permits `/` before the scheme (so
// `…/mailto:beedrill@…` links).
fn email_left_boundary_ok(input: &str, index: usize, auto_mailto: bool) -> bool {
    if index == 0 {
        return true;
    }
    let Some(previous) = input[..index].chars().next_back() else {
        return true;
    };
    if previous.is_ascii_alphanumeric() {
        if auto_mailto
            && input[index..].starts_with('+')
            && prefix_ends_with_gfm_email(input, index)
        {
            return true;
        }
        return false;
    }
    if auto_mailto && previous == '/' {
        return false;
    }
    true
}

fn prefix_ends_with_gfm_email(input: &str, end: usize) -> bool {
    let start = input[..end]
        .rfind(char::is_whitespace)
        .map_or(0, |offset| offset + 1);
    let candidate = &input[start..end];
    let Some(at) = candidate.rfind('@') else {
        return false;
    };
    email_local_is_valid(&candidate[..at], true) && is_gfm_email_domain(&candidate[at + 1..], false)
}

// Validate the email local part. For the bare form, every char must be a GFM
// email atext byte (`[A-Za-z0-9.+_-]` plus the dot-separated structure). For
// the extended-protocol forms, the part after the scheme is validated.
fn email_local_is_valid(local: &str, auto_mailto: bool) -> bool {
    let body = if auto_mailto {
        local
    } else if let Some(rest) = strip_ci_prefix(local, "mailto:") {
        rest
    } else if let Some(rest) = strip_ci_prefix(local, "xmpp:") {
        rest
    } else {
        local
    };
    !body.is_empty() && body.bytes().all(is_gfm_email_local_byte)
}

// GFM email local-part charset (autolink-1): a narrower set than RFC atext,
// matching cmark's rewind class `[A-Za-z0-9.+_-]`.
fn is_gfm_email_local_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'+' | b'_' | b'-')
}

fn is_email_local_part(input: &str) -> bool {
    !input.is_empty()
        && input
            .split('.')
            .all(|segment| !segment.is_empty() && segment.bytes().all(is_email_atext))
}

fn is_email_atext(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'/'
                | b'='
                | b'?'
                | b'^'
                | b'_'
                | b'`'
                | b'{'
                | b'|'
                | b'}'
                | b'~'
                | b'-'
        )
}

// Port of cmark-gfm's email-domain scan (`postprocess_text`). Scans forward
// from `index` over the email domain, accepting alphanumerics, `-`, `_`, and
// `.`; for the `xmpp:` form a `/` is also accepted (path). A dot only counts
// toward the "at least one dot" requirement when it is followed by an
// alphanumeric. The scanned span must be >= 1 byte, contain at least one such
// dot, and end in an alphabetic char or a dot. Returns the domain end offset
// (before trailing trim), or `None` when invalid.
fn literal_email_domain_end(input: &str, index: usize, is_xmpp: bool) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut end = index;
    let mut np = 0usize;
    while end < bytes.len() {
        let byte = bytes[end];
        if byte.is_ascii_alphanumeric() {
            end += 1;
        } else if byte == b'.' && end + 1 < bytes.len() && bytes[end + 1].is_ascii_alphanumeric() {
            np += 1;
            end += 1;
        } else if byte == b'-' || byte == b'_' || (byte == b'/' && is_xmpp) {
            // `-`/`_` always continue the domain; `/` continues only the xmpp
            // path form.
            end += 1;
        } else {
            break;
        }
    }
    if end <= index {
        return None;
    }
    let len = end - index;
    let last = bytes[end - 1];
    if len < 1 || np == 0 || !(last.is_ascii_alphabetic() || last == b'.') {
        return None;
    }
    Some(end)
}

// Final structural validation of the trimmed email domain. The cmark scan
// already enforced the dot/last-char rules; this re-checks them after the
// shared trailing trim removed any delimiters, and rejects a domain ending in
// `-`/`_` (autolink-7: a hyphen in the final label disqualifies the link).
fn is_gfm_email_domain(input: &str, is_xmpp: bool) -> bool {
    if input.is_empty() {
        return false;
    }
    // A `/` path is only legal in the `xmpp:` form; split it off for the host
    // structural checks.
    let host = if is_xmpp {
        input.split('/').next().unwrap_or(input)
    } else {
        input
    };
    if !host.contains('.') {
        return false;
    }
    let last = host.as_bytes()[host.len() - 1];
    // The final label must not end in `-` or `_`, and the trailing label may
    // not be all ASCII digits.
    if matches!(last, b'-' | b'_') {
        return false;
    }
    host.split('.').all(|label| {
        !label.is_empty()
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    })
}

fn is_email_domain(input: &str, min_labels: usize) -> bool {
    let mut label_count = 0usize;
    for label in input.split('.') {
        label_count += 1;
        let bytes = label.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 63
            || !bytes
                .first()
                .is_some_and(|byte| byte.is_ascii_alphanumeric())
            || !bytes
                .last()
                .is_some_and(|byte| byte.is_ascii_alphanumeric())
            || !bytes
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'-')
        {
            return false;
        }
    }
    label_count >= min_labels
}

fn is_footnote_label(label: &str) -> bool {
    !label.is_empty()
        && reference_label_is_within_limit(label)
        && !label.chars().any(char::is_whitespace)
}

fn find_footnote_definition_label_end(input: &str) -> Option<usize> {
    let close = find_footnote_reference_label_end(input, 2)?;
    if input.as_bytes().get(close + 1) == Some(&b':') {
        Some(close)
    } else {
        None
    }
}

fn find_footnote_reference_label_end(input: &str, mut cursor: usize) -> Option<usize> {
    while cursor < input.len() {
        let (next, char) = next_char(input, cursor)?;
        if char == ']' && !is_escaped_at(input, cursor) {
            return Some(cursor);
        }
        cursor = next;
    }
    None
}

fn find_inline_footnote_end(input: &str, mut cursor: usize) -> Option<usize> {
    let mut depth = 0usize;
    while cursor < input.len() {
        let (next, char) = next_char(input, cursor)?;
        if !is_escaped_at(input, cursor) {
            match char {
                '[' => depth += 1,
                ']' if depth == 0 => return Some(cursor),
                ']' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
        cursor = next;
    }
    None
}

#[cfg(test)]
mod tests {
    use alloc::{string::String, vec::Vec};

    use super::*;

    fn lix_gfm_options() -> SyntaxOptions {
        let mut options = SyntaxOptions::gfm();
        options.constructs.frontmatter = true;
        options.parse.preserve_character_escapes = true;
        options.parse.preserve_character_references = true;
        options
    }

    fn parse_with_legacy_definition_prepass(input: &str, options: &SyntaxOptions) -> ParseOutput {
        parse_with_definitions(input, options, collect_definitions(input, options))
    }

    fn assert_inline_parse_matches_legacy(input: &str, options: &SyntaxOptions) {
        let definitions = vec![String::from("known-reference")];
        let mut optimized_diagnostics = Vec::new();
        let mut legacy_diagnostics = Vec::new();
        let optimized = parse_inlines(input, 17, options, &definitions, &mut optimized_diagnostics);
        let legacy = parse_inlines_with_context(
            input,
            17,
            options,
            &definitions,
            &mut legacy_diagnostics,
            InlineContext::default(),
        );

        assert_eq!(optimized, legacy, "inline AST must match for {input:?}");
        assert_eq!(
            optimized_diagnostics, legacy_diagnostics,
            "inline diagnostics must match for {input:?}"
        );
    }

    #[test]
    fn plain_inlines_use_one_literal_text_node() {
        let options = lix_gfm_options();
        for input in [
            "A calm sentence. It has commas, quotes \"like this\", and parentheses (too).",
            "Unicode café — 東京 … with a #hash and 42%.",
            "A / slash; a question? and a greater-than > stay literal.",
            "\ttabs and trailing spaces  ",
        ] {
            assert!(
                is_single_text_inline(input),
                "expected no inline syntax trigger in {input:?}"
            );
            assert_inline_parse_matches_legacy(input, &options);

            let mut diagnostics = Vec::new();
            assert_eq!(
                parse_inlines(input, 17, &options, &[], &mut diagnostics),
                vec![Inline::Text(Text {
                    meta: NodeMeta::new(Some(Span::new(17, 17 + input.len()))),
                    value: input.into(),
                })],
            );
            assert!(diagnostics.is_empty());
        }
    }

    #[test]
    fn every_inline_syntax_trigger_uses_the_legacy_parser() {
        let options = [
            SyntaxOptions::commonmark(),
            lix_gfm_options(),
            SyntaxOptions::default(),
            SyntaxOptions::mdx(),
        ];
        for input in [
            "\\\\escape",
            "&amp;",
            "line\nnext",
            "`code`",
            "||spoiler||",
            "*emphasis*",
            "_emphasis_",
            "++insert++",
            "==mark==",
            "~subscript~",
            "~~delete~~",
            "^[note]",
            "^sup^",
            "![alt](https://example.test)",
            "[label](https://example.test)",
            "$math$",
            "<https://example.test>",
            "{expression}",
            ":shortcode:",
            "https://example.test",
            "ftp://example.test",
            "www.example.test",
            "WwW.example.test",
            "person@example.test",
            "nul\0byte",
        ] {
            assert!(
                !is_single_text_inline(input),
                "every possible syntax starter must retain the fallback for {input:?}"
            );
            for options in &options {
                assert_inline_parse_matches_legacy(input, options);
            }
        }
    }

    #[test]
    fn marker_free_inputs_match_the_legacy_definition_prepass() {
        let options = lix_gfm_options();
        for input in [
            "# Heading\n\nText *emphasis*, [inline link](https://example.test), and `code`.\n",
            "---\ntitle: Test\n---\n\n| A | B |\n| --- | --- |\n| *x* | `y` |\n",
            "- [x] done\n- [ ] todo\n\n> quote with \\*escape\\* and &amp;\n",
            "```rust\nlet array = [1, 2, 3];\n```\n",
        ] {
            assert!(!input.contains("]:"));
            let optimized_definitions = definition_identifiers(input, &options);
            assert!(optimized_definitions.is_empty());
            assert_eq!(
                parse_with_definitions(input, &options, optimized_definitions),
                parse_with_legacy_definition_prepass(input, &options),
                "marker-free input must exactly match the legacy parser output",
            );
        }
    }

    #[test]
    fn definition_and_footnote_markers_keep_the_legacy_prepass() {
        let options = lix_gfm_options();
        let inputs: [(&str, &[&str]); 5] = [
            (
                "[text][Label]\n\n[Label]: <https://example.test> 'title'\n",
                &["label"],
            ),
            (
                "[text][multi label]\n\n[multi\n label]: /target\n",
                &["multi label"],
            ),
            ("> [text][nested]\n>\n> [nested]: /target\n", &["nested"]),
            ("Text[^note].\n\n[^note]: note\n", &[]),
            ("```text\nnot a definition ]:\n```\n", &[]),
        ];

        for (input, expected_definitions) in inputs {
            assert!(input.contains("]:"));
            let optimized_definitions = definition_identifiers(input, &options);
            assert_eq!(
                optimized_definitions,
                expected_definitions
                    .iter()
                    .map(|definition| String::from(*definition))
                    .collect::<Vec<_>>(),
            );
            assert_eq!(
                parse_with_definitions(input, &options, optimized_definitions),
                parse_with_legacy_definition_prepass(input, &options),
                "marker input must use the established definition collector",
            );
        }
    }
}
