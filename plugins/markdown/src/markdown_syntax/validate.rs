//! AST validation: [`Document::validate`] walks the tree and reports each
//! invalid or unsupported node shape as a [`Diagnostic`]. Serialization and HTML
//! rendering run this first and refuse an invalid document.

use alloc::vec::Vec;

use crate::{
    ast::{
        Autolink, AutolinkKind, Block, CodeInline, ContainerDirective, DirectiveAttribute,
        Document, Escape, Heading, HtmlContainer, HtmlContainerContent, Inline, LeafDirective,
        List, MathInlineKind, Table, TextDirective,
    },
    diagnostic::Diagnostic,
    span::Span,
};

impl Document {
    /// Validate this document's AST shape, returning a diagnostic for each
    /// invalid or unsupported node (empty when the document is well-formed).
    pub fn validate(&self) -> Vec<Diagnostic> {
        validate_document(self)
    }
}

pub(crate) fn validate_document(document: &Document) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    for block in &document.children {
        validate_block(block, &mut diagnostics);
    }
    diagnostics
}

fn validate_block(block: &Block, diagnostics: &mut Vec<Diagnostic>) {
    match block {
        Block::Paragraph(paragraph) => validate_inlines(&paragraph.children, diagnostics),
        Block::Heading(heading) => validate_heading(heading, diagnostics),
        Block::BlockQuote(block_quote) => {
            for child in &block_quote.children {
                validate_block(child, diagnostics);
            }
        }
        Block::Alert(alert) => {
            for child in &alert.children {
                validate_block(child, diagnostics);
            }
        }
        Block::List(list) => {
            validate_list_start(list, diagnostics);
            for item in &list.children {
                for child in &item.children {
                    validate_block(child, diagnostics);
                }
            }
        }
        Block::DescriptionList(list) => {
            for item in &list.children {
                validate_inlines(&item.term, diagnostics);
                if item.details.is_empty() {
                    diagnostics.push(Diagnostic::invalid(
                        item.meta.span,
                        "description item must contain at least one details block",
                    ));
                }
                for details in &item.details {
                    for child in &details.children {
                        validate_block(child, diagnostics);
                    }
                }
            }
        }
        Block::Table(table) => validate_table(table, diagnostics),
        Block::FootnoteDefinition(definition) => {
            if definition.identifier.is_empty() {
                diagnostics.push(Diagnostic::invalid(
                    definition.meta.span,
                    "footnote definition identifier cannot be empty",
                ));
            }
            for child in &definition.children {
                validate_block(child, diagnostics);
            }
        }
        Block::Definition(definition) => {
            if definition.identifier.trim().is_empty() {
                diagnostics.push(Diagnostic::invalid(
                    definition.meta.span,
                    "definition identifier cannot be empty",
                ));
            }
        }
        Block::LeafDirective(directive) => validate_leaf_directive(directive, diagnostics),
        Block::ContainerDirective(directive) => {
            validate_container_directive(directive, diagnostics)
        }
        Block::HtmlContainer(container) => validate_html_container(container, diagnostics),
        Block::ThematicBreak(_)
        | Block::CodeBlock(_)
        | Block::HtmlBlock(_)
        | Block::MathBlock(_)
        | Block::Frontmatter(_)
        | Block::MdxEsm(_)
        | Block::MdxExpression(_)
        | Block::MdxJsx(_) => {}
    }
}

fn validate_html_container(container: &HtmlContainer, diagnostics: &mut Vec<Diagnostic>) {
    if container.opening.name.is_empty() {
        diagnostics.push(Diagnostic::invalid(
            container.opening.meta.span,
            "HTML container opening tag name cannot be empty",
        ));
    }
    if container.closing.name.is_empty() {
        diagnostics.push(Diagnostic::invalid(
            container.closing.meta.span,
            "HTML container closing tag name cannot be empty",
        ));
    }
    if container.opening.name != container.closing.name {
        diagnostics.push(Diagnostic::invalid(
            container.meta.span,
            "HTML container opening and closing tag names must match",
        ));
    }
    if container.opening.raw.is_empty() {
        diagnostics.push(Diagnostic::invalid(
            container.opening.meta.span,
            "HTML container opening tag source cannot be empty",
        ));
    }
    if container.closing.raw.is_empty() {
        diagnostics.push(Diagnostic::invalid(
            container.closing.meta.span,
            "HTML container closing tag source cannot be empty",
        ));
    }

    match &container.content {
        HtmlContainerContent::Blocks(children) => {
            for child in children {
                validate_block(child, diagnostics);
            }
        }
        HtmlContainerContent::Inlines(children) => validate_inlines(children, diagnostics),
    }
}

fn validate_heading(heading: &Heading, diagnostics: &mut Vec<Diagnostic>) {
    if heading.depth == 0 || heading.depth > 6 {
        diagnostics.push(Diagnostic::invalid(
            heading.meta.span,
            "heading depth must be in the range 1..=6",
        ));
    }
    validate_inlines(&heading.children, diagnostics);
}

fn validate_table(table: &Table, diagnostics: &mut Vec<Diagnostic>) {
    if table.rows.is_empty() {
        diagnostics.push(Diagnostic::invalid(
            table.meta.span,
            "table must contain at least a header row",
        ));
        return;
    }

    let width = table.rows[0].cells.len();
    if width == 0 {
        diagnostics.push(Diagnostic::invalid(
            table.meta.span,
            "table header row must contain at least one cell",
        ));
    }

    if table.alignments.len() != width {
        diagnostics.push(Diagnostic::invalid(
            table.meta.span,
            "table alignment count must match header width",
        ));
    }

    for row in &table.rows {
        if row.cells.len() != width {
            diagnostics.push(Diagnostic::invalid(
                row.meta.span,
                "table row width must match header width",
            ));
        }
        for cell in &row.cells {
            validate_inlines(&cell.children, diagnostics);
        }
    }
}

fn validate_leaf_directive(directive: &LeafDirective, diagnostics: &mut Vec<Diagnostic>) {
    validate_directive_name(directive.meta.span, &directive.name, diagnostics);
    validate_directive_attributes(&directive.attributes, diagnostics);
    validate_inlines(&directive.label, diagnostics);
}

fn validate_container_directive(directive: &ContainerDirective, diagnostics: &mut Vec<Diagnostic>) {
    validate_directive_name(directive.meta.span, &directive.name, diagnostics);
    validate_directive_attributes(&directive.attributes, diagnostics);
    validate_inlines(&directive.label, diagnostics);
    for child in &directive.children {
        validate_block(child, diagnostics);
    }
}

fn validate_inlines(inlines: &[Inline], diagnostics: &mut Vec<Diagnostic>) {
    if let Some(Inline::LineBreak(node)) = inlines.last() {
        diagnostics.push(Diagnostic::invalid(
            node.meta.span,
            "hard line break cannot be the final inline of its container",
        ));
    }
    for inline in inlines {
        match inline {
            Inline::Emphasis(node) => {
                validate_emphasis_container(&node.children, node.meta.span, diagnostics)
            }
            Inline::Strong(node) => {
                validate_emphasis_container(&node.children, node.meta.span, diagnostics)
            }
            Inline::Underline(node) => {
                validate_emphasis_container(&node.children, node.meta.span, diagnostics)
            }
            Inline::Delete(node) => {
                validate_emphasis_container(&node.children, node.meta.span, diagnostics)
            }
            Inline::Insert(node) => {
                validate_emphasis_container(&node.children, node.meta.span, diagnostics)
            }
            Inline::Mark(node) => {
                validate_emphasis_container(&node.children, node.meta.span, diagnostics)
            }
            Inline::Subscript(node) => {
                validate_emphasis_container(&node.children, node.meta.span, diagnostics)
            }
            Inline::Superscript(node) => {
                validate_emphasis_container(&node.children, node.meta.span, diagnostics)
            }
            Inline::Spoiler(node) => {
                validate_emphasis_container(&node.children, node.meta.span, diagnostics)
            }
            Inline::Shortcode(node) => {
                if node.name.is_empty() {
                    diagnostics.push(Diagnostic::invalid(
                        node.meta.span,
                        "shortcode name cannot be empty",
                    ));
                }
            }
            Inline::Link(node) => validate_inlines(&node.children, diagnostics),
            Inline::Image(node) => validate_inlines(&node.alt, diagnostics),
            Inline::LinkReference(node) => {
                if node.identifier.is_empty() {
                    diagnostics.push(Diagnostic::invalid(
                        node.meta.span,
                        "link reference identifier cannot be empty",
                    ));
                }
                validate_inlines(&node.children, diagnostics);
            }
            Inline::ImageReference(node) => {
                if node.identifier.is_empty() {
                    diagnostics.push(Diagnostic::invalid(
                        node.meta.span,
                        "image reference identifier cannot be empty",
                    ));
                }
                validate_inlines(&node.alt, diagnostics);
            }
            Inline::Escape(node) => validate_escape(node, diagnostics),
            Inline::CharacterReference(node) => {
                if node.reference.is_empty() {
                    diagnostics.push(Diagnostic::invalid(
                        node.meta.span,
                        "character reference source cannot be empty",
                    ));
                }
                if node.value.is_empty() {
                    diagnostics.push(Diagnostic::invalid(
                        node.meta.span,
                        "character reference value cannot be empty",
                    ));
                }
            }
            Inline::TextDirective(node) => validate_text_directive(node, diagnostics),
            Inline::FootnoteReference(node) => {
                if node.identifier.is_empty() {
                    diagnostics.push(Diagnostic::invalid(
                        node.meta.span,
                        "footnote reference identifier cannot be empty",
                    ));
                }
            }
            Inline::InlineFootnote(node) => validate_inlines(&node.children, diagnostics),
            Inline::WikiLink(node) => {
                if node.target.is_empty() {
                    diagnostics.push(Diagnostic::invalid(
                        node.meta.span,
                        "wikilink target cannot be empty",
                    ));
                }
            }
            Inline::Code(node) => validate_code_inline(node, diagnostics),
            Inline::Autolink(node) => validate_autolink(node, diagnostics),
            Inline::Math(node) => {
                if let MathInlineKind::Dollar { dollars: 0 } = node.kind {
                    diagnostics.push(Diagnostic::invalid(
                        node.meta.span,
                        "dollar-fenced inline math must have a fence length of at least 1",
                    ));
                }
            }
            Inline::Text(_)
            | Inline::Html(_)
            | Inline::SoftBreak(_)
            | Inline::LineBreak(_)
            | Inline::MdxExpression(_)
            | Inline::MdxJsx(_) => {}
        }
    }
}

fn validate_emphasis_container(
    children: &[Inline],
    span: Option<Span>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if children.is_empty() {
        diagnostics.push(Diagnostic::invalid(
            span,
            "emphasis-like inline container cannot have empty children",
        ));
    }
    validate_inlines(children, diagnostics);
}

fn validate_escape(escape: &Escape, diagnostics: &mut Vec<Diagnostic>) {
    if !escape.value.is_ascii_punctuation() {
        diagnostics.push(Diagnostic::invalid(
            escape.meta.span,
            "escaped value must be an ASCII punctuation character",
        ));
    }
}

fn validate_autolink(autolink: &Autolink, diagnostics: &mut Vec<Diagnostic>) {
    // GFM literal autolinks carry a synthesized destination that MAY contain
    // `>` (the renderer percent-encodes it). Only angle-bracket autolinks
    // forbid whitespace, `<`, and `>` in the destination.
    if matches!(autolink.kind, AutolinkKind::GfmLiteral { .. }) {
        return;
    }
    if autolink
        .destination
        .chars()
        .any(|char| char.is_whitespace() || char == '<' || char == '>')
    {
        diagnostics.push(Diagnostic::invalid(
            autolink.meta.span,
            "autolink destination cannot contain whitespace, `<`, or `>`",
        ));
    }
}

fn validate_code_inline(code: &CodeInline, diagnostics: &mut Vec<Diagnostic>) {
    if code.fence_length == 0 {
        return;
    }
    // A code span fence of length N is closed only by a backtick run of exactly
    // length N. A run shorter or longer than the fence is inert, so only an
    // exactly-matching interior run would close the raw passthrough early.
    if raw_has_backtick_run(&code.raw, code.fence_length) {
        diagnostics.push(Diagnostic::invalid(
            code.meta.span,
            "inline code raw passthrough contains a backtick run equal to its fence length",
        ));
    }
}

fn raw_has_backtick_run(input: &str, length: usize) -> bool {
    let mut current = 0;
    for byte in input.bytes() {
        if byte == b'`' {
            current += 1;
        } else {
            if current == length {
                return true;
            }
            current = 0;
        }
    }
    current == length
}

fn validate_list_start(list: &List, diagnostics: &mut Vec<Diagnostic>) {
    if !list.ordered {
        return;
    }
    let Some(start) = list.start else {
        return;
    };
    if start > 999_999_999 {
        diagnostics.push(Diagnostic::invalid(
            list.meta.span,
            "ordered list start must be representable in at most 9 digits",
        ));
    }
}

fn validate_text_directive(directive: &TextDirective, diagnostics: &mut Vec<Diagnostic>) {
    validate_directive_name(directive.meta.span, &directive.name, diagnostics);
    validate_directive_attributes(&directive.attributes, diagnostics);
    validate_inlines(&directive.label, diagnostics);
}

fn validate_directive_name(span: Option<Span>, name: &str, diagnostics: &mut Vec<Diagnostic>) {
    if !is_directive_name(name) {
        diagnostics.push(Diagnostic::invalid(
            span,
            "directive name must start with a letter and contain letters, digits, `_`, or `-`",
        ));
    }
}

fn validate_directive_attributes(
    attributes: &[DirectiveAttribute],
    diagnostics: &mut Vec<Diagnostic>,
) {
    for attribute in attributes {
        if !is_attribute_name(&attribute.name) {
            diagnostics.push(Diagnostic::invalid(
                None,
                "directive attribute name must start with a letter, `_`, or `-`",
            ));
        }
    }
}

pub(crate) fn is_directive_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() {
        return false;
    }
    chars.all(|char| char.is_ascii_alphanumeric() || char == '_' || char == '-')
}

pub(crate) fn is_attribute_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_' || first == '-') {
        return false;
    }
    chars.all(|char| char.is_ascii_alphanumeric() || char == '_' || char == '-' || char == ':')
}
