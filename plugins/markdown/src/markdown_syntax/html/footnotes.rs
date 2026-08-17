//! Footnote document pre-pass + section emission (GFM shape only).
//!
//! Before rendering the body, we walk the whole document to learn:
//!   * the first-reference order of footnote ids (regular `[^id]` and
//!     synthesized inline `__inline_{N}`), which gives each its 1-based number;
//!   * how many times each id is referenced (drives the backref count and the
//!     per-reference `id="fnref-{id}{-k}"` suffix);
//!   * each referenced id's definition body (regular def children, or the
//!     harvested children of an inline footnote);
//!   * which ids are actually referenced (unreferenced defs are dropped).
//!
//! The body renderer consumes this read-only context; only the references it
//! actually emits advance the per-id reference counter, so the suffixes match.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::Cell;

use crate::ast::{Block, HtmlContainerContent, Inline};

use super::escape::{attr_escape_gfm, encode_href};

/// Read-only footnote state shared across the body render.
pub struct FootnoteContext {
    /// Distinct ids in first-reference order; 1-based index = display number.
    order: Vec<String>,
    /// Id → display number (1-based).
    numbers: BTreeMap<String, usize>,
    /// Id → footnote body blocks (def children or harvested inline children).
    defs: BTreeMap<String, Vec<Block>>,
    /// Id → total number of references (for backref anchors).
    ref_totals: BTreeMap<String, usize>,
    /// Ids actually referenced (only these are emitted).
    referenced: BTreeSet<String>,
    /// Folded id → first definition's ORIGINAL-case label, used for the
    /// displayed `fn-`/`fnref-` ids (matching still uses the folded id).
    display_labels: BTreeMap<String, String>,
    /// Running per-id reference counter consumed during the body render,
    /// interior-mutable so the read-only `&Ctx` thread can advance it.
    seen_during_render: BTreeMap<String, Cell<usize>>,
    /// Running inline-footnote counter consumed during the body render so the
    /// N-th `^[..]` maps to the same `__inline_{N}` minted in the pre-pass.
    inline_emitted: Cell<usize>,
}

impl FootnoteContext {
    /// True when at least one footnote was referenced (gates section output).
    pub fn has_any(&self) -> bool {
        !self.referenced.is_empty()
    }

    /// 1-based display number for an id (0 if unknown, which never happens for
    /// a referenced id).
    pub fn number(&self, id: &str) -> usize {
        self.numbers.get(id).copied().unwrap_or(0)
    }

    /// True when `id` resolves to a definition (regular or harvested inline).
    /// Undefined references are never registered, so this is `numbers`-backed.
    pub fn is_defined(&self, id: &str) -> bool {
        self.numbers.contains_key(id)
    }

    /// The DISPLAY id for `id`: the first definition's original-case label when
    /// known, else the id itself (covers `__inline_{N}`).
    fn display_id<'a>(&'a self, id: &'a str) -> &'a str {
        self.display_labels
            .get(id)
            .map(String::as_str)
            .unwrap_or(id)
    }
}

/// Pre-pass: build the footnote context from the whole document.
pub fn build(blocks: &[Block]) -> FootnoteContext {
    let mut display_labels: BTreeMap<String, String> = BTreeMap::new();
    let mut refs = RefBuilder {
        order: Vec::new(),
        numbers: BTreeMap::new(),
        ref_totals: BTreeMap::new(),
        referenced: BTreeSet::new(),
        defs: BTreeMap::new(),
        inline_counter: 0,
    };

    // First collect all regular definitions (anywhere in the tree).
    collect_defs(blocks, &mut refs.defs, &mut display_labels);

    // Then walk in document order to learn reference order + inline bodies.
    refs.walk_blocks(blocks);

    let seen_during_render = refs
        .order
        .iter()
        .map(|id| (id.clone(), Cell::new(0)))
        .collect();

    FootnoteContext {
        order: refs.order,
        numbers: refs.numbers,
        defs: refs.defs,
        ref_totals: refs.ref_totals,
        referenced: refs.referenced,
        display_labels,
        seen_during_render,
        inline_emitted: Cell::new(0),
    }
}

fn collect_defs(
    blocks: &[Block],
    defs: &mut BTreeMap<String, Vec<Block>>,
    display_labels: &mut BTreeMap<String, String>,
) {
    for block in blocks {
        match block {
            Block::FootnoteDefinition(fd) => {
                defs.entry(fd.identifier.clone())
                    .or_insert_with(|| fd.children.clone());
                // First definition for a folded id wins the displayed label.
                display_labels
                    .entry(fd.identifier.clone())
                    .or_insert_with(|| fd.label.clone());
            }
            Block::BlockQuote(bq) => collect_defs(&bq.children, defs, display_labels),
            Block::Alert(a) => collect_defs(&a.children, defs, display_labels),
            Block::List(list) => {
                for item in &list.children {
                    collect_defs(&item.children, defs, display_labels);
                }
            }
            Block::DescriptionList(dl) => {
                for item in &dl.children {
                    for details in &item.details {
                        collect_defs(&details.children, defs, display_labels);
                    }
                }
            }
            Block::ContainerDirective(dir) => collect_defs(&dir.children, defs, display_labels),
            Block::HtmlContainer(container) => {
                if let HtmlContainerContent::Blocks(children) = &container.content {
                    collect_defs(children, defs, display_labels);
                }
            }
            _ => {}
        }
    }
}

struct RefBuilder {
    order: Vec<String>,
    numbers: BTreeMap<String, usize>,
    defs: BTreeMap<String, Vec<Block>>,
    ref_totals: BTreeMap<String, usize>,
    referenced: BTreeSet<String>,
    inline_counter: usize,
}

impl RefBuilder {
    fn walk_blocks(&mut self, blocks: &[Block]) {
        for block in blocks {
            match block {
                Block::Paragraph(p) => self.walk_inlines(&p.children),
                Block::Heading(h) => self.walk_inlines(&h.children),
                Block::BlockQuote(bq) => self.walk_blocks(&bq.children),
                Block::Alert(a) => self.walk_blocks(&a.children),
                Block::List(list) => {
                    for item in &list.children {
                        self.walk_blocks(&item.children);
                    }
                }
                Block::DescriptionList(dl) => {
                    for item in &dl.children {
                        self.walk_inlines(&item.term);
                        for details in &item.details {
                            self.walk_blocks(&details.children);
                        }
                    }
                }
                Block::Table(table) => {
                    for row in &table.rows {
                        for cell in &row.cells {
                            self.walk_inlines(&cell.children);
                        }
                    }
                }
                Block::FootnoteDefinition(fd) => self.walk_blocks(&fd.children),
                Block::ContainerDirective(dir) => {
                    self.walk_inlines(&dir.label);
                    self.walk_blocks(&dir.children);
                }
                Block::HtmlContainer(container) => match &container.content {
                    HtmlContainerContent::Blocks(children) => self.walk_blocks(children),
                    HtmlContainerContent::Inlines(children) => self.walk_inlines(children),
                },
                Block::LeafDirective(dir) => self.walk_inlines(&dir.label),
                _ => {}
            }
        }
    }

    fn walk_inlines(&mut self, inlines: &[Inline]) {
        for inline in inlines {
            match inline {
                Inline::FootnoteReference(fr) => {
                    // Only register a reference that resolves to a definition; an
                    // undefined `[^x]` is rendered as literal text, not numbered.
                    if self.defs.contains_key(&fr.identifier) {
                        self.register_ref(&fr.identifier);
                    }
                }
                Inline::InlineFootnote(node) => {
                    self.inline_counter += 1;
                    let id = format!("__inline_{}", self.inline_counter);
                    self.defs
                        .insert(id.clone(), block_wrap_inline(&node.children));
                    self.register_ref(&id);
                    // Inline-footnote children may themselves contain footnotes.
                    self.walk_inlines(&node.children);
                }
                Inline::Emphasis(n) => self.walk_inlines(&n.children),
                Inline::Strong(n) => self.walk_inlines(&n.children),
                Inline::Underline(n) => self.walk_inlines(&n.children),
                Inline::Delete(n) => self.walk_inlines(&n.children),
                Inline::Insert(n) => self.walk_inlines(&n.children),
                Inline::Mark(n) => self.walk_inlines(&n.children),
                Inline::Subscript(n) => self.walk_inlines(&n.children),
                Inline::Superscript(n) => self.walk_inlines(&n.children),
                Inline::Spoiler(n) => self.walk_inlines(&n.children),
                Inline::Link(n) => self.walk_inlines(&n.children),
                Inline::LinkReference(n) => self.walk_inlines(&n.children),
                Inline::TextDirective(d) => self.walk_inlines(&d.label),
                _ => {}
            }
        }
    }

    fn register_ref(&mut self, id: &str) {
        if !self.numbers.contains_key(id) {
            self.order.push(String::from(id));
            self.numbers.insert(String::from(id), self.order.len());
        }
        *self.ref_totals.entry(String::from(id)).or_insert(0) += 1;
        self.referenced.insert(String::from(id));
    }
}

fn block_wrap_inline(children: &[Inline]) -> Vec<Block> {
    use crate::ast::{NodeMeta, Paragraph};
    alloc::vec![Block::Paragraph(Paragraph {
        meta: NodeMeta::default(),
        children: children.to_vec(),
    })]
}

/// Per-reference render hook: advance the running counter for `id` and return
/// the `(number, fnref_id_with_suffix)` pair for the `<sup>` marker.
pub fn reference_marker(ctx: &FootnoteContext, id: &str) -> (usize, String) {
    let number = ctx.number(id);
    let count = ctx
        .seen_during_render
        .get(id)
        .map(|c| {
            let v = c.get() + 1;
            c.set(v);
            v
        })
        .unwrap_or(1);
    let enc = footnote_id_encode(ctx.display_id(id));
    let fnref = if count == 1 {
        format!("fnref-{}", enc)
    } else {
        format!("fnref-{}-{}", enc, count)
    };
    (number, fnref)
}

/// The houdini+attr-encoded DISPLAY id for `id` (preserved-case label), used by
/// the `#fn-` href of a reference marker.
pub fn reference_fn_target(ctx: &FootnoteContext, id: &str) -> String {
    footnote_id_encode(ctx.display_id(id))
}

/// Resolve the id minted for the next inline footnote encountered during the
/// body render (matches the pre-pass `__inline_{N}` numbering).
pub fn next_inline_id(ctx: &FootnoteContext) -> String {
    let n = ctx.inline_emitted.get() + 1;
    ctx.inline_emitted.set(n);
    format!("__inline_{}", n)
}

/// GFM footnote id encoding: houdini href-encode, then attr-escape (so a
/// literal `'`→`&#x27;`). `__inline_{N}` is ASCII so this is a no-op there.
pub fn footnote_id_encode(id: &str) -> String {
    attr_escape_gfm(&encode_href(id))
}

/// Emit the document-end footnote section, or `""` when nothing referenced.
/// `render_blocks` renders a footnote body's blocks the same way the body
/// renderer renders any block list (joined with `\n`).
pub fn emit_footnote_section<F>(ctx: &FootnoteContext, render_blocks: F) -> String
where
    F: Fn(&[Block]) -> String,
{
    if !ctx.has_any() {
        return String::new();
    }
    let mut out = String::from("<section class=\"footnotes\" data-footnotes>\n<ol>\n");
    for id in &ctx.order {
        if !ctx.referenced.contains(id) {
            continue;
        }
        let number = ctx.number(id);
        let enc = footnote_id_encode(ctx.display_id(id));
        out.push_str(&format!("<li id=\"fn-{}\">\n", enc));

        let empty: Vec<Block> = Vec::new();
        let body = ctx.defs.get(id).unwrap_or(&empty);
        let rendered = render_blocks(body);
        let total = ctx.ref_totals.get(id).copied().unwrap_or(1);
        let backrefs = build_backrefs(&enc, number, total);

        out.push_str(&place_backrefs(&rendered, &backrefs));
        out.push('\n');
        out.push_str("</li>\n");
    }
    out.push_str("</ol>\n</section>");
    out
}

/// Build the space-joined backref anchors for a footnote referenced `total`
/// times.
fn build_backrefs(enc: &str, number: usize, total: usize) -> String {
    let mut anchors: Vec<String> = Vec::new();
    let total = total.max(1);
    for k in 1..=total {
        if k == 1 {
            anchors.push(format!(
                "<a href=\"#fnref-{enc}\" class=\"footnote-backref\" data-footnote-backref data-footnote-backref-idx=\"{number}\" aria-label=\"Back to reference {number}\">\u{21a9}</a>",
            ));
        } else {
            anchors.push(format!(
                "<a href=\"#fnref-{enc}-{k}\" class=\"footnote-backref\" data-footnote-backref data-footnote-backref-idx=\"{number}-{k}\" aria-label=\"Back to reference {number}-{k}\">\u{21a9}<sup class=\"footnote-ref\">{k}</sup></a>",
            ));
        }
    }
    anchors.join(" ")
}

/// Place the backref anchors: inside the last paragraph child when the body's
/// last block rendered as a `<p>…</p>` (append ` {backrefs}` before `</p>`);
/// otherwise on their own trailing line after the last block.
fn place_backrefs(rendered: &str, backrefs: &str) -> String {
    if backrefs.is_empty() {
        return String::from(rendered);
    }
    if rendered.is_empty() {
        return String::from(backrefs);
    }
    if rendered.ends_with("</p>") {
        let head = &rendered[..rendered.len() - "</p>".len()];
        return format!("{head} {backrefs}</p>");
    }
    format!("{rendered}\n{backrefs}")
}
