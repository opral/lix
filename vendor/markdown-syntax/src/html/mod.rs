//! AST to HTML rendering.
//!
//! The renderer is safe by default: raw HTML is escaped, dangerous protocols
//! are blanked, and image `src` values use the same protocol filter unless
//! explicitly relaxed through [`HtmlOptions`].

mod blocks;
mod escape;
mod footnotes;
mod inlines;
mod refs;
mod tables;

use alloc::{string::String, vec::Vec};

use crate::{ast::Document, diagnostic::Diagnostic, validate::validate_document};

use self::footnotes::FootnoteContext;
use self::refs::DefMap;

/// How raw HTML is handled in safe mode (when `allow_dangerous_html` is off).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SafeRawHtmlForm {
    /// Escape raw HTML as text. This is the default safe form and uses the
    /// CommonMark/micromark link-scheme allowlist for unknown URI schemes.
    EscapeText,
    /// Emit the GFM raw-HTML placeholder. This matches cmark-gfm oracle
    /// conventions, including its link-scheme denylist for unknown URI schemes.
    OmitPlaceholder,
}

/// The attribute order on disabled task-list checkbox inputs (an oracle-parity
/// convention).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TasklistAttrOrder {
    /// Emit `disabled=""` before `checked=""` on disabled task-list inputs.
    DisabledFirst,
    /// Emit `checked=""` before `disabled=""` on disabled task-list inputs.
    CheckedFirst,
}

/// HTML rendering options. The default is safe: raw HTML is escaped, dangerous
/// link/image protocols are blanked, and task-list checkboxes are disabled.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct HtmlOptions {
    /// Emit raw HTML blocks/inlines verbatim.
    pub allow_dangerous_html: bool,
    /// Keep dangerous link/image protocols instead of blanking the attribute.
    pub allow_dangerous_protocol: bool,
    /// Let image `src` values bypass the protocol filter.
    pub allow_any_img_src: bool,
    /// Apply the GFM tagfilter to raw HTML when dangerous HTML is enabled.
    pub gfm_tagfilter: bool,
    /// Omit `disabled=""` from task-list checkbox inputs.
    pub tasklist_checkable: bool,
    /// Safe-mode raw HTML convention.
    pub safe_raw_html_form: SafeRawHtmlForm,
    /// Attribute ordering convention for disabled task-list checkbox inputs.
    pub tasklist_attr_order: TasklistAttrOrder,
}

impl Default for HtmlOptions {
    fn default() -> Self {
        Self {
            allow_dangerous_html: false,
            allow_dangerous_protocol: false,
            allow_any_img_src: false,
            gfm_tagfilter: false,
            tasklist_checkable: false,
            safe_raw_html_form: SafeRawHtmlForm::EscapeText,
            tasklist_attr_order: TasklistAttrOrder::DisabledFirst,
        }
    }
}

/// Why HTML rendering failed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HtmlError {
    /// The AST failed validation before rendering.
    InvalidDocument(Vec<Diagnostic>),
}

/// Render-time context threaded through the block/inline renderers. Holds the
/// resolved definition map, the footnote document state, and the config flags.
pub(crate) struct Ctx<'a> {
    pub defs: &'a DefMap,
    pub footnotes: &'a FootnoteContext,
    pub allow_dangerous_html: bool,
    pub allow_dangerous_protocol: bool,
    pub allow_any_img_src: bool,
    pub gfm_tagfilter: bool,
    pub tasklist_checkable: bool,
    pub safe_raw_html_form: SafeRawHtmlForm,
    pub tasklist_attr_order: TasklistAttrOrder,
}

impl<'a> Ctx<'a> {
    /// GFM (cmark-gfm) suites use a URL-scheme denylist (keep everything but
    /// `javascript:`/`vbscript:`/`file:`/`data:`); CommonMark (micromark)
    /// suites use an allowlist. The two oracles genuinely disagree on unknown
    /// schemes (cmark-gfm keeps `smb:`; micromark blanks `made-up-scheme:`), so
    /// the conformance runner selects the GFM policy through the GFM raw-HTML
    /// convention option.
    pub fn gfm_url_denylist(&self) -> bool {
        matches!(self.safe_raw_html_form, SafeRawHtmlForm::OmitPlaceholder)
    }
}

impl Document {
    /// Render this document to safe-by-default HTML with default options.
    pub fn to_html(&self) -> Result<String, HtmlError> {
        self.to_html_with(&HtmlOptions::default())
    }

    /// Render this document to HTML with explicit options.
    pub fn to_html_with(&self, options: &HtmlOptions) -> Result<String, HtmlError> {
        let diagnostics = validate_document(self);
        if !diagnostics.is_empty() {
            return Err(HtmlError::InvalidDocument(diagnostics));
        }

        Ok(render_document(self, options))
    }
}

/// Render a whole validated document to HTML.
///
/// Top-level blocks are rendered, empty-string results are filtered, survivors
/// are joined with a single `\n` (no leading/trailing newline), then the
/// document-end footnote section is appended when any footnote was referenced.
fn render_document(doc: &Document, options: &HtmlOptions) -> String {
    let defs = DefMap::build(&doc.children);
    let footnote_ctx = footnotes::build(&doc.children);

    let ctx = Ctx {
        defs: &defs,
        footnotes: &footnote_ctx,
        allow_dangerous_html: options.allow_dangerous_html,
        allow_dangerous_protocol: options.allow_dangerous_protocol,
        allow_any_img_src: options.allow_any_img_src,
        gfm_tagfilter: options.gfm_tagfilter,
        tasklist_checkable: options.tasklist_checkable,
        safe_raw_html_form: options.safe_raw_html_form,
        tasklist_attr_order: options.tasklist_attr_order,
    };

    let mut out = blocks::render_blocks_joined(&doc.children, &ctx);

    let section = footnotes::emit_footnote_section(&footnote_ctx, |body| {
        blocks::render_blocks_joined(body, &ctx)
    });
    if !section.is_empty() {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&section);
    }

    out
}
