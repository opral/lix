//! Maps each suite case's captured options → parse [`SyntaxOptions`] +
//! [`HtmlOptions`], runs parse→render→compare, and collects [`Report`].
//!
//! This is the single place the messy option vocabulary is interpreted, so the
//! suite fixtures can stay faithful token-capturers and the renderer a pure
//! function of `(Document, HtmlOptions)`.
//!
//! The fixtures store only runnable cases, so there is no skip path here: every
//! case maps to a `(SyntaxOptions, HtmlOptions)` and is run.

use crate::markdown_syntax::{HtmlOptions, SafeRawHtmlForm, SyntaxOptions, TasklistAttrOrder};

use super::extractor;
use super::normalizer::compare;
use super::report::{CaseResult, Outcome, Report};
use super::types::{Category, OracleTuple};

fn token(t: &OracleTuple, name: &str) -> bool {
    t.option_tokens.iter().any(|tok| tok == name)
}

fn ext(t: &OracleTuple, name: &str) -> bool {
    // GFM bracket entry, e.g. "extension.table"
    let needle = format!("extension.{name}");
    t.option_tokens.iter().any(|tok| tok == &needle)
}

/// Map a case's option tokens to parse options + render options.
///
/// The two former per-suite token vocabularies are token-DISJOINT: the
/// commonmark-suite tokens (`gfm`, `math`, `frontmatter`, `*_off`, plus the
/// danger/tagfilter/tasklist render tokens) and the gfm-suite tokens
/// (`extension.*`, `render.unsafe*`, `render.tasklist_classes`) never co-occur,
/// so both blocks are applied unconditionally — each clause only fires for
/// tokens that are actually present. The render config always uses the single
/// GFM math form; only the suite [`Category`] is carried for the two
/// category-divergent oracle conventions.
fn plan(t: &OracleTuple) -> (SyntaxOptions, HtmlOptions) {
    // Base parse options: the `gfm` token selects the GFM preset; otherwise
    // CommonMark, then the gfm-suite extension toggles are layered on top.
    let mut opts = if token(t, "gfm") {
        SyntaxOptions::gfm()
    } else {
        SyntaxOptions::commonmark()
    };

    // --- commonmark-suite token block ---
    if token(t, "math") {
        opts.constructs.math_block = true;
        opts.constructs.math_inline = true;
    }
    if token(t, "frontmatter") {
        opts.constructs.frontmatter = true;
    }
    if token(t, "code_indented_off") {
        opts.constructs.indented_code = false;
    }
    if token(t, "html_flow_off") {
        opts.constructs.html_block = false;
    }
    if token(t, "single_tilde_off") {
        opts.parse.single_tilde_strikethrough = false;
    }

    // --- gfm-suite extension block ---
    let c = &mut opts.constructs;
    if ext(t, "table") {
        c.gfm_table = true;
    }
    if ext(t, "strikethrough") {
        c.gfm_strikethrough = true;
        // The GFM strikethrough extension treats a single `~` as a valid
        // delimiter by default.
        opts.parse.single_tilde_strikethrough = true;
    }
    if ext(t, "tasklist") {
        c.gfm_task_list_item = true;
    }
    if ext(t, "autolink") {
        c.gfm_autolink_literal = true;
    }
    // cmark-gfm "relaxed" URL autolinks. The `gfm()` preset turns this on for
    // the PUBLIC api, but the bench must reproduce each oracle case's exact
    // option set, so set it explicitly from the token (overriding the preset):
    // strict autolink cases run with it off, `parse.relaxed_autolinks` cases on.
    c.relaxed_autolinks = token(t, "parse.relaxed_autolinks");
    if ext(t, "footnotes") {
        c.footnote_definition = true;
        c.footnote_reference = true;
        c.inline_footnote = true;
    }
    if ext(t, "alerts") {
        c.gfm_alert = true;
    }
    if ext(t, "description_lists") {
        c.description_list = true;
    }
    if ext(t, "underline") {
        c.underline = true;
    }
    if ext(t, "subscript") {
        c.subscript = true;
    }
    if ext(t, "superscript") {
        c.superscript = true;
    }
    if ext(t, "spoiler") {
        c.spoiler = true;
    }
    if ext(t, "shortcodes") {
        c.shortcode = true;
    }
    if ext(t, "highlight") {
        c.highlight = true;
    }
    if ext(t, "insert") {
        c.insert = true;
    }
    if ext(t, "wikilinks_title_before_pipe") {
        c.wikilink_title_before_pipe = true;
    }
    if ext(t, "wikilinks_title_after_pipe") {
        c.wikilink_title_after_pipe = true;
    }
    if ext(t, "math_dollars") {
        c.math_inline = true;
        // GFM has no flow math block from `$$`: a `$$…$$` run spanning
        // newlines stays an inline display span inside the paragraph, so
        // `math_block` is intentionally left off here.
    }
    if ext(t, "math_code") {
        // GFM math_code (`` $`…`$ ``) is an INLINE construct.
        c.math_inline = true;
    }

    // Render options: single GFM math form; flags from both token vocabularies.
    let mut cfg = HtmlOptions::default();
    cfg.safe_raw_html_form = match t.category {
        Category::CommonMark => SafeRawHtmlForm::EscapeText,
        Category::Gfm => SafeRawHtmlForm::OmitPlaceholder,
    };
    cfg.tasklist_attr_order = match t.category {
        Category::CommonMark => TasklistAttrOrder::DisabledFirst,
        Category::Gfm => TasklistAttrOrder::CheckedFirst,
    };
    cfg.allow_dangerous_html = token(t, "allow_dangerous_html");
    cfg.allow_dangerous_protocol = token(t, "allow_dangerous_protocol");
    cfg.allow_any_img_src = token(t, "allow_any_img_src");
    cfg.gfm_tagfilter = token(t, "gfm_tagfilter");
    cfg.tasklist_checkable = token(t, "tasklist_checkable");
    // GFM `render.unsafe_` (raw identifier `render.r#unsafe`) → danger.
    if token(t, "render.unsafe_") || token(t, "render.r#unsafe") || token(t, "render.unsafe") {
        cfg.allow_dangerous_html = true;
        cfg.allow_dangerous_protocol = true;
    }
    if ext(t, "tagfilter") {
        cfg.gfm_tagfilter = true;
    }
    if token(t, "render.tasklist_classes") {
        cfg.tasklist_checkable = true;
    }

    (opts, cfg)
}

pub fn run_all() -> Report {
    let tuples = extractor::load_all();
    let mut results = Vec::with_capacity(tuples.len());

    for t in &tuples {
        let (opts, cfg) = plan(t);
        let output = opts.parse(&t.input);
        let outcome = match output.document.to_html_with(&cfg) {
            Ok(html) => {
                let cmp = compare(&html, &t.expected_html);
                if cmp.raw_match {
                    Outcome::PassRaw
                } else if cmp.normalized_match {
                    Outcome::PassNormalized
                } else {
                    Outcome::Fail {
                        input: t.input.clone(),
                        expected: t.expected_html.clone(),
                        actual: html,
                    }
                }
            }
            Err(e) => Outcome::ParseError(format!("html render error: {e:?}")),
        };
        results.push(CaseResult {
            source_file: t.source_file,
            category: t.category,
            label: t.label.clone(),
            outcome,
        });
    }

    Report { results }
}
