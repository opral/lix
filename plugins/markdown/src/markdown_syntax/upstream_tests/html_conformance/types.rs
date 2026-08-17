//! Shared, dependency-free types for the AST→HTML conformance bench.
//!
//! These are the FROZEN interface between the extractor and runner/report. The
//! extractor produces [`OracleTuple`]s; the runner maps each tuple's captured
//! option tokens to parse [`crate::markdown_syntax::SyntaxOptions`] plus public
//! [`crate::markdown_syntax::HtmlOptions`].
//!
//! The bench renders ONE convention (GFM). The only spec-layer split that
//! survives is the suite [`Category`] (commonmark vs gfm), kept purely so a
//! handful of category-divergent oracle conventions (raw-HTML safe-mode form,
//! task-list checkbox attribute order) and the per-suite report grouping stay
//! faithful. It does NOT switch the rendering convention: math is always GFM.

/// Which suite directory a tuple came from (`suite/commonmark/` vs
/// `suite/gfm/`). A spec-layer grouping label for reporting, plus the two
/// oracle conventions whose expected HTML legitimately differs by suite layer
/// (safe-mode raw HTML, task-list checkbox attribute order).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Category {
    CommonMark,
    Gfm,
}

/// One extracted `(markdown input → expected HTML)` oracle case.
#[derive(Clone, Debug)]
pub struct OracleTuple {
    /// Suite-relative path of the `.cases` file this came from (for reporting).
    pub source_file: &'static str,
    /// Suite-layer category, derived from the fixture subdirectory.
    pub category: Category,
    /// Human label (CommonMark 3rd assert arg, or synthesized `fn`+index).
    pub label: Option<String>,
    /// The markdown source fed to the parser.
    pub input: String,
    /// The expected HTML from the upstream oracle.
    pub expected_html: String,
    /// Raw option identifiers captured at the call site, e.g.
    /// `"allow_dangerous_html"`, `"Options::gfm"`, `"extension.table"`,
    /// `"render.unsafe_"`, `"ParseOptions::mdx"`, `"closure"`, `"math"`.
    /// The runner interprets these into parse options + [`crate::markdown_syntax::HtmlOptions`].
    pub option_tokens: Vec<String>,
}
