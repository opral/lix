//! Parser configuration: which Markdown constructs are recognized and how.
//!
//! [`SyntaxOptions`] is the entry point — pick a preset, optionally tune it with
//! the [`Construct`] builder, then call [`SyntaxOptions::parse`]. [`Constructs`]
//! is the exhaustive per-feature flag set behind it, and [`ParseOptions`] holds
//! the lexing knobs.

use alloc::string::String;

/// The full set of syntactic constructs the parser may recognize, one boolean
/// per feature. This is the exhaustive escape hatch; most callers use the
/// [`Constructs::commonmark`]/[`gfm`](Constructs::gfm)/[`mdx`](Constructs::mdx)/
/// [`max`](Constructs::max) presets or the [`Construct`] builder instead of
/// setting fields directly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Constructs {
    /// Raw HTML blocks, e.g. a `<div>…</div>` block at the top level.
    pub html_block: bool,
    /// Raw inline HTML, e.g. `<span>` within a paragraph.
    pub html_inline: bool,
    /// Structured Markdown-compatible HTML containers such as `<details>`.
    pub html_container: bool,
    /// Indented code blocks (each line indented four spaces or a tab).
    pub indented_code: bool,
    /// GFM pipe tables: a `| a | b |` row over a `|---|---|` delimiter row.
    pub gfm_table: bool,
    /// GFM task list items: `- [ ]` (unchecked) and `- [x]` (checked).
    pub gfm_task_list_item: bool,
    /// GFM strikethrough: `~~text~~`.
    pub gfm_strikethrough: bool,
    /// GFM literal autolinks: a bare `https://…`, `www.…`, or email becomes a
    /// link without angle brackets.
    pub gfm_autolink_literal: bool,
    /// cmark-gfm "relaxed" URL autolinks: bare `scheme://` URLs (and a bare
    /// leading `://`) are auto-linkified without angle brackets, e.g. `smb://`,
    /// `irc://`, `rdar://`. This is a cmark extension beyond the GFM spec (which
    /// defines only `http(s)://`/`www.`/email); on by default in `gfm()` for
    /// GitHub/cmark-gfm parity. The angle form `<scheme:…>` works regardless.
    pub relaxed_autolinks: bool,
    /// GFM alerts: a `> [!NOTE]` (TIP/IMPORTANT/WARNING/CAUTION) blockquote.
    pub gfm_alert: bool,
    /// Underline spans: `__text__`. This overrides CommonMark's `__`-as-strong,
    /// so it is off in the [`max`](Constructs::max) default.
    pub underline: bool,
    /// CriticMarkup-style insertions: `++text++`.
    pub insert: bool,
    /// Highlight / "mark" spans: `==text==`.
    pub highlight: bool,
    /// Subscript: a single-tilde span `~text~` (no spaces).
    pub subscript: bool,
    /// Superscript: `^text^`.
    pub superscript: bool,
    /// Spoiler spans: `||text||`.
    pub spoiler: bool,
    /// Emoji-style shortcodes: `:tada:`.
    pub shortcode: bool,
    /// Description (definition) lists: a term followed by `:`-led details.
    pub description_list: bool,
    /// Footnote definitions: `[^1]: the footnote body`.
    pub footnote_definition: bool,
    /// Footnote references: `[^1]` in running text.
    pub footnote_reference: bool,
    /// Inline footnotes: `^[the note inline]` (also needs `footnote_reference`).
    pub inline_footnote: bool,
    /// Block math: a `$$ … $$` fenced block.
    pub math_block: bool,
    /// Inline math: `$x$` (and the math-code form `` $`x`$ ``).
    pub math_inline: bool,
    /// A leading frontmatter block at the start of the document: `---` YAML or
    /// `+++` TOML.
    pub frontmatter: bool,
    /// Wikilinks with the display title after the pipe: `[[target|title]]`
    /// (the Obsidian convention). Mutually exclusive with the before-pipe order.
    pub wikilink_title_after_pipe: bool,
    /// Wikilinks with the display title before the pipe: `[[title|target]]`.
    /// Mutually exclusive with the after-pipe order.
    pub wikilink_title_before_pipe: bool,
    /// MDX ESM: `import`/`export` statement lines.
    pub mdx_esm: bool,
    /// MDX block-level `{ … }` expressions.
    pub mdx_expression_block: bool,
    /// MDX inline `{ … }` expressions within text.
    pub mdx_expression_inline: bool,
    /// MDX block-level JSX: `<Component/>` as a block. Conflicts with raw HTML.
    pub mdx_jsx_block: bool,
    /// MDX inline JSX: `<Component/>` within text. Conflicts with raw HTML.
    pub mdx_jsx_inline: bool,
    /// Inline directive: `:name[label]{key=val}`. A directive, not MDX.
    pub directive_text: bool,
    /// Leaf directive: `::name[label]{key=val}` on its own line. A directive,
    /// not MDX.
    pub directive_leaf: bool,
    /// Container directive: a `:::name … :::` fenced block. A directive, not MDX.
    pub directive_container: bool,
}

impl Constructs {
    /// The CommonMark baseline: raw HTML and indented code, no extensions.
    pub const fn commonmark() -> Self {
        Self {
            html_block: true,
            html_inline: true,
            html_container: false,
            indented_code: true,
            gfm_table: false,
            gfm_task_list_item: false,
            gfm_strikethrough: false,
            gfm_autolink_literal: false,
            relaxed_autolinks: false,
            gfm_alert: false,
            underline: false,
            insert: false,
            highlight: false,
            subscript: false,
            superscript: false,
            spoiler: false,
            shortcode: false,
            description_list: false,
            footnote_definition: false,
            footnote_reference: false,
            inline_footnote: false,
            math_block: false,
            math_inline: false,
            frontmatter: false,
            wikilink_title_after_pipe: false,
            wikilink_title_before_pipe: false,
            mdx_esm: false,
            mdx_expression_block: false,
            mdx_expression_inline: false,
            mdx_jsx_block: false,
            mdx_jsx_inline: false,
            directive_text: false,
            directive_leaf: false,
            directive_container: false,
        }
    }

    /// GitHub Flavored Markdown: CommonMark plus tables, task lists,
    /// strikethrough, literal autolinks, and footnotes.
    pub const fn gfm() -> Self {
        let mut constructs = Self::commonmark();
        constructs.gfm_table = true;
        constructs.gfm_task_list_item = true;
        constructs.gfm_strikethrough = true;
        constructs.gfm_autolink_literal = true;
        constructs.relaxed_autolinks = true;
        constructs.footnote_definition = true;
        constructs.footnote_reference = true;
        constructs
    }

    /// MDX: CommonMark with raw HTML and indented code off, and MDX ESM,
    /// expressions, and JSX on.
    pub const fn mdx() -> Self {
        let mut constructs = Self::commonmark();
        constructs.html_block = false;
        constructs.html_inline = false;
        constructs.html_container = false;
        constructs.indented_code = false;
        constructs.mdx_esm = true;
        constructs.mdx_expression_block = true;
        constructs.mdx_expression_inline = true;
        constructs.mdx_jsx_block = true;
        constructs.mdx_jsx_inline = true;
        constructs
    }

    /// The maximal non-MDX construct set, and the default dialect: every
    /// construct that does not reinterpret a core CommonMark delimiter. MDX is
    /// off (it conflicts with raw HTML and reinterprets `{…}`/`<…>`), and
    /// `underline` is off because it would parse `__bold__` as underline,
    /// overriding CommonMark strong. The wikilink title order is after-pipe.
    pub const fn max() -> Self {
        Self {
            html_block: true,
            html_inline: true,
            html_container: true,
            indented_code: true,
            gfm_table: true,
            gfm_task_list_item: true,
            gfm_strikethrough: true,
            gfm_autolink_literal: true,
            relaxed_autolinks: true,
            gfm_alert: true,
            underline: false,
            insert: true,
            highlight: true,
            subscript: true,
            superscript: true,
            spoiler: true,
            shortcode: true,
            description_list: true,
            footnote_definition: true,
            footnote_reference: true,
            inline_footnote: true,
            math_block: true,
            math_inline: true,
            frontmatter: true,
            wikilink_title_after_pipe: true,
            wikilink_title_before_pipe: false,
            mdx_esm: false,
            mdx_expression_block: false,
            mdx_expression_inline: false,
            mdx_jsx_block: false,
            mdx_jsx_inline: false,
            directive_text: true,
            directive_leaf: true,
            directive_container: true,
        }
    }
}

impl Default for Constructs {
    fn default() -> Self {
        Self::max()
    }
}

/// Lexing knobs that tune how existing constructs are read or how source text is
/// preserved, separate from which constructs are recognized ([`Constructs`]).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ParseOptions {
    /// Treat a single `~text~` as strikethrough (in addition to `~~text~~`).
    /// Inert unless `gfm_strikethrough` is also enabled.
    pub single_tilde_strikethrough: bool,
    /// Keep backslash character escapes (e.g. `\*`) as `Escape` nodes instead of
    /// folding them into text, so the original source can be reproduced.
    pub preserve_character_escapes: bool,
    /// Keep character references (e.g. `&amp;`) as `CharacterReference` nodes
    /// instead of resolving them to their value.
    pub preserve_character_references: bool,
}

/// A full syntax configuration: which [`Constructs`] are recognized plus the
/// [`ParseOptions`] lexing knobs. Build one with a preset
/// ([`commonmark`](SyntaxOptions::commonmark)/[`gfm`](SyntaxOptions::gfm)/
/// [`mdx`](SyntaxOptions::mdx)/[`default`](SyntaxOptions::default)), optionally
/// tune it with [`enable`](SyntaxOptions::enable)/[`disable`](SyntaxOptions::disable),
/// then call [`parse`](SyntaxOptions::parse).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyntaxOptions {
    /// Which syntactic constructs are recognized.
    pub constructs: Constructs,
    /// Lexing / source-preservation knobs.
    pub parse: ParseOptions,
}

impl SyntaxOptions {
    /// The strict CommonMark dialect.
    pub fn commonmark() -> Self {
        Self {
            constructs: Constructs::commonmark(),
            parse: ParseOptions::default(),
        }
    }

    /// GitHub Flavored Markdown (also enables single-tilde strikethrough).
    pub fn gfm() -> Self {
        Self {
            constructs: Constructs::gfm(),
            parse: ParseOptions {
                single_tilde_strikethrough: true,
                preserve_character_escapes: false,
                preserve_character_references: false,
            },
        }
    }

    /// The MDX dialect (JSX, expressions, ESM; no raw HTML).
    pub fn mdx() -> Self {
        Self {
            constructs: Constructs::mdx(),
            parse: ParseOptions::default(),
        }
    }

    /// Enable a [`Construct`] on top of these options, returning the modified
    /// options for chaining. Grouped constructs (footnotes, math, directives, …)
    /// flip every flag in the group so no member is left silently inert.
    pub fn enable(mut self, construct: Construct) -> Self {
        construct.apply(&mut self.constructs, true);
        self
    }

    /// Disable a [`Construct`], the inverse of [`SyntaxOptions::enable`].
    pub fn disable(mut self, construct: Construct) -> Self {
        construct.apply(&mut self.constructs, false);
        self
    }

    /// Check for contradictory construct combinations (MDX JSX with raw HTML;
    /// both wikilink title orders). Returns `Ok(())` for every preset; only a
    /// hand-built config can trip a [`SyntaxConfigError`].
    pub fn validate(&self) -> Result<(), SyntaxConfigError> {
        if (self.constructs.mdx_jsx_block || self.constructs.mdx_jsx_inline)
            && (self.constructs.html_block
                || self.constructs.html_inline
                || self.constructs.html_container)
        {
            return Err(SyntaxConfigError::MdxHtmlConflict);
        }
        if self.constructs.wikilink_title_after_pipe && self.constructs.wikilink_title_before_pipe {
            return Err(SyntaxConfigError::WikilinkTitleOrderConflict);
        }

        Ok(())
    }
}

impl Default for SyntaxOptions {
    fn default() -> Self {
        Self {
            constructs: Constructs::max(),
            parse: ParseOptions::default(),
        }
    }
}

/// Where a wikilink's display title sits relative to the `|` separator. The two
/// orders are mutually exclusive ([`SyntaxConfigError::WikilinkTitleOrderConflict`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WikiLinkOrder {
    /// `[[target|title]]` — the Obsidian convention, and the maximal default.
    TitleAfterPipe,
    /// `[[title|target]]`.
    TitleBeforePipe,
}

/// A discoverable, typo-proof front door for toggling a syntax feature via
/// [`SyntaxOptions::enable`] / [`SyntaxOptions::disable`]. Each variant maps to
/// one conceptual feature; grouped features flip every underlying [`Constructs`]
/// flag together. The raw [`Constructs`] struct remains the exhaustive escape
/// hatch for fine-grained control.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Construct {
    /// GFM pipe tables: `| a | b |` over `|---|---|`.
    Table,
    /// GFM task list items: `- [ ]` / `- [x]`.
    TaskList,
    /// Strikethrough: `~~text~~`.
    Strikethrough,
    /// GFM literal autolinks plus the cmark relaxed `scheme://` extension.
    Autolink,
    /// GFM alerts: `> [!NOTE]` callouts.
    Alert,
    /// Footnote definitions, references, and inline footnotes.
    Footnotes,
    /// Inline and block math.
    Math,
    /// A leading `---`/`+++` frontmatter block.
    Frontmatter,
    /// Underline: `__text__` (overrides CommonMark strong).
    Underline,
    /// Insertions: `++text++`.
    Insert,
    /// Highlight / mark: `==text==`.
    Highlight,
    /// Subscript: `~text~`.
    Subscript,
    /// Superscript: `^text^`.
    Superscript,
    /// Spoilers: `||text||`.
    Spoiler,
    /// Emoji-style shortcodes: `:tada:`.
    Shortcode,
    /// Description / definition lists.
    DescriptionList,
    /// Wikilinks `[[…]]` with the given title order.
    Wikilinks(WikiLinkOrder),
    /// Markdown-compatible raw HTML containers such as `<details>`.
    HtmlContainers,
    /// MDX JSX (block and inline). Conflicts with raw HTML; pair with
    /// `disable`-ing HTML or start from [`SyntaxOptions::mdx`].
    MdxJsx,
    /// MDX `{…}` expressions (block and inline).
    MdxExpressions,
    /// MDX ESM `import`/`export` lines.
    MdxEsm,
    /// The `:name` / `::name` / `:::name` directive family.
    Directives,
}

impl Construct {
    fn apply(self, c: &mut Constructs, on: bool) {
        match self {
            Construct::Table => c.gfm_table = on,
            Construct::TaskList => c.gfm_task_list_item = on,
            Construct::Strikethrough => c.gfm_strikethrough = on,
            Construct::Autolink => {
                c.gfm_autolink_literal = on;
                c.relaxed_autolinks = on;
            }
            Construct::Alert => c.gfm_alert = on,
            Construct::Footnotes => {
                c.footnote_definition = on;
                c.footnote_reference = on;
                c.inline_footnote = on;
            }
            Construct::Math => {
                c.math_block = on;
                c.math_inline = on;
            }
            Construct::Frontmatter => c.frontmatter = on,
            Construct::Underline => c.underline = on,
            Construct::Insert => c.insert = on,
            Construct::Highlight => c.highlight = on,
            Construct::Subscript => c.subscript = on,
            Construct::Superscript => c.superscript = on,
            Construct::Spoiler => c.spoiler = on,
            Construct::Shortcode => c.shortcode = on,
            Construct::DescriptionList => c.description_list = on,
            Construct::Wikilinks(order) => {
                c.wikilink_title_after_pipe = on && matches!(order, WikiLinkOrder::TitleAfterPipe);
                c.wikilink_title_before_pipe =
                    on && matches!(order, WikiLinkOrder::TitleBeforePipe);
            }
            Construct::HtmlContainers => c.html_container = on,
            Construct::MdxJsx => {
                c.mdx_jsx_block = on;
                c.mdx_jsx_inline = on;
            }
            Construct::MdxExpressions => {
                c.mdx_expression_block = on;
                c.mdx_expression_inline = on;
            }
            Construct::MdxEsm => c.mdx_esm = on,
            Construct::Directives => {
                c.directive_text = on;
                c.directive_leaf = on;
                c.directive_container = on;
            }
        }
    }
}

/// A contradictory [`SyntaxOptions`] configuration, reported by
/// [`SyntaxOptions::validate`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyntaxConfigError {
    /// MDX JSX and raw HTML were both enabled; they both claim `<`.
    MdxHtmlConflict,
    /// Both wikilink title orders (before- and after-pipe) were enabled.
    WikilinkTitleOrderConflict,
}

impl SyntaxConfigError {
    /// A human-readable description of the conflict.
    pub fn message(&self) -> String {
        match self {
            Self::MdxHtmlConflict => "MDX JSX and raw HTML syntax cannot both be enabled".into(),
            Self::WikilinkTitleOrderConflict => {
                "wikilink title-before-pipe and title-after-pipe cannot both be enabled".into()
            }
        }
    }
}
