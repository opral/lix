//! The owned Markdown AST that [`parse()`](crate::parse::parse) produces and
//! `Document::to_markdown`/`to_html` consume. Every node carries a [`NodeMeta`]
//! with an optional source [`Span`]. [`Block`] and [`Inline`] are the two node
//! enums; everything else is a concrete node struct or a small enum describing
//! a node's variant.

use alloc::{string::String, vec::Vec};

use crate::span::Span;

/// Metadata attached to every AST node; currently just the source span.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeMeta {
    /// The node's source location, or `None` for hand-built nodes.
    pub span: Option<Span>,
}

impl NodeMeta {
    /// Wrap an optional [`Span`] into a [`NodeMeta`].
    pub const fn new(span: Option<Span>) -> Self {
        Self { span }
    }
}

/// The root of a parsed document: a sequence of top-level [`Block`]s.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Document {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The document's top-level blocks, in source order.
    pub children: Vec<Block>,
}

/// A block-level node: the building blocks of a document's vertical structure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Block {
    /// A paragraph of inline content.
    Paragraph(Paragraph),
    /// An ATX (`# h`) or setext (underlined) heading.
    Heading(Heading),
    /// A thematic break / horizontal rule: `---`, `***`, or `___`.
    ThematicBreak(ThematicBreak),
    /// A block quote: lines prefixed with `> `.
    BlockQuote(BlockQuote),
    /// A GFM alert / admonition: `> [!NOTE]` etc.
    Alert(Alert),
    /// A bullet or ordered list.
    List(List),
    /// A description / definition list (term + details).
    DescriptionList(DescriptionList),
    /// A fenced (```` ``` ````) or indented code block.
    CodeBlock(CodeBlock),
    /// A raw HTML block.
    HtmlBlock(HtmlBlock),
    /// A structured raw HTML container whose interior is parsed as Markdown.
    HtmlContainer(HtmlContainer),
    /// A link reference definition: `[label]: url "title"`.
    Definition(Definition),
    /// A footnote definition: `[^id]: text`.
    FootnoteDefinition(FootnoteDefinition),
    /// A GFM pipe table.
    Table(Table),
    /// A display math block: `$$ … $$`.
    MathBlock(MathBlock),
    /// A leading frontmatter block (`---` YAML or `+++` TOML).
    Frontmatter(Frontmatter),
    /// An MDX ESM block (`import`/`export` statements).
    MdxEsm(MdxEsm),
    /// A block-level MDX expression: `{ … }`.
    MdxExpression(MdxExpression),
    /// A block-level MDX JSX element.
    MdxJsx(MdxJsx),
    /// A leaf directive: `::name[label]{attrs}` (distinct from MDX).
    LeafDirective(LeafDirective),
    /// A container directive: `:::name … :::` (distinct from MDX).
    ContainerDirective(ContainerDirective),
}

/// A paragraph: a run of inline content. Source: any plain text line(s).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Paragraph {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The paragraph's inline content.
    pub children: Vec<Inline>,
}

/// A heading. Source: `# Title` (ATX) or `Title\n===` (setext).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Heading {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// Heading level, 1..=6.
    pub depth: u8,
    /// Whether the heading used ATX or setext syntax.
    pub kind: HeadingKind,
    /// The heading's inline content.
    pub children: Vec<Inline>,
}

/// Which heading syntax produced a [`Heading`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeadingKind {
    /// ATX heading: `# Title` … `###### Title`.
    Atx,
    /// Setext heading: `Title` underlined with `===` (level 1) or `---` (level 2).
    Setext,
}

/// A thematic break / horizontal rule. Source: `---`, `***`, or `___`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ThematicBreak {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// Which character formed the break.
    pub marker: ThematicBreakMarker,
}

/// The character used to draw a [`ThematicBreak`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThematicBreakMarker {
    /// Dashes: `---`.
    Dash,
    /// Asterisks: `***`.
    Asterisk,
    /// Underscores: `___`.
    Underscore,
}

/// A block quote: content prefixed with `> `.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockQuote {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The quoted block content.
    pub children: Vec<Block>,
}

/// A GFM alert / admonition. Source: `> [!NOTE]` followed by quoted content.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Alert {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The alert severity / type.
    pub kind: AlertKind,
    /// An optional custom title following the `[!KIND]` marker.
    pub title: Option<String>,
    /// The alert's block content.
    pub children: Vec<Block>,
}

/// The kind of a GFM [`Alert`] (the `[!KIND]` marker).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlertKind {
    /// `> [!NOTE]`.
    Note,
    /// `> [!TIP]`.
    Tip,
    /// `> [!IMPORTANT]`.
    Important,
    /// `> [!WARNING]`.
    Warning,
    /// `> [!CAUTION]`.
    Caution,
}

/// A bullet or ordered list. Source: `- a` / `1. a` lines.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct List {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// `true` for an ordered list, `false` for a bullet list.
    pub ordered: bool,
    /// The starting number of an ordered list (e.g. `3.` => `Some(3)`).
    pub start: Option<u64>,
    /// The marker delimiter used by the list items.
    pub delimiter: ListDelimiter,
    /// `true` if the list is tight (no blank lines between items / no `<p>`).
    pub tight: bool,
    /// The list's items.
    pub children: Vec<ListItem>,
}

/// The marker character that delimits a list's items.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListDelimiter {
    /// Bullet `-`.
    Dash,
    /// Bullet `*`.
    Asterisk,
    /// Bullet `+`.
    Plus,
    /// Ordered `1.`.
    Period,
    /// Ordered `1)`.
    Paren,
}

/// A single list item, optionally a GFM task-list checkbox.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListItem {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// Task-list state: `Some(true)` for `[x]`, `Some(false)` for `[ ]`, `None` otherwise.
    pub checked: Option<bool>,
    /// The item's block content.
    pub children: Vec<Block>,
}

/// A description / definition list of term + details pairs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescriptionList {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// `true` if the list is tight (no blank lines between items).
    pub tight: bool,
    /// The list's term/details items.
    pub children: Vec<DescriptionItem>,
}

/// One entry of a [`DescriptionList`]: a term and its detail blocks. Source: a
/// term line followed by `: details` lines.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescriptionItem {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The term's inline content.
    pub term: Vec<Inline>,
    /// The detail group(s) attached to this term.
    pub details: Vec<DescriptionDetails>,
}

/// The details (`: …`) attached to a [`DescriptionItem`]'s term.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescriptionDetails {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The details' block content.
    pub children: Vec<Block>,
}

/// A code block. Source: ```` ```lang … ``` ```` (fenced) or 4-space-indented lines.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodeBlock {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// Whether the block is fenced (and with what fence) or indented.
    pub kind: CodeBlockKind,
    /// The info string after a fence (e.g. the `rust` in ```` ```rust ````).
    pub info: Option<String>,
    /// The literal code contents.
    pub value: String,
}

/// Whether a [`CodeBlock`] is fenced or indented.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CodeBlockKind {
    /// A fenced code block; records the fence char and its run length.
    Fenced {
        /// Which character formed the fence (backtick or tilde).
        marker: FenceMarker,
        /// The number of fence characters in the opening fence (>=3).
        length: usize,
    },
    /// A 4-space-indented code block.
    Indented,
}

/// The character used to fence a [`CodeBlock`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FenceMarker {
    /// Backtick fence: ```` ``` ````.
    Backtick,
    /// Tilde fence: `~~~`.
    Tilde,
}

/// A raw HTML block: HTML emitted verbatim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HtmlBlock {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The literal HTML source.
    pub value: String,
}

/// A structured raw HTML container, such as `<details> ... </details>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HtmlContainer {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The opening HTML tag.
    pub opening: HtmlTag,
    /// The parsed container content.
    pub content: HtmlContainerContent,
    /// The closing HTML tag.
    pub closing: HtmlTag,
}

/// A raw HTML tag that bounds an [`HtmlContainer`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HtmlTag {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The normalized lowercase tag name.
    pub name: String,
    /// The literal tag source, including angle brackets and attributes.
    pub raw: String,
}

/// The parsed content inside an [`HtmlContainer`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HtmlContainerContent {
    /// Block-level Markdown content.
    Blocks(Vec<Block>),
    /// Inline-level Markdown content.
    Inlines(Vec<Inline>),
}

/// A link reference definition. Source: `[label]: destination "title"`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Definition {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The label as written in the source (e.g. `Foo Bar`).
    pub label: String,
    /// The normalized lookup key (case-folded, whitespace-collapsed) for matching references.
    pub identifier: String,
    /// The link target URL.
    pub destination: String,
    /// How the destination was delimited (bare or `<…>`).
    pub destination_kind: LinkDestinationKind,
    /// The optional link title.
    pub title: Option<String>,
    /// How the title was quoted, if present.
    pub title_kind: Option<LinkTitleKind>,
}

/// A footnote definition. Source: `[^id]: footnote text`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FootnoteDefinition {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The label as written in the source (the text after `^`).
    pub label: String,
    /// The normalized lookup key matching [`FootnoteReference`]s.
    pub identifier: String,
    /// The footnote's block content.
    pub children: Vec<Block>,
}

/// A GFM pipe table: a header row, an alignment row, then body rows. Source:
/// `| a | b |` / `|---|---|` / `| 1 | 2 |`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Table {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// Per-column alignment from the delimiter row.
    pub alignments: Vec<TableAlignment>,
    /// All rows; the first is the header row.
    pub rows: Vec<TableRow>,
}

/// The alignment of a [`Table`] column, from the `:---:` delimiter row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableAlignment {
    /// No explicit alignment: `---`.
    None,
    /// Left-aligned: `:---`.
    Left,
    /// Center-aligned: `:---:`.
    Center,
    /// Right-aligned: `---:`.
    Right,
}

/// A single row of a [`Table`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableRow {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The row's cells.
    pub cells: Vec<TableCell>,
}

/// A single cell of a [`TableRow`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableCell {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The cell's inline content.
    pub children: Vec<Inline>,
}

/// A display math block. Source: `$$ … $$`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MathBlock {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The literal math contents (between the `$$` fences).
    pub value: String,
}

/// A leading frontmatter block. Source: `---` YAML or `+++` TOML at the top of
/// the document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frontmatter {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// Whether the frontmatter is YAML or TOML.
    pub kind: FrontmatterKind,
    /// The literal frontmatter contents (between the fences).
    pub value: String,
}

/// The format of a [`Frontmatter`] block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontmatterKind {
    /// YAML frontmatter, fenced by `---`.
    Yaml,
    /// TOML frontmatter, fenced by `+++`.
    Toml,
}

/// An MDX ESM block: top-level `import`/`export` statements (distinct from directives).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MdxEsm {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The literal ESM source.
    pub value: String,
}

/// A block-level MDX expression: `{ … }` (distinct from directives).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MdxExpression {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The literal expression source (between the braces).
    pub value: String,
}

/// A block-level MDX JSX element (distinct from directives).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MdxJsx {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The literal JSX source.
    pub value: String,
}

/// A leaf directive. Source: `::name[label]{attrs}` (a directive feature,
/// not MDX).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeafDirective {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The directive name following the `::`.
    pub name: String,
    /// The optional `[label]` inline content.
    pub label: Vec<Inline>,
    /// The optional `{attrs}` attributes.
    pub attributes: Vec<DirectiveAttribute>,
}

/// A container directive. Source: `:::name[label]{attrs}` … `:::` (a directive
/// feature, not MDX).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerDirective {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The directive name following the `:::`.
    pub name: String,
    /// The optional `[label]` inline content.
    pub label: Vec<Inline>,
    /// The optional `{attrs}` attributes.
    pub attributes: Vec<DirectiveAttribute>,
    /// The directive's enclosed block content.
    pub children: Vec<Block>,
}

/// An inline-level node: the leaf and span content inside blocks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Inline {
    /// Literal text.
    Text(Text),
    /// A backslash escape such as `\*`.
    Escape(Escape),
    /// A character reference such as `&amp;` or `&#247;`.
    CharacterReference(CharacterReference),
    /// Emphasis: `*text*` or `_text_`.
    Emphasis(Emphasis),
    /// Strong emphasis: `**text**` or `__text__`.
    Strong(Strong),
    /// Underline: `__text__`/`___text___` (underscore extension).
    Underline(Underline),
    /// Strikethrough: `~~text~~`.
    Delete(Delete),
    /// A CriticMarkup-style insertion: `++text++`.
    Insert(Insert),
    /// A highlight / "mark" span: `==text==`.
    Mark(Mark),
    /// Subscript: `~x~`.
    Subscript(Subscript),
    /// Superscript: `^x^`.
    Superscript(Superscript),
    /// A spoiler span: `||text||`.
    Spoiler(Spoiler),
    /// An emoji-style shortcode: `:name:`.
    Shortcode(Shortcode),
    /// An inline code span: `` `code` ``.
    Code(CodeInline),
    /// An inline link: `[text](url)`.
    Link(Link),
    /// An inline image: `![alt](url)`.
    Image(Image),
    /// A reference link: `[text][label]`.
    LinkReference(LinkReference),
    /// A reference image: `![alt][label]`.
    ImageReference(ImageReference),
    /// An autolink: `<url>` or a GFM bare URL.
    Autolink(Autolink),
    /// Raw inline HTML such as `<span>`.
    Html(HtmlInline),
    /// A soft line break (a plain newline within a paragraph).
    SoftBreak(SoftBreak),
    /// A hard line break (`\` or two trailing spaces).
    LineBreak(LineBreak),
    /// Inline math: `$x$`.
    Math(MathInline),
    /// A footnote reference: `[^id]`.
    FootnoteReference(FootnoteReference),
    /// An inline footnote: `^[inline note]`.
    InlineFootnote(InlineFootnote),
    /// A wiki link: `[[target|label]]`.
    WikiLink(WikiLink),
    /// An inline MDX expression: `{ … }` (distinct from directives).
    MdxExpression(MdxExpressionInline),
    /// An inline MDX JSX element (distinct from directives).
    MdxJsx(MdxJsxInline),
    /// A text directive: `:name[label]{attrs}` (distinct from MDX).
    TextDirective(TextDirective),
}

/// Literal text content.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Text {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The text value.
    pub value: String,
}

/// A backslash escape such as `\*` or `\\`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Escape {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The escaped (literal) character.
    pub value: char,
}

/// A character reference such as `&amp;` or `&#247;`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CharacterReference {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The reference as written, including `&` and `;` (e.g. `amp` for `&amp;`).
    pub reference: String,
    /// The resolved character value (e.g. `&` for `&amp;`).
    pub value: String,
}

/// Emphasis (typically italic): `*text*` or `_text_`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Emphasis {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The emphasized inline content.
    pub children: Vec<Inline>,
}

/// Strong emphasis (typically bold): `**text**` or `__text__`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Strong {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The strongly-emphasized inline content.
    pub children: Vec<Inline>,
}

/// Underline (underscore extension): `__text__` or `___text___`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Underline {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The underlined inline content.
    pub children: Vec<Inline>,
}

/// Strikethrough: `~~text~~` (or single `~text~` when single-tilde is enabled).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Delete {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// Whether the span used one or two tildes.
    pub marker: DeleteMarker,
    /// The struck-through inline content.
    pub children: Vec<Inline>,
}

/// Which tilde run delimited a [`Delete`] span.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteMarker {
    /// Single-tilde strikethrough: `~text~`.
    SingleTilde,
    /// Double-tilde strikethrough: `~~text~~`.
    DoubleTilde,
}

/// A CriticMarkup-style insertion: `++text++`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Insert {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The inserted inline content.
    pub children: Vec<Inline>,
}

/// A highlight / "mark" span: `==text==`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Mark {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The highlighted inline content.
    pub children: Vec<Inline>,
}

/// Subscript: `~x~`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Subscript {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The subscripted inline content.
    pub children: Vec<Inline>,
}

/// Superscript: `^x^`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Superscript {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The superscripted inline content.
    pub children: Vec<Inline>,
}

/// A spoiler span: `||text||`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Spoiler {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The hidden inline content.
    pub children: Vec<Inline>,
}

/// An emoji-style shortcode: `:name:`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Shortcode {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The shortcode name between the colons (e.g. `smile` for `:smile:`).
    pub name: String,
}

/// An inline code span: `` `code` ``.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodeInline {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The normalized code text (trimmed/collapsed per CommonMark).
    pub value: String,
    /// The raw text between the backtick fences, before normalization.
    pub raw: String,
    /// The number of backticks in the fence.
    pub fence_length: usize,
}

/// An inline link: `[text](destination "title")`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Link {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The link target URL.
    pub destination: String,
    /// How the destination was delimited (bare or `<…>`).
    pub destination_kind: LinkDestinationKind,
    /// The optional link title.
    pub title: Option<String>,
    /// How the title was quoted, if present.
    pub title_kind: Option<LinkTitleKind>,
    /// The link's inline content (the visible text).
    pub children: Vec<Inline>,
}

/// An inline image: `![alt](destination "title")`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Image {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The image source URL.
    pub destination: String,
    /// How the destination was delimited (bare or `<…>`).
    pub destination_kind: LinkDestinationKind,
    /// The optional image title.
    pub title: Option<String>,
    /// How the title was quoted, if present.
    pub title_kind: Option<LinkTitleKind>,
    /// The image's alt-text inline content.
    pub alt: Vec<Inline>,
}

/// How a link/image destination was delimited in the source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkDestinationKind {
    /// A bare destination: `(url)`.
    Bare,
    /// An angle-bracketed destination: `(<url>)`.
    Angle,
    /// No destination present: `()`.
    Omitted,
}

/// How a link/image title was quoted in the source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkTitleKind {
    /// Double-quoted: `"title"`.
    DoubleQuote,
    /// Single-quoted: `'title'`.
    SingleQuote,
    /// Parenthesized: `(title)`.
    Paren,
}

/// A reference link: `[text][label]`, `[text][]`, or `[text]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LinkReference {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The normalized lookup key matching a [`Definition`].
    pub identifier: String,
    /// The label as written in the source.
    pub label: String,
    /// Whether the reference is full, collapsed, or shortcut form.
    pub kind: ReferenceKind,
    /// The link's inline content (the visible text).
    pub children: Vec<Inline>,
}

/// A reference image: `![alt][label]`, `![alt][]`, or `![alt]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageReference {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The normalized lookup key matching a [`Definition`].
    pub identifier: String,
    /// The label as written in the source.
    pub label: String,
    /// Whether the reference is full, collapsed, or shortcut form.
    pub kind: ReferenceKind,
    /// The image's alt-text inline content.
    pub alt: Vec<Inline>,
}

/// The form of a reference link/image.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReferenceKind {
    /// Full reference: `[text][label]`.
    Full,
    /// Collapsed reference: `[label][]`.
    Collapsed,
    /// Shortcut reference: `[label]`.
    Shortcut,
}

/// An autolink: `<url>` or a GFM bare URL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Autolink {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The resolved link href.
    pub destination: String,
    /// Whether the link was angle-bracketed or a GFM literal.
    pub kind: AutolinkKind,
}

/// Whether an [`Autolink`] is angle-bracketed or a GFM bare literal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AutolinkKind {
    /// An angle-bracket autolink `<dest>`. The destination is the raw text
    /// between the brackets; `>` is forbidden in the destination and the
    /// serializer re-emits `<dest>`.
    Angle,
    /// A GFM literal autolink (bare `www.`/`http(s)://`/`mailto:`/`xmpp:` URL
    /// or email). `original` is the raw source text that produced the link
    /// (the visible label); `destination` is the synthesized href (e.g. a
    /// `http://`/`mailto:` prefix may have been prepended). The serializer
    /// re-emits `original`, which re-parses to the same literal.
    GfmLiteral {
        /// The raw source text that produced the link (the visible label).
        original: String,
    },
}

/// Raw inline HTML such as `<span>` or `</em>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HtmlInline {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The literal HTML source.
    pub value: String,
}

/// A soft line break: a plain newline within a paragraph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SoftBreak {
    /// Node metadata (source span).
    pub meta: NodeMeta,
}

/// A hard line break: a trailing `\` or two trailing spaces.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineBreak {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// Which syntax produced the break.
    pub kind: LineBreakKind,
}

/// Which syntax produced a hard [`LineBreak`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LineBreakKind {
    /// A trailing backslash: `\`.
    Backslash,
    /// Two or more trailing spaces.
    Spaces,
}

/// Which syntax delimited an inline [`MathInline`] span.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MathInlineKind {
    /// Dollar-fenced inline math (`$…$`, `$$…$$`, …); `dollars` is the fence
    /// length (>=1). Dollar math always renders inline, while a 2-dollar fence
    /// is conventionally treated as display elsewhere in the ecosystem.
    Dollar {
        /// The number of `$` characters in the fence (>=1).
        dollars: u8,
    },
    /// Math-code span: `$`…`$`.
    Code,
}

/// Inline math: `$x$`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MathInline {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The literal math contents (between the fences).
    pub value: String,
    /// Which delimiter syntax was used.
    pub kind: MathInlineKind,
}

/// A footnote reference: `[^id]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FootnoteReference {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The label as written in the source (the text after `^`).
    pub label: String,
    /// The normalized lookup key matching a [`FootnoteDefinition`].
    pub identifier: String,
}

/// An inline footnote: `^[inline note]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InlineFootnote {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The footnote's inline content.
    pub children: Vec<Inline>,
}

/// A wiki link: `[[target|label]]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WikiLink {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The link target (page name).
    pub target: String,
    /// The visible label.
    pub label: String,
    /// Whether the label appeared before or after the `|` in the source.
    pub label_order: WikiLinkLabelOrder,
}

/// Whether a [`WikiLink`]'s label preceded or followed the `|` separator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WikiLinkLabelOrder {
    /// Target then label: `[[target|label]]`.
    AfterPipe,
    /// Label then target: `[[label|target]]`.
    BeforePipe,
}

/// An inline MDX expression: `{ … }` (distinct from directives).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MdxExpressionInline {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The literal expression source (between the braces).
    pub value: String,
}

/// An inline MDX JSX element (distinct from directives).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MdxJsxInline {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The literal JSX source.
    pub value: String,
}

/// A text directive. Source: `:name[label]{attrs}` (a directive feature,
/// not MDX).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TextDirective {
    /// Node metadata (source span).
    pub meta: NodeMeta,
    /// The directive name following the `:`.
    pub name: String,
    /// The optional `[label]` inline content.
    pub label: Vec<Inline>,
    /// The optional `{attrs}` attributes.
    pub attributes: Vec<DirectiveAttribute>,
}

/// One attribute of a directive's `{name=value}` block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectiveAttribute {
    /// The attribute name.
    pub name: String,
    /// The attribute value, or `None` for a valueless attribute.
    pub value: Option<String>,
}

// ---------------------------------------------------------------------------
// Ergonomic accessors and a minimal construction layer.
//
// Every node carries a `meta: NodeMeta`, so `meta()`/`span()` are uniform across
// the enums and free callers from writing an exhaustive match just to read a
// span. `From`/`new` collapse the `Variant(Struct { meta, .. })` boilerplate for
// hand-built ASTs; the raw struct literals remain available for full control.
// ---------------------------------------------------------------------------

macro_rules! impl_meta_accessors {
    ($enum:ident { $($variant:ident),+ $(,)? }) => {
        impl $enum {
            /// Borrow this node's [`NodeMeta`].
            pub fn meta(&self) -> &NodeMeta {
                match self { $( $enum::$variant(node) => &node.meta, )+ }
            }

            /// This node's source span, if it carries one.
            pub fn span(&self) -> Option<Span> {
                self.meta().span
            }
        }
    };
}

macro_rules! impl_from_variants {
    ($enum:ident { $($variant:ident($ty:ty)),+ $(,)? }) => {
        $(
            impl From<$ty> for $enum {
                fn from(node: $ty) -> Self {
                    $enum::$variant(node)
                }
            }
        )+
    };
}

impl_meta_accessors!(Block {
    Paragraph,
    Heading,
    ThematicBreak,
    BlockQuote,
    Alert,
    List,
    DescriptionList,
    CodeBlock,
    HtmlBlock,
    HtmlContainer,
    Definition,
    FootnoteDefinition,
    Table,
    MathBlock,
    Frontmatter,
    MdxEsm,
    MdxExpression,
    MdxJsx,
    LeafDirective,
    ContainerDirective,
});

impl_from_variants!(Block {
    Paragraph(Paragraph), Heading(Heading), ThematicBreak(ThematicBreak),
    BlockQuote(BlockQuote), Alert(Alert), List(List), DescriptionList(DescriptionList),
    CodeBlock(CodeBlock), HtmlBlock(HtmlBlock), Definition(Definition),
    HtmlContainer(HtmlContainer),
    FootnoteDefinition(FootnoteDefinition), Table(Table), MathBlock(MathBlock),
    Frontmatter(Frontmatter), MdxEsm(MdxEsm), MdxExpression(MdxExpression),
    MdxJsx(MdxJsx), LeafDirective(LeafDirective), ContainerDirective(ContainerDirective),
});

impl_meta_accessors!(Inline {
    Text,
    Escape,
    CharacterReference,
    Emphasis,
    Strong,
    Underline,
    Delete,
    Insert,
    Mark,
    Subscript,
    Superscript,
    Spoiler,
    Shortcode,
    Code,
    Link,
    Image,
    LinkReference,
    ImageReference,
    Autolink,
    Html,
    SoftBreak,
    LineBreak,
    Math,
    FootnoteReference,
    InlineFootnote,
    WikiLink,
    MdxExpression,
    MdxJsx,
    TextDirective,
});

impl_from_variants!(Inline {
    Text(Text), Escape(Escape), CharacterReference(CharacterReference),
    Emphasis(Emphasis), Strong(Strong), Underline(Underline), Delete(Delete),
    Insert(Insert), Mark(Mark), Subscript(Subscript), Superscript(Superscript),
    Spoiler(Spoiler), Shortcode(Shortcode), Code(CodeInline), Link(Link), Image(Image),
    LinkReference(LinkReference), ImageReference(ImageReference), Autolink(Autolink),
    Html(HtmlInline), SoftBreak(SoftBreak), LineBreak(LineBreak), Math(MathInline),
    FootnoteReference(FootnoteReference), InlineFootnote(InlineFootnote), WikiLink(WikiLink),
    MdxExpression(MdxExpressionInline), MdxJsx(MdxJsxInline), TextDirective(TextDirective),
});

impl Inline {
    /// The inline subtree of this node, or an empty slice for a leaf. Covers the
    /// `alt`/`label` fields uniformly, so a generic walker never silently skips
    /// an image's alt text or a directive's label.
    pub fn children(&self) -> &[Inline] {
        match self {
            Inline::Emphasis(n) => &n.children,
            Inline::Strong(n) => &n.children,
            Inline::Underline(n) => &n.children,
            Inline::Delete(n) => &n.children,
            Inline::Insert(n) => &n.children,
            Inline::Mark(n) => &n.children,
            Inline::Subscript(n) => &n.children,
            Inline::Superscript(n) => &n.children,
            Inline::Spoiler(n) => &n.children,
            Inline::Link(n) => &n.children,
            Inline::Image(n) => &n.alt,
            Inline::LinkReference(n) => &n.children,
            Inline::ImageReference(n) => &n.alt,
            Inline::InlineFootnote(n) => &n.children,
            Inline::TextDirective(n) => &n.label,
            _ => &[],
        }
    }
}

impl Text {
    /// A text node with the given string value.
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            meta: NodeMeta::default(),
            value: value.into(),
        }
    }
}

impl From<&str> for Text {
    fn from(value: &str) -> Self {
        Text::new(value)
    }
}

impl From<String> for Text {
    fn from(value: String) -> Self {
        Text::new(value)
    }
}

impl Paragraph {
    /// A paragraph from any iterator of inline-convertible children.
    pub fn new<I, T>(children: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<Inline>,
    {
        Self {
            meta: NodeMeta::default(),
            children: children.into_iter().map(Into::into).collect(),
        }
    }
}

impl Heading {
    /// An ATX heading of the given depth.
    pub fn new<I, T>(depth: u8, children: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<Inline>,
    {
        Self {
            meta: NodeMeta::default(),
            depth,
            kind: HeadingKind::Atx,
            children: children.into_iter().map(Into::into).collect(),
        }
    }
}

impl Link {
    /// A bare-destination link with no title.
    pub fn new<I, T>(destination: impl Into<String>, children: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<Inline>,
    {
        Self {
            meta: NodeMeta::default(),
            destination: destination.into(),
            destination_kind: LinkDestinationKind::Bare,
            title: None,
            title_kind: None,
            children: children.into_iter().map(Into::into).collect(),
        }
    }
}

impl CodeInline {
    /// An inline code span with a single-backtick fence.
    pub fn new(value: impl Into<String>) -> Self {
        let value = value.into();
        Self {
            meta: NodeMeta::default(),
            raw: value.clone(),
            value,
            fence_length: 1,
        }
    }
}

impl List {
    /// A tight, dash-delimited bullet list.
    pub fn new<I>(children: I) -> Self
    where
        I: IntoIterator<Item = ListItem>,
    {
        Self {
            meta: NodeMeta::default(),
            ordered: false,
            start: None,
            delimiter: ListDelimiter::Dash,
            tight: true,
            children: children.into_iter().collect(),
        }
    }
}
