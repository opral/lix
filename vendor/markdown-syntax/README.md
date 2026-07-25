# markdown-syntax

[![crates.io](https://img.shields.io/crates/v/markdown-syntax.svg)](https://crates.io/crates/markdown-syntax)
[![docs.rs](https://docs.rs/markdown-syntax/badge.svg)](https://docs.rs/markdown-syntax)
[![CI](https://github.com/plimeor/markdown-syntax/actions/workflows/ci.yml/badge.svg)](https://github.com/plimeor/markdown-syntax/actions/workflows/ci.yml)
[![license](https://img.shields.io/crates/l/markdown-syntax.svg)](#license)

A `no_std + alloc` Rust crate that parses Markdown source into an owned AST and serializes the AST back to canonical Markdown — with opt-in, safe-by-default HTML rendering behind the `html` feature.

## At a glance

- **AST-first** — `parse` returns an owned enum tree over `alloc`; the output verbs live on the `Document` you hold.
- **Tolerant** — problems are collected as diagnostics, never thrown; `parse` is infallible.
- **Maximal default dialect** — GFM + footnotes + math + frontmatter + wikilinks + directives + extra inline marks, out of the box.
- **Lean core** — zero runtime dependencies, `no_std + alloc`, MSRV 1.82.

## Install

```console
cargo add markdown-syntax
```

For the opt-in HTML renderer:

```console
cargo add markdown-syntax --features html
```

Or in `Cargo.toml`:

```toml
[dependencies]
markdown-syntax = "0.1"
```

## Quickstart

```rust
use markdown_syntax::parse;

// `parse` is infallible and returns a `ParseOutput { document, diagnostics }`.
let output = parse("# Title\n\nHello *world*.");
assert!(output.diagnostics.is_empty());

// Serialize the AST back to canonical Markdown (this is the fallible step).
let markdown = output.document.to_markdown()?;
assert_eq!(markdown, "# Title\n\nHello *world*.\n");
# Ok::<(), markdown_syntax::SerializeError>(())
```

[`parse`](https://docs.rs/markdown-syntax/latest/markdown_syntax/fn.parse.html) is infallible — the output verbs live on the [`Document`](https://docs.rs/markdown-syntax/latest/markdown_syntax/ast/struct.Document.html) you hold.

## Common tasks

`parse` is the one obvious path. When you need to narrow the dialect, read diagnostics, walk the tree, or render HTML, each task is one small snippet below.

- [Pick a dialect (presets)](#pick-a-dialect-presets)
- [Tune one construct (builder)](#tune-one-construct-builder)
- [Walk the AST](#walk-the-ast)
- [Handle diagnostics (tolerant vs strict)](#handle-diagnostics-tolerant-vs-strict)
- [Customize serialization](#customize-serialization)
- [Source positions (optional)](#source-positions-optional)
- [Build an AST by hand](#build-an-ast-by-hand)

### Pick a dialect (presets)

```rust
use markdown_syntax::SyntaxOptions;

// Named presets each build a `SyntaxOptions`; call `.parse` to run them.
let cm = SyntaxOptions::commonmark().parse("~~kept literal~~");
let gfm = SyntaxOptions::gfm().parse("~~done~~ and https://example.com");
let mdx = SyntaxOptions::mdx().parse("<Component/>\n\ntext");

// `parse(input)` is exactly `SyntaxOptions::default().parse(input)` — the
// maximal non-MDX dialect.
let default = SyntaxOptions::default().parse("H~2~O and x^2^");
let _ = (cm, gfm, mdx, default);
```

`commonmark` / `gfm` / `mdx` are the named presets; `default` == the maximal non-MDX dialect, and `parse(input)` is sugar for `SyntaxOptions::default().parse(input)`. See [`SyntaxOptions`](https://docs.rs/markdown-syntax/latest/markdown_syntax/options/struct.SyntaxOptions.html).

### Tune one construct (builder)

```rust
use markdown_syntax::{SyntaxOptions, Construct, WikiLinkOrder};

// Tune a preset with the typo-proof `Construct` builder (grouped constructs
// such as `Math`, `Footnotes`, `Directives` flip every flag in the group).
let no_math = SyntaxOptions::default().disable(Construct::Math).parse("price $5");

let with_wikilinks = SyntaxOptions::commonmark()
    .enable(Construct::Strikethrough)
    .enable(Construct::Wikilinks(WikiLinkOrder::TitleAfterPipe))
    .parse("~~old~~ see [[target|label]]");

let _ = (no_math, with_wikilinks);
```

[`Construct`](https://docs.rs/markdown-syntax/latest/markdown_syntax/options/enum.Construct.html) is a typo-proof front door over the full [`Constructs`](https://docs.rs/markdown-syntax/latest/markdown_syntax/options/struct.Constructs.html) flag set. Grouped constructs (`Math`, `Footnotes`, `Directives`) flip a whole family at once, `HtmlContainers` recognizes Markdown-compatible HTML containers, and `Wikilinks` is the one parameterized variant.

### Walk the AST

```rust
use markdown_syntax::{parse, Block, Inline};

let document = parse("Hello *world*.").document;

for block in &document.children {
    if let Block::Paragraph(paragraph) = block {
        for inline in &paragraph.children {
            if let Inline::Text(text) = inline {
                assert_eq!(text.value, "Hello ");
                break;
            }
        }
    }
}
```

`document.children` is a `Vec<Block>`; block content (like `Paragraph.children`) is a `Vec<Inline>`. See the [`ast`](https://docs.rs/markdown-syntax/latest/markdown_syntax/ast/index.html) module, [`Block`](https://docs.rs/markdown-syntax/latest/markdown_syntax/ast/enum.Block.html), and [`Inline`](https://docs.rs/markdown-syntax/latest/markdown_syntax/ast/enum.Inline.html).

### Handle diagnostics (tolerant vs strict)

```rust
use markdown_syntax::{SyntaxOptions, DiagnosticSeverity, ParseStrictError};

// Tolerant parse: problems are collected, never thrown.
let output = SyntaxOptions::default().parse(":::note\nunclosed container");
for diagnostic in &output.diagnostics {
    let _ = (diagnostic.severity, diagnostic.code, diagnostic.span, &diagnostic.message);
    if diagnostic.severity == DiagnosticSeverity::Error {
        // handle an error-severity diagnostic
    }
}

// `parse_strict` promotes any error-severity diagnostic (or a config conflict)
// to a hard `Err`.
match SyntaxOptions::default().parse_strict("# clean input") {
    Ok(out) => assert!(out.diagnostics.iter().all(|d| d.severity != DiagnosticSeverity::Error)),
    Err(ParseStrictError::Config(_)) => {}
    Err(ParseStrictError::Diagnostic(_)) => {}
}
```

`span` is `Option<Span>` because a hand-built node may lack a source location. Parser diagnostics, AST validation, and serializer/HTML pre-validation are three separate domains that share one [`Diagnostic`](https://docs.rs/markdown-syntax/latest/markdown_syntax/diagnostic/struct.Diagnostic.html) type.

### Customize serialization

```rust
use markdown_syntax::{parse, SerializeOptions, LineEnding};

// `SerializeOptions` is #[non_exhaustive]: mutate a default rather than using a
// struct literal.
let mut options = SerializeOptions::default();
options.line_ending = LineEnding::CrLf;
options.final_newline = false;

let markdown = parse("# Title").document.to_markdown_with(&options)?;
assert_eq!(markdown, "# Title");
# Ok::<(), markdown_syntax::SerializeError>(())
```

Because `SerializeOptions` is `#[non_exhaustive]`, external code cannot struct-literal-construct it (even with `..Default::default()`, E0639) — mutate a `default()` instead.

### Source positions (optional)

```rust
use markdown_syntax::{parse, LineIndex};

let source = "# Title\n\nHello.";
let document = parse(source).document;
let index = LineIndex::new(source);

// Spans are absolute, half-open UTF-8 byte ranges; `None` for hand-built nodes.
if let Some(first) = document.children.first() {
    if let Some(span) = first.span() {
        let (start, end) = index.span(span);
        // 1-based line/column.
        assert_eq!(start.line, 1);
        assert_eq!(start.column, 1);
        let _ = (span.start, span.end, span.len(), end.line, end.column);
    }
}
```

Spans are absolute half-open UTF-8 byte ranges, `None` for hand-built nodes. [`LineIndex`](https://docs.rs/markdown-syntax/latest/markdown_syntax/span/struct.LineIndex.html) turns a [`Span`](https://docs.rs/markdown-syntax/latest/markdown_syntax/span/struct.Span.html) into 1-based [`LinePosition`](https://docs.rs/markdown-syntax/latest/markdown_syntax/span/struct.LinePosition.html) line/column.

### Build an AST by hand

The [`prelude`](https://docs.rs/markdown-syntax/latest/markdown_syntax/prelude/index.html) imports the common surface in one line:

```rust
use markdown_syntax::prelude::*;

let document = Document {
    meta: NodeMeta::default(),
    children: vec![
        Heading::new(1, [Text::from("Title")]).into(),
        Paragraph::new([Text::from("hello")]).into(),
    ],
};
// Hand-built nodes carry no span.
assert_eq!(document.children[0].span(), None);
assert_eq!(document.to_markdown().unwrap(), "# Title\n\nhello\n");
```

## HTML rendering (opt-in)

The HTML renderer ships behind the non-default `html` feature and is safe by default: it validates the AST first, escapes raw HTML, blanks dangerous link/image protocols, and disables task-list checkboxes.

```console
cargo add markdown-syntax --features html
```

```rust,ignore
// Requires `--features html`; the default doctest build has no html feature,
// so this block is `rust,ignore`.
use markdown_syntax::{parse, HtmlOptions, HtmlError, SafeRawHtmlForm};

let document = parse("# Hi\n\n<script>alert(1)</script>").document;

// Default is safe: raw HTML is escaped, dangerous link/image protocols blanked.
let safe: Result<String, HtmlError> = document.to_html();
assert!(safe.is_ok());

// `HtmlOptions` is #[non_exhaustive]: mutate a default to opt into raw HTML.
let mut options = HtmlOptions::default();
options.allow_dangerous_html = true;
options.safe_raw_html_form = SafeRawHtmlForm::OmitPlaceholder;
let _ = document.to_html_with(&options);
```

See [`HtmlOptions`](https://docs.rs/markdown-syntax/latest/markdown_syntax/html/struct.HtmlOptions.html). docs.rs builds with the `html` feature enabled, so the renderer's API is fully documented there.

## Dialects & constructs reference

| Preset | `.parse` builder | Membership note |
| --- | --- | --- |
| `commonmark` | `SyntaxOptions::commonmark()` | CommonMark core only |
| `gfm` | `SyntaxOptions::gfm()` | CommonMark + tables, task lists, strikethrough, autolinks, footnotes |
| `mdx` | `SyntaxOptions::mdx()` | MDX JSX/expressions/ESM on; raw HTML off |
| `default` (== max) | `SyntaxOptions::default()` / `parse` | Maximal non-MDX dialect, including Markdown-compatible HTML containers (see below) |

`underline` (`__text__`) is **off** in `default` because it would override CommonMark strong; **MDX is off** by default and conflicts with raw HTML; wikilinks default to title-after-pipe. For the full `Construct` and `Constructs` surface, see [`Construct`](https://docs.rs/markdown-syntax/latest/markdown_syntax/options/enum.Construct.html) and [`Constructs`](https://docs.rs/markdown-syntax/latest/markdown_syntax/options/struct.Constructs.html) on docs.rs.

Cargo features:

| Feature | Default | What it adds |
| --- | --- | --- |
| `default` | `[]` (empty) | Byte-stable `no_std + alloc` core: parser, AST, serializer, validation, `Span`/`LineIndex`, prelude. Zero runtime deps. |
| `html` | off | Opt-in, additive, safe-by-default `to_html` / `to_html_with` and the `html` module. Stays `no_std + alloc`, zero runtime deps. |

## How it works

- **AST-first public API** — `parse` produces an owned `Document`; parser event streams and internal block operations are private, not v1 compatibility surfaces.
- **Owned enum tree** over `alloc` types.
- **Optional source spans** — half-open absolute byte ranges on every node, `None` for hand-built nodes; line/column derived via `LineIndex`.
- **Tolerant by default** — diagnostics are collected, not thrown.

## Scope & limitations

In scope — the maximal default dialect: GFM (tables, task lists, strikethrough, literal/relaxed autolinks, alerts), footnotes (incl. inline), inline + block math, frontmatter (`---` / `+++`), wikilinks (title-after-pipe default), the extra inline marks (insert `++`, highlight `==`, subscript `~`, superscript `^`, spoiler `||`, shortcodes `:tada:`), description lists, Markdown-compatible HTML containers (`details` / `summary`), and the `:name` / `::name` / `:::name` directive family.

Non-goals:

- `underline` (`__text__`) is **off** by default — it would override CommonMark strong.
- **MDX** (JSX / expressions / ESM) is **off** by default and conflicts with raw HTML.
- Raw HTML, HTML containers, and MDX are represented **only** as Markdown syntax nodes — no HTML rendering/sanitization, no MDX evaluation, no syntax highlighting, and no DOM post-processing in the default build.
- The serializer performs **no** HTML safety filtering and does **not** preserve byte-for-byte authoring style from a bare AST.
- Validation is conservative and does not prove every semantic invariant of a hand-written AST.
- **Directives** (`:name` / `::name` / `:::name`) are a distinct family and are **never** MDX.

## Compatibility

`no_std + alloc` (crate root is `#![no_std]` + `extern crate alloc`). Default features are empty; the opt-in `html` feature also stays `no_std + alloc`. Zero runtime dependencies. MSRV 1.82 (edition 2021).

## Contributing & conformance

Tests live in `tests/`. AST→HTML correctness is measured against vendored CommonMark/GFM oracles; observe the current numbers with `cargo test --features html --test html_conformance -- --nocapture`.

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
