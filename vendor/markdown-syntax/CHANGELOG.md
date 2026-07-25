# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0](https://github.com/plimeor/markdown-syntax/compare/v0.1.2...v0.2.0) - 2026-07-05

### Other

- Add details HTML container parsing

## [0.1.2](https://github.com/plimeor/markdown-syntax/compare/v0.1.1...v0.1.2) - 2026-06-23

### Other

- Honor raw source spans in parser

## [0.1.1](https://github.com/plimeor/markdown-syntax/compare/v0.1.0...v0.1.1) - 2026-06-20

### Other

- Rewrite README for usage-first readability
- Add README badges and release-plz automation

## [0.1.0] - 2026-06-20

Initial release.

### Added

- `no_std + alloc` Markdown parser: source to an owned AST via `parse`, with
  optional half-open byte `Span`s and a `LineIndex` for line/column mapping.
- Canonical serializer: AST to Markdown via `Document::to_markdown` /
  `Document::to_markdown_with`.
- AST validation via `Document::validate`, sharing one `Diagnostic` type with
  the parser.
- Configurable dialects: `parse` recognizes a maximal non-MDX dialect (GFM plus
  footnotes, math, frontmatter, wikilinks, directives, and the extra inline
  marks); `SyntaxOptions::{commonmark, gfm, mdx}` presets and a `Construct`
  enable/disable builder sit on top of the exhaustive `Constructs` /
  `ParseOptions` flags.
- Opt-in `html` feature: safe-by-default AST to HTML rendering
  (`Document::to_html` / `Document::to_html_with`) that validates first, escapes
  raw HTML, and filters dangerous link/image protocols.
- A `prelude` module for one-line imports.

[0.1.0]: https://github.com/plimeor/markdown-syntax/releases/tag/v0.1.0
