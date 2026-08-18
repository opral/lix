# Markdown syntax test corpus

This directory preserves the complete test fixture and conformance corpus from
[`markdown-syntax` v0.2.0](https://github.com/plimeor/markdown-syntax/tree/v0.2.0),
commit `a67b5a8d824eb4704aa428254e6bbf3f1f23c2a4`.

The parser's original MIT notice is retained beside its source in
`src/markdown_syntax/LICENSE-MIT`. The corpus includes material derived from
CommonMark, GFM, comrak, and markdown-rs. Their provenance and applicable
notices are preserved within `fixtures/conformance/THIRD-PARTY-LICENSES`,
`fixtures/roundtrip/cases`, and `fixtures/roundtrip/examples`.

The applicable Rust harness lives in `src/markdown_syntax/upstream_tests` so it
exercises the plugin's integrated parser directly. The upstream HTML renderer
and renderer-dependent tests are intentionally not included because the
Markdown plugin does not render HTML.
