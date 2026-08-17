# markdown-rs Test Corpus

Fixtures under `../commonmark/`, `../gfm/`, `../../roundtrip/cases/`, and
`../../roundtrip/examples/` include Markdown inputs and expected-HTML oracle
snapshots derived from `wooorm/markdown-rs` at local comparison commit
`1506572`.

The upstream Rust test sources are not present in this tree. This crate stores
only the byte-counted fixture snapshots it executes or audits locally; upstream
assertions that target `markdown-rs` APIs, renderer modes, or serializer output
are not executed verbatim.

The generated CommonMark suite also carries CommonMark specification provenance
and `CC-BY-SA-4.0` licensing; see `../../roundtrip/examples/NOTICE.md`.

The upstream license text is retained in `LICENSE.markdown-rs`.
