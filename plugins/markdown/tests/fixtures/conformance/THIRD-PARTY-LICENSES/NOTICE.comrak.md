# Comrak Fixture Texts

Fixtures under `../gfm/` and `../../roundtrip/cases/` include Markdown inputs
and expected-HTML oracle snapshots derived from `kivikakk/comrak` at local
comparison commit `d2da7a0`.

The upstream Rust test sources are not present in this tree. This crate stores
only the byte-counted fixture snapshots it executes or audits locally; upstream
assertions that target Comrak APIs, renderer-only options, dynamic performance
inputs, source positions, or helper behavior are not executed verbatim.

`../gfm/comrak_html_edges.cases` contains executable AST-to-HTML conformance
cases derived from Comrak HTML expectations at the same comparison commit. It
includes only non-duplicate GFM task-list and table renderer cases whose option
surface is supported by this bench.

The upstream license text is retained in `COPYING.comrak`.
