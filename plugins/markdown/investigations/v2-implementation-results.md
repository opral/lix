# Markdown plugin v2 implementation results

Measured on 2026-07-12 from the implementation in this worktree. The v2 state
model is authoritative; retained raw Markdown is not part of the model.

## Implemented representation

- One self-referencing `markdown_node` schema, including the `document` root.
- Structural rows for block quotes, lists/items, tables/columns/rows, and
  footnote definitions.
- Leaf rows for paragraphs, headings, code, HTML, definitions, and table cells.
- Inline GFM DTOs embedded in leaves. Non-text inline atoms carry stable UUIDs;
  text stays in its leaf row.
- Tables have durable column IDs. Cells are children of rows and reference a
  `column_id`.
- Siblings render by `(order_key, id)`, so equal concurrent order keys are
  deterministic.
- Reconciliation reserves exact and compatible local identities across the
  complete tree before adopting unique exact cross-parent moves. Ambiguous
  copies mint IDs, so traversal order cannot steal an ID from a local edit.
- Global candidates use precomputed fixed-size subtree hashes for lookup and
  then full signatures for equality, avoiding large recursive signature maps
  without accepting hash collisions as identity matches.

## Format contract

The state includes render-effective, typed formatting for:

- LF/CRLF and final-newline presence;
- ATX versus setext headings;
- list delimiter spelling;
- thematic-break marker;
- indented/fenced code plus fence marker and length;
- emphasis and strong markers;
- hard-break spelling;
- character-reference spelling;
- inline-code raw spelling and fence length;
- resource-link delimiters/title quotes;
- reference form and label spelling;
- autolink form/original spelling.

Inter-block blank counts, ATX closing hashes, table widths/padding/pipes, mixed
line endings, list indentation, and fence-info spacing canonicalize.

## Round-trip matrix

| Corpus | Exact bytes | Semantic round-trip | Idempotent |
| --- | ---: | ---: | ---: |
| Curated edge cases | 22/41 | 41/41 | 41/41 |
| Repository Markdown | 5/34 | 34/34 | 34/34 |
| Official GFM 0.29 examples | 300/670 | 670/670 | 670/670 |
| Official CommonMark 0.31.2 examples | 287/652 | 652/652 | 652/652 |

The lower exact-byte score versus the old block-source implementation is
intentional: exact bytes are not the v2 contract. The scoped format fixtures in
`tests/roundtrip.rs` all round-trip exactly. Unsupported formatting reaches a
stable canonical representation before state reconciliation.

The previously red semantic regressions now pass:

| Scenario | Result |
| --- | --- |
| Definition followed by `---` | Semantic pass, idempotent |
| Definition followed by indented text | Semantic pass, idempotent |
| Definition followed by custom HTML-like text | Semantic pass, idempotent |
| Unclosed fence with a blank code line | Semantic pass, idempotent |
| Ambiguous `*one**two*` delimiter boundary | Semantic pass, idempotent |
| Literal delimiters inside `__foo__bar__baz__` | Semantic pass, idempotent |
| Empty fence in nested block quotes/lists | Semantic pass, idempotent |

## Entity locality matrix

| Edit | Changed rows |
| --- | ---: |
| One paragraph among 1,000 paragraphs | 1 paragraph |
| One item in a 100-item list | 1 paragraph leaf |
| Move a nested list subtree among siblings | 1 list-item structural row |
| Change a 100-item list from bullets to ordered | 1 list row |
| Edit one table cell | 1 table-cell row |
| Change one table column alignment | 1 table-column row |
| Swap two populated table columns | 1 table-column order row; 0 cells |
| `*em*` to `_em_` | 1 paragraph leaf, `impact=format` |
| `-` to `*` list markers | 1 list row, `impact=format` |
| Add/remove final newline | 1 document row, `impact=format` |

The regression suite also proves independent one-cell undo, identity retention
through a 100-row table header edit, five-item Flashtype-style list rewrites,
non-Latin reorders, duplicate insertion, cross-parent moves, and conservative
copy identity. It covers the order-dependent local-ID-stealing regression,
whole-table cross-parent moves with durable column references, and table-copy
versus local-edit ambiguity. Equal and nested sibling order-key collisions
rebalance without losing IDs; rendering remains deterministic by
`(order_key, id)`.

## V1 versus v2

| Dimension | V1 top-level blocks | V2 syntax entities |
| --- | --- | --- |
| State shape | Root plus one raw Markdown row per top-level block | One rooted structural/leaf graph with inline DTOs in text leaves |
| Curated semantic round-trip | 37/41 | **41/41** |
| Curated exact bytes | **25/41** | 22/41; canonical outside the bounded format tier |
| Curated idempotence | 41/41 | 41/41 |
| One paragraph edit | 1 row, 0.09 KiB | 1 row, 0.17 KiB |
| One item in a 100-item list | 1 whole-list row, 1.44 KiB | 1 paragraph leaf, 0.20 KiB |
| One cell in a 100-row table | 1 whole-table row, 2.94 KiB | 1 table-cell row, 0.25 KiB |
| Active rows for that list | **2** | 202 |
| Active rows for that table | **2** | 307 |
| Move a nested subtree | 1 whole-list rewrite | 1 moved-root parent/order update |
| Edit two cells independently | 1 inseparable table change | 2 independently reviewable/undoable cell changes |
| Nested stable diff keys | None below the top-level block | Durable structural, leaf, inline-atom, row, cell, and column IDs |

V2 therefore does not reduce the number of durable changes for every operation.
It reduces the *scope and snapshot size* of each leaf change and makes nested
review/undo possible, in exchange for a larger active graph that every plugin
invocation currently receives. Bulk edits inside one v1 block can produce fewer
rows in v1; those rows remain inseparable.

The following is an apples-to-apples native comparison: median of nine release
runs of the same generated inputs and benchmark harness, applied to the v1 base
and v2 worktree. It measures plugin `detect_changes` and `render`, not the full
JS/WASM/SQLite commit path.

| Scenario | Size | V1 detect | V2 detect | V1 render | V2 render |
| --- | ---: | ---: | ---: | ---: | ---: |
| Paragraph | 100 | **0.43 ms** | 0.98 ms | **0.07 ms** | 0.27 ms |
| Paragraph | 1,000 | **5.58 ms** | 12.85 ms | **0.74 ms** | 4.22 ms |
| Paragraph | 5,000 | 92.44 ms | **81.77 ms** | **4.24 ms** | 20.04 ms |
| List item | 100 | **0.38 ms** | 2.45 ms | **0.003 ms** | 0.66 ms |
| List item | 1,000 | **4.15 ms** | 36.55 ms | **0.019 ms** | 7.36 ms |
| List item | 5,000 | **85.63 ms** | 170.59 ms | **0.10 ms** | 34.78 ms |
| Table cell | 100 | **0.87 ms** | 5.44 ms | **0.003 ms** | 1.01 ms |
| Table cell | 1,000 | **28.10 ms** | 55.93 ms | **0.024 ms** | 11.14 ms |

V1 is generally faster, especially when an entire list or table is only two
active rows. V2's higher runtime is the cost of reconstructing and reconciling
the complete fine-grained graph. The exception in this run was detection across
5,000 independent paragraphs, where `markdown-syntax` plus cached subtree
hashes outperformed v1's `markdown-rs`/block diff path. The reason to choose v2
is semantic stability and nested identity/locality, not an unqualified speedup.

## V2 native release-mode benchmark

Median of nine in-process runs. `changed KiB` is the sum of upsert snapshot
content, excluding unchanged active rows.

| Scenario | Logical items | Active rows | Changed rows | Changed KiB | Detect median | Render median |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Paragraph | 100 | 101 | 1 | 0.17 | 0.98 ms | 0.27 ms |
| List item | 100 | 202 | 1 | 0.20 | 2.45 ms | 0.66 ms |
| Paragraph | 1,000 | 1,001 | 1 | 0.17 | 12.85 ms | 4.22 ms |
| List item | 1,000 | 2,002 | 1 | 0.20 | 36.55 ms | 7.36 ms |
| Paragraph | 5,000 | 5,001 | 1 | 0.17 | 81.77 ms | 20.04 ms |
| List item | 5,000 | 10,002 | 1 | 0.20 | 170.59 ms | 34.78 ms |
| Table cell | 100 | 307 | 1 | 0.25 | 5.44 ms | 1.01 ms |
| Table cell | 1,000 | 3,007 | 1 | 0.25 | 55.93 ms | 11.14 ms |

Write amplification is one row for the tested leaf edits. Runtime is instead
bounded by parsing and transferring the complete active graph, so active row
count remains the scaling limit.

Reproduce with:

```sh
cargo run --release -p plugin_md_v2 --example v2_write_amplification
cargo test -p plugin_md_v2 --all-targets
```
