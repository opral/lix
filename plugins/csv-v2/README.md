# Incremental CSV plugin (Component API v2)

This crate is the production CSV/TSV Component plugin. Each Wasm instance is a
single-file actor. Its `document` resources are immutable,
cheaply forkable versions backed by one byte blob and a chunked compact index.

The hot paths are incremental:

- `file-changed` applies base-relative splices, reparses only the touched row
  window, reuses the rest of the index, and emits complete changes only for
  affected rows. A descriptor-only `.csv`/`.tsv` rename is the deliberate
  exception: because the path selects the delimiter, it reparses and reconciles
  the complete accepted bytes so warm behavior equals a fresh open;
- `entities-changed` renders an existing-row update or delete, a row insert,
  and a row reorder as one or two base-relative byte splices. Imported ID and
  slot indexes are immutable bases with sparse overlays, so these transitions
  copy only boundary chunks rather than document-sized identity/location
  arrays. Dialect changes and the uncommon unterminated-EOF reorder use the
  exact cold-render fallback;
- `open-file` and `open-entities` are the streaming cold/bootstrap paths.
  Durable entity pages are compacted immediately into byte arenas instead of
  retaining every decoded JSON record, and the full cold renderer edit shares
  the accepted document blob. Explicit large-file acceptance tests exercise
  the 220,000-row, 10.68 MB / 10.19 MiB fixture and keep its estimated retained state
  below 64 MiB. The integrated host defaults to a configurable 128 MiB per v2
  actor and at most four live Component Stores across one engine, including
  cold candidates and active transaction leases.

Cold reopen is byte-exact for accepted CSV lexical form, not merely
semantically equivalent. The table entity stores the preferred dialect and a
row optionally stores only exceptional layout:

- `layout.force_quote` is an unpadded base64url bitset for otherwise
  unnecessary field quotes (least-significant bit first, with trailing zero
  bytes omitted). Required quotes and doubled quote escaping remain derivable
  from the decoded cells;
- `layout.terminator` appears only when a row differs from the preferred table
  terminator; `""` represents an unterminated final row.

Canonical rows still contain only `id`, `order_key`, and `cells`; they gain no
retained in-memory layout object or per-row pointer. Lexical-only file edits
emit a complete `format-only` row update so the sparse facts become durable.
Entity-supplied dialects are accepted only when the delimiter is tab or
printable ASCII, the optional quote is printable non-space ASCII, neither is a
line ending, and delimiter and quote differ. A quote-less dialect is rejected
when its cells cannot be rendered into a self-openable file.

Generated row IDs are exactly the v2 mutation-scoped namespace plus a
big-endian ordinal, encoded as 32 unpadded base64url characters. Supplied
durable IDs are kept byte-for-byte.

For a new production v2 plugin, start with the
[authoring quickstart](../../packages/engine/wit/v2/README.md), then read the
[WIT contract](../../packages/engine/wit/v2/lix-plugin-v2.wit) and the
[packet-v1 rules](../../packages/engine/wit/v2/packet-v1.md). This crate's
`bindings.rs` and `packet.rs` are currently reference glue, not a published
generic SDK. Its [manifest](manifest.json),
[host-allocated row schema](schema/csv_v2_row.json), and
[tests](src/tests.rs) are the executable production examples.

From the repository root:

```sh
cargo test -p plugin_csv_v2
cargo build --release -p plugin_csv_v2 --target wasm32-wasip2
```

The benchmark-scale acceptance lane is intentionally excluded from ordinary
unit-test runs:

```sh
cargo test -p plugin_csv_v2 -- --ignored --nocapture
cargo test -p lix_sdk --lib csv_v2_ -- --ignored --nocapture
```
