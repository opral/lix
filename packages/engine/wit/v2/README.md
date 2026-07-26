# Component plugin runtime contract

The author-facing API lives in
[`packages/plugin-api`](../../../plugin-api/README.md). Its WIT copy is the
public contract used by the Rust plugin API package.

This directory contains the package-local mirror used by the engine's Wasmtime
bindings. It must be byte-identical to
[`packages/plugin-api/wit/lix-plugin-v2.wit`](../../../plugin-api/wit/lix-plugin-v2.wit);
CI enforces that. Keeping the mirror lets the published engine crate generate
its host bindings without depending on files outside its own archive.

## What plugin authors use

Authors depend on `lix_plugin_api_v2`, implement
`FormatPlugin`, and export it with `export_v2!`. The public API has four
typed transitions:

| Transition | Input | Output |
|---|---|---|
| `open_file` | initial bytes | durable entity changes |
| `open_entities` | durable entities | sparse byte edits |
| `file_changed` | verified base-relative byte splices | durable entity changes |
| `entities_changed` | final entity changes | sparse byte edits |

The four transitions are intentional: they cover cold/warm ×
bytes-to-entities/entities-to-bytes without an untyped event enum or invalid
runtime states. A document must be immutable and cheap to clone because the
runtime forks it for speculative work.

`FormatPlugin::resolve_conflict` is a fifth, stateless operation for colliding
semantic entities. Its default deterministically takes canonical `b` (or
deletes when `b` is absent); formats override it only for safe composition
rules such as distinct CSV cells or disjoint Markdown text spans.

WIT resources, packet paging, output attachments, limits, and error lowering
are internal to the API package. Normal format code works with typed entities,
changes, conflict values/resolutions, IDs, sources, and sparse edits. Read
[`packages/plugin-api/README.md`](../../../plugin-api/README.md) before using
the lower-level protocol material here.

## Format contract

A plugin manifest declares `runtime: "wasm-component-v2"`,
`api_version: "2.1.0"`, matchers, its component entry, and schemas. Each
schema declares `x-lix-key` and `x-lix-primary-key`. A schema that lets the
plugin create entities also declares `x-lix-id-allocation:
"host-allocated"`; preserve known IDs and use `ids.id(ordinal)` only for new
ones. Positions, row numbers, and byte offsets are not identities.

- `open-file`: parse initial bytes, return an immutable `document`, and stream
  complete initial entity upserts.
- `open-entities`: rebuild a cold document from durable entity pages and return
  edits against the empty byte string.
- `document.fork`: return a cheap immutable alias. Never mutate the accepted
  document in place.
- `document.file-changed`: consume accepted-base byte splices and return a
  successor document plus sparse, complete semantic changes.
- `document.entities-changed`: consume final merge-resolved changes and return
  a successor document plus sparse byte edits in accepted-base coordinates.
- `resolve-conflicts`: consume a lazy, statically scoped batch of colliding
  semantic entities and return one deterministic aligned resolution per input.
  It has no `document` resource; the host renders the resulting changes later
  through `entities-changed`.
- Every cursor must produce bounded, non-empty pages and permanent EOF. A
  transition is not accepted until the host drains and validates its output;
  traps, rejected output, and discarded transitions must leave the old
  document usable.

The host routes file-scoped semantic SQL writes through `entities-changed`.
Several statements in one transaction chain from a private pending document;
the accepted actor changes only at commit, and rollback discards the chain.
Mixing blob and semantic writes for the same file in one transaction is
rejected.

Use `plugin-error.invalid-input` when caller-provided bytes or a
format-specific semantic operation is unsupported. The host reports it as
`LIX_INVALID_PARAM`, discards only that prospective transition, and keeps the
accepted document and Store reusable. A malformed packet, trap, or
`plugin-error.internal` remains an invalid-plugin failure.

## Conflict-resolution rule

The engine supplies each colliding entity as a lazy `base` / `a` / `b`
triple only for one common live file incarnation with an identical descriptor
and full path at all three merge roots. A rename, extension change, or
ancestor-directory move is an ordinary merge conflict in this version. `a`
and `b` are already canonically ordered by durable
`(updated_at, change_id)`, so a resolver must not use branch direction or page
arrival order as authority. Its cursor returns exactly one result per supplied
conflict and echoes the host-assigned ordinal: `take(base|a|b)`, a
complete replacement snapshot, or `delete`. `take(b)` is the required deterministic fallback
when a format cannot safely compose the change, and does not require reading a
large attachment into guest memory.

Use a bounded, format-local heuristic only where it is clear. For example, a
CSV row entity may combine independent changes to distinct stable cell slots;
same-cell edits and row-layout/shape changes should take canonical `b`.
A Markdown paragraph entity may combine non-overlapping textual edits and take
`b` for overlap or syntax it cannot safely preserve. The resolver never
hydrates a document solely to make this decision, and it does not represent
unresolved alternatives durably. Persisted JJ-style conflict rows and
interactive resolution are deferred to a later data-model API.

## Stable IDs

For a schema marked `x-lix-id-allocation: "host-allocated"`, preserve every
acknowledged ID supplied by durable entities. Allocate an ID only for a truly
new entity. Encode the supplied namespace's `high` and `low` halves as 16
big-endian bytes, append one deterministic big-endian `u64` ordinal, and encode
the 24 bytes as exactly 32 unpadded base64url characters. The same logical
operation under the same explicit mutation identity must choose the same
ordinal. Transport replay requires a separate protocol identity and is not
provided by this API. Never use a row number, array index, or current file
position as identity.

The host binds and durably reserves the namespace to the mutation, file
incarnation, plugin, and generation. A plugin must not mint a different
namespace or reuse an old namespace for a new entity.

## Plugin selection

The CSV/TSV, JSON, Markdown, and Excalidraw components above are the in-tree
production references. Plugin selection applies the Component contract above;
there is no cross-runtime selection behavior.

Installation accepts the exact `2.1.0` API version. Replacing an owned plugin
is a compatible generation update: its API version, matcher, content type,
schema-key set, and ID-allocation contract remain stable.

## Build and test

From the repository root, the production reference commands are:

```sh
cargo test -p plugin_csv_v2 -p plugin_json_incremental_v2 -p plugin_markdown_incremental_v2 -p plugin_excalidraw_v2
cargo build --release -p plugin_csv_v2 -p plugin_json_incremental_v2 -p plugin_markdown_incremental_v2 -p plugin_excalidraw_v2 --target wasm32-wasip2
cargo test -p lix_sdk_tests \
  v2_csv_blob_api_preserves_multiplayer_authority_and_rollback -- --nocapture
cargo test -p lix_sdk_tests --test e2e \
  v2_json_ten_mib_real_wasm_edit_stays_sparse_and_bounded \
  -- --ignored --exact --nocapture
```

Run the benchmark-scale acceptance lane explicitly:

```sh
cargo test -p plugin_csv_v2 -- --ignored --nocapture
cargo test -p plugin_json_incremental_v2 -- --ignored --nocapture
cargo test -p lix_sdk --lib csv_v2_ -- --ignored --nocapture
cargo test --release -p lix_sdk_tests --test e2e \
  v2_cold_open_materialized_csv_and_json_benchmark \
  -- --ignored --exact --nocapture
```

The cold-open benchmark seeds durable materialized bytes and entities, then
opens a fresh engine and Wasm actor for each sample. It reports p50/p95
latency, Component-boundary traffic, packet/output reads, and guest linear
memory for the 220,000-row CSV plus flat and nested 10 MiB JSON fixtures.

The component build output is
`target/wasm32-wasip2/release/plugin_csv_v2.wasm`. A new plugin should add its
own artifact dependency and host integration test rather than relying only on
native core tests.

## Packet boundary

[`packet-v1.md`](packet-v1.md) normatively defines format version 1 at the
Component boundary. It is a transient arena, not a RocksDB/SlateDB storage
format. Packet framing and resource glue are SDK/runtime concerns and are not a
frozen general authoring facade. Format code should operate on typed entities,
entity changes, conflict triples/resolutions, and byte edits behind that
adapter. Conflict input and output remain lazy and paged: a selection result
does not copy the selected snapshot through guest memory, while a merged value
uses one bounded replacement attachment when necessary.
