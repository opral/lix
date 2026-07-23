# Wasm Component plugin API v2

This directory is the source of truth for the production v2 Component
contract. The only production implementation today is the
[incremental CSV/TSV vertical slice](../../../../plugins/csv-v2/README.md).
Treat that slice as an executable reference, not as proof that v2 is already a
general-purpose plugin SDK or a frozen API for every file format.

Keeping v2 below `packages/engine/wit/v2` prevents existing v1 hosts and
plugins, which bind from the parent directory, from resolving two packages or
worlds named `plugin`.

## Authoring map

| Concern | Canonical source |
|---|---|
| Component types, resources, limits, and lifecycle | [`lix-plugin-v2.wit`](lix-plugin-v2.wit) |
| Packet framing, ordering, snapshot semantics, and validation | [`packet-v1.md`](packet-v1.md) |
| Generated Rust binding invocation and typed WIT adapter | [`plugins/csv-v2/src/bindings.rs`](../../../../plugins/csv-v2/src/bindings.rs) |
| Checked packet codec used by the production slice | [`plugins/csv-v2/src/packet.rs`](../../../../plugins/csv-v2/src/packet.rs) |
| Manifest fields and archive paths | [`plugins/csv-v2/manifest.json`](../../../../plugins/csv-v2/manifest.json) |
| Schema annotations and stable-ID declaration | [`csv_row.json`](../../../../plugins/csv-v2/schema/csv_row.json) and [`csv_table.json`](../../../../plugins/csv-v2/schema/csv_table.json) |
| Incremental document implementation | [`plugins/csv-v2/src/core.rs`](../../../../plugins/csv-v2/src/core.rs) |
| Executable behavior tests | [`plugins/csv-v2/src/tests.rs`](../../../../plugins/csv-v2/src/tests.rs) |
| Host archive construction and end-to-end tests | [`packages/rs-sdk-tests/tests/e2e.rs`](../../../rs-sdk-tests/tests/e2e.rs) |
| Minimal production authoring AX task | [`production-v2-tsv.md`](../../../../experiments/plugin-api-v2/ax-eval/tasks/production-v2-tsv.md) |

## Authoring quickstart

1. Use `plugins/csv-v2` as the crate-layout reference. It is a `cdylib` using
   `wit-bindgen` and exports the `plugin` world. Keep format logic separate from
   the WIT and packet adapter, as `core.rs` is separate from `bindings.rs` and
   `packet.rs` in the reference.
2. Generate the Rust traits from this directory. A plugin crate beside
   `plugins/csv-v2` uses:

   ```rust
   wit_bindgen::generate!({
       path: "../../packages/engine/wit/v2",
       world: "plugin",
   });
   ```

   Adjust the relative path for a crate elsewhere. `wit-bindgen` generates the
   WIT resources and traits; it does **not** currently generate a typed entity
   packet facade. The CSV crate's `bindings.rs` and `packet.rs` are the checked
   production reference for that adapter. There is no standalone production v2
   author SDK to depend on yet.
3. Add a `manifest.json` with `runtime: "wasm-component-v2"`, the exact
   `api_version: "2.0.0"`, a unique `key`, a `match.path_glob`, optional
   `match.content_type` (`"text"` or `"binary"`), `entry: "plugin.wasm"`,
   and every schema path in `schemas`.
4. Give each schema an `x-lix-key` and an `x-lix-primary-key` array of JSON
   Pointers. Add `x-lix-id-allocation: "host-allocated"` only to a v2 schema
   whose plugin allocates new primary keys from the transition namespace. The
   current production gate also requires schemas in which JSON number nodes are
   unreachable; use strings for values such as order keys. See the
   [Snapshot JSON durable-representation gate](packet-v1.md#durable-representation-gate).
5. Implement every lifecycle entry point below, then build the Wasm component
   and exercise both the format core and the host integration.

The installable `.lixplugin` is a ZIP with the manifest and schemas at their
declared paths and the built component at the manifest's `entry` path. The CSV
end-to-end test's `build_csv_v2_plugin_archive` helper is the current packaging
reference; there is no generic v2 packaging CLI yet.

## Lifecycle checklist

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
- Every cursor must produce bounded, non-empty pages and permanent EOF. A
  transition is not accepted until the host drains and validates its output;
  traps, rejected output, and discarded transitions must leave the old
  document usable.

## Stable IDs

For a schema marked `x-lix-id-allocation: "host-allocated"`, preserve every
acknowledged ID supplied by durable entities. Allocate an ID only for a truly
new entity. Encode the supplied namespace's `high` and `low` halves as 16
big-endian bytes, append one deterministic big-endian `u64` ordinal, and encode
the 24 bytes as exactly 32 unpadded base64url characters. The same logical
operation must choose the same ordinal on retry. Never use a row number, array
index, or current file position as identity.

The host binds and durably reserves the namespace to the mutation, file
incarnation, plugin, and generation. A plugin must not mint a different
namespace or reuse an old namespace for a new entity.

## Build and test

From the repository root, the production reference commands are:

```sh
cargo test -p plugin_csv_v2
cargo build --release -p plugin_csv_v2 --target wasm32-wasip2
cargo test -p lix_sdk_tests \
  v2_csv_blob_api_preserves_multiplayer_authority_and_rollback -- --nocapture
```

The component build output is
`target/wasm32-wasip2/release/plugin_csv_v2.wasm`. A new plugin should add its
own artifact dependency and host integration test rather than relying only on
native core tests.

## Packet boundary

[`packet-v1.md`](packet-v1.md) normatively defines format version 1 at the
Component boundary. It is a transient arena, not a RocksDB/SlateDB storage
format. Packet framing and resource glue are SDK/runtime concerns and are not a
frozen general authoring facade. Format code should operate on typed entities,
changes, merge groups, and byte edits behind that adapter.
