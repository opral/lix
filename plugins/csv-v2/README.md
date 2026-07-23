# Incremental CSV plugin (Component API v2)

This crate is the production CSV vertical slice for `wasm-component-v2`. Each
Wasm instance is a single-file actor. Its `document` resources are immutable,
cheaply forkable versions backed by one byte blob and a chunked compact index.

The hot paths are incremental:

- `file-changed` applies base-relative splices, reparses only the touched row
  window, reuses the rest of the index, and emits complete changes only for
  affected rows;
- `entities-changed` renders an existing-row update or delete, a row insert,
  and a row reorder as one or two base-relative byte splices. Imported ID and
  slot indexes are immutable bases with sparse overlays, so these transitions
  copy only boundary chunks rather than document-sized identity/location
  arrays. Dialect changes and the uncommon unterminated-EOF reorder use the
  exact cold-render fallback;
- `open-file` and `open-entities` are the streaming cold/bootstrap paths.
  Durable entity pages are compacted immediately into byte arenas instead of
  retaining every decoded JSON record, and the full cold renderer edit shares
  the accepted document blob. The 220,000-row, 10.68 MiB fixture is exercised
  end-to-end under Wasmtime's production 64 MiB guest-memory ceiling.

Generated row IDs are exactly the v2 retry-stable namespace plus a big-endian
ordinal, encoded as 32 unpadded base64url characters. Supplied durable IDs are
kept byte-for-byte.

For a new production v2 plugin, start with the
[authoring quickstart](../../packages/engine/wit/v2/README.md), then read the
[WIT contract](../../packages/engine/wit/v2/lix-plugin-v2.wit) and the
[packet-v1 rules](../../packages/engine/wit/v2/packet-v1.md). This crate's
`bindings.rs` and `packet.rs` are currently reference glue, not a published
generic SDK. Its [manifest](manifest.json),
[host-allocated row schema](schema/csv_row.json), and
[tests](src/tests.rs) are the executable production examples.

From the repository root:

```sh
cargo test -p plugin_csv_v2
cargo build --release -p plugin_csv_v2 --target wasm32-wasip2
```
