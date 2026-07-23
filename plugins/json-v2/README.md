# Incremental JSON plugin (Component API v2)

This crate is the production JSON vertical slice for `wasm-component-v2`. It
models each top-level member of a JSON object as one `json_property` entity.
The decoded member name is the natural entity identity, `order_key` preserves
object-member order, and `value_json` contains the complete JSON value as text.
Keeping arbitrary JSON inside a string lets values contain numbers while the
production packet-v1 durable snapshot itself remains number-free.

The supported document root is a JSON object with unique decoded member names.
Root arrays and scalars, duplicate object keys, invalid UTF-8, and invalid JSON
are rejected. The outer object uses the canonical compact envelope emitted by
the plugin: no whitespace around the braces, keys, separators, or property
values, and canonical JSON string encoding for property names. Nested arrays,
objects, strings, numbers, booleans, and null are supported as complete raw
values of a top-level property, including their internal whitespace and exact
number spelling. This is an explicit vertical-slice scope, not yet full parity
with the Component v1 JSON plugin's root and formatting model.

The hot path is a localized edit to one existing top-level property. Immutable
document versions share their accepted byte backing and retain an index of
top-level member ranges. A localized edit reparses the affected property and
emits one complete sparse property upsert. Structural ambiguity, edits spanning
multiple properties, property insertion or deletion, and uncommon formatting
or ordering changes within that compact envelope use the exact full-document
fallback. Cold `open-file` and `open-entities` paths likewise perform complete
validation and reconstruction. Complete snapshots larger than a packet page
are exposed through the contract's page-local lazy attachment table rather
than rejected or copied into oversized packet records.

The plugin contract and transient packet encoding are defined by:

- [the production v2 WIT contract](../../packages/engine/wit/v2/lix-plugin-v2.wit);
- [the packet-v1 rules](../../packages/engine/wit/v2/packet-v1.md); and
- [the v2 authoring guide](../../packages/engine/wit/v2/README.md).

The `bindings.rs` and `packet.rs` modules implement the Component boundary.
Format parsing, indexing, identity, and incremental transition logic stay in
`core.rs`.

From the repository root:

```sh
cargo test -p plugin_json_incremental_v2
cargo build --release -p plugin_json_incremental_v2 --target wasm32-wasip2
```
