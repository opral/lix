# Incremental JSON plugin (Component API v2)

This crate is an opt-in experimental recursive JSON reference for the
production `wasm-component-v2` API. It is not a
bundled or production-default replacement while the ownership/reachability
rollout gate below remains open. It models the document as stable semantic
nodes rather than using a full RFC 6901 JSON Pointer as entity identity:

- `json_root` is the single stable document root.
- `json_object_member` identifies an object slot by its stable parent
  container and decoded key.
- `json_array_item` uses an opaque stable ID plus an independent
  `order_key`. A numeric array index is only a locator and never identity.

An object or array is coalesced with the root, object member, or array item
that contains it. Its children are separate entities. This avoids redundant
container entities while allowing recursive leaf edits, array inserts, moves,
and subtree deletion to stay semantically precise.

Object-member identities are the two primary-key components `parent_id` and
`key`. When an object member contains another object or array, `container_id`
is deterministically derived from that identity and becomes the `parent_id` of
its children. An array item already has a stable opaque `id`, so that ID also
identifies any container held by the item. Only `json_array_item` declares
`x-lix-id-allocation: "host-allocated"`: newly inserted items use the
transition's namespace, while acknowledged IDs survive content edits and
moves.

Every node records its `kind`. Scalar values use `scalar_json`, which contains
the complete raw JSON scalar as text. Numbers therefore preserve their exact
spelling without placing JSON number nodes in the durable packet snapshot.
Objects and arrays omit `scalar_json` and carry their content in child
entities. `order_key` preserves deterministic object-member and array-item
order independently of identity.

Lossless layout is durable but sparse. Optional `prefix_json`, `suffix_json`,
and `empty_json` fields retain outer whitespace, container whitespace, and raw
object-key spelling only when they differ from compact canonical JSON. A
canonical 10 MB fixture therefore pays no per-node layout-string allocation,
while pretty JSON, escaped key spellings, and formatting-only edits still
round-trip exactly through `open-entities` after actor eviction.

The hot path is a localized edit to one existing scalar. Immutable document
versions share their accepted byte backing and retain a chunked byte-range
index. A localized scalar edit reparses only the affected value and emits one
complete sparse upsert. Same-length edits share the span index unchanged;
length-changing edits copy its compact chunk directory, shift later chunk
bases, and copy only the chunks containing the scalar or its ancestors. They
do not parse or materialize the complete document. The 10 MB flat object
fixture therefore retains the same one-property fast path even though the
semantic model also supports nested objects and arrays. Structural edits
reconcile object members by decoded key and array items by stable identity,
using the exact full-document fallback when a bounded local reconciliation is
not possible.

File-originated container deletion currently emits explicit descendant
tombstones. Making this generation the default also requires the RFC's
schema-declared ownership/reachability support in the host so concurrent
entity-side detach and reactivation are enforced durably rather than only by a
warm plugin document. That host capability is a rollout gate, not a WIT API
change.

The plugin contract and transient packet encoding are defined by:

- [the production v2 WIT contract](../../packages/engine/wit/v2/lix-plugin-v2.wit);
- [the packet-v1 rules](../../packages/engine/wit/v2/packet-v1.md); and
- [the v2 authoring guide](../../packages/engine/wit/v2/README.md).

The `bindings.rs` and `packet.rs` modules implement the Component boundary.
Format parsing, recursive indexing, identity reconciliation, and incremental
transition logic stay in `core.rs`.

From the repository root:

```sh
cargo test -p plugin_json_incremental_v2
cargo build --release -p plugin_json_incremental_v2 --target wasm32-wasip2
```

The two 10 MB sparse-edit checks are explicit acceptance gates rather than
ordinary unit-test cost:

```sh
cargo test -p plugin_json_incremental_v2 -- --ignored --nocapture
```
