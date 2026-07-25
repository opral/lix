# Incremental JSON plugin (Component API v2)

This crate is the recursive JSON reference for the production
`wasm-component-v2` API. It models the document as stable semantic nodes
rather than using a full RFC 6901 JSON Pointer as entity identity:

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
semantic model also supports nested objects and arrays.

## JSON lifecycle policy

Byte writes own JSON structure and lossless layout: additions, deletions,
container conversions, moves, ordering, and whitespace/escaping changes all
go through `file-changed`. The plugin reconciles a byte-side structural change
by decoded object key and stable array identity; deleting a container emits
explicit tombstones for its descendants.

Semantic SQL writes intentionally cover the 80% path only: one existing scalar
value per transition. A scalar may change JSON kind when `kind` and
`scalar_json` agree, but it cannot be added, deleted, moved, reparented,
reordered, converted into a container, or reformatted through
`entities-changed`. Those requests return `LIX_INVALID_PARAM` and must be
expressed as an authoritative byte write instead.

Actor transitions serialize direct scalar writes, so two writes to the same
scalar use durable commit order as deterministic last-write-wins; edits to
different scalars compose. A structural byte change fences a stale scalar or
multi-entity rebase rather than resurrecting or partially recreating removed
nodes. The caller rereads the file and retries from the new structure. There
are no first-class JSON conflict records in this version, so this stale-rebase
fence also reports `LIX_INVALID_PARAM` with a retry-oriented message.

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
cargo test -p lix_sdk_tests --test e2e \
  v2_json_ten_mib_real_wasm_edit_stays_sparse_and_bounded \
  -- --ignored --exact --nocapture
```

The second command builds and installs the real Wasm component, imports an
exact 10 MiB / 39,870-property fixture through the host and Canonical ABI, then
applies one verified-cache-backed provenance byte edit. It gates exact
materialized bytes, one semantic change, zero warm full-blob scans or source
reads, a sub-64 KiB warm boundary payload, and the production 128 MiB
guest-memory ceiling.
