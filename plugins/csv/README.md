# CSV Component API v1

Fused, push-based initial CSV import used only for runtime and boundary
benchmarking.

Prototype B emits bounded batches containing compact local row references,
order ranks, decoded UTF-8 cells, and sparse lexical layout. The Wasm guest
does not allocate row UUID strings or JSON snapshots. The current host adapter
rebuilds canonical snapshots into one shared page arena so the unchanged v2
transaction path can provide a correctness-matched comparison.

That adapter is intentionally the boundary for Prototype C: C must lower these
batches into transaction/storage-native columns without a `Vec<Entity>` or a
second full row materialization.
