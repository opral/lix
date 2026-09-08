# Testing and maintaining file plugins

The SDK provides helpers through `lix::plugin`. Plugins still implement the four
`FileProjection` hooks explicitly. Incremental fallback remains a plugin decision:
a full rebuild must preserve existing row identities, and warm calls intentionally
avoid loading all accepted rows.

## Validated edits

`EditSet` validates all edits against the length of the accepted file before any
bytes are read or changed. Offsets refer to that original file, starts must be
strictly increasing, and deletion ranges must not overlap. Adjacent edits are
allowed; multiple insertions at the same offset must be combined by the caller.

```rust,ignore
let edits = input.file_edits.validated(input.before.len())?;
let first_bytes = edits.read_range(&input.before, 0, edits.len().min(128))?;
```

`read_range` uses coordinates in the resulting file and reads only intersecting
unchanged bytes from the snapshot. Inserted bytes come directly from the edits.
`apply(&bytes)` materializes the complete resulting file when that is appropriate.
The helper handles binary bytes; encoding, record boundaries, and syntax are
still the plugin's responsibility. Use the same accepted snapshot used to
validate the edit set; matching lengths alone cannot establish file identity.

## Private state cleanup

All four output types implement `StateOutput`. Shared helpers can accept
`&mut impl StateOutput` without repeating an adapter trait in each plugin.

```rust,ignore
fn replace_index(output: &mut impl lix::plugin::StateOutput) -> lix::plugin::Result<()> {
    output.delete_state_prefix(b"my-plugin/index/")?;
    output.put_state(b"my-plugin/index/root", b"new index")
}
```

Prefix deletion includes accepted keys and writes staged earlier in the same
transition. Later writes recreate keys. Like other state writes, deletion commits
with the successful transition and rolls back on failure. Empty prefixes and
prefixes overlapping the host's reserved state namespace are rejected. Include a
separator in your namespace to avoid unintentionally matching similarly named
keys. This is a cleanup operation for rebuilds, not a replacement for sparse
updates on every edit.

## Conformance expectations

For a plugin that promises lossless editing, test these properties:

- Replaying unchanged rows retains exact bytes, including encoding, BOM,
  whitespace, quoting, and line endings.
- Incremental edits and full parsing agree on semantic content.
- Untouched rows keep their identities through editing and reopening.
- Evicting disposable indexes does not change the result of the next edit.
- Invalid or unrepresentable edits leave accepted bytes, rows, and state intact.
- Repeated identical inputs produce identical outputs.

Formatting hints can become stale after row edits. Normalize hints such as CSV
quote flags against the new content before rendering. Preserve authoritative
formatting information in durable rows or recover it from accepted bytes; do not
depend on an index surviving cache eviction.

Keep compiled-component integration tests as well: native tests do not verify
the Wasm boundary, host schema validation, identity reconciliation, or SQL
transactions.

## Native projection driver

`lix::plugin::testing::Harness<MyPlugin>` calls the four real `FileProjection`
hooks with in-memory snapshots, typed row pages, and staged outputs. It is
available on native targets without an additional feature flag.

```rust,ignore
use lix::plugin::{CreateContext, FileEdit};
use lix::plugin::testing::{Harness, Snapshot};

let driver = Harness::<MyPlugin>::default();
let creates = CreateContext::from_namespace_bytes([1; 12]);
let file = Snapshot {
    file_id: "test-file".into(),
    path: "example.md".into(),
    bytes: b"Hello\n".to_vec(),
    ..Snapshot::default()
};
let parsed = driver.parse(&file, creates)?;
assert!(!parsed.row_changes.is_empty());
let file = parsed.into_snapshot(); // Explicitly accept the staged result.
let edit = FileEdit { offset: 0, delete_len: 5, insert: b"World".to_vec() };
let changed = driver.parse_changes(
    &file, &file.path, &[edit], None,
    CreateContext::from_namespace_bytes([2; 12]),
)?;
assert_eq!(changed.snapshot().bytes, b"World\n");
```

`serialize` accepts complete `TypedRowRecord` values and an optional accepted
snapshot. `serialize_changes` accepts sparse `TypedRowChange` values. Results
expose decoded row changes, the row-replacement flag, emitted byte edits, and
an optional complete file replacement. Inspect the staged successor with
`snapshot()` and commit it with `into_snapshot()`. An error leaves the input
snapshot unchanged, including state writes emitted before the error.

For a cold incremental call, clear the snapshot's disposable `state` and pass
complete accepted rows in `parse_changes`'s `cold_rows` argument. The driver
intentionally does not maintain a database: tests apply emitted row changes to
their own fixture rows. A create's `local_ref` resolves to `creates.id(local_ref)`;
the fixture must supply generated columns and primary keys when turning creates
into complete durable rows. Use a new deterministic create namespace for each
transition. See the Markdown plugin's `src/adapter_qa_tests.rs` for a complete
parse, sparse-edit, row-edit, and cold-reopen lifecycle.

Additional examples live in `plugins/json/src/adapter_qa_tests.rs` and
`plugins/excalidraw/src/qa_tests.rs`. They exercise composite and native primary
keys, repeated edits, exact scalar/object spelling, and embedded files. JSON's
row-edit contract supports scalar updates, insertion, deletion, reordering, moves,
and scalar/container conversion. Scalar updates use small byte splices; structural
batches rebuild the tree and stream a complete replacement, preserving unchanged
scalar spelling and row identities. Invalid final trees reject the entire plugin
call. Deleting a container requires deleting or moving its descendants in the same
row batch; there is no implicit cascade.

SQL projects each statement separately, including within `execute_batch`. Delete
a subtree in one `DELETE` statement, and remove children before changing their
parent's kind. Object keys are primary keys: rename them with a delete and insert.
Valid existing key-layout hints adapt to the new key while preserving whitespace;
empty-container whitespace hints are ignored after conversion to a scalar. Structural writes use normal
row upsert semantics, so a later stale writer can recreate a deleted key. The
plugin does not provide a separate deletion-wins concurrency policy.

The harness uses a conservative 1 MiB batch limit by default. Set
`driver.max_batch_bytes = 2 * 1024 * 1024` to match the standard runtime page
budget when testing large state pages or embedded files. Cold-file admission can
raise the real host's budget further. The native driver checks per-operation
limits, while compiled-component tests cover the complete host budget.
