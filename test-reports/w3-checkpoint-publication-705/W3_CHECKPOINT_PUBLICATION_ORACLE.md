# W3 checkpoint/snapshot-pin publication oracle

Status: TEST/REPORT ONLY. This package is anchored to immutable reader
correction head `705440f55eccba9e2d55c0951d6a684737005d76` and makes no
production or Cargo changes. It materializes the accepted W3 contract from
report SHA `20f715beb27bb9dc8a4b2f32085e6bc876c375e84f16b5e540581751ee33fdfa`.

## Required transaction shape

An accepted checkpoint/snapshot-pin publication must lower through one
caller-owned coherent read and the existing transaction boundary:

1. one authenticated `CoherentView`;
2. one `PreparedPublication` and one `into_storage_plan`;
3. checkpoint/recovery references, branch/global selector epochs, runtime,
   idempotency, catalog, and revision metadata appended to that same write set;
4. one existing `prepare_write_set`;
5. one existing prepared commit at the transaction boundary.

`PreparedPublication::commit`, an independent checkpoint/recovery writer,
second coherent read, retry publication, cache, format, compatibility path,
or alternate durable authority is forbidden. W4 GC/recovery sweeping and W5
writer families are out of scope.

## Focused acceptance matrix

The future runnable successor must bind the model to public transaction tests
on Memory, RocksDB, and SlateDB. Required cases are:

- 65 checkpoint/snapshot-pin rotations, including the 64-interval retention
  boundary plus suffix;
- selected history members, intermediate commits, and parent override;
- checkpoint and recovery references in the same atomic publication;
- branch-first and GC-first races, with stale retry from a fresh view;
- same-owner stale selector/recovery rejection and unrelated-owner behavior;
- true no-op, savepoint/rollback, and unsupported zero-write cohorts;
- duplicate, out-of-order, ordinal, back-edge, missing-parent, and wrong-parent
  corruption with no partial publication;
- cold reopen preserving branch, checkpoint, recovery, and user state.

Every accepted result must prove no independent publication path was used.
Counters should report one view, one plan, one prepare, and one commit.

## Calibrated RED control

On exact `705440f55…`, `transaction/commit.rs` still rejects non-empty
`checkpoint_publications` with:

```text
checkpoint publication requires the ForkTree snapshot-root lowering slice
```

The verifier must exit `1` for this intentional RED. A future W3 candidate
must preserve the exact-head/tree guard, remove this pre-planning rejection by
lowering checkpoint intent into the ordinary transaction plan, and pass the
same structural controls without adding a second writer.

No runtime, Cargo build, adapter benchmark, or public test was run here because
the pinned source remains compiler-red. The model is intentionally un-wired;
it cannot create a false green result on the old frontier.
