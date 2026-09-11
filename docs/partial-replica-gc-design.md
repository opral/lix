# Partial replica object ownership and garbage collection

A partial replica with on-demand sync cannot use authority garbage collection. The authority planner currently traverses semantic history, mutation owners and complete scoped trees; an absent local object is not evidence that a descendant is unreachable. `stage_repository_gc_with_preconditions` therefore rejects a durable partial-replica receipt before staging mutations. This is a safety boundary, not an implemented partial collector.

## Ownership records required before collecting partial storage

Keep records separate from the bounded opening receipt. Opening reads their roots/revisions, not all records.

- **Native object inventory:** typed family/address, resident encoded bytes, origin (`downloaded` or `locally authored`), publication status, and inventory revision. The same address can have several owners; origin alone is insufficient to authorize eviction.
- **Owner-to-object edges:** owners include a prepared operation/scope, pending local commit, retained local checkpoint, recovery export and active baseline descriptor. Immutable bytes can be shared by multiple owners. Releasing one owner must not remove another owner's objects.
- **Local commit ownership:** each durable commit atomically records its authored objects, mutation/export dependencies, native parent/root references, and the authority baseline it extends. Pending commits and their validation/export/recovery dependencies are pinned until a defined transition releases them.
- **Remote boundary references:** an absent native descendant may remain referenced by its immutable address. Its authority lease protects availability; it does not make the bytes locally resident. A missing remote boundary terminates local traversal only because ownership explicitly says the boundary is remote, never merely because lookup returned no bytes.
- **Coverage ownership:** deleting any object required by an unpinned cached scope removes or downgrades that scope's completeness in the same transaction. Pinned prepared operations remain warm. Absence proofs and schema/context dependencies are owners too.

One possible physical layout uses separately keyed owner records and owner-object edges plus an address-oriented reverse index. Publish indexes atomically with writes/coverage. Refcounts alone are fragile without idempotent owner transitions and crash recovery; the owner edges remain the recoverable source of truth.

## Collection algorithm

1. Read the partial receipt, ownership revision and bounded eviction candidate page at one local snapshot.
2. Exclude addresses referenced by pending commits, pinned prepared operations, recovery/checkpoint owners, or the active bounded descriptor.
3. For downloaded unowned cache objects, stage removal together with affected unpinned coverage. No remote traversal or network request is necessary.
4. For locally authored objects, require a local ownership proof that no retained local owner references them. Do not discard an object merely because its commit uploaded: an acknowledgment must attest accepted authority state and transition owners without dropping newer local descendants.
5. Commit against receipt/ownership/coverage revisions. Concurrent preparation, hydration, local commit and acknowledgment must invalidate a stale deletion decision.
6. Bound candidates, bytes and mutations per collection slice. Continue from a durable cursor; never enumerate authority history while opening or collecting the browser cache.

An authority may share the same semantic checkpoint/commit graph, but its collector remains distinct: it owns a complete repository and must retain leased baseline roots before deleting native objects. A lease expiry with pending local work enters explicit recovery; it does not authorize local work deletion.

## Checkpoints

Checkpoint preparation is separate from preparing ordinary row edits. The current native checkpoint publisher can reuse an immutable state alias and current HOT generation, but needs its declared checkpoint parent's topology/replay-debt authority. The feasibility probe adds only that specific native header. It does not load the checkpoint's mutation inventory, tree, rows, or earlier parent history.

Do not interpret success after this header preparation as GC readiness. Checkpoint publication, complete-state alias validation, dirty-index retirement, local checkpoint ownership, export/admission and recovery must each pass with the base absent. Any path that retires a packed/root-backed serving generation must preserve the still-referenced native boundary and its ownership.

## Required tests

- Collection with an omitted remote subtree preserves locally authored descendants and pending commits without fetching the subtree.
- Overlapping scope owners release independently; prepared offline reads/writes remain local after unrelated eviction.
- Local commit, hydration and preparation racing with collection defeat stale deletion preconditions.
- Acknowledging an ancestor retains a newer local descendant and all export/recovery inputs.
- Crash/reopen at each owner transition reproduces identical retention decisions.
- Checkpoint, reset and lease expiry preserve pending work; an expired boundary causes explicit recovery rather than data loss.
- Candidate work remains bounded as unrelated repository rows, history, branches and blobs grow.
