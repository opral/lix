# ForkTree bounded GC owner contract

Status: test/evidence artifact only. This file changes no production source and
does not authorize Stage 2.

## Immutable input

- Architecture base: `8e3ffe632bc27e1ab84fe9a6102b099ab2e9f441`, tree
  `8da56ca4e5d77aa25e57e611fbf4aaad4c01dd10`.
- Reviewed Stage-1 successor: `4b7b3aa25ebed5f022ed258c172c27e4dc64753d`,
  tree `5cafd24b60112220e86c5bccaf5fb382416f2666`.
- Current unbounded implementation terms:
  `forktree/reachability.rs::stream_selector_roots` returns all roots in one
  `Vec<ObjectId>`; `mark_reachable` retains the reachable-ID set and validation
  claim maps; `discover_sweep_plan` retains every orphan ID. Its peak is
  `O(S + U + R + O)` IDs/claims for selectors `S`, current untracked rows `U`,
  reachable objects `R`, and orphan objects `O`.
- The replacement below hard-deletes `GcMarkPackV1`, `GcProgressV1`, the
  in-memory discovery functions, and `SweepPlan.orphan_object_ids`. There is no
  old-format reader or alternate sweep.

## Decision

Use one persisted, authenticated, rebuildable maintenance graph inside the
existing immutable object space, named by the existing singleton GC-progress
selector in the selector/epoch plane. The graph consists of bounded radix
mark packs, bounded queue packs, a live-branch inventory, and one continuation.
It is never a logical root authority:

1. The only semantic roots remain the exact global, branch, checkpoint,
   recovery, undo, redo, upload, and current-untracked owners.
2. Removing or losing GC progress can only lose work. It cannot change a read,
   retain a semantic object permanently, or authorize deletion.
3. Every sweep batch exact-CASes the raw global selector loaded with the current
   GC-progress selector. Mark data without that precondition is inert.
4. Repository open/public reads never consult GC objects. A malformed GC graph
   disables reclamation, not repository reads; the owner may exact-CAS-remove
   the malformed progress selector and restart without deleting semantic data.

This is deliberately not a Git-style positional bitmap. Git binds a bitmap to
one pack or MIDX checksum and requires full closure; its bit positions depend
on that stable object order. Lix's online object space has no stable dense
ordinal across publications. Lix borrows the full-closure/checksum binding,
but keys marks by authenticated `ObjectId`. See the official
[Git bitmap format](https://git-scm.com/docs/bitmap-format.html).

The 256-way first-byte fanout and sorted IDs follow the useful shape of Git's
OID fanout/lookup chunks, while remaining disposable maintenance data rather
than history authority. See the official
[Git commit-graph format](https://git-scm.com/docs/commit-graph-format.html).

Dolt/Noms confirms the relevant content-addressed pattern: start at every live
root, walk the Merkle graph, and reclaim the rest. Its online design also shows
why unpublished dependencies must be fenced from collection. Lix uses its one
publication/reclamation epoch and writer retry/restaging instead of a second
session-root authority. See Dolt's official
[GC overview](https://www.dolthub.com/blog/2020-10-16-garbage-collection-in-dolt/)
and [session-aware GC analysis](https://www.dolthub.com/blog/2025-03-21-session-aware-gc-technical-details/).

IPFS's direct/recursive pin distinction supports treating selectors as typed
root declarations and children as indirect reachability. Lix does not copy the
pin database: its existing selector plane is the sole root owner. See the
official [IPFS pin/GC documentation](https://docs.ipfs.tech/how-to/pin-files/).

RocksDB snapshots are coherent read-only views and SlateDB snapshots bind
visibility to one sequence, but neither adapter snapshot is a durable Lix
continuation across process restart. Therefore every resumed page opens a new
`StorageRead`, verifies the persisted fence, and resumes after an exact key.
See the official [RocksDB snapshot/iterator documentation](https://github.com/facebook/rocksdb/wiki/Basic-Operations/8b0db11192422ae154253ae6e76123f28b09488a#snapshots)
and [SlateDB `DbSnapshot`](https://docs.rs/slatedb/latest/slatedb/struct.DbSnapshot.html).

## Physical ownership and canonical encodings

No space is added. All immutable records below are new object domains in
`forktree.object.v1`; the only mutable record is the replacement value at the
existing singleton `gc-progress` key in `forktree.selector.v1`. Every immutable
encoding is domain-separated and its complete canonical bytes determine its
BLAKE3 `ObjectId`.

The hard-cut domain table is exact: domain 12 becomes `GcMarkPackV2`, domain 13
becomes `GcProgressV2`, and new domains 14/15/16 are
`GcRadixNodeV1`/`GcQueuePackV1`/`GcLiveBranchPackV1`. The V1 decoders for 12
and 13 are deleted. Object framing remains `LIXFTO\0\x01` followed by the
`u32` domain and canonical payload. The selector uses a new
`LIXFTC\0\x02` magic and `"lix forktree gc-progress selector v2"` hash domain;
the V1 selector decoder is deleted. The repository protocol hard cut rejects
any pre-cut bytes, so no ambiguous dual decoding is permitted.

All integers are unsigned big-endian. Counts are checked before allocation,
reserved bytes must be zero, IDs must be nonzero, keys/children are strictly
ordered and unique, and every decoder consumes the complete value.

Every `Option<T>` is encoded as one byte (`0` absent, `1` present) followed by
the canonical `T`; every variable byte field is `u32 length || bytes` and is
checked against its field-specific protocol maximum before allocation. A radix
node encodes children in ascending bitmap-byte/bit order and carries no
redundant child count. These rules make a logical value have exactly one byte
encoding.

```text
GcProgressSelectorV2 {
    cycle_id: [u8; 16],
    progress_object_id: ObjectId,
    selector_generation: u64,
    authenticated_domain_and_checksum,
}

GcProgressV2 {
    cycle_id: [u8; 16],
    phase: RootSelectors | RootUntracked | Traverse | Sweep | Cleanup,
    expected_global_digest: [u8; 32],
    expected_global_epoch: u64,
    selector_resume_after: OptionalBoundedKey,
    untracked_resume_after: OptionalBoundedKey,
    object_resume_after: Optional<ObjectId>,
    maintenance_resume_after: Optional<ObjectId>,
    saw_global_selector: bool,
    live_branch_index_root: Option<ObjectId>,
    mark_index_root: Option<ObjectId>,
    queue_index_root: Option<ObjectId>,
    queue_pop_sequence: u64,
    queue_push_sequence: u64,
    marked_count: u64,
    validated_count: u64,
    reclaimed_count: u64,
}
```

`cycle_id = first_16(BLAKE3("forktree.gc-cycle.v2" || raw_global ||
next_gc_selector_generation))`. A progress decode authenticates all referenced
maintenance roots and requires their `cycle_id` to match.

The persisted cursor is the exact last returned storage key. ForkTree defines
a protocol maximum for selector/untracked keys; values beyond it fail at write
and open, rather than producing an unbounded cursor. Object-space cursors are
always the fixed 32-byte `ObjectId` key.

```text
GcRadixNodeV1 {
    cycle_id: [u8; 16],
    kind: LiveBranch | Mark | Queue,
    consumed_prefix_len: u8,       // 0..32
    consumed_prefix: [u8; consumed_prefix_len],
    child_bitmap: [u8; 32],        // 256 possible next bytes
    child_object_ids: [ObjectId; popcount(child_bitmap)],
}

GcMarkPackV2 {
    cycle_id: [u8; 16],
    consumed_prefix: BoundedBytes, // canonical Patricia leaf prefix
    entries: 1..=4096 strictly sorted {
        object_id: ObjectId,
        expected_domain: u16,
    },
}

GcQueuePackV1 {
    cycle_id: [u8; 16],
    entries: 1..=1024 strictly increasing {
        sequence: u64,
        object_id: ObjectId,
        expected_domain: u16,
        edge_cursor: Optional<GcEdgeCursorV1>,
    },
}

GcLiveBranchPackV1 {
    cycle_id: [u8; 16],
    entries: 1..=4096 strictly sorted {
        key_digest: [u8; 32],
        branch_id: CanonicalBranchId,
    },
}

GcEdgeCursorV1 {
    source_object_id: ObjectId,
    source_domain: u16,
    next_edge_ordinal: u64,
    owner_cursor: BoundedBytes,
}
```

The radix key is the full `ObjectId` for marks, the domain-separated digest of
the full branch ID for live-branch membership, and
`[0; 24] || sequence.to_be_bytes()` for queue entries. Patricia prefix
compression prevents long unary chains. A pack splits canonically by the next
key byte when it exceeds its maximum. This gives at most 32 radix-node reads
per point operation—constant in `R`, not an `O(log R)` tree whose height can
grow without a protocol bound. Batch edits sort and merge one bounded pack at
a time.

The mark value records the expected object domain. Encountering the same ID
with a different expected domain is corruption. Parent-specific constraints
(catalog key/back-edge, commit ordinal/generation, branch chronology, declared
chunk length/digest, receipt aggregate) are validated while decoding that
parent's edge page; they are not accumulated in global claim maps.

The owner exposes an `EdgePager` for every object domain. One call authenticates
the object and returns at most 256 typed edges plus an optional bounded cursor.
An unbounded commit member list, manifest, receipt tree, or catalog leaf must be
segmented before this GC ships. Resume re-authenticates the source object; no
opaque hasher state or unauthenticated decoder position is persisted.

## Hard bounds

```text
selector scan page                 256 fixed-size selectors
untracked scan page                1 row (a row may hold a large encoded value)
edge page                          256 typed edges
traversal work batch               128 queue claims
mark pack                          4096 IDs
queue pack                         1024 claims
object key sweep page              256 fixed 32-byte keys
object delete batch                256 IDs
radix node                         256 child IDs maximum
one decoded object/chunk window    current immutable-value protocol maximum
```

The implementation sequences, rather than co-retains, the mark-pack, queue
pack, edge page, and sweep page. Peak retained IDs/claims are therefore bounded
by one pack/node/page combination (under 6K IDs and under 512 KiB of GC index
metadata), plus one decoded object or chunk window. It never scales with
`S + U + R + O`. The existing storage API's row-count-only scan cannot promise
a byte-bounded multirow page, so potentially wide untracked values are scanned
one at a time. Object sweep uses key-only pages and loads at most one unmarked
candidate for authentication before deletion.

## State machine

### 0. Start

One coherent read loads raw global selector and exact absence of GC progress.
The owner atomically creates `GcProgressV2(RootSelectors)`, creates its typed
selector, and writes the same `RepositoryRoot` with epoch + 1. Exact
preconditions are raw-global equality and GC-progress absence. This commit
occurs before the first selector scan, so the maintenance key set is stable.

### 1. RootSelectors

Open a new coherent read; load raw global and GC progress together. Require
`BLAKE3(raw_global) == expected_global_digest`, the exact epoch, cycle, and
selector generation. Scan at most 256 selectors after the persisted key.

- Authenticate the global selector exactly once and enqueue its repository
  root.
- Authenticate every branch, checkpoint, recovery, undo, redo, and upload
  selector and enqueue its typed object root.
- Insert each live branch ID into the persisted live-branch radix inventory.
- Recognize the current GC-progress selector, validate that it names this exact
  progress object, and do not treat it as a semantic root.
- Unknown keys, duplicate logical identities, key/value mismatch, malformed
  role, missing target, or zero ID fail closed.

Each unique root is inserted into the mark radix and append queue in the same
bounded checkpoint. The checkpoint exact-CASes raw global and old GC progress,
publishes the new immutable maintenance objects and progress selector, and
rotates the global epoch. The new progress records the resulting raw-global
digest/epoch. The typed checkpoint operation is the only operation allowed to
change epoch without changing semantic roots.

Although each checkpoint opens a fresh adapter snapshot, an external selector
or logical write between pages changes raw global and causes restart. The
owner's own checkpoint changes only epoch/progress, so advancing its stored
fence is safe. A terminal empty/`has_more=false` page, strict increasing keys,
and `saw_global_selector` are required before phase transition.

### 2. RootUntracked

Scan one current-untracked row after the persisted cursor. Authenticate its
key/value. Its blob/plugin object edges are roots only when its branch ID is in
the persisted live-branch inventory. Insert unique roots into mark/queue and
checkpoint exactly as above. Every untracked logical writer is required to
rotate the same global epoch, so insertion or deletion before/behind the cursor
invalidates the cycle. The terminal page moves to `Traverse`.

### 3. Traverse

Pop at most 128 queue claims. For each:

1. load and authenticate key, complete object bytes, object ID, and expected
   domain;
2. decode at most 256 owner edges;
3. immediately validate all semantic relationships represented on that page;
4. fail closed on missing/corrupt/mismatched edges;
5. insert each new `(ObjectId, expected_domain)` into the mark radix and append
   exactly one queue claim; and
6. append a continuation claim when the source has another edge page.

Only the first mark insertion enqueues an object. A continuation does not mark
a second object and cannot advance past an unauthenticated edge page. The queue
pop cursor advances only in the same atomic progress checkpoint. When
`queue_pop_sequence == queue_push_sequence` and there is no edge continuation,
every mark has been authenticated; move to `Sweep`. There is no separate
`BTreeSet`, `VecDeque`, chunk claim map, sequence claim map, or catalog claim
map.

Queue packs wholly below the pop cursor and superseded radix/pack path objects
are deleted by the same typed maintenance-rewrite plan that moves the progress
selector. That planner is private to the radix owner and may delete only
authenticated, same-cycle nodes proven absent from the new maintenance roots.
It cannot name semantic domains.

### 4. Sweep

Scan 256 object keys after `object_resume_after`. In sorted order, merge the
object page against a streaming mark-radix range iterator. This avoids one
radix lookup per object and makes a complete sweep linear in sorted IDs.

- Marked objects are retained; their bytes were already authenticated.
- An unmarked candidate is loaded and authenticated before action. Malformed
  key/value/domain fails closed rather than being silently deleted.
- Active-cycle `Gc*` objects are retained for `Cleanup`.
- Superseded objects from older GC cycles are ordinary unmarked garbage.
- At most 256 authenticated unmarked semantic IDs enter one private
  `SweepBatch`.

The batch exact-CASes raw global and old GC progress, deletes only those IDs,
publishes the next progress object/selector, and rotates epoch atomically. A
publication that wins first makes this batch stale. A sweep batch that wins
first makes the publication stale; its retry must re-check/restage every absent
deduplicated object before moving any selector. Deletions already committed by
an earlier sweep page remain safe if a later external write aborts the cycle:
they were unreachable under that page's exact fence, and the later writer had
not yet won publication.

### 5. Cleanup and finish

After the terminal semantic object page, marks are no longer needed. Scan the
object space by key and delete at most 256 authenticated same-cycle maintenance
objects per batch, excluding the exact current progress object. Each batch
also deletes the immediately superseded progress object. Finally exact-CAS
remove the GC-progress selector and delete its last progress object while
rotating epoch. No semantic object is deletable in this phase.

A crash leaves either the old complete checkpoint or the new complete
checkpoint because object writes, selector move, epoch move, and bounded
deletes share one adapter transaction. Reopen authenticates the whole progress
closure before continuing. Missing/corrupt maintenance state stops GC; an
epoch-fenced abort may remove only the progress selector/current maintenance
closure and restart. It may never sweep from a partially readable mark graph.

## Race and final-reference rules

- **Publication first:** any semantic/root/upload/dedup/untracked writer rotates
  global epoch; stale GC progress or sweep CAS fails and the cycle restarts.
- **GC first:** the stale writer CAS fails. Retry must perform owner presence
  probes and restage reclaimed chunks/objects before selector publication.
- **GC self-progress:** each successful GC checkpoint rotates epoch and records
  the resulting fence. A foreign change is distinguishable because it cannot
  satisfy the old raw-global and progress-selector preconditions.
- **Receipt completion:** upload-selector removal and branch/global manifest
  publication are one epoch-rotating adapter commit, leaving no reachability
  gap.
- **Final reference:** an object is marked once regardless of the number of
  branch/history/checkpoint/recovery/undo/redo/upload/plugin/untracked edges.
  Releasing one edge does not reclaim it. Only a later complete cycle after the
  final authoritative edge is atomically released can delete it.
- **Crash/reopen:** no adapter snapshot token is persisted. Resume uses exact
  key cursors plus a freshly loaded coherent read and the recorded global
  fence. A cursor without that fence is invalid.

## Complexity and storage amplification

Let `E` be authenticated object edges and `Q` the maximum live queue frontier.

```text
root discovery        O(S + U) work
traversal             O(R + E) work; <=32 fixed radix steps per new ID
sweep                 O(R + O) ordered merge work
total                 O(S + U + R + E + O)
peak GC memory        O(pack + page + one object/chunk), independent of totals
live maintenance disk O(R + Q)
```

The 32-byte ObjectId caps radix depth, so the fixed radix factor does not alter
the asymptotic bound. Page-sorted batch edits and the sweep range iterator are
required acceptance gates; a per-object unbounded tree search is not.

A mark entry is approximately 34 bytes before canonical framing; a queued
claim is approximately 42 bytes without an edge cursor. Radix children add one
32-byte object ID each, amortized across packs. The target logical maintenance
budget is at most roughly `56*R + 64*Q` bytes plus headers and the current
object window. Exact qualification must report encoded bytes, object count,
backend bytes/calls, peak RSS, and settled RocksDB/SlateDB disk. Superseded
maintenance paths are atomically retired during progress; terminal cleanup
prevents a full extra cycle of mark data. LSM tombstones may transiently exceed
logical bytes and must be reported pre/post compaction or SlateDB settling.

## Compiler/API sealing (including equivalent-token forgery)

Hiding `OBJECT_SPACE` alone is insufficient because Stage 1 exposes public
`SpaceId(pub u32)`, public `StorageSpace` fields, and public constructors. The
hard cut is:

```text
pub struct SpaceId(u32);                    // private field
pub struct StorageSpace {                   // all fields private
    id: SpaceId,
    name: &'static str,
    value_semantics: ValueSemantics,
    _engine_brand: private::EngineDeclared,
}

impl StorageSpace {
    pub fn id(&self) -> u32;                 // read-only adapter access
    pub fn name(&self) -> &'static str;
    pub fn value_semantics(&self) -> ValueSemantics;
    pub(crate) const fn engine_declared(...); // sole constructor
}
```

Third-party adapters can inspect a descriptor passed into trait methods but
cannot construct, mutate, or clone-and-retarget an equivalent reserved token.
Storage conformance uses fixed engine-minted test descriptors; it exposes no
arbitrary-ID factory. The object/selector descriptors stay private to the
ForkTree owner.

Raw `StorageWrite::{put_many,delete_many,delete_range}` may remain adapter
operations, but no safe external caller can manufacture a reserved descriptor.
Within Lix, the ForkTree owner exposes only typed private-field plans:

```text
stage_publication(...) -> PreparedPublication
advance_gc(storage, GcBudget) -> GcStepStatus
abort_corrupt_gc(storage) -> GcStepStatus
```

`gc.rs`, sessions, and other owners receive neither raw object IDs to delete
nor any object/selector `StorageSpace`. `SweepBatch`, maintenance rewrite
deletes, expected raw selector bytes, and object puts have private fields and
are constructed/consumed in the owner module. The sealed-owner source gate
permits reserved-space mutation only in that module.

Required external compile-fail probes:

1. import `forktree::OBJECT_SPACE` or `SELECTOR_SPACE`;
2. construct `SpaceId(0x0009_0001)` or any same numeric token;
3. use a `StorageSpace` literal or public immutable/mutable constructor;
4. alter a legitimate descriptor's ID/name/semantics;
5. pass an equivalent token to generic put/delete/delete-range; and
6. construct/modify `SweepBatch`, `PreparedPublication`, or a GC maintenance
   rewrite plan.

All must fail in a separate dependent crate using safe Rust. Static conformance
also rejects reexports, reserved numeric IDs/names, and generic reserved-space
mutation outside the sealed owner. This is the compiler proof that the typed
owner—not convention—controls both insertion and reclamation.

## Acceptance contract for Ryzen-V

1. Hard-delete the V1 in-memory discovery/sweep and codecs; add no reader,
   migration, feature flag, fallback, or second mark space.
2. Implement the exact progress phases, bounded packs/pages, and same-cycle
   validation above in the one object/selector plane.
3. Prove 1K/50K roots+reachable+orphans have the same bounded peak IDs/claims,
   while work grows linearly and RocksDB/SlateDB reopen resumes exactly.
4. Prove malformed pack/radix/cursor/domain/count/order/cycle/fence and missing
   maintenance nodes never reach sweep.
5. Prove publication-first and GC-first for ordinary, deduplicated, upload,
   root-only, and untracked writers; completion handoff; shared final release.
6. Land the descriptor-sealing cut and all six external compile-fail probes in
   Stage 1 before any Stage-2 reader/writer connection.

Any implementation that retains all roots/reachable/orphans/claims in memory,
persists a semantic pin/root table, trusts a progress object without exact raw
global CAS, resumes an adapter snapshot token after reopen, or exposes a
forgeable equivalent storage token is a blocker.
