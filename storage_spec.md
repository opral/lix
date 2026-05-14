# Lix Storage v2 Architecture Spec

`storage_v2` is the internal Lix storage runtime over `backend_v2`.

It is not the public persistence plugin API. Most users should bring their own
backend. `storage_v2` exists so Lix domain stores can share transactions,
write batching, spaces, prefix lowering, caller-order reconstruction, and other
domain-neutral storage mechanics. It also exposes baseline read/write shape
stats so later benchmarks and optimizations can prove the physical access shape.
Cursor wrapping, capability-aware lowering, fallback accounting, residual
filtering loops, and projection/delete-range fallbacks are planned
optimization-hardening work, not implemented baseline behavior yet.

## Layering

```text
Lix domain / engine
  commits, tracked state, diffs, merge, queries, user semantics

Domain stores
  tracked_state store
  commit store
  json/blob store
  indexes
  visibility rules
  payload refs
  key encoding
  envelope decoding
  residual filtering
  hydration

Generic storage adapter: storage_v2
  write sets
  read/write scopes
  named space declarations
  batching helpers
  prefix-to-range lowering
  caller-order point reconstruction
  read-shape stats
  write-set stats

Planned storage_v2 optimization extensions:
  storage cursor tokens
  capability-aware lowering
  projection fallback
  residual filtering loops
  delete_range/precondition fallback safety gates
  fallback accounting

Backend: backend_v2
  ordered byte keys
  opaque byte values
  get_many
  scan_range
  put_many / delete_many
  begin_read / begin_write
  atomic commit
```

The clean rule:

```text
backend_v2:
  physical persistence

storage_v2:
  generic Lix storage mechanics over backend_v2

domain stores:
  Lix-aware physical schemas and subsystem semantics
```

## Non-Goals

`storage_v2` must not become a catch-all for domain layouts.

Do not put these in generic storage:

```text
tracked_state root/chunk/by-file key encoding
commit graph semantics
schema/file/entity-specific layouts
JSON pack formats
payload hydration policy
branch visibility semantics
index rebuild semantics
GC reachability semantics
```

Those belong in domain stores such as `tracked_state`, `commit_store`,
`json_store`, or future dedicated domain-store modules.

Do not expose `storage_v2` as the main public extension point. The public
extension point is `backend_v2`.

## What Storage v2 Owns

`storage_v2` owns the write-side complexity boundary and may own other
domain-neutral storage mechanics:

```text
write set aggregation across domain stores
conversion from storage write sets to backend PutBatch/delete_many calls

read transaction wrapper
read scopes shared by multiple domain stores
named space declarations
caller-order point reconstruction
duplicate requested-key handling
prefix-to-range lowering
read-shape stats
write-set stats
baseline storage adapter conformance
```

Planned storage_v2 optimization-hardening responsibilities:

```text
storage cursor token construction and validation
fallback stats
capability-aware lowering helpers
limit-after-residual scan loops
projection/envelope helpers, only when domain-neutral
delete_range/precondition fallback safety gates
```

The current engine already has this shape:

```text
tracked_state
  -> StorageReader / StorageWriteSet
  -> StorageContext transaction
  -> backend transaction
```

`storage_v2` should be the backend_v2-era version of that generic adapter, not
a replacement for `tracked_state/storage.rs` or `tracked_state/codec.rs`.

For writes, this layer is mandatory for engine-visible mutations. Domain stores
own what to write, but they stage into `StorageWriteSet`; storage_v2 aggregates,
lowers, and commits the aggregate through one backend write transaction.

## What Domain Stores Own

Domain stores own Lix-aware physical schemas.

Examples:

```text
tracked_state:
  root refs
  chunk storage
  tree key encoding
  by-file index
  delta pack encoding
  tracked-state materialization policy

commit_store:
  commit object encoding
  commit parent refs
  commit metadata
  commit graph queries

json/blob stores:
  payload refs
  pack layouts
  payload materialization
  checksum/identity rules

visibility/index stores:
  truth rows vs derived rows
  publication order
  index rebuild policy
  residual filtering rules
```

A domain store may use `storage_v2` helpers, but its keys and values are its own
schema.

## Storage Spaces

`backend_v2` uses numeric `SpaceId`.

Domain stores should not hand-roll numeric `SpaceId` values. `storage_v2` should
provide declaration helpers so spaces are stable and easy to audit. The current
implementation has named `StorageSpace` declarations and validates conflicting
same-id/different-name declarations inside a `StorageWriteSet`; it does not have
a global runtime registry yet.

Example shape:

```rust
pub struct StorageSpace {
    pub id: SpaceId,
    pub name: &'static str,
}

pub const TRACKED_STATE_CHUNK_SPACE: StorageSpace = StorageSpace {
    id: SpaceId(1),
    name: "tracked_state.tree.chunk",
};
```

Space names are for diagnostics and validation. Backends see only `SpaceId`.

Open question: whether space definitions live centrally in `storage_v2/spaces.rs`
or next to each domain store and are registered centrally. Prefer the second if
centralization starts leaking domain concepts into generic storage.

## Write Sets

Storage writes must be staged before being applied to a backend write
transaction. `StorageWriteSet` is not just a convenience helper; it is the
required write-side complexity boundary between domain stores and `backend_v2`.

This preserves cross-domain atomicity:

```text
tracked_state stages rows
commit_store stages commit object
json_store stages payload refs
visibility store stages publication rows
storage_v2 applies all mutations to one BackendWrite
BackendWrite commits atomically
```

And it preserves the write-side Big-O shape:

```text
domain stores emit K logical mutations
storage_v2 stages them in memory
storage_v2 groups by SpaceId and operation
storage_v2 lowers to put_many/delete_many batches
backend_v2 commits once
```

Anti-goal:

```text
wrong:
  each domain store calls backend.put_many independently
  each row calls backend put/commit

bad result:
  O(K) backend calls
  O(K) transaction/fsync boundaries
  no global cross-domain atomicity
```

Sketch:

```rust
pub struct StorageWriteSet {
    groups: Vec<StorageWriteGroup>,
    // Optional small map from SpaceId -> group index if G is no longer tiny.
}

pub struct StorageWriteGroup {
    pub space: SpaceId,
    pub puts: Vec<PutEntry>,
    pub deletes: Vec<Key>,
}

pub struct StoragePut {
    pub space: SpaceId,
    pub key: Key,
    pub value: StoredValue,
}

pub struct StorageDelete {
    pub space: SpaceId,
    pub key: Key,
}
```

`StoragePut` and `StorageDelete` are useful conceptual shapes, but the write set
should group as it stages rather than flattening everything and sorting later.
It must preserve enough grouping information to lower
efficiently to:

```text
put_many(space, PutBatch)
delete_many(space, &[Key])
```

The write set should not encode domain semantics such as "publish commit
visibility". Domain stores decide what to stage; storage_v2 decides how to batch
and lower it.

API sketch:

```rust
impl StorageWriteSet {
    pub fn stage_put(&mut self, space: StorageSpace, key: Key, value: StoredValue) {
        self.group_mut(space.id).puts.push(PutEntry { key, value });
    }

    pub fn stage_delete(&mut self, space: StorageSpace, key: Key) {
        self.group_mut(space.id).deletes.push(key);
    }

    pub fn extend(&mut self, other: StorageWriteSet) {
        // O(K_other), or O(G_other) when group ownership can be moved.
    }

    pub fn lower_into<W: BackendWrite>(
        self,
        write: &mut W,
    ) -> Result<StorageWriteSetStats, BackendError> {
        for group in self.groups {
            if !group.puts.is_empty() {
                write.put_many(group.space, PutBatch { entries: group.puts })?;
            }
            if !group.deletes.is_empty() {
                write.delete_many(group.space, &group.deletes)?;
            }
        }
        Ok(StorageWriteSetStats::default())
    }
}
```

High-level commit path:

```rust
pub fn commit_storage_write_set<B: Backend>(
    backend: &B,
    write_set: StorageWriteSet,
    opts: WriteOptions,
) -> Result<CommitResult, BackendError> {
    let mut write = backend.begin_write(opts)?;
    let storage_stats = write_set.lower_into(&mut write)?;
    let commit = write.commit()?;
    Ok(commit.with_storage_stats(storage_stats))
}
```

`with_storage_stats` is illustrative: final API shape can either extend
`CommitResult` or return a storage-level commit result that wraps backend stats.

### Duplicate-Key Semantics

A `StorageWriteSet` is a set of final mutations, not an ordered script.

For v0:

```text
A sealed StorageWriteSet must contain at most one final mutation for a given
(SpaceId, Key).

Conflicting duplicate keys are invalid at the storage_v2 boundary.

Domain stores must canonicalize local overwrites before staging.
```

This keeps lowering cheap and avoids backend-specific behavior for put/delete
ordering within one write set.

Optional debug validation:

```text
seal/validate:
  O(K) expected with a hash set
```

## Read Scopes

Multiple domain stores often need to share the same coherent read view. Storage
v2 should provide read scopes over a backend read transaction.

Sketch:

```rust
pub struct StorageReadScope<R> {
    read: R,
}

pub struct ScopedStorageReader<R> {
    read: R,
}
```

A read scope is useful when one high-level operation needs tracked_state,
commit_store, and payload_store reads to agree on one backend snapshot.

For reads, storage_v2 can stay lighter than the write path. Domain-store hot
paths may use storage_v2 helpers without routing every operation through a heavy
context object. But if a high-level operation needs coherent reads across
domain stores, those reads must share one `StorageReadScope` instead of opening
independent backend read views.

## Read Adapter Helpers

`backend_v2` intentionally exposes a small physical read API. `storage_v2`
provides the Lix-friendly read shapes above it.

Point reads:

```text
domain store requests M keys, possibly with duplicates
storage_v2 may dedupe to U unique keys
backend_v2 get_many reads the unique/requested batch
storage_v2 reconstructs caller-order slots, duplicate slots, and missing slots
```

Target:

```text
backend point I/O:
  O(U) native batch, not O(M) independent physical calls

storage reconstruction:
  O(M + U) time
  O(U) memory
```

Prefix reads:

```text
prefix -> [prefix, next_prefix) KeyRange
all-0xff prefix -> [prefix, unbounded)
empty prefix -> whole space
```

Native prefix scan is a backend extension. Generic correctness comes from range
lowering.

Storage cursors:

```text
planned, not implemented yet

public cursor token binds:
  read scope / snapshot identity
  space
  range or prefix
  projection
  direction, when reverse fallback is used
  predicate/residual set
  last emitted key
```

A storage cursor must be valid only for the same storage read scope unless the
backend exposes a long-lived snapshot/cursor extension. A last-key alone is not
a public cursor; it is only the physical resume point inside a validated storage
cursor.

If storage exposes an exact "no cursor means no more eligible rows" contract, it
must perform lookahead or buffering after residual filtering. Otherwise the
public contract must allow an extra empty-page read.

## Capability-Aware Lowering

Domain stores express the desired physical access shape:

```text
point batch
range scan
prefix scan
projection
optional physical predicates
limits
cursor
```

`storage_v2` may provide helpers for lowering these into backend calls and
interpreting support metadata from extension APIs.

Rules:

```text
Exact support:
  storage/domain store can trust the backend result for that feature

Inexact support:
  domain store must apply residual filtering or verification

Unsupported:
  storage/domain store falls back or rejects the high-level operation
```

Limit correctness remains important:

```text
If any eligibility-affecting predicate is inexact, final user limits belong
above residual filtering.
```

This means storage must keep scanning backend pages until it has enough rows
after residual filtering or proves end-of-range. Backend `limit_rows` is a page
hint whenever predicates/projections are not exact for final eligibility.

For writes, storage_v2 is mandatory. For reads, storage_v2 helpers are
encouraged; direct backend reads are allowed only when they preserve read scope,
batching, projection policy, and fallback stats.

## Envelope Helpers

`backend_v2` supports opaque `FullValue` and logical `KeyOnly` as core
projections. Envelope slices such as `Header`, `Refs`, `HeaderAndRefs`, and
`Payload` are optional backend extensions.

Storage v2 may provide generic helpers for envelope mechanics only if they are
domain-neutral:

```text
encode/decode stable envelope frame
split header/refs/payload
map requested storage projection to core or extension backend projection
verify returned core or extension projected-value shape
```

Domain-specific header fields and refs remain owned by the domain store.

Storage-level scan helpers must require an explicit storage projection. Payload
bytes are not read unless the projection requires payload. If storage falls back
from `Header`, `Refs`, or `HeaderAndRefs` to `FullValue`, that fallback must be
recorded in stats.

For operations whose contract is "no payload physical I/O," storage must require
native envelope projection or reject the operation. Projection fallback preserves
correctness, but it changes the physical I/O cost and must be reported.

This section describes planned envelope/projection hardening. The current
storage_v2 baseline exposes only backend core projections (`FullValue` and
`KeyOnly`) and has no envelope fallback API yet.

## Complexity Contract

Notation:

```text
K = total staged mutations
G = touched backend groups, usually distinct (SpaceId, operation)
M = point keys requested
Q = rows emitted by a scan or touched by a scan/delete fallback
P = payload bytes read/written
S = backend segments/files/objects touched
```

Write-set staging:

```text
stage_put/stage_delete:
  O(1) amortized per mutation with O(1) group lookup
  O(G) with tiny Vec group lookup, acceptable only while G is bounded/small

total staging memory:
  O(K)
```

Write-set merge:

```text
O(K_other) total
or O(G_other) when group ownership can be moved directly
```

Write-set lower:

```text
O(K + G)

backend write calls:
  O(G), not O(K)
  at most one put_many and one delete_many per touched space

atomic commit:
  one BackendWrite commit boundary

overall write path:
  O(K + G + backend_commit_cost)
```

Domain-store writes:

```text
must stage into StorageWriteSet for engine-visible commits
must not call backend_v2 commit independently
```

Read scope:

```text
one coherent backend read view shared across domain stores when a high-level
operation needs cross-domain consistency
```

Read-side targets:

```text
point batch:
  O(U) backend batch for U unique keys plus O(M + U) caller-order
  reconstruction for M requested keys

prefix/range scan:
  O(log_B N + Q) for tree/ordered-backend shaped implementations

storage cursor resume:
  O(1) cursor validation/construction plus backend range resume cost

payload hydrate:
  O(P + S), only when requested

residual filtering:
  O(candidate rows decoded for residual fields)
```

Projection fallback:

```text
lack of envelope projection may increase decoded/read bytes
storage_v2 must record that fallback in stats
```

Delete-range fallback:

```text
native:
  backend-native delete_range when exact and supported

fallback:
  O(Q) scan plus delete_many batches
  one commit boundary
```

Exact fallback safety:

```text
scan-and-delete is exact only when one of these holds:
  storage has single-writer/exclusive access for the affected space/range
  backend has native conflict detection/preconditions that bind the scanned
    range to commit
  the high-level operation explicitly means "delete keys observed in this
    snapshot"

otherwise:
  storage must reject exact delete_range or require a native delete-range
  backend extension
```

This is the main place where moving behavior upward can silently become a
correctness bug. A separate read snapshot followed by a write can miss keys
inserted into the range by a concurrent writer.

Precondition fallback follows the same matrix: storage may emulate only under
single-writer/exclusive conditions, or when the backend can atomically bind the
check to commit. Concurrent backends without native preconditions must not get
silent check-then-write semantics.

The clean write rule:

```text
Domain stores own what to write.
StorageWriteSet owns aggregating all writes.
storage_v2 owns lowering to put_many/delete_many.
backend_v2 owns atomic persistence.
```

## Stats

Storage v2 stats should make the complexity contract observable.

Implemented read-shape stats:

```rust
pub struct StorageReadStats {
    pub requested_keys: u64,
    pub unique_backend_keys: u64,
    pub backend_calls: u64,
    pub prefix_lowered: u64,
}

pub struct StorageReadResult<T> {
    pub value: T,
    pub stats: StorageReadStats,
}
```

The no-stats read helpers remain available. The `_with_stats` variants expose
the same operation result together with shape counters for tests, benchmarks,
and future workload accounting.

Implemented write-set stats:

```rust
pub struct StorageWriteSetStats {
    pub staged_puts: u64,
    pub staged_deletes: u64,
    pub touched_spaces: u64,
    pub put_batches: u64,
    pub delete_batches: u64,
    pub backend_calls: u64,
    pub written_bytes: u64,
}
```

Planned fallback stats:

```rust
pub enum FallbackKind {
    ProjectionFallbackToFullValue,
    DeleteRangeScanAndDelete,
    PredicateResidualFilter,
    ReverseScanForwardBuffer,
    CallerOrderReorder,
    PrefixLoweredToRange,
    StorageCursorLookahead,
}
```

Future fallback stats should answer questions such as:

```text
Did this scan hydrate payload bytes?
Did this query fall back from header/refs projection to FullValue?
Was delete_range native or scan-and-delete fallback?
```

## Storage Adapter Tests

Backend conformance proves backend correctness. Storage adapter tests should
prove storage_v2 preserves batching and complexity boundaries.

The current code has an internal storage conformance runner over
`ConformanceBackend`, plus focused counting-backend unit tests for write-set
batching, read-shape stats, and failure behavior.

Implemented storage_v2 test themes:

```text
write_set_batches_by_space:
  K puts across G spaces lowers to G put_many calls and one commit

caller_order_reconstruction:
  backend returns found entries for unique keys; storage reconstructs requested
  slots, duplicate keys, and duplicate missing keys

read_shape_stats:
  point reads report requested keys, unique backend keys, and backend calls;
  range/prefix scans report backend calls and prefix lowering

prefix_lowering:
  empty prefix, normal prefix, and all-0xff prefix lower to correct ranges

read_scope_pinning:
  one StorageReadScope keeps observing its opened backend read view across later
  commits

named_space_validation:
  same SpaceId with different StorageSpace names is rejected before opening a
  backend write

write_lifecycle_failures:
  duplicate/conflicting write-set validation happens before begin_write; lower
  failures roll back once; commit failures are reported without pretending
  success
```

Planned optimization-hardening test themes:

```text
cross_domain_write_aggregation:
  tracked_state, commit_store, json_store, and visibility-style staged writes
  lower through one BackendWrite and one commit

no_direct_write_bypass_for_engine_commits:
  high-level commits stage StorageWriteSet mutations instead of calling
  backend.begin_write independently from domain stores

delete_range_fallback:
  when native delete_range is unavailable, storage does scan_range plus
  delete_many batches with one commit boundary only under the safety matrix

projection_fallback_accounting:
  missing HeaderAndRefs support may read FullValue, but stats record fallback

payload_hydration_guard:
  operations that should inspect only headers/refs report zero payload bytes

storage_cursor_scope:
  public cursors reject changed read scope, space, range/prefix, projection, and
  predicate identity before re-entering backend

residual_limit_correctness:
  final user limits are applied after residual filtering, not to raw backend
  pages when predicates are not exact
```

## Suggested Initial File Structure

Keep the first scaffold small:

```text
packages/engine/src/storage_v2/
  mod.rs
  context.rs
  spaces.rs
  write_set.rs
  read_scope.rs
  reader.rs
  point.rs
  scan.rs
  stats.rs
  conformance.rs
```

Planned files once the optimization extensions land:

```text
packages/engine/src/storage_v2/
  cursor.rs
  lowering.rs
  envelope.rs
```

Purpose:

```text
mod.rs:
  module wiring and exports

context.rs:
  generic wrapper around a backend_v2 Backend

spaces.rs:
  StorageSpace type and registration/declaration helpers

write_set.rs:
  domain-neutral staged puts/deletes and backend lowering

read_scope.rs:
  shared read transaction/scope helpers

reader.rs:
  StorageReader trait over a shared read scope

point.rs:
  caller-order point reconstruction and requested-key dedupe

scan.rs:
  prefix lowering and scan helper mechanics

conformance.rs:
  internal baseline storage adapter conformance tests

cursor.rs:
  storage cursor token encoding, scope validation, and last-key resume state

lowering.rs:
  helpers for prefix lowering, caller-order reconstruction, projections,
  predicates, support reports, and fallback decisions

envelope.rs:
  optional domain-neutral envelope frame helpers

stats.rs:
  storage-level fallback/cost accounting
```

Do not add domain-specific key modules here. For example:

```text
wrong:
  storage_v2::keys::tracked_by_file(...)

right:
  tracked_state::codec::encode_by_file_key(...)
  tracked_state::storage_v2::stage_by_file_root(...)
```

## Public API Boundary

`backend_v2` can become public and later be re-exported by `rs-sdk` for backend
authors.

`storage_v2` should remain internal unless Lix intentionally exposes an
advanced "bring your own domain store" API.

Recommended visibility during incubation:

```rust
pub mod backend_v2;
pub(crate) mod storage_v2;
```

## Open Questions

```text
Should StorageContext be generic over B: Backend or object-safe over dyn Backend?

Should space declarations live centrally or next to each domain store?

How much envelope support should be generic vs domain-store-owned?

Should conformance eventually include storage_v2 adapter tests, separate from
backend_v2 backend conformance?

For read hot paths, should domain stores call backend_v2 directly or always go
through storage_v2 helpers?
```

Default answers for now:

```text
Use generic B: Backend first.
Keep domain spaces next to domain stores, with central registration only.
Keep envelope helpers minimal.
Backend conformance belongs to backend_v2 first; storage_v2 gets adapter tests
for batching and complexity boundaries.
For writes, domain stores must stage through StorageWriteSet. For reads, let
domain stores use storage_v2 helpers, but do not force a heavy abstraction until
repeated patterns emerge.
```
