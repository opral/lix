# Lix Storage v2 Architecture Spec

`storage_v2` is the internal Lix storage runtime over `backend_v2`.

It is not the public persistence plugin API. Most users should bring their own
backend. `storage_v2` exists so Lix domain stores can share transactions,
write batching, spaces, capability-aware lowering, fallback accounting, and
other domain-neutral storage mechanics.

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
  namespace/space registration
  batching helpers
  capability-aware lowering
  fallback accounting

Backend: backend_v2
  ordered byte keys
  byte/envelope values
  get_many
  scan_range / scan_prefix
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
write transaction wrapper
read scopes shared by multiple domain stores
namespace/space registration
capability-aware lowering helpers
fallback stats
cursor handling policy at the generic adapter boundary
shared projection/envelope helpers, only when domain-neutral
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
provide a small registry or declaration mechanism so spaces are stable and easy
to audit.

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

Space names are for diagnostics and registration. Backends see only `SpaceId`.

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
interpreting support metadata.

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

For writes, storage_v2 is mandatory. For reads, storage_v2 helpers are
encouraged; direct backend reads are allowed only when they preserve read scope,
batching, projection policy, and fallback stats.

## Envelope Helpers

`backend_v2` supports `FullValue` and `KeyOnly` as core projections, and optional
envelope slices such as `Header`, `Refs`, `HeaderAndRefs`, and `Payload`.

Storage v2 may provide generic helpers for envelope mechanics only if they are
domain-neutral:

```text
encode/decode stable envelope frame
split header/refs/payload
map requested storage projection to backend ValueProjection
verify returned ProjectedValue shape
```

Domain-specific header fields and refs remain owned by the domain store.

Storage-level scan helpers must require an explicit storage projection. Payload
bytes are not read unless the projection requires payload. If storage falls back
from `Header`, `Refs`, or `HeaderAndRefs` to `FullValue`, that fallback must be
recorded in stats.

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
  O(M) expected via backend get_many

prefix/range scan:
  O(log_B N + Q) for tree/ordered-backend shaped implementations

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

The clean write rule:

```text
Domain stores own what to write.
StorageWriteSet owns aggregating all writes.
storage_v2 owns lowering to put_many/delete_many.
backend_v2 owns atomic persistence.
```

## Stats

Storage v2 stats should make the complexity contract observable.

Write-set stats:

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

Fallback stats:

```rust
pub enum FallbackKind {
    ProjectionFallbackToFullValue,
    DeleteRangeScanAndDelete,
    PredicateResidualFilter,
    ReverseScanForwardBuffer,
    CallerOrderReorder,
}
```

Stats should answer questions such as:

```text
Did this scan hydrate payload bytes?
Did this query fall back from header/refs projection to FullValue?
How many backend calls did this write set lower into?
Was delete_range native or scan-and-delete fallback?
```

## Storage Adapter Tests

Backend conformance proves backend correctness. Storage adapter tests should
prove storage_v2 preserves batching and complexity boundaries.

Use a counting backend for these tests.

Required storage_v2 test themes:

```text
write_set_batches_by_space:
  K puts across G spaces lowers to G put_many calls and one commit

cross_domain_write_aggregation:
  tracked_state, commit_store, json_store, and visibility-style staged writes
  lower through one BackendWrite and one commit

no_direct_write_bypass_for_engine_commits:
  high-level commits stage StorageWriteSet mutations instead of calling
  backend.begin_write independently from domain stores

delete_range_fallback:
  when native delete_range is unavailable, storage does scan_range plus
  delete_many batches with one commit boundary

projection_fallback_accounting:
  missing HeaderAndRefs support may read FullValue, but stats record fallback

payload_hydration_guard:
  operations that should inspect only headers/refs report zero payload bytes
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
  lowering.rs
  envelope.rs
  stats.rs
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

lowering.rs:
  helpers for projections, predicates, support reports, and fallback decisions

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
