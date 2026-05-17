# Lix Backend API Spec v0

The stable core is:

```text
An ordered byte-key entry backend with coherent read views, batched point
access, forward row-bounded range visits/cursors, and atomic batched writes.
```

Everything else is either `storage_v2` adapter behavior or an additive backend
extension: prefix lowering, caller-order reconstruction, public storage cursor
tokens, projection fallback, residual filtering, preconditions, predicate
pushdown, envelope slicing, object/segment pruning, byte-bounded chunks,
long-lived cursors, parallel scan partitions, and native idempotent commits.

The public extension point is backend, not "bring your own storage".

Terminology:

```text
Backend:
  public physical plugin
  ordered byte-key persistence

Generic storage adapter:
  internal Lix storage support
  write sets, read/write scopes, batching helpers, capability-aware lowering

Domain stores:
  tracked_state, commit_store, json/blob stores, indexes, visibility
  Lix-aware physical schemas over the generic storage adapter
```

The important cut:

```text
Backend:
  ordered byte keys
  opaque byte values
visit_many
with_scan_cursor / cursor.visit_next
put_many / delete_many
begin_read / begin_write
capabilities

Generic storage adapter:
  write sets
  read/write scopes
  namespace/space registration
  batching helpers
  prefix lowering
  caller-order point reconstruction
  storage cursor tokens
  capability-aware lowering
  projection fallback
  residual filtering loops
  precondition fallback when safe
  fallback stats

Domain stores:
  tracked_state roots/chunks/by-file index
  commit store
  JSON/blob payload refs
  indexes
  visibility rules
  key encoding
  envelope decoding
  residual filtering
  hydration
  fallback accounting
```

This keeps the backend close to FoundationDB/RocksDB-style primitives while
preserving the important goals: explicit access shape, chunked reads, batching,
snapshots, atomic writes, and Big-O-visible fallback costs in `storage_v2`.

Most users should bring their own backend. A backend author implements the
physical substrate and should not need to know Lix commit visibility,
tracked-state indexes, JSON refs, branch diff logic, schema/file scans, or
payload hydration.

"Bring your own storage" is a different, larger extension point. It means
replacing a Lix-aware domain store such as tracked_state, commit_store,
payload_store, index_store, or visibility_store. That is not the default plugin
story for backend authors.

## Core Principles

### Required Correctness Core

Every v0 backend must support:

```text
begin_read
begin_write
visit_many
with_scan_cursor
put_many
delete_many
delete_range
commit
rollback
```

Required semantics:

```text
keys:
  bytes, ordered lexicographically within a space
  ordering is raw unsigned byte order, not text collation or locale order

values:
  opaque bytes; FullValue is core, and KeyOnly is a core logical read shape

read transaction:
  coherent read view
  once opened, the view remains pinned across any number of later commits

write transaction:
  atomic mutation unit; write handles are mutation sinks, not read handles

range scans:
  forward, row-bounded, 
  continuation resumes strictly after the last emitted key

point reads:
  batched reads over requested keys; storage_v2 may reconstruct caller-order,
  duplicate, and missing-key slots above the raw backend result

batching:
  first-class, not syntactic sugar over loops

capabilities:
  lack of capability changes cost, or causes storage_v2 to reject an operation
  whose exact semantics cannot be safely emulated
```

### Optional Performance Capabilities

Optional capabilities include:

```text
header-only scan
refs-only scan
payload-only read
native prefix scan
reverse scan
atomic precondition registration
idempotent commit
exact predicate pushdown
inexact segment/object pruning
parallel scan partitions
byte-bounded chunks
long-lived cursors
```

This is the "boring ordered KV first" direction: keep v0 tiny, then let
storage_v2 use extensions to avoid payload read/decode, push predicates down,
or resume scans with backend-native tokens when a backend can do that cheaply
and correctly.

## Rust API

Use `begin_read` and `begin_write`.

No `begin_session`. No required `prepare_read`. No required generic `ReadPlan`
at the backend boundary.

```rust
pub trait Backend {
    type Read<'a>: BackendRead + 'a
    where
        Self: 'a;

    type Write<'a>: BackendWrite + 'a
    where
        Self: 'a;

    fn capabilities(&self) -> BackendCapabilities;

    fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError>;

    fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError>;
}
```

Backend author mapping:

```text
FoundationDB:
  visit_many   -> transaction get calls
  scan cursor  -> transaction get_range stream
  put_many     -> set
  delete_many  -> clear
  delete_range -> clear range
  commit       -> commit

RocksDB:
  visit_many   -> MultiGet
  scan cursor  -> iterator seek + next
  put_many     -> WriteBatch
  delete_many  -> WriteBatch delete
  delete_range -> WriteBatch delete range or exact range delete loop
  commit       -> DB::write / Transaction::Commit

SQLite:
  visit_many   -> SELECT ... WHERE key IN (...)
  scan cursor  -> prepared SELECT ... WHERE key range ORDER BY key
  put_many     -> transaction + batched INSERT/UPDATE
  delete_many  -> transaction + batched DELETE
  delete_range -> transaction + indexed DELETE WHERE key range
```

## Core Types

Use byte keys and byte values. `bytes::Bytes` lets backends return owned,
shared, or sliced buffers without forcing repeated copies.

```rust
use bytes::Bytes;
use std::ops::Bound;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SpaceId(pub u32);

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key(pub Bytes);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Value(pub Bytes);

#[derive(Clone, Debug)]
pub struct ReadEntry {
    pub key: Key,
    pub value: ProjectedValue,
}

#[derive(Clone, Debug, Default)]
pub struct ReadBatch {
    pub entries: Vec<ReadEntry>,
}

#[derive(Clone, Debug)]
pub struct PutEntry {
    pub key: Key,
    pub value: StoredValue,
}

#[derive(Clone, Debug, Default)]
pub struct PutBatch {
    pub entries: Vec<PutEntry>,
}

#[derive(Clone, Debug)]
pub struct StoredValue {
    pub bytes: Bytes,
}

#[derive(Clone, Debug)]
pub struct KeyRange {
    pub lower: Bound<Key>,
    pub upper: Bound<Key>,
}

#[derive(Clone, Debug)]
pub struct Prefix {
    pub bytes: Bytes,
}

impl Prefix {
    pub fn to_range(&self) -> Result<KeyRange, BackendError> {
        let lower = Key(self.bytes.clone());
        let mut upper = self.bytes.to_vec();

        while let Some(last) = upper.last_mut() {
            if *last == u8::MAX {
                upper.pop();
            } else {
                *last += 1;
                return Ok(KeyRange {
                    lower: Bound::Included(lower),
                    upper: Bound::Excluded(Key(Bytes::from(upper))),
                });
            }
        }

        Ok(KeyRange {
            lower: Bound::Included(lower),
            upper: Bound::Unbounded,
        })
    }
}
```

V0 writes store opaque values only. Envelope-aware storage and native envelope
projection are extension paths above this opaque byte value.

`SpaceId` is a storage-level logical namespace marker. It is not passed to the
backend v0 API. `storage_v2` maps each `StorageSpace` to a physical byte prefix,
currently:

```text
physical_key = big_endian_u32(SpaceId) || logical_key
```

Storage/domain code can map:

```text
tracked_state.root    -> SpaceId(1)
tracked_state.by_file -> SpaceId(2)
commit_membership     -> SpaceId(3)
json_payloads         -> SpaceId(4)
blob_payloads         -> SpaceId(5)
```

The backend does not know what these mean. It sees one ordered physical byte-key
space.

## Read API

```rust
pub trait BackendRead {
    type ScanCursor<'cursor>: BackendScanCursor;

    fn visit_many<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized;

    fn with_scan_cursor<T, F>(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
        f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut Self::ScanCursor<'_>) -> Result<T, BackendError>;

    fn close(self) -> Result<(), BackendError>
    where
        Self: Sized,
    {
        Ok(())
    }
}

pub trait PointVisitor {
    fn visit(
        &mut self,
        index: usize,
        key: &Key,
        value: Option<ProjectedValueRef<'_>>,
    ) -> Result<(), BackendError>;
}

pub trait ScanVisitor {
    fn visit(
        &mut self,
        key: KeyRef<'_>,
        value: ProjectedValueRef<'_>,
    ) -> Result<(), BackendError>;
}

pub trait BackendScanCursor {
    fn visit_next<V>(
        &mut self,
        limit_rows: usize,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized;
}

pub fn visit_range<R>(
    read: &R,
    range: KeyRange,
    opts: ScanOptions<'_>,
    visitor: &mut V,
) -> Result<ScanResult, BackendError>
where
    R: BackendRead,
    V: ScanVisitor + ?Sized,
{
    let limit_rows = opts.limit_rows;
    read.with_scan_cursor(range, opts, |cursor| cursor.visit_next(limit_rows, visitor))
}
```

The scan cursor API is callback-scoped, but it is still monomorphic in the
emitted-row loop. This keeps statement-backed engines such as SQLite and
transaction-backed engines such as redb safe without self-referential cursor
objects, while still letting in-memory and RocksDB keep a native iterator and a
generic scan visitor. There is no separate "fast cursor" API.

### Get Options

```rust
#[derive(Clone, Copy, Debug)]
pub struct GetOptions<'a> {
    pub projection: CoreProjection,
    /// Reserved for extension traits. Core v0 does not accept predicates.
    pub _reserved: std::marker::PhantomData<&'a ()>,
}

impl Default for GetOptions<'_> {
    fn default() -> Self {
        Self {
            projection: CoreProjection::FullValue,
            _reserved: std::marker::PhantomData,
        }
    }
}
```

`visit_many` is required because point batching is a core backend primitive, not
a looping convenience.

V0 `visit_many` calls the visitor once per physical key in the slice passed to
the backend, in that exact order. Missing keys are passed as `None`.

The visitor-shaped contract keeps backend values borrowed and avoids forcing
every backend to allocate a materialized point result. Callers already know the
requested logical keys, and `storage_v2` owns duplicate preservation,
caller-order reconstruction, and missing-key slots. Storage normally dedupes to
unique physical keys before calling the backend. Backends must still implement
`visit_many` as a batched point operation over the requested physical key set,
not as a required loop of independent physical reads.

### Scan Options

```rust
#[derive(Clone, Copy, Debug)]
pub struct ScanOptions<'a> {
    pub projection: CoreProjection,
    pub limit_rows: usize,
    /// Resume strictly after this key within the same range and read view.
    pub resume_after: Option<&'a Key>,
}

impl Default for ScanOptions<'_> {
    fn default() -> Self {
        Self {
            projection: CoreProjection::FullValue,
            limit_rows: 1024,
            resume_after: None,
        }
    }
}
```

Backend scans are visitor-first and row-bounded:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyRef<'a>(pub &'a [u8]);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScanResult {
    pub emitted: usize,
    pub has_more: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectedValueRef<'a> {
    KeyOnly,
    FullValue(&'a [u8]),
}

#[derive(Clone, Debug)]
pub struct GetManyResult {
    /// One value slot per key passed to a materializing get_many helper,
    /// in caller order.
    /// None means the requested key was missing.
    pub values: Vec<Option<ProjectedValue>>,
}
```

`GetManyResult` is a materialized helper result above the backend core. It is
useful for conformance tests and simple callers, but backend authors implement
`visit_many`, not owned `get_many`.

V0 scans are forward only. Reverse scans, byte limits, predicates, and
long-lived opaque cursors are extension paths. Storage-level helpers may normalize
`limit_rows = 0`, wrap public cursor tokens, validate cursor scope, and perform
lookahead/buffering when they want an exact public "no cursor means no more
eligible rows" promise.

`KeyRef` and `ProjectedValueRef` are borrowed, ephemeral row references. They
are valid only during the visitor call. A backend may point them at an iterator
buffer, SQLite row value, RocksDB slice, redb table value, or in-memory map
entry. Callers that need to retain rows must materialize them above the backend
boundary.

Materialized scan chunks are a storage adapter convenience:

```rust
pub struct ScanChunk {
    pub entries: ReadBatch,
    pub has_more: bool,
}
```

Cursorized scans are core because deep small-chunk drains should not be forced
to re-open the same physical range once per chunk. Backend cursors are
callback-scoped so statement-backed backends can keep native temporary iterators
alive without self-referential structs. `visit_range` is a storage/backend helper
outside the backend trait: it opens a callback-scoped cursor and calls
`visit_next(opts.limit_rows, visitor)` for callers that want a one-shot scan.

The unit of continuation is a scan chunk: `cursor.visit_next(limit_rows,
visitor)` returns at most `limit_rows` rows from the same opened scan. A backend
cursor is bound to its read view, range, projection, and initial resume point,
and it may only be used inside the `with_scan_cursor` callback. It is not a
public, long-lived storage cursor token.

## Projection / Envelope Extensions

The required backend value is opaque bytes. V0 has only core logical projection
shapes:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoreProjection {
    /// Core. Backend may internally read value bytes and discard them.
    KeyOnly,

    /// Core.
    FullValue,
}

#[derive(Clone, Debug)]
pub enum ProjectedValue {
    KeyOnly,
    FullValue(Bytes),
}
```

Envelope slicing is an optional extension. The Lix storage/domain-store layer
defines the envelope; backends may opt into returning slices of it.
The current Rust scaffold exposes only the core `ProjectedValue` variants;
envelope request/result types land with the envelope extension API.

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvelopeProjection {
    /// Optional envelope projection: return small storage-defined header bytes.
    Header,

    /// Optional envelope projection: return storage-defined refs bytes.
    Refs,

    /// Optional envelope projection: return header + refs, but not payload.
    HeaderAndRefs,

    /// Optional envelope projection: return payload bytes only.
    Payload,

}

#[derive(Clone, Debug)]
pub enum EnvelopeProjectedValue {
    Header(Bytes),
    Refs(Bytes),
    HeaderAndRefs {
        header: Bytes,
        refs: Bytes,
    },
    Payload(Bytes),
}
```

Recommended physical envelope:

```text
value =
  magic/version
  header_offset/header_len
  refs_offset/refs_len
  payload_offset/payload_len
  header bytes
  refs bytes
  payload bytes or payload ref
```

Backend mappings:

```text
SQLite:
  space, key, header, refs, payload columns

RocksDB:
  byte value with fixed-size envelope prefix

Object backend:
  header/refs in manifest, payload in objects
```

This is the biggest read performance hook: Lix scans can return key/header/refs
without hydrating payload bytes. If a backend lacks native envelope projection,
`storage_v2` may fall back to `FullValue` and decode the envelope itself, but
hot operations that require "no payload physical I/O" must require the extension
or reject the operation.

## Write API

```rust
pub trait BackendWrite {
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError>;

    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError>;

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError>;

    fn commit(self) -> Result<CommitResult, BackendError>
    where
        Self: Sized;

    fn rollback(self) -> Result<(), BackendError>
    where
        Self: Sized;
}
```

`commit(self)` and `rollback(self)` consume the write handle. This prevents use
after commit.

Write handles are mutation sinks. They do not implement `BackendRead` and are
not required to support read-your-writes. If storage needs validation reads,
conflict discovery, or fallback hydration, it uses an explicit `BackendRead`
view and/or a storage-layer staged-mutation overlay.

Required write semantics:

```text
put_many/delete_many/delete_range mutations staged in one WriteTxn are committed atomically.
Before commit, no other read transaction is required to observe them.
After successful commit, future read transactions observe them according to
durability/visibility rules.
rollback discards all staged mutations.
delete_range removes exactly the keys in the requested raw byte-key range,
including keys staged earlier in the same write transaction.
```

Optional write semantics:

```text
atomic precondition registration
idempotent_commit
```

Optional write behavior is intended to live behind extension traits. These
traits are part of the extension design, not the v0 core trait surface:

```rust
pub trait BackendPreconditionExt: BackendWrite {
    fn require(
        &mut self,
        preconditions: &[Precondition],
    ) -> Result<PreconditionSupportReport, BackendError>;
}
```

The current `backend_v2` scaffold may expose only the capability metadata and
pending conformance entries for these extensions until their positive
conformance suites are implemented.

### Preconditions

Preconditions are optional at the backend level but important for
distributed/remote/concurrent backends.

```rust
#[derive(Clone, Debug)]
pub enum Precondition {
    KeyAbsent {
        key: Key,
    },
    KeyPresent {
        key: Key,
    },
    KeyValueHashEquals {
        key: Key,
        hash: [u8; 32],
    },
    RangeEmpty {
        range: KeyRange,
    },
    VersionEquals {
        ref_key: Key,
        expected: Bytes,
    },
}

#[derive(Clone, Debug)]
pub struct PreconditionSupportReport {
    pub items: Vec<PreconditionItemSupport>,
}

#[derive(Clone, Debug)]
pub struct PreconditionItemSupport {
    pub index: usize,
    pub support: Support,
}
```

Storage fallback rule:

```text
If backend has exact preconditions:
  register them with BackendPreconditionExt::require.

Preconditions registered with require():
  must hold at commit.

If backend can enforce only at commit:
  require may report Exact, and commit may fail with PreconditionFailed.

If backend cannot bind the preconditions atomically to commit:
  require must return Unsupported.

If backend lacks preconditions but storage serializes writes:
  storage may check under the exclusive write path.

If backend lacks preconditions and allows external concurrent writers:
  storage must not emulate silently.
```

`delete_range` is part of the v0 write core so storage does not need a
check-then-delete fallback for exact range deletion. A backend may implement it
internally as scan-and-delete only if that scan is bound to the same atomic
write transaction and cannot miss concurrent range inserts under the backend's
advertised write-concurrency profile.

## Capability Model

Capabilities are additive. They are not required for correctness unless the
storage layer explicitly opts into a backend profile that requires them.

```rust
#[derive(Clone, Debug)]
pub struct BackendCapabilities {
    pub profile: BackendProfile,
    pub projection: ProjectionCapabilities,
    pub scan: ScanCapabilities,
    pub write: WriteCapabilities,
    pub pushdown: PushdownCapabilities,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendProfile {
    /// Ordered byte keys, coherent read views, chunked forward scans,
    /// batched visit_many, and atomic write commit.
    V0 {
        write_concurrency: WriteConcurrency,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteConcurrency {
    SingleWriter,
    ConcurrentWithConflictDetection,
    ConcurrentWithoutConflictDetection,
}

#[derive(Clone, Debug, Default)]
pub struct ProjectionCapabilities {
    /// FullValue is core. KeyOnly is core as a logical output shape.
    /// These fields describe optional envelope projection extensions.
    pub header: bool,
    pub refs: bool,
    pub header_and_refs: bool,
    pub payload: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ScanCapabilities {
    /// Prefix scan is storage_v2 range lowering by default. This means the
    /// backend has a better native prefix path.
    pub native_prefix_scan: bool,

    /// Forward scan is core. Reverse is optional.
    pub reverse: bool,

    /// Row-bounded forward chunks are core. Byte-bounded chunks are optional.
    pub limit_bytes: bool,

    /// Core v0 scan continuation is key-resume. This means the backend exposes
    /// native opaque cursors that can survive transaction/session boundaries or
    /// avoid expensive reseeks.
    pub long_lived_cursors: bool,

    pub parallel_partitions: bool,
}

#[derive(Clone, Debug, Default)]
pub struct WriteCapabilities {
    /// Atomic precondition registration via BackendPreconditionExt.
    pub preconditions: bool,

    /// Useful for remote/distributed backends where commit success can be
    /// ambiguous.
    pub idempotent_commit: bool,
}

#[derive(Clone, Debug, Default)]
pub struct PushdownCapabilities {
    pub key: PredicateSupportLevel,
    pub header: PredicateSupportLevel,
    pub refs: PredicateSupportLevel,
    pub object_pruning: PredicateSupportLevel,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PredicateSupportLevel {
    #[default]
    None,
    Inexact,
    Exact,
}
```

Capability rule:

```text
capability false:
  storage falls back or rejects that high-level operation as too expensive or
  unsupported

capability true:
  backend may still report per-operation Unsupported for a specific projection,
  predicate, or layout

per-operation support wins over global capability
```

Object APIs are not part of v0 capabilities because the v0 trait does not expose
object methods. If Lix adds object-native backends later, model them as an
extension trait with its own capabilities:

```rust
pub trait ObjectBackendExt {
    fn append_object(
        &mut self,
        namespace: SpaceId,
        object_id: Bytes,
        bytes: Bytes,
        checksum: [u8; 32],
    ) -> Result<(), BackendError>;

    fn read_object_range(
        &self,
        namespace: SpaceId,
        object_id: Bytes,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes, BackendError>;
}
```

## Pushdown Extension Path

Do not make a generic SQL-like `ReadPlan` the backend core. Core v0 does not
accept predicates. Pushdown is a planned extension path with its own result
types and support reporting:

```rust
pub trait BackendPushdownExt {
    fn visit_many_pushdown<V>(
        &self,
        keys: &[Key],
        opts: PushdownGetOptions<'_>,
        visitor: &mut V,
    ) -> Result<PushdownPointResult, BackendError>
    where
        V: PointVisitor + ?Sized;

    fn scan_range_pushdown(
        &self,
        range: KeyRange,
        opts: PushdownScanOptions<'_>,
    ) -> Result<PushdownScanChunk, BackendError>;
}
```

The current `backend_v2` scaffold may expose the predicate/support data model
before this extension trait is wired into the public Rust API.

Only exact pushdown removes a predicate from residual evaluation.

### Predicate Type

Keep this intentionally small. No arbitrary expression engine in v0.

```rust
#[derive(Clone, Debug)]
pub struct BackendPredicate {
    pub id: PredicateId,
    pub expr: PredicateExpr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PredicateId(pub u32);

#[derive(Clone, Debug)]
pub enum PredicateExpr {
    Key(KeyPredicate),
    Header(HeaderPredicate),
    Refs(RefsPredicate),
}

#[derive(Clone, Debug)]
pub enum KeyPredicate {
    Eq(Key),
    StartsWith(Prefix),
    Range(KeyRange),
}

#[derive(Clone, Debug)]
pub enum HeaderPredicate {
    FieldEq {
        field: HeaderFieldId,
        value: ScalarValue,
    },
    FieldIn {
        field: HeaderFieldId,
        values: Vec<ScalarValue>,
    },
    FieldRange {
        field: HeaderFieldId,
        lower: Bound<ScalarValue>,
        upper: Bound<ScalarValue>,
    },
    IsDeleted(bool),
}

#[derive(Clone, Debug)]
pub enum RefsPredicate {
    HasRef {
        kind: RefKind,
        value: Bytes,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HeaderFieldId(pub u16);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RefKind(pub u16);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScalarValue {
    Bool(bool),
    U64(u64),
    I64(i64),
    Bytes(Bytes),
}
```

The Lix storage/domain-store layer maps Lix predicates to this small physical
predicate language:

```text
Lix deleted = false
  -> HeaderPredicate::IsDeleted(false)

schema_id = X
  -> usually key prefix/range, not predicate

file_id = Y
  -> usually key prefix/range, not predicate

payload_ref = P
  -> RefsPredicate::HasRef(...)
```

### Pushdown Support Result

The support structures below belong to pushdown/envelope extension results.
Core v0 `visit_many` and `with_scan_cursor` / `cursor.visit_next` do not return
support metadata.

```rust
#[derive(Clone, Debug)]
pub struct ReadSupport {
    pub projection: ProjectionSupport,
    pub predicates: Vec<PredicatePushdown>,
    pub order: OrderSupport,
    pub limit: LimitSupport,
}

#[derive(Clone, Debug)]
pub struct ProjectionSupport {
    pub requested: ProjectionRequest,
    pub returned: ProjectionRequest,
    pub support: Support,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionRequest {
    Core(CoreProjection),
    Envelope(EnvelopeProjection),
}

#[derive(Clone, Debug)]
pub struct PredicatePushdown {
    pub id: PredicateId,
    pub support: Support,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Support {
    Exact,
    Inexact,
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderSupport {
    Exact,
    ChangedToKeyAsc,
    Unordered,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LimitSupport {
    Final,
    ChunkHintOnly,
    NotApplied,
}
```

Rules:

```text
Exact:
  backend guarantees emitted entries satisfy the predicate.
  storage does not re-evaluate that predicate.

Inexact:
  backend may have pruned candidates, but cannot prove correctness.
  storage must re-evaluate the predicate.

Unsupported:
  backend ignored the predicate.
  storage must evaluate it.

LimitSupport::Final:
  only allowed if all eligibility-affecting predicates are Exact or
  Unsupported-not-applied.

LimitSupport::ChunkHintOnly:
  backend may use limit for batching, but storage owns final limit.

LimitSupport::NotApplied:
  backend ignored limit.
```

The limit rule matters. If an inexact predicate prunes candidates and the
backend applies a final limit before residual filtering, storage can under-return
entries.

## Stats

Stats are layered. Backend stats are optional diagnostics about physical calls
and bytes. Storage stats are the normative place for fallback/cost accounting:
projection fallback, residual filtering, caller-order reconstruction, reverse
buffering, payload hydration, delete-range lowering, and write-set lowering.

```rust
#[derive(Clone, Debug, Default)]
pub struct BackendIoStats {
    pub backend_calls: u64,
    pub bytes_read: Option<u64>,
    pub bytes_written: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct WriteStats {
    pub put_entries: u64,
    pub deleted_entries: u64,
    pub deleted_ranges: u64,
    pub written_bytes: u64,
    pub backend_calls: u64,
}

#[derive(Clone, Debug)]
pub struct CommitResult {
    pub commit_id: Option<Bytes>,
    pub stats: WriteStats,
}
```

## Error Model

```rust
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("unsupported capability: {0:?}")]
    Unsupported(Capability),

    #[error("invalid key encoding")]
    InvalidKey,

    #[error("cursor is invalid for this backend/read view")]
    InvalidCursor,

    #[error("read transaction is no longer valid")]
    ReadExpired,

    #[error("write conflict")]
    WriteConflict,

    #[error("precondition failed: {0:?}")]
    PreconditionFailed(Vec<PreconditionFailure>),

    #[error("durability failure")]
    Durability,

    #[error("backend corruption: {0}")]
    Corruption(String),

    #[error("io error: {0}")]
    Io(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Capability {
    Projection(ProjectionRequest),
    KeyOrderedPoints,
    UnorderedPoints,
    ReverseScan,
    DeleteRange,
    Preconditions,
    IdempotentCommit,
    PredicatePushdown,
    ParallelPartitions,
}

#[derive(Clone, Debug)]
pub struct PreconditionFailure {
    pub index: usize,
}
```

## Read / Write Options

```rust
#[derive(Clone, Debug, Default)]
pub struct ReadOptions {
    pub snapshot: Option<SnapshotRef>,
    pub consistency: ReadConsistency,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadConsistency {
    #[default]
    Snapshot,

    /// Backend may serve a stale local view if storage allows it.
    StaleOk,

    /// Backend should use latest committed view.
    Latest,
}

#[derive(Clone, Debug, Default)]
pub struct WriteOptions {
    pub base_snapshot: Option<SnapshotRef>,
    pub durability: Durability,
    pub idempotency_key: Option<Bytes>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRef(pub Bytes);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Durability {
    /// Durable before commit returns for non-ephemeral backends.
    #[default]
    Durable,
}
```

`idempotency_key` matters for remote/distributed backends where commit success
can be ambiguous.

For the v0 baseline, conformance only relies on the default option values:

```rust
ReadOptions {
    snapshot: None,
    consistency: ReadConsistency::Snapshot,
}

WriteOptions {
    base_snapshot: None,
    durability: Durability::Durable,
    idempotency_key: None,
}
```

Non-default `SnapshotRef`, `ReadConsistency::StaleOk`, `ReadConsistency::Latest`,
`base_snapshot`, and `idempotency_key` are reserved extension points until the
API exposes a normative way to obtain snapshot refs and defines
conflict/idempotency semantics. v0 has one durability policy: commits are
durable for non-ephemeral backends before `commit` returns. Additional policies
such as relaxed or backend-default durability may be added later only after they
are specified and measured against real backends.

## Zero-Cost Abstraction Path

### Core Operations Are Native-Shaped

Do not make every backend implement this as the core:

```rust
read(ReadPlan) -> ReadChunk
```

Make the core look like native backend calls:

```text
visit_many
with_scan_cursor / cursor.visit_next
put_many
delete_many
```

The backend does not receive Lix semantics.

### Use Generic Associated Types

The trait uses associated transaction types:

```rust
type Read<'a>: BackendRead + 'a;
type Write<'a>: BackendWrite + 'a;
```

This lets storage be generic over `B: Backend` and avoid hot-path trait objects.

For plugin-style dynamic backends, add an object-safe adapter later. Do not make
that the performance-critical core.

### Keep Pushdown In Extensions

Core v0 does not accept predicates. Pushdown extensions should still use slices:

```rust
predicates: &'a [BackendPredicate]
```

Do not use:

```rust
Vec<Box<dyn Predicate>>
```

This keeps extension calls allocation-free without putting predicate rejection
work on every v0 backend.

### Return Support Per Extension Result

Backend support reporting is extension metadata:

```rust
ReadSupport {
    projection,
    predicates,
    order,
    limit,
}
```

The Lix storage/domain-store layer uses that to decide residual work. Core v0
reads either return exact raw results for the core operation or fail.

## Storage-Side Lowering Examples

### Exact Tracked Row

The domain store wants:

```text
commit_id -> tracked projection -> root_ref
```

Backend call, using core full-value reads:

```rust
let read = backend.begin_read(ReadOptions {
    consistency: ReadConsistency::Snapshot,
    ..Default::default()
})?;

let result = backend_v2::get_many(
    &read,
    &[TRACKED_ROOT_SPACE.encode_key(&state_row_key)],
    GetOptions::default(),
)?;

Storage/domain code decodes the value or uses `BackendEnvelopeExt` when that
extension is available.
```

### Schema / File Scan

The domain store wants:

```text
prefix scan
deleted = false
key/header/refs only
no payload hydration
```

Storage lowering:

```rust
let deleted_false = BackendPredicate {
    id: PredicateId(1),
    expr: PredicateExpr::Header(HeaderPredicate::IsDeleted(false)),
};

let range = Prefix { bytes: schema_file_prefix }.to_range()?;
let physical_range = TRACKED_BY_FILE_SPACE.encode_range(range, storage_cursor.last_key());
let mut chunk_entries = Vec::new();
let result = backend_v2::visit_range(
    read,
    physical_range,
    ScanOptions {
        projection: CoreProjection::FullValue,
        limit_rows: 2048,
        resume_after: None,
    },
    &mut |key, value| {
        chunk_entries.push((key.to_owned_key(), value.to_owned()));
        Ok(())
    },
)?;
```

Storage/domain code then applies residual filtering and projection fallback.
When pushdown/envelope extensions are available and safe, storage uses their
extension results and support metadata to decide which predicates still need
residual evaluation.

### Commit Write

The Lix storage/domain-store layer owns logical publication order:

```text
1. write truth/payload objects
2. write index/projection rows
3. write visibility/ref rows last
4. atomically commit
```

Storage prefixes logical-space keys before lowering. Backend sees only ordered
byte mutations:

```rust
let mut write = backend.begin_write(WriteOptions::default())?;

write.put_many(PutBatch { entries: payload_entries })?;
write.put_many(PutBatch { entries: index_entries })?;
write.put_many(PutBatch { entries: root_entries })?;
write.put_many(PutBatch { entries: visibility_entries })?;

write.commit()?;
```

No `PublishCommitVisibility` backend method. That is a storage concept.

## Extension Boundaries

### Bring Your Own Backend

This is the expected public extension point. A custom backend answers:

```text
How are ordered byte keys persisted?
How are range scans implemented?
How are writes committed atomically?
Is this SQLite, RocksDB, FoundationDB, object storage, memory, or remote KV?
```

A backend implements `Backend`, `BackendRead`, and `BackendWrite`. It should not
implement Lix commit visibility, tracked-state indexes, JSON refs, branch diff
logic, schema/file scans, or payload hydration. Those are storage/domain-store
concerns.

### Generic Storage Adapter

The generic storage adapter is an internal Lix layer over `backend_v2`. It may
own:

```text
write batching
read/write scope wrappers
namespace/space registration
capability-aware lowering
fallback stats
shared envelope/projection helpers, if they are domain-neutral
```

It must not own tracked-state-specific key builders such as
`tracked_by_file_key(...)`. Those belong to `tracked_state`, just as the current
engine keeps tracked-state namespaces, tree encoding, chunks, delta packs, and
the by-file index under `tracked_state`.

### Bring Your Own Domain Store

This is an advanced/internal extension point. A domain store answers:

```text
How does tracked_state encode a root?
How does commit visibility work?
Which keys form an index?
How are payload refs interpreted?
Which rows are truth rows vs derived rows?
How does Lix recover from missing derived index rows?
```

Replacing a domain store changes Lix-aware physical schema or subsystem
semantics. It is much more invasive than bringing a backend and should not be
presented as the normal persistence plugin API.

## Not In v0 Core

Do not require these in the backend core:

```text
prepare_read / execute_read
general ReadPlan
named secondary index API
Lix table names
Lix visibility steps
general SQL predicates
cost-based optimizer
automatic index selection
columnar execution
adaptive replanning
distributed transactions
```

The backend API should not know:

```text
tracked_state
schema/file scan
commit body
branch diff
merge planning
index rebuild
GC mark
payload hydration policy
```

The Lix storage adapter and domain stores map those to:

```text
space
key prefix/range
projection
optional physical predicate
chunked scan
point batch
atomic mutation batch
```

Rule of thumb:

```text
If code mentions SpaceId, Key, cursor, projection, write batch, transaction, or
fallback stats generically, it can belong in the generic storage adapter.

If code mentions tracked_state, commit, schema, file_id, entity_id, JSON pack,
branch visibility, or payload hydration policy, it belongs in a domain store or
higher engine layer, not in backend_v2.
```

## Versioned Capability Ladder

### v0: Correctness Core

```text
begin_read
begin_write
visit_many
with_scan_cursor / cursor.visit_next
put_many
delete_many
commit
rollback
```

Required guarantees:

```text
ordered keys
coherent read view
batched visit_many over requested keys
forward row-bounded chunked scans
key-resume continuation within the same read view
atomic write commit
FullValue and KeyOnly projections
```

### v1: Envelope Projection

```text
Header
Refs
HeaderAndRefs
Payload
```

This is the first performance extension to ship.

### v2: Write Preconditions

```text
KeyAbsent
KeyPresent
KeyValueHashEquals
RangeEmpty
VersionEquals
idempotency_key
```

This makes remote/distributed/concurrent backends safer.

### v3: Predicate Pushdown

```text
BackendPredicate
Support::{Exact, Inexact, Unsupported}
residual filtering in storage
limit correctness rules
```

### v4: Advanced Scan

```text
native prefix optimization
reverse scan
byte-bounded chunks
long-lived cursors
parallel partitions
object/segment pruning
backend diagnostics
```

## Backend Conformance Test Suite

The backend API should ship with an executable conformance suite. During the
backend_v2 incubation, keep it colocated with the API:

```text
packages/engine/src/backend_v2/conformance/
  mod.rs
  factory.rs
  runner.rs
  model.rs
  fixtures.rs
  persistence.rs
  baseline.rs
  projection.rs
  scan.rs
  write.rs
  pushdown.rs
```

`baseline.rs` contains the required tests every backend must pass before
capabilities matter. It validates the v0 invariants: raw lexicographic byte-key
ordering, opaque byte value preservation, coherent reads pinned across later
commits, batched `visit_many`, forward row-bounded scans, multi-chunk key-resume
draining without repeats/skips, atomic writes, rollback for
new/overwrite/delete mutations, and `FullValue`/`KeyOnly`.

The other conformance modules are capability-gated or profile-gated:

```text
persistence.rs -> non-ephemeral fixture reopen semantics
projection.rs  -> envelope slices
scan.rs        -> native prefix, reverse, byte limits, long-lived cursors
write.rs       -> preconditions, idempotent commit
pushdown.rs    -> exact/inexact/unsupported pushdown reporting
```

Storage-level conformance currently validates caller-order point reconstruction,
duplicate/missing slots, prefix lowering, read-scope pinning, named-space
validation, and write-set batching/lowering. It should grow next to cover public
cursor scope, projection fallback, residual filtering, delete-range helpers,
support/stat interpretation, and read-side stats.

The conformance runner should expose a function-first API:

```rust
run_backend_conformance(&factory)
```

Backend authors should normally only call that one function from their own
test:

```rust
#[test]
fn my_backend_passes_backend_v2_conformance() {
    run_backend_conformance(&MyBackendFactory).assert_no_failures();
}
```

Under the hood, the runner uses an explicit test lifecycle:

```text
BackendFactory -> BackendFixture -> Backend
```

`BackendFactory` creates a fresh isolated fixture for each conformance test.
`BackendFixture` owns the lifecycle identity for one physical test target: a
temp directory, database file, object-store prefix, remote namespace, or
in-memory shared state. `BackendFixture::open()` opens a `Backend` handle
against that same target.

This keeps the public testing API function-first while still allowing the
runner to test both normal runtime behavior and reopen behavior. Baseline tests
can simply create a fresh fixture and open one backend handle. Persistence tests
can open a backend, commit data, drop all handles, reopen the same fixture, and
verify the committed bytes are still present. Persistence tests run whenever
`BackendTestConfig::ephemeral` is `false`; they are skipped for ephemeral
fixtures.

```rust
pub trait BackendFactory {
    type Backend: Backend;
    type Fixture: BackendFixture<Backend = Self::Backend>;

    fn create_fixture(&self) -> Self::Fixture;

    fn config(&self) -> BackendTestConfig {
        BackendTestConfig::default()
    }
}

pub trait BackendFixture {
    type Backend: Backend;

    fn open(&self) -> Self::Backend;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendTestConfig {
    pub max_key_len: usize,
    pub max_value_len: usize,

    /// If true, committed data is not required to survive reopening the same
    /// fixture. Persistence/reopen tests are skipped.
    ///
    /// If false, committed data must survive dropping all backend handles and
    /// opening a new handle from the same fixture. This is the default.
    pub ephemeral: bool,

    pub supports_concurrent_writers: bool,
}
```

Example for a file-backed backend:

```rust
struct MyBackendFactory;

impl BackendFactory for MyBackendFactory {
    type Backend = MyBackend;
    type Fixture = MyBackendFixture;

    fn create_fixture(&self) -> Self::Fixture {
        MyBackendFixture::new_temp_file()
    }
}

struct MyBackendFixture {
    path: PathBuf,
}

impl BackendFixture for MyBackendFixture {
    type Backend = MyBackend;

    fn open(&self) -> Self::Backend {
        MyBackend::open(&self.path).unwrap()
    }
}
```

Later, rs-sdk can re-export the same conformance API for backend authors.

## Final Shape

```rust
pub trait Backend {
    type Read<'a>: BackendRead + 'a
    where
        Self: 'a;

    type Write<'a>: BackendWrite + 'a
    where
        Self: 'a;

    fn capabilities(&self) -> BackendCapabilities;

    fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError>;

    fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError>;
}

pub trait BackendRead {
    type ScanCursor<'cursor>: BackendScanCursor;

    fn visit_many<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized;

    fn with_scan_cursor<T, F>(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
        f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut Self::ScanCursor<'_>) -> Result<T, BackendError>;
}

pub trait BackendScanCursor {
    fn visit_next<V>(
        &mut self,
        limit_rows: usize,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized;
}

pub fn visit_range<R, V>(
    read: &R,
    range: KeyRange,
    opts: ScanOptions<'_>,
    visitor: &mut V,
) -> Result<ScanResult, BackendError>
where
    R: BackendRead,
    V: ScanVisitor + ?Sized,
{
    let limit_rows = opts.limit_rows;
    read.with_scan_cursor(range, opts, |cursor| cursor.visit_next(limit_rows, visitor))
}

pub trait BackendWrite {
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError>;

    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError>;

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError>;

    fn commit(self) -> Result<CommitResult, BackendError>
    where
        Self: Sized;

    fn rollback(self) -> Result<(), BackendError>
    where
        Self: Sized;
}
```

Clean cut:

```text
required:
  ordered byte-key backend

early performance:
  envelope projection

later performance:
  exact/inexact/unsupported pushdown

never required:
  backend-level Lix semantics
```

This gives backend authors a freely mappable core while leaving a clear path to
projection, late hydration, predicate pushdown truth, paging, batching,
snapshots, atomicity, and Big-O-visible fallback costs.
