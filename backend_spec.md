# Lix Backend API Spec v0

The stable core is:

```text
An ordered byte-key entry backend with coherent read views, batched point
access, paged range/prefix scans, and atomic batched writes.
```

Everything else is additive capability: envelope slicing, reverse scans, range
delete, preconditions, predicate pushdown, object/segment pruning, byte-bounded
pages, long-lived cursors, parallel scan partitions, and native idempotent
commits.

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
  byte values or envelope-sliceable byte values
  get_many
  scan_range / scan_prefix
  put_many / delete_many
  begin_read / begin_write
  capabilities

Generic storage adapter:
  write sets
  read/write scopes
  namespace/space registration
  batching helpers
  capability-aware lowering

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
preserving the important goals: explicit access shape, explicit projection,
honest pushdown reporting, paged reads, late hydration, batching, snapshots, and
atomic writes.

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
get_many
scan_range
scan_prefix, via the default range lowering or a native implementation
put_many
delete_many
commit
rollback
```

Required semantics:

```text
keys:
  bytes, ordered lexicographically within a space

values:
  opaque bytes; FullValue and KeyOnly projections are core

read transaction:
  coherent read view

write transaction:
  atomic mutation unit; reads through the write handle observe staged mutations

range scans:
  forward, row-bounded, paged

point reads:
  caller-order slots preserve duplicates and missing keys

batching:
  first-class, not syntactic sugar over loops

capabilities:
  lack of capability changes cost, not correctness
```

### Optional Performance Capabilities

Optional capabilities include:

```text
header-only scan
refs-only scan
payload-only read
reverse scan
delete_range
atomic precondition registration
idempotent commit
exact predicate pushdown
inexact segment/object pruning
parallel scan partitions
byte-bounded pages
long-lived cursors
```

This is the "Ordered KV + Value Envelope Slices" direction: keep the backend
simple, but let it avoid payload read/decode when it can return key/header/ref
slices directly.

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
  get_many     -> transaction get calls
  scan_range   -> transaction get_range
  scan_prefix  -> prefix range
  put_many     -> set
  delete_many  -> clear
  commit       -> commit

RocksDB:
  get_many     -> MultiGet
  scan_range   -> iterator seek + next
  scan_prefix  -> iterator seek prefix + next until prefix ends
  put_many     -> WriteBatch
  delete_many  -> WriteBatch delete
  commit       -> DB::write / Transaction::Commit

SQLite:
  get_many     -> SELECT ... WHERE key IN (...)
  scan_range   -> WHERE space = ? AND key >= ? AND key < ? ORDER BY key LIMIT ?
  put_many     -> transaction + batched INSERT/UPDATE
  delete_many  -> transaction + batched DELETE
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
pub enum StoredValue {
    FullValue(Bytes),

    /// Optional shape for backends that store envelope columns/slices natively.
    Envelope {
        header: Bytes,
        refs: Bytes,
        payload: Bytes,
    },
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
        todo!("storage/backend helper computes [prefix, next_prefix) bounds")
    }
}

#[derive(Clone, Debug)]
pub struct Cursor(pub Bytes);
```

Read entries may contain projected values. Write entries never do: `put_many`
accepts only `StoredValue`, so a backend is never asked to persist `KeyOnly`,
`Header`, `Refs`, or another partial projection by accident.

`SpaceId` is a physical namespace. It is not a Lix table name. Storage can map:

```text
tracked_state.root    -> SpaceId(1)
tracked_state.by_file -> SpaceId(2)
commit_membership     -> SpaceId(3)
json_payloads         -> SpaceId(4)
blob_payloads         -> SpaceId(5)
```

The backend does not know what these mean.

## Read API

```rust
pub trait BackendRead {
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<GetManyResult, BackendError>;

    fn scan_range(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<ScanPage, BackendError>;

    fn scan_prefix(
        &self,
        space: SpaceId,
        prefix: Prefix,
        opts: ScanOptions<'_>,
    ) -> Result<ScanPage, BackendError> {
        self.scan_range(space, prefix.to_range()?, opts)
    }

    fn close(self) -> Result<(), BackendError>
    where
        Self: Sized,
    {
        Ok(())
    }
}
```

### Get Options

```rust
#[derive(Clone, Copy, Debug)]
pub struct GetOptions<'a> {
    pub projection: ValueProjection,
    pub order: PointOrder,
    pub preserve_duplicates: bool,
    pub predicates: &'a [BackendPredicate],
}

impl Default for GetOptions<'_> {
    fn default() -> Self {
        Self {
            projection: ValueProjection::FullValue,
            order: PointOrder::Caller,
            preserve_duplicates: true,
            predicates: &[],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PointOrder {
    /// Return one slot per requested key, preserving duplicates and missing keys.
    Caller,

    /// Return entries in lexicographic key order. Duplicate requested keys may
    /// be coalesced unless preserve_duplicates is true.
    KeyAsc,

    /// Backend may return any order. Duplicate requested keys may be coalesced.
    Unordered,
}
```

`get_many` is required because point batching is a core backend primitive, not a
looping convenience.

`PointOrder::Caller` is the only point order that preserves requested position,
duplicates, and missing keys by default. `PointOrder::KeyAsc` and
`PointOrder::Unordered` may coalesce duplicate requested keys unless the caller
sets `preserve_duplicates`.

### Scan Options

```rust
#[derive(Clone, Copy, Debug)]
pub struct ScanOptions<'a> {
    pub projection: ValueProjection,
    pub direction: ScanDirection,
    pub limit_rows: Option<usize>,
    pub limit_bytes: Option<usize>,
    pub cursor: Option<&'a Cursor>,
    pub predicates: &'a [BackendPredicate],
}

impl Default for ScanOptions<'_> {
    fn default() -> Self {
        Self {
            projection: ValueProjection::FullValue,
            direction: ScanDirection::Forward,
            limit_rows: None,
            limit_bytes: None,
            cursor: None,
            predicates: &[],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Reverse,
}
```

Scans must be paged:

```rust
#[derive(Clone, Debug)]
pub struct ScanPage {
    pub entries: ReadBatch,
    pub next_cursor: Option<Cursor>,
    pub support: ReadSupport,
    pub stats: ReadStats,
}

#[derive(Clone, Debug)]
pub struct GetManyResult {
    pub entries: Vec<GetSlot>,
    pub support: ReadSupport,
    pub stats: ReadStats,
}

#[derive(Clone, Debug)]
pub struct GetSlot {
    /// Present for caller-order results and for ordered/unordered results that
    /// preserve duplicate request positions.
    pub requested_index: Option<usize>,
    pub key: Key,
    pub value: Option<ProjectedValue>,
}
```

If `direction = Reverse` and the backend lacks reverse scan support, the backend
must return `BackendError::Unsupported(Capability::ReverseScan)`. It must not
silently return forward order. Storage may explicitly fallback with forward scan
and bounded buffering only when that is safe for the high-level operation.

A cursor is valid only for the same backend instance, space, range or prefix,
direction, projection class, read transaction/snapshot, and predicate set.
Backends may encode stricter constraints. Resuming with a cursor outside its
scope must return `BackendError::InvalidCursor`.

## Projection / Envelope API

The required backend value is opaque bytes. The first major performance
capability is an optional stable envelope layout.

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueProjection {
    /// Core. Backend may internally read value bytes and discard them.
    KeyOnly,

    /// Optional envelope projection: return small storage-defined header bytes.
    Header,

    /// Optional envelope projection: return storage-defined refs bytes.
    Refs,

    /// Optional envelope projection: return header + refs, but not payload.
    HeaderAndRefs,

    /// Optional envelope projection: return payload bytes only.
    Payload,

    /// Core.
    FullValue,
}

#[derive(Clone, Debug)]
pub enum ProjectedValue {
    KeyOnly,
    Header(Bytes),
    Refs(Bytes),
    HeaderAndRefs {
        header: Bytes,
        refs: Bytes,
    },
    Payload(Bytes),
    FullValue(Bytes),
}
```

The Lix storage/domain-store layer defines the envelope. Backend only knows how
to slice it.

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

This is the biggest performance hook: Lix scans can return key/header/refs
without hydrating payload bytes.

## Write API

```rust
pub trait BackendWrite: BackendRead {
    fn put_many(
        &mut self,
        space: SpaceId,
        entries: PutBatch,
    ) -> Result<(), BackendError>;

    fn delete_many(
        &mut self,
        space: SpaceId,
        keys: &[Key],
    ) -> Result<(), BackendError>;

    fn delete_range(
        &mut self,
        space: SpaceId,
        range: KeyRange,
    ) -> Result<(), BackendError> {
        let _ = (space, range);
        Err(BackendError::Unsupported(Capability::DeleteRange))
    }

    fn require(
        &mut self,
        preconditions: &[Precondition],
    ) -> Result<PreconditionSupportReport, BackendError> {
        let _ = preconditions;
        Err(BackendError::Unsupported(Capability::Preconditions))
    }

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

Write handles implement `BackendRead` so storage can perform precondition
discovery, conflict validation, and fallback reads within the write transaction.
Reads through a write handle must observe that write transaction's staged
mutations.

Required write semantics:

```text
put_many/delete_many mutations staged in one WriteTxn are committed atomically.
Before commit, no other read transaction is required to observe them.
After successful commit, future read transactions observe them according to
durability/visibility rules.
rollback discards all staged mutations.
```

Optional write semantics:

```text
atomic precondition registration
idempotent_commit
delete_range
```

### Preconditions

Preconditions are optional at the backend level but important for
distributed/remote/concurrent backends.

```rust
#[derive(Clone, Debug)]
pub enum Precondition {
    KeyAbsent {
        space: SpaceId,
        key: Key,
    },
    KeyPresent {
        space: SpaceId,
        key: Key,
    },
    KeyValueHashEquals {
        space: SpaceId,
        key: Key,
        hash: [u8; 32],
    },
    RangeEmpty {
        space: SpaceId,
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
  register them with BackendWrite::require.

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
    /// Ordered byte keys, coherent read views, paged forward scans,
    /// caller-order get_many, readable writes with read-your-writes, and atomic
    /// write commit.
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
    pub header: bool,
    pub refs: bool,
    pub header_and_refs: bool,
    pub payload: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ScanCapabilities {
    /// Prefix scan is core via range lowering. This means the backend has a
    /// better native prefix path.
    pub native_prefix_scan: bool,

    /// Forward scan is core. Reverse is optional.
    pub reverse: bool,

    /// Row-bounded pages are core. Byte-bounded pages are optional.
    pub limit_bytes: bool,

    /// Caller-order get_many is core. These are optional alternate point-return
    /// modes.
    pub unordered_points: bool,
    pub key_ordered_points: bool,

    /// Cursor continuation within the same read transaction is core. This
    /// means cursors can survive transaction/session boundaries.
    pub long_lived_cursors: bool,

    pub parallel_partitions: bool,
}

#[derive(Clone, Debug, Default)]
pub struct WriteCapabilities {
    /// put_many/delete_many/commit/rollback are core.
    pub delete_range: bool,

    /// Atomic precondition registration. Per-item support is still reported by
    /// require(...).
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

## Pushdown Path

Do not make a generic SQL-like `ReadPlan` the backend core. Add pushdown through
`GetOptions.predicates` / `ScanOptions.predicates` and support reporting.

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
    pub requested: ValueProjection,
    pub returned: ValueProjection,
    pub support: Support,
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
    PageHintOnly,
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

LimitSupport::PageHintOnly:
  backend may use limit for batching, but storage owns final limit.

LimitSupport::NotApplied:
  backend ignored limit.
```

The limit rule matters. If an inexact predicate prunes candidates and the
backend applies a final limit before residual filtering, storage can under-return
entries.

## Stats

Stats are part of the API because they make fallback costs visible.

```rust
#[derive(Clone, Debug, Default)]
pub struct ReadStats {
    pub scanned_entries: u64,
    pub emitted_entries: u64,
    pub skipped_by_backend: u64,
    pub decoded_bytes: u64,
    pub payload_bytes: u64,
    pub backend_calls: u64,
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
    Projection(ValueProjection),
    ReverseScan,
    DeleteRange,
    Preconditions,
    CompareAndSet,
    RangeEmpty,
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
    /// Let backend choose.
    #[default]
    Default,

    /// Durable before commit returns.
    Durable,

    /// May be buffered; only legal for tests or explicit relaxed mode.
    Relaxed,
}
```

`idempotency_key` matters for remote/distributed backends where commit success
can be ambiguous.

## Zero-Cost Abstraction Path

### Core Operations Are Native-Shaped

Do not make every backend implement this as the core:

```rust
read(ReadPlan) -> ReadPage
```

Make the core look like native backend calls:

```text
get_many
scan_range
scan_prefix
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

### Keep Pushdown As Slices

Use:

```rust
predicates: &'a [BackendPredicate]
```

Do not use:

```rust
Vec<Box<dyn Predicate>>
```

For the common path:

```rust
predicates: &[]
```

there is no expression tree, allocation, or dynamic dispatch.

### Return Support Per Result

Backend support reporting is metadata:

```rust
ReadSupport {
    projection,
    predicates,
    order,
    limit,
}
```

The Lix storage/domain-store layer uses that to decide residual work. No backend
optimizer is required.

## Storage-Side Lowering Examples

### Exact Tracked Row

The domain store wants:

```text
commit_id -> tracked projection -> root_ref
```

Backend call:

```rust
let read = backend.begin_read(ReadOptions {
    snapshot: Some(snapshot),
    consistency: ReadConsistency::Snapshot,
})?;

let result = read.get_many(
    TRACKED_ROOT_SPACE,
    &[state_row_key],
    GetOptions {
        projection: ValueProjection::Refs,
        order: PointOrder::Caller,
        preserve_duplicates: true,
        predicates: &[],
    },
)?;
```

### Schema / File Scan

The domain store wants:

```text
prefix scan
deleted = false
key/header/refs only
no payload hydration
```

Backend call:

```rust
let deleted_false = BackendPredicate {
    id: PredicateId(1),
    expr: PredicateExpr::Header(HeaderPredicate::IsDeleted(false)),
};

let page = read.scan_prefix(
    TRACKED_BY_FILE_SPACE,
    Prefix { bytes: schema_file_prefix },
    ScanOptions {
        projection: ValueProjection::HeaderAndRefs,
        direction: ScanDirection::Forward,
        limit_rows: Some(2048),
        limit_bytes: Some(1 << 20),
        cursor: cursor.as_ref(),
        predicates: &[deleted_false],
    },
)?;
```

The Lix storage/domain-store layer inspects support:

```rust
for pushed in &page.support.predicates {
    match pushed.support {
        Support::Exact => {
            // Do not re-evaluate this predicate.
        }
        Support::Inexact | Support::Unsupported => {
            // Apply residual filter in storage.
        }
    }
}
```

### Commit Write

The Lix storage/domain-store layer owns logical publication order:

```text
1. write truth/payload objects
2. write index/projection rows
3. write visibility/ref rows last
4. atomically commit
```

Backend sees only ordered byte mutations:

```rust
let mut write = backend.begin_write(WriteOptions {
    durability: Durability::Durable,
    idempotency_key: Some(commit_id_bytes),
    ..Default::default()
})?;

write.put_many(PAYLOAD_SPACE, payload_entries)?;
write.put_many(INDEX_SPACE, index_entries)?;
write.put_many(TRACKED_ROOT_SPACE, root_entries)?;
write.put_many(VISIBILITY_SPACE, visibility_entries)?;

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
paged scan
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
get_many
scan_range
scan_prefix, via the default range lowering or a native implementation
put_many
delete_many
commit
rollback
```

Required guarantees:

```text
ordered keys
coherent read view
caller-order get_many with duplicate and missing-key preservation
forward row-bounded paged scans
cursor continuation within the same read view
atomic write commit
FullValue and KeyOnly projections
read-your-writes through BackendWrite
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
byte-bounded pages
long-lived cursors
parallel partitions
object/segment pruning
native delete_range
stats-driven fallback reports
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
  baseline.rs
  projection.rs
  scan.rs
  write.rs
  pushdown.rs
```

`baseline.rs` contains the required tests every backend must pass before
capabilities matter. It validates the v0 invariants: ordered byte keys,
coherent reads, caller-order `get_many`, forward row-bounded scans,
same-read-view cursor continuation, atomic writes, rollback, read-your-writes,
space isolation, and `FullValue`/`KeyOnly`.

The other conformance modules are capability-gated:

```text
projection.rs -> envelope slices
scan.rs       -> native prefix, reverse, byte limits, long-lived cursors
write.rs      -> delete_range, preconditions, idempotent commit
pushdown.rs   -> exact/inexact/unsupported pushdown reporting
```

The conformance runner should expose a function-first API:

```rust
run_backend_conformance(&factory)
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
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<GetManyResult, BackendError>;

    fn scan_range(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<ScanPage, BackendError>;

    fn scan_prefix(
        &self,
        space: SpaceId,
        prefix: Prefix,
        opts: ScanOptions<'_>,
    ) -> Result<ScanPage, BackendError> {
        self.scan_range(space, prefix.to_range()?, opts)
    }
}

pub trait BackendWrite: BackendRead {
    fn put_many(
        &mut self,
        space: SpaceId,
        entries: PutBatch,
    ) -> Result<(), BackendError>;

    fn delete_many(
        &mut self,
        space: SpaceId,
        keys: &[Key],
    ) -> Result<(), BackendError>;

    fn delete_range(
        &mut self,
        space: SpaceId,
        range: KeyRange,
    ) -> Result<(), BackendError> {
        let _ = (space, range);
        Err(BackendError::Unsupported(Capability::DeleteRange))
    }

    fn require(
        &mut self,
        preconditions: &[Precondition],
    ) -> Result<PreconditionSupportReport, BackendError> {
        let _ = preconditions;
        Err(BackendError::Unsupported(Capability::Preconditions))
    }

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
