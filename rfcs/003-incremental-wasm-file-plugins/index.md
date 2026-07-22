---
date: "2026-07-22"
status: research
---

# Incremental Wasm file plugins

## Decision

Replace the stateless whole-state plugin calls with one SDK-facing immutable
`Document` abstraction. A document receives either byte splices or merge-resolved
entity changes and returns an immutable successor plus the opposite kind of
delta. Immutable file versions stay behind a host-owned, range-readable
`Source`; the Wasm document normally retains only format-specific syntax and
identity indexes. The engine owns transaction acceptance, rollback, session
views, observation handles, deletion authority, revision checks, storage,
scheduling, and eviction.

The recommended author-facing shape is intentionally small:

```rust
pub trait FilePlugin {
    type Document: Document;

    // Initial import: bytes exist, durable semantic entities do not.
    fn open_file(
        &self,
        input: OpenFile<'_>,
    ) -> Result<FileTransition<Self::Document>, Error>;

    // Cold canonical shared-renderer recovery: durable entities exist,
    // plugin-backed bytes do not.
    fn open_entities(
        &self,
        input: OpenEntities<'_>,
    ) -> Result<EntityTransition<Self::Document>, Error>;
}

pub trait Document: Sized + 'static {
    fn file_changed(
        &self,
        input: FileUpdate<'_>,
    ) -> Result<FileTransition<Self>, Error>;

    fn entities_changed(
        &self,
        input: EntityUpdate<'_>,
    ) -> Result<EntityTransition<Self>, Error>;

}
```

`FileUpdate` contains before/after descriptors, base-relative sorted
non-overlapping byte splices, lazy before/after sources, and a retry-stable ID
allocator whose calls name the schema, composite-PK scope, and deterministic
ordinal. `FileTransition` contains the successor document and inline or bounded
paged semantic merge groups, so an initial import need not collect every
upsert in guest memory. `EntityUpdate` contains before/after descriptors, the
final merge-resolved entity deltas as one bounded stateful source, and a second
bounded stateful fallback containing the transaction-local prospective state
after those deltas, before commit.
`EntityTransition` contains the successor document and inline or paged byte
splices; large replacement bytes may remain behind a lazy output. The generated
SDK, not the author type, implements the WIT resource lifecycle; one
branch/file actor serializes access, so a guest `Document` need not satisfy host
`Send`/`Sync` constraints.

The descriptor contains path, media type, and the host-selected
content-addressed plugin generation. Descriptor changes are semantic inputs: a
rename-only transition is delivered even when the byte-splice list is empty.
Every host source read, entity/change page, ID allocation, edit page, and lazy
output read shares one aggregate top-level transition budget covering record
size, page size/count, total bytes, and a non-renewing deadline. Paging is a
bounded fallback, not a way to reset the five-second deadline per `next` call.

Both cold directions are mandatory because plugin-backed files do not retain a
durable raw blob. `open_file` parses a newly created raw file and emits its
first entity-group pages. `open_entities` streams the canonical durable entities of
one branch/file incarnation, renders a complete file from an empty base, and
constructs its shared renderer after restart or eviction. It is not a recovery
constructor for an arbitrary private session view: that view may contain exact
noncanonical bytes or proposals that never won the shared merge. A private
document, its exact byte source, and its exact semantic root therefore share
one bounded lease. Evicting any non-reconstructible part revokes its observation
handle and forces a reread. Cold work may be `O(document)`; the warm transition
methods may not silently fall back to that cost without instrumentation.

The evaluated facade's generic optional `snapshot`/`metadata` fields were too
permissive: one Candidate B CSV implementation preserved row IDs during a
reorder but failed to commit the new order, so a later render could not
reproduce the file. The selected semantic delta is therefore explicit:

```rust
pub enum EntityChange {
    Upsert {
        entity: Entity,       // complete schema entity, including order_key
        effect: ChangeEffect, // content or the typed format-only hint
    },
    Delete(EntityKey),
}

pub struct MergeGroup(pub Vec<EntityChange>);
pub struct EntityChanges(pub Vec<MergeGroup>);
pub enum EntityChangeOutput {
    Inline(EntityChanges),
    Paged(Box<dyn ChangePageReader>),
}
```

There is no partial upsert and no generic transport metadata escape hatch.
Schema-defined order, parentage, native references, and other render-critical
state live in the complete entity snapshot. Deletion is a separate variant.
Every entity key may appear at most once in a transition. Most changes are
singleton merge groups; coupled facts such as the two sides of an Excalidraw
binding can win or lose a conflict together without coupling unrelated edits
from the same file write. A group is never split across pages, and the host
validates duplicate keys across the complete drained cursor, not merely inside
each page.

A basic plugin may ignore locality. The following is deliberately pseudocode
for proposed SDK convenience helpers, not compileable methods on the checked
facade. `read_all_bounded`, `collect_pages_bounded`, `apply_and_read_all`, and
`replace_all` must remain charged to the same aggregate transition budget:

```text
open_file(input):
  bytes = sdk.read_all_bounded(input.file)
  return parse(input.descriptor, bytes, input.ids)

open_entities(input):
  entities = sdk.collect_pages_bounded(input.entities)
  document = from_entities(input.descriptor, entities)
  return (document, sdk.replace_all(empty_base, document.render()))

file_changed(update):
  next_bytes = sdk.apply_and_read_all(update.before, update.edits, update.after)
  return reparse(update.after_descriptor, next_bytes, update.ids)

entities_changed(update):
  next = apply_complete_changes(update.after_descriptor, update.changes)
  return (next, sdk.replace_all(update.before.len(), next.render_lazy()))
```

An optimized plugin consumes the same methods, but reads only the affected
ranges from `update.before`/`update.after`, reparses the grammatical closure of
each splice, and emits local byte patches. There is no separate “fast plugin
API.” Keeping complete bytes on the host is an implementation property of the
`Source` capability, not a second author model.

This is an RFC and executable research package, not a production API merge. The
production implementation is gated on the full-engine RocksDB and cached
SlateDB results defined below.

## Why change the API

The current contract sends every active entity and a complete blob to
`detect-changes`, then sends every active entity again to `render`:

```wit
detect-changes: func(
  state: list<entity-state>,
  file: file,
) -> result<list<detected-change>, plugin-error>;

render: func(
  state: list<entity-state>,
) -> result<list<u8>, plugin-error>;
```

For a one-property edit in a 50,000-property JSON document or a one-cell edit
in a 200,000-row CSV file, this makes the work proportional to all active
entities. It also converts the same state through storage rows, owned Rust
strings, nested Canonical ABI values, guest JSON values, and format-specific
projection structures.

The Wasm sandbox is not the limiting constraint. A byte-only Component Model
control is near the copy floor. The scaling problem is repeated rich
materialization and stateless reconstruction.

## Goals

- Keep every workspace plugin inside Wasm.
- Preserve stable `entity_pk` values across edits, moves, and reorderings.
- Make warm localized work proportional to the affected grammatical closure,
  not total file entities.
- Keep the ordinary client surface: SQL reads and writes complete blobs.
- Give plugin authors one comprehensible model with a full-reparse fallback.
- Make transaction abort, retry, crash, eviction, and concurrent session
  behavior correct without plugin-managed commits.
- Work for CSV, Markdown, JSON, Excalidraw, and text rather than standardizing a
  format-specific AST.
- Require a measured improvement greater than 20% before accepting a
  performance- or storage-motivated breaking change.

## Non-goals

- A generic AST or engine-owned identity matcher.
- CRDT APIs, client base commits, or direct entity editing requirements.
- App-level packing of unrelated durable KVs into ad-hoc storage pages.
- Treating P3 streams as a latency optimization by themselves.
- Guaranteeing bounded incremental parsing for malformed or globally scoped
  syntax. Plugins may expand an invalidation region or request a full resync.

## The semantic model

### One concept for authors, distinct roles in the engine

Plugin authors implement one `FilePlugin`/`Document` pair. Internally the engine
holds different document versions for different authority domains:

- One shared renderer document represents each current merged
  `(workspace, branch, path, file incarnation, plugin generation)` state.
- A bounded set of private documents represents exact byte/entity roots that a
  session demonstrably received or subsequently submitted on that same path.

Those versions may share immutable guest structures, but they are never one
mutable object. Applying a stale client's splice directly to the shared
renderer would reintroduce overwrite bugs.

Each acknowledgement-safe unique-file response carries an opaque observation
handle. Aggregates, transformed bytes, ambiguous joins, and broad scans do not.
The handle is unforgeably bound to the session, workspace, branch, path, file
incarnation, and plugin generation, and addresses exactly one immutable byte
source, semantic root, and private guest document. Only the SDK's later echo
proves that it received the response; creating or attempting to send the
response is not authority. The SDK does this without exposing commit state to
the application. Base/result hashes still validate splice reconstruction, but
a byte hash is not a root identity: byte-identical views can carry different
stable entity IDs after different histories. A hash alone must never select a
private root or grant authority. A server-side read attempt whose response may
have been lost on the network likewise grants no authority; possession and
validation of the session/path-bound handle is required.

There is no generic “safe upsert” fallback when that exact observation is
missing. For an ID-less format, parsing an entire submitted blob without its
identity root cannot distinguish updates from new entities or duplicate an
existing ID safely. If the path is absent, explicit creation may call
`open_file`; if the submitted result is byte-identical to the current canonical
source, the engine may return a no-op. Every other existing-file mutation with
a missing, expired, wrong-path, wrong-branch, or wrong-generation observation
fails closed with a retryable reread/`410` response before invoking the plugin.
An optional format-specific blind-import capability would need separate
identity proofs and is not part of v2. Ordinary applications still issue SQL
blob reads and writes; this observation token is hidden SDK/transport
provenance, not a `baseCommitId` or client-managed merge state.

The remote protocol, not plugin WIT, carries it:

```text
exact unique-file response -> { ordinary SQL result, opaque observation }
next SDK mutation          -> { ordinary SQL request, splice hashes, observation }
successful mutation        -> { ordinary SQL result, successor observation }
```

The server validates the token before constructing any guest source/document.
Lost responses yield no authority. Missing/expired, wrong-session,
wrong-workspace/branch/path/incarnation/generation, replayed-after-success, and
evicted handles return `410` without invoking the plugin; the SDK never silently
retries a stale write. Integration tests cover byte-identical/different-ID
roots, full-blob fallback, one token per uniquely identified file in a batch,
SSE/reconnect, and publication of the successor token only after commit. V3
currently carries splice hashes but not this root capability, so the ordinary
SQL *application* surface is unchanged while the SDK/protocol changes.

The engine calls `file_changed` only on the exact private version selected by
the observation handle. It validates authority for every member of every merge
group before conflict resolution: a delete or update of an existing key must
refer to that observed root, while a new key must be a validated native ID or
come from the operation-scoped allocator and be absent from current shared
state (except for an idempotent retry of the same operation). Native-ID
collisions with an unseen concurrent entity are rejected. If one member fails,
the entire transition is rejected; the engine never filters a group into a
different semantic operation.

Conflict resolution is group-level and deterministic. Each group receives one
retry-stable engine LWW rank from the operation order plus a host canonical
group key derived from its sorted member schema/PK keys; cursor page/list order
is never rank authority. The first validated proposal is retained for an
idempotent retry of the same operation.
Overlapping concurrent candidate groups are considered in descending rank; a
group is selected only when all of its keys are free, otherwise none of it is
selected.
The engine retains enough group provenance to recompute that frontier when a
later higher-ranked group displaces an earlier group, including restoring prior
values for the displaced group's non-overlapping keys. This makes the result
independent of arrival order and prevents a coupled Excalidraw binding from
being half-applied. Per-entity LWW without this group provenance is not a valid
implementation of `MergeGroup`.

The storage representation is unresolved production work, not a free metadata
field. It needs a versioned conflict frontier, a visibility/branch horizon for
GC, and a bounded policy for displaced values. Arrival-order, restart, branch,
and compaction suites are required. RocksDB and cached SlateDB acceptance runs
must include lookup cost plus WAL/live-byte amplification; unbounded retention
or more than 20% steady-state amplification without a separately selected >2x
latency win is rejected.

The engine then calls `entities_changed` on the branch's shared renderer with
only the final merge-resolved groups *before* durable commit. Only after the
prospective graph and byte patches validate does storage commit and atomically
publish the shared successor/source. The private successor is the client's
submitted view, not a merged view the client never received.

Host private semantic roots are structurally shared persistent maps, not full
row vectors. A received observation stores an `Arc`-like root in `O(1)`; a
sparse submission path-copies affected keys; authority is membership in the
handle-selected root. This host structure is required alongside guest
tree/source sharing or session fan-out would remain
`O(sessions × entities)`. Private roots are not durable branch state and cannot
be reconstructed by cold-opening the current shared entities. The bounded
observation entry therefore retains its exact source/root/document together;
eviction expires the handle and fails the next stale write closed.

### Immutable transitions make rollback ordinary

An accepted document is borrowed immutably. A plugin returns a new document.
The host retains both resources until storage commits:

1. Resolve and validate the observation handle, then invoke its exact private
   accepted document and receive `(successor, proposal)`.
2. Validate every proposal group as a unit and deterministically resolve the
   group-level LWW frontier into final groups.
3. Invoke the shared accepted renderer and validate its prospective successor
   and output patches.
4. Commit authoritative storage.
5. Publish the shared cache pointer/source and the next private
   source/root/document lease with non-failing pointer swaps; only then issue
   its observation handle.
6. On abort or failed validation, drop successors. If a guest call may have
   mutated an accepted private resource, revoke that observation; it cannot be
   cold-reopened from shared state.
7. On a shared-renderer trap or uncertain completion, evict that disposable
   resource and reopen the branch's prior authoritative root from storage.

If cache publication is unexpectedly unavailable after a successful commit,
the commit remains successful: evict derived resources and cold-open from the
new durable entity root. Do not issue a private observation unless its exact
source/root/document lease was installed; the client must reread. Never report
a durable commit as failed because a disposable cache could not be published.

No plugin-visible `prepare`, `accept`, `abort`, transaction ID, branch ID, or
commit ID is required. The safe SDK never gives ordinary authors mutable access
to the accepted document. A malicious Wasm guest can ignore that facade and
mutate its resource internally, so the host must not trust an accepted resource
after any call whose engine transaction did not commit. Guest state is always a
disposable cache, never rollback or commit authority.

### Stable identity remains format-owned

The API standardizes lifecycle and deltas, not matching policy. It supports:

- Native IDs already encoded in a file, such as Excalidraw element IDs.
- Plugin-generated IDs reconciled against syntax, such as CSV rows, text lines,
  and Markdown nodes.
- Structural identities, such as JSON object slots.

The host binds each ID allocator to operation identity, file incarnation, and
plugin generation. Each call supplies a schema key, composite-PK scope, and
deterministic ordinal; the result is the scope plus one opaque host component.
The same tuple returns the same ID on retries, distinct tuples cannot collide,
and identity never depends on call order. The guest may use a native file ID
when one exists, but it must not use time or unseeded randomness for retry
identity.

For ID-less formats, “stable” cannot mean recovering unknowable user intent.
Two byte-identical duplicate rows swapped in a CSV have no observable identity
signal. The contract instead requires that IDs never derive from mutable array
indices or byte offsets, preserves an existing ID whenever the format matcher
has an unambiguous correspondence, and makes ambiguous matching deterministic.
Native Excalidraw IDs are exact. JSON object slots and array items use opaque
IDs. Object slots are matched by stable parent ID plus decoded key, so a value
edit or key reorder preserves identity while a key rename is intentionally a
delete plus insert. Array-item IDs survive insertion and reorder.

The engine validates every tombstone, existing-key upsert, and newly allocated
identity as part of its complete merge group against the exact
observation-selected private identity root. Guest resources, byte hashes, and
derived caches never grant authority.

### Coordinate and atomicity rules

- All splices in one update use coordinates in the same previous byte string.
- Splices are sorted and non-overlapping. The SDK validates this before guest
  code runs.
- The engine verifies the before and after content hashes. Remote v3 already
  sends an exact prefix/delete/insert splice with SHA-256 validation; that edit
  provenance should survive parameter decoding. Hash validation proves bytes,
  while the separate observation handle selects the semantic root.
- Each change belongs to an explicit merge group. Singleton groups are the
  default; authority is validated for the whole group, and all members of a
  coupled group win or lose deterministic conflict resolution together.
  Transaction atomicity alone is insufficient under per-entity LWW.
- One entity key may occur only once across all groups in one transition;
  empty groups and upsert/delete duplicates are rejected.
- A renderer receives only final merge-resolved entity deltas, never the client's
  unmerged proposal.
- Full replacement is represented by one splice and remains a first-class
  fallback.

### File, branch, and plugin-generation lifecycle

The document API models content transitions inside one live file incarnation;
it does not overload empty bytes as file deletion or silently carry resources
across lifecycle boundaries:

- A path creation is serialized against path state, allocates a fresh file
  incarnation, and calls `open_file`. A zero-byte file is still a live file.
- Explicit whole-file deletion is an engine operation that requires a valid
  observation for the current incarnation. It competes with concurrent content
  writes at the file-incarnation boundary under one deterministic LWW rank,
  rather than asking a plugin to emit an unbounded list of tombstones. If the
  delete wins, the incarnation and all of its semantic entities are tombstoned
  atomically; if a content write wins, the file remains live.
- Recreating the same path after a winning delete allocates a new incarnation
  and calls `open_file`. Old observation handles, IDs scoped to the old
  incarnation, guest documents, and byte sources can never authorize or mutate
  the replacement.
- A rename requires an observation bound to the old path, serializes both path
  slots, preserves the file incarnation, and delivers distinct before/after
  descriptors even when bytes are unchanged. A successful move revokes
  old-path observations and issues only destination-bound successors;
  destination collisions are resolved explicitly at the file-lifecycle layer.
- There is exactly one shared renderer actor per
  `(workspace, branch, path, incarnation, plugin generation)`. Branch forks
  may structurally share immutable roots and sources, but their renderer
  pointers and serialization queues are distinct. Branch deletion drops its
  actor; branch merge applies the target branch's final resolved groups through
  its renderer rather than reusing another branch's mutable lifecycle.
- A plugin-generation upgrade first stops that branch/file actor and revokes
  every old-generation observation. A schema-compatible generation must
  `open_entities` from the durable root and validate its complete render before
  an atomic generation-pointer swap. A schema or identity change requires an
  explicit migration transaction; reparsing bytes with `open_file` and silently
  reallocating IDs is forbidden. Failure leaves the old generation
  authoritative.
- Warm `file_changed`/`entities_changed` requires the before/after descriptors
  to select the same plugin key and generation. If a rename or media-type
  change triggers reselection, the engine performs the stop/revoke/cold-open or
  identity-migration handoff above; one guest is never asked to transition into
  a different plugin.

These rules make restart, deletion, path reuse, branch isolation, and plugin
upgrade explicit engine state transitions without expanding the author-facing
`Document` interface.

## Candidate APIs tested

The research package evaluates the SDK facade, not generated WIT boilerplate.
All candidates keep execution in Wasm and use the same semantic fixtures.

### Candidate V1: stateless whole state

```rust
fn detect_changes(state: &[Entity], file: &[u8]) -> Result<Vec<Change>>;
fn render(state: &[Entity]) -> Result<Vec<u8>>;
```

This is the control. It has a small signature but requires full active-state
hydration, full parsing, and full rendering on every operation.

### Candidate A: persistent document, complete blobs

```rust
fn file_changed(&self, next_file: &[u8]) -> Result<(Self, Vec<Change>)>;
fn entities_changed(&self, changes: &[Change]) -> Result<(Self, Vec<u8>)>;
```

This isolates the value of persistence. It retains the author mental model but
still transfers and locates changes in complete blobs.

### Candidate B/B2: persistent document, splices and patches

This is the recommended surface shown in the decision. It is Candidate A plus
locality hints and lazy/full fallbacks in the same types. The B control retained
a complete file buffer in Wasm. B2 retains only the format index in Wasm and
reads immutable before/after byte ranges from the host. The author facade is
identical; B2 is the selected runtime/storage composition.

The benchmark implementation mutates one thread-local document/index in place;
it does not allocate an immutable successor, retain accepted/successor versions
through commit, exercise abort, or measure 1/8/32-session structural sharing.
Its numbers are a localized detection/source-access lower bound. Production
must measure an immutable relative-offset tree, path-copy/allocation, and
retained-session RSS/storage before the full-engine gate can pass.

The post-evaluation refinement keeps the sparse edit/source data flow, but
replaces v1-shaped optional change fields with
`Upsert(complete entity, typed effect) | Delete(entity key)`, adds explicit
`open_file`/`open_entities` cold directions, groups coupled merge facts, and
drops guest `Send`/`Sync`. A later audit also aligned entity input with the
stateful WIT cursor and made broad semantic output paged. These are
correctness/operability changes rather than a new performance candidate. The AX
results therefore cover the earlier B lifecycle/splice concept and one frozen
pre-final refined facade, not every final paging, observation, or error detail.

### Candidate C: pure copied checkpoint reducer

```rust
fn file_changed(checkpoint: &[u8], update: FileUpdate<'_>)
    -> Result<(Checkpoint, Vec<Change>)>;
fn entities_changed(checkpoint: &[u8], update: EntityUpdate<'_>)
    -> Result<(Checkpoint, FileEdits)>;
```

This has clean functional semantics and trivial crash recovery. Its hypothesis
is that checkpoint serialization is cheap enough. Large identity/span indexes
cross the component boundary and risk an `O(document)` storage rewrite per
edit, so it must beat Candidate B rather than merely look simpler.

### Candidate D: host-owned plugin KV context

```rust
fn file_changed(ctx: &mut DocumentContext, edits: &[Splice])
    -> Result<Vec<Change>>;
fn entities_changed(ctx: &mut DocumentContext, changes: &[Change])
    -> Result<FileEdits>;
```

This minimizes retained guest memory and makes persistence automatic, but asks
authors to design, version, query, and compact a private index. It also risks
many fine-grained host calls. It is accepted only if memory or eviction results
beat Candidate B by more than 20% and AX evaluation does not show a correctness
or usability loss.

## Format requirements

| Format | Identity | Normal invalidation | Required fallback/correctness |
|---|---|---|---|
| Text | UUID plus order key per line | edited line range with CRLF lookaround | terminal newline, mixed line endings, duplicate lines, reorder |
| CSV | UUID plus order key per record | safe record boundaries; quote state may resynchronize later | multiline quotes, duplicate rows, dialect/root changes, reorder |
| Markdown | UUID graph for structural and inline nodes | enclosing block/container/table | subtree moves, compatible kind changes, tables, format-only changes |
| JSON | stable identity graph; object slots and opaque ordered array items | smallest enclosing value/container | array-front insertion, subtree tombstones, pointer escaping |
| Excalidraw | native element/asset IDs and fractional index | changed top-level element or asset | bindings, domain `isDeleted`, asset filtering, reorder |

No Excalidraw plugin currently exists in the repository. The research fixture is
a contract probe based on the format's native IDs; it must not be presented as
an existing-plugin regression result.

### JSON requires a separate schema break

RFC 6901 pointers are useful locators but not stable array-element identities.
Inserting one value at index zero changes every numeric suffix pointer. The new
JSON plugin should use:

- a fixed root ID;
- opaque object-slot IDs matched by stable parent ID plus decoded key;
- opaque array-item IDs plus independent order keys; and
- pointers as derived locators rather than primary keys.

Duplicate object keys are rejected in v2 rather than silently collapsed.
Unambiguous subtree moves may preserve opaque IDs through format-owned hash
matching only where the owning identity semantics permit it; ambiguous
duplicates use a documented deterministic match. Moving or renaming an object
slot changes its parent/key identity, while an unambiguous array item move may
preserve that item's opaque ID and its nested value identities. Allocator output
and every emitted schema/snapshot/PK correspondence are host-validated.

This correctness change is required even if the plugin API itself remains
unchanged.

## Persistent indexes and cold starts

Each document keeps only the format-specific correspondence needed to update
locally. Complete immutable bytes remain in the host source:

- Text/CSV: a byte or record tree, IDs/order keys, parser checkpoints, content
  hashes, and a hash-to-ID multimap for distant moves.
- Markdown: source spans, syntax tree, IDs/parent/order graph, subtree hashes,
  inline anchors, and table references.
- JSON: incremental syntax tree plus the stable object-slot/array-item identity
  graph.
- Excalidraw: top-level JSON spans, native ID/index maps, reference indexes, and
  asset reachability.

The index is not a duplicate rich snapshot graph. Entity payloads remain
ordinary engine entities and are delivered only for cold `open_entities`, a
full-state fallback, or when a local render needs them.

The measured B2 prototype uses a flat absolute-offset vector. A
length-changing edit therefore shifts every following offset and is
`O(entities after the edit)`, even though its source reads and emitted entity
are local. Production must use a relative-offset/interval tree (or equivalent
piece-tree annotations) and demonstrate logarithmic lookup plus work
proportional to the affected grammatical region on start-, middle-, and
end-of-file length-changing edits. The B2 benchmark proves affected-entity
exactness for generated fixture scanners; it does not validate a complete cold
render. Every production format must additionally prove that
`open_entities(full durable state)` reconstructs exactly the canonical bytes
produced by the warm renderer, including after eviction and at the 10 MiB
64 MiB guest-memory gate.

Warm operations must not hydrate every active entity. Cold `open_file` streams
the file, while cold `open_entities` honestly streams the full durable entity
set: today's engine cannot extract format-specific identity fields from opaque
snapshots without decoding them. A later plugin-emitted identity projection or
durable checkpoint is a separate optimization and must clear the greater-than-
20% end-to-end benefit gate without greater-than-20% write amplification.
Persisting a monolithic 10 MiB checkpoint after every one-byte edit is rejected.
Packing unrelated entity KVs above RocksDB/SlateDB is also rejected: both stores
already have block/SST packing, and an extra packing layer can hurt point reads.

Lazy WIT attachments alone do not make one huge durable entity lazy. The
current JSON/changelog path still decompresses and materializes a complete
snapshot as host bytes/strings. A 4 MiB Excalidraw asset or giant CSV row
therefore needs either a small typed entity envelope with independently
addressed content/chunk attachments, or an explicit `record-too-large` failure.
Per-entity CAS attachments are distinct from packing unrelated KVs, but still
require RocksDB/cached-SlateDB point-read, cold-stream, WAL/live-byte, dedupe,
and GC benchmarks before any storage/capacity benefit is claimed.

Render-effective formatting must survive eviction of the shared renderer.
Applying emitted entities to the base semantic root and then cold
`open_entities` must produce the same canonical bytes as applying the warm
renderer's patches. Markdown therefore
stores required format fields; text must either store each line terminator or
explicitly canonicalize mixed line endings on first ingest rather than relying
on ephemeral spans.

Host byte sources are also process-local derived views, not a new durable file
store. A shared renderer source and session sources must use immutable ropes or
piece trees so an acknowledged fork is constant-time and a local edit shares
unchanged chunks. Retaining one independent 10 MiB `Vec` per session would only
move the memory problem out of Wasm and is rejected. The same structural
sharing rule applies to guest syntax/identity indexes within a file actor.

## Wasm-facing implementation

The high-level SDK compiles to Component Model resources. The important wire
property is ownership, not exposing this WIT to plugin authors:

```wit
record transition-limits {
  max-record-bytes: u32,
  max-page-bytes: u32,
  max-pages: u32,
  max-total-bytes: u64,
  max-inline-edits: u32,
  max-inline-input-bytes: u64,
  total-deadline-nanoseconds: u64,
}

resource transition-budget {
  limits: func() -> transition-limits;
  remaining-nanoseconds: func() -> u64;
}

resource byte-source {
  len: func() -> u64;
  read: func(budget: borrow<transition-budget>, offset: u64, length: u32)
    -> result<bytes, source-error>;
}

record packet-page {
  format-version: u16,
  record-count: u32,
  payload: bytes,
  attachments: list<own<byte-source>>,
}

resource packet-source {
  next: func(budget: borrow<transition-budget>, max-bytes: u32)
    -> result<option<packet-page>, source-error>;
}

resource id-allocator {
  allocate: func(
    budget: borrow<transition-budget>,
    schema-key: string,
    scope: list<string>,
    ordinal: u64,
  ) -> result<list<string>, source-error>;
}

record plugin-selection {
  plugin-key: string,
  generation: string,
}

record file-descriptor {
  path: option<string>,
  media-type: option<string>,
  plugin: plugin-selection,
}

resource byte-output {
  len: func() -> u64;
  read: func(budget: borrow<transition-budget>, offset: u64, length: u32)
    -> result<bytes, plugin-error>;
}

record file-update {
  before-descriptor: file-descriptor,
  after-descriptor: file-descriptor,
  before: own<byte-source>,
  edits: list<input-splice>,
  after: own<byte-source>,
  ids: own<id-allocator>,
}

record entity-update {
  before-descriptor: file-descriptor,
  after-descriptor: file-descriptor,
  before: own<byte-source>,
  changes: own<packet-source>,
  current-entities: own<packet-source>,
}

variant output-bytes {
  inline(bytes),
  output(own<byte-output>),
}

record output-splice {
  offset: u64,
  delete-len: u64,
  insert: output-bytes,
}

record edit-page {
  edits: list<output-splice>,
}

resource edit-cursor {
  next: func(
    budget: borrow<transition-budget>,
    max-edits: u32,
    max-inline-bytes: u32,
  ) -> result<option<edit-page>, plugin-error>;
}

record change-page {
  format-version: u16,
  record-count: u32,
  payload: bytes,
  attachments: list<own<byte-output>>,
}

resource change-cursor {
  next: func(budget: borrow<transition-budget>, max-bytes: u32)
    -> result<option<change-page>, plugin-error>;
}

record file-transition {
  document: own<document>,
  changes: own<change-cursor>,
}

record entity-transition {
  document: own<document>,
  edits: own<edit-cursor>,
}

resource document {
  fork: func() -> own<document>;
  file-changed: func(budget: borrow<transition-budget>, update: file-update)
    -> result<file-transition, plugin-error>;
  entities-changed: func(budget: borrow<transition-budget>, update: entity-update)
    -> result<entity-transition, plugin-error>;
}

record open-file-input {
  descriptor: file-descriptor,
  file: own<byte-source>,
  ids: own<id-allocator>,
}

record open-entities-input {
  descriptor: file-descriptor,
  entities: own<packet-source>,
}

open-file: func(budget: borrow<transition-budget>, input: open-file-input)
  -> result<file-transition, plugin-error>;
open-entities: func(budget: borrow<transition-budget>, input: open-entities-input)
  -> result<entity-transition, plugin-error>;
```

`fork` is constant-time at the resource layer: it aliases the same immutable
accepted document. Successors structurally share unchanged rope/tree nodes.

Small splices and change batches use a versioned packed byte arena. Merge-group
boundaries and complete upsert records are encoded in that transient packet.
Large cold hydration and full replacement use lazy sources/outputs, upgraded to
P3 streams when the toolchain gate clears. Rich nested
`list<entity-state>` values are not the wire format. Bounds checks and packet
version checks run on the host before semantic application. Both transition
directions receive the accepted host byte source; an optimized plugin keeps
offsets, hashes, parser checkpoints, and IDs rather than a duplicate blob.

Small insertion bytes are inline. A large input splice names a range in the
immutable `after` source; a large renderer result returns a guest-owned lazy
byte output. Cursor pages share one global coordinate space, and the host
validates ordering/non-overlap across all pages. The SDK presents both forms as
the same `Splice` abstraction. Because `file-update.edits` is an inline WIT
list, the host also enforces `max-inline-edits` and
`max-inline-input-bytes` **before** Canonical-ABI lowering; large replacements
normally remain one `after-range` splice. The before/after plugin selections
must match before either warm call is lowered.

Packet pages use explicit framing and never split an entity/change record.
Every non-EOF page is non-empty and advances; EOF is permanent; oversized
indivisible records fail explicitly. The host passes the same
`transition-budget` through the top-level call and all later cursor/output
draining, so repeated reads, page count, aggregate bytes, and elapsed time stay
bounded across the whole transition rather than restarting on each resource
method. The author-facing Rust facade hides this plumbing behind bounded
`EntitySource`/`EntityChangeSource`, inline/paged `EntityChangeOutput` and
`FileEdits`, and lazy `ByteOutput` abstractions. Entity and resolved-change input
cursors are stateful like WIT `packet-source.next`; change-key uniqueness and
edit ordering/non-overlap/base bounds are validated across complete drained
cursors, including page boundaries and permanent EOF.

### WASI 0.3 / P3

P3 is useful for bounded memory, backpressure, cancellation, and asynchronous
lazy hydration. It does not make CPU-bound parsing parallel and does not remove
Canonical ABI copies by itself. Inline small deltas should stay inline; streams
should be used for large or naturally asynchronous transfers. P3 adoption is a
transport implementation choice under the SDK facade and must not create a
second author API. WASI 0.3 was ratified on 2026-06-11; the current Wasmtime 45
line can run the ABI behind flags and the Bytecode Alliance announced that
Wasmtime 46 will enable final 0.3.0 Component Model Async by default. Guest
toolchains are still converging on the final WIT pins.

The Bytecode Alliance also reports roughly 3.5x overhead in the current async
task machinery even for purely synchronous component calls. That reinforces a
split transport: keep warm splice/change calls synchronous and inline, and use
P3 streams/futures for cold scans, cache misses, and large fallbacks where
suspension or bounded buffering is useful. Toolchain and sync-call overhead are
implementation gates, not reasons to change the `Document` contract.

## Runtime ownership and concurrency

The current runtime caches one instantiated component/store per plugin key and
serializes every call through one mutex. The v2 runtime instead caches compiled
components and creates bounded actors per active
branch/path/plugin-generation/file-incarnation tuple:

- one shared renderer and structurally shared private session versions live in
  the same actor when safe;
- different branch/files can execute in parallel;
- one branch/file's mutation remains serialized;
- per-file and aggregate memory budgets are explicit;
- LRU eviction cold-reopens only canonical shared renderers; evicting a private
  noncanonical source/root/document lease revokes its observation and requires
  the session to reread;
- guest CPU runs on bounded workers, not general async executor threads; and
- a trap/cancellation discards only that actor, not every file using the plugin.

The benchmark must include same-plugin/different-file throughput, 1/8/32 session
fan-out, p95 latency, guest memory, and process RSS. A single warm document
microbenchmark is insufficient.

## Storage boundary

The API win depends on not reconstructing the old full row graph before the
guest call. Production integration should:

1. Pass only merge-resolved changed entities to a warm renderer before commit.
2. Retain observation-addressed exact source/root/document leases and
   structurally shared semantic roots rather than rich row vectors per session;
   hashes validate bytes but do not identify those roots.
3. Use RocksDB native `MultiGet` for unavoidable sparse cold hydration.
4. Use SlateDB bounded dense-run scans or a real batched lookup, retaining its
   block, metadata, and object-store caches.
5. Keep sparse-key over-read below a configured budget.

This is not a proposal to pack individual logical KVs into Lix-owned packs.
RocksDB and SlateDB already group data into physical blocks/SSTs. The remaining
opportunity is fewer logical reads and less decoding, achieved through deltas,
storage-native batching, persistent semantic roots, and warm document state.

## Evidence-ranked implementation targets

The ranks combine measured headroom with architectural dependency. Profile
percentages below are inclusive whole-process active-sample attribution and
overlap; they are not isolated phase timings and must not be summed.

The full-engine baseline was rerun on final `origin/main` at `c789a2b1`, after
the exact `lix_file` point-read fast path and the intervening SQL, observation,
RocksDB, SlateDB, and CAS optimizations. Against the immutable `66ad14da`
baseline, the four warm medians move by only -2.97% to +0.34%, so those
mainline improvements do not remove the plugin-backed whole-state path. See
[`full-engine-v1-baseline-c789a2b1.md`](../../perf-results/plugin-api-v2/full-engine-v1-baseline-c789a2b1.md)
for raw samples, RSS, storage, logical I/O, profiles, and caveats; the
[`66ad14da` artifact](../../perf-results/plugin-api-v2/full-engine-v1-baseline-66ad14da.md)
is retained as historical comparison.

| Rank | Target | Exact evidence | Decision gate |
|---:|---|---|---|
| 1 | Integrate B2 with observation-selected sparse host roots and a relative-offset document tree | Current one-row 10.68 MiB CSV writes request 226,349 RocksDB-filesystem / 226,350 cached-SlateDB keys and cannot initialize under 64 MiB guest memory. Isolated B2 p50 is 0.0126-0.0710 ms, 264.9-1462.6x over its optimistic v1 control, with 77.99-91.93% lower guest high-water than B. | Must beat current full SQL p50 and p95 by >20% on both backends, pass 64 MiB, and pass observation/lifecycle/full cold-render tests. |
| 2 | Add adaptive SlateDB batched/dense-run reads after the warm path stops requesting the world | Cached SlateDB current-v1 p50 is 3,971 ms/edit and 4,204 ms/render; 39.60% / 22.40% of whole-process active samples include `get_snapshot_values` / SST iterator initialization. | >20% full-engine improvement with a configured sparse-key over-read budget. |
| 3 | Reuse the already validated renderer splice/materialization in `LocalFilesystem` | RocksDB-filesystem exact render p50 is 926 ms, and 41.92% of whole-process active samples include `LocalFilesystem::sync_from_lix` on its separate sync thread. | >20% end-to-end RocksDB-filesystem win with identical bytes and acknowledgement/commit ordering. |
| 4 | Produce a packed transient Component packet directly from sparse state | Rich-record versus arena boundary probes are 6.25-84.0x faster; the two 218,454-entity cases reduce guest peak from 64.88 to 39.32 MB and 63.11 to 28.84 MB. The file-byte control is 0.95x. | >20% after sparse retrieval; constructing rich rows first does not count. |
| 5 | Use P3 streams only for cold/large transfers | A 10 MiB stream reduced payload high-water 81-90% while p50 stayed 2.686-2.718 ms versus 2.692 ms for `list<u8>`. | Capacity/backpressure win without >5% hot-call regression; not a warm latency claim. |

Persistence with complete blobs (A) is rejected at only 1.00-1.05x. Copied
checkpoints (C) are rejected despite 10.54-16.69x core-Wasm latency because
10.05-12.78 MiB crosses each direction per edit. Host KV access (D) remains an
in-memory access probe, and generic Lix-owned packed storage pages are not a
ranked target.

## Hypotheses and acceptance gates

| ID | Hypothesis | Boundary | Gate |
|---|---|---|---|
| H1 | Persistence alone removes repeated parse/index hydration | Wasm and full engine | >20% p50 and p95 |
| H2 | Splice input and patch output beat persistent full blobs | Wasm and full engine | >20% over H1 |
| H2b | Host byte sources remove the retained guest blob without exposing storage KVs | Wasm memory and full engine | >20% memory win over H2, no >5% warm latency loss |
| H3 | Copied checkpoints remain competitive | Wasm, memory, storage | within 20% of H2 with no >20% write amplification |
| H4 | Host-owned KV context materially reduces fan-out memory | 1/8/32 sessions | >20% RSS win and no >20% AX/correctness regression |
| H5 | Packed small batches reduce boundary cost | Component boundary | >20% and full-engine composition demonstrated |
| H6 | P3 streams improve large-transfer capacity | 10 MiB hydration | >20% peak guest-memory win; latency may be neutral |
| H7 | Incremental grammar work is format-independent at the lifecycle level | five fixtures | all stable-ID/correctness tests pass |
| H8 | Persistent resources survive real storage costs | RocksDB/cached SlateDB | >20% full SQL update p50/p95 on both |

Performance runs exclude SQLite. Report p50, p95, peak guest linear memory,
process RSS, bytes crossing the component boundary, logical storage reads and
writes, physical SlateDB object/WAL bytes, and RocksDB WAL/database size.
Fixture construction, plugin compilation, and backend open stay outside warm
timers. Cold compile/open are reported separately.

The checked mechanism matrix (30 warm calls per cell in one process) and the
latest-main discovery baseline (one serial N=11 run per backend) are diagnostic,
not an acceptance A/B. Production H8 requires at least 31 warm samples,
counterbalanced fresh-process ordering, and a 95% interval below a 0.80
candidate/baseline ratio; slow Slate samples may be divided across at least
three counterbalanced processes. No v2 integration is claimed to pass H8 here.

The mechanism scanners and AX task adapters do not satisfy H7/H8. Those gates
require production parser ports plus complete cold render, observation-selected
out-of-order views, expired private-view failure, actor eviction, abort/trap,
file deletion/recreation, branch isolation, plugin-generation upgrade, warm
range/entity-read counters, mixed-line-ending round trips, relative-offset
length-changing edits, and concurrent group-level coupled-reference tests on
both backends.

## AX evaluation protocol

The API candidates are evaluated with the repository's `ax-eval` research
package and raw transcripts. Each prompt follows the minimal canonical shape:

```text
Implement and test the assigned <format> plugin using <candidate SDK path>
```

The planned screen gave A, C, and D one isolated agent per format, followed by
the ax-eval default of ten B/B2 agents. Execution completed A=5, B=9, C=4, and
D=4 before the environment's agent-write capacity was exhausted. Missing cells
are reported as missing rather than imputed. Only one
submission workspace is visible at a time; completed workspaces are archived
outside the repository before the next agent begins. An independent judge
evaluates every transcript and workspace. Deterministic parsing—not agent
self-report—counts duration, tool calls, interruptions, commands, and tool
errors. Success additionally requires the format's stable-ID and local-update
tests.

The pinned model in the supplied skill is unavailable in this Codex runtime, so
the result metadata records the model/tool/temperature overrides. Raw rollout
JSONL, judge output, and the per-tool index are retained only in the author's
local `~/.ax-eval`/Codex rollout directories; this branch checks in the compact
schema-valid result JSON. Reviewers can inspect per-agent prompts, metrics,
verdicts, and substitutions but cannot replay complete transcripts from the
PR. Deltas under ten score points at N=10 are treated as noise.

After the correctness review froze the refined signatures, a targeted N=3
follow-up assigned one isolated agent each to lifecycle/cold-open/rename, CSV
reorder/cold reconstruction, and Excalidraw coupled-group/lazy-large-entity
work. All three implementations and their seven tests passed independent
judgment. Scores were 86, 85, and 82 (median 85). This demonstrates that the
added concepts are usable in focused tasks, but the heterogeneous N=3 result is
not statistically comparable with the earlier N=9 B cohort. The Excalidraw
task exercised bounded 64 KiB reads and lazy output for a 4 MiB entity, but its
test implementation eventually accumulated the payload; it is not peak-memory
evidence. That N=3 cohort froze candidate hash `b66a024...`; a subsequent audit
made semantic output and resolved input paged, aligned entity/change input with
stateful WIT cursors, added transition-wide key/edit validation and pre-call
splice caps, defined the prospective-state fallback, and forbade warm plugin
reselection. A fresh implementer for the checked-in
[`final-aligned.md`](../../experiments/plugin-api-v2/ax-eval/tasks/final-aligned.md)
task was blocked by the environment approval reviewer before any file change;
no retry, result, or score exists. Those final signatures remain
compile/test/WIT-audit evidence (facade SHA-256 `9ec4a63c...`, WIT SHA-256
`d28796b3...`) and are not silently credited with the earlier scores.

The decision order is correctness, then the >20% performance/storage gate,
then AX usability. A pleasant API cannot rescue an `O(document)` warm path; a
fast API that agents routinely misuse cannot ship without a safer facade.
The main comparative cohort evaluates the pre-refinement candidate facades.
The targeted follow-up covers the pre-final `open_file`/`open_entities`,
descriptors, scoped allocation, lazy bytes, complete order upserts, and
merge-group construction. Final cross-page validation, aggregate-budget
failures, observation expiry/retry, generated bindings, and realistic recovery
still require controlled follow-up before the API is frozen.

## Rollout plan

1. Preserve remote blob-splice/hash metadata through SQL parameter binding,
   add opaque session/branch/path-bound observation handles, and reject an
   existing-file mutation whose exact identity root is unavailable.
2. Introduce structurally shared host byte and semantic roots plus
   observation-addressed private leases; benchmark 1/8/32 sessions and expiry.
3. Introduce the refined SDK/WIT facade with both cold constructors, merge
   groups, transition-wide cursor validation, pre-call splice caps, and
   precommit renderer validation.
4. Port text first, then CSV, because their grammatical invalidation boundaries
   are easiest to verify.
5. Break JSON identity before claiming stable array behavior.
6. Port Markdown with retained source spans and subtree indexes.
7. Add the Excalidraw plugin using native IDs and separate streamed assets.
8. Add per-branch/file actors, memory admission, private-handle expiry,
   rollback, trap, whole-file deletion/recreation, plugin upgrade, and
   multi-session tests.
9. Prototype bounded group-provenance GC and large-entity CAS attachments (or
   explicit record limits), measuring RocksDB and cached SlateDB amplification.
10. Enable v2 by measured format/backend cohort; retain v1 only for migration,
   not as an automatic large-file fallback.

## Open questions

- Whether a compact periodic durable checkpoint clears the cold-open and
  storage-amplification gates after the first full-entity-streaming release.
- Whether one Wasm store per file actor provides the best fault isolation, or a
  small actor pool per plugin generation is necessary for tiny files.
- Which persistent semantic-map implementation gives the best root sharing and
  sparse merge behavior at 1/8/32 retained session views.
- Which incremental parser implementation each format should use; the API does
  not mandate Tree-sitter or any other parser.
- Which bounded group-conflict frontier and visibility horizon preserve
  arrival-order independence without unbounded durable provenance.
- Whether per-entity content-addressed attachments beat an explicit maximum
  entity record size for huge assets/rows on both storage backends.

## References

- [Isolated persistent-CSV Wasm source and result](../../experiments/persistent-csv-wasm/README.md)
- [Async Component stream source, raw TSV, and result](../../experiments/p3-stream-probe/RESULTS.md)
- [Rich-vs-packed Component ABI harness](../../packages/plugin-abi-bench/README.md)
- [Retained packed ABI matrix](../../perf-results/plugin-api-v2/prior-probes/packed-abi-component-matrix-2026-07-21.md)

- [WASI 0.3 launch and async Component Model primitives](https://bytecodealliance.org/articles/WASI-0.3)
- [Component Model 1.0 roadmap and current synchronous-call overhead](https://bytecodealliance.org/articles/the-road-to-component-model-1-0)
- [Component Model Canonical ABI](https://component-model.bytecodealliance.org/advanced/canonical-abi.html)
- [Tree-sitter incremental edits and structurally shared trees](https://tree-sitter.github.io/tree-sitter/using-parsers/3-advanced-parsing.html)
- [RocksDB MultiGet performance rationale](https://github.com/facebook/rocksdb/wiki/MultiGet-Performance)
- [SlateDB read path](https://slatedb.io/docs/design/reads/)
- [SlateDB cache layers](https://slatedb.io/docs/design/caching/)
- [Excalidraw element types](https://github.com/excalidraw/excalidraw/blob/53732f08f430ded353121c64c230b448282be37a/packages/element/src/types.ts#L42-L82)
- [Excalidraw serialization](https://github.com/excalidraw/excalidraw/blob/53732f08f430ded353121c64c230b448282be37a/packages/excalidraw/data/json.ts#L26-L74)
