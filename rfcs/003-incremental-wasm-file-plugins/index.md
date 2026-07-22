---
date: "2026-07-22"
status: research
---

# Incremental Wasm file plugins

## Decision

Replace the stateless whole-state plugin calls with one SDK-facing immutable
`Document` abstraction. A document receives either byte splices or committed
entity changes and returns an immutable successor plus the opposite kind of
delta. Immutable file versions stay behind a host-owned, range-readable
`Source`; the Wasm document normally retains only format-specific syntax and
identity indexes. The engine owns transaction acceptance, rollback, session
views, deletion authority, revision checks, storage, scheduling, and eviction.

The recommended author-facing shape is intentionally small:

```rust
pub trait FilePlugin {
    type Document: Document;

    fn open(&self, input: OpenDocument<'_>) -> Result<Self::Document, Error>;
}

pub trait Document: Sized + Send + Sync + 'static {
    fn file_changed(
        &self,
        input: FileUpdate<'_>,
    ) -> Result<FileTransition<Self>, Error>;

    fn entities_changed(
        &self,
        input: EntityUpdate<'_>,
    ) -> Result<EntityTransition<Self>, Error>;

    fn checkpoint(&self) -> Result<Option<Checkpoint>, Error> {
        Ok(None)
    }
}
```

`FileUpdate` contains base-relative, sorted, non-overlapping byte splices and a
retry-stable ID allocator. `FileTransition` contains the successor document and
one atomic semantic change set. `EntityUpdate` contains only the final committed
entity deltas. `EntityTransition` contains the successor document and byte
splices against its previous bytes.

A basic plugin may ignore locality and use these fallbacks:

```rust
fn file_changed(&self, update: FileUpdate<'_>) -> Result<FileTransition<Self>, Error> {
    let next_bytes = update.apply_and_read_all()?;
    self.reparse(next_bytes, update.ids())
}

fn entities_changed(
    &self,
    update: EntityUpdate<'_>,
) -> Result<EntityTransition<Self>, Error> {
    let next = self.apply_changes(update.changes())?;
    Ok(update.replace_all(next.render()?).with_document(next))
}
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

Plugin authors implement only `Document`. Internally the engine holds different
document versions for different authority domains:

- A shared renderer document represents the current merged branch/file state.
- A session document represents exactly the bytes and entities that session
  received or subsequently submitted.

Those versions may share immutable guest structures, but they are never one
mutable object. Applying a stale client's splice directly to the shared
renderer would reintroduce overwrite bugs.

The engine calls `file_changed` on the exact private session version. It merges
the returned semantic changes into current shared state, commits them, and only
then calls `entities_changed` on the shared renderer with the final merged
changes. The session successor is the client's submitted view, not a merged
view the client never received.

### Immutable transitions make rollback ordinary

An accepted document is borrowed immutably. A plugin returns a new document.
The host retains both resources until storage commits:

1. Invoke the accepted document and receive `(successor, output)`.
2. Validate the output and stage the authoritative engine mutation.
3. On commit, swap the host cache pointer to the successor.
4. On abort or failed validation, drop the successor and evict the accepted
   guest resource; reopen the previous authoritative root when it is needed.
5. On a trap or uncertain completion, evict the disposable resource and reopen
   from authoritative engine state.

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

New IDs come from a host allocator keyed by operation identity and ordinal.
Retries therefore allocate the same IDs. The guest may use a native file ID
when one exists, but it must not use time or unseeded randomness for retry
identity.

For ID-less formats, “stable” cannot mean recovering unknowable user intent.
Two byte-identical duplicate rows swapped in a CSV have no observable identity
signal. The contract instead requires that IDs never derive from mutable array
indices or byte offsets, preserves an existing ID whenever the format matcher
has an unambiguous correspondence, and makes ambiguous matching deterministic.
Native Excalidraw IDs are exact. JSON object-slot IDs are stable for value edits
and key reorder but a key rename is intentionally a delete plus insert; opaque
array-item IDs survive insertion and reorder.

The engine validates every tombstone against the exact private identity root
acknowledged by that session. A guest resource and checkpoint are acceleration
only; neither grants delete authority.

### Coordinate and atomicity rules

- All splices in one update use coordinates in the same previous byte string.
- Splices are sorted and non-overlapping. The SDK validates this before guest
  code runs.
- The engine verifies the before and after content hashes. Remote v3 already
  sends an exact prefix/delete/insert splice with SHA-256 validation; that edit
  provenance should survive parameter decoding.
- All entity changes returned by one call are atomic. This supports Markdown
  table references and Excalidraw binding updates.
- A renderer receives only final committed entity deltas, never the client's
  unmerged proposal.
- Full replacement is represented by one splice and remains a first-class
  fallback.

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
- object-slot identity derived from stable parent ID plus decoded key;
- opaque array-item IDs plus independent order keys; and
- pointers as derived locators rather than primary keys.

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
ordinary engine entities and are delivered only when a local render needs
them.

Warm operations must not hydrate every active entity. A cold open may rebuild
from the file and an identity-only projection. An optional, versioned checkpoint
can avoid that rebuild when it matches the plugin hash, ABI version, file
incarnation, and immutable semantic root. Checkpoints are disposable derived
data; missing or corrupt checkpoints fall back to rebuild.

The first production version should keep checkpoints process-local until a
storage design proves more than 20% end-to-end benefit without more than 20%
write amplification. Persisting a monolithic 10 MiB checkpoint after every
one-byte edit is rejected. Packing unrelated entity KVs above RocksDB/SlateDB is
also rejected: both stores already have block/SST packing, and an extra packing
layer can hurt point reads.

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
resource document {
  fork: func() -> own<document>;
  file-changed: func(update: file-update)
    -> result<tuple<own<document>, own<change-cursor>>, plugin-error>;
  entities-changed: func(update: entity-update)
    -> result<tuple<own<document>, own<edit-cursor>>, plugin-error>;
  checkpoint: func() -> result<option<own<byte-source>>, plugin-error>;
}
```

`fork` is constant-time at the resource layer: it aliases the same immutable
accepted document. Successors structurally share unchanged rope/tree nodes.

Small splices and change batches use a versioned packed byte arena. Large cold
hydration, full replacement, and checkpoint traffic use streams. Rich nested
`list<entity-state>` values are not the wire format. Bounds checks and packet
version checks run on the host before semantic application. Both transition
directions receive the accepted host byte source; an optimized plugin keeps
offsets, hashes, parser checkpoints, and IDs rather than a duplicate blob.

### WASI 0.3 / P3

P3 is useful for bounded memory, backpressure, cancellation, and asynchronous
lazy hydration. It does not make CPU-bound parsing parallel and does not remove
Canonical ABI copies by itself. Inline small deltas should stay inline; streams
should be used for large or naturally asynchronous transfers. P3 adoption is a
transport implementation choice under the SDK facade and must not create a
second author API. WASI 0.3 was ratified on 2026-06-11; the current Wasmtime 45
line exposes the release-candidate implementation, while Wasmtime 46 enables
0.3.0 Component Model Async by default. Toolchain readiness should therefore be
an implementation gate, not a reason to change the document contract.

## Runtime ownership and concurrency

The current runtime caches one instantiated component/store per plugin key and
serializes every call through one mutex. The v2 runtime instead caches compiled
components and creates bounded actors per active plugin-generation/file
incarnation:

- one shared renderer and structurally shared private session versions live in
  the same actor when safe;
- different files can execute in parallel;
- one file's mutation remains serialized;
- per-file and aggregate memory budgets are explicit;
- LRU eviction drops disposable resources and cold-reopens them;
- guest CPU runs on bounded workers, not general async executor threads; and
- a trap/cancellation discards only that actor, not every file using the plugin.

The benchmark must include same-plugin/different-file throughput, 1/8/32 session
fan-out, p95 latency, guest memory, and process RSS. A single warm document
microbenchmark is insufficient.

## Storage boundary

The API win depends on not reconstructing the old full row graph before the
guest call. Production integration should:

1. Pass only committed changed entities to a warm renderer.
2. Retain exact private view roots rather than rich row vectors per session.
3. Use RocksDB native `MultiGet` for unavoidable sparse cold hydration.
4. Use SlateDB bounded dense-run scans or a real batched lookup, retaining its
   block, metadata, and object-store caches.
5. Keep sparse-key over-read below a configured budget.

This is not a proposal to pack individual logical KVs into Lix-owned packs.
RocksDB and SlateDB already group data into physical blocks/SSTs. The remaining
opportunity is fewer logical reads and less decoding, achieved through deltas,
storage-native batching, identity-only projection, and warm document state.

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

## AX evaluation protocol

The API candidates are evaluated with the repository's `ax-eval` research
package and raw transcripts. Each prompt follows the minimal canonical shape:

```text
Implement and test the assigned <format> plugin using <candidate SDK path>
```

The screening round gives A, C, and D one isolated agent per format. Candidate
B/B2 then receives the ax-eval default of ten agents, two per format. Only one
submission workspace is visible at a time; completed workspaces are archived
outside the repository before the next agent begins. An independent judge
evaluates every transcript and workspace. Deterministic parsing—not agent
self-report—counts duration, tool calls, interruptions, commands, and tool
errors. Success additionally requires the format's stable-ID and local-update
tests.

The pinned model in the supplied skill is unavailable in this Codex runtime, so
the result metadata records the model/tool/temperature overrides. Raw rollout
JSONL, judge output, result JSON, and the per-tool index are retained. Deltas
under ten score points at N=10 are treated as noise.

The decision order is correctness, then the >20% performance/storage gate,
then AX usability. A pleasant API cannot rescue an `O(document)` warm path; a
fast API that agents routinely misuse cannot ship without a safer facade.

## Rollout plan

1. Preserve remote blob-splice metadata through SQL parameter binding and add a
   local full-blob-to-splice fallback.
2. Introduce the SDK facade and v2 resources behind a new plugin API version.
3. Port text first, then CSV, because their grammatical invalidation boundaries
   are easiest to verify.
4. Break JSON identity before claiming stable array behavior.
5. Port Markdown with retained source spans and subtree indexes.
6. Add the Excalidraw plugin using native IDs and separate streamed assets.
7. Add per-file actors, memory admission, eviction, rollback, trap, upgrade,
   and multi-session tests.
8. Enable v2 by measured format/backend cohort; retain v1 only for migration,
   not as an automatic large-file fallback.

## Open questions

- Whether process-local checkpoints are sufficient for the first release or a
  compact periodic durable checkpoint clears the storage gate.
- Whether one Wasm store per file actor provides the best fault isolation, or a
  small actor pool per plugin generation is necessary for tiny files.
- Whether strict network receipt needs an opaque delivery confirmation token
  before delete authority is granted.
- Which incremental parser implementation each format should use; the API does
  not mandate Tree-sitter or any other parser.

## References

- [WASI 0.3 launch and async Component Model primitives](https://bytecodealliance.org/articles/WASI-0.3)
- [Component Model Canonical ABI](https://component-model.bytecodealliance.org/advanced/canonical-abi.html)
- [Tree-sitter incremental edits and structurally shared trees](https://tree-sitter.github.io/tree-sitter/using-parsers/3-advanced-parsing.html)
- [RocksDB MultiGet performance rationale](https://github.com/facebook/rocksdb/wiki/MultiGet-Performance)
- [SlateDB read path](https://slatedb.io/docs/design/reads/)
- [SlateDB cache layers](https://slatedb.io/docs/design/caching/)
- [Excalidraw element types](https://github.com/excalidraw/excalidraw/blob/53732f08f430ded353121c64c230b448282be37a/packages/element/src/types.ts#L42-L82)
- [Excalidraw serialization](https://github.com/excalidraw/excalidraw/blob/53732f08f430ded353121c64c230b448282be37a/packages/excalidraw/data/json.ts#L26-L74)
