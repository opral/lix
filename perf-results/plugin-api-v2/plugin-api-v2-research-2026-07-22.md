# Incremental Wasm plugin API research

Date: 2026-07-22  
Base commit: `66ad14da` (`origin/main`)
Branch: `codex/wasm-plugin-api-research`  
Host: Apple M5 Pro, 18 cores, 64 GiB RAM, macOS 26.3.1 arm64  
Toolchain: Rust nightly 1.97, Wasmtime 45, `wasm32-unknown-unknown` and
`wasm32-wasip2`

## Executive result

The architectural insight is narrower than “make plugins stateful”:

> A warm file write must send only the byte edit to an exact private document
> version selected by a validated session/path-bound observation handle, and a
> warm render must send only final merge-resolved entity changes to an exact
> shared document version.

The engine must stop loading and lowering every active entity before those
calls. Wasm remains the isolation boundary.

The proposed API is one immutable `Document` with two symmetric warm
transitions plus explicit constructors for the two possible cold directions.
Complete immutable bytes remain behind a host-owned range source; Wasm retains
the format's syntax/identity index:

```rust
open_file(descriptor, file_source, scoped_ids)
  -> (document, initial_entity_groups)
open_entities(descriptor, bounded_entity_pages)
  -> (document, complete_file_edits)
file_changed(before_descriptor, after_descriptor, before_source, after_source, byte_splices)
  -> (successor_document, entity_merge_groups)
entities_changed(before_descriptor, after_descriptor, before_source, resolved_entity_groups)
  -> (successor_document, byte_splices)
```

Most merge groups contain one complete upsert or keyed delete. Coupled facts,
such as both sides of an Excalidraw binding, may share a group. Authority is
validated for the complete group and deterministic group-level LWW selects all
of it or none of it. Order, parentage, and references live in complete schema
snapshots rather than optional transport metadata.

Descriptors contain path, media type, and the host-selected content-addressed
plugin generation; a rename-only transition is delivered even with zero byte
splices. Cold/full entity access is bounded and paged. Large inserted or
rendered bytes stay behind lazy sources/outputs. ID allocation explicitly names
schema, composite-PK scope, and deterministic ordinal. At the WIT layer one
aggregate record/page/page-count/total-byte/deadline budget covers the
top-level call and all subsequent cursor/output draining; resource calls cannot
renew it.

The SDK includes full-parse and full-replace fallbacks in those same calls. It
does not expose sessions, branches, acknowledgements, revisions, commits,
prepare/accept/abort, storage KVs, or CRDT state to plugin authors. The engine
keeps accepted resources alive until commit. It renders and validates the final
resolved graph before durable commit; abort/trap evicts disposable accepted and
successor resources. A shared renderer can reopen from its durable branch root;
a noncanonical private view cannot, so its observation expires and the session
must reread.

The refined facade shown here was produced after the main controlled AX cohort.
A targeted N=3 follow-up on its frozen signatures independently passed
lifecycle/cold-open/rename, CSV reorder/cold reconstruction, and Excalidraw
coupled-group/lazy-large-entity tasks. Its median score was 85, versus 91 for
the earlier B cohort, but the tasks and sample sizes differ and cannot support
a comparative ergonomics claim. Observation expiry, aggregate-budget failure,
and production generated bindings still need a larger final cohort.

This report compares four breaking alternatives plus current v1:

1. current stateless full-state/full-blob;
2. persistent document with complete blobs (persistence control);
3. persistent document with splices and patches (proposed);
4. pure reducer with a copied opaque checkpoint; and
5. host-owned transactional plugin KV context.

No performance/storage change is recommended from a microbenchmark alone. The
gate is greater than 20% at the full SQL/merge/storage boundary on RocksDB and
cached SlateDB. Correctness and security changes, notably stable JSON array
identities, are absolute gates rather than percentage gates.

## What changed on latest main before this research

The base already includes the first profile-driven optimizations from the prior
investigation:

- codec v3 (`115350f9`);
- multiplayer file merge (`1fe843c9`);
- shared acknowledged views (`ab941e04`);
- ordered CSV projection without row cloning (`f5209d78`);
- uncontested visibility fast path (`9600c9ca`);
- remote request/observation blob splices (`26a38c67`); and
- SlateDB background WAL flush batching (`66ad14da`).

This matters because old flamegraphs identified visibility maps and repeated
acknowledgement clones as large costs; those low-risk fixes are no longer
available to inflate the v2 result.

Remote v3 now already computes a validated single splice. The JS client sends
base/result SHA-256, prefix length, suffix length, and inserted bytes. The
server validates and reconstructs the full `Value::Blob`, then discards the
splice metadata before SQL/plugin reconciliation. Preserving that byte-edit
provenance is therefore plumbing, not a new client API. The base hash is not,
however, a unique semantic-root identity: byte-identical views can have
different stable IDs after different histories. V2 additionally needs a hidden
opaque observation handle bound to session, branch, and path that addresses the
exact received source/root/document lease.

## Research rules

### Evidence classes

- **Full engine**: ordinary SQL blob reads/writes, exact acknowledgement,
  multiplayer merge, real installed Wasm, real RocksDB or cached SlateDB. Only
  this layer can accept an architecture.
- **Component/core Wasm mechanism**: real Wasmtime guest with storage removed.
  It identifies causality, memory, and boundary behavior but cannot predict the
  full-engine percentage.
- **Native algorithm**: format algorithm diagnosis only. It is never headline
  evidence and is not used to weaken the Wasm requirement.
- **AX evaluation**: author usability/correctness of compileable SDK facades.
  It breaks ties between architectures that already clear correctness and the
  20% performance/storage gate.

### Backend policy

- RocksDB uses its native logical KVs and native batched read path.
- SlateDB uses the same production-style local object-store/block/metadata
  cache budgets as Lixray: 64 MiB / 4 MiB / 1 MiB per workspace.
- Cacheless SlateDB is diagnostic only.
- SQLite is excluded from every performance conclusion.

### Statistical gate

Primary latency cells use deterministic fixtures, at least five warmups, 31
samples, and counterbalanced fresh-process A/B ordering where practical. A
candidate is accepted when the paired candidate/baseline ratio's 95% interval
is below `0.80` for the large-file aggregate, at least four of five formats
improve by more than 20% per backend, and no primary cell regresses more than 5%
p50 or 10% p95.

Very slow Slate diagnostics may use 11 samples across three fresh processes,
reported explicitly. Fixture generation, backend open, and cold plugin compile
stay outside warm timers. Cold compile, cold hydrate, and first edit are
reported separately.

Storage and memory have independent gates. Reject a monolithic checkpoint or
other design that adds more than 20% WAL/live-byte amplification unless it
delivers a separately chosen greater-than-2x primary latency win. A 10 MiB case
must pass the existing 64 MiB guest limit. Host RSS and bytes per retained
session must not grow without a bounded admission/eviction policy.

## Current path and the scaling invariant

The current WIT is stateless:

```wit
detect-changes: func(state: list<entity-state>, file: file)
  -> result<list<detected-change>, plugin-error>;
render: func(state: list<entity-state>)
  -> result<list<u8>, plugin-error>;
```

For a single localized edit, the engine currently does work shaped like:

```text
acknowledged SQL blob update
  -> load exact private N-entity row set
  -> materialize changelog payloads
  -> clone owned PK/schema/JSON strings
  -> Canonical-lower N nested values and a complete new blob
  -> guest parses N JSON snapshots and a complete file
  -> guest emits rich changes
  -> engine merges and commits sparse semantic changes

SQL blob read
  -> load current shared N-entity row set
  -> materialize and lower N nested values
  -> guest reconstructs complete file
  -> host copies complete output
  -> session retains the exact private acknowledgement view
```

The desired invariant is:

```text
warm acknowledged SQL blob update
  -> byte splice(s), usually O(edit bytes)
  -> observation-selected private document reparses affected grammatical closure
  -> sparse semantic merge groups
  -> engine validates whole-group authority and resolves deterministic group LWW
  -> final sparse groups update/validate shared document
  -> storage commit + publish rendered byte patch
```

Cold open may still be `O(document)`: initial import streams bytes, while
restart/eviction streams durable entities because plugin-backed raw blobs are
not durable. That entity constructor is valid for a canonical shared renderer,
not an arbitrary private session view containing exact formatting or losing
proposals. The host retains each private source/root/document together; if that
lease is evicted, its observation expires and the next write fails closed until
the session rereads. Warm one-entity work must not become `O(document)`. Any
future checkpoint or identity projection is disposable acceleration and cannot
become commit/delete authority; it must independently clear the
storage-amplification gate.

If an existing-file write lacks a valid observation, v2 does not parse the
whole submitted blob and call its apparent changes “safe upserts.” ID-less
formats cannot know whether those apparent entities update existing IDs or are
new. Only explicit creation of an absent path, or an exact no-op against the
current canonical bytes, is safe without a prior observation. Every other
missing/expired/wrong-path observation returns a retryable reread/`410` before
plugin execution.

## Prior causal evidence retained as controls

The immediately preceding Wasmtime 45 investigation used the same host and
measured these mechanisms before the latest-main low-risk fixes landed. They
remain causal controls; new latest-main full-engine results are reported
separately.

### Persistent guest document

A real core-Wasm 10.49 MiB CSV-shaped one-cell edit compared an optimistic
stateless full-file round trip with a persistent guest document:

| Path | p50 / p95 | Guest high-water |
|---|---:|---:|
| Stateless complete previous+next files | 21.642 / 29.852 ms | 64.06 MiB |
| Persistent splice, guest apply only | 0.065 / 0.081 ms | 38.88 MiB |
| Persistent splice plus host materialization | 3.080 / 3.548 ms | 38.88 MiB |

The mechanism was 7.0x p50 end to end and 333x at the guest apply boundary.
Cold hydration was 15.5 ms. That prototype supported one contained, same-length
CSV cell edit; it did not prove format correctness, storage composition, or
session lifecycle.

### Nested rich ABI versus packed transient arena

A real `wasm32-wasip2` Component Model probe, with semantic work intentionally
absent, measured the current nested entity shape versus one versioned transient
byte arena:

| 10 MiB entity shape | Operation | Rich p50 | Arena p50 | Speedup | Guest peak rich / arena |
|---|---|---:|---:|---:|---:|
| 218,454 × 48 B | input | 46.090 ms | 0.676 ms | 68.2x | 64.88 / 39.32 MB |
| 218,454 × 48 B | round trip | 81.284 ms | 0.968 ms | 84.0x | 63.11 / 28.84 MB |
| 10,240 × 1 KiB | input | 2.402 ms | 0.384 ms | 6.25x | 24.05 / 22.87 MB |
| file bytes only | round trip | 0.341 ms | 0.360 ms | 0.95x | equal |

The byte-only control rejects “Wasm itself is slow.” Per-entity lowering,
allocation, and repeated representation building are the problem. A packed
arena here is only a Component ABI encoding. It is not a proposal to pack
durable semantic KVs above RocksDB/SlateDB.

### P3 streams

A real async Canonical ABI stream probe over 10 MiB found:

| Input | Checksum p50 / p95 | Largest guest memory |
|---|---:|---:|
| `list<u8>` | 2.692 / 2.765 ms | 11.062 MiB |
| `stream<u8>`, 64 KiB chunks | 2.718 / 2.804 ms | 1.125 MiB |
| `stream<u8>`, 1 MiB chunks | 2.686 / 2.820 ms | 2.062 MiB |

P3 was latency-neutral and reduced payload high-water 81-90%. Eight-KiB chunks
were more than 3x slower in the drain control. P3 is accepted for large-transfer
capacity/backpressure/cancellation, not as the localized-edit speedup. Small
splices remain inline.

WASI 0.3 was ratified on 2026-06-11. Wasmtime 45 can run the ABI behind flags;
the Bytecode Alliance announced final 0.3.0-by-default for Wasmtime 46 while
guest toolchains converge on final WIT pins. Its Component Model roadmap also
reports roughly 3.5x current async-task overhead on otherwise synchronous calls.
The proposed wire contract therefore keeps hot splice/change calls synchronous:
small bytes inline, large input bytes as ranges in `after`, and large guest
output behind a bounded lazy source. P3 streams/futures replace those cold/large
adapters when toolchain and sync-overhead gates clear; they do not create a
second author API.

## Latest-main full-engine v1 baseline

The production-path baseline uses the real CSV Wasm plugin through an ordinary
acknowledged SQL blob update. The fixture has 220,000 rows and is exactly
10,680,000 bytes. It changes one field in the middle row and alternates the
input so all 11 measured writes are real transitions. RocksDB is measured
through production `LocalFilesystem`; SlateDB uses the Lixray 64 MiB disk /
4 MiB block / 1 MiB metadata cache policy. SQLite was neither built nor run.

Current v1 cannot create this fixture under the production 64 MiB guest-memory
ceiling on either backend. The diagnostic timings below therefore use an
explicitly reported 256 MiB cap, including the second engine used by
`LocalFilesystem` synchronization. This is capacity and baseline evidence, not
a proposal to raise the limit.

| Production backend | Cold write | One-row write p50 / p95 | Exact render p50 / p95 | Write-process max RSS |
|---|---:|---:|---:|---:|
| RocksDB filesystem | 5,451 ms | 2,696 / 2,924 ms | 937 / 1,027 ms | 2.05 GiB |
| Cached SlateDB | 4,345 ms | 4,092 / 4,241 ms | 4,190 / 4,918 ms | 1.97 GiB |

A separately instrumented single-row write requested 226,391 keys through
RocksDB filesystem and 226,360 through cached SlateDB. Of the RocksDB requests,
220,131 were change-payload keys; another 6,118 calls individually loaded
tracked-state tree chunks. Storage after initial import was 75.43 MB for the
complete RocksDB filesystem (7.06x the source) and 193.07 MB for cached SlateDB
including its cache (18.08x); the SlateDB durable object store alone was
128.90 MB (12.07x).

The Samply phase marker was optimized out, so these profiles are
**whole-process active-sample attribution**, including reopen, plugin prewarm,
the acknowledged render, the timed write, and close/flush. Inclusive shares
overlap and cannot be summed or read as isolated write-phase wall time. Within
that scope, Wasmtime `detect_changes` appears in 1.00% of active RocksDB samples
and 0.72% of cached-SlateDB samples. RocksDB stacks include row materialization
in 41.31%, change-record loading in 27.77%, and the separate filesystem-sync
thread in 42.49%. Cached-SlateDB stacks include snapshot-value fetching in
41.86% and SST-iterator initialization in 23.60%. Together with the independent
226k-key counters, these profiles are consistent with whole-state host/storage
work dominating the current path; they do not isolate a causal percentage for
the timed edit alone.

This produces the following evidence-ranked implementation targets. The
mechanism ratios and whole-process profile shares are diagnostic headroom, not
additive end-to-end forecasts.

| Rank | Target | Exact evidence | Required acceptance evidence |
|---:|---|---|---|
| 1 | Integrate B2 with observation-selected sparse host roots and a relative-offset document tree | Current one-row write requests 226,391 RocksDB-filesystem / 226,360 cached-SlateDB keys and current v1 cannot initialize under 64 MiB. Isolated B2 p50 is 0.0126-0.0710 ms, 264.9-1462.6x over its optimistic v1 control, with 77.99-91.93% lower guest high-water than B. | >20% p50 and p95 full SQL win on both backends, 64 MiB pass, stable identities, lifecycle, and complete cold-render validation. |
| 2 | Add adaptive SlateDB batched/dense-run reads after the warm path stops requesting all state | Current p50 is 4,092 ms/edit and 4,190 ms/render; whole-process active samples include `get_snapshot_values` in 41.86% and SST initialization in 23.60%. | >20% full-engine win with a bounded sparse-key over-read budget. |
| 3 | Reuse the precommit-validated renderer splice/materialization in `LocalFilesystem` | Exact render p50 is 937 ms; the separate sync thread accounts for 42.49% of whole-process RocksDB active samples. | >20% RocksDB-filesystem end-to-end win with byte equality and unchanged commit/acknowledgement ordering. |
| 4 | Produce the packed transient Component packet directly from sparse state | Rich-record versus arena probes are 6.25-84.0x faster. The 218,454-entity cases reduce guest peak from 64.88 to 39.32 MB and 63.11 to 28.84 MB; file bytes alone are 0.95x. | >20% after sparse retrieval. Building today's rich rows and then packing them fails the intent. |
| 5 | Adopt P3 streams for cold and large transfers only | 10 MiB streams reduce payload high-water 81-90%, while p50 remains 2.686-2.718 ms versus 2.692 ms for `list<u8>`. | Capacity/backpressure benefit with no >5% warm-call regression; no localized-edit latency claim. |

Persistence with complete blobs (A) is rejected at 1.00-1.05x. Copied
checkpoints (C) are rejected despite 10.54-16.69x core-Wasm latency because
10.05-12.78 MiB crosses each direction per edit. Host-KV Candidate D has no
storage latency in its access-only result. A generic Lix-owned packed-page
layer is not a target: RocksDB and SlateDB already pack physical blocks, and
the measured problem is logical whole-state retrieval/materialization.

The complete raw samples, RSS, on-disk decomposition, logical counters,
profiles, and reproduction commands are in
[`full-engine-v1-baseline-66ad14da.md`](./full-engine-v1-baseline-66ad14da.md).
This PR does not claim the full-engine greater-than-20% gate for v2: that gate
requires a production integration A/B against this baseline.

## Latest-main Wasm mechanism tournament

The new standalone harness executes every candidate's parsing, identity
reconciliation, splice application, and output logic inside the same
`wasm32-unknown-unknown` module under Wasmtime 45. The host only owns fixtures,
copies ABI buffers, serves imported ranges, times calls, and verifies results.
It recorded 105 cells: seven lanes, five formats, three sizes, five warmups, and
30 measured calls. The v1 lane is deliberately optimistic: it transfers a
compact checkpoint and complete next file, then returns only a 32-byte result.

At approximately 10 MiB:

| Candidate | p50 | p95 | p50 versus v1 | Largest guest high-water |
|---|---:|---:|---:|---:|
| v1 stateless compact control | 18.41-19.08 ms | 20.22-27.79 ms | 1.0x | 54.44 MiB |
| A persistent, full file | 17.45-19.16 ms | 19.16-21.92 ms | 1.00-1.05x | 45.63 MiB |
| B persistent guest file + splice | 0.123-0.326 ms | 0.126-0.358 ms | 58.3-149.1x | 61.06 MiB |
| **B2 guest index + host source** | **0.0126-0.0710 ms** | **0.0227-0.0718 ms** | **264.9-1462.6x** | **13.44 MiB** |
| C copied checkpoint | 1.10-1.80 ms | 1.80-4.01 ms | 10.54-16.69x | 59.00 MiB |
| D host context, fine/batched | <0.001 ms | <0.004 ms | access-only | 1.13 MiB guest |

Candidate A falsifies “persistence is enough.” Candidate B proves that local
edit provenance is decisive, but its duplicate guest file buffer approaches
the production 64 MiB ceiling. B2 keeps the same SDK model while moving only
the immutable byte source to the host. It was another 4.50-9.81x faster than B
and reduced guest high-water by 77.99-91.93%. Each warm edit made exactly two
range imports carrying 116-426 bytes. Its Wasm-owned index was 1.14 MiB for
50,000 entities and 4.58 MiB for 200,000 entities; cold streamed hydration took
20.35-26.15 ms. That hydration scans the generated file source to build the
index; it is not the required `open_entities` cold render from durable semantic
state.

The B2 prototype also stores absolute offsets in a flat vector. A
length-changing edit updates every following entity offset, so work is
`O(entities after the edit)` even though source I/O and semantic output are
local. The 0.070 ms CSV/text cells already include this 200,000-entity suffix
shift; they are not proof of asymptotically local indexing. Production needs a
relative-offset/interval tree (or equivalent piece-tree annotations) and tests
length-changing edits at the beginning, middle, and end of a 10 MiB file.

B2 does not authorize one full host `Vec` per client. The source implementation
must be an immutable rope/piece tree: a session acknowledgement forks the
shared source in constant time and a splice path-copies only changed pieces.
Guest document forks similarly share syntax/identity nodes inside the file
actor. Without that fan-out design, 32 sessions on a 10 MiB file would merely
move hundreds of MiB from guest to host and fail the memory gate.

Candidate C's latency clears the threshold, but it copies a 10.05-12.78 MiB
checkpoint into and out of Wasm on every edit. Persisting it would also rewrite
`O(document)` derived state, so it fails the bandwidth/storage architecture
despite a fast core-memory copy. Candidate D shows why range imports must be
batched: the fine lane made 123-433 calls, versus nine for batched access. Its
sub-microsecond timing excludes any storage lookup and cannot justify exposing
host KVs to authors.

All 105 cells preserved the target stable ID. V1/A/B/C verified complete
reconstructed bytes, B2 returned and verified the exact affected-entity bytes,
and D returned and verified only the affected-entity FNV hash. B2 and D
therefore do not establish whole-document reconstruction. The scanners
implement generated fixture grammars, not the complete production parsers.
Before acceptance, each real format must pass `open_entities` full cold-render
equality against warm canonical bytes after eviction, as well as relative-offset
locality and the full-engine gate. This is causal mechanism evidence, not that
acceptance result.

## Format source audit

### Text

Current text entities use UUIDv7 line IDs plus independent order keys. Detection
fully parses and globally matches sequence equalities, exact content, then
positional replacements. Normal invalidation is the edited line interval with
CRLF lookaround. A global content-hash→IDs multimap is needed to recognize a
distant move without hydrating rich line snapshots.

Correctness traps are terminal newline representation, duplicate lines,
LF/CRLF/CR root metadata, encoding canonicalization, reorder identity, and
deterministic order-key repair. Current text collapses mixed terminators to a
document-level choice; v2 must either store per-line terminators or explicitly
canonicalize on first ingest so eviction cannot change bytes.

### CSV

CSV also uses generated UUIDv7 row IDs plus order keys. The current plugin fully
parses, globally diffs, and then matches duplicate rows. Normal invalidation is
the affected record range, but a quoted multiline field or malformed quote may
expand until parser state resynchronizes or to EOF. Dialect/encoding/terminator
metadata is document-level.

The persistent index needs record boundaries, quote-state checkpoints,
IDs/order keys, hashes, and a global duplicate/move multimap. It does not need a
second copy of every rich row snapshot.

### Markdown

Markdown has UUID structural nodes, inline atom IDs, parent/order graphs, table
column references, compatible kind transitions, and global subtree move
matching. Its parser exposes source spans today, but projection discards them.
The v2 document must retain spans, syntax/subtree hashes, IDs, inline anchors,
and table references.

One paragraph edit normally invalidates one block; list, table, fence, or
container changes expand to the enclosing construct; definitions or malformed
syntax may force full resync. Output-local “one changed row” tests do not prove
computational locality.

### JSON

The current JSON primary key is an RFC 6901 pointer and arrays are compared by
numeric index. A front insertion therefore changes the apparent identity of
every suffix item. No incremental cache can correct that schema.

The required breaking model is:

- fixed root ID;
- opaque object-slot IDs matched by stable parent ID plus decoded property key;
- opaque array-item ID plus order key; and
- JSON Pointer retained as a derived locator, not an entity primary key.

Duplicate object keys are rejected rather than silently collapsed. The host
validates allocator scope/collisions and every emitted PK/schema/snapshot
correspondence.

Normal invalidation is the smallest enclosing value/container. Container to
scalar transitions and subtree deletion can legitimately emit many
tombstones.

### Excalidraw

There is no Excalidraw plugin in current main, so this research uses a contract
fixture rather than claiming regression parity. Excalidraw already has native
element IDs, fractional `index`, version fields, binding references, domain
`isDeleted`, and stable asset IDs.

A shape edit normally invalidates one top-level JSON object. Binding changes put
their referenced element facts in one merge group; transaction atomicity alone
would not prevent a per-entity LWW merge from selecting half the update. Large
base64 assets must be lazy or streamed so a shape update never copies unchanged
assets through Wasm.

### What stable identity can guarantee

No API can infer the intent behind swapping two byte-identical rows in an
ID-less file. The cross-format guarantee is therefore operational: primary keys
do not derive from byte offsets or array indices; unambiguous edits, moves, and
reorders preserve the matched ID; duplicate ambiguity is resolved
deterministically; and new IDs are retry-stable. Excalidraw's native IDs are
exact. JSON object slots are location-semantic (stable parent plus decoded key),
but their primary keys are opaque rather than derived path strings. A value
edit or property reorder preserves identity while a key rename is a
delete/insert. JSON array items use opaque IDs and independent order keys.
Duplicate object keys are rejected in v2; subtree moves preserve IDs only when
the format matcher has an unambiguous correspondence.

## API hypothesis tournament

### H1 — persistence without splice input

Candidate A retains an immutable document but receives/returns complete blobs.
It isolates repeated state/index hydration from edit localization. Expected:
clear win over v1, but `O(file bytes)` boundary traffic and change location
remain.

### H2 — persistent splices and render patches

Candidate B adds base-relative splices, lazy complete-result fallback, and
patch output. Expected: the largest warm localized win and bounded memory, with
the same fallback simplicity as Candidate A.

The first B control retained complete bytes in Wasm. B2 keeps the same author
facade but leaves immutable versioned bytes behind the host `Source`; the guest
retains only offsets, hashes, parser checkpoints, and semantic IDs. This
separates the API decision from the physical placement of the file buffer.

### H3 — pure copied checkpoint reducer

Candidate C makes rollback/crash reasoning trivial, but an identity/span index
crosses the boundary each edit. A monolithic checkpoint also rewrites
`O(document)` storage. Expected: strong AX score, weak large-file latency and
storage.

### H4 — host-owned transactional private KV

Candidate D minimizes retained guest memory and delegates rollback/persistence
to the host. It exposes index schema/versioning/compaction and may turn local
parser work into many host calls. It is selected only for a greater-than-20%
memory/eviction win without an equivalent AX or latency loss.

### H5 — P3 async range/entity sources

Large cold hydration can be streamed and lazy reads can overlap Slate/object
latency. Local warm changes should not require P3 at all. Expected: capacity and
cold-I/O benefit, no single-edit CPU latency benefit.

### H6 — per-file actors instead of one plugin Store mutex

Current main caches one instantiated component per plugin key/hash; its Store is
mutex-protected. Same-plugin files serialize and share one memory/failure
domain. The v2 host should cache compiled components, then use bounded actors
per active branch/path/file-incarnation/plugin-generation tuple (or a measured
pool), allowing cross-file and cross-branch parallelism while serializing one
branch/file renderer.

Required tests are eight concurrent same-plugin files, one slow plus seven tiny
files, and 1/8/32 retained sessions on one 10 MiB file.

## Controlled agent-experience evaluation

The API facades were evaluated with the supplied ax-eval v2 rubric. Each tested
agent received only the canonical one-line implementation prompt, a format task,
an isolated starter workspace, and one candidate API path. Runs were sequential:
only one workspace was visible below `runs/` at a time, then it was archived
before the next run. Metrics came from raw Codex JSONL rather than agent
self-report, and an independent judge read each complete transcript to decide
task success.

The deterministic final score is `0.30 friction + 0.25 speed + 0.20 efficiency + 0.25 error recovery`.
All 22 main-cohort runs and all three targeted refined runs needed zero
follow-ups and exposed zero tool-transport errors, so every run scored 100 for
friction and error recovery. Nonzero shell exits inside a successful Codex tool
response do not count as transport errors; that rubric field therefore does not
mean every implementation compiled on its first attempt. The remaining score
variation is elapsed time and tool-call count, not a subjective API grade.

| Candidate | Scored formats | N | Final scores | Mean | p25 / median / p75 | Independently judged success |
|---|---|---:|---|---:|---:|---:|
| A: persistent document + full blobs | Text, CSV, JSON, Markdown, Excalidraw | 5 | 93, 92, 90, 91, 92 | 91.6 | 91 / 92 / 92 | 5/5 (100%) |
| B: persistent document + splices/patches | Text ×2, CSV ×2, JSON ×2, Markdown ×2, Excalidraw | 9 | 92, 87, 92, 90, 89, 93, 91, 93, 86 | 90.3 | 89 / 91 / 92 | 8/9 (88.9%) |
| C: copied checkpoint reducer | Text, CSV, JSON, Markdown | 4 | 91, 91, 92, 92 | 91.5 | 91 / 91.5 / 92 | 4/4 (100%) |
| D: host transactional private KV | Text, CSV, JSON, Markdown | 4 | 90, 83, 85, 92 | 87.5 | 84.5 / 87.5 / 90.5 | 4/4 (100%) |

These cohorts do not establish an AX-score winner. A, C, and B are within 1.3
mean points, far below ax-eval's 10-point exploratory noise threshold. A, C,
and D are format screens smaller than the default N=10, while the finalist B
cohort stopped at N=9. Candidate B is selected by the separately measured Wasm
latency and memory result, not by this score table. The AX evidence says its
earlier persistent-document/splice facade remained about as straightforward to
implement as the full-blob and checkpoint controls. It does not measure the
final refined facade.

The post-review facade was then frozen and tested with three deliberately
harder, heterogeneous tasks:

| Refined task | Score | Duration / tool calls | Independent result |
|---|---:|---:|---:|
| Both cold directions, paging/scoped IDs, rename, malformed input | 86 | 210.1 s / 13 | pass (3 tests) |
| CSV reorder, complete order upserts, cold reconstruction, no warm full scan | 85 | 205.1 s / 15 | pass (1 test) |
| Excalidraw coupled group and lazy 4 MiB entity | 82 | 261.4 s / 16 | pass (3 tests) |

All three were independently judged successful; the targeted mean was 84.3,
median 85, and p25/p75 83.5/85.5. The Excalidraw task exercised 64 KiB lazy
reads/pages and lazy output, but its test implementation ultimately accumulated
the 4 MiB payload, so it is API-usage evidence rather than guest peak-memory
evidence. N=3 is exploratory and not statistically comparable with the earlier
N=9 B cohort.

The one valid correctness failure is important. B's first CSV agent preserved
row IDs during a reorder but emitted no committed order updates for unchanged
moved rows; it then weakened the reorder assertion until tests passed. The
judge correctly rejected the run because the resulting shared entity state
could not reproduce the submitted order. This is evidence against hiding order
inside optional generic metadata. A production change should make an upsert
carry the complete schema entity, including its order key, and represent delete
as a separate keyed variant. The optimized source/splice mechanism can remain
unchanged.

The post-cohort correctness review further added explicit byte/entity cold
constructors, per-change merge groups, precommit renderer validation, and
observation-selected private roots, and removed guest `Send`/`Sync`. The
refined facade and WIT compile/validate, and the targeted follow-up covers its
descriptors, scoped allocation, bounded pages/lazy output, complete upserts,
groups, and both cold constructors. It was not retroactively mixed into the
main cohort. A larger homogeneous follow-up still must cover aggregate-budget
errors, observation expiry/retry, generated bindings, and realistic error
recovery before the final API can be called frozen.

Excluded data was never scored: the initial A and B text tasks had a
contradictory terminal-newline fixture; early workspaces could see sibling
solutions; two C Excalidraw attempts were blocked by infrastructure write
limits; and the first B Excalidraw judge inspected the wrong live workspace
instead of judging the archived transcript. A clean second independent judge
for that same tested transcript passed it. C therefore has no valid
Excalidraw result, and no D Excalidraw run was completed; neither omission is
presented as format coverage.

The pinned `claude-opus-4-7` runner was unavailable. Every included tested and
judge agent used `gpt-5.6-sol` at low reasoning effort with `fork_turns=none`.
Codex does not expose temperature or a hard turn cap, so the schema's nominal
temperature 0 and max-turns 40 are explicitly marked as unverified overrides.
The Codex tool/system/sandbox substitutions and the inability to strip MCP
servers per agent are also recorded in every result; no MCP calls occurred.

Schema-valid compact results are checked in for [A](ax-eval/candidate-a-result.json),
[B](ax-eval/candidate-b-result.json), [C](ax-eval/candidate-c-result.json),
[D](ax-eval/candidate-d-result.json), and the
[targeted refined facade](ax-eval/candidate-b-refined-targeted-result.json).
Raw main-cohort tested/judge transcripts are archived under
`~/.ax-eval/lix-plugin-api-{a,b,c,d}/`; the targeted result records its frozen
source hashes and raw Codex rollout locations.

## Post-tournament API correctness review

An independent source-level review tried to execute the proposed lifecycle
against current engine durability, acknowledgement, and merge behavior. It
found that the measured B2 contained affected-entity mechanism is sound within
its generated fixtures, but does not cover suffix-index complexity, cold
rendering, or the engine lifecycle; the first API draft was not ready to
freeze. The refined compileable facade/WIT addresses these design requirements:

| Priority | Finding | Refined decision |
|---:|---|---|
| 1 | Plugin-backed durable state has entities but no raw blob after restart; initial import has the inverse shape | Separate `open_file -> document + entities` and `open_entities -> document + complete file` |
| 2 | One “latest session document” or a byte hash can select the wrong identity root for lost/out-of-order or byte-identical views | Opaque observation handles are session/branch/path/incarnation/generation-bound and address one exact source/root/document lease; no valid handle means reject an existing-file mutation, not “safe upserts” |
| 3 | Rendering after commit can leave committed state unreadable if the guest rejects/traps | Render and validate final resolved groups before storage commit, then publish disposable caches |
| 4 | Transaction atomicity does not preserve coupled facts under per-entity LWW | Validate authority per whole group and retain group provenance for deterministic all-or-none group LWW; singleton default; duplicate keys invalid |
| 5 | Guest/source sharing alone leaves current full-row session views at `O(sessions × entities)` | Structurally shared persistent host semantic roots with sparse path-copy |
| 6 | Generic “identity-only cold projection” cannot extract format fields from opaque snapshots | First release streams full entities on cold open; optimize only with measured sidecar/checkpoint evidence |
| 7 | Ephemeral spans can make warm bytes differ from cold bytes, while private losing/noncanonical views are not in durable shared state | Require shared semantic-state cold render to equal warm-patch canonical output; retain exact private source/root/document together or expire its observation on eviction |
| 8 | Empty files, path deletion/recreation, branches, and plugin upgrades cross document authority domains | Make whole-file deletion an explicit file-incarnation operation, allocate a fresh incarnation on recreation, keep one shared renderer per branch tuple, and revoke observations on generation migration |
| 9 | B2's generated-fixture flat offset vector shifts an entire suffix and never exercises `open_entities` | Require a relative-offset tree plus start/middle/end length-changing tests and complete cold-render validation with production parsers |
| 10 | Path/media-type/plugin selection can affect semantics without changing bytes | Supply before/after file descriptors and invoke rename-only transitions with zero splices |
| 11 | Unbounded entity/output cursors can evade per-call memory and deadline limits | Bound entity/change/edit pages and lazy byte output under one non-renewing aggregate transition budget |

This review also removed `Send + Sync` from guest documents and made allocation
explicitly name schema, composite-PK scope, and deterministic ordinal. The
checked wire facade uses bounded packet framing, bounded entity/edit pages, and
lazy input/output bytes, all charged to one top-level transition budget. These
changes are correctness/operability requirements; they do not borrow the B2
microbenchmark's speedup as evidence that the full engine is already accepted.

## Storage decision

Do not add Lix-owned packs of individual semantic KVs.

RocksDB already stores sorted KVs in compressed/cacheable blocks and has native
`MultiGet`; SlateDB stores blocks in SSTs with decoded block/metadata and raw
object-store caches. Another general packing layer risks point-read regression
and rewrite amplification—the failure mode raised in review.

The v2 path instead removes logical reads on warm operations. Cold hydration
uses:

- bounded full-entity streaming for cold `open_entities` in the first version;
- RocksDB native `MultiGet` for unavoidable sparse exact cache misses;
- SlateDB storage-native batching or bounded dense runs with an explicit
  over-read budget; and
- optional future identity projections/checkpoints only after independent
  latency and WAL/live-byte evidence.

A versioned transient packed arena between storage materialization and Wasm is
still useful if it is produced directly. Constructing today's rich row graph
and then packing it would retain most host cost.

## Correctness model

### Private detector and shared renderer versions

The author sees one `Document` type. The engine retains separate immutable
versions:

```text
session exact private document
  --file_changed(client splice)--> proposed private successor + semantic delta
                                      |
                                      v
                    engine validates whole-group authority
                    + resolves deterministic group LWW
                                      |
                                      v
shared branch document
  --entities_changed(resolved delta)--> validate shared successor + patch
                                      |
                                      v
                         storage commit + publish caches
```

The private successor represents the submitted bytes, not merged bytes the
client did not receive. Unchanged persistent rope/tree nodes may be shared.

### Observation and group authority

Private documents/semantic roots are retained in a bounded map addressed by an
opaque observation handle. That handle is bound to the session, workspace,
branch, path, file incarnation, and plugin generation, and selects one exact
immutable byte source, semantic root, and guest document. Only an
acknowledgement-safe unique-file response may carry one; aggregates,
transformed bytes, ambiguous joins, and broad scans do not. A read whose
network response was lost is not proof of receipt; the client SDK must possess
and echo the handle. The base/result hashes validate splice bytes but do not
select a semantic root, because byte-identical views can carry different entity
IDs. Document resources, hashes, and caches cannot invent authority.

Authority validation covers each complete group. A delete or update of an
existing key must name an entity in the observation-selected root. A new key
must be a validated native identity or come from that operation's retry-stable
allocator, and it must be absent from current shared state except for an
idempotent retry of that operation. A native-ID collision with an unseen
concurrent entity is rejected. One invalid member rejects the entire transition
rather than silently filtering the group. Without a valid observation there is
no generic safe-upsert path: whole-file parsing cannot recover existing IDs for
CSV, text, Markdown, or JSON arrays. Apart from explicit absent-path creation
and an exact canonical no-op, the engine returns a retryable reread/`410`
before calling the plugin. The handle remains hidden transport provenance, not
a `baseCommitId`, CRDT API, or client-managed commit state; ordinary clients
still read and write blobs.

The host semantic root is a structurally shared persistent map, not today's
full row vector. A read retains a root in `O(1)` and sparse submissions
path-copy only changed keys. Otherwise guest/source sharing would leave the
host's `O(sessions × entities)` acknowledgement fan-out untouched.

### Abort, trap, retry, and eviction

- The SDK borrows the accepted resource immutably.
- A call returns a distinct owned successor.
- The private transition proposes changes; the engine resolves them and invokes
  the shared renderer before storage commit.
- Storage commit publishes the shared successor and installs the next private
  source/root/document lease with non-failing pointer swaps; only then may the
  response carry its observation. Abort drops successors.
- Abort, failed validation, trap, or uncertain completion evicts the accepted
  resource as well. A malicious guest can bypass the SDK and mutate it
  internally, so guest immutability cannot be rollback authority. A shared
  renderer reopens from durable entities; a private observation is revoked.
- `open_file` is for an explicit new incarnation. `open_entities` cold-opens
  only a canonical shared durable semantic root, validating plugin hash, ABI,
  branch, and incarnation. It cannot recreate a private noncanonical or losing
  view; its exact source/root/document must remain leased together or fail
  closed after eviction.
- If derived-cache publication is unavailable after durable commit, the commit
  remains successful. The shared renderer cold-opens from the new durable root,
  no incomplete private observation is issued, and the client rereads.
- The host binds allocation to operation identity, file incarnation, and plugin
  generation. Each call names schema, composite-PK scope, and a deterministic
  ordinal; the result appends one opaque component to the scope. Retries return
  the same ID without depending on allocation call order.

### Merge groups and cold renderability

Transaction atomicity does not make coupled entity facts merge atomically.
Every returned change belongs to a merge group; singleton is the default, while
authority and conflicts are resolved for all members together. Every candidate
group has one deterministic engine LWW rank. For overlapping concurrent groups,
the resolver considers highest rank first and accepts a group only if all of
its keys are unclaimed; otherwise none of it contributes. Durable group
provenance lets a higher-ranked arrival displace every member of an older group
and restore prior values for that group's other keys, making results independent
of arrival order. Empty groups, duplicate keys, partial upserts, mismatched
schema/PK fields, invalid packet bounds, and upsert/delete duplicates are
rejected before plugin output reaches storage.

Warm patches are a cache optimization, not hidden durable state. Applying the
returned semantic changes and then cold-rendering those entities must reproduce
the same canonical bytes. This requires durable Markdown format fields and a
defined text policy for mixed line terminators (store per-line terminators or
canonicalize explicitly on ingest).

The measured B2 index does not clear this gate: it uses flat absolute offsets,
so length-changing edits shift an `O(N)` suffix, and its cold hydration scans a
file source rather than rendering from durable entities. Production ports need
a relative-offset/interval tree, start/middle/end length-changing edit tests,
and full `open_entities` equality after eviction for every format.

### Whole-file, branch, and generation lifecycle

- Empty bytes represent a live empty file. Explicit file deletion is an engine
  operation requiring a current-incarnation observation; it competes with
  concurrent writes under one file-level deterministic LWW rank and atomically
  tombstones the winning incarnation rather than synthesizing plugin changes.
- Recreating the path allocates a fresh incarnation. No old observation,
  allocator scope, entity ID, byte source, or guest resource crosses into it.
- Rename serializes the source and destination path slots, validates an
  old-path observation, preserves the incarnation, and invokes the plugin with
  before/after descriptors even for zero byte splices. Commit revokes old-path
  observations and creates only destination-bound successors.
- One shared renderer and serialization queue exists per
  `(workspace, branch, path, incarnation, plugin generation)`. Branch forks may
  share immutable storage roots, but never a mutable renderer pointer.
- A plugin-generation upgrade stops the actor and revokes old observations. A
  schema-compatible generation must cold-open durable entities and validate its
  complete render before an atomic swap. Schema/identity changes require an
  explicit migration transaction; silent `open_file` reparsing is forbidden.

## Reproduction entry points

The new research package is in `experiments/plugin-api-v2`:

```sh
# Real-Wasm API mechanism matrix
cargo build --manifest-path experiments/plugin-api-v2/Cargo.toml \
  -p plugin-api-v2-guest --target wasm32-unknown-unknown --release
cargo run --manifest-path experiments/plugin-api-v2/Cargo.toml \
  -p plugin-api-v2-host --release -- --iterations 30

# Compileable API facades
cargo test --manifest-path \
  experiments/plugin-api-v2/api-candidates/Cargo.toml

# Deterministic AX adapter
python3 -m unittest discover \
  -s experiments/plugin-api-v2/ax-eval/tests -v
```

The latest-main real CSV Wasm/full-engine harness is
`packages/rs-sdk/benches/profile_merge_10k.rs`. Its report commands and backend
cache policy are recorded beside the result artifacts after the run.

## Primary external references

- [WASI 0.3 launch](https://bytecodealliance.org/articles/WASI-0.3)
- [Component Model 1.0 roadmap and current synchronous-call overhead](https://bytecodealliance.org/articles/the-road-to-component-model-1-0)
- [Component Model Canonical ABI](https://component-model.bytecodealliance.org/advanced/canonical-abi.html)
- [WIT resources](https://component-model.bytecodealliance.org/design/wit.html#resources)
- [Wasmtime guest-backed lists](https://docs.wasmtime.dev/api/wasmtime/component/struct.WasmList.html)
- [Wasmtime fast instantiation](https://docs.wasmtime.dev/examples-fast-instantiation.html)
- [Tree-sitter incremental edit and structural sharing](https://tree-sitter.github.io/tree-sitter/using-parsers/3-advanced-parsing.html)
- [RocksDB MultiGet](https://github.com/facebook/rocksdb/wiki/MultiGet-Performance)
- [RocksDB block cache](https://github.com/facebook/rocksdb/wiki/Block-Cache)
- [SlateDB reads](https://slatedb.io/docs/design/reads/)
- [SlateDB cache layers](https://slatedb.io/docs/design/caching/)
- [Excalidraw element fields](https://github.com/excalidraw/excalidraw/blob/53732f08f430ded353121c64c230b448282be37a/packages/element/src/types.ts#L42-L82)
- [Excalidraw serialization](https://github.com/excalidraw/excalidraw/blob/53732f08f430ded353121c64c230b448282be37a/packages/excalidraw/data/json.ts#L26-L74)
