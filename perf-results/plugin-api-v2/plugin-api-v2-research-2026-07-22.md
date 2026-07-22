# Incremental Wasm plugin API research

Date: 2026-07-22  
Base commit: `26a38c67` (`origin/main`)  
Branch: `codex/wasm-plugin-api-research`  
Host: Apple M5 Pro, 18 cores, 64 GiB RAM, macOS 26.3.1 arm64  
Toolchain: Rust nightly 1.97, Wasmtime 45, `wasm32-unknown-unknown` and
`wasm32-wasip2`

## Executive result

The architectural insight is narrower than “make plugins stateful”:

> A warm file write must send only the byte edit to an exact private document
> version, and a warm render must send only final committed entity changes to
> an exact shared document version.

The engine must stop loading and lowering every active entity before those
calls. Wasm remains the isolation boundary.

The proposed API is one immutable `Document` with two symmetric transitions.
Complete immutable bytes remain behind a host-owned range source; Wasm retains
the format's syntax/identity index:

```rust
file_changed(before_source, after_source, byte_splices)
  -> (successor_document, atomic_entity_changes)
entities_changed(before_source, committed_entity_changes)
  -> (successor_document, byte_splices)
```

The SDK includes full-parse and full-replace fallbacks in those same calls. It
does not expose sessions, branches, acknowledgements, revisions, commits,
prepare/accept/abort, storage KVs, or CRDT state to plugin authors. The engine
keeps accepted resources alive until commit; an abort just drops the immutable
successor.

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
- uncontested visibility fast path (`9600c9ca`); and
- remote request/observation blob splices (`26a38c67`).

This matters because old flamegraphs identified visibility maps and repeated
acknowledgement clones as large costs; those low-risk fixes are no longer
available to inflate the v2 result.

Remote v3 now already computes a validated single splice. The JS client sends
base/result SHA-256, prefix length, suffix length, and inserted bytes. The
server validates and reconstructs the full `Value::Blob`, then discards the
splice metadata before SQL/plugin reconciliation. Preserving that provenance
is therefore plumbing, not a new client API.

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
  -> private document reparses affected grammatical closure
  -> sparse semantic changes
  -> engine merge + commit
  -> final sparse changes update shared document
  -> rendered byte patch
```

Cold open may still be `O(document)` and may read an identity-only projection.
Warm one-entity work must not. An optional exact-root checkpoint can reduce cold
work, but is disposable and cannot become commit or delete authority.

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
20.35-26.15 ms.

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

All 105 cells preserved the target stable ID and verified exact reconstructed
bytes (or D's exact affected-entity result). The scanners implement the
generated fixture grammars, not the complete production parsers; this is causal
mechanism evidence, not the full-engine acceptance result.

## Format source audit

### Text

Current text entities use UUIDv7 line IDs plus independent order keys. Detection
fully parses and globally matches sequence equalities, exact content, then
positional replacements. Normal invalidation is the edited line interval with
CRLF lookaround. A global content-hash→IDs multimap is needed to recognize a
distant move without hydrating rich line snapshots.

Correctness traps are terminal newline representation, duplicate lines,
LF/CRLF/CR root metadata, encoding canonicalization, reorder identity, and
deterministic order-key repair.

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
- object-slot ID derived from stable parent ID plus decoded property key;
- opaque array-item ID plus order key; and
- JSON Pointer retained as a derived locator, not an entity primary key.

Normal invalidation is the smallest enclosing value/container. Container to
scalar transitions and subtree deletion can legitimately emit many
tombstones.

### Excalidraw

There is no Excalidraw plugin in current main, so this research uses a contract
fixture rather than claiming regression parity. Excalidraw already has native
element IDs, fractional `index`, version fields, binding references, domain
`isDeleted`, and stable asset IDs.

A shape edit normally invalidates one top-level JSON object. Binding changes may
atomically touch multiple referenced elements. Large base64 assets must be lazy
or streamed so a shape update never copies unchanged assets through Wasm.

### What stable identity can guarantee

No API can infer the intent behind swapping two byte-identical rows in an
ID-less file. The cross-format guarantee is therefore operational: primary keys
do not derive from byte offsets or array indices; unambiguous edits, moves, and
reorders preserve the matched ID; duplicate ambiguity is resolved
deterministically; and new IDs are retry-stable. Excalidraw's native IDs are
exact. JSON object slots are location-semantic (stable parent plus decoded key),
so a value edit or property reorder preserves identity while a key rename is a
delete/insert. JSON array items use opaque IDs and independent order keys.

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
per active file incarnation (or a measured pool), allowing cross-file
parallelism while serializing one file.

Required tests are eight concurrent same-plugin files, one slow plus seven tiny
files, and 1/8/32 retained sessions on one 10 MiB file.

## Storage decision

Do not add Lix-owned packs of individual semantic KVs.

RocksDB already stores sorted KVs in compressed/cacheable blocks and has native
`MultiGet`; SlateDB stores blocks in SSTs with decoded block/metadata and raw
object-store caches. Another general packing layer risks point-read regression
and rewrite amplification—the failure mode raised in review.

The v2 path instead removes logical reads on warm operations. Cold hydration
uses:

- identity-only projection when full rich payloads are unnecessary;
- RocksDB native `MultiGet` for sparse exact keys;
- SlateDB storage-native batching or bounded dense runs with an explicit
  over-read budget; and
- optional derived checkpoints measured independently for WAL/live bytes.

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
                              engine merge + commit
                                      |
                                      v
shared branch document
  --entities_changed(final delta)--> shared successor + rendered patch
```

The private successor represents the submitted bytes, not merged bytes the
client did not receive. Unchanged persistent rope/tree nodes may be shared.

### Delete authority

Only the engine's exact acknowledged private identity root grants deletion
authority. Every guest tombstone is filtered against that root. A document
resource, checkpoint, or plugin claim cannot invent authority. On missing exact
state after restart/eviction/routing, the existing process-local session policy
fails closed.

### Abort, trap, retry, and eviction

- The SDK borrows the accepted resource immutably.
- A call returns a distinct owned successor.
- Storage commit swaps the cache pointer; abort drops the successor.
- Abort, failed validation, trap, or uncertain completion evicts the accepted
  resource as well. A malicious guest can bypass the SDK and mutate it
  internally, so guest immutability cannot be rollback authority.
- Reopen validates plugin hash, ABI, file incarnation, and semantic root.
- New IDs are host-allocated from operation identity plus ordinal so retries
  return the same IDs.

## Reproduction entry points

The new research package is in `experiments/plugin-api-v2`:

```sh
# Real-Wasm API mechanism matrix
cargo build --manifest-path experiments/plugin-api-v2/Cargo.toml \
  -p plugin-api-v2-guest --target wasm32-unknown-unknown --release
cargo run --manifest-path experiments/plugin-api-v2/Cargo.toml \
  -p plugin-api-v2-host --release -- --iterations 31

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
