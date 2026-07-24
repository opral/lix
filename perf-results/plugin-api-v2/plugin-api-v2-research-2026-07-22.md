# Incremental Wasm plugin API research

Date: 2026-07-22
Base commit: `e1a57ec3` (`origin/main` when the final baseline was recorded)
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
calls. Wasm remains the host memory/capability/resource boundary; plugin
semantic correctness is trusted within the files it owns.

The proposed API is one immutable `Document` with two symmetric warm
transitions plus explicit constructors for the two possible cold directions.
Complete immutable bytes remain behind a host-owned range source; Wasm retains
the format's syntax/identity index:

```rust
open_file(descriptor, file_source, inline_ids)
  -> (document, initial_entity_group_pages)
open_entities(descriptor, bounded_entity_pages)
  -> (document, complete_file_edits)
file_changed(before_descriptor, after_descriptor, before_source, after_source, byte_splices)
  -> (successor_document, entity_merge_group_pages)
entities_changed(before_descriptor, after_descriptor, before_source,
                 resolved_entity_group_pages, activated_entity_pages,
                 prospective_entity_pages)
  -> (successor_document, byte_splices)
```

Initial import is deliberately a two-step host protocol, not
`open_file -> commit`. `open_file` creates a **provisional document phase**:
neither its document nor its raw bytes are the shared canonical state. The
engine drains and validates the initial groups,
resolves them against the empty incarnation, and then unconditionally calls
`entities_changed` on the returned document with the uploaded raw source,
the final resolved groups, and the complete prospective state. The resulting
successor and validated edits define the canonical shared bytes—even when the
resolved group stream is empty. The engine publishes that canonical successor
as the shared renderer. If the mutation response actually returns those bytes,
the session may receive a matching observation. Otherwise it may retain only a
bounded private continuation bound to its submitted raw bytes/document and its
existing-or-newly-authored authority, or require a reread; it must not treat
unseen canonical bytes as acknowledged. A plugin must therefore either store
every render-effective syntax choice durably or explicitly canonicalize it
during this first render.

Most merge groups contain one complete upsert or keyed delete. Coupled facts,
such as both sides of an Excalidraw binding, may share a group. Authority is
validated for the complete group and deterministic group-level LWW selects all
of it or none of it. Order, parentage, and references live in complete schema
snapshots rather than optional transport metadata.

A typed `FormatOnly` effect still carries a changed complete durable snapshot.
It is only a conflict/notification/incremental-render classification; it is not
ephemeral metadata. An effect-only upsert with an unchanged snapshot is rejected
so warm patch output and a later cold render cannot diverge.

Sparse semantic output is inline; initial import and other broad transitions
use bounded lazy pages. A 200,000-row `open_file` therefore need not collect
200,000 complete upserts in guest memory before the host can drain them. The
host validates duplicate keys and group/page bounds across the whole cursor,
not just within one page.

All three renderer semantic inputs are stateful bounded sources. The change
source carries final merge-resolved groups; the hydration source carries
sorted/unique newly activated prospective entities not already present as
complete upserts; and the complete fallback carries the prospective after-state
obtained by applying those groups, transaction-local and before commit. A
simple renderer consumes the fallback, while an incremental renderer normally
consumes only the small change and activation sources.

Descriptors contain path, media type, and the host-selected content-addressed
plugin generation; a rename-only transition is delivered even with zero byte
splices. Cold/full entity access is bounded and paged. Large inserted or
rendered bytes stay behind lazy sources/outputs. The host passes one compact
retry-stable allocation namespace inline per file transition; the SDK derives
one-component IDs from deterministic ordinals locally with zero allocator
imports. At the WIT layer one
aggregate record/page/page-count/total-byte/deadline budget covers the
top-level call and all subsequent cursor/output draining; resource calls cannot
renew it.

The SDK includes full-parse and full-replace fallbacks in those same calls. It
does not expose sessions, branches, acknowledgements, revisions, commits,
prepare/accept/abort, storage KVs, or CRDT state to plugin authors. The engine
keeps accepted resources alive until commit. It renders and validates the final
resolved graph before durable commit. A deterministic rejection or known
storage abort drops the unaccepted successor while retaining the accepted
resource under the trusted immutability contract; a trap or uncertain guest
completion retires the entire actor. A shared renderer can reopen from its
durable branch root; a noncanonical private view cannot, so an actor retirement
expires its observation and the session must reread.

An earlier refined facade snapshot was produced after the main controlled AX
cohort. A targeted N=3 follow-up on those frozen signatures independently passed
lifecycle/cold-open/rename, CSV reorder/cold reconstruction, and Excalidraw
coupled-group/lazy-large-entity tasks. Its median score was 85, versus 91 for
the earlier B cohort, but the tasks and sample sizes differ and cannot support
a comparative ergonomics claim. A final evidence audit then aligned the Rust
sketch with WIT by making broad semantic output and resolved input lazy,
statefully consuming entity/change sources, validating keys and edits across
pages, capping splices before lowering, defining the prospective-state fallback,
forbidding warm plugin reselection, and removing a facade-only exact lookup.
Those changes were not part of the N=3 score. Observation expiry,
aggregate-budget failure, production generated bindings, and realistic retry
recovery still need a larger cohort. One fresh final-aligned implementer later
passed the evaluated frozen signatures with a score of 87 and 5/5 acceptance tests,
including a 200,000-row paged initial import. That N=1 run is a signature and
usability check, not a comparative ergonomics cohort.

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

Delivery is intentionally split. This first pull request contains the RFC,
executable API/protocol sketches, mechanism probes, AX evidence, exact-main
profiles, and acceptance design; it changes no production file behavior. A
hidden SDK profiling feature, large-file benchmark, and ABI-probe workspace do
expand the opt-in build/CI surface; they are not default runtime semantics. A
second pull request is stacked on it for the production CSV vertical slice. It
must eliminate 220,092 change-payload requests and 220,089 returned-payload
materializations on both the private detector and shared renderer/filesystem-
sync paths, pass the production 64 MiB limit, and clear the preregistered
greater-than-20% p50 **and** p95 gates on RocksDB filesystem and cached SlateDB
before the CSV slice is accepted for review.

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

The earlier `c789a2b1` baseline additionally included the exact `lix_file`
point-read fast path, coalesced/shared observation results, chunked remote blob
comparisons, one DataFusion write session and one provider set per transaction,
SQL-provider pruning, fixed-schema read elision, large RocksDB read-buffer
reuse, lazy SlateDB range-delete snapshots, and CAS dedupe presence markers.
The `5ffab346` baseline additionally includes RocksDB filtering for missing
point reads, lower CAS read-copy amplification, and registering only the
write-target provider. The final rebase at `e1a57ec3` then skips unused durable
function state (`684e6b5d`) and shares the RocksDB adapter with
`LocalFilesystem` (`e1a57ec3`). Because both touch the measured path, the full
engine timing, logical-I/O, and RocksDB **and** SlateDB profiles below were rerun
at exact e1. Older baselines are historical comparisons.

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
  cache budgets as Lixray: 64 MiB / 4 MiB / 1 MiB per workspace. This is a
  controlled algorithmic lane; remote object-store latency/miss/retry/tail
  validation is separate and required before a production Lixray claim.
- Cacheless SlateDB is diagnostic only.
- SQLite is excluded from every performance conclusion.

### Statistical gate

The eventual production integration A/B will use deterministic fixtures and a
sample count preregistered from a separate pilot: at least 12 paired
fresh-process blocks, with the minimum split exactly six in baseline/candidate
order and six in candidate/baseline order. A pilot-selected larger even count is
preregistered and exactly counterbalanced. Each block uses at least five
warmups followed by 20 measured warm operations per arm (at least 240
observations per cell at the minimum). The
primary latency operation is one localized ordinary-SQL blob update. Results
remain stratified by backend and format. For p50 and p95 separately, compute
each format's candidate/baseline ratio and the geometric mean of the eligible
format ratios per backend. Excalidraw enters that aggregate only after it has an
algorithm-identical stateless control; until then it is a separately reported
guard cell.

A candidate passes only when the one-sided 95% upper bound of the aggregate
ratio is below `0.80` for both p50 and p95, at least four of five per-format
point ratios are below `0.80` for both, and the corresponding one-sided upper
bound for every guarded cell is at most `1.05` p50 and `1.10` p95. Exact render
is guarded; a render-specific optimization must separately clear the same
greater-than-20% aggregate rule. Both latency arms use the same diagnostic
guest-memory cap, and the candidate must separately pass the production 64 MiB
limit.

Intervals use a fixed-seed (`0x4c495832`) 10,000-draw percentile hierarchical
cluster bootstrap of log ratios: resample paired process blocks, then warm
observations within each selected arm/block, recompute pooled p50/p95 ratios,
and exponentiate the geometric-mean upper bound. The paired fresh process, not
an individual warm call, is the independent experimental unit.

The stacked CSV production slice uses the same preregistered paired design, but
its decision unit is the CSV cell: its one-sided upper candidate/baseline bound
must be below `0.80` for both p50 and p95 on each backend, with the same render,
correctness, and 64 MiB guards. Passing that gate makes the CSV implementation
reviewable; it does not substitute one format for the later five-format
aggregate required to freeze and roll out the API generally.

The mechanism matrix below uses 30 warm samples per cell, while the expensive
latest-main discovery baseline uses one serial N=11 run per backend. They are
diagnostic and cannot accept a production architecture. Fixture generation,
backend open, and cold plugin compile stay outside warm timers. A final slow
Slate A/B uses the same 12 paired fresh-process blocks; cold compile, cold
hydrate, and first edit are reported separately.

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

The isolated persistent-CSV prototype was 7.0x p50 across its host scan, guest
call, and host materialization interval, and 333x at the guest apply boundary.
Cold hydration was 15.5 ms. It supported one contained, same-length CSV cell
edit; it did not prove full-engine behavior, format correctness, storage
composition, or session lifecycle. The complete standalone harness, controls, commands, and
limitations are checked in at
[`experiments/persistent-csv-wasm`](../../experiments/persistent-csv-wasm/README.md).

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

The real Component guests, WIT, host harness, and full retained matrix are
checked in under [`packages/plugin-abi-bench`](../../packages/plugin-abi-bench/README.md)
and [`prior-probes/packed-abi-component-matrix-2026-07-21.md`](./prior-probes/packed-abi-component-matrix-2026-07-21.md).

### P3 streams

A real async Canonical ABI stream probe over 10 MiB found:

| Input | Checksum p50 / p95 | Largest guest memory |
|---|---:|---:|
| `list<u8>` | 2.692 / 2.765 ms | 11.062 MiB |
| `stream<u8>`, 64 KiB chunks | 2.718 / 2.804 ms | 1.125 MiB |
| `stream<u8>`, 1 MiB chunks | 2.686 / 2.820 ms | 2.062 MiB |

P3 was latency-neutral and reduced the largest single guest linear-memory size
81-90%. Eight-KiB chunks
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

The async Component ABI guest/host source, raw TSV, and retained result are in
[`experiments/p3-stream-probe`](../../experiments/p3-stream-probe/RESULTS.md).

## Exact-e1 full-engine v1 baseline

The production-path baseline uses the real CSV Wasm plugin through an ordinary
acknowledged SQL blob update. The fixture has 220,000 rows and is exactly
10,680,000 bytes (10.68 MB / 10.19 MiB). It changes one field in the middle
row and alternates the input so all 11 measured writes are real transitions.
RocksDB is measured through production `LocalFilesystem`; SlateDB runs over a
local object store with Lixray's 64 MiB disk / 4 MiB block / 1 MiB metadata
cache budgets. The Slate lane is controlled algorithmic evidence, not a claim
about remote-object-store latency, misses, retries, or tails. SQLite was neither
built nor run.

The exact-`e1a57ec3` diagnostic timings use a reported 256 MiB guest cap,
including the second engine used by `LocalFilesystem` synchronization. The
prior `5ffab346` run failed initial import at the production 64 MiB cap on both
backends; exact e1 did not repeat that capacity cell, so the historical failure
is not relabeled as a current measurement. Raising the limit is not proposed.

After this immutable capture, `origin/main` advanced to `4aac696c`; its
`35f7e1c6` parent moves DataFusion result scalars instead of cloning them and can
especially affect blob-return/render cost. The e1 capture remains bottleneck-
discovery evidence, not PR2's acceptance baseline. PR2 must rebuild and measure
both paired arms from the same then-current main commit.

| Storage lane | Cold write | One-row write p50 / p95 | Exact render p50 / p95 | Write-process max RSS |
|---|---:|---:|---:|---:|
| RocksDB filesystem | 5,841 ms | 2,568 / 2,718 ms | 855 / 960 ms | 2.43 GiB |
| Cached SlateDB | 4,625 ms | 3,836 / 4,412 ms | 4,080 / 4,374 ms | 1.69 GiB |

A separately instrumented fresh-state single-row write requested 226,318 keys
through RocksDB filesystem and 226,283 through cached SlateDB. Both requested
exactly 220,092 change-payload keys and returned 220,089 payload values. Versus
`5ffab346`, only 21 RocksDB and 31 SlateDB tracked-tree history keys disappeared;
every non-tree semantic space is unchanged. These counters observe the outer
Lix semantic store, not the second RocksDB engine used by filesystem sync.

Initial directory snapshots were 75.42 MB for the complete RocksDB filesystem
(7.062x the source) and 193.08 MB for cached SlateDB including its cache
(18.079x); SlateDB's durable object store was 128.91 MB (12.070x). Those are
gross one-workspace retained footprints, not marginal per-file amplification:
they include the materialized source, installed plugin/fixed metadata,
cache/WAL/manifest/compaction state, and one snapshot. A production storage gate
must subtract empty and plugin-only controls and measure repeated quiesced live
bytes plus physical WAL/write counters.

Relative to the clean exact-`5ffab346` run, recurring medians changed by less
than 7%: RocksDB edit +2.59%, RocksDB render -5.06%, SlateDB edit -6.87%, and
SlateDB render -1.98%. None crosses 10%, let alone the greater-than-20% gate.
With N=11, nearest-rank p95 is the maximum and remains diagnostic. Cold setup is
one variable sample and is not used to rank the architecture.

The latest clean RocksDB Samply phase marker was optimized out, so it is
**whole-process active-sample attribution**, including reopen, plugin prewarm,
the acknowledged render, the timed write, and close/flush. Inclusive shares
overlap and cannot be summed or read as isolated write-phase wall time. Within
that scope, RocksDB stacks include complete render in 69.12%, tracked-state scan
in 43.98%, row materialization in 37.22%, the separate filesystem-sync thread
in 36.45%, change-record loading in 25.38%, and RocksDB `MultiGet` in 15.66%.
Wasm component render appears in 19.12%, while the detect wrapper/guest appear
in 6.36%/5.97%.

The matching exact-e1 SlateDB capture includes `get_snapshot_values` in 34.94%
of active samples, fetch scheduling in 20.96%, plugin reconciliation in 20.58%,
SST initialization in 18.69%, block advancement in 12.48%, and object
`read_range` in 9.94%. Its detect wrapper/guest appear in 4.38%/4.07%. Shares
overlap, async work spans threads, and profile denominators differ, so this is
hypothesis ranking rather than an elapsed-time decomposition. The classifier
uses exact blocking leaves, not `poll`/`park` substrings; total and active views
plus unresolved-system-symbol limits are retained in the exact baseline report. Together with the
independent 226k-key counters, both profiles locate the big lever in whole-state
host/storage/render work rather than the isolated guest call alone.

This produces the following evidence-ranked implementation targets. The
mechanism ratios and whole-process profile shares are diagnostic headroom, not
additive end-to-end forecasts.

| Rank | Target | Exact evidence | Required acceptance evidence |
|---:|---|---|---|
| 1 | Integrate B2 with observation-selected sparse host roots and a relative-offset document tree | One-row writes request 226,318 RocksDB-filesystem / 226,283 cached-SlateDB keys. Isolated B2 p50 is 0.0126-0.0710 ms, an observed 264.9-1462.6x over its deliberately optimistic v1 mechanism control, with 77.99-91.93% lower guest high-water than B—but host successor construction was outside the timer. | The defined localized-SQL-update p50 **and** p95 aggregate gate must clear >20% on both backends, plus 64 MiB, stable-identity, actor lifecycle, and complete cold-render tests. |
| 2 | Add adaptive SlateDB batched/dense-run reads after the warm path stops requesting all state | Current Slate p50 is 3,836 ms/edit and 4,080 ms/render; one-row edit still requests 220,092 change-payload keys, and `get_snapshot_values` appears in 34.94% of whole-process active samples. | >20% full-engine update win under the defined gate with a bounded sparse-key over-read budget and separate remote-object-store validation. |
| 3 | Reuse the precommit-validated renderer splice/materialization in `LocalFilesystem` | Rocks exact-render p50 is 855 ms; the filesystem-sync thread appears in 36.45% of whole-process active samples. | The defined >20% render-specific RocksDB-filesystem gate, byte equality, and unchanged commit/acknowledgement ordering. |
| 4 | Produce the packed transient Component packet directly from sparse state | Rich-record versus arena probes are 6.25-84.0x faster. The 218,454-entity cases reduce guest peak from 64.88 to 39.32 MB and 63.11 to 28.84 MB; file bytes alone are 0.95x. | >20% after sparse retrieval. Building today's rich rows and then packing them fails the intent. |
| 5 | Adopt P3 streams for cold and large transfers only | 10 MiB streams reduce the largest single guest linear-memory size 81-90%, while p50 remains 2.686-2.718 ms versus 2.692 ms for `list<u8>`. | Capacity/backpressure benefit with no >5% warm-call regression; no localized-edit latency claim. |

Persistence with complete blobs (A) is rejected at 1.00-1.05x. Copied
checkpoints (C) are rejected despite 10.54-16.69x core-Wasm latency because
10.04-12.78 MiB crosses each direction per edit. Host-KV Candidate D has no
storage latency in its access-only result. A generic Lix-owned packed-page
layer is not a target: RocksDB and SlateDB already pack physical blocks, and
the measured problem is logical whole-state retrieval/materialization.

The complete raw samples, RSS, on-disk decomposition, logical counters,
profiles, and reproduction commands are in
[`full-engine-v1-baseline-e1a57ec3.md`](./full-engine-v1-baseline-e1a57ec3.md).
The [`5ffab346`](./full-engine-v1-baseline-5ffab346.md),
[`c789a2b1`](./full-engine-v1-baseline-c789a2b1.md), and
[`66ad14da`](./full-engine-v1-baseline-66ad14da.md) reports/profiles are
historical comparisons only.
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

The 264.9-1462.6x range is the observed N=30 p50 ratio for this isolated guest
mechanism, not a 265-1463x Lix write-speed claim. If B2 only eliminated the
current Wasmtime `detect_changes` work, its 5.97% inclusive share in the exact-e1
RocksDB whole-process profile would produce about 1.06x under a naive serial
Amdahl calculation. That number is not a valid ceiling: the profile share is
overlapping, inclusive, and sampled across threads rather than an elapsed-time
phase decomposition. It only illustrates the distinction. The architecture
becomes a large full-system win if sparse roots also remove the 220,092
requested change-payload keys, materialization, and full renders; that
integrated result has not yet been measured.

Candidate A falsifies “persistence is enough.” Candidate B proves that local
edit provenance is decisive, but its duplicate guest file buffer approaches
the production 64 MiB ceiling. B2 keeps the same SDK model while moving only
the immutable byte source to the host. It was another 4.50-9.81x faster than B
and reduced guest high-water by 77.99-91.93%. Each warm edit made exactly two
range imports carrying 116-426 bytes. Its Wasm-owned index was 1.14 MiB for
50,000 entities and 4.58 MiB for 200,000 entities; cold streamed hydration took
20.35-26.15 ms. Those hydration figures are one initialization observation per
format/size cell (`N=1`), not a latency distribution. Hydration scans the
generated file source to build the index; it is not the required
`open_entities` cold render from durable semantic state.

The host prebuilt and installed B2's immutable `before`/`after` `Arc` sources
and import context before starting the call timer. The measured interval covers
guest allocation/copy, Wasm, range imports, result copy, and deallocation, but
not construction of an immutable successor source, rope/tree path copying, or
root publication. The 4.50-9.81x B2-over-B ratio therefore isolates removal of
the retained guest blob; it is not an end-to-end successor-construction ratio.

The B2 prototype also stores absolute offsets in a flat vector. A
length-changing edit updates every following entity offset, so work is
`O(entities after the edit)` even though source I/O and semantic output are
local. The 0.070 ms CSV/text cells already include this 200,000-entity suffix
shift; they are not proof of asymptotically local indexing. Production needs a
relative-offset/interval tree (or equivalent piece-tree annotations) and tests
length-changing edits at the beginning, middle, and end of a 10 MiB file.

B and B2 also mutate one thread-local guest document/index in place. The timed
call does not allocate an immutable successor, retain accepted and successor
versions through commit, exercise abort, or measure structural sharing across
1/8/32 sessions. The result proves the sparse byte/source/scanner mechanism,
not the complete immutable lifecycle cost. Production acceptance therefore
requires an immutable relative-offset tree with source construction and
path-copy accounting inside the timer, abort tests, and retained-session
RSS/storage measurements; “the warm mechanism is unchanged” means its data
flow, not that the benchmark already implements the proposed ownership model.

B2 does not authorize one full host `Vec` per client. The source implementation
must be an immutable rope/piece tree: a session acknowledgement forks the
shared source in constant time and a splice path-copies only changed pieces.
Guest document forks similarly share syntax/identity nodes inside the file
actor. Without that fan-out design, 32 sessions on a 10 MiB file would merely
move hundreds of MiB from guest to host and fail the memory gate.

Candidate C's latency clears the threshold, but it copies a 10.04-12.78 MiB
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
expand until parser state resynchronizes or to EOF. Delimiter, quote, and one
detected terminator are document-level; source encoding/BOM and mixed
terminators are currently discarded during decode and must become durable or
be explicitly canonicalized.

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

- fixed root node ID;
- nonrecursive object/array container nodes whose snapshots store only their
  structural kind, never recursively embed descendants;
- object-slot entities whose deterministic schema-natural `entity_pk` is
  composed from stable parent ID plus the exact decoded property key and scoped
  externally by the host's file incarnation, and whose complete snapshot points
  to a child node and carries an independent semantic order key;
- opaque array-item entities with parent, child, and independent order key;
- scalar node entities carrying only the scalar payload; and
- JSON Pointer retained as a derived locator, not an entity primary key.

Duplicate object keys are rejected rather than silently collapsed. Deterministic
object-slot identity means concurrent insertions of the same property compete
on one key instead of allocating two unrelated IDs. The operation-scoped inline
namespace remains for genuinely identity-less sequence members such as new CSV
rows, text lines, JSON array items, and Markdown nodes. Generated IDs are
file/schema-global rather than parent-scoped; parentage and order stay in
snapshots. The host validates natural-key derivation and generated-namespace
reservations plus every emitted PK/schema/snapshot correspondence.

Production acceptance includes a 200,000-entity initial import using one inline
namespace and zero allocator imports, one 32-character PK component, retry and
distinct-operation/collision-reservation behavior, cross-SDK golden vectors,
and reservation live/WAL bytes. Migration preserves all existing UUID PKs and
accepts them by observation-root membership; only new entities use the compact
encoding. Legacy edit/reorder/delete, a new insertion, and restart must round
trip. The hierarchical suite also restores a detached 50,000-node subtree after
renderer eviction, verifies `activated_entities` is exactly the newly reachable
closure with changed upserts excluded, and records zero unrelated active-row
reads.

Normal invalidation is the smallest enclosing value/container. The current
flat/tombstone model can make a container replacement or subtree deletion emit
every descendant. V2 instead changes one bounded schema-declared owning edge;
reachability hides the detached subtree and later GC reclaims it. A localized
scalar edit in a 50,000-property object must load and upsert only the affected
node/slot plus the parser's bounded syntax path; it must not rewrite, lower, or
load a recursively embedded 50,000-property root snapshot. Production
acceptance records semantic rows requested and emitted to enforce that
invariant.

### Excalidraw

There is no Excalidraw plugin in current main, so this research uses a contract
fixture rather than claiming regression parity. Excalidraw already has native
element IDs, fractional `index`, version fields, binding references, domain
`isDeleted`, and stable asset IDs.

A shape edit normally invalidates one top-level JSON object. Binding changes put
their referenced element facts in one merge group; transaction atomicity alone
would not prevent a per-entity LWW merge from selecting half the update. Large
base64 assets must be lazy or streamed so a shape update never copies unchanged
assets through Wasm. Excalidraw's `isDeleted: true` is a render-effective
domain field on a complete element upsert, not an engine `Delete`; the latter
means the semantic entity is absent.

### Initial-import canonicalization gate

The two-step import protocol is required for every format. CSV must durably
represent or canonicalize encoding/BOM, delimiter/quoting policy, and mixed
record terminators. JSON must do the same for whitespace, number and escape
lexemes, object order, and trailing-newline policy. Markdown must retain every
render-effective format field. Text must retain per-line terminators or
normalize them explicitly. Excalidraw must define canonical JSON serialization
and asset handling. For each port, applying the initial `entities_changed`
edits to the submitted source and cold `open_entities` from the resulting
durable state must yield byte-identical canonical output.

Snapshot equality is not delegated to whichever JSON library an SDK happens to
use. Packet v1 defines Snapshot JSON v1: reject duplicate keys recursively and
unpaired surrogates; preserve Unicode scalar strings exactly without
normalization; arrays are ordered; objects are semantically unordered and
canonically sorted by raw UTF-8 key bytes. Numbers use an arbitrary-precision
normalized decimal `(sign, coefficient, effective base-10 exponent)` model,
strip insignificant zeroes, make zero positive, and compare mathematically, so
`1`, `1.0`, and `1e0` are equal and `-0` equals `0`. Canonical output has no
whitespace, deterministic JSON escaping, sorted object keys, and normalized
number spelling. Cross-SDK golden vectors are an acceptance gate. A
`format-only` upsert is rejected only when this normalized snapshot equals its
base; the effect does not erase a real durable snapshot change.

Current Lix `serde_json::Value` storage is not arbitrary-precision, so it cannot
implement that number model without loss. Production v2 therefore requires a
breaking versioned durable snapshot codec with a tagged normalized-decimal node
and migration/hash/equality golden tests before packet v1 can ship. Silent
binary-float coercion or range rejection is not allowed; constraining numbers
would require an explicit new packet-version decision. The checked WIT remains
a research contract until this durable representation exists.

### What stable identity can guarantee

No API can infer the intent behind swapping two byte-identical rows in an
ID-less file. The cross-format guarantee is therefore operational: primary keys
do not derive from byte offsets or array indices; unambiguous edits, moves, and
reorders preserve the matched ID; duplicate ambiguity is resolved
deterministically; and new IDs are retry-stable. Excalidraw's native IDs are
exact. JSON object slots are location-semantic: their schema-natural primary
`entity_pk` is the deterministic `(parent ID, decoded property key)` composite,
not a pointer and not an operation allocation; the host's file key scopes it to
one incarnation. A value edit or
property reorder preserves identity; reorder updates the slot's durable order
key, while a key rename is a delete/insert.
JSON array items use operation-allocated opaque IDs and independent order keys.
Duplicate object keys are rejected in v2; subtree moves preserve IDs only when
the format matcher has an unambiguous correspondence **and the owning identity
semantics permit it**. Moving an object property to another parent, or changing
its key, creates a new object-slot identity; moving an unambiguous array item
may preserve that item's opaque ID and the identities inside its value.

## API hypothesis tournament

### Persistence without splice input

Candidate A retains an immutable document but receives/returns complete blobs.
It isolates repeated state/index hydration from edit localization. Expected:
clear win over v1, but `O(file bytes)` boundary traffic and change location
remain.

### Persistent splices and render patches

Candidate B adds base-relative splices, lazy complete-result fallback, and
patch output. Expected: the largest warm localized win and bounded memory, with
the same fallback simplicity as Candidate A.

The first B control retained complete bytes in Wasm. B2 keeps the same author
facade but leaves immutable versioned bytes behind the host `Source`; the guest
retains only offsets, hashes, parser checkpoints, and semantic IDs. This
separates the API decision from the physical placement of the file buffer.

### Pure copied checkpoint reducer

Candidate C makes rollback/crash reasoning trivial, but an identity/span index
crosses the boundary each edit. A monolithic checkpoint also rewrites
`O(document)` storage. Expected: strong AX score, weak large-file latency and
storage.

### Host-owned transactional private KV

Candidate D minimizes retained guest memory and delegates rollback/persistence
to the host. It exposes index schema/versioning/compaction and may turn local
parser work into many host calls. It is selected only for a greater-than-20%
memory/eviction win without an equivalent AX or latency loss.

### P3 async range/entity sources

Large cold hydration can be streamed and lazy reads can overlap Slate/object
latency. Local warm changes should not require P3 at all. Expected: capacity and
cold-I/O benefit, no single-edit CPU latency benefit.

### Per-file actors instead of one plugin Store mutex

Current main caches one instantiated component per plugin key/hash; its Store is
mutex-protected. Same-plugin files serialize and share one memory/failure
domain. The v2 host should cache compiled components, then use bounded actors
per active branch/path/file-incarnation/plugin-generation tuple. A measured
multi-file instance pool is eligible only after adapter-proven resource/global
isolation; otherwise the entire pool is one joint failure domain and every
actor in it retires together. Separate actors allow cross-file and cross-branch
parallelism while serializing one branch/file renderer.

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
All 22 main-cohort runs, all three targeted refined runs, and the final-aligned
N=1 run needed zero follow-ups and exposed zero tool-transport errors, so every
included run scored 100 for friction and error recovery. Nonzero shell exits
inside a successful Codex tool response do not count as transport errors; that
rubric field therefore does not mean every implementation compiled on its
first attempt. The remaining score variation is elapsed time and tool-call
count, not a subjective API grade.

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
implement as the full-blob and checkpoint controls. The main cohort does not
measure the final refined facade.

The first post-review facade snapshot (SHA-256 `b66a024...`) was frozen and
tested with three deliberately harder, heterogeneous tasks:

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

A final-aligned task covering 200,000 paged initial upserts, stateful
entity/change sources, prospective-state rendering, cross-page change/edit
validation, pre-call splice caps, and plugin reselection is checked in at
[`final-aligned.md`](../../experiments/plugin-api-v2/ax-eval/tasks/final-aligned.md).
A fresh implementer against the frozen final facade (`candidate_b_refined.rs`
SHA-256
`132b4d483c538834112f21878c7fdbbfd18e0584ee36ddb508ebbfd0ca8af0ea`;
WIT SHA-256
`685dcdf248b83ae21d5c937b43dfeb84d0f76427ed8a67a084911890179ada33`)
scored 87 in 193.4 seconds with 13 tool calls. An independent judge accepted
the result after the agent's final formatting check and all five
acceptance-focused tests passed; independent post-run verification also passed
Clippy with warnings denied. Those tests included the 200,000-row paged
initial stream, stateful resolved changes, prospective-state rendering,
cross-page edit validation, pre-call caps, rename, and plugin-reselection
rejection.

The immediate post-run `rustfmt` snapshot had SHA-256 `23aa66d7...` and was
token-equivalent to the evaluated facade, but it is historical rather than the
current checked-in contract. Correctness review then made substantive
post-evaluation changes: one multiplex attachment table, a prospective
`activated_entities` hydration source, a compact inline operation namespace
with zero allocator imports, exact packet/group ordering, and cold-constructor
clarifications. The current facade is SHA-256
`319ede7ce4035c1df6145f6f43ad63e4ca0e69330811df0bd754430d69fffca1`;
the current WIT is
`cbf722584936d08f93e912525941caaecfb389625ceb77625a171c3f6acb4d89`;
and packet v1 is
`d64ba556916c8cafb6f77b09b7edbacde87db6b7fc4ec62ec437d65fa97ef89e`.
Those semantic revisions were not AX-evaluated, and the N=1 task used an
isolated Rust facade rather than generated Component bindings.

That single run proves a fresh agent could implement the evaluated signatures and
the simple renderer consuming complete prospective `current_entities`. It is
not statistically meaningful and is not compared with the N=9 candidate B
cohort. The isolated Rust facade is also not generated Component Model/Wasm
code: the 200,000-row test clones its fixture source and materializes compact
offset tuples, and the resolved-change test has only two nonempty pages. Stable
IDs in the test are deterministic row ordinals rather than allocator-derived
identities exercised through insertion and reorder. A larger homogeneous
follow-up still must cover the current activation source, inline namespace and
legacy-ID migration, semantic-schema descriptor, normalized-decimal durable
codec, aggregate-budget errors, observation expiry/retry, generated bindings,
and realistic error recovery before the API can be called frozen.

The one valid correctness failure is important. B's first CSV agent preserved
row IDs during a reorder but emitted no committed order updates for unchanged
moved rows; it then weakened the reorder assertion until tests passed. The
judge correctly rejected the run because the resulting shared entity state
could not reproduce the submitted order. This is evidence against hiding order
inside optional generic metadata. A production change should make an upsert
carry the complete schema entity, including its order key, and represent delete
as a separate keyed variant. The optimized source/splice mechanism can remain
unchanged.

The post-cohort correctness review added explicit byte/entity cold
constructors, per-change merge groups, precommit renderer validation, and
observation-selected private roots, and removed guest `Send`/`Sync`. The
targeted follow-up covers that snapshot's descriptors, scoped allocation, lazy
bytes, complete upserts, groups, and both cold constructors. A final audit then
made broad semantic output paged, aligned entity input with WIT's stateful
cursor, streamed resolved-change input, added transition-wide key/edit
validation and pre-call splice caps, defined the prospective-state fallback,
and forbade warm plugin reselection. Those later signatures are not credited
with the N=3 score; the separate N=1 final-aligned check exercised them but is
not a comparative cohort.

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
[targeted refined facade](ax-eval/candidate-b-refined-targeted-result.json),
plus the
[final-aligned signature check](ax-eval/candidate-b-final-aligned-result.json).
Raw main-cohort tested/judge transcripts are archived under
`~/.ax-eval/lix-plugin-api-{a,b,c,d}/`, not in this Git branch. The targeted
and final-aligned results record their frozen source hashes, but their raw
Codex rollouts are also local-only. GitHub reviewers can audit the compact
per-agent metrics, prompts, verdicts, and declared substitutions, but cannot
replay the complete transcripts from this PR. That is an evidence-retention
limitation, not a hidden success claim.

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
| 11 | Unbounded entity/output cursors can evade per-call memory and deadline limits | Bound entity/change/edit pages and lazy byte output under one non-renewing aggregate transition budget; validate edit ordering/bounds across pages |
| 12 | Initial import could still accumulate every upsert, and page-local validation misses duplicate keys across pages | Make semantic output inline-or-paged, never split groups, and host-validate key uniqueness across the complete cursor |
| 13 | Rust entity/change input could be replayable or eager while WIT supplies stateful cursors | Align both with permanent-EOF stateful `next_page`; remove the resume token/eager resolved-change list |
| 14 | `file-update.edits` is lifted before guest code can observe a budget | Enforce splice-count and aggregate-inline-byte caps on the host before Canonical-ABI lowering; large data uses `after-range` |
| 15 | Descriptor changes may trigger a different plugin selection | Warm calls require identical plugin key/generation; reselection is a stop/revoke/cold-open or identity-migration handoff |
| 16 | Lazy entity attachments do not prevent today's host storage layer from materializing one huge snapshot | Prototype per-entity CAS/chunk attachments or fail explicitly with `record-too-large`; benchmark both backends |
| 17 | Fixed-rank replay/application-order independence requires displaced-value and proposal-base provenance | Specify a bounded causal frontier/GC layout and measure lookup plus WAL/live-byte amplification before production |
| 18 | “Complete current entities” is ambiguous during precommit rendering | Define it as the transaction-local prospective state after applying final resolved groups, before durable commit |
| 19 | A version number without a packet schema is not an interoperable ABI | Define entity/group records, primitive encodings, attachment references, canonical validation, and limits in [`packet-v1.md`](../../experiments/plugin-api-v2/wit/packet-v1.md) |
| 20 | Same-offset zero-width splices are order-dependent despite passing ordinary non-overlap checks | Require strictly increasing start coordinates across all pages, coalesce equal-coordinate operations, and apply validated edits in reverse base order or one immutable-base pass |
| 21 | Bounded splices can still describe an `after` source with an inconsistent length | Checked-sum deleted/inserted bytes before lowering, require `before - deleted + inserted == after.len`, then reconstruct and hash-validate exact bytes |
| 22 | `open_file` alone can preserve raw syntax that cannot be reconstructed from its emitted durable entities | Resolve the initial groups, then always run the returned document's `entities_changed` against the raw source and prospective state before commit; its edits define the canonical successor |
| 23 | Operation allocation gives concurrent additions of the same JSON object property different IDs | Make object-slot `entity_pk` the deterministic schema-natural `(parent ID, decoded property key)` composite under host-owned file-incarnation scope; use the retry-stable inline namespace only for identity-less sequence members |
| 24 | Group LWW without a causal baseline can let a later sequential `{A}` proposal roll back `B` from an earlier observed `{A,B}` group | Persist the proposal's observation/root version; values visible in that base are baseline, and only causally unordered groups compete under the total rank |
| 25 | One indivisible bounded group cannot enumerate a 50,000-node JSON or Markdown subtree deletion | Use schema-declared owning-edge entities and a fixed root; active state is the reachable closure, so one bounded owning-edge deletion detaches the subtree while retained descendants remain bounded provenance until visibility-horizon GC |
| 26 | Wasm capability isolation does not make co-resident guest document aliases or globals logically immutable | Trust plugin semantic correctness inside files it owns, validate its file/key authority at the host, and retire the actor on traps/uncertain failures; stronger adversarial cross-root isolation needs a separately benchmarked store-per-root or checked checkpoint design |
| 27 | “Versioned durable JSON” does not define cross-SDK equality or canonical bytes | Normatively define Snapshot JSON v1: recursive duplicate rejection, Unicode-scalar strings without normalization, ordered arrays, unordered/raw-UTF-8-sorted objects, arbitrary-precision normalized decimal numbers, and golden canonical-encoding vectors |
| 28 | “Arrival-order independent” contradicts a writer rank assigned as requests are accepted | Define last-write by the accepted writer sequence and guarantee replay/application-order independence only after assignment; use one exact lexicographic `(branch-write-sequence, mutation-id, canonical-group-key)` rank |
| 29 | A small packet can return an unbounded list of owned attachment handles | Add attachment-reference limits and one multiplexed byte-source/output table per page; count bounded indices before opening the table and use indexed output ranges for edits |
| 30 | WIT's opaque schema key cannot tell the host how to validate natural IDs or compute hierarchical reachability | Add a content-addressed v2 plugin-archive semantic-schema descriptor for exact PK rules (including one-component host allocation), order fields, fixed root, and owning versus non-owning references; bind it to plugin generation and require migration when it changes |
| 31 | Current `serde_json::Value` durable snapshots cannot represent packet v1's arbitrary-precision decimal equality | Gate production v2 on a versioned durable normalized-decimal node and storage/migration/cross-SDK golden tests; never round silently |
| 32 | A natural `(parent, property key)` JSON slot PK preserves identity but not file order by itself | Carry an independent durable semantic order key in the complete object-slot snapshot and verify warm reorder equals cold render |
| 33 | Author-selected allocation scopes add validation surface, prevent cross-parent moves, and can amplify Component calls | Pass one compact retry-stable namespace inline per file transition; generated IDs are file/schema-global one-component values, while parent/order remain snapshot fields |

This review also removed `Send + Sync` from guest documents and made allocation
an inline operation namespace so broad imports derive compact ordinal IDs with
zero allocator imports. The
checked wire facade uses bounded packet framing, bounded entity/edit pages, and
lazy input/output bytes, all charged to one top-level transition budget. It now
also pages broad semantic output, statefully consumes entity input, validates
resolved changes, and prospective state, validates keys/edits across pages, and
caps inline splices before lowering. Each page owns at most one multiplexed
attachment table; packets and edits carry bounded table indices/ranges, and the
host counts all references before invoking a table resource. This prevents a
small packet from amplifying into unbounded Component resource handles.
Packet version 1 has a normative transient entity/merge-group encoding and a
cross-SDK Snapshot JSON v1 model in
[`packet-v1.md`](../../experiments/plugin-api-v2/wit/packet-v1.md); generated
SDKs hide it behind typed values. The hidden
observation transport, large-entity storage, group-provenance layout, and
immutable runtime remain production work. These changes are
correctness/operability requirements; they do not borrow the B2 microbenchmark's
speedup as evidence that the full engine is already accepted.

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

Lazy ABI attachments do **not** yet make one giant entity lazy in storage.
Today's JSON/changelog path decompresses and materializes a complete snapshot
as host bytes/strings before WIT lowering, so a 4 MiB Excalidraw asset or one
giant CSV row can still exceed a record/memory budget. Production must either
(a) store large snapshot content as a small typed entity envelope plus
content-addressed byte/chunk attachments, or (b) reject it with an explicit
`record-too-large` limit. Option (a) is not packing unrelated KVs: each entity
keeps its own key and independently addressable payload. It must be benchmarked
on RocksDB and cached SlateDB for point reads, cold streaming, WAL/live bytes,
GC, and dedupe; no storage or 64 MiB benefit is claimed until that experiment
clears the configured >20% gate or is required solely for correctness/capacity.

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
must either be a host-validated schema-natural/native identity or decode to the
transition's retry-stable inline namespace and one ordinal. A generated identity
must be absent from current shared state except for an exact coalesced/cached
retry. A schema-natural conflict identity—most importantly JSON object slot
`(parent ID, decoded key)`—may have been created concurrently after the
proposal's observation; that is a valid competing write to one key and enters
group LWW rather than being blanket-rejected. Namespace/one-component mismatches
and identities that do not satisfy the schema's derivation rule are rejected. One invalid
member rejects the entire transition rather than silently filtering the group.
Without a valid observation there is
no generic safe-upsert path: whole-file parsing cannot recover existing IDs for
CSV, text, Markdown, or JSON arrays. Apart from explicit absent-path creation
and an exact canonical no-op, the engine returns a retryable reread/`410`
before calling the plugin. The handle remains hidden transport provenance, not
a `baseCommitId`, CRDT API, or client-managed commit state; ordinary clients
still read and write blobs.

The required remote transport contract is deliberately outside plugin WIT:

```text
exact unique-file response -> { ordinary SQL result, opaque observation }
next SDK mutation          -> { ordinary SQL request, splice hashes, observation,
                                hidden mutation-id }
successful mutation        -> { ordinary SQL result, successor observation }
```

The SDK stores and echoes the token without exposing it to application SQL. It
also generates one opaque 256-bit mutation ID per logical mutation and reuses
it only for an exact transport retry. The server derives a hidden operation key
from session/workspace/branch, every accepted observation, that mutation ID,
and a normalized digest of SQL parameters plus splice/result hashes. An exact
duplicate coalesces with an in-flight operation or returns the cached committed
result and identical successor observations without invoking the plugin or
allocating IDs twice. Same ID with a different digest is invalid; a new ID
against a consumed observation, or an exact duplicate after its bounded replay
record expires, returns `410` and requires a reread rather than uncertain
re-execution.

After validating every observation, the serialized workspace writer assigns
one monotonic branch write rank and retains it in the in-flight/replay record;
failed-operation gaps are harmless. Exact retries reuse that rank, committed
group provenance persists it, and only committed ranks participate in LWW. The
mutation ID is a deterministic tie-breaker, not a clock.

The server resolves and validates the observation and operation key **before**
constructing `before_source`, the semantic root, or the guest `Document`; the
plugin cannot inspect or forge either. A lost read response yields no usable
token. Missing/expired, wrong-session, wrong-workspace/branch/path/incarnation/
generation, stale-new-operation, and evicted tokens fail with `410`, and the SDK
does not silently retry a stale write. Protocol integration must additionally
test in-flight coalescing, lost mutation responses, exact committed replay,
same-ID/different-digest rejection, replay-record expiry, byte-identical roots
with different stable IDs, multi-file/batched SQL, the full-blob fallback,
SSE/reconnect behavior, and successor-token publication only after commit.
Current v3 carries splice hashes but not this identity-root capability or
mutation dedupe, so “ordinary SQL unchanged” describes the application surface,
not an already implemented transport.

The host semantic root is a structurally shared persistent map, not today's
full row vector. A read retains a root in `O(1)` and sparse submissions
path-copy only changed keys. Otherwise guest/source sharing would leave the
host's `O(sessions × entities)` acknowledgement fan-out untouched.

Natural identity and reachability require static host-readable schema metadata;
they cannot be inferred from WIT's opaque `schema_key`. A v2 plugin archive
therefore needs a content-addressed semantic-schema descriptor bound to the
plugin generation. It declares each schema's exact PK rule (one generated
component, native, or a natural composite over named snapshot fields), independent order fields,
the fixed file root, and owning versus non-owning references. The host adds file
incarnation scope, validates emitted keys/edges and prospective reachability,
and treats descriptor changes as explicit schema migrations. The exact archive
encoding/generator is still production work; the WIT deliberately does not
repeat or let the guest redefine this authority metadata on every call.

### Abort, trap, retry, and eviction

- WIT resources and the Wasm capability sandbox do not enforce logical
  immutability of co-resident guest aliases or globals. V2 therefore trusts a
  plugin's semantic correctness, including immutability of accepted documents,
  inside the files it owns; the host still validates file scope, keys, budgets,
  and authority. The complete branch/file/plugin-generation actor and its
  Wasmtime store/instance are one failure domain. Multiple private documents
  and the shared renderer may coexist, but a trap or uncertain failure cannot
  be contained to one handle; multiple file actors must not share an instance.
- The SDK borrows the accepted resource immutably.
- A call returns a distinct owned successor.
- The private transition proposes changes; the engine resolves them and invokes
  the shared renderer before storage commit.
- Storage commit publishes the shared successor and installs the next private
  source/root/document lease with non-failing pointer swaps; only then may the
  response carry its observation.
- A deterministic validation rejection or known storage/commit abort discards
  every unaccepted successor and leaves the accepted resources intact under the
  declared trusted-plugin immutability contract.
- A trap, cancellation/unwind, or any completion whose guest state is uncertain
  retires the entire file-actor/store trust domain and revokes every observation
  backed by it. A guest can alias document internals or mutate globals, so
  evicting only the accepted handle is insufficient after uncertain execution;
  successful-but-dishonest mutation remains inside the explicitly trusted
  semantic boundary. A shared
  renderer reopens from durable entities in a fresh instance; a private
  noncanonical observation cannot be reconstructed and is revoked.
- `open_file` is for an explicit new incarnation. `open_entities` cold-opens
  only a canonical shared durable semantic root, validating plugin hash, ABI,
  branch, and incarnation. It cannot recreate a private noncanonical or losing
  view; its exact source/root/document must remain leased together or fail
  closed after eviction.
- If derived-cache publication is unavailable after durable commit, the commit
  remains successful. The shared renderer cold-opens from the new durable root,
  no incomplete private observation is issued, and the client rereads.
- The host binds one inline 128-bit allocation namespace to the hidden operation
  key, file incarnation, and plugin generation. The SDK appends a big-endian
  `u64` ordinal and encodes one 32-character unpadded base64url component, so
  broad import makes zero allocator imports and generated IDs remain movable
  across parents. Exact coalesced/cached retries receive the same namespace.
  Reservations become durable only when a new ID commits, are collision-checked
  against live/provenance namespaces, and are visibility-horizon garbage-
  collected; their lookup, WAL, and live-byte costs remain in the storage gate.

This minimum isolation makes actor restart and 1/8/32 retained-document fan-out
material acceptance metrics. Strong adversarial isolation between authority
roots would require one store/instance per root or host-checked immutable
checkpoints and must clear the same latency/memory gates. Pooling multiple file
actors or evicting only one failed handle is an optional optimization only after
adapter-enforced isolation is demonstrated; it is not assumed by the B2 lower
bound.

### Merge groups and cold renderability

Transaction atomicity does not make coupled entity facts merge atomically.
Every returned change belongs to a merge group; singleton is the default, while
authority and conflicts are resolved for all members together. Every candidate
persists the exact observation/root version against which it was proposed.
Facts and accepted groups already visible in that base are causal baseline, not
concurrent competitors. Thus a sequential `{A}` proposal based on an observed
`{A,B}` group changes `A` without rolling `B` back. Only causally unordered
groups with overlapping keys compete.

For those competitors, the writer assigns one exact lexicographic total rank
`(branch_write_sequence, mutation_id, canonical_group_key)`, where the final
component is the host canonical encoding of sorted member schema/PK keys.
Cursor page/list order is never authority, and exact retries reuse the retained
proposal and rank. The branch write sequence intentionally implements
last-accepted-write semantics; the guarantee is replay/application-order
independence after rank assignment, not network-arrival independence. The
resolver considers highest rank first and accepts a group only if all of its
keys are unclaimed; otherwise none of it contributes. Durable group provenance
lets a higher-ranked concurrent group displace every member of an older group
and restore its base values for the older group's other keys. Empty groups,
duplicate keys, partial upserts, mismatched schema/PK fields, invalid packet
bounds, and upsert/delete duplicates are rejected before plugin output reaches
storage.

That provenance is not free. The production design still needs a versioned
group/frontier layout, a visibility horizon for garbage collection, and a
bounded rule for retained displaced values. Fixed-rank permutation,
sequential-versus-concurrent, compaction, branch, and restart tests are
mandatory. RocksDB and SlateDB benchmarks must report
WAL/live-byte amplification and conflict-frontier lookup cost; unbounded
history or more than 20% steady-state amplification without a separately
chosen >2x latency win is rejected. This RFC specifies the correctness rule,
not a completed storage implementation.

Large hierarchical deletion does not create an unbounded merge group. JSON and
Markdown schemas declare a fixed file root plus owning-edge entities. Active
shared state is the closure reachable through those edges. Deleting or
replacing one bounded owning edge atomically detaches its subtree; detached
descendants are excluded from active queries, `current_entities`, and
`open_entities`, while the bounded observation/conflict provenance needed for
concurrent resolution is retained until the visibility horizon permits GC.
Non-owning references never confer reachability, and the host rejects ownership
cycles and multiple owning parents. This makes a 50,000-node subtree deletion
bounded without packing unrelated KVs or splitting one semantic decision across
partially winning groups. The reachability index and detached-state GC remain
production storage work and must report latency plus WAL/live-byte cost on both
backends.

Reattachment and conflict-frontier restoration are allowed. The renderer's
`activated_entities` source carries sorted/unique complete prospective entities
that were durable-but-inactive before and become active, excluding keys already
present as complete upserts in `changes`. It is hydration-only, so no duplicate
application precedence exists. The host derives and loads only that closure; an
evicted renderer can cold-open the previously active state and then hydrate the
restored subtree without scanning unrelated entities. This work may scale with
the activated subtree/output, which is unavoidable, but not with all active
document state. Flat formats and ordinary local edits receive an empty source.

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
  allocation namespace, entity ID, byte source, or guest resource crosses into it.
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
- Warm `file_changed`/`entities_changed` calls require identical selected
  plugin key and generation in the before/after descriptors. A rename or media
  type change that causes plugin reselection is an explicit handoff: stop the
  old actor, revoke observations, and either cold-open schema-compatible state
  with the new plugin or run an identity migration. One guest can never
  transition itself into another plugin or generation.

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
`packages/rs-sdk/benches/profile_plugin_large_file.rs`. Its report commands and backend
cache policy are recorded beside the result artifacts after the run.

## Primary external references

Local format implementations audited:

- [CSV](../../plugins/csv/src/csv.rs)
- [Text](../../plugins/text/src/text.rs)
- [JSON](../../plugins/json/src/lib.rs)
- [Markdown](../../plugins/markdown/src/markdown_file.rs)
- Excalidraw has no current plugin; the checked
  [contract task](../../experiments/plugin-api-v2/ax-eval/tasks/excalidraw.md)
  is explicitly not regression evidence.

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
