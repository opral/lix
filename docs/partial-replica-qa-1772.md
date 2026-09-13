# Partial replica QA, PR 1772

Starting revision: `f6136a19c519c3abc9c5840b0e0aa026366db0c9`.
Working branch: `fix/partial-replica-qa-1772`.

## Current status

Verification is complete on the round-fourteen source and pinned
artifacts. Native tests, strict Clippy, optimized WASM, doctests, adapters,
SDK/OPFS checks, manual native profiles and all eight pinned browser runs passed.
Implementation commit: `d4e32178c62845f97e8095ab96b67c7b10ebe0e6`.
Three fresh independent post-commit reviewers reported no actionable findings.
Earlier pending/failure notes below describe their historical snapshots, not
the completed local QA loop. No merge or deployment claim is made.

The fresh convergence reviewer independently passed 48 focused test executions;
the migration/GC reviewer passed another 16. The performance reviewer checked
the traversal and migration bounds, raw profile results, gate logs and source
manifest. Runtime and test source matched the pinned inputs; only this evidence
document changed after profiling. No reviewer changed code or rebuilt artifacts.

Residual coverage limits: browser tests establish graceful offline durability,
reopen and owner lifecycle, not every browser reconnect, lost-ACK, divergence or
abrupt process-death interleaving. Native and worker tests cover portions of those
contracts. Cold arbitrary merge proofs still require linear metadata round trips;
GC cost includes retained mutation metadata and separate directory traversals.
The measured warm-read overhead below remains an optimization opportunity, not
a demonstrated scaling defect or a claim of parity.

Final adapters passed all 111 tests with zero skipped in 1.842 seconds (run
`d9b35028-da8d-437b-ab19-ac57ded9fe3c`; total including lock wait was 10m20s).
All final native profiles ran without concurrent compilation. The eight-run
browser driver exited zero, with eight passing browser tests and eight clean
authority exits. It verified both `sourceManifestVerifiedUnchanged` and
`sdkAndNativeArtifactsVerifiedUnchanged` across the measurement window.

Evidence root: `/root/repos/evidence/lix-pr1772-qa/release-profile-round14`.
`provenance.json` records the exact inputs and all run result paths;
`opening-comparison.json` contains the six independent opening samples.
Pinned SHA-256 values:

- WASM: `c7c95c2dfe9df42c17683a80251fe29336fa3ae739ec5077588459b3279ca9b0`.
- Native binary: `bbaea58c6f920020553fc91f670b5033a8eff984b1b1d1b7144c77a3bf33c133`.
- Source manifest: `966c65c90bacc10e81aa15c83b87a0cdb58fed6cbd02c9b48bfc8d93875c3715`.
- SDK artifact manifest: `d2de2cbb5e51827cfb21029e3941344e46b64cde165d4635a40de1caf38847e0`.

The paired browser SQL run measured warm SELECT medians of 7.35/8.00ms for
partial replicas versus 1.25/1.10ms for complete replicas at 16/16,000 rows.
UPDATE medians were 8.50/10.80ms versus 6.90/8.75ms. This is a visible fixed
overhead limitation, not performance parity; the background workloads are not
identical. See `paired-local-sql/result.json` for the operation samples.

Three fresh-browser/authority samples per opening arm compared a small authority
with 320MiB of unrelated physical blob data. Median request-to-open residuals
were 42.5/42.3ms, with two requests transferring approximately 2.3KB per opening.
The large arm retained 335,547,662 physical bytes in 286 chunks. Total startup
times varied; the residual is not pure network or engine time. Three samples
do not support a population p95 claim. The file-batching run also passed on the
same pinned artifacts; its raw results are in `file-batching/result.json`.

Final native debug profiles are recorded in `native-warm-profile-round14.log`,
`native-opening-profile-round14.log`, `native-descriptor-profile-round14.log`
and `native-ancestry-profile-round14.log` in the parent evidence directory.
Thirty-operation warm SELECT medians were partial/full 1458/235us at 16 rows
and 1594/516us at 1600 rows; UPDATE medians were 1906.5/1275us and
2178.5/1822.5us. Warm exact reads used 180 get calls, 180 keys and 2220 bytes
per 30 operations at both widths, with zero tree reads or scans. Warm UPDATE
tree-chunk reads were 60/120, so the no-tree-read claim applies only to SELECT.
Cold read preparation fetched 8/16 objects plus four metadata records,
6083/21,291 bytes, in 16.086/24.592ms. Cold write preparation fetched one
149-byte metadata record; the prepared spine required no fetches.

Five-sample native opening medians ranged from 3.425 to 3.750ms and reopen
medians from 0.583 to 0.669ms across row counts 16/1600/16,000, 128 branches,
256 historical commits and unrelated 8/32MiB blobs. All 35 samples used two
foreground requests and no background request before return. Descriptor work
was 10 calls/11 keys without scans for row/history arms and 11 calls/12 keys
for 128 branches or schemas; read bytes were 1616-1618 or 2027, and wire bytes
1518-1549. These are native measurements, not browser latency estimates.

The final ancestry gate visited 128/512/2048/8192 records for
64/256/1024/4096 merge pairs, taking 782/1911/8100/33,730us in single samples.
Shared cycle checks at history 32 with 1/32 claims visited 33/64 nodes and
32/63 edges; history 3200 visited 3201/3232 nodes and 3200/3231 edges.
These counts support linear traversal and shared completed work, not a
statistical timing or speedup claim. Browser SQL used 30 measured operations
after five excluded warmups, without telemetry or timed foreground hydration;
partial background push attempts remained present unlike the complete arm.

## Acceptance criteria

- Independent correctness, coverage/recovery, and performance reviews, repeated
  after fixes and commits until the final review produces no actionable issues.
- Native server acceptance preserves both histories, applies incoming changes
  once, preserves newer local descendants, and does not replay checkpoint intent.
- Unrelated authority metadata changes do not permanently strand pending work.
- Missing native data remains unknown until coherently hydrated; covered offline
  reads and writes preserve local durability and read-your-writes.
- Lost responses, reopen, interrupted publication and protocol upgrades preserve
  pending work and expose actionable terminal errors.
- Public API remains stable. Any physical layout change includes migration.
- Profile opening, warm operations and synchronization; report input sizes,
  measured work and algorithmic complexity separately from wall-clock latency.
- Run engine simulations and doctests before commits, relevant adapter tests,
  browser/SDK recovery checks, and repository validation scripts.

## Review round 1

Three independent reviewers examined convergence, coverage/recovery and storage/
performance. Confirmed findings under implementation:

1. Current-authority merge semantics require a sync protocol hard cut. Version
   13 clients interpret a receipt using the originally captured remote parent.
2. Authority GLOBAL advancement can strand a frozen selected merge attempt,
   including after acceptance but before settlement.
3. Dense authority merge history defeats linear ancestry jumps and exhausts a
   fixed ancestry budget, preventing an old replica from converging.
4. A terminal background protocol mismatch stops synchronization without exposing
   that terminal state to foreground operations.
5. More than 1024 pending local commits cannot enter reconciliation after remote
   divergence. Merge attempts must capture bounded prefixes of pending work.
6. Checkpoint compaction removes intervening working commits from causal parent
   history. Acceptance and duplicate suppression need a distinct incorporation
   proof through authenticated complete-state sources. Partial selections alone
   do not establish incorporation of their entire source.

The content-derived immutable segment identity change does not alter physical
locator bytes: existing opaque segment addresses remain readable. Adapter
rollback/reopen validation is still required.

## Proof obligations

- An attempt identifies immutable submitted coordinates. Retries recover that
  attempt's receipt before interpreting the authority's current controls.
- Causal ancestry follows commit parents. Incorporation additionally follows
  authenticated complete-state provenance independently of mutation membership;
  a selected subset is not a proof that its entire source was accepted.
  Checkpoint lineage still uses causal parents. Missing legacy provenance is
  unknown and cannot establish a negative inclusion result.
- An authority transaction validates incoming rows against its current catalog,
  preserves its current unrelated rows, and guards the controls it observed.
- An accepted prefix confirms only that prefix. Exact local control guards and
  final settlement preserve every later local descendant and checkpoint intent.
- Negative incorporation queries may stop at a confirmed base only after proving
  the distinct local suffix extends that base; no missing input proves absence.
- Hydration installs validated immutable inputs under the admitted epoch and
  retries without a mutable transaction spanning network I/O.
- An immutable segment's identity includes its contents. Publication still
  rejects changing bytes already assigned to a committed immutable key.

## Historical validation

The following entries record earlier snapshots. Historical performance documents
and the starting commit's handoff are context, not verification of the final
revision. Completion evidence is summarized above; earlier results alone do not
establish completion or browser convergence.

The initial native run (`a1ad3f0b-22c7-48be-b468-5c3968630897`) compiled an early
round-1 source snapshot with all simulations and server protocol. It ran 4093
tests: 4081 passed, 12 failed, 80 skipped. Failures exposed the checkpoint proof
and retained-body issues, the dense-merge ancestry limit, missing fixture
hydration, and two source-structure checks. Fixes require a fresh compiled run.
Seven SDK worker recovery/upgrade tests passed. Remote SDK tests and the OPFS
build could not run before generating this revision's WASM/SDK artifacts.

The second native run (`60b4adf7-e513-4ad5-89fc-828f1aaaf96c`) ran 4104 tests:
4090 passed, 14 failed, 80 skipped. The real SQL checkpoint regression confirmed
that the existing complete-state alias does not cover all full selections or
partial-checkpoint working continuations. Related authority and retained-body
proof failures remain unresolved. The terminal-error regression failed during
setup, before mismatch injection; its pending update now uses the fixture's
hydrating execution helper. These results do not validate subsequent edits.

The adapter rerun (`2285f364-ddd4-4495-a44b-2329202e0d9b`) passed all 111
RocksDB/SlateDB tests. SDK and OPFS builds and typechecks passed. Generated
pre-format-80 browser artifacts passed 73 SDK worker/remote tests and 55 OPFS
browser tests; these must be rebuilt and rerun after the incorporation migration.

Implementation now includes a format-80 migration workstream: complete
incorporation must be separate from selected mutation membership. Legacy
missing evidence must remain unknown, and sparse-replica upgrade must preserve
pending work. That work is not yet validated.

The first format-80 native run executed 4117 tests: 4085 passed, 32 failed,
80 skipped. After fixing legacy fixture publication, sparse migration stack
growth, upload dependency ordering and selected locators, the second format-80
run executed 4121 tests: 4101 passed, 20 failed, 80 skipped. Compilation took
5m50s and test execution 127.659s. Logs are retained outside the worktree under
`/root/repos/evidence/lix-pr1772-qa/native-format80-round{1,2}.log`.

The remaining failures expose three shared contracts, not independent transport
patches: checkpoint Added rows intentionally rebase creation timestamps;
ordinary native source commits can be rootless; and graph proof retention must
include physically retained sources even when their payload is not retired.
GC tests must verify payload reclamation independently of retained proof graph
metadata. These corrections and an exact multi-root dependency cycle check are
under implementation. No format-80 acceptance claim is made yet.

The second format-80 binary passed the ignored descriptor and HTTP opening
profiles. The warm SQL profile completed the width-16 measurements, then failed
its final stale-writer assertion before reaching width 1600. A valid replacement
admission is currently misclassified as malformed state by the read-scope owner;
the fixture also needs a coherent replacement binding while preserving pending
state. Fix and rerun that gate before using its measurements as complete
evidence. Optimized WASM,
browser profiles, final adapter tests, doctests and post-commit review remain
required.

The third format-80 run (`74ac087c-c16b-4651-8d5c-8299d8d05f8e`) ran 4126
tests: 4119 passed, seven failed, 80 skipped, in 56.814 seconds after 6m07s
compilation. The optimized WASM build also passed (6m32s). Remaining native
failures are four GC integration checks that still require graph deletion,
an over-specific migration fixture layout assertion, an incoherent deferred
cycle fixture, and a genuine checkpoint certificate mismatch for transient
deleted rows. Native checkpoint diff explicitly equates absence and tombstones;
the certificate must use that same logical deletion rule without rewriting
historical members. Fixes are pending fresh execution.

The third binary passed the manual warm SQL gate at widths 16 and 1600,
including stale read/write rejection and a fresh owner's offline pending read.
The manual upload gate needs to prepare its SELECT footprint before testing
post-ACK offline coverage; it previously prepared only UPDATE. The manual file
gate needs a payload exceeding the current 256 KiB inline transfer limit to
exercise its explicit chunk-transfer assertions. These are additional gates,
not included in the native-suite pass count.

The fourth format-80 run passed 4125 of 4126 tests in 57.470 seconds, with
80 skipped. Its only failure expected opportunistic legacy provenance recovery.
The upload-recovery and file-content manual gates passed on this binary, and
the optimized WASM build passed. These results precede the canonical migration
correction below and do not validate that correction.

Review found that deriving immutable provenance from locally resident source
rows or checkpoint nominations produces different headers for the same commit
on full and sparse replicas. The migration now preserves explicit current-format
provenance and maps every legacy header to `LegacyUnknown`, independently of
local coverage and physical alias representation. Existing alias fields remain
independent proof edges. Read-time canonical graph and member facts may exclude
hidden incorporation for ordinary commits without rewriting their headers;
ambiguous checkpoints remain unavailable for negative proofs. Native hydration,
known-wire equality, ordinary divergence, and ambiguous-checkpoint regressions
are awaiting the fifth full run.

The fifth format-80 run executed 4130 tests: 4127 passed and 3 failed in
55.685 seconds. Failures were the explicit legacy-sidecar fixture, an empty
rootless fixture that emitted no header, and the legacy ambiguous-checkpoint
regression. The native log is
`/root/repos/evidence/lix-pr1772-qa/native-format80-round5.log`. These failures
are being corrected; this run is not a final acceptance result.

Round six is green: 4131 native tests passed, 80 skipped, in 55.815 seconds
after 6m04s compilation (run `713ca2fb-dd7a-403a-ba73-bfa020cc266b`).
Optimized WASM passed in 6m12s. The SDK worker/remote gate passed 100 tests
across nine files; OPFS browser compatibility passed 55 tests across six files.
SDK and OPFS typechecks, protocol documentation validation and changenote
validation passed. Manual upload recovery and file-content gates also passed
on this native binary.

This snapshot adds a shared native/materialized alias-source lookup to ancestry
and cycle validation, with a real snapshot regression covering both positive
incorporation and a mixed incoming/native cycle. Review subsequently found GC
does not yet seed or traverse the same materialized alias proof closure. A
retention correction and regressions for hydrated and never-hydrated source
history are required before final profiling and commit; round-six results do
not cover that correction.

Round seven ran 4132 native tests: 4131 passed, one failed, 80 skipped, in
55.941 seconds. The new materialized-alias GC regression fails because an
active serving dependency has no authenticated physical manifest after snapshot
import. Its dependency origin must be resolved without forcing absent history
to hydrate or ignoring genuinely required physical data. The other tests pass.
Optimized WASM passed in 6m14s, all 10 doctests passed, and all 55 OPFS browser
compatibility tests passed on the refreshed artifact.

The separate real browser owner test passed after correcting its assumption:
public opens join the existing shared engine, whereas the physical owner lock
remains exclusive. It checks that lock while a child survives the root, then
checks release after the last child and a fresh offline reopen of pending edits.
Round-six native warm SQL, both prepared dependency-closure gates, indexed
descriptor profiling and canonical HTTP opening profiling also passed.

Round eight again passed 4131 of 4132 native tests (57.808 seconds, 80
skipped). The new assertion identifies the absent manifest as the actual
semantic source S and proves it lacks a deferred-history marker. Snapshot
import currently marks only advertised non-body headers, while authenticated
snapshot rows can name omitted sources not advertised as headers. GC now
separates semantic provenance from physical dependencies, but the producer and
existing-snapshot migration contract still need correction; its strict missing
physical-data checks are not relaxed.

Both storage adapters passed all 111 tests (run
`b6f5a75b-796d-4253-aa47-625c60cbca0d`), all 10 doctests passed, and optimized
WASM passed. Bundled SDK plugins built successfully and the revised OPFS test
typecheck passed. A focused three-case GC classifier regression is being added
to exercise deferred semantic-only, deferred physical, and unmarked missing
dependencies at the exact decision point.

Snapshot import now records omitted semantic owners as `omitted-local`,
`omitted-global`, or `omitted-unknown`, distinct from deferred bodies whose graph
headers are present. These mutable markers neither fabricate graph records nor
infer local scope from missing metadata. The same atomic v79-to-v80 migration
repairs existing snapshots only from durable certified replica coordinates and
independently materialized complete-state roots. Traversal and marker output
share entry/byte limits; standalone missing owners receive no blanket exemption.
Immutable legacy incorporation remains uniformly `LegacyUnknown`.

Round nine ran 4137 native tests: 4135 passed and two failed in 57.732 seconds.
The original legacy-GC regression, bounded migration repair and omission
classification tests passed. Failures were the new test's sanctioned-write audit
entry (corrected afterward) and the fresh-snapshot GC strict-source regression,
which still required a fix at that snapshot. Optimized WASM passed in 6m30s. These results do not
establish final acceptance; the remaining correction and final native/browser
gates are pending.

Round ten ran 4139 native tests: 4138 passed, one failed and 80 were skipped in
58.474 seconds. The sole failure,
`snapshot_omitted_source_survives_gc_then_hydrates_normal_history`, now reaches
GC replacement but reports a selected change without an authoritative locator.
The remaining distinction is between a standalone canonical `ChangeRecord`
installed from snapshot transport and a native authored-payload owner: the former
can supply the value without an authored native locator. Treating a selected
reference as a required native payload owner loses that distinction. A narrow GC
fix was pending at that snapshot; arbitrary missing native dependencies must
remain errors.

Round-ten optimized WASM passed in 6m32s. Adapter tests passed all 111 tests with
zero skipped in 1.869 seconds (run
`12868937-beba-4d3e-9b9c-daf282152efe`). All 10 doctests passed; merged compilation
and execution took 2.78 seconds. Evidence is in
`native-format80-round10.log`, `wasm-format80-round10.log`,
`adapters-format80-round10.log` and `doctests-format80-round10.log` under the
evidence directory. Final acceptance still requires the GC correction and
current-artifact validation, including browser profiles; these results do not
validate subsequent edits.

Round eleven passed all 4140 native tests, with 80 skipped, in 58.388 seconds
(run `adf5869f-9ef4-4ea7-af68-1c8469ebfbc7`; compilation took 6m07s).
This includes the corrected fresh-snapshot GC regression and resolves the
round-ten failure. Optimized WASM passed in 6m32s. SDK worker tests passed all
100 tests across nine files in 706ms; OPFS browser tests passed all 55 tests
across six files in 6.23 seconds. Both TypeScript checks and `build:ts` passed.
Protocol-document validation passed for 42 paths, and changenote validation
passed for eight fragments.

Round-eleven Clippy reported only `cmp_owned` at `sync/commit.rs:460`. The
comparison was subsequently changed to compare typed IDs without allocating
`source.to_string()`. Round-twelve Clippy, native, optimized WASM, doctest and
adapter checks are queued or running against that correction; their results
remain pending. Final acceptance also requires current-artifact browser
profiles (including uninstrumented local comparison and separate telemetry),
the requested commit, and a fresh post-commit review. Earlier profile timings
are historical evidence, not measurements of the final artifact.

Round-twelve changed-file `rustfmt --check` reported only unchanged baseline
formatting in `commit_graph/context.rs` near line 1275 (the metadata-address
constructor and operation assertion). The task's changes in that file are three
incorporation fixture fields, with no new formatting violations. Baseline
formatting was preserved, as with the previously observed full-tree
plugin/runtime formatting differences. See `rustfmt-round12.log`.

Round twelve passed all 4140 native tests, with 80 skipped, in 57.132 seconds
(run `bd9a8192-6946-40e0-bede-550611b2fb41`; compilation including lock wait
took 7m30s). Optimized WASM passed in 6m15s, and all-feature/all-target Clippy
with `-D warnings` passed in 1m48s. All 10 doctests passed in 0.83 seconds.
Adapters passed all 111 tests with zero skipped in 1.868 seconds (run
`09a1f8e8-73c3-4a9e-9437-3f0d01f27ccd`; total including lock wait was 9m38s).

Subsequent review found an uncharged canonical-payload read and repeated
per-locator owner-inventory reads in legacy locator repair. The pending
correction batches canonical reads by scan page, classifies owners/segments
globally, and shares raw-read budget accounting with omission repair. New
regressions cover malformed canonical data and byte-budget failure followed by
an atomic retry. Round-twelve results predate this correction; its verification
and the final browser-profile, commit and fresh-review gates remain pending.

Round thirteen ran 4142 native tests: 4140 passed, two failed and 80 were
skipped in 58.143 seconds (run `c6de18ca-241c-4ce1-bc31-1dcd7daa9594`).
The failures were a sealed-owner check on the collector's direct import of
the migration reader implementation, and the malformed-payload test expecting
a storage error instead of the decoder's internal-error contract. The reader
is now private behind the migration root's `MigrationBoundedRead` facade; the
test now checks `CODE_INTERNAL_ERROR` and the precise change-record decoder
message prefix. The existing binary's source-ownership scan passed against the
corrected files in 1.49 seconds; it does not validate the changed compiled test.

Round-thirteen optimized WASM passed in 6m32s, all 10 doctests passed in 0.83
seconds, and all 111 adapter tests passed in 1.815 seconds (run
`13ef9987-6459-4ce4-aa2e-8c531199b3ae`). Strict Clippy round 13b passed in
1m44s. Round-fourteen Clippy, native, optimized WASM, doctest and adapter checks
are running against the two corrections. Their results and the final browser
profiles, commit and fresh review remain pending.

Round fourteen passed all 4142 native tests, with 80 skipped, in 58.105 seconds
(run `6e24b4d4-8967-4258-8523-f472d4deb61a`; compilation including lock wait
took 7m41s). Both round-thirteen failures are resolved. All-feature/all-target
Clippy with `-D warnings` passed in 1m50s, optimized WASM passed in 6m15s, and
all 10 doctests passed in 0.84 seconds including merged compilation.
`build:ts`, plugin builds and SDK/OPFS TypeScript checks passed. SDK worker
tests passed all 100 tests across nine files in 1.39 seconds; OPFS browser
tests passed all 55 tests across six files in 11.88 seconds.

Manual upload recovery, file-content and both prepared-native-closure gates
passed. The browser owner regression passed its single test in 575ms, and its
authority fixture stopped cleanly. These checks overlapped compilation and
establish correctness only; their timings are not latency claims. Adapter
round fourteen is still running. Final profiling must use pinned artifacts
with no concurrent compilation; those profiles, the commit and fresh
post-commit review remain pending.

## Ancestry complexity and profiling

The original ancestry proof capped both its accumulated graph cache and pending
frontier at 1,024 records. Native jumps reset at merge nodes, so repeated ordinary
authority acceptance merges can exhaust that cap even though an offline base
remains a valid ancestor. The existing 4,096-commit linear-history regression did
not exercise this shape.

The revised proof retains its pending edges, visited identities and decoded
records across cold metadata fetches. Work slices yield cooperatively instead of
turning the total history length into a terminal failure. Missing data remains a
typed demand; corrupt topology remains an error. Linear jumps still cannot skip
secondary merge ancestry. The shared demand runtime completes the topology proof
before retrying the surrounding analysis, avoiding an ever-growing replay after
each missing record. It never holds a storage read across a network request.

For a reached subgraph with V records and E edges, metadata loads and edge visits
are O(V + E), ordered-map/set bookkeeping is O((V + E) log V), and transient
memory is O(V + E). Linear intervals retain native jump acceleration. This is not
an indexed logarithmic proof for arbitrary merge DAGs: a long cold merge chain
still needs O(V) exact metadata round trips. No row payload, file content or
unrelated repository inventory is fetched by the proof. Cancellation or a lease
refresh discards in-memory progress but preserves already hydrated immutable
records; a new attempt must reconstruct its traversal. The resumable causal
walk itself requires no physical format or public API changes. Checkpoint
incorporation is a separate format-80 migration workstream.

Pre-format-80 measurements from the focused native binary (not final validation):
descriptor generation used 10 point-read calls/11 keys and zero scans for 16,
1,600 and 16,000 rows, and for 256 history commits. The 128-branch and 128-schema
fixtures used 11 calls/12 keys. Canonical HTTP opening made exactly two foreground
requests for every fixture, including 32 MiB of file data. Five-sample median
opening times were 3.385 ms (16 rows), 3.493 ms (16,000 rows), and 3.548 ms
(32 MiB file data); median offline reopen times were 0.577, 0.580 and 0.584 ms.
These are local native debug-profile measurements, not browser latency estimates.
Repeat them against the final build and retain final artifacts before acceptance.

The second format-80 binary repeated descriptor and HTTP opening profiles with
no concurrent compilation. Descriptor I/O stayed at 10 calls/11 keys/zero scans
for 16 through 16,000 rows and 256 history commits, and 11 calls/12 keys for
128 branches or schemas. HTTP opening still used two foreground requests.
Five-sample median opening/offline-reopen times were 3.456/0.586 ms (16 rows),
3.489/0.648 ms (16,000 rows), and 3.487/0.591 ms (32 MiB file data).
These results characterize that binary only; later correctness edits require
final artifact-pinned measurement.

The same binary's dense-merge ancestry gate visited 128, 512, 2,048 and 8,192
records for 64, 256, 1,024 and 4,096 authority merges, respectively. Recorded
times were 825, 1,882, 7,908 and 31,988 microseconds. The counted record growth
supports the linear traversal claim; these individual native debug timings are
not statistical latency estimates or browser measurements.

Third-binary warm SQL medians across 30 operations (microseconds): partial/full
SELECT 1421.5/234 at 16 rows and 1593.5/513.5 at 1600 rows; partial/full UPDATE
1883/1283 and 2167/1811, respectively. These paired local debug measurements
show a real partial-serving overhead; they do not establish optimized browser
parity. Exact partial reads used no native tree reads after preparation and
zero scans. Current-artifact browser comparison remains required.

Checkpoint compaction requires a distinct incorporation relation. A complete
state-source fence can preserve a working commit without retaining it as a
causal parent, and its source can have a higher causal generation than the
compacted checkpoint. `incorporated` first tries the accelerated causal proof;
if false, a resumable fallback reads published topology headers and follows only
causal parents, complete-state-source edges and explicit complete checkpoint
incorporation sources. The fallback neither prunes by
generation nor skips checkpoint headers through linear jumps. Selected subset
source ranges never establish whole-commit incorporation. The same asymptotic
bounds apply with one additional header read per reached node in this fallback.
`checkpoint_incorporation_requires_complete_state_not_selected_sources` covers
real full and partial checkpoints and their subsequent working continuations.

Format 80 adds the compact incorporation source to the LXCS13 native header,
independently of physical aliases and selected mutation membership. Planner
publication records the full checkpoint or the partial checkpoint's complete
working continuation; the selected subset checkpoint does not claim its whole
source. Every legacy header migrates to `LegacyUnknown`, regardless of resident
rows, graph records, nominations or physical alias representation.
Traversal still accepts an available positive witness, but an otherwise negative
proof that encountered unknown provenance reports proof unavailable instead of
silently replaying already incorporated work.

GC retains source graph/header proof closure, authenticated mutation inventories
and directory/scoped metadata, not otherwise unneeded historical row or file
payload. This metadata scales with historical mutation metadata, not O(1) per
commit; existing bounded inline deltas remain in inventories. A completed
retirement marker distinguishes retained proof metadata from readable mutation
payload inventory and avoids repeatedly retiring the same source. The focused
`checkpoint_proof_survives_payload_retirement_and_second_gc` regression checks
proof retention, retired row locators, reclaimed overwritten binary chunks and
completion of a second GC pass. This nonignored regression passed in the
round-ten native suite; the distinct fresh-snapshot GC regression above also
passed after its correction in round eleven.
GC uses the same native/materialized alias relation as incorporation and cycle
checks. A materialized snapshot may never have hydrated its detached source;
collection preserves resident proof metadata without fetching absent history.
Explicit complete-incorporation dependencies allow absent proof metadata only
when that node has an authenticated omission/deferred marker; this permission
does not extend to unmarked ancestors. Actual physical dependencies remain
strict. A snapshot
regression checks both never-hydrated sources and a hydrated historical proof
before/after two GC passes, with obsolete payload retirement.

For an incoming head L, negative inclusion at R stops at the confirmed base B
only after proving that B is a strict incorporation predecessor of L. This
avoids scanning history before B for ordinary recent divergence, without using
unsafe generation comparisons across checkpoint aliases. The exclusion boundary
travels with the resumable hydration marker. A single causal-jump probe at the
first complete-state source accelerates the common checkpointed linear suffix;
limiting that probe to once per walk avoids repeatedly visiting overlapping
source histories. Unusual recent merges with independent side paths that do not
reach B may still require their topology. The
`recent_divergence_incorporation_does_not_scan_old_authority_history` gate varies
old history from 4 to 400 commits and counts actual graph/header point reads.

Focused gates passed in the round-ten native suite (the earlier recorded
measurements above remain tied to their original binary):

- `repeated_authority_merges_preserve_offline_ancestor_admission`: 64, 256, 1,024
  and 4,096 ordinary merge pairs; emits graph reads and elapsed microseconds.
- `cold_merge_ancestry_resumes_without_replaying_completed_edges`: 256 merge
  pairs; each of 512 missing records is fetched once and traversal steps are
  bounded by three times the fixture's node count.
- `ancestry_demand_hydrates_complete_proof_before_retrying_analysis`: shared
  demand-runtime integration with a metadata authority, eight merge pairs and
  exact request counts before the final local proof.

Existing ignored profiles selected for current-revision execution are
`partial_handle_http_opening_profile`,
`descriptor_only_sql_hydrates_then_reads_and_writes_offline`, and
`partial_replica_descriptor_large_profile`. Run individually with
`cargo test -p lix --features server-protocol FILTER -- --ignored --nocapture`.
These measure native HTTP opening, matched warm SQL and indexed descriptor I/O;
they do not substitute for optimized browser/OPFS measurements.

Untrusted checkpoint source admission separately checks cycles in the declared
causal/complete-source graph. One iterative three-color DFS shares completed
nodes across all claims in an upload: O(V + E) node/edge work and metadata
loads, O((V + E) log V) ordered-set bookkeeping. Ordinary uploads without new
source claims do not invoke it. `LegacyUnknown` is semantic uncertainty, not an
undeclared physical edge, so it does not invalidate this structural cycle proof;
an absent native header remains a typed metadata error. The guard runs on the
full authority, not the replica's sparse hydration path. A failed request retry
restarts validation, and separate checkpoint uploads may revisit old history.
The `shared_ancestry_is_visited_once_for_many_source_claims` gate counts nodes
and edges for 32/3,200 historical nodes and 1/32 shared-source claims. These
bounds do not claim indexed constant-time or logarithmic checkpoint admission.

The selected-locator migration classifies its globally bounded candidate set by
owner and requested segment, rather than reloading an owner's full inventory
for each locator. Header/inventory reads are constant per owner (up to two for
indirect expansion), and each requested indirect segment is decoded once.
Canonical payload reads and native metadata reads share the preflight byte
budget; only canonical row identities survive the input-page decode. This
repairs positively proven historical selected aliases, not arbitrary locator
corruption: unsupported layouts are retained and normal readers remain strict.
`migration_locator_classification_shares_owner_reads_and_isolates_missing_parts`
checks 3, 300 and 3,000 same-owner candidates, mixed membership, missing parts,
and resident identity/ordinal errors. This gate passed in the final round-fourteen
suite at 3, 300 and 3,000 locators.
