# ForkTree semantic vertical-slice evidence

Date: 2026-08-07. Baseline: exact current main
`deea8a4ae9c7a948827dfe9f9a44879910247211`, tree
`615f9978286a9f6545251ea42d658550bf772b74`. This streaming phase starts from
frozen prototype head `143210be4a51f9af302fbf7febd11265701558d0`.

## Decision

**GO for manager review of a production hard-cut plan; do not wire or open a
production PR yet.** The ordered-history architecture still clears both
adapters. The multimedia phase now also eliminates whole-payload construction,
O(L) adapter staging, global edit rechunking, and per-chunk publication fences.
Fresh ingest has no critical latency regression and localized edit is about
51% faster on both adapters while preserving 74/75 chunks. Exact-repeat raw
wall remains +5.8% Rocks/+10.6% Slate because the streaming side must receive
and authenticate all bytes while the current comparator starts with a complete
`Blob`; source-copy-excluded work is faster. Skipping authentication or adding
a full-payload arena was rejected. A zero-copy segmented source interface is a
production-design question, not a second chunker or compatibility path.

This is one replacement layout, not an adjacent index: all commit, tree,
delta, value-pack, blob-manifest, and blob-chunk objects live in
`forktree_objects`; only branch/checkpoint/redo selectors and one epoch live in
`forktree_refs`. No production serving path reads it and no compatibility or
dual-write path exists.

## Fixtures and oracle

- Ordered gate: 50,000 current package-derived rows, sorted K=32 updates, seven
  samples, two warmups, 15 transactions/sample. Current Lix and ForkTree receive
  identical keys and JSON values.
- History gate: 1,000 rows and 1,000 K=1 commits, then point/range reads,
  hash-pruned diff, disjoint branch updates/merge, undo/redo-equivalent root
  movement, checkpoint, retention cut, flush/drop/reopen/recovery, checkpoint
  release, and reclamation.
- Original blob gate: deterministic incompressible 64 MiB payload, FastCDC 256
  KiB/1 MiB/4 MiB min/average/max. Corrected streaming gate: the same payload
  semantics with one canonical 512 KiB/512 KiB/2 MiB profile, an 8 MiB recycled
  engine window, a bounded two-window source prefetcher, and a 4 KiB middle
  edit. Both run branch/diff/merge/checkpoint, 64 KiB and full reads,
  flush/drop/reopen, retained-root sweep, final release, and sweep.
- Every read result, diff cardinality, merge result, range, and BLAKE3 full-blob
  hash is asserted. Objects are authenticated against their BLAKE3 key on
  load. Both adapters perform a real close/drop/reopen.

All cells completed in well under 20 minutes. Commands:

```text
cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- <backend> <layout> 50000 32 7 2 15
cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- history <backend> <layout> 1000 1 1 0 1000
cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- blob <backend> <layout> 1000 1 1 0 1
FORKTREE_BLOB_MIB=64 cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- blob-profile <backend> <layout> 1000 1 1 0 1
FORKTREE_BLOB_MIB=512 cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- blob-profile <backend> <layout> 1000 1 1 0 1
```

## Focused ordered gate

Medians; setup excluded. Disk is post-flush/post-close. Lower is better.

| Adapter | Layout | K=32 latency | CPU | Allocated | Reads / value bytes | Puts / bytes | Disk |
|---|---:|---:|---:|---:|---:|---:|---:|
| RocksDB | current | 6.557 ms | 6.667 ms | 29.683 MB | 125 / 2.003 MB | 110 / 35.05 KB | 9.531 MB |
| RocksDB | ForkTree | 0.570 ms | 0.667 ms | 0.488 MB | 114 / 40.49 KB | 116 / 46.03 KB | 8.124 MB |
| SlateDB | current | 7.574 ms | 8.667 ms | 34.567 MB | 125 / 1.990 MB | 110 / 35.07 KB | 24.684 MB |
| SlateDB | ForkTree | 1.237 ms | 1.333 ms | 2.688 MB | 114 / 40.49 KB | 116 / 46.03 KB | 9.849 MB |

ForkTree improves latency 91.3%/83.7% and allocation 98.4%/92.2% on
RocksDB/SlateDB. Logical object puts and bytes are 5.5%/31.3% higher, but
post-close disk is 14.8%/60.1% lower. The former touched-leaf amplification is
therefore falsified for this packing, while per-operation internal-node bytes
remain an optimization target.

## 1K-history lifecycle

Latency is one operation except the update row, which is per commit.

| Phase | Rocks current | Rocks ForkTree | Change | Slate current | Slate ForkTree | Change |
|---|---:|---:|---:|---:|---:|---:|
| Point | 759.5 us | 44.8 us | -94.1% | 808.9 us | 90.0 us | -88.9% |
| Range 32 | 1,256.7 us | 115.5 us | -90.8% | 1,205.4 us | 319.0 us | -73.5% |
| K=1 update | 2,226.5 us | 30.3 us | -98.6% | 2,564.5 us | 106.2 us | -95.9% |
| Hash-pruned diff | 2,234.1 us | 21.9 us | -99.0% | 2,205.4 us | 80.1 us | -96.4% |
| Branch root | 1,915.5 us | 14.9 us | -99.2% | 2,035.1 us | 15.6 us | -99.2% |
| Merge | 164.45 ms | 93.1 us | -99.94% | 174.65 ms | 218.9 us | -99.87% |
| Undo | 408.4 us | 9.6 us | -97.6% | 1,117.7 us | 16.7 us | -98.5% |
| Redo | 352.6 us | 8.2 us | -97.7% | 580.0 us | 7.4 us | -98.7% |
| Checkpoint | 10.079 ms | 7.3 us | -99.93% | 12.030 ms | 11.5 us | -99.90% |
| Cold reopen/recovery | 9.218 ms | 0.384 ms | -95.8% | 12.501 ms | 10.358 ms | -17.1% |

For the 1,000-update phase, Rocks allocation falls from 5.316 GB to 22.792
MB, CPU ticks from 223 to 3, read value bytes from 105.499 MB to 1.878 MB,
and write bytes from 57.467 MB to 2.384 MB. Slate allocation falls from 6.736
GB to 121.499 MB, CPU ticks from 279 to 14, and has the same 98.2%/95.9%
read/write-byte reductions. RSS growth during updates falls from 60.1/87.8 MB
to 3.1/4.5 MB on Rocks/Slate.

The checkpoint pins all 7,296 objects and the retained sweep reclaims zero.
After release, the same production-shaped owner walk retains 275 objects and
reclaims 7,021 objects / 2.105 MB. Sweep cost is 10.13 ms and 6.90 MB allocated
on Rocks, but 76.05 ms and 200.62 MB on Slate; the latter is a clear offline-GC
cost. Final disk is 3.088 MB versus current 2.960 MB on Rocks (+4.34%, within
5%) and 3.871 MB versus 7.327 MB on Slate (-47.2%). Rocks tombstones do not
produce immediate physical reclamation without later LSM compaction.

## Frozen-predecessor 64 MiB blob lifecycle

| Phase | Rocks current | Rocks ForkTree | Change | Slate current | Slate ForkTree | Change |
|---|---:|---:|---:|---:|---:|---:|
| Ingest | 94.89 ms | 112.23 ms | +18.3% | 32.67 ms | 45.79 ms | +40.2% |
| Unchanged branch | 2.609 ms | 158.4 us | -93.9% | 2.840 ms | 88.7 us | -96.9% |
| 4 KiB edit | 17.99 ms | 23.07 ms | +28.2% | 20.11 ms | 22.61 ms | +12.4% |
| Diff | 1.204 ms | 36.0 us | -97.0% | 1.324 ms | 85.9 us | -93.5% |
| Merge | 5.886 ms | 39.1 us | -99.3% | 6.122 ms | 92.8 us | -98.5% |
| Range 64 KiB | 1.342 ms | 266.8 us | -80.1% | 865.2 us | 145.0 us | -83.2% |
| Full read | 65.98 ms | 54.64 ms | -17.2% | 37.87 ms | 19.29 ms | -49.1% |
| Checkpoint | 10.781 ms | 75.9 us | -99.3% | 12.750 ms | 75.3 us | -99.4% |
| Cold reopen/read | 115.48 ms | 101.27 ms | -12.3% | 47.16 ms | 28.31 ms | -40.0% |

The edit retains 58 of 59 chunks, writes one new chunk plus three metadata
objects (690.6 KB object bytes), and reports two IDs in the symmetric diff.
Compared with current Lix, it allocates 0.724/1.006 MB instead of 12.826/16.797
MB and writes 690.8 KB instead of 1.056 MB on Rocks/Slate. Range read fetches
one 690.6 KB chunk rather than 2.099 MB. Final disk is 68.164/68.043 MB versus
68.791/69.283 MB current (-0.9%/-1.8%).

Retained-root sweep reclaims zero. After checkpoint/source release, GC retains
331 of 340 objects and reclaims nine objects / 691,053 bytes: the replaced
chunk and obsolete metadata. The merged blob remains hash-verified after cold
reopen. Rocks reports logical reclamation but, as expected for an LSM, final
physical disk remains near the live 64 MiB payload until compaction.

### Measured regression ceiling

FastCDC itself consumes 14.377 ms of the 23.065 ms Rocks edit (62.3%) and
14.479 ms of the 22.611 ms Slate edit (64.0%). Perfect elimination leaves
8.688/8.132 ms, 51.7%/59.6% faster than current. On ingest it consumes 14.387
ms of 112.228 ms on Rocks (12.8%; residual is still 3.1% slower than current)
and 17.562 ms of 45.793 ms on Slate (38.4%; residual is 13.6% faster). Thus the
edit regression has a large isolated removable ceiling; Rocks ingest also
requires eliminating repeated full-payload hashing/copying and the 61
one-chunk `put_many` calls. Packing those calls is an object-publication
implementation change, not a side index or second format.

The caller owns two 64 MiB buffers in this fixture. In addition, Rocks ingest
RSS rises about 144 MB and Slate about 82 MB because the adapters retain the
single atomic write's immutable chunk values. The model therefore has bounded
chunk construction, but this vertical prototype does **not** yet demonstrate
the target `O(chunk_size)` end-to-end memory bound. A hard cut must prewrite
authenticated immutable objects in bounded batches and atomically publish only
the small commit/ref/epoch, accepting reclaimable unpublished objects after a
crash.

## Streaming multimedia phase

The hard cut is one format and one authority:

- a recycled 8 MiB `BytesMut` window is split into zero-copy raw chunk values;
- chunk IDs retain the exact domain-separated BLAKE3 identity of the former
  tagged encoding, so authentication and public bytes are unchanged;
- each completed window performs one bounded KeyOnly dedup read and one
  immutable commit, then releases every payload slice;
- the final transaction writes only manifest/delta/commit plus selector/epoch;
  exact selector and epoch equality reject root or GC races. GC is the only
  chunk deleter and rotates this epoch on every deleting page, so the former
  `KeyPresent` precondition for every chunk was duplicate `O(chunks)` work;
- matching IDs from the current authenticated manifest prove canonical
  boundaries and skip CDC/presence reads. A mismatch invokes the same canonical
  FastCDC profile and resynchronizes by ID. There is no fallback chunker,
  compatibility codec, side presence index, or second durable byte authority.

### Fresh ingest medians

Three one-operation samples; setup and bounded source prefetch initialization
are excluded consistently with current Lix's already-materialized input.

| Size/adapter | Current | ForkTree | Raw change | Alloc current / FT | Phase RSS growth current / FT |
|---|---:|---:|---:|---:|---:|
| 64 MiB Rocks | 85.345 ms | 77.450 ms | -9.25% | 68.96 / 8.46 MB | 133.8 / 84.6 MB |
| 64 MiB Slate | 29.138 ms | 28.877 ms | -0.90% | 70.97 / 8.98 MB | 68.3 / 10.9 MB |
| 512 MiB Rocks | 671.075 ms | 663.151 ms | -1.18% | 539.47 / 8.96 MB | 1,144.6 / 173.2 MB |
| 512 MiB Slate | 196.042 ms | 190.608 ms | -2.77% | 545.75 / 14.18 MB | 614.0 / 12.9 MB |

At 64 MiB the engine-owned payload peak is exactly 8 MiB and the bounded
source has two fixed 8 MiB buffers. At 512 MiB those bounds do not grow.
RocksDB's process RSS still grows by 173 MB because of adapter write buffers and
cache state, but the 8x payload increase grows ForkTree allocation only 0.5 MB
and phase RSS about 2x, not 8x; current retains roughly one additional GiB.
Slate's RSS is effectively flat. Logical bytes are 67,114,612/536,919,860 for
ForkTree versus 67,122,608/536,950,516 current. Post-flush disk is within 0.1%
at 512 MiB and 0.5% at 64 MiB.

Rocks pays 11/81 commits at 64/512 MiB instead of current's one commit. Its
512 MiB timed ingest reports about 101 foreground CPU ticks versus 67 because
bounded commits expose work current defers. A warm whole-process measurement
including flush/close reverses that attribution: current is 1.36 CPU seconds
and 1.68 s elapsed versus ForkTree 1.25 CPU seconds and 0.96 s elapsed. Slate
timed CPU falls from a 45-tick median to 24 ticks at 512 MiB. The rejected
16/32 MiB windows worsened 64 MiB Rocks latency to 88.50/101.97 ms and raised
allocations to 16.85/33.63 MB; 8 MiB is the measured common optimum.

### Repeat and 4 KiB edit

Three-sample candidate medians use the same 64 MiB lifecycle. Current values
are the exact current-main lifecycle comparator.

| Phase | Rocks current | Rocks FT | Change | Slate current | Slate FT | Change |
|---|---:|---:|---:|---:|---:|---:|
| Fully deduplicated publication | 7.945 ms | 8.409 ms | +5.85% | 7.935 ms | 8.774 ms | +10.57% |
| 4 KiB localized edit | 18.467 ms | 9.141 ms | -50.50% | 18.107 ms | 8.828 ms | -51.24% |

The repeated path has 75 locality hits, zero CDC, zero chunk emission, four
backend reads, one tiny commit, and 8.42/8.45 MB allocated on Rocks/Slate. Its
raw regression is source delivery plus mandatory authentication of every byte;
subtracting measured source-copy time leaves it faster than current. A
segmented zero-copy source could remove that copy in a production API. Skipping
authentication, trusting an unauthenticated whole-file signature, or retaining
the payload was rejected.

The edit has 74 locality hits and one mismatch: CDC falls from 14.4 ms in the
frozen predecessor to about 53 us, reuse improves from 58/59 to 74/75, and only
one new 725.4 KiB raw chunk plus three metadata objects are written. Presence
work is one bounded probe; logical write bytes are 725,798 versus current's
1,055,920. Allocation is 8.42/8.48 MB versus current's 13.08/17.36 MB.

The exact lifecycle also verifies authenticated 64 KiB/full reads, hash-pruned
diff, merge, O(1) branch/checkpoint publication, retained-root preservation,
flush/drop/cold reopen, and final-reference reclamation on both adapters. The
edit reports 74 shared chunks; retained sweep reclaims zero, final release
reclaims 11 objects / 726,272 bytes, and the merged bytes remain BLAKE3-verified
after reopen. Final disk is 68.20 MB versus current 68.79 MB on Rocks and 68.08
MB versus 69.29 MB on Slate. Authentication and manifest-declared sizes fail
closed on reads; deterministic crash-point/corruption injection remains a
production-cut gate rather than a property claimed by this benchmark.

### Rejected focused variants

- The frozen 256 KiB/1 MiB/4 MiB profile costs about 14.4 ms per edit.
  FastCDC v2020 at 512 KiB min/average and 2 MiB max lowers focused chunking to
  5.6--7.6 ms and gives 74/75 edit reuse. A 1 MiB average profile retained only
  53 chunks and was rejected; the Ronomon implementation retained 62 but cut
  CPU only about 13% and was rejected.
- One in-flight asynchronous emission batch did not overlap adapter commits:
  Rocks stayed near 80 ms, Slate worsened to 35.59 ms, and peak buffering grew
  to 18.9 MB. It was deleted.
- Parallel per-chunk BLAKE3/Rayon doubled hash wall time and raised Rocks edit
  CPU to 27 ticks. It was deleted; authentication remains single-threaded.
- The redundant final per-chunk presence fence made 512 MiB Rocks publication
  alone cost 239.8 ms. The single publication/GC epoch proves the same race
  invariant and restores publication to about 50 us.

## Complexity and authority result

- Current common tracked-state materialization is `O(N + D log_F N)` at a
  rollover and the general path is `O(M log_F N)`. ForkTree bulk build is
  `O(N)`; K value updates are `O(K log_F N + Z)` with unchanged paths shared.
- Point/range are `O(log_F N + returned blocks)`. Aligned hash-pruned diff is
  `O(D log_F N + Z_d)`. Branch/checkpoint/undo/redo are `O(1)` selector plus
  epoch writes. Disjoint merge is diff plus changed-path apply.
- Blob ingest is `O(L)` CPU and fresh physical bytes with `O(W + C)` payload
  memory. Repeat/edit still read and authenticate `O(L)`, locality makes CDC
  proportional to mismatch regions, and physical writes are `O(Z + metadata)`.
  Final publication is `O(chunks)` manifest metadata but no longer performs
  `O(chunks)` adapter preconditions. A range read is `O(requested bytes +
  touched chunk bytes)`.
- GC is `O(pins + reachable objects + scanned orphan candidates)`, scans in
  512-object pages, and keeps `O(reachable IDs + page)` memory. Production must
  replace the in-memory mark set for truly bounded large-repository GC.

A gross source inventory (including inline tests) maps 53,003 lines in
`tracked_state`, 3,423 in `changelog`, 3,121 in `commit_graph`, 4,728 in
`binary_cas`, 7,149 in GC/checkpoint/undo-redo, and 1,088 in branch control:
72,512 lines total. These are an upper-bound replacement surface, not a claim
that all public facades disappear. The directly inspected physical authorities
contain at least 24 named storage spaces across tracked roots/deltas/indexes,
changelog, branch control, CAS, and GC; this prototype collapses that physical
authority to two spaces. Query/API facades, plugin semantics, and conflict UX
remain.

Rootless semantic deltas are still needed for authored change identity/audit,
metadata-only or no-op events, plugin-defined merge intent, and byte-range edit
hints that can avoid full-blob rechunking. They do not require a second root or
durable authority: they remain authenticated delta/change objects referenced by
the commit in the same object space.

## Hard-cut prerequisites

1. Add canonical local split/merge/repacking for inserts and deletes; prove
   shape-changing diff and bulk apply on both adapters.
2. Design a segmented zero-copy source API so fully deduplicated publication
   does not pay the prototype `Read` copy while preserving mandatory
   authentication and bounded backpressure. Streaming immutable publication
   and bounded ingest-owned memory are now demonstrated.
3. Implement general three-way conflicts and preserve semantic/rootless deltas
   required by plugins, audit, and metadata-only commits.
4. Move the reachable mark set to bounded/external storage and qualify GC on a
   repository-scale object graph, especially SlateDB.
5. Add deterministic corruption, crash-at-every-publication-point, concurrent
   root publication versus sweep, and recovery tests before any production
   serving cut.
