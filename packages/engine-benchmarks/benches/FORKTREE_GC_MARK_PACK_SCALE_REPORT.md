# ForkTree bounded-memory mark/pack/sweep scaling qualification

## Verdict and scope

GREEN for the test/model-only bounded-memory GC contract at 10K, 100K, and 1M
objects on RocksDB and SlateDB. This changes no production, Stage2, cursor, SQL,
or persisted reader-lease code. It is based on Stage1 `138b55e1` and the
no-lease recovery-oracle parent `e5164e5`.

This result does **not** clear the independently reported `e5164e5` publication
versus deletion-page race. The scaling harness has no publication path and
therefore cannot qualify that fence. `e5164e5` remains blocked until a separate
test/model successor rotates one authoritative mutation/reclamation epoch on
every deletion page and publication, adds a process-local per-read nonce, and
invalidates cursors automatically after real read/decode errors.

## Dominant term and contract

The prior research oracle collected all objects and all reachable identities in
`Vec`/`BTreeSet` structures. Current cost is process memory `O(N + L)` and CPU
`O((N + E) log L)`, where `N` is stored objects, `L` live objects, and `E`
authenticated edges. Its perfect-elimination ceiling is the entire `O(N + L)`
process-memory term; an exact collector cannot eliminate reading roots, the live
closure, or every sweep candidate without another durable authority.

The proposed model persists only rebuildable frontier rows, authenticated fixed
mark packs, and one raw GC progress/fence. It streams root enumeration and sweep
in configured pages. No reader/cursor/lease state is persisted.

- CPU/backend work: `O(R + L + E + N)`, with `R` semantic roots.
- Process memory: `O(P * row_bytes + Q * M/8)`, where page size is `P`, mark-pack
  width is `M` bits, and distinct packs touched by one page `Q <= P`.
- Rebuildable working disk: `O(frontier + N/8)` bits plus authenticated framing.
- Backend round trips: root/mark/sweep paging plus point batches, asymptotically
  `O(ceil(R/P) + ceil((L+E)/P) + ceil(N/P))` for a well-filled frontier.
- Exact sweep work remains `Omega(N)` without a persisted generation/index;
  object authentication remains `Omega(bytes visited)`.

## Correctness and boundedness

Every cell forced a flush/drop/reopen after GC start and its first root page.
Every cell then verified exact live count, exact orphan reclamation, empty work
and mark spaces, removed progress, and absence of `lease/`, `reader/`, or
`cursor/` metadata. Separate per-adapter controls proved:

- two semantic selectors retain their shared final object;
- removing one selector retains it;
- removing the final selector reclaims it;
- a checksum-corrupt mark pack fails closed before deletion;
- malformed state does not turn into reader-lease authority.

At fixed `P=512`, `M=4096`, peak object/root pages stayed exactly 512 from 10K
through 1M. Peak mark bytes rose only with packs touched by one page: 1,536 B at
10K/90%, 11,264 B at 100K/90%, and 112,640 B at 1M/90%, below the configuration
ceiling `P*M/8 = 262,144 B`, not `O(N)` process state. The 100K/50% configuration
controls also passed at `(P=128,M=1024)` with 4,864 B peak marks and
`(P=1024,M=8192)` with 7,168 B peak marks on both adapters.

## Resource summary

The complete raw table is `FORKTREE_GC_MARK_PACK_SCALE_RESULTS.csv`. Selected
fixed-configuration GC-only results (setup excluded):

| Backend | Objects | Live | Wall | CPU | Allocated | Peak RSS | Physical reads | Physical writes | Settled path bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| RocksDB | 10K | 10/50/90% | 15.9/23.7/34.9 ms | 17.6/25.5/37.2 ms | 6.1/9.5/13.0 MB | 43.0/43.3/45.1 MB | n/a | n/a | 3.13/3.09/3.06 MB |
| SlateDB | 10K | 10/50/90% | 46.5/105.5/176.7 ms | 35.0/106.8/194.0 ms | 0.33/0.48/0.68 GB | 102.7/110.7/109.1 MB | 107/132/163 objects | 24/24/25 objects | 2.95/3.12/3.01 MB |
| RocksDB | 100K | 10/50/90% | 0.10/0.31/0.71 s | 0.12/0.34/0.73 s | 0.06/0.09/0.13 GB | 73.7/83.7/87.9 MB | n/a | n/a | 28.6/28.3/27.9 MB |
| SlateDB | 100K | 10/50/90% | 0.54/2.22/6.19 s | 0.64/2.48/6.53 s | 11.6/16.7/26.6 GB | 139.8/174.9/161.8 MB | 8.4K/11.0K/17.3K objects | 28/44/83 objects | 28.4/28.6/28.7 MB |
| RocksDB | 1M | 10/50/90% | 1.52/14.28/22.85 s | 1.75/14.59/23.26 s | 0.58/1.07/1.76 GB | 128.9/142.6/149.7 MB | n/a | n/a | 27.9/277.5/274.1 MB |
| SlateDB | 1M | 10/50/90% | 25.3/174.6/398.2 s | 74.1/227.8/465.8 s | 2.56/3.38/5.32 TB | 481.6/457.0/461.1 MB | 0.51M/0.72M/1.33M objects | 262/1,039/1,753 objects | 283.7/286.2/281.0 MB |

Logical object/selector outputs, deletion counts, page peaks, and final state were
identical between adapters. No production path changed, so this package creates
no production latency/resource regression. No correctness or resource cell
crossed the 20-minute cap.

## Ranked next seam

The bounded-memory contract succeeds, but SlateDB cumulative allocations and
physical object reads are the new dominant term. At 1M/90% the process remained
bounded at 461 MB peak RSS while cumulative allocation reached 5.32 TB and
physical reads reached 1.33M objects / 8.60 GB. This is backend/frontier churn,
not live-set retention in process memory. Any Stage2 implementation should keep
the accepted page/pack contract while separately profiling transaction and
immutable-object amplification; this package makes no optimization claim for
that seam.

## Invocation

```text
cargo bench -q -p lix_benchmarks --bench forktree_gc_mark_pack_scale --features storage-benches,slatedb --no-run
<binary> <rocksdb|slatedb> <objects> <live_percent> <page_size> <pack_bits>
```

Qualification matrix: objects `10_000, 100_000, 1_000_000`, live percentages
`10, 50, 90`, page `512`, pack bits `4096`; plus 100K/50% controls at
`128/1024` and `1024/8192`.
