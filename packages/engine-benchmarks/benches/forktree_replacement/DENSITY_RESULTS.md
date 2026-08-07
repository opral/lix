# ForkTree authenticated-block density evidence

Date: 2026-08-07.

Exact current-main comparator: `e9b3a6aaab8d621fd561e74768f4f05563291571`,
tree `6423472d3031185114435e5842cef8fb77df215e`. The complete benchmark-only
prototype was replayed into a clean isolated worktree at that commit. Before
this focused density change, the benchmark directory was byte-identical to the
frozen relational prototype at `ec874a7d1f94238dd8d4c47187b42f7626407cb4`
(tree `d52d909c852b9930cb0f357bcb31caf85342ead4`). No production code or public
serving path is wired to ForkTree.

## Decision

**GO on the physical-density architecture gate; still NO-GO on production
wiring.**

One canonical layout change from 8-row leaves / 8-child internal nodes to
64-row leaves / 32-child internal nodes removes the 2.5x--4.6x tiny-row disk
blocker. It uses the existing authenticated compressed node and value-pack
encoding. There is still one immutable object space and one selector/epoch
plane: no side index, cache authority, dual writer, alternate format, or
rootless state delta was introduced.

At 50K/K=500, explicitly settled ForkTree disk is 18.1% below exact current
Lix on RocksDB and close-settled disk is 15.5% below it on SlateDB. Exact-final
serial medians remain 89.3%/82.8% faster and allocate 94.7%/90.0% less on
RocksDB/SlateDB. K=1 and 10% churn also have disk wins on both adapters.

The remaining production question is engineering integration, not a
fundamental physical-layout limit. This phase does not authorize that work.

## Authority and Big-O

The persistent authority remains:

1. `forktree_objects`: authenticated immutable nodes, value packs, commits,
   semantic change objects, blob manifests, and blob chunks.
2. `forktree_refs`: branch/checkpoint/redo roots and one publication/GC epoch.

For `N` live identities, `U` changed identities, fanout `F=32`, and `Z` copied
or split blocks:

- Publication remains `O(U log_F N + Z)` reads/writes and `O(U + Z)` working
  memory. Larger canonical blocks change constants, not authority or order.
- Diff/merge remains `O(changed paths + output + conflicts)` through hash
  pruning.
- Branch/checkpoint/undo/redo remains `O(1)` selector movement.
- Mark/sweep remains `O(selectors + reachable objects + scanned objects)`.
- The read-only density fold is `O(objects + reachable edges)`, with
  `O(unique object IDs + frontier + page)` memory and only one bounded object
  body resident at a time. It authenticates every object before classifying it.

The larger leaf has an explicit write-amplification tradeoff: a localized path
copy can rewrite more compressed leaf bytes. At 50K/K=500 that raises RocksDB
prototype allocation from the prior 5.97 MB to 13.95 MB, but exact current is
263.60 MB. SlateDB improves from 46.87 MB to 28.50 MB. Both retain much more
than the required 10% resource win versus current.

## Dominant term and causal ceiling

The original 8-row 1K/K=32 post-mutation state contained 349 objects. The
authenticated logical accounting was 56,289 B, but RocksDB occupied 142,696 B
and SlateDB 85,199 B after settling. The body was already compressed; object
count multiplied immutable key framing, per-object authentication, adapter
framing, manifests/WALs, and underfilled ancestors.

| 1K/K=32 component | 8-row layout | 64-row layout | Change |
|---|---:|---:|---:|
| Objects | 349 | 60 | -82.8% |
| Physical object-key bytes | 12,564 | 2,160 | -82.8% |
| Authentication/header bytes | 2,921 | 516 | -82.3% |
| Encoded leaf bytes | 20,465 | 10,774 | -47.4% |
| Encoded internal bytes | 14,160 | 1,628 | -88.5% |
| Encoded value-pack bytes | 8,163 | 2,869 | -64.9% |
| Accounted authenticated bytes | 56,289 | 18,368 | -67.4% |
| Leaf fill | 93.45% | 84.46% | -8.99 pp |
| Internal fill | 75.98% | 57.81% | -18.17 pp |

Although final-block fill percentages are lower, there are far fewer blocks;
total leaf/internal capacity and encoded bytes fall sharply. Selector metadata
is invariant at two rows, 24 physical key bytes, and 44 value bytes. There were
zero unreachable objects after publication because both measured commit roots
were intentionally retained. Thus obsolete objects and selector metadata were
not the causal term.

At 50K/K=500, the accepted layout has 2,314 retained objects and 721,796 B of
authenticated accounted bytes: 83,304 B physical object keys, 20,498 B
headers, 410,809 B compressed leaves, 79,447 B internal nodes, 138,407 B value
packs, and small commit/delta objects. RocksDB's settled 889,706 B consists of
749,532 B SST plus 140,174 B options/manifest/other metadata. SlateDB's
close-settled 881,707 B consists of 103,400 B compacted tables, 98,713 B WAL,
4,036 B manifest, and 675,558 B adapter/object-store metadata. The latter is a
fixed-file-heavy 50K fixture floor, not another row authority.

Perfectly deleting all 8-row per-object overhead would have removed 82.8% of
objects and 67.4% of accounted bytes at 1K. The measured accepted cut realizes
that ceiling and reverses the disk regression on both adapters.

## Focused iterations

1. The inherited 8-row layout established the decomposition and the
   2.5x--4.6x 50K disk regression.
2. A single-format 32-row/16-child trial improved CPU and allocation but left
   50K/K=500 RocksDB at 1,184,686 B versus current 1,028,012 B (+15.2%). It was
   rejected and replaced, not retained as an alternate codec.
3. The 64-row/32-child layout cleared the 1K gate, then 50K and history. No
   further packing layer was justified because it already wins disk on every
   required accepted cell.

An early compaction diagnostic reopened RocksDB with default column-family
options and expanded current to 6.14 MB. That cell is invalid and rejected.
The final helper exactly reproduces the adapter's mutable/immutable column
family compression and blob settings before compacting every column family.
An initial 256-byte fixture used non-UTF-8 identity bytes; it failed before a
measurement and was replaced by deterministic ASCII data. Neither rejected
artifact contributes to the tables below.

## Exact-final serial CRUD gate

Medians use three measured fresh databases after one warmup; setup is excluded.
Disk below is post-flush/close. The decisive RocksDB K=32 and K=500 disk column
uses explicit post-close compaction in the following settled table.

| Rows/K | Adapter | Current wall | ForkTree wall | Wall change | Current CPU | ForkTree CPU | Current alloc | ForkTree alloc | Current disk | ForkTree disk |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1K/32 | Rocks | 4.913 ms | 0.393 ms | -92.0% | 2.654 ms | 0.565 ms | 7.00 MB | 0.469 MB | 0.149 MB | 0.102 MB |
| 1K/32 | Slate | 4.938 ms | 0.535 ms | -89.2% | 2.913 ms | 0.876 ms | 10.58 MB | 2.37 MB | 0.121 MB | 0.027 MB |
| 50K/1 | Rocks | 5.238 ms | 0.161 ms | -96.9% | 3.543 ms | 0.179 ms | 5.62 MB | 0.066 MB | 0.653 MB | 0.558 MB |
| 50K/1 | Slate | 5.072 ms | 0.217 ms | -95.7% | 3.009 ms | 0.219 ms | 6.70 MB | 0.228 MB | 0.621 MB | 0.570 MB |
| 50K/500 | Rocks | 65.761 ms | 7.012 ms | -89.3% | 63.708 ms | 7.588 ms | 263.60 MB | 13.95 MB | 1.028 MB | 0.830 MB |
| 50K/500 | Slate | 69.742 ms | 12.021 ms | -82.8% | 67.955 ms | 12.202 ms | 285.46 MB | 28.50 MB | 1.031 MB | 0.880 MB |
| 50K/5K | Rocks | 243.394 ms | 10.628 ms | -95.6% | 241.617 ms | 11.278 ms | 931.12 MB | 23.84 MB | 1.488 MB | 1.039 MB |
| 50K/5K | Slate | 287.519 ms | 18.264 ms | -93.6% | 296.715 ms | 18.450 ms | 1,104.08 MB | 45.30 MB | 1.684 MB | 1.099 MB |

Settled compaction/close medians:

| Fixture | Adapter | Current | ForkTree | Change |
|---|---|---:|---:|---:|
| 1K/K=32 | Rocks explicit compaction | 209,256 B | 161,588 B | -22.8% |
| 1K/K=32 | Slate close-settled | 131,635 B | 28,033 B | -78.7% |
| 50K/K=500 | Rocks explicit compaction | 1,086,724 B | 889,706 B | -18.1% |
| 50K/K=500 | Slate close-settled | 1,042,982 B | 881,707 B | -15.5% |

At 50K/K=500 ForkTree performs 6 authenticated get calls over 1,251 keys and
178,711 value bytes, then writes 724 objects/selector rows and 264,093 logical
adapter bytes in one commit. Of 722 object writes, 667 are leaves and 52 are
internal nodes. SlateDB reports four physical object reads / 187,099 B and one
physical write / 252,481 B. Current's corresponding detailed evidence records
1,381 get calls / 3,103 keys / about 8.1 MB read and 729 puts / about 468 KB
logical writes.

## Row width and compressibility

The fixture was rerun with 256-byte deterministic values. `compressible`
values are repeated ASCII with identity prefixes; `incompressible` values are
deterministic high-entropy ASCII. All rows returned exact expected bytes.

| Rows/K, shape | Adapter | Current wall | ForkTree wall | Current alloc | ForkTree alloc | Current disk | ForkTree disk |
|---|---|---:|---:|---:|---:|---:|---:|
| 1K/32, compressible | Rocks | 4.870 ms | 0.388 ms | 9.85 MB | 0.477 MB | 152,570 B | 102,830 B |
| 1K/32, compressible | Slate | 5.611 ms | 0.611 ms | 13.44 MB | 2.13 MB | 123,985 B | 27,902 B |
| 1K/32, incompressible | Rocks | 7.412 ms | 0.440 ms | 10.28 MB | 0.480 MB | 555,638 B | 300,669 B |
| 1K/32, incompressible | Slate | 6.877 ms | 0.617 ms | 15.53 MB | 2.01 MB | 529,278 B | 225,354 B |
| 50K/500, compressible | Rocks | 88.671 ms | 8.378 ms | 713.74 MB | 14.07 MB | 1,170,328 B | 886,374 B |
| 50K/500, compressible | Slate | 94.851 ms | 12.916 ms | 736.17 MB | 28.53 MB | 1,175,909 B | 933,736 B |
| 50K/500, incompressible | Rocks | 313.745 ms | 8.312 ms | 1,047.08 MB | 14.12 MB | 20,716,131 B | 10,677,033 B |
| 50K/500, incompressible | Slate | 245.252 ms | 12.387 ms | 1,203.48 MB | 28.57 MB | 20,771,325 B | 10,704,978 B |

The accepted layout wins disk and resources for narrow, wide-compressible, and
wide-incompressible rows. Backend compression helps both authorities, but the
ForkTree win persists when payload entropy removes that advantage.

## History retention, reclamation, and settled LSM tradeoff

The exact-final 1K-history lifecycle retains 1,000 K=1 commits, exercises
point/range read, hash-pruned diff, branch, merge, undo/redo, checkpoint,
flush/drop/reopen, then retention and final reclamation.

| Phase | Rocks current | Rocks ForkTree | Slate current | Slate ForkTree |
|---|---:|---:|---:|---:|
| K=1 history update | 2,229 us/op | 34.8 us/op | 2,534 us/op | 91.4 us/op |
| Point read | 695 us | 41.7 us | 680 us | 69.2 us |
| Range 32 | 1,273 us | 203 us | 1,120 us | 312 us |
| Hash-pruned diff | 2,068 us | 53.7 us | 2,242 us | 144 us |
| O(1) branch root | 1,889 us | 14.8 us | 2,052 us | 18.6 us |
| Merge | 162.8 ms | 129 us | 172.2 ms | 241 us |
| Cold reopen/read | 9.20 ms | 0.410 ms | 13.30 ms | 1.07 ms |

With three retained selectors, authenticated GC sees 5,054 reachable objects
and reclaims zero. After final release it keeps 39 reachable objects and
reclaims 5,015 objects / 2,467,559 logical bytes on both adapters. Reopen and
reads pass before and after the lifecycle transition.

RocksDB immediately post-flush is 3,217,379 B ForkTree versus 2,960,362 B
current (+8.7%) because immutable-object deletion is represented by LSM
tombstones. After explicit settled compaction it is 336,061 B versus 2,994,106
B (-88.8%). SlateDB's ordinary close settles background work and ends at
3,725,691 B versus 7,276,017 B (-48.8%). The transient Rocks number is an LSM
reclamation latency tradeoff, not a live-layout footprint or second authority.

## Correctness and scope

The exact-final oracle passes unchanged on RocksDB and SlateDB. It covers
insert/update/delete/mixed transactions, point/range reads, authenticated
hash-pruned diff, disjoint and conflicting three-way merge, NULL and identity
uniqueness, O(1) branch/checkpoint/undo/redo, flush/drop/reopen, retained-root
and final-reference GC, partial path-copy crash orphans, conflicting selector
races, and fail-closed tree/blob corruption.

The 8 MiB segmented multimedia smoke remains in that oracle and proves
localized edit, full/range read, branch/merge/reopen, shared chunks, and final
reclamation under the denser relational tree constants. Blob chunking and
encoding were not changed; the frozen predecessor's 64/512 MiB evidence is
preserved unchanged.

All benchmark cells completed far below 20 minutes. The final source is
formatted, passes `git diff --check`, compiles, and passes warnings-denied
Clippy for the benchmark with `storage-benches,slatedb`.

Representative commands:

```text
FORKTREE_DENSITY_PROFILE=1 FORKTREE_SETTLE_COMPACTION=1 cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- relational rocksdb <layout> 50000 500 3 1 1
FORKTREE_DENSITY_PROFILE=1 cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- relational slatedb <layout> 50000 500 3 1 1
FORKTREE_RELATIONAL_VALUE_BYTES=256 FORKTREE_RELATIONAL_VALUE_SHAPE=<compressible|incompressible> cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- relational <backend> <layout> 50000 500 3 1 1
FORKTREE_SETTLE_COMPACTION=1 cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- history <backend> <layout> 1000 1 1 0 1000
FORKTREE_RELATIONAL_ORACLE=1 cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- relational <backend> forktree 1000 32 1 0 1
```
