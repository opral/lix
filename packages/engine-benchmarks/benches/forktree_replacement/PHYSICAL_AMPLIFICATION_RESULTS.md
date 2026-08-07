# ForkTree OLTP physical amplification and safe-point evidence

Date: 2026-08-07.

## Immutable provenance and scope

- Accepted storage-backed prototype: `bc82385ec42b1789018fbd1213f637c19104a02c`,
  tree `abfaa70faf12c3cdcbe3f990dbf8b4e01340af4a`.
- Exact current control: `f77f5b9e2ff582f749d1c487d95e6c0e8e4d3662`.
- The control replays only the accepted benchmark directory and its three
  benchmark dependencies onto exact f77 production source.
- No production source, persisted format, cache, index, or serving path was
  changed. The extension only parameterizes the accepted history fixture,
  adds 64 highly shared root-only branches, and adds a standalone reader-pin
  safe-point oracle.

## Decision

**KEEP the accepted 64-row leaf / 32-child internal geometry.** Localized
publication has the intended `O(log_32 N)` shape: five objects/two nodes per
update at 1K and six objects/three nodes at 10K. Reducing block size would add
object framing and ancestor writes without addressing the measured residual,
which is adapter reclamation latency after immutable-object deletion.

There is no confirmed post-compaction physical regression greater than 5%.
At the complete 1K dual-adapter gate, explicitly compacted RocksDB is 88.8%
smaller than exact f77 current and ordinarily settled SlateDB is 39.9%
smaller. RocksDB's immediate post-flush ForkTree state is temporarily 8.2%
larger, proving an LSM tombstone/compaction-latency tradeoff rather than a live
layout regression. At 10K, both exact-current update controls hit the strict
20-minute cell cap, so no unsupported exact-current settled ratio is claimed.

## Localized update gate

All update cohorts mutate one row per adapter commit and retain history until
the checkpoint/branch release phase.

| Rows / updates | Adapter/layout | Wall/update | CPU ticks total | Alloc/update | RSS delta | Gets/update | Puts/update | Adapter write/update | Object/node writes per update |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1K / 1K | Rocks ForkTree | 71.498 us | 8 | 62.6 KiB | +3.29 MiB | 5 | 7 | 2,679.6 B | 5 / 2 |
| 1K / 1K | Rocks f77 current | 3,845.845 us | 384 | 5.316 MiB | +54.02 MiB | 101.0 | 58.5 | 57,467.3 B | n/a |
| 1K / 1K | Slate ForkTree | 308.348 us | 43 | 130.2 KiB | +14.55 MiB | 5 | 7 | 2,679.6 B logical; 2,778.0 B physical | 5 / 2 |
| 1K / 1K | Slate f77 current | 4,789.973 us | 590 | 6.645 MiB | +87.40 MiB | 101.0 | 58.5 | 57,467.3 B logical; 24,293.4 B physical | n/a |
| 10K / 10K | Rocks ForkTree | 86.982 us | 87 | 61.0 KiB | +42.25 MiB | 6 | 8 | 4,091.9 B | 6 / 3 |
| 10K / 10K | Slate ForkTree | 294.555 us | 404 | 142.3 KiB | +37.40 MiB | 6 | 8 | 4,091.9 B logical; 4,253.6 B physical | 6 / 3 |
| 10K / 10K | Rocks f77 current | >120,000 us lower bound | — | — | — | — | — | — | timed out in update loop |
| 10K / 10K | Slate f77 current | >120,000 us lower bound | — | — | — | — | — | — | timed out in update loop |

ForkTree logical row bytes were 222,893 B at 1K and 2,488,894 B at 10K.
Authenticated object bytes were 2,459,600 B (11.04x) and 38,399,046 B
(15.43x); total logical adapter writes were 12.02x and 16.44x respectively.
The increase is exactly the additional canonical path-copy level, not an
`O(N)` rewrite.

## Disk, retention, and reclamation

| Cohort | Layout/adapter | Immediate post-flush | Settled/compacted | Versus exact current |
|---|---|---:|---:|---:|
| 1K | ForkTree Rocks | 3,217,407 B | 336,082 B | +8.2% immediate; **-88.8% compacted** |
| 1K | f77 current Rocks | 2,972,906 B | 3,006,405 B | control |
| 1K | ForkTree Slate | 3,490,574 B | 3,490,574 B ordinary close | **-39.9%** |
| 1K | f77 current Slate | 5,803,393 B | 5,803,393 B ordinary close | control |
| 10K + 64 shared branches | ForkTree Rocks | 44,579,729 B | 670,114 B | f77 cell timed out |
| 10K + 64 shared branches | ForkTree Slate | 49,122,822 B | 49,122,822 B ordinary close | f77 cell timed out |

The 64 shared branches each publish only a selector/root and epoch: 7.968 us
and 67 logical bytes per branch on RocksDB; 33.110 us on SlateDB. Before
release, 67 roots retain 60,344 objects and GC reclaims zero. After deleting
all child/recovery/checkpoint roots, one root retains 326 objects and GC
reclaims 60,018 objects / 38,411,082 logical bytes. Final GC is 177.690 ms and
77.31 MB allocation on RocksDB; 1.386 s and 2.170 GB allocation on SlateDB.
This is linear `O(R + O)` work but exposes SlateDB reclamation/allocation as
the next physical seam; it does not justify a different relational block size.

## Read/reopen controls

| Cohort | Adapter/layout | Point read | Cold reopen + point/range/recovery |
|---|---|---:|---:|
| 1K | Rocks ForkTree | 74.460 us | 874.708 us |
| 1K | Rocks f77 current | 625.228 us | 13,852.675 us |
| 1K | Slate ForkTree | 180.730 us | 1,737.594 us |
| 1K | Slate f77 current | 734.788 us | 29,630.585 us |
| 10K | Rocks ForkTree | 82.849 us | 1,025.498 us |
| 10K | Slate ForkTree | 130.520 us | 2,014.724 us |

## Reader-pin safe-point accounting

The standalone model uses one root set and one generation low watermark.
Root attribution is incremental after the current root so shared objects are
never double-counted. Unmarked objects are deletable only when
`object.generation < reader_low_watermark`.

| Scale | Current unique | Active reader unique | Child branch unique | Open upload unique | Old/new abandoned | Deleted while pinned / deferred | Deleted after watermark advance |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1K | 1,001 / 128,096 B | 251 / 36,096 B | 101 / 16,096 B | 126 / 32,128 B | 50 / 50 | 50 / 50 | 528 / 93,920 B |
| 10K | 10,001 / 1,280,096 B | 2,501 / 360,096 B | 1,001 / 160,096 B | 1,251 / 320,128 B | 500 / 500 | 500 / 500 | 5,253 / 936,320 B |

With a generation-4 reader pin, generation-5 abandoned objects are deferred
and the historical reader closure remains byte-readable after sweep. Only
after the reader closes and the low watermark advances to generation 10 does
the model reclaim reader-only, child-branch, upload, and deferred orphan
objects. The current root remains readable throughout. Work scales linearly:
the 10x fixture increases each closure's object/edge reads by approximately
10x; no compaction or sweep can invalidate an active historical root.

## Big-O and recommendation

- Localized publication: `O(log_F N)` authenticated reads and copied nodes;
  measured depth rises from two to three nodes between 1K and 10K for `F=32`.
- Root-only branch/checkpoint publication: `O(1)` selector/epoch writes.
- Reader-pin/root marking and sweep: `O(S + R + O)`, page-bounded in the
  accepted bounded-GC contract; deletion is gated by the minimum active-reader
  generation.
- Final physical reclamation: logically linear, with adapter-specific LSM or
  object-store settling.

Recommendation to production implementation: retain leaf `B=64`, internal
fanout `F=32`, and the single immutable object/selector authority. Preserve
explicit RocksDB compaction diagnostics and add bounded SlateDB delete/settle
accounting. Do not add a cache/index or shrink blocks to treat tombstone lag;
advance reclamation only after the reader-pin low watermark and all branch,
checkpoint, upload, and final-reference roots release the object.

## Rejected cells

The first 10K Rocks current-control attempt used `/tmp` and failed when the
16 GiB tmpfs had only 1.2 GiB free. It is a host-path rejection, not a source
failure. The exact cell was rerun once with `TMPDIR` on the workspace volume
and reached the 20-minute cap cleanly. Both final exact-current 10K controls
are recorded as bounded timeouts, not extrapolated completed measurements.
