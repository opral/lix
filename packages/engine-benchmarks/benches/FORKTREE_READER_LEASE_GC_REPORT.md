# ForkTree reader-lease / GC safe-point guardrail

Verdict: **YES for the Stage-2 contract below**, benchmark/model and test-only.
No production, Stage-2, adapter, or PR source is changed.

## Immutable comparators

- Approved Stage-1: `138b55e1de90806c380ad27b2b349f4c66a1387f`, tree
  `26a3e6ead4d690bf1fe2ebca1e2da7d597256b84`.
- Current-main semantic comparator: `b5e78190f49cab5de7bb19b6f967706c214363b6`,
  tree `c913465505bc773d21a6e2804530287ee937a3f1`.

Current main's storage contract pins a coherent view only for a live
`StorageRead`; dropping the handle releases its snapshot. It has explicit
`ReadExpired`/`InvalidCursor` behavior but no persisted ForkTree reader owner.
Stage-1 adds authenticated retained snapshot selectors for checkpoint,
recovery, undo, redo, and branch tombstones, but no ordinary reader role.
Stage-1's existing deterministic test proves an old in-process `StorageRead`
can still access its backend snapshot after sweep. That does not make an
ordinary cursor durable across process failure or reopen.

## One authority contract

1. Acquiring a reader lease is allowed only from one coherent selected branch
   view. One authenticated selector binds `lease_id`, globally unique monotonic
   `lease_generation`, exact immutable `root`, `view_id`, and
   `valid_through_global_epoch`.
2. Lease acquisition, renewal, release, branch publication, and GC start each
   CAS the exact raw global selector. There remains one global commit version
   and GC watermark; leases do not introduce another epoch/root authority.
3. A cursor authenticates the exact lease ID/generation, root, view ID, and
   resume position. Every resumed page opens one coherent read containing the
   current raw global, exact lease selector, and immutable root. Missing,
   malformed, renewed, released, or logically expired leases fail closed before
   serving the object.
4. Renewal rotates the global epoch and lease generation atomically. Old
   cursors fail. Release deletes the exact lease under global+lease CAS; shared
   roots survive until the final lease/root selector is gone.
5. Expiry uses global publication epochs, never wall-clock time or grace.
   A reader must renew before its logical bound. A crashed process cannot read;
   after enough publications/GC starts its lease is expired. In a quiescent
   repository the stale row can remain longer, which trades reclamation
   liveness for safety without relying on clock quality.
6. GC reads selectors and leases from one coherent raw-global view. GC start
   rotates that global and persists a rebuildable progress proof containing the
   minimum live lease generation, canonical live-lease count/digest, and exact
   fenced raw global. Expired lease rows are removed in that same start commit
   under exact-value preconditions.
7. Every mark/sweep/cleanup batch requires both the exact fenced raw global and
   exact progress bytes. Renewal/publication first makes a stale GC start fail;
   GC first makes a stale renewal fail. A retried renewal rotates global and
   invalidates the GC progress, so neither order can reclaim a revived root.
8. Checkpoint/history/undo/redo, child-branch, and open-upload selectors remain
   independent authenticated roots. Reader leases cannot substitute for or
   weaken those owners.

The progress minimum/digest is derived and rebuildable, not a second source of
root truth. Missing/malformed lease, root, object, checksum, key identity,
generation, cursor, global fence, or progress proof rejects before deletion.

## Complexity and perfect-elimination ceiling

For R reader leases, K non-reader roots, V reachable immutable objects, and O
unreachable/orphan objects:

- acquire/renew/release: O(1) coherent points plus one atomic publication;
- safe-point root collection: O(R + K);
- mark/sweep: O(V + O);
- complete cycle: **O(R + K + V + O)**;
- transient memory: O(R + K + page) for the root frontier and fixed-size
  batches, not O(V); marks are persisted rebuildable maintenance data.

The exact reader scan and one lease row per live reader are the safety lower
bound. Eliminating O(R) would require a second persisted root/index authority
or unsafe probabilistic/stale summaries. Perfect elimination is therefore zero
for reader validation. Stage-2 may page/persist a very large root frontier, but
that queue remains rebuildable maintenance state and must not become authority.

## Deterministic correctness coverage

Each RocksDB and SlateDB process runs the same pre-measurement oracles:

- coherent old `StorageRead` while branches publish and GC runs;
- page resume in one exact view and fail-closed resume after renewal/release;
- child branch, checkpoint, history, undo, redo, and open-upload roots;
- abandoned unpublished objects reclaimed;
- publication-first/GC-first and renewal-first/GC-first races;
- crash before/after lease publication and before/after renewal;
- cold flush/drop/reopen of a renewed lease, old-cursor rejection, and new
  cursor success;
- two leases sharing one root, first release retention, and final release
  reclamation;
- logical expiry, failed renewal/resume, and atomic expired-row cleanup;
- malformed lease and corrupted persisted minimum/digest abort before sweep.

No grace-time assumption appears in an oracle.

## Focused measurements

Three fresh-process repetitions per cell; table values are medians. Setup,
embedded oracles, flush, and reopen are excluded from phase wall times. The
fixture has `2R + 6` roots: R moved branch heads, R old reader roots, and six
child/checkpoint/history/undo/redo/upload roots. Every graph has four objects
and each reader contributes a four-object orphan graph.

| Backend | Readers | Acquire us/reader | Renew us/reader | GC us | CPU us | Allocated | RSS delta | Settled disk |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| RocksDB | 1 | 13.365 | 6.823 | 115.417 | below sampling floor | 103,401 B | 12,288 B | 71,257 B |
| RocksDB | 10 | 5.066 | 4.010 | 367.571 | below sampling floor | 403,326 B | 28,672 B | 90,603 B |
| RocksDB | 100 | 4.141 | 4.046 | 3,003.065 | 4,988.532 | 3,385,988 B | 2,433,024 B | 283,190 B |
| SlateDB | 1 | 18.114 | 6.112 | 470.143 | 747.741 | 2,067,661 B | 2,207,744 B | 11,480 B |
| SlateDB | 10 | 18.269 | 14.833 | 2,006.802 | 3,619.118 | 4,446,630 B | 4,194,304 B | 44,625 B |
| SlateDB | 100 | 8.846 | 6.902 | 14,394.241 | 22,773.451 | 40,852,820 B | 6,299,648 B | 369,945 B |

Exact work scales with the terms above:

| Readers | Roots / reachable / orphans | Marked | Swept | Peak root queue | Gets | Scanned rows | Commits | Logical write bytes |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 8 / 32 / 4 | 32 | 4 | 8 | 37 | 76 | 7 | 4,772 |
| 10 | 26 / 104 / 40 | 104 | 40 | 26 | 130 | 274 | 35 | 18,578 |
| 100 | 206 / 824 / 400 | 824 | 400 | 206 | 1,064 | 2,254 | 293 | 156,638 |

SlateDB physical read objects/bytes are 61/13,090 at R=1, 234/42,236 at
R=10, and 2,210/354,536 at R=100. Settled read/write bytes are
13,870/11,480, 45,100/44,625, and 357,400/369,945 respectively. RocksDB has no
physical object counters; its logical calls/bytes and settled directory sizes
are reported above. No retained-file leak occurs: every orphan is swept, live
reader/static roots survive, expired rows are reaped, and shared objects retire
only after the final reference.

## Stage-2 decision

**YES**, conditional on implementing exactly this sole-authority boundary:
logical-epoch reader leases; exact cursor generation/view/root binding; global
and progress CAS on every GC batch; derived persisted minimum/digest; no
wall-clock correctness; and fail-closed expiry/reopen/corruption semantics.
Do not replace the scan with a second authoritative index, permit arbitrary
old-root acquisition, or allow renewal of an already expired generation.

Invocation:

```text
forktree_reader_lease_gc <rocksdb|slatedb> <1|10|100>
```
