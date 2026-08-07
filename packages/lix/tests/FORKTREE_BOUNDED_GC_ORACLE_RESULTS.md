# ForkTree persisted bounded-GC independent oracle

Date: 2026-08-07.

## Provenance and decision

- Accepted design artifact:
  `FORKTREE_BOUNDED_GC_OWNER_PLAN.md`, SHA-256
  `aef525087703b1c22a0f6519d2d61813a0a5bc27c484f38bec06b0eccc650d77`.
- Design transport commit: `b647ab95a3df2ee65bf45ef9cadf827583e68632`,
  tree `bbf417ec50e4e686060e11a6af84925f85a58491`.
- Standalone oracle source SHA-256:
  `f506e13a1a9f19bb2d5902716658e85e6183d896c6141b9bebd43c0e7271c287`.
- Optimized warnings-denied binary SHA-256:
  `6138041612b89d20fc8e376d70496d7645608657d17c667a7e5f725308c457b4`.
- Source size: 2,375 lines / 78,701 bytes.

**MODEL APPROVE.** The accepted persisted radix mark-pack, queue-pack, and
continuation state machine is executable with bounded executor memory,
deterministic page/resume order, byte-exact crash/reopen, fail-closed
maintenance corruption, exact epoch fencing, and final-reference behavior.
This does not approve a production head. The unchanged source/compile gate must
pass against Ryzen-V's next immutable Stage-1 successor.

## Executed correctness gate

`conformance` passed five deterministic groups:

1. crash and byte-codec cold reopen after every checkpoint in every phase,
   including a 4,097-edge object requiring 17 bounded edge pages;
2. malformed authenticated snapshot, missing pack, duplicate/misordered mark,
   duplicate queue sequence, and corrupt orphan all fail before deletion;
3. publication-first rejects stale GC and GC-first rejects stale publication;
4. upload completion atomically moves receipt reachability to a manifest,
   abort reclaims only upload-exclusive chunks, and shared chunks survive until
   final release; and
5. positive and negative V1/in-memory/API-sealing source fixtures.

Frozen bounds exercised by assertions:

```text
root page 256; untracked page 1; edge page 256; traversal batch 128;
mark pack 4096; queue pack 1024; sweep page 256; delete batch 256;
peak retained IDs <= 6000; peak GC metadata <= 512 KiB.
```

## Scaling results

The Memory rows measure the bounded algorithm and perform one final encoded
cold reopen. The File rows encode/write/read after every checkpoint to test
crash durability; their allocation/checkpoint-byte totals include the
standalone harness's whole-model snapshot codec and are not proposed
production write amplification.

| Backend/shape | Roots | Reachable | Orphans | Wall | CPU | Alloc bytes | RSS HWM | Peak IDs | Peak GC metadata | Peak maintenance | Model reads/calls | Model writes/calls | Deletes | Final/checkpoint bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Memory normal F=32 | 1K | 1,032 | 100 | 3.847 ms | <10 ms tick | 1.120 MB | 3,120 KiB | 2,342 | 94,204 B | 82,784 B | 2,993,430 B / 1,218 | 2,432,384 B / 93 | 100 / 5 batches | 118,615 B |
| Memory fanout=4097, shared 8-byte prefix | 1K | 5,097 | 100 | 25.561 ms | 20 ms | 10.425 MB | 4,824 KiB | 5,353 | 196,346 B | 326,568 B | 69,598,172 B / 7,627 | 64,537,710 B / 2,418 | 100 / 21 | 447,880 B |
| Memory normal F=32 | 50K | 50,032 | 5,000 | 1.794 s | 1.790 s | 55.649 MB | 27,504 KiB | 5,376 | 197,120 B | 2,310,672 B | 39,003,900 B / 56,539 | 11,820,062 B / 1,706 | 5,000 / 215 | 5,802,615 B |
| Memory adversarial shared 8-byte prefix | 50K | 50,032 | 5,000 | 1.882 s | 1.880 s | 62.907 MB | 27,500 KiB | 5,376 | 197,120 B | 2,302,512 B | 39,113,004 B / 58,236 | 11,929,166 B / 3,403 | 5,000 / 215 | 5,802,615 B |
| File normal F=32, reopen every checkpoint | 1K | 1,032 | 100 | 42.649 ms | 30 ms | 20.614 MB | 3,760 KiB | 2,342 | 94,204 B | 82,784 B | 2,993,430 B / 1,218 | 2,432,384 B / 93 | 100 / 5 | 118,615 B settled; 3,887,225 B checkpoint writes |
| File fanout=4097, reopen every checkpoint | 1K | 5,097 | 100 | 295.980 ms | 300 ms | 333.804 MB | 7,728 KiB | 5,353 | 196,346 B | 326,568 B | 69,598,172 B / 7,627 | 64,537,710 B / 2,418 | 100 / 21 | 447,880 B settled; 59,988,340 B checkpoint writes |

Every cell completed in under two seconds except the largest cells, which were
also under two seconds; all are far below the 20-minute cap.

## Big-O and boundedness

For selectors/roots `S`, current untracked roots `U`, reachable objects `R`,
authenticated edges `E`, and scanned orphan candidates `O`:

```text
root discovery     O(S + U)
traversal          O(R + E), with at most 32 fixed ObjectId radix steps
sweep              O(R + O) ordered work
maintenance disk   O(R + Q), Q = persisted queue frontier/history
executor memory    O(one pack + one page + one object), independent of totals
```

From 1K to 50K normal roots (50x), deterministic checkpoints/events grew
20 -> 804 (40.2x), modeled reads 1,218 -> 56,539 (46.4x), delete batches
5 -> 215 (43x), and live maintenance 82,784 -> 2,310,672 bytes (27.9x).
Peak retained IDs grew only 2,342 -> 5,376 and stopped below the fixed 6K
ceiling; peak GC metadata grew only 94,204 -> 197,120 bytes. The 50K
adversarial shared-prefix cell retained the same peak and added only 3.0% read
calls; its additional radix writes remain a fixed 32-byte-key-depth factor.

The model's actual process RSS includes the complete semantic fixture and
in-memory persistence backend, so it grows with database size; the separately
instrumented executor working set is the acceptance metric. Model backend
bytes count bounded pack/path operations after batch coalescing. Physical
RocksDB/SlateDB framing, WAL, compaction, and object-store bytes remain a
production-head qualification requirement.

## Adapter decision

No RocksDB/SlateDB model cell was linked. Doing so on the blocked predecessor
would require constructing the public generic `StorageSpace` token that the
accepted compiler-sealing cut must remove, making the supposedly immutable
gate depend on the superseded API. The byte-exact File backend covers
crash/reopen independently. Once the corrected owner exposes its typed GC-step
entry point, this unchanged oracle/source gate can qualify both adapters
without reintroducing raw space access.

## Current predecessor source-gate result

Running the frozen gate against `4b7b3aa25ebed5f022ed258c172c27e4dc64753d`
correctly exits 1 at the first known blocker:

```text
ERROR forbidden source residue: SpaceId(pub
```

The immutable successor must additionally remove `GcMarkPackV1`,
`GcProgressV1`, `discover_sweep_plan`, `orphan_object_ids`, retained
`BTreeSet<ObjectId>`/`VecDeque<ObjectId>` paths, expose the V2 bounded symbols,
and make both frozen external import/forgery probes fail to compile.
