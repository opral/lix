# ForkTree global-version handoff model

Verdict: **YES for a storage-instance-local Stage-2 publication executor; NO
claim of a distributed worst-case bound.** Benchmark/model only.

## Authority contract

The accepted scoped model still lets N same-instant writers race one global
`(commit_version, gc_watermark)` value. Although prepared immutable work is
reused, a synchronized distinct-owner cohort performs N(N+1)/2 atomic commit
attempts and N(N-1)/2 global-only retries and rereads.

The handoff model uses one disposable process-local queue:

1. every waiter retains the exact owner selector observed before preparation;
2. the current turn atomically CASes the sole durable global selector and that
   exact owner selector while publishing immutable objects;
3. only after success, the resulting global bytes are handed to the next turn;
4. owner mismatch rejects stale; an external global mismatch performs the
   accepted coherent global+owner reread and exact CAS retry;
5. losing the queue or a reservation in a crash has no durable effect.

The queue schedules work but cannot authorize a read, write, version, GC, or
recovery decision. There is no reservation row, second root, alternate selector,
cache, format, compatibility path, relaxed GC fence, or O(branches) copy.

Within one storage instance, current scoped publication is Θ(N²) CAS attempts
and retry reads for a same-instant cohort; handoff is Θ(N). Preparation and
successful object bytes remain Θ(NP). Across K independently scheduling
processes, adversarial external interleaving can still restore Θ(N²) retries;
the model intentionally falls back to the global CAS rather than adding a
distributed reservation authority.

## Median focused results

Three fresh-process repetitions per cell. Each publication prepares a 256 KiB
authenticated immutable object. Setup/oracles/flush/reopen are outside timing.

| Backend | N / rounds | scoped wall us/pub | handoff wall us/pub | wall change | scoped -> handoff retries | CPU us/pub | allocation change |
|---|---:|---:|---:|---:|---:|---:|---:|
| RocksDB | 1 / 500 | 243.295 | 243.095 | -0.08% | 0 -> 0 | 340 -> 340 | -0.45% |
| SlateDB | 1 / 500 | 90.832 | 87.435 | -3.74% | 0 -> 0 | 120 -> 100 | -1.60% |
| RocksDB | 10 / 200 | 302.730 | 304.327 | +0.53% | 9,000 -> 0 | 380 -> 365 | -2.47% |
| SlateDB | 10 / 20 | 180.103 | 84.660 | -53.00% | 892 -> 0 | 550 -> 100 | -89.11% |
| RocksDB | 100 / 1 | 382.086 | 240.085 | -37.16% | 4,950 -> 0 | 400 -> 200 | -19.03% |
| SlateDB | 100 / 1 | 1,672.871 | 82.119 | -95.09% | 4,933 -> 0 | 12,600 -> 100 | -98.91% |

At Slate N=10, measured publication-phase physical reads fall from median 892
objects / 233,846,720 bytes to zero. Commit attempts fall 1,092 -> 200 while
successful writes remain 200 objects / 52,432,000 bytes. Settled disk is
62,417,141 -> 62,416,692 bytes (byte-neutral noise).

At Slate N=100, measured reads fall from 4,933 objects / 1,293,235,280 bytes
to zero and attempts from 5,033 -> 100. Settled disk is 36,191,951 ->
36,191,572 bytes. RocksDB exposes logical rather than physical retry reads in
this harness; its N=100 attempts fall 5,050 -> 100 and settled disk is identical
at 36,289,446 bytes.

The critical gates pass: Slate N=10 wall/backend reads improve by more than
20%; N=1 changes are within 5% on both adapters; Rocks N=10 is within 5%; and
both adapters improve at N=100.

## Correctness controls

Every measured process proves:

- ten unrelated branch, catalog, and upload owners all publish;
- ten same-owner writers produce one success and nine exact stale rejects;
- GC-first causes global-only publication retry;
- publication-first causes stale-GC rejection;
- reader pin and open upload survive unrelated publication and GC;
- crash before commit creates no authority; crash after commit survives reopen;
- exactly one durable global authority remains.

## Recommendation

Ryzen-V may use this contract only at an existing storage-instance publication
executor seam. It is a meaningful general cut for co-located writers and is
safe under external writers because every turn still performs the authoritative
global+owner CAS. It is not evidence that independently scheduled SlateDB
clients have an O(N) global bound. Do not add a persisted reservation frontier
to extend it: that would require a separate authority/product design.

Invocation:

```text
forktree_conflict_scope <rocksdb|slatedb> <scoped|handoff> <branch|catalog|upload> <1|10|100> <rounds>
```
