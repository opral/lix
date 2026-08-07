# ForkTree global-order / conflict-scope guardrail

Verdict: **YES for the Stage-2 authority contract; benchmark/model only.** The
prototype is not a production cut and makes no claim that the remaining global
CAS contention is eliminated.

## Immutable provenance

- Approved Stage-1 source: `138b55e1de90806c380ad27b2b349f4c66a1387f`
  (tree `26a3e6ead4d690bf1fe2ebca1e2da7d597256b84`).
- Accepted model provenance: `bc82385ec42b1789018fbd1213f637c19104a02c`
  (tree `abfaa70faf12c3cdcbe3f990dbf8b4e01340af4a`), independent report SHA-256
  `b96d2420d157ca3e569e165351ceaa6dcf89270a295c3e0409296fcd34e12f82`.
- This successor adds only a benchmark/model executable and its Cargo target;
  no ForkTree, adapter, GC, or other production source is changed.
- Build: `CARGO_TARGET_DIR=<isolated> CARGO_BUILD_JOBS=2 cargo bench -q -p
  lix_benchmarks --bench forktree_conflict_scope --features
  storage-benches,slatedb --no-run`.

## Causal source audit and contract

`PreparedPublication::commit` in `packages/lix/src/forktree/publication.rs`
installs the exact global selector as precondition zero before all owner-local
selector preconditions (lines 781-808). The same transaction stages immutable
objects, rotates the global selector, changes owner selectors, and deletes the
rebuildable GC progress selector (lines 828-900). Its own comment identifies
the exact global bytes as the GC publication fence (lines 791-795).

Consequently, N unrelated branch/catalog/upload writers prepared at one global
epoch produce N(N-1)/2 global-precondition failures even though no owner-local
authority is stale. The guardrail model preserves:

1. one durable global `(commit_version, gc_watermark)` authority;
2. one atomic global CAS with each successful owner-selector publication;
3. exact owner-selector CAS as the conflict decision (same-owner stale writers
   still reject);
4. GC-first writer retry and publication-first stale-GC rejection under the
   same exact global fence;
5. immutable prepared bytes as non-authoritative/reusable work only.

On global-only mismatch the proposed Stage-2 contract rereads the coherent
global+relevant-owner selector pair and retries using the same prepared bytes.
An owner mismatch rejects. It adds no persisted cache/index, second epoch/root,
compatibility route, or O(branches) copy. A crash before selector publication
has no published state; crash/reopen after publication reads the atomic global
and owner pair. Reader pins and open uploads remain explicit owner selectors.

## Complexity and ceiling

Let N be simultaneous unrelated owners and P be authenticated preparation
bytes. With a synchronized cohort, current external retry performs
`N(N+1)/2` publication attempts and `N(N-1)/2` false global conflicts, rebuilding
P on every attempt: preparation CPU/allocation is Θ(N²P), backend CAS work is
Θ(N²), and settled authoritative/object bytes are Θ(NP).

The scoped model reduces preparation to Θ(NP), keeps owner validation O(1),
and exposes zero semantic false conflicts. It deliberately retains Θ(N²)
worst-case global CAS attempts under a same-instant herd because one globally
ordered commit version/GC fence remains authoritative. Thus the perfect
elimination ceiling for repeated preparation is `(N-1)/(N+1)` (81.8% at N=10,
98.0% at N=100), while the perfect-elimination ceiling for global CAS attempts
is zero under this contract. A process-local scheduler may reduce local herd
work but cannot be correctness authority or replace cross-process CAS.

## Correctness oracles

Every measured process first passes deterministic RocksDB or SlateDB controls:

- unrelated branch, catalog, and upload cohorts all publish;
- ten same-owner writers yield exactly one success and nine stale rejects;
- GC-first forces a global-only publication retry; publication-first rejects
  the stale GC plan;
- a reader pin and open upload survive unrelated publication and GC fencing;
- flush/drop/reopen preserves exact global epoch/watermark and owner selectors;
- retry leaves one global version per successful publication.

No public/prod code is linked to the scoped mode.

## Measurements

All values are medians of three fresh-process repetitions unless noted. Each
publication prepares a 256 KiB authenticated immutable object. Setup/oracles,
flush, and reopen are excluded from timed latency. Positive wall values below
are improvements from current to scoped.

| Backend | owners | branch wall | catalog wall | upload wall | allocation reduction |
|---|---:|---:|---:|---:|---:|
| RocksDB | 10 | 26.91% | 26.95% | 27.20% | 81.33% |
| SlateDB | 10 | 33.35% | 36.74% | 36.28% | 29.63-30.19% |
| RocksDB | 100 | 73.83% | 73.55% | 74.37% | 97.55% |
| SlateDB | 100 | 47.92% | 48.79% | 50.37% | 32.66-32.79% |

At N=10, Rocks CPU medians move from 350 us/publication to 250-300 us;
SlateDB moves from 550-650 us to 450-550 us. At N=100, Rocks moves from
1.4-1.5 ms to 0.3-0.4 ms and SlateDB from 16.0-16.6 ms to 11.6-12.3 ms.
CPU tick resolution is 10 ms, so wall and exact allocation counts are the
primary small-cell evidence.

The long N=1 control uses five paired 500-publication repetitions. Its pooled
paired wall median is -0.208% on RocksDB and +1.469% on SlateDB; allocations
and logical writes are identical. Per-kind Slate timings swing in opposite
directions despite executing byte-identical zero-retry code, so the pooled
paired control is used for the critical-regression gate.

At N=10, current has about 900 global retries per 200 successes; at N=100 it
has about 4,950 per 100. Scoped mode hides none of this physical work: it
reports the same retry count but zero surfaced false conflicts and reuses
prepared bytes (1,100 -> 200 preparations at N=10; 5,050 -> 100 at N=100).
SlateDB physical reads therefore remain approximately 236 MiB at N=10 and
1.21 GiB at N=100. Successful object writes, backend bytes, and immediate disk
are unchanged within retry-scheduling noise.

Representative post-flush settled branch cells are byte-neutral:

| Backend | owners | current bytes | scoped bytes |
|---|---:|---:|---:|
| RocksDB | 1 | 36,027,253 | 36,027,253 |
| RocksDB | 10 | 62,257,397 | 62,257,397 |
| RocksDB | 100 | 36,027,134 | 36,027,133 |
| SlateDB | 1 | 35,929,323 | 35,929,307 |
| SlateDB | 10 | 62,154,941 | 62,154,888 |
| SlateDB | 100 | 35,930,007 | 35,929,728 |

At N=100, peak RSS delta falls 34-35% on RocksDB and 3.5-17% on SlateDB.
At N=10 it is neutral within allocator/backend granularity. No retained-file,
disk, object-write, or GC-fence regression was observed.

## Stage-2 acceptance contract

**YES:** make unrelated-owner global mismatch an internal retry over the same
prepared immutable work, but only after rereading one coherent global + exact
relevant owner-selector view. Continue to publish one globally ordered commit
version and GC watermark atomically with owner changes. Fail same-owner stale
writers; never synthesize or persist a second ordering authority. Keep GC plans
bound to exact global bytes. Do not claim O(N) physical publication under a
global CAS herd: this model removes Θ(N²P) rebuild work, not Θ(N²) CAS attempts.

Invocation:

```text
forktree_conflict_scope <rocksdb|slatedb> <current|scoped> <branch|catalog|upload> <1|10|100> <rounds>
```
