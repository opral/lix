# GC reachability accelerator experiment (00f)

## Identity and verdict

- Production parent: `00f65e8fdf2344ecf671c212b5c42d176859a586`
- Parent tree: `94fb7144c43c8d9990c935bf184f202b0a5deaa0`
- Scope: benchmark/report only; production is unchanged.
- Verdict: **NO-CUT / unqualified for implementation on this lineage**.
- PR #1480 merge blockers found: **none**. The findings below are performance and
  experimental-authority constraints, not a correctness defect in the PR.

## Current production owner

`gc::load_authenticated_repository_retention` loads branch and recovery roots,
then calls `collect_ref_reachable_commit_ids` (`packages/lix/src/gc.rs:1154-1182`).
That function performs one `CommitGraphReader::load_node` per reachable commit
and follows every parent (`gc.rs:1199-1218`). The resulting set is then expanded
through serving/history/CAS retention before retirement (`gc.rs:1415-1463`).
Branch controls supply head, serving generation, revision and checkpoint root,
but carry no externally authenticated digest for a derived transitive summary
(`packages/lix/src/branch/control.rs:48-66`).

## Real-adapter live-history measurements

Release bench binary SHA-256:
`6253e9d037d970b3f031018843f2f2ba24daffb8bedef150f32b7c00bd57b0f7`.
Original benchmark source SHA-256:
`979c13fa31a2fdf791b7a34e96f5fb53243e2807ba45fbd80603df113feb3054`.

The benchmark-only live-root extension retained the generated history instead
of deleting its branch. Ten measured plans followed three warmups.

| Backend | Live H | GC p50 | GC p95 | backend reads (10 samples) | read bytes (10 samples) |
|---|---:|---:|---:|---:|---:|
| RocksDB | 100 | 5.896 ms | 6.080 ms | unavailable in adapter | unavailable |
| RocksDB | 1,000 | 56.830 ms | 58.054 ms | unavailable in adapter | unavailable |
| RocksDB | 10,000 | 686.276 ms | 736.252 ms | unavailable in adapter | unavailable |
| SlateDB | 100 | 21.623 ms | 22.868 ms | 19,840 | 6,528,050 |
| SlateDB | 1,000 | 370.856 ms | 401.108 ms | 190,307 | 57,947,084 |
| SlateDB | 10,000 | 4,052.432 ms | 4,198.338 ms | 1,904,959 | 601,321,674 |

The H=10,000 Slate fixture used for the reported row completed all 10,000
commits. Its attempted extra branch failed after seeding due to a benchmark-only
duplicate fixture UUID, so it is honestly classified as a one-branch result.
An earlier nominal B=1 Slate setup fenced at 3,697 commits and is excluded.

The attempted B=10/B=100 extension reused the original branch UUID for its
first extra branch. Those fanout labels are invalid and are not evidence.
The valid H=100/H=1,000 single-root data and source call graph already establish
the depth slope. No further cells were started under release closure.

For comparison, deleted-branch sweep planning (not the target reachability
term) was 2.544 ms Rocks / 8.601 ms Slate at H=100 and 21.399 ms Rocks at
H=1,000; these results are not credited to the accelerator model.

## Accelerator model and authority gate

A deterministic 64-commit authenticated chunk chain would reduce graph object
loads from `H` to at most `ceil(H/64) + 63`: 38 at H=100, 79 at H=1,000
(conservative tail bound), and 220 at H=10,000. This gives a credible GC-only
ceiling well above 20% on both adapters.

However, a rebuildable side row keyed only by the current roots/generation is
self-signed: a same-owner writer can omit a reachable commit, recompute the
side-row checksum, and cause unsafe reclamation. Exact 00f has no canonical
content-addressed ForkTree object/root envelope to carry the expected summary
digest. A safe design therefore requires an incompatible commit/control
protocol change that binds a deterministic accumulator root in the canonical
commit envelope, plus publication and merge maintenance. That is outside this
release-closure experiment and cannot be honestly qualified against the
`<=5%` normal commit/branch guard without implementation.

Missing, malformed or stale unbound summaries may be discarded and rebuilt,
but accepting them cannot be made fail-closed without that external binding.
Accordingly no production candidate was created.

## Artifacts and caveats

- Raw logs: `/root/repos/evidence/gc-reachability-accelerator-00f/raw/`
- Raw checksum manifest SHA-256:
  `5c4ad3d362f2933dfe4149f72912ae83c3c2df6e85c7294fd5242935366ec13f`
- Databases: `/root/repos/evidence/gc-reachability-accelerator-00f/db/`
- Every command was bounded by 1,200 seconds.
- No production source, PR ref, or integration ref was changed.
