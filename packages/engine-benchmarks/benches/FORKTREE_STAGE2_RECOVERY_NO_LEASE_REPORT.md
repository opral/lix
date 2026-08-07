# ForkTree Stage-2 crash/recovery oracle — live-read ownership

Verdict: **GREEN** for the no-persisted-reader contract below. This is a
benchmark/model package only; it changes no production, Stage-2, cursor,
adapter, or persisted production format.

## Explicit invalidation of the prior control

`331dd9719f47532c89e109c16126d9e8eef5ecec` is **REJECTED and invalid as a
Stage-2 architecture input** because its parent
`ac23754c8ba4a943e69da1304e371d8416456f1b` models persisted reader leases.
It remains frozen only as an invalid control. None of its source, parentage,
authority records, spaces, renewal state, expiry state, grace semantics, or
evidence is used or inherited here.

This corrected package starts directly from approved Stage-1
`138b55e1de90806c380ad27b2b349f4c66a1387f`, tree
`26a3e6ead4d690bf1fe2ebca1e2da7d597256b84`.

## Explicit invalidation and correction of `e5164e5`

The initial no-lease oracle `e5164e5a8fd2d4436f46093c0adbc1b80754e4e3`
is **REJECTED as a Stage-2 architecture input**. Its deletion pages compared
authority/progress but did not rotate either value. A writer prepared at
progress `p0` could therefore publish after a `p0` deletion page had removed
its staged root. It also did not assign a unique identity to every
`StorageRead` when an adapter returned no snapshot cache key, and page errors
did not poison the still-held view.

Independent red oracle `5f21411ae3d4831827714b0987d5d7a66d6f987d`
(full-index diff SHA-256
`eec325610f0625af4a9aae191b0e60feb177f1fa339b61766df0b84c0d464e13`)
proved five failures on exact `e5164e5`: deletion-page-first and real-page
error on both adapters, plus same-root distinct reads on RocksDB. Its red log
SHA-256 is
`1a2c09608f0128a43a8353482150542d3d4601d5b9d29db7f531605a17cc3480`.

This successor hard-cuts the test/model progress format from `NLG1` to `NLG2`.
Every deletion page atomically increments the authenticated progress revision
with its deletes under exact raw authority/progress CAS. Publication CASes the
same exact bytes and removes progress. Exactly one side can win in either race
order. There is no fallback or compatibility decoder.

## Accepted ownership contract

1. A live coherent `StorageRead` is the complete traversal pin. The adapter's
   read snapshot keeps every version needed by that view observable even when
   newer reads observe the same object/row physically swept.
2. Cursor bytes are process-local, derived, authenticated continuation hints.
   They bind exact view identity, immutable root, and last delivered logical
   key. Every `StorageRead` receives a monotonically unique process-local nonce
   even when root, authority, and optional adapter snapshot cache key are
   identical. Neither nonce nor cursor is persisted or scanned as a root.
3. Resume requires the original live `StorageRead`. Reuse on a different view
   returns `InvalidCursor`. The view itself owns atomic process-local validity;
   any cursor decode, storage, ordering, or row decode error poisons it. Every
   later page attempt returns `ReadExpired` without manual drop or a stub.
4. Restart opens one fresh coherent view and scans with
   `Excluded(last_authenticated_delivered_key)`. The key belongs to the last
   successfully returned row, never a decoded-but-undelivered row.
5. One authenticated global authority remains the sole selected root,
   publication generation, commit epoch, and GC watermark. Recovery,
   checkpoint, undo, redo, child-branch, and open-upload selectors are semantic
   roots, not reader substitutes or alternate epochs.
6. Immutable objects/rows are staged before publication. Publication CASes the
   exact raw authority plus exact present/absent GC progress and records the
   displaced root as recovery authority.
7. GC persists only a rebuildable exact root-count/digest and deletion revision
   bound to the fenced raw authority. Each deletion page atomically rotates the
   authenticated revision. Deletion-first makes a `p0` publication fail;
   publication-first changes authority/removes progress and makes a prepared
   `p0` deletion fail. A losing writer must restage reclaimed immutable bytes,
   prepare against the new fence, and retry.
8. Complete typed graph validation precedes deletion. Missing, torn,
   mistyped, checksum-corrupt, identity-mismatched, or non-decreasing edges
   fail closed without authorizing sweep.

There is no reader lease, persisted reader selector, reader space, renewal,
reader low-watermark, expiry generation, or grace-time deletion rule.

## Deterministic coverage

The same sequence passes on RocksDB and SlateDB:

- crash before staging, after staging/before selector CAS, after selector CAS,
  and after GC start/before completion, each with flush/drop/cold reopen;
- live old-root object access and ordered page continuation after new reads
  observe its objects and 24 rows swept;
- old cursor rejection on a fresh view, drop/cancellation expiry, malformed
  cursor rejection, malformed-page termination, and exact fresh-view
  `Excluded(last authenticated key)` restart;
- recovery/checkpoint/undo/redo restores and child-branch dependency;
- open upload retention and final release;
- abandoned unpublished objects and losing-writer objects;
- publication-first and GC-first races plus competing same-root writers;
- a real deletion-page-first race, publication-first reverse race,
  restage/retry, and cold reopen on RocksDB and SlateDB;
- same-root/no-write cursors rejected across distinct `StorageRead` instances;
- genuine later-page corruption poisoning the live view, followed by a fresh
  `Excluded(last authenticated key)` restart;
- final-reference retention until the last semantic selector is deleted;
- malformed selector, missing graph, kind substitution, non-decreasing edge,
  and independently corrupt catalog/state/blob/edge/receipt objects;
- runtime scans of forbidden `lease/`, `reader/`, and `cursor/` prefixes plus an
  allowlist assertion over every metadata key, before and after cold reopen.

Source residue audit finds no lease type, generation, validity, renewal,
low-watermark, grace, selector, or space. The only matching bytes are the three
negative runtime probe prefixes above.

## Complexity

For `N` staged bytes/objects, `K` semantic roots, `V` reachable typed
objects/rows, `O` unreachable objects/rows, and page output `P`:

- staging: `O(N)`;
- publication/selector operation: `O(1)` coherent points plus one atomic CAS;
- live cursor page: `O(log S + P)` in the ordered adapter, with `O(P)` output;
- fresh restart: `O(log S + P)` using one exclusive lower bound;
- root collection and graph validation: `O(K + V)`;
- complete recovery/GC: `O(K + V + O)` work and `O(O)` deletion work.

The tiny executable oracle retains an exact `O(V)` observer set for diagnostic
comparison. The Stage-2 contract remains paged/persisted rebuildable marking
with `O(K + frontier + page)` transient memory and no second root authority.
The irreducible safety floor is the live storage snapshot plus exact
authority/progress CAS; eliminating either breaks a covered race.

## Exact evidence

All build and runtime cells completed below 20 minutes.

| Backend | Wall | CPU | Allocated | Peak RSS | Gets / scans / commits | Swept objects / rows | Disk | Physical object I/O |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| RocksDB | 22.439ms | 17.633ms | 1,420,128 B | 22,640 KiB | 896 / 148 / 129 | 46 / 264 | 375,061 B | not exposed |
| SlateDB | 30.738ms | 18.128ms | 92,945,437 B | 85,720 KiB | 896 / 148 / 129 | 46 / 264 | 108,966 B | 1,067 reads / 468,431 B; 110 writes / 126,380 B; 5 deletes |

Every named output field is `pass`. The independent adversarial executable is
6/6 GREEN in 0.03s. Source gates: `cargo fmt --all -- --check` PASS;
`git diff --check` PASS; warnings-denied focused Clippy PASS.

## Runnable acceptance commands

```text
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
  cargo bench -p lix_benchmarks \
  --bench forktree_stage2_recovery_no_lease \
  --features storage-benches,slatedb --no-run

<isolated-target>/release/deps/forktree_stage2_recovery_no_lease-<hash> rocksdb
<isolated-target>/release/deps/forktree_stage2_recovery_no_lease-<hash> slatedb

CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
  cargo test --release -p lix_benchmarks \
  --test forktree_stage2_recovery_no_lease_adversarial \
  --features storage-benches,slatedb -- --test-threads=1

cargo fmt --all -- --check
git diff --check 138b55e1de90806c380ad27b2b349f4c66a1387f..HEAD
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
  cargo clippy -p lix_benchmarks \
  --bench forktree_stage2_recovery_no_lease \
  --features storage-benches,slatedb -- -D warnings
```

Corrected full-oracle release binary before commit freeze:
`77fbe2077887d75f2a67253d9f9229cb1efb4cd720053012b47e7e5165071ed7`.
Corrected adversarial release binary before commit freeze:
`6067edc5bcab427815d3353423b2e205887b0d2506eddf4f3fc0b3fc3cae6247`.
