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

## Accepted ownership contract

1. A live coherent `StorageRead` is the complete traversal pin. The adapter's
   read snapshot keeps every version needed by that view observable even when
   newer reads observe the same object/row physically swept.
2. Cursor bytes are process-local, derived, authenticated continuation hints.
   They bind exact view identity, immutable root, and last delivered logical
   key; they are never persisted or scanned as roots.
3. Resume requires the original live `StorageRead`. Reuse on a different view
   returns `InvalidCursor`. After drop, cancellation, page error, or malformed
   page, continuation returns `ReadExpired`; no durable state revives it.
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
7. GC persists only a rebuildable exact root-count/digest bound to the fenced
   raw authority. Publication-first makes stale GC start fail; GC-first allows
   publication only by atomically consuming exact progress, making stale sweep
   fail.
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
| RocksDB | 21.584ms | 16.810ms | 1,391,875 B | 23,420 KiB | 887 / 148 / 129 | 46 / 264 | 376,798 B | not exposed |
| SlateDB | 29.568ms | 18.292ms | 92,315,967 B | 82,952 KiB | 887 / 148 / 129 | 46 / 264 | 108,888 B | 1,056 reads / 458,308 B; 110 writes / 125,633 B; 5 deletes |

Every named output field is `pass`. Source gates: `cargo fmt --all -- --check`
PASS; `git diff --check` PASS; warnings-denied focused Clippy PASS in 36.80s.

## Runnable acceptance commands

```text
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
  cargo bench -p lix_benchmarks \
  --bench forktree_stage2_recovery_no_lease \
  --features storage-benches,slatedb --no-run

<isolated-target>/release/deps/forktree_stage2_recovery_no_lease-<hash> rocksdb
<isolated-target>/release/deps/forktree_stage2_recovery_no_lease-<hash> slatedb

cargo fmt --all -- --check
git diff --check 138b55e1de90806c380ad27b2b349f4c66a1387f..HEAD
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
  cargo clippy -p lix_benchmarks \
  --bench forktree_stage2_recovery_no_lease \
  --features storage-benches,slatedb -- -D warnings
```

Accepted local release binary before commit freeze:
`980ac5dd23dd9a589e870411132e0f09a1bacfd7ad9c98f12eb5554095339a29`.
