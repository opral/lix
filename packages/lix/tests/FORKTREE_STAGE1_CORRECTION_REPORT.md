# ForkTree Stage-1 authority correction report

Status: unwired Stage 1 only. This artifact does not authorize the Stage-2
reader/writer hard cut, a production PR, or a persisted physical-format freeze.

## Provenance

- Stage-1 predecessor: `4b7b3aa25ebed5f022ed258c172c27e4dc64753d`,
  tree `5cafd24b60112220e86c5bccaf5fb382416f2666`.
- Architecture main/base: `8e3ffe632bc27e1ab84fe9a6102b099ab2e9f441`,
  tree `8da56ca4e5d77aa25e57e611fbf4aaad4c01dd10`.
- Accepted bounded-GC owner plan SHA-256:
  `aef525087703b1c22a0f6519d2d61813a0a5bc27c484f38bec06b0eccc650d77`.
- Final bounded-GC model/source oracle SHA-256:
  `12f8b4819520d3f86487940b6a374012c84afc73eda3291333f4f0c8c0c4d004`.
- Accepted architecture plan SHA-256:
  `0a3367f2271bf0600f7ad0acea9450c3d78dc58cc63b33c5b4d8b62fc2c16f27`.
- Independently approved prototype:
  `bc82385ec42b1789018fbd1213f637c19104a02c`, tree
  `abfaa70faf12c3cdcbe3f990dbf8b4e01340af4a`.

The successor commit, tree, full-index diff hash, and this artifact's hash are
reported out of band after the immutable commit is created; embedding them here
would make the evidence self-referential.

## Closed Stage-1 blockers

1. `SpaceId` and every `StorageSpace` field/constructor are sealed. Engine
   descriptors are minted only inside Lix; adapters receive read-only numeric,
   name, and value-semantics accessors. Fixed conformance descriptors and a
   dedicated benchmark-only synthetic range replace arbitrary public token
   construction. ForkTree object and selector descriptors remain private.
2. The old in-memory root/reachable/orphan/claim accumulation and V1 progress
   formats are deleted. V2 uses the existing immutable object space and one GC
   progress selector: authenticated Patricia mark packs, queue packs, live
   branch packs, exact resumable cursors, and bounded sweep batches.
3. GC maintenance is rebuildable and non-authoritative. Every checkpoint and
   sweep exact-CASes raw global plus old progress and rotates the same epoch.
   Ordinary/root-only/upload/untracked publication rotates global and
   atomically discards an active maintenance selector, so the epoch orders GC
   without becoming a permanent all-writer conflict scope.
4. A private `SweepBatch` and private maintenance edit are the only deletion
   plans. Semantic deletion and same-cycle maintenance rewrites are separated;
   malformed or missing maintenance state halts reclamation and cannot name a
   semantic deletion.
5. One serving-owned lazy validator authenticates each visited retained Commit
   parent generation and every member's reverse unified-ChangeCatalog
   owner/ordinal. The same authority validates standalone RefChange catalog
   ownership, target Commit domains, branch continuity, predecessor target,
   and strictly increasing ChangeId chronology. Open remains bounded: unrelated
   immutable corruption is latent until its edge is visited.
6. Patricia traversal authenticates cycle, index kind, content hash/domain,
   canonical pack ordering, and every parent-to-child consumed-prefix edge.
   A structurally substituted child therefore cannot turn corruption into a
   false negative during live-branch lookup or mark-range sweep.
7. The complete Stage-1 root universe remains one selector/object authority:
   global and branch snapshots, checkpoint/recovery/undo/redo targets, open
   multipart receipts, current untracked blob roots for live branches, tracked
   state/catalog/history roots, blob manifests, and shared chunks. Upload
   completion moves receipt reachability to tracked state atomically. Final
   release prunes catalog edges with the last retained selector.
8. Native `StorageRead` snapshots are the in-process reader pin and safe point.
   Logical deletion commits under the exact epoch; RocksDB/SlateDB retain old
   physical versions until old reads drop. Durable cross-reopen pins remain
   authenticated selectors only. There is no process registry, persisted read
   token, cache pin, clock grace, or out-of-band file deletion.

## Bounds and complexity

Hard bounds are selector page 256, current-untracked page 1, authenticated edge
page 256, traversal claims 128 with roughly one edge page of changed index paths
per checkpoint, mark pack 4096, queue pack 1024, object page 256, and delete
batch 256. Peak owner memory is one decoded object/chunk plus bounded pack,
radix path, queue page, edge page, and sweep page; it is independent of total
selectors, roots, reachable objects, and orphans.

- Coherent open: one `begin_read`, one selector-pair `get_many`, and a fixed
  directly selected root-envelope set; `O(1)` selector/root work and memory.
- State/catalog point: `O(log_F N)`; ordered range/resume:
  `O(log_F N + output)` with view-bound cursors.
- Visited history: `O(page + authenticated visited edges)` plus bounded catalog
  owner lookups; unrelated history is not eagerly materialized.
- Publication: `O(U log_F N + copied blocks + immutable bytes staged)` and one
  atomic adapter commit. Branch/checkpoint/undo/redo selector movement is
  `O(1)`.
- GC roots/traversal/sweep: `O(S + U + R + E + O)` total work; live maintenance
  disk `O(R + Q)`; owner memory `O(pack + page + one object/chunk)`.

The frozen model conformance gate passed 5/5. Representative observed bounds:

- 1K/normal/memory: peak 2,232 IDs, 89,744 metadata bytes.
- 50K/normal/memory: 56,539 reads, peak 5,376 IDs and 197,120 metadata bytes.
- 4,097 adversarial fanout: peak 5,341 IDs and 195,794 metadata bytes.
- File crash/reopen and all six bounded model cells completed below two seconds
  in the frozen independent package.

## Correctness and compiler gates

Commands run on this successor worktree:

```text
cargo test -p lix --lib 'forktree::tests' -j1 -- --nocapture --test-threads=1
cargo test -p lix --test integration sealed_owner_violations_are_empty -j1 -- --nocapture
cargo check --workspace --all-targets --all-features -j1
cargo clippy --workspace --all-targets --all-features -j1 -- -D warnings
cargo fmt --all -- --check
git diff --check
forktree_bounded_gc_oracle source-gate <successor-worktree>
forktree_bounded_gc_oracle conformance
forktree_bounded_gc_oracle scale ...  # six frozen cells
```

Results: 24/24 ForkTree owner tests pass; sealed-owner structure passes; full
workspace check passes; warnings-denied all-target/all-feature Clippy passes;
format and diff checks pass; bounded source and model gates pass. Deterministic
tests cover pre/post durable publication and GC crashes, stale publication in
both race orders, active-GC cancellation by publication, old-reader safe point
and cursor invalidation, malformed maintenance abort, retained historical
generation/catalog/ref chronology, upload complete/abort, real shared chunks,
and final-reference reclamation.

Independent reader-pin evidence is green on Memory, RocksDB, and SlateDB:
test-only head `d76e215869e4156eb0e1ea9ad724a3ed5d1c0262`, cumulative diff SHA-256
`0d79f3a0d5490fe2e7030e414cfc4bda5d029306309eb42b7f812f5e984cd400`.
Independent dual-adapter application of the frozen Stage-1 corruption/blob
oracle and bounded-GC conformance package to the immutable successor remains a
required review gate before Stage 2.

## Explicit non-goals and integration blockers

- Object IDs depend only on canonical authenticated bytes, never physical
  extent placement. A future locator may be rebuildable routing metadata only.
- Fixed 1 MiB/F64/Q8 multimedia blocking and the accepted one-copy exact-extent
  transfer remain later seams; neither is implemented here.
- Roots are intentionally history-dependent. Ordinary path copying retains
  99.6591--99.9528% object bytes and uses 12--14 diff gets, while eager
  canonical rebuilding costs 122x--9600x publication for only 3.2% bulk-byte
  reduction. Adversarial independently reconstructed equal states may still
  degrade cold diff to `O(N + M)`; preserve a future deterministic local-resync
  seam without a second format or authority.
- Stage 2 is additionally blocked on authenticated block/range iteration and
  early field projection. The frozen 10K OLAP gate observed row-at-a-time
  materialization regressions of +213%..+665% RocksDB and +400%..+730% SlateDB.
- The branch A/B cold-diff gate remains unresolved (+10.2% RocksDB, +174.4%
  SlateDB). Do not accept or widen production integration until the independent
  iterator/fetch seam clears the exact dual-adapter thresholds.

No Stage-2 reader or writer is connected, no legacy production space is
deleted by this correction, and no PR is opened by this artifact.
