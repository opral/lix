# ForkTree successive-delete root-collapse correction contract

## Evidence and causal boundary

- Accepted runnable model: `bc82385ec42b1789018fbd1213f637c19104a02c`, tree `abfaa70faf12c3cdcbe3f990dbf8b4e01340af4a`.
- Frozen delete reproducer: `9713361663df727af88dcf88aa05bd4b998c4149`, tree `a1b1ef1bed7f2a48b9f11a1a6288f325b3f64590`, parent exact `bc823`; full-index SHA-256 `6d633a6d61b33700f12b05b5f38486a16941eb556c40bba7a5e3c42004ebf065`; stable patch ID `12ac627dff19618013f300812f427fa6677685b0`.
- Stage2 source map only: approved reader milestone `54e90dbf2bcf55c74de0be6ea4b217dc02cec89c`, tree `5a8da9f8b11d83bf8216e266beaf4042cee84068`. It is not a runnable writer qualification.
- The 971 model preloads only changed paths into `node_cache`. With 65 rows, the fixed 64-row blocks produce two leaves (64 + 1). Sequential deletion 63 empties the first leaf. Parent rewriting retains the one-row second leaf only as its authenticated `NodeRef`; `finish_root` follows that sole child, but `load_pending_or_cached_node` cannot resolve its unchanged body and reports it absent from the operation-local working set. A 64-row single leaf and the 65-row b100 cohort do not cross this missing-body boundary.

## Smallest accepted correction

At the moment root collapse follows an unchanged sole child that is absent from pending/new objects and the operation-local changed-path working set:

1. Resolve exactly that child's ObjectId through the mutation/publication owner's already-retained `StorageRead`. Do not call `begin_read`, refresh a snapshot, or retry.
2. Authenticate the fetched immutable object against ObjectId/domain/length and decode it as the expected ordered-tree node kind.
3. Exact-compare the decoded body-derived `NodeRef` (identity, maximum key, summary/count) with the sole sibling reference authenticated by the parent. Missing, malformed, wrong-domain/kind, identity mismatch, summary mismatch, or boundary mismatch fails the entire publication before write acquisition.
4. Use the authenticated child as the next root. It remains an existing immutable object and must not be rewritten, copied, re-keyed, or inserted into the candidate's durable object writes.
5. Any positive body retained for the remainder of the operation is merely the existing bounded mutation working set tied to that same `StorageRead`; it is discarded at operation end and is not a cache, index, locator, selector, or authority.

This preserves one ObjectId authority, path copying, and `O(U log_F N + copied blocks)` time / `O(U log_F N)` bounded operation memory. It adds at most one point read per collapsed unchanged level (bounded by tree height), no full-tree reconstruction, and no new write or format.

## Forbidden corrections

- No second `StorageRead`, snapshot refresh, retry, fallback-to-rebuild, full scan, or full tree materialization.
- No persisted or cross-operation sibling cache/index/locator and no second object/root/selector authority.
- No blind trust in the parent `NodeRef` once that child becomes the selected root; the child body must be fetched and authenticated before publication.
- No copying the unchanged sibling into pending/durable object writes merely to make it locally visible.
- No weakening of ObjectId, domain, kind, summary, max-key, count, ordering, or selector CAS validation.

## Discriminating owner oracle

The adjacent `forktree_successive_delete_owner_oracle.rs` is a test-only sealed-facade contract. The writer owner maps it to the accepted production owner without exposing raw spaces or ObjectIds. It runs both RocksDB and SlateDB and requires:

- 64 rows, sequential b1: pass, exact bytes/order, cold reopen, 64 publication reads, 64 tree gets/keys, 64 reachable node puts, 64 commits.
- 65 rows, sequential b1: pass through delete index 63, exact bytes/order, cold reopen; exact totals are 65 publication reads, 130 tree get calls/keys, 127 reachable node puts, 65 commits. The collapse attempt itself is one retained read, three tree object gets (root, removed child, unchanged sibling), zero tree node puts, and one atomic publication.
- 65 rows, b100 control: pass and cold reopen; one publication read, two batched tree get calls / three keys, one final empty-leaf put, one commit.
- Missing, malformed, and validly encoded wrong-kind sole sibling: each fails before write acquisition/publication, with one retained read, three tree gets, zero node/selector writes and zero commit. The prior 63 deletes remain exact after cold reopen.

Counters are owner-scoped: selector/catalog/commit envelope reads outside ordered-tree traversal must be reported separately and cannot hide an extra `begin_read`. A candidate passes only when the same test blob is green on both adapters and source review confirms the read handle is the caller-owned publication view.

## Independent frozen replay

The immutable 971 reproducer was rebuilt once in 14m07s and run on fresh fixtures:

```sh
CARGO_TARGET_DIR=/root/repos/lix-forktree-delete-971-target CARGO_BUILD_JOBS=2 \
  cargo build --release --manifest-path packages/engine-benchmarks/Cargo.toml \
  --bench forktree_delete_repro --features 'storage-benches slatedb' -j2

BIN=/root/repos/lix-forktree-delete-971-target/release/deps/forktree_delete_repro-5a11639c34637129
$BIN <rocksdb|slatedb> <fresh-path> 64 1 expect-pass
$BIN <rocksdb|slatedb> <fresh-path> 65 1 expect-fail
$BIN <rocksdb|slatedb> <fresh-path> 65 100 expect-pass
```

- Reproducer source SHA-256: `652abfe9dde5a1ff09b45b63c59a5efc6f9e53a7b5ac280a14f0161328c6f533`.
- Accepted model source SHA-256: `818818f673249bf50fb623199e3c8884985146683c41344ec8a2cf74a6d070ea`.
- Release binary SHA-256: `f098488ef3775615a0771a464d55e39ff43026d8d59026883f6d544d57c529ec`.
- Rocks 64/b1 PASS log: `e9a5920c0801c64eea6fe4d6babfbc4e699a183e72f4986d3a36df95bc65d775`.
- Rocks 65/b1 expected failure log: `6907e3d3f62ae0a66eeb8684eb67b5a58f101b2e76e4ee7962d6447313179119`.
- Rocks 65/b100 PASS log: `402c23edb0495beb8db8d44fb2ac750a3f0b27b16cc59190a40c51a21326d17f`.
- Slate 64/b1 PASS log: `f5b2a6dbefc7d1c5a7b9acb2fa0b7d8012295bbdbbddfe20657434f3d2cf1829`.
- Slate 65/b1 expected failure log: `88c09d7a89b574fa3503bb8bd2385893c11879e8ddebdf92dea91ed3387438f3`.
- Slate 65/b100 PASS log: `51c058dbe6aa166c4a01aa6f383db1685f2e8315cd248af8f3cbd673815b55ae`.

Both 65/b1 runs fail identically at `chunk_index=63`, after 63 committed deletes, with two cold-reopen rows retained and unchanged sibling ObjectId `dcc6f4c15f90b3a925955786cfdb719796f40009dc38de94b0795b6989f1ad3b` reported absent from the working set. This exact identity is evidence only and must not enter the production API or fix.

## Application sequencing

Do not apply this contract to non-runnable Milestone 5A (`5c4cae810324a34c0adbbb5a1a0be5fba5348054`). It is frozen for Ryzen-V to apply only after the first accepted writer hard-cut milestone exposes the sealed production owner and routes every relevant publication family through transaction-owned atomic lowering.
