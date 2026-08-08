# ForkTree Stage 2 first-runnable artifact recipe

Status: preparation only. Do not run this recipe against a non-runnable
milestone and do not apply artifacts to Ryzen-V's production worktree.

## Readiness frontier

The independently approved topology milestone is:

- ref `origin/codex/forktree-stage2-milestone3e-topology-owned-reader`;
- head `af7899f41c489fe763ce1a64c5468083570979e2`;
- tree `da097bd739b50629ea39b155d4fa9efc870654e0`;
- parent `2e0cea1b91558179e6ed90847bc8b04b23de246f`;
- parent-to-head canonical full-index diff SHA-256
  `942d05f6c92f89e6c32c3b706c82c4e506e498263b5798c92eb2af607a219587`;
- parent-to-head stable patch ID
  `6ed511438fe08387ea40a5b6861f7db9f3544764`;
- `a12..head` canonical full-index diff SHA-256
  `734d02bfe332e4f8384301de243d85248b639aec0edeffac48b3f56a4ec271e5`.

This approval covers the one-`StorageRead` topology owner/cache contract and is
an ancestor of the latest approved reader frontier.

### Superseded blocked BlobRef predecessor

The first immutable BlobRef reader milestone was inspected and blocked:

- ref `origin/codex/forktree-stage2-milestone4-blobref-owned-view`;
- head `08f8dd5cf20842f79996fae9eb7b0924f074a084`;
- tree `19c8706d6bc3d1dbe9217b4f8386b19c66f027a8`;
- parent `af7899f41c489fe763ce1a64c5468083570979e2`;
- parent-to-head full-index SHA-256
  `d7217fafa02e3c50a6c10b7e3a7a0985697b4ba82beb1ba896d2cf636f34d71f`;
- `a12..head` full-index SHA-256
  `2a06b554cea5f28a24117dfb52c3e24be9ddc408bbb59405ea66b051b73ddb47`;
- stable patch ID `472848104c8fd79ddb3d1d7a4aa96b6a0690a703`.

The exact source review blocks range reads because the authenticated row's
semantic `BlobId` is not bound to the selected manifest on the range path.
`validate_manifest_owner` checks only logical size; a different valid manifest
with the same size can supply authenticated range bytes under the wrong public
blob identity. Full reads detect the mismatch only after complete
materialization. A corrected milestone must authenticate a canonical semantic
BlobId in the manifest (or equivalent canonical identity material), compare it
with the row ID before full or range publication, and add a same-size
manifest-substitution fail-closed test. The existing mismatched-size test is
insufficient. No compatibility or fallback path is permitted.

`08f8dd5cf20842f79996fae9eb7b0924f074a084` remains identity-pinned blocker
evidence only; it is superseded, not erased.

### Latest approved BlobRef readiness base

Two independent source reviews approved the narrow immutable successor:

- ref `origin/codex/forktree-stage2-milestone4b-blob-manifest-identity`;
- head `54e90dbf2bcf55c74de0be6ea4b217dc02cec89c`;
- tree `5a8da9f8b11d83bf8216e266beaf4042cee84068`;
- parent `08f8dd5cf20842f79996fae9eb7b0924f074a084`;
- parent-to-head full-index SHA-256
  `c507282c79b8de8b9cdec3960157276efef2769e2866a895b4c0d015b77fa8f1`;
- `a12..head` full-index SHA-256
  `d5adb4a322dbf98a590d765c9ee2179a3a1e583211cb6a41c3fe2bf2cd786bae`;
- stable patch ID `242302af3d9db6ecb81f258570b1ed0ec99cde3c`.

The successor authenticates an owner-private canonical BlobId inside the
manifest and compares it to the selected state-row identity before any payload
chunk load on full and range reads. It adds same-size multi-chunk substitution
negative coverage and a valid control without an index, cache, fallback, or
second serving owner. This approval is source-only: `54e90dbf...` remains
non-runnable and is not an acceptance-matrix result.

### Held writer milestone

The first ordinary atomic-writer slice is pinned but not approved for readiness:

- ref `origin/codex/forktree-stage2-milestone5a-ordinary-atomic-writer`;
- head `5c4cae810324a34c0adbbb5a1a0be5fba5348054`;
- tree `16741cdf6efce6bccdcf469406be1e1bce9b5f37`;
- parent `54e90dbf2bcf55c74de0be6ea4b217dc02cec89c`;
- parent-to-head full-index SHA-256
  `80f48db60e9205b2d3f242ee966f8d61d539a9dff65030d002a71c7f82bfaf2d`;
- `a12..head` full-index SHA-256
  `ad49aa816f4cece010f361f6fadf7f7b59f2003d6e905bd8f44341d613d08f56`;
- stable patch ID `2b57e2e9e23bd79343068a3b237ce20581c56526`.

Its ordinary single-branch cohort lowers once into the existing sole backend
commit, but upload/checkpoint/history/multi-branch/reachability publication
families remain. It is compile-red and has not cleared R2 atomicity or H2
deletion/residue review. Do not apply artifacts or treat it as a readiness base.

## Eligibility fence

The integrator may start only after the coordinator advertises an immutable
ref, exact head and tree as compile-green. Before creating a worktree:

1. fetch that exact ref into a private verification ref;
2. require its commit and tree to equal the advertised values;
3. require `54e90dbf2bcf55c74de0be6ea4b217dc02cec89c` to be an ancestor, unless the
   coordinator explicitly names a replacement accepted lineage;
4. require the BlobRef same-size manifest-substitution correction above to
   remain byte-identical in the candidate lineage;
5. require independent R2 approval that transaction, upload, and GC lower into
   one atomic owner publication/epoch fence with no second commit boundary;
6. require H2 deletion/residue approval and zero surviving independent ForkTree
   publication entry points for those three writer families;
7. run the frozen ten-ref verifier and require every identity/file check green;
8. check disk, then create a fresh detached disposable worktree and isolated
   Cargo target outside Ryzen-V's production worktree.

Example read-only preflight, with values supplied by the future handoff:

```sh
git fetch --no-tags origin \
  +refs/heads/<immutable-blob-spi-ref>:refs/stage2-candidate/next
test "$(git rev-parse refs/stage2-candidate/next^{commit})" = "<advertised-head>"
test "$(git rev-parse refs/stage2-candidate/next^{tree})" = "<advertised-tree>"
git merge-base --is-ancestor \
  54e90dbf2bcf55c74de0be6ea4b217dc02cec89c \
  refs/stage2-candidate/next
timeout 20m \
  packages/rs-sdk-tests/tests/forktree_stage2_acceptance_verify.sh .
```

Do not substitute a mutable branch tip, local working tree, or compile-red
milestone.

## Disposable artifact application

Create a fresh detached worktree only after the eligibility fence passes.
Materialize each row's test/report artifacts from its private verified ref;
never merge an artifact branch. Before applying a row, generate a path-limited
patch and reject any hunk outside tests, benches, scripts, reports, or purely
additive test/bench target registration. Production owner/facade blobs must
remain byte-identical to the candidate. Missing SPI is a blocker, not authority
to copy an oracle model, widen visibility, or restore a legacy API.

After each materialization, record candidate head/tree, artifact ref/head,
prospective tree, patch SHA-256, and changed paths. Use a fresh nonexistent
database/evidence path and the row's isolated target. Stop at the first failure.

The fixed execution order is:

1. fmt/diff, residue/delegation/CLI/cursor source gates;
2. production-owner 65-row batch-1 delete, RocksDB then SlateDB;
3. 1K point read, RocksDB then SlateDB; stop unless both show a meaningful
   improvement greater than 10% and no critical regression greater than 5%;
4. SQL RocksDB/SlateDB;
5. three-row checkpoint RocksDB/SlateDB;
6. no-lease and sealed GC/publication RocksDB/SlateDB;
7. OLAP 10K RocksDB/SlateDB plus corruption, then 50K, then 500K;
8. multimedia and broader version-control closeout.

Every build and process cell is capped at 20 minutes. A compile timeout is a
host boundary, not a pass. No broad gate runs after a focused blocker.
