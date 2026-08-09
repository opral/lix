# Large-file hash-pruned diff/merge experiment — NO-CUT

## Immutable base

- Base/head before this experiment: `f3616b2c8ca4ed079bc42f0f4373e059539a0765`
- Base tree: `5aea0e1274cc1edd685e5030ca2a6f3144cb1908`
- Source-only full-index working-diff SHA-256 before this report:
  `f27fb73e92b341ecb9ec672ac9a8e7c8a3ca004cd2924ef94ee3737b8e350173`
- Source-only stable patch ID: `785fad8a087283eb645030134965ac112670fef2`

## Decision

**NO-CUT. Do not integrate this commit into the ForkTree hard-cut draft.**

The requested acceptance boundary was a greater-than-20-percent improvement on
both RocksDB and SlateDB for public large-file branch diff and true merge,
including backend bytes and RSS, with identical public digests. That gate was
not reached and no cross-adapter branch/merge performance claim is made.

Independent source review found that the public branch-diff and merge-analysis
paths compare authenticated state rows and change identities; they do not load
BlobChunk payloads. Blob-Merkle pruning at the implemented boundary therefore
cannot provide the requested branch/merge improvement. The only live payload
materialization owner found was metadata-only `lix_file_history`.

## Experimental source cut

The experiment changes metadata-only file history so it:

- binds every live file row to its exact authenticated BlobRef and manifest;
- validates a first manifest closure without constructing a terminal full-file
  `Vec`, retaining at most eight chunk payloads at once;
- compares later same-geometry manifests by authenticated ObjectId and descends
  only changed Merkle branches/chunks;
- fully authenticates append/truncate or incompatible geometry;
- preserves the existing complete payload path when `content` is projected;
- uses the existing retained ForkTree read, with no persistent cache, fallback,
  alternate object format, BlobId-only lookup, or second authority.

Changed source paths:

1. `packages/lix/src/forktree/merkle.rs`
2. `packages/lix/src/forktree/blob.rs`
3. `packages/lix/src/forktree/view.rs`
4. `packages/lix/src/forktree/model.rs`
5. `packages/lix/src/sql2/providers/file_history.rs`
6. `packages/lix/src/forktree/tests.rs`

## Complexity result

- Public branch diff remains a full authenticated state comparison; this
  experiment does not change its complexity.
- True merge remains two authenticated state/change comparisons plus conflict
  resolution; this experiment does not change its complexity.
- Metadata-only file history changes from full payload materialization for each
  selected historical row to one fully authenticated initial closure plus
  changed-subtree traversal for same-geometry successors. Peak chunk retention
  is bounded to an eight-chunk operation-local page.

This is a useful local proof of the file-history owner, but it is not the
requested large-file diff/merge hard cut.

## Verification

All commands used the warm target
`/root/repos/target-forktree-2f1-oltp` and task-local TMPDIR
`/root/repos/.tmp-merkle-diff-merge`.

- `cargo fmt --all -- --check`: PASS
- `git diff --check`: PASS
- `cargo test -p lix --lib --features all-simulations --no-run`: PASS
  (15 inherited warnings, no experiment diagnostic)
- `historical_merkle_validation_prunes_equal_subtrees_and_chunks`: PASS 1/1
- `forktree::merkle::tests`: PASS 12/12
- `sql2::providers::file_history::tests`: PASS 17/17

The new pruning test proves that a one-chunk overwrite validates fewer object
keys than a complete successor closure. Existing Merkle tests retain malformed
shape, missing/wrong-domain object, substitution, cycle, range, append,
truncate, insertion, deletion, and successor-identity controls. Existing file
history tests retain owner, BlobRef, tombstone, grouping, ordering, and
projection controls.

No RocksDB/SlateDB branch-diff or true-merge benchmark was run because the
implemented code is not on either live path. Running an unrelated large-file
read benchmark would not satisfy the acceptance gate.

## Independent local reviews

The source reviewer confirmed the authenticated owner boundary and that the
smallest safe Blob-Merkle helper belongs in `forktree/merkle.rs`, reached through
the retained `blob.rs`/`view.rs` read. The reviewer also confirmed that public
diff and merge do not traverse BlobChunks and that shared immutable chunks must
remain governed solely by existing reachability.

The performance reviewer returned NO-CUT for the requested blanket claim:
metadata-only file history is a plausible win, but branch diff needs state-root
pruning and true merge normally does not materialize blob payloads. No existing
paired RocksDB/SlateDB benchmark measures this experimental history path.

## Preservation

This ref is frozen only as rejected experimental evidence. It must not be
composed into PR #1264. The approved f361 worktree and target remain preserved.
