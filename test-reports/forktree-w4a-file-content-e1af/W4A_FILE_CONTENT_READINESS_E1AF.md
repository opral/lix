# W4a file-content publication readiness package for e1af

Status: `TEST/REPORT-ONLY`, source gate `RED` as expected, standalone model
`GREEN`, adapters and production builds `UNRUN`.

This is the smallest W4a cut. It covers ordinary `file_content_writes` only.
Multipart open/part/progress/abort lifecycle is excluded; an already completed
and authenticated manifest/Blob closure is an input to the file-content
publication. W5 root traversal and reclamation are a handoff contract, not a
W4a implementation.

## Immutable binding

Production baseline:

- commit: `e1af471b9ab0f598dafa7c2ddec7867667c81740`
- tree: `bfa0d271a723da8250ab76ada16fda90926f1099`
- parent: `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`
- parent..e1af full-index binary diff SHA-256:
  `9795ee3da81a06657a45a47a50417522a6a6bd7057e21eeb75597096417c9f3c`
- parent..e1af stable patch ID: `31cc575644bf17e65c59d558a03acffc848c2e20`

The source map uses the frozen W4 v2 contract at
`origin/codex/forktree-w4-fileblob-upload-readiness-e1af` / head
`ff79e87fdc9cf8db7d1b47158cf9c8715b7471a9`, report SHA
`f2bd370af93df7e9267592cf4dde8692e20a4aae81420e08307a68d8564f37a5`.

## Exact W4a production path allowlist

The future production cut may touch only these existing seams, and only for
the stated ownership conversion:

1. `packages/lix/src/transaction/commit.rs` — lower
   `PreparedWriteSet::file_content_writes` after stale reconciliation and
   remove its typed fail-closed rejection only when the lowerer is complete.
2. `packages/lix/src/transaction/context.rs` — only a transaction-local
   publication-plan handoff or counter adjustment if compiler-driven wiring
   requires it; never restore direct CAS publication or a second commit.
3. `packages/lix/src/transaction/staging.rs` and `transaction/types.rs` —
   only to preserve/consume existing `TransactionFileContent` and rollback
   snapshots. `PreparedCas` remains deferred with multipart and is not deleted
   in W4a.
4. `packages/lix/src/forktree/publication.rs` and `forktree/blob.rs` — only
   internal typed integration of the existing completed-manifest input; no new
   format, authority, cache, or public API.
5. `packages/lix/src/sql2/providers/file.rs` — only if the existing provider
   needs a typed completed-manifest handoff; SQL row/path/empty/tombstone
   semantics must remain unchanged.

No W4a production change is made by this package. Multipart
`session/media_upload.rs`, legacy CAS deletion, GC, checkpoint, selected
history, and multi-branch publication are explicitly deferred.

## Required one-authority chain

The accepted file-content operation must have exactly this shape:

```text
one retained commit CoherentView
  -> authenticate visible lix_binary_blob_ref + completed BlobManifestV1
  -> validate ordered BlobChunkV1 identities, sizes, and content digest
  -> extend one PreparedPublication with the state transition
  -> one into_storage_plan()
  -> transaction-owned prepare_write_set()
  -> transaction-owned prepared_commit.commit()
```

The visible `lix_binary_blob_ref` row remains the semantic BlobId owner.
`BlobManifestV1` and ordered `BlobChunkV1` objects are the authenticated
physical closure. A caller cannot supply an unverified BlobId or replace the
manifest with a fallback full writer. Existing transaction metadata,
idempotency receipts, selector fences, and filesystem index updates join the
same plan and commit.

The current e1af source correctly already has the final transaction boundary
at `transaction/context.rs:1476-1690`, including one commit-time read,
`commit::prepare_forktree_publication_with_parent_heads`, one
`into_storage_plan`, one `prepare_write_set`, and one backend commit. But
`transaction/commit.rs:1182-1189` still rejects nonempty
`file_content_writes`; that is W4a RED-01 and the primary production blocker.

## File-content correctness contract

The lowerer must authenticate before staging visible state:

- visible row identity equals the completed manifest's canonical BlobId;
- manifest logical length equals the declared row size;
- every named chunk has the expected domain/size/hash and ordered position;
- same-length replacement of any chunk or manifest fails closed;
- malformed, absent, wrong-kind, wrong-size, wrong-hash, or transplanted
  manifest/chunk closure produces no partial state, selector, or visible row;
- empty content and tombstone deletion remain distinct valid semantic cases;
- rollback/savepoint restores the pending file-content write and never leaves
  a staged manifest visible;
- stale same-owner state, stale selector/epoch, and idempotency conflicts
  reject atomically; an identical idempotency replay is a no-op;
- full reads authenticate the complete closure; bounded range reads use the
  retained view, authenticate the manifest/visited chunks, and never perform
  an unbounded payload scan; and
- cold reopen reproduces the same visible BlobId and bytes.

The fixed-chunk target is 64 MiB as 64 x 1 MiB. A 1% update must hash/write
only the changed chunk(s), reuse unchanged authenticated chunk IDs, and stage
one successor manifest/state edit. The standalone model checks the exact
64-chunk identity accounting without allocating adapter storage. Adapter
qualification remains future work.

## W5 final-root handoff

After the one W4a commit, W5 receives the visible BlobId root and must observe
it from a fresh coherent read before reachability work. W5 then authenticates
the transitive manifest/chunk closure, protects roots from branch/history/
checkpoint/serving selectors, retains shared chunks, and reclaims only after
the final root is retired. Corrupt or missing roots fail closed. W4a does not
delete immutable objects, call a GC writer, or recreate a root registry.

## Negative fixtures

The standalone model has executable, state-preserving rejection fixtures for:

| fixture | required result |
|---|---|
| wrong declared size | typed failure, no state change |
| wrong chunk identity | typed failure, no state change |
| same-size substituted chunk/manifest | typed failure before publication |
| second coherent read | reject; no silent refresh |
| second writer/plan or commit | reject; no second publication |
| direct CAS publication | reject; transaction boundary remains sole owner |
| fallback writer | reject; no unauthenticated downgrade |
| stale generation | reject atomically |
| idempotency conflict | reject; identical replay is a no-op |
| missing W5 root on reopen/handoff | fail closed |

The source verifier separately checks the existing e1af one-read/one-prepare/
one-commit boundary, no direct `PreparedPublication::commit`, and reports the
expected file-content rejection. The old multipart direct-CAS bridge is
reported `DEFERRED`, not silently accepted as part of W4a.

## Expected source RED

Run from the exact e1af source:

```bash
bash test-reports/forktree-w4a-file-content-e1af/verify_w4a_source.sh \
  "$PWD" e1af471b9ab0f598dafa7c2ddec7867667c81740
```

Expected exit is `1` because RED-01 (`file_content_writes` rejection) and the
known stale `binary_cas::kv` reference remain. The source verifier must not
fail because multipart's deferred `stage_atomic_cas_publication` and prepared
path bridge still exist; those are W4b/multipart deletion work.

## Standalone model command and contract

The model is pure Rust and has no Lix, storage, adapter, or network dependency:

```bash
rustc --edition=2021 -D warnings --test \
  test-reports/forktree-w4a-file-content-e1af/w4a_file_content_model.rs \
  -o /tmp/w4a-file-content-model-e1af
/tmp/w4a-file-content-model-e1af --test-threads=1
```

Expected model result is 7/7 passing. The model is not a production
authentication implementation; it is a deterministic oracle for ownership,
identity, atomicity, range bounds, reuse accounting, and W5 root handoff.

Frozen standalone evidence:

- model source SHA-256: `76f5e454b800d3e8dcc1d3925b971fe7a0b56a0b16847ebffdf114e90ab2d3ef`
- verifier source SHA-256: `4fea7713d70245dec6ca998edf4408f8eac58ee3ad90c01c57595523bf3e8429`
- compiled test executable SHA-256:
  `3d619843f6c7b17bbc87dd74e94ab4e91e8e056d47829219466b28bc0a998ae4`
- model run log SHA-256: `4aa987fa5e7bfbb435aa82a15da6237139a16afd5dc6221d8fb65f45dc1fe520`
- expected source-RED log SHA-256:
  `bc873d73c10a3d078cc784a4893184275b4114e793013ea3ff594d7975c9edfc`

## Future adapter command order (UNRUN)

After an immutable production successor wires the allowlisted cut, use separate
exact-SHA targets and stop on the first blocker:

```bash
set -Eeuo pipefail
TARGET=/root/repos/target-w4a-file-content
timeout --foreground --kill-after=5s 1200s cargo fmt --all -- --check
timeout --foreground --kill-after=5s 1200s \
  env CARGO_TARGET_DIR="$TARGET" cargo check -p lix
timeout --foreground --kill-after=5s 1200s \
  env CARGO_TARGET_DIR="$TARGET" cargo clippy -p lix --lib -- -D warnings
timeout --foreground --kill-after=5s 1200s \
  env CARGO_TARGET_DIR="$TARGET" cargo test -p lix --lib --no-run
```

Then run the focused oracle in exactly `Memory -> RocksDB -> SlateDB`, each
with `timeout --foreground --kill-after=5s 1200s`; a failed or expired cell
prevents later adapters. Record result digests, visible-row/manifest/chunk
reads and writes, changed/unchanged chunk bytes, allocation/RSS, backend
calls, disk after flush/reopen, and partial-publication state. Do not widen to
multipart lifecycle, 512 MiB, or comparator workloads from this W4a package.

## Package status

No production compilation, adapter runtime, benchmark, PR mutation, or merge
is performed here. This package is ready for independent review once its
standalone model and source RED command are hashed below.
