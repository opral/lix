# W4a file-content publication correction package

Status: TEST/REPORT-ONLY. This package is the direct report-only successor of
29f83418ddfbd7509ac7f9ba0245b6340a5fa522. It changes no production source,
adapter, PR, or merge state.

## Immutable source binding

The frozen production baseline is:

- commit: e1af471b9ab0f598dafa7c2ddec7867667c81740
- tree: bfa0d271a723da8250ab76ada16fda90926f1099
- parent: b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
- parent..baseline full-index binary SHA-256:
  9795ee3da81a06657a45a47a50417522a6a6bd7057e21eeb75597096417c9f3c
- parent..baseline stable patch ID:
  31cc575644bf17e65c59d558a03acffc848c2e20

This package freezes the baseline RED calibration and adds a candidate-aware
source verifier with a genuine GREEN self-test. The verifier accepts:

  BASE_ROOT BASE_COMMIT
  BASE_ROOT BASE_COMMIT CANDIDATE_ROOT CANDIDATE_COMMIT

The base must be the exact e1af commit/tree/parent. The candidate must be the
actual checked-out candidate commit, and its diff from the supplied base must
be limited to the W4a closure allowlist.

## Exact candidate scope

The candidate source allowlist is deliberately narrow:

1. packages/lix/src/transaction/commit.rs: lower file_content_writes only after
   authenticated ForkTree lowering; remove the old typed rejection.
2. packages/lix/src/transaction/context.rs: transaction-local handoff only;
   one operation-owned coherent read, one publication plan, and the existing
   sole backend prepare/commit boundary.
3. packages/lix/src/transaction/staging.rs and transaction/types.rs: preserve
   existing file-content staging and rollback types only.
4. packages/lix/src/forktree/publication.rs and forktree/blob.rs: typed
   BlobId/BlobManifestV1/BlobChunkV1 closure authentication and publication
   lowering only.
5. packages/lix/src/sql2/providers/file.rs: existing typed file-provider
   handoff only when required.

Any changed path outside this set is a candidate blocker. Multipart upload,
legacy CAS deletion, GC, checkpoint, history, selected branch, W5
implementation, new format, cache, compatibility reader, fallback writer,
or second durable authority is outside this package.

## Source-proof contract

The candidate verifier is structural and argument-aware, not a token/counter
oracle. It inspects the commit_prepared operation through its actual single
storage commit and requires:

  operation-owned begin_read binding
    -> the same bound read argument enters the one
       prepare_forktree_publication_with_parent_heads call
    -> one into_storage_plan
    -> one prepare_write_set
    -> one prepared_commit.commit

It rejects a second operation read, a second plan, a second backend prepare or
commit, and any direct PreparedPublication commit. The closure must expose
BlobId, BlobManifestV1, BlobChunkV1, CoherentView, and
PreparedPublication. It rejects the independent or legacy route spellings
stage_atomic_cas_publication, execute_fast_lix_file_prepared_path_write,
binary_cas::kv, fallback_full_write, and legacy_file_content_writer in the
W4a closure.

The verifier first freezes the e1af RED baseline. Its two-argument mode must
exit 1 with RED-01 and RED-06. Its four-argument mode runs the same baseline
calibration, enforces the whole candidate diff scope, and can return genuine
GREEN only when all argument-aware checks pass. The self-test creates an
ephemeral two-commit source repository and exercises the same four-argument
candidate path; it is not a production source or adapter claim.

## File-content model

The pure Rust model is ownership-shaped:

  CoherentView
    -> PreparedPublication
    -> into_storage_plan
    -> PreparedCommit
    -> commit

There are no caller-supplied read, plan, commit, direct-CAS, or fallback flags
that can make an accepted publication valid. Legacy paths are represented only
as rejected enum fixtures; the transaction-owned route never consumes them.
BlobId is derived from the authenticated manifest/chunk closure rather than
accepted from a caller as an independent authority.

The model and negative fixtures cover:

- manifest size, chunk identity, BlobId identity, and same-size substitution;
- partial reads that authenticate a visited chunk before returning its bytes;
- no partial state on malformed size/chunk or manifest failure;
- stale view/generation and same-owner idempotency conflict;
- identical idempotency replay as a no-op;
- second-read, second-writer, direct-CAS, and fallback rejection fixtures;
- exact 64 x 1 MiB layout with 63 unchanged chunk identities and one rehash;
- cold reopen and W5 final-reference handoff;
- missing root failure on both reopen and W5 handoff.

The model uses deterministic u64 fingerprints and no production codec or
adapter. It is a structural/semantic preflight oracle, not production
authentication evidence.

## Replayed evidence

Baseline source calibration:

  bash verify_w4a_source.sh /root/repos/lix-dead-module-audit-e1af \
    e1af471b9ab0f598dafa7c2ddec7867667c81740

Expected exit is 1. It reproduces one coherent read, one prepare, one backend
commit, no direct PreparedPublication commit, RED-01 file-content rejection,
and RED-06 stale Binary CAS KV references.

Candidate-parametric GREEN self-test:

  bash verify_w4a_source.sh --self-test

It creates a temporary base/candidate repository, invokes the four-argument
path, validates the allowlisted diff and bound read argument, and reports
CANDIDATE-GREEN-RESULT=GREEN.

Standalone model:

  rustfmt --edition 2021 --check \
    test-reports/forktree-w4a-file-content-e1af/w4a_file_content_model.rs

  rustc --edition=2021 -D warnings --test \
    test-reports/forktree-w4a-file-content-e1af/w4a_file_content_model.rs \
    -o w4a-model

  ./w4a-model --test-threads=1

The corrected model is rustfmt-clean, compiles with warnings denied, and has
8/8 passing tests. The exact logs are included in this package and verified by
SHA256SUMS.

## Future production qualification

This package does not claim Memory, RocksDB, or SlateDB execution. After a
candidate source gate turns GREEN, run the existing bounded checks in order:

  cargo fmt --all -- --check
  cargo check -p lix
  cargo clippy -p lix --lib -- -D warnings
  cargo test -p lix --lib --no-run

Then run the focused file-content oracle in Memory, RocksDB, and SlateDB, one
adapter at a time with a 1200-second cap and stop on first blocker. It must
prove one retained view, one PreparedPublication/plan/backend commit,
BlobId-only visible authority, authenticated manifest/chunk closure, bounded
partial reads, stale/rollback CAS, 63/64 unchanged-chunk reuse, cold reopen,
corruption fail-closed, and W5 final-reference retention/reclamation. No
token-only model result substitutes for these adapter gates.

## Package state

No production edits, adapter runtime, production build, PR mutation, or merge
was performed. Independent review is still required; this package is not
self-approved.
