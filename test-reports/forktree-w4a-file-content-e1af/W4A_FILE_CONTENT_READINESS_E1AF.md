# W4a file-content publication correction package

Status: TEST/REPORT-ONLY. This is the direct immutable successor of
3e9a7f2c611a1bbad12fd271ca7a43332a4fe1c5. It changes no production source,
adapter, PR, or merge state and makes no production-runtime claim.

## Frozen baseline

- commit: e1af471b9ab0f598dafa7c2ddec7867667c81740
- tree: bfa0d271a723da8250ab76ada16fda90926f1099
- parent: b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
- parent..baseline full-index binary SHA-256:
  9795ee3da81a06657a45a47a50417522a6a6bd7057e21eeb75597096417c9f3c
- parent..baseline stable patch ID: 31cc575644bf17e65c59d558a03acffc848c2e20

The exact e1af source verifier remains RED: file-content writes are still
rejected before ForkTree lowering and stale Binary-CAS KV owner references
remain. The baseline is calibrated before any candidate checks.

## Correction contract

The candidate-parametric verifier accepts only a checked-out candidate whose
diff from e1af is limited to the seven-file W4a allowlist:

1. `packages/lix/src/transaction/commit.rs` — lower `file_content_writes`
   after authenticated ForkTree lowering and remove the typed rejection.
2. `packages/lix/src/transaction/context.rs` — operation-local handoff only:
   one retained coherent read, one publication, one plan, one prepare, and
   the existing single transaction commit.
3. `packages/lix/src/transaction/staging.rs` and
   `packages/lix/src/transaction/types.rs` — existing staging/rollback types.
4. `packages/lix/src/forktree/publication.rs` and
   `packages/lix/src/forktree/blob.rs` — the authenticated file-content owner.
5. `packages/lix/src/sql2/providers/file.rs` — existing typed provider handoff
   only where required.

Any changed path outside this closure is a blocker. Multipart upload, legacy
CAS deletion, GC, checkpoint/history, selected-branch state, W5 production
logic, new format, cache/index, compatibility reader, fallback writer, and a
second durable authority are outside the package.

### BlobId authority

The accepted proof must not treat a caller-provided BlobId token as authority.
The owner-private BlobId is structurally derived from the authenticated ordered
`BlobManifestV1`/`BlobChunkV1` closure, including order and manifest shape. The
derived value is compared with the row identity before payload bytes are read.
The row identity is obtained from the operation-owned retained read/view; it is
not a `blob_id` parameter to the publication constructor. A caller-supplied ID
route is a negative fixture and a source blocker. Malformed manifest/chunk,
same-size substitution, wrong row identity, or wrong BlobId fails before
publication and leaves durable state unchanged.

### One-read publication authority

The structural proof is argument-aware:

```text
one operation-owned begin_read
  -> the exact bound read/view argument
  -> one prepare_forktree_publication_with_parent_heads
  -> one into_storage_plan
  -> one prepare_write_set
  -> one prepared_commit.commit
```

The retained read/lease is non-copyable in the model and the verifier rejects
read cloning, a second begin-read in the operation, or pairing the publication
with another read. The verifier scans the complete allowlisted closure for
`begin_write`, generic put/delete/commit/write calls, direct prepared-publication
commit, durable cache/index tokens, alternate authority, fallback, legacy
writer/reader, and a second file-content publication. A token-only enum check
is not sufficient.

## Pure model and negative controls

`w4a_file_content_model.rs` is a production-independent Rust model. Its
owner-private `BlobId` is computed from authenticated ordered chunks and the
manifest total. `FileOperation` receives no BlobId; its retained `ReadLease`
contains the authenticated row identity. A separate rejected fixture proves
that a caller-supplied BlobId cannot enter the accepted route.

The model covers:

- valid one-read/one-publication/one-plan/one-prepare/one-commit and reopen;
- wrong-size, malformed chunk, wrong chunk identity, and same-size manifest
  substitution before durable mutation;
- row-identity mismatch before payload bytes and authenticated owner derivation;
- stale read/generation, same-owner idempotency replay/conflict, and rollback;
- two distinct read IDs, non-copy operation binding, direct-CAS/fallback/
  second-writer rejection, and caller-supplied-ID rejection;
- partial range reads with visited-chunk authentication;
- exact 64 x 1 MiB layout with 63 unchanged chunk identities and one changed
  chunk;
- cold reopen and W5 final-reference handoff with missing-root failure.

The model is a structural preflight only. It is not a production codec,
adapter, authentication, or performance claim.

## Reproducible gates

Baseline RED (expected exit 1):

```text
bash verify_w4a_source.sh /root/repos/lix-w4a-baseline-e1af \
  e1af471b9ab0f598dafa7c2ddec7867667c81740
```

Candidate-parametric genuine GREEN self-test:

```text
bash verify_w4a_source.sh --self-test
```

The self-test creates a temporary base/candidate repository, validates the
allowlisted diff, checks the exact call/argument chain, proves the structural
BlobId ordering, scans the closure for forbidden publication routes, and emits
`CANDIDATE-GREEN-RESULT=GREEN`.

Warnings-denied model:

```text
rustfmt --edition 2021 --check \
  test-reports/forktree-w4a-file-content-e1af/w4a_file_content_model.rs
rustc --edition=2021 -D warnings --test \
  test-reports/forktree-w4a-file-content-e1af/w4a_file_content_model.rs \
  -o w4a-model-v2
./w4a-model-v2 --test-threads=1
```

The exact model result is 9/9 passing. `SOURCE_RED.log`, `SOURCE_GREEN.log`,
and `MODEL_RUN.log` are included and checksummed.

## Future production qualification

This report does not claim adapter execution. Only after a real production
candidate turns the source gate GREEN, run the existing bounded checks in
order, one cell at a time with a 1200-second cap and stop on first blocker:

```text
cargo fmt --all -- --check
cargo check -p lix
cargo clippy -p lix --lib -- -D warnings
cargo test -p lix --lib --no-run
```

Then run the focused file-content oracle in Memory, RocksDB, and SlateDB. It
must prove one retained view and one transaction-owned
read→publication→plan→prepare→commit, owner-derived BlobId before any bytes,
authenticated manifest/chunk identity, bounded partial reads, stale/rollback
CAS, 63/64 unchanged-chunk reuse, cold reopen, corruption fail-closed, and W5
final-reference retention/reclamation. No token-only or model-only result
substitutes for adapter gates.

## State

No production edits, adapter runtime, production build, PR mutation, or merge
was performed. This package is frozen for independent review and is not
self-approved.
