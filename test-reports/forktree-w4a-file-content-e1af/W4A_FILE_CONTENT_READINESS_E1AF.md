# W4a v3 file-content publication correction package

Status: TEST/REPORT-ONLY. This is a direct immutable successor of
`f2f4c41bd3a64187f8288ca0396fd364a1f2f8fe` and changes no production source,
adapter, PR, or merge state. It closes the prior package-model blockers for
shared chunks, exact final-reference release, persisted cold reopen, and
discriminating source-negative fixtures.

## Frozen anchor

- direct parent: `f2f4c41bd3a64187f8288ca0396fd364a1f2f8fe`
- parent tree: `957002895e57facad058b10bd73d68ecb7a0c864`
- original baseline: `e1af471b9ab0f598dafa7c2ddec7867667c81740`
- baseline tree: `bfa0d271a723da8250ab76ada16fda90926f1099`
- baseline parent: `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`
- parent..baseline full-index binary SHA-256:
  `9795ee3da81a06657a45a47a50417522a6a6bd7057e21eeb75597096417c9f3c`
- parent..baseline stable patch ID: `31cc575644bf17e65c59d558a03acffc848c2e20`

The exact e1af source verifier remains RED: file-content writes are rejected
before ForkTree lowering and stale Binary-CAS KV owner references remain.

## Source contract

The candidate-parametric verifier requires the candidate to descend from its
anchor and limits the complete committed diff to the seven W4a production
paths: transaction commit/context/staging/types, ForkTree publication/blob,
and the typed SQL file provider. Cargo, workflow, native, compatibility,
fallback, cache/index, GC, checkpoint/history, selector, and any other path is
a hard failure.

The accepted authority is one operation-owned coherent read, one ForkTree
publication, one storage plan, one prepared write set, and one commit. The
private canonical BlobId is derived from authenticated ordered manifest/chunk
identity and compared with the exact retained-read row identity and size
before the bounded range payload request. The verifier rejects a copied read,
second view/read/plan/commit, caller-supplied BlobId, swapped row/read
identity, mismatched publication argument, validation after bytes, unbounded
payload, alternate writer/cache/index/authority, compatibility, fallback, or
legacy route. Its self-test exercises each negative fixture against the actual
candidate checker rather than only recording token counts.

## Persisted model contract

The warnings-denied Rust model now reconstructs authenticated persisted state
on cold reopen. It stores manifest references, domain-authenticated chunk
objects, semantic row BlobId/size, and typed branch/history/checkpoint/upload
roots. Reopen validates object kind, content/domain hash, manifest total and
canonical BlobId, ordered chunk identity/lengths, duplicate/order/missing
objects, retained root, and row identity before returning any state.

The model explicitly covers:

- multiple manifests/files/branches sharing unchanged chunks;
- branch, history, checkpoint, and open-upload roots retaining shared data;
- a 65-entry checkpoint retention window;
- exact final-reference reclamation with no premature delete or leak;
- same-size multi-chunk substitution and malformed/missing/wrong-kind,
  wrong-content, wrong-order, duplicate, and row-size/identity corruption;
- cold reopen followed by bounded partial range authentication with zero
  output/state mutation on a visited corruption;
- stale generation, rollback/idempotency atomicity, one view/plan/commit,
  private owner BlobId, and forbidden-route controls;
- actual 64 MiB = 64 x 1 MiB chunks with 63 unchanged and one rehashed.

No production codec, adapter, runtime, performance, or compatibility claim is
made by this package.

## Reproducible gates

Baseline RED (expected exit 1):

```text
bash verify_w4a_source.sh /root/repos/lix-w4a-baseline-e1af \
  e1af471b9ab0f598dafa7c2ddec7867667c81740
```

Candidate source-positive and negative-fixture gate:

```text
bash verify_w4a_source.sh --self-test
```

Expected output includes `CANDIDATE-GREEN-RESULT=GREEN` and ten
`NEGATIVE-PASS` fixtures.

Warnings-denied model:

```text
rustfmt --edition 2021 --check \
  test-reports/forktree-w4a-file-content-e1af/w4a_file_content_model.rs
rustc --edition=2021 -D warnings --test \
  test-reports/forktree-w4a-file-content-e1af/w4a_file_content_model.rs \
  -o w4a-model-v3
./w4a-model-v3 --test-threads=1
```

The exact model result is 13/13 passing. `SOURCE_RED.log`, `SOURCE_GREEN.log`,
and `MODEL_RUN.log` are included and checksummed.

## Stop boundary

This is a package-local correction only. No production build/runtime or
Memory/RocksDB/SlateDB qualification is authorized by this artifact. A future
production candidate must independently prove the same one-read/one-plan/
one-commit authority, shared/final-reference behavior, persisted cold-reopen
authentication, corruption fail-closed behavior, and bounded range semantics
before adapter qualification.
