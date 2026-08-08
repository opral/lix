# W1b-5 working-diff provider readiness report

Status: test/report-only calibration; not an approval.

## Immutable anchor

- target: `4107bef177c00694574b4fc65d6bb209239ee877`
- tree: `9f3ff98a6745daae54102a7754036ef1ced111dd`
- parent: `c8992e070a9a988a695bdb77f9a49e214431a5bc`
- subject: `sql2: share operation changelog facade and graph`

The package is separate from production source. It contains no adapter,
runtime, benchmark, PR, merge, or persisted-format change.

## Exact source-gate calibration

Command:

```text
test-reports/w1b5-working-diff-4107/verify_source_contract.sh \
  /root/repos/lix-w1b5-review \
  4107bef177c00694574b4fc65d6bb209239ee877
```

Exit status is expected `1` for this pre-cut source. The self-test is GREEN
(one positive structural fixture and six negative fixtures rejected); anchor
and production-scope checks are GREEN. The candidate-parametric production
gate reports exactly 12 RED findings:

1. SQL working-diff lacks authenticated ascending ordering.
2. SQL working-diff still uses `BranchRefReader`/`selected_heads`.
3. Filesystem working-diff lacks authenticated ascending ordering.
4. Filesystem working-diff still uses `BranchRefReader`/`selected_heads`.
5. Checkpoint provider lacks authenticated ascending ordering.
6. Checkpoint provider still uses `BranchRefReader`/`selected_heads`.
7. Filesystem working-diff lacks a `BlobRef`/`BlobId` identity seam.
8. Filesystem working-diff lacks authenticated blob-payload validation.
9. `gc.rs` retains current-layout `TrackedHead`/`BranchHeadControl`
   ownership.
10. `init.rs` retains current-layout `TrackedHead`/`BranchHeadControl`
    ownership.
11. `live_state/context.rs` retains current-layout
    `TrackedHead`/`BranchHeadControl` ownership.
12. `transaction/context.rs` retains a `TrackedStateStoreReader`
    factory/callback.

Source replay log SHA-256:
`f563bb18b6914fba3175abc90eb273378568071d0c82a8ce4b4ac95f1ec41c7a`.

## Standalone semantic oracle

The model is compiled without the workspace and with warnings denied:

```text
rustfmt --edition 2024 --check test-reports/w1b5-working-diff-4107/working_diff_oracle.rs
rustc --edition 2024 --test -D warnings \
  test-reports/w1b5-working-diff-4107/working_diff_oracle.rs \
  -o /dev/shm/w1b5-working-diff-oracle-4107-final
/dev/shm/w1b5-working-diff-oracle-4107-final
```

The result is 7/7 GREEN. It covers checkpoint-to-ordinary history,
branch/global overlays, tracked/untracked visibility, NULL versus tombstone,
file/blob identity and payload, projection/order/LIMIT, marker-to-walked-root
chronology, missing/malformed/wrong-kind/identity corruption, reopen, exact
base/head identities, deterministic digest, and zero partial output/writes.

- model binary SHA-256:
  `93e74d51aca7f5b3fb94db734d08476afbd0a12dc86eb7c20fb62468b002ec72`
- replay log is `ORACLE_REPLAY.log` in this package.

## Candidate contract

The future W1b-5 candidate gate is whole-scope and argument-aware. It must
thread one operation-owned retained ForkTree facade/graph capability through
SQL working-diff, filesystem working-diff, and checkpoint-baseline reads;
derive the marker from the walked commit/root rather than a persisted marker
authority; bind exact base/head identities; validate tracked, untracked,
file, and BlobRef content before projection; preserve deterministic ascending
ordering and LIMIT; and fail closed on missing, NULL, tombstone, malformed,
wrong-kind, substituted, or duplicate required content. It rejects a second
read, fresh graph construction, raw store access, fallback, cache, alternate
authority, and legacy TrackedState/TrackedHead readers.

The dependency order is reader-first: operation-owned view and chronology,
semantic row/blob validation, query ordering/projection, provider reader
deletion, then handoff of current-layout init/GC/TrackedHead owners to their
separate W3/W5 cuts. Public working-diff tables and DTO names remain semantic
facades, not durable authorities.

No Memory, RocksDB, or SlateDB runtime claim is made from this package; those
commands remain dormant until a compile-green immutable production successor.
No self-approval is issued.
