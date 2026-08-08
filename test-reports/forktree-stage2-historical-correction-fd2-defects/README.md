# ForkTree historical correction oracle (fd2 defect successor)

This package is a test/report-only direct successor to immutable head
`2edc5cda354c456b1ece54f3f3a81485276e728d`. Its actual parent is
`b493056059136ac1a394c912c80416d3d4b7fde4`; the fd2 source anchor remains
`fd2be256d763f17e9f127d4c984e36fba191cb82` with its separate `fd2..2ed`
diff identity. No production source, Cargo manifest, adapter, PR, or main
branch is changed.

The pure model and structural verifier cover:

1. authenticated descriptor row key, snapshot ID, descriptor ID, and file ID;
2. valid descriptor tombstone as logical absence/removal;
3. live content-bearing rows requiring exactly one BlobRef and authenticated
   key, snapshot, descriptor, file ID, BlobId, declared size, and payload;
4. metadata-only projection performing the same authentication while omitting
   only byte materialization;
5. zero, multiple, missing, malformed, wrong-kind, substituted, wrong-size,
   wrong-BlobId, and missing-payload failures; and
6. a valid empty-file payload and its transition to a valid tombstone removal.

The verifier uses balanced, function-scoped Rust extraction for production
source checks and explicit model markers for the field-complete oracle. The
negative fixtures are executable model cases, not token-only assertions.

## Frozen commands

From the repository root:

```text
bash test-reports/forktree-stage2-historical-correction-fd2-defects/verify_source_contract.sh audit
rustfmt --edition 2021 --check test-reports/forktree-stage2-historical-correction-fd2-defects/correction_model.rs
rustc --edition=2021 --test test-reports/forktree-stage2-historical-correction-fd2-defects/correction_model.rs -o <isolated-model-binary>
<isolated-model-binary> --nocapture --test-threads=1
```

The exact fd2 source audit remains expected RED. The model is the executable
test-only specification; no production or adapter runtime is claimed. Future
candidate qualification, if authorized, runs fresh Memory, RocksDB, then
SlateDB cells only after the source and model gates pass.
