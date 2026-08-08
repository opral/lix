# Frozen fd2 defect-oracle correction successor

Status: **test/report-only successor ready for independent review**. No
production source, Cargo manifest, adapter runtime, PR, main branch, or prior
ref was changed.

## Immutable provenance

This new package is a direct successor to:

```text
parent:      2edc5cda354c456b1ece54f3f3a81485276e728d
parent tree: bf3e42c39c458dcd4e2c72e94e5ce59d94b71ea6
actual parent of 2ed: b493056059136ac1a394c912c80416d3d4b7fde4
```

The independent fd2 source anchor is preserved separately:

```text
fd2 anchor:             fd2be256d763f17e9f127d4c984e36fba191cb82
fd2 anchor tree:        20110ca5e3c33d34217630fff0a2b784b545317a
fd2..2ed full diff:     849d73bdfd09687f2708c702d73df6550a63dd7c0175849fbe8ae676bbf2450e
fd2..2ed stable patch:  932662594f98b01e1dcec6cd2f99ad69800b2345
b493..2ed full diff:    5e5c82ec4f6bf7f11acaa454ff5f2f429492190b778ba8ade4d3db5614a0efc2
b493..2ed stable patch: 790bf44f85fca2407b782491185879bde7ee1ad5
```

The prior six-file payload commit was `b493056059136ac1a394c912c80416d3d4b7fde4`,
whose `fd2..b493` full diff is
`d2125ba574d94088460a83dcae6ffb00ce3938b19ed74f484d1fad6109a9f495` and
stable patch ID is `1a1b6e09ca9b0ddea155a23c174134ac981c0870`. The new final
head/tree and direct-parent diff identities are frozen in the handoff outside
this self-describing report.

## Exact fd2 baseline

`bash verify_source_contract.sh audit` remains expected exit 1 on exact fd2:

```text
STATUS=BLOCKED_EXPECTED_RED
DEFECT=descriptor_tombstone_rejected
DEFECT=blob_validation_projection_gated
RELATED=blob_reference_cardinality_unchecked
PRESERVED=one-retained-ForkTree-history-view-and-fail-closed-chronology
```

The reproduced baseline log SHA-256 is
`cae2be6540521df3fe15a854af2ba5a14945826a3dbb15a2dca0dd83ca1136fd`.
The corrected mode remains dormant for production candidates, but it now
requires the field-complete model and reports the fd2 defects until a
production correction exists.

## Executable correction model

The standalone model compiles with `-D warnings` and runs six tests, all
passing. It binds row key, snapshot ID, descriptor ID, file ID, BlobId,
declared size, and payload bytes; requires exact-one BlobRef cardinality; and
authenticates these fields independently of metadata-only projection.

The six tests execute descriptor missing/malformed/wrong-kind/substitution
failures, valid tombstone removal, zero/multiple BlobRef failures, every
identity field substitution, BlobId and size mismatches, missing/malformed/
wrong-kind Blob authority, missing payload, metadata-only authentication, an
authenticated empty payload, empty-to-tombstone removal, and chronology
marker/missing-parent/cycle controls. No production or adapter runtime is
claimed.

Commands:

```text
rustfmt --edition 2021 --check test-reports/forktree-stage2-historical-correction-fd2-defects/correction_model.rs
rustc --edition=2021 --test test-reports/forktree-stage2-historical-correction-fd2-defects/correction_model.rs -o <isolated-model-binary>
<isolated-model-binary> --nocapture --test-threads=1
```

## Structural source gate

The verifier uses balanced, function-scoped Rust extraction for the retained
ForkTree history source and fd2 RED defects. Its corrected mode additionally
requires explicit model markers for every integrity field, exact-one
validation, projection-independent authentication, executable negative
fixtures, and the valid empty transition. This avoids token-only false
positives. The package changes only files below
`test-reports/forktree-stage2-historical-correction-fd2-defects/`.

Future candidate qualification, if separately authorized, runs source/model
gates first and then fresh single-threaded Memory, RocksDB, and SlateDB cells
in that order. This package performs no adapter runtime or benchmark.
