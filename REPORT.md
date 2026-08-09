# a33 unchanged-child-chunk authentication oracle report

## Verdict

**RED oracle PASS / a33 remains BLOCKED.** The deterministic oracle catches the
exact a33 failure: a valid StateKey and base manifest with one corrupted
unchanged child is accepted by the old a33-shaped path, while the required
complete-closure path rejects before any publication counters change.

This is test/report-only evidence. It is not a production approval, adapter
runtime result, PR, or merge.

## Bound immutable candidate

- ref: `origin/codex/forktree-authenticated-splice-0499-compose`
- head: `a33b7b9e12d84bbb95d64a29561a0b7572072ab2`
- tree: `e32e6be39b627c92fb9f2fd8e5ea273b7589157b`
- parent/base: `0499bcf9ab5d21a42da308509bb3b257ebc9d0ce`
- base tree: `9262c4e7b7d6158b3d5f1dfd00373093ff009765`
- parent..head full-index binary diff SHA-256:
  `2887c4ef0084b7ecf236a1fd867bd9191eb71c5f6035c67eea25fbc5278c7a24`
- stable patch-id: `5a43d62e9001b1a6a41db0a945f3e48d4774bcfb`
- changed production paths: exactly seven, as advertised in the bound review

## Frozen oracle artifacts

- source: `packages/rs-sdk-tests/examples/authenticated_splice_corruption_oracle.rs`
- source SHA-256: `8937df8414fcb51b0100b2f54e39472dd8cc331fd456b7932fb9e1c0fb05bac6`
- warnings-denied binary SHA-256: `befebf077c5b69dc27b937444f9ddbd46d11a58fd18821f993850255f2bc4368`
- raw output SHA-256: `046eb4f1b6983e16b9adc01fe505b11e506e718d2d9bf9fc3583ce2c6acfa910`
- contract SHA-256: `496179f48f650a19f958936a9d21fdc5320c92f4ef85fd370fac8e5e933458c1`
- command manifest SHA-256: `e340faa1b9ecab271ec792da82673c2e4ed34805ea3925a7e3293d1eed25ddb0`

## Executed gate

Command:

```sh
rustc --edition=2024 -D warnings \
  packages/rs-sdk-tests/examples/authenticated_splice_corruption_oracle.rs \
  -o oracle.bin
./oracle.bin
```

The model ran the valid lifecycle and all four corruption cases for named
RocksDB and SlateDB controls:

| control | valid | changed | reused | cold reopen | corruption cases | rollback/no writes |
|---|---:|---:|---:|---|---:|---|
| RocksDB model | pass | 1 | 63 | pass | 4/4 | pass |
| SlateDB model | pass | 1 | 63 | pass | 4/4 | pass |

Cases were missing, malformed, wrong-domain, and same-size-substituted bytes.
For every case the output records `a33_accepts=true`,
`oracle_rejects_before_write=true`, `selector_writes=0`,
`receipt_writes=0`, and `rollback=pass`. The valid path confirms all 63 reused
child IDs and digests plus the one changed child on the same read ID.

The named adapter rows are model controls, not actual RocksDB/SlateDB runtime
claims. The required future adapter gate is preserved in `RUN_COMMANDS.md`:
Memory first, then each adapter with flush/drop/cold reopen and zero partial
publication assertions.

## Required correction

The production successor must load and authenticate all 64 base child objects on
the existing retained `CoherentView`, recompute the base content digest and
BlobId against the StateKey owner, reject all four corruption classes before
staging, then make one `PreparedPublication`/plan. It must not introduce a raw
CAS authority, cache, fallback, second read, compatibility format, or second
writer.
