# W4a file-content correction manifest

TEST/REPORT-ONLY direct successor of immutable 3e9a7f2c611a1bbad12fd271ca7a43332a4fe1c5.

No production source, adapter, PR, or merge changes are present. The package
freezes the exact e1af RED baseline and a candidate-parametric structural
GREEN verifier plus a warnings-denied pure model.

## Immutable baseline

- commit: e1af471b9ab0f598dafa7c2ddec7867667c81740
- tree: bfa0d271a723da8250ab76ada16fda90926f1099
- parent: b484e20d845aee3f8137bfa3496f9b3cd0e8cd35

## Package artifacts

SHA256SUMS covers every listed artifact below; entries are regenerated only
after the final package content is frozen.

| file | SHA-256 |
|---|---|
| W4A_FILE_CONTENT_READINESS_E1AF.md | e0d772acc409682c15ad19369696b84cbe990557d501bc66493b2e37e4637542 |
| verify_w4a_source.sh | c8831e9aa2b02882e37d83eb765afa0baf8570e738efa63dbf87b9367e561801 |
| w4a_file_content_model.rs | 01e31e297262e810706f249aeb070b9f8ee011999176fe764e6ce13c6fadffb7 |
| MODEL_RUN.log | 54fba877eb4351b8955fd1b14e485006859b7b6ff25ebc89d4adeb1cf50af59b |
| SOURCE_RED.log | 05041181ccd76fcc137c0c4c7161fae4231aec8410c7defb220c755d69e1ea50 |
| SOURCE_GREEN.log | 28a6b3b15b3a9dd82e92d7aebb4b18c8dae46bbf50ff20f58a884774f07ec73d |

SHA256SUMS itself is the checksum index and is intentionally not self-listed.

## Required acceptance properties

- private owner BlobId is derived from authenticated ordered manifest/chunks;
  it is compared to retained-read row identity before payload bytes;
- no caller-supplied BlobId argument, generic storage writer, durable cache or
  index, alternate authority, fallback, or second publication;
- one non-copy retained read/view is argument-bound through one publication,
  storage plan, prepare, and commit;
- stale/rollback, unchanged-chunk reuse, corruption/reopen, and W5 handoff
  remain covered;
- exact e1af RED, genuine candidate-parametric GREEN, and rustc `-D warnings`
  model evidence are included.

## Gate status

- baseline source verifier: RED, expected exit 1
- candidate verifier self-test: GREEN
- pure model: 9/9, warnings denied
- Memory/RocksDB/SlateDB production runtime: UNRUN by scope
- production source/build/PR/merge: untouched
