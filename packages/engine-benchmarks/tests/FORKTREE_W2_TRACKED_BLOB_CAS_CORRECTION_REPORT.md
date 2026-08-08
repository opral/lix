# W2 tracked-state + Blob/CAS oracle correction

This is a TEST/REPORT-ONLY correction directly descended from the blocked
oracle at `0a1955269c0d1fd5d23bac24f0a35f4e9a51d687`. It changes no production
source, Cargo implementation, adapter, PR, or durable format.

## Correction scope

The standalone model now uses domain-tagged `ObjectId` and a distinct semantic
`BlobId`. It authenticates canonical manifests and ordered chunk identities,
lengths and hashes; validates row identity and BlobRef links; rejects duplicate,
reordered, missing, malformed, wrong-kind and same-size substituted objects;
and distinguishes point/range/diff/materialization, NULL, tombstone and
ordering behavior. Range reads validate metadata first and count only selected
payload bytes; full-payload reads are counted separately.

`PersistedImage` models flush/drop/reopen. Reopen reauthenticates roots, row
identity, manifests and every chunk, rejecting malformed, missing, wrong-kind,
substituted and digest-corrupt state. `Counters` separately records durable
writes/commits, point reads, scans, metadata reads, selected payload bytes and
full payload reads, so read-side effect assertions are not tautological.

The structural verifier now requires explicit base/target ancestry, enforces the
W2 production path allowlist, scans the complete `packages/lix/src` closure,
rejects legacy tracked-state/binary-CAS owners, raw storage, new/fresh views,
fallback/cache/compatibility paths, and requires typed view/object/blob symbols.
Four structural fixtures cover positive retained-reader propagation, fresh
second views, raw storage, and mismatched reader arguments. Two Rust compile
fixtures prove `ObjectId` and `BlobId`/manifest/chunk domain types are not
interchangeable, with intended `mismatched types` diagnostics.

## Exact identity

- correction parent: `0a1955269c0d1fd5d23bac24f0a35f4e9a51d687`
- correction parent tree: `2637eb75d99dc5fb11e8112f3d86a38585201504`
- original base: `e92ea2e505ee3d96abbb529dbaedb23d4908ff42`
- original blocked diff: `bdfa8f449526cf18ce25ea9983ceb045393cef1995137ee9a51bbfe8a74400fe`
- original blocked patch: `bf7b325d8addf8d53a2b70ea07d51d45c050fea7`

The final successor commit/tree/full-index/patch are supplied in the immutable
handoff. The correction package artifacts are hashed below.

## Local package gates

- `rustfmt --edition 2021 --check`: PASS.
- `rustc --edition=2021 --test -D warnings`: PASS.
- Corrected standalone model: 9/9 PASS.
- Corrected model executable SHA-256:
  `63396e8f979f3cf602f4f1f135094c3b09d8ee2997465dd57e9784dac0332611`.
- Model output log SHA-256:
  `181bd6630883b5fd78da14b2f50873ca0d14f496bce814e2cab3be0e2e125fde`.
- Compile fixture gate: positive 1/1 and typed negatives 2/2 PASS, each negative
  requiring the intended `mismatched types` diagnostic.
- Compile fixture output SHA-256:
  `aa657406e58eb77d042834d77a5aaec56a68ae749472c9c02794dee8a0990400`.
- Structural fixture gate: positive 1/1 and four discriminating negatives 4/4
  PASS.
- Structural self-test output SHA-256:
  `af1c9686179a1684e69d99a24d5bb321ab96a28b7c9d4b7d6493d7de448dc310`.
- Exact e92-to-parent source calibration: 113 RED findings, log hash recorded
  as `dfed7ba4caa629dacf43485adbf7273d248f61088be5e6c4aa560265d9359cef`;
  this is expected because the production old owners remain. No adapter or
  integrated Cargo runtime claim is made.

## Acceptance boundary

The first production candidate must pass the exact ancestry/path/source gate,
zero legacy residue, all model cases, warnings-denied compile gates, and then
wire equivalent cases through Memory, RocksDB and SlateDB. Durable adapters
must add flush/drop/reopen and corruption controls. This package is not a
production approval and contains no current-main performance claim.
