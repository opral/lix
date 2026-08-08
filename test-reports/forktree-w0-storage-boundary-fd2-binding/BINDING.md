# W0 v3 storage-boundary oracle bound to fd2

Status: TEST/REPORT-only binding. This package adds no production files, does
not rewrite either source object, does not build or run an adapter, and does
not claim adapter qualification.

## Immutable objects

The accepted W0 v3 source/report oracle is the separate immutable object:

```text
W0 commit: 6a91df3f88177e9b6d53d20d5ba6554df8fd6b9a
W0 parent: dc4323d56be98237c54099c67d46bfc0e3b2ef63
W0 tree:   0d194d75190caca4219779edd87469c57f9db8b8
W0 parent..commit full-index binary SHA-256:
  847d6f8e8554c21933d5a89238dbca9ae36bdadb64ce761d80c669e59399067e
W0 stable patch ID: f89895331cb4b7c18db0c79b9ff47a8261a076b2
```

The exact fd2 production source being diagnosed is:

```text
fd2 commit: fd2be256d763f17e9f127d4c984e36fba191cb82
fd2 parent: cd91b9b90f7f468158b4df154adbed9551eb5d60
fd2 tree:   20110ca5e3c33d34217630fff0a2b784b545317a
fd2 parent..commit full-index binary SHA-256:
  1a410542cff54e3b1c83a5cfb2cdea568dc9f1f71fc0c3f8598e8936d944a277
fd2 stable patch ID: c275ab15f3306c503e6830afee2a66bacf1fb974
```

This binding is carried by the existing report-only fd2 package:

```text
package parent: fd2be256d763f17e9f127d4c984e36fba191cb82
package anchor: e2503fd1d43b95d3ebfd133b9868a4be0647ee3d
package tree:   9223d01c5c38457edbe3048f12d90f2305f84a31
```

The W0 and fd2 objects are deliberately different lines. No production
patch is synthesized between them.

## Boundary contract

The W0 source gate requires path/function-scoped ownership of the sealed
engine boundary:

- `StorageSpace`, `ObjectId`, `ObjectDomain`, and `CoherentView` are
  engine-declared/private or crate-visible as specified by W0;
- only `OBJECT_SPACE`, `SELECTOR_SPACE`, and `UNTRACKED_ROW_SPACE` remain;
- raw constructors, legacy columnar/tracked/changelog/Binary-CAS owner
  registries, alternate writers, compatibility readers, and second durable
  authorities are forbidden;
- view/reopen authentication binds object bytes and domain; wrong-domain
  reopen is a typed `WrongDomain`, not a missing-row result;
- authenticated absence, explicit NULL, and tombstone remain distinct.

The exact W0 evidence is bound, not regenerated here: 607 tracked files,
598 scanned source files, and 955 structural residues with zero missing
boundary tokens. The W0 standalone model was recorded as 6/6 green, with
model binary SHA-256 `d2955ecca3d9f66b9eff72950bf688e9b462581de2205783a17ad0d5e86adfe8`
and model log SHA-256
`d63dc63486f4cef75e6bb0625ce70adb7bf3ab366e9dfca9f7ed51e9333e603f`.
The W0 static calibration is intentionally RED on the blocked source; its
recorded log SHA-256 is
`c517336db118100dc1ae6689d4a0f6595949d5d85b4d50ea819cfa818ea9823c`.

## Three storage-support diagnostics

The binding preserves the three independent diagnostics from W0 rather than
collapsing them into a token-only check:

1. **Rust public API diagnostics.** The five actual external probes must fail
   with the expected compiler code and removed symbol: raw space (`E0423`,
   `SpaceId`), columnar owner (`E0599`, `load_columnar_row_group`), tracked /
   changelog (`E0599`, `load_commit_state_manifest` and `load_tracked_state`),
   Binary-CAS owner (`E0599`, `load_binary_cas_manifest`), and legacy owner
   (`E0599`, `load_branch_head_control`). The positive descriptor probe must
   remain a real compile-pass control.
2. **TypeScript/native binding diagnostics.** The TypeScript probe imports
   `LocalFilesystem` and `LixBinding` from the actual JS SDK source and must
   fail with `TS2339` for `syncAllFiles`, `lixDir`,
   `importFilesystemPaths`, and `syncDiskToLix`. It may not declare those
   members itself. The native Rust registration scan must independently find
   none of those exports or `LocalFilesystemOpenOptions`.
3. **Authenticated reopen diagnostic.** The W0 model must distinguish
   `reopen(wrong_domain_id, complete_wrong_domain_bytes)` as `WrongDomain`,
   separately from `open_view` wrong-domain, missing-root, corrupted-bytes,
   wrong-view, and expired-cursor controls.

The fd2 package remains independently source-RED for its known
`owner.schema_keys()` fallback in
`packages/lix/src/sql2/providers/file_history.rs::file_history_owner_schema_keys`.
That expected fd2 diagnostic is recorded, not masked or reclassified as W0
success.

## Future qualification order

From a clean checkout, first run this binding verifier. Then run the exact W0
source/static commands against the W0 object and fd2 package in separate
worktrees. No adapter or production runtime is part of this package:

```sh
python3 test-reports/forktree-w0-storage-boundary-fd2-binding/verify_binding.py "$PWD"

git worktree add --detach /tmp/lix-w0-v3 6a91df3f88177e9b6d53d20d5ba6554df8fd6b9a
node scripts/forktree_w0_storage_boundary_residue_verify.mjs --root /tmp/lix-w0-v3
cargo fmt --all -- --check
git -C /tmp/lix-w0-v3 diff --check

git worktree add --detach /tmp/lix-fd2-exact e2503fd1d43b95d3ebfd133b9868a4be0647ee3d
bash /tmp/lix-fd2-exact/test-reports/forktree-stage2-fd2-correction-oracle/source_gate.sh /tmp/lix-fd2-exact
```

Only after a future production candidate is compile-green may the unchanged
W0 compile-probe runner be used. Every future command/seed cell is capped at
20 minutes. Memory, RocksDB, and SlateDB point/range/reopen/corruption
runtime qualification is explicitly UNRUN here.
