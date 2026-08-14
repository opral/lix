# Native current-state carrier composition qualification

## Immutable composition

- Base: `91d059332bb00df0aaa4fad5babb6f7018175e25`
  (tree `49173b0580aa328f03e1417af0907a8dc7d1b2de`).
- Advertised carrier: `0edb80ab9d091b697b455f694685414b42654ac9`
  (tree `350633e52bb40ec6a37a8b76d685ce5afd6b3329`, parent
  `5089b964d5e9b0143656c5278e525db9100e2b61`).
- Combined production commit: `b358c1ec15748146910e06ae1ca8e443c8bb0ec1`
  (tree `e1a9865781e59db9619428b416c4e0573db4f408`, direct parent exact
  `91d059332bb00df0aaa4fad5babb6f7018175e25`).
- Combined production full-index diff SHA-256:
  `041eb73d5a83ab737b0c097ec6581b6df51a1723ae61df3cb4b64076745e0305`.
- Advertised and composed stable patch ID:
  `f3eca149b0d35d3e7a1e030ad8eabe5da2250d42`.
- Frozen harness commit:
  `4b582a95ce4b57d13b4e71d55258f569df1b6882`.
- Release binary SHA-256:
  `04630cb4d8df45c170f18e41a5052804f2644f00f16ea158b91c436ee9f41b4c`.

The carrier applied without conflicts. Its stable patch ID is identical on the
advertised `5089` parent and on exact `91d`. The `6da` control fixes and `91d`
canonical key-order logic remain present. No production edits were added.

## Source and compile gates

- `cargo fmt --all -- --check`: PASS.
- `git diff --check`: PASS.
- `cargo check -p lix --lib --all-features --message-format short`: PASS.
- Public Schema-v1 seven-type smoke: 1/1 PASS.

Log SHA-256:

- lib all-features:
  `b11106cb68371034452e2a6ec72d8b1ac805d7167c8a47965572536562761a86`
- seven-type smoke:
  `0ce83e170351c97f023ae553f451dd8092eb84e3db3c38c48354fc18ef18f0d9`

## Public runtime matrix

| Backend | OLTP | OLAP | File | VCS |
|---|---:|---:|---:|---:|
| RocksDB | PASS | PASS | BLOCK | BLOCK |
| SlateDB | PASS | PASS | BLOCK | BLOCK |

The failures are deterministic and backend-independent.

### File blocker

The first file insert fails:

```text
LIX_STORAGE_ERROR: native current-state row has no trusted Schema v1 plan for
'qualification_row'
```

The fail-closed owner is `packages/lix/src/native_row.rs`, where carrier
encoding requires a trusted plan for every staged current-state row. The file
transaction includes the public qualification schema row in the same native
publication, but that plan is not carried into this route.

### VCS blocker

The first VCS SQL surface fails:

```text
LIX_STORAGE_ERROR: Schema-v1 entity 'lix_commit' uses the removed JSON
current-state representation
```

The fail-closed owner is the entity projection in
`packages/lix/src/sql2/providers/entity.rs`. The carrier hard-cuts JSON
current-state rows but does not supply a native typed-row projection for the
public `lix_commit` surface exercised by the VCS harness.

No timing was run after these correctness failures.

Verify log SHA-256 values:

- Rocks OLTP `3a6e9540b177d2ea762e46f92cc43997e343c56043e0a679617fc06b9edd615e`
- Rocks OLAP `44f88ad3c4829d4875e4363f773b994b6e81c3fdc9bca579c117a1df875688e6`
- Rocks file `8f2f77df3443ed09f4ef8a71116f237033ab03d97fe812f78e2ea10a543076d9`
- Rocks VCS `65bcf3d69106875e2b5d3347311d0b95b3a96adca56e772f3d23093775a21e1d`
- Slate OLTP `9f160f6e3d4cb563ae7931994ffcb7be964e38927635188da6ae2b60b27fc2d6`
- Slate OLAP `b24ed32cfa5325a2e5f51f75f9c257de776f8fe375905bb1b7ceb027509b0f39`
- Slate file `e4d254dc061d1a00e3c9c4eb9d89e73890ddcd30d0d10754dd9942a6a6c8f186`
- Slate VCS `6fad0bc5e5199b00c3d43d2b22bb600ccd202cf7668748609b9bcdcfd9422415`

## Verdict

**BLOCKED as an integration base.** The exact carrier composition is
compiler-green and its direct seven-type entity path works, but it does not
cover the public file transaction or VCS entity projection. The correction
contract is not a compatibility fallback: carry the authenticated Schema-v1
plan into every native file publication row and provide the native typed-row
topology projection for `lix_commit`. Until both public cells pass on both
adapters, this composition must not replace `91d` as the integration base.
