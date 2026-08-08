# W4 production implementation preflight manifest

This is a TEST/REPORT-ONLY package. It contains no production implementation,
build output, benchmark output, adapter result, PR mutation, or merge.

## Source binding

- exact preflight source: `e1af471b9ab0f598dafa7c2ddec7867667c81740`
- source tree: `bfa0d271a723da8250ab76ada16fda90926f1099`
- source parent: `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`
- parent..source full-index binary SHA-256:
  `9795ee3da81a06657a45a47a50417522a6a6bd7057e21eeb75597096417c9f3c`
- source stable patch ID: `31cc575644bf17e65c59d558a03acffc848c2e20`

## Frozen W4 v2 contract binding

- contract ref: `origin/codex/forktree-w4-fileblob-upload-readiness-e1af`
- contract head: `ff79e87fdc9cf8db7d1b47158cf9c8715b7471a9`
- contract tree: `674ed66dd0bcc2ab0cd9bb7dee7d6e5fc8645d3a`
- contract parent: `bd313e7e6880e4bd02fff51d7ed7d37d3dd9dcfb`
- contract parent..head full-index diff SHA-256:
  `3141365b69c99e9aa21f3de11621d5638e993bd669a8a632ae61aadaba90e08b`
- contract stable patch ID: `0d34c7b5dd0cf8d521177c7916bcf526db12ce68`
- contract report SHA-256:
  `f2bd370af93df7e9267592cf4dde8692e20a4aae81420e08307a68d8564f37a5`
- contract source-RED log SHA-256:
  `834223a468cf787dad96030f924778dd0f07627ae15ebae3408c8d518091e26d`

## Package files

| path | SHA-256 |
|---|---|
| `W4_IMPLEMENTATION_PREFLIGHT_E1AF.md` | `d450a4013293eaebd490b97ec7479e1670f44b0cb65287c94ff7fafcfdd0520d` |
| `MANIFEST.md` | recorded in `SHA256SUMS` |

## Scope

The report maps the exact e1af production call chains for ordinary file
content, multipart parts/progress/receipt/completion, authenticated reads,
legacy deletion, and W5 root handoff. It defines the compiler-driven order,
forbidden widening, and the post-freeze Memory -> RocksDB -> SlateDB command
contract with a 1200-second per-cell timeout and stop-on-first-blocker.

Runtime and compilation are explicitly UNRUN.
