# Corrected unified base plus canonical key-order qualification

## Immutable provenance

- Corrected unified base: `6da4944c5e46b3d26578fd038b6d94874b5819b5`
  (tree `b69726dbad2301d5ca2d74d36001deb27f1d93f9`).
- Exact production correction: `91d059332bb00df0aaa4fad5babb6f7018175e25`
  (tree `49173b0580aa328f03e1417af0907a8dc7d1b2de`, direct parent
  `6da4944c5e46b3d26578fd038b6d94874b5819b5`).
- Production delta is exactly one path: `packages/lix/src/state/mod.rs`.
- Production full-index diff SHA-256:
  `e1a843979110435d0087e89dd4b30c67ef6d339fb679206ecd6c83d6d4d0d85e`.
- Frozen public harness commit:
  `4b582a95ce4b57d13b4e71d55258f569df1b6882`.
- Harness source blob: `e2d34e5f0d6e943c2d501372e066db309c60be60`.
- Harness source SHA-256:
  `d90c11168243ccf3ff149621edb49b0cd02a2f8c1897c1ceba46bc948a674f81`.
- Release binary SHA-256:
  `4aa71351174930253d242574b1d2d836bd9344277cf27372634532254e4004f9`.

The benchmark overlay is byte-identical to the previously reviewed harness.
No production source beyond the exact `91d05933` object is changed.

## Eight-cell runtime qualification

| Backend | OLTP | OLAP | File | VCS |
|---|---:|---:|---:|---:|
| RocksDB | PASS | PASS | PASS | PASS |
| SlateDB | PASS | PASS | PASS | PASS |

The file cell covers insert, read, update, delete, branch read,
delete-after-branch, and cold reopen. The VCS cell covers history, diff,
working diff, checkpoint, branch, merge, undo, redo, and cold reopen. All
operation and final digests match the corrected `6da` qualification.

Verify log SHA-256 values:

- Rocks OLTP `098eddcc7809f01accdf042bebe3ccc69b27ccdfc52549c8905f68980505d7bd`
- Rocks OLAP `9a86d7774507388bca12a3621bb6379478f42e82eba6eda792db7e87de02b39e`
- Rocks file `2a074505e57c130a90b0604d89d016d30f7de013d798a031ef90d53bac1c7b0f`
- Rocks VCS `f76ac61a900df7396cda0cbc503942915ec7bbcf53784ae872dc14a70f87684c`
- Slate OLTP `4f2781f02b4e72064daa97a731fabef96d2eb720ca05d214d9d0a4f60f47c0c9`
- Slate OLAP `b8c82b638e80475929b1f04cac45784d9ceaae66fa2471eee1f1e3247dd161b2`
- Slate file `c5d8b75aab41f7af4cb43d6c4ff6324a0f715c812d5dc138963108853ba052cf`
- Slate VCS `8796715a34f3daa3ef1453d4a8626bb6cf9ed2252f0577d7bff0e148c180677c`

## Focused VCS timing and counters

Five samples, `N=1000`, `H=10`. Times are p50/p95 microseconds. The comparison
is against the immediately preceding corrected base using the same harness.

| Backend | Operation | `91d` | Corrected `6da` | Result |
|---|---|---:|---:|---:|
| Rocks | diff | 1060 / 1089 | 1059 / 1222 | neutral |
| Rocks | merge | 1197 / 1214 | 1196 / 1204 | neutral |
| Rocks | working diff | 61182 / 61883 | 61537 / 61782 | -0.6% p50 |
| Rocks | cold reopen | 1970 / 2285 | 1950 / 2017 | +1.0% p50 |
| Slate | diff | 1351 / 1464 | 1330 / 1365 | +1.6% p50 |
| Slate | merge | 1884 / 1960 | 1881 / 1912 | neutral |
| Slate | working diff | 78279 / 78573 | 78033 / 79023 | +0.3% p50 |
| Slate | cold reopen | 2632 / 3584 | 2604 / 3472 | +1.1% p50 |

Median physical counters are unchanged:

| Operation | Calls | Keys | Read bytes | Puts | Logical write bytes |
|---|---:|---:|---:|---:|---:|
| diff | 91 | 107 | about 178.6 KiB | 0 | 0 |
| merge | 180 | 299 | about 393.4 KiB | 8 | 6,625 |
| working diff | 7,130 | 7,195 | about 24.17 MiB | 0 | 0 |
| cold reopen | 52 | 84 | about 227 KiB | 0 | 0 |

Rocks timed log SHA-256:
`f5a11b4829d0b3e48cd44e224d68acbf540f80d7722c603bbe01d450f923d990`.
Slate timed log SHA-256:
`f2be9381674ad7494bc446289e1489084e769d1cfe205ab237df1c5542130563`.

## Commands

```sh
CARGO_TARGET_DIR=/root/repos/lix/target timeout 1200 \
  cargo build --release -p lix-schema-v1-public-qualification

timeout 1200 lix-schema-v1-public-qualification.release \
  verify <rocksdb|slatedb> <oltp|olap|file|vcs> 10 1 1

timeout 1200 lix-schema-v1-public-qualification.release \
  run <rocksdb|slatedb> vcs 1000 10 5
```

Raw evidence is retained under
`/root/repos/lix-evidence/schema-v1-public-qualification/key-order-91d/logs`.

## Verdict

**RUNTIME QUALIFICATION APPROVE for the canonical key-order production
correction.** The exact one-path delta preserves all public semantics, digests,
storage calls, bytes, and cold-reopen behavior. It is performance-neutral and
does not address the independent working-diff amplification. This immutable
child is suitable as the corrected integration base for the native carrier
composition; the carrier qualification must continue to treat the 7,130-call
working-diff path as the dominant performance blocker.
