# Operation-scoped parsed-plugin batch plan

Verdict: **qualified NO-CUT**. No production source was retained. The proposed
registry/actor/read batching shape has a perfect-elimination ceiling below the
required 20% on the critical 1,000-row SlateDB cell, while the work it cannot
remove is required guest hydration and rendering.

## Pinned identity

- production parent: `00f65e8fdf2344ecf671c212b5c42d176859a586`
- production tree: `94fb7144c43c8d9990c935bf184f202b0a5deaa0`
- exact-main control retained from the prior lane:
  `d2c634b2aeb780aff46013ec04902fcbb5c6f846`, tree
  `d321745bf83a7e7358b038880ad40004fc888ee5`
- unchanged all-plugin runner SHA-256:
  `161436990c538ea278d32eabaf71f8f6739bafcfb93815a7dae88cde20110ef7`
- instrumented binary SHA-256:
  `5d42fbc78f3bcedd71fc59a5c40c79ef190d9d63b0aed6937523ba4ea6826042`
- reverted instrumentation patch SHA-256:
  `e2ac915481ea01333c51619490fe5c335154642065e34e96b0d91e6dca9536a7`

The instrumentation added nested timing spans only. It was removed after
measurement; the frozen ref contains no engine/runtime change.

## Real public workload

The gate uses the public all-plugin Markdown/CSV/JSON/text/Excalidraw branch
merge workload. It installs real component archives, creates public files,
performs semantic row edits, preview and merge, validates output and change
statistics, preserves unaffected files and source branch identities, then
flushes, drops, and cold-reopens RocksDB and SlateDB. The retained one-file
Markdown control is the previous 105-row public `lix_file` workload with one
semantic update (0.95%), point/range/full/history/diff and reopen. Multi-file
cells cover 17 and 100 affected files at 100 and 1,000 rows/file. The 100-row
primary Markdown/CSV files change one row (1%); the separate 1,000-row/50-file
cell changes about 1% of aggregate semantic rows once the one-row extra files
are included.

Every result/preview/merge/source-isolation/idempotency/plugin-resolution and
cold-reopen assertion passed. No fallback, synthetic provider, cross-operation
cache, or second authority was introduced.

## Measured 17-file crossover

Five fresh-database samples per cell:

| adapter | rows/file | merge p50/p95 ms | CPU p50 ms | allocations | reopen p50 ms | settled bytes |
|---|---:|---:|---:|---:|---:|---:|
| RocksDB | 100 | 27.105 / 29.092 | 29.536 | 41,746,147 | 11.388 | 8,447,006 |
| SlateDB | 100 | 27.037 / 29.406 | 29.482 | 47,383,321 | 5.878 | 8,639,296 |
| RocksDB | 1,000 | 104.924 / 109.137 | 114.485 | 80,071,585 | 12.327 | 10,056,773 |
| SlateDB | 1,000 | 105.970 / 130.992 | 114.653 | 87,252,306 | 6.338 | 9,990,337 |

The 1,000-row approximately-1%-aggregate-change cell at 50 affected files is
110.336 ms p50 RocksDB and 122.759 ms SlateDB. The 100-affected-file cell is
115.063 ms and 141.058 ms respectively. Scaling is driven by rows/file, not
actor count or backend I/O.

## Subphase attribution

For 17 affected files at 1,000 rows/file:

| phase (ms p50) | RocksDB | SlateDB |
|---|---:|---:|
| total merge | 104.924 | 105.970 |
| plugin selection (inclusive) | 94.286 | 90.694 |
| actor cold-open (inclusive) | 51.421 | 51.464 |
| actor instantiation | 1.002 | 0.918 |
| semantic-row load/host preparation | 13.628 | 13.197 |
| guest `open_rows` | 34.643 | 35.537 |
| semantic render | 34.823 | 36.079 |
| begin-read + materialization ref + blob lookup | 0.713 | 0.745 |

The proposed plan can safely target actor instantiation, exact reads, native
row preparation, and selection work exclusive of guest hydration/rendering.
The executable model gives it the impossible best case of eliminating all of
those costs. It predicts 22.29% on RocksDB but only **17.00% on SlateDB** at
1,000 rows/file. Real savings must be lower because plugin matching, owner/root
validation, and typed-row authority checks cannot all disappear.

Source confirms why the larger apparent 70-ms opportunity is not removable by
the requested plan:

- `plugin/runtime/contract.rs:1916-1927` requires one isolated branch/file
  actor with instance-local handles.
- `plugin/runtime/component.rs:210-239` already shares compiled code by plugin;
  only the file Store/instance is distinct.
- `transaction/context.rs:2637-2824` authenticates the exact materialization,
  typed rows, blob and row authorities before `open_rows`.
- `transaction/context.rs:6035-6148` already reuses acknowledged actors inside
  a transaction and performs the required final semantic render.

Sharing one actor across files would therefore either mix instance-local
document authority, abandon the per-file acknowledged actor publication, or
require a new multi-document guest ABI. Even a new ABI must still execute the
measured guest hydration and render work. Skipping it would change plugin/file
semantics and corruption behavior, so it is outside the accepted contract.

## Model and evidence

- model source SHA-256:
  `ef072929b12d30d2378527b2916ee67d846d042a286226c1ab9be6e6fb56a39c`
- warnings-denied model binary:
  `4f7ccb024cd9d0e3f889c6be1acf594e84cd20ae7cee7a13be065224497fcdaf`
- model output:
  `38b8b7317f51d8cb9efab9076f9a814ace300ff614640e966e32f73461c459ed`
- 100-file raw baseline / summary:
  `61fb3d54f4300f34017f9d5445b4c2ceb6b437f85e0a6dd29cfed8ebd266c497` /
  `26cb62b82774fcea6d3ddb20ebc0ea0f75a1ab9f077ba5279d808ccfe63c5edd`
- instrumented 100-file raw / summary:
  `c3b7140bd9f54f3075549b220e14d2e1c650a5e6c1b1f41f41e04d9ee496b132` /
  `b723cc87db6623fcaa7cd1229438d15eb5f665924c992bfddcc4ffac88594d20`
- instrumented 17-file raw / summary:
  `463375509d2eb974f3b1bc8b8dd9848fcf11a1cb61b24cad2fd0858744211324` /
  `708e9536e51896207345dce7231946fc8034fed722250088af6db896a3febec0`
- approximately-1%-change raw / summary:
  `c40331efa69da94d23562b3d67b9df9dde2a49f3f7bebfcebf18b763ef2e65dc` /
  `cc3ebc0bcd73b0a85e3c8bd5d71d69d0a305e94e61ae69f8128e4e29c775fa59`

All cells completed below the 1,200-second cap. The plan fails the required
cross-adapter 20% gate before production implementation, so no reviewer was
spawned and no production candidate was published.
