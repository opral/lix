# Current plugin checkpoint storage results

This hard cut moves rebuildable plugin checkpoints out of immutable repository
history. A checkpoint is now one current value keyed by the raw 16-byte branch
UUID and 16-byte file UUID. Each update overwrites that value atomically with
the file, semantic rows, and branch head. Generation and file-blob hashes in
the value fence stale state; malformed or mismatched cache data is ignored and
rebuilt. Raw writes and file deletion remove the exact owner key, while branch
deletion removes its UUID-prefixed key range.

The boundary is format-neutral. The engine stores opaque runtime and authority
bytes produced by any WASM component plugin. It contains no Git, text, binary,
LFS, media-type, extension, or plugin-name policy. Files without a plugin
checkpoint continue through the ordinary binary CAS, including initial Git LFS
materialization. The repository protocol is bumped to v39 and the two old
checkpoint-hash fields are removed from `lix_binary_blob_ref`; there is no
migration path.

This follows established database practice:

- Git stores derived commit-graph metadata separately from repository objects,
  verifies it against object identity, replaces graph layers, and expires
  unused files. See Git's [commit-graph format][git-commit-graph-format] and
  [commit-graph maintenance][git-commit-graph].
- PostgreSQL materialized views persist derived query results for fast reads,
  and `REFRESH MATERIALIZED VIEW` replaces the old contents rather than adding
  every refresh to table history. See [materialized views][postgres-matview]
  and [refresh semantics][postgres-refresh].
- RocksDB provides atomic overwrite/delete operations across column families,
  while compaction removes overwritten versions and tombstones. See the
  [RocksDB overview][rocks-overview] and [compaction][rocks-compaction].

## Git replay corpus

The baseline is `main` at `3a792c12d`; the candidate release binary SHA-256 is
`41826a32edb748412bf4ef48246f7ce598f333c5613137025d5bf0669b4c32ec`.
Every successful run used `lix exp git-replay --plugins all`, a scoped parent
bootstrap, periodic checkpoints, explicit adapter flush, and final Git-tree
verification. RocksDB bytes are the closed database directory. SlateDB bytes
are attributable SST plus immutable binary-segment bytes, excluding
close-timing-dependent WAL and manifest control files.

| repository | adapter | replay before | replay after | time delta | bytes before | bytes after | size delta |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `microsoft/vscode-docs`, 100 commits | RocksDB | 4668.278 ms | 4496.238 ms | -3.69% | 72,622,119 | 60,044,560 | -17.32% |
| `microsoft/vscode-docs`, 100 commits | SlateDB | 4809.890 ms | 4761.489 ms | -1.01% | 72,151,959 | 56,906,626 | -21.13% |
| `home-assistant/brands`, 80 commits | RocksDB | 313.681 ms | 309.614 ms | -1.30% | 15,853,294 | 15,840,929 | -0.08% |
| `home-assistant/brands`, 80 commits | SlateDB | 374.387 ms | 377.196 ms | +0.75% | 15,727,248 | 15,715,604 | -0.07% |
| `wesnoth/wesnoth`, 15 commits | RocksDB | 121.232 ms | 124.242 ms | +2.48% | 4,345,582 | 4,233,493 | -2.58% |
| `wesnoth/wesnoth`, 15 commits | SlateDB | 124.612 ms | 123.935 ms | -0.54% | 4,303,176 | 4,193,333 | -2.55% |
| **aggregate** | | **10412.081 ms** | **10192.712 ms** | **-2.11%** | **185,003,378** | **156,934,545** | **-15.17%** |

All six final trees verified. VS Code materialized the same 97 unique Git LFS
objects and 42,011,887 logical LFS bytes; Brands and Wesnoth materialized none.
The media-heavy Brands control remains flat, so text checkpoint optimization
does not trade away binary-asset layout.

Increasing-batch runs cover the full 100-commit VS Code window. Brands is
clean through 80 commits and its 100-commit attempt reaches an existing
filesystem namespace conflict near commit 91. Wesnoth is clean through 15;
its 50-commit attempt reaches 20 before the existing nested-Wasmtime-runtime
panic. These are replay-tool edge cases, not storage mismatches, and do not
invalidate the final-tree-verified windows above.

## Every binary byte attributed

The benchmark inventory recursively loads inline and out-of-band JSON, assigns
every binary-CAS manifest to its owning hash field, and emits an explicit
`unowned` category. On the final VS Code/SlateDB database it finds:

| owner | references | manifests | logical bytes | chunk values | encoded chunk bytes | layout |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| file blobs | 2,193 | 2,140 | 76,563,532 | 684 | 48,247,845 | 641 single, 15 chunked, 1,484 flat deltas |
| plugin WASM | 10 | 4 | 3,220,058 | 5 | 1,154,229 | 3 single, 1 chunked |
| unowned or cross-owner shared | 0 | 0 | 0 | 0 | 0 | none |

Before the hard cut, VS Code had 1,967 runtime-checkpoint manifests carrying
17,653,016 logical bytes and 886 authority-checkpoint manifests carrying
2,053,864 bytes. The candidate removes all 2,853 historical manifests. It
retains 360 current checkpoint rows with 3,141,505 runtime bytes and 133,544
authority bytes. All 360 runtime payloads are unique; 358 authority payloads
are unique, and the only exact duplicates total 40 bytes. Immutable binary-CAS
chunk values fall from 67,267,601 to 49,402,074 bytes. The current checkpoint
space occupies 3,238,123 compressed physical bytes. All 689 encoded chunk
values are reachable from exactly one reported owner category; none is orphaned
or shared across categories.

Wesnoth retains 40 current checkpoints with 87,381 runtime bytes and 474,696
authority bytes; every component is unique. Brands retains eight checkpoints
with 5,288 component bytes; every component is unique. Current divergent
branches therefore retain distinct serving state, while superseded edits no
longer retain checkpoint history.

## Matched CRUD, merge, and cold-open benchmark

The deterministic Markdown corpus is 524,721 bytes. Baseline and candidate use
50 byte edits, 50 semantic edits, 15 unrelated-entity merges, and 30 cold
opens. Values are medians from release builds.

| operation | before | after | delta |
| --- | ---: | ---: | ---: |
| initial import | 32.040 ms | 32.562 ms | +1.63% |
| hot byte edit | 1.897 ms | 1.805 ms | -4.85% |
| semantic edit | 6.005 ms | 5.912 ms | -1.55% |
| cold sparse edit total | 7.874 ms | 7.875 ms | +0.01% |
| unrelated-entity merge | 7.533 ms | 7.552 ms | +0.25% |
| cold materialized read | 0.504 ms | 0.515 ms | +2.18% |

The history-live main-branch database changes from 11,163,356 to 11,226,533
bytes (+0.57%). After 15 live divergent merge-source branches it changes from
20,556,982 to 23,601,901 bytes (+14.81%): that delta is one distinct,
rebuildable current accelerator per live branch, not retained edit history.
Removing it would trade cold branch-edit performance for cache eviction, so it
is deliberately retained and lifecycle-cleaned.

An application-level Zstd prototype for certified semantic pages was rejected.
It reduced logical page bytes by about 86%, but adapter block compression had
already captured the redundancy: physical size regressed 0.38-0.45% on
RocksDB and 0.20-0.21% on SlateDB, with VS Code/SlateDB replay 2.1% slower. No
part of that prototype is retained.

## Verification

- `cargo test -p lix_engine --features all-simulations`: 1,664 unit tests and
  800 base/rebuild integration simulations passed.
- RocksDB: 15 tests passed, including storage conformance.
- SlateDB: 58 tests passed, including storage conformance and cached storage.
- SQLite: 2 tests passed, including storage conformance.
- `cargo check -p lix_engine_benchmarks --all-features` passed.
- `git_text_plugin` cold reopen passed and restored the durable checkpoint
  without full semantic-state hydration.
- The matched benchmark passed its byte, semantic, merge, and cold-open
  correctness assertions.

[git-commit-graph-format]: https://git-scm.com/docs/gitformat-commit-graph
[git-commit-graph]: https://git-scm.com/docs/git-commit-graph
[postgres-matview]: https://www.postgresql.org/docs/current/rules-materializedviews.html
[postgres-refresh]: https://www.postgresql.org/docs/current/sql-refreshmaterializedview.html
[rocks-overview]: https://github.com/facebook/rocksdb/wiki/RocksDB-Overview
[rocks-compaction]: https://github.com/facebook/rocksdb/wiki/Compaction
