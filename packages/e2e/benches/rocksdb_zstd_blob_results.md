# RocksDB Zstd blob compression

Measured from `991236c1532ffec774a186b20faa807601f18a72` with the
release `lix exp git-replay` binary. Every replay installed all bundled
plugins and created four or more evenly spaced checkpoints. Database size is
`du -sb` after Lix close and the explicit storage flush.

The candidate sets RocksDB's integrated blob compression to Zstd. SSTs already
used Zstd level 1; the baseline left blob files at RocksDB's default
`kNoCompression`. This is a physical-format cut and does not migrate existing
databases.

| Repository / window | Replay before | Replay after | Delta | Bytes before | Bytes after | Delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `microsoft/vscode-docs`, 100 commits from `3c1f5bf3` | 4,824.505 ms | 4,584.217 ms | -4.98% | 99,407,629 | 79,797,770 | -19.73% |
| `home-assistant/brands`, 80 commits from `d923dadb` | 313.377 ms | 306.244 ms | -2.28% | 16,002,393 | 15,854,220 | -0.93% |
| `wesnoth/wesnoth`, 15 commits from `e4a9a7d1` | 124.679 ms | 127.074 ms | +1.92% | 6,690,831 | 4,488,069 | -32.92% |

The small `brands` workload has no values large enough to benefit materially.
The two repositories with large values shrink by 19.7-32.9%. Timed replay
improves on the largest lane and stays within 2% on the smallest lane.

The repository windows are intentionally pinned. A longer `brands` window
hits a pre-existing filesystem namespace conflict, and a longer `wesnoth`
all-plugin window hits a pre-existing nested Wasmtime runtime panic. Those
semantic/plugin edge cases are outside this storage-format measurement.

Representative command (substitute repository, ref, window, and adapter):

```sh
lix exp git-replay \
  --repo-path /bench/vscode-docs \
  --output-path /bench/output/vscode-docs-rocksdb \
  --storage rocksdb \
  --plugins all \
  --branch main \
  --from-commit 3c1f5bf3dabbc38d99940e731a4d7cc69d30f50c \
  --num-commits 100 \
  --checkpoint-every 25 \
  --profile-json /bench/profiles/vscode-docs-rocksdb.json
```

Validation:

```sh
cargo test -p lix_storage_rocksdb
```

RocksDB persists `blob_compression_type=kZSTD` in its options file. The adapter
test asserts that setting in addition to exercising storage conformance,
snapshot reads, deletes, process locking, and large-value blob placement.
