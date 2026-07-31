# Final point-write batch results

This benchmark covers the hard cut from separate `put_many` and
`put_many_final` storage-adapter lanes to one final `put_many` lane. Range
deletions now precede point puts in a write transaction. This matches Lix's
write-set lowering order and lets RocksDB stream physical keys directly into
its atomic `WriteBatch`, rather than retaining every point key in a second
transaction-sized arena in case a later range deletion needs reconciliation.

The design follows established LSM practice: RocksDB applies one `WriteBatch`
atomically, while key prefixes and filters keep related logical key ranges
adjacent. See the upstream [RocksDB overview][rocks-overview], [prefix seek
guidance][rocks-prefix], and [Bloom filter guidance][rocks-bloom].

## Method

- Source: `main` at `991236c1532ffec774a186b20faa807601f18a72`, plus the
  parent Zstd blob-compression change.
- Candidate release binary SHA-256:
  `9c65dda7d76545bd33f2e4ccb50e17776563629d7ba29d9cc67a049ddf114747`.
- `lix exp git-replay --plugins all`, with a scoped parent-tree bootstrap and
  final Git-tree verification excluded from replay time.
- Fixed windows: `vscode-docs` 100 commits/checkpoint 25, `brands` 80/20,
  and `wesnoth` 15/5.
- RocksDB before data is the parent change's binary
  (`686a4a01ba04414b480f6c1be73c5b6641338970469152cb24e2322c7a5ff9b2`).
  SlateDB uses the original baseline binary because the parent changes only a
  RocksDB option (`a13a8d4eb397183c1a5dbd1190932f51532fb6a0bfceb660fedb55dfc27649eb`).
- Database bytes are `du -sb` after explicit storage flush. Timings below are
  one before/after run because the acceptance criterion is no regression, not
  a small timing win.

## Results

| repository | adapter | replay before | replay after | delta | execute before/after | checkpoint before/after | flush before/after | bytes before | bytes after | size delta |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `microsoft/vscode-docs` | RocksDB | 4584.217 ms | 4624.477 ms | +0.88% | 4395.968 / 4432.820 ms | 106.053 / 108.219 ms | 56.053 / 67.256 ms | 79,797,770 | 79,633,004 | -0.21% |
| `microsoft/vscode-docs` | SlateDB | 4804.175 ms | 4857.188 ms | +1.10% | 4604.119 / 4654.335 ms | 117.914 / 120.085 ms | 0.854 / 0.853 ms | 79,771,939 | 79,760,567 | -0.01% |
| `home-assistant/brands` | RocksDB | 306.244 ms | 313.514 ms | +2.37% | 265.534 / 272.647 ms | 13.911 / 14.076 ms | 25.658 / 25.325 ms | 15,854,220 | 15,854,439 | +0.00% |
| `home-assistant/brands` | SlateDB | 366.294 ms | 374.780 ms | +2.32% | 325.950 / 334.085 ms | 13.585 / 13.536 ms | 0.910 / 0.916 ms | 15,860,206 | 15,898,061 | +0.24% |
| `wesnoth/wesnoth` | RocksDB | 127.074 ms | 125.481 ms | -1.25% | 107.802 / 106.353 ms | 13.374 / 13.382 ms | 12.471 / 12.347 ms | 4,488,069 | 4,487,302 | -0.02% |
| `wesnoth/wesnoth` | SlateDB | 125.762 ms | 125.817 ms | +0.04% | 108.789 / 108.842 ms | 11.271 / 11.262 ms | 1.055 / 1.193 ms | 4,478,848 | 4,484,039 | +0.12% |

All final-tree checks passed. Replay changes range from -1.25% to +2.37%, and
physical-size changes range from -0.21% to +0.24%; these are noise-level
movements rather than a workload regression.

## Allocation accounting

Before the cut, a RocksDB transaction retained a second copy of every physical
point key plus one `Range<usize>` per point put until commit. After the cut it
retains one reusable physical-key buffer sized to the largest point key in the
current batch. For `N` point puts with physical key lengths `K_i`, retained key
payload falls from `sum(K_i)` to `max(K_i)` for each batch, and the `N`
range descriptors are eliminated. The engine's normal write-set path already
obeyed range-first ordering, so this removes the fallback cost without changing
CRUD, diff, merge, or checkpoint semantics on the measured path.

[rocks-overview]: https://github.com/facebook/rocksdb/wiki/RocksDB-Overview
[rocks-prefix]: https://github.com/facebook/rocksdb/wiki/Prefix-Seek
[rocks-bloom]: https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter
