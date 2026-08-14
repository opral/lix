# Parsed Markdown qualification — exact d2c baseline

Status: exact-main baseline qualified; future candidate replay remains pending.

## Provenance

- Product base: `d2c634b2aeb780aff46013ec04902fcbb5c6f846`
- Original benchmark handoff: `c8afda0e6597f69e71aea590a1969f4f2e3bb96a`
- Adapter order: RocksDB, then SlateDB
- Fixture: 3,226-byte syntax-rich Markdown plus one atomic 17-file transaction
- Samples: 3 warmups and 10 measured executions per adapter
- Each adapter execution has a 1,200-second timeout

Build:

```sh
CARGO_TARGET_DIR=/root/repos/lix-exp-hotcold-04/target \
  CARGO_BUILD_JOBS=2 \
  cargo bench --manifest-path packages/e2e/Cargo.toml \
  --bench parsed_markdown_qualification \
  --features storage-benches,slatedb,sdk-tests --no-run
```

Run:

```sh
packages/e2e/benches/run_parsed_markdown_qualification.sh \
  /root/repos/lix-exp-hotcold-04/target/release/deps/parsed_markdown_qualification-f6aed0c1ae5207ed \
  /root/repos/evidence/parsed-markdown-d2c-h2/exact-main-baseline 3 10
```

## Correctness

All 10 measured executions per adapter passed. RocksDB and SlateDB produced the
same fixture, rendered-file, exact-row, bounded-row, full-row, 17-file batch,
and cold-reopen results. The typed-row digest excludes nested generated
Markdown `id` and `column_id` fields, matching the plugin's semantic content
signature; typed rows are still fully decoded and the rendered file digest is
checked independently.

- fixture: `eb7b2c198ca59ec951526c9f3c4cf03e71fad5155a3f05fea863f8d990d3a2ab`
- rendered: `de25014eb75f07c6ca4d664b6190f1b63bf46fd218ef64626fc270e21c960f3e`
- exact row: `6929719945db7d5b5c6e2fb6a93254159fa2fafe575ab44afebd799f5111c103`
- bounded rows: `416300b036b5f60e05790ee68e207deca1870f10f68774d8566a0a7a557a37dc`
- full rows: `bf8d028bd2709e972509767b17c95b4fec63c3b1d644a64678e436a902b2500f`
- 17-file batch: `e2c12403a8560f2cbbe89bc4911eb5fd1f797f685ec43fb23509c676bef08f35`
- reopened Markdown rows: 105

The durable reopen boundary is close, drop, adapter flush, then open. This is
the same explicit boundary used by retained adapter cold-reopen tests.

## Exact-main baseline

Times are milliseconds. Allocation and settled-byte values are medians.

| Operation | Rocks p50 | Rocks p95 | Slate p50 | Slate p95 |
|---|---:|---:|---:|---:|
| parse to native rows | 43.949 | 47.812 | 37.268 | 39.226 |
| exact typed row | 0.740 | 1.033 | 0.650 | 0.924 |
| bounded typed rows | 1.484 | 1.751 | 1.498 | 1.550 |
| full typed rows | 1.180 | 1.543 | 1.154 | 1.315 |
| semantic row update | 4.521 | 5.998 | 4.967 | 5.380 |
| history depth one | 1.360 | 1.767 | 1.455 | 1.856 |
| historical diff | 1.090 | 1.438 | 1.315 | 1.695 |
| 17-file transaction | 20.732 | 25.593 | 26.862 | 30.802 |
| branch create/switch/read | 4.984 | 6.949 | 6.952 | 8.256 |
| cold reopen/read | 5.837 | 7.109 | 18.366 | 21.861 |

- Parse allocations: RocksDB 54,715,060 bytes; SlateDB 55,091,526 bytes.
- Cold-reopen allocations: RocksDB 5,934,929 bytes; SlateDB 27,198,565 bytes.
- Maximum observed RSS high-water mark: RocksDB 229,032 KiB; SlateDB 219,788 KiB.
- Settled database bytes: RocksDB 12,236,663; SlateDB 12,194,931.5 (median).

`/proc/self/io` reports process-level physical I/O deltas and can remain zero
for page-cache-backed reads; it is diagnostic rather than an adapter counter.

## Evidence identities

- Baseline summary SHA-256: `605af535de0782bf233889122fa84fe9b346ca57d6ec19670290470a1ae24070`
- Correctness smoke summary SHA-256: `b5529b2b3e21d5acf56d0b40d74c4a9f4eacbcbea25bc3f63512df2dab3faf7c`
- Build log SHA-256: `18f5a61591101a58ed6f2969d4bb65174bf195ac2af29d84f5d80c2f2b78687f`
- Benchmark binary SHA-256: `82549ab98a75e8750b1a932febd6f276ecd41a063d81588331e9c4377313eec6`

No production source is changed. Candidate timing is intentionally absent
until an immutable latest-main ForkTree integration head can run this exact
harness and fixture.
