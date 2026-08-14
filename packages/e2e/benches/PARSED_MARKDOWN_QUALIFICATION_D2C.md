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
  /root/repos/evidence/parsed-markdown-d2c-h2/exact-main-baseline-v2 3 10
```

## Correctness

All 10 measured executions per adapter passed. RocksDB and SlateDB produced the
same fixture, rendered-file, exact-row, bounded-row, full-row, 17-file batch,
history, diff, and cold-reopen results. The typed-row digest excludes nested generated
Markdown `id` and `column_id` fields, matching the plugin's semantic content
signature; typed rows are still fully decoded and the rendered file digest is
checked independently.

- fixture: `eb7b2c198ca59ec951526c9f3c4cf03e71fad5155a3f05fea863f8d990d3a2ab`
- rendered: `de25014eb75f07c6ca4d664b6190f1b63bf46fd218ef64626fc270e21c960f3e`
- exact row: `6929719945db7d5b5c6e2fb6a93254159fa2fafe575ab44afebd799f5111c103`
- bounded rows: `416300b036b5f60e05790ee68e207deca1870f10f68774d8566a0a7a557a37dc`
- full rows: `bf8d028bd2709e972509767b17c95b4fec63c3b1d644a64678e436a902b2500f`
- diff (1 row): `c75640ed04b41b5de5dd7e0f856149c2235b6ea72d3d53ead787de195c979455`
- history (105 rows): `bf8d028bd2709e972509767b17c95b4fec63c3b1d644a64678e436a902b2500f`
- 17-file batch: `e2c12403a8560f2cbbe89bc4911eb5fd1f797f685ec43fb23509c676bef08f35`
- reopened Markdown rows: 105

The durable reopen boundary is close, drop, adapter flush, then open. This is
the same explicit boundary used by retained adapter cold-reopen tests.

## Exact-main baseline

Times are milliseconds. Allocation and settled-byte values are medians.

| Operation | Rocks p50 | Rocks p95 | Slate p50 | Slate p95 |
|---|---:|---:|---:|---:|
| parse to native rows | 47.699 | 53.726 | 40.237 | 43.342 |
| exact typed row | 0.910 | 1.137 | 0.865 | 0.962 |
| bounded typed rows | 1.617 | 2.031 | 1.662 | 2.021 |
| full typed rows | 1.243 | 1.722 | 1.265 | 1.800 |
| semantic row update | 4.743 | 5.765 | 5.290 | 6.484 |
| history depth one | 1.621 | 1.755 | 1.752 | 2.188 |
| historical diff | 1.259 | 1.565 | 1.424 | 1.765 |
| 17-file transaction | 25.499 | 28.808 | 27.883 | 35.913 |
| branch create/switch/read | 5.514 | 6.788 | 7.334 | 9.218 |
| cold reopen/read | 6.765 | 6.902 | 19.652 | 22.389 |

- Parse allocations: RocksDB 54,716,802 bytes; SlateDB 55,088,354 bytes.
- Cold-reopen allocations: RocksDB 5,934,956 bytes; SlateDB 27,254,652.5 bytes.
- Maximum observed RSS high-water mark: RocksDB 229,572 KiB; SlateDB 221,256 KiB.
- Settled database bytes: RocksDB 12,236,619; SlateDB 12,194,833 (median).

`/proc/self/io` reports process-level physical I/O deltas and can remain zero
for page-cache-backed reads; it is diagnostic rather than an adapter counter.

## Evidence identities

- Baseline summary SHA-256: `fd15f445597e84e19af79719c98fd98d89a5acdb25ec9bcc5ab6bd842eea189c`
- Correctness smoke summary SHA-256: `25b017bdc3f66aa9a3844d2d132b3eeee74d000ba1b3139f3ffe2a649c5fca5e`
- Build log SHA-256: `83341c102e4b216e0834428947ae3ed35269e9b889ebcdf3483e265f50e09d1a`
- Benchmark binary SHA-256: `b40f13c03f66f84bf12b9ca4523fc7cfc1204b11b188f3560bf7feb660d2108b`

No production source is changed. Candidate timing is intentionally absent
until an immutable latest-main ForkTree integration head can run this exact
harness and fixture.
