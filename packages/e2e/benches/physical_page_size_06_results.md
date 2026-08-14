# EXP-PAGE-SIZE-06: authenticated C2 page-size sweep

## Scope and provenance

- Parent: `aecf821658644f95724f22e3d29deda04573fdf1`, tree
  `5e504b7ecf2e0d080dd0c79f407ea72387c8279b`.
- The parent is the approved C2 schema-partitioned slotted-page experiment on
  the exact Schema-v1 checkpoint. This experiment changes only its benchmark
  model. It is not a durable production-row cutover.
- Canonical tuple bytes, key-prefix compression, content-defined split rule,
  root directory, object envelope, schema fingerprint, and recursive
  authentication are unchanged.
- Fixed decoded targets are 2/4/8/16/32/64 KiB. The existing deterministic
  adaptive policy is also measured: 4 KiB through 1 KiB canonical rows and
  16 KiB for 4 KiB rows.
- The balanced factorial covers every target at N=1K/10K/50K/100K and row
  widths 64/256/1024/4096 using integer keys. UUID, text, and composite key
  shapes hold N=10K and width=256 fixed, isolating key geometry without
  redundantly multiplying the 100K x 4 KiB corpus.

## Decision: qualified no-win

Keep the approved deterministic 4/16 KiB policy. No fixed target provides an
important-path improvement greater than 5% without a critical regression
greater than 5%. This advances the global no-win streak from 6/20 to 7/20.

Ratios below are arithmetic means over 53 matched cells versus adaptive; lower
is better. Update CPU is model construction time and is not claimed as a
production path-copy latency.

| Target | Point bytes | D update bytes | Short-range bytes | Scan bytes | Point CPU | Update build CPU |
|---:|---:|---:|---:|---:|---:|---:|
| 2 KiB | 0.642 | 0.676 | 1.104 | 1.218 | 0.729 | 1.509 |
| 4 KiB | 0.867 | 0.916 | 1.010 | 1.029 | 0.935 | 1.107 |
| 8 KiB | 1.319 | 1.282 | 1.010 | 0.912 | 1.115 | 0.883 |
| 16 KiB | 2.035 | 1.949 | 1.160 | 0.820 | 1.735 | 0.663 |
| 32 KiB | 3.126 | 2.965 | 1.237 | 0.757 | 2.383 | 0.499 |
| 64 KiB | 4.761 | 4.149 | 1.795 | 0.714 | 3.416 | 0.421 |

The physical trade-off is decisive:

- 2 KiB reduces point and sparse-update bytes by 36% and 32%, but increases
  scan bytes by 22%, model update CPU by 51%, and representative settled bytes
  by about 30%.
- 8 KiB reduces scan bytes by 9%, model update CPU by 12%, and settled bytes by
  about 15%, but increases point and sparse-update bytes by 32% and 28%.
- Larger targets continue reducing scan/storage/build costs while causing
  severe point and sparse-write amplification. They violate the OLTP-first
  no-regression rule.

Thus the crossover remains tuple-width-derived, not workload-tuned. Runtime N,
backend, PK shape, or observed workload never changes the selected encoding.

## Repeated timing and storage evidence

Five release samples were retained. Representative N=100K D=1 adaptive p50 /
p95 microseconds are:

| Canonical row width | Target | Point auth | Full model rebuild |
|---:|---:|---:|---:|
| 64 B | 4 KiB | 13 / 13 | 39,181 / 39,304 |
| 256 B | 4 KiB | 11 / 12 | 96,570 / 96,864 |
| 1 KiB | 4 KiB | 19 / 20 | 365,433 / 365,724 |
| 4 KiB | 16 KiB | 33 / 34 | 682,377 / 683,111 |

The 100K x 4 KiB process peaked near 715 MiB RSS. The harness exposes
authenticated logical object calls/bytes and decoded bytes, but not physical
adapter-internal calls or allocator counts; those fields are deliberately left
unqualified. C2 is opaque at the page layer, so projected OLAP reads decode the
same authenticated leaf as full-row reads; it receives no invented projection
credit.

All 28 representative RocksDB/SlateDB target/N cells flush, drop, cold reopen,
batch-fetch the exact authenticated closure, and recursively verify it. The
adaptive settled-byte p50/p95 values are stable:

| Backend | N | Settled p50 / p95 bytes |
|---|---:|---:|
| RocksDB | 10K | 1,108,327 / 1,108,327 |
| RocksDB | 100K | 10,546,005 / 10,546,005 |
| SlateDB | 10K | 1,100,418 / 1,100,418 |
| SlateDB | 100K | 10,962,983 / 10,962,983 |

## Correctness and complexity

- Exact point proof: `O(log_F N)` objects; adjacent missing keys use the same
  bounded authenticated leaf/path shape.
- Short range returning R rows: `O(log_F N + R/page_rows)` authenticated
  objects.
- Full scan: explicit `O(number_of_pages)` operation.
- D sparse replacements and VCS branch divergence: changed content-defined
  pages plus root paths; 168 D=1/10/1% branch-sharing rows cover targets,
  widths, and N=10K/100K. Unchanged branches share the exact root/page object
  set.
- Missing root, same-size payload substitution, truncation, envelope/domain,
  fingerprint, embedded bounds, directory, and branch-root substitution all
  fail closed. Canonical content-defined boundary and local insert/delete
  stability controls pass.

## Evidence

- Final correctness/model CSV SHA-256:
  `0efbeb49aa8c2eda0deb68ce8e95ca10052ba7390f3a7054c1fd270d245fd3c8`.
- Final stderr SHA-256:
  `2d5a63f5e6dd74d1d71d5ea46430cb483bc0c44fc95cf672f2d2452c40c4e1e6`.
- Physical backend CSV SHA-256:
  `5d8138887c2c722407e79a0243bda0503a76913b68726740bcb9dedbe7efe160`.
- Representative p50/p95 CSV SHA-256:
  `edcfeb1392b7649e8af89181ada07d45e7b4b162322e621ba362c016fdf0cc2c`.
- Aggregate hash of five model and five backend sample hashes:
  `08c0a2c7c0d81e59cbcb4ca2ead4d0c391360c950e799aab67a1a6a8c5d374fd`.
- Raw evidence directory: `/root/repos/evidence/exp-page-size-06`.

Because the result is no-win, no independent reviewer was spawned, as required
by the experiment contract.
