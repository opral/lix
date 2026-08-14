# Corrected unified-base public qualification

## Immutable inputs

- Corrected production base: `6da4944c5e46b3d26578fd038b6d94874b5819b5`
  (tree `b69726dbad2301d5ca2d74d36001deb27f1d93f9`, parent
  `5089b964d5e9b0143656c5278e525db9100e2b61`).
- Exact main comparator: `dc4f42917937150fa20fcb7517c46c21d1840045`.
- Benchmark overlay source commit: `4b582a95ce4b57d13b4e71d55258f569df1b6882`.
- Workload source blob: `e2d34e5f0d6e943c2d501372e066db309c60be60`.
- Workload source SHA-256:
  `d90c11168243ccf3ff149621edb49b0cd02a2f8c1897c1ceba46bc948a674f81`.
- Corrected release binary SHA-256:
  `f00c9b036fdab5a867baaf8f5125e329c865163c411bcc021966c387ad0f9402`.
- Exact-main release binary SHA-256:
  `a53458176d79aed629cac1a72d097728e164f5c8f10a114b7b388a772641e0ba`.

The overlay is byte-identical to the previously reviewed public harness. It
uses public `Lix` APIs, public SQL, and the public storage boundary. This child
contains no production changes beyond the four-path correction already frozen
at `6da4944c`.

## Verify-only result

All eight corrected-base cells pass:

| Backend | OLTP | OLAP | File | VCS |
|---|---:|---:|---:|---:|
| RocksDB | PASS | PASS | PASS | PASS |
| SlateDB | PASS | PASS | PASS | PASS |

The file suite includes insert, exact read, update, delete, branch read,
delete-after-branch, and cold reopen. The VCS suite includes branch,
checkpoint, history, diff, working diff, merge, undo, redo, and cold reopen.
Every operation and final digest is stable across all five measured samples.

## Corrected-base OLTP and OLAP

Five samples, `N=1000`, `D=10`; values are p50/p95 microseconds.

| Backend | Operation | Corrected p50/p95 | Exact-main p50/p95 |
|---|---|---:|---:|
| Rocks | point hit | 874 / 1713 | 679 / 1588 |
| Rocks | insert | 868 / 947 | 460 / 658 |
| Rocks | update D=1 | 885 / 944 | 337 / 492 |
| Rocks | range | 2069 / 2498 | 2003 / 2465 |
| Rocks | full scan | 2529 / 2642 | 1832 / 1990 |
| Rocks | cold reopen | 1931 / 2077 | 2313 / 2664 |
| Slate | point hit | 1152 / 1689 | 538 / 1272 |
| Slate | insert | 1200 / 1226 | 441 / 515 |
| Slate | update D=1 | 1286 / 1311 | 300 / 363 |
| Slate | range | 2347 / 2508 | 1701 / 1967 |
| Slate | full scan | 2771 / 2796 | 1731 / 1798 |
| Slate | cold reopen | 3085 / 3391 | 2835 / 3775 |
| Rocks | OLAP projected | 2172 / 2866 | 1633 / 2366 |
| Rocks | OLAP aggregate | 2227 / 2414 | 1394 / 1656 |
| Rocks | OLAP full | 2725 / 2898 | 1471 / 1641 |
| Slate | OLAP projected | 2361 / 3067 | 1700 / 2404 |
| Slate | OLAP aggregate | 2287 / 2573 | 1734 / 2012 |
| Slate | OLAP full | 2750 / 2903 | 1813 / 2114 |

The correction does not change the previously identified OLTP/OLAP physical
owner. Median corrected point reads still perform 36 `get_many` calls, 59
keys, and about 85.7 KiB. D=1 updates perform 107 calls, 176 keys, about
215.7 KiB read, 15 puts, and about 16.5 KiB logical writes.

## Public file lifecycle

Five samples, one MiB public payload; values are p50/p95 microseconds.

| Backend | Operation | Corrected | Exact main | Corrected delta |
|---|---|---:|---:|---:|
| Rocks | insert | 2957 / 3256 | 2358 / 2666 | +25.4% |
| Rocks | read | 2069 / 2360 | 846 / 1087 | +144.6% |
| Rocks | update | 3122 / 3375 | 1828 / 2384 | +70.8% |
| Rocks | delete | 3375 / 3494 | 1317 / 1412 | +156.3% |
| Rocks | branch read | 2096 / 2179 | 1933 / 2127 | +8.4% |
| Rocks | delete after branch | 3263 / 3281 | 1067 / 1088 | +205.8% |
| Rocks | cold reopen | 2227 / 2263 | 2567 / 2643 | -13.2% |
| Slate | insert | 3081 / 3226 | 1338 / 1492 | +130.3% |
| Slate | read | 1938 / 2085 | 425 / 474 | +356.0% |
| Slate | update | 3635 / 4035 | 1292 / 1560 | +181.3% |
| Slate | delete | 4157 / 4388 | 1152 / 1248 | +260.9% |
| Slate | branch read | 1887 / 1900 | 1548 / 1613 | +21.9% |
| Slate | delete after branch | 4233 / 4267 | 1178 / 1181 | +259.3% |
| Slate | cold reopen | 2496 / 2591 | 3318 / 3816 | -24.8% |

Representative corrected median physical counters are backend-independent at
the public storage boundary: insert 135 calls / 213 keys / 399 KiB read / 18
puts / 1.066 MiB logical write; read 64 / 102 / 1.287 MiB; update 150 / 243 /
401 KiB / 17 puts / 1.064 MiB; delete 240 / 347 / 808 KiB / 14 puts / 15.5
KiB. The payload write volume is close to main, but metadata reads and delete
writes remain materially amplified.

## Public VCS lifecycle

Five samples, `N=1000`, `H=10`; values are p50/p95 microseconds.

| Backend | Operation | Corrected | Exact main | Corrected delta |
|---|---|---:|---:|---:|
| Rocks | branch | 1422 / 1651 | 907 / 1047 | +56.8% |
| Rocks | checkpoint | 6601 / 6707 | 6788 / 7485 | -2.8% |
| Rocks | history | 1039 / 1457 | 493 / 758 | +110.8% |
| Rocks | diff | 1059 / 1222 | 650 / 689 | +62.9% |
| Rocks | working diff | 61537 / 61782 | 2154 / 2761 | +2756% |
| Rocks | merge | 1196 / 1204 | 1327 / 1344 | -9.9% |
| Rocks | undo | 1142 / 1153 | 374 / 386 | +205.3% |
| Rocks | redo | 1090 / 1096 | 311 / 377 | +250.5% |
| Rocks | cold reopen | 1950 / 2017 | 3328 / 3501 | -41.4% |
| Slate | branch | 2005 / 2073 | 1383 / 1413 | +45.0% |
| Slate | checkpoint | 7446 / 7602 | 7556 / 7705 | -1.5% |
| Slate | history | 1446 / 1812 | 487 / 851 | +196.9% |
| Slate | diff | 1330 / 1365 | 761 / 910 | +74.8% |
| Slate | working diff | 78033 / 79023 | 2464 / 2532 | +3067% |
| Slate | merge | 1881 / 1912 | 2091 / 2146 | -10.0% |
| Slate | undo | 1855 / 1895 | 561 / 570 | +230.7% |
| Slate | redo | 1781 / 1836 | 471 / 484 | +278.1% |
| Slate | cold reopen | 2604 / 3472 | 4660 / 5164 | -44.1% |

ForkTree's strongest qualified physical result is checkpoint publication:
about 837 KiB written over five samples versus 1.676 MiB on main, a 50%
reduction, while wall time is slightly faster. Merge and cold reopen also win.
The dominant promotion blocker is working diff: the corrected base performs
7,130 object calls, 7,195 keys and about 24.2 MiB of authenticated reads per
operation. Branch, history, diff, undo and redo remain slower despite matching
digests.

## Commands and evidence

Build:

```sh
CARGO_TARGET_DIR=/root/repos/lix/target timeout 1200 \
  cargo build --release -p lix-schema-v1-public-qualification
```

Verify and timed cells:

```sh
timeout 1200 lix-schema-v1-public-qualification.release \
  verify <rocksdb|slatedb> <oltp|olap|file|vcs> 10 1 1
timeout 1200 lix-schema-v1-public-qualification.release \
  run <rocksdb|slatedb> <oltp|olap|file|vcs> 1000 10 5
```

Corrected timed log SHA-256 values:

- Rocks: OLTP `53293aa539099e0e2ad9b9c8af25ce55938e0f626009cd7391abb434a1548704`,
  OLAP `ab266117c3e597c9f4f803ad3e17acab77ae48c9c8e10772b6d39d544e3e5b7b`,
  file `79075c3580acceb731ae1bb901802ab82c0bd8ea9d2e9eb5dda17416927ad691`,
  VCS `72a209ebb248999f47edde18bbbbf96f40e303f9747955442df2a9d04f760aed`.
- Slate: OLTP `b7bd2b77f4f8c32d4042a8743269d2819f2ee7c102263b4364dad9d488fa2e8d`,
  OLAP `408718c683a2a2947a0d9beb4f41e19ce013d0ae403bddf18c139a2fd8178e62`,
  file `227c9ad1fea08a0a8a32778ec7361389823eae967c08a935ae9e856852745b2c`,
  VCS `2f2dc501d54a89813e88d9ab9e4ce5efa057a7b4a4fdbba41cba6ba192d431b0`.

All raw logs are retained under
`/root/repos/lix-evidence/schema-v1-public-qualification/corrected-6da/logs`.

## Verdict and next lane

**SEMANTIC QUALIFICATION PASS; PERFORMANCE NO-PROMOTE.** The correction makes
all eight public cells green on both adapters and leaves the public workload
unchanged. It does not close the authenticated topology/read amplification.
The next composed carrier/PK-major qualification should retain this base and
must specifically measure the 7,130-call working-diff owner, file delete
metadata amplification, and the already-positive checkpoint/merge/reopen
properties.
