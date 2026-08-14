# Schema-v1 public qualification: dc4 main vs unified ForkTree 5089

## Immutable inputs

- Exact Schema-v1 main: `dc4f42917937150fa20fcb7517c46c21d1840045`.
- Exact unified Schema-v1 + ForkTree: `5089b964d5e9b0143656c5278e525db9100e2b61`.
- Main overlay commit: `39dcf91241162d196d5fd8b40440bb70b277b553`.
- Unified overlay commit: `4b582a95ce4b57d13b4e71d55258f569df1b6882`.
- Workload source Git blob on both commits: `e2d34e5f0d6e943c2d501372e066db309c60be60`.
- Workload source SHA-256 on both commits: `d90c11168243ccf3ff149621edb49b0cd02a2f8c1897c1ceba46bc948a674f81`.
- Main release binary SHA-256: `a53458176d79aed629cac1a72d097728e164f5c8f10a114b7b388a772641e0ba`.
- Unified release binary SHA-256: `dc0c192a24bcc64d5a4b5e6a67ada5a8fa0156e0038e4d2f1dc3dcea6dc5f5ab`.

The overlay changes only the workspace member list, lockfile, and
`benchmark-overlays/schema_v1_public/**`. It uses `lix::Lix`, public SQL, and
the public storage boundary. It has no candidate branches, private engine
hooks, or production changes. Setup/seed is outside every measured operation.
The schema exercises `text`, `uuid`, `int8`, `float8`, `boolean`, `jsonb`, and
`timestamptz` with deterministic values and digests.

Independent workload review initially found that the observer inherited the
default `StorageRead::snapshot_cache_key() -> None`, which disabled SlateDB's
snapshot-keyed derived caching. Before final measurement, the wrapper was
corrected identically on both anchors to delegate the inner key. It also counts
`delete_range`, backend deleted ranges, and backend storage calls. All evidence
below is from the corrected binaries and logs.

## Verify-only result

Exact main passed all 8 backend/suite cells. Unified passed OLTP and OLAP on
both backends. File and VCS stop at the first deterministic public semantic
failure on both backends:

| Anchor | Rocks OLTP | Slate OLTP | Rocks OLAP | Slate OLAP | Rocks file | Slate file | Rocks VCS | Slate VCS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| dc4 main | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| unified 5089 | PASS | PASS | PASS | PASS | BLOCK | BLOCK | BLOCK | BLOCK |

The unified file prefix proves public insert, exact read, update, and delete
before branch creation. The unified VCS prefix proves history, diff, and
working diff before checkpoint creation. No workaround or reduced workload was
used after either failure.

### Unified blockers

1. Public branch creation fails with
   `LIX_ERROR_SCHEMA_VALIDATION: write for schema 'lix_branch_descriptor' requires entity_pk because the schema has no primary_key`.
2. Public checkpoint creation fails with
   `LIX_ERROR_SCHEMA_DEFINITION: schema 'lix_checkpoint_marker' is not visible to this transaction`.

Both fail during transaction normalization, before backend publication.

Branch chain:

`Lix::create_branch -> SessionContext::create_branch -> branch_descriptor_stage_row -> Transaction::stage_write -> prepare_transaction_rows_with_homogeneous -> TransactionSchemaResolver::catalog_for_row_normalization -> normalize_raw_write_row_in_place -> resolve_entity_pk`.

The builtin Schema-v1 document declares `primary_key: ["id"]`, but unified
`SchemaPlan::compile` still calls `primary_key_paths`, which reads only legacy
`x-lix-primary-key`. Exact dc4 parses Schema v1 and projects
`schema.primary_key`. The hard-cut correction is to make Schema-v1 parsed
fields the sole identity/constraint source; adding an ad-hoc `entity_pk` at the
branch caller would hide the authority bug.

Checkpoint chain:

`Lix::create_checkpoint -> SessionContext::create_checkpoint -> checkpoint_marker_stage_row -> Transaction::stage_write -> prepare_transaction_rows_with_homogeneous -> catalog_for_row_normalization -> normalize_raw_write_row_in_place`.

Unified still bootstraps and writes `lix_checkpoint_marker`, but the composition
deleted its builtin Schema-v1 registration. Exact dc4 writes the registered
`lix_checkpoint` schema instead. The correction contract is one hidden builtin
Schema-v1 definition for the retained ForkTree marker (`branch_id uuid NOT
NULL`, primary key `branch_id`), seeded before writes and absent from public SQL
surfaces. Do not weaken visibility validation.

## Qualified five-sample crossover

Commands used `N=1000`, `D=10`, five samples, release binaries, identical
workload bytes. Values are p50/p95 microseconds; percentage is unified p50
relative to main. Digests match for every row below.

| Backend | Operation | Main p50/p95 | Unified p50/p95 | Unified delta |
|---|---|---:|---:|---:|
| RocksDB | point hit | 679 / 1588 | 1020 / 1654 | +50.2% |
| RocksDB | insert | 460 / 658 | 864 / 997 | +87.8% |
| RocksDB | update D=1 | 337 / 492 | 879 / 934 | +160.8% |
| RocksDB | update D=10 | 391 / 482 | 886 / 927 | +126.6% |
| RocksDB | range | 2003 / 2465 | 2148 / 2352 | +7.2% |
| RocksDB | full scan | 1832 / 1990 | 2609 / 2625 | +42.4% |
| RocksDB | first read after cold reopen | 2313 / 2664 | 1895 / 1969 | -18.1% |
| SlateDB | point hit | 538 / 1272 | 971 / 1769 | +80.5% |
| SlateDB | insert | 441 / 515 | 1199 / 1280 | +171.9% |
| SlateDB | update D=1 | 300 / 363 | 1273 / 1306 | +324.3% |
| SlateDB | update D=10 | 354 / 407 | 1307 / 1357 | +269.2% |
| SlateDB | range | 1701 / 1967 | 2260 / 2487 | +32.9% |
| SlateDB | full scan | 1731 / 1798 | 2772 / 2781 | +60.1% |
| SlateDB | first read after cold reopen | 2835 / 3775 | 2573 / 3387 | -9.2% |

OLAP:

| Backend | Operation | Main p50/p95 | Unified p50/p95 | Unified delta |
|---|---|---:|---:|---:|
| RocksDB | projected scan | 1633 / 2366 | 2087 / 2828 | +27.8% |
| RocksDB | aggregate | 1394 / 1656 | 2102 / 2342 | +50.8% |
| RocksDB | full scan | 1471 / 1641 | 2650 / 2840 | +80.1% |
| SlateDB | projected scan | 1700 / 2404 | 2306 / 3067 | +35.6% |
| SlateDB | aggregate | 1734 / 2012 | 2304 / 2520 | +32.9% |
| SlateDB | full scan | 1813 / 2114 | 2825 / 2953 | +55.8% |

## Physical public-storage counters

Median counters below are observed at the same public Storage boundary. Bytes
are backend bytes returned by `get_many`; settled bytes are the maximum physical
directory bytes observed in that cell.

| Backend/operation | Anchor | get_many calls / keys / bytes | puts / write bytes | settled bytes |
|---|---|---:|---:|---:|
| Rocks point | main | 11 / 12 / 799 | 0 / 0 | 203,745 |
| Rocks point | unified | 36 / 59 / 84,966 | 0 / 0 | 364,765 |
| Rocks update D=1 | main | 27 / 30 / 2,797 | 11 / 1,526 | 212,800 |
| Rocks update D=1 | unified | 107 / 176 / 213,844 | 15 / 16,088 | 406,857 |
| Slate point | main | 11 / 12 / 799 | 0 / 0 | 130,410 |
| Slate point | unified | 36 / 59 / 84,976 | 0 / 0 | 296,412 |
| Slate update D=1 | main | 27 / 30 / 2,797 | 11 / 1,526 | 131,878 |
| Slate update D=1 | unified | 107 / 176 / 213,750 | 15 / 16,087 | 329,077 |
| Rocks full scan | main | 14 / 16 / 800 | 0 / 0 | 230,117 |
| Rocks full scan | unified | 52 / 84 / 219,916 | 0 / 0 | 454,463 |
| Slate full scan | main | 14 / 16 / 800 | 0 / 0 | 134,371 |
| Slate full scan | unified | 52 / 84 / 219,859 | 0 / 0 | 374,975 |

The qualified evidence therefore does not support promotion yet. Unified's
dominant observable owner is authenticated object/topology work: point reads
perform 3.3x the calls, 4.9x the keys and about 106x the returned bytes; D=1
updates perform about 4x the calls, 5.9x the keys, 76x the read bytes, and 10.5x
the durable write bytes. This is not a Slate-only effect, although Slate wall
penalties are larger.

The successful one-shot public file prefix also shows the same direction. On
Rocks, unified/main wall microseconds are insert 2865/2556, read 1461/941,
update 2619/2224, delete 1679/834. On Slate they are 2208/1807, 1102/610,
2516/1916, 2106/932. File digests match through delete. Branch sharing and
cold-reopen file qualification remain blocked by branch creation and are not
claimed.

## Commands and log hashes

Build on each checkout:

```sh
CARGO_TARGET_DIR=/root/repos/lix/target timeout 1200 \
  cargo build --release -p lix-schema-v1-public-qualification
```

Verify cell:

```sh
timeout 1200 lix-schema-v1-public-qualification.release \
  verify <rocksdb|slatedb> <oltp|olap|file|vcs> 10 1 1
```

Qualified timed cell:

```sh
timeout 1200 lix-schema-v1-public-qualification.release \
  run <rocksdb|slatedb> <oltp|olap> 1000 10 5
```

Key final log SHA-256 values:

- Main verify: Rocks OLTP `04d00109...`, OLAP `521318d6...`, file
  `c7cff982...`, VCS `5a97f51a...`; Slate OLTP `09341f88...`, OLAP
  `cb9a488a...`, file `5c3c2608...`, VCS `c673b548...`.
- Unified verify: Rocks OLTP `231b9884...`, OLAP `336bd0b7...`, file
  `f49dacbc...`, VCS `346d059a...`; Slate OLTP `8d1ea775...`, OLAP
  `2a060273...`, file `86405ef4...`, VCS `09e3836a...`.
- Main timed: Rocks OLTP `bfb0ad84...`, OLAP `71d69490...`; Slate OLTP
  `bc21c2e2...`, OLAP `e6b9fc1e...`.
- Unified timed: Rocks OLTP `13af83e7...`, OLAP `88072f53...`; Slate OLTP
  `55d641e2...`, OLAP `1f3892f6...`.

`cargo fmt --all -- --check` and exact-anchor `git diff --check` pass on
both overlay commits.

Independent workload/source review is **APPROVE** after the observer correction:
the complete overlay trees are byte-identical, the Slate snapshot identity is
preserved, every public storage method is delegated, and no private,
candidate-specific, or hidden benchmark path exists. Its non-blocking caveats
are that reopen timing covers the first query after reopen (not adapter open),
VCS receipts use deterministic success markers rather than state-derived
digests, and allocation counters include process-wide work during each measured
interval.

## Terminal qualification

**BLOCKED for unified promotion.** The public harness itself is valid and exact
main is fully green. Unified is semantically blocked for branch/checkpoint VCS
flows and is slower with materially higher authenticated reads/writes on every
qualified hot path except cold reopen. Fix the two Schema-v1 authority seams
first; only then is expanded VCS/file scaling legitimate.
