# Optimization Log 20: storage_v2 Baseline

Date: 2026-05-14

This log captures the first `storage_v2` adapter microbench baseline. Most
benchmarks intentionally use fake/counting `backend_v2` implementations so the
numbers mostly measure storage-layer shape and adapter overhead, not SQLite,
filesystem, cache, or transaction behavior. The final group runs the same paths
against `ConformanceBackend` to include real in-memory backend costs.

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench storage_v2
```

## Shape Guards

The benchmark asserts the access shape while measuring:

- write set lowering: staged puts/deletes across `G` spaces lower to at most
  one `put_many` and one `delete_many` call per touched space, then one commit.
- point reads: `M` requested keys lowers to `U` unique backend keys and one
  backend `get_many` call, then reconstructs `M` caller-order slots.
- prefix scans: `scan_prefix` lowers to one backend `scan_range` call.

## Complexity Contract

The timings below are useful only if they preserve the intended asymptotic
shape. Each bench group has an expected curve and a failure mode to watch for.

### Write Set Lowering Complexity

Expected today:

```text
K = staged mutations
G = touched spaces
B = backend write batches

stage:       O(K log G) with current BTreeMap grouping
validate:    O(K log K) with current duplicate-detection BTreeSet
stats:       O(K)
lower:       O(K + G)
backend I/O: B <= 2G
commit:      1 backend commit
```

Target after optimization:

```text
stage + validate + stats + lower: O(K + G) expected
backend I/O: O(G), not O(K)
commit: 1
```

Watch for:

```text
O(K * G) group lookup
O(K) backend calls
multiple commit boundaries
repeated full passes that can be fused
value-size costs caused by avoidable cloning/copying
```

Observed in this baseline:

```text
K scaling is roughly linear:
  128 -> 1024 -> 8192 puts scales in the expected direction.

G scaling does not show K*G blowup:
  G=16, G=64, and G=256 are not catastrophically worse.

Value size is visible:
  64 KiB values are much slower even with a counting backend, so value
  construction/accounting is part of the measured path.
```

### Point Read Adapter Complexity

Expected:

```text
M = requested caller-order slots
U = unique backend keys
F = found unique entries returned by backend

dedupe:              O(M)
backend get_many:    1 call over U keys
found map:           O(F)
slot reconstruction: O(M)

total adapter shape: O(M + U + F)
```

Watch for:

```text
O(M * U) reconstruction
O(M * F) missing checks
excess key/value cloning
missing-heavy reads getting slower despite fewer found entries
```

Observed in this baseline:

```text
M scaling is roughly linear:
  100 -> 1000 -> 10000 requested slots scales in the expected direction.

U matters:
  M=1000/U=100 is faster than M=1000/U=1000.

Missing-heavy reads are faster:
  fewer found entries reduce found-map work while storage still emits M slots.
```

### Prefix Scan Adapter Complexity

Expected:

```text
Q = emitted rows
P = prefix length

prefix-to-range: O(P)
backend scan_range: 1 call
result handling: O(Q)

total adapter shape: O(P + Q)
```

Watch for:

```text
more than one backend scan_range call
superlinear row handling
prefix lowering that allocates proportional to table size
```

Observed in this baseline:

```text
Q scaling is roughly linear:
  100 -> 1000 -> 10000 rows scales in the expected direction.

The q0 case isolates fixed adapter overhead:
  about 60 ns in this run.
```

### Conformance Backend Complexity

Expected:

```text
N = backend rows in the in-memory BTreeMap
K = committed mutations
U = unique point keys
M = requested point slots
Q = scan rows emitted

commit_puts: O(N snapshot + K log N)
get_many:    O(N snapshot + U log N + M)
scan_range:  O(N snapshot + N), with current snapshot cloning and full-map scan
```

This is intentionally not the same as the fake-backend adapter benches. The
`ConformanceBackend` is a correctness reference with real in-memory map and
snapshot behavior, so it includes costs that a production in-memory backend may
avoid with structural sharing or cheaper snapshots.

Watch for:

```text
ConformanceBackend being orders of magnitude slower than fake backend
snapshot cloning dominating every read
scan_range behaving like O(N) even for small Q
```

Observed in this baseline:

```text
ConformanceBackend is close enough to fake-backend timing for the current
small cases to be useful as an end-to-end in-memory reference.

The current implementation snapshots by cloning the BTreeMap and scan_range
iterates the snapshot instead of using a BTreeMap range. Larger N / small Q
bench cases should be added before using it as the final in-memory layout
answer.

Target production in-memory backend shape:

```text
begin_read:  O(1) or cheap structural-share snapshot
get_many:    O(U log N + M)
scan_range:  O(log N + Q)
commit_puts: O(K log N) plus one atomic publication step
```
```

## Baseline Results

### Write Set Lowering

| Case                       |      Mean |     Throughput |
| -------------------------- | --------: | -------------: |
| `puts_k128_g1_v32`         | 14.157 us | 9.0416 Melem/s |
| `puts_k1024_g1_v32`        | 165.55 us | 6.1853 Melem/s |
| `puts_k1024_g16_v32`       | 121.41 us | 8.4342 Melem/s |
| `puts_k8192_g16_v32`       | 1.3470 ms | 6.0815 Melem/s |
| `puts_k1024_g64_v32`       | 100.53 us | 10.186 Melem/s |
| `puts_k4096_g256_v32`      | 500.54 us | 8.1831 Melem/s |
| `deletes_k1024_g16`        | 121.41 us | 8.4342 Melem/s |
| `mixed80_20_k1024_g16_v32` | 115.28 us | 8.8830 Melem/s |
| `puts_k1024_g16_v1024`     | 128.57 us | 7.9646 Melem/s |
| `puts_k1024_g16_v65536`    | 492.47 us | 2.0793 Melem/s |

### Point Read Adapter

| Case                   |      Mean |     Throughput |
| ---------------------- | --------: | -------------: |
| `m100_u100`            | 7.7849 us | 12.845 Melem/s |
| `m1000_u1000`          | 95.488 us | 10.472 Melem/s |
| `m1000_u100`           | 60.558 us | 16.513 Melem/s |
| `m10000_u100`          | 686.52 us | 14.566 Melem/s |
| `m1000_u100_missing10` | 59.608 us | 16.776 Melem/s |
| `m1000_u100_missing90` | 47.981 us | 20.842 Melem/s |

### Prefix Scan Adapter

| Case     |      Mean |     Throughput |
| -------- | --------: | -------------: |
| `q0`     | 59.522 ns |            n/a |
| `q100`   | 410.77 ns | 243.45 Melem/s |
| `q1000`  | 4.7263 us | 211.58 Melem/s |
| `q10000` | 44.593 us | 224.25 Melem/s |

### Conformance Backend

| Case                        |      Mean |     Throughput |
| --------------------------- | --------: | -------------: |
| `commit_puts_k1024_g16_v32` | 170.02 us | 6.0228 Melem/s |
| `get_many_m1000_u100`       | 92.077 us | 10.860 Melem/s |
| `scan_range_q1000`          | 43.983 us | 22.736 Melem/s |

## Notes

- `puts_k1024_g16_v32` being faster than `puts_k1024_g1_v32` is plausible with the current
  implementation because each group has fewer staged entries to move/lower;
  revisit after inspecting the write-set grouping internals.
- High space counts are not obviously worse in this fake backend baseline:
  `puts_k1024_g64_v32` is faster than the one-space case. This suggests the
  current hot cost is not just `BTreeMap` group lookup; validation, vector move
  size, and per-group batch construction should be inspected together.
- The 64 KiB value case is much slower, even though the counting backend does
  not persist bytes. This points at staging/value construction and `written_bytes`
  accounting as expected pressure points.
- Point read cost scales with caller-order reconstruction and unique-key
  dedupe. The `m1000_u100` case is faster than `m1000_u1000`, which is the
  intended benefit of deduping before backend access.
- Missing-heavy point reads are faster because the fake backend returns fewer
  found entries, but storage still reconstructs all `M` caller-order slots.
- Prefix scan numbers are mostly the fake backend cloning returned rows plus
  the prefix-to-range adapter path. This is useful as a shape baseline, not a
  real backend scan throughput result.
- The `ConformanceBackend` group is the first in-memory end-to-end reference:
  it includes `BTreeMap` backend behavior and snapshot cloning, so it should be
  read separately from the fake-backend adapter-only groups.

## Optimization Entries

### 2026-05-14: Remove Duplicate Commit Validation

Change:

```text
StorageWriteSet::commit() now validates once before opening BackendWrite,
then lowers through an internal lower_validated_into() path.

StorageWriteSet::lower_into() still validates for direct callers.
```

Why:

```text
The previous commit path validated twice:

  commit()
    validate()
    lower_into()
      validate()

This added a second O(K log K) duplicate-detection pass. The first attempted
patch removed the commit-side validation entirely, but tests caught that this
opened BackendWrite before rejecting invalid write sets. The final patch keeps
the pre-open validation invariant while removing the duplicate pass.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2
```

Write-set scorecard:

| Case                       | Baseline Mean |  New Mean | Criterion Change |
| -------------------------- | ------------: | --------: | ---------------: |
| `puts_k128_g1_v32`         |     14.157 us | 9.0113 us |   36.295% faster |
| `puts_k1024_g1_v32`        |     165.55 us | 97.211 us |   41.334% faster |
| `puts_k1024_g16_v32`       |     121.41 us | 75.632 us |   40.709% faster |
| `puts_k8192_g16_v32`       |     1.3470 ms | 809.60 us |   40.026% faster |
| `puts_k1024_g64_v32`       |     100.53 us | 65.529 us |   34.365% faster |
| `puts_k4096_g256_v32`      |     500.54 us | 305.77 us |   41.780% faster |
| `deletes_k1024_g16`        |     121.41 us | 73.754 us |   39.530% faster |
| `mixed80_20_k1024_g16_v32` |     115.28 us | 70.281 us |   37.419% faster |
| `puts_k1024_g16_v1024`     |     128.57 us | 82.878 us |   35.268% faster |
| `puts_k1024_g16_v65536`    |     492.47 us | 435.13 us |   13.489% faster |

ConformanceBackend write-path scorecard:

| Case                        | Baseline Mean |  New Mean | Criterion Change |
| --------------------------- | ------------: | --------: | ---------------: |
| `commit_puts_k1024_g16_v32` |     170.02 us | 125.36 us |   28.777% faster |

Read/prefix notes:

```text
Point-read and prefix-scan cases showed only noise or unrelated variance.
This patch should affect write-set commit paths only.
```

Complexity impact:

```text
Before:
  commit validate: O(K log K)
  lower_into validate: O(K log K)
  stats: O(K)
  lower: O(K + G)

After:
  commit validate: O(K log K)
  lower_into internal path: no second validation
  stats: O(K)
  lower: O(K + G)

Remaining target:
  fuse validate + stats + lower, or replace duplicate validation with an
  expected O(K) strategy.
```

### 2026-05-14: Hash-Based Point Read Reconstruction

Change:

```text
get_many_caller_order_with_stats() now uses:

  HashSet + Vec for first-seen unique backend keys
  HashMap for found backend entries
  preallocated Vec<PointSlot> for caller-order reconstruction

The previous implementation used:

  BTreeSet for dedupe
  BTreeMap for found entries
  collect() for output slots
```

Semantic note:

```text
The storage adapter no longer sorts backend get_many keys as an accidental
side effect of BTreeSet. It sends unique backend keys in first-seen caller
order. Storage still reconstructs the final result in exact caller order with
duplicates and missing slots preserved.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2
```

Point-read scorecard:

| Case                   | Previous Mean |  New Mean | Criterion Change |
| ---------------------- | ------------: | --------: | ---------------: |
| `m100_u100`            |     7.8403 us | 5.9119 us |   23.987% faster |
| `m1000_u1000`          |     96.331 us | 64.843 us |   31.720% faster |
| `m1000_u100`           |     64.128 us | 35.089 us |   44.821% faster |
| `m10000_u100`          |     687.71 us | 317.81 us |   53.705% faster |
| `m1000_u100_missing10` |     59.064 us | 34.251 us |   42.175% faster |
| `m1000_u100_missing90` |     50.114 us | 27.673 us |   44.002% faster |

Other scorecard notes:

```text
Write-set cases were unchanged apart from noise, as expected.
Prefix scan cases were unchanged apart from noise, as expected.
ConformanceBackend get_many_m1000_u100 did not show a significant change,
which suggests that the current correctness/reference in-memory backend costs
still dominate that group.

ConformanceBackend commit/scan showed regressions in this run, but those paths
do not use the point-read adapter and should be treated as run-to-run variance
until reproduced by a targeted profile.
```

Complexity impact:

```text
Before:
  dedupe: BTreeSet, O(M log U)
  found map: BTreeMap, O(F log F)
  reconstruct: O(M log F)

After:
  dedupe: HashSet + Vec, O(M) expected
  found map: HashMap, O(F) expected
  reconstruct: O(M) expected

Target shape:
  O(M + U + F) expected
```

### 2026-05-14: Fuse Write-Set Stats Into Lowering

Change:

```text
lower_validated_into() now computes StorageWriteSetStats while lowering
groups into backend put_many/delete_many calls.

The public StorageWriteSet::stats() helper remains available, but commit()
and lower_into() no longer call it as a separate full write-set pass.
```

Why:

```text
After removing duplicate validation, the write path still had:

  validate pass
  stats pass
  lower pass

This patch fuses stats and lower:

  validate pass
  lower + stats pass

The remaining dominant algorithmic cost is duplicate validation.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2
```

Write-set scorecard:

| Case                       | Previous Mean |  New Mean | Criterion Change |
| -------------------------- | ------------: | --------: | ---------------: |
| `puts_k128_g1_v32`         |     8.7785 us | 8.9700 us |     within noise |
| `puts_k1024_g1_v32`        |     95.330 us | 95.372 us |        no change |
| `puts_k1024_g16_v32`       |     75.273 us | 73.993 us |     within noise |
| `puts_k8192_g16_v32`       |     788.57 us | 790.55 us |        no change |
| `puts_k1024_g64_v32`       |     65.337 us | 65.358 us |        no change |
| `puts_k4096_g256_v32`      |     312.20 us | 331.71 us |    7.491% slower |
| `deletes_k1024_g16`        |     73.561 us | 73.988 us |     within noise |
| `mixed80_20_k1024_g16_v32` |     72.349 us | 70.676 us |        no change |
| `puts_k1024_g16_v1024`     |     86.662 us | 82.678 us |    6.513% faster |
| `puts_k1024_g16_v65536`    |     448.23 us | 440.62 us |        no change |

Other scorecard notes:

```text
Most write-set cases were unchanged or within noise. The 1 KiB value case
improved, but the high-space-count G=256 case regressed in this run. Since the
patch removes a full pass but leaves duplicate validation untouched, the modest
scorecard is plausible.

Point-read and prefix-scan changes in this run are unrelated variance. The
ConformanceBackend improvements likely reflect run-to-run variance and should
not be attributed to this patch.
```

Complexity impact:

```text
Before this patch:
  validate: O(K log K)
  stats: O(K)
  lower: O(K + G)

After this patch:
  validate: O(K log K)
  lower + stats: O(K + G)

Remaining target:
  replace duplicate validation with an expected O(K) strategy or canonicalize
  duplicates during staging/sealing.
```

### 2026-05-14: Hash Duplicate Validation

Change:

```text
StorageWriteSet::validate() now uses a pre-sized HashSet for duplicate
mutation detection instead of BTreeSet.

The duplicate rule is unchanged: a sealed write set may contain at most one
final mutation per (SpaceId, Key).
```

Why:

```text
Focused samply profiles of write_set_lowering showed duplicate validation as
the strongest real storage_v2 signal after stats fusion, around 20% inclusive
for small-value write-set commits.

The previous validator was ordered but did not need ordered semantics.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 write_set_lowering
```

Write-set scorecard:

| Case                       | Previous Mean |  New Mean | Criterion Change |
| -------------------------- | ------------: | --------: | ---------------: |
| `puts_k128_g1_v32`         |     8.9700 us | 5.5251 us |   38.658% faster |
| `puts_k1024_g1_v32`        |     95.372 us | 44.867 us |   53.058% faster |
| `puts_k1024_g16_v32`       |     73.993 us | 45.524 us |   44.993% faster |
| `puts_k8192_g16_v32`       |     790.55 us | 365.31 us |   54.088% faster |
| `puts_k1024_g64_v32`       |     65.358 us | 47.089 us |   28.162% faster |
| `puts_k4096_g256_v32`      |     331.71 us | 191.70 us |   44.389% faster |
| `deletes_k1024_g16`        |     73.988 us | 40.631 us |   44.777% faster |
| `mixed80_20_k1024_g16_v32` |     70.676 us | 44.597 us |   36.842% faster |
| `puts_k1024_g16_v1024`     |     82.678 us | 53.383 us |   41.230% faster |
| `puts_k1024_g16_v65536`    |     440.62 us | 406.59 us |    8.044% faster |

Post-change profile:

```text
Profile:
  target/storage_v2_profiles/hash_validation/write_k1024_g16.json

Main signals:
  benchmark key formatting/setup is now the dominant visible cost
  StorageWriteSet::commit remains visible
  CountingWrite::put_many remains visible
  StorageWriteSet::validate is much smaller than before

This suggests the next step should improve the benchmark harness so setup/key
construction does not obscure storage_v2 internals before chasing smaller
storage implementation changes.
```

Complexity impact:

```text
Before:
  duplicate validation: O(K log K)

After:
  duplicate validation: O(K) expected

Target shape:
  validation remains a required commit-path pass, but no longer adds a tree
  factor to write-set commits.
```

### 2026-05-14: Remove Fixture Construction From Storage Benches

Change:

```text
The storage_v2 benchmark harness now prebuilds write mutations and read fixtures
outside measured loops.

write_set_lowering:
  still measures staging, lowering, and commit
  no longer measures per-iteration format! key construction
  no longer measures per-iteration value byte generation

conformance_backend/get_many and scan_range:
  seed once outside the measured loop
  reuse a read snapshot for read-path timing
```

Why:

```text
Post-validation profiles showed benchmark setup dominating the write-set and
conformance paths. The largest noise source was format! key construction and
per-iteration seeding, which hid the real storage_v2 costs.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2 write_set_lowering
cargo bench -p lix_engine --features storage-benches --bench storage_v2 conformance_backend
```

Write-set scorecard:

| Case                       | Previous Mean |  New Mean | Criterion Change |
| -------------------------- | ------------: | --------: | ---------------: |
| `puts_k128_g1_v32`         |     5.5251 us | 2.7507 us |   51.127% faster |
| `puts_k1024_g1_v32`        |     44.867 us | 22.751 us |   48.506% faster |
| `puts_k1024_g16_v32`       |     45.524 us | 22.982 us |   55.243% faster |
| `puts_k8192_g16_v32`       |     365.31 us | 196.52 us |   47.594% faster |
| `puts_k1024_g64_v32`       |     47.089 us | 24.431 us |   48.975% faster |
| `puts_k4096_g256_v32`      |     191.70 us | 103.79 us |   45.191% faster |
| `deletes_k1024_g16`        |     40.631 us | 20.753 us |   50.289% faster |
| `mixed80_20_k1024_g16_v32` |     44.597 us | 25.216 us |   46.112% faster |
| `puts_k1024_g16_v1024`     |     53.383 us | 26.016 us |   50.024% faster |
| `puts_k1024_g16_v65536`    |     406.59 us | 40.899 us |   90.224% faster |

Conformance backend scorecard:

| Case                        | Previous Mean |  New Mean | Criterion Change |
| --------------------------- | ------------: | --------: | ---------------: |
| `commit_puts_k1024_g16_v32` |     103.03 us | 76.227 us |   25.244% faster |
| `get_many_m1000_u100`       |     73.467 us | 50.878 us |   29.188% faster |
| `scan_range_q1000`          |     52.960 us | 9.5034 us |   81.757% faster |

Post-cleanup profiles:

```text
Profiles:
  target/storage_v2_profiles/clean_harness/write_k1024_g16.json
  target/storage_v2_profiles/clean_harness/point_m10000_u100.json
  target/storage_v2_profiles/clean_harness/conformance_get_many.json

Remaining signals:
  write-set path:
    staging Bytes clones/drops
    duplicate-validation hashing
    HashMap inserts inside validation

  point read path:
    PointSlot key cloning
    ProjectedValue/Bytes cloning
    HashMap/HashSet hashing

  conformance get_many:
    storage_v2 point reconstruction dominates
    conformance backend get_many is visible but secondary
```

Optimization implications:

```text
The next large implementation win is likely a values-only point-read result:

  Vec<Option<ProjectedValue>>

instead of:

  Vec<PointSlot { key: Key, value: Option<ProjectedValue> }>

The caller already has the requested keys, so echoing a cloned Key per slot is
not needed for most storage/domain-store paths.
```

### 2026-05-14: Values-Only Point Reads

Change:

```text
Added values-only caller-order point read helpers:

  get_many_values_caller_order()
  get_many_values_caller_order_with_stats()

The existing PointSlot APIs remain for callers that need echoed keys, but they
now build on top of the values-only helper.

Point dedupe now uses HashSet<&Key> plus Vec<Key>, cloning each unique backend
key once instead of cloning keys into both the dedupe set and backend request.
```

Why:

```text
After cleaning the benchmark harness, point-read profiles showed the hottest
storage_v2 costs were:

  PointSlot key cloning
  ProjectedValue/Bytes cloning
  HashSet/HashMap hashing

Most domain-store callers already retain the requested keys, so returning
PointSlot { key, value } on the hot path repeats information they already have.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2 point_read_adapter
cargo bench -p lix_engine --features storage-benches --bench storage_v2 conformance_backend/get_many_m1000_u100
```

Point-read scorecard:

| Case                   | Previous Mean |  New Mean | Criterion Change |
| ---------------------- | ------------: | --------: | ---------------: |
| `m100_u100`            |     6.6343 us | 5.1355 us |   22.575% faster |
| `m1000_u1000`          |     64.619 us | 52.567 us |   18.646% faster |
| `m1000_u100`           |     36.681 us | 28.876 us |   21.272% faster |
| `m10000_u100`          |     335.95 us | 244.80 us |   26.890% faster |
| `m1000_u100_missing10` |     34.583 us | 26.809 us |   22.479% faster |
| `m1000_u100_missing90` |     27.616 us | 21.637 us |   21.654% faster |

Conformance backend scorecard:

| Case                  | Previous Mean |  New Mean | Criterion Change |
| --------------------- | ------------: | --------: | ---------------: |
| `get_many_m1000_u100` |     49.741 us | 39.572 us |   19.424% faster |

Post-change profiles:

```text
Profiles:
  target/storage_v2_profiles/values_point/point_m10000_u100.json
  target/storage_v2_profiles/values_point/conformance_get_many.json

Remaining signals:
  point-read path:
    HashMap insert/hash while building found map
    hashing while deduping requested keys
    ProjectedValue/Bytes clone/drop for duplicated result values

  conformance get_many:
    storage_v2 point reconstruction still dominates
    ConformanceRead::get_many is visible and currently does a linear duplicate
    check over returned entries
```

Optimization implications:

```text
The next point-read win is likely reducing hash work:

  - use a faster non-cryptographic hasher for storage-local maps, or
  - specialize small/mostly-unique point batches, or
  - change backend get_many/result shape to avoid building a found HashMap
    when a backend can return values aligned with requested unique keys.

The next in-memory-backend win is separate: ConformanceBackend remains a
correctness backend, not an optimized in-memory backend.
```

### 2026-05-14: Fast Storage Hash Maps

Change:

```text
Storage-local hash maps/sets now use ahash with fixed seeds for the hot
storage_v2 adapter paths:

  point read dedupe/reconstruction
  write-set duplicate validation

The hash choice is intentionally local to storage adapter internals. It is not
used for persistence, content identity, checksums, or externally visible order.
```

Why:

```text
Hash microbenchmarks showed the storage hot paths are dominated by Rust
HashMap/HashSet behavior over many small structured keys, not by hashing large
contiguous byte buffers.

ahash was fastest for the actual map/set workloads, even when raw hash-only
benchmarks were close or favored another hasher.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 hash_algorithms
cargo bench -p lix_engine --features storage-benches --bench storage_v2 point_read_adapter
cargo bench -p lix_engine --features storage-benches --bench storage_v2 write_set_lowering
```

Representative hash scorecard:

| Case                            |      std |    ahash | rustc_fx |     xxh3 |    blake3 |
| ------------------------------- | -------: | -------: | -------: | -------: | --------: |
| `point_reconstruct_m10000_u100` | 248.83us | 127.43us | 129.76us | 252.03us | 1.3980 ms |
| `point_reconstruct_m1000_u1000` | 47.209us | 26.844us | 29.718us | 48.829us | 219.53 us |
| `write_validate_k1024`          | 19.233us | 9.6647us | 11.169us | 20.832us | 79.382 us |

Complexity impact:

```text
The asymptotic shape is unchanged:

  point reads: O(M + U + F) expected
  write validation: O(K) expected

The constant factor for storage-local hash work is substantially lower.
```

### 2026-05-14: Index-Based Point Reconstruction

Change:

```text
get_many_values_caller_order_with_stats() now dedupes into:

  unique backend keys
  requested-slot -> unique-key indexes

After the backend call, returned values are placed into a unique-value vector
and caller-order results are rebuilt by integer index.

This removes the second found-entry HashMap and removes one hash lookup per
requested caller-order slot.
```

Why:

```text
Post-ahash profiles showed the top remaining point-read cost was algorithmic:

  dedupe requested keys
  build found HashMap from returned entries
  hash every requested key again while reconstructing M output slots

For duplicate-heavy reads like M=10000/U=100, that final M hash-lookup pass is
avoidable. Integer reconstruction preserves the same API and shape guards.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2 point_read_adapter
cargo bench -p lix_engine --features storage-benches --bench storage_v2 conformance_backend/get_many_m1000_u100
```

Point-read scorecard:

| Case                   |      Mean | Criterion Change |
| ---------------------- | --------: | ---------------: |
| `m100_u100`            | 3.1521 us |    5.190% faster |
| `m1000_u1000`          | 33.029 us |    8.943% faster |
| `m1000_u100`           | 12.905 us |   20.518% faster |
| `m10000_u100`          | 102.99 us |   26.439% faster |
| `m1000_u100_missing10` | 12.476 us |   18.951% faster |
| `m1000_u100_missing90` | 9.0254 us |   11.695% faster |

Conformance backend scorecard:

| Case                  |      Mean | Criterion Change |
| --------------------- | --------: | ---------------: |
| `get_many_m1000_u100` | 26.742 us |    8.999% faster |

Complexity impact:

```text
Before:
  dedupe/index requested keys: O(M) expected
  found map from backend entries: O(F) expected
  reconstruct caller slots: O(M) expected with M hash lookups

After:
  dedupe/index requested keys: O(M) expected
  assign backend entries to unique slots: O(F) expected
  reconstruct caller slots: O(M) integer indexing

The Big-O remains O(M + U + F), but the hot duplicate-heavy path now avoids
hashing the requested key again for every output slot.
```

### 2026-05-14: Borrowed Write-Set Validation

Change:

```text
StorageWriteSet::validate() now stores borrowed keys in its duplicate-check
HashSet:

  HashSet<(SpaceId, &Key)>

instead of cloning every key into:

  HashSet<(SpaceId, Key)>

The duplicate-mutation error path still clones the offending key to report the
same owned error value.
```

Why:

```text
After hash-based validation, the success path still cloned every staged key
just to prove there were no duplicate final mutations. Validation does not need
ownership; it only needs stable references for the duration of the pass.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 write_set_lowering
```

Write-set scorecard:

| Case                       |      Mean |      Criterion Change |
| -------------------------- | --------: | --------------------: |
| `puts_k128_g1_v32`         | 1.1258 us |        36.387% faster |
| `puts_k1024_g1_v32`        | 9.4771 us |        44.458% faster |
| `puts_k1024_g16_v32`       | 9.6148 us |        49.473% faster |
| `puts_k8192_g16_v32`       | 113.00 us |        22.387% faster |
| `puts_k1024_g64_v32`       | 11.432 us |        32.664% faster |
| `puts_k4096_g256_v32`      | 42.617 us |        36.093% faster |
| `deletes_k1024_g16`        | 7.4618 us |        41.389% faster |
| `mixed80_20_k1024_g16_v32` | 14.864 us | no significant change |
| `puts_k1024_g16_v1024`     | 10.311 us |        30.596% faster |
| `puts_k1024_g16_v65536`    | 10.289 us |        30.772% faster |

Complexity impact:

```text
The asymptotic shape is unchanged:

  validation: O(K) expected

The success path no longer clones K keys during validation, so write-set
lowering is closer to pure hashing plus grouped batch transfer.
```

### 2026-05-14: Indexed Point Read Results

Change:

```text
Added an indexed point-read result shape:

  IndexedPointValues {
    unique_values: Vec<Option<ProjectedValue>>,
    requested_to_unique: Vec<usize>,
  }

The existing materialized caller-order values API remains, but now layers over
the indexed representation by cloning values into caller-order slots only when
that materialized shape is requested.
```

Why:

```text
After index-based reconstruction, duplicate-heavy point reads still cloned the
same ProjectedValue once per requested caller-order slot. For M=10000/U=100,
that means materializing 10k value slots even though only 100 unique backend
values exist.

The indexed shape preserves caller-order semantics while letting domain stores
that can consume indexes avoid duplicate value clones.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2 point_read_adapter
cargo bench -p lix_engine --features storage-benches --bench storage_v2 point_read_indexed_adapter
```

Materialized point-read scorecard:

| Case                   |      Mean | Criterion Change |
| ---------------------- | --------: | ---------------: |
| `m100_u100`            | 2.8788 us |   18.022% faster |
| `m1000_u1000`          | 31.483 us |   15.404% faster |
| `m1000_u100`           | 12.353 us |   24.999% faster |
| `m10000_u100`          | 102.57 us |     within noise |
| `m1000_u100_missing10` | 11.931 us |    9.077% faster |
| `m1000_u100_missing90` | 8.5907 us |    7.067% faster |

Indexed point-read scorecard:

| Case                   |      Mean |
| ---------------------- | --------: |
| `m100_u100`            | 2.4703 us |
| `m1000_u1000`          | 26.701 us |
| `m1000_u100`           | 7.3173 us |
| `m10000_u100`          | 51.644 us |
| `m1000_u100_missing10` | 7.1528 us |
| `m1000_u100_missing90` | 6.1998 us |

Shape comparison:

```text
m10000/u100 materialized:
  102.57 us

m10000/u100 indexed:
  51.644 us

Indexed is about 2x faster for the large duplicate-heavy case because it
avoids cloning duplicate value slots.
```

Complexity impact:

```text
Materialized shape:
  O(M + U + F) plus cloning one output value per requested slot

Indexed shape:
  O(M + U + F) with one value slot per unique backend key and integer indexes
  for caller-order reconstruction

The Big-O is unchanged, but the duplicate-heavy constant factor is much lower
and the output allocation scales with U values plus M indexes instead of M
values.
```

### 2026-05-14: Lean Indexed Point Benchmark Backend

Change:

```text
Added a second indexed point-read benchmark group:

  storage_v2/point_read_indexed_lean_backend

The existing point-read benchmark backend remains as a shape-checking fixture:
it records requested backend keys and returns fixture entries. The lean backend
is intentionally fixture-shaped and avoids that request recording so the bench
better isolates storage_v2 indexed adapter overhead.
```

Why:

```text
After adding IndexedPointValues, the fake benchmark backend became visible in
the measured path. This split lets us compare:

  shape-checking fake backend
  lean fake backend

before changing backend_v2 or storage_v2 APIs.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2 point_read_indexed_lean_backend
```

Lean indexed point-read scorecard:

| Case                   |      Mean |
| ---------------------- | --------: |
| `m100_u100`            | 2.1063 us |
| `m1000_u1000`          | 23.640 us |
| `m1000_u100`           | 7.4548 us |
| `m10000_u100`          | 53.928 us |
| `m1000_u100_missing10` | 7.2741 us |
| `m1000_u100_missing90` | 6.1794 us |

Interpretation:

```text
The lean backend improves small and unique-heavy indexed point cases, which
confirms the shape-checking fixture has measurable overhead.

For the large duplicate-heavy m10000/u100 case, the result remains around the
same 50us band and varies run-to-run. That suggests the dominant remaining cost
is storage_v2 dedupe/index construction over M requested keys, not fake backend
request recording.
```

### 2026-05-14: Reusable Point Request Plan

Change:

```text
Added a reusable point request plan:

  PointRequestPlan {
    unique_keys: Vec<Key>,
    requested_to_unique: Vec<usize>,
  }

The plan also stores an internal key -> unique-index map for mapping backend
returned entries into unique value slots. Existing one-shot point helpers now
build a plan and execute it, preserving the existing API.

Added planned read helpers:

  get_many_indexed_values_for_plan()
  get_many_indexed_values_for_plan_with_stats()

and a benchmark group:

  storage_v2/point_read_planned_lean_backend
```

Why:

```text
IndexedPointValues removed duplicate value clones, but repeated reads still
rebuilt the same M-key dedupe/index mapping every call. Domain stores often
have stable point-read shapes inside loops or repeated query paths, so the
dedupe/index work can be planned once and reused.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2 point_read_planned_lean_backend
```

Planned lean point-read scorecard:

| Case                   |       Mean |
| ---------------------- | ---------: |
| `m100_u100`            |  1.3645 us |
| `m1000_u1000`          |  14.797 us |
| `m1000_u100`           |  1.4481 us |
| `m10000_u100`          |  2.6940 us |
| `m1000_u100_missing10` |  1.7282 us |
| `m1000_u100_missing90` | 426-646 ns |

Shape comparison:

```text
m10000/u100 one-shot indexed lean:
  about 50-60 us

m10000/u100 planned indexed lean:
  about 2.7-3.1 us

The win comes from removing repeated O(M) dedupe/index construction from the
measured read loop. The per-read path still performs one backend get_many over
U unique keys and fills U unique value slots.
```

Complexity impact:

```text
One-shot indexed read:
  O(M + U + F) per read

Planned indexed read:
  O(M + U) once to build PointRequestPlan
  O(U + F) per read, plus copying/owning the requested_to_unique index vector
  in the current result shape

This keeps backend_v2 unchanged and makes repeated point-read shapes much
cheaper in storage_v2.
```

### 2026-05-14: Full Scorecard After Point Request Plans

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench storage_v2
```

Representative results:

| Group / Case                         |      Mean |
| ------------------------------------ | --------: |
| `write_set/puts_k1024_g16_v32`       |  9.797 us |
| `write_set/puts_k8192_g16_v32`       | 74.027 us |
| `write_set/deletes_k1024_g16`        |  7.327 us |
| `write_set/mixed80_20_k1024_g16_v32` |  9.464 us |
| `point materialized m10000/u100`     | 102.00 us |
| `point indexed m10000/u100`          | 51.751 us |
| `point indexed lean m10000/u100`     | 51.692 us |
| `point planned lean m10000/u100`     |  2.684 us |
| `prefix/q1000`                       |  4.994 us |
| `prefix/q10000`                      | 49.091 us |
| `conformance commit k1024/g16`       | 68.936 us |
| `conformance get_many m1000/u100`    | 29.271 us |
| `conformance scan q1000`             | 15.122 us |

Point-read comparison:

```text
m10000/u100 materialized:
  102.00 us

m10000/u100 indexed:
  51.751 us

m10000/u100 indexed lean:
  51.692 us

m10000/u100 planned lean:
  2.684 us
```

Interpretation:

```text
PointRequestPlan is the largest optimization so far for repeated point-read
shapes. For the duplicate-heavy m10000/u100 case, planned indexed reads are
roughly 19x faster than one-shot indexed reads because they avoid rebuilding
the M-key dedupe/index mapping on every read.

The lean benchmark shows fake backend request recording is not the dominant
cost for the duplicate-heavy one-shot case; storage_v2's dedupe/index
construction over M requested keys is.
```

Tradeoff:

```text
The existing one-shot indexed/materialized helpers now build an owned
PointRequestPlan internally. That preserves behavior and gives the reusable
plan path, but it regresses small or unique-heavy one-shot reads compared with
the earlier borrowed one-shot path.

Example from this scorecard:

  indexed m1000/u1000:
    35.272 us

  planned m1000/u1000:
    23.134 us

Next likely fix:

  add an internal BorrowedPointRequestPlan<'a> for one-shot reads and keep the
  owned PointRequestPlan for reusable reads.
```

### 2026-05-14: Borrowed One-Shot Point Request Plan

Change:

```text
Added an internal one-shot borrowed request plan:

  BorrowedPointRequestPlan<'a>

One-shot point-read helpers now use borrowed key references in their temporary
key -> unique-index map. The owned PointRequestPlan remains the reusable API
for repeated read shapes.
```

Why:

```text
PointRequestPlan gave the intended repeated-read win, but the first
implementation made one-shot helpers build the owned reusable plan internally.
That regressed small and unique-heavy one-shot reads because the temporary path
paid for owned map keys it did not need to keep.

The borrowed one-shot plan restores the cheaper temporary shape while keeping
the reusable owned plan for repeated reads.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2 point_read_indexed_adapter/m1000_u1000
cargo bench -p lix_engine --features storage-benches --bench storage_v2 point_read_indexed_lean_backend/m10000_u100
cargo bench -p lix_engine --features storage-benches --bench storage_v2 point_read_planned_lean_backend/m10000_u100
```

Focused scorecard:

| Case                               |      Mean |
| ---------------------------------- | --------: |
| `indexed_adapter/m1000_u1000`      | 32.295 us |
| `indexed_lean_backend/m10000_u100` | 59.226 us |
| `planned_lean_backend/m10000_u100` |  3.101 us |

Interpretation:

```text
The polluted parallel run made the first focused numbers look much worse than
they were. Sequential reruns show the intended shape:

  one-shot unique-heavy reads are back near the pre-owned-plan band
  one-shot duplicate-heavy reads are back in the ~50-60us band
  planned duplicate-heavy reads remain around ~3us

So the storage_v2 point-read API now has both:

  cheap one-shot borrowed planning
  reusable owned planning for repeated read shapes
```

### 2026-05-14: Borrowed Planned Point Results

Change:

```text
Added BorrowedIndexedPointValues<'a> for planned reads:

  BorrowedIndexedPointValues<'a> {
    unique_values: Vec<Option<ProjectedValue>>,
    requested_to_unique: &'a [usize],
  }

The owned planned API still exists, but now converts from the borrowed result
when ownership is needed. The planned benchmark uses the borrowed result shape.
```

Why:

```text
After PointRequestPlan moved dedupe/index construction out of repeated reads,
the planned hot path still cloned the M-length requested_to_unique vector on
every read:

  requested_to_unique: plan.requested_to_unique.clone()

Borrowing the index slice from the plan removes that per-read O(M) clone.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2 point_read_planned_lean_backend/m10000_u100
cargo bench -p lix_engine --features storage-benches --bench storage_v2 point_read_planned_lean_backend/m1000_u1000
```

Focused scorecard:

| Case                       | Before Mean | After Mean | Criterion Change |
| -------------------------- | ----------: | ---------: | ---------------: |
| `planned_lean/m10000_u100` |    2.684 us |   1.322 us |   48.896% faster |
| `planned_lean/m1000_u1000` |   23.134 us |  15.927 us |   32.581% faster |

Complexity impact:

```text
Before:
  planned read: O(U + F) backend/result work plus O(M) clone of
  requested_to_unique into the owned result

After:
  borrowed planned read: O(U + F) backend/result work, with requested indexes
  borrowed from PointRequestPlan

Owned planned reads still pay the O(M) clone by calling into_owned(), but hot
repeated read loops can use the borrowed result directly.
```

### 2026-05-14: Backend Point Reads Are Requested-Order Slots

Change:

```text
Changed GetManyResult to the slot-shaped core contract:

  values: Vec<Option<ProjectedValue>>

The vector has one slot per key passed to backend get_many. Storage uses those
slots directly for planned point reads and no longer supports a backend-native
found-entry result at the v0 boundary.
```

Why:

```text
Post-borrowed-plan profiles showed the planned point-read hot path was mostly:

  backend get_many over U unique keys
  hash returned ReadEntry keys back into unique indexes
  Bytes clone/drop in fake backend results

The cleaner contract is also the faster contract: callers already know the
requested keys, and storage_v2 can dedupe to U unique keys before calling the
backend when that is useful.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo test -p lix_engine backend_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2 storage_v2/point_read_planned_lean_backend/m10000_u100
cargo bench -p lix_engine --features storage-benches --bench storage_v2 storage_v2/point_read_planned_lean_backend/m1000_u1000
```

Focused scorecard:

| Case                       | Previous Committed Mean | After Mean | Delta vs Committed |
| -------------------------- | ----------------------: | ---------: | -----------------: |
| `planned_lean/m10000_u100` |                1.322 us |   0.344 us |      ~74.0% faster |
| `planned_lean/m1000_u1000` |               15.927 us |   4.065 us |      ~74.5% faster |

Complexity impact:

```text
Old found-entry result:
  planned read: O(U + F) plus returned-entry hash/index mapping

Requested-order slot result:
  planned read: O(U) slot copy/fill after backend get_many

This is now the v0 backend contract. Backends preserve duplicate requested keys
and missing-key slots; storage_v2 handles dedupe/planning above that when it
wants to reduce backend key count.
```

### 2026-05-14: Known-Unique Point Plans

Change:

```text
Added PointRequestPlan::from_unique_keys(Vec<Key>).

This constructor is for domain stores that already know the point batch is
unique. It skips dedupe hashing and builds the identity requested_to_unique
index directly.
```

Why:

```text
After slot-ordered backend get_many, one-shot point-read profiles showed the
remaining storage-side cost was PointRequestPlan construction:

  arbitrary keys: must inspect M keys and dedupe to U keys
  already-unique keys: no semantic need to hash/dedupe

The Big-O lower bound for arbitrary caller-order point reads is still O(M), but
known-unique callers can use the tighter O(U) identity plan path with much lower
constants.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2 storage_v2/point_request_plan
```

Focused scorecard:

| Case                               | Dedupe Plan | Known-Unique Plan |         Delta |
| ---------------------------------- | ----------: | ----------------: | ------------: |
| `point_request_plan/m100_u100`     |    2.037 us |          0.200 us | ~90.2% faster |
| `point_request_plan/m1000_u1000`   |   18.519 us |          1.592 us | ~91.4% faster |
| `point_request_plan/m10000_u10000` |  173.410 us |         16.866 us | ~90.3% faster |

Complexity impact:

```text
PointRequestPlan::new(keys):
  O(M + U), hashes requested keys to dedupe

PointRequestPlan::from_unique_keys(unique_keys):
  O(U), builds identity indexes without dedupe hashing

This does not change arbitrary one-shot point-read complexity. It gives domain
stores an explicit fast path when uniqueness is already guaranteed by their
physical key construction.
```

### 2026-05-14: Identity Point Plan Mapping

Change:

```text
PointRequestPlan now stores requested-to-unique mapping as:

  RequestedToUnique::Identity { len }
  RequestedToUnique::Indexes(Vec<usize>)

Known-unique plans use the implicit identity mapping instead of allocating and
filling a Vec<usize> with 0..U. Borrowed planned results carry the same mapping
as a lightweight RequestedToUniqueRef.
```

Why:

```text
PointRequestPlan::from_unique_keys() is specifically for domain stores that
already know the request keys are unique. In that case, requested slot i maps
to unique slot i by construction, so storing an explicit U-length index vector
is pure overhead.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo test -p lix_engine backend_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 storage_v2/point_request_plan
cargo bench -p lix_engine --features storage-benches --bench storage_v2 storage_v2/point_read_planned_lean_backend
```

Focused point-plan scorecard:

| Case                               | Prior Log Mean | After Mean |
| ---------------------------------- | -------------: | ---------: |
| `point_request_plan/m100_u100`     |       0.200 us |   0.159 us |
| `point_request_plan/m1000_u1000`   |       1.592 us |   1.431 us |
| `point_request_plan/m10000_u10000` |      16.866 us |  16.023 us |

Focused planned-read scorecard:

| Case                                | After Mean |
| ----------------------------------- | ---------: |
| `planned_lean/m100_u100`            |   0.426 us |
| `planned_lean/m1000_u1000`          |   4.406 us |
| `planned_lean/m1000_u100`           |   0.366 us |
| `planned_lean/m10000_u100`          |   0.376 us |
| `planned_lean/m10000_u10000`        |  48.847 us |
| `planned_lean/m1000_u100_missing10` |   0.399 us |
| `planned_lean/m1000_u100_missing90` |   0.102 us |

Interpretation:

```text
The direct plan-construction win is modest because the known-unique plan still
owns and drops U keys; the removed work is the identity index vector.

The API/layout result is still important: the repeated-read hot path now has a
zero-allocation representation for identity requested-to-unique mappings. This
keeps the fast known-unique path honest without changing backend_v2.
```

Complexity impact:

```text
Before:
  from_unique_keys: O(U) to build an explicit 0..U index vector

After:
  from_unique_keys: O(1) mapping construction, plus ownership/drop cost for the
  U keys themselves

Arbitrary PointRequestPlan::new(keys) remains O(M + U) expected because it must
detect duplicates and build explicit indexes when requested slots are not an
identity mapping.
```
