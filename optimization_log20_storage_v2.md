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
