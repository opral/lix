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

### 2026-05-14: New Baseline After Identity Point Plans

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench storage_v2
```

Focused profiles:

```text
target/storage_v2_profiles/new_baseline/planned_m10000_u100.json
target/storage_v2_profiles/new_baseline/planned_m10000_u10000.json
target/storage_v2_profiles/new_baseline/indexed_m10000_u100.json
target/storage_v2_profiles/new_baseline/write_k8192_g16.json
target/storage_v2_profiles/new_baseline/conformance_get_many.json
```

Note:

```text
Criterion filters are substring filters. The planned_m10000_u100 profile also
captured planned_m10000_u10000. The exact u10000 case was profiled separately.
```

#### Scorecard

Write set lowering:

| Case                       |      Mean |
| -------------------------- | --------: |
| `puts_k128_g1_v32`         |  1.103 us |
| `puts_k1024_g1_v32`        |  9.381 us |
| `puts_k1024_g16_v32`       | 14.332 us |
| `puts_k8192_g16_v32`       | 68.675 us |
| `puts_k1024_g64_v32`       | 13.396 us |
| `puts_k4096_g256_v32`      | 42.674 us |
| `deletes_k1024_g16`        |  7.130 us |
| `mixed80_20_k1024_g16_v32` |  9.165 us |
| `puts_k1024_g16_v1024`     |  8.925 us |
| `puts_k1024_g16_v65536`    |  8.924 us |

Point request plans:

| Case                         |       Mean |
| ---------------------------- | ---------: |
| `dedupe/m100_u100`           |   1.348 us |
| `known_unique/m100_u100`     |   0.156 us |
| `dedupe/m1000_u1000`         |  15.219 us |
| `known_unique/m1000_u1000`   |   1.424 us |
| `dedupe/m10000_u10000`       | 158.038 us |
| `known_unique/m10000_u10000` |  16.123 us |

Point reads:

| Group / Case                        |       Mean |
| ----------------------------------- | ---------: |
| `materialized/m10000_u100`          | 109.702 us |
| `materialized/m10000_u10000`        |  66.191 ms |
| `indexed/m10000_u100`               |  60.019 us |
| `indexed/m10000_u10000`             |  70.853 ms |
| `indexed_lean/m10000_u100`          |  45.151 us |
| `indexed_lean/m10000_u10000`        | 157.273 us |
| `planned_lean/m10000_u100`          |   0.433 us |
| `planned_lean/m10000_u10000`        |  42.294 us |
| `planned_lean/m1000_u100_missing90` |   0.095 us |

Prefix and conformance backend:

| Group / Case                        |      Mean |
| ----------------------------------- | --------: |
| `prefix/q0`                         |   62.7 ns |
| `prefix/q1000`                      |  4.820 us |
| `prefix/q10000`                     | 47.365 us |
| `conformance/commit_puts_k1024_g16` | 57.224 us |
| `conformance/get_many_m1000_u100`   | 13.356 us |
| `conformance/scan_range_q1000`      |  8.604 us |

#### Profile Ranking

1. One-shot indexed reads still spend most of their time building temporary
   point plans:

```text
indexed_m10000_u100:
  ~24% ahash hash_one
  ~19% HashMap/HashSet allocation
  ~16% HashMap insert inclusive
  ~8%  Bytes drop
  ~5%  Bytes clone
```

This confirms the earlier first-principles conclusion: arbitrary one-shot point
reads have an O(M) planning lower bound. The correct API answer is to avoid this
path in repeated domain-store loops by using `PointRequestPlan`, and to use
`from_unique_keys` when the domain store already guarantees uniqueness.

2. Planned duplicate-heavy reads are now tiny:

```text
planned_lean/m10000_u100:
  ~0.4 us
```

There is no obvious storage_v2 algorithmic win left in that case. It is mostly
backend-slot fill and benchmark overhead.

3. Planned unique-heavy reads are proportional to U:

```text
planned_lean/m10000_u10000:
  ~42 us

profile:
  ~31% allocation
  ~25% Vec collect/from_iter
  ~22% Bytes clone
  ~19% Bytes drop
```

That is expected for the current fake backend/result shape: U unique requested
keys means U backend slots and U returned values. The next win would have to be
backend/result ownership layout, not storage-side dedupe.

4. Write-set lowering is no longer validation-bound:

```text
write_k8192_g16:
  ~26% StorageWriteSet::stage_put
  ~24% HashMap insert inclusive
  ~11% ahash hash_one
  ~9%  Bytes drop
  ~6%  CountingWrite::put_many
```

The remaining write-side cost is staging and validation/hash work, plus Bytes
clone/drop. There is no longer a tree-factor validation hotspot.

5. Conformance get_many is allocation-heavy:

```text
conformance_get_many:
  ~37% allocation
  ~15% key clone path inclusive
  ~8%  Bytes clone
  ~6%  hash_one
  ~6%  HashMap/HashSet allocation
```

This reinforces that `ConformanceBackend` is a correctness backend. The next
in-memory performance work should be a separate optimized in-memory backend,
not contorting the conformance backend.

#### Interpretation

```text
storage_v2 now has three distinct point-read lanes:

1. arbitrary one-shot:
   O(M + U), still pays planning/hash cost

2. known-unique one-shot plan:
   O(U), skips dedupe hash map and explicit identity index allocation

3. reusable planned reads:
   O(M + U) once, then O(U) per read

The new baseline says the API shape is sound for repeated reads. The remaining
large storage-side concern is that the public materialized/indexed one-shot
helpers route through a general plan path and are not the right hot path for
unique-heavy loops. Domain stores should either use reusable plans or structured
range scans.
```

#### Ranked Next Optimizations

1. Build a production-oriented in-memory backend candidate.

```text
Do not optimize ConformanceBackend as if it were production.
Add a separate in-memory backend with cheap snapshots and BTreeMap::range scans,
then run backend conformance plus the storage_v2 scorecard against it.
```

2. Add a storage/domain-store API guideline:

```text
Repeated point reads must use PointRequestPlan.
Known-unique point reads should use PointRequestPlan::from_unique_keys.
Large structured key families should use range/prefix scans.
```

3. Consider restoring a cheaper one-shot unique helper only if domain usage
shows it matters.

```text
The current one-shot unique-heavy materialized/indexed helper is slow compared
with planned/lean paths. Before adding more API, wait for real domain callsites
or the optimized in-memory backend to prove this matters.
```

### 2026-05-14: InMemoryBackend Profile Lane

Change:

```text
Added storage_v2 bench coverage for the production-oriented backend_v2
InMemoryBackend:

  storage_v2/in_memory_backend/commit_puts_k1024_g16_v32
  storage_v2/in_memory_backend/get_many_m1000_u100
  storage_v2/in_memory_backend/scan_range_q1000
```

Validation:

```sh
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo test -p lix_engine backend_v2 --no-fail-fast
cargo test -p lix_engine storage_v2 --no-fail-fast
```

Focused bench results:

| Case                        |      Mean |
| --------------------------- | --------: |
| `commit_puts_k1024_g16_v32` | 57.483 us |
| `get_many_m1000_u100`       | 14.393 us |
| `scan_range_q1000`          |  7.974 us |

Profiles:

```text
target/storage_v2_profiles/in_memory_backend/commit_puts_k1024_g16.json
target/storage_v2_profiles/in_memory_backend/get_many_m1000_u100.json
target/storage_v2_profiles/in_memory_backend/scan_range_q1000.json
```

Profile ranking:

```text
commit_puts_k1024_g16:
  ~36% BTreeMap::insert exclusive
  ~64% InMemoryWrite::put_many inclusive
  ~14% StorageWriteSet::stage_put inclusive
  ~9%  old map drop / Arc publication cleanup inclusive

get_many_m1000_u100:
  ~41% allocation inclusive
  ~38% HashMap/HashSet allocation inclusive
  ~20% Vec collect/from_iter inclusive
  ~13% hash_one exclusive
  ~6%  Bytes drop
  ~6%  Bytes clone

scan_range_q1000:
  ~43% Key/Bytes clone in upper_bound exclusive
  ~20% Vec growth inclusive
  ~18% ReadEntry drop inclusive
  BTreeMap::range itself is not the visible bottleneck
```

Interpretation:

```text
The InMemoryBackend lane validates the intended split:

  ConformanceBackend = correctness reference
  InMemoryBackend    = performance candidate

For scans, using BTreeMap::range worked: range lookup is not hot. The visible
cost is result materialization and avoidable bound/key cloning.

For commits, BTreeMap insertion is now the honest backend cost. That is useful:
storage_v2 write-set overhead is no longer hiding the backend write path.

For get_many, the storage adapter still dominates this M=1000/U=100
materialized caller-order path. Reusable planned reads remain the right
domain-store API for hot repeated point shapes.
```

Ranked next optimizations:

```text
1. Fix InMemoryBackend scan bound construction:
   avoid cloning the upper/lower bound keys on every scan when possible, or
   store keys in a shape that can use borrowed range bounds.

2. Add an InMemoryBackend planned-point bench:
   measure get_many with PointRequestPlan so backend costs are not obscured by
   one-shot storage planning.

3. Consider a backend map layout keyed by SpaceId -> BTreeMap<Key, Bytes>:
   this could avoid synthetic (next SpaceId, empty key) upper bounds and reduce
   tuple-key cloning/comparison work.
```

### 2026-05-14: InMemoryBackend Planned Point Bench

Change:

```text
Added:

  storage_v2/in_memory_backend/planned_get_many_m1000_u100

This uses PointRequestPlan and the borrowed indexed result shape so the
benchmark isolates backend get_many plus planned storage result handling,
instead of one-shot point planning.
```

Focused result:

| Case                          |     Mean |
| ----------------------------- | -------: |
| `planned_get_many_m1000_u100` | 2.915 us |

Comparison:

```text
in_memory get_many_m1000_u100 one-shot materialized:
  ~14.4 us

in_memory planned_get_many_m1000_u100:
  ~2.9 us
```

Profile:

```text
target/storage_v2_profiles/in_memory_backend/planned_get_many_m1000_u100.json
```

Profile readout:

```text
~94% Vec collect/allocation inclusive
~90% iterator fold inclusive
~4%  Bytes drop
~2%  Bytes clone

The backend lookup itself is not visibly hot in this case. The remaining cost
is mostly allocating/filling the U-slot result vector.
```

Interpretation:

```text
The planned point path removes the one-shot storage planning cost, as intended.
For M=1000/U=100 it is roughly 5x faster than the materialized one-shot path.

The next point-read optimization, if needed, is not another dedupe tweak. It is
result ownership/layout: avoid allocating a fresh Vec<Option<ProjectedValue>>
per read, or let hot domain-store paths consume backend slots in a reusable
buffer.
```

### 2026-05-14: InMemoryBackend Per-Space Map Layout

Change:

```text
Changed InMemoryBackend storage from:

  BTreeMap<(SpaceId, Key), Bytes>

to:

  BTreeMap<SpaceId, BTreeMap<Key, Bytes>>

Scan bounds now operate over the per-space key map, and a follow-up patch uses
borrowed bounds for BTreeMap::range so scan setup does not clone range keys.
```

Why:

```text
The previous scan profile showed BTreeMap::range itself was not hot. The
visible cost was synthetic tuple-bound construction and key/value/result
materialization. Since backend spaces are separate ordered key domains, the
in-memory physical layout should model that directly.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine backend_v2 --no-fail-fast
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 storage_v2/in_memory_backend
```

Focused in-memory scorecard:

| Case                          | Before Mean | After Mean | Criterion Change |
| ----------------------------- | ----------: | ---------: | ---------------: |
| `commit_puts_k1024_g16_v32`   |   57.483 us |  52.620 us |    7.822% faster |
| `get_many_m1000_u100`         |   14.393 us |  13.620 us |    4.999% faster |
| `planned_get_many_m1000_u100` |    2.915 us |   2.680 us |    8.122% faster |
| `scan_range_q1000`            |    7.974 us |   7.628 us |    8.506% faster |

Profile:

```text
target/storage_v2_profiles/in_memory_backend_space_map/scan_range_q1000_borrowed_bounds.json
```

Profile readout:

```text
The old upper_bound key clone hotspot disappeared after borrowed bounds.

The remaining scan profile is mostly:
  bounds/range setup symbol attribution
  ReadEntry Vec allocation/growth
  Key/Bytes clone/drop for emitted rows

BTreeMap::range lookup remains tiny.
```

Interpretation:

```text
The per-space layout is a modest but consistent improvement across commit,
point, planned point, and scan paths. More importantly, it better matches the
backend abstraction: each SpaceId is its own ordered byte-key domain.

The next scan win is result materialization, not range lookup. KeyOnly scans
still emit owned ReadEntry keys, so Q=1000 implies Q key clones and a growing
Vec<ReadEntry>.
```

### 2026-05-14: InMemoryBackend Next Hotspot Profile

Command:

```sh
samply record --save-only --unstable-presymbolicate ...
```

Profiles:

```text
target/storage_v2_profiles/in_memory_backend_next/commit_puts_k1024_g16.json
target/storage_v2_profiles/in_memory_backend_next/planned_get_many_m1000_u100.json
target/storage_v2_profiles/in_memory_backend_next/scan_range_q1000.json
```

Focused results:

| Case                          |      Mean |
| ----------------------------- | --------: |
| `commit_puts_k1024_g16_v32`   | 51.587 us |
| `planned_get_many_m1000_u100` |  2.590 us |
| `scan_range_q1000`            |  7.704 us |

Profile ranking:

```text
commit_puts_k1024_g16:
  ~46% BTreeMap::insert inclusive
  ~58% InMemoryWrite::put_many inclusive
  ~16% StorageWriteSet::stage_put inclusive
  ~11% old map drop / Arc cleanup inclusive

planned_get_many_m1000_u100:
  ~94% BTreeMap::get path inclusive
  ~92% Vec collect/from_iter inclusive
  ~89% BTreeMap search_tree inclusive

scan_range_q1000:
  BTreeMap::range remains tiny
  ~19% Vec growth inclusive
  ~18% ReadEntry Vec drop inclusive
  ~11% Bytes clone
  ~10% Bytes drop
```

Interpretation:

```text
The per-space map layout did its job. The remaining backend costs are now
honest:

  writes: BTreeMap insert
  planned points: BTreeMap get into a freshly allocated result vector
  scans: result materialization, not range lookup
```

Ranked next optimizations:

```text
1. Preallocate scan result Vec capacity.
   InMemoryBackend::scan_range currently starts with Vec::new(). For bounded
   scans, use Vec::with_capacity(min(limit_rows, space_entries.len())).
   This directly targets the visible Vec growth hotspot.

2. Add an in-memory planned unique-heavy bench.
   Current planned get_many is M=1000/U=100. Add m10000/u10000 so we can see
   whether BTreeMap get remains acceptable for unique-heavy point reads.

3. Consider result-buffer reuse only after real domain callsites exist.
   planned_get_many is now mostly fresh Vec<Option<ProjectedValue>> allocation
   plus BTreeMap lookup. A reusable buffer could help, but it widens API.

4. Leave commit path alone for now.
   BTreeMap insertion is the expected backend write cost. Optimizing it would
   mean changing the physical data structure, not polishing storage_v2.
```

### 2026-05-14: Preallocate InMemory Scan Results

Change:

```text
InMemoryBackend::scan_range now creates its ReadEntry vector with:

  Vec::with_capacity(min(limit_rows, space_entries.len()))

instead of starting with Vec::new().
```

Why:

```text
The post-space-map scan profile showed Vec growth as the next concrete hotspot:

  ~19% Vec growth inclusive
  ~18% ReadEntry Vec drop inclusive

The backend already knows the page limit and per-space map length, so the
bounded page can reserve its maximum possible emitted row count.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine backend_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 storage_v2/in_memory_backend/scan_range_q1000
```

Focused result:

| Case               | Before Mean | After Mean | Criterion Change |
| ------------------ | ----------: | ---------: | ---------------: |
| `scan_range_q1000` |    7.704 us |   6.601 us |   14.967% faster |

Interpretation:

```text
This is a clean backend-local win. Range lookup was already cheap; the scan
path now spends less time growing the output vector. Remaining scan cost is
mostly unavoidable owned result materialization: cloning emitted keys and
dropping ReadEntry values after the benchmark consumes the page.
```

### 2026-05-14: Borrowed Scan Visitor Experiment

Change:

```text
Added an experimental InMemoryRead::visit_scan_range API that visits borrowed
keys and borrowed values instead of materializing an owned ScanPage.

For KeyOnly scans, the visitor receives:

  (&Key, None)

so the backend does not clone keys, clone Bytes, allocate ReadEntry rows, or
drop an owned result batch after the scan.
```

Why:

```text
The scan profile after preallocation showed the next dominant cost was owned
result materialization, not range lookup. This experiment tests the harder API
cut directly while leaving the v0 BackendRead trait unchanged.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine backend_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 storage_v2/in_memory_backend/scan_range
```

Focused result:

| Case                              |      Mean |     Throughput |
| --------------------------------- | --------: | -------------: |
| `scan_range_q1000`                |  6.328 us | 158.04 Melem/s |
| `scan_range_visit_key_only_q1000` | 768.59 ns | 1.3011 Gelem/s |

Interpretation:

```text
The visitor path is about 8.2x faster for the key-only scan microbench.

That is a first-principles signal: when a caller only needs to walk ordered
keys, the owned ScanPage API forces avoidable work. The visitor shape should
remain experimental until domain callsites prove they can use it ergonomically,
but it is now the strongest evidence for a future scan extension.
```

### 2026-05-15: Pre-API-Change Scan Visitor Baseline Matrix

Goal:

```text
Before changing backend_v2 core from owned ScanPage scans to visitor-first
range scans, run a focused baseline matrix on the current API plus the
experimental InMemoryRead::visit_scan_range path.
```

Bench harness:

```text
Group:
  storage_v2/scan_visitor_baseline

Backend:
  InMemoryBackend

Measurement:
  sample_size = 10
  warm_up_time = 500ms
  measurement_time = 1s
```

Validation:

```sh
cargo fmt -p lix_engine
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2 storage_v2/scan_visitor_baseline
```

Key-only range scans:

| Case   | Owned ScanPage | Borrowed Visitor | Visitor Speedup |
| ------ | -------------: | ---------------: | --------------: |
| q0     |      42.433 ns |        31.619 ns |           1.34x |
| q1     |      80.406 ns |        48.167 ns |           1.67x |
| q10    |      134.81 ns |        83.350 ns |           1.62x |
| q100   |      789.49 ns |        187.71 ns |           4.21x |
| q1000  |      6.8907 us |        919.73 ns |           7.49x |
| q10000 |      69.444 us |        17.661 us |           3.93x |

Full-value range scans:

| Case         | Owned ScanPage | Borrowed Visitor | Visitor Speedup |
| ------------ | -------------: | ---------------: | --------------: |
| q1000 v32    |      9.1617 us |        1.1463 us |           7.99x |
| q1000 v1024  |      10.271 us |        1.2618 us |           8.14x |
| q1000 v65536 |      10.377 us |        1.2535 us |           8.28x |

Storage materialization from visitor:

| Case                 | Current Comparable Owned | Visitor Collects ScanPage |       Result |
| -------------------- | -----------------------: | ------------------------: | -----------: |
| key-only q1000       |                6.8907 us |                 6.8789 us |       parity |
| full-value q1000 v32 |                9.1617 us |                 14.732 us | slower/noisy |

Limit/page-size scans:

| Case            | Owned ScanPage | Borrowed Visitor | Visitor Speedup |
| --------------- | -------------: | ---------------: | --------------: |
| q1000 limit10   |      371.05 ns |        262.66 ns |           1.41x |
| q1000 limit100  |      1.1977 us |        404.42 ns |           2.96x |
| q1000 limit1000 |      12.051 us |        1.6803 us |           7.17x |

Pagination drain:

| Case          | Owned Drain | Visitor Drain | Visitor Speedup |
| ------------- | ----------: | ------------: | --------------: |
| q1000 page10  |   36.017 us |     31.369 us |           1.15x |
| q1000 page100 |   11.536 us |     7.3646 us |           1.57x |

Interpretation:

```text
The visitor-first scan primitive is strongly justified for streaming/key-walk
callers:

  key-only q1000:       ~7.5x faster
  full-value q1000:     ~8x faster
  limit1000 key-only:   ~7.2x faster

The Big-O remains O(log_B N + Q), but the visitor path removes forced O(Q)
owned ReadEntry allocation/cloning/drop work when callers do not need a
materialized page.

The important regression guard is storage materialization. Visitor -> owned
ScanPage is at parity for key-only q1000, which means storage can preserve the
old ergonomic API without losing much. Full-value materialization was slower
and noisy in this short matrix; before replacing backend_v2::scan_range
entirely, rerun that case with a longer measurement window and inspect whether
the slowdown is closure overhead, allocation behavior, or benchmark noise.
```

Decision pressure:

```text
Change backend_v2 core only if we accept this split:

  backend_v2:
    visit_range as the physical primitive

  storage_v2:
    scan_range / scan_prefix owned-page helpers
    cursor and residual-filter loops
    streaming key/value visitor helpers for domain hot paths

This aligns with the reference systems: physical layers iterate/stream/fill
caller-provided output; higher layers materialize pages or result batches when
needed.
```

### 2026-05-15: Post-API-Change Visitor-First Backend Baseline

Goal:

```text
After changing backend_v2 core from owned scan_range -> ScanPage to
visitor-first visit_range -> ScanResult, establish the new storage_v2 scorecard
baseline.
```

Validation:

```sh
cargo check -p lix_engine --features storage-benches
cargo fmt -p lix_engine
cargo test -p lix_engine backend_v2 --no-fail-fast
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo test -p lix_engine --test backend --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2
```

Note:

```text
Criterion's change column compared against pre-cut history and showed noisy
point-read/write changes unrelated to the scan API. Treat the absolute values
below as the new baseline for future optimization comparisons.
```

Write-set lowering:

| Case                     | New Median |
| ------------------------ | ---------: |
| puts_k128_g1_v32         |  1.0326 us |
| puts_k1024_g1_v32        |  8.7081 us |
| puts_k1024_g16_v32       |  8.6996 us |
| puts_k8192_g16_v32       |  69.651 us |
| puts_k1024_g64_v32       |  10.415 us |
| puts_k4096_g256_v32      |  41.357 us |
| deletes_k1024_g16        |  6.9476 us |
| mixed80_20_k1024_g16_v32 |  8.8084 us |
| puts_k1024_g16_v1024     |  9.0055 us |
| puts_k1024_g16_v65536    |  10.753 us |

Point request planning:

| Case                       | New Median |
| -------------------------- | ---------: |
| dedupe m100_u100           |  1.6450 us |
| known_unique m100_u100     |  193.10 ns |
| dedupe m1000_u1000         |  36.817 us |
| known_unique m1000_u1000   |  2.6042 us |
| dedupe m10000_u10000       |  313.53 us |
| known_unique m10000_u10000 |  28.588 us |

Point read adapters:

| Case                 | Materialized |   Indexed | Indexed Lean | Planned Lean |
| -------------------- | -----------: | --------: | -----------: | -----------: |
| m100_u100            |    21.865 us | 12.260 us |    1.3140 us |    331.68 ns |
| m1000_u1000          |    1.2958 ms | 741.35 us |    13.408 us |    3.8872 us |
| m1000_u100           |    44.120 us | 16.462 us |    5.5666 us |    324.45 ns |
| m10000_u100          |    200.86 us | 55.660 us |    44.587 us |    332.99 ns |
| m10000_u10000        |    69.557 ms | 68.433 ms |    136.79 us |    41.951 us |
| m1000_u100_missing10 |    20.671 us | 16.242 us |    5.7966 us |    303.68 ns |
| m1000_u100_missing90 |    10.605 us | 7.5242 us |    5.2110 us |    91.368 ns |

Prefix scan adapter:

| Case   | New Median |
| ------ | ---------: |
| q0     |  76.760 ns |
| q100   |  642.48 ns |
| q1000  |  6.4085 us |
| q10000 |  69.978 us |

Backend scorecard:

| Case                                      | New Median |
| ----------------------------------------- | ---------: |
| conformance commit_puts_k1024_g16_v32     |  67.628 us |
| conformance get_many_m1000_u100           |  30.016 us |
| conformance scan_range_q1000              |  27.554 us |
| in_memory commit_puts_k1024_g16_v32       |  120.20 us |
| in_memory get_many_m1000_u100             |  32.614 us |
| in_memory planned_get_many_m1000_u100     |  4.4627 us |
| in_memory scan_range_q1000                |  15.110 us |
| in_memory scan_range_visit_key_only_q1000 |  3.2952 us |

Visitor-first scan matrix:

| Case                         | Materialized/Owned Median | Visitor Median | Visitor Speedup |
| ---------------------------- | ------------------------: | -------------: | --------------: |
| key-only q0                  |                 57.244 ns |      36.197 ns |           1.58x |
| key-only q1                  |                 81.119 ns |      52.080 ns |           1.56x |
| key-only q10                 |                 149.34 ns |      87.172 ns |           1.71x |
| key-only q100                |                 735.77 ns |      290.05 ns |           2.54x |
| key-only q1000               |                 7.0825 us |      1.9936 us |           3.55x |
| key-only q10000              |                 65.907 us |      18.804 us |           3.50x |
| full-value q1000 v32         |                 8.6376 us |      2.2986 us |           3.76x |
| full-value q1000 v1024       |                 9.8225 us |      2.4124 us |           4.07x |
| full-value q1000 v65536      |                 8.2023 us |      2.2731 us |           3.61x |
| key-only q1000 limit10       |                 191.37 ns |      179.90 ns |           1.06x |
| key-only q1000 limit100      |                 689.93 ns |      276.56 ns |           2.49x |
| key-only q1000 limit1000     |                 6.7208 us |      1.9792 us |           3.40x |
| drain key-only q1000 page10  |                 21.974 us |      18.655 us |           1.18x |
| drain key-only q1000 page100 |                 7.5217 us |      5.8824 us |           1.28x |

Storage materialization from visitor:

| Case                                   | New Median |
| -------------------------------------- | ---------: |
| visit_materialize_key_only_q1000       |  9.6111 us |
| visit_materialize_full_value_q1000_v32 |  8.7213 us |

Hash reference benches were unchanged by this API cut; see the earlier
hash-selection entry.

Interpretation:

```text
The hard cut preserved the Big-O shape and made visitor scans the physical
backend primitive.

Streaming/key-walk scan callers now have a clear win:
  key-only q1000:     7.0825 us -> 1.9936 us
  full-value q1000:   8.6376 us -> 2.2986 us
  key-only q10000:    65.907 us -> 18.804 us

Storage-owned materialization remains available, but key-only materialization
from visitor is now slower than the pre-cut owned page baseline. That is the
next scan-specific optimization target if materialized scans remain hot.

The point-read numbers should not be interpreted as caused by the scan API
change. They are included so this run can serve as the new baseline for later
point-plan/read tuning.
```

### 2026-05-15: Generic Visitor Recovery

Change:

```text
Replace `visitor: &mut dyn ScanVisitor` in BackendRead::visit_range with a
generic visitor parameter, and split InMemoryBackend's projection branch outside
the row loop.
```

Validation:

```sh
cargo test -p lix_engine backend_v2 --no-fail-fast
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2 storage_v2/scan_visitor_baseline
```

Scan visitor deltas:

| Case                         | dyn visitor baseline | generic visitor |        Delta |
| ---------------------------- | -------------------: | --------------: | -----------: |
| key-only q1000               |            1.9936 us |       1.2278 us | 38.4% faster |
| key-only q10000              |            18.804 us |       17.962 us |  4.5% faster |
| full-value q1000 v32         |            2.2986 us |       1.4873 us | 35.3% faster |
| full-value q1000 v1024       |            2.4124 us |       1.4986 us | 37.9% faster |
| full-value q1000 v65536      |            2.2731 us |       1.4963 us | 34.2% faster |
| key-only q1000 limit1000     |            1.9792 us |       1.1993 us | 39.4% faster |
| drain key-only q1000 page100 |            5.8824 us |       4.1289 us | 29.8% faster |

Materialization deltas:

| Case                                   | dyn visitor baseline | generic visitor |        Delta |
| -------------------------------------- | -------------------: | --------------: | -----------: |
| visit_materialize_key_only_q1000       |            9.6111 us |       6.5535 us | 31.8% faster |
| visit_materialize_full_value_q1000_v32 |            8.7213 us |       8.3095 us |  4.7% faster |

Interpretation:

```text
The lost post-cut scan performance was mostly the per-row dynamic visitor call.
The generic visitor shape keeps the backend API visitor-first while letting hot
loops monomorphize. Splitting projection outside the in-memory row loop removes
one more per-row branch.

The remaining gap to the original experimental visitor path is now much smaller:
  key-only q1000:     919.73 ns pre-cut experiment vs 1.2278 us generic core
  full-value q1000:   1.1463 us pre-cut experiment vs 1.4873 us generic core

That residual gap is likely from the more general `ProjectedValueRef`/Result
visitor contract and benchmark noise, not from forced dynamic dispatch.
```

### 2026-05-15: Generic Visitor Next Hotspot Profile

Profiles:

```text
target/storage_v2_profiles/generic_visitor_next/visit_key_only_q1000_syms.json
target/storage_v2_profiles/generic_visitor_next/visit_materialize_key_only_q1000_syms.json
target/storage_v2_profiles/generic_visitor_next/planned_lean_m10000_u100_syms.json
target/storage_v2_profiles/generic_visitor_next/in_memory_planned_get_many_syms.json
```

Commands:

```sh
samply record --save-only --unstable-presymbolicate -o ... -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  '^storage_v2/scan_visitor_baseline/visit_key_only_q1000$'

samply record --save-only --unstable-presymbolicate -o ... -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  '^storage_v2/scan_visitor_baseline/visit_materialize_key_only_q1000$'

samply record --save-only --unstable-presymbolicate -o ... -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  '^storage_v2/point_read_planned_lean_backend/m10000_u100$'

samply record --save-only --unstable-presymbolicate -o ... -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  '^storage_v2/in_memory_backend/planned_get_many_m1000_u100$'
```

Findings:

```text
Pure streaming scan:
  The remaining executable samples are concentrated in
  InMemoryRead::visit_scan_range / its BTreeMap range loop. There is no longer
  a visible per-row dyn-dispatch hotspot. This is the desired shape.

Materialized key-only scan:
  The hot leaf frames are now Bytes clone/drop, Vec<ReadEntry> construction,
  and the ScanVisitor closure. This means the storage-owned materialization
  path is paying exactly the cost we expect: clone keys into ReadEntry and drop
  the materialized page after the benchmark iteration.

Planned point read, lean fake backend:
  The hot frames are mostly Bytes clone/drop and Vec construction for returned
  value slots. This is a benchmark/backend-return-shape cost, not scan API
  cost.

Planned point read, InMemoryBackend:
  The hot path is InMemoryRead::get_many plus Vec::from_iter and Bytes clone.
  The remaining first-principles question is whether get_many should have a
  borrowed/visitor-style value path analogous to scan, or whether point reads
  should remain owned because storage/domain callers naturally materialize
  point results.
```

Ranked next optimizations:

```text
1. Do not tune pure streaming scan further right now.
   It is down to ordered-map iteration and visitor callback work.

2. If materialized scans are hot, add a storage-owned reusable ScanPage buffer
   or scan collector so repeated scans can reuse Vec allocation and possibly
   clear/drop entries more cheaply.

3. For point reads, consider a borrowed/planned point result path for
   InMemoryBackend:
     get_many_borrowed_for_plan(plan, visitor/indexed output)
   This would avoid cloning Bytes values into owned slots for repeated planned
   reads.

4. Defer deeper in-memory map-layout work until domain-shaped scans exist.
   BTreeMap iteration itself is not the obvious bottleneck in this profile.
```

### 2026-05-15: Storage Scan Buffer Experiment

Change:

```text
Added StorageScanBuffer plus StorageReader::scan_range_into /
scan_prefix_into. The existing owned scan helpers now materialize through the
same collector and then move the buffer's Vec into ScanPage, so the default
owned path does not add another clone.
```

Validation:

```sh
cargo check -p lix_engine --features storage-benches
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  storage_v2/scan_visitor_baseline
samply record --save-only --unstable-presymbolicate \
  -o target/storage_v2_profiles/storage_scan_buffer/storage_buffer_key_only_q1000_syms.json -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  '^storage_v2/scan_visitor_baseline/storage_buffer_key_only_q1000$'
```

Focused benchmark results from the scan visitor group:

| Case                 | Existing materialize | StorageScanBuffer |
| -------------------- | -------------------: | ----------------: |
| key-only q1000       |            15.787 us |         18.497 us |
| full-value q1000 v32 |            25.783 us |         26.875 us |

Interpretation:

```text
The reusable storage buffer does not produce a clear win in this focused run.
It removes repeated Vec allocation, but materialized scans are dominated by
owned ReadEntry construction and Bytes clone/drop. That matches the previous
profile: the cost is not primarily capacity allocation.

The API is still useful as a low-level storage hook for callers that repeatedly
materialize pages and want ownership over scratch memory, but it should not be
treated as the main materialized-scan optimization. The first-principles next
cut would be borrowed scan entries or domain visitors that avoid materializing
ReadEntry at all, but that is a larger semantic/API decision.
```

### 2026-05-15: Storage Visitor Cut

Change:

```text
Added StorageReader::visit_scan_range and visit_scan_prefix. These expose the
backend's borrowed visitor path through storage_v2, preserving StorageSpace and
prefix-to-range lowering while avoiding ScanPage / ReadEntry materialization.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  storage_v2/scan_visitor_baseline
```

Focused benchmark results:

| Case                                   |      Time |
| -------------------------------------- | --------: |
| visit_key_only_q1000                   |  1.499 us |
| storage_visit_key_only_q1000           |  2.998 us |
| visit_materialize_key_only_q1000       | 18.428 us |
| storage_buffer_key_only_q1000          | 15.074 us |
| visit_full_value_q1000_v32             |  2.403 us |
| storage_visit_full_value_q1000_v32     |  2.589 us |
| visit_materialize_full_value_q1000_v32 | 23.906 us |
| storage_buffer_full_value_q1000_v32    | 23.114 us |

Interpretation:

```text
The storage visitor path is the right cut for scan-heavy domain callers that do
not need owned rows. It is 5-6x faster than materializing key-only pages in this
run and about 9x faster than materializing full-value v32 pages.

The storage visitor is not always identical to the raw in-memory inherent
visitor benchmark. The remaining gap is mostly API shape: storage goes through
the backend trait visitor with Result-returning callbacks and storage-space
wrapping, while the raw inherent benchmark is the thinnest in-memory loop.

This confirms the first-principles split:
  - use visit_scan_* for filtering/counting/index walks that can consume rows
    immediately;
  - use scan_* / scan_*_into only when callers need an owned page.
```

### 2026-05-15: Isolated Storage Visitor Gap Profile

Profiles:

```text
target/storage_v2_profiles/storage_visitor_gap/raw_visit_key_only_q1000_syms.json
target/storage_v2_profiles/storage_visitor_gap/storage_visit_key_only_q1000_syms.json
```

Commands:

```sh
samply record --save-only --unstable-presymbolicate \
  -o target/storage_v2_profiles/storage_visitor_gap/raw_visit_key_only_q1000_syms.json -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  '^storage_v2/scan_visitor_baseline/visit_key_only_q1000$'

samply record --save-only --unstable-presymbolicate \
  -o target/storage_v2_profiles/storage_visitor_gap/storage_visit_key_only_q1000_syms.json -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  '^storage_v2/scan_visitor_baseline/storage_visit_key_only_q1000$'
```

Isolated benchmark results:

| Case                         |      Time |
| ---------------------------- | --------: |
| raw visit_key_only_q1000     | 818.93 ns |
| storage_visit_key_only_q1000 | 793.02 ns |

Interpretation:

```text
The earlier group-run result showed storage_visit_key_only_q1000 around 2.998
us versus raw visit_key_only_q1000 around 1.499 us. The isolated samply runs do
not reproduce that gap. Both paths are effectively the same order and the
storage wrapper is not visible as a first-principles hotspot.

Conclusion: do not optimize the storage visitor wrapper right now. Treat the
previous gap as benchmark noise/interference from the larger scan group. The
next useful profiling target should be point-read planning/output or write-set
lowering, not storage scan wrapper overhead.
```

### 2026-05-15: Point Read Next Profile

Profiles:

```text
target/storage_v2_profiles/point_read_next/planned_lean_m10000_u100_syms.json
target/storage_v2_profiles/point_read_next/planned_lean_m10000_u10000_syms.json
target/storage_v2_profiles/point_read_next/in_memory_planned_get_many_m1000_u100_syms.json
target/storage_v2_profiles/point_read_next/direct_planned_lean_m10000_u10000_syms.json
target/storage_v2_profiles/point_read_next/direct_in_memory_planned_m1000_u100_syms.json
```

Commands:

```sh
samply record --save-only --unstable-presymbolicate \
  -o target/storage_v2_profiles/point_read_next/planned_lean_m10000_u100_syms.json -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  '^storage_v2/point_read_planned_lean_backend/m10000_u100$'

samply record --save-only --unstable-presymbolicate \
  -o target/storage_v2_profiles/point_read_next/planned_lean_m10000_u10000_syms.json -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  '^storage_v2/point_read_planned_lean_backend/m10000_u10000$'

samply record --save-only --unstable-presymbolicate \
  -o target/storage_v2_profiles/point_read_next/in_memory_planned_get_many_m1000_u100_syms.json -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  '^storage_v2/in_memory_backend/planned_get_many_m1000_u100$'
```

Timing results:

| Case                                      |      Time |
| ----------------------------------------- | --------: |
| planned lean m10000 u100                  | 340.98 ns |
| planned lean m10000 u10000                | 40.510 us |
| in-memory planned get_many m1000 u100     |  2.742 us |
| direct planned lean m10000 u10000 profile | 41.881 us |
| direct in-memory planned m1000 u100       |  2.638 us |

Interpretation:

```text
The duplicate-heavy planned path is already extremely cheap once the
PointRequestPlan exists: m=10000/u=100 completes in ~341 ns against the lean
backend. That means the repeated-read API shape is doing its job for high
duplicate ratios.

The all-unique lean path is linear in U: m=10000/u=10000 takes ~40-42 us. That
is the expected O(U) output/write-slot cost, not a planning cost.

The real in-memory planned get_many m1000/u100 case takes ~2.6-2.7 us. Since the
lean backend m10000/u100 case is sub-microsecond, most of the real in-memory
case is backend lookup plus cloning returned Bytes values, not storage
requested-to-unique reconstruction.
```

Next optimization implication:

```text
Do not add more point-read planning APIs yet. The plan reuse path is already
fast. If point reads are the next target, the first-principles cut is inside the
in-memory backend/result ownership shape:

  - a borrowed point visitor/result path for in-memory reads, or
  - a domain-facing API that consumes point values without cloning them.

This mirrors the scan result: materialization/ownership dominates once the
storage adapter shape is lean.
```

### 2026-05-15: Point Visitor Cut

Change:

```text
Added BackendRead::visit_many with a default owned implementation and an
InMemoryRead override that visits borrowed point values directly. Added
StorageReader::visit_unique_point_values_for_plan for repeated planned reads
that can consume one value per unique backend key instead of materializing an
IndexedPointValues result.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches
cargo test -p lix_engine backend_v2 --no-fail-fast
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
```

Focused benchmark results:

| Case                         | Owned/result path | Unique visitor |       Delta |
| ---------------------------- | ----------------: | -------------: | ----------: |
| planned lean m10000/u100     |          1.345 us |       304.6 ns | 4.4x faster |
| planned lean m10000/u10000   |         175.97 us |       31.01 us | 5.7x faster |
| in-memory planned m1000/u100 |          7.069 us |       5.383 us | 1.3x faster |

Interpretation:

```text
The visitor cut is a large win for the lean backend because it removes owned
ProjectedValue vector construction. For the real in-memory backend the win is
smaller because BTreeMap lookup and Arc/BTree snapshot layout dominate more of
the cost than owned value cloning.

This says the API cut is directionally useful, but the next in-memory-specific
optimization is probably not another storage point adapter. It is more likely
an in-memory map/layout question:
  - reduce BTree lookup overhead for point batches,
  - specialize point lookups by space,
  - or use a domain-shaped/in-memory index once real domain access patterns are
    known.
```

### 2026-05-15: Next High-Cut Profile

Profiles:

```text
target/storage_v2_profiles/next_high_cut/in_memory_planned_visit_unique_m1000_u100_syms.json
target/storage_v2_profiles/next_high_cut/write_puts_k8192_g16_v32_syms.json
target/storage_v2_profiles/next_high_cut/write_puts_k4096_g256_v32_syms.json
```

Commands:

```sh
samply record --save-only --unstable-presymbolicate \
  -o target/storage_v2_profiles/next_high_cut/in_memory_planned_visit_unique_m1000_u100_syms.json -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  '^storage_v2/in_memory_backend/planned_visit_unique_m1000_u100$'

samply record --save-only --unstable-presymbolicate \
  -o target/storage_v2_profiles/next_high_cut/write_puts_k8192_g16_v32_syms.json -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  '^storage_v2/write_set_lowering/puts_k8192_g16_v32$'

samply record --save-only --unstable-presymbolicate \
  -o target/storage_v2_profiles/next_high_cut/write_puts_k4096_g256_v32_syms.json -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  '^storage_v2/write_set_lowering/puts_k4096_g256_v32$'
```

Timing results:

| Case                               |      Time |
| ---------------------------------- | --------: |
| in-memory planned visit m1000/u100 |  6.303 us |
| write set puts k8192/g16/v32       | 232.60 us |
| write set puts k4096/g256/v32      | 170.27 us |

Interpretation:

```text
The next high-cut area is write-set commit/lowering, not point reads.

Point reads are now single-digit microseconds in the in-memory backend. Write
set lowering is two orders of magnitude larger in the storage-layer scorecard,
even with a CountingBackend that only counts put_many calls.

The current write path still performs commit-time O(K) duplicate validation by
building a HashSet over all staged mutations, then performs another O(K) pass
to compute stats/written_bytes during lower_validated_into. Those checks are
correct but they are not structurally necessary at commit time if StorageWriteSet
becomes a canonical final-mutation set while staging.
```

Ranked next optimization:

```text
1. Move duplicate detection and write stats into staging.

   Today:
     stage_put/stage_delete: append to groups
     commit: validate O(K) with HashSet
     lower: recompute staged counts and written bytes O(K)

   Proposed:
     StorageWriteSet owns a mutation index while staging:
       seen: HashSet<(SpaceId, Key)> or per-group HashSet<Key>
       stats: StorageWriteSetStats

     try_stage_put/try_stage_delete detects duplicates immediately and updates
     stats once. commit then only checks conflicting space declarations and
     lowers groups.

   Expected shape:
     staging remains O(1) expected per mutation
     commit/lower removes one or two O(K) storage-side passes
     backend calls remain O(G)

2. Keep the existing infallible stage_* methods as convenience wrappers only if
   we want compatibility, but the hardened path should be fallible:
     try_stage_put(...) -> Result<(), StorageWriteSetError>
     try_stage_delete(...) -> Result<(), StorageWriteSetError>

3. After that, rerun write_set_lowering and only then inspect in-memory point
   map layout. Point read layout is no longer the largest removable cost.
```

## 2026-05-15: staged write-set duplicate detection

Change:

```text
StorageWriteSet now records duplicate mutations and write stats while staging.

Before:
  stage_put/stage_delete appended to groups
  commit/lower validated duplicates with a fresh HashSet over K mutations
  lower recomputed staged counts and written bytes

After:
  stage_put/stage_delete keep compatibility by recording duplicate errors
  try_stage_put/try_stage_delete expose immediate duplicate rejection
  commit/lower reuse staged stats and only add batch/backend-call counters
```

Focused scorecard:

```sh
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/write_set_lowering/(puts_k8192_g16_v32|puts_k4096_g256_v32)'
```

| Case                    |    Before | After median | Criterion change |
| ----------------------- | --------: | -----------: | ---------------: |
| puts k8192 / g16 / v32  | 232.60 us |    188.54 us |          -37.56% |
| puts k4096 / g256 / v32 | 170.27 us |     84.95 us |          -51.88% |

Notes:

```text
The high-G case benefits most because the old lowering still paid the full
duplicate-validation and stats recomputation cost before issuing many small
grouped put_many calls. The new shape moves those costs to staging, which is
where storage already pays O(1) expected work per mutation to build the final
mutation set.
```

Follow-up profile:

```text
After duplicate validation moved out of commit, the remaining write-set frames
were mostly group lookup during staging (`StorageWriteSet::group_mut`), mutation
hash entry work, hash-table rehashing, and benchmark value clone/drop.
```

Follow-up cut:

```text
Replace BTreeMap<SpaceId, StorageWriteGroup> with:
  Vec<StorageWriteGroup>
  HashMap<SpaceId, usize>

The write set does not require sorted group order. The hot operation is finding
the append bucket for each staged mutation, so a direct group index is a better
fit than a tree map.
```

Focused scorecard after Vec+index groups:

| Case                    | Previous median | After median | Criterion note             |
| ----------------------- | --------------: | -----------: | -------------------------- |
| puts k8192 / g16 / v32  |       188.54 us |    175.78 us | noisy/no clear change      |
| puts k4096 / g256 / v32 |        84.95 us |     69.88 us | -52.65% vs stored baseline |

Interpretation:

```text
The Vec+index group layout helps the many-space case, which matches the profile:
it removes per-mutation BTreeMap lookup cost when G is large. The low-G case is
mostly neutral/noisy because group lookup was already cheap relative to moving
the staged entries and benchmark value clone/drop.
```

Capacity experiment:

```text
StorageWriteSet::with_capacity(expected_mutations, expected_spaces) was added so
callers with known write-set shape can preallocate group and mutation indexes.
Using it directly in the current write_set_lowering scorecard did not produce a
stable win, so the scorecard continues to use StorageContext::new_write_set().

Reason: this benchmark measures staging plus lowering, so eager allocation must
pay for itself immediately. At the current K/G sizes the result was noisy and
sometimes slower. The API is still useful for future domain stores that already
know their mutation count, but it is not treated as a baseline speedup yet.
```

Final focused rerun after keeping normal benchmark staging:

| Case                    |    Median | Criterion note              |
| ----------------------- | --------: | --------------------------- |
| puts k8192 / g16 / v32  | 143.43 us | improved vs stored baseline |
| puts k4096 / g256 / v32 | 132.19 us | noisy/no clear change       |

## 2026-05-15: storage_v2 smoke bench mode and in-memory backend cut

Change:

```text
Added STORAGE_V2_BENCH_SMOKE=1 for storage_v2 benches.

Smoke mode keeps the same benchmark names and assertions but uses:
  warmup:       100 ms
  measurement: 250 ms
  samples:      10

This is intentionally noisy. It is for deciding what to inspect next, not for
claiming final deltas.
```

Smoke command:

```sh
STORAGE_V2_BENCH_SMOKE=1 cargo bench -p lix_engine \
  --features storage-benches --bench storage_v2 \
  '^storage_v2/in_memory_backend/(commit_puts_k1024_g16_v32|commit_puts_k128_g16_existing10k_untouched_v32|commit_puts_k128_g16_existing10k_touched_v32|planned_visit_unique_m1000_u100|scan_range_q1000|scan_range_visit_key_only_q1000)$'
```

Smoke scorecard:

| Case                                      | Smoke median |
| ----------------------------------------- | -----------: |
| commit puts k1024/g16 into empty backend  |    262.42 us |
| commit puts k128/g16 into existing 10k    |      2.55 ms |
| planned visit unique m1000/u100           |      4.19 us |
| materialized key-only scan q1000          |     15.80 us |
| borrowed key-only scan q1000              |      2.01 us |

Interpretation:

```text
The remaining storage + in-memory backend bottleneck is write snapshot/update
shape, not read adapters.

The new existing-data write smoke case exposes the current InMemoryBackend
begin_write cost:
  begin_write clones the whole InMemoryMap snapshot
  small writes into a large existing backend pay O(N_total) before applying K

That is the wrong shape for a production-oriented in-memory backend. The next
first-principles cut is copy-on-write by space:

  current:
    Arc<BTreeMap<SpaceId, BTreeMap<Key, Bytes>>>
    begin_write clones all spaces and all entries

  proposed:
    Arc<BTreeMap<SpaceId, Arc<BTreeMap<Key, Bytes>>>>
    begin_write clones only the outer space map
    first write to a touched space clones that space map
    commit publishes a new outer Arc

Expected shape:
  begin_write: O(number_of_spaces)
  write to touched spaces: O(entries_in_touched_spaces + K log N_space)
  untouched spaces: O(1) Arc clone, not O(entries)

Point visitor and borrowed scan are already small enough for now. Materialized
scan remains intentionally more expensive because it owns cloned ReadEntry keys.
```

Change applied:

```text
InMemoryBackend now stores each space as an independently shared ordered map:
  Arc<BTreeMap<SpaceId, Arc<BTreeMap<Key, Bytes>>>>

begin_write clones the outer space map only. put_many/delete_many call
Arc::make_mut for touched spaces. This preserves coherent read snapshots while
avoiding an O(N_total) clone for writes that do not touch existing large spaces.

A bench-only fork_snapshot() helper lets smoke benches start each iteration from
the same preseeded immutable snapshot without reseeding 10k rows inside the
timed loop.
```

Post-change smoke scorecard:

| Case                                         | Smoke median |
| -------------------------------------------- | -----------: |
| commit puts k1024/g16 into empty backend     |    127.12 us |
| commit puts k128/g16 with existing 10k untouched | 11.56 us |
| commit puts k128/g16 with existing 10k touched   | 346.97 us |
| planned visit unique m1000/u100              |      8.43 us |
| materialized key-only scan q1000             |     23.17 us |
| borrowed key-only scan q1000                 |      3.19 us |

Interpretation:

```text
The COW-by-space cut worked for the actual target:
  existing 10k untouched: ~2.55 ms -> ~11.6 us

The touched-space case remains much slower because the first write to that space
must clone its BTreeMap:
  existing 10k touched: ~347 us

So the remaining in-memory backend write bottleneck is no longer global snapshot
copying. It is per-touched-space BTreeMap clone/update. The next harder cut, if
needed before real backends, is a persistent/overlay per-space write map:

  base Arc<BTreeMap<Key, Bytes>>
  staged puts/deletes in mutable overlay
  commit materializes or publishes a layered space map

That would avoid O(entries_in_touched_space) for small writes into large spaces,
but it is a larger semantic/layout change. The current COW-by-space layout is
good enough to move on unless domain-shaped workloads prove touched-space small
writes dominate.
```

## 2026-05-15: in-memory per-space overlay writes

Change:

```text
InMemoryBackend now supports committed overlay layers per space.

Space state:
  Empty
  Flat(BTreeMap<Key, Bytes>)
  Layered {
    base: Arc<SpaceState>,
    puts: BTreeMap<Key, Bytes>,
    deletes: BTreeSet<Key>,
  }

Writes stage per-space overlays. commit publishes touched spaces as overlay
layers instead of cloning the touched space BTreeMap. Point reads walk the layer
chain. Range scans keep a direct fast path for Flat spaces and merge layered
spaces into a sorted temporary view.
```

Expected shape:

```text
small write into untouched existing data:
  O(number_of_spaces + K log K)

small write into large touched space:
  before: O(entries_in_touched_space + K log N)
  after:  O(K log K) to publish overlay

flat scan:
  same direct BTreeMap::range fast path

layered scan:
  merge base/puts/deletes into sorted visible rows
```

Smoke command:

```sh
STORAGE_V2_BENCH_SMOKE=1 cargo bench -p lix_engine \
  --features storage-benches --bench storage_v2 \
  '^storage_v2/in_memory_backend/(commit_puts_k1024_g16_v32|commit_puts_k128_g16_existing10k_untouched_v32|commit_puts_k128_g16_existing10k_touched_v32|planned_visit_unique_m1000_u100|scan_range_q1000|scan_range_visit_key_only_q1000)$'
```

Post-overlay smoke scorecard:

| Case                                             | Smoke median |
| ------------------------------------------------ | -----------: |
| commit puts k1024/g16 into empty backend         |    124.62 us |
| commit puts k128/g16 with existing 10k untouched |     12.48 us |
| commit puts k128/g16 with existing 10k touched   |     10.34 us |
| planned visit unique m1000/u100                  |      7.35 us |
| materialized key-only scan q1000                 |     16.42 us |
| borrowed key-only scan q1000                     |      3.77 us |

Interpretation:

```text
The overlay cut fixed the touched-space small-write case:
  COW-by-space touched 10k: ~346.97 us
  overlay touched 10k:      ~10.34 us

The flat scan fast path kept read-side smoke in the same band. The remaining
thing to watch is layered scan depth. If many commits stack overlays on the same
space, point reads walk layers and layered scans perform a merge over each
layer. A later compaction policy may be useful:

  compact a space after N overlay layers
  or compact when overlay rows / base rows crosses a threshold

For now this is the right in-memory backend shape for small writes into large
spaces without penalizing flat read snapshots.
```
