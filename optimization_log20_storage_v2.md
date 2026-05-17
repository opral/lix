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

The backend already knows the chunk limit and per-space map length, so the
bounded chunk can reserve its maximum possible emitted row count.
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
dropping ReadEntry values after the benchmark consumes the chunk.
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

Limit/chunk-size scans:

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
materialized chunk.

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
    scan_range / scan_prefix owned-chunk helpers
    cursor and residual-filter loops
    streaming key/value visitor helpers for domain hot paths

This aligns with the reference systems: physical layers iterate/stream/fill
caller-provided output; higher layers materialize chunks or result batches when
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
from visitor is now slower than the pre-cut owned chunk baseline. That is the
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
  the materialized chunk after the benchmark iteration.

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
materialize chunks and want ownership over scratch memory, but it should not be
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
not need owned rows. It is 5-6x faster than materializing key-only chunks in this
run and about 9x faster than materializing full-value v32 chunks.

The storage visitor is not always identical to the raw in-memory inherent
visitor benchmark. The remaining gap is mostly API shape: storage goes through
the backend trait visitor with Result-returning callbacks and storage-space
wrapping, while the raw inherent benchmark is the thinnest in-memory loop.

This confirms the first-principles split:
  - use visit_scan_* for filtering/counting/index walks that can consume rows
    immediately;
  - use scan_* / scan_*_into only when callers need an owned chunk.
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

| Case                                     | Smoke median |
| ---------------------------------------- | -----------: |
| commit puts k1024/g16 into empty backend |    262.42 us |
| commit puts k128/g16 into existing 10k   |      2.55 ms |
| planned visit unique m1000/u100          |      4.19 us |
| materialized key-only scan q1000         |     15.80 us |
| borrowed key-only scan q1000             |      2.01 us |

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

| Case                                             | Smoke median |
| ------------------------------------------------ | -----------: |
| commit puts k1024/g16 into empty backend         |    127.12 us |
| commit puts k128/g16 with existing 10k untouched |     11.56 us |
| commit puts k128/g16 with existing 10k touched   |    346.97 us |
| planned visit unique m1000/u100                  |      8.43 us |
| materialized key-only scan q1000                 |     23.17 us |
| borrowed key-only scan q1000                     |      3.19 us |

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

## 2026-05-15: overlay-depth smoke benches and scan fast path

Change:

```text
Added in-memory backend smoke benches for:
  direct backend write commit, bypassing StorageWriteSet
  overlay-depth point reads at d0/d1/d8/d32
  overlay-depth scans at d0/d1/d8/d32

These separate backend write mechanics from storage staging and make overlay
depth visible before adding compaction policy.
```

Initial finding:

```text
Layered point reads degraded gradually with depth, as expected.

Layered scans were bad immediately when depth >= 1, even when overlay keys were
outside the scanned range:
  d0 scan: ~3.44 us
  d1 scan: ~237 us
  d8 scan: ~298 us
  d32 scan: ~273 us

That exposed a first-principles bug in the overlay scan path: it merged the base
space into a temporary BTreeMap even when no overlay row could affect the query
range.
```

Change:

```text
Layered range scans now check whether the current layer has puts/deletes inside
the requested range. If not, scan delegates directly to the base layer. Flat
spaces still use the direct BTreeMap::range path.
```

Post-change smoke scorecard:

| Case                         | Smoke median |
| ---------------------------- | -----------: |
| direct commit k1024/g16      |     78.96 us |
| direct touched 10k k128/g16  |      6.94 us |
| overlay point d0 m1000/u100  |      3.99 us |
| overlay point d1 m1000/u100  |      5.19 us |
| overlay point d8 m1000/u100  |      8.30 us |
| overlay point d32 m1000/u100 |     23.31 us |
| overlay scan d0 q1000        |      1.44 us |
| overlay scan d1 q1000        |      2.18 us |
| overlay scan d8 q1000        |      3.17 us |
| overlay scan d32 q1000       |      2.60 us |

Interpretation:

```text
The scan fast path removed the immediate layered-scan cliff without needing
compaction. Overlay point reads still scale with depth because they walk each
layer until they find the key or reach the base.

Do not add compaction yet. The benchmark now tells us the actual threshold
question:
  if domain workloads stack many overlays on one hot point-read space, compact
  after some depth;
  otherwise keep the overlay design simple and move to real backends.

The direct write bench gives a cleaner backend-only floor:
  direct k1024/g16: ~79 us
Compared with storage commit smoke, this says the remaining storage overhead is
mostly write-set staging and grouped lowering, which is expected and already
shape-guarded.
```

## 2026-05-15: in-memory final profile and compaction decision

Focused smoke lanes:

| Case                         | Smoke range |
| ---------------------------- | ----------: |
| direct commit k1024/g16      |   77-181 us |
| storage commit k1024/g16     |  100-524 us |
| overlay point d32 m1000/u100 |    29-58 us |
| overlay scan d32 q1000       |  2.9-5.1 us |

Profiles:

```text
target/storage_v2_profiles/in_memory_final/direct_commit_k1024_g16_syms.json
target/storage_v2_profiles/in_memory_final/storage_commit_k1024_g16_syms.json
target/storage_v2_profiles/in_memory_final/overlay_point_d32_syms.json
target/storage_v2_profiles/in_memory_final/overlay_scan_d32_syms.json
```

Profile read:

```text
direct commit:
  dominated by BTreeMap::insert in InMemoryWrite::put_many, plus remaining
  bench setup noise from seeded_in_memory_backend_with_value_size and
  write_mutations.

storage commit:
  adds StorageWriteSet::stage_put / try_stage_put and commit/lowering overhead
  above the same backend BTreeMap::insert floor.

overlay point d32:
  dominated by InMemoryRead::visit_many -> SpaceState::get recursion and
  memcmp/key comparison. This is the expected layered lookup cost.

overlay scan d32:
  dominated by visit_scan_range -> InMemoryRead::visit_range ->
  visit_space_range. The expensive merge path is no longer visible for the
  base-range case; the fast path now delegates through empty overlay ranges.
```

Decision:

```text
Do not add compaction yet.

Compaction would improve the d32 point-read lane by reducing layer walking, but
it would add write-time flattening policy and make snapshot layout more complex.
The scan lane is already clean, and the point-read cost is only meaningful for
workloads that stack many tiny commits on one hot point-read space.

Next step: keep compaction as a measured policy hook and move to real backend
workloads. If SQLite/RocksDB/redb or domain-shaped traces show deep hot overlays,
add a policy such as "compact after N layers" or "compact when overlay rows /
base rows crosses a threshold."
```

## 2026-05-15: backend matrix scaffold

Change:

```text
Added a reusable storage backend benchmark family scaffold:

  StorageBenchBackend
    name()
    open_empty()
    seed_points()
    fork_for_write()

The first backend family is in_memory. SQLite, redb, and RocksDB can now plug
into the same storage-level lanes without duplicating benchmark logic.
```

Matrix lanes:

```text
writes:
  commit_puts_k1024_g16_v32
  mixed80_20_k1024_g16_v32
  commit_puts_k128_g16_existing10k_touched_v32

point reads:
  planned_visit_unique_m1000_u100
  planned_get_many_m1000_u100

scans:
  scan_range_visit_key_only_q1000
  scan_range_q1000
  prefix_scan_q1000
```

Validation:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run

STORAGE_V2_BENCH_SMOKE=1 cargo bench -p lix_engine --features storage-benches \
  --bench storage_v2 \
  '^storage_v2/backend_matrix/in_memory/(commit_puts_k1024_g16_v32|mixed80_20_k1024_g16_v32|commit_puts_k128_g16_existing10k_touched_v32|planned_visit_unique_m1000_u100|planned_get_many_m1000_u100|scan_range_visit_key_only_q1000|scan_range_q1000|prefix_scan_q1000)$'
```

Smoke baseline:

| Case                             | Smoke range |
| -------------------------------- | ----------: |
| commit puts k1024/g16            |  114-354 us |
| mixed 80/20 k1024/g16            |   87-116 us |
| commit touched existing k128/g16 | 8.9-10.9 us |
| planned visit unique m1000/u100  |  4.6-4.8 us |
| planned get many m1000/u100      |  3.7-4.3 us |
| scan range visit key-only q1000  |  2.0-3.9 us |
| scan range materialized q1000    |    21-24 us |
| prefix scan materialized q1000   |    12-20 us |

Interpretation:

```text
The matrix scaffold is ready for real backends. The in-memory smoke numbers are
in the same shape as the specialized in-memory benches, so the generic harness
does not appear to distort the measurement target.

The next backend additions should implement only the StorageBenchBackend fixture
operations first, then use this exact lane set for SQLite temp-file, redb
temp-file, and RocksDB temp-dir comparisons.
```

## 2026-05-15: SQLite temp backend matrix fixture

Change:

```text
Added sqlite_temp as the first real backend family in the storage_v2 backend
matrix.

Fixture shape:
  open_empty() creates a fresh temp-file SQLite backend
  seed_points() seeds a temp-file backend and checkpoints WAL
  fork_for_write() checkpoints the seed and copies the SQLite file to a fresh
    temp path for mutation benchmarks

The bench fixture reuses the backend-v2 SQLite implementation from integration
test support, so conformance and perf exercise the same SQLite physical
contract.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches
cargo test -p lix_engine --test backend --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run

STORAGE_V2_BENCH_SMOKE=1 cargo bench -p lix_engine --features storage-benches \
  --bench storage_v2 \
  '^storage_v2/backend_matrix/sqlite_temp/(commit_puts_k1024_g16_v32|mixed80_20_k1024_g16_v32|commit_puts_k128_g16_existing10k_touched_v32|planned_visit_unique_m1000_u100|planned_get_many_m1000_u100|scan_range_visit_key_only_q1000|scan_range_q1000|prefix_scan_q1000)$'
```

Smoke baseline:

| Case                             | Smoke range |
| -------------------------------- | ----------: |
| commit puts k1024/g16            | 2.7-10.0 ms |
| mixed 80/20 k1024/g16            |  2.6-5.4 ms |
| commit touched existing k128/g16 |  2.8-7.3 ms |
| planned visit unique m1000/u100  |  286-658 us |
| planned get many m1000/u100      |  275-505 us |
| scan range visit key-only q1000  |  220-385 us |
| scan range materialized q1000    |  216-642 us |
| prefix scan materialized q1000   |  222-800 us |

Interpretation:

```text
The SQLite fixture lifecycle works and passes backend-v2 conformance. The write
lanes are noisy because each measured iteration uses a temp-file write
transaction and, for seeded-write cases, a copied seed file. That is intentional
for now: it keeps iterations isolated and prevents writes from accumulating in
the shared seed.

Next SQLite optimization questions are backend-specific:
  - put_many/delete_many currently execute one SQLite statement per row inside
    one transaction;
  - point get_many builds a VALUES list and returns caller-order values through
    storage;
  - scans use ordered BLOB range predicates.

Before optimizing SQLite, add redb/rocksdb to the same fixture contract so the
matrix can show which costs are SQLite-specific versus generic real-backend
costs.
```

## 2026-05-15: redb temp backend matrix fixture

Change:

```text
Added redb_temp as the second real backend family in the storage_v2 backend
matrix.

Fixture shape:
  open_empty() creates a fresh temp-file redb backend
  seed_points() seeds a temp-file backend
  fork_for_write() copies the seeded redb file to a fresh temp path for isolated
    mutation benchmarks

The redb backend is a backend-v2 support implementation with one ordered table:
  encoded key = big-endian SpaceId || raw backend key
  value       = raw stored value bytes
```

Validation:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches
cargo test -p lix_engine --test backend --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run

STORAGE_V2_BENCH_SMOKE=1 cargo bench -p lix_engine --features storage-benches \
  --bench storage_v2 \
  '^storage_v2/backend_matrix/redb_temp/(commit_puts_k1024_g16_v32|mixed80_20_k1024_g16_v32|commit_puts_k128_g16_existing10k_touched_v32|planned_visit_unique_m1000_u100|planned_get_many_m1000_u100|scan_range_visit_key_only_q1000|scan_range_q1000|prefix_scan_q1000)$'
```

Smoke baseline:

| Case                             | Smoke range |
| -------------------------------- | ----------: |
| commit puts k1024/g16            |    20-27 ms |
| mixed 80/20 k1024/g16            |    23-26 ms |
| commit touched existing k128/g16 |    20-27 ms |
| planned visit unique m1000/u100  |   38-134 us |
| planned get many m1000/u100      |    48-72 us |
| scan range visit key-only q1000  |  141-302 us |
| scan range materialized q1000    |  313-518 us |
| prefix scan materialized q1000   |  223-429 us |

Interpretation:

```text
redb passes backend-v2 conformance and fits the matrix fixture contract.

The first smoke numbers show a sharp split:
  - redb point reads are much faster than SQLite in this simple fixture;
  - redb writes are much slower than SQLite, likely dominated by redb commit /
    durability and file-copy isolated setup rather than storage_v2 shape.

Do not optimize redb yet. Add RocksDB to the same matrix first, then profile the
slowest write lane and fastest point/scan lanes across all real backends.
```

## 2026-05-15: RocksDB temp backend matrix fixture

Change:

```text
Added rocksdb_temp as the third real backend family in the storage_v2 backend
matrix.

Fixture shape:
  open_empty() creates a fresh temp-dir RocksDB backend
  seed_points() seeds and flushes the backend
  fork_for_write() flushes the seed and recursively copies the RocksDB dir to a
    fresh temp path for isolated mutation benchmarks

The RocksDB backend-v2 support implementation uses:
  encoded key = big-endian SpaceId || raw backend key
  value       = raw stored value bytes
  read txns   = RocksDB snapshots
  writes      = RocksDB WriteBatch
```

Validation:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches
cargo test -p lix_engine --test backend --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run

STORAGE_V2_BENCH_SMOKE=1 cargo bench -p lix_engine --features storage-benches \
  --bench storage_v2 \
  '^storage_v2/backend_matrix/rocksdb_temp/(commit_puts_k1024_g16_v32|mixed80_20_k1024_g16_v32|commit_puts_k128_g16_existing10k_touched_v32|planned_visit_unique_m1000_u100|planned_get_many_m1000_u100|scan_range_visit_key_only_q1000|scan_range_q1000|prefix_scan_q1000)$'
```

Smoke baseline:

| Case                             | Smoke range |
| -------------------------------- | ----------: |
| commit puts k1024/g16            |  596-920 us |
| mixed 80/20 k1024/g16            |  683-965 us |
| commit touched existing k128/g16 | 601-1063 us |
| planned visit unique m1000/u100  |  100-160 us |
| planned get many m1000/u100      |  143-246 us |
| scan range visit key-only q1000  |  463-701 us |
| scan range materialized q1000    |  439-972 us |
| prefix scan materialized q1000   |  359-679 us |

Interpretation:

```text
RocksDB now passes backend-v2 conformance and fits the matrix fixture contract.

The first smoke numbers are directionally different from both SQLite and redb:
  - writes are far faster than redb and SQLite in this fixture;
  - point reads are slower than redb but faster than SQLite;
  - scans are slower than redb/SQLite in this simple key-only q1000 case.

Now the real-backend matrix is complete enough to rank optimization work.
```

## 2026-05-15: full real-backend smoke matrix

Command:

```sh
STORAGE_V2_BENCH_SMOKE=1 cargo bench -p lix_engine --features storage-benches \
  --bench storage_v2 \
  '^storage_v2/backend_matrix/(in_memory|sqlite_temp|redb_temp|rocksdb_temp)/(commit_puts_k1024_g16_v32|mixed80_20_k1024_g16_v32|commit_puts_k128_g16_existing10k_touched_v32|planned_visit_unique_m1000_u100|planned_get_many_m1000_u100|scan_range_visit_key_only_q1000|scan_range_q1000|prefix_scan_q1000)$'
```

Smoke comparison:

| Case                             |  in_memory | sqlite_temp | redb_temp | rocksdb_temp |
| -------------------------------- | ---------: | ----------: | --------: | -----------: |
| commit puts k1024/g16            |  166-255us |  3.2-28.0ms |   20-29ms |    0.6-2.4ms |
| mixed 80/20 k1024/g16            |   89-286us |   2.9-5.7ms |   20-31ms |    0.5-1.3ms |
| commit touched existing k128/g16 |    10-25us |  4.8-12.2ms |   17-29ms |    0.5-2.1ms |
| planned visit unique m1000/u100  | 4.7-12.7us |   465-811us |   13-15us |      63-66us |
| planned get many m1000/u100      | 6.3-13.3us |   157-216us |   14-22us |      70-96us |
| scan visit key-only q1000        |  2.8-4.5us |   261-890us | 115-153us |    206-225us |
| scan materialized q1000          |    12-25us |   173-328us | 184-390us |    306-425us |
| prefix materialized q1000        |    21-42us |   186-264us | 180-255us |    286-395us |

Ranking read:

```text
Writes:
  in_memory is the storage/backend overhead floor.
  rocksdb_temp is the fastest durable real backend in this fixture.
  sqlite_temp is much slower than RocksDB on writes but much faster than redb.
  redb_temp write cost is the biggest outlier.

Point reads:
  redb_temp is surprisingly close to in_memory for m1000/u100.
  rocksdb_temp is next.
  sqlite_temp is slowest for visitor point reads but less bad for materialized
  planned get_many in this run.

Scans:
  in_memory is the floor.
  redb_temp and sqlite_temp are close for materialized prefix/range scans.
  rocksdb_temp is slower on q1000 scan materialization in this fixture, though
  its visitor scan improved substantially after snapshot/range fixes.
```

Next optimization candidates:

```text
1. Profile redb_temp writes:
     commit_puts_k1024_g16_v32
     commit_puts_k128_g16_existing10k_touched_v32

   Goal: decide whether cost is redb commit/durability, per-row table insert,
   or fixture file-copy setup.

2. Profile sqlite_temp planned_visit_unique_m1000_u100 and get_many:
   Goal: compare VALUES-list point lookup versus one prepared statement per key
   or a temp requested-key table.

3. Profile rocksdb_temp scan_range_visit_key_only_q1000:
   Goal: determine whether scan cost is iterator creation, snapshot, key decode,
   value materialization despite KeyOnly, or storage visitor overhead.

Do not change the backend_v2 API based on this smoke pass alone. The matrix now
shows backend-specific implementation costs more strongly than generic storage
shape costs.
```

## 2026-05-15: direct backend profile harness

Added `storage_v2/backend_direct_profile/{backend}` lanes to separate backend
hot paths from fixture setup. These benches set up files/seeds once outside the
sampled loop and measure only:

```text
write:
  begin_write -> put_many/delete_many batches -> commit

read:
  begin_read -> get_many / visit_many / visit_range -> close
```

Smoke command:

```sh
STORAGE_V2_BENCH_SMOKE=1 cargo bench -p lix_engine --features storage-benches \
  --bench storage_v2 'storage_v2/backend_direct_profile'
```

Representative smoke ranges:

| Case                         | in_memory | sqlite_temp | redb_temp | rocksdb_temp |
| ---------------------------- | --------: | ----------: | --------: | -----------: |
| direct commit puts k1024/g16 | 109-257us |  1.8-11.6ms | 4.7-6.3ms |    1.6-4.1ms |
| direct mixed 80/20 k1024/g16 | 104-185us |   2.3-7.3ms | 5.6-7.5ms |    1.7-4.1ms |
| direct touched k128/g16      |   10-22us |   2.4-4.9ms | 4.1-4.9ms |    206-554us |
| direct get_many m1000/u100   |  78-116us |   2.6-4.7ms | 289-430us |   994-2013us |
| direct visit_many m1000/u100 |  40-109us |   2.1-3.9ms | 362-734us |  1174-3106us |
| direct scan visit q1000      | 2.2-6.1us |   1.3-2.0ms | 196-460us |   334-5078us |
| direct scan materialized     |   12-20us |   1.3-1.7ms | 262-423us |    523-858us |

Interpretation:

```text
The old backend_matrix remains useful for end-to-end storage shape.
The new direct_profile group is the one to use for flamegraphs and Instruments
when ranking backend implementation work.

This removes DB open/copy/seed work from the sampled loop. The next profiles
should target direct_profile lanes first:
  - redb direct commits
  - sqlite direct point reads/scans
  - rocksdb direct scans and get_many
```

## 2026-05-15: focused direct backend profiles

Added direct-profile selectors so `samply` can run exactly one backend and one
case. Criterion substring filters are not enough by themselves because bench
group setup still runs before filtering.

Selector environment:

```sh
STORAGE_V2_BENCH_DIRECT_PROFILE_ONLY=1
STORAGE_V2_BENCH_DIRECT_PROFILE_BACKEND=sqlite_temp|redb_temp|rocksdb_temp
STORAGE_V2_BENCH_DIRECT_PROFILE_CASE=<direct case name>
```

Focused profiles:

```text
target/storage_v2_profiles/direct_backend_profile_focused/redb_commit_k1024.json
target/storage_v2_profiles/direct_backend_profile_focused/sqlite_get_many.json
target/storage_v2_profiles/direct_backend_profile_focused/sqlite_scan_visit.json
target/storage_v2_profiles/direct_backend_profile_focused/rocksdb_get_many.json
target/storage_v2_profiles/direct_backend_profile_focused/rocksdb_scan_visit.json
```

Focused bench means:

| Case                     |     Mean |
| ------------------------ | -------: |
| redb commit k1024/g16    | 4.789 ms |
| sqlite get_many u100     | 2.439 ms |
| sqlite scan visit q1000  | 2.571 ms |
| rocksdb get_many u100    | 1.631 ms |
| rocksdb scan visit q1000 |   477 us |

Profile read:

```text
redb commit:
  dominated by commit durability:
    File::sync_all / fcntl ~66.7% main-thread inclusive
    redb WriteTransaction::commit_inner ~76%
    TransactionalMemory::commit ~74.9%
  Per-row insert work is small by comparison.

sqlite get_many and scan:
  dominated by begin_read, which prepares BEGIN / transaction statements and
  opens a SQLite read transaction for every backend read:
    SqliteBackend::begin_read ~48-51%
    sqlite3Prepare / parser / sqlite3RunParser ~41-47%
    SQLite pager/WAL shared-lock path ~22-35%
    SqliteRead::close/drop connection path ~22-24%
  Actual row stepping is not the top cost.

rocksdb get_many:
  dominated by RocksDB MultiGet and block table lookup:
    RocksDbRead::get_many ~72%
    rocksdb_multi_get / DBImpl::MultiGet ~56-60%
    Version::Get / TableCache::Get / BlockBasedTable::Get ~40-46%
    DataBlockIter::SeekImpl ~17%
    malloc/free/memcmp/memmove visible in leaves

rocksdb scan:
  dominated by iterator traversal plus value/key copying:
    RocksDbRead::visit_range ~79%
    DB iterator next ~46%
    DBIter::Next ~31%
    MergingIterator Next ~15-16%
    Bytes::copy_from_slice ~12.4%
    malloc/free are large leaves
```

Ranked next cuts:

```text
1. SQLite read transaction/prepared statement lifecycle.
   Reusing a connection/read transaction or cached BEGIN/COMMIT/SELECT
   statements is likely higher leverage than changing get_many SQL shape first.

2. RocksDB scan allocation/copying.
   KeyOnly scans should avoid value copies completely and minimize key Bytes
   allocation. Investigate whether the backend copies keys before visitor use.

3. RocksDB get_many query shape.
   MultiGet is doing real block lookup work. Try sorted/deduped unique keys,
   read options, and possible point-lookup cache behavior before API changes.

4. redb commit durability policy.
   The slow path is fsync/fcntl. Optimization is mostly durability-mode or
   transaction policy, not storage_v2 write-set shape.
```

## 2026-05-15: SQLite read lifecycle optimization

Change:

```text
SqliteBackend now keeps a small read connection pool.
SqliteRead owns a pooled connection, rolls it back on close/drop, and returns it
to the pool.
begin_read reuses cached BEGIN / snapshot-pin statements.
get_many and visit_range use rusqlite prepare_cached for repeated query shapes.
```

Why:

```text
The focused profile showed SQLite reads were dominated by:
  begin_read connection/transaction setup
  sqlite3Prepare/parser
  pager/WAL shared-lock path
  SqliteRead close/drop connection path

Opening and dropping a SQLite connection per read was not representative of the
backend API cost we want to study.
```

Validation:

```sh
cargo test -p lix_engine --test backend sqlite -- --nocapture
```

Result:

```text
sqlite_backend_passes_backend_v2_conformance ... ok
```

Focused bench deltas:

| Case                              | Before focused profile | After read pool | After prepare_cached |
| --------------------------------- | ---------------------: | --------------: | -------------------: |
| sqlite direct get_many m1000/u100 |               2.439 ms |          528 us |               298 us |
| sqlite direct scan visit q1000    |               2.571 ms |          347 us |               305 us |

Interpretation:

```text
The read pool was the big cut:
  get_many improved about 4.6x
  scan visit improved about 7.4x

prepare_cached then removed most remaining parser cost:
  get_many improved another ~1.8x
  scan visit improved another ~1.1x

Compared to the original focused profile:
  get_many is now about 8.2x faster
  scan visit is now about 8.4x faster
```

## 2026-05-15: RocksDB KeyOnly scan copy removal

Change:

```text
RocksDbRead::visit_range now branches on CoreProjection.
For KeyOnly scans it no longer copies the RocksDB value into Bytes only to
discard it.
FullValue scans keep the existing value-copy behavior.
```

Validation:

```sh
cargo test -p lix_engine --test backend rocksdb -- --nocapture
```

Result:

```text
rocksdb_backend_passes_backend_v2_conformance ... ok
```

Focused bench:

```sh
STORAGE_V2_BENCH_DIRECT_PROFILE_ONLY=1 \
STORAGE_V2_BENCH_DIRECT_PROFILE_BACKEND=rocksdb_temp \
STORAGE_V2_BENCH_DIRECT_PROFILE_CASE=direct_scan_visit_key_only_q1000 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'direct_scan_visit_key_only_q1000'
```

Delta:

| Case                                    | Before |  After |
| --------------------------------------- | -----: | -----: |
| rocksdb direct scan visit q1000 KeyOnly | 477 us | 279 us |

Interpretation:

```text
About 1.7x faster for direct RocksDB KeyOnly scans.

The post-change profile confirms value-copy cost is gone. Remaining cost is
mostly RocksDB iterator traversal and key-side allocation/copying:
  RocksDbRead::visit_range ~79.5%
  DB iterator next ~50.7%
  DBIter::Next ~34.1%
  MergingIterator next ~15-18%
  allocator/free/memmove/memcmp remain visible

Next RocksDB scan cut, if needed:
  avoid key Bytes allocation by adding a borrowed-key scan visitor path or by
  changing backend_v2 visitor semantics to allow borrowed backend keys.
```

## 2026-05-15: Borrowed scan API hard cut, partial full-matrix run

Change:

```text
backend_v2 scan visitors now receive borrowed row data:
  KeyRef<'_>
  ProjectedValueRef::FullValue(&[u8])

The hard cut removes owned Key/Bytes materialization from the backend scan
visitor path. storage_v2 materializes owned ReadEntry rows only for APIs that
return ScanPage-shaped results.
```

Validation before bench:

```sh
cargo test -p lix_engine backend_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
```

Result:

```text
backend_v2 conformance passed for:
  in-memory
  conformance backend
  sqlite_temp
  redb_temp
  rocksdb_temp

storage_v2 bench target compiled.
```

Full bench command:

```sh
cargo bench -p lix_engine --features storage-benches --bench storage_v2
```

Run status:

```text
Partial run only. Criterion completed storage-only, conformance backend,
in_memory, sqlite_temp, redb_temp, rocksdb_temp, and part of direct in-memory
profiling. The process then aborted with stack overflow during
backend_direct_profile/in_memory, after direct write cases and before the full
direct-profile block completed.
```

Important post-cut measurements from the completed portion:

| Case                                   |      Time |
| -------------------------------------- | --------: |
| in_memory scan visit key-only q1000    |  1.519 us |
| in_memory materialized scan q1000      |  20.03 us |
| sqlite_temp scan visit key-only q1000  |  43.02 us |
| sqlite_temp materialized scan q1000    |  64.83 us |
| sqlite_temp prefix materialized q1000  |  61.86 us |
| redb_temp scan visit key-only q1000    |  29.71 us |
| redb_temp materialized scan q1000      |  39.06 us |
| redb_temp prefix materialized q1000    |  41.84 us |
| rocksdb_temp scan visit key-only q1000 |  90.42 us |
| rocksdb_temp materialized scan q1000   | 120.43 us |
| rocksdb_temp prefix materialized q1000 | 109.93 us |

Write/read reference points from the same run:

| Case                                     |      Time |
| ---------------------------------------- | --------: |
| in_memory commit puts k1024/g16          |  43.57 us |
| sqlite_temp commit puts k1024/g16        | 909.93 us |
| redb_temp commit puts k1024/g16          |  16.77 ms |
| rocksdb_temp commit puts k1024/g16       | 201.76 us |
| sqlite_temp planned get_many m1000/u100  |  37.26 us |
| redb_temp planned get_many m1000/u100    |   9.93 us |
| rocksdb_temp planned get_many m1000/u100 |  41.38 us |

Criterion-reported scan deltas versus the previous saved baseline:

| Case                                   | Reported change |
| -------------------------------------- | --------------: |
| sqlite_temp scan visit key-only q1000  |    92.8% faster |
| sqlite_temp materialized scan q1000    |    70.3% faster |
| redb_temp scan visit key-only q1000    |    81.6% faster |
| redb_temp materialized scan q1000      |    83.2% faster |
| rocksdb_temp scan visit key-only q1000 |    81.7% faster |
| rocksdb_temp materialized scan q1000   |    77.1% faster |

Interpretation:

```text
The borrowed scan API is a real first-principles win for real backends.

The biggest confirmed gains are exactly where expected:
  SQLite scan no longer copies row key/value blobs before visitor use.
  redb scan can pass table key/value slices directly.
  RocksDB scan avoids both value and key Bytes construction on the visitor path.

Materialized scans also improved because storage_v2 now owns the only required
copy, instead of layering backend materialization plus storage materialization.
```

Follow-up before treating this as the new official full baseline:

```text
Fix or isolate the backend_direct_profile/in_memory stack overflow.
Then rerun either:
  full storage_v2 matrix
or:
  STORAGE_V2_BENCH_DIRECT_PROFILE_ONLY focused direct-profile matrix
```

## 2026-05-15: Direct-profile stack overflow investigation

Finding:

```text
The stack overflow was caused by the benchmark harness, not by the borrowed
scan API.

backend_direct_profile write cases reused the same in-memory backend for all
Criterion iterations. InMemoryBackend represents commits as per-space overlay
layers:

  SpaceState::Layered { base, puts, deletes }

Repeated direct commits into the same backend created a very deep recursive
overlay chain. When the benchmark moved on and dropped that backend, recursive
drop of the SpaceState chain overflowed the stack.
```

Why this mattered:

```text
The direct-profile write benchmarks were not measuring a stable
begin_write -> put_many/delete_many -> commit cost. They were also measuring
the side effects of ever-growing benchmark state.
```

Harness fix:

```text
Each direct-write benchmark iteration now receives a fresh/forked backend from
Criterion setup:

  empty write cases:
    setup opens a fresh empty backend

  touched-existing case:
    setup forks the seeded backend

The measured routine still contains only:
  begin_write -> put_many/delete_many -> commit
```

Validation:

```sh
STORAGE_V2_BENCH_DIRECT_PROFILE_ONLY=1 \
STORAGE_V2_BENCH_DIRECT_PROFILE_BACKEND=in_memory \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  backend_direct_profile/in_memory
```

Result:

```text
Completed without stack overflow.
```

Direct in-memory profile after harness fix:

| Case                                                |     Time |
| --------------------------------------------------- | -------: |
| direct_commit_puts_k1024_g16_v32                    | 40.13 us |
| direct_mixed80_20_k1024_g16_v32                     | 35.96 us |
| direct_commit_puts_k128_g16_existing10k_touched_v32 |  4.03 us |
| direct_get_many_m1000_u100                          | 27.36 us |
| direct_visit_many_m1000_u100                        | 23.63 us |
| direct_scan_visit_key_only_q1000                    | 1.055 us |
| direct_scan_materialized_q1000                      | 20.87 us |
```

## 2026-05-16: Point reads hard-cut to visitor-first backend API

Change under test:

```text
BackendRead::get_many(...) -> removed from the required backend trait
BackendRead::visit_many(...) -> required core point-read API
backend_v2::get_many(...) -> materializing helper layered above visit_many
```

This makes point reads match the scan-side cut: backend authors implement the
borrowed visitor path; storage_v2 owns materialization when callers ask for
owned values.

Validation:

```sh
cargo test -p lix_engine backend_v2 --no-fail-fast
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run
cargo bench -p lix_engine --features storage-benches --bench storage_v2
```

All tests passed, and the full bench completed.

Backend matrix scorecard:

| Case                             | in_memory | sqlite_temp | redb_temp | rocksdb_temp |
| -------------------------------- | --------: | ----------: | --------: | -----------: |
| commit puts k1024/g16            |  40.38 us |   897.20 us |  21.72 ms |    218.86 us |
| mixed 80/20 k1024/g16            |  36.79 us |   984.64 us |  20.76 ms |    223.14 us |
| commit touched existing k128/g16 |   4.17 us |     1.01 ms |  16.42 ms |    199.69 us |
| planned visit unique m1000/u100  |   2.29 us |    34.19 us |   7.08 us |     40.53 us |
| planned get many m1000/u100      |   4.77 us |    37.00 us |   9.24 us |     43.68 us |
| scan visit key-only q1000        |   1.02 us |    44.65 us |  29.79 us |     91.17 us |
| scan materialized q1000          |  19.67 us |    62.62 us |  39.10 us |    108.86 us |
| prefix materialized q1000        |  19.51 us |    62.66 us |  39.19 us |    109.74 us |

Direct backend profile:

| Case                             | in_memory | sqlite_temp | redb_temp | rocksdb_temp |
| -------------------------------- | --------: | ----------: | --------: | -----------: |
| direct get_many m1000/u100       |  50.05 us |   107.95 us |  91.79 us |    423.60 us |
| direct visit_many m1000/u100     |  23.59 us |    80.21 us |  66.90 us |    400.86 us |
| direct scan visit key-only q1000 |   1.39 us |    45.39 us |  23.59 us |     92.82 us |
| direct scan materialized q1000   |  20.06 us |    64.40 us |  39.50 us |    109.16 us |

Focused point-read adapter scorecard:

| Case                                                     |      Time |      Criterion-reported change |
| -------------------------------------------------------- | --------: | -----------------------------: |
| point_read_adapter m1000/u100                            |  45.43 us |                  130.5% slower |
| point_read_indexed_adapter m1000/u100                    |  35.37 us |                   78.7% slower |
| point_read_planned_lean_backend m10000/u100              |   6.29 us |                 1750.9% slower |
| point_read_planned_lean_backend visit_unique m10000/u100 | 291.71 ns |                  217.8% slower |
| conformance_backend get_many m1000/u100                  |  16.71 us |                   24.0% slower |
| in_memory_backend planned_get_many m1000/u100            |   4.95 us | 65.4% faster vs old noisy lane |
| in_memory_backend planned_visit_unique m1000/u100        |   2.33 us | 74.8% faster vs old noisy lane |

Interpretation:

```text
The hard cut is directionally clean but not a free point-read win.

Direct visitor point reads are faster than materialized helper reads for every
real backend in the direct profile:

  in_memory:   50.05 us -> 23.59 us
  sqlite_temp: 107.95 us -> 80.21 us
  redb_temp:   91.79 us -> 66.90 us
  rocksdb:    423.60 us -> 400.86 us

But the storage-only and lean synthetic materialized point-read lanes regressed
badly. The old backend trait allowed custom owned get_many implementations to
fill Vec<Option<ProjectedValue>> directly. The new hard cut routes owned point
results through per-slot PointVisitor calls and then materializes above the
backend boundary.
```

So the current result is:

```text
Good:
  backend API is simpler and borrowed-first
  direct visitor point reads are cheaper than materialized helper reads
  real backend planned visitor lanes improved or stayed competitive
  scan visitor gains remain intact

Bad:
  materialized point reads now pay a visible helper/visitor/materialization tax
  synthetic lean lanes expose that tax brutally
```

Next first-principles question:

```text
Can storage_v2 keep the visitor-first backend core while adding a monomorphic
storage-owned point collector path that avoids the generic PointVisitor tax for
materialized point results?

Possible shape:
  backend.visit_many_collect(space, keys, opts, &mut PointValueCollector)

or:
  BackendRead::visit_many receives a concrete enum/collector instead of a
  generic PointVisitor for the materialized fast path.

Do not re-add required BackendRead::get_many yet. The direct profile says the
borrowed visitor API is valuable. The missing piece is an efficient
materializing adapter above it.
```

## 2026-05-16: Reusable point value buffer

Change under test:

```text
Added storage_v2::PointValueBuffer.
Added get_many_indexed_values_for_plan_into(..., &mut PointValueBuffer).
Added get_many_indexed_values_for_plan_into_with_stats(...).

BackendRead remains visitor-first. This is a storage-owned materialization
buffer, not a required backend get_many revival.
```

Validation:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine storage_v2 --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench storage_v2 --no-run

cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/(point_read_planned_lean_backend|in_memory_backend/(planned_get_many_m1000_u100|planned_get_many_buffered_m1000_u100|planned_visit_unique_m1000_u100)|backend_matrix/(in_memory|sqlite_temp|redb_temp|rocksdb_temp)/(planned_visit_unique_m1000_u100|planned_get_many_m1000_u100|planned_get_many_buffered_m1000_u100))'
```

Focused lean scorecard:

| Case                 | fresh materialized | buffered materialized | visit_unique |
| -------------------- | -----------------: | --------------------: | -----------: |
| m100/u100            |            1.87 us |               1.78 us |     88.30 ns |
| m1000/u1000          |           18.71 us |              18.04 us |    859.98 ns |
| m1000/u100           |            1.87 us |               1.80 us |     87.96 ns |
| m10000/u100          |            1.89 us |               1.79 us |     87.45 ns |
| m10000/u10000        |          185.68 us |             181.94 us |      9.19 us |
| m1000/u100 missing10 |            1.74 us |               1.64 us |     86.01 ns |
| m1000/u100 missing90 |          408.25 ns |             349.51 ns |     71.00 ns |

Focused real-backend scorecard:

| Backend      | planned visit_unique | fresh materialized | buffered materialized |
| ------------ | -------------------: | -----------------: | --------------------: |
| in_memory    |              2.37 us |            4.86 us |               4.75 us |
| sqlite_temp  |             34.17 us |           37.11 us |              36.69 us |
| redb_temp    |              6.93 us |            9.15 us |               9.29 us |
| rocksdb_temp |             40.42 us |           42.33 us |              42.53 us |

In-memory-specific scorecard:

| Case                                 |    Time |
| ------------------------------------ | ------: |
| planned_get_many_m1000_u100          | 4.98 us |
| planned_get_many_buffered_m1000_u100 | 4.80 us |
| planned_visit_unique_m1000_u100      | 2.35 us |

Interpretation:

```text
The reusable buffer has the desired limited impact:

  - It removes repeated Vec allocation/capacity churn.
  - It gives small but consistent wins in lean materialized lanes.
  - It does not and cannot remove owned value clones.
  - On real backends the result is mostly noise to small win, because backend
    lookup dominates more than allocation.

The first-principles ranking remains:

  1. visit_unique_point_values_for_plan
     fastest; no owned point result and no storage materialization clones

  2. get_many_indexed_values_for_plan_into
     middle lane; reusable allocation but still owns/clones values

  3. get_many_borrowed_indexed_values_for_plan / caller-order helpers
     convenience lanes; allocate owned result per call
```

Conclusion:

```text
PointValueBuffer is worth keeping because it gives a clean storage-owned
materialization lane with no backend API cost. But it does not solve the whole
materialized point-read tax. The actual hot path should remain the visitor API,
and domain stores should prefer decoding/accumulating inside
visit_unique_point_values_for_plan whenever possible.

The next possible cut is not another buffer tweak. It is either:
  - move domain hot reads to the visitor lane, or
  - add a non-required backend/storage collector extension only if profiling
    still shows the PointVisitor shim itself dominating.
```

## 2026-05-16: Real-backend profiles after visitor-first point API

Command shape:

```sh
STORAGE_V2_BENCH_DIRECT_PROFILE_ONLY=1 \
STORAGE_V2_BENCH_DIRECT_PROFILE_BACKEND=<sqlite_temp|redb_temp|rocksdb_temp> \
STORAGE_V2_BENCH_DIRECT_PROFILE_CASE=<case> \
samply record --save-only --unstable-presymbolicate \
  -o target/storage_v2_profiles/real_backend_next/<profile>.json -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2

samply record --save-only --unstable-presymbolicate \
  -o target/storage_v2_profiles/real_backend_next/<profile>.json -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/backend_matrix/<backend>/planned_visit_unique_m1000_u100'
```

Focused timings:

| Backend      | direct visit_many m1000/u100 | planned visit_unique m1000/u100 | scan visit key-only q1000 | direct commit puts k1024/g16 |
| ------------ | ---------------------------: | ------------------------------: | ------------------------: | ---------------------------: |
| sqlite_temp  |                     82.52 us |                        37.42 us |                  48.56 us |                    966.94 us |
| redb_temp    |                     70.37 us |                         7.89 us |                  25.86 us |                     16.78 ms |
| rocksdb_temp |                    413.52 us |                        44.07 us |                  96.14 us |                    295.84 us |

Profile notes:

```text
sqlite planned point:
  SqliteRead::visit_many -> rusqlite row advance / sqlite3_step
  backend-side key collection is still visible

redb planned point:
  RedbRead::visit_many -> encode key Vec allocation -> redb Btree::get_helper

rocksdb planned point:
  RocksDbRead::visit_many -> rocksdb multi_get -> DBImpl/TableCache/Version::Get

scan key-only:
  redb is mostly BtreeRangeIter
  sqlite is sqlite3_step / rusqlite row advance
  rocksdb is DBIterator::next / DBIter::Next

write profiles:
  still not clean enough for API conclusions.
  The direct write lanes include real-backend setup/open/copy cost:
    redb shows File::sync_all/open_empty
    rocksdb shows DB::Open/open_empty
    sqlite shows open_empty plus sqlite3_step
```

Interpretation:

```text
The visitor-first backend core is no longer the obvious cross-backend
bottleneck. Planned storage point reads are now mostly backend engine work:
  redb ~8 us
  sqlite ~37 us
  rocksdb ~44 us

The remaining first-principles API smell is that backend_v2 still owns two
concepts storage_v2 is better positioned to own:

  1. logical spaces
     every backend encodes (SpaceId, Key) into a physical key

  2. duplicate/caller-order point semantics
     storage already builds PointRequestPlan and can provide unique keys

That shows up as repeated encode_entry_key Vec construction in redb/rocksdb and
backend-local de-dupe/reconstruction work in sqlite.
```

Proposed next hard cut:

```text
Make backend_v2 a single ordered physical byte-key space.

storage_v2:
  maps StorageSpace -> physical key prefix
  encodes logical keys into physical keys
  owns caller-order slots, duplicate keys, and missing-key reconstruction

backend_v2:
  sees only physical byte keys
  visit_many receives unique physical keys
  visit_range scans physical byte-key ranges
  put_many/delete_many write physical byte-key batches
```

Expected Big-O impact:

```text
No asymptotic change:
  point reads: O(U)
  range scans: O(log N + Q)
  write lowering: O(K + G)

Constant-factor improvements:
  remove per-backend SpaceId/key encoding
  remove backend-local duplicate handling
  make SQLite schema a single BLOB primary key instead of (space_id, key)
  let storage cache/reuse physical point plans directly
```

This follows the same first-principles direction as the previous cuts: backend
becomes a boring ordered byte-key engine; storage owns Lix keyspaces and
adapter semantics.

## 2026-05-16: Cleaner real-backend write profile lane

Added:

```text
storage_v2/backend_direct_profile/<backend>/
  direct_commit_puts_reused_backend_k1024_g16_v32
```

This lane keeps one backend open and repeatedly commits the same 1024 put rows
across 16 spaces. It measures overwrite commits rather than empty-database
insert commits, but it removes the previous temp-file setup/copy/open noise from
the sampled profile.

Validation/profile commands:

```sh
STORAGE_V2_BENCH_DIRECT_PROFILE_ONLY=1 \
STORAGE_V2_BENCH_DIRECT_PROFILE_BACKEND=<backend> \
STORAGE_V2_BENCH_DIRECT_PROFILE_CASE=direct_commit_puts_reused_backend_k1024_g16_v32 \
samply record --save-only --unstable-presymbolicate \
  -o target/storage_v2_profiles/real_backend_next/<backend>_commit_puts_reused_backend.json -- \
  cargo bench -p lix_engine --features storage-benches --bench storage_v2
```

Focused timings:

| Backend      | reused backend commit puts k1024/g16 |
| ------------ | -----------------------------------: |
| sqlite_temp  |                            799.28 us |
| redb_temp    |                              4.06 ms |
| rocksdb_temp |                            520.25 us |

Profile notes:

```text
sqlite_temp:
  sqlite3_step / rusqlite Statement::execute dominate.
  No open/copy/checkpoint stack remains in the hot lane.

redb_temp:
  redb WriteTransaction::commit -> File::sync_all dominates.
  This is durability policy / backend behavior, not storage write-set shape.

rocksdb_temp:
  rocksdb DBImpl::Write -> WriteBatchInternal::InsertInto -> MemTable::Add dominates.
  This is backend engine write path.

all three:
  DirectWriteBatches::clone remains visible because backend_v2::put_many consumes
  owned PutBatch. That clone is benchmark repetition overhead, not the normal
  storage_v2 write-set lowering path where the write set is consumed once.
```

Interpretation:

```text
The write profile is now clean enough to rank backend-engine costs:
  rocksdb overwrite commit is fastest among real file backends here
  sqlite is ~1.5x rocksdb
  redb is much slower because commit sync dominates

It is still not clean enough for a required write API cut, because the benchmark
must clone owned PutBatch values to repeat the same commit. If we want to study
write API constants next, the right harness is either:

  1. storage-owned reusable write buffers that can be refilled without cloning
  2. an optional borrowed write path experiment:
       put_many_ref(space, &[PutEntryRef])

Do not make that a v0 backend requirement yet. The read-side physical-key cut is
the higher-confidence API simplification.
```

## 2026-05-16: Implemented physical-key-only backend v2 core

Implemented the hard API cut proposed above:

```text
backend_v2:
  one ordered physical byte-key space
  visit_many(keys, opts, visitor)
  visit_range(range, opts, visitor)
  put_many(PutBatch)
  delete_many(&[Key])

storage_v2:
  StorageSpace owns the logical space id/name
  physical_key = big_endian_u32(SpaceId) || logical_key
  point plans encode unique backend keys
  scan/prefix helpers lower logical ranges into physical ranges
  write sets encode keys during lower_into()
```

Implementation notes:

```text
Removed SpaceId from the backend read/write trait surface.
Moved space isolation into storage_v2 key encoding and tests.
Updated ConformanceBackend, InMemoryBackend, SQLite, redb, and RocksDB test
backends to store only physical keys.
Removed backend-level space-isolation conformance; storage conformance now owns
that invariant.
Updated direct backend benchmarks to use physical keys/ranges so the scorecard
continues to measure the intended layer.
```

Expected complexity impact:

```text
No asymptotic change:
  point reads: O(U)
  scans: O(log N + Q)
  write-set lowering: O(K + G)

Constant-factor target:
  less per-backend key encoding
  simpler backend schemas and range bounds
  storage can cache/reuse physical point/range plans
```

## 2026-05-16: Physical-key-only API smoke delta

Command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/backend_matrix/(in_memory|sqlite_temp|redb_temp|rocksdb_temp)/(commit_puts_k1024_g16_v32|planned_visit_unique_m1000_u100|planned_get_many_m1000_u100|planned_get_many_buffered_m1000_u100|scan_range_visit_key_only_q1000|scan_range_q1000|prefix_scan_q1000)'

STORAGE_V2_BENCH_SMOKE=1 \
STORAGE_V2_BENCH_DIRECT_PROFILE_ONLY=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/backend_direct_profile/(in_memory|sqlite_temp|redb_temp|rocksdb_temp)/(direct_visit_many_m1000_u100|direct_get_many_m1000_u100|direct_scan_visit_key_only_q1000|direct_scan_materialized_q1000|direct_commit_puts_reused_backend_k1024_g16_v32)'
```

This is a smoke delta, not a final long-run scorecard. It compares against the
most recent pre-physical-key entries in this log.

Backend matrix, current smoke means:

| Case                                 | in_memory | sqlite_temp | redb_temp | rocksdb_temp |
| ------------------------------------ | --------: | ----------: | --------: | -----------: |
| commit puts k1024/g16                |  92.87 us |    884.7 us |  17.47 ms |    208.25 us |
| planned visit unique m1000/u100      |   4.48 us |    39.59 us |   6.54 us |     41.06 us |
| planned get many m1000/u100          |   6.86 us |    40.86 us |  10.18 us |     44.36 us |
| planned get many buffered m1000/u100 |   7.14 us |    45.75 us |  10.86 us |     43.80 us |
| scan visit key-only q1000            |   1.53 us |    40.45 us |  30.06 us |     90.17 us |
| scan materialized q1000              |  21.44 us |    63.67 us |  58.25 us |    110.48 us |
| prefix materialized q1000            |  19.96 us |    61.93 us |  68.53 us |    113.81 us |

Approximate delta vs previous backend-matrix/focused entries:

| Case                            |   in_memory | sqlite_temp |  redb_temp | rocksdb_temp |
| ------------------------------- | ----------: | ----------: | ---------: | -----------: |
| commit puts k1024/g16           | 130% slower |   1% faster | 20% faster |    5% faster |
| planned visit unique m1000/u100 |  96% slower |  16% slower |  8% faster |    1% slower |
| planned get many m1000/u100     |  44% slower |  10% slower | 10% slower |    2% slower |
| scan visit key-only q1000       |  50% slower |   9% faster |       flat |    1% faster |
| scan materialized q1000         |   9% slower |        flat | 49% slower |    1% slower |
| prefix materialized q1000       |   2% slower |        flat | 75% slower |    4% slower |

Direct backend profile, current smoke means:

| Case                             | in_memory | sqlite_temp | redb_temp | rocksdb_temp |
| -------------------------------- | --------: | ----------: | --------: | -----------: |
| direct reused commit k1024/g16   |  69.62 us |   805.92 us |   4.05 ms |    531.71 us |
| direct get_many m1000/u100       |  46.74 us |   109.18 us |  75.42 us |    401.00 us |
| direct visit_many m1000/u100     |  22.04 us |    78.65 us |  42.03 us |    389.65 us |
| direct scan visit key-only q1000 |   1.43 us |    44.60 us |  29.38 us |     91.84 us |
| direct scan materialized q1000   |  20.56 us |    64.88 us |  44.90 us |    115.31 us |

Direct-profile interpretation:

```text
Physical-key-only helps or holds the raw backend point path:
  in_memory direct visit_many: ~6.6% faster
  sqlite direct visit_many: ~2% faster / noise
  redb direct visit_many: ~37% faster
  rocksdb direct visit_many: ~3% faster / noise

The storage matrix now exposes storage-side key prefix encoding costs:
  in_memory is the clearest regression because backend I/O is tiny
  real backends mostly hide the prefix cost behind engine work

Scan results are mixed:
  SQLite scan visitor improved a little
  RocksDB scan visitor is effectively flat
  redb materialized/prefix scans regressed in this smoke run and need a focused
  profile before drawing a first-principles conclusion
```

Conclusion:

```text
The hard API cut did simplify backend implementations and improved the direct
backend point path, especially redb. It did not automatically improve
storage-level in-memory performance because physical-key construction moved into
storage_v2 and is now paid visibly in the fastest backend.

Next candidate optimization:
  cache physical keys/ranges in PointRequestPlan and prefix/range plans so
  repeated reads do not rebuild big_endian_u32(SpaceId) || logical_key every time.
```

## 2026-05-16 - Physical point request plan

Change:

```text
Added PhysicalPointRequestPlan:
  logical_unique_keys
  physical_unique_keys = big_endian_u32(SpaceId) || logical_key
  requested_to_unique

Storage planned point reads can now reuse pre-encoded backend keys instead of
rebuilding physical keys on each call. The storage visitor still receives
logical keys; only the backend request vector is physical.
```

Command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/(point_read_planned_lean_backend|backend_matrix/(in_memory|sqlite_temp|redb_temp|rocksdb_temp)/(planned_visit_unique_m1000_u100|planned_get_many_m1000_u100|planned_get_many_buffered_m1000_u100))'
```

Focused before/after smoke means:

| Case                                  |    Before |     After |      Delta |
| ------------------------------------- | --------: | --------: | ---------: |
| lean planned get m1000/u100           |  3.999 us |  2.166 us | 46% faster |
| lean planned buffered m1000/u100      |  3.891 us |  2.109 us | 46% faster |
| lean planned visit m1000/u100         |  2.193 us |  0.131 us | 94% faster |
| lean planned get m10000/u10000        | 383.03 us | 214.54 us | 44% faster |
| lean planned buffered m10000/u10000   | 391.49 us | 244.94 us | 37% faster |
| lean planned visit m10000/u10000      | 230.09 us |  11.85 us | 95% faster |
| in_memory planned visit m1000/u100    |  4.344 us |  2.618 us | 40% faster |
| in_memory planned get m1000/u100      |  6.669 us |  5.665 us | 15% faster |
| in_memory planned buffered m1000/u100 |  6.567 us |  5.337 us | 19% faster |
| sqlite planned visit m1000/u100       |  35.14 us |  38.60 us | 10% slower |
| sqlite planned get m1000/u100         |  43.14 us |  40.41 us |  6% faster |
| sqlite planned buffered m1000/u100    |  39.01 us |  40.40 us | flat/noise |
| redb planned visit m1000/u100         |  6.440 us |  5.220 us | 19% faster |
| redb planned get m1000/u100           |  9.775 us |  9.321 us |  5% faster |
| redb planned buffered m1000/u100      |  10.52 us |  9.136 us | 13% faster |
| rocksdb planned visit m1000/u100      |  43.53 us |  44.24 us | flat/noise |
| rocksdb planned get m1000/u100        |  44.74 us |  48.25 us |  8% slower |
| rocksdb planned buffered m1000/u100   |  46.71 us |  46.94 us | flat/noise |

Interpretation:

```text
Confirmed for the storage adapter and the in-memory/redb paths:
  repeated planned reads should cache physical backend keys.

The improvement is largest where backend work is cheap:
  the lean backend and InMemoryBackend expose the storage adapter cost directly.

SQLite and RocksDB are dominated by backend engine work in these lanes:
  SQLite materialized reads are essentially flat/slightly better.
RocksDB point reads are noisy and one materialized lane regressed; this does
not invalidate the storage cut, but it says the next RocksDB-specific cut
should be based on direct backend profiling, not storage key encoding.
```

## 2026-05-16 - SQLite requested-order point reads

Change:

```text
SQLite visit_many now uses:
  WITH requested(ord, key) AS (VALUES (?, ?), ...)
  SELECT r.ord, e.value
  FROM requested r
  LEFT JOIN entries e ON e.key = r.key
  ORDER BY r.ord

This removes the Rust-side BTreeSet/BTreeMap reconstruction from the SQLite
adapter and avoids SQLite row_number() overhead. Storage already sends planned
unique physical keys, so the backend can return requested-order slots directly.
```

Command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/backend_matrix/sqlite_temp/(planned_visit_unique_m1000_u100|planned_get_many_m1000_u100|planned_get_many_buffered_m1000_u100)'
```

Focused before/after smoke means:

| Case                               |   Before |    After |      Delta |
| ---------------------------------- | -------: | -------: | ---------: |
| sqlite planned visit m1000/u100    | 33.69 us | 22.71 us | 33% faster |
| sqlite planned get m1000/u100      | 39.94 us | 23.11 us | 42% faster |
| sqlite planned buffered m1000/u100 | 38.83 us | 21.26 us | 45% faster |

Interpretation:

```text
The first attempted requested-order SQL shape used row_number() over VALUES and
was flat/slower. Explicit ordinals in VALUES are the right SQLite shape here.

The API/layout lesson holds:
  once storage_v2 owns duplicate removal and physical plans, concrete backends
  should consume the provided key order directly instead of rebuilding their own
  sorted/request map.

Next related cuts:
  - check RocksDB direct materialized point cost separately; storage planned
    lanes are now fast enough that backend engine behavior dominates.
  - add physical scan/range plans if repeated scan workloads show bound
    encoding in profiles.
```

## 2026-05-16 - Fresh backend matrix after point-plan and SQLite point cuts

Command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/backend_matrix/(in_memory|sqlite_temp|redb_temp|rocksdb_temp)/(commit_puts_k1024_g16_v32|planned_visit_unique_m1000_u100|planned_get_many_m1000_u100|planned_get_many_buffered_m1000_u100|scan_range_visit_key_only_q1000|scan_range_q1000|prefix_scan_q1000)'
```

Fresh smoke means:

| Case                                 | in_memory | sqlite_temp | redb_temp | rocksdb_temp |
| ------------------------------------ | --------: | ----------: | --------: | -----------: |
| commit puts k1024/g16                |  92.76 us |   924.78 us |  15.96 ms |    223.50 us |
| planned visit unique m1000/u100      |   2.42 us |    18.52 us |   4.34 us |     40.03 us |
| planned get many m1000/u100          |   5.01 us |    21.73 us |   7.67 us |     42.68 us |
| planned get many buffered m1000/u100 |   4.64 us |    20.33 us |   7.79 us |     41.15 us |
| scan visit key-only q1000            |   1.54 us |    48.04 us |  31.19 us |     97.52 us |
| scan materialized q1000              |  21.62 us |    63.20 us |  46.42 us |    120.54 us |
| prefix materialized q1000            |  24.20 us |    70.86 us |  49.91 us |    116.63 us |

Interpretation:

```text
Point-read path is now stable:
  in_memory and redb expose low storage overhead.
  sqlite point reads are now in the low 20us range after requested-order SQL.
  rocksdb planned point reads sit around 40us, matching direct unique_u100
  RocksDB point reads.

The remaining larger numbers are not point-plan issues:
  redb writes are commit/durability dominated.
  sqlite writes are transaction/SQLite dominated.
  rocksdb scans are iterator/materialization dominated and noisy.

Next profiling target:
  scan/prefix small-Q lanes. If q10/q100 show measurable adapter overhead, add
  physical scan/prefix plans. If only q1000 differs, avoid a scan-plan API cut
  and treat the difference as backend iterator/materialization noise.
```

## 2026-05-16 - Small-Q scan/prefix profile

Change:

```text
Added backend matrix scan lanes for q10 and q100:
  scan_range_visit_key_only_q{10,100}
  scan_range_q{10,100}
  prefix_scan_q{10,100}

The existing q1000 lanes remain the large-scan comparison point.
```

Command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/backend_matrix/(in_memory|sqlite_temp|redb_temp|rocksdb_temp)/(scan_range_visit_key_only_q10|scan_range_q10|prefix_scan_q10|scan_range_visit_key_only_q100|scan_range_q100|prefix_scan_q100|scan_range_visit_key_only_q1000|scan_range_q1000|prefix_scan_q1000)'
```

Fresh smoke means:

| Backend      | range visit q10 | range q10 | prefix q10 | range visit q100 | range q100 | prefix q100 | range visit q1000 | range q1000 | prefix q1000 |
| ------------ | --------------: | --------: | ---------: | ---------------: | ---------: | ----------: | ----------------: | ----------: | -----------: |
| in_memory    |       137.79 ns | 338.32 ns |  325.67 ns |        317.06 ns |    2.24 us |     2.24 us |           1.63 us |    20.82 us |     20.51 us |
| sqlite_temp  |       824.19 ns |   1.03 us |    1.08 us |          4.52 us |    6.49 us |     6.49 us |          40.77 us |    63.63 us |     60.76 us |
| redb_temp    |       628.21 ns | 801.73 ns |  881.05 ns |          3.40 us |    5.05 us |     5.38 us |          30.01 us |    46.64 us |     48.94 us |
| rocksdb_temp |         1.53 us |   1.87 us |    1.74 us |          9.33 us |   11.51 us |    11.66 us |          89.20 us |   110.98 us |    110.51 us |

Interpretation:

```text
Do not add PhysicalScanPlan / PhysicalPrefixPlan yet.

Range and prefix materialized scans are effectively the same once rows dominate:
  q100 and q1000 are flat across in_memory/sqlite/rocksdb, with redb prefix only
  slightly slower.

Small q10 differences are sub-microsecond to low-microsecond and not a clear
API/layout bottleneck. The current scan path is already shaped correctly:
  one backend range call, storage-owned prefix lowering, and visitor-first scan.

The larger remaining scan cost is row iteration/materialization, not repeated
bound encoding.

Next optimization target:
  materialized scan allocation/reuse or backend-specific scan iterator tuning,
  not a new scan-plan API.
```

## 2026-05-16 - Flatten StorageWriteSet lowering experiment

Experiment:

```text
Tested lowering all staged puts into one physical backend put_many() call and
all staged deletes into one physical backend delete_many() call.

The candidate storage contract was:
  put_batches <= 1
  delete_batches <= 1
  backend write calls <= 2 before commit

This would replace the current per-logical-space lowering where write calls
scale with G.
```

Before command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/backend_matrix/(in_memory|sqlite_temp|redb_temp|rocksdb_temp)/(commit_puts_k1024_g16_v32|mixed80_20_k1024_g16_v32|commit_puts_k128_g16_existing10k_touched_v32)'
```

Before smoke means:

| Case                             | in_memory | sqlite_temp | redb_temp | rocksdb_temp |
| -------------------------------- | --------: | ----------: | --------: | -----------: |
| commit puts k1024/g16            |  83.32 us |   880.45 us |  16.50 ms |    189.44 us |
| mixed 80/20 k1024/g16            | 106.96 us |   812.72 us |  16.65 ms |    217.72 us |
| commit touched existing k128/g16 |   8.43 us |     1.16 ms |  15.41 ms |    155.32 us |

After smoke means:

| Case                             | in_memory | sqlite_temp | redb_temp | rocksdb_temp |
| -------------------------------- | --------: | ----------: | --------: | -----------: |
| commit puts k1024/g16            | 109.62 us |   980.90 us |  15.96 ms |    239.99 us |
| mixed 80/20 k1024/g16            |  96.96 us |   861.79 us |  16.51 ms |    252.59 us |
| commit touched existing k128/g16 |   9.66 us |     1.23 ms |  16.22 ms |    235.06 us |

Delta from the explicit before run:

| Case                             | in_memory | sqlite_temp | redb_temp | rocksdb_temp |
| -------------------------------- | --------: | ----------: | --------: | -----------: |
| commit puts k1024/g16            |    +31.6% |      +11.4% |     -3.2% |       +26.7% |
| mixed 80/20 k1024/g16            |     -9.3% |       +6.0% |     -0.8% |       +16.0% |
| commit touched existing k128/g16 |    +14.6% |       +5.4% |     +5.2% |       +51.3% |

Interpretation:

```text
The API shape is cleaner, but this smoke run does not show a universal
performance win.

The intended Big-O contract improved:
  old write lowering: O(G) backend write calls
  new write lowering: O(1) backend write calls, at most put + delete

The measured constants are mixed:
  redb is effectively unchanged, likely commit/durability dominated.
  in_memory improves for mixed writes but regresses for put-only lanes.
  sqlite and rocksdb regress in this smoke matrix despite fewer backend calls.

Likely cause:
  flattening introduces one larger storage-side materialization buffer and moves
  all encoded physical keys through it before the backend sees the batch. For
  local embedded backends, reducing 16 cheap same-transaction calls to 1 call is
  not enough to offset the changed materialization/cache behavior in this run.

Next decision:
  do not keep this implementation.

  The code was reverted to grouped-by-space lowering after profiling showed the
  storage-only write_set_lowering lane regressed from about 6us to about 31us
  for puts_k1024_g16_v32. The architectural simplification may still be right,
  but only with a borrowed/streamed write path or earlier physical-key staging.
  Simply flattening into one owned PutBatch is not the right cut.
```

## 2026-05-16 - Borrowed write sink experiment

Experiment:

```text
Tested an additive BackendWrite fast path:
  put_many_with(|sink| sink.put(KeyRef, value_bytes))
  delete_many_with(|sink| sink.delete(KeyRef))

storage_v2 encoded physical keys into a reusable scratch buffer and handed the
borrowed bytes to the backend sink immediately. The owned put_many/delete_many
API stayed as the fallback.
```

Focused storage-only results:

| Case                     | Flattened owned batch | Borrowed sink |
| ------------------------ | --------------------: | ------------: |
| puts_k1024_g16_v32       |              30.92 us |      11.91 us |
| puts_k8192_g16_v32       |             160.32 us |      75.71 us |
| mixed80_20_k1024_g16_v32 |              21.45 us |      12.24 us |

Backend matrix smoke means with the borrowed sink:

| Case                             | in_memory | sqlite_temp | redb_temp | rocksdb_temp |
| -------------------------------- | --------: | ----------: | --------: | -----------: |
| commit puts k1024/g16            | 105.02 us |     1.08 ms |  17.18 ms |    200.54 us |
| mixed 80/20 k1024/g16            | 137.21 us |     1.06 ms |  16.42 ms |    253.10 us |
| commit touched existing k128/g16 |  16.12 us |     1.28 ms |  16.40 ms |    175.31 us |

Interpretation:

```text
Do not keep this implementation.

Borrowed sinks fix much of the owned-flattening storage-only regression, but
they still do not beat the pre-flatten grouped baseline:
  old grouped puts_k1024_g16_v32: about 8.70 us
  borrowed sink puts_k1024_g16_v32: about 11.91 us

The real backend matrix is mixed:
  redb improves in the large put lane
  rocksdb is mostly flat/noisy
  in_memory and sqlite regress, likely from per-entry dyn sink dispatch and
  backend-specific sink wrappers

Conclusion:
  a generic dyn write sink is not the right backend_v2 API cut. If we revisit
  borrowed writes, it should be monomorphic or backend-specific, and it should
  be justified by a domain-shaped workload where redb-style table insertion is
  the limiting path. For now, keep grouped owned put_many/delete_many.
```

## 2026-05-16 - Canonical write-set staging experiment

Experiment:

```text
Add a storage_v2 write-set construction path for callers that already emit
canonical final mutations:

  StorageWriteSet::canonicalized_with_capacity(...)
  StorageWriteSet::reserve_space(...)
  StorageWriteSet::stage_canonical_put(...)
  StorageWriteSet::stage_canonical_delete(...)

This path skips the per-mutation duplicate HashMap and preserves first-seen
space order before lowering to the existing grouped put_many/delete_many calls.
The checked stage_put/stage_delete path remains available for defensive callers
and tests.
```

Before command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/(write_set_lowering/(puts_k1024_g16_v32|mixed80_20_k1024_g16_v32|puts_k8192_g16_v32)|backend_matrix/(in_memory|rocksdb_temp)/(commit_puts_k1024_g16_v32|mixed80_20_k1024_g16_v32|commit_puts_k128_g16_existing10k_touched_v32))'
```

Before smoke means:

| Case                                |    Before |
| ----------------------------------- | --------: |
| write_set puts k1024/g16            |  28.76 us |
| write_set puts k8192/g16            | 221.58 us |
| write_set mixed 80/20 k1024/g16     |  28.70 us |
| in_memory commit puts k1024/g16     |  86.87 us |
| in_memory mixed 80/20 k1024/g16     | 128.30 us |
| in_memory touched existing k128/g16 |   8.58 us |
| rocksdb commit puts k1024/g16       | 238.53 us |
| rocksdb mixed 80/20 k1024/g16       | 202.92 us |
| rocksdb touched existing k128/g16   | 146.07 us |

After smoke means, after preserving first-seen space order:

| Case                                |     After |
| ----------------------------------- | --------: |
| write_set puts k1024/g16            |  25.92 us |
| write_set puts k8192/g16            | 226.66 us |
| write_set mixed 80/20 k1024/g16     |  29.83 us |
| in_memory commit puts k1024/g16     |  93.66 us |
| in_memory mixed 80/20 k1024/g16     | 117.36 us |
| in_memory touched existing k128/g16 |   9.87 us |
| rocksdb commit puts k1024/g16       | 232.43 us |
| rocksdb mixed 80/20 k1024/g16       | 219.85 us |
| rocksdb touched existing k128/g16   | 168.15 us |

Focused rerun for the most sensitive lanes:

| Case                            | Focused after |
| ------------------------------- | ------------: |
| write_set puts k1024/g16        |      25.69 us |
| in_memory commit puts k1024/g16 |      89.78 us |
| rocksdb commit puts k1024/g16   |     239.96 us |

Delta from the explicit before run:

| Case                                |  Delta |
| ----------------------------------- | -----: |
| write_set puts k1024/g16            |  -9.9% |
| write_set puts k8192/g16            |  +2.3% |
| write_set mixed 80/20 k1024/g16     |  +3.9% |
| in_memory commit puts k1024/g16     |  +7.8% |
| in_memory mixed 80/20 k1024/g16     |  -8.5% |
| in_memory touched existing k128/g16 | +15.0% |
| rocksdb commit puts k1024/g16       |  -2.6% |
| rocksdb mixed 80/20 k1024/g16       |  +8.3% |
| rocksdb touched existing k128/g16   | +15.1% |

Interpretation:

```text
This is not a clean cross-backend performance win.

The API distinction is still useful:
  checked write sets: defensive staging, duplicate validation
  canonical write sets: final mutations from domain stores

But the current smoke scorecard says canonical staging mostly moves small
constants around. It improves the storage-only 1024-put lowering lane and one
in-memory mixed lane, but does not improve the in-memory/rocksdb write matrix
consistently.

The likely reason is that these benches mostly measure lowering/commit after
Criterion setup has already built the write set. The new path attacks
construction, while the remaining measured cost is physical key encoding and
backend insertion.

Next benchmark hardening:
  add explicit write-set construction benches:
    checked_stage_put/delete
    canonical_stage_put/delete
    build_and_commit end-to-end

Do not make a larger backend_v2 API cut from this result alone.
```

## 2026-05-16 - Write-set construction and build+commit benches

Added benchmark groups to measure the thing the canonical write-set path was
designed to improve:

```text
storage_v2/write_set_construction/{checked,canonical}/...
storage_v2/write_set_build_and_commit/<backend>/{checked,canonical}/...
```

The construction group measures only write-set building from prebuilt
mutations. The build+commit group measures write-set construction plus
`StorageContext::commit_write_set` against real backend families.

Command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/write_set_(construction|build_and_commit/(in_memory|sqlite_temp|redb_temp|rocksdb_temp))/(checked|canonical)/(puts_k1024_g16_v32|mixed80_20_k1024_g16_v32|puts_k128_g16_v32)'
```

Construction-only smoke means:

| Case                     |  Checked | Canonical |      Delta |
| ------------------------ | -------: | --------: | ---------: |
| puts_k1024_g16_v32       | 31.51 us |  18.54 us | 41% faster |
| mixed80_20_k1024_g16_v32 | 33.78 us |  19.06 us | 44% faster |

Build+commit smoke means:

| Backend      | Case                     |   Checked | Canonical |        Delta |
| ------------ | ------------------------ | --------: | --------: | -----------: |
| in_memory    | puts_k1024_g16_v32       | 118.33 us | 107.38 us |    9% faster |
| in_memory    | mixed80_20_k1024_g16_v32 | 136.36 us | 122.27 us |   10% faster |
| sqlite_temp  | puts_k1024_g16_v32       |   1.01 ms |   1.36 ms | noisy/slower |
| sqlite_temp  | mixed80_20_k1024_g16_v32 | 939.37 us | 919.37 us |    2% faster |
| redb_temp    | puts_k1024_g16_v32       |  15.73 ms |  17.18 ms | noisy/slower |
| redb_temp    | mixed80_20_k1024_g16_v32 |  15.34 ms |  15.25 ms |         flat |
| rocksdb_temp | puts_k1024_g16_v32       | 242.54 us | 223.17 us |    8% faster |
| rocksdb_temp | mixed80_20_k1024_g16_v32 | 258.44 us | 223.18 us |   14% faster |

Interpretation:

```text
This confirms the split is real:

Checked staging:
  useful for defensive construction and tests
  pays per-mutation duplicate HashMap cost

Canonical staging:
  useful for domain stores that already emit final mutations
  construction is ~40-45% faster in these synthetic cases
  improves in_memory and rocksdb build+commit by ~8-14%

SQLite and redb remain dominated by backend transaction/commit behavior in this
smoke shape. The storage construction win is too small relative to their
backend floor to show up consistently.
```

API conclusion:

```text
Keep backend_v2 write API boring:
  put_many
  delete_many
  commit
  rollback

Keep the storage_v2 split:
  checked write-set construction for safety
  canonical write-set construction for domain-store hot paths

The next likely Big-O/backend-family cuts are:
  1. delete_range extension and fallback safety matrix
  2. SQLite prepared statement caching inside backend write/read txns
  3. domain-shaped write workloads to prove canonical builders map to real Lix
     stores rather than only synthetic mutation vectors
```

## 2026-05-16 - Missing primitive benchmark lanes

Added benchmark groups for the next suspected API cuts:

```text
storage_v2/delete_range_fallback/<backend>/delete_prefix_q{100,1000,10000}
storage_v2/scan_chunking/<backend>/{materialized,visit}/drain_range_q10000_chunk{1,10,100}
storage_v2/durability_matrix/<backend>/{default,durable,relaxed}/puts_k1024_g16_v32
```

Command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/(delete_range_fallback|scan_chunking|durability_matrix)/(in_memory|sqlite_temp|redb_temp|rocksdb_temp)'
```

Smoke baseline, selected means:

| Group                        | in_memory | sqlite_temp | redb_temp | rocksdb_temp |
| ---------------------------- | --------: | ----------: | --------: | -----------: |
| delete prefix q100           |  13.45 us |   815.16 us |  16.71 ms |    160.13 us |
| delete prefix q1000          | 155.17 us |     1.39 ms |  18.93 ms |    637.65 us |
| delete prefix q10000         |   1.65 ms |     6.18 ms |  20.66 ms |      2.91 ms |
| drain chunk1 materialized    |   3.08 ms |     6.57 ms |   6.98 ms |     12.17 ms |
| drain chunk1 visit           |   2.31 ms |     5.89 ms |   6.59 ms |     12.34 ms |
| drain page10 materialized    | 521.04 us |     1.32 ms |   1.12 ms |      2.32 ms |
| drain page10 visit           | 432.15 us |     1.16 ms |   1.12 ms |      2.22 ms |
| drain page100 materialized   | 273.27 us |   766.02 us | 523.79 us |      1.31 ms |
| drain page100 visit          | 236.74 us |   692.17 us | 455.18 us |      1.45 ms |
| durability default k1024/g16 |  89.22 us |   956.24 us |  16.65 ms |    251.74 us |
| durability durable k1024/g16 |  90.92 us |   971.93 us |  16.43 ms |    248.72 us |
| durability relaxed k1024/g16 |  91.61 us |   999.03 us |  17.62 ms |    225.68 us |

Interpretation:

```text
delete_range:
  The fallback path is now visible and scales roughly with deleted rows for
  in_memory/sqlite/rocksdb. redb is dominated by transaction/commit cost in
  this smoke shape. This is enough evidence to benchmark a native
  delete_range/clear_prefix extension next, especially for SQLite and RocksDB.

scan scan chunking:
  Tiny chunk sizes are expensive across all real backends because storage
  resumes by issuing many range scans. chunk1 is the stress case. page10/page100
  are much healthier. This creates the evidence lane needed before considering
  an optional cursorized scan extension.

durability:
  WriteOptions::durability is either currently unmapped or within smoke noise
  for these backends. Before drawing conclusions about redb/sqlite write cost,
  wire durability policy explicitly and rerun this group.
```

API conclusion:

```text
Do not change the read core yet.

The next backend/storage API experiment with clear evidence is:
  required delete_range(range) on BackendWrite
  storage_v2 delete_prefix/delete_range/clear_space helpers
  compare backend primitive against scan key-only chunks + delete_many fallback

Cursorized scans remain a second candidate, but only if domain workloads
actually drain large ranges with very small chunk sizes.
```

## 2026-05-16 - Required backend delete_range core

Changed `backend_v2::BackendWrite` to require:

```rust
fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError>;
```

This makes exact range deletion a v0 backend correctness primitive, not an
optional capability. The conformance suite now checks:

```text
baseline::delete_range_removes_exact_range
baseline::delete_range_applies_after_staged_puts
baseline::put_many_applies_after_delete_range
```

The staged-write tests matter because a backend write transaction must apply
`put_many(...); delete_range(...); put_many(...)` as one ordered mutation
stream. A backend that implements range delete by scanning only committed state
can otherwise miss keys staged earlier in the same write, and a backend using a
native range tombstone must still let later puts survive.

Implemented backend behavior:

```text
in_memory:
  removes overlay puts in range and stages deletes for base visible keys

sqlite_temp:
  one indexed DELETE FROM entries WHERE key range inside the write transaction

redb_temp:
  collects keys from the write transaction's table range and removes them

rocksdb_temp:
  uses WriteBatch::delete_range for finite raw-byte ranges after normalizing
  inclusive/exclusive bounds; falls back to exact point deletes for unbounded
  upper ranges
```

Native delete_range smoke:

| Backend      |      q100 |     q1000 |   q10000 |
| ------------ | --------: | --------: | -------: |
| in_memory    |   9.40 us | 120.39 us |  1.32 ms |
| sqlite_temp  | 779.33 us |   1.13 ms |  3.20 ms |
| redb_temp    |  15.14 ms |  20.15 ms | 20.33 ms |
| rocksdb_temp | 130.37 us | 625.16 us |  3.52 ms |

Command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/delete_range_native/(in_memory|sqlite_temp|redb_temp|rocksdb_temp)'
```

Interpretation:

```text
SQLite gets the clearest win over the fallback lane at larger ranges because it
can issue one indexed DELETE. In-memory also improves by avoiding storage-level
scan/materialize/delete lowering.

redb remains dominated by commit/durability cost in this smoke shape.

rocksdb q100/q1000 are roughly comparable to fallback because the first exact
implementation still collected concrete keys to preserve staged-put semantics.
That motivated the follow-up below.
```

Follow-up optimization before committing:

```text
rocksdb_temp:
  changed finite ranges to WriteBatch::delete_range after translating Lix
  bounds into RocksDB's half-open [from, to) shape:
    Included(lower) -> lower
    Excluded(lower) -> lower || 0x00
    Excluded(upper) -> upper
    Included(upper) -> upper || 0x00

redb_temp:
  tried retain_in(), but smoke regressed:
    q100   ~18.15 ms vs prior ~15.14 ms
    q1000  ~21.84 ms vs prior ~20.15 ms
    q10000 ~41.34 ms vs prior ~20.33 ms
  kept the measured-faster range collect + remove implementation.
```

RocksDB optimized smoke:

| Backend      |      q100 |     q1000 |    q10000 |
| ------------ | --------: | --------: | --------: |
| rocksdb_temp | 103.80 us | 119.74 us | 147.97 us |

RocksDB delta from the first exact implementation:

| Case   |    Before |     After |      Delta |
| ------ | --------: | --------: | ---------: |
| q100   | 130.37 us | 103.80 us | 20% faster |
| q1000  | 625.16 us | 119.74 us | 81% faster |
| q10000 |   3.52 ms | 147.97 us | 96% faster |
```

## 2026-05-16 - storage_v2 delete range helpers

Added storage-facing helpers on `StorageContext`:

```rust
delete_range(space, range, opts)
delete_prefix(space, prefix, opts)
clear_space(space, opts)
```

Each helper opens one backend write transaction, encodes the logical
`StorageSpace` range into the physical byte-key space, calls exactly one
backend `delete_range`, and commits.

Shape tests now assert:

```text
delete_range  -> one backend delete_range, zero delete_many calls
delete_prefix -> one backend delete_range, zero delete_many calls
clear_space   -> one backend delete_range, zero delete_many calls
```

Command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/delete_range_storage_helpers/(in_memory|sqlite_temp|redb_temp|rocksdb_temp)'
```

Smoke baseline, selected means:

| Backend      | helper        |      q100 |     q1000 |        q10000 |
| ------------ | ------------- | --------: | --------: | ------------: |
| in_memory    | delete_range  |   9.07 us | 120.25 us |       1.29 ms |
| in_memory    | delete_prefix |   9.64 us | 119.34 us |       1.27 ms |
| in_memory    | clear_space   |   9.80 us | 124.35 us |       1.28 ms |
| sqlite_temp  | delete_range  | 796.17 us |   1.16 ms |       3.29 ms |
| sqlite_temp  | delete_prefix | 779.65 us |   1.10 ms |       3.26 ms |
| sqlite_temp  | clear_space   | 669.59 us |   1.14 ms | 8.51 ms noisy |
| redb_temp    | delete_range  |  17.81 ms |  21.81 ms |      19.49 ms |
| redb_temp    | delete_prefix |  14.74 ms |  19.35 ms |      20.60 ms |
| redb_temp    | clear_space   |  20.81 ms |  19.39 ms |      24.56 ms |
| rocksdb_temp | delete_range  |  87.96 us | 152.54 us |     142.13 us |
| rocksdb_temp | delete_prefix |  98.38 us | 110.40 us |     116.56 us |
| rocksdb_temp | clear_space   | 131.86 us | 118.53 us |     113.00 us |

Interpretation:

```text
The storage helper path is now close to the direct native delete_range path.
The remaining cost is backend transaction/open/commit behavior plus a tiny
amount of logical-to-physical range encoding.

This completes the storage-facing half of making delete_range a required
backend primitive. Domain stores can now clear logical ranges, prefixes, and
spaces without scan/materialize/delete fallback.
```

## 2026-05-16 - delete_range focused scorecard

Ran the focused scorecard for the delete-range cut only:

```text
delete_range_fallback
delete_range_native
delete_range_storage_helpers/delete_range
```

across:

```text
in_memory
sqlite_temp
redb_temp
rocksdb_temp
```

Command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/delete_range_(fallback|native|storage_helpers)/(in_memory|sqlite_temp|redb_temp|rocksdb_temp)'
```

Criterion mean estimates:

| Backend      | Case   |  fallback |    native | storage helper |
| ------------ | ------ | --------: | --------: | -------------: |
| in_memory    | q100   |  12.90 us |   8.64 us |        8.79 us |
| in_memory    | q1000  | 161.20 us | 105.56 us |      111.23 us |
| in_memory    | q10000 |   1.59 ms |   1.25 ms |        1.36 ms |
| sqlite_temp  | q100   | 790.15 us | 722.32 us |      671.77 us |
| sqlite_temp  | q1000  |   1.44 ms |   1.16 ms |        1.12 ms |
| sqlite_temp  | q10000 |   5.83 ms |   3.16 ms |        3.25 ms |
| redb_temp    | q100   |  14.56 ms |  16.74 ms |       16.31 ms |
| redb_temp    | q1000  |  18.90 ms |  19.62 ms |       19.31 ms |
| redb_temp    | q10000 |  20.08 ms |  19.73 ms |       20.92 ms |
| rocksdb_temp | q100   | 136.70 us | 113.72 us |       91.14 us |
| rocksdb_temp | q1000  | 550.07 us | 103.02 us |       92.67 us |
| rocksdb_temp | q10000 |   2.96 ms | 107.85 us |      107.01 us |

Delta vs fallback:

| Backend      | Case   |     native | storage helper |
| ------------ | ------ | ---------: | -------------: |
| in_memory    | q100   | 33% faster |     32% faster |
| in_memory    | q1000  | 35% faster |     31% faster |
| in_memory    | q10000 | 21% faster |     14% faster |
| sqlite_temp  | q100   |  9% faster |     15% faster |
| sqlite_temp  | q1000  | 19% faster |     22% faster |
| sqlite_temp  | q10000 | 46% faster |     44% faster |
| redb_temp    | q100   | 15% slower |     12% slower |
| redb_temp    | q1000  |  4% slower |      2% slower |
| redb_temp    | q10000 |  2% faster |      4% slower |
| rocksdb_temp | q100   | 17% faster |     33% faster |
| rocksdb_temp | q1000  | 81% faster |     83% faster |
| rocksdb_temp | q10000 | 96% faster |     96% faster |

Interpretation:

```text
The delete_range hard cut is validated for in_memory, SQLite, and especially
RocksDB. Storage helper overhead is effectively negligible against the backend
primitive; the helper sometimes benchmarks faster than direct native due to
smoke noise and fixture variance.

redb is not improved by the primitive in this smoke shape because its range
delete implementation is still range iteration/removal plus commit durability.
The scorecard reinforces that redb's next meaningful work is durability policy
or a better redb-specific range deletion primitive if the crate exposes one
later.

For RocksDB, the native range tombstone shape is the big win:
  q10000 fallback ~2.96 ms
  q10000 storage helper ~107 us

This completes the delete-range API/storage cut from a performance perspective.
```

## 2026-05-17 - scan chunked-scan cursor probe

Added chunking lanes to decide whether a cursorized scan API is worth the
backend/lifetime complexity:

```text
drain_range_q10000_single
drain_range_q10000_chunk1
drain_range_q10000_chunk10
drain_range_q10000_chunk100
drain_prefix_q10000_chunk10
drain_prefix_q10000_single
```

Command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/scan_chunking'
```

Criterion mean estimates, visitor path:

| Backend      | range single | range chunk1 | range chunk10 | range chunk100 | prefix single | prefix chunk10 |
| ------------ | -----------: | -----------: | ------------: | -------------: | ------------: | -------------: |
| in_memory    |    189.40 us |      2.54 ms |     438.68 us |      209.59 us |     191.62 us |      443.69 us |
| sqlite_temp  |    464.68 us |      5.54 ms |       1.64 ms |      599.44 us |     483.30 us |        1.03 ms |
| redb_temp    |    377.65 us |      6.54 ms |       1.02 ms |      463.86 us |     374.97 us |        1.00 ms |
| rocksdb_temp |      1.13 ms |     11.85 ms |       2.33 ms |        1.23 ms |       1.07 ms |        2.22 ms |

Ratios versus single-drain visitor baseline:

| Backend      | range chunk1 | range chunk10 | range chunk100 | prefix chunk10 |
| ------------ | -----------: | ------------: | -------------: | -------------: |
| in_memory    |        13.4x |          2.3x |           1.1x |           2.3x |
| sqlite_temp  |        11.9x |          3.5x |           1.3x |           2.1x |
| redb_temp    |        17.3x |          2.7x |           1.2x |           2.7x |
| rocksdb_temp |        10.5x |          2.1x |           1.1x |           2.1x |

Interpretation:

```text
The repeated-resume scan chunking cost is real. Chunk size 1 is 10-17x slower
than single-drain on every backend. Chunk size 10 is still 2-3.5x slower across
all backends. Chunk size 100 is close to single-drain, so large chunks do not
justify cursor complexity.

This validates cursorized scan as the next API candidate if Lix has hot paths
that deeply chunk ranges/prefixes with small chunk sizes. Keep visit_range as the
required simple primitive; add cursoring as an extension or storage-selected
fast path only if domain traces show small-chunk drains are common.
```

## 2026-05-17 - required backend scan cursor API

Implemented the cursor cut:

```text
BackendRead:
  type ScanCursor<'a>
  open_scan_cursor(range, opts)

BackendScanCursor:
  visit_next(limit_rows, visitor)

visit_range:
  default one-shot convenience over open_scan_cursor + visit_next
```

Storage now exposes backend/read-scope-local scan cursors and the scan chunking
bench includes a `cursor_visit` lane. The current real backend implementations
use a buffered cursor baseline, so this measures whether the API shape is worth
keeping before deeper native iterator/statement cursor work.

Command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'storage_v2/scan_chunking'
```

Criterion mean estimates:

| Backend      | range single visit | range single cursor | range chunk1 visit | range chunk1 cursor | range chunk10 visit | range chunk10 cursor | prefix chunk10 visit | prefix chunk10 cursor |
| ------------ | -----------------: | ------------------: | -----------------: | ------------------: | ------------------: | -------------------: | -------------------: | --------------------: |
| in_memory    |          224.89 us |           233.98 us |            3.00 ms |           274.59 us |           479.21 us |            245.81 us |            513.50 us |             230.85 us |
| sqlite_temp  |          523.69 us |           581.09 us |            6.76 ms |           611.95 us |             1.19 ms |            565.78 us |              1.14 ms |             562.20 us |
| redb_temp    |          407.37 us |           450.49 us |            7.04 ms |           479.63 us |             1.26 ms |            447.12 us |              1.08 ms |             442.75 us |
| rocksdb_temp |            1.20 ms |             1.24 ms |           12.45 ms |             1.25 ms |             2.39 ms |              1.24 ms |              2.43 ms |               1.31 ms |

Speedup of `cursor_visit` over repeated `visit`:

| Backend      | range single | range chunk1 | range chunk10 | range chunk100 | prefix single | prefix chunk10 |
| ------------ | -----------: | -----------: | ------------: | -------------: | ------------: | -------------: |
| in_memory    |        0.96x |        10.9x |          1.9x |           1.0x |         0.90x |           2.2x |
| sqlite_temp  |        0.90x |        11.1x |          2.1x |           1.2x |         0.90x |           2.0x |
| redb_temp    |        0.90x |        14.7x |          2.8x |           1.2x |         0.90x |           2.4x |
| rocksdb_temp |        0.97x |         9.9x |          1.9x |           1.1x |         1.09x |           1.8x |

Interpretation:

```text
The cursor API is worth it for small-chunk drains. Chunk size 1 improves about
10-15x across all four backends, and chunk size 10 improves about 1.8-2.8x.

Single-shot scans are flat to slightly slower because cursor setup adds a small
adapter cost and, for the current baseline implementations, some backends
materialize cursor rows. That is acceptable because callers should keep using
visit_range for one-shot scans.

The next optimization is backend-local native cursor implementation, not another
storage API cut:

  SQLite: keep a prepared statement / Rows cursor instead of buffering.
  redb: keep the range iterator alive when lifetime ergonomics allow it.
  RocksDB: keep an iterator and call next across chunks.
  in_memory: keep a range iterator or lightweight borrowed row cursor.

The API shape has earned its place. The implementation still has room to become
more native per backend.
```

## 2026-05-17 - backend-local native scan cursor pass

Implemented the first backend-local cursor optimization without changing the
storage/backend API:

```text
in_memory:
  Flat snapshots now keep a borrowed BTreeMap range iterator across chunks.
  Layered snapshots still fall back to BufferedScanCursor.

rocksdb_temp:
  Scan cursors now keep a RocksDB iterator and a one-row pending slot across
  chunks instead of pre-buffering the whole scan.

sqlite_temp / redb_temp:
  Left buffered for now. Their safe Rust APIs expose Rows/range iterators that
  borrow a Statement/table, which would require self-referential storage or
  unsafe plumbing to keep alive as a backend cursor.
```

Baseline command before the implementation and repeated after:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'scan_chunking/.*/cursor_visit/drain_range_q10000_chunk10'
```

Criterion mean estimates:

| Backend      | Case     | Before buffered cursor | After native/local cursor | Delta |
| ------------ | -------- | ---------------------: | ------------------------: | ----: |
| in_memory    | chunk10  |              213.07 us |                  23.50 us | 9.1x faster |
| in_memory    | chunk100 |              200.45 us |                  21.55 us | 9.3x faster |
| sqlite_temp  | chunk10  |              499.13 us |                 546.75 us | 10% slower/noise |
| sqlite_temp  | chunk100 |              500.05 us |                 542.61 us | 9% slower/noise |
| redb_temp    | chunk10  |              392.52 us |                 436.34 us | 11% slower/noise |
| redb_temp    | chunk100 |              397.06 us |                 446.99 us | 13% slower/noise |
| rocksdb_temp | chunk10  |                1.12 ms |                   1.03 ms | 1.1x faster |
| rocksdb_temp | chunk100 |                1.25 ms |                   1.00 ms | 1.2x faster |

Interpretation:

```text
The in-memory backend had the largest real win because the previous cursor
materialized 10k rows before returning the first chunk. Keeping the BTreeMap
range iterator cuts that overhead almost entirely on flat snapshots.

RocksDB improves modestly. The scan is still engine/value iteration dominated,
but keeping the iterator alive avoids the full pre-buffering pass and row
materialization.

SQLite and redb were intentionally unchanged; the small regressions are
Criterion/noise or shared-code effects from rebuilding the bench binary. Do not
force unsafe self-referential statement/table cursors yet. If these backends need
native cursors later, use a backend-specific safe abstraction or an explicit
unsafe wrapper with tests around drop order and snapshot lifetime.
```

## 2026-05-17 - callback-scoped cursor hard cut

Changed backend scan cursors from returned cursor objects to callback-scoped
cursors:

```text
BackendRead::with_scan_cursor(range, opts, |cursor| { ... })
BackendScanCursor::visit_next(limit_rows, &mut dyn ScanVisitor)
```

This lets SQLite keep a prepared statement + `Rows` cursor and redb keep a table
range iterator inside the callback without self-referential structs or unsafe
drop-order plumbing. Storage now exposes callback-scoped cursor helpers as well.

Command before and after:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'scan_chunking/.*/cursor_visit/drain_range_q10000_chunk10'
```

Criterion mean estimates:

| Backend      | Case     | Before returned cursor | After callback cursor | Delta |
| ------------ | -------- | ---------------------: | --------------------: | ----: |
| in_memory    | chunk10  |               21.93 us |              32.94 us | 1.5x slower |
| in_memory    | chunk100 |               20.65 us |              28.81 us | 1.4x slower |
| sqlite_temp  | chunk10  |              643.84 us |             344.86 us | 1.9x faster |
| sqlite_temp  | chunk100 |              602.85 us |             321.16 us | 1.9x faster |
| redb_temp    | chunk10  |              447.42 us |             413.05 us | ~neutral |
| redb_temp    | chunk100 |              411.39 us |             424.51 us | ~neutral |
| rocksdb_temp | chunk10  |              912.99 us |               1.00 ms | 1.1x slower |
| rocksdb_temp | chunk100 |              904.24 us |               1.03 ms | 1.1x slower |

Interpretation:

```text
The hard cut succeeds at the primary goal: SQLite now uses native statement rows
and improves about 1.9x on cursor chunk drains. redb is roughly neutral after
moving to a scoped native range iterator.

The cost is visible on in_memory and RocksDB because the callback-scoped cursor
uses a dyn BackendScanCursor / dyn ScanVisitor path. That adds dispatch overhead
to backends whose native returned cursor already fit Rust lifetimes cleanly.

The next decision is whether to recover the easy-backend fast path with a
monomorphic helper while keeping callback-scoped semantics for SQLite/redb, or
accept the small universal abstraction cost for a simpler backend API.
```

## 2026-05-17 - fast monomorphic scan cursor extension

Added an optional fast scan cursor extension:

```text
BackendReadFastScan::with_fast_scan_cursor(...)
FastBackendScanCursor::visit_next_fast<V: ScanVisitor>(...)
```

The required backend API remains callback-scoped and object-safe for SQLite/redb.
The fast path is implemented for in-memory and RocksDB, where the native cursor
can borrow cleanly from the read snapshot and keep the row visitor monomorphic.

Before command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'scan_chunking/(in_memory|rocksdb_temp|sqlite_temp|redb_temp)/cursor_visit/drain_range_q10000_chunk(10|100)'
```

After command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'scan_chunking/(in_memory|rocksdb_temp)/(cursor_visit|fast_cursor_visit)/drain_range_q10000_chunk(10|100)'
```

Criterion mean estimates:

| Backend      | Case     | Required cursor before | Required cursor after | Fast cursor after | Fast vs before |
| ------------ | -------- | ---------------------: | --------------------: | ----------------: | -------------: |
| in_memory    | chunk10  |               31.50 us |              33.11 us |          25.93 us |     1.2x faster |
| in_memory    | chunk100 |               24.39 us |              28.61 us |          19.61 us |     1.2x faster |
| rocksdb_temp | chunk10  |              874.37 us |             934.78 us |         925.39 us |         neutral |
| rocksdb_temp | chunk100 |              877.87 us |             986.05 us |         891.12 us |         neutral |

Interpretation:

```text
The extension pays off clearly for in_memory, where the row loop is cheap enough
that Rust dispatch and adapter shape still matter. The fast path recovers and
slightly improves the pre-callback cursor cost while preserving the SQLite-safe
required API.

RocksDB is essentially neutral in smoke measurements. The fast path removes the
storage-side dyn visitor boundary, but RocksDB scan cost is now dominated by the
engine iterator/value path and benchmark noise rather than the storage adapter.

Do not hard-require the fast cursor extension. Keep it as an optimization
extension for backends with easy native cursor lifetimes.
```

## 2026-05-17 - consolidated single scan cursor API

Replaced the temporary required-cursor plus fast-cursor split with one scan
cursor API:

```text
BackendRead::ScanCursor<'cursor>
BackendRead::with_scan_cursor(...)
BackendScanCursor::visit_next<V: ScanVisitor + ?Sized>(...)
```

This keeps the callback-scoped lifetime model needed by SQLite/redb, while
making the emitted-row visitor generic on the one required cursor path. The
backend API is back to "one API, one job": open a scan cursor and visit the next
chunk.

Command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'scan_chunking/(in_memory|rocksdb_temp|sqlite_temp|redb_temp)/cursor_visit/drain_range_q10000_chunk(10|100)'
```

Criterion mean estimates:

| Backend      | Case     | Callback dyn cursor | Unified cursor | Delta |
| ------------ | -------- | ------------------: | -------------: | ----: |
| in_memory    | chunk10  |            33.11 us |       24.35 us | 1.4x faster |
| in_memory    | chunk100 |            28.61 us |       21.04 us | 1.4x faster |
| sqlite_temp  | chunk10  |           304.84 us |      336.69 us | 10% slower |
| sqlite_temp  | chunk100 |           292.72 us |      307.16 us | 5% slower |
| redb_temp    | chunk10  |           368.27 us |      307.28 us | 1.2x faster |
| redb_temp    | chunk100 |           343.36 us |      304.08 us | 1.1x faster |
| rocksdb_temp | chunk10  |           934.78 us |        1.04 ms | 11% slower |
| rocksdb_temp | chunk100 |           986.05 us |        1.04 ms | 6% slower |

Interpretation:

```text
The unified cursor removes the conceptual split and recovers the in-memory
benefit on the required path. redb also improves in this smoke lane. SQLite and
RocksDB are slightly slower in this run; those lanes are now dominated by
backend-local statement/iterator/value costs and smoke-run noise more than the
storage API shape.

Given the API simplicity win, keep the single associated cursor unless a fuller
scorecard shows a durable RocksDB regression large enough to justify more
complexity.
```

## 2026-05-17 - full scan chunking scorecard after unified cursor

Ran the full scan-chunking smoke scorecard after consolidating the scan cursor API.

Command:

```sh
STORAGE_V2_BENCH_SMOKE=1 \
cargo bench -p lix_engine --features storage-benches --bench storage_v2 \
  'scan_chunking'
```

Representative Criterion mean estimates for `cursor_visit`:

| Backend      | Case                  | Mean |
| ------------ | --------------------- | ---: |
| in_memory    | range single          | 18.75 us |
| in_memory    | range chunk1          | 115.71 us |
| in_memory    | range chunk10         | 22.77 us |
| in_memory    | range chunk100        | 18.59 us |
| in_memory    | prefix chunk10        | 23.40 us |
| in_memory    | prefix single         | 18.35 us |
| sqlite_temp  | range single          | 276.44 us |
| sqlite_temp  | range chunk1          | 517.81 us |
| sqlite_temp  | range chunk10         | 296.05 us |
| sqlite_temp  | range chunk100        | 276.16 us |
| sqlite_temp  | prefix chunk10        | 316.50 us |
| sqlite_temp  | prefix single         | 276.95 us |
| redb_temp    | range single          | 270.42 us |
| redb_temp    | range chunk1          | 499.33 us |
| redb_temp    | range chunk10         | 282.21 us |
| redb_temp    | range chunk100        | 272.27 us |
| redb_temp    | prefix chunk10        | 274.93 us |
| redb_temp    | prefix single         | 263.49 us |
| rocksdb_temp | range single          | 901.34 us |
| rocksdb_temp | range chunk1          | 931.04 us |
| rocksdb_temp | range chunk10         | 890.21 us |
| rocksdb_temp | range chunk100        | 905.94 us |
| rocksdb_temp | prefix chunk10        | 927.62 us |
| rocksdb_temp | prefix single         | 877.56 us |

Interpretation:

```text
The full scan scorecard vindicates the unified cursor API. Cursor drains are the
winning shape for deep/chunked scans, especially compared with materialized or
visit paths that re-open the range for every chunk.

RocksDB did not show a durable regression in the full scorecard. Its cursor
lanes improved versus the prior Criterion baseline across single, chunked, and
prefix drains. Do not profile RocksDB scan cursor for a regression yet.

The scan API is likely good enough to freeze for now: one callback-scoped cursor
API, associated cursor type, generic visit_next visitor, storage-owned prefix
lowering and stats.
```
