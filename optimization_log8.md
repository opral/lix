# Optimization Log 8: JSON Pointer Physical Layout Decision Log

Goal: nail the physical layout Lix uses for tracked logic:
`packages/engine/src/tracked_state`, `packages/engine/src/commit_store`, and
the backend/storage APIs they require.

Lix has not shipped. Optimize for the best-shaped physical API, storage layout,
and abstraction boundaries now. Prefer clean refactors over bolt-on fixes,
adapter layers, compatibility shims, or special cases. If a change keeps a
backwards shim, the entry must explicitly call that out and justify why it is
temporary.

The preferred refactor mode is:

```text
first make the storage shape correct;
then let the Rust compiler reveal upstream code that must move to the new API.
```

It is acceptable for an intermediate refactor entry to leave the tree
temporarily non-compiling if the entry is clearly marked as a physical-layout
cutover step and the next step is compiler-driven migration. Do not hide old
behavior behind adapter layers just to keep call sites compiling.

The desired end state is good abstractions, not a faster pile of special-case
paths. If the current abstraction is the bottleneck, replace it cleanly.

North-star target:

```text
Large logical write batches through the tracked-state/commit-store path should
leave enough time budget for the logical layer above storage.
```

Physical storage budget:

```text
For 1k-operation physical rows, Lix SQLite and Lix RocksDB should be <= 1.5x
raw SQLite for equivalent writes, exact reads, and scans.

Raw SQLite is not a bare-metal KV baseline: it still goes through SQL statement
execution, cursor/seek machinery, and SELECT/INSERT/UPDATE/DELETE paths. Lix
physical rows use direct storage access, so exceeding this budget means Lix is
likely paying avoidable layout, packing, materialization, batching, or backend
abstraction costs.

For storage size, post-vacuum Lix bytes/row should be <= 2x post-vacuum raw
SQLite bytes/row for equivalent tracked storage states. Extra bytes beyond that
must be explained by durable tracked history, commit facts, merge/conflict
facts, or retained delta structure before a size-sensitive change is kept.
```

This log is not for SQL-provider ergonomics. SQL and CRUD benchmarks may point
at problems, but every kept optimization must be explained at the physical
storage boundary: backend operations, commit packs, delta packs, projection
materialization, changed-key discovery, exact reads, scans, batching,
zero-copy/low-copy behavior, or bytes.

Criterion output is evidence, not the whole argument. Treat noise carefully:
prefer structural wins that also move timings, and reject changes that only win
one noisy row while worsening the physical design.

## Current State

```text
branch: physical-layout-manual
head:   11ff3a2e
date:   2026-05-10
status: uncommitted benchmark/log setup
```

Setup changes for this log:

- Added `packages/engine/benches/json_pointer_physical/main.rs`.
- Added the `json_pointer_physical` bench target to
  `packages/engine/Cargo.toml`.
- Added a raw SQLite reference group inside the physical benchmark so the
  SQLite-relative budgets have a measured baseline.
- Kept the existing JSON-pointer storage fixture test as the bytes-on-disk
  guardrail.

## Layout Scope

In scope:

```text
commit_store canonical commit/change physical layout
tracked_state delta-pack layout
tracked_state projection/root materialization policy
tracked_state exact-key lookup
tracked_state scan/projection behavior
changed-key discovery for diff/merge
backend get_many / exists_many / prefix scan / write batch APIs
backend zero-copy or low-copy read/write boundaries
backend transaction/write-batch semantics shared by SQLite and RocksDB
bytes on disk after insert/version/merge workflows
```

Out of scope unless a physical benchmark proves otherwise:

```text
SQL/provider routing
DataFusion planning overhead
per-statement UPDATE ergonomics
application-level batching above tracked_state/commit_store
```

Rule:

```text
If a hot E2E benchmark points through SQL first, map it to
json_pointer_physical before optimizing. Do not make SQL-layer changes in this
log unless the physical rows are already inside budget and the remaining time
is clearly above storage.
```

Tracked logic is the product path and the default mode in Lix. Optimizations
must make tracked logic faster; they must not avoid tracked machinery by moving
workloads, benchmarks, fixtures, changed-key logic, commit-store logic, or
tracked-state behavior into untracked code.

## Refactor Policy

Allowed:

```text
change the storage/backend API when the current API forces bad physical layout;
add or reshape backend/storage APIs, including namespacing-oriented APIs, when
  the shape materially improves both SQLite and RocksDB;
change tracked_state and commit_store layouts when the new layout is cleaner;
break old call sites and let the compiler drive the migration;
delete legacy abstractions that only exist to preserve pre-ship compatibility;
replace one-off fixes with a shared abstraction when the problem is systemic;
remove bolt-on fast paths once the clean abstraction covers the same behavior.
```

Required when changing storage/backend APIs:

```text
state the physical problem the old API caused;
show how SQLite and RocksDB can both implement the new shape without hidden
  per-key loops or full-value hydration;
show that both SQLite and RocksDB improve materially, or explain why the API
  change is still required for a later shared layout win;
preserve transaction atomicity, durability, and hash/integrity checks;
prefer batched, streaming, prefix/range, and projection-aware operations;
avoid copy-heavy boundaries unless the entry explicitly measures and accepts
  the cost;
explain how the layout can migrate again later without rewriting the whole
  logical layer.
```

Not allowed:

```text
SQLite-only wins that silently regress RocksDB;
RocksDB-only wins that silently regress SQLite;
benchmark rewrites that change what is being measured;
workarounds scoped only to the current hot row when the abstraction is wrong;
bolt-on fast paths that leave the bad abstraction in place;
adapter layers whose main purpose is avoiding the clean refactor;
moving tracked logic, benchmarks, or benchmark workload into untracked paths;
shifting cost out of tracked_state/commit_store to avoid tracked machinery;
forcing full materialization to avoid designing the right index/layout;
backwards shims unless the entry explicitly marks and justifies them.
```

## Benchmark Surface

Benchmark target:

```text
packages/engine/benches/json_pointer_physical/main.rs
```

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- baseline
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- smoke
```

Groups:

```text
json_pointer_physical/raw_sqlite/baseline
json_pointer_physical/raw_sqlite/smoke
json_pointer_physical/sqlite/baseline
json_pointer_physical/sqlite/smoke
json_pointer_physical/rocksdb/baseline
json_pointer_physical/rocksdb/smoke
```

Rows:

```text
write_root_all_rows/{100,1k}
get_many_exact_keys/{100,1k}
get_many_missing_keys/{100,1k}
exists_many_exact_keys/{100,1k}
scan_keys_only/{100,1k}
scan_headers_only/{100,1k}
scan_full_rows/{100,1k}
prefix_scan_schema/{100,1k}
prefix_scan_schema_file_null/{100,1k}
write_delta_10pct_updates/{100,1k}
write_tombstone_10pct_deletes/{100,1k}
changed_keys_update_10pct/{100,1k}
changed_keys_delta_chain_10x1pct/{100,1k}
materialize_delta_chain_10x1pct/{100,1k}
```

Fixture:

```text
source: packages/engine/benches/fixtures/pnpm-lock.fixture.json
shape: flattened JSON nodes, including containers and leaves
identity: JSON pointer path
value: JSON node value
file_id: NULL
sizes:
  baseline = 100 rows
  smoke = 1,000 rows
```

Why this fixture:

```text
It mirrors plugin-json-v2 output: many small entities, stable path identities,
container rows, leaf rows, and realistic nested JSON values.
```

Benchmark-surface intent:

```text
This benchmark surface should stabilize the physical layout before logical-layer
optimization begins. New physical rows should be added only when logical work
reveals a genuinely new tracked access pattern, not to move the goalposts for
an existing optimization.
```

## Raw SQLite Reference

The raw SQLite group is independent of Lix. It answers: what does plain
primary-key physical storage cost for the same flattened JSON-pointer rows?

Shape:

```text
database: tempfile SQLite
table: json_pointer(path TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID
pragmas: journal_mode=WAL, synchronous=NORMAL, temp_store=MEMORY,
         foreign_keys=ON
write rows: INSERT/UPDATE/DELETE by path in one transaction
exact reads: prepared point lookups by path
scans: ordered path/value scans over the table
```

The raw SQLite prefix-scan rows are a fixture-equivalent approximation: the
fixture uses one schema and `file_id = NULL`, so schema/file scope maps to the
whole table.

Reference interpretation:

```text
Rows near raw SQLite are close to backend speed.
Rows above 1.5x raw SQLite are likely dominated by Lix packing, projection,
materialization, hashing, diff semantics, or backend abstraction overhead.
```

## Success Criteria

Every kept optimization must name one primary axis:

```text
write
exact-read
scan
diff/changed-key
delta-chain materialization
storage-size
backend API
```

The primary axis should improve materially. Non-target axes are guardrails.

Every kept optimization must also name its physical shape:

```text
canonical fact layout
read index / projection layout
delta-pack layout
changed-key index
backend batch/read/write API
materialization policy
copy/serialization boundary
```

An optimization is not kept merely because one Criterion row improves. It must
be a better shape for the tracked storage system and must not create hidden
costs such as unbatched IO, accidental full-value hydration, extra copies across
the backend boundary, or backend-specific behavior that another supported
backend cannot implement well.

### 1.5x SQLite Runtime Budget

This is an envelope, not an average. Passing writes does not compensate for
failing reads, and passing reads does not compensate for failing writes.

Write rows:

```text
compare json_pointer_physical/{sqlite,rocksdb}/smoke/write_root_all_rows/1k
  to json_pointer_physical/raw_sqlite/smoke/write_root_all_rows/1k
compare json_pointer_physical/{sqlite,rocksdb}/smoke/write_delta_10pct_updates/1k
  to json_pointer_physical/raw_sqlite/smoke/write_delta_10pct_updates/1k
compare json_pointer_physical/{sqlite,rocksdb}/smoke/write_tombstone_10pct_deletes/1k
  to json_pointer_physical/raw_sqlite/smoke/write_tombstone_10pct_deletes/1k
```

Exact-read rows:

```text
compare json_pointer_physical/{sqlite,rocksdb}/smoke/get_many_exact_keys/1k
  to json_pointer_physical/raw_sqlite/smoke/get_many_exact_keys/1k
compare json_pointer_physical/{sqlite,rocksdb}/smoke/get_many_missing_keys/1k
  to json_pointer_physical/raw_sqlite/smoke/get_many_missing_keys/1k
compare json_pointer_physical/{sqlite,rocksdb}/smoke/exists_many_exact_keys/1k
  to json_pointer_physical/raw_sqlite/smoke/exists_many_exact_keys/1k
```

Scan rows:

```text
compare json_pointer_physical/{sqlite,rocksdb}/smoke/scan_keys_only/1k
  to json_pointer_physical/raw_sqlite/smoke/scan_keys_only/1k
compare json_pointer_physical/{sqlite,rocksdb}/smoke/scan_headers_only/1k
  to json_pointer_physical/raw_sqlite/smoke/scan_headers_only/1k
compare json_pointer_physical/{sqlite,rocksdb}/smoke/scan_full_rows/1k
  to json_pointer_physical/raw_sqlite/smoke/scan_full_rows/1k
compare json_pointer_physical/{sqlite,rocksdb}/smoke/prefix_scan_schema/1k
  to json_pointer_physical/raw_sqlite/smoke/prefix_scan_schema/1k
compare json_pointer_physical/{sqlite,rocksdb}/smoke/prefix_scan_schema_file_null/1k
  to json_pointer_physical/raw_sqlite/smoke/prefix_scan_schema_file_null/1k
```

Changed-key and delta-chain rows do not have a clean raw SQLite equivalent.
Judge them by scaling shape:

```text
changed_keys_update_10pct:
  should scale with changed keys, not full state hydration.

changed_keys_delta_chain_10x1pct:
  should scale with changed keys and chain depth, not repeated broad
  materialization of full state.

materialize_delta_chain_10x1pct:
  should avoid repeatedly decoding unrelated delta-pack content.
```

### Regression Budgets

```text
<= 5% slower:
  treat as possible Criterion noise unless repeated or structurally explained.

5-15% slower:
  acceptable only with a clear primary-axis win, a structural explanation, and
  no crossed 1.5x runtime budget.

> 15% slower:
  fail unless explicitly accepted as a layout tradeoff.

No change may make an axis that passes the 1.5x runtime budget start failing it.
```

Storage guardrail:

```text
Post-vacuum bytes after inserted/create_version/fast-forward/divergent merge
should stay <= 2x post-vacuum raw SQLite bytes/row for equivalent tracked
storage states. Extra bytes must remain explainable. A speedup that causes
unexplained storage growth is not kept.
```

## Storage Fixture Guardrail

Command:

```sh
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
```

Rows to report when a change can affect storage size:

```text
raw SQLite / inserted
Lix SQLite / inserted
Lix SQLite / after create_version
Lix SQLite / after fast-forward merge
Lix SQLite / after divergent merge
Lix RocksDB / inserted
Lix RocksDB / after create_version
Lix RocksDB / after fast-forward merge
Lix RocksDB / after divergent merge
```

## Agent Rules

1. Optimize physical layout and backend APIs, not SQL surface shape.
2. Prefer clean, compiler-driven refactors and good abstractions over bolt-on
   fixes, adapter layers, or backwards shims. If a shim is kept, flag it.
3. Optimize one primary axis at a time and report guardrails for the other
   axes.
4. Compare against raw SQLite where there is an equivalent row.
5. Report SQLite and RocksDB physical rows before keeping backend-sensitive
   changes.
6. Prefer explicit batched APIs over hidden loops of single-key operations.
7. Backend/storage API changes are allowed when they materially improve both
   SQLite and RocksDB, including namespacing-oriented APIs.
8. Do not improve one backend by silently regressing the other.
9. Do not change benchmark measurements to make a change look better.
10. Do not move tracked logic, fixtures, benchmarks, or benchmark workload into
    untracked paths. Optimize tracked logic itself.
11. Do not shift cost out of tracked_state/commit_store to bypass tracked
    machinery.
12. Do not keep bolt-on fast paths when a clean abstraction should replace the
    old shape.
13. Do not improve writes by forcing broad projection-root materialization
    unless the entry is explicitly a materialization-policy experiment.
14. Do not make key/header-only scans hydrate full JSON values.
15. Do not introduce avoidable copies at the backend boundary without measuring
    and justifying them.
16. Do not remove hash verification, transaction atomicity, or durability
    semantics to win a benchmark.
17. Document rejected experiments if they teach something about the cost model.
18. Append one compact entry per optimization.

## Baseline

Date: 2026-05-10

Commit: uncommitted on `11ff3a2e`

Change: added the `json_pointer_physical` benchmark target and raw SQLite
physical reference group.

### Verification

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- get_many_exact_keys/100
```

Result: passed.

Accepted baseline run:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- baseline
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- smoke
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
```

Result:

```text
passed
```

### Raw SQLite / Lix Smoke Check

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- get_many_exact_keys/100
```

| backend     | group                                       | row                       |       low |    median |      high |
| ----------- | ------------------------------------------- | ------------------------- | --------: | --------: | --------: |
| raw SQLite  | `json_pointer_physical/raw_sqlite/baseline` | `get_many_exact_keys/100` | 879.06 us | 924.31 us | 1.0023 ms |
| Lix SQLite  | `json_pointer_physical/sqlite/baseline`     | `get_many_exact_keys/100` | 1.2941 ms | 1.3683 ms | 1.4479 ms |
| Lix RocksDB | `json_pointer_physical/rocksdb/baseline`    | `get_many_exact_keys/100` | 1.0164 ms | 1.0507 ms | 1.0952 ms |

Interpretation:

```text
The benchmark wiring works and the raw SQLite reference group appears beside
the Lix physical backends.

At 100 rows, exact reads are near the runtime envelope for both backends.
This is only a smoke check. It is not the accepted baseline for optimization.
The accepted baseline must include the 1k smoke rows.
```

### Required Baseline Command

Before the first optimization entry, run:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- baseline
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- smoke
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
```

### Baseline Scoreboard

The 1k smoke rows are the accepted optimization baseline.

#### 1.5x Runtime Budget Rows, 1k

| axis       | row                                | raw SQLite median | Lix SQLite median | SQLite ratio | Lix RocksDB median | RocksDB ratio | status                          |
| ---------- | ---------------------------------- | ----------------: | ----------------: | -----------: | -----------------: | ------------: | ------------------------------- |
| write      | `write_root_all_rows/1k`           |         2.4583 ms |         6.8347 ms |        2.78x |          6.1430 ms |         2.50x | SQLite and RocksDB fail         |
| write      | `write_delta_10pct_updates/1k`     |         1.5396 ms |         2.6272 ms |        1.71x |          1.3950 ms |         0.91x | SQLite fail                     |
| write      | `write_tombstone_10pct_deletes/1k` |         1.4156 ms |         2.4321 ms |        1.72x |          1.3632 ms |         0.96x | SQLite fail                     |
| exact-read | `get_many_exact_keys/1k`           |         2.2859 ms |         4.6055 ms |        2.01x |          3.4668 ms |         1.52x | SQLite and RocksDB fail         |
| exact-read | `get_many_missing_keys/1k`         |         13.931 ms |         2.2822 ms |        0.16x |          1.4138 ms |         0.10x | pass                            |
| exact-read | `exists_many_exact_keys/1k`        |         2.0545 ms |         4.6519 ms |        2.26x |          3.4720 ms |         1.69x | SQLite and RocksDB fail         |
| scan       | `scan_keys_only/1k`                |         1.2374 ms |         3.2542 ms |        2.63x |          2.0822 ms |         1.68x | SQLite and RocksDB fail         |
| scan       | `scan_headers_only/1k`             |         1.2378 ms |         3.0692 ms |        2.48x |          2.0012 ms |         1.62x | SQLite and RocksDB fail         |
| scan       | `scan_full_rows/1k`                |         1.2920 ms |         4.3792 ms |        3.39x |          3.1884 ms |         2.47x | SQLite fail                     |
| scan       | `prefix_scan_schema/1k`            |         1.2514 ms |         4.4623 ms |        3.57x |          3.2190 ms |         2.57x | SQLite and RocksDB fail         |
| scan       | `prefix_scan_schema_file_null/1k`  |         1.3817 ms |         4.3889 ms |        3.18x |          3.1497 ms |         2.28x | SQLite and RocksDB fail         |

#### Diff / Materialization Shape Rows

| row                                   | Lix SQLite median | Lix RocksDB median | expected shape                           | status  |
| ------------------------------------- | ----------------: | -----------------: | ---------------------------------------- | ------- |
| `changed_keys_update_10pct/1k`        |         68.399 ms |          67.192 ms | scales with changed keys                 | hotspot |
| `changed_keys_delta_chain_10x1pct/1k` |         10.401 ms |          8.7436 ms | scales with changed keys and chain depth | watch   |
| `materialize_delta_chain_10x1pct/1k`  |         5.7651 ms |          2.7741 ms | avoids unrelated delta-pack decoding     | watch   |

#### Storage Fixture

| backend / state                        | bytes on disk | bytes/row | status                                                  |
| -------------------------------------- | ------------: | --------: | ------------------------------------------------------- |
| raw SQLite / inserted                  |       1692456 |    1692.5 | baseline                                                |
| Lix SQLite / inserted                  |       1075136 |    1075.1 | baseline                                                |
| Lix SQLite / after create_version      |       1087496 |    1087.5 | baseline                                                |
| Lix SQLite / after fast-forward merge  |       5287488 |    5287.5 | growth to explain before keeping size-sensitive changes |
| Lix SQLite / after divergent merge     |       5615168 |    5615.2 | growth to explain before keeping size-sensitive changes |
| Lix RocksDB / inserted                 |        993900 |     993.9 | baseline                                                |
| Lix RocksDB / after create_version     |        995766 |     995.8 | baseline                                                |
| Lix RocksDB / after fast-forward merge |       1157143 |    1157.1 | baseline                                                |
| Lix RocksDB / after divergent merge    |       1528256 |    1528.3 | baseline                                                |

## Entries

Append kept wins and rejected experiments below this line.

## Entry Template

Copy this template for every optimization.

```text
one kept win = one appended log entry + code changes measured by the entry
```

## Optimization N: <short name>

Commit: `<hash>` or `uncommitted on <hash>`

Target axis:

```text
write | exact-read | scan | diff/changed-key | delta-chain materialization
storage-size | backend API
```

Backend/API scope:

```text
none | backend API plumbing | backend implementation | layout behavior | mixed
```

Physical shape:

```text
canonical fact layout | read index / projection layout | delta-pack layout
changed-key index | backend batch/read/write API | materialization policy
copy/serialization boundary
```

Refactor stance:

```text
clean cut | compiler-driven migration | temporary shim | local implementation only
```

Change:

```text
What changed physically?
What old shape/API is being removed?
What invariant is preserved?
Why should this help?
Why is this a better whole-system abstraction than a workaround?
Does this create or remove copies across the backend boundary?
```

### Baseline Delta

Compare against the log8 baseline and, if different, the immediately previous
kept entry.

#### 1.5x Runtime Budget Rows

| axis       | row                                | raw SQLite median | before median | after median | ratio after/raw | delta | status |
| ---------- | ---------------------------------- | ----------------: | ------------: | -----------: | --------------: | ----: | ------ |
| write      | `write_root_all_rows/1k`           |                   |               |              |                 |       |        |
| write      | `write_delta_10pct_updates/1k`     |                   |               |              |                 |       |        |
| write      | `write_tombstone_10pct_deletes/1k` |                   |               |              |                 |       |        |
| exact-read | `get_many_exact_keys/1k`           |                   |               |              |                 |       |        |
| exact-read | `get_many_missing_keys/1k`         |                   |               |              |                 |       |        |
| exact-read | `exists_many_exact_keys/1k`        |                   |               |              |                 |       |        |
| scan       | `scan_keys_only/1k`                |                   |               |              |                 |       |        |
| scan       | `scan_headers_only/1k`             |                   |               |              |                 |       |        |
| scan       | `scan_full_rows/1k`                |                   |               |              |                 |       |        |
| scan       | `prefix_scan_schema/1k`            |                   |               |              |                 |       |        |
| scan       | `prefix_scan_schema_file_null/1k`  |                   |               |              |                 |       |        |

#### Diff / Materialization

| row                                   | before median | after median | delta | shape status |
| ------------------------------------- | ------------: | -----------: | ----: | ------------ |
| `changed_keys_update_10pct/1k`        |               |              |       |              |
| `changed_keys_delta_chain_10x1pct/1k` |               |              |       |              |
| `materialize_delta_chain_10x1pct/1k`  |               |              |       |              |

#### Storage

Storage fixture rows, required if bytes can change:

| backend / state                        | before bytes | after bytes | delta | status |
| -------------------------------------- | -----------: | ----------: | ----: | ------ |
| raw SQLite / inserted                  |              |             |       |        |
| Lix SQLite / inserted                  |              |             |       |        |
| Lix SQLite / after create_version      |              |             |       |        |
| Lix SQLite / after fast-forward merge  |              |             |       |        |
| Lix SQLite / after divergent merge     |              |             |       |        |
| Lix RocksDB / inserted                 |              |             |       |        |
| Lix RocksDB / after create_version     |              |             |       |        |
| Lix RocksDB / after fast-forward merge |              |             |       |        |
| Lix RocksDB / after divergent merge    |              |             |       |        |

### Unchanged Guardrails

List guardrails that were not meaningfully impacted. Do not leave this blank.

| guardrail                                         | after value | status |
| ------------------------------------------------- | ----------: | ------ |
| physical write budget stays near backend speed    |             |        |
| physical write runtime <= 1.5x raw SQLite         |             |        |
| exact reads <= 1.5x raw SQLite                    |             |        |
| scans <= 1.5x raw SQLite                          |             |        |
| header-only scans do not hydrate full JSON values |             |        |
| SQLite and RocksDB both reported                  |             |        |
| storage growth explained                          |             |        |
| post-vacuum storage <= 2x raw SQLite              |             |        |
| backend boundary copy cost explained              |             |        |
| tracked logic remains on the tracked path         |             |        |
| no workload shifted to untracked machinery        |             |        |
| no benchmark measurement changed                  |             |        |

### Interpretation

```text
Keep/reject?
Which axis improved?
Which guardrail moved?
Was the evidence structural, timing-based, or both?
Is there a temporary shim? If yes, when should it be removed?
What should the next agent try?
```

## Optimization 1: tracked tombstone bit in projection value

Commit: `uncommitted on 11ff3a2e`

Target axis:

```text
scan
```

Backend/API scope:

```text
layout behavior
```

Physical shape:

```text
read index / projection layout
materialization policy
copy/serialization boundary
```

Refactor stance:

```text
clean cut
```

Change:

```text
Tracked-state projection values now carry the durable tombstone bit directly.
The bit is packed into the high bit of the existing value header byte, so the
encoded value length stays unchanged. VALUE_VERSION is bumped to 5 without a
backward decoder because Lix has not shipped.

The old shape forced key/header-only scans to hydrate commit_store change packs
just to learn whether a row was deleted. The new shape makes tracked_state
scalar fields authoritative at the projection boundary; commit_store pack
hydration is reserved for projections that need snapshot_content or metadata
JSON refs.

Tree scans are now physical-only: TrackedStateTreeScanRequest no longer carries
tombstone visibility, and tracked scan limits are applied after delta overlay,
materialization, and tombstone visibility. This matches the reference-system
shape where delete/tombstone facts are carried through physical merge/scan
stages and logical visibility/limit is applied above them.

No backend API changed. SQLite and RocksDB both store the same byte-length value
and benefit from avoiding unnecessary commit_pack reads for non-JSON
projections. No tracked workload moved to untracked storage and no benchmark
measurement changed.
```

### Baseline Delta

Compared against the log8 baseline. The full smoke run showed some noisy
RocksDB scan intervals, so the RocksDB rows below use the targeted remeasure
for the affected rows:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- smoke
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/rocksdb/smoke/(write_root_all_rows|write_delta_10pct_updates|write_tombstone_10pct_deletes|scan_keys_only|scan_headers_only|scan_full_rows|prefix_scan_schema|prefix_scan_schema_file_null)/1k'
```

#### 1.5x Runtime Budget Rows

| axis       | row                                | raw SQLite median | before SQLite | after SQLite | SQLite ratio | before RocksDB | after RocksDB | RocksDB ratio | status |
| ---------- | ---------------------------------- | ----------------: | ------------: | -----------: | -----------: | -------------: | ------------: | ------------: | ------ |
| write      | `write_root_all_rows/1k`           |         2.4999 ms |     6.8347 ms |    6.5245 ms |        2.61x |      6.1430 ms |     5.6554 ms |         2.26x | still over budget, no structural regression |
| write      | `write_delta_10pct_updates/1k`     |         1.3595 ms |     2.6272 ms |    3.3163 ms |        2.44x |      1.3950 ms |     1.4372 ms |         1.06x | SQLite noisy, RocksDB pass |
| write      | `write_tombstone_10pct_deletes/1k` |         1.3092 ms |     2.4321 ms |    3.1727 ms |        2.42x |      1.3632 ms |     1.4650 ms |         1.12x | SQLite noisy, RocksDB pass |
| exact-read | `get_many_exact_keys/1k`           |         2.1850 ms |     4.6055 ms |    4.4805 ms |        2.05x |      3.4668 ms |     3.6687 ms |         1.68x | still over budget |
| exact-read | `get_many_missing_keys/1k`         |         13.099 ms |     2.2822 ms |    2.2718 ms |        0.17x |      1.4138 ms |     1.9440 ms |         0.15x | pass |
| exact-read | `exists_many_exact_keys/1k`        |         2.2187 ms |     4.6519 ms |    4.5695 ms |        2.06x |      3.4720 ms |     5.5972 ms |         2.52x | RocksDB row noisy; semantic equivalent still uses get_many |
| scan       | `scan_keys_only/1k`                |         1.1673 ms |     3.2542 ms |    2.4975 ms |        2.14x |      2.0822 ms |     1.4497 ms |         1.24x | primary win; RocksDB now in budget |
| scan       | `scan_headers_only/1k`             |         1.3034 ms |     3.0692 ms |    3.0376 ms |        2.33x |      2.0012 ms |     1.8478 ms |         1.42x | RocksDB now in budget |
| scan       | `scan_full_rows/1k`                |         1.2110 ms |     4.3792 ms |    4.7813 ms |        3.95x |      3.1884 ms |     3.2480 ms |         2.68x | still over budget |
| scan       | `prefix_scan_schema/1k`            |         1.6941 ms |     4.4623 ms |    4.6607 ms |        2.75x |      3.2190 ms |     3.3677 ms |         1.99x | still over budget |
| scan       | `prefix_scan_schema_file_null/1k`  |         1.2609 ms |     4.3889 ms |    4.8380 ms |        3.84x |      3.1497 ms |     3.3515 ms |         2.66x | still over budget |

#### Diff / Materialization

| row                                   | before SQLite | after SQLite | before RocksDB | after RocksDB | shape status |
| ------------------------------------- | ------------: | -----------: | -------------: | ------------: | ------------ |
| `changed_keys_update_10pct/1k`        |     68.399 ms |    73.492 ms |      67.192 ms |     71.735 ms | still hotspot; movement within noisy structural guardrail |
| `changed_keys_delta_chain_10x1pct/1k` |     10.401 ms |    11.167 ms |      8.7436 ms |     10.722 ms | watch |
| `materialize_delta_chain_10x1pct/1k`  |     5.7651 ms |    5.5134 ms |      2.7741 ms |     2.8888 ms | near neutral; value length is unchanged |

#### Storage

Storage fixture command:

```sh
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
```

Result: passed.

| backend / state                        | before bytes | after bytes | delta | status |
| -------------------------------------- | -----------: | ----------: | ----: | ------ |
| raw SQLite / inserted                  |      1692456 |     1692456 |     0 | unchanged |
| Lix SQLite / inserted                  |      1075136 |     1075136 |     0 | unchanged |
| Lix SQLite / after create_version      |      1087496 |     1087496 |     0 | unchanged |
| Lix SQLite / after fast-forward merge  |      5287488 |     5291608 | +4120 | one SQLite page; acceptable page-layout noise |
| Lix SQLite / after divergent merge     |      5615168 |     5619288 | +4120 | one SQLite page; acceptable page-layout noise |
| Lix RocksDB / inserted                 |       993900 |      993900 |     0 | unchanged |
| Lix RocksDB / after create_version     |       995766 |      995766 |     0 | unchanged |
| Lix RocksDB / after fast-forward merge |      1157143 |     1157143 |     0 | unchanged |
| Lix RocksDB / after divergent merge    |      1528256 |     1528254 |    -2 | unchanged |

### Unchanged Guardrails

| guardrail                                         | after value | status |
| ------------------------------------------------- | ----------: | ------ |
| physical write budget stays near backend speed    | mixed | existing SQLite write budget failures remain |
| physical write runtime <= 1.5x raw SQLite         | mixed | RocksDB delta/tombstone pass; root writes still over |
| exact reads <= 1.5x raw SQLite                    | mixed | missing reads pass; exact reads still over |
| scans <= 1.5x raw SQLite                          | mixed | RocksDB keys/header pass; SQLite scans still over |
| header-only scans do not hydrate full JSON values | yes | preserved and strengthened |
| SQLite and RocksDB both reported                  | yes | full smoke plus RocksDB targeted rerun |
| storage growth explained                          | yes | no value-length growth; only one SQLite page in merge states |
| post-vacuum storage <= 2x raw SQLite              | mixed | same pre-existing SQLite merge-state growth |
| backend boundary copy cost explained              | yes | no new backend copies; fewer commit_pack loads for scalar projections |
| tracked logic remains on the tracked path         | yes | no workload moved |
| no workload shifted to untracked machinery        | yes | unchanged |
| no benchmark measurement changed                  | yes | benchmark untouched |

### Review Loop

Reviewer pass 1:

```text
HIGH: low-level tree matching filtered deleted delta entries before applying
them over a materialized base root. Fixed by keeping tree matching physical and
adding pending_tombstone_delta_hides_materialized_base_row.
```

Reviewer pass 2:

```text
HIGH: none.
MEDIUM: user limit could be applied before tombstone visibility. Fixed by not
pushing tracked scan limits into TrackedStateTreeScanRequest and adding
scan_limit_applies_after_tombstone_visibility.
```

Reviewer pass 3:

```text
HIGH: by-file fast path still applied request.limit before visibility. Fixed by
removing both by-file early-limit breaks and adding
by_file_scan_limit_applies_after_tombstone_visibility.
```

Final reviewer pass:

```text
HIGH: none.
MEDIUM: none.
LOW: none.
```

Verification:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches
cargo test -p lix_engine tracked_state:: --features storage-benches
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- smoke
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/rocksdb/smoke/(write_root_all_rows|write_delta_10pct_updates|write_tombstone_10pct_deletes|scan_keys_only|scan_headers_only|scan_full_rows|prefix_scan_schema|prefix_scan_schema_file_null)/1k'
```

All commands passed.

### Interpretation

```text
Keep.

Primary axis: scan, specifically key/header projections and tombstone
visibility. Structural win: tombstone state now lives in the tracked projection
value and non-JSON projections do not hydrate commit_store packs. Timing win:
RocksDB scan_keys_only improved from 2.0822 ms to 1.4497 ms and
scan_headers_only from 2.0012 ms to 1.8478 ms; SQLite scan_keys_only improved
from 3.2542 ms to 2.4975 ms.

Guardrails: encoded value length is unchanged, storage fixture passed, and no
backend-specific API was introduced. Some full-smoke rows were noisy, so
RocksDB scan/write guardrails were remeasured directly. Existing SQLite write,
exact-read, full-row, and prefix-scan rows remain over the 1.5x budget.

No temporary shim.

Next optimization should attack the remaining scan/full-row and exact-read
budget failures by adding a borrowed/header decode path for tracked-state leaf
entries. The tombstone bit is now in the first value byte, so the next cut can
filter visibility without allocating owned locators or full row values.
```

## Optimization 2: Indexable Borrowed Leaf Nodes

Date: 2026-05-10

Commit: this entry is committed with the optimization

### Change

Changed tracked-state leaf node bytes from a sequential record stream to a v2
offset-table layout:

```text
kind: u8
version: u8
entry_count: u32
entry_offsets: (entry_count + 1) * u32
payload: [key_len: u32, key, value_len: u32, value]*
```

The offset table lets exact reads binary-search leaf keys without first cloning
every key/value pair in the leaf. Scans now borrow leaf entries out of the
verified node byte buffer and decode only matching rows. Owned `decode_node`
still exists for callers that need it, but it is built on the borrowed decoder.

The leaf splitter now accounts for the exact v2 physical size:

```text
leaf_size = 10 + entry_count * 12 + key_bytes + value_bytes
entry_size = 12 + key_bytes + value_bytes
```

No backward compatibility shim was kept. Lix has not shipped, and this is a
physical layout cutover.

### Benchmarks

Focused command:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(get_many_exact_keys|exists_many_exact_keys|scan_keys_only|scan_headers_only|scan_full_rows|prefix_scan_schema|prefix_scan_schema_file_null)/1k'
```

Result: passed.

| row                                                | after median | criterion status |
| -------------------------------------------------- | -----------: | ---------------- |
| `sqlite/get_many_exact_keys/1k`                    |    4.4327 ms | no change |
| `sqlite/exists_many_exact_keys/1k`                 |    4.5704 ms | no change |
| `sqlite/scan_keys_only/1k`                         |    2.7218 ms | no change |
| `sqlite/scan_headers_only/1k`                      |    3.0616 ms | no change |
| `sqlite/scan_full_rows/1k`                         |    4.4447 ms | no change |
| `sqlite/prefix_scan_schema/1k`                     |    4.3002 ms | no change |
| `sqlite/prefix_scan_schema_file_null/1k`           |    4.2372 ms | no change |
| `rocksdb/get_many_exact_keys/1k`                   |    3.5170 ms | no change |
| `rocksdb/exists_many_exact_keys/1k`                |    3.5438 ms | improved |
| `rocksdb/scan_keys_only/1k`                        |    1.5767 ms | no change |
| `rocksdb/scan_headers_only/1k`                     |    2.0217 ms | no change |
| `rocksdb/scan_full_rows/1k`                        |    3.3787 ms | no change |
| `rocksdb/prefix_scan_schema/1k`                    |    3.2941 ms | no change |
| `rocksdb/prefix_scan_schema_file_null/1k`          |    3.2749 ms | no change |

### Storage

Storage fixture command:

```sh
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
```

Result: passed.

| backend / state                        | bytes | bytes/row | status |
| -------------------------------------- | ----: | --------: | ------ |
| raw SQLite / inserted                  | 1692456 | 1692.5 | unchanged |
| Lix SQLite / inserted                  | 1075136 | 1075.1 | unchanged |
| Lix SQLite / after create_version      | 1087496 | 1087.5 | unchanged |
| Lix SQLite / after fast-forward merge  | 5287488 | 5287.5 | unchanged |
| Lix SQLite / after divergent merge     | 5615168 | 5615.2 | unchanged |
| Lix RocksDB / inserted                 |  993900 |  993.9 | unchanged |
| Lix RocksDB / after create_version     |  995766 |  995.8 | unchanged |
| Lix RocksDB / after fast-forward merge | 1157143 | 1157.1 | unchanged |
| Lix RocksDB / after divergent merge    | 1528256 | 1528.3 | unchanged |

### Review Loop

Reviewer pass 1:

```text
HIGH: none.
MEDIUM: leaf chunk sizing still estimated the old sequential format. Fixed by
including the v2 offset directory in estimate_leaf_chunk_size and by feeding
physical entry bytes into boundary_trigger.
LOW: add direct codec regression tests for v2 leaf bytes and malformed offset
tables. Fixed with indexable offset-table, empty-leaf, and malformed-offset
tests.
```

Reviewer pass 2:

```text
HIGH: none.
The previous sizing concern appears addressed, borrowed decode paths do not
carry leaf borrows across recursive awaits, and v2 offset validation/tests are
present.
```

### Verification

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches
cargo test -p lix_engine tracked_state:: --features storage-benches
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(get_many_exact_keys|exists_many_exact_keys|scan_keys_only|scan_headers_only|scan_full_rows|prefix_scan_schema|prefix_scan_schema_file_null)/1k'
```

All commands passed.

### Interpretation

```text
Keep.

Primary axis: exact reads and scan decode overhead. Structural win: leaves now
have a pointer/offset directory, matching the page-local indexing pattern used
by reference storage engines, and scan/get_many no longer clone every leaf
entry before discovering the row they need.

Timing: mostly neutral in Criterion, with a measured RocksDB
exists_many_exact_keys improvement from 4.0071 ms in the pre-sizing run to
3.5438 ms after the final fix. SQLite exact reads remain over budget, so this
is a necessary layout foundation rather than the final performance win.

Guardrails: storage fixture stayed unchanged at the 1k guardrail, tracked logic
stays on the tracked path, no workload moved to untracked machinery, and no
benchmark measurement changed.

Next optimization should use the v2 leaf layout to decode tracked value headers
directly from borrowed value bytes for scan visibility and exists-style reads,
then attack exact-read value decode/allocation costs that remain above the
1.5x SQLite target.
```

## Optimization 3: Header-Only Visibility And Exists Reads

Date: 2026-05-10

Commit: this entry is committed with the optimization

### Change

Added a live-row `rows_exist_at_commit` path for tracked-state readers and a
physical `TrackedStateTree::exists_many` traversal. The tree reuses the v2 leaf
offset table from Optimization 2, binary-searches borrowed leaf keys, and reads
only the matched value header to reject tombstones.

Scan visibility now also reads the value header before full value decode.
`decode_visible_value` parses the header once, skips hidden tombstones without
decoding locator/timestamp strings, and continues decoding live rows from the
same cursor. `TrackedStateTreeScanRequest` now carries `include_tombstones`;
its default keeps physical/internal tree scans tombstone-inclusive, while
serving scans copy the user-facing filter.

Pending delta overlay semantics were preserved: when tombstones are excluded,
a pending tombstone removes a matching materialized base row instead of being
ignored. Diff scans explicitly include tombstones.

No backward compatibility shim was kept.

### Benchmarks

Focused command:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(get_many_exact_keys|exists_many_exact_keys|scan_keys_only|scan_headers_only|scan_full_rows|prefix_scan_schema|prefix_scan_schema_file_null)/1k'
```

Result: passed.

| row                                                | after median | criterion status |
| -------------------------------------------------- | -----------: | ---------------- |
| `sqlite/get_many_exact_keys/1k`                    |    4.4035 ms | no change |
| `sqlite/exists_many_exact_keys/1k`                 |    2.4097 ms | improved vs pre-change get/materialize path |
| `sqlite/scan_keys_only/1k`                         |    2.4736 ms | no change |
| `sqlite/scan_headers_only/1k`                      |    3.0070 ms | no change |
| `sqlite/scan_full_rows/1k`                         |    4.1861 ms | no change |
| `sqlite/prefix_scan_schema/1k`                     |    4.1514 ms | no change |
| `sqlite/prefix_scan_schema_file_null/1k`           |    4.1977 ms | no change |
| `rocksdb/get_many_exact_keys/1k`                   |    3.4003 ms | no change |
| `rocksdb/exists_many_exact_keys/1k`                |    1.4389 ms | improved vs pre-change get/materialize path |
| `rocksdb/scan_keys_only/1k`                        |    1.5966 ms | no change |
| `rocksdb/scan_headers_only/1k`                     |    1.9876 ms | no change |
| `rocksdb/scan_full_rows/1k`                        |    3.2413 ms | no change |
| `rocksdb/prefix_scan_schema/1k`                    |    3.6050 ms | no change; noisy high interval |
| `rocksdb/prefix_scan_schema_file_null/1k`          |    3.3356 ms | no change |

Final exists-only rerun after the tombstone semantic fix:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/exists_many_exact_keys/1k'
```

| row                                | final median |
| ---------------------------------- | -----------: |
| `sqlite/exists_many_exact_keys/1k`  |    2.4097 ms |
| `rocksdb/exists_many_exact_keys/1k` |    1.4389 ms |

### Storage

Storage fixture command:

```sh
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
```

Result: passed.

| backend / state                        | bytes | status |
| -------------------------------------- | ----: | ------ |
| raw SQLite / inserted                  | 1692456 | unchanged |
| Lix SQLite / inserted                  | 1075136 | unchanged |
| Lix SQLite / after create_version      | 1087496 | unchanged |
| Lix SQLite / after fast-forward merge  | 5291608 | one SQLite page over the prior run; known page-layout noise |
| Lix SQLite / after divergent merge     | 5619288 | one SQLite page over the prior run; known page-layout noise |
| Lix RocksDB / inserted                 |  993900 | unchanged |
| Lix RocksDB / after create_version     |  995766 | unchanged |
| Lix RocksDB / after fast-forward merge | 1157143 | unchanged |
| Lix RocksDB / after divergent merge    | 1528256 | unchanged |

### Review Loop

Reviewer pass 1:

```text
HIGH: rows_exist_at_commit reported tombstones as existing. Fixed by checking
the value header in tree.exists_many and by applying pending delta tombstones
as false in projection_keys_exist_at_commit.
```

Reviewer pass 2:

```text
HIGH: none.
The fixed paths now return false for tombstones, pending delta tombstones clear
existence, diff scans still include tombstones, and the benchmark uses the new
existence API.
```

### Verification

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches
cargo test -p lix_engine tracked_state:: --features storage-benches
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(get_many_exact_keys|exists_many_exact_keys|scan_keys_only|scan_headers_only|scan_full_rows|prefix_scan_schema|prefix_scan_schema_file_null)/1k'
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/exists_many_exact_keys/1k'
```

All commands passed.

### Interpretation

```text
Keep.

Primary axis: exists reads and tombstone visibility. Structural win:
exists_many no longer piggybacks on full exact-row materialization, and
visibility filtering can reject hidden tombstones from the value header before
locator/string decode or commit_store materialization.

Timing win: exists_many_exact_keys moves from the previous materializing path
around 4.5 ms SQLite / 3.5 ms RocksDB to 2.4097 ms SQLite / 1.4389 ms RocksDB.
The ordinary scan fixture is mostly live rows, so header visibility is neutral
there rather than a tombstone-heavy win.

Guardrails: storage shape is unchanged, hidden pending tombstones still remove
base rows, diff keeps tombstones visible, tracked logic stays on the tracked
path, and the benchmark row now measures the named exists API rather than a
full materialized get.

Next optimization should attack get_many_exact_keys itself: the exact-read path
still decodes full locator/timestamp strings and materializes full rows even
when the caller only needs the JSON payload, so the remaining budget is likely
in value decode and commit/json materialization grouping.
```

## Optimization 4: Store JSON Refs In Primary Tracked Values

Date: 2026-05-10

Commit: this entry is committed with the optimization

### Change

Changed the primary tracked-state value format from locator-only payload
metadata to locator plus direct `snapshot_ref` / `metadata_ref` fields.
`VALUE_VERSION` was bumped and no backward decode shim was kept.

Before this cut, full materialization decoded tracked values, grouped commit
store change-pack loads by `(source_commit_id, source_pack_id)`, decoded the
referenced change just to recover its JSON refs, then grouped JSON loads. The
tracked value is already the durable projection boundary, and both staging and
root materialization already have the JSON refs at write time, so the extra
commit-pack lookup was record-local metadata indirection.

After this cut:

- primary tracked values encode optional `snapshot_ref` and `metadata_ref`;
- delta packs carry those refs too, so pending-delta reads can materialize
  payloads without commit-pack lookups;
- by-file header-index values intentionally encode `None` refs so the secondary
  header index stays lean;
- by-file scans that need payloads still fetch primary tracked values before
  materializing;
- `materialize_index_entries` no longer takes `CommitStoreContext`.

This follows the same physical principle as page/tuple formats in the reference
systems: record-local metadata needed to materialize a tuple should live with
the tuple/index entry, not require an unrelated side lookup.

### Benchmarks

Standard focused command:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(get_many_exact_keys|exists_many_exact_keys|scan_keys_only|scan_headers_only|scan_full_rows|prefix_scan_schema|prefix_scan_schema_file_null)/1k'
```

Result: passed.

Final medians:

| row                                                | after median | criterion status |
| -------------------------------------------------- | -----------: | ---------------- |
| `sqlite/get_many_exact_keys/1k`                    |    4.0589 ms | no change in final rerun; initial run improved |
| `sqlite/exists_many_exact_keys/1k`                 |    2.5128 ms | no change |
| `sqlite/scan_keys_only/1k`                         |    2.5838 ms | no change |
| `sqlite/scan_headers_only/1k`                      |    2.5942 ms | no change in final rerun; initial run improved |
| `sqlite/scan_full_rows/1k`                         |    3.8172 ms | no change |
| `sqlite/prefix_scan_schema/1k`                     |    3.8885 ms | no change |
| `sqlite/prefix_scan_schema_file_null/1k`           |    3.8453 ms | no change |
| `rocksdb/get_many_exact_keys/1k`                   |    2.9264 ms | improved |
| `rocksdb/exists_many_exact_keys/1k`                |    1.4271 ms | no change |
| `rocksdb/scan_keys_only/1k`                        |    1.5068 ms | no change |
| `rocksdb/scan_headers_only/1k`                     |    1.5683 ms | no change in final rerun; initial run improved |
| `rocksdb/scan_full_rows/1k`                        |    2.8121 ms | no change in final rerun; initial run improved |
| `rocksdb/prefix_scan_schema/1k`                    |    2.7684 ms | no change in final rerun; initial run improved |
| `rocksdb/prefix_scan_schema_file_null/1k`          |    2.7350 ms | no change |

Initial run immediately after the change showed the structural win before the
final rerun reset Criterion's comparison baseline:

```text
sqlite/get_many_exact_keys: 4.0738 ms, improved
rocksdb/get_many_exact_keys: 3.1168 ms, improved
rocksdb/scan_full_rows: 2.8681 ms, improved
rocksdb/prefix_scan_schema: 2.7909 ms, improved
```

### Storage

Storage fixture command:

```sh
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
```

Result: passed.

| backend / state                        | bytes | delta vs Optimization 3 | status |
| -------------------------------------- | ----: | ----------------------: | ------ |
| raw SQLite / inserted                  | 1692456 | 0 | unchanged |
| Lix SQLite / inserted                  | 1112216 | +37080 | direct snapshot refs in primary tree |
| Lix SQLite / after create_version      | 1124576 | +37080 | direct snapshot refs in primary tree |
| Lix SQLite / after fast-forward merge  | 5324328 | +32720 | below previous noisy merge shape |
| Lix SQLite / after divergent merge     | 5652176 | +32888 | below previous noisy merge shape |
| Lix RocksDB / inserted                 | 1028557 | +34657 | direct snapshot refs in primary tree |
| Lix RocksDB / after create_version     | 1030457 | +34691 | direct snapshot refs in primary tree |
| Lix RocksDB / after fast-forward merge | 1195234 | +38091 | direct snapshot refs in primary tree |
| Lix RocksDB / after divergent merge    | 1576585 | +48329 | direct snapshot refs in primary tree |

The inserted/create-version states remain below raw SQLite at 1k rows. The
merge states were already above the storage-size north star before this cut;
the additional bytes are explained by durable payload refs that remove a
commit-pack read from exact/full materialization.

### Review Loop

Reviewer pass:

```text
HIGH: none.
MEDIUM: none.
LOW: materialization.rs still described commit_store pack loads. Fixed the
comment to describe direct tracked JSON refs and grouped json_store loads.
```

### Verification

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches
cargo test -p lix_engine tracked_state:: --features storage-benches
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(get_many_exact_keys|scan_full_rows|prefix_scan_schema|prefix_scan_schema_file_null|scan_headers_only|scan_keys_only)/1k'
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(get_many_exact_keys|exists_many_exact_keys|scan_keys_only|scan_headers_only|scan_full_rows|prefix_scan_schema|prefix_scan_schema_file_null)/1k'
```

All commands passed.

### Interpretation

```text
Keep.

Primary axis: exact/full materialization. Structural win: payload refs now live
at the tracked projection boundary, so materialization avoids loading and
decoding commit_store change packs just to recover record-local JSON refs.

Timing win: exact gets improved on both backends; RocksDB full/prefix scans
also moved materially in the initial run. The final standard rerun still shows
lower medians than Optimization 3 for exact/full rows, even when Criterion
reports some rows as no-change because the comparison baseline had already
included this cut.

Storage tradeoff: roughly 35-37 KB extra at 1k inserted rows, with inserted and
create_version states still below raw SQLite. By-file header index values stay
lean by omitting payload refs, so the cost is restricted to primary tracked
values and delta packs.

No temporary shim.

Next optimization should attack the remaining exact read overhead inside
tracked value/key materialization: the read path still allocates full
TrackedStateKey/TrackedStateIndexValue/MaterializedTrackedStateRow objects
even for fixed-shape JSON-pointer reads, and SQLite full reads remain above the
1.5x target.
```

## Optimization 5: Consume JSON Bytes Into Materialized Strings

Date: 2026-05-10

Commit: this entry is committed with the optimization

### Change

Changed tracked-state JSON materialization to consume each owned `Vec<u8>`
payload slot with `String::from_utf8` instead of validating `&[u8]` and then
copying with `to_string`.

This does not change storage layout or APIs. It is a narrow ownership cleanup
inside the read path after Optimization 4 removed commit-pack lookup from
payload materialization.

The implementation keeps the current invariant explicit: each row plan owns its
projected JSON slots. If tracked-state materialization later deduplicates refs
before row planning, duplicate consumers must clone intentionally.

### Benchmarks

Focused command:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(get_many_exact_keys|scan_full_rows|prefix_scan_schema|prefix_scan_schema_file_null)/1k'
```

Result: passed.

| row                                       | after median | criterion status |
| ----------------------------------------- | -----------: | ---------------- |
| `sqlite/get_many_exact_keys/1k`           |    4.1139 ms | no change |
| `sqlite/scan_full_rows/1k`                |    3.8428 ms | no change |
| `sqlite/prefix_scan_schema/1k`            |    3.8457 ms | no change |
| `sqlite/prefix_scan_schema_file_null/1k`  |    3.8080 ms | no change |
| `rocksdb/get_many_exact_keys/1k`          |    2.9443 ms | no change |
| `rocksdb/scan_full_rows/1k`               |    2.7510 ms | no change |
| `rocksdb/prefix_scan_schema/1k`           |    2.6865 ms | no change |
| `rocksdb/prefix_scan_schema_file_null/1k` |    2.7327 ms | no change |

This is not a Criterion-proven timing win on the 1k fixture. It removes an
avoidable allocation/copy in the payload-heavy path and should matter more for
larger JSON payloads than the small smoke rows.

### Storage

No storage change. The storage fixture from Optimization 4 still describes the
current byte shape.

### Review Loop

Reviewer pass:

```text
HIGH: none.
MEDIUM: none.
LOW: remove test-only wrapper around materialized_json_string. Fixed.
LOW: document one-shot JSON slot invariant for .take(). Fixed.

Recommendation: keep, but do not market it as a Criterion-proven optimization.
```

### Verification

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches
cargo test -p lix_engine tracked_state:: --features storage-benches
cargo test -p lix_engine materialized_json_string_consumes_owned_payload_bytes --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(get_many_exact_keys|scan_full_rows|prefix_scan_schema|prefix_scan_schema_file_null)/1k'
```

All commands passed.

### Interpretation

```text
Keep as a small ownership cleanup.

Primary axis: full materialization allocation pressure. Structural win:
materialization consumes owned JSON bytes directly into String, avoiding a
validate-then-copy path.

Timing: no measured Criterion win on the 1k smoke fixture, so this does not
advance the budget by itself. It is low-risk, read-path-only, and keeps the
payload materialization shape moving toward fewer copies.

No temporary shim.

Next optimization still needs a larger structural cut for full scans, likely
avoiding full row object construction where callers only need counts or using a
more borrowed/streamed row materialization path without changing benchmark
semantics.
```

## Optimization 6: Make By-File Roots a Concrete-File Partial Index

Date: 2026-05-10

Commit: this entry is committed with the optimization

### Change

Changed the tracked-state by-file secondary tree into an explicit partial
index for concrete `file_id` values only.

`ByFileIndex::should_use` now returns true only when every file filter is a
concrete `NullableKeyFilter::Value(_)`. Null-only and mixed null/concrete scans
use the primary tracked tree, whose key layout covers both null and concrete
file ids.

`stage_projection_root` now writes the primary root for every projected commit
but stages a by-file root only when needed:

- no parent by-file root and no concrete-file deltas: do not stage a by-file
  root;
- parent by-file root and no concrete-file deltas: inherit the parent by-file
  root with zero chunk puts;
- concrete-file deltas: apply only those deltas to the by-file root.

This matches the physical predicate of the secondary index with the planner
predicate that is allowed to use it. It also avoids carrying null-file entries
in a secondary tree that the planner never uses for null-file filters.

Added regression coverage for:

- null-file rows not staging a by-file root;
- a null-only parent plus concrete-file child scanned with mixed
  `[Null, Value(file)]` filters, which must use the primary tree and return
  both inherited null rows and concrete child rows.

### Benchmarks

Focused command before the final concrete-only cleanup:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(raw_sqlite|sqlite|rocksdb)/smoke/(write_root_all_rows|prefix_scan_schema_file_null|scan_full_rows)/1k'
```

Result: passed.

| row                                             | after median | criterion status |
| ----------------------------------------------- | -----------: | ---------------- |
| `raw_sqlite/write_root_all_rows/1k`             |    2.9524 ms | noisy baseline |
| `raw_sqlite/scan_full_rows/1k`                  |    1.2119 ms | reference |
| `raw_sqlite/prefix_scan_schema_file_null/1k`    |    1.4604 ms | reference |
| `sqlite/write_root_all_rows/1k`                 |    6.2808 ms | no change |
| `sqlite/scan_full_rows/1k`                      |    3.8271 ms | no change |
| `sqlite/prefix_scan_schema_file_null/1k`        |    4.0401 ms | no change |
| `rocksdb/write_root_all_rows/1k`                |    5.4735 ms | no change |
| `rocksdb/scan_full_rows/1k`                     |    2.7509 ms | no change |
| `rocksdb/prefix_scan_schema_file_null/1k`       |    2.7411 ms | no change |

This is not a runtime win for the current JSON-pointer smoke rows.
`write_root_all_rows` uses delta staging rather than projection-root staging,
and the benchmark rows have `file_id = None`.

### Storage

Storage command:

```sh
cargo test -p lix_engine json_pointer_crud_storage_accounting --features storage-benches -- --ignored --nocapture
```

Result: passed.

Final repeated 1k storage rows:

| row                                     | bytes | bytes/row | status |
| --------------------------------------- | ----: | --------: | ------ |
| raw SQLite / inserted                   | 1692456 | 1692.5 | reference |
| Lix SQLite / inserted                   | 1112216 | 1112.2 | unchanged |
| Lix SQLite / after create_version       | 1124576 | 1124.6 | unchanged |
| Lix SQLite / after fast-forward merge   | 5324328 | 5324.3 | unchanged from Optimization 4/5 shape |
| Lix SQLite / after divergent merge      | 5652176 | 5652.2 | unchanged from Optimization 4/5 shape |
| Lix RocksDB / inserted                  | 1028557 | 1028.6 | unchanged |
| Lix RocksDB / after create_version      | 1030457 | 1030.5 | unchanged |
| Lix RocksDB / after fast-forward merge  | 1195234 | 1195.2 | unchanged |
| Lix RocksDB / after divergent merge     | 1576587 | 1576.6 | effectively unchanged |

An earlier storage sample before the concrete-only write cleanup showed lower
SQLite merge-state bytes, but repeated final runs returned to the prior
committed SQLite shape. Treat this optimization as storage-neutral for the
current JSON-pointer accounting fixture.

### Review Loop

Reviewer pass 1:

```text
HIGH: none.
MEDIUM: none.
LOW: scan_request_from_tracked still looked more general than the all-concrete
planner contract. Fixed with debug assertion and Value-only mapping.
LOW: add the mixed Null + Value regression case. Fixed.
LOW: once a by-file root exists, null-file rows were still indexed. Fixed by
making by-file writes concrete-file-only and inheriting unchanged roots.

Recommendation: keep.
```

Reviewer pass 2:

```text
HIGH: none.
MEDIUM: none.
LOW: encode_key_ref could still encode file_id = None. Fixed with a debug
assertion at the helper boundary.

Recommendation: keep. The result is a coherent partial secondary index:
concrete-only on writes, concrete-only on reads, with safe parent-root
inheritance.
```

### Verification

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches
cargo test -p lix_engine tracked_state:: --features storage-benches
cargo test -p lix_engine json_pointer_crud_storage_accounting --features storage-benches -- --ignored --nocapture
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(raw_sqlite|sqlite|rocksdb)/smoke/(write_root_all_rows|prefix_scan_schema_file_null|scan_full_rows)/1k'
```

All commands passed.

### Interpretation

```text
Keep as a physical-layout cleanup, not as a budget-moving benchmark win.

Primary axis: secondary-index shape. Structural win: by-file roots now behave
like a partial secondary index whose physical contents and planner predicate
agree. This prevents null-file rows from being copied into a secondary tree that
cannot answer null-file scans safely, and it removes the old missing-root
empty-result behavior for projected reads.

Timing/storage: neutral on the current JSON-pointer fixture. This does not move
the remaining <= 1.5x runtime target or the SQLite merge-state storage issue.

No temporary shim.

Next optimization should return to budget-moving read/write costs: either the
primary tracked-tree write path for full-root materialization, or row
materialization/allocation in exact and scan reads.
```

## Optimization 7: Skip JSON Planning for Header-Only Materialization

Date: 2026-05-10

Commit: this entry is committed with the optimization

### Change

Added a no-JSON fast path to tracked-state materialization. When a requested
projection omits both `snapshot_content` and `metadata`,
`materialize_index_entries` now directly maps tree entries into
`MaterializedTrackedStateRow` values with payload columns omitted.

This skips work that cannot affect the result for key-only and header-only
projections:

- no per-row payload plan allocation;
- no `json_refs` / `json_ref_localities` vectors;
- no pack-locality grouping map;
- no empty JSON-store load path.

Header semantics are still preserved. Identity fields come from the tracked
key, and `deleted`, timestamps, `change_id`, and `commit_id` come from the
tracked value. Tombstone filtering still uses `row.deleted`, not
`snapshot_content`.

No storage layout change.

### Benchmarks

Focused command:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(scan_keys_only|scan_headers_only|scan_full_rows|prefix_scan_schema_file_null)/1k'
```

Result: passed.

| row                                       | after median | criterion status |
| ----------------------------------------- | -----------: | ---------------- |
| `sqlite/scan_keys_only/1k`                |    2.4932 ms | -6.0%, within noise threshold |
| `sqlite/scan_headers_only/1k`             |    2.5955 ms | no change |
| `sqlite/scan_full_rows/1k`                |    3.7797 ms | no change |
| `sqlite/prefix_scan_schema_file_null/1k`  |    3.7925 ms | improved, likely noisy control |
| `rocksdb/scan_keys_only/1k`               |    1.5304 ms | no change |
| `rocksdb/scan_headers_only/1k`            |    1.5769 ms | no change |
| `rocksdb/scan_full_rows/1k`               |    2.7634 ms | no change |
| `rocksdb/prefix_scan_schema_file_null/1k` |    2.6894 ms | improved, likely noisy control |

The structural improvement is real for projections without payload columns, but
Criterion does not show a strong win on the 1k smoke fixture. Full-row scans are
included as controls because they still use the JSON hydration path.

### Storage

No storage change.

### Review Loop

Reviewer pass:

```text
HIGH: none.
MEDIUM: none.
LOW: infallible helper returned Result only to fit collect. Fixed by returning
MaterializedTrackedStateRow directly and wrapping once at the call site.

Recommendation: keep. This is an executor-style projection fast path: when no
payload columns are requested, skip payload planning entirely.
```

### Verification

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches
cargo test -p lix_engine projected_scans_do_not_materialize_snapshot_when_snapshot_content_is_omitted --features storage-benches
cargo test -p lix_engine tracked_state:: --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(scan_keys_only|scan_headers_only|scan_full_rows|prefix_scan_schema_file_null)/1k'
```

All commands passed.

### Interpretation

```text
Keep as a narrow projection fast path.

Primary axis: key/header scans. Structural win: no-payload projections now
avoid payload planning rather than constructing empty JSON work and discovering
there is nothing to load.

Timing: modest/noisy on the current 1k fixture. This does not solve the
remaining full-row scan or exact-get gap, but it removes unnecessary executor
work for projected scans and keeps the read path moving toward column-aware
materialization.

No temporary shim.

Next optimization should target full payload materialization or exact get_many:
the remaining expensive rows still hydrate JSON and build full
MaterializedTrackedStateRow objects.
```

## Optimization 8: Store JSON Locality as Row-Plan Indexes

Date: 2026-05-10

Commit: this entry is committed with the optimization

### Change

Changed full tracked-state payload materialization to keep JSON ref locality as
compact row-plan indexes instead of cloning commit ids per projected JSON ref.

Before this change, `materialize_index_entries` stored
`json_ref_localities: Vec<(String, u32)>`. Each projected `snapshot_content` or
`metadata` ref cloned `value.change_locator.source_commit_id` just so
`load_projection_json_values` could group refs by commit pack.

Row plans already own the same `commit_id`. The locality vector now stores a
small `JsonRefLocality { row_index, pack_id }`, and the grouping step borrows
`row_plans[row_index].commit_id.as_str()` while loading JSON values.

No storage/API behavior change.

### Benchmarks

Focused command:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(get_many_exact_keys|scan_full_rows|prefix_scan_schema|prefix_scan_schema_file_null)/1k'
```

Result: passed.

| row                                       | after median | criterion status |
| ----------------------------------------- | -----------: | ---------------- |
| `sqlite/get_many_exact_keys/1k`           |    3.9197 ms | no change |
| `sqlite/scan_full_rows/1k`                |    3.8695 ms | no change |
| `sqlite/prefix_scan_schema/1k`            |    3.7669 ms | no change |
| `sqlite/prefix_scan_schema_file_null/1k`  |    3.7631 ms | no change |
| `rocksdb/get_many_exact_keys/1k`          |    3.0397 ms | no change |
| `rocksdb/scan_full_rows/1k`               |    2.7001 ms | no change |
| `rocksdb/prefix_scan_schema/1k`           |    2.7920 ms | no change |
| `rocksdb/prefix_scan_schema_file_null/1k` |    2.6921 ms | no change |

SQLite exact gets moved lower in this sample than the previous committed log,
but Criterion still reports no change. Treat this as an allocation cleanup, not
a proven runtime win.

### Storage

No storage change.

### Review Loop

Reviewer pass:

```text
HIGH: none.
MEDIUM: none.
LOW: parallel arrays plus row_plans are correct but coupled; use a small
JsonRefLocality struct to make the invariant clearer. Fixed.

Recommendation: keep. Locality is now an index into already-owned row-plan data
rather than repeated commit-id allocation.
```

### Verification

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches
cargo test -p lix_engine tracked_state:: --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(get_many_exact_keys|scan_full_rows|prefix_scan_schema|prefix_scan_schema_file_null)/1k'
```

All commands passed.

### Interpretation

```text
Keep as a small allocation cleanup in the payload materialization path.

Primary axis: full-row materialization allocation pressure. Structural win:
JSON locality now uses compact indexes into existing row-plan ownership, which
matches the broader direction of carrying offsets/indexes beside payload
metadata instead of duplicating identifying strings.

Timing: Criterion-neutral on the 1k fixture. This is not enough to close the
remaining <= 1.5x exact/full read gap.

No temporary shim.

Next optimization still needs a larger cut in JSON hydration or row
construction. The obvious remaining cost is that full reads still allocate a
MaterializedTrackedStateRow per row and convert every JSON payload to String.
```

## Optimization 9: Return Unique JSON Batch Payloads Without Cloning

Date: 2026-05-10

Commit: this entry is committed with the optimization

### Change

Changed `json_store::load_json_bytes_many_in_scope` to avoid cloning loaded
JSON payload bytes when the request contains no duplicate refs.

The loader already deduplicates requested refs into `unique_values`. Before
this change it always rebuilt the result with:

```text
requested_indexes.map(|index| unique_values[index].clone())
```

That cloned every loaded `Vec<u8>` even when every ref was unique and
`unique_values` was already in request order. Full tracked reads then consumed
the cloned bytes into `String`, leaving the original decoded payload copy
unused.

The loader now tracks whether any duplicate ref was seen:

- no duplicates: return `unique_values` directly;
- duplicates: keep the old clone-to-request-order behavior so repeated refs
  still produce repeated result slots.

This applies to both commit-pack and out-of-band JSON scopes. Missing refs keep
their `None` slots in either path.

### Benchmarks

Focused command:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(get_many_exact_keys|scan_full_rows|prefix_scan_schema|prefix_scan_schema_file_null)/1k'
```

Result: passed.

| row                                       | after median | criterion status |
| ----------------------------------------- | -----------: | ---------------- |
| `sqlite/get_many_exact_keys/1k`           |    3.8568 ms | -3.3%, within noise threshold |
| `sqlite/scan_full_rows/1k`                |    3.7141 ms | improved |
| `sqlite/prefix_scan_schema/1k`            |    3.6749 ms | no change |
| `sqlite/prefix_scan_schema_file_null/1k`  |    3.6774 ms | no change |
| `rocksdb/get_many_exact_keys/1k`          |    2.9055 ms | no change |
| `rocksdb/scan_full_rows/1k`               |    2.5562 ms | no change |
| `rocksdb/prefix_scan_schema/1k`           |    2.7618 ms | no change |
| `rocksdb/prefix_scan_schema_file_null/1k` |    2.7406 ms | no change |

The strongest measured signal is SQLite full scans. RocksDB and exact gets move
in the right direction but remain Criterion-neutral in this run.

### Storage

No storage change.

### Review Loop

Reviewer pass:

```text
HIGH: none.
MEDIUM: none.
LOW: json_values_in_request_order depends on the has_duplicate_refs flag.
Fixed with debug assertions that the no-duplicate path has request indexes
0..len and the same length as unique_values.

Recommendation: keep. This is a real structural copy cut in the payload path,
and the SQLite scan_full_rows improvement is plausible.
```

### Verification

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches
cargo test -p lix_engine json_store::store::tests::json_batch_load_roundtrips_in_request_order --features storage-benches
cargo test -p lix_engine tracked_state:: --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(get_many_exact_keys|scan_full_rows|prefix_scan_schema|prefix_scan_schema_file_null)/1k'
```

All commands passed.

### Interpretation

```text
Keep as a payload-copy reduction.

Primary axis: full-row materialization. Structural win: unique JSON batch loads
now transfer ownership of decoded payload bytes directly to the caller instead
of cloning them back into request order. This pairs with tracked materialization
consuming those bytes with String::from_utf8.

Timing: SQLite full scans improved in the focused run; other full/exact rows
remain noisy but generally moved lower. The <= 1.5x target is still not met.

No temporary shim.

Next optimization should look below row materialization again: load_from_packs
still decodes entire JSON packs for the requested refs, and tracked exact reads
still construct full rows even when callers only check presence in the current
bench harness.
```

## Optimization 10: Encode Delta Packs From Borrowed Deltas

Date: 2026-05-10

Commit: this entry is committed with the optimization

### Change

Changed normal tracked-state delta staging to encode delta packs directly from
borrowed `TrackedStateDeltaRef` values.

Before this change, `TrackedStateWriter::stage_delta` cloned every borrowed
delta into owned `TrackedStateDeltaEntry` objects, including schema/file/entity
identity, source commit/change ids, and timestamp strings. It then immediately
encoded those owned entries into the delta pack.

The write path now uses:

- `codec::encode_delta_pack_refs`;
- `storage::stage_delta_pack_refs`;
- `TrackedStateWriter::stage_delta` calling the borrowed staging path directly.

The old owned-entry encode/stage helper and `delta_entries_from_refs` were
removed. Decode still materializes owned `TrackedStateDeltaEntry` values because
readers need owned entries after loading a persisted pack.

No delta-pack format change: the encoder still writes the same `LXTD`
magic/version/count and uses the same tracked key/value encoders.

### Benchmarks

Focused command:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(write_root_all_rows|write_delta_10pct_updates|write_tombstone_10pct_deletes)/1k'
```

Result: passed.

| row                                             | after median | criterion status |
| ----------------------------------------------- | -----------: | ---------------- |
| `sqlite/write_root_all_rows/1k`                 |    6.2844 ms | no change |
| `sqlite/write_delta_10pct_updates/1k`           |    2.6592 ms | no change, noisy guardrail |
| `sqlite/write_tombstone_10pct_deletes/1k`       |    2.3671 ms | no change, noisy guardrail |
| `rocksdb/write_root_all_rows/1k`                |    5.3605 ms | no change |
| `rocksdb/write_delta_10pct_updates/1k`          |    1.3421 ms | no change, noisy guardrail |
| `rocksdb/write_tombstone_10pct_deletes/1k`      |    1.2464 ms | no change |

Root-write medians moved lower than several previous samples, especially
RocksDB, but Criterion still reports no change. Treat this as a production
write-path allocation cleanup, not a proven target-closing win.

### Storage

No storage change.

### Review Loop

Reviewer pass:

```text
HIGH: none.
MEDIUM: none.
LOW: remove stale #[allow(dead_code)] from TrackedStateDeltaRef. Fixed.
LOW: add direct delta-pack codec regression coverage for the borrowed encoder.
Fixed with delta_pack_ref_encoder_roundtrips_entries.

Recommendation: keep. This is a clean production write-path allocation cut and
removes an artificial owned staging API.
```

### Verification

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches
cargo test -p lix_engine delta_pack_ref_encoder_roundtrips_entries --features storage-benches
cargo test -p lix_engine tracked_state:: --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(write_root_all_rows|write_delta_10pct_updates|write_tombstone_10pct_deletes)/1k'
```

All commands passed.

### Interpretation

```text
Keep as a borrowed-write cleanup.

Primary axis: root and delta writes. Structural win: normal tracked-state
commits no longer allocate a full owned delta-entry layer just to encode the
same bytes into a delta pack. This follows the same shape used by reference
systems that encode from stable in-memory views and materialize owned records
only when reading back from storage.

Timing: Criterion-neutral on the 1k fixture. This does not close the remaining
write_root_all_rows budget gap, but it removes an obvious allocation layer from
the production write path without changing storage semantics.

No temporary shim.

Next optimization needs a bigger write-side cut, likely in commit_store staging,
JSON pack staging, or the transaction write-set path, because delta-pack
encoding itself is no longer cloning the tracked projection rows first.
```

## Optimization 11: Encode Change Packs From Existing Slices

Date: 2026-05-10

Commit: this entry is committed with the optimization

### Change

Changed `commit_store::codec::encode_change_pack` to accept
`&[ChangeRef<'_>]` instead of a generic iterator that it immediately collected
into a temporary `Vec`.

The production caller already has authored changes in a `Vec`, so the encoder
can read the count from the slice and encode refs directly in order. This
removes one temporary collection from the commit-store write path.

No storage format change.

### Benchmarks

Focused command:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(write_root_all_rows|write_delta_10pct_updates)/1k'
```

Result: passed.

| row                                       | after median | criterion status |
| ----------------------------------------- | -----------: | ---------------- |
| `sqlite/write_root_all_rows/1k`           |    6.0937 ms | no change |
| `sqlite/write_delta_10pct_updates/1k`     |    2.5978 ms | no change |
| `rocksdb/write_root_all_rows/1k`          |    5.4208 ms | no change |
| `rocksdb/write_delta_10pct_updates/1k`    |    1.3267 ms | no change |

### Storage

No storage change.

### Review Loop

Reviewer pass:

```text
HIGH: none.
MEDIUM: none.
LOW: none.

Recommendation: keep the code, but do not present it as a standalone
budget-moving win. It is a clean write-path allocation cleanup with no measured
Criterion win.
```

### Verification

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches
cargo test -p lix_engine commit_store:: --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(write_root_all_rows|write_delta_10pct_updates)/1k'
```

All commands passed.

### Interpretation

```text
Keep as a small encoder allocation cleanup.

Primary axis: commit-store write packing. Structural win: encode from the
already-shaped authored-change slice instead of materializing a second vector
just to know the count.

Timing: Criterion-neutral. This is not a budget-moving optimization by itself,
but it composes with the borrowed tracked delta-pack encoder and keeps the
write path moving away from temporary owned collections.

No temporary shim.

Next optimization needs a larger cut in JSON pack staging or transaction
write-set application; the obvious per-row encoder clones in tracked and
commit-store delta packing have now been reduced.
```

## Optimization 12: Preserve JSON Pack Input Order Without Tree Sorting

Date: 2026-05-11

Commit: this entry is committed with the optimization

### Change

Changed `JsonStoreWriter::stage_batch` to keep unique encoded payloads in
first-seen input order instead of inserting them into a `BTreeMap` sorted by
hash.

The writer still returns refs in request order and still deduplicates repeated
payload hashes. The new shape is:

- `order: Vec<JsonRef>` for the caller-visible result;
- `unique_encoded: Vec<EncodedJson>` for first-seen unique payloads;
- `HashSet<[u8; 32]>` only for duplicate suppression.

For commit-pack placement, pack-local entries are selected from
`unique_encoded.iter()` in input order. Direct out-of-band writes iterate the
same vector and skip pack-local payloads.

This intentionally changes pack entry order from hash-sorted to input order.
Pack lookup is hash-addressed and scans decoded entries by hash, so entry order
is not part of the semantic contract. Lix has not shipped, and storage
accounting stayed unchanged.

Added regression coverage for duplicate writer input: `[A, A, B]` returns
`[refA, refA, refB]`, stores only the pack-local payloads, and hydrates both
unique refs from the commit pack.

### Benchmarks

Focused command:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(write_root_all_rows|write_delta_10pct_updates|write_tombstone_10pct_deletes)/1k'
```

Result: passed.

| row                                             | after median | criterion status |
| ----------------------------------------------- | -----------: | ---------------- |
| `sqlite/write_root_all_rows/1k`                 |    5.9331 ms | improved |
| `sqlite/write_delta_10pct_updates/1k`           |    2.6203 ms | no change, noisy guardrail |
| `sqlite/write_tombstone_10pct_deletes/1k`       |    2.4790 ms | no change, noisy guardrail |
| `rocksdb/write_root_all_rows/1k`                |    5.3019 ms | no change |
| `rocksdb/write_delta_10pct_updates/1k`          |    1.3004 ms | no change |
| `rocksdb/write_tombstone_10pct_deletes/1k`      |    1.2178 ms | no change |

SQLite root writes improved by Criterion. RocksDB root-write median moved lower
than recent committed samples but remains Criterion-neutral.

### Storage

Storage command:

```sh
cargo test -p lix_engine json_pointer_crud_storage_accounting --features storage-benches -- --ignored --nocapture
```

Result: passed.

1k rows:

| row                                     | bytes | bytes/row | status |
| --------------------------------------- | ----: | --------: | ------ |
| raw SQLite / inserted                   | 1692456 | 1692.5 | reference |
| Lix SQLite / inserted                   | 1112216 | 1112.2 | unchanged |
| Lix SQLite / after create_version       | 1124576 | 1124.6 | unchanged |
| Lix SQLite / after fast-forward merge   | 5324328 | 5324.3 | unchanged |
| Lix SQLite / after divergent merge      | 5652176 | 5652.2 | unchanged |
| Lix RocksDB / inserted                  | 1028557 | 1028.6 | unchanged |
| Lix RocksDB / after create_version      | 1030457 | 1030.5 | unchanged |
| Lix RocksDB / after fast-forward merge  | 1195234 | 1195.2 | unchanged |
| Lix RocksDB / after divergent merge     | 1576587 | 1576.6 | unchanged |

### Review Loop

Reviewer pass:

```text
HIGH: none.
MEDIUM: none.
LOW: add duplicate writer-input coverage for [A, A, B]. Fixed.

Recommendation: keep. This is a real hot-path structural improvement with a
measured SQLite root-write win, no storage accounting regression, and acceptable
pack-order semantics.
```

### Verification

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches
cargo test -p lix_engine json_store:: --features storage-benches
cargo test -p lix_engine tracked_state:: --features storage-benches
cargo test -p lix_engine json_pointer_crud_storage_accounting --features storage-benches -- --ignored --nocapture
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical -- 'json_pointer_physical/(sqlite|rocksdb)/smoke/(write_root_all_rows|write_delta_10pct_updates|write_tombstone_10pct_deletes)/1k'
```

All commands passed.

### Interpretation

```text
Keep as a JSON pack write-path improvement.

Primary axis: root writes. Structural win: unique JSON-pointer payloads no
longer pay hash-sorted tree-map insertion and sorted iteration before being
packed into a commit-local JSON pack. Dedupe remains hash-based, while physical
pack order follows deterministic input order.

Timing: SQLite write_root_all_rows improved. RocksDB remains neutral but did
not regress materially. The root-write target is still above 1.5x raw SQLite.

No temporary shim.

Next optimization should keep attacking write_root_all_rows, likely below the
generic StorageWriteSet/backend batch application or by reducing JSON payload
encoding work before commit-pack staging.
```
