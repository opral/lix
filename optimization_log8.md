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
