# Optimization Log 8: JSON Pointer Physical Layout Decision Log

Goal: nail the physical layout Lix uses for tracked logic:
`packages/engine/src/tracked_state`, `packages/engine/src/commit_store`, and
the backend/storage APIs they require.

Lix has not shipped. This log should prefer the best-shaped physical API and
layout over compatibility shims. If a change keeps a backwards shim, the entry
must explicitly call that out and justify why it is temporary.

The preferred refactor mode is:

```text
first make the storage shape correct;
then let the Rust compiler reveal upstream code that must move to the new API.
```

It is acceptable for an intermediate refactor entry to leave the tree
temporarily non-compiling if the entry is clearly marked as a physical-layout
cutover step and the next step is compiler-driven migration. Do not hide old
behavior behind adapter layers just to keep call sites compiling.

North-star target:

```text
10k logical writes through the tracked-state/commit-store path should complete
in less than 100ms end-to-end.
```

Physical storage budget:

```text
The storage layer should leave budget for the logical layer:
  target: <= 30-50ms for 10k physical writes
  hard review trigger: > 3x raw SQLite for equivalent writes, exact reads, or scans
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
  `3x SQLite` budget has a measured baseline.
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

Tracked logic stays tracked. Do not move benchmarks, fixtures, changed-key
logic, commit-store logic, or tracked-state behavior from tracked to untracked
code to improve numbers.

## Refactor Policy

Allowed:

```text
change the storage/backend API when the current API forces bad physical layout;
change tracked_state and commit_store layouts when the new layout is cleaner;
break old call sites and let the compiler drive the migration;
delete legacy abstractions that only exist to preserve pre-ship compatibility;
replace one-off fixes with a shared abstraction when the problem is systemic.
```

Required when changing storage/backend APIs:

```text
state the physical problem the old API caused;
show how SQLite and RocksDB can both implement the new shape without hidden
  per-key loops or full-value hydration;
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
moving tracked logic or benchmarks into untracked paths;
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
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical
```

Groups:

```text
json_pointer_physical/raw_sqlite/baseline
json_pointer_physical/raw_sqlite/smoke
json_pointer_physical/raw_sqlite/scale
json_pointer_physical/sqlite/baseline
json_pointer_physical/sqlite/smoke
json_pointer_physical/sqlite/scale
json_pointer_physical/rocksdb/baseline
json_pointer_physical/rocksdb/smoke
json_pointer_physical/rocksdb/scale
```

Rows:

```text
write_root_all_rows/{100,1k,10k}
get_many_exact_keys/{100,1k,10k}
get_many_missing_keys/{100,1k,10k}
exists_many_exact_keys/{100,1k,10k}
scan_keys_only/{100,1k,10k}
scan_headers_only/{100,1k,10k}
scan_full_rows/{100,1k,10k}
prefix_scan_schema/{100,1k,10k}
prefix_scan_schema_file_null/{100,1k,10k}
write_delta_10pct_updates/{100,1k,10k}
write_tombstone_10pct_deletes/{100,1k,10k}
changed_keys_update_10pct/{100,1k,10k}
changed_keys_delta_chain_10x1pct/{100,1k,10k}
materialize_delta_chain_10x1pct/{100,1k,10k}
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
  scale = 10,000 rows
```

Why this fixture:

```text
It mirrors plugin-json-v2 output: many small entities, stable path identities,
container rows, leaf rows, and realistic nested JSON values.
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
Rows above 3x raw SQLite are likely dominated by Lix packing, projection,
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

### 3x SQLite Budget

This is an envelope, not an average. Passing writes does not compensate for
failing reads, and passing reads does not compensate for failing writes.

Write rows:

```text
compare json_pointer_physical/{sqlite,rocksdb}/scale/write_root_all_rows/10k
  to json_pointer_physical/raw_sqlite/scale/write_root_all_rows/10k
compare json_pointer_physical/{sqlite,rocksdb}/scale/write_delta_10pct_updates/10k
  to json_pointer_physical/raw_sqlite/scale/write_delta_10pct_updates/10k
compare json_pointer_physical/{sqlite,rocksdb}/scale/write_tombstone_10pct_deletes/10k
  to json_pointer_physical/raw_sqlite/scale/write_tombstone_10pct_deletes/10k
```

Exact-read rows:

```text
compare json_pointer_physical/{sqlite,rocksdb}/scale/get_many_exact_keys/10k
  to json_pointer_physical/raw_sqlite/scale/get_many_exact_keys/10k
compare json_pointer_physical/{sqlite,rocksdb}/scale/get_many_missing_keys/10k
  to json_pointer_physical/raw_sqlite/scale/get_many_missing_keys/10k
compare json_pointer_physical/{sqlite,rocksdb}/scale/exists_many_exact_keys/10k
  to json_pointer_physical/raw_sqlite/scale/exists_many_exact_keys/10k
```

Scan rows:

```text
compare json_pointer_physical/{sqlite,rocksdb}/scale/scan_keys_only/10k
  to json_pointer_physical/raw_sqlite/scale/scan_keys_only/10k
compare json_pointer_physical/{sqlite,rocksdb}/scale/scan_headers_only/10k
  to json_pointer_physical/raw_sqlite/scale/scan_headers_only/10k
compare json_pointer_physical/{sqlite,rocksdb}/scale/scan_full_rows/10k
  to json_pointer_physical/raw_sqlite/scale/scan_full_rows/10k
compare json_pointer_physical/{sqlite,rocksdb}/scale/prefix_scan_schema/10k
  to json_pointer_physical/raw_sqlite/scale/prefix_scan_schema/10k
compare json_pointer_physical/{sqlite,rocksdb}/scale/prefix_scan_schema_file_null/10k
  to json_pointer_physical/raw_sqlite/scale/prefix_scan_schema_file_null/10k
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
  no crossed 3x budget.

> 15% slower:
  fail unless explicitly accepted as a layout tradeoff.

No change may make an axis that passes the 3x budget start failing it.
```

Storage guardrail:

```text
Bytes after inserted/create_version/fast-forward/divergent merge must remain
explainable. A speedup that causes unexplained storage growth is not kept.
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
2. Prefer clean, compiler-driven refactors over backwards shims. If a shim is
   kept, flag it.
3. Optimize one primary axis at a time and report guardrails for the other
   axes.
4. Compare against raw SQLite where there is an equivalent row.
5. Report SQLite and RocksDB physical rows before keeping backend-sensitive
   changes.
6. Prefer explicit batched APIs over hidden loops of single-key operations.
7. Do not improve one backend by silently regressing the other.
8. Do not change benchmark measurements to make a change look better.
9. Do not move tracked logic, fixtures, or benchmarks into untracked paths.
10. Do not improve writes by forcing broad projection-root materialization
    unless the entry is explicitly a materialization-policy experiment.
11. Do not make key/header-only scans hydrate full JSON values.
12. Do not introduce avoidable copies at the backend boundary without measuring
    and justifying them.
13. Do not remove hash verification, transaction atomicity, or durability
    semantics to win a benchmark.
14. Document rejected experiments if they teach something about the cost model.
15. Append one compact entry per optimization.

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

Not yet run:

```text
Full json_pointer_physical baseline.
10k scale rows for the 3x SQLite budget.
json_pointer_crud_storage fixture guardrail.
```

Reason:

```text
This entry establishes the benchmark surface first. The next entry should run
and paste the full baseline before the first optimization is kept.
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

At 100 rows, exact reads are inside the 3x SQLite envelope for both backends.
This is only a smoke check. It is not the accepted baseline for optimization.
The accepted baseline must use the 1k smoke and 10k scale rows.
```

### Required Baseline Command

Before the first optimization entry, run:

```sh
cargo bench -p lix_engine --features storage-benches --bench json_pointer_physical
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
```

### Baseline Scoreboard Template

Fill this section after the full baseline run.

#### 3x Budget Rows, 10k

| axis       | row                                 | raw SQLite median | Lix SQLite median | SQLite ratio | Lix RocksDB median | RocksDB ratio | status |
| ---------- | ----------------------------------- | ----------------: | ----------------: | -----------: | -----------------: | ------------: | ------ |
| write      | `write_root_all_rows/10k`           |                   |                   |              |                    |               |        |
| write      | `write_delta_10pct_updates/10k`     |                   |                   |              |                    |               |        |
| write      | `write_tombstone_10pct_deletes/10k` |                   |                   |              |                    |               |        |
| exact-read | `get_many_exact_keys/10k`           |                   |                   |              |                    |               |        |
| exact-read | `get_many_missing_keys/10k`         |                   |                   |              |                    |               |        |
| exact-read | `exists_many_exact_keys/10k`        |                   |                   |              |                    |               |        |
| scan       | `scan_keys_only/10k`                |                   |                   |              |                    |               |        |
| scan       | `scan_headers_only/10k`             |                   |                   |              |                    |               |        |
| scan       | `scan_full_rows/10k`                |                   |                   |              |                    |               |        |
| scan       | `prefix_scan_schema/10k`            |                   |                   |              |                    |               |        |
| scan       | `prefix_scan_schema_file_null/10k`  |                   |                   |              |                    |               |        |

#### Diff / Materialization Shape Rows

| row                                    | Lix SQLite median | Lix RocksDB median | expected shape                           | status |
| -------------------------------------- | ----------------: | -----------------: | ---------------------------------------- | ------ |
| `changed_keys_update_10pct/10k`        |                   |                    | scales with changed keys                 |        |
| `changed_keys_delta_chain_10x1pct/10k` |                   |                    | scales with changed keys and chain depth |        |
| `materialize_delta_chain_10x1pct/10k`  |                   |                    | avoids unrelated delta-pack decoding     |        |

#### Storage Fixture

| backend / state                        | bytes on disk | bytes/row | status |
| -------------------------------------- | ------------: | --------: | ------ |
| raw SQLite / inserted                  |               |           |        |
| Lix SQLite / inserted                  |               |           |        |
| Lix SQLite / after create_version      |               |           |        |
| Lix SQLite / after fast-forward merge  |               |           |        |
| Lix SQLite / after divergent merge     |               |           |        |
| Lix RocksDB / inserted                 |               |           |        |
| Lix RocksDB / after create_version     |               |           |        |
| Lix RocksDB / after fast-forward merge |               |           |        |
| Lix RocksDB / after divergent merge    |               |           |        |

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

#### 3x Budget Rows

| axis       | row                                 | raw SQLite median | before median | after median | ratio after/raw | delta | status |
| ---------- | ----------------------------------- | ----------------: | ------------: | -----------: | --------------: | ----: | ------ |
| write      | `write_root_all_rows/10k`           |                   |               |              |                 |       |        |
| write      | `write_delta_10pct_updates/10k`     |                   |               |              |                 |       |        |
| write      | `write_tombstone_10pct_deletes/10k` |                   |               |              |                 |       |        |
| exact-read | `get_many_exact_keys/10k`           |                   |               |              |                 |       |        |
| exact-read | `get_many_missing_keys/10k`         |                   |               |              |                 |       |        |
| exact-read | `exists_many_exact_keys/10k`        |                   |               |              |                 |       |        |
| scan       | `scan_keys_only/10k`                |                   |               |              |                 |       |        |
| scan       | `scan_headers_only/10k`             |                   |               |              |                 |       |        |
| scan       | `scan_full_rows/10k`                |                   |               |              |                 |       |        |
| scan       | `prefix_scan_schema/10k`            |                   |               |              |                 |       |        |
| scan       | `prefix_scan_schema_file_null/10k`  |                   |               |              |                 |       |        |

#### Diff / Materialization

| row                                    | before median | after median | delta | shape status |
| -------------------------------------- | ------------: | -----------: | ----: | ------------ |
| `changed_keys_update_10pct/10k`        |               |              |       |              |
| `changed_keys_delta_chain_10x1pct/10k` |               |              |       |              |
| `materialize_delta_chain_10x1pct/10k`  |               |              |       |              |

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
| 10k physical write budget <= 30-50ms target       |             |        |
| 10k physical write hard review <= 3x raw SQLite   |             |        |
| exact reads <= 3x raw SQLite                      |             |        |
| scans <= 3x raw SQLite                            |             |        |
| header-only scans do not hydrate full JSON values |             |        |
| SQLite and RocksDB both reported                  |             |        |
| storage growth explained                          |             |        |
| backend boundary copy cost explained              |             |        |
| no tracked logic moved to untracked paths         |             |        |
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
