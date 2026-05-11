# Optimization Log 9: SQL2 Logical CRUD

Goal: make the logical work inside `sql2` fast for an isolated JSON-pointer
CRUD benchmark surface owned by this log.

The pure target is SQL2 overhead: statement classification, SQL parsing,
DataFusion logical planning, provider scan planning, DML normalization, SQL
runtime collection, parameter conversion, and result conversion.

All optimization changes in this log must stay inside the `sql2` module. If a
profile shows that SQL2 is slow because it lacks a better read/write primitive
from another module, record that as an outside-SQL2 follow-up and keep the code
change out of this log.

## Benchmark Fit

The scorecard benchmark for this log is:

```sh
cargo bench -p lix_engine --bench optimization9_sql2 --features storage-benches -- 'optimization9_sql2/smoke_crud'
```

The important isolated E2E groups are:

```text
optimization9_sql2/smoke_crud/lix_sqlite
optimization9_sql2/smoke_crud/lix_rocksdb
```

The CRUD operations already exercise the SQL2 path through
`SessionContext::execute`:

```text
insert_all_rows/1k
select_all_path_value/1k
select_one_by_pk/1k
update_all_values/1k
update_one_by_pk/1k
delete_all_rows/1k
delete_one_by_pk/1k
```

No raw SQLite, raw storage, branch, merge, or shared fixture rows belong to the
Log 9 scorecard. If a profile points below SQL2, record the finding as an
outside-SQL2 follow-up instead of expanding this benchmark.

```text
optimization9_sql2/smoke_crud:
  isolated Log 9 scorecard

optimization9_sql2 diagnostic groups:
  planning/execution/literal-vs-parameterized microscope
```

This keeps the SQL2 CRUD campaign independent from other benchmark suites and
optimization logs.

## Why This Is A SQL2 Benchmark

Each Lix CRUD benchmark iteration excludes fixture setup via Criterion
`iter_batched`, then measures one user-visible SQL operation. Inside the measured
operation, the call path is:

```text
SessionContext::execute
  -> sql2::classify_statement
  -> sql2::create_logical_plan or sql2::create_write_logical_plan
  -> build_read_session or build_write_session
  -> DataFusion create_logical_plan
  -> provider logical planning / DML normalization
  -> sql2::execute_logical_plan
  -> sql2::runtime::collect_dataframe
  -> query_result_from_batches / affected_rows_from_query_result
```

That is exactly the logical SQL2 surface we need to optimize. The benchmark is
especially useful because it covers both:

```text
read SQL:
  SELECT path, value FROM json_pointer ORDER BY path
  SELECT path, value FROM json_pointer WHERE path = '<path>'

write SQL:
  INSERT INTO json_pointer (path, value) VALUES ...
  UPDATE json_pointer SET value = ...
  UPDATE json_pointer SET value = ... WHERE path = '<path>'
  DELETE FROM json_pointer
  DELETE FROM json_pointer WHERE path = '<path>'
```

## Dedicated Diagnostic Bench

`optimization9_sql2` is the dedicated SQL2 diagnostic suite for this log:

```sh
cargo bench -p lix_engine --bench optimization9_sql2 --features storage-benches
```

It uses local copies of the JSON-pointer fixture and schema so the suite is
isolated from `json_pointer_crud` and `plugin-json-v2` paths:

```text
packages/engine/benches/optimization9_sql2/pnpm-lock.fixture.json
packages/engine/benches/optimization9_sql2/json_pointer.schema.json
```

It is intentionally small and self-contained. Its job is to separate SQL2
planning cost from SQL2 execution cost and to compare literal vs parameterized
point CRUD statements.

Benchmark groups:

```text
optimization9_sql2/smoke_crud/lix_sqlite
optimization9_sql2/smoke_crud/lix_rocksdb

optimization9_sql2/planning_only/lix_sqlite
optimization9_sql2/planning_only/lix_rocksdb

optimization9_sql2/execute_preplanned/lix_sqlite
optimization9_sql2/execute_preplanned/lix_rocksdb

optimization9_sql2/e2e_literal/lix_sqlite
optimization9_sql2/e2e_literal/lix_rocksdb

optimization9_sql2/e2e_parameterized/lix_sqlite
optimization9_sql2/e2e_parameterized/lix_rocksdb
```

Diagnostic rows:

```text
smoke_crud:
  insert_all_rows/1k
  select_all_path_value/1k
  select_one_by_pk/1k
  update_all_values/1k
  update_one_by_pk/1k
  delete_all_rows/1k
  delete_one_by_pk/1k

planning_only:
  select_all_path_value/1k
  select_one_by_pk/1k
  insert_500_values/1k
  update_all_values/1k
  delete_all_rows/1k

execute_preplanned:
  select_all_path_value/1k
  select_one_by_pk/1k

e2e_literal:
  select_one_by_pk/1k
  update_one_by_pk/1k
  delete_one_by_pk/1k

e2e_parameterized:
  select_one_by_pk/1k
  update_one_by_pk/1k
  delete_one_by_pk/1k
```

The split means:

```text
smoke_crud:
  isolated 1k CRUD scorecard for this optimization log

planning_only:
  parse/classify/session construction/DataFusion logical planning/provider setup

execute_preplanned:
  physical collection/provider scan/result conversion after read SQL is planned

e2e_literal vs e2e_parameterized:
  statement planning plus execution through public SessionContext::execute
```

Write `execute_preplanned` rows are intentionally not present yet. SQL2 write
providers currently rely on a transaction-scoped `SqlWriteContext` pointer whose
planning and execution must stay inside the same write frame. The suite records
write planning separately and uses E2E literal/parameterized rows for write
execution until SQL2 has a safe write-plan diagnostic boundary.

## Profiler Workflow

Use the profiler before changing code. Profile one operation at a time so the
flamegraph is readable.

Primary filters:

```sh
cargo bench -p lix_engine --features storage-benches --bench optimization9_sql2 -- 'optimization9_sql2/smoke_crud/lix_sqlite'
cargo bench -p lix_engine --features storage-benches --bench optimization9_sql2 -- 'optimization9_sql2/planning_only/lix_sqlite/insert_500_values/1k'
cargo bench -p lix_engine --features storage-benches --bench optimization9_sql2 -- 'optimization9_sql2/planning_only/lix_sqlite/update_all_values/1k'
cargo bench -p lix_engine --features storage-benches --bench optimization9_sql2 -- 'optimization9_sql2/planning_only/lix_sqlite/delete_all_rows/1k'
cargo bench -p lix_engine --features storage-benches --bench optimization9_sql2 -- 'optimization9_sql2/execute_preplanned/lix_sqlite/select_one_by_pk/1k'
cargo bench -p lix_engine --features storage-benches --bench optimization9_sql2 -- 'optimization9_sql2/e2e_parameterized/lix_sqlite/select_one_by_pk/1k'
```

Repeat the same filters for `lix_rocksdb` only after the SQLite profile has a
clear hypothesis. If both backends show the same SQL2 stack, optimize SQL2. If
they diverge below the SQL2 boundary, capture the missing primitive or backend
cost as a later outside-SQL2 optimization lead.

Record the top stacks in each entry with this classification:

```text
sql2 planning:
  classify_statement, validate_supported_statement_ast, build_*_session,
  create_logical_plan, validate_supported_logical_plan,
  validate_json_predicates_in_logical_plan, provider table scan planning

sql2 execution glue:
  execute_logical_plan, collect_dataframe, parameter conversion,
  query_result_from_batches, affected row conversion

provider logical work:
  predicate extraction, projection mapping, DML normalization,
  insert/update/delete batch construction, value JSON coercion

not SQL2:
  backend IO, tracked-state materialization, delta decoding, commit graph,
  RocksDB/SQLite storage write application

outside-SQL2 follow-up:
  missing read/write primitive, storage/provider API limitation, layout issue,
  or backend-specific behavior that SQL2 cannot fix internally
```

## Initial Scorecard

The scorecard for this log is isolated in `optimization9_sql2/smoke_crud`.
Do not use rows from any other benchmark suite as Log 9 baselines.

Baseline command:

```sh
cargo bench -p lix_engine --features storage-benches --bench optimization9_sql2
```

Baseline commit:

```text
1010c12c plus uncommitted Log 9 benchmark files
```

Initial isolated 1k smoke CRUD rows after rebasing onto
`origin/physical-layout-manual-goal-ii-`:

| operation               |       Lix SQLite |      Lix RocksDB |
| ----------------------- | ---------------: | ---------------: |
| `insert_all_rows`       | 62.714-72.740 ms | 52.627-57.653 ms |
| `select_all_path_value` | 18.980-20.138 ms | 9.9962-11.163 ms |
| `select_one_by_pk`      | 7.6860-9.2848 ms | 2.2846-2.7899 ms |
| `update_all_values`     | 53.337-123.20 ms | 19.038-20.238 ms |
| `update_one_by_pk`      | 8.5795-13.785 ms | 4.5116-4.7572 ms |
| `delete_all_rows`       | 30.914-33.230 ms | 21.999-25.876 ms |
| `delete_one_by_pk`      | 7.2750-7.9671 ms | 4.4184-5.2644 ms |

Initial `optimization9_sql2` diagnostic rows after rebase:

| group                | operation                  |       Lix SQLite |      Lix RocksDB |
| -------------------- | -------------------------- | ---------------: | ---------------: |
| `planning_only`      | `select_all_path_value/1k` | 3.3115-3.7821 ms | 1.6485-1.9012 ms |
| `planning_only`      | `select_one_by_pk/1k`      | 2.9706-4.9726 ms | 1.6292-1.8691 ms |
| `planning_only`      | `insert_500_values/1k`     | 11.099-11.953 ms | 11.316-12.420 ms |
| `planning_only`      | `update_all_values/1k`     | 3.5833-3.9703 ms | 2.1247-2.3981 ms |
| `planning_only`      | `delete_all_rows/1k`       | 3.6369-4.0269 ms | 2.0014-2.2900 ms |
| `execute_preplanned` | `select_all_path_value/1k` | 8.7746-9.3653 ms | 8.8134-9.7773 ms |
| `execute_preplanned` | `select_one_by_pk/1k`      | 1.3400-1.4785 ms | 1.4099-1.8420 ms |
| `e2e_literal`        | `select_one_by_pk/1k`      | 3.8340-4.1884 ms | 2.4221-3.5113 ms |
| `e2e_literal`        | `update_one_by_pk/1k`      | 7.0420-8.2160 ms | 4.4839-5.3388 ms |
| `e2e_literal`        | `delete_one_by_pk/1k`      | 7.4717-7.9987 ms | 4.2601-5.5313 ms |
| `e2e_parameterized`  | `select_one_by_pk/1k`      | 3.7137-4.0738 ms | 2.1038-2.4607 ms |
| `e2e_parameterized`  | `update_one_by_pk/1k`      | 7.5761-9.0774 ms | 4.1165-4.7877 ms |
| `e2e_parameterized`  | `delete_one_by_pk/1k`      | 7.4651-8.2425 ms | 4.4257-5.1296 ms |

Hetzner CX33 baseline rerun on 2026-05-11:

```text
Machine: Hetzner CX33
Host: ubuntu-32gb-hil-1
CPU: 8 vCPU, AMD EPYC-Milan Processor, KVM
Kernel: Linux 6.8.0-90-generic x86_64
Commit: 9ff4f9cb
Command: cargo bench -p lix_engine --features storage-benches --bench optimization9_sql2
```

Hetzner CX33 isolated 1k smoke CRUD rows:

| operation               |       Lix SQLite |      Lix RocksDB |
| ----------------------- | ---------------: | ---------------: |
| `insert_all_rows`       | 70.105-71.910 ms | 67.767-68.316 ms |
| `select_all_path_value` | 17.530-17.943 ms | 13.421-13.936 ms |
| `select_one_by_pk`      | 6.6463-6.9219 ms | 2.9247-3.0022 ms |
| `update_all_values`     | 34.429-35.507 ms | 25.341-25.724 ms |
| `update_one_by_pk`      | 10.367-10.581 ms | 6.3116-6.4393 ms |
| `delete_all_rows`       | 35.935-36.724 ms | 26.690-27.071 ms |
| `delete_one_by_pk`      | 10.616-10.778 ms | 6.4811-6.6185 ms |

Hetzner CX33 `optimization9_sql2` diagnostic rows:

| group                | operation                  |       Lix SQLite |      Lix RocksDB |
| -------------------- | -------------------------- | ---------------: | ---------------: |
| `planning_only`      | `select_all_path_value/1k` | 5.7264-5.8371 ms | 2.1837-2.3126 ms |
| `planning_only`      | `select_one_by_pk/1k`      | 5.3823-5.5152 ms | 2.2103-2.2705 ms |
| `planning_only`      | `insert_500_values/1k`     | 14.105-14.283 ms | 12.987-13.275 ms |
| `planning_only`      | `update_all_values/1k`     | 6.3326-6.4489 ms | 2.7961-2.8708 ms |
| `planning_only`      | `delete_all_rows/1k`       | 6.2279-7.0361 ms | 2.6768-2.7504 ms |
| `execute_preplanned` | `select_all_path_value/1k` | 11.515-11.711 ms | 11.964-12.364 ms |
| `execute_preplanned` | `select_one_by_pk/1k`      | 1.5469-1.5784 ms | 1.6215-1.6790 ms |
| `e2e_literal`        | `select_one_by_pk/1k`      | 6.3640-6.4680 ms | 2.9476-2.9911 ms |
| `e2e_literal`        | `update_one_by_pk/1k`      | 9.9933-10.128 ms | 6.1048-6.2638 ms |
| `e2e_literal`        | `delete_one_by_pk/1k`      | 10.509-11.015 ms | 6.4548-6.6268 ms |
| `e2e_parameterized`  | `select_one_by_pk/1k`      | 6.5033-6.6564 ms | 3.1192-3.2197 ms |
| `e2e_parameterized`  | `update_one_by_pk/1k`      | 10.169-11.111 ms | 6.4063-6.6222 ms |
| `e2e_parameterized`  | `delete_one_by_pk/1k`      | 10.407-10.631 ms | 6.4029-6.5440 ms |

Interpretation:

```text
The benchmark suite is good enough to start optimizing SQL2 CRUD now.
The highest-value SQL2 profiles are insert_all_rows, delete_all_rows, and
update_all_values, with PK read/update/delete as planning/provider overhead
probes. Full scan is the lowest priority within this isolated scorecard because
it is already much closer than insert and bulk writes.
```

SQL2-only boundary:

```text
Allowed edit surface:
  packages/engine/src/sql2/**

Not allowed in this log:
  storage layout changes
  tracked-state reader/writer changes
  live-state changes
  transaction staging changes outside SQL2
  benchmark success achieved by changing backend behavior

Required handling for outside-SQL2 findings:
  Record the profile evidence, name the missing primitive or non-SQL2 bottleneck,
  and leave it for a future non-SQL2 optimization log.
```

## Optimization Order

1. `insert_all_rows`
2. `delete_all_rows`
3. `update_all_values`
4. `update_one_by_pk` and `delete_one_by_pk`
5. `select_one_by_pk`
6. `select_all_path_value`

Rationale:

```text
Insert is still hundreds of milliseconds for 1k rows and executes the richest
SQL2 write path: large VALUES planning, JSON literal coercion, insert
normalization, identity/default handling, and staging.

Bulk delete and update are the best probes for avoidable provider logical work
over many current rows.

Single-row PK operations isolate per-statement SQL2 overhead. They are small in
absolute time now, but they reveal whether SQL2 is doing too much planning or
provider setup for point operations.
```

## Candidate Optimization Themes

Do not implement these blindly. Each needs a profile entry first.

```text
Session/catalog setup:
  avoid rebuilding expensive read/write DataFusion session state per statement
  when visible schemas and functions are unchanged inside a benchmark session

Logical-plan validation:
  collapse repeated recursive walks over the same DataFusion logical plan
  combine support validation, JSON predicate validation, notices, and statement
  kind classification where possible

DML normalization:
  reduce per-row cloning and JSON string/value round trips for INSERT VALUES
  build typed row batches directly from DataFusion expressions when safe

Provider scan planning:
  push path equality filters into exact-key load requests early
  avoid broad scan request construction for single-PK SELECT/UPDATE/DELETE

Result conversion:
  avoid unnecessary cloning of column metadata and JSON values
  keep affected-row write results minimal

Runtime collection:
  make SQL2 collect only the needed batches/columns for affected-row DML
  avoid full row materialization when the operation only needs a count
```

## Keep Criteria

For every kept optimization:

```text
primary:
  improves the targeted Lix SQLite 1k smoke CRUD row by >= 10%
  does not regress any other Lix SQLite 1k CRUD row by > 5%

cross-backend:
  improves or stays neutral on the matching Lix RocksDB row
  any backend split is explained by profile evidence

guardrails:
  benchmark suite stays isolated to optimization9_sql2 fixture/schema files
  any non-SQL2 bottleneck is recorded as outside-SQL2 follow-up
  sql2 and code-structure tests pass
```

Verification commands:

```sh
cargo bench -p lix_engine --features storage-benches --bench optimization9_sql2
cargo test -p lix_engine sql2
cargo test -p lix_engine --test code_structure sql2
```

## Entry Template

Use one entry per kept SQL2 optimization.

```text
## Optimization N: <short name>

Commit:
  <hash> or uncommitted on <hash>

Target operation:
  insert_all_rows | select_all_path_value | select_one_by_pk |
  update_all_values | update_one_by_pk | delete_all_rows |
  delete_one_by_pk

Profile before:
  command:
  top SQL2 stacks:
  non-SQL2 stacks:
  conclusion:

Change:
  What changed?
  Why does this reduce logical SQL2 work?
  What semantic invariant is preserved?

Results:
  Include impacted optimization9_sql2 diagnostic rows.
  Include optimization9_sql2/smoke_crud Lix SQLite and Lix RocksDB rows for
  every CRUD operation.

Guardrails:
  Confirm the benchmark still uses only local optimization9_sql2 fixture/schema
  files.

Outside-SQL2 follow-up:
  If the profile points to a missing primitive or non-SQL2 bottleneck, record it
  here. Do not include that implementation in this log.

Decision:
  Keep, revert, or follow-up.
```

## Optimization 1: Reuse Parsed DataFusion Statement For Write Planning

Commit:
  uncommitted on 80f4f68a

Target operation:
  logical planning for optimization9_sql2/planning_only/lix_sqlite/insert_500_values/1k

Profile before:
  command:
    perf record --output=/tmp/sql2-insert-plan.perf.data -F 499 -g --call-graph dwarf target/release/deps/optimization9_sql2-bd3fa4efccf19070 --bench 'optimization9_sql2/planning_only/lix_sqlite/insert_500_values/1k' --profile-time 8
    perf report --stdio --quiet --no-inline --input=/tmp/sql2-insert-plan.perf.data --no-call-graph --sort=symbol --percent-limit=1
  top SQL2 stacks:
    sqlparser::tokenizer::Tokenizer::tokenize_quoted_string: 17.10% self
    sqlparser parser/tokenizer helpers collectively appeared below that hotspot
  non-SQL2 stacks:
    _int_malloc: 7.35%, __memmove_avx_unaligned_erms: 6.55%, malloc: 2.31%
  conclusion:
    SQL2 write planning parsed the same INSERT text multiple times: once for Lix AST validation/history target extraction and again through DataFusion planning. Large literal INSERT statements spend significant time tokenizing quoted JSON strings, so the duplicate parse is a first-order logical planning bottleneck.

Change:
  create_write_logical_plan now parses once into DataFusion's Statement with the SQL session parser, validates supported Lix SQL against that AST, extracts read-only history DML targets from the same AST, and passes the same Statement to SessionState::statement_to_plan.
  The cheap parse/validate/read-only phase now runs before write provider registration. The write session is built only after parse and policy checks succeed.
  DML target extraction normalizes unquoted identifiers to lowercase while preserving quoted identifiers, matching DataFusion's identifier normalization rule.
  Added coverage for read-only history DML through lowercase, uppercase, schema-qualified uppercase, and EXPLAIN-wrapped DELETE targets.
  Read planning remains on the previous path, so this optimization is scoped to SQL2 write planning.

  Best-practice references:
    DataFusion exposes and uses the parse-once lower-level flow: sql_to_statement followed by statement_to_plan (artifact/datafusion/datafusion/core/src/execution/session_state.rs).
    DataFusion normalizes unquoted identifiers before planning (artifact/datafusion/datafusion/sql/src/planner.rs and artifact/datafusion/datafusion/sql/src/utils.rs).
    SpiceAI intercepts parsed statements before planning for DataFusion integration work (artifact/spiceai/crates/runtime/src/datafusion/planner/mod.rs).
    Turso's standalone DB flow parses SQL into AST before translation/codegen (artifact/turso/docs/manual.md).

  Semantic invariant preserved:
    Statement support checks, history-view read-only enforcement, and DataFusion logical planning all inspect the same parsed statement. Unsupported DataFusion extension statements are still rejected before planning.

Results:
  Focused planning rows after review fixes:
    optimization9_sql2/planning_only/lix_sqlite/insert_500_values/1k:
      [7.5696 ms 7.7067 ms 7.9807 ms]
      vs logged baseline [14.105 ms 14.283 ms], about 44-46% faster.
    optimization9_sql2/planning_only/lix_sqlite/update_all_values/1k:
      [6.5332 ms 6.6237 ms 6.7164 ms], neutral vs logged baseline [6.3326 ms 6.4489 ms].
    optimization9_sql2/planning_only/lix_sqlite/delete_all_rows/1k:
      [6.3737 ms 6.4816 ms 6.6179 ms], neutral vs logged baseline [6.2279 ms 7.0361 ms].

  Smoke CRUD guardrail after review fixes:
    Lix SQLite:
      insert_all_rows: [59.787 ms 60.000 ms 60.251 ms], faster than baseline [70.105 ms 71.910 ms]
      select_all_path_value: [16.936 ms 17.095 ms 17.266 ms], neutral/faster than baseline [17.530 ms 17.943 ms]
      select_one_by_pk: [6.4369 ms 6.5101 ms 6.5946 ms], neutral/faster than baseline [6.6463 ms 6.9219 ms]
      update_all_values: [33.796 ms 34.192 ms 34.606 ms], neutral/faster than baseline [34.429 ms 35.507 ms]
      update_one_by_pk: [10.334 ms 10.408 ms 10.480 ms], neutral vs baseline [10.367 ms 10.581 ms]
      delete_all_rows: [34.715 ms 34.957 ms 35.215 ms], neutral/faster than baseline [35.935 ms 36.724 ms]
      delete_one_by_pk: [10.624 ms 10.686 ms 10.751 ms], neutral vs baseline [10.616 ms 10.778 ms]
    Lix RocksDB:
      insert_all_rows: [59.644 ms 60.006 ms 60.461 ms], faster than baseline [67.767 ms 68.316 ms]
      select_all_path_value: [13.053 ms 13.142 ms 13.238 ms], neutral/faster than baseline [13.421 ms 13.936 ms]
      select_one_by_pk: [2.9783 ms 2.9920 ms 3.0078 ms], neutral vs baseline [2.9247 ms 3.0022 ms]
      update_all_values: [25.567 ms 25.748 ms 25.948 ms], neutral vs baseline [25.341 ms 25.724 ms]
      update_one_by_pk: [6.3481 ms 6.4059 ms 6.4673 ms], neutral vs baseline [6.3116 ms 6.4393 ms]
      delete_all_rows: [27.078 ms 27.294 ms 27.545 ms], neutral vs baseline [26.690 ms 27.071 ms]
      delete_one_by_pk: [6.4115 ms 6.4388 ms 6.4659 ms], neutral/faster than baseline [6.4811 ms 6.6185 ms]

Post-change profile:
  command:
    perf record --output=/tmp/sql2-insert-plan-after.perf.data -F 499 -g --call-graph dwarf target/release/deps/optimization9_sql2-bd3fa4efccf19070 --bench 'optimization9_sql2/planning_only/lix_sqlite/insert_500_values/1k' --profile-time 8
    perf report --stdio --quiet --no-inline --input=/tmp/sql2-insert-plan-after.perf.data --no-call-graph --sort=symbol --percent-limit=1
  result:
    sqlparser::tokenizer::Tokenizer::tokenize_quoted_string dropped from 17.10% to 8.53% self. This profiler percentage is diagnostic evidence that the targeted duplicate-parse hot stack was reduced; it is not the keep threshold.
    The keep threshold is benchmark speedup: insert_500_values planning improved by about 44-46%, and the corresponding SQLite smoke insert row improved by about 14-17%, both above the required >=10% speed improvement.
    Remaining top entries are allocator/memory movement or broadly distributed DataFusion/schema work.

Review:
  First review reported no HIGH findings and two MEDIUM findings:
    normalize unquoted DML target identifiers consistently with DataFusion;
    parse/validate before write session/provider construction.
  Both MEDIUM findings were implemented.
  Second review reported no HIGH or MEDIUM findings.

Guardrails:
  Benchmark remains isolated to optimization9_sql2 fixture/schema files.
  SQL2 and code-structure tests pass:
    cargo test -p lix_engine execute_sql_rejects_writes_to_history_views_before_planning --features storage-benches
    cargo test -p lix_engine sql2 --features storage-benches

Outside-SQL2 follow-up:
  SessionContext::execute still performs a separate pre-SQL2 classification parse in packages/engine/src/session/execute.rs before dispatching to create_write_logical_plan. This is outside the SQL2-only implementation scope and should be addressed separately if end-to-end parse elimination is desired.

Decision:
  Keep.

Completion audit:
  Additional post-change logical-planning profiles were used as diagnostics after verifying the benchmark speedup. They check whether the optimization exposed another dominant planning stack, but the keep/revert decision remains based on >=10% benchmark speed improvement:
    insert_500_values/1k:
      sqlparser::tokenizer::Tokenizer::tokenize_quoted_string: 8.53%
      _int_malloc: 8.01%
    select_all_path_value/1k:
      _int_malloc: 6.24%
      malloc: 2.78%
      DataFusion simplification symbols below 1%
    select_one_by_pk/1k:
      _int_malloc: 6.43%
      malloc: 2.36%
      DataFusion simplification symbols below 1%
    delete_all_rows/1k:
      _int_malloc: 7.38%
      malloc: 2.30%
      DataFusion simplification symbols below 1%

  The previous insert-planning SQL tokenizer hot stack was reduced, and the benchmark speedup exceeds the required >=10% improvement. The remaining visible costs are allocator/general DataFusion work spread across the logical-planning profiles.
