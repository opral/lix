# SQL changelog one-read/one-facade correction oracle

This directory is TEST/REPORT-only. It defines the narrow correction gate for
the immutable c899 SQL changelog successor. It does not edit production, add a
runtime path, compile, run adapters, or replace the v4 semantic approval.

## Immutable anchors

```text
candidate base/head: c8992e070a9a988a695bdb77f9a49e214431a5bc
candidate tree:      3f154834ae88b69a71c373b28af13eebc4e42c4b
parent:              e1af471b9ab0f598dafa7c2ddec7867667c81740
parent..head diff:   854629db1d8d4d3f07aee09154863b60ab095e6714eae84a9e3c2dec1fecb594
stable patch-id:     6452320dc67df83314b0d64ffbc628717881e488

approved v4:         d36e1fe34c4bf93c01478b876a5f73a8dccd33de
v4 tree:             7fcaeb5f965d5c8b641e5e82a741c75cc18365b6
v4 exact RED:        74d2a1d2512ece658aa213e235142935c161a81bd3d859b2c1ffa8ae2006c0a5

runtime checklist:   53e65da95f8127dd2c7f785c1792b174085853e42f84672a0d3ec6fcc88ff677
dormant f1dc:        f1dcfb94d7ad46dfa237e0518acc58780b45ed25
```

The oracle must be run against an immutable detached candidate. Its expected
calibration on c899 is `ONE_FACADE_ORACLE=RED`; no compiler/runtime claim is
made while the candidate remains at the reported 133 errors / 9 warnings.

## Exact package scope

The candidate diff from c899 may contain only these package files:

```text
test-reports/stage2-sql-change-one-facade-oracle-c899/README.md
test-reports/stage2-sql-change-one-facade-oracle-c899/REPORT.md
test-reports/stage2-sql-change-one-facade-oracle-c899/SHA256SUMS
test-reports/stage2-sql-change-one-facade-oracle-c899/verify_one_facade.py
test-reports/stage2-sql-change-one-facade-oracle-c899/fixtures/two_facades.rs
test-reports/stage2-sql-change-one-facade-oracle-c899/fixtures/separate_history_graph_reader.rs
test-reports/stage2-sql-change-one-facade-oracle-c899/fixtures/valid_shared_reader.rs
```

Any production, harness, adapter, compatibility, format, or unrelated report
path is an immediate scope failure.

## Structural contract

The SQL read-session boundary must bind exactly one operation-owned
`ChangelogQuerySource` and exactly one operation-scoped `CommitGraphReader`
over the same retained `StorageRead`. The same source identity (or its exact
`Arc` clone) must be passed to `lix_change`, `lix_diff`, history, and working-
diff registration. The graph capability is intentionally permitted here as a
single shared W1a authority; total `CommitGraphReader` deletion is out of this
correction.

The verifier requires, structurally and function-scoped:

* one source acquisition and one graph acquisition in each normal and
  transaction SQL read-session boundary;
* one total `ForkTreeReadFacade` construction in each session/transaction/test
  source factory, with no history-specific second construction;
* `register_read`, `register_transaction`, and their catalog helper receive
  the source and shared graph as parameters rather than calling context
  constructors per provider;
* `change.rs` uses only the exact operation source's typed ForkTree reader for
  direct and derived scans/lookups;
* `diff.rs` carries the exact source reader through `DiffFunction`, `DiffSpec`,
  and both before/after chronology calls;
* history chronology uses the shared graph capability, while state and
  working-diff traversal uses the exact operation source's ForkTree reader;
* no provider-local `ForkTreeReadFacade::new`, `CommitGraphContext::new().reader`,
  `begin_read`, raw store, cache/reset, compatibility reader, fallback, or
  mismatched receiver/call argument;
* no read-phase write, commit, flush, or compaction route.

The two negative fixtures reject an extra facade and a local/per-provider graph
reader. The positive fixture proves the verifier is not an unconditional RED
switch and checks the shared reader/graph call shape.

## v4 semantic controls retained unchanged

The later compiler-green runtime gate must preserve all ten controls:

1. direct authenticated change;
2. derived `lix_commit` change without duplicate output;
3. authenticated absence as an empty result;
4. missing required catalog/record as typed corruption;
5. malformed required change as typed corruption;
6. wrong kind/domain substitution as typed corruption;
7. wrong embedded change ID as typed identity failure;
8. duplicate logical ID rejected before output/order/`LIMIT`;
9. canonical merged ordering and deduplication;
10. validation and ordering before `LIMIT`.

Cold reopen must reproduce canonical `lix_change`, history, and `lix_diff`
digests. Memory, RocksDB, and SlateDB must agree; read operations must issue
one retained read, zero backend writes, and zero logical commits/flushes.

## Dormant focused commands

These are inherited f1dc commands, frozen but not executable until the source
gate and bounded compiler frontier are green. Every cell is capped at 1200s:

```bash
LIX_CHANGE_BACKEND=memory LIX_CHANGE_ROWS=1000 LIX_CHANGE_HISTORY_DEPTH=4 \
LIX_CHANGE_FIXTURE=runtime-v1 \
CARGO_TARGET_DIR=/tmp/lix-stage2-sql-change-runtime-memory-target \
timeout 1200 cargo test -p lix_benchmarks \
  --test sql_changelog_runtime_qualification \
  --features 'storage-benches slatedb' -- \
  --ignored --exact memory_sql_changelog_runtime --nocapture --test-threads=1

LIX_CHANGE_BACKEND=rocksdb LIX_CHANGE_ROWS=1000 LIX_CHANGE_HISTORY_DEPTH=4 \
LIX_CHANGE_FIXTURE=runtime-v1 \
CARGO_TARGET_DIR=/tmp/lix-stage2-sql-change-runtime-rocks-target \
timeout 1200 cargo test -p lix_benchmarks \
  --test sql_changelog_runtime_qualification \
  --features 'storage-benches slatedb' -- \
  --ignored --exact rocksdb_sql_changelog_runtime --nocapture --test-threads=1

LIX_CHANGE_BACKEND=slatedb LIX_CHANGE_ROWS=1000 LIX_CHANGE_HISTORY_DEPTH=4 \
LIX_CHANGE_FIXTURE=runtime-v1 \
CARGO_TARGET_DIR=/tmp/lix-stage2-sql-change-runtime-slate-target \
timeout 1200 cargo test -p lix_benchmarks \
  --test sql_changelog_runtime_qualification \
  --features 'storage-benches slatedb' -- \
  --ignored --exact slatedb_sql_changelog_runtime --nocapture --test-threads=1
```

The commands remain dormant if the target is absent, source is RED, compiler
diagnostics remain, or any identity/corruption/read-write control fails.
