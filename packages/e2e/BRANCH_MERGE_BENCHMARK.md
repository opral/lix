# Branch and merge qualification benchmark

`branch_merge_benchmark` is a correctness-gated benchmark for branch creation,
branch switching and deletion, historical diff, merge preview, and merge commit. The controller launches one
fresh process per scenario so allocator retention, RocksDB caches, and WASM
instances cannot leak between samples. Each worker emits exactly one JSON
object on stdout; concatenate stdout to retain JSONL suitable for comparison.

RocksDB is the default backend. Set
`LIX_BRANCH_MERGE_BENCH_STORAGE=slatedb` to run the same qualification or
worker against SlateDB.

## Commands

Build and run qualification cases. Publishable artifacts must embed the exact
source revision and compiler identity; each worker also hashes its executable
before starting the process timer:

```sh
LIX_BENCH_COMMIT_SHA="$(git rev-parse HEAD)" \
LIX_BENCH_RUSTC_VERSION="$(rustc --version)" \
  cargo build --release -p lix_e2e --example branch_merge_benchmark
target/release/examples/branch_merge_benchmark qualification > branch-merge.jsonl
```

Each schema-v2 record includes those values, the executable SHA-256, Cargo
profile, target architecture, and target OS. A record containing `unrecorded`
provenance is useful for local diagnosis but is not publishable evidence.

Set `LIX_BRANCH_MERGE_BENCH_SAMPLES=11` to run every configuration in eleven
separate child processes. Each JSON object includes its zero-based `sample`, so
consumers can calculate p50/p95/p99 without mixing process state.

Run the scaling matrix:

```sh
target/release/examples/branch_merge_benchmark sweep > branch-merge-sweep.jsonl
```

Run an individual normalized-row worker. Arguments are layer, scenario, total
rows, changed rows per side, divergent commits per side, common-history
commits, live branches, and payload bytes. For file workers, the branches slot
sets the number of unaffected control files per plugin:

```sh
target/release/examples/branch_merge_benchmark \
  worker rows clean 10000 100 10 100 8 64
```

Run the all-plugin semantic-file qualification worker:

```sh
target/release/examples/branch_merge_benchmark \
  worker files all_plugins_resolvable 2 5 1 0 1 32
```

By default temporary RocksDB databases are created under `target/` rather than
the often size-limited system temporary filesystem. Set
`LIX_BRANCH_MERGE_BENCH_ROOT` to place them on a dedicated benchmark volume.

## Measurements and correctness gates

Workers report wall and CPU time, exact Rust allocation traffic, operation-local
baseline/sampled-peak/retained RSS, process physical I/O deltas, merge tracing
phases, parameters, merge outcome, and plugin transition counters. Row workers
report storage size separately after merge and after durable fanout-branch
deletion, and include deletion-local process I/O. The 1 ms RSS
sampler begins immediately before each operation and stops immediately afterward;
incremental peak is the sampled peak minus the pre-operation RSS. Because a
sub-millisecond RSS spike can fall between samples, use allocation traffic and
retained RSS as the gates for short operations; sampled peak RSS is a hard gate
only when the measured operation lasts at least 5 ms. Native RocksDB allocations
remain visible to RSS but not the Rust allocator counter. Use a release binary on
an otherwise idle host for publishable results.

Linux process-I/O counters come from `/proc/self/io`. The harness does not evict
or otherwise control the OS page cache, so these counters are a lower bound on
physical device traffic: a zero read delta does not prove zero logical reads or
absence of read amplification. Use isolated cold-cache runs or backend-native
logical counters when making cold-read amplification claims.

Normalized-row cases compare Lix with a separate row-level three-way model and
compare `lix_diff` row counts with an independent map diff. Branch fanout reports
first, median, last, and maximum creation latency instead of hiding progression in
one batch mean. They assert preview non-mutation, preview/commit agreement, failed-merge
atomicity, source and target branch isolation, correct two-parent merge commits,
idempotent re-merge, and close/reopen stability. The qualification matrix covers
already-up-to-date, fast-forward, disjoint modifications, equal convergence,
modify/modify conflicts, deletes, delete/modify conflicts, equal additions,
conflicting additions, and batches mixing clean picks with conflicts.

The file case installs text, Markdown, JSON, CSV, and Excalidraw together. It
creates divergent semantic changes in all five file types, requires plugin
conflict resolvers to run, checks exact Text/Markdown/CSV materialization,
parses and checks JSON/Excalidraw values, checks semantic row counts, verifies
the source branch is unchanged, proves one unaffected file per plugin retained
its original change identity, and asserts exact plugin transition counters so
unaffected owners cannot run and discard identical output. The sweep scales
unaffected controls from 5 to 500 files. Both semantic and byte checks repeat
after a cold database reopen.

## Interpreting scaling runs

Treat absolute thresholds as host-specific and derive them from repeated
candidate/baseline runs on the same machine. The required scaling properties
are host-independent:

- Branch creation must remain independent of total tracked rows and history.
- Clean preview and merge must scale with divergent rows, not repository rows.
- Historical diff must scale with changed rows, not repository rows or common history.
- Unrelated common history may affect merge-base lookup but must not amplify
  row analysis or plugin materialization.
- Incremental peak RSS must scale with changed rows and affected files, not
  all live rows or all installed plugins.
- Preview must retain no durable state; rejected merge must retain neither rows
  nor a branch-head move.
- File merge must invoke only the owners of affected files and must not
  materialize unaffected files.
- With five affected files, scaling unaffected files from 5 to 500 must keep
  preview and merge p50 within 2× and add no plugin transition calls.

For CI gates, collect at least 11 isolated samples per point, compare medians
for latency/RSS and p95 for tail latency, and reject only when both the relative
regression and a practical absolute floor are exceeded. Initial numeric budgets
belong in a dated results document produced from release-mode baseline runs;
they must not be guessed from debug builds.
