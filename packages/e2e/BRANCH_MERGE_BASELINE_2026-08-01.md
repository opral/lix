# Branch and merge baseline — 2026-08-01

This qualification baseline was captured from merged PR #1094 (`7fc7320a2`) plus
the benchmark and preview-instrumentation changes in this worktree. The host was Linux
7.0.0-15, one 16-core AMD EPYC-Genoa socket, 30 GiB RAM, and
`rustc 1.97.0-nightly (b954122bb 2026-05-20)`. Measurements used the release
example with RocksDB. These are single isolated samples for initial diagnosis;
numeric gates below require eleven samples before they are enforced in CI.

## Qualification results

All 13 correctness-gated cases passed. This includes already-up-to-date,
fast-forward, clean divergence, equal convergence, add/modify/delete and
delete/modify combinations, ordinary and mixed conflicts, warm all-plugin
merge, and cold-reopen all-plugin merge. Every case also passed preview
non-mutation, preview/commit agreement, source isolation, graph-parent, model
or semantic-byte oracle, and close/reopen checks.

| workload | preview | merge | preview peak RSS delta | merge peak RSS delta |
| --- | ---: | ---: | ---: | ---: |
| already up to date, 1k rows | 0.28 ms | 0.20 ms | 64 KiB | 0 |
| clean, 10k rows, 100 changes, 10 commits/side | 20.57 ms | 22.01 ms | 2.92 MiB | 1.30 MiB |
| modify conflict, same shape | 21.27 ms | 17.37 ms | 3.48 MiB | 0.68 MiB |
| all five plugins, warm | 2.14 ms | 4.48 ms | 96 KiB | 132 KiB |
| all five plugins, cold reopen | 9.24 ms | 30.80 ms | 132 KiB | 708 KiB |

The cold result varied between 14 and 31 ms in single runs, reinforcing that
published tail requirements need the documented 11-process sample protocol.

## Scaling findings

Branch creation is the dominant correctness-adjacent capacity problem. With
16 live branches, one base commit, one changed row per side, and fixed history,
mean creation latency was 16.7 ms at 1k rows, 73.6 ms at 10k, and 779.8 ms at
100k. Total retained RSS added while creating the branches was respectively
10.6, 67.0, and 276.3 MiB. At 10k rows, increasing live branches from 1 to 256
grew total creation time from 63 ms to 12.76 s and retained RSS from 14.1 to
136.2 MiB. Branch creation is therefore copying or materializing state in
proportion to repository contents instead of remaining a small ref operation.

Merge itself is substantially delta-oriented after base seeding is isolated in
one transaction. With 1/10/100 changed rows, one divergent commit, and 1,000
common-history commits, increasing repository rows from 1k to 100k changed
merge latency from 10.9/12.2/19.4 ms to 12.9/13.7/23.3 ms. However, unrelated
common history remains linear inside `merge_analysis`: at 10k rows and 100
changes, merge latency for 0/10/100/1k/10k history commits was
2.41/2.46/4.44/19.87/182.48 ms, of which analysis consumed
0.93/1.09/2.91/18.42/180.62 ms. Merge-base lookup stayed below 0.06 ms; the
optimization target is historical tracked-state analysis, not graph ancestry.

The all-plugin path is bounded at the tested scales. Warm merge rose from 4.23
ms for 5 affected files to 8.39 ms for 25, and from 4.23 ms at two semantic
rows per primary file to 8.18 ms at 100. Incremental merge RSS stayed below
235 KiB. Installing all five plugins added about 55.5 MiB RSS. In cold merge,
`merge_stage_semantic_rows` was the largest observed phase at roughly 10 ms;
in the warm case it was about 1.7 ms.

Unrelated plugin-owned files expose a separate scaling defect while keeping
plugin invocation bounded. With five affected files, increasing unaffected
controls from 5 to 500 raised preview from 2.21 ms to 18.55 ms and merge from
4.68 ms to 23.41 ms. Exact plugin counters stayed constant (one resolver call,
one full reparse, three semantic rows materialized, and seven guest exports), so
the amplification is host-side merge/diff analysis rather than plugin fanout.

## Qualification requirements

The following are the initial actionable budgets. Evaluate p50 latency and
incremental/retained RSS plus p95 latency over at least 11 isolated release
workers. A timing regression gate requires both a 15% relative regression and
a 2 ms absolute regression to avoid noise-only failures.

| requirement | budget |
| --- | --- |
| Correctness | Every oracle and invariant must pass; no tolerance or retry. |
| Branch creation complexity | At 100k rows, p50 ≤ 10 ms per branch and ≤ 2× the 1k-row result. |
| Branch memory | Retained RSS ≤ 1 MiB per newly created branch at 100k rows. |
| Branch switching | Round trip p50 ≤ 10 ms and incremental peak/retained RSS ≤ 1 MiB. |
| Row merge delta scaling | For 100 changes and one commit/side, p50 ≤ 25 ms at 100k rows and ≤ 1.5× the 1k-row result. |
| Unrelated history | At 10k unrelated commits, p50 merge ≤ 25 ms and `merge_analysis` ≤ 20 ms. |
| Row merge memory | Incremental peak RSS ≤ 8 MiB for 100 changed rows at 100k total rows. |
| Historical diff | At 10k unrelated commits and 100 changed rows, p50 ≤ 20 ms and ≤ 2× the zero-history result. |
| All-plugin warm | Preview p50 ≤ 5 ms, merge p50 ≤ 10 ms, incremental peak RSS ≤ 1 MiB. |
| All-plugin cold reopen | Preview p50 ≤ 15 ms, merge p50 ≤ 25 ms, incremental peak RSS ≤ 2 MiB. |
| Plugin baseline | Installing all five plugins adds ≤ 64 MiB RSS. |
| File scaling | 25 affected files and 100 rows/file each remain ≤ 12 ms merge p50 and ≤ 2 MiB incremental peak RSS. |
| Unaffected files | At five affected files, 500 unaffected files remain ≤ 2× the 5-file preview/merge p50 with identical plugin counters. |

The current implementation fails the branch-creation and unrelated-history
requirements. The expanded harness also gates direct historical diff against the
same history sweep; its dated measurements are recorded with the first optimization
PR so every stacked PR compares against its direct parent. Those are the first optimization targets. File and row merge
budgets otherwise qualify on initial p50-like samples, while cold plugin p95
still needs the full repeated run before being declared stable.

## Reproduction

```sh
cargo build --release -p lix_e2e --example branch_merge_benchmark
LIX_BRANCH_MERGE_BENCH_SAMPLES=11 \
  target/release/examples/branch_merge_benchmark qualification > qualification.jsonl
LIX_BRANCH_MERGE_BENCH_SAMPLES=11 \
  target/release/examples/branch_merge_benchmark sweep > sweep.jsonl
```
