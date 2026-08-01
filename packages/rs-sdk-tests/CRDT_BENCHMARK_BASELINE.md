# CRDT benchmark baseline

Measured 2026-08-01 against the latest
[`dmonad/crdt-benchmarks`](https://github.com/dmonad/crdt-benchmarks) `main`
workloads, with `N = 6000`. Every external package was upgraded to its npm
`latest` dist-tag before measurement. Breaking Ywasm and Loro API changes were
adapted without changing the B2.1 or B3.1 workload.

## Corrected API model

The earlier adapter created one branch per simulated client and merged those
branches sequentially. That measured the branch merge API, not database
transactions, and was removed.

The replacement uses the existing public API only:

1. Open multiple Lix clients against one database.
2. Call `begin_transaction()` on every client before any client commits.
3. Stage overlapping semantic edits from the common opening snapshot.
4. Commit concurrently, allow the owning plugin to resolve stale semantic
   overlaps, and verify that every client reads the same final bytes.

Remote clients exercise the same engine transaction through an internal wire
capability. No branch or merge API and no new public transaction API is part of
the benchmark.

## Acceptance assertions

- Ordinary concurrent `execute()` calls serialize normally and produce zero
  plugin conflict-resolver calls.
- Same-base explicit transactions produce resolver calls, all clients
  converge, and per-client durable commit service p95 is below 100 ms.

The per-commit service timer starts when each commit future is first polled and
stops when that client's durable commit returns. A separate commit-batch timer
starts before any commit future is scheduled and stops after all 1,540 commits
return. The service p95 is the real-time request-processing gate; the batch
timer exposes scheduler queueing and total convergence throughput.

## Same-machine results

External values are medians of five suite invocations and rounded to whole
milliseconds. Lix values are optimized release builds on the same machine.
Lower is better within a column, but the Lix B3.1 cell reports both durable
per-commit service p95 and the total commit batch window.

| System | B2.1: two length-6000 Markdown prefix inserts | B3.1: 1,540 concurrent JSON map sets |
| --- | ---: | ---: |
| Ywasm 0.27.3 | 1 ms | 45 ms total |
| Yjs 13.6.31 | 2 ms | 64 ms total |
| Diamond Types 1.0.2 | 3 ms | unsupported |
| Loro 1.13.8 | 6 ms | 40 ms total |
| Automerge 3.4.0 | 47 ms | 1,412 ms total |
| Lix transactions + plugin resolver | **38.039 ms p50 / 39.436 ms p95** | **23.546 ms service p95; 17,668.298 ms commit batch** |

The B3.1 Lix row contains 1,540 per-client service observations from one full
cardinality run. It passes the 100-ms service gate with 78.153 ms of headroom.
The complete test, including workspace/plugin setup, opening and staging 1,540
transactions, committing, convergence reads, and cleanup, finished in 18.61 s.
The 17.668-second commit batch means this implementation does **not** provide
sub-100-ms all-client convergence when 1,540 commits arrive simultaneously;
both numbers are retained so service latency cannot hide queueing.

## Before/after

| 100-client JSON adapter | Before: synthetic branch merges | After: same-base transactions |
| --- | ---: | ---: |
| API operations | 100 branches + 100 sequential merges | 100 existing `begin_transaction()` calls + concurrent commits |
| Resolver scheduling | conflict discovery and plugin resolution once per merge | stale conflicts discovered at commit; one batched plugin call per affected file |
| Measured latency | 598.560 ms total merge region | 14.703 ms commit p95 in an unoptimized diagnostic build |
| Client convergence | inferred from one branch head | asserted from every client |

At the exact upstream B3.1 cardinality, the old 1,540-branch run did not finish
within two minutes. The corrected transaction run completes and records a
23.546 ms release service p95. The algorithm still must inspect each conflicting
semantic record—arbitrary exact resolution has an Ω(N) lower bound—but it no
longer creates and merges a synthetic branch for every client. Within each
transaction, conflicts are indexed once, grouped by owning file, passed to that
plugin in one accumulated batch, and committed atomically.

## Environment

- AMD EPYC-Genoa, 16 cores, one hardware thread per core
- Linux 7.0.0-22-generic x86-64
- Node v22.22.1 (the benchmark declares Node 20; this is a comparability caveat)
- Rust 1.97.0-nightly (`nightly-2026-05-21`)
- optimized Rust `release` profile

## Reproduction

The CRDT workloads are ignored profiling tests so normal CI remains fast. The
ordinary-execute correctness assertion is part of the normal test run.

```bash
cargo test -p lix_sdk_tests --test crdt_benchmarks_baseline \
  ordinary_concurrent_execute_serializes_without_plugin_resolution -- --exact

cargo test -p lix_sdk_tests --test crdt_benchmarks_baseline --release \
  crdt_benchmarks_b2_1_markdown_concurrent_prefix_inserts \
  -- --ignored --exact --nocapture

LIX_CRDT_SAMPLES=1 cargo test -p lix_sdk_tests \
  --test crdt_benchmarks_baseline --release \
  crdt_benchmarks_b3_1_json_concurrent_map_sets \
  -- --ignored --exact --nocapture
```

`LIX_CRDT_B3_CLIENTS` overrides the default 1,540-client cardinality and
`LIX_CRDT_SAMPLES` controls independent workspace samples.

The separate [`REALTIME_COLLABORATION_CAPACITY.md`](REALTIME_COLLABORATION_CAPACITY.md)
defines the realistic 50-100 collaborator, gradual-arrival, client-observed
capacity gate. The 1,540-client B3.1 burst remains a saturation comparison and
is not substituted for that real-time workload.
