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
starts before any commit future is scheduled and stops after all 100 commits
return. The default capacity run uses 100 concurrent clients. The service p95
is the real-time request-processing gate; the batch
timer exposes scheduler queueing and total convergence throughput.

## Same-machine results

External values are medians of five suite invocations and rounded to whole
milliseconds. Lix values are optimized release builds on the same machine.
Lower is better within a column, but the Lix B3.1 cell reports both durable
per-commit service p95 and the total commit batch window.

| System | B2.1: two length-6000 Markdown prefix inserts | B3.1: concurrent JSON map sets |
| --- | ---: | ---: |
| Ywasm 0.27.3 | 1 ms | 45 ms total |
| Yjs 13.6.31 | 2 ms | 64 ms total |
| Diamond Types 1.0.2 | 3 ms | unsupported |
| Loro 1.13.8 | 6 ms | 40 ms total |
| Automerge 3.4.0 | 47 ms | 1,412 ms total |
| Lix transactions + plugin resolver | **38.039 ms p50 / 39.436 ms p95** | **10.934 ms service p95; 7.603-11.270 ms commit batch (100 clients)** |

The external B3.1 rows retain the upstream suite's 1,540-operation workload.
The Lix row uses the product capacity target of 100 simultaneously active
clients: eight independent workspaces produced 800 per-client observations,
with every wave converging through one durable commit. It passes the 100-ms
service gate with 89.066 ms of headroom. Cardinalities are shown explicitly so
the system comparison is not mistaken for an identical persistence workload.

## Before/after

| 100-client JSON adapter | Before: synthetic branch merges | After: same-base transactions |
| --- | ---: | ---: |
| API operations | 100 branches + 100 sequential merges | 100 existing `begin_transaction()` calls + concurrent commits |
| Resolver scheduling | conflict discovery and plugin resolution once per merge | conflicts indexed once; one plugin batch per balanced reduction round |
| Measured latency | current `main`: 151.383 ms p95 | cohort commit: 10.934 ms p95 |
| Client convergence | inferred from one branch head | asserted from every client |

At the 100-client real-time target, current `main` records 151.383 ms p95 and
fails the gate. Cohort commit records 10.934 ms p95, a 92.8% reduction (13.8x),
and publishes one durable commit. The algorithm still must inspect each
conflicting semantic record—arbitrary exact resolution has an Ω(N) lower
bound—but it no longer creates one durable transition per client. Conflicts are
indexed once, grouped by owning file, reduced in deterministic balanced batches,
and committed atomically.

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
cargo test -p lix_e2e --test crdt_benchmarks_baseline \
  ordinary_concurrent_execute_serializes_without_plugin_resolution -- --exact

cargo test -p lix_e2e --test crdt_benchmarks_baseline --release \
  crdt_benchmarks_b2_1_markdown_concurrent_prefix_inserts \
  -- --ignored --exact --nocapture

LIX_CRDT_SAMPLES=1 cargo test -p lix_e2e \
  --test crdt_benchmarks_baseline --release \
  crdt_benchmarks_b3_1_json_concurrent_map_sets \
  -- --ignored --exact --nocapture
```

`LIX_CRDT_B3_CLIENTS` overrides the default 100-client cardinality and
`LIX_CRDT_SAMPLES` controls independent workspace samples.

The separate [`REALTIME_COLLABORATION_CAPACITY.md`](REALTIME_COLLABORATION_CAPACITY.md)
defines the realistic 50-100 collaborator, gradual-arrival, client-observed
capacity gate. Larger bursts are saturation workloads and must account for the
coordinator's bounded cohort capacity rather than asserting one durable commit.
