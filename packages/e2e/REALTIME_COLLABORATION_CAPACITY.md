# Real-time collaboration capacity

Measured 2026-08-01 in optimized Rust `release` builds on the same machine as
the CRDT baseline. The capacity gate is client-observed commit-to-convergence
p95 below 100 ms with 50-100 active collaborators on one document.

## Workload

- 50 or 100 independent Lix sessions keep a live observation on one hot file.
- The run performs one edit per collaborator in waves of five edits.
- Waves are scheduled on one absolute clock 50 ms apart. Four same-base edits
  commit concurrently; a fifth same-base marker edit closes the wave. A slow
  wave cannot move later deadlines: missed-deadline lag is included in the
  next wave's convergence latency.
- Two writers overlap in every fourth wave. That is exactly 10% overlapping
  operations; the other 90% target distinct semantic rows.
- A wave converges only after every client reaches the marker's mutation
  generation through its own query stream. One designated observer verifies
  the marker bytes and publishes that generation as the wave receipt; the
  remaining observers compare their scalar `mutation_sequence` instead of
  rescanning and, remotely, Base64-decoding the complete document. The timer
  begins before commits are scheduled, so it includes
  commit queueing, stale conflict discovery, plugin reconciliation, the
  in-memory storage commit, invalidation, query reevaluation, and observation
  fan-out.
- Per-commit service latency is retained as a diagnostic, but it is not the
  capacity gate.
- Every non-overlapping edit must survive, conflict waves must invoke the
  owning plugin resolver, final bytes must parse, and all clients must agree.
- Local SDK and server-protocol measurements run through the same private
  phase adapter. The shared driver owns wave construction, arrival deadlines,
  overlap percentage, percentile calculation, convergence assertions, and the
  100 ms gate; adapters own only transport-specific read, stage, commit, and
  observation delivery.
- Every run emits one machine-readable
  `lix.collaboration-capacity.v1` JSON record. It includes p50/p95/p99 samples,
  schedule lag, resolver calls, and deterministic logical resource counts.

## Results

Each row reports the worst value observed across the initial measurement and
five fresh-process repetitions. This avoids selecting the fastest sample.

| Adapter / format | Clients | Service p95 | Client-observed convergence p95 | p99 | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Local / JSON | 50 | 4.182 ms | **14.813 ms** | 14.909 ms | pass |
| Local / JSON | 100 | 5.073 ms | **18.095 ms** | 19.462 ms | pass |
| Local / CSV | 100 | 5.484 ms | **23.108 ms** | 23.818 ms | pass |
| Local / Markdown | 100 | 7.952 ms | **24.780 ms** | 28.850 ms | pass |
| Local / text | 100 | 5.520 ms | **20.922 ms** | 21.276 ms | pass |
| Lix Server Protocol / JSON | 100 | 13.787 ms | **25.977 ms** | not recorded | pass |

The server-protocol row runs 100 independent protocol sessions and 100 SSE
streams through the canonical Axum router. It covers wire value decoding,
transaction capability routing, response serialization, and SSE delivery, but
uses the in-process router and therefore excludes TCP and deployment network
RTT. Production SLOs must add measured network latency; this result must not be
presented as a public-internet end-to-end number.

Both capacity rows use Lix's canonical in-memory storage so they isolate the
engine and transport paths. A deployment using RocksDB, SlateDB, or a
remote object store must run the same gate on that storage before claiming its
own production capacity.

After the shared-driver and private tracing cut, one full 100-client release
validation reported 14.422 ms local JSON convergence p95 and 21.754 ms
in-process server-protocol p95. The corresponding pre-refactor worst values
above were 18.095 ms and 25.977 ms. These single post-refactor runs are a
regression check, not a replacement for the six-process worst-value matrix;
they show that structured reporting and disabled-by-default stage spans did
not consume the latency budget.

After switching convergence detection to the verified generation receipt, one
100-client release validation reported 12.547 ms local JSON convergence p95
and 18.172 ms in-process server-protocol p95. The immediately preceding local
JSON run on the stacked transaction changes was 16.420 ms. The receipt changes
benchmark bookkeeping, not commit or observation semantics: every client still
consumes its own event at or beyond the verified marker generation.

Replacing split protocol lease/transaction flags with one RAII-owned activity
state left the same server workload flat at 18.068 ms convergence p95 versus
18.172 ms immediately before (-0.6%, within run variance). This cut is a
cancellation-safety and maintainability change, not a throughput claim.

## Profile and change

The first realistic run failed before producing a latency result. Same-file
byte edits that touched disjoint semantic rows overlapped in file-storage
bookkeeping, and the stale transaction path classified that bookkeeping as an
unsafe non-plugin conflict. This made the 90% disjoint workload return
`LIX_TRANSACTION_CONFLICT`.

The commit path now identifies the stable plugin-owned file from its overlapping
bookkeeping rows, rebases the retained semantic edits through the existing
plugin actor, keeps the final combined rendering, and commits once. Disjoint
edits do not call the conflict resolver. Actual semantic overlaps remain
grouped by file and invoke the resolver once for the accumulated conflict set.

Private `lix_transaction` tracing spans now split stale commit work into diff,
classification, reconciliation, resolver invocation, and semantic replay.
They add no SDK method or protocol field and can be enabled only by a host
subscriber when a slower capacity run needs attribution.

Across the corrected six-run matrix, worst local service p95 was 7.952 ms and
worst local convergence p95 was 24.780 ms. Absolute-deadline schedule-lag p95
never exceeded 2.081 ms, proving the workload sustained five edits every 50 ms
instead of moving arrivals after convergence. The in-process Lix Server Protocol's
worst service and convergence p95 values were 13.787 ms and 25.977 ms, with
schedule-lag p95 at or below 1.768 ms. These endpoint measurements identify
protocol processing and fan-out as the remaining larger slices; a production
network measurement is still required for a deployment-specific SLO.

## Resource soak

The ignored soak opens, stages, abandons, and closes 100 transactions and
sessions per round. Ten rounds cover 1,000 abandoned transactions. No staged
write becomes visible. On the measured run:

- warm post-cleanup RSS: 84,520,960 bytes
- peak RSS: 84,520,960 bytes
- final RSS: 84,520,960 bytes
- post-warmup growth: **0 bytes**
- allowed post-warmup growth: 67,108,864 bytes

RSS is allocator- and platform-dependent, so the bound detects unbounded
retention rather than promising a fixed production memory footprint. The soak
also emits a `lix.collaboration-soak.v1` JSON record with exact opened, closed,
staged, abandoned, and visible-write counts so lifecycle regressions are not
inferred from RSS alone.

## Reproduction

```bash
for format in json csv markdown text; do
  LIX_COLLAB_CLIENTS=100 \
  LIX_COLLAB_OPERATIONS=100 \
  LIX_COLLAB_ARRIVAL_MS=50 \
  LIX_COLLAB_FORMAT="$format" \
  cargo test -p lix_e2e \
    --test realtime_collaboration_capacity --release \
    realtime_collaboration_commit_to_convergence_capacity \
    -- --ignored --exact --nocapture
done

LIX_COLLAB_CLIENTS=100 \
LIX_COLLAB_SOAK_ROUNDS=10 \
cargo test -p lix_e2e \
  --test realtime_collaboration_capacity --release \
  abandoned_transactions_and_sessions_release_resources \
  -- --ignored --exact --nocapture

# ⚠️ INERT — this command runs zero tests and exits 0. Do not read it as a pass.
# `server_protocol_converges_to_one_hundred_clients_below_one_hundred_ms_p95`
# carries `#[cfg(any())]` in packages/lix/src/server_protocol/mod.rs, together
# with the whole `RemoteCapacityBackend` apparatus it needs, so it compiles in
# no configuration. Measured: exit 0, `running 0 tests ... 2301 filtered out`.
# The filter is also mis-spelled independently of that — the emitted name is
# `server_protocol::tests::…`, so `--exact` on `tests::…` matches nothing even
# once the item is compiled back in. The two `--test
# realtime_collaboration_capacity` commands above are the live gate; unlike this
# one they name a target, so a stale name there exits 101 instead of 0.
cargo test -p lix --features "server-protocol storage-benches" --release \
  server_protocol::tests::server_protocol_converges_to_one_hundred_clients_below_one_hundred_ms_p95 \
  -- --ignored --exact --nocapture
```

`LIX_COLLAB_GATE_MS` defaults to 100. Both adapters accept the same client,
operation, arrival, and gate environment variables. The benchmark asserts the
gate rather than only printing it.
