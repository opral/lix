# Public awaited history benchmark

`packages/e2e/benches/partial_replica_history.rs` uses normal awaited SQL on a
fresh partial replica, a real loopback HTTP connection, and the canonical Lix
server handler. Authority and replica use separate temporary RocksDB stores.
The Memory backend cannot prove the durable admission boundary required by
public synchronization; it is used only to construct exported synthetic seeds.

The benchmark restores the same snapshot before every sample, then measures
opening, selecting the file, cold history, and warm history with the server
made unavailable. Snapshot restoration and authority verification are outside
the measured intervals. It verifies identical ordered history rows. Every
sample asserts zero native hydration during opening, one handshake and one
initial descriptor, and at most 8 KiB of opening response bodies. Canceled
background descriptor watches are counted separately.

Build from the repository root:

```sh
CARGO_BUILD_JOBS=8 cargo build --manifest-path tooling/Cargo.toml -p lix_e2e \
  --config 'profile.dev.package.lix.opt-level=2' \
  --features server-protocol --bench partial_replica_history
```

Copy the resulting executable for each revision before rebuilding. Run with
`LIX_PROFILE_FIXTURES` pointing to a shared synthetic snapshot directory.
`LIX_PROFILE_CASES` selects comma-separated `dense4`, `dense64`, `sparse64`,
`dense256`, `wide16000`, or `blob8m`; the last two control unrelated repository
size. `LIX_PROFILE_SAMPLES` defaults to 10. `LIX_PROFILE_RTT_MS` adds a fixed
request/response delay; it models latency, not bandwidth or network jitter.

```sh
python3 docs/partial-sync-experiments/paired_history.py BASELINE CANDIDATE \
  --fixtures /tmp/lix-history-fixtures --output /tmp/history-pairs.jsonl \
  --pairs 10 --cases dense64,sparse64 --rtt 0
```

The runner alternates revision order, checks snapshot identity and row counts,
and reports seeded bootstrap intervals for median paired improvement. Each
executable independently checks the full ordered result against its authority.
Run comparisons without concurrent compiler or benchmark workloads. Development
build timings are diagnostic and must be labeled as such; request counts also
report exact work independently of compiler optimization.

Response byte counters measure HTTP body bytes, not headers/TLS framing.
Opening is measured after authority restoration and server initialization.
The offline check asserts no new native requests; background poll failures are
allowed. This benchmark deliberately contains no caller hydration/retry loop
and no precomputed dependency list.

Initial unoptimized smoke runs established correctness and deterministic counts.
Eight dense-64 samples each used 381 native history requests; the ninth hit the
30-second read deadline under concurrent development workloads. That failed
run is retained and is not treated as a successful timing sample. Paired timing
uses an optimized engine (`opt-level=2`) with identical dependency build settings
on both revisions; this is still not a production browser latency claim.
