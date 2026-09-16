# E03: discover independent commit metadata together — accepted

A diff descriptor requires both its immutable state header and graph record.
Previously the first missing header stopped discovery, although the graph
record's identity was already known. The runtime fetched it, retried SQL, and
only then discovered the missing graph record.

The helper now attempts both reads and reports their combined missing frontier
through the existing bounded metadata batch. Resident corruption remains a hard
error even if the other record is absent. No endpoint, protocol version, storage
format, migration, persistent cache, or application API changes. Applications
still issue ordinary awaited SQL. This is a measured performance improvement,
not a claim that the complete history architecture has been simplified.

## Paired result

Ten alternating pairs per case, identical exported synthetic snapshots, separate
fresh RocksDB replicas, canonical authority over loopback HTTP, zero injected
RTT. The Lix engine uses opt-level=2; dependencies use the development profile.
These are native benchmark measurements, not browser production timings.

| 64-checkpoint case | Native requests, before → after | Median cold history | Median paired improvement, 95% bootstrap interval |
| --- | --- | --- | --- |
| File changes every checkpoint | 381 → 320 (16.0% fewer) | 14.092 s → 11.787 s | 16.45% [15.86%, 17.00%] |
| File changes every eighth checkpoint | 213 → 152 (28.6% fewer) | 3.529 s → 2.507 s | 29.11% [28.26%, 29.46%] |

Both latency intervals exceed the 10% acceptance threshold. Request counts are
identical across all ten runs of each revision/case; their reduction is an exact
work measurement. Every run compares complete ordered history rows with the
authority and then repeats the query with the server unavailable.

Opening remains two foreground requests and zero native reads. Dense opening
medians are 9.389 → 9.125 ms; sparse 8.729 → 8.712 ms. Warm history medians are
67.780 → 67.721 ms and 26.713 → 26.979 ms. Warm paired improvement intervals
include zero and exclude a 10% regression. Sparse file selection moved from
44.961 to 47.355 ms; its paired interval includes zero (-8.34% to +1.60%) and
request counts are unchanged. This small variation is reported, not hidden in
the history result.

Three candidate runs each with 16,000 unrelated files and an unopened 8 MiB blob
retain the same two-request opening contract (2,280 and 2,274 response-body
bytes). Ordered history and offline warm checks pass. These size controls
establish bounded transfer and correctness; their timings are not paired speed
claims. The original unoptimized baseline's deadline failure is documented in
[the benchmark method](../public-history-benchmark.md); it is not dropped into
this timing comparison as though it had completed.

## Validation and limits

- `cargo nextest run -p lix --features all-simulations`: 4,073 passed, 74 skipped.
- `cargo test -p lix --doc`: 10 passed.
- Focused tests cover combined missing inputs and corruption in either resident
  input while its peer is absent.
- No storage adapter implementation or contract changed.
- Old repositories use the existing metadata decoders and migration path; this
  change requires no format migration or storage reset.

The helper still retries the whole SQL operation after hydration. It reduces
avoidable sequential discovery; it does not eliminate repeated prefix work or
unknown ancestor dependencies. The next experiment groups both diff endpoints.

Raw samples, summary, build/source manifest, and size-control samples are stored
alongside this report. The source manifest identifies the candidate by its base
revision and patch digest because measurements precede its acceptance commit.

Decision: accept. Consecutive non-improvements: **0**.
