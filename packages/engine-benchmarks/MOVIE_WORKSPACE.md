# Movie workspace qualification

The release qualification models the 1 TiB media-inclusive happy path with
resumable ingest, two concurrent 100 Mbit/s proxy readers, 5,000 project
history commits, and concurrent project saves. Run the local SlateDB profile
with:

```sh
cargo test -p lix_engine_benchmarks \
  --features 'storage-benches slatedb' \
  --test movie_workspace_qualification \
  slatedb_movie_workspace_interference \
  --release -- --ignored --nocapture
```

The exact 1 TiB projection contains 164 representative files: 20 × 20 GiB,
32 × 10 GiB, 64 × 4 GiB, and 48 × 1 GiB. The timed physical sample is 512 MiB
and preserves the 1 MiB chunk and 16 MiB upload-part ratios used to project
row and request amplification.

On 2026-08-01, the raw-only immutable-payload build measured the following on
the local filesystem-backed SlateDB fixture:

| CAS chunk | Save p95 | Late proxy reads | Ingest |
| --- | ---: | ---: | ---: |
| 1 MiB | 103.126 ms | 0 / 0 | 153.1 MiB/s |
| 4 MiB | 103.470 ms | 0 / 0 | 153.1 MiB/s |

SlateDB coalesces sequential immutable chunks into 64 MiB sidecar segments in
this workload, so the larger CAS unit did not improve ingest and slightly
regressed save p95. The engine therefore retains 1 MiB chunks; remote
object-store latency profiles may justify revisiting the choice.

## Artificial object-store latency

The latency qualification wraps the local object store and delays every
operation after fixture seeding. It compares one upload part with a four-part
window while project saves and two proxy streams run concurrently:

```sh
cargo test -p lix_engine_benchmarks \
  --features 'storage-benches slatedb' \
  --test movie_workspace_qualification \
  slatedb_movie_workspace_latency_simulation \
  --release -- --ignored --nocapture
```

The 30 ms and 50 ms remote-latency profiles gate on >20% median ingest
improvement. The 10 ms profile is a non-gating local-overhead control because
CPU and filesystem work can dominate at that latency. Qualification alternates
serial/four-part order over four balanced repetitions, compares median ingest,
and requires every four-part repetition to keep save p95 below 500 ms with zero
late playback reads.

## Structural accounting

The 512 MiB physical ingest committed 32 temporary manifest leaves summarizing
512 chunk receipts, exactly 16× fewer temporary rows. The arithmetic 1 TiB
row-count projection is
65,536 leaves instead of 1,048,576 per-chunk rows. Chunk content hashing
records actual hash input, including optimistic retries. Segment identity
hashing records only ordered key identities and lengths; the identity hasher is
never updated with payload bytes, so publication has no second payload-sized
hashing pass.

The harness reports locator staging attempts, logical object-store calls,
cache filesystem attempts, and writer-gate wait for every run. Locator staging
includes optimistic retry work; only atomically committed locators become
visible.
