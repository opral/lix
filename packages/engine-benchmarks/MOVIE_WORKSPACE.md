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
