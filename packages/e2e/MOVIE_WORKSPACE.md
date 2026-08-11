# Movie workspace qualification

The release qualification models the 1 TiB media-inclusive happy path with
resumable ingest, two concurrent 100 Mbit/s proxy readers, 5,000 project
history commits, and concurrent project saves. Run the local SlateDB profile
with:

```sh
cargo test -p lix_e2e \
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

## Each actor runs as its own task

Ingest, project saves and the two proxy streams are spawned as four separate
tokio tasks. They were previously branches of a single `tokio::join!` future,
which put all four on one task: a slow write in any branch could not overlap a
playback read, and the resulting scheduling delay was charged to the playback
deadline.

That is not a hypothetical. On SlateDB one project save per run — the save at
t = 1200 ms — takes ~110 ms while every other save takes under 8 ms. Joined on
one task, that save delayed both playback wake-ups by 69-70 ms against an 80 ms
budget, and `stream_N_late` flipped to 1 whenever concurrent ingest pushed the
delay past 80 ms. The proxy reads themselves were never slow: p99 5.6 ms, max
5.9 ms, against the same 80 ms budget.

The null control (`slatedb_movie_workspace_playback_control`, identical
schedule with the ingest removed) reproduced the same 69-70 ms delay in 6 of 6
stream-runs, which is what identified the save rather than ingest interference
as the cause. `LIX_MOVIE_PLAYBACK_TRACE=1` prints per-sample read latency, per
save latency, upload-window completion times, and a spawned runtime watchdog
whose worst gap stayed at 2.0-2.6 ms throughout — proving the executor was not
starved and the stall was confined to the joined task.

The ~110 ms SlateDB save and the ~200 ms fixed cost of every
`resumable_initial_write` in `large_blob_updates` are unexplained write-path
stalls, tracked separately. Neither is a read-path defect.

## Diagnostic environment variables

None of these are set by the qualification itself.

| Variable | Effect |
| --- | --- |
| `LIX_MOVIE_PLAYBACK_TRACE=1` | per-sample playback/save traces, upload-window times, runtime watchdog |
| `LIX_MOVIE_WORKER_THREADS=N` | runtime width; the qualification profile is 4 |
| `LIX_MOVIE_STREAM_SAMPLES=N` | playback sample count, for looking past the 40-sample window |
| `LIX_MOVIE_UPLOAD_CONCURRENCY=N` | upload parts in flight, 1-4 |

## First-window upload contention

`chunk_hash_bytes_including_retries` measures payload actually hashed,
including optimistic retries, so re-hashed parts are visible directly. For the
512 MiB timed ingest it is exactly `512 MiB + (concurrency - 1) x 16 MiB`:

| Upload concurrency | Hashed bytes | Re-hashed | Ingest |
| ---: | ---: | ---: | ---: |
| 1 | 536,870,912 (512 MiB) | 0 | 153.1 MiB/s |
| 2 | 553,648,128 (528 MiB) | 16 MiB | 152.8 MiB/s |
| 4 | 587,202,560 (560 MiB) | 48 MiB | 153.1 MiB/s |

Exactly one part per extra in-flight part is re-hashed, and only in the first
window, where `UPLOAD_STATE_SPACE` is absent for every concurrent part and the
losers retry. It is bounded wasted work, not a scaling term, and ingest on this
local-filesystem profile does not move with concurrency at all.

## Artificial object-store latency

The latency qualification wraps the local object store and delays every
operation after fixture seeding. It compares one upload part with a four-part
window while project saves and two proxy streams run concurrently:

```sh
cargo test -p lix_e2e \
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
