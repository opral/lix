# Transparent partial replica recovery: performance

Measured on September 15, 2026 against baseline `54c023f8f3c2e97a74caf89688513df8beeaa7a8`. The accompanying [measurement artifact](partial-replica-transparent-recovery-performance.json) records the measured source hash, observations, and environment.

The final profiles pass: opening remains bounded, cached reads complete locally, and pending-edit recovery returns a successful SQL result. These measurements do not establish a speedup.

## Method

- AMD Ryzen 9 9950X, Linux x86_64, shared development host.
- Rust `1.97.0-nightly (b954122bb 2026-05-20)`.
- Cargo test profile: engine optimization level 0, dependencies level 1;
  `server-protocol,all-simulations`, `CARGO_INCREMENTAL=0`.
- The final test binary also enables `offline-migration` for the migration
  regression suite. These profiling workloads do not invoke that detached migration path; baseline and final runtime features otherwise match.
- Canonical `Memory` storage. These numbers do not measure IndexedDB, OPFS,
  browser scheduling, release builds, or production network latency.
- Opening uses loopback HTTP and the canonical server handler, five fresh
  replicas per fixture. Repository seeding and server startup are excluded.
- Cold SQL uses in-process native hydration, one cold sample per repository
  width. Warm SQL uses 30 reads interleaved with local updates. A direct warm session has no network callback and must complete entirely from local data.
- Recovery uses the normal public SQL API and an in-process authority with a
  deliberately expired baseline, with three runs per case. The explicit transaction control uses five runs of 100 reads and 100 updates, comparing the same implementation with its hydration sender disabled and enabled.

Elapsed times are diagnostic observations, not statistically established speedups or production latency guarantees. Request counts and successful offline reads provide stronger evidence for the architecture than small timing differences on this host.

## Opening

The profile requires exactly two foreground requests (handshake and descriptor) and at most 8 KiB of foreground response bodies, including the 16,000-row and 32 MiB file fixtures. Offline reopening must issue no network requests.

Median elapsed time in milliseconds, five samples per row:

| Fixture | Baseline open | Final open | Final offline reopen |
| --- | ---: | ---: | ---: |
| 16 rows | 3.54 | 4.07 | 0.74 |
| 1,600 rows | 3.66 | 3.61 | 0.69 |
| 16,000 rows | 3.55 | 3.48 | 0.57 |
| 128 additional branches | 3.55 | 3.49 | 0.53 |
| 256 history updates | 3.46 | 3.49 | 0.54 |
| 8 MiB file | 3.48 | 3.51 | 0.60 |
| 32 MiB file | 3.56 | 3.66 | 0.62 |

All 35 final openings used exactly two foreground requests and 2,310–2,362 response-body bytes. All offline reopenings used zero network requests.

## Cold and cached SQL

Engine-only opening excludes descriptor acquisition and bootstrap publication; it must not be confused with complete HTTP opening above. Cold request counts include separate native-object and native-metadata requests; payload bytes exclude HTTP framing. Warm samples measure cached reads after local updates.

Times in milliseconds; warm values are medians of 30 reads or updates:

| Rows | Revision | Engine open | Cold read | Warm read | Warm update | Cold object / metadata requests | Cold bytes |
| --- | --- | ---: | ---: | ---: | ---: | --- | ---: |
| 16 | Baseline | 0.67 | 20.46 | 1.88 | 2.45 | 8 / 4 | 6,083 |
| 16 | Final | 0.47 | 17.57 | 1.50 | 1.97 | 10 / 4 | 6,246 |
| 1,600 | Baseline | 0.45 | 24.59 | 1.68 | 2.27 | 16 / 4 | 21,289 |
| 1,600 | Final | 0.45 | 24.19 | 1.68 | 2.24 | 16 / 4 | 21,289 |

The 16-row request-count difference prompted three additional final runs. Each used 8 object requests, 4 metadata requests, and 6,083 bytes; cold reads ranged from 15.63 to 21.43 ms. The 1,600-row counts stayed unchanged in all repeats. The fixture therefore shows run-dependent request counts; the raw artifact retains the original observation and all repeats. Warm reads made zero network requests throughout.

## Recovery and explicit transactions

The baseline dirty-expiry regression explicitly expected an error. Its failed operation is not a meaningful latency baseline for successful recovery.

Final public API results, medians of three runs. Native request counts exclude descriptor and other non-native endpoints. The timer ends when the initial operation returns, before later warm-read and value assertions.

| Scenario | Elapsed ms | Native requests |
| --- | ---: | ---: |
| Expired clean baseline | 32.12 | 21 |
| Pending edit, unchanged authority | 21.32 | 11 |
| Pending edit, advanced authority | 50.36 | 23 |
| Global conflict resolved by authority | 43.16 | 21 |
| Frozen selected upload with global divergence | 50.68 | 18 |
| Explicit transaction, cold small file | 23.77 | 16 |
| Explicit transaction, cold 5 MiB file | 39.85 | 16 |
| Explicit transaction, cold file update and commit | 37.99 | 20–22 |

These fixtures have different prepared working sets; request counts across rows are not a comparison of equivalent workloads. Subsequent warm reads within each execution path require no additional fetches. Moving from an explicit transaction to the ordinary session can require additional initial dependencies; that session's next identical read is also checked as local.

The lower-level lease-recovery fixture used one descriptor request for clean cases and two for pending-edit reconciliation, with one expired request in each case. All three repetitions completed successfully; detailed observations are in the artifact.

Warm explicit transaction control, median per operation across five runs:

| Hydration sender | Read ms | Update ms | Native demands |
| --- | ---: | ---: | ---: |
| Disabled | 0.967 | 0.577 | 0 |
| Enabled | 1.000 | 0.603 | 0 |

The measured additional time is about 0.033 ms per read and 0.026 ms per update in this fixture. This is a same-build control, not a baseline-revision comparison, and does not establish overhead for larger transactions.

Recovery is bounded by the required working set and pending history; these fixtures do not establish a constant recovery cost for arbitrary histories. The delta cached-upload fallback can reconstruct the logical attempt within existing limits; it is not equivalent to constant-time paging of flat cached uploads.

## Reproduction

Run each revision separately on the same host. For the final revision, add `offline-migration` to the feature list to match the recorded test binary:

```sh
export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR=/tmp/lix-transparent-target
cargo test -p lix --lib --features server-protocol,all-simulations \
  partial_handle_http_opening_profile -- --ignored --nocapture
cargo test -p lix --lib --features server-protocol,all-simulations \
  descriptor_only_sql_hydrates_then_reads_and_writes_offline -- --ignored --nocapture
```

Additional recovery and transaction profiles are available on the changed revision:

```sh
cargo test -p lix --lib --features server-protocol,all-simulations \
  sync::partial_public_api_tests -- --nocapture --test-threads=1
cargo test -p lix --lib --features server-protocol,all-simulations \
  expired_foreground_read -- --nocapture --test-threads=1
cargo test -p lix --lib --features server-protocol,all-simulations \
  profile_warm_partial_explicit_transaction -- --ignored --nocapture
```

Compilation is excluded from all reported times.
