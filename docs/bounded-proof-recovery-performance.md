# Bounded proof recovery performance evidence

This change reduces metadata-walk round trips for bounded proof recovery. The local browser comparison shows fewer proof pages but flat end-to-end equality medians; it does not demonstrate a user-visible speedup.

## Matched browser comparison

The UI 61 fixture ran 60 edits at 500 ms configured sync latency with fresh isolated storage prefixes. The same test source, final byte-equality check, and reopen assertion were used in both arms. All six reported runs passed. An earlier B1/C1R2 diagnostic pair passed capture checks but lacked the primary Playwright equality labels, so it is excluded from the three-run comparison.

| Run | Arm | Post-edit to equality (ms) | Burst to equality (ms) | Proof pages | Manifest members | Encoded bytes | Inline bytes |
|---|---|---:|---:|---:|---:|---:|---:|
| C2 | Candidate | 14038 | 27277 | 3 | 36 | 20501 | 9354 |
| B2 | Control | 10995 | 24669 | 5 | 40 | 22659 | 10303 |
| B3 | Control | 11069 | 24821 | 5 | 40 | 22659 | 10303 |
| C3 | Candidate | 11065 | 24391 | 3 | 39 | 22322 | 10218 |
| B4 | Control | 14068 | 27878 | 6 | 35 | 19862 | 9042 |
| C4 | Candidate | 11003 | 24493 | 3 | 39 | 22322 | 10218 |

The median post-edit-to-equality result was 11,069 ms for proto32 and 11,065 ms for proto33. Burst-to-equality medians were 24,821 ms and 24,493 ms. With n=3 per arm, these distributions overlap. Equality polling and marker observation use 100/250/500/1000 ms intervals; the 4 ms post-edit median difference is below that resolution, and the 328 ms burst difference is not statistically established. No sub-100 ms or end-to-end UX speedup claim is supported.

Proof paging fell from 5–6 pages to 3. Returned manifest member counts and encoded/inline payload sizes were similar. Across completed private profile attempts, proto33 used 15 metadata-walk calls versus 23 for proto32, returning 433 versus 407 records and 133,242 versus 124,824 raw bytes (about 7% more). Native object-range call counts were 57 in each arm. These are operation counters, not a browser critical-path analysis.

## Targeted mechanism result

A real-client ancestry fixture with 30 ms simulated transport delay reduced the relevant walk from 3 calls / 96.037872 ms to 1 call / 33.022921 ms. This controlled fixture demonstrates fewer round trips in the targeted path; it is not a production S3 or end-to-end browser measurement.

## Bounded JSON response writer

Four native response-writer correctness tests passed in the **unoptimized test profile**. Separately, an **optimized standalone synthetic microbenchmark** measured median encoding times of 6,394 to 4,251 ns for 17 KiB and 152,652 to 88,047 ns for 400 KiB, with identical output bytes. The over-limit test used a 67,108,867-byte encoded body against a 64 MiB cap: the legacy path materialized the body before rejecting it, while the bounded writer retained 1 byte before rejection. The measured capacities are output `Vec` capacities, not process RSS or total memory. The correctness tests are not performance measurements, and the benchmark is not browser or database latency.

## Bounds and provenance

The candidate source is tree `95b83b9005f3f15882e91742e5cca5e87e297abf`, patch SHA-256 `5a8cc56ef47cca7da25976711bfce4f261a4ef09279de76000a91d78fd212135`, based on `e6ddad383d7c595c350de1a4ae6ecc6871069d97`. It uses sync protocol 33 and physical storage format 87. The bounded path limits provider pages and the queued frontier to 32 records, logical ancestry walks to 64 commits, walk responses to 128 records, and aggregate walk-response bytes to 256 KiB. Checkpoint pointer and coverage collectors use separate bounded-input budgets.

The matched proto32/proto33 UI and server image IDs, SDK WASM hashes, native binary hashes, and source trees are recorded in [`bounded-proof-recovery-performance.json`](bounded-proof-recovery-performance.json). Both server arms include the bounded JSON writer, so the comparison isolates the proof-page behavior. Private profiling hooks were enabled only in the client SDK artifacts for both arms and are not shipped; the native server profile feature was disabled. The test used local MinIO on bounded 8 GiB tmpfs with fresh isolated prefixes. It does not qualify production S3 behavior.

## Validation status

The matched seven-case UI suite passed 7/7 in each arm, with no unexpected or skipped cases. The native library suite passed 4,988 tests with 74 skipped; doctests passed 10/10; and the default native check passed. Browser SDK tests passed 60 with 3 skipped, OPFS passed 73, packed-browser smoke passed, and the four bounded-writer correctness tests passed in the unoptimized test profile. The optimized standalone serializer microbenchmark completed separately.

Native workspace clippy, all-features and docs checks, Node SDK tests, cloud CI, and the exact production preview remain pending. The n=3 browser result is not a speedup gate; final release qualification depends on those pending checks.

## Measurement limits

- Browser transfer-busy and summed-request-latency diagnostics include background `/sync/` activity, except `/sync/events`, and can clip active intervals at final equality. They are not used as critical-path measurements.
- Private Rust profiles include completed preparation attempts only; canceled attempts are omitted. Transport counters include decode/validation, and inclusive timers are not summed into wall time or CPU.
- Three profile error-outcome events occurred per arm, one per run; the enclosing browser runs and final equality/reopen checks passed. No error-code cause is attributed.
- No request-correlated waterfall establishes that object-range work is the browser critical chain.
- Serializer benchmark capacities do not measure allocator-wide memory or RSS.

Evidence artifact filenames and SHA-256 digests are listed in the companion JSON; the artifacts themselves are not embedded here.
