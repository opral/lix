# ForkTree authenticated OLAP range/projection correction

## Verdict

**BLOCKER under the strict all-metrics gate, with a recommended implementation cut.** The single correction removes the dominant row-at-a-time I/O and full-row projection terms and beats current main materially on wall, CPU, allocation, logical/physical bytes, and disk at 10K and 50K. At 50K, however, SlateDB needs six physical reads per one-range query versus five on current main (twelve versus ten for the two-range join). The absolute extra authenticated path-level read is a 20% object-count regression, so this model does not claim unconditional acceptance under the required `<=5%` critical-regression rule.

The result remains source-equivalent rather than SQL-level: exact current main uses public SQL/DataFusion; the ForkTree prototype has no SQL/DataFusion provider and applies identical deterministic operators to authenticated source materialization.

## Authority and algorithm

One `StorageRead` snapshots the branch selector, immutable commit, complete traversed root path, selected leaf blocks, and every distinct referenced value-pack object. Each dependency level is fetched with one `get_many`; all objects authenticate against their sole `ObjectId` before decode. Parent bounds must equal the authenticated child's actual maximum key, repeated/cyclic nodes and non-increasing output fail closed, and malformed/truncated packs are fully decoded before a projected value is exposed.

The ordinary byte range API delegates to this iterator; no old range algorithm, cache, index, second format, compatibility path, or second authority remains. Field projection runs only after complete pack authentication and structural validation, but before allocating/copying full row fields.

Current-main and corrected ForkTree logical work are `O(N + Q)`. The rejected prototype's request work was `O(N)` and repeatedly loaded each 64-row pack. Corrected request work is `O(H)` rounds with `O(B)` object keys/bytes for touched blocks, where `H` is authenticated tree height and `B` is touched blocks. Projection allocation is `O(N * selected_width)` rather than `O(N * full_row_width)`. Working memory is `O(B + output)` and is discarded with the snapshot.

## Correctness gates

- Memory 10K: all seven exact result digests pass.
- Exactly one coherent read per range (two for the join) is structurally asserted.
- Authenticated malformed, truncated, and child-bound-substituted blocks fail closed.
- RocksDB and SlateDB: every 10K and 50K sample matches exact count/digest; all seven queries match again after cold reopen.
- Timed query phases perform zero writes.

## Median A/B

Wall and allocation deltas are corrected ForkTree versus exact current main. CPU tracked wall within measurement noise.

| Rows | Adapter | Query | Current wall ms | ForkTree wall ms | Wall | Allocation | Gets | Logical bytes |
|---:|---|---|---:|---:|---:|---:|---:|---:|
| 10K | Rocks | narrow | 12.20 | 4.18 | -65.7% | -76.0% | 18 -> 6 | 406,521 -> 115,716 |
| 10K | Rocks | wide | 37.31 | 7.91 | -78.8% | -52.1% | 18 -> 6 | 1,103,765 -> 711,102 |
| 10K | Rocks | filter | 9.15 | 3.87 | -57.7% | -81.1% | 18 -> 6 | 406,470 -> 115,716 |
| 10K | Rocks | group | 10.35 | 3.45 | -66.6% | -81.7% | 18 -> 6 | 406,470 -> 115,716 |
| 10K | Rocks | order/limit | 10.67 | 3.15 | -70.5% | -81.6% | 18 -> 6 | 406,470 -> 115,716 |
| 10K | Rocks | join | 12.30 | 3.36 | -72.7% | -81.3% | 33 -> 12 | 421,374 -> 118,314 |
| 10K | Rocks | projection | 22.48 | 4.92 | -78.1% | -76.7% | 18 -> 6 | 1,103,765 -> 711,102 |
| 10K | Slate | narrow | 12.52 | 4.77 | -61.9% | -74.4% | 18 -> 6 | 406,523 -> 115,716 |
| 10K | Slate | wide | 37.30 | 8.61 | -76.9% | -51.3% | 18 -> 6 | 1,103,919 -> 711,102 |
| 10K | Slate | filter | 9.18 | 4.77 | -48.1% | -77.1% | 18 -> 6 | 406,472 -> 115,716 |
| 10K | Slate | group | 10.44 | 3.56 | -65.9% | -80.4% | 18 -> 6 | 406,472 -> 115,716 |
| 10K | Slate | order/limit | 10.70 | 3.55 | -66.9% | -80.3% | 18 -> 6 | 406,472 -> 115,716 |
| 10K | Slate | join | 12.36 | 3.78 | -69.4% | -80.4% | 33 -> 12 | 421,376 -> 118,314 |
| 10K | Slate | projection | 23.57 | 5.34 | -77.4% | -74.7% | 18 -> 6 | 1,103,919 -> 711,102 |
| 50K | Rocks | narrow | 85.69 | 17.68 | -79.4% | -74.9% | 18 -> 7 | 1,984,384 -> 572,524 |
| 50K | Rocks | wide | 222.14 | 41.76 | -81.2% | -50.3% | 18 -> 7 | 5,485,121 -> 3,581,905 |
| 50K | Rocks | filter | 66.85 | 16.41 | -75.5% | -80.2% | 18 -> 7 | 1,984,332 -> 572,524 |
| 50K | Rocks | group | 86.61 | 16.50 | -80.9% | -81.0% | 18 -> 7 | 1,984,332 -> 572,524 |
| 50K | Rocks | order/limit | 89.04 | 16.88 | -81.0% | -80.9% | 18 -> 7 | 1,984,332 -> 572,524 |
| 50K | Rocks | join | 101.35 | 17.93 | -82.3% | -80.5% | 33 -> 14 | 2,003,745 -> 576,216 |
| 50K | Rocks | projection | 145.56 | 25.92 | -82.2% | -76.0% | 18 -> 7 | 5,485,121 -> 3,581,905 |
| 50K | Slate | narrow | 85.78 | 28.91 | -66.3% | -56.9% | 18 -> 7 | 1,984,666 -> 572,524 |
| 50K | Slate | wide | 220.99 | 54.06 | -75.5% | -42.5% | 18 -> 7 | 5,485,275 -> 3,581,905 |
| 50K | Slate | filter | 69.52 | 27.49 | -60.5% | -61.9% | 18 -> 7 | 1,984,614 -> 572,524 |
| 50K | Slate | group | 87.63 | 27.61 | -68.5% | -63.4% | 18 -> 7 | 1,984,614 -> 572,524 |
| 50K | Slate | order/limit | 89.22 | 27.91 | -68.7% | -63.6% | 18 -> 7 | 1,984,614 -> 572,524 |
| 50K | Slate | join | 103.51 | 29.07 | -71.9% | -65.4% | 33 -> 14 | 2,004,028 -> 576,216 |
| 50K | Slate | projection | 146.65 | 36.72 | -75.0% | -61.0% | 18 -> 7 | 5,485,275 -> 3,581,905 |

At 10K, Slate physical reads are equal to current main at five objects per one-range query and ten for the join; bytes fall from 0.41/1.10 MB to 0.12/0.72 MB. At 50K, corrected ForkTree reads six objects (twelve for join) versus five (ten), while bytes fall from 1.99/5.49 MB to 0.60/3.61 MB. This one extra dependency-level object is the ranked residual blocker. RocksDB exposes logical but not comparable physical-read counters in this harness.

Settled disk is lower in the source model: RocksDB 0.93 vs 1.60 MB at 10K and 4.39 vs 7.61 MB at 50K; SlateDB 0.89 vs 1.55 MB and 4.48 vs 7.54 MB. These are not production storage claims because the model omits current SQL/catalog/history ownership. Query-local RSS is stable; absolute process RSS is likewise not compared across different retained fixture ownership.

## Ceiling closure and implementer contract

Against the rejected prototype, calls fall from about 10,165 to 6 at 10K and to 7 at 50K. Narrow bytes fall from 4.07 MB to 0.12 MB; wide/projection bytes from 42.09 MB to 0.71 MB. Projection allocation falls from 603/713 MB on Rocks/Slate to 9.5/10.8 MB. This realizes nearly the full measured elimination ceiling for repeated value-pack reads and full-row projection.

Ryzen-V should implement exactly this owner boundary:

1. open one coherent snapshot and resolve selector -> commit -> root without duplicating either identity;
2. fetch each reachable path level as one ordered multi-key batch, authenticate every object before decode, and validate parent bounds against child contents;
3. deduplicate value-pack ObjectIds and fetch each pack once in the same snapshot;
4. completely validate/decompress each authenticated pack, but project requested fields before allocating full rows;
5. preserve strict global key ordering and fail closed on missing, malformed, truncated, substituted, repeated, cyclic, or out-of-range references;
6. make the existing range reader delegate to this implementation; retain no row-at-a-time compatibility path;
7. resolve or explicitly accept the 50K Slate extra path-level read before production acceptance, then rerun through honest SQL/DataFusion wiring.

No production code, alternate OLAP format, cache, index, Stage 2 path, or PR is part of this model result.
