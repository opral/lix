# E17: explain warm-history timing and accept scoped file indexes

**Accepted: the combined scoped-index implementation from E16.** E17 revises the earlier rejection after identifying a CPU-performance-state confound, confirming it without instrumentation, and completing the previously inconclusive controls. It is a validation follow-up to the same implementation, not five additional optimization successes. The accepted change resets the consecutive non-improvement count to **0**.

## What the investigation found

The earlier 25% warm-history difference was real elapsed time on this host, but it did not reflect more measured query work. In all **600 diagnostic warm queries**, both implementations perform exactly 56 node decodes, 11 tree-chunk reads, 70 measured storage calls, 78 keys, 43,107 returned bytes, and zero root replays. No native fetch occurs during covered warm reads. Decode-time difference is only about 18 microseconds; measured I/O differs by about 58 microseconds. Neither explains the roughly 1.54 ms total gap.

The host runs an AMD Ryzen 9 9950X with the Linux `powersave` governor. Hardware counters localize the dominant difference to execution conditions: E16 executes fewer instructions and cycles in the measured warm-read region, yet its slower runs have substantially fewer user cycles per CPU-second. Matching CPU affinity and conditioning removes the gap. This is consistent with the much cheaper preceding file lookup leaving the CPU in a different performance state; the kernel's exact frequency-selection policy is not inferred from timings alone.

| Diagnostic condition after 25 ms-latency hydration | Baseline / E16 steady median | Baseline / E16 user cycles per CPU ns |
|---|---:|---:|
| Natural placement | 5.794 / 7.253 ms | 5.542 / 4.347 |
| Both pinned to CPU 2 | 5.867 / 7.253 ms | 5.502 / 4.355 |
| CPU 2, equal arithmetic conditioning | 5.813 / 5.738 ms | 5.490 / 5.489 |

Five alternating pairs per hardware condition; each region includes 20 warm SQL calls plus identical result assertions and diagnostic collection. Counts are region totals, not SQL-only instruction counts. The final column is user-mode cycles divided by total task CPU time, a GHz-equivalent diagnostic rather than a literal hardware-frequency reading. All counters report 100% enabled coverage. E16 uses 1.1–1.6% fewer instructions in the natural/pinned conditions and 1.3–5.7% fewer cycles; it does not add the work implied by a naive interpretation of elapsed time.

Two additional controls support the finding: with 100 ms between reads, both variants converge near 8 ms (8.134 / 8.024 ms). After zero-latency hydration, both converge near 5.78 ms (5.779 / 5.778 ms). Repeated reads from one replica are correlated: statistics use one per-replica steady median, not 20 independent samples.

## Uninstrumented confirmation

Matching normal `server-protocol` builds, identical harness and persisted synthetic fixture, real HTTP with 25 ms injected latency, and the same CPU affinity. Arithmetic conditioning lasts 500 ms before the warm section and does not read Lix data. It is excluded from query timers and is a **benchmark control only**. No CPU spin, affinity setting, or governor change is added to Lix.

| Control | Pairs | Baseline → E16 median | Paired improvement, 95% interval |
|---|---:|---:|---:|
| Unconditioned first covered read | 10 | 5.9300 → 7.2795 ms | −22.96% [−25.31%, −20.53%] |
| Unconditioned steady reads | 10 | 5.7530 → 7.1610 ms | −24.49% [−25.22%, −24.17%] |
| Conditioned first covered read | 20 | 7.7570 → 7.3605 ms | +3.77% [−0.14%, +12.53%] |
| Conditioned steady reads | 20 | 5.7760 → 5.6805 ms | +1.54% [+1.17%, +1.83%] |

The first conditioned read is noisier than steady execution; its interval does not establish a speedup, but excludes the 10% material-regression bound. The initial 10-pair interval was inconclusive; all original pairs were retained when adding 10 more. These controls correct the attribution of E16's warm-history result. They do not erase the natural workload measurements or promise identical CPU behavior in browsers or other machines.

## Accepted implementation and remaining costs

Finite canonical file-ID predicates keep the filesystem index path, but scope the index to selected files and their directory ancestors. Scoped indexes use the existing revision cache, keyed by IDs, branches and projection. Exact parent identities are batched at each frontier. An internal parent-closure operation resolves the visible domain once for the walk, and root reads reuse the tracked-state context already supplied to the hot-state context.

The semantic `FilesystemPaths` recipe retains the selected IDs so candidate preparation includes future matches and changed ancestry. Scoped indexes are evicted on filesystem revision changes. Existing complete-index delta updates remain available; transaction overlays still build a complete index. Noncanonical ID literals and exact-path selection retain their previous routes. All public reads remain ordinary awaited SQL with transparent hydration.

| Cold finite-ID lookup vs E14, zero injected latency | Pairs | Paired improvement, 95% interval | Native requests |
|---|---:|---:|---:|
| dense64 | 20 | +29.08% [+27.36%, +30.00%] | 17 → 12 |
| 1,600 unrelated directories | 10 | +91.41% [+91.22%, +91.54%] | 62 → 16 |
| 16,000 files | 10 | +99.56% [+99.54%, +99.56%] | 256 → 15 |
| 16 ancestors | 20 | +17.14% [+14.78%, +19.34%] | 18 → 13 |
| 64 ancestors | 20 | −6.09% [−7.77%, −5.13%] | 18 → 13 |

At 25 ms latency, dense64/deep16/deep64 improve 29.72%/27.01%/25.02% in 20-pair controls. Deep64's 6.09% zero-latency slowdown remains an explicit tradeoff. Warm file lookup is 2.79% slower on dense64 (interval 0.97–7.96% slower), 4.91% slower on deep16 (interval spans 0.12% faster to 8.54% slower), and 2.93% slower on deep64 (interval 0.41–4.02% slower). These remain below the material-regression bound. Other raw controls, including unfavorable values, are retained.

This is a selective-read performance improvement, **not an overall architecture simplification**: it adds optional scope to the request/cache/recipe plus a closure operation. Reusing the supplied tracked-state context avoids creating a separate cache; its existing verified-byte LRU has 4,096 entries and a 16 KiB admission cap, about 64 MiB payload excluding metadata and live references. It caches encoded bytes, not decoded nodes. No protocol or storage-format bump is added by this change.

See [E16's diagram and prototype history](../e16/README.md) for the distinction between semantic dependency recipes and the concrete hydration frontier. Batching known identities does not remove the dependency of each unknown parent on its child's row.

## Validation and provenance

- All eight query forms pass on dense64, wide16000, dirs1600 and deep64: exact/missing IDs, 16/256-ID batches, exact/missing paths, prefix, invalid ID. This completes the E16 follow-up suite that had previously been canceled.
- Twenty-pair small/deep controls retain the original ten pairs and all added pairs. Source, binaries, fixtures, mode, result count and history limit stay matched. Every run compares complete ordered rows against the authority and verifies covered reads offline.
- The accepted engine patch is byte-identical to E16 combined: SHA256 `b2323b6011521d1cae4d0e18e283760c5c9f6a9faa542ba049456f32a1e1edbb` against engine base `0b46fb063ac28610cef9ee11d9dc528fdcef7eb5`. All 670 tracked engine files were compared against the previously validated candidate. The controlled E14/E16 experiments both used release metadata 0.16.1; integration has 0.17.0 metadata and receives a separate full engine gate.
- The final integration gate passes **4,331 engine tests**, 84 skipped, with `all-simulations,server-protocol`, plus **10 doc tests** and scoped formatting. Existing partial publication/reopen tests verify ancestor renames and new matches through atomic preparation/publication. No new production instrumentation is added.
- A benchmark-control bug was fixed: this `perf` build NUL-terminates ACK messages. The first harness rejected that framing after its first measured region; all `e17-perf-v1-*` files are retained and excluded from summaries. The corrected parser trims NUL and whitespace, and all 40 subsequent hardware runs pass.
- No compilers or tests run during timed matrices. Main-workspace integration tests force source timestamp freshness to avoid the shared-target artifact issue found in E16. Canonical benchmark source is restored after each build. Binary hashes come from actual compiler artifacts, not guessed executable paths. Target paths in artifact metadata can later be replaced by Cargo; verify the recorded hash before reuse.

`manifest.json` checksums the retained evidence. Scripts contain the original local research/worktree paths; adapt these to isolated checkouts before reproducing. Export a synthetic fixture once with the retained harness and use the same snapshot for both variants. Hardware/idle/conditioning diagnostics are separate from uninstrumented acceptance measurements. Natural workflow timing, hardware work counts, and matched-state timing answer different questions and should be reported together.
