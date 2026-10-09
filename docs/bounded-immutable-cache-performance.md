# Bounded immutable SlateDB cache reuse

Bounded point, prefix, and scan reads formerly disabled the immutable sidecar cache to avoid the ordinary reader's aligned 8 MiB prefetch. That coupled byte admission to cache eligibility: even a repeated read of the same immutable inputs downloaded their sidecar ranges again.

Bounded readers now use the shared exact/coalesced range planner while retaining the existing immutable cache, range identities, fetch locks, digest checks, and disk budget. Ordinary readers retain their aligned range planner. Immutable locator lengths are admitted against the operation's logical result budget before hydration; duplicate result slots count separately.

Cache reads admit the exact encoded size before allocating. They check metadata on the open file handle, read into a fixed-size buffer, check EOF, and verify the digest. Wrong-size or corrupt cache entries are soft misses and are repaired from authoritative storage. The sparse-file regression checks repair; the allocation guard is independently code reviewed, not measured by that regression.

## Local adapter profile

The real SlateDB adapter test `bounded_immutable_reads_reuse_exact_ranges_and_repair_bad_cache_files` seeds twelve 1 MiB immutable values in one sidecar and requests two separated values. Its in-memory object store adds 30 ms to immutable range requests. The point-read snapshot, requested values, and cache configuration remain fixed between cold and warm samples.

| Sample | Elapsed | Origin `get_ranges` calls | Origin bytes |
| --- | ---: | ---: | ---: |
| Cold bounded read | 31,414 µs | 1 | 2,097,184 |
| Warm 1 | 727 µs | 0 | 0 |
| Warm 2 | 594 µs | 0 | 0 |
| Warm 3 | 586 µs | 0 | 0 |
| Warm 4 | 550 µs | 0 | 0 |

The call counts measure the adapter object-store API, not individual HTTP requests. All samples return exactly the same values. Bounded prefixes, scans, and duplicate slots reuse those exact spans without origin reads. Separate checks cover duplicate-slot aggregate admission before cache/origin access, digest corruption, truncation, oversized sparse cache files, and warm reuse after the origin segment is unavailable.

This profile demonstrates bounded-to-bounded reuse and avoids the 8 MiB remote overfetch. Exact range keys generally differ from aligned keys written by ordinary readers, so this does not establish reuse of those aligned entries. Timings are a local diagnostic with simulated object-store delay, not browser end-to-end latency or a sub-100 ms collaboration claim. Deployed acceptance retains the existing peer convergence and OPFS reopen assertions.

No remote physical format, migration, or sync protocol changes are required.
