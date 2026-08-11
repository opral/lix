# Mandatory tracked-root results — 2026-08-02

This result compares `agent/mandatory-tracked-roots` with its direct parent,
the benchmark PR at `8855e6080`. Both use the release-mode RocksDB worker on
the host documented in `BRANCH_MERGE_BASELINE_2026-08-01.md`. Values are
single isolated diagnostic samples; the benchmark's eleven-process protocol
remains required for statistical CI gates.

The implementation makes a content-addressed tracked-state root mandatory on
every commit and rejects repositories using the previous protocol. Ordinary
publication no longer enters the legacy missing-root reconstruction path;
existing-value updates path-copy bounded tree chunks and share the transaction's
immutable-node cache.

## History sweep

The workload has 10,000 live rows, 100 changed rows per side, one divergent
commit per side, eight branches, and the indicated unrelated common history.

| common commits | parent merge | rooted merge | rooted diff | rooted preview | rooted merge root phase |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 2.41 ms | 6.24 ms | 2.50 ms | 1.06 ms | 3.64 ms |
| 10 | 2.46 ms | 6.30 ms | 2.51 ms | 1.09 ms | 3.59 ms |
| 100 | 4.44 ms | 6.22 ms | 2.48 ms | 1.07 ms | 3.66 ms |
| 1,000 | 19.87 ms | 6.31 ms | 2.64 ms | 1.35 ms | 3.66 ms |
| 10,000 | 182.48 ms | 6.82 ms | 2.70 ms | 1.13 ms | 3.88 ms |

At 10,000 commits, merge is 26.8x faster than the direct parent and stays
within 1.09x of its zero-history result. Direct diff stays within 1.08x and
preview within 1.07x. Merge allocations remain bounded: 8.72 MiB at zero
history and 8.94 MiB at 10,000; sampled incremental merge RSS was 0 and
424 KiB respectively. The legacy implementation's merge analysis alone grew
from 0.93 ms to 180.62 ms over the same sweep.

The setup trace also caught a publication-side quadratic regression during
development: validating an already-rooted parent through the legacy recovery
helper took 14.61 seconds across 100 commits and made the following merge's
root phase 309.66 ms. Enforcing the new protocol invariant directly reduces
those measurements to 8.60 ms total setup root work and 3.66 ms merge root
work. Across 10,000 commits, setup root work is 590.16 ms, or about 59 us per
commit.

## Plugin and integrity qualification

With text, Markdown, JSON, CSV, and Excalidraw installed together, the warm
file worker completed preview/merge in 1.38/4.03 ms and the cold-reopen worker
in 8.62/15.79 ms. Both passed semantic and byte oracles, resolver invocation,
unaffected-owner exclusion, source isolation, graph parents, idempotence, and
close/reopen checks. These production paths also pass after removing the old
certified-packet fallback: packet storage remains payload authority, while
every certified semantic identity must now be present in its commit root.

Current-protocol reads fail closed when root metadata or a referenced chunk is
missing. Unit coverage deletes root metadata and a root chunk, verifies reads
fail, performs the explicit repair, and verifies structural diff succeeds.

All independent merge-model, historical-diff, branch-isolation, graph-parent,
preview/commit, idempotence, and close/reopen checks passed for every point.
