# Performance observations

This is a living backlog for evidence-backed performance work that is larger
than a bounded, independently benchmarked pull request. Only cross-cutting
refactors expected to touch more than roughly five files belong here; a small
filter pushdown, cache, or index improvement should instead be implemented and
measured in the PR stack. Entries stay until measurement shows they are not
worthwhile, at which point their status and rationale should be updated rather
than silently removed.

## Active: incrementally maintain the filesystem path index

- **Evidence:** The path index makes indexed reads fast, but an invalidated
  branch still rebuilds descriptors from live state before the first indexed
  read can use it.
- **Potential:** Applying committed filesystem descriptor deltas to the cached
  index could turn the cold rebuild cost from `O(files)` into `O(changes)` and
  benefit many read paths at once.
- **Why it is not a small PR:** It crosses transaction staging, branch views,
  cache invalidation, and correctness recovery. It needs a clear rebuild
  fallback and memory-growth policy.
- **Next measurement:** Profile index construction, memory use, and write
  invalidation frequency on 2.5k, 10k, and 50k-file fixtures.
- **Status:** observing; deferred while targeted pushdowns remain available.

## Active: cache stable plugin reconciliation discovery beyond one batch

- **Evidence:** PR #621 removes repeated filesystem scans inside one
  multi-value write batch by reusing its reconciliation view. The same
  discovery work can still recur across independent write transactions.
- **Potential:** A correctly invalidated negative/stable discovery cache may
  improve normal small uploads substantially in plugin-free workspaces.
- **Why it is not a small PR:** Plugin installation, archive writes, explicit
  transactions, and filesystem changes all need precise invalidation. A stale
  result would be a correctness bug.
- **Next measurement:** Benchmark normal single-file and small-batch uploads
  with and without plugin files, then quantify the cache hit rate needed to
  justify the invalidation complexity.
- **Status:** observing; deferred in favor of the safe per-batch cache.

## Active: batch correlated blob-ref point reads beyond the indexed scan threshold

- **Evidence:** The bounded large-list fix in PR #630 avoids the independent
  `entity_pk × file_id` Cartesian fanout for more than 32 selected files. It
  completes a 2,048-file directory list where the old exact-filter plan
  materialized about 4.19 million identities and was OOM-killed. The current
  planner intentionally falls back above 2,048 matches, so larger lists cannot
  use that bounded fast path.
- **Potential:** A batch API for correlated `(entity_pk, file_id)` blob-ref
  lookups could use the existing tracked and mutable-state multi-get primitives
  without either an `O(n²)` identity expansion or one serial prefix scan per
  file. That could extend predictable list performance beyond the current
  threshold.
- **Why it is not a small PR:** Correctly preserving active/global/untracked
  visibility, tombstones, projection materialization, and duplicate request
  handling crosses the live-state reader, mutable index, tracked-state, and
  file provider layers (roughly six production files).
- **Next measurement:** Compare the current fallback with a correlated batch
  prototype at 2,048, 2,500, and 10,000 selected files using the real MCP
  directory-list projection and matched storage snapshots.
- **Status:** observing; deferred while the bounded prefix strategy is safe
  and materially faster for indexed lists.

## Active: runtime dynamic join-filter integration

- **Evidence:** A one-row `lix_file` → `lix_change` join cannot pass the file
  row's runtime `lixcol_change_id` into `ChangeSpec`; the provider must scan
  the global direct/derived change surface. The bounded Markdown fix instead
  issues a point `id + file_id` change lookup after reading the file.
- **Potential:** General runtime dynamic-filter support could make correlated
  joins use point reads without an application-level split query.
- **Why it is not a small PR:** It requires filter-aware `PlannedScan` /
  `SpecScanExec` plumbing plus per-provider dynamic-filter semantics and
  validation of direct versus derived change rows, touching core SQL execution
  across more than five files.
- **Next measurement:** Prototype runtime filter binding for the exact
  file-to-change join and compare observer re-execution at 2.5k, 10k, and
  50k changes.
- **Status:** observing; deferred while explicit 90%-path point lookups remain
  sufficient.
