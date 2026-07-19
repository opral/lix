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
