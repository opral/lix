# Pinned Git + Git LFS workload contract

All commands run against the exact pinned checkout and a disposable local
clone/worktree derived from it. Setup/import is reported separately from
operation timings. No Lix executable, Cargo target, SQL engine, or adapter is
invoked.

## Identity and storage accounting

Capture:

```text
remote, HEAD commit, HEAD tree, first parent, git ls-tree -r digest,
git object count/size, .git directory bytes, LFS path count, LFS pointer
OID list digest, LFS object count/bytes, working-tree file count/bytes
```

The tree identity is the Git tree object ID and the complete recursive listing
is hashed separately. LFS OIDs are recorded from `git lfs ls-files --long` and
hashed as an ordered stream.

## Ordered workload cells

Each row is one independent timed cell, with raw stdout/stderr and
`/usr/bin/time -v` retained:

1. `import`: exact network clone/import command and LFS filter completion.
2. `replay`: enumerate all reachable commits/objects and batch-check object
   type/size.
3. `status`: clean working-tree status and branch metadata.
4. `history`: full reachable commit count and deterministic recent-history
   stream.
5. `branch`: create a temporary branch at HEAD and delete it.
6. `diff`: parent-to-HEAD name/status diff and stat.
7. `merge`: temporary branch at the parent, fast-forward merge to HEAD, then
   delete the temporary ref; publication is the ref update only.
8. `reopen_checkout`: fresh local no-checkout clone, detached checkout, and
   LFS checkout of the pinned commit.
9. `lfs_inventory`: LFS environment, pointer inventory, and status.
10. `lfs_fsck`: pointer/object verification.
11. `lfs_fetch`: fetch the exact pinned commit from the configured origin,
    recording whether any transfer occurred; all-history fetch is explicitly
    out of scope.
12. `gc_repack`: Git GC/repack plus `git lfs prune --dry-run`.

The trace must never call `git push`, mutate the public remote, or claim that
Git's merge/ref update is semantically equivalent to ForkTree publication.

## Optimization-target fields

Reference rows expose import/replay/status/history/branch/diff/merge/reopen/GC
latency and resource counters, Git object/read/write effects, LFS pointer and
payload counts/bytes, and exact output/tree/reuse digests. These identify
target terms for a later ForkTree comparison; they are not an acceptance gate
for Lix.
