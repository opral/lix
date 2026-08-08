# W1a future gates

These commands are specifications only. They were not run for this package;
the package is intentionally test/report-only and anchored to compiler-red
e1af.

## Source gate

```sh
git worktree add --detach /tmp/lix-w1a-review <W1A_HEAD>
bash packages/lix/tests/w1a_historical_reader_readiness/verify_w1a_source_boundary.sh \
  /tmp/lix-w1a-review e1af471b9ab0f598dafa7c2ddec7867667c81740
```

Expected corrected-head result: exit 0, no `RED-*` findings, production diff
limited to the seven paths in `W1A_PRODUCTION_ALLOWLIST.tsv`, and no changes to
merge, undo/redo, typed transition, changelog/change, writer, selector, GC,
CAS, or W3-W5 paths.

## Compiler order

```sh
timeout 20m env CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
  cargo check -p lix --message-format short
timeout 20m env CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
  cargo test -p lix --lib --no-run --message-format short
```

The first compiler wave must touch only the W1a source allowlist. Use compiler
errors to remove `HistoryQuerySource.store/json_reader`, provider
`CommitGraphReader` fields, `load_history_entries`, and graph-parent helper
parameters. Do not add a compatibility adapter.

## Correctness gates

Run the focused history/provider tests on Memory, RocksDB, and SlateDB once the
candidate compiles. Each adapter must cover entity, directory, and file/plugin
history with projection variants, exact as-of routing, filter-before-limit,
stable ordering, NULL versus tombstone, source-change deduplication, cold
reopen, and typed corruption.

Required corruption cases are the rows in `NEGATIVE_FIXTURES.tsv`: missing or
malformed CommitCatalog/CommitRecord/member/state root, wrong kind, identity
substitution, missing/deleted registry or owner, conflicting duplicate source
change, and second-view/raw-source attempts. Missing authority must fail before
any row or LIMIT is returned; an authenticated absent key remains a normal
absence.

## Forbidden scope

Do not run or modify merge analysis, stale transaction/plugin reconciliation,
undo/redo, typed transitions, changelog/change provider, writers/publication,
selector/BranchRef, GC, CAS/blob layout, or W3-W5 lanes as part of W1a.
