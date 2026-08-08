# W1b-2 stale transaction/plugin/cohort readiness package — exact e1af

Status: test/report-only, frozen for independent review. No production edit,
Cargo/build, adapter runtime, benchmark, PR, or merge was performed.

## Pinned source and scope

- Anchor commit: e1af471b9ab0f598dafa7c2ddec7867667c81740
- Anchor tree: bfa0d271a723da8250ab76ada16fda90926f1099
- Anchor parent: b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
- Source worktree: /tmp/lix-w1b2-stale-reconciliation-e1af
- Exact production allowlist: SOURCE_ALLOWLIST.md
- Package-only path: test-reports/w1b2-stale-reconciliation-e1af/

This is W1b-2 only. Merge analysis/W1b-1, W1a/changelog,
undo/redo, typed transitions, checkpoint/history, working-diff,
selector/BranchRef, writer/publication, GC, CAS/blob layout, and W3-W5 are
explicitly excluded.

## Current e1af call chain

Commit-time reconciliation begins at:

    Transaction::commit_prepared
      transaction/context.rs:1476-1527
      -> begin_read once at the commit boundary
      -> replace transaction.opening_read with the current coherent read
      -> reconcile_stale_disjoint_writes

The stale dispatcher is:

    reconcile_stale_disjoint_writes
      transaction/context.rs:750-940
      -> load current global/active branch heads through branch_ref_reader(read)
      -> reject unsupported untracked/global/multi-branch/checkpoint cohorts
      -> hydrate ordered mutation predecessors
      -> tracked_state.reader(read):842
      -> changed_identities_in_first_parent_interval
         or tracked.diff_commits(opening_head,current_head)
      -> classify_stale_commit: transaction/stale_commit.rs:34-100
      -> direct/revalidate, plugin reconcile, or unsafe conflict

The plugin reconciliation chain is:

    reconcile_stale_plugin_writes
      transaction/context.rs:942-1268
      -> tracked_state.reader(read):978
      -> base/current plugin-owner rows
      -> base/current global plugin-registry row
      -> registry JSON and owner/version/schema-key validation
      -> filesystem_path_index active-branch path binding
      -> candidate base/current semantic rows
      -> ConflictRank deterministic ordering
      -> resolve_plugin_conflicts
      -> replay complete per-file replacement batches

The cohort chain is:

    commit_merged_cohort
      transaction/context/cohort.rs:136-242
      -> reconcile_cohort_files:327-500
      -> load_cohort_plugin_groups:549-658
      -> tracked readers at 386 and 572
      -> base rows, owner rows, registry, path identity
      -> deterministic frontier pairing and plugin resolution
      -> one consolidated semantic replacement per file
      -> existing transaction writer

The pure classifier in stale_commit.rs must remain semantic authority for
overlap classification. W1b-2 replaces only the historical reads feeding it;
it does not redesign the classifier, actor resolver, idempotency writer, or
commit publication.

## One retained-read authority contract

The future candidate must use one transaction-owned
ForkTreeReadFacade/CoherentView over the exact current coherent read supplied
by the commit boundary. That owner must cover:

1. active/global selector and branch-head observations;
2. opening/current commit identity and generation chronology;
3. file-owner rows and plugin registry rows;
4. plugin key, schema-key set, archive-generation, revision/change identity;
5. candidate base/current semantic rows and authenticated payloads;
6. path/descriptors, deterministic conflict inputs, and terminal replay
   staging decisions.

The transaction-opening owner may be borrowed by branch-bound descriptors, but
no reconciliation helper may begin, refresh, clone, extract, replace, or
cross-use a read. The required invariant is one retained view/read identity
for the reconciliation operation, not a detached cache. A single commit
boundary read is allowed; nested helper reads are not.

Forbidden paths are raw HistoryQuerySource/JsonStoreReader access, a
detached TrackedStateStoreReader, a second CommitGraphReader, a fallback or
retry authority, a durable cache/index, a second plugin-owner projection, or
compatibility/dual behavior. Decoded owner/registry/row values are ephemeral
terminal inputs only.

## Required stale semantics

The future candidate must preserve:

- same-owner stale overlap versus unrelated-owner success;
- global schema/plugin-state change as a typed transaction conflict;
- missing active branch as branch-not-found, not empty success;
- owner file_id, plugin key, schema-key set, archive generation, revision, and
  change_id binding;
- registry owner/generation agreement and invalid JSON as typed invalid-plugin;
- base/current semantic row identity, payload, NULL, tombstone, and absence;
- deterministic ConflictRank and plugin conflict ordering;
- ordinary INSERT revalidation and unsafe non-plugin overlap behavior;
- complete-set journal predecessor certification;
- idempotency replay as one terminal success with no duplicate publication;
- one consolidated per-file replay batch and the existing one-commit writer;
- missing, malformed, wrong-kind, identity-substituted, or corrupt catalog,
  owner, registry, row, path, or payload authority failing closed before
  partial reconciliation.

The model's read trace rejects multiple begin events, multiple reader
instances, or cross-view event identities. stale_reconciliation_oracle.rs is
not a production implementation and is intentionally not compiled or run in
this task.

## Expected exact-e1af RED calibration

verify_source_contract.sh is source-only and intentionally exits 1 on exact
e1af. It must identify:

1. stale disjoint reconciliation's legacy tracked-state reader;
2. stale plugin reconciliation's legacy tracked-state reader;
3. cohort reconciliation's legacy tracked-state reader;
4. cohort owner/version discovery through legacy projected batch loading;
5. plugin owner/version/revision discovery through legacy projected batch
   loading.

Opening retained ForkTree facade, pure stale classifier, commit boundary, and
deterministic plugin resolver are positive controls. EXPECTED_RED.txt captures
the exact output. This is a readiness RED only; no compiler or adapter result
is inferred.

## Compiler-driven deletion order

1. Add or verify the smallest ForkTree authenticated owner/version/revision,
   exact-row, registry, and payload operations. Do not touch merge analysis or
   delete the shared legacy reader while W1b-3 still needs it.
2. Convert reconcile_stale_disjoint_writes to borrow the one retained facade;
   preserve branch-head/global-head checks and classify_stale_commit.
3. Convert stale plugin owner/registry/row/path reads and preserve the
   deterministic resolver, actor retirement, and complete per-file replay.
4. Convert cohort group loading and frontier replay over the same operation
   view; preserve one consolidated batch and the existing writer.
5. Only after compiler reachability proves no W1b-2 caller remains, remove
   W1b-2-specific reader plumbing. Do not remove the shared
   TrackedStateStoreReader itself until W1b-1, W1b-3, and later cohorts are
   independently closed.

## Future commands, each capped at 1200 seconds

Run only on an immutable candidate after the source/compiler gate:

    timeout 1200s test-reports/w1b2-stale-reconciliation-e1af/verify_source_contract.sh "$PWD" HEAD e1af471b9ab0f598dafa7c2ddec7867667c81740

    timeout 1200s rustc --edition=2024 --test test-reports/w1b2-stale-reconciliation-e1af/stale_reconciliation_oracle.rs -o /tmp/w1b2-stale-reconciliation-oracle
    timeout 1200s /tmp/w1b2-stale-reconciliation-oracle

    timeout 1200s cargo test -p lix stale_commit --lib
    timeout 1200s cargo test -p lix reconcile_stale --lib
    timeout 1200s cargo test -p lix cohort --lib
    timeout 1200s cargo test -p lix --lib --features slatedb reconcile_stale

The future adapter order is Memory/default, exact RocksDB, then SlateDB.
Focused controls must include same-owner stale conflict, unrelated-owner
success, global/plugin-generation change, owner/registry/path corruption,
NULL/tombstone/absence, deterministic repeated resolution, idempotency replay,
cohort consolidation, cold reopen, and zero second reads. No broad matrix is
authorized before the source/compiler gate passes.

## Review boundary

This is a readiness package, not production approval. A future candidate is
blocked by any source path outside the allowlist, merge/W1a/W1b-3+ widening,
second read/view, raw or detached reader, cache/index/fallback/compatibility
authority, changed writer/publication semantics, partial corruption success,
nondeterministic plugin resolution, duplicate idempotency publication, or
loss of same-owner versus unrelated-owner discrimination.
