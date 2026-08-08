# W1b-5 exact 4107 consumer and deletion map

Anchor: 4107bef177c00694574b4fc65d6bb209239ee877
Tree: 9f3ff98a6745daae54102a7754036ef1ced111dd
This is a source map, not runtime qualification.

## Already ForkTree-owned reader seams

- packages/lix/src/forktree/view.rs:371-547 owns
  scan_state_rows_at_commit, diff_state_rows_between_commits,
  checkpoint_history_from_head, and latest_checkpoint_for_branch. Its
  checkpoint walk validates first-parent cycles, required commit records,
  root-as-implicit checkpoint, marker branch identity, malformed/null marker
  payload, and marker commit_id equal to the walked commit_id.
- packages/lix/src/sql2/context.rs:51-95 carries a retained
  ForkTreeReadFacade in HistoryQuerySource/ChangelogQuerySource. This is the
  intended operation read, but it also carries JsonStoreReader and providers
  still receive a separate BranchRefReader.
- packages/lix/src/session/checkpoint.rs:44-210 uses one transaction
  forktree_read_facade for history and interval diff, then lowers selected
  tracked rows into checkpoint publication. It still uses public
  TrackedStateDiff* projection types; those are semantic DTOs here, not a
  read authority.

## SQL working-diff consumers

- packages/lix/src/sql2/providers/working_diff.rs:27-193 registers
  lix_working_diff and lix_working_diff_by_branch. It selects heads through
  BranchRefReader, calls latest_checkpoint_for_branch and
  diff_state_rows_between_commits, filters through TrackedStateFilter, and
  materializes rows with projection and LIMIT. Current ordering is None and
  no explicit BlobRef/payload validation is present.
- packages/lix/src/sql2/providers/filesystem_working_diff.rs:29-247 plus
  load_rows at 215. It repeats BranchRefReader head selection, checkpoint
  lookup, before/after state scans, descriptor projection, and file/directory
  filtering. Current ordering is None and content-bearing file rows do not
  authenticate one BlobRef/payload before projection.
- packages/lix/src/sql2/providers/checkpoint.rs:19-162 exposes checkpoint
  history through the retained ForkTree facade, but selects heads through the
  separate BranchRefReader and has no explicit ordered scan contract.
- packages/lix/src/sql2/context.rs:51-95 supplies the query source. Its
  retained JsonStoreReader must not become a second authority for tracked
  working-diff rows.

## Session/transaction/checkpoint consumers

- packages/lix/src/session/checkpoint.rs:44-210 is the reader-first checkpoint
  consumer. Its historical diff must remain one operation-owned view and its
  marker/base/head identities must be passed unchanged to one publication.
- packages/lix/src/transaction/context.rs:7300-7320 retains
  TrackedStateStoreReader construction/callbacks; lines 7711-7768 retain
  tracked-state checkpoint/working-diff lowering. These are deletion blockers,
  not alternate W1b-5 reader APIs.
- packages/lix/src/session/execute.rs:5562-5572 reads a baseline head through
  the old session/ref path; 6584-6642 queries public working-diff tables for
  checkpoint/replacement assertions. Public table names remain semantic
  surfaces, while the provider owner must move.
- packages/lix/src/branch/refs.rs:40-56 reads branch heads through ForkTree,
  but test/control construction at 87-193 still contains legacy control shape.

## Current-state, init, and GC owners

- packages/lix/src/live_state/context.rs:36-130 owns
  BranchHeadControlCache and TrackedHeadContext; 636-793 and related paths
  stage current state and working-diff coverage through the current layout.
- packages/lix/src/init.rs:455-496 creates TrackedHeadContext and stages the
  initial current state/working-diff epoch.
- packages/lix/src/gc.rs:114, 2532-2569, 6110-6120, and 7541-7548 retain
  TrackedHead/BranchHeadControl roots and current-layout working-diff staging.
- packages/lix/src/engine.rs:1246-1458 and SQL bind/catalog/provider
  registration keep public working-diff surfaces. These names are retained
  semantics; their owner must be ForkTree-only after migration.
- packages/lix/src/sql2/bind/table.rs:258-313 and
  packages/lix/src/sql2/catalog/registry.rs:249-313 register public
  working-diff schemas. They are facade surfaces, not permission to preserve a
  second storage authority.
- packages/lix/src/sql2/providers/mod.rs:366-425 and 894-1018 register the
  SQL/file providers. Registration remains; provider bodies must be moved.

## Dependency-ordered W1b-5 slice

1. Reader gate: pass one caller-owned retained facade plus its graph/chronology
   capability into SQL working-diff, filesystem working-diff, and checkpoint
   provider callbacks. Eliminate separate head refresh and any fresh reader.
2. Chronology gate: retain root-as-implicit behavior and require marker
   commit_id == walked commit_id, exact base/head IDs, cycle/missing/malformed/
   wrong-kind fail-closed errors.
3. Semantic gate: project tracked and untracked rows with NULL distinct from
   absence and tombstone; preserve file/directory descriptors and require
   exactly one authenticated BlobRef/payload for live content rows.
4. Query gate: preserve projection, deterministic ascending order, and LIMIT
   after identity validation; contradictory/malformed filters return empty or
   fail closed according to the public contract without partial rows.
5. Delete the provider-local TrackedState/BranchRef/current-layout readers only
   after the preceding consumer gates move. Public table/schema names remain.
6. Hand the TrackedHeadContext, BranchHeadControlCache, init/gc current-layout
   writers to their separate W3/W5 hard-cut owners; this package must not
   smuggle their deletion into a provider-only slice.

## Explicit exclusions

No writer/publication implementation, selector/epoch migration, GC owner,
CAS/upload layout, BranchHeadControl deletion, TrackedHead deletion, or
adapter/runtime qualification is approved by this package. These remain
fail-closed dependencies and are reported as blockers.
