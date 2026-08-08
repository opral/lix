# Frozen acceptance contract

## Authority and operation boundary

The future reader/reconciliation operation must acquire one
operation-owned `CoherentView` / `StorageRead` and retain it through snapshot
opening, conflict reconciliation, undo/redo target calculation, and
publication planning. Branch descriptors, history selectors, and transaction
objects may borrow that same read; none may call `begin_read`, refresh, extract,
or pair a detached cache with another read.

The ForkTree rows, authenticated catalog roots, selectors, and state/control
records remain the only persisted authority. `TrackedStateStoreReader`, old
tracked-state materialized-row readers, raw storage getters, ad-hoc caches,
legacy DTOs, and physical-layout fallbacks are forbidden in the new path. A
typed unsupported publication family must fail before any plan, write,
selector/epoch mutation, or receipt. An explicit authenticated empty bootstrap
is valid; a missing selected root/row is not an empty bootstrap.

The transaction owns exactly one publication plan, one `prepare_write_set`,
and one backend `prepared_commit.commit`. ForkTree returns in-memory writes and
exact raw preconditions only. No ForkTree commit/retry or second writer is
allowed.

## Required semantics

1. Opening snapshot: authenticated global plus branch overlay, typed key order,
   and retention scope are fixed by the one view.
2. Same-owner stale publication revalidates/reconciles and succeeds. An
   unrelated-owner publication composes disjoint changes without erasing them;
   an unsafe mixed conflict fails closed.
3. Undo/redo advances first-parent chronology and durable cursor state
   atomically; a new divergent publication discards redo.
4. Concurrent reconciliation uses exact owner/key identity and never starts a
   second view or silently restarts through a legacy reader.
5. Savepoint/rollback restores staged rows, runtime state, result identity,
   and planned selectors without a backend write.
6. Idempotency receipt and precondition are in the same plan; a repeat returns
   the prior result without a second transition.
7. Checkpoint/recovery pins retain required history/undo roots until release.
8. Cold reopen reconstructs identical state and chronology. Missing,
   malformed, wrong-kind, substituted, duplicate, reordered, or undecodable
   selected authority fails closed before output or writes.

## Exact future source paths

Allowed production ownership is narrow and compiler-driven:

- `packages/lix/src/live_state/forktree_reader.rs` and the typed
  `CoherentView`/ForkTree read facade;
- `packages/lix/src/live_state/context.rs` only for operation-owned reader
  construction and global/branch overlay handoff;
- `packages/lix/src/transaction/stale_commit.rs` for owner/key conflict
  classification;
- `packages/lix/src/transaction/context.rs` and `commit.rs` for one plan,
  preconditions, rollback, and the existing commit boundary;
- `packages/lix/src/session/undo_redo.rs` for durable chronology;
- `packages/lix/src/session/execute.rs` only for existing transaction entry
  points, statement checkpoints, and one-view threading;
- existing ForkTree serving/model/view code for authenticated facts and
  in-memory publication plans.

Forbidden in this lane: tracked-state writer/reader replacement, SQL
binder/executor changes, CAS/blob layout, selector/format changes, GC
algorithms, persisted reverse indexes, caches, retries, compatibility
decoders, and a direct `PreparedPublication::commit` seam.

## Exact replay order (future runnable successor only)

Each command is a separate bounded cell. Setup is excluded. Record result/order
digests, selector/epoch changes, write-set count, and backend commit count.

### Memory

```sh
cargo test -p lix --lib session::undo_redo -- --nocapture
cargo test -p lix --lib transaction::stale_commit -- --nocapture
cargo test -p lix --lib transaction::context -- --nocapture
cargo test -p lix --lib forktree:: -- --nocapture
```

### RocksDB

```sh
cargo test -p lix_tests --test e2e \
  stale_transaction_reconciliation_undo_redo_checkpoint_reopen_rocksdb \
  -- --nocapture
```

### SlateDB

```sh
cargo test -p lix_tests --test e2e \
  stale_transaction_reconciliation_undo_redo_checkpoint_reopen_slatedb \
  -- --nocapture
```

The last two exact filters are reserved names for the future immutable
successor; it must publish runnable test names before execution. Do not infer
acceptance from compiler-red or model-only output. Run Memory, then RocksDB,
then SlateDB, stopping on the first safety blocker.
