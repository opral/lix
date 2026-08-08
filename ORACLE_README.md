# ForkTree TrackedState transaction/reconciliation/undo migration oracle

This dormant test/report-only package is bound to b59e1f11a51153e0a787a81f0f25bf104d150aaf
(tree 700fd04d21bc40c05425c9fc9e10d65c9e1eda24, parent
713455a3557907ce705d06f720fcdc4486bddd4a). It contains no production edit,
compatibility reader, runtime result, or build result.

## Source map

The exact b59 direct closure is checked by the sibling source verifier.

| Duty | b59 source evidence | Required owner |
| --- | --- | --- |
| Opening read | transaction/context.rs:1389-1410, 7390-7417 | one caller-owned retained StorageRead and CoherentView |
| Stale reconciliation | transaction/context.rs:839-1067, context/cohort.rs:386,572 | authenticated selected-root diff and selector/epoch precondition |
| Staged transition | transaction/context.rs:7468-7640 | state_point/state_range and authenticated commit/change edges |
| Selected history | transaction/context.rs:7506, 8116+, sql2 history providers | same view, topology and state roots |
| Checkpoint floor | transaction/context.rs:7817-7965, session/checkpoint.rs:73,103 | authenticated checkpoint root and one publication |
| Undo/redo | session/undo_redo.rs:197,299,350,416,480 | target selector/root, history identity, one publication |
| Savepoint/rollback | transaction/context.rs:1915,1928 | transaction-local overlay only |
| Idempotency | transaction/context.rs:1724 and commit path | intent-first plan and replay-safe receipt semantics |
| Generation/current state | transaction/context.rs:8926-8966, tracked_state/context.rs:893-1061 | ForkTree branch/global selectors and state tree |

Direct legacy reader use also remains in session merge analysis/branch and the
SQL checkpoint, diff, working-diff, directory-history, file-history,
filesystem-working-diff, and entity providers. The tracked_state/context.rs
reader implementation spans TrackedStateContext::reader at 905,
TrackedStateStoreReader at 977, and its read/diff/payload methods beginning at
1061. tracked_state/diff.rs imports the reader at 17 and accepts it at 350.
The source gate includes these paths and requires the owner modules to be
deleted after their production callers move.

## Replacement and semantics

Open one authenticated CoherentView from one retained read. Bind the raw
global/branch selectors, epoch, state/catalog/checkpoint roots, branch owner,
and view_id. Historical and current reads, stale reconciliation, transitions,
checkpoint selection, undo/redo, savepoint decisions, and idempotency checks
must not open a second reader or consult a cache/fallback/compatibility path.

Classify intent before making a plan. Genuine no-op and unsupported cohorts
make zero plan, prepare, write, commit, and epoch rotation. Supported work
uses exactly one PreparedPublication -> into_storage_plan ->
prepare_write_set -> commit with raw selector/epoch CAS and owner,
generation, and expected-change checks. A failure before or during publication
must leave selectors, roots, history, rows, and counters unchanged.

NULL is a value; tombstone is suppression; absence falls through from local to
global. Branch scope, parent generation, CommitId, and ChangeId are
authenticated identities. Cold reopen must validate the same root graph and
return the same rows.

## Future dormant order

Use candidate-specific targets under /root/repos and cap each cell at 1200
seconds:

1. verify_tracked_state_transaction_source.sh CANDIDATE b59e1f11a51153e0a787a81f0f25bf104d150aaf
2. cargo fmt --all -- --check and git diff --check
3. rustc --edition=2021 --test model.rs -o /root/repos/forktree-b59-model-tests
4. CARGO_TARGET_DIR=/root/repos/target-forktree-b59-memory timeout 1200 cargo test -p lix_tests --test forktree_tracked_state_transaction_oracle memory -- --exact --nocapture --test-threads=1
5. CARGO_TARGET_DIR=/root/repos/target-forktree-b59-rocks timeout 1200 cargo test -p lix_tests --test forktree_tracked_state_transaction_oracle rocksdb -- --exact --nocapture --test-threads=1
6. CARGO_TARGET_DIR=/root/repos/target-forktree-b59-slate timeout 1200 cargo test -p lix_tests --test forktree_tracked_state_transaction_oracle slatedb -- --exact --nocapture --test-threads=1

Those adapter commands are dormant recipes only. No current b59 model,
source gate, no-run, Memory, RocksDB, or SlateDB command was executed.
