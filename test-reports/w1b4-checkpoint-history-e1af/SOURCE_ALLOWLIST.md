# W1b-4 future source boundary

The only production paths that may change in the future W1b-4 correction are:

```text
packages/lix/src/forktree/view.rs
packages/lix/src/forktree/serving.rs
packages/lix/src/forktree/tests.rs
packages/lix/src/sql2/providers/checkpoint.rs
packages/lix/src/transaction/context.rs
```

The `transaction/context.rs` slice is restricted to the read side of
`execute_checkpoint_selection`: construct one local operation-owned
`ForkTreeReadFacade` from the transaction opening read and use it for both
checkpoint chronology and state-diff reconstruction. It must not change
publication, transaction preconditions, branch/selector state, or any writer
call.

`session/checkpoint.rs` is intentionally outside the allowlist. Its marker
publication, checkpoint write, and reachability/GC behavior are owned by the
writer/GC lanes and are only acceptance consumers here. No W1b-4 candidate may
edit it.

Forbidden production scope includes all of W1a/W1b-1/W1b-2/W1b-3,
working-diff, changelog, selector/BranchRef, writer, GC algorithm, CAS,
upload, scalar/W2-W5, SQL provider ownership, format, migration, fallback,
compatibility, or alternate authority changes. In particular, do not add a
`TrackedStateStoreReader`, `JsonStoreReader`, raw store accessor, per-provider
graph/cache, second `begin_read`, detached cursor, or retry path.

The existing `CheckpointSpec` may hold a lightweight facade clone only when
that type remains an operation-owned view over the exact retained read; a
clone must not expose or replace the read, refresh it, or become a durable
cache/authority. Future source review must prove this structurally.

The frozen verifier command is:

```sh
test-reports/w1b4-checkpoint-history-e1af/verify_source_contract.sh \
  WORKTREE BASE_COMMIT TARGET_COMMIT
```

`BASE_COMMIT..TARGET_COMMIT` must change only the five production paths above;
the verifier checks the complete diff rather than merely checking that files
exist. It then parses the target's `execute_checkpoint_selection` body and
requires exactly one `let <view> = self.forktree_read_facade()` binding, with
both `checkpoint_history_from_head` and
`diff_state_rows_between_commits` called on that exact `<view>`. It rejects a
second facade, fresh `begin_read`, `ForkTreeReadFacade::new`, graph reader,
raw store, fallback/cache, or legacy reader in the operation. The positive and
five negative fixtures are run by:

```sh
python3 test-reports/w1b4-checkpoint-history-e1af/verify_source_contract.py \
  --self-test
```

The exact anchor invocation remains byte-compatible with the original RED
calibration by passing the same e1af commit as both base and target.
