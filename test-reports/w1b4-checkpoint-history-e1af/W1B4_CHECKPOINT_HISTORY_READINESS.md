# W1b-4 checkpoint/history reconstruction readiness

## Anchor and decision boundary

The package is anchored directly to `e1af471b9ab0f598dafa7c2ddec7867667c81740`
(tree `bfa0d271a723da8250ab76ada16fda90926f1099`, parent
`b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`). The package itself is
TEST/REPORT-only. It is not an implementation and does not qualify an
adapter.

The v3 correction is bound to the independent blocker report SHA
`87b94fe452eecaeea107c5e03b3311e06f192d1f06b5aa288e0ddea5a084fc78`.

The current source already has a useful ForkTree checkpoint-history reader:
`ForkTreeReadFacade::checkpoint_history_from_head` walks authenticated
commit records through one retained read, treats the parentless root as an
implicit checkpoint, authenticates marker rows against the commit being
walked, and rejects malformed/null/wrong-branch/wrong-commit markers. The
remaining readiness RED is ownership at the transaction checkpoint-selection
boundary: the current path obtains `forktree_read_facade()` separately for
history reconstruction and state-diff reconstruction. A future candidate must
make the operation graph capability explicit and share the exact retained read
without creating a second reader, snapshot, cache, or authority.

## Required semantic contract

1. One caller-owned `ForkTreeReadFacade` (and its one retained
   `StorageRead`) spans checkpoint chronology, state reconstruction, history,
   and undo/redo reads for one operation. `begin_read`, snapshot refresh, raw
   read extraction, detached graph construction, and per-provider
   reader/cache construction are forbidden.
2. The walked commit is the chronology authority. For every commit, its
   authenticated generation and first parent must agree with the walk. A
   parentless generation-zero commit is the only implicit root checkpoint.
3. A checkpoint marker is accepted only when it is an authenticated marker for
   the exact walked commit and requested branch. Missing/deleted marker means
   “not a checkpoint” for a non-root commit; null, malformed, wrong-kind,
   wrong-branch, or substituted commit identity is a typed failure. An
   authenticated state root must be present, canonical, and bound to the
   walked commit before the entry is returned.
4. Checkpoint floor is separate from chronology. A floor identifies the
   oldest retained checkpoint/replay boundary; it must not truncate the
   first-parent walk. All walked entries above and below the floor remain
   available to history/undo retention, subject to the existing operation
   contract.
5. History and undo retention are identity-preserving: commit IDs, generation,
   marker identity, state-root identity, and order survive 65 successive
   rotations and cold reopen. Equal-looking payloads do not authorize a
   substituted commit/root.
6. Missing commit/root, malformed or wrong-kind record, identity substitution,
   non-adjacent generation, cycle, missing floor, and cold-reopen corruption
   fail closed before rows, history entries, undo entries, or LIMIT output are
   exposed. No partial success, empty-success downgrade, retry, or fallback is
   allowed.
7. The checkpoint SQL provider consumes the same operation graph capability.
   Filter/order/LIMIT operate after authenticated chronology and retain exact
   historical ordering. It may not acquire a second view or hydrate a legacy
   tracked-state graph.

## Source call-chain map

The intended call chain is:

```text
transaction opening read
  -> one ForkTreeReadFacade
     -> checkpoint_history_from_head
        -> load_required_commit_record
        -> load_state_rows_at_commit / marker authentication
     -> diff_state_rows_between_commits
     -> history/undo retention reconstruction
  -> sql2/providers/checkpoint.rs row planning/filter/order/LIMIT
```

The current anchor has the relevant ForkTree implementation and a checkpoint
provider using the ForkTree reader, but its transaction checkpoint-selection
path constructs the facade more than once. That is the expected RED captured
by `verify_source_contract.sh`; no source correction is included here.

The existing checkpoint publication path in `session/checkpoint.rs`, and the
writer/GC portion of `transaction/context.rs`, are acceptance consumers only.
They are explicitly excluded from W1b-4 source changes.

## Structural source gate

The package's verifier is candidate-parametric:

```sh
verify_source_contract.sh WORKTREE BASE_COMMIT TARGET_COMMIT
```

It rejects any changed path outside the five-path production allowlist before
source inspection. It lexically masks Rust comments and literals, extracts the
complete `execute_checkpoint_selection` body using balanced braces, and binds
the receiver identity rather than counting token occurrences. A future GREEN
candidate must contain exactly one local opening-read facade binding, and the
same binding must receive both chronology and state-diff calls. The operation
body cannot construct a fresh read/facade/graph, use a raw store, or route
through a legacy reader/fallback/cache. `view.rs` and the checkpoint provider
are checked for the authenticated marker/root and ForkTree-only authority
contracts. The `--self-test` suite compiles and runs the typed structural
fixture with warnings denied. Its positive case proves one retained
StorageRead/CoherentView/facade, a complete plan, and one atomic commit; five
negative cases reject swapped aliases, fresh reads, raw/parallel graph
authority, fallback/cache/compatibility, and incomplete/duplicate commits.

The verifier does not claim that the current anchor is GREEN. The exact e1af
source RED remains the frozen two-condition calibration, while a future child
is evaluated against its real base and complete diff.

## Discriminating fixtures

The standalone model supplies five deterministic tests:

- 65 rotations over a root plus 64 marker-bearing commits, with exact
  generation/order, a checkpoint floor in the middle, and all commit IDs
  retained for history/undo replay.
- root implicit-checkpoint and absent/deleted non-root-marker controls.
- null, malformed, wrong-branch, and substituted marker controls, plus missing,
  malformed, wrong-kind, and substituted root authority controls.
- missing parent, generation gap, duplicate/reordered parent, and cyclic
  parent controls.
- one-view/one-read provider use, rejection of a duplicated reader/view, and
  cold reopen with identical chronology and retained identities.

The production successor must additionally run these controls through Memory,
RocksDB, and SlateDB only after the repository is compile-green. No adapter
command is run by this package.

## Future commands (all bounded to 1200 seconds)

Standalone package model (the only command run here):

```sh
timeout 1200s rustc --edition=2024 --test -D warnings \
  test-reports/w1b4-checkpoint-history-e1af/checkpoint_history_oracle.rs \
  -o /tmp/w1b4-checkpoint-history-oracle
timeout 1200s /tmp/w1b4-checkpoint-history-oracle --nocapture

```

Warnings-denied structural fixture:

```sh
timeout 1200s python3 \
  test-reports/w1b4-checkpoint-history-e1af/verify_source_contract.py \
  --self-test
```

Future compile/test gate after the production slice is compiler-green:

```sh
timeout 1200s cargo test -p lix --lib checkpoint_history -- --nocapture
timeout 1200s cargo test -p lix --lib checkpoint -- --nocapture
timeout 1200s cargo clippy -p lix --lib --tests -- -D warnings
timeout 1200s cargo fmt --all -- --check
timeout 1200s git diff --check
```

The adapter acceptance sequence is future-only and must remain bounded per
cell: Memory first, then one focused RocksDB cell, then one focused SlateDB
cell. Each must cover 65 rotations, cold reopen, missing/malformed/cyclic
authority, retained history/undo identity, and exact row order/limit. No
matrix widening is justified until those gates pass.

## Expected current-anchor result

The source verifier must exit non-zero with an explicit RED for the multiple
facade construction in transaction checkpoint selection. It must also report
the inherited legacy `TrackedStateStoreReader` surface as a future deletion
requirement, without pretending this package removed it. Positive checks for
the current ForkTree history reader, marker identity check, and checkpoint
provider must pass. The exact output is frozen in `EXPECTED_RED.txt`.
