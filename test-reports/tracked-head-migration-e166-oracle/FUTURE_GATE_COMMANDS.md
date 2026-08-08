# Future execution order

This is an exact command order for the first runnable candidate. It is a
correctness oracle only: no benchmark, current-main comparison, or performance
matrix is authorized. Each adapter cell has a 20-minute cap and later cells
must not run after an earlier blocker.

Set these variables to the candidate worktree and preserved evidence location:

```bash
export CANDIDATE_ROOT=/path/to/frozen-candidate-worktree
export ANCHOR=e1666edd0b4d814a88d985086ecc5a477b5d32e6
export ORACLE="$CANDIDATE_ROOT/test-reports/tracked-head-migration-e166-oracle"
export EVIDENCE=/root/repos/lix-evidence/tracked-head-migration-e166-runtime
mkdir -p "$EVIDENCE"
```

Run the source/model gates first:

```bash
bash "$ORACLE/verify_source_contract.sh" "$CANDIDATE_ROOT" "$ANCHOR" \
  >"$EVIDENCE/00-source.log" 2>&1
rustc --edition=2021 --test "$ORACLE/tracked_head_contract_model.rs" \
  -o "$EVIDENCE/tracked-head-contract-model"
"$EVIDENCE/tracked-head-contract-model" \
  >"$EVIDENCE/01-model.log" 2>&1
```

The source gate must be GREEN and the model must pass before an adapter is
started. Hash both logs and the executable before proceeding.

## Memory first

The candidate's test-only harness target is named
`tracked_head_migration_oracle`; its Memory entry point is `memory`. The exact
future command is:

```bash
CARGO_TARGET_DIR="$EVIDENCE/target-memory" \
  cargo test -p engine-benchmarks --test tracked_head_migration_oracle -- \
  memory --nocapture \
  >"$EVIDENCE/02-memory.log" 2>&1
```

Memory must cover all model cases, one-plan counters, malformed authority,
stale/unrelated races, flush/drop/reopen, and obsolete-source negatives.

## RocksDB second

Only after the Memory command exits 0:

```bash
CARGO_TARGET_DIR="$EVIDENCE/target-rocksdb" \
  cargo test -p engine-benchmarks --test tracked_head_migration_oracle -- \
  rocksdb --nocapture \
  >"$EVIDENCE/03-rocksdb.log" 2>&1
```

The RocksDB cell must use a fresh temporary database, flush/drop, cold reopen,
and preserve exact result digests and one-plan/write counters. It must include
missing/malformed/wrong-kind corruption and branch/GC race cases.

## SlateDB third

Only after the RocksDB command exits 0:

```bash
CARGO_TARGET_DIR="$EVIDENCE/target-slatedb" \
  cargo test -p engine-benchmarks --test tracked_head_migration_oracle -- \
  slatedb --nocapture \
  >"$EVIDENCE/04-slatedb.log" 2>&1
```

The SlateDB cell must use a fresh object-store fixture, explicit flush/drop,
cold reopen, and the same digest/corruption/atomicity assertions. No hidden
cache may serve a newer view.

## Required terminal packaging

After all three cells pass, record:

```bash
sha256sum "$EVIDENCE"/*.log "$EVIDENCE/tracked-head-contract-model"
git -C "$CANDIDATE_ROOT" rev-parse HEAD^{tree}
git -C "$CANDIDATE_ROOT" diff --check
```

If any cell fails, preserve the first causal log and stop. Do not rerun a later
adapter, add a compatibility fallback, or broaden into performance work.
