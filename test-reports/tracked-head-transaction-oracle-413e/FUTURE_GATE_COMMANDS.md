# Exact future Memory -> RocksDB -> SlateDB order

No production build matrix or adapter runtime is run for the 413e calibration.
For a runnable candidate, stop at the first failure; each adapter cell is capped
at 20 minutes.

```bash
export CANDIDATE_ROOT=/path/to/frozen-candidate-worktree
export ANCHOR=413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d
export WHOLE_GATE=0b9ab47f7ac7da600b215d0b8aa2ab04db6bd36d
export ORACLE="$CANDIDATE_ROOT/test-reports/tracked-head-transaction-oracle-413e"
export EVIDENCE=/root/repos/lix-evidence/tracked-head-transaction-oracle-runtime
mkdir -p "$EVIDENCE"
```

## Source and pure model

```bash
bash "$ORACLE/verify_transaction_migration_source.sh" \
  "$CANDIDATE_ROOT" "$ANCHOR" "$WHOLE_GATE" \
  >"$EVIDENCE/00-source.log" 2>&1
rustc --edition=2021 --test "$ORACLE/transaction_migration_model.rs" \
  -o "$EVIDENCE/transaction-migration-model"
"$EVIDENCE/transaction-migration-model" >"$EVIDENCE/01-model.log" 2>&1
```

The source gate must be GREEN and the pure model must pass before adapters.

## Memory first

```bash
CARGO_TARGET_DIR="$EVIDENCE/target-memory" \
  cargo test -p engine-benchmarks --test tracked_head_transaction_migration_oracle -- \
  memory --nocapture >"$EVIDENCE/02-memory.log" 2>&1
```

Require working-diff/generation digests, savepoint/rollback, stale same-owner
and unrelated-owner rejection, selector+epoch CAS, one-plan/one-commit
counters, no-op zero writes, corruption fail-closed, and cold reopen.

## RocksDB second

Only after Memory exits 0:

```bash
CARGO_TARGET_DIR="$EVIDENCE/target-rocksdb" \
  cargo test -p engine-benchmarks --test tracked_head_transaction_migration_oracle -- \
  rocksdb --nocapture >"$EVIDENCE/03-rocksdb.log" 2>&1
```

Use a fresh RocksDB fixture and verify exact result digests, selector/root
ownership, backend commit cardinality, corruption failure, and cold reopen.

## SlateDB third

Only after RocksDB exits 0:

```bash
CARGO_TARGET_DIR="$EVIDENCE/target-slatedb" \
  cargo test -p engine-benchmarks --test tracked_head_transaction_migration_oracle -- \
  slatedb --nocapture >"$EVIDENCE/04-slatedb.log" 2>&1
```

Use a fresh object-store fixture, flush/drop, cold reopen, and the same
authority, digest, race, no-op, and corruption assertions.

## Final evidence

```bash
sha256sum "$EVIDENCE"/*.log "$EVIDENCE/transaction-migration-model"
git -C "$CANDIDATE_ROOT" diff --check
git -C "$CANDIDATE_ROOT" rev-parse HEAD^{tree}
```

Do not widen to init, GC, SQL production paths, current-main comparisons, or
performance measurements from this first migration oracle.
