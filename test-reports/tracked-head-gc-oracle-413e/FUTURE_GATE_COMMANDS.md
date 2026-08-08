# Exact future Memory -> RocksDB -> SlateDB order

No production build matrix or adapter runtime is run for the 413e calibration.
For a runnable candidate, stop at the first failure; each adapter cell is capped
at 20 minutes.

```bash
export CANDIDATE_ROOT=/path/to/frozen-candidate-worktree
export ANCHOR=413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d
export WHOLE_GATE=0b9ab47f7ac7da600b215d0b8aa2ab04db6bd36d
export ORACLE="$CANDIDATE_ROOT/test-reports/tracked-head-gc-oracle-413e"
export EVIDENCE=/root/repos/lix-evidence/tracked-head-gc-oracle-runtime
mkdir -p "$EVIDENCE"
```

## Source and pure model

```bash
bash "$ORACLE/verify_gc_migration_source.sh" \
  "$CANDIDATE_ROOT" "$ANCHOR" "$WHOLE_GATE" 413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d \
  >"$EVIDENCE/00-source.log" 2>&1
rustc --edition=2021 --test "$ORACLE/gc_migration_model.rs" \
  -o "$EVIDENCE/gc-migration-model"
"$EVIDENCE/gc-migration-model" >"$EVIDENCE/01-model.log" 2>&1
```

The source gate must be GREEN and the model must pass before adapters.

## Memory first

```bash
CARGO_TARGET_DIR="$EVIDENCE/target-memory" \
  cargo test -p engine-benchmarks --test tracked_head_gc_migration_oracle -- \
  memory --nocapture >"$EVIDENCE/02-memory.log" 2>&1
```

Require selector/catalog generation resolution, branch/global roots, same-view
epoch/progress fences, publication-first and GC-first races, stale owners,
65-entry drain, no-spin debt, corruption, no-op, and cold reopen.

## RocksDB second

Only after Memory exits 0:

```bash
CARGO_TARGET_DIR="$EVIDENCE/target-rocksdb" \
  cargo test -p engine-benchmarks --test tracked_head_gc_migration_oracle -- \
  rocksdb --nocapture >"$EVIDENCE/03-rocksdb.log" 2>&1
```

Use a fresh RocksDB fixture, flush/drop, cold reopen, exact live-set digest,
progress sequence, and backend commit/fence counters.

## SlateDB third

Only after RocksDB exits 0:

```bash
CARGO_TARGET_DIR="$EVIDENCE/target-slatedb" \
  cargo test -p engine-benchmarks --test tracked_head_gc_migration_oracle -- \
  slatedb --nocapture >"$EVIDENCE/04-slatedb.log" 2>&1
```

Use a fresh object-store fixture, explicit flush/drop, cold reopen, and the
same root, progress, race, corruption, and 65-entry assertions.

## Evidence closure

```bash
sha256sum "$EVIDENCE"/*.log "$EVIDENCE/gc-migration-model"
git -C "$CANDIDATE_ROOT" diff --check
git -C "$CANDIDATE_ROOT" rev-parse HEAD^{tree}
```

Do not widen to init, transaction, SQL, current-main comparisons, or
performance measurements from this first GC oracle.
