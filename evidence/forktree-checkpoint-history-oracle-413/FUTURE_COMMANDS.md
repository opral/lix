# Future serialized Memory → RocksDB → SlateDB commands

These commands are frozen for a future corrected candidate. They were not run
here; no production build or adapter matrix was attempted. Every cell is
serialized, capped at 20 minutes, and stops on the first failure.

The future test target is
`packages/engine-benchmarks/tests/forktree_checkpoint_history_migration.rs`.
It must use public session/branch/checkpoint/undo/redo APIs and expose only
read/view counters, not raw storage spaces.

```bash
ORACLE_ROOT=/root/repos/lix-evidence/forktree-checkpoint-history-oracle-413
ORACLE_TMP="$ORACLE_ROOT/tmp"
ORACLE_TARGET="$ORACLE_ROOT/target"
mkdir -p "$ORACLE_TMP"

# Source-only RED gates; no adapter build.
bash evidence/forktree-checkpoint-history-oracle-413/source_verifier.sh --expect-red
bash evidence/forktree-historical-failclosed-sql-413/source_verifier_413.sh --expect-red

# Pure model: chronology, recovery/floor separation, 65 rotations, cells,
# undo/redo, branch bridge, and retention roots.
rustc --edition 2021 --test \
  evidence/forktree-checkpoint-history-oracle-413/model.rs \
  -o "$ORACLE_TMP/checkpoint-history-model"
"$ORACLE_TMP/checkpoint-history-model" --nocapture

# Memory: all nine checklist groups, including 65 rotations and GC retention.
timeout 20m env CARGO_TARGET_DIR="$ORACLE_TARGET" CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks --test forktree_checkpoint_history_migration \
  --features storage-benches \
  checkpoint_history_migration_memory -- --exact --nocapture --test-threads=1

# RocksDB: identical lifecycle plus flush/drop/cold reopen and final GC.
timeout 20m env CARGO_TARGET_DIR="$ORACLE_TARGET" CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks --test forktree_checkpoint_history_migration \
  --features storage-benches \
  checkpoint_history_migration_rocksdb -- --exact --nocapture --test-threads=1

# SlateDB: identical lifecycle plus flush/drop/cold reopen and final GC.
timeout 20m env CARGO_TARGET_DIR="$ORACLE_TARGET" CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks --test forktree_checkpoint_history_migration \
  --features storage-benches,slatedb \
  checkpoint_history_migration_slatedb -- --exact --nocapture --test-threads=1
```

Each adapter test must print exact commit IDs, first-parent lists, recovery
pair, merge base, checkpoint count, retained-root set, and one-read counters.
It must fail before an empty result on missing/corrupt commit/root data and
must distinguish valid absence, null, tombstone, and value.
