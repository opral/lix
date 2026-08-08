# Exact future gate order

No adapter build or runtime is run for the b59 calibration. Each future cell
uses an immutable candidate, isolated target/evidence directories, and a
20-minute cap. Stop at the first failure.

## Required source-edit order before compilation

The candidate must demonstrate these ordered source cuts:

1. Direct SQL entity, typed-PK, and columnar paths are unchanged; any diff
   touching the four excluded paths fails immediately.
2. Transaction opening, reconciliation, stale-writer, savepoint/rollback,
   selected-history, and publication callers use one retained CoherentView.
   Delete reader factories and merge callback wrappers after their callers
   move; no second read, cache, wrapper, or fallback is accepted.
3. Init, schema resolution, and SQL working-diff consumers use ForkTree
   selector/state/catalog roots.
4. GC and current-generation consumers use authenticated ForkTree roots and
   one epoch/progress fence.
5. Test-support and engine-benchmark consumers stop recreating legacy state.
6. Delete TrackedHead modules/reexports, marker spaces, cache symbols, and
   old factories. Only then run the first compiler/no-run gate.

The source gate and negative consumer must fail if this order is violated:
legacy names or paths remain, the direct consumer resolves, a merge callback
reopens a reader, excluded SQL paths change, or a corrupt/unsupported/no-op
cohort creates durable work.

## Reproducible source gate and direct negative consumer

```bash
export CANDIDATE_ROOT=/path/to/frozen-candidate-worktree
export ANCHOR=b59e1f11a51153e0a787a81f0f25bf104d150aaf
export ORACLE="$CANDIDATE_ROOT/test-reports/tracked-head-whole-module-oracle-b59-corrected"
export EVIDENCE=/path/to/isolated-evidence
mkdir -p "$EVIDENCE"

bash "$ORACLE/verify_whole_module_source.sh" "$CANDIDATE_ROOT" "$ANCHOR" \
  >"$EVIDENCE/source.log" 2>&1

rustc --edition=2021 --test "$ORACLE/whole_module_contract_model.rs" \
  -o "$EVIDENCE/whole-module-model"
"$EVIDENCE/whole-module-model" >"$EVIDENCE/model.log" 2>&1

CARGO_TARGET_DIR="$EVIDENCE/target-negative" cargo build -p lix --lib
lix_rlib=$(find "$EVIDENCE/target-negative/debug/deps" -maxdepth 1 \
  -name 'liblix-*.rlib' -print | sort | tail -n 1)
test -n "$lix_rlib"
set +e
rustc --edition=2021 --crate-name obsolete_tracked_head_consumer \
  "$ORACLE/obsolete_consumer.rs" \
  --extern "lix=$lix_rlib" \
  -L dependency="$EVIDENCE/target-negative/debug/deps" \
  -o "$EVIDENCE/obsolete-consumer" \
  >"$EVIDENCE/negative-consumer.log" 2>&1
negative_status=$?
set -e
test "$negative_status" -ne 0
rg 'tracked_head|TrackedHeadContext|TRACKED_WORKING_DIFF_MARKER_SPACE' \
  "$EVIDENCE/negative-consumer.log"
```

The direct consumer must fail for the deleted module/type/reexport/space, not
because a Cargo test target is absent. The source gate must be GREEN on the
candidate while the negative consumer remains a genuine unresolved-symbol
failure.

## Memory → RocksDB → SlateDB

```bash
CARGO_TARGET_DIR="$EVIDENCE/target-memory" \
  cargo test -p lix_benchmarks --test tracked_head_whole_module_oracle -- \
  memory --nocapture

CARGO_TARGET_DIR="$EVIDENCE/target-rocksdb" \
  cargo test -p lix_benchmarks --test tracked_head_whole_module_oracle -- \
  rocksdb --nocapture

CARGO_TARGET_DIR="$EVIDENCE/target-slatedb" \
  cargo test -p lix_benchmarks --test tracked_head_whole_module_oracle -- \
  slatedb --nocapture
```

Only after the preceding cell is green may the next adapter run. Every adapter
must cover transaction working-diff/reconciliation, init publication,
deterministic generation, schema overlay/rollback, SQL working_diff, GC
shared/final roots and branch-first/GC-first races, corruption, and
flush/drop/cold reopen. Require one coherent view; one plan/write-set/commit
for publication; and zero writes/publication for no-op, unsupported, and
fail-closed cohorts.

The candidate must not touch the separate public-SQL entity/PK/columnar lane.
