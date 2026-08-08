# Exact future gate order

No adapter build or runtime is run for the b59 calibration. Once a runnable
candidate exists, execute these commands in order and stop at the first
failure. Each adapter cell is capped at 20 minutes.

```bash
export CANDIDATE_ROOT=/path/to/frozen-candidate-worktree
export ANCHOR=b59e1f11a51153e0a787a81f0f25bf104d150aaf
export ORACLE="$CANDIDATE_ROOT/test-reports/tracked-head-whole-module-oracle-b59"
export EVIDENCE=/root/repos/lix-evidence/tracked-head-whole-oracle-runtime
mkdir -p "$EVIDENCE"
```

## Source and pure model

```bash
bash "$ORACLE/verify_whole_module_source.sh" "$CANDIDATE_ROOT" "$ANCHOR" \
  >"$EVIDENCE/00-source.log" 2>&1
rustc --edition=2021 --test "$ORACLE/whole_module_contract_model.rs" \
  -o "$EVIDENCE/whole-module-model"
"$EVIDENCE/whole-module-model" >"$EVIDENCE/01-model.log" 2>&1
```

The source verifier must be GREEN and the model must pass. Then run the
intentional compiler-fail consumer against the candidate's test-only target;
this command must exit non-zero and mention an unresolved old module/type or
space:

```bash
set +e
CARGO_TARGET_DIR="$EVIDENCE/target-negative" \
  cargo check -p lix --test tracked_head_whole_module_negative \
  >"$EVIDENCE/02-negative-consumer.log" 2>&1
negative_status=$?
set -e
test "$negative_status" -ne 0
```

The candidate must wire the consumer only in an external/test-only harness;
it must never become a normal passing workspace target.

## Memory first

```bash
CARGO_TARGET_DIR="$EVIDENCE/target-memory" \
  cargo test -p lix_benchmarks --test tracked_head_whole_module_oracle -- \
  memory --nocapture >"$EVIDENCE/03-memory.log" 2>&1
```

Require transaction, init, deterministic, schema, SQL, GC, corruption, race,
one-plan, zero-write, and flush/drop/reopen cases to pass.

## RocksDB second

Only after Memory exits 0:

```bash
CARGO_TARGET_DIR="$EVIDENCE/target-rocksdb" \
  cargo test -p lix_benchmarks --test tracked_head_whole_module_oracle -- \
  rocksdb --nocapture >"$EVIDENCE/04-rocksdb.log" 2>&1
```

Use a fresh RocksDB fixture and cold reopen. Verify exact result digests,
selector/root ownership, backend commit cardinality, and fail-closed corruption.

## SlateDB third

Only after RocksDB exits 0:

```bash
CARGO_TARGET_DIR="$EVIDENCE/target-slatedb" \
  cargo test -p lix_benchmarks --test tracked_head_whole_module_oracle -- \
  slatedb --nocapture >"$EVIDENCE/05-slatedb.log" 2>&1
```

Use a fresh object-store fixture, explicit flush/drop, cold reopen, and the same
digest/authority/corruption assertions. Do not widen to performance or current
main.

## Evidence closure

```bash
sha256sum "$EVIDENCE"/*.log "$EVIDENCE/whole-module-model"
git -C "$CANDIDATE_ROOT" diff --check
git -C "$CANDIDATE_ROOT" rev-parse HEAD^{tree}
```
