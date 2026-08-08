# Future adapter execution recipe (adapter cells UNRUN)

This package is report-only. The adapter commands below are the exact future
order, not results, and remain UNRUN while freezing this package.

## Common rules

* Work from a clean detached checkout of an immutable candidate based on
  `b59e1f11a51153e0a787a81f0f25bf104d150aaf`.
* Use a distinct target directory per adapter and exact candidate SHA; never
  reuse a target or binary between base/control/candidate.
* Use `timeout 20m` for every cell, `CARGO_BUILD_JOBS=2`, and save stdout,
  stderr, executable hashes, input digests, result digests, and environment
  metadata under `/root/repos/lix-evidence/forktree-history-independence-b59/`.
* Run the pure model first, then Memory, RocksDB, and SlateDB. Do not run
  50K/500K before the focused 10K pair and corruption cells pass.
* No current-main performance comparison is part of this oracle.

## Pure model (standalone correction gate: PASS 6/6)

```bash
rustc --edition=2021 --test \
  test-reports/forktree-history-independence-b59/history_independence_model.rs \
  -o /root/repos/lix-evidence/forktree-history-independence-b59/model-test
timeout 20m /root/repos/lix-evidence/forktree-history-independence-b59/model-test \
  --nocapture
```

This standalone model command was run for the correction only; it does not
open Lix storage or assert adapter behavior. The observed executable SHA-256
was `f6db5617abd2109d3601e10229b996ac09e6083de504644fa54c39ffe1229310`.

## Adapter order (UNRUN)

The future landing harness must provide these exact test names; the commands
are intentionally compile/runtime gates for the next owner and are not
pretended to work on compiler-red b59:

```bash
BASE=b59e1f11a51153e0a787a81f0f25bf104d150aaf
EVID=/root/repos/lix-evidence/forktree-history-independence-b59

timeout 20m env CARGO_TARGET_DIR=/root/repos/target-history-b59-memory \
  cargo test -p engine-benchmarks --test forktree_history_independence \
  --features memory -- --exact history_independence_10k_memory --nocapture \
  >"$EVID/memory-10k.log" 2>&1

timeout 20m env CARGO_TARGET_DIR=/root/repos/target-history-b59-rocks \
  cargo test -p engine-benchmarks --test forktree_history_independence \
  --features rocksdb -- --exact history_independence_10k_rocks --nocapture \
  >"$EVID/rocks-10k.log" 2>&1

timeout 20m env CARGO_TARGET_DIR=/root/repos/target-history-b59-slate \
  cargo test -p engine-benchmarks --test forktree_history_independence \
  --features slatedb -- --exact history_independence_10k_slatedb --nocapture \
  >"$EVID/slate-10k.log" 2>&1
```

The owner must confirm the actual package/feature names before execution
because this report intentionally does not add test wiring to production or
workspace manifests. If the named harness is absent, the command is a
compile-red control, not permission to substitute current-main performance.

## Conditional scale and artifact commands (UNRUN)

Only after all three 10K semantic/corruption/reopen cells pass, repeat the
same three commands with exact test names `history_independence_50k_*` and
then optionally `history_independence_500k_*`. Hash each executable with
`sha256sum`, hash each JSONL/log with `sha256sum`, and include settled disk
measurements after the same flush/compaction/reopen boundary. No third-party
comparator is required for this decision oracle.
